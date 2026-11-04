package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Rooter's own answer to "what actually happened on this call".
//
// Three surfaces, deliberately: /rooter/metrics for a dashboard that wants
// aggregates and cheap scraping, /rooter/events for the telemetry store that
// wants one record per call, and the existing log line for whoever is tailing.
// They carry the same field names, because three vocabularies for one event is
// how you end up not trusting any of them.
//
// The ring is bounded and in-memory on purpose. Rooter runs on a box already
// doing GPU work next to the kitchen; persisting events is the store's job, and
// a proxy that needs a disk to answer questions about itself is a proxy with a
// new failure mode.

// eventRing holds the most recent calls, newest last, with a monotonic sequence
// so a consumer can ask for "everything after N" without a clock.
type eventRing struct {
	mu      sync.Mutex
	events  []ringEvent
	seq     int64
	dropped int64
	max     int
}

type ringEvent struct {
	Seq int64       `json:"seq"`
	Rec *callRecord `json:"rec"`
}

func newEventRing(max int) *eventRing {
	if max <= 0 {
		max = 2048
	}
	if max < 16 {
		max = 16
	}
	return &eventRing{max: max}
}

func (e *eventRing) push(rec *callRecord) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	if len(e.events) == e.max {
		// Drop the oldest half rather than one at a time: shifting a full slice
		// on every call is real work on a hot path, and the half we drop was never
		// going to be read anyway.
		keep := e.max / 2
		copy(e.events, e.events[e.max-keep:])
		e.events = e.events[:keep]
		e.dropped += int64(e.max - keep)
	}
	e.events = append(e.events, ringEvent{Seq: e.seq, Rec: rec})
	return e.seq
}

// since returns up to limit events with seq > after, oldest first.
func (e *eventRing) since(after int64, limit int) ([]*callRecord, int64, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if limit <= 0 || limit > e.max {
		limit = e.max
	}
	out := make([]*callRecord, 0, limit)
	cur := after
	for _, ev := range e.events {
		if ev.Seq <= after {
			continue
		}
		out = append(out, ev.Rec)
		cur = ev.Seq
		if len(out) >= limit {
			break
		}
	}
	return out, cur, e.dropped
}

// modelStats is the per-(backend, model) aggregate. It exists because the
// single number a proxy can report without reading anything - "requests" - is
// not the number anyone actually wants, which is whether a given model on a
// given box is slow, or whether the cache is being hit at all.
type modelStats struct {
	mu      sync.Mutex
	byKey   map[string]*modelAgg
	latency *hdr
	ttft    *hdr
	prefill *hdr
	decode  *hdr
	tokens  *tokAcc
}

type modelAgg struct {
	Requests int64
	Gen      int64
	Errors   int64
	Aborted  int64
	Prompt   int64
	Complete int64
	Cached   int64
	TotalMs  float64
	TTFTMs   float64
}

func newModelStats() *modelStats {
	return &modelStats{
		byKey:   map[string]*modelAgg{},
		latency: newHDR(50e3, 20*60e3), // 50ms .. 20min, the useful window for a generation
		ttft:    newHDR(10, 10*60e3),   // 10ms .. 10min
		prefill: newHDR(1, 10*60e3),    // prefill of a long prompt is not fast
		decode:  newHDR(1, 20*60e3),
		tokens:  newTokAcc(),
	}
}

func modelKey(backend, model string) string {
	if backend == "" {
		backend = "-"
	}
	if model == "" {
		model = "-"
	}
	return backend + "|" + model
}

func (m *modelStats) observe(rec *callRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := modelKey(rec.Backend, rec.Model)
	a := m.byKey[k]
	if a == nil {
		a = &modelAgg{}
		m.byKey[k] = a
	}
	a.Requests++
	if rec.Kind == "gen" || rec.Kind == "spill" || rec.Kind == "rebind" {
		a.Gen++
	}
	if rec.Status >= 400 {
		a.Errors++
	}
	if rec.Aborted {
		a.Aborted++
	}
	a.TotalMs += rec.TotalMs
	a.TTFTMs += rec.TTFTMs
	if rec.PromptTokens != nil {
		a.Prompt += int64(*rec.PromptTokens)
	}
	if rec.CompletionTokens != nil {
		a.Complete += int64(*rec.CompletionTokens)
	}
	if rec.CachedTokens != nil {
		a.Cached += int64(*rec.CachedTokens)
	}
	m.latency.add(rec.TotalMs)
	if rec.TTFTMs > 0 {
		m.ttft.add(rec.TTFTMs)
	}
	if rec.PrefillMs != nil {
		m.prefill.add(*rec.PrefillMs)
	}
	if rec.DecodeMs != nil {
		m.decode.add(*rec.DecodeMs)
	}
	m.tokens.add(rec)
}

// ---------------------------------------------------------------------------
// histograms
// ---------------------------------------------------------------------------

// hdr is a log-linear histogram. It is not a port of the HdrHistogram algorithm
// and does not claim to be: fixed log-spaced buckets, which is all a Prometheus
// summary needs and all a process this small can afford to maintain.
type hdr struct {
	min, max float64
	factor   float64
	counts   []int64
	overflow int64
	sum      float64
	n        int64
}

func newHDR(minMs, maxMs float64) *hdr {
	// 1.5x per bucket: about 40 buckets per decade, so a reported percentile is
	// within ~50% of truth, which is plenty for "is this model slow".
	h := &hdr{min: minMs, max: maxMs, factor: 1.5}
	n := 0
	for v := minMs; v <= maxMs; v *= h.factor {
		n++
	}
	h.counts = make([]int64, n)
	return h
}

func (h *hdr) idx(v float64) int {
	if v < h.min {
		return 0
	}
	if v > h.max {
		return -1
	}
	i := int(math.Log(v/h.min) / math.Log(h.factor))
	if i < 0 {
		i = 0
	}
	if i >= len(h.counts) {
		i = len(h.counts) - 1
	}
	return i
}

func (h *hdr) add(v float64) {
	if v <= 0 {
		return
	}
	h.sum += v
	h.n++
	i := h.idx(v)
	if i < 0 {
		h.overflow++
		return
	}
	h.counts[i]++
}

// upper returns the bucket boundary at index i, in the same unit as the samples.
func (h *hdr) upper(i int) float64 {
	return h.min * math.Pow(h.factor, float64(i+1))
}

func (h *hdr) quantile(q float64) float64 {
	if h.n == 0 {
		return 0
	}
	want := int64(math.Ceil(q * float64(h.n)))
	var seen int64
	for i, c := range h.counts {
		seen += c
		if seen >= want {
			return h.upper(i)
		}
	}
	return h.max
}

func (h *hdr) mean() float64 {
	if h.n == 0 {
		return 0
	}
	return h.sum / float64(h.n)
}

// tokAcc accumulates token totals per (backend, model), so a rate can be
// computed by a dashboard without it having to sum per-call records.
type tokAcc struct {
	by map[string]*tokRow
}

type tokRow struct {
	Prompt   int64
	Complete int64
	Cached   int64
	Reqs     int64
}

func newTokAcc() *tokAcc { return &tokAcc{by: map[string]*tokRow{}} }

func (t *tokAcc) add(rec *callRecord) {
	k := modelKey(rec.Backend, rec.Model)
	row := t.by[k]
	if row == nil {
		row = &tokRow{}
		t.by[k] = row
	}
	row.Reqs++
	if rec.PromptTokens != nil {
		row.Prompt += int64(*rec.PromptTokens)
	}
	if rec.CompletionTokens != nil {
		row.Complete += int64(*rec.CompletionTokens)
	}
	if rec.CachedTokens != nil {
		row.Cached += int64(*rec.CachedTokens)
	}
}

// ---------------------------------------------------------------------------
// wiring
// ---------------------------------------------------------------------------

// observeCall is the single place a finished exchange becomes data. It is called
// from settle, which means it runs on the panic path too - the abandonment case
// is precisely the one that used to vanish.
func (r *Router) observeCall(rec *callRecord) {
	if r.cfg.NoMeta && r.cfg.Quiet {
		return
	}
	r.events.push(rec)
	r.models.observe(rec)
}

// eventsJSON serves the ring. A consumer stores the cursor it is handed back and
// asks for everything after it; the record id is the dedup key, the cursor is
// only a position.
func (r *Router) eventsJSON(w http.ResponseWriter, req *http.Request) {
	after := int64(0)
	if c := req.URL.Query().Get("cursor"); c != "" {
		v, err := strconv.ParseInt(c, 10, 64)
		if err != nil {
			http.Error(w, "cursor must be a number", http.StatusBadRequest)
			return
		}
		after = v
	}
	limit := 0
	if l := req.URL.Query().Get("limit"); l != "" {
		v, err := strconv.Atoi(l)
		if err != nil || v < 0 {
			http.Error(w, "limit must be a number", http.StatusBadRequest)
			return
		}
		limit = v
	}
	// Optional narrowing, so a runner that only cares about its own sessions does
	// not have to pull the whole box's traffic to find them.
	key := req.URL.Query().Get("key")
	backend := req.URL.Query().Get("backend")
	model := req.URL.Query().Get("model")
	failedOnly := req.URL.Query().Get("failed") == "1"

	recs, cursor, dropped := r.events.since(after, limit)
	out := make([]*callRecord, 0, len(recs))
	for _, rec := range recs {
		if key != "" && rec.Key != shortKey(key) {
			continue
		}
		if backend != "" && rec.Backend != backend {
			continue
		}
		if model != "" && rec.Model != model && rec.ModelRequested != model {
			continue
		}
		if failedOnly && rec.Status < 400 && !rec.Aborted {
			continue
		}
		out = append(out, rec)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	_ = enc.Encode(eventsPage{
		Cursor:    cursor,
		Stale:     after > 0 && dropped > 0 && len(out) == 0,
		Dropped:   dropped,
		Available: r.events.len(),
		BootID:    r.bootID,
		Events:    out,
	})
}

// eventsPage is the pull response. `stale` says the cursor asked for something
// the ring has already evicted, so a consumer that lost its place can tell
// "nothing new" apart from "you missed it" without guessing from an empty list.
type eventsPage struct {
	Cursor    int64         `json:"cursor"`
	Stale     bool          `json:"stale"`
	Dropped   int64         `json:"dropped_total"`
	Available int           `json:"available"`
	BootID    string        `json:"boot_id"`
	Events    []*callRecord `json:"events"`
}

// fmtMs keeps the text metrics readable without inventing a unit nobody asked for.
func fmtMs(v float64) string {
	if v >= 1000 {
		return strconv.FormatFloat(v/1000, 'f', 3, 64) + "s"
	}
	return strconv.FormatFloat(v, 'f', 1, 64) + "ms"
}

func (e *eventRing) len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.events)
}

// ---------------------------------------------------------------------------
// Prometheus
// ---------------------------------------------------------------------------

// modelSnapshot copies the aggregates out under the lock, so emitting text never
// holds it while a generation is finishing.
type modelSnapshot struct {
	byKey   map[string]*modelAgg
	latency *hdr
	ttft    *hdr
	prefill *hdr
	decode  *hdr
	tokens  map[string]*tokRow
}

func (m *modelStats) snapshot() modelSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := modelSnapshot{
		byKey:   make(map[string]*modelAgg, len(m.byKey)),
		latency: m.latency.clone(),
		ttft:    m.ttft.clone(),
		prefill: m.prefill.clone(),
		decode:  m.decode.clone(),
		tokens:  make(map[string]*tokRow, len(m.tokens.by)),
	}
	for k, a := range m.byKey {
		cp := *a
		out.byKey[k] = &cp
	}
	for k, row := range m.tokens.by {
		cp := *row
		out.tokens[k] = &cp
	}
	return out
}

func (h *hdr) clone() *hdr {
	cp := *h
	cp.counts = append([]int64(nil), h.counts...)
	return &cp
}

// prom writes the per-(backend, model) families. The labels are always exactly
// backend and model, so a query can group by either without parsing a string.
func (r *Router) promModels(w http.ResponseWriter) {
	s := r.models.snapshot()
	keys := make([]string, 0, len(s.byKey))
	for k := range s.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := func(name, help string, hgs ...*hdr) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
		for _, k := range keys {
			backend, model := splitModelKey(k)
			for _, hg := range hgs {
				var cum int64
				for i, c := range hg.counts {
					cum += c
					if c == 0 {
						continue
					}
					fmt.Fprintf(w, "%s_bucket{backend=%q,model=%q,le=%q} %d\n",
						name, backend, model, numStr(hg.upper(i)), cum)
				}
				fmt.Fprintf(w, "%s_bucket{backend=%q,model=%q,le=%q} %d\n",
					name, backend, model, "+Inf", hg.n)
				fmt.Fprintf(w, "%s_sum{backend=%q,model=%q} %s\n", name, backend, model, numStr(hg.sum/1000))
				fmt.Fprintf(w, "%s_count{backend=%q,model=%q} %d\n", name, backend, model, hg.n)
			}
		}
	}

	h("rooter_model_duration_seconds", "Time from request to a finished response, per backend and model", s.latency)
	h("rooter_model_ttft_seconds", "Time from request to the first response byte, per backend and model", s.ttft)
	h("rooter_model_prefill_seconds", "Prompt evaluation time reported by the runner", s.prefill)
	h("rooter_model_decode_seconds", "Token generation time reported by the runner", s.decode)

	e := func(name, help, typ string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	e("rooter_model_requests_total", "Requests proxied for a backend and model", "counter")
	for _, k := range keys {
		backend, model := splitModelKey(k)
		a := s.byKey[k]
		fmt.Fprintf(w, "rooter_model_requests_total{backend=%q,model=%q} %d\n", backend, model, a.Requests)
	}
	e("rooter_model_errors_total", "Requests that ended in an error status, per backend and model", "counter")
	e("rooter_model_aborted_total", "Generations the client hung up on, per backend and model", "counter")
	e("rooter_model_avg_ttft_seconds", "Mean time to first byte over the lifetime of the process", "gauge")
	e("rooter_model_cache_hit_ratio", "Share of prompt tokens served from the KV cache, per backend and model", "gauge")
	for _, k := range keys {
		backend, model := splitModelKey(k)
		a := s.byKey[k]
		fmt.Fprintf(w, "rooter_model_errors_total{backend=%q,model=%q} %d\n", backend, model, a.Errors)
		fmt.Fprintf(w, "rooter_model_aborted_total{backend=%q,model=%q} %d\n", backend, model, a.Aborted)
		if a.Requests > 0 {
			fmt.Fprintf(w, "rooter_model_avg_ttft_seconds{backend=%q,model=%q} %s\n",
				backend, model, numStr(a.TTFTMs/float64(a.Requests)/1000))
		}
		if row := s.tokens[k]; row != nil && row.Prompt > 0 {
			fmt.Fprintf(w, "rooter_model_cache_hit_ratio{backend=%q,model=%q} %s\n",
				backend, model, numStr(float64(row.Cached)/float64(row.Prompt)))
		}
	}
	e("rooter_model_tokens_total", "Tokens reported by the runner, per backend and model", "counter")
	for _, k := range keys {
		backend, model := splitModelKey(k)
		row := s.tokens[k]
		// A call the client hung up on never reported a count, and emitting a
		// row of zeros would make "no data" look like "used no tokens" - which
		// is the difference between a model that is cheap and one that was
		// abandoned.
		if row == nil || (row.Prompt == 0 && row.Complete == 0 && row.Cached == 0) {
			continue
		}
		for _, f := range []struct {
			kind string
			v    int64
		}{
			{"prompt", row.Prompt}, {"completion", row.Complete}, {"cached", row.Cached},
		} {
			fmt.Fprintf(w, "rooter_model_tokens_total{backend=%q,model=%q,kind=%q} %d\n",
				backend, model, f.kind, f.v)
		}
	}
}

func splitModelKey(k string) (string, string) {
	i := strings.IndexByte(k, '|')
	if i < 0 {
		return k, ""
	}
	return k[:i], k[i+1:]
}

// numStr renders a float the way the exposition format wants it, and never as
// exponent notation, which some scrapers mangle.
func numStr(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (e *eventRing) droppedTotal() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dropped
}

// summary is the per-model view inside /rooter/stats, so the same numbers are
// readable without a Prometheus scrape.
func (m *modelStats) summary() []map[string]any {
	s := m.snapshot()
	keys := make([]string, 0, len(s.byKey))
	for k := range s.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		a := s.byKey[k]
		backend, model := splitModelKey(k)
		row := map[string]any{
			"backend":     backend,
			"model":       model,
			"requests":    a.Requests,
			"generations": a.Gen,
			"errors":      a.Errors,
			"aborted":     a.Aborted,
		}
		if a.Requests > 0 {
			row["avg_total_ms"] = round1(a.TotalMs / float64(a.Requests))
			row["avg_ttft_ms"] = round1(a.TTFTMs / float64(a.Requests))
		}
		if row2 := s.tokens[k]; row2 != nil {
			row["prompt_tokens"] = row2.Prompt
			row["completion_tokens"] = row2.Complete
			row["cached_tokens"] = row2.Cached
			if row2.Prompt > 0 {
				row["cache_hit_ratio"] = round3(float64(row2.Cached) / float64(row2.Prompt))
			}
		}
		out = append(out, row)
	}
	return out
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }
func round3(f float64) float64 { return math.Round(f*1000) / 1000 }
