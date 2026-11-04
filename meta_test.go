package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// newTestSrv is a bare handler-backed backend, for the cases where the shape of
// the reply is the point of the test rather than the routing.
func newTestSrv(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }

// A runner that answers like llama.cpp does, so the metadata tests read the real
// shape rather than one rooter invented for them.
//
// The timings object is built as a map and marshalled, because these tests are
// about a key rooter has to find under two different spellings and hand-writing
// that inside a Go raw string is how you end up testing your own quotes.
func richFake(t *testing.T, name, model string) *httptest.Server {
	t.Helper()
	timings := map[string]any{
		"prompt_n":             1000,
		"prompt_ms":            2000.0,
		"prompt_per_second":    500.0,
		"predicted_n":          40,
		"predicted_ms":         800.0,
		"predicted_per_second": 50.0,
		cacheKey(name):         900,
	}
	tj := mustJSON(t, timings)
	usage := `,"usage":{"prompt_tokens":1000,"completion_tokens":40}`

	plain := `{"id":"c1","created":1793000000,"model":"` + model + `","choices":[{"message":{"content":"hi"},"finish_reason":"stop"}]` +
		usage + `,"timings":` + tj + `}`
	chunkA := `data: {"id":"c1","object":"chat.completion.chunk","model":"` + model +
		`","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}` + "\n\n"
	chunkB := `data: {"id":"c1","object":"chat.completion.chunk","model":"` + model +
		`","choices":[]` + usage + `,"timings":` + tj + `}` + "\n\n"

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"`+model+`"}]}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			io.WriteString(w, chunkA)
			fl.Flush()
			// The last chunk is where llama.cpp puts the numbers.
			io.WriteString(w, chunkB)
			io.WriteString(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, plain)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// strata spells the cache field cache_n, sushi spells it cached_n. Neither is
// wrong and neither is going to change, so the tests cover both and rooter
// folds them into one place.
func cacheKey(name string) string {
	if strings.Contains(name, "sushi") {
		return "cached_n"
	}
	return "cache_n"
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func jsonQuote(s string) string { b, _ := json.Marshal(s); return string(b) }

func TestHeadersNameTheBackendAndModelOnEveryResponse(t *testing.T) {
	s := richFake(t, "sushi", "sushi-model")
	e := newEnv(t, []string{"sushi=" + s.URL}, nil)
	e.pool.CheckAll()

	for _, tc := range []struct{ name, body string }{
		{"non-stream", `{"model":"auto","messages":[{"role":"user","content":"x"}]}`},
		{"stream", `{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, out := e.do(withSession(postJSON("/v1/chat/completions", tc.body), "hdr"))
			if res.StatusCode != 200 {
				t.Fatalf("status %d %s", res.StatusCode, out)
			}
			// The header is written before a byte of the response moves, so it can
			// only carry what rooter routed as. Naming the model that actually
			// answered is the body object's job, which is exactly why there are
			// two surfaces rather than one.
			for h, want := range map[string]string{
				"X-Rooter-Backend": "sushi",
				"X-Rooter-Model":   "auto",
				"X-Rooter-Route":   "gen",
			} {
				if got := res.Header.Get(h); got != want {
					t.Errorf("%s = %q, want %q", h, got, want)
				}
			}
			if res.Header.Get("X-Rooter-Request-Id") == "" {
				t.Errorf("no request id")
			}
			if res.Header.Get("X-Rooter-Wait-Ms") == "" {
				t.Errorf("no queue wait")
			}
		})
	}
}

// The model field is the whole reason this exists: an upstream echoes "auto"
// back, so nothing in the response says which model answered.
func TestNonStreamingBodyCarriesTheRooterObject(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "obj"))

	var doc struct {
		Model  string `json:"model"`
		Rooter struct {
			ID               string   `json:"id"`
			Backend          string   `json:"backend"`
			Model            string   `json:"model"`
			ModelRequested   string   `json:"model_requested"`
			Kind             string   `json:"kind"`
			TTFTMs           float64  `json:"ttft_ms"`
			PromptTokens     *int     `json:"prompt_tokens"`
			CompletionTokens *int     `json:"completion_tokens"`
			CachedTokens     *int     `json:"cached_tokens"`
			PrefillTokS      *float64 `json:"prefill_tok_s"`
			DecodeTokS       *float64 `json:"decode_tok_s"`
			CacheHitRate     *float64 `json:"cache_hit_rate"`
			FinishReason     string   `json:"finish_reason"`
			Extra            struct {
				CacheN *int `json:"cache_n"`
			} `json:"extra"`
		} `json:"rooter"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("body is not valid JSON after injection: %v\n%s", err, out)
	}
	// The fake echoes the real model name, so this checks rooter added a key
	// without touching the one the runner already wrote.
	if doc.Model != "qwen-model" {
		t.Errorf("the upstream's own model field was changed: %q", doc.Model)
	}
	if doc.Rooter.Backend != "strata" {
		t.Errorf("backend = %q", doc.Rooter.Backend)
	}
	if doc.Rooter.Model != "qwen-model" {
		t.Errorf("model = %q, want the model that answered", doc.Rooter.Model)
	}
	if doc.Rooter.ModelRequested != "auto" {
		t.Errorf("model_requested = %q, want what the client asked for", doc.Rooter.ModelRequested)
	}
	if doc.Rooter.PromptTokens == nil || *doc.Rooter.PromptTokens != 1000 {
		t.Errorf("prompt_tokens = %v", doc.Rooter.PromptTokens)
	}
	if doc.Rooter.CachedTokens == nil || *doc.Rooter.CachedTokens != 900 {
		t.Errorf("cached_tokens = %v", doc.Rooter.CachedTokens)
	}
	if doc.Rooter.CacheHitRate == nil || *doc.Rooter.CacheHitRate != 0.9 {
		t.Errorf("cache_hit_rate = %v, want 0.9", doc.Rooter.CacheHitRate)
	}
	if doc.Rooter.DecodeTokS == nil || *doc.Rooter.DecodeTokS != 50 {
		t.Errorf("decode_tok_s = %v", doc.Rooter.DecodeTokS)
	}
	if doc.Rooter.FinishReason != "stop" {
		t.Errorf("finish_reason = %q", doc.Rooter.FinishReason)
	}
	if doc.Rooter.Extra.CacheN == nil {
		t.Errorf("the upstream's own timings were not preserved: %v", doc.Rooter.Extra)
	}
	// The rest of the body must survive untouched.
	if !strings.Contains(out, `"content":"hi"`) {
		t.Errorf("the answer itself was damaged: %s", out)
	}
}

// Both cache spellings, one field.
func TestSushiCacheSpellingFoldsIntoTheSameField(t *testing.T) {
	s := richFake(t, "sushi", "sushi-model")
	e := newEnv(t, []string{"sushi=" + s.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "s"))
	if !strings.Contains(out, `"cached_tokens":900`) {
		t.Errorf("cached_n was not folded into cached_tokens: %s", out)
	}
}

// A stream is the user's tokens arriving. Rooter must not touch those bytes.
func TestStreamIsPassedThroughByteForByte(t *testing.T) {
	s := richFake(t, "sushi", "sushi-model")
	e := newEnv(t, []string{"sushi=" + s.URL}, nil)
	e.pool.CheckAll()

	req := withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`), "stream")
	req.URL.Scheme = "http"
	req.URL.Host = e.addr()
	res, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type %q", ct)
	}
	if res.Header.Get("X-Rooter-Streamed") != "1" {
		t.Errorf("a stream was not marked as one")
	}
	// Reading the whole body is the test: if rooter had held a chunk back to be
	// clever about metadata, the client would have seen a delay or a gap.
	all, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("stream broke: %v", err)
	}
	if strings.Contains(string(all), `"rooter"`) {
		t.Errorf("rooter injected into a live stream:\n%s", all)
	}
	if strings.Count(string(all), "data: ") != 3 {
		t.Errorf("the stream gained or lost events:\n%s", all)
	}
	// The final chunk still carries the runner's own timings, which is what a
	// streaming client reads instead of an injected object.
	if !strings.Contains(string(all), `"predicted_per_second":50`) {
		t.Errorf("the runner's timings did not survive: %s", all)
	}
	// Rooter still read the tail, so the record is complete server-side.
	recs, _, _ := e.router.events.since(0, 10)
	var found *callRecord
	for _, rec := range recs {
		if rec.Kind == "gen" {
			found = rec
		}
	}
	if found == nil {
		t.Fatal("no generation record")
	}
	if found.CompletionTokens == nil || *found.CompletionTokens != 40 {
		t.Errorf("the tail was not read: %v", found.CompletionTokens)
	}
	if found.CacheHitRate == nil || *found.CacheHitRate != 0.9 {
		t.Errorf("cache_hit_rate from the stream tail = %v", found.CacheHitRate)
	}
}

// The event ring is the telemetry store's source. It has to be pullable by
// cursor, and it has to say what it evicted.
func TestEventsRingIsPullableByCursor(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, nil)
	e.pool.CheckAll()

	for i := 0; i < 5; i++ {
		e.do(withSession(postJSON("/v1/chat/completions",
			`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "cursor"))
	}
	_, page := e.do(get("/rooter/events?limit=2"))
	var first eventsPage
	if err := json.Unmarshal([]byte(page), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 {
		t.Fatalf("limit=2 returned %d", len(first.Events))
	}
	if first.Cursor == 0 {
		t.Fatal("no cursor returned")
	}
	_, page2 := e.do(get("/rooter/events?cursor=" + itoa(first.Cursor)))
	var second eventsPage
	if err := json.Unmarshal([]byte(page2), &second); err != nil {
		t.Fatal(err)
	}
	// A cursor is a position, not a timestamp: it must not re-deliver.
	for _, rec := range second.Events {
		for _, old := range first.Events {
			if rec.ID == old.ID {
				t.Errorf("cursor %d re-delivered %s", first.Cursor, rec.ID)
			}
		}
	}
	// Ids are the dedup key, so they have to be unique.
	seen := map[string]bool{}
	_, all := e.do(get("/rooter/events"))
	var full eventsPage
	json.Unmarshal([]byte(all), &full)
	for _, rec := range full.Events {
		if seen[rec.ID] {
			t.Errorf("duplicate call id %s", rec.ID)
		}
		seen[rec.ID] = true
	}
}

// The dashboard needs the per-model families, and the labels have to be exactly
// backend and model so a query can group by either.
func TestMetricsCarryPerModelFamilies(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, nil)
	e.pool.CheckAll()
	e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "m"))

	_, m := e.do(get("/rooter/metrics"))
	for _, want := range []string{
		`rooter_model_requests_total{backend="strata",model="qwen-model"}`,
		`rooter_model_tokens_total{backend="strata",model="qwen-model",kind="prompt"} 1000`,
		`rooter_model_tokens_total{backend="strata",model="qwen-model",kind="cached"} 900`,
		`rooter_model_cache_hit_ratio{backend="strata",model="qwen-model"} 0.9`,
		`rooter_model_duration_seconds_bucket{backend="strata",model="qwen-model"`,
		`rooter_model_ttft_seconds_count{backend="strata",model="qwen-model"} 1`,
		`rooter_model_prefill_seconds_count{backend="strata",model="qwen-model"} 1`,
		`rooter_model_aborted_total{backend="strata",model="qwen-model"} 0`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// -no-meta has to mean it: no headers, no body object, no tail reading.
func TestNoMetaLeavesTheResponseAlone(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, func(c *Config) { c.NoMeta = true })
	e.pool.CheckAll()
	res, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "nm"))
	if res.Header.Get("X-Rooter-Backend") != "" {
		t.Errorf("-no-meta still set headers")
	}
	if strings.Contains(out, `"rooter"`) {
		t.Errorf("-no-meta still injected a body object: %s", out)
	}
	if out != `{"backend":"strata"}` && !strings.Contains(out, "qwen-model") {
		t.Errorf("unexpected body %s", out)
	}
}

// An upstream that already answers for itself with a rooter key is not argued
// with; the injection is conditional on the key being absent.
func TestInjectionSkipsWhenUpstreamAlreadyHasARooterKey(t *testing.T) {
	s := newTestSrv(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/health") {
			io.WriteString(w, "ok")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"data":[{"id":"m1"}]}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"model":"m1","rooter":{"mine":1}}`)
	})
	e := newEnv(t, []string{"x=" + s.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(postJSON("/v1/chat/completions", `{"model":"m1","messages":[]}`))
	if !strings.Contains(out, `"mine":1`) {
		t.Errorf("the upstream's own rooter object was replaced: %s", out)
	}
	if strings.Contains(out, "cache_hit_rate") {
		t.Errorf("rooter argued with an existing rooter key: %s", out)
	}
}

func TestTTFTIsMeasuredNotInvented(t *testing.T) {
	s := richFake(t, "slow", "slow-model")
	e := newEnv(t, []string{"slow=" + s.URL}, nil)
	e.pool.CheckAll()
	res, _ := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`), "ttft"))
	if got := res.Header.Get("X-Rooter-Wait-Ms"); got == "" || got == "0.000" {
		t.Logf("wait was %q (fine: the box was free)", got)
	}
	recs, _, _ := e.router.events.since(0, 10)
	for _, rec := range recs {
		if rec.TTFTMs <= 0 {
			t.Errorf("ttft_ms = %v, want a real measurement", rec.TTFTMs)
		}
		if rec.TotalMs < rec.TTFTMs {
			t.Errorf("total %v < ttft %v", rec.TotalMs, rec.TTFTMs)
		}
	}
	time.Sleep(5 * time.Millisecond)
}

// The join key to a runner's own per-request detail has to be the id the runner
// generated, not rooter's, or a query cannot line the two up.
func TestUpstreamIDIsTheJoinKey(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "join"))
	var doc struct {
		Rooter struct {
			ID              string `json:"id"`
			UpstreamID      string `json:"upstream_id"`
			UpstreamCreated int64  `json:"upstream_created"`
		} `json:"rooter"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Rooter.UpstreamID != "c1" {
		t.Errorf("upstream_id = %q, want the runner's own id", doc.Rooter.UpstreamID)
	}
	if doc.Rooter.ID == doc.Rooter.UpstreamID {
		t.Errorf("rooter's id and the runner's id are the same value: %q", doc.Rooter.ID)
	}
	// The runner's own timestamp is the only value its self-indexed per-request
	// detail and rooter's record reliably agree on, so it has to be captured.
	if doc.Rooter.UpstreamCreated == 0 {
		t.Errorf("the runner's own completion timestamp was not captured")
	}
	// A stream's id arrives in the first chunk, so it is captured the same way.
	req := withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`), "join2")
	req.URL.Scheme, req.URL.Host = "http", e.addr()
	res, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	recs, _, _ := e.router.events.since(0, 64)
	var found bool
	for _, rec := range recs {
		if rec.Stream && rec.UpstreamID == "c1" {
			found = true
		}
	}
	if !found {
		t.Errorf("a streamed call did not capture the runner's id")
	}
}

// A held body lets rooter publish time-to-first-byte as a header; a stream does
// not, because a stream's first byte and its headers are the same instant.
// Publishing a number that cannot be measured would be worse than publishing none.
func TestTTFTHeaderExistsOnlyWhereItCanBeMeasured(t *testing.T) {
	s := richFake(t, "strata", "qwen-model")
	e := newEnv(t, []string{"strata=" + s.URL}, nil)
	e.pool.CheckAll()

	res, _ := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "ttft-a"))
	hdr := res.Header.Get("X-Rooter-Ttft-Ms")
	if hdr == "" {
		t.Fatalf("a held response published no time-to-first-byte")
	}
	if ms, err := strconv.ParseFloat(hdr, 64); err != nil || ms <= 0 {
		t.Errorf("X-Rooter-Ttft-Ms = %q, want a positive number of milliseconds", hdr)
	}
	// The header and the object the same client also receives must be the same
	// number, or a consumer has to guess which one the other agrees with.
	res2, out2 := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "ttft-b"))
	var doc struct {
		Rooter struct {
			TTFTMs float64 `json:"ttft_ms"`
		} `json:"rooter"`
	}
	if err := json.Unmarshal([]byte(out2), &doc); err != nil {
		t.Fatal(err)
	}
	hdr2 := res2.Header.Get("X-Rooter-Ttft-Ms")
	ms2, _ := strconv.ParseFloat(hdr2, 64)
	if doc.Rooter.TTFTMs != ms2 {
		t.Errorf("header says %v ms and the body object says %v ms", ms2, doc.Rooter.TTFTMs)
	}

	req := withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`), "ttft-c")
	req.URL.Scheme, req.URL.Host = "http", e.addr()
	sres, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, sres.Body)
	sres.Body.Close()
	if got := sres.Header.Get("X-Rooter-Ttft-Ms"); got != "" {
		t.Errorf("a stream claimed a time-to-first-byte of %q that rooter cannot measure", got)
	}
}

// The live shape that broke this: strata reported a 54-token prompt, of which it
// served 49 from the KV cache and evaluated only 5. Reading prompt_n as the prompt
// length made the prompt look like 5 tokens and the cache hit rate 9.8, which is
// not a number a dashboard can use.
func TestPromptNIsTheWorkNotThePrompt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			io.WriteString(w, "ok")
			return
		}
		if r.URL.Path == "/models" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"object":"list","data":[{"id":"qwen-model"}]}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"c1","model":"qwen-model","choices":[{"message":{"content":"hi"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":54,"completion_tokens":8,"prompt_tokens_details":{"cached_tokens":49}},`+
			`"timings":{"prompt_n":5,"prompt_ms":142.8,"prompt_per_second":35,"predicted_n":8,"predicted_ms":169.2,`+
			`"predicted_per_second":47.3,"cache_n":49}}`)
	}))
	t.Cleanup(srv.Close)
	e := newEnv(t, []string{"strata=" + srv.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "pn"))
	var doc struct {
		Rooter callRecord `json:"rooter"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	rec := doc.Rooter
	if rec.PromptTokens == nil || *rec.PromptTokens != 54 {
		t.Errorf("prompt_tokens = %v, want the 54-token prompt total", deref(rec.PromptTokens))
	}
	if rec.PromptTokensComputed == nil || *rec.PromptTokensComputed != 5 {
		t.Errorf("prompt_tokens_computed = %v, want the 5 tokens the runner evaluated", deref(rec.PromptTokensComputed))
	}
	if rec.CachedTokens == nil || *rec.CachedTokens != 49 {
		t.Errorf("cached_tokens = %v, want 49", deref(rec.CachedTokens))
	}
	if rec.CacheHitRate == nil || *rec.CacheHitRate > 1 {
		t.Errorf("cache_hit_rate = %v, want 49/54 and never above 1", deref(rec.CacheHitRate))
	}
	// Prefill throughput is priced against the work done, so a warm cache cannot
	// invent speed: 5 tokens over 142.8ms is 35 tok/s, not 54/142.8.
	if rec.PrefillTokS == nil || *rec.PrefillTokS != 35 {
		t.Errorf("prefill_tok_s = %v, want 35", deref(rec.PrefillTokS))
	}
}

// A runner with no usage block at all - a plain llama.cpp /completion - still has
// to produce a prompt total, which is the work plus whatever was cached.
func TestPromptTotalIsReconstructedWhenOnlyTimingsExist(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			io.WriteString(w, "ok")
			return
		}
		if r.URL.Path == "/models" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"object":"list","data":[{"id":"qwen-model"}]}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"c1","model":"qwen-model","timings":{"prompt_n":5,"prompt_ms":142.8,`+
			`"predicted_n":8,"predicted_ms":169.2,"predicted_per_second":47.3,"cache_n":49}}`)
	}))
	t.Cleanup(srv.Close)
	e := newEnv(t, []string{"strata=" + srv.URL}, nil)
	e.pool.CheckAll()
	_, out := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "pn2"))
	var doc struct {
		Rooter callRecord `json:"rooter"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Rooter.PromptTokens == nil || *doc.Rooter.PromptTokens != 54 {
		t.Errorf("prompt_tokens = %v, want 54 reconstructed from 5 evaluated plus 49 cached", deref(doc.Rooter.PromptTokens))
	}
	if doc.Rooter.CacheHitRate == nil || *doc.Rooter.CacheHitRate > 1 {
		t.Errorf("cache_hit_rate = %v, want 49/54", deref(doc.Rooter.CacheHitRate))
	}
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// A client that only reads headers should not be locked out of the runner's own
// numbers, and a measurement nobody made must not arrive dressed as a zero.
func TestRunnerMeasurementsArePublishedAsHeaders(t *testing.T) {
	srv := fakeTimingsServer(t)
	e := newEnv(t, []string{"strata=" + srv.URL}, nil)
	e.pool.CheckAll()
	res, _ := e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","messages":[{"role":"user","content":"x"}]}`), "mh"))
	for name, want := range map[string]string{
		"X-Rooter-Completion-Tokens": "8",
		"X-Rooter-Prefill-Tok-S":     "27",
		"X-Rooter-Decode-Tok-S":      "48.2",
		"X-Rooter-Cache-Hit-Rate":    "0.9074074074074074",
	} {
		if got := res.Header.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

// An abandoned stream has no measurements to publish, and publishing nothing is
// the difference between "no data" and "the model produced nothing".
func TestAbandonedCallPublishesNoMeasurementHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			io.WriteString(w, "ok")
			return
		case "/models":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"object":"list","data":[{"id":"qwen-model"}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	e := newEnv(t, []string{"strata=" + srv.URL}, nil)
	e.pool.CheckAll()

	req := withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"x"}]}`), "mh-abandon")
	req.URL.Scheme, req.URL.Host = "http", e.addr()
	res, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close() // hang up before the tail, which is the whole point
	for _, name := range []string{"X-Rooter-Completion-Tokens", "X-Rooter-Decode-Tok-S", "X-Rooter-Cache-Hit-Rate"} {
		if got := res.Header.Get(name); got != "" {
			t.Errorf("%s = %q on a call that reported nothing", name, got)
		}
	}
}

// fakeTimingsServer replays the numbers a live strata call actually produced:
// a 54-token prompt, 49 of it served from the KV cache, 5 evaluated.
func fakeTimingsServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"qwen-model"}]}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"chatcmpl-fake","created":1793761307,"model":"qwen-model",`+
			`"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":54,"completion_tokens":8,"total_tokens":62,`+
			`"prompt_tokens_details":{"cached_tokens":49}},`+
			`"timings":{"prompt_n":5,"prompt_ms":185.2,"prompt_per_second":27,"predicted_n":8,`+
			`"predicted_ms":165.9,"predicted_per_second":48.2,"cache_n":49}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// The runner opens a stream with an empty delta, so the first byte and the first
// word are different moments and only one of them is what a reader experienced.
// Rooter has to report both or a dashboard silently graphs queue admission as
// responsiveness.
func TestFirstByteAndFirstWordAreSeparateMeasurements(t *testing.T) {
	const model = "qwen-model"
	gap := 250 * time.Millisecond

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[{"id":"`+model+`"}]}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		// What llama.cpp actually sends first.
		io.WriteString(w, `data: {"id":"c9","object":"chat.completion.chunk","model":"`+model+
			`","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`+"\n\n")
		fl.Flush()
		time.Sleep(gap)
		io.WriteString(w, `data: {"id":"c9","object":"chat.completion.chunk","model":"`+model+
			`","choices":[{"index":0,"delta":{"content":"one"}}]}`+"\n\n")
		fl.Flush()
		io.WriteString(w, `data: {"id":"c9","object":"chat.completion.chunk","model":"`+model+
			`","choices":[],"usage":{"completion_tokens":3},"timings":{"prompt_n":3,"predicted_n":3,"predicted_ms":15,"treat_toks":true}}`+"\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	e := newEnv(t, []string{"strata=" + srv.URL}, nil)
	e.pool.CheckAll()
	e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"auto","stream":true,"messages":[{"role":"user","content":"go"}]}`), "words"))

	_, page := e.do(get("/rooter/events?limit=1"))
	var got eventsPage
	if err := json.Unmarshal([]byte(page), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 {
		t.Fatalf("%d events", len(got.Events))
	}
	rec := got.Events[0]
	if rec.FirstContentMs == nil {
		t.Fatal("no first_content_ms on a streamed call")
	}
	first := *rec.FirstContentMs
	if first < 200 {
		t.Errorf("first_content_ms = %.1f, the gap was %s", first, gap)
	}
	if first <= rec.TTFTMs {
		t.Errorf("first word (%.1f) arrived no later than first byte (%.1f)", first, rec.TTFTMs)
	}
	// The runner's own throughput is unaffected by any of this, and the point of
	// keeping both numbers is that a reader can subtract one from the other.
	if rec.DecodeTokS == nil || *rec.DecodeTokS <= 0 {
		t.Errorf("decode_tok_s = %v", rec.DecodeTokS)
	}
}

// An empty delta is not content, and neither is a null one. A scan that called
// either a first word would report the opening chunk of every stream as a token.
func TestEmptyAndNullContentAreNotWords(t *testing.T) {
	for _, chunk := range []string{
		`data: {"choices":[{"delta":{"role":"assistant","content":""}}]}`,
		`data: {"choices":[{"delta":{"content":null}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"id":"1"}]}}]}`,
	} {
		if at := firstContentOffset([]byte(chunk)); at >= 0 {
			t.Errorf("%q looked like content at %d", chunk, at)
		}
	}
	for _, chunk := range []string{
		`data: {"choices":[{"delta":{"content":"x"}}]}`,
		`data: {"choices":[{"delta":{"content":"{\"json\": true}"}}]}`,
		`data: {"choices":[{"text":"x"}]}`,
	} {
		if at := firstContentOffset([]byte(chunk)); at < 0 {
			t.Errorf("%q looked like nothing", chunk)
		}
	}
	// Split across two writes is the reason the scan reads the ring and not the
	// chunk: the marker can straddle a boundary.
	whole := []byte(`prefix "content":"he`)
	if at := firstContentOffset(whole); at < 0 {
		t.Error("a marker at the end of a chunk was missed")
	}
}
