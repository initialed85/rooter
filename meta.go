package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// What a client needs to know about a call, and when rooter is able to know it.
//
// Identity is settled before a single response byte moves, so it goes in headers:
// which box answered, which model it actually is, how long the request queued,
// and why it went there. A client rendering a footer can have all of that without
// parsing a body.
//
// The per-token numbers are only knowable at the end. Rooter reads them from the
// response, but reading is not the same as rewriting: a streamed body is handed
// to the client chunk by chunk and those bytes are not rooter's to hold back or
// splice into. So a stream is passed through untouched and its tail is parsed for
// rooter's own record; a non-streaming JSON body, which is one object the client
// cannot act on until it is complete anyway, gets the rooter object added.
//
// The field names below are a contract with the model-call telemetry store, which
// reads the same record off /rooter/events. Renaming one is a cross-repo change.

// header names, all set before the response body starts.
const (
	hdrRequestID = "X-Rooter-Request-Id"
	hdrBackend   = "X-Rooter-Backend"
	hdrModel     = "X-Rooter-Model"
	hdrRoute     = "X-Rooter-Route"
	hdrWaitMs    = "X-Rooter-Wait-Ms"
	hdrStreamed  = "X-Rooter-Streamed"
)

// callRecord is one proxied exchange, as rooter saw it.
type callRecord struct {
	ID string `json:"id"`
	// UpstreamID is the chat completion id the runner generated. It is the join
	// key to a runner's own per-request detail, and the reason it lives here
	// rather than being left to the client: a runner's request log is keyed by its
	// own id, which rooter sees in the response body and a client has no way to
	// attribute back to a routed call.
	UpstreamID string `json:"upstream_id,omitempty"`
	TS         string `json:"ts"`
	Path       string `json:"path"`
	Method     string `json:"method"`
	Kind       string `json:"kind"` // read | gen | spill | rebind
	Key        string `json:"key"`
	KeySource  string `json:"key_source"`
	Backend    string `json:"backend"`
	BackendURL string `json:"backend_url"`

	// Model is what actually answered. Only rooter can say this: an upstream
	// echoes whatever the client asked for, so "auto" comes back as "auto".
	Model          string `json:"model"`
	ModelRequested string `json:"model_requested"`
	Stream         bool   `json:"stream"`

	Status  int    `json:"status"`
	Bytes   int64  `json:"bytes"`
	Aborted bool   `json:"aborted,omitempty"`
	Error   string `json:"error,omitempty"`

	WaitMs  float64 `json:"wait_ms"`
	TTFTMs  float64 `json:"ttft_ms"`
	TotalMs float64 `json:"total_ms"`

	// From the response body. Absent when the client hung up before the tail,
	// which is the whole reason these are pointers rather than zero defaults.
	PromptTokens     *int     `json:"prompt_tokens,omitempty"`
	CompletionTokens *int     `json:"completion_tokens,omitempty"`
	CachedTokens     *int     `json:"cached_tokens,omitempty"`
	PrefillMs        *float64 `json:"prefill_ms,omitempty"`
	PrefillTokS      *float64 `json:"prefill_tok_s,omitempty"`
	DecodeMs         *float64 `json:"decode_ms,omitempty"`
	DecodeTokS       *float64 `json:"decode_tok_s,omitempty"`
	CacheHitRate     *float64 `json:"cache_hit_rate,omitempty"`
	FinishReason     string   `json:"finish_reason,omitempty"`

	// Extra is the upstream's own timings object, verbatim. Rooter normalises the
	// fields above and keeps nothing of the source shape, so anything it does not
	// model is still recoverable - the two boxes already disagree here, strata
	// saying cache_n where sushi says cached_n.
	Extra json.RawMessage `json:"extra,omitempty"`
}

// timingsDoc is the union of what the runners put in a response. Both the
// OpenAI-shaped usage block and llama.cpp's timings block are read from one
// parse, and the two spellings of the cache field are folded together.
type timingsDoc struct {
	ID      string          `json:"id"`
	Model   string          `json:"model"`
	Timings json.RawMessage `json:"timings"`
	Usage   struct {
		PromptTokens     *int `json:"prompt_tokens"`
		CompletionTokens *int `json:"completion_tokens"`
		PromptTokensDet  struct {
			Cached *int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

type timingsRaw struct {
	PromptN            *int     `json:"prompt_n"`
	PromptMS           *float64 `json:"prompt_ms"`
	PromptPerSecond    *float64 `json:"prompt_per_second"`
	PredictedN         *int     `json:"predicted_n"`
	PredictedMS        *float64 `json:"predicted_ms"`
	PredictedPerSecond *float64 `json:"predicted_per_second"`
	// strata spells this cache_n, sushi spells it cached_n. Neither is wrong and
	// neither is going to change, so rooter reads both and the client sees one.
	CacheN  *int `json:"cache_n"`
	CachedN *int `json:"cached_n"`
}

// absorb folds one parsed response object into the record. Called once for a
// non-streaming body and once per SSE data line for a stream, which is how the
// final chunk's timings end up in the same place a plain response's do.
func (c *callRecord) absorb(doc *timingsDoc) {
	if c.UpstreamID == "" && doc.ID != "" {
		c.UpstreamID = doc.ID
	}
	if doc.Model != "" && doc.Model != "auto" && doc.Model != "default" {
		c.Model = doc.Model
	}
	if v := doc.Usage.PromptTokens; v != nil {
		c.PromptTokens = v
	}
	if v := doc.Usage.CompletionTokens; v != nil {
		c.CompletionTokens = v
	}
	if v := doc.Usage.PromptTokensDet.Cached; v != nil {
		c.CachedTokens = v
	}
	for _, ch := range doc.Choices {
		if ch.FinishReason != "" {
			c.FinishReason = ch.FinishReason
		}
	}
	if len(doc.Timings) == 0 {
		return
	}
	var t timingsRaw
	if err := json.Unmarshal(doc.Timings, &t); err != nil {
		return
	}
	c.Extra = doc.Timings
	setf := func(p **float64, v *float64) {
		if v != nil {
			*p = v
		}
	}
	seti := func(p **int, v *int) {
		if v != nil {
			*p = v
		}
	}
	setf(&c.PrefillMs, t.PromptMS)
	setf(&c.PrefillTokS, t.PromptPerSecond)
	setf(&c.DecodeMs, t.PredictedMS)
	setf(&c.DecodeTokS, t.PredictedPerSecond)
	seti(&c.PromptTokens, t.PromptN)
	seti(&c.CompletionTokens, t.PredictedN)
	// The cache field under either spelling, and usage.cached_tokens if the
	// runner got there first.
	if t.CacheN != nil {
		c.CachedTokens = t.CacheN
	} else if t.CachedN != nil {
		c.CachedTokens = t.CachedN
	}
	// Derive the rates when a runner gave counts and a duration but no throughput,
	// so the footer works against any llama.cpp build rather than only the two
	// shapes seen on this network.
	if c.PrefillTokS == nil && c.PrefillMs != nil && c.PromptTokens != nil && *c.PrefillMs > 0 && *c.PromptTokens > 0 {
		v := float64(*c.PromptTokens) / (*c.PrefillMs / 1000)
		c.PrefillTokS = &v
	}
	if c.DecodeTokS == nil && c.DecodeMs != nil && c.CompletionTokens != nil && *c.DecodeMs > 0 && *c.CompletionTokens > 0 {
		v := float64(*c.CompletionTokens) / (*c.DecodeMs / 1000)
		c.DecodeTokS = &v
	}
	if c.CachedTokens != nil && c.PromptTokens != nil && *c.PromptTokens > 0 {
		v := float64(*c.CachedTokens) / float64(*c.PromptTokens)
		c.CacheHitRate = &v
	}
}

// absorbJSON folds a whole response body in, non-streaming shape.
func (c *callRecord) absorbJSON(body []byte) {
	var doc timingsDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return
	}
	c.absorb(&doc)
}

// absorbSSETail folds the trailing events of a stream in. It reads data: lines
// from the end backwards, because the interesting one (usage + timings) is last
// and everything before it is token deltas nobody needs re-parsing.
func (c *callRecord) absorbSSETail(tail []byte) {
	lines := bytes.Split(tail, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		line := bytes.TrimSpace(lines[i])
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		if len(payload) == 0 || bytes.HasPrefix(payload, []byte("[DONE]")) {
			continue
		}
		var doc timingsDoc
		if err := json.Unmarshal(payload, &doc); err != nil {
			continue
		}
		if len(doc.Timings) == 0 && doc.Usage.PromptTokens == nil && doc.Usage.CompletionTokens == nil {
			continue // a token delta; keep walking back
		}
		c.absorb(&doc)
		return
	}
}

// injectRooterObject adds the rooter key to a non-streaming JSON body. It splices
// rather than remarshal-ing: re-encoding a body the client was going to parse
// anyway would reorder keys and reformat numbers for no reason, and this is a
// byte-level change to somebody else's response.
func injectRooterObject(body []byte, rec any) ([]byte, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return nil, false
	}
	if _, exists := fields["rooter"]; exists {
		return nil, false // the upstream already answered for itself; do not argue
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return nil, false
	}
	// Insert before the final "}", preserving the body's own bytes. A body that
	// ends in whitespace is rebuilt around it rather than reformatted.
	end := bytes.LastIndexByte(trimmed, '}')
	if end <= 0 {
		return nil, false
	}
	out := make([]byte, 0, len(trimmed)+len(value)+16)
	out = append(out, trimmed[:end]...)
	if last := bytes.TrimRight(trimmed[:end], " \t\n\r"); len(last) > 1 {
		out = append(out, ',')
	}
	out = append(out, `"rooter":`...)
	out = append(out, value...)
	out = append(out, '}')
	return out, true
}

// headerWriter sets rooter's headers. Every one of these is known before the
// response starts, which is exactly why they are headers and not body fields.
func setMetaHeaders(h http.Header, rec *callRecord) {
	h.Set(hdrRequestID, rec.ID)
	h.Set(hdrBackend, rec.Backend)
	if rec.Model != "" {
		h.Set(hdrModel, rec.Model)
	}
	h.Set(hdrRoute, rec.Kind)
	h.Set(hdrWaitMs, strconv.FormatFloat(rec.WaitMs, 'f', 3, 64))
	if rec.Stream {
		h.Set(hdrStreamed, "1")
	}
}

// newCallRecord builds the identity half of the record. Everything here is known
// before a slot is even requested, so none of it depends on the response.
func (r *Router) newCallRecord(req *http.Request, info requestInfo, id string) *callRecord {
	return &callRecord{
		ID:             id,
		TS:             time.Now().UTC().Format(time.RFC3339Nano),
		Path:           req.URL.Path,
		Method:         req.Method,
		ModelRequested: info.Model,
		Model:          info.Model,
		Key:            shortKey(info.Key),
		KeySource:      info.KeySource,
	}
}

// streamRequested asks, without a full unmarshal, whether the client is about to
// be answered with an SSE stream. It decides whether rooter may hold the body
// briefly to add a key, and it is the one thing about the response that is known
// from the request alone.
func streamRequested(body []byte) bool {
	const key = `"stream"`
	i := bytes.Index(body, []byte(key))
	if i < 0 {
		return false
	}
	rest := body[i+len(key):]
	trim := func() {
		for len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\t') {
			rest = rest[1:]
		}
	}
	trim()
	if len(rest) == 0 || rest[0] != ':' {
		return false
	}
	rest = rest[1:]
	trim()
	return bytes.HasPrefix(rest, []byte("true"))
}

// nextID mints a per-call id. It is the idempotency key for anything that
// consumes these events, so it has to be unique across restarts as well as
// within a process - a bare counter would collide the moment rooter restarted,
// and a restart is exactly when a client is most likely to retry.
func (r *Router) nextID() string {
	n := atomic.AddInt64(&r.seq, 1)
	return fmt.Sprintf("%s/%d", r.bootID, n)
}

// shortFloat keeps the metrics text readable without losing the numbers a
// dashboard aggregates.
func shortFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', 3, 64)
}

// newBootID is a per-process identity for call ids. A monotonic counter alone
// would re-issue the same id after a restart, and a restart is exactly when a
// client is most likely to be retrying - so the id carries both.
func newBootID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 16)
}

// finishRecord closes out one call: reads the response for the runner's own
// numbers, adds the rooter object where that is safe to do, and hands the record
// to the event ring and the metrics.
//
// It runs on the panic path as well as the normal one, so nothing here may
// assume the response was complete.
func (r *Router) finishRecord(w *replyWriter, rec *callRecord, res *attempt, b *Backend, out *Outcome, start time.Time, aborted bool) {
	rec.Status = res.status
	rec.Bytes = res.bytes
	rec.Aborted = aborted
	rec.TTFTMs = float64(res.ttfb) / float64(time.Millisecond)
	rec.TotalMs = float64(time.Since(start)) / float64(time.Millisecond)
	if res.err != nil && !aborted {
		rec.Error = res.err.Error()
	}
	if rec.Backend == "" {
		rec.Backend = b.Name
		rec.BackendURL = b.Base.String()
	}

	if !r.cfg.NoMeta {
		// A held body is complete, so read all of it; a stream is only ever read
		// from the tail, and only to learn what the runner said.
		if w.buffering {
			// A held body is complete, so read all of it rather than a tail.
			rec.absorbJSON(w.buf)
			w.hook = func(in []byte) []byte {
				if injected, ok := injectRooterObject(in, rec); ok {
					return injected
				}
				return in
			}
		} else if len(w.tail) > 0 {
			// A stream is only ever read, never rewritten: those bytes are the
			// user's tokens arriving.
			if rec.Stream || bytes.Contains(w.tail, []byte("\ndata:")) {
				rec.absorbSSETail(w.tail)
			} else {
				rec.absorbJSON(w.tail)
			}
		}
		// Abandoned streams have no tail worth reading, and that is the point:
		// the usage and timing fields stay null rather than reading as zero, so
		// "the client hung up" cannot be mistaken for "the model was fast".
		w.finalize()
	}
	r.observeCall(rec)
}
