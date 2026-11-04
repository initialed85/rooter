package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
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
	hdrTtftMs    = "X-Rooter-Ttft-Ms"

	hdrCompletionTokens = "X-Rooter-Completion-Tokens"
	hdrPrefillTokS      = "X-Rooter-Prefill-Tok-S"
	hdrDecodeTokS       = "X-Rooter-Decode-Tok-S"
	hdrCacheHitRate     = "X-Rooter-Cache-Hit-Rate"
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
	// UpstreamCreated is the epoch second the runner stamped on the completion.
	// It is here because the per-request detail a runner keeps about itself is
	// indexed by its own clock and its own id, neither of which is the id in the
	// body: strata's /metrics calls the same call "cbace0dbb29f" while answering
	// with "chatcmpl-23a156df...". A consumer that wants the runner's richer row
	// has to join on something, and the runner's own timestamp plus its token and
	// timing counts are the only values both sides agree on.
	UpstreamCreated int64  `json:"upstream_created,omitempty"`
	TS              string `json:"ts"`
	Path            string `json:"path"`
	Method          string `json:"method"`
	Kind            string `json:"kind"` // read | gen | spill | rebind
	Key             string `json:"key"`
	KeySource       string `json:"key_source"`
	Backend         string `json:"backend"`
	BackendURL      string `json:"backend_url"`

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
	// FirstContentMs is when the client received a character to read, on a stream.
	// It is not a refinement of ttft_ms, it is a different quantity: ttft_ms is the
	// first byte, and a runner that opens its stream with an empty delta answers
	// that in tens of milliseconds while the first word is seconds away. Consumers
	// graphing responsiveness need this one; consumers graphing whether the box was
	// admitting work need ttft_ms. Both are kept because both are true.
	FirstContentMs *float64 `json:"first_content_ms,omitempty"`

	// From the response body. Absent when the client hung up before the tail,
	// which is the whole reason these are pointers rather than zero defaults.
	PromptTokens *int `json:"prompt_tokens,omitempty"`
	// PromptTokensDerived marks the rows where rooter computed prompt_tokens itself
	// as prompt_tokens_computed + cached_tokens, because the runner sent timings but
	// no usage block - which is what a plain llama.cpp /completion does. When it is
	// false the number came straight out of usage.prompt_tokens.
	//
	// It is published because prompt_tokens is the denominator of every cache rate
	// anyone builds from this, and a reader who cannot tell a read value from a
	// reconstructed one is trusting arithmetic they cannot see. Both are the full
	// prompt length by construction; the flag is for auditing, not for a different
	// formula.
	PromptTokensDerived bool `json:"prompt_tokens_derived,omitempty"`
	// RunnerPromptNIncludesCache records that this backend's `prompt_n` counted the
	// cached prefix inside the prompt, so rooter subtracted it to get the work done.
	// It is an audit flag for a normalisation, not a measurement: a reader who wants
	// the runner's raw number can recover it by adding cached_tokens back.
	RunnerPromptNIncludesCache bool `json:"runner_prompt_n_includes_cache,omitempty"`
	// PromptTokensComputed is what the runner actually evaluated this call, which
	// llama.cpp calls prompt_n and is NOT the prompt length: it is the prompt minus
	// whatever it served from the KV cache. On a live strata call the prompt was 54
	// tokens and prompt_n was 5, because 49 were cached. Publishing prompt_n as
	// prompt_tokens makes a cache hit look like a tiny prompt and pushes
	// cache_hit_rate above 1, which is how this bug reached production.
	PromptTokensComputed *int     `json:"prompt_tokens_computed,omitempty"`
	CompletionTokens     *int     `json:"completion_tokens,omitempty"`
	CachedTokens         *int     `json:"cached_tokens,omitempty"`
	PrefillMs            *float64 `json:"prefill_ms,omitempty"`
	PrefillTokS          *float64 `json:"prefill_tok_s,omitempty"`
	DecodeMs             *float64 `json:"decode_ms,omitempty"`
	DecodeTokS           *float64 `json:"decode_tok_s,omitempty"`
	CacheHitRate         *float64 `json:"cache_hit_rate,omitempty"`
	FinishReason         string   `json:"finish_reason,omitempty"`

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
	Created int64           `json:"created"`
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
	if c.UpstreamCreated == 0 && doc.Created > 0 {
		c.UpstreamCreated = doc.Created
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
	// The cache field under either spelling, and usage.cached_tokens if the
	// runner got there first.
	if t.CacheN != nil {
		c.CachedTokens = t.CacheN
	} else if t.CachedN != nil {
		c.CachedTokens = t.CachedN
	}
	// `prompt_n` does not mean the same thing on both runners, and the difference is
	// not cosmetic - it is a factor of a hundred in prefill_tok_s on a warm cache.
	//
	// Taken live off the network on 2026-11-04, same field name, two backends:
	//
	//   strata  prompt_n=44    cached=95545 prompt_ms=560.5    prompt_per_second=78.5
	//           44 / 0.5605s    = 78.5   matches what it states
	//   sushi   prompt_n=49161 cached=48726 prompt_ms=1923.964 prompt_per_second=226.096
	//           49161 / 1.924s  = 25552  nothing like what it states
	//           435 / 1.924s    = 226.1  exactly what it states
	//
	// So strata's prompt_n is the work done and sushi's is the prompt size with the
	// cached prefix still inside it. Neither is wrong; they are two builds of llama.cpp
	// with two conventions and no field name to tell them apart.
	//
	// The runner reveals which one it is by doing its own arithmetic in front of us:
	// it publishes a throughput, and only one of prompt_n/prompt_ms or
	// (prompt_n-cached)/prompt_ms reproduces it. Rooter divides the same way the runner
	// did, which is the only way both backends can share a column.
	c.PromptTokensComputed = t.PromptN
	if t.PromptN != nil && c.CachedTokens != nil && *c.CachedTokens > 0 && *c.CachedTokens <= *t.PromptN &&
		t.PromptMS != nil && *t.PromptMS > 0 && t.PromptPerSecond != nil {
		overFull := float64(*t.PromptN) / (*t.PromptMS / 1000)
		overWork := float64(*t.PromptN-*c.CachedTokens) / (*t.PromptMS / 1000)
		nearFull := nearlyEqual(overFull, *t.PromptPerSecond)
		nearWork := nearlyEqual(overWork, *t.PromptPerSecond)
		if nearWork && !nearFull {
			// This runner's prompt_n still contains the cached prefix.
			work := *t.PromptN - *c.CachedTokens
			c.PromptTokensComputed = &work
			c.RunnerPromptNIncludesCache = true
			c.markRunnerPromptNIncludesCache()
		}
	}
	if c.PromptTokens == nil && c.PromptTokensComputed != nil {
		// Reconstruct the prompt length from whichever convention this runner used.
		// Under the inflated convention prompt_n already WAS the prompt length; under
		// the work convention it is the part that was not already in the KV cache.
		total := *c.PromptTokensComputed
		if c.RunnerPromptNIncludesCache {
			total = *t.PromptN
		} else if c.CachedTokens != nil {
			total += *c.CachedTokens
		}
		c.PromptTokens = &total
		// Say so on the row. This is the only place rooter turns two measurements
		// into a third, and `prompt_tokens` is the denominator of every cache rate
		// anyone computes from this. A reader who cannot tell a read value from a
		// reconstructed one is trusting arithmetic they cannot see.
		c.PromptTokensDerived = true
	}
	seti(&c.CompletionTokens, t.PredictedN)
	// Derive the rates when a runner gave counts and a duration but no throughput,
	// so the footer works against any llama.cpp build rather than only the two
	// shapes seen on this network. Prefill is priced against the tokens the runner
	// actually evaluated, not the prompt length, or a warm cache invents speed.
	if c.PrefillTokS == nil && c.PrefillMs != nil && c.PromptTokensComputed != nil && *c.PrefillMs > 0 && *c.PromptTokensComputed > 0 {
		v := float64(*c.PromptTokensComputed) / (*c.PrefillMs / 1000)
		c.PrefillTokS = &v
	}
	if c.DecodeTokS == nil && c.DecodeMs != nil && c.CompletionTokens != nil && *c.DecodeMs > 0 && *c.CompletionTokens > 0 {
		v := float64(*c.CompletionTokens) / (*c.DecodeMs / 1000)
		c.DecodeTokS = &v
	}
	// A hit rate is only meaningful against the prompt total. Cached tokens cannot
	// exceed it, and if the two disagree the runner said something rooter cannot
	// reconcile, so rooter says nothing rather than publishing 9.8.
	if c.CachedTokens != nil && c.PromptTokens != nil && *c.PromptTokens > 0 && *c.CachedTokens >= 0 && *c.CachedTokens <= *c.PromptTokens {
		v := float64(*c.CachedTokens) / float64(*c.PromptTokens)
		c.CacheHitRate = &v
	} else {
		c.CacheHitRate = nil
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
// roundMs is the single place a duration becomes a number a client reads.
// nearlyEqual compares two rates the way a human reading a footer would: close
// enough that the two agree, with room for the rounding a runner applies before it
// prints its own number.
func nearlyEqual(a, b float64) bool {
	if a == b {
		return true
	}
	tolerance := 0.02
	if larger := math.Max(math.Abs(a), math.Abs(b)); larger > tolerance {
		tolerance = larger * 0.02
	}
	return math.Abs(a-b) <= tolerance
}

// markRunnerPromptNIncludesCache records the normalisation in `extra` as well as on
// the row, so whichever surface a reader is looking at tells them what happened.
func (c *callRecord) markRunnerPromptNIncludesCache() {
	if c.Extra == nil {
		c.Extra = json.RawMessage(`{"runner_prompt_n_includes_cache":true}`)
		return
	}
	var m map[string]any
	if err := json.Unmarshal(c.Extra, &m); err != nil || m == nil {
		return
	}
	m["runner_prompt_n_includes_cache"] = true
	if b, err := json.Marshal(m); err == nil {
		c.Extra = b
	}
}

func roundMs(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
}

// setNumberHeader publishes an optional measurement. Absent means "nobody told us",
// which is a different claim from a header that says 0.
func setNumberHeader[T int | float64](h http.Header, name string, value *T) {
	if value == nil {
		return
	}
	h.Set(name, strconv.FormatFloat(float64(*value), 'f', -1, 64))
}

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
	// Millisecond resolution is what these measurements are worth, and rounding
	// once here is what makes the header, the body object and the event record
	// carry the identical number instead of three renderings of one value.
	rec.TTFTMs = roundMs(res.ttfb)
	rec.TotalMs = roundMs(time.Since(start))
	// Only a stream needs this, and only a stream can supply it: on a held body the
	// first byte is the whole response and the two numbers would be the same one
	// printed twice.
	if !w.buffering && w.contentSeen {
		ms := roundMs(w.firstContent)
		rec.FirstContentMs = &ms
	}
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
		// Time to first byte is publishable as a header only while the header has
		// not gone out yet, and a held body is the one case where it has not: a
		// stream's first byte and its headers are the same instant. Set it from the
		// record rather than the writer so the header, the body object and the
		// event ring cannot disagree about what the number means.
		if w.buffering {
			if rec.TTFTMs > 0 {
				w.Header().Set(hdrTtftMs, strconv.FormatFloat(rec.TTFTMs, 'f', -1, 64))
			}
			// The rest of what the runner said, as headers as well as body fields.
			// A footer wants these while it is drawing, and a client that only ever
			// looks at headers should not be locked out of the runner's own numbers.
			setNumberHeader(w.Header(), hdrCompletionTokens, rec.CompletionTokens)
			setNumberHeader(w.Header(), hdrPrefillTokS, rec.PrefillTokS)
			setNumberHeader(w.Header(), hdrDecodeTokS, rec.DecodeTokS)
			setNumberHeader(w.Header(), hdrCacheHitRate, rec.CacheHitRate)
		}
		// Abandoned streams have no tail worth reading, and that is the point:
		// the usage and timing fields stay null rather than reading as zero, so
		// "the client hung up" cannot be mistaken for "the model was fast".
		w.finalize()
	}
	r.observeCall(rec)
}
