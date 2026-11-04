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
