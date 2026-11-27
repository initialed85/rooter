package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const abortStreamBody = `{"model":"test-model","messages":[{"role":"user","content":"x"}],"stream":true}`

// TestAbandonedStreamsDoNotLeakAccounting is the regression test for a hole that
// was visible in production as rooter_inflight sitting at 1131 on a box with
// eight slots and seven open connections.
//
// When a client hangs up mid-response, httputil.ReverseProxy panics with
// http.ErrAbortHandler and net/http recovers that sentinel without logging it.
// The accounting that used to live after dispatch therefore never ran, while the
// deferred slot release did - so capacity looked fine and the counters quietly
// lied. Worse, every abandoned generation vanished from byStatus, which is the
// one outcome that says something about whether the answer was good enough.
func TestAbandonedStreamsDoNotLeakAccounting(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) { c.RequestTimeout = 0 })
	a.setLatency(1 * time.Second)

	front := httptest.NewServer(e.router)
	defer front.Close()

	const n = 10
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequest("POST", front.URL+"/v1/chat/completions", strings.NewReader(abortStreamBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			cancel()
			continue
		}
		buf := make([]byte, 1)
		res.Body.Read(buf) // one byte of the stream, then walk away
		res.Body.Close()
		cancel()
	}
	// The upstream still thinks it is streaming for another latency/4 per chunk;
	// give it time to find out nobody is listening.
	time.Sleep(2500 * time.Millisecond)

	st := e.router.Stats()
	t.Logf("inflight=%v aborted=%v slots=%v status=%v", st["inflight"], st["aborted"], st["slots"], st["status"])
	if v, _ := st["inflight"].(int64); v != 0 {
		t.Errorf("rooter_inflight leaked: %d after %d abandoned streams", v, n)
	}
	if v, _ := st["aborted"].(int64); v == 0 {
		t.Errorf("abandonments were not counted at all")
	}
	status, _ := st["status"].(map[string]int64)
	if status["499"] == 0 {
		t.Errorf("abandoned calls missing from the status histogram: %v", status)
	}

	// Slots must still be free for a fresh request.
	if res, _ := e.do(postJSON("/v1/chat/completions", chatBody("s", "still works"))); res.StatusCode != http.StatusOK {
		t.Errorf("fresh request after the abandonments got %d", res.StatusCode)
	}

	rec := httptest.NewRecorder()
	e.router.prometheus(rec)
	if !strings.Contains(rec.Body.String(), "rooter_aborted_total") {
		t.Errorf("rooter_aborted_total missing from /rooter/metrics:\n%s", rec.Body.String())
	}
}

// An abandoned stream has no timings and no usage, because the runner never got
// to send them. The record has to say "unknown" rather than "zero", or a
// dashboard reads a hung-up generation as a very fast one.
func TestAbandonedCallLeavesUsageNullNotZero(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(1 * time.Second)
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) { c.RequestTimeout = 0 })

	front := httptest.NewServer(e.router)
	defer front.Close()

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequest("POST", front.URL+"/v1/chat/completions", strings.NewReader(abortStreamBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Session-Id", "abandon")
		res, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			cancel()
			continue
		}
		res.Body.Read(make([]byte, 1)) // one byte of the stream, then walk away
		res.Body.Close()
		cancel()
	}
	time.Sleep(2500 * time.Millisecond)

	recs, _, _ := e.router.events.since(0, 64)
	got := 0
	for _, rec := range recs {
		if !rec.Aborted {
			continue
		}
		got++
		if rec.PromptTokens != nil || rec.CompletionTokens != nil || rec.CachedTokens != nil {
			t.Errorf("an abandoned call invented token counts: %+v", rec)
		}
		if rec.PrefillMs != nil || rec.DecodeMs != nil || rec.DecodeTokS != nil {
			t.Errorf("an abandoned call invented timings: %+v", rec)
		}
		if rec.Status != statusAborted {
			t.Errorf("abandoned call status = %d, want %d", rec.Status, statusAborted)
		}
		// What rooter measures itself is still there, which is the whole reason
		// rooter is the event source rather than leaving it to the runner.
		if rec.TotalMs <= 0 || rec.Key == "" || rec.Backend != "a" {
			t.Errorf("abandoned call lost rooter's own measurements: %+v", rec)
		}
	}
	if got == 0 {
		t.Fatal("no aborted record reached the event ring")
	}
	// And the aggregate says so too, or a dashboard has nothing to alert on.
	_, m := e.do(get("/rooter/metrics"))
	if !strings.Contains(m, `rooter_model_aborted_total{backend="a",model="test-model"} 3`) {
		t.Errorf("abandonments missing from the per-model counter:\n%s", m)
	}
	// No tokens were ever reported, so the token family must not invent a row.
	if strings.Contains(m, `rooter_model_tokens_total{backend="a",model="test-model",kind="prompt"} 0`) {
		t.Errorf("a zero token count was emitted for calls that never reported one:\n%s", m)
	}
}

// TestAbandonedStreamReportsWhatReachedTheClient is the regression test for a
// blind spot in the log that made the fleet's slowest population unreadable.
//
// When a client hangs up mid-stream, ReverseProxy panics with ErrAbortHandler and
// the deferred recover builds the attempt itself. It used to build it from `res`,
// which dispatch never returned, so it was still the zero value: every abandoned
// stream logged bytes=0 and ttfb=0. Live consequence, retained log 2026-11-18 ->
// 2026-11-27: all 5,756 abandoned generations carried bytes=0 ttfb=0, and that
// made it impossible to tell a call the backend never started from one that
// streamed for four minutes - which is exactly the question a summarisation
// deadline has to be sized against.
func TestAbandonedStreamReportsWhatReachedTheClient(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(2 * time.Second) // four chunks, 500ms apart
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) { c.RequestTimeout = 0 })

	front := httptest.NewServer(e.router)
	defer front.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequest("POST", front.URL+"/v1/chat/completions", strings.NewReader(abortStreamBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Session-Id", "abandoned-midstream")
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	// Take the opening chunk, let a second one through, then hang up.
	buf := make([]byte, 64)
	if n, _ := res.Body.Read(buf); n == 0 {
		t.Fatal("the stream produced nothing to read")
	}
	time.Sleep(700 * time.Millisecond)
	res.Body.Close()
	cancel()
	time.Sleep(2500 * time.Millisecond)

	recs, _, _ := e.router.events.since(0, 64)
	var rec *callRecord
	for i := range recs {
		if recs[i].Aborted {
			rec = recs[i]
		}
	}
	if rec == nil {
		t.Fatal("no aborted record reached the event ring")
	}
	if rec.TTFTMs <= 0 {
		t.Errorf("an abandoned stream that had already started logging claims no byte ever arrived: ttft_ms=%.1f", rec.TTFTMs)
	}
	if rec.Bytes <= 0 {
		t.Errorf("an abandoned stream that had already delivered chunks logs bytes=%d", rec.Bytes)
	}
	// And the per-model histogram has to see the same thing, or the Prometheus
	// quantiles of "how long does an abandoned call run" stay a mystery.
	m := e.router.models.snapshot().byKey[modelKey("a", "test-model")]
	if m == nil || m.latency.n == 0 {
		t.Fatal("no duration sample recorded for the abandoned call")
	}
}

// TestPerModelHistogramsAreNotTheFleetAverage is the regression test for the
// series being wrong rather than missing.
//
// modelStats kept one latency/ttft/prefill/decode accumulator for the whole
// process and promModels printed it under every (backend, model) key. So all four
// live series in the fleet carried identical sums - 232 requests and 1,435.5 s of
// prefill on every key, including the keys that had served nothing - and any query
// that grouped by model was reading a fleet average wearing a model's name. A
// `sum(rate(...))` alert over those series was over-counting by the number of keys.
// The token counters were always keyed correctly, which is what made the mismatch
// visible: sushi/auto showed 2.5M prompt tokens against strata's own prefill time.
func TestPerModelHistogramsAreNotTheFleetAverage(t *testing.T) {
	m := newModelStats()
	quickMs, slowPrefillMs := 20.0, 90000.0
	quick := &callRecord{Backend: "strata", Model: "auto", Kind: "gen", Status: 200,
		TotalMs: quickMs, TTFTMs: 5, PrefillMs: &quickMs}
	slow := &callRecord{Backend: "sushi", Model: "auto", Kind: "gen", Status: 200,
		TotalMs: 90000, TTFTMs: 80000, PrefillMs: &slowPrefillMs}
	m.observe(quick)
	m.observe(slow)

	s := m.snapshot()
	strata, sushi := s.byKey["strata|auto"], s.byKey["sushi|auto"]
	if strata == nil || sushi == nil {
		t.Fatalf("both keys must exist: %v", s.byKey)
	}
	if strata.latency.n != 1 || sushi.latency.n != 1 {
		t.Errorf("each key must count only its own calls: strata=%d sushi=%d", strata.latency.n, sushi.latency.n)
	}
	if strata.latency.sum > 1000 {
		t.Errorf("strata/auto's duration sum is %.1f ms; the fleet average leaked into a per-model series", strata.latency.sum)
	}
	if sushi.latency.sum < 60000 {
		t.Errorf("sushi/auto's duration sum is %.1f ms; it lost its own 90s sample", sushi.latency.sum)
	}
	if strata.prefill.sum == sushi.prefill.sum {
		t.Errorf("prefill series are still shared: both keys report %.1f", strata.prefill.sum)
	}
}
