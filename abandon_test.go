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
