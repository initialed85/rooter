package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBackend is an OpenAI-ish upstream. It records what it received and how
// many requests it had in flight at once.
type fakeBackend struct {
	srv *httptest.Server

	mu        sync.Mutex
	bodies    []string
	headers   []http.Header
	paths     []string
	queries   []string
	inflight  int
	maxSeen   int
	requests  int
	latency   time.Duration
	breakConn bool // accept the request, then hang up without answering
	token     string
	model     string
}

func newFake(t *testing.T, name, model, token string) *fakeBackend {
	t.Helper()
	f := &fakeBackend{model: model, token: token}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
			http.Error(w, `{"error":{"message":"missing or wrong API key"}}`, http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests++
		f.inflight++
		if f.inflight > f.maxSeen {
			f.maxSeen = f.inflight
		}
		f.bodies = append(f.bodies, string(body))
		f.headers = append(f.headers, r.Header.Clone())
		f.paths = append(f.paths, r.URL.Path)
		f.queries = append(f.queries, r.URL.RawQuery)
		latency, brk := f.latency, f.breakConn
		f.mu.Unlock()
		defer func() {
			f.mu.Lock()
			f.inflight--
			f.mu.Unlock()
		}()
		if brk {
			c, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				c.Close()
				return
			}
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/models"):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"object":"list","data":[{"id":%q}]}`, f.model)
			return
		case strings.HasSuffix(r.URL.Path, "/health"):
			fmt.Fprintln(w, "ok")
			return
		}
		if strings.Contains(string(body), `"stream":true`) || strings.Contains(string(body), `"stream": true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			for i := 0; i < 4; i++ {
				fmt.Fprintf(w, "data: {\"n\":%d,\"m\":%q}\n\n", i, name)
				fl.Flush()
				time.Sleep(latency / 4)
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
			fl.Flush()
			return
		}
		time.Sleep(latency)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"backend":%q}`, name)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeBackend) url() string { return f.srv.URL }

// genCounts counts chat/completions hits only, ignoring /models health checks.
func (f *fakeBackend) genCounts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, p := range f.paths {
		if strings.Contains(p, "/chat/completions") {
			n++
		}
	}
	return n
}

func (f *fakeBackend) counts() (requests, inflight, maxSeen int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests, f.inflight, f.maxSeen
}

func (f *fakeBackend) lastBody() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		return ""
	}
	return f.bodies[len(f.bodies)-1]
}

func (f *fakeBackend) lastHeader() http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.headers) == 0 {
		return http.Header{}
	}
	return f.headers[len(f.headers)-1]
}

func (f *fakeBackend) lastPath() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.paths) == 0 {
		return ""
	}
	return f.paths[len(f.paths)-1]
}

func (f *fakeBackend) lastQuery() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queries) == 0 {
		return ""
	}
	return f.queries[len(f.queries)-1]
}

func (f *fakeBackend) setLatency(d time.Duration) {
	f.mu.Lock()
	f.latency = d
	f.mu.Unlock()
}

func (f *fakeBackend) setBreak(b bool) {
	f.mu.Lock()
	f.breakConn = b
	f.mu.Unlock()
}

type testEnv struct {
	t      *testing.T
	router *Router
	pool   *Pool
	sess   *SessionStore
	srv    *httptest.Server
	cfg    *Config
}

func newEnv(t *testing.T, specs []string, tweak func(*Config)) *testEnv {
	t.Helper()
	backends, err := parseBackends(specs, 1)
	if err != nil {
		t.Fatalf("parseBackends: %v", err)
	}
	cfg := &Config{
		SessionTTL:    time.Hour,
		MaxSessions:   1000,
		StickyGrace:   30 * time.Millisecond,
		Retries:       1,
		RetryWait:     time.Second,
		ScanWindow:    1 << 20,
		MaxBody:       8 << 20,
		FreePaths:     defaultFreePaths,
		HealthTimeout: 2 * time.Second,
	}
	if tweak != nil {
		tweak(cfg)
	}
	lg := log.New(io.Discard, "", 0)
	pool := NewPool(backends, PoolOpts{
		Poll:          20 * time.Millisecond,
		HealthTimeout: cfg.HealthTimeout,
		Verbose:       cfg.Verbose,
	}, &http.Client{Timeout: cfg.HealthTimeout}, lg)
	sess := NewSessionStore(cfg.SessionTTL, cfg.MaxSessions, lg)
	r := NewRouter(pool, sess, cfg, lg)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return &testEnv{t: t, router: r, pool: pool, sess: sess, srv: srv, cfg: cfg}
}

func (e *testEnv) addr() string { return e.srv.Listener.Addr().String() }

// do sends a request through rooter and returns the status and body.
func (e *testEnv) do(req *http.Request) (*http.Response, string) {
	e.t.Helper()
	res, body, _ := e.doErr(req)
	if body == "" && res == nil {
		e.t.Fatalf("request failed: no response")
	}
	return res, body
}

func (e *testEnv) doErr(req *http.Request) (*http.Response, string, error) {
	e.t.Helper()
	req.URL.Scheme = "http"
	req.URL.Host = e.addr()
	res, err := e.srv.Client().Do(req)
	if err != nil {
		return nil, "", err
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	return res, string(body), nil
}

// clientReq builds a *client* request (httptest.NewRequest makes server-side
// ones, which cannot be fed to http.Client).
func clientReq(method, path string, body io.Reader) *http.Request {
	r, err := http.NewRequest(method, "http://rooter"+path, body)
	if err != nil {
		panic(err)
	}
	return r
}

func postJSON(path, body string) *http.Request {
	r := clientReq(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func get(path string) *http.Request { return clientReq(http.MethodGet, path, nil) }

func withSession(r *http.Request, id string) *http.Request {
	r.Header.Set("X-Session-Id", id)
	return r
}

func chatBody(system, user string) string {
	return fmt.Sprintf(`{"model":"test-model","messages":[{"role":"system","content":%q},{"role":"user","content":%q}]}`, system, user)
}

// ---------------------------------------------------------------------------

func TestStickyByHeader(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)

	body := chatBody("sys", "hello")
	for i := 0; i < 3; i++ {
		res, out := e.do(withSession(postJSON("/v1/chat/completions", body), "sess-1"))
		if res.StatusCode != 200 {
			t.Fatalf("status %d %s", res.StatusCode, out)
		}
	}
	if n, _, _ := a.counts(); n != 3 {
		t.Errorf("backend a got %d requests, want 3 (sticky)", n)
	}
	if n, _, _ := b.counts(); n != 0 {
		t.Errorf("backend b got %d requests, want 0", n)
	}
	if got := e.sess.Stats()["sessions"].(int); got != 1 {
		t.Errorf("sessions = %d, want 1", got)
	}
}

func TestStickyByPrefixHashWithoutHeaders(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)

	// A conversation grows turn by turn. The prefix (system prompt plus the
	// first user turn) never changes, so every turn must land on one backend.
	for i := 0; i < 4; i++ {
		body := fmt.Sprintf(`{"model":"test-model","messages":[{"role":"system","content":"be nice"},{"role":"user","content":"turn 1"},{"role":"assistant","content":"ok"},{"role":"user","content":"turn %d"}]}`, i+2)
		res, out := e.do(postJSON("/v1/chat/completions", body))
		if res.StatusCode != 200 {
			t.Fatalf("status %d %s", res.StatusCode, out)
		}
	}
	ta, _, _ := a.counts()
	tb, _, _ := b.counts()
	if ta != 4 && tb != 4 {
		t.Errorf("prefix hash did not pin the conversation: a=%d b=%d", ta, tb)
	}
	// A second conversation, same shape, must be tracked separately and may use
	// the other backend for all of its turns.
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"model":"test-model","messages":[{"role":"system","content":"be nice"},{"role":"user","content":"other session"},{"role":"assistant","content":"ok"},{"role":"user","content":"turn %d"}]}`, i)
		e.do(postJSON("/v1/chat/completions", body))
	}
	ta2, _, _ := a.counts()
	tb2, _, _ := b.counts()
	if ta2-ta != 0 && tb2-tb != 0 {
		t.Errorf("second conversation split across backends: a %+d b %+d", ta2-ta, tb2-tb)
	}
	if ta2 == 5 && tb2 == 2 {
		t.Errorf("both conversations stuck on a: a=%d b=%d", ta2, tb2)
	}
}

func mustCounts(f *fakeBackend) int {
	n, _, _ := f.counts()
	return n
}

func TestNewSessionsSpreadAcrossFreeBackends(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	a.setLatency(80 * time.Millisecond)
	b.setLatency(80 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := withSession(postJSON("/v1/chat/completions", chatBody(fmt.Sprintf("sys %d", i), "hi")), fmt.Sprintf("s%d", i))
			res, body, err := e.doErr(req)
			if err != nil {
				t.Errorf("session %d: %v", i, err)
				return
			}
			if res.StatusCode != 200 {
				t.Errorf("session %d status %d %s", i, res.StatusCode, body)
			}
		}(i)
	}
	wg.Wait()
	na, _, maxa := a.counts()
	nb, _, maxb := b.counts()
	if na == 0 || nb == 0 {
		t.Errorf("new sessions did not spread: a=%d b=%d", na, nb)
	}
	if maxa > 1 || maxb > 1 {
		t.Errorf("concurrency=1 exceeded: a max %d, b max %d", maxa, maxb)
	}
}

func TestBlocksUntilASlotIsFree(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(200 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=1"}, nil)

	type outcome struct {
		code int
		body string
		dur  time.Duration
	}
	ch := make(chan outcome, 2)
	send := func(sess string) {
		go func() {
			start := time.Now()
			res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody(sess, "hi")), sess))
			ch <- outcome{res.StatusCode, body, time.Since(start)}
		}()
	}
	send("blocker")
	time.Sleep(40 * time.Millisecond)
	send("waiter")
	<-time.After(50 * time.Millisecond)
	if q := e.router.Stats()["queued"].(int64); q != 1 {
		t.Logf("queued = %d, want 1 waiter", q)
	}
	got := []outcome{<-ch, <-ch}
	for _, o := range got {
		if o.code != 200 {
			t.Fatalf("code %d body %s", o.code, o.body)
		}
	}
	fast, slow := got[0].dur, got[1].dur
	if fast > slow {
		fast, slow = slow, fast
	}
	if fast > 250*time.Millisecond || slow < 300*time.Millisecond {
		t.Errorf("expected one request to queue behind the other: first=%s second=%s", got[0].dur, got[1].dur)
	}
	if _, _, max := a.counts(); max > 1 {
		t.Errorf("concurrency exceeded: %d in flight at once", max)
	}
}

func TestMaxWaitReturns503(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(400 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=1"}, func(c *Config) {
		c.MaxWait = 80 * time.Millisecond
	})
	go e.do(withSession(postJSON("/v1/chat/completions", chatBody("busy", "hi")), "busy"))
	time.Sleep(50 * time.Millisecond)
	res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody("late", "hi")), "late"))
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %s, want 503", res.StatusCode, body)
	}
	if !strings.Contains(body, "busy") {
		t.Errorf("body %s should report busy", body)
	}
}

func TestPassthroughIsByteExact(t *testing.T) {
	a := newFake(t, "a", "test-model", "Password1")
	e := newEnv(t, []string{"a=" + a.url() + "/v1,token=Password1"}, nil)
	raw := `{"model":"test-model","messages":[{"role":"user","content":"literal \"quotes\" and unicode 🐫"}],"temperature":0.42,"stream":false}`
	req := postJSON("/v1/chat/completions?x=1&y=two", raw)
	req.Header.Set("Authorization", "Bearer client-key-must-be-replaced")
	req.Header.Set("X-Custom-Header", "kept")
	res, body := e.do(req)
	if res.StatusCode != 200 {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	if got := a.lastBody(); got != raw {
		t.Errorf("backend received %q, want exactly %q", got, raw)
	}
	if got := a.lastPath(); got != "/v1/chat/completions" {
		t.Errorf("backend received path %q", got)
	}
	if got := a.lastQuery(); got != "x=1&y=two" {
		t.Errorf("query became %q", got)
	}
	h := a.lastHeader()
	if h.Get("X-Custom-Header") != "kept" {
		t.Errorf("custom header dropped: %v", h)
	}
	if got := h.Get("Authorization"); got != "Bearer Password1" {
		t.Errorf("Authorization = %q, want the backend token", got)
	}
	if got := h.Get("X-Forwarded-For"); got != "" {
		t.Errorf("X-Forwarded-For added: %q", got)
	}
}

func TestClientTokenPassesThroughWhenBackendHasNone(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)
	req := withSession(postJSON("/v1/chat/completions", chatBody("s", "hi")), "auth")
	req.Header.Set("Authorization", "Bearer whatever-the-client-sent")
	e.do(req)
	if got := a.lastHeader().Get("Authorization"); got != "Bearer whatever-the-client-sent" {
		t.Errorf("Authorization = %q, want the client's value untouched", got)
	}
}

func TestPathJoining(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url() + "/v1"}, nil)
	// A backend registered with a /v1 prefix must not be given /v1/v1/...
	res, body := e.do(get("/v1/models"))
	if res.StatusCode != 200 {
		t.Fatalf("status %d %s", res.StatusCode, body)
	}
	if p := a.lastPath(); p != "/v1/models" {
		t.Errorf("path = %q, want /v1/models", p)
	}
	// A path that does not carry the prefix gets it added.
	res, body = e.do(get("/health"))
	if res.StatusCode != 200 {
		t.Fatalf("status %d %s", res.StatusCode, body)
	}
	if p := a.lastPath(); p != "/v1/health" {
		t.Errorf("path = %q, want /v1/health", p)
	}
}

func TestStreamingSSEIsIncremental(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(400 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url()}, nil)
	req := withSession(postJSON("/v1/chat/completions", `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"go"}]}`), "sse")
	req.URL.Scheme = "http"
	req.URL.Host = e.addr()
	start := time.Now()
	res, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer res.Body.Close()
	buf := make([]byte, 4096)
	n, err := res.Body.Read(buf)
	firstRead := time.Since(start)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(buf[:n]), "data:") {
		t.Errorf("first read had no SSE data: %q", buf[:n])
	}
	rest, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(rest), "[DONE]") {
		t.Errorf("stream truncated: %q %q", buf[:n], rest)
	}
	if firstRead > 150*time.Millisecond {
		t.Errorf("first chunk took %s, the stream was buffered", firstRead)
	}
	if _, _, max := a.counts(); max > 1 {
		t.Errorf("streaming should use one slot, saw %d", max)
	}
}

func TestRetryOntoAnotherBackend(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	a.setBreak(true)
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, func(c *Config) { c.Retries = 2 })

	res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody("please retry", "hi")), "retry-1"))
	if res.StatusCode != 200 {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	if n, _, _ := b.counts(); n == 0 {
		t.Errorf("backend b never used; no retry happened (a=%d)", mustCounts(a))
	}
	if !strings.Contains(body, "b") {
		t.Errorf("response should come from b: %s", body)
	}
	if got, ok := e.sess.Get(BindKey("h:retry-1", "test-model")); !ok || got != "b" {
		t.Errorf("session not rebound onto b after the retry: %q ok=%v", got, ok)
	}
}

func TestUnhealthyBackendIsSkippedForNewSessions(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)
	e.pool.SetHealth(e.pool.Lookup("a"), false)
	for i := 0; i < 3; i++ {
		res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody("x", "hi")), fmt.Sprintf("u%d", i)))
		if res.StatusCode != 200 {
			t.Fatalf("%d %s", res.StatusCode, body)
		}
	}
	if n, _, _ := a.counts(); n != 0 {
		t.Errorf("unhealthy backend got %d requests", n)
	}
	if n, _, _ := b.counts(); n != 3 {
		t.Errorf("healthy backend got %d requests, want 3", n)
	}
}

func TestModelEligibility(t *testing.T) {
	a := newFake(t, "a", "model-a", "")
	b := newFake(t, "b", "model-b", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)
	e.pool.CheckAll()
	if f := e.pool.ModelFilter("model-b"); f == nil {
		t.Fatal("expected a filter for a model one backend serves")
	}
	for i := 0; i < 2; i++ {
		body := fmt.Sprintf(`{"model":"model-b","messages":[{"role":"user","content":"hi %d"}]}`, i)
		res, out := e.do(withSession(postJSON("/v1/chat/completions", body), fmt.Sprintf("m%d", i)))
		if res.StatusCode != 200 {
			t.Fatalf("status %d %s", res.StatusCode, out)
		}
		if out != `{"backend":"b"}` {
			t.Errorf("routed to the wrong backend: %s", out)
		}
	}
	if n := a.genCounts(); n != 0 {
		t.Errorf("model-b requests leaked to a: %d", n)
	}
	// A model nobody lists used to be sent anywhere, which meant llama.cpp answered
	// it with whichever model it happened to have loaded - a silent model swap that
	// looks exactly like a bad answer from the model the user asked for. With every
	// backend's list known, that has to become a visible 404.
	req := withSession(postJSON("/v1/chat/completions", `{"model":"model-zzz","messages":[{"role":"user","content":"hi"}]}`), "zzz")
	if res, out := e.do(req); res.StatusCode != http.StatusNotFound {
		t.Errorf("unknown model: got %d %s, want 404", res.StatusCode, out)
	}
	// A typo is not overload: rooter_rejected_total is the capacity metric.
	if n := e.router.Stats()["rejected"].(int64); n != 0 {
		t.Errorf("a 404 bumped the rejected counter to %d", n)
	}
	// "default" is what llama.cpp clients send when they do not care; it is a
	// request for anything, not a request for a model that does not exist.
	req = withSession(postJSON("/v1/chat/completions", `{"model":"default","messages":[{"role":"user","content":"hi"}]}`), "dflt")
	if res, out := e.do(req); res.StatusCode != 200 {
		t.Errorf("model=default rejected: %d %s", res.StatusCode, out)
	}
}

// The regression this guards: a session bound to backend a must not be kept
// there when it asks for a model only b serves. Stickiness outranking the model
// filter is not a cache miss, it is a silent model swap, because llama.cpp
// answers a model it does not have with the one it does.
func TestStickyYieldsToModelEligibility(t *testing.T) {
	a := newFake(t, "a", "model-a", "")
	b := newFake(t, "b", "model-b", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)
	e.pool.CheckAll()

	owner := map[string]string{"model-a": "a", "model-b": "b"}
	// Ping-pong the two models on one session id. Each must land on its own
	// backend every time, so neither backend's cache gets thrown away.
	for i, m := range []string{"model-a", "model-b", "model-a", "model-b"} {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"turn %d"}]}`, m, i)
		res, out := e.do(withSession(postJSON("/v1/chat/completions", body), "switcher"))
		if res.StatusCode != 200 {
			t.Fatalf("turn %d (%s): %d %s", i, m, res.StatusCode, out)
		}
		if want := `{"backend":"` + owner[m] + `"}`; out != want {
			t.Errorf("turn %d model %s -> %s, want %s", i, m, out, want)
		}
	}
	if n := a.genCounts(); n != 2 {
		t.Errorf("a served %d turns, want 2", n)
	}
	if n := b.genCounts(); n != 2 {
		t.Errorf("b served %d turns, want 2", n)
	}
	if got, _ := e.sess.Get(BindKey("h:switcher", "model-a")); got != "a" {
		t.Errorf("model-a binding = %q, want a", got)
	}
	if got, _ := e.sess.Get(BindKey("h:switcher", "model-b")); got != "b" {
		t.Errorf("model-b binding = %q, want b", got)
	}
}

// Clients append a quant tag to the repo id ("...GGUF:UD-Q6_K_XL") while /models
// reports the base id, so the two have to be treated as the same model or every
// such request becomes a 404.
func TestQuantTaggedAliasRoutesToItsBackend(t *testing.T) {
	a := newFake(t, "a", "repo/GGUF", "")
	b := newFake(t, "b", "other-model", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)
	e.pool.CheckAll()

	body := `{"model":"repo/GGUF:UD-Q6_K_XL","messages":[{"role":"user","content":"hi"}]}`
	res, out := e.do(withSession(postJSON("/v1/chat/completions", body), "alias"))
	if res.StatusCode != 200 {
		t.Fatalf("quant-tagged alias: %d %s", res.StatusCode, out)
	}
	if out != `{"backend":"a"}` {
		t.Errorf("alias routed to %s, want a", out)
	}
	if n := b.genCounts(); n != 0 {
		t.Errorf("alias leaked %d requests to the wrong backend", n)
	}
}

// A backend that has never answered /models cannot be held against a model id it
// might well serve; refusing the request there would be a 404 out of ignorance.
func TestUnpolledBackendKeepsModelMatchingLenient(t *testing.T) {
	a := newFake(t, "a", "model-a", "")
	b := newFake(t, "b", "model-b", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)
	e.pool.CheckAll()
	// Lookup takes the pool lock itself, so it has to happen outside it.
	unpolled := e.pool.Lookup("b")
	e.pool.mu.Lock()
	unpolled.Models = nil // pretend /models has never been answered
	e.pool.mu.Unlock()

	body := `{"model":"model-zzz","messages":[{"role":"user","content":"hi"}]}`
	res, out := e.do(withSession(postJSON("/v1/chat/completions", body), "lenient"))
	if res.StatusCode != 200 {
		t.Errorf("unknown model with an unpolled backend: %d %s, want 200", res.StatusCode, out)
	}
}

func TestMetadataRequestsDoNotConsumeSlots(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(250 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=1"}, nil)
	go e.do(withSession(postJSON("/v1/chat/completions", chatBody("hold", "hi")), "hold"))
	time.Sleep(50 * time.Millisecond)
	done := make(chan int, 1)
	go func() {
		res, _ := e.do(get("/v1/models"))
		done <- res.StatusCode
	}()
	select {
	case code := <-done:
		if code != 200 {
			t.Errorf("GET /v1/models status %d", code)
		}
	case <-time.After(120 * time.Millisecond):
		t.Fatal("metadata request queued behind generation")
	}
}

func TestStickySessionRebindsWhenItsBackendDies(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, func(c *Config) {
		c.StickyGrace = 80 * time.Millisecond
		c.MaxWait = 0
	})
	res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody("sys", "first")), "die-hard"))
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, body)
	}
	name, ok := e.sess.Get(BindKey("h:die-hard", "test-model"))
	if !ok {
		t.Fatal("session was not bound")
	}
	e.pool.SetHealth(e.pool.Lookup(name), false)
	req := withSession(postJSON("/v1/chat/completions", chatBody("sys", "second")), "die-hard")
	start := time.Now()
	res2, body2 := e.do(req)
	if res2.StatusCode != 200 {
		t.Fatalf("after kill: %d %s", res2.StatusCode, body2)
	}
	if got, _ := e.sess.Get(BindKey("h:die-hard", "test-model")); got == name {
		t.Errorf("session stayed bound to the dead backend %s", got)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Errorf("rebound in %s, before the sticky grace period elapsed", elapsed)
	}
}

func TestStickySessionWaitsForItsOwnBackend(t *testing.T) {
	// Two backends, one slot each. Session "slow" is pinned to a and a is busy;
	// its next turn must queue even though b is idle.
	a := newFake(t, "a", "test-model", "")
	b := newFake(t, "b", "test-model", "")
	a.setLatency(300 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=1", "b=" + b.url() + ",concurrency=1"}, nil)

	go e.do(withSession(postJSON("/v1/chat/completions", chatBody("slow", "hi")), "slow"))
	time.Sleep(60 * time.Millisecond)
	// Bound the way a real request would bind it: per (session, model).
	e.sess.Bind(BindKey("h:mine", "test-model"), "a")
	req := withSession(postJSON("/v1/chat/completions", chatBody("mine", "hi")), "mine")
	start := time.Now()
	res, body := e.do(req)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, body)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Errorf("sticky request returned in %s; it should have waited for a", elapsed)
	}
	if n, _, _ := b.counts(); n != 0 {
		t.Errorf("sticky session leaked to b (b=%d)", n)
	}
}

func TestConcurrencyLimitIsRespected(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(60 * time.Millisecond)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=3"}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 9; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := withSession(postJSON("/v1/chat/completions", chatBody(fmt.Sprintf("s%d", i), "hi")), fmt.Sprintf("c%d", i))
			res, body, err := e.doErr(req)
			if err != nil {
				t.Errorf("%d: %v", i, err)
				return
			}
			if res.StatusCode != 200 {
				t.Errorf("%d: %d %s", i, res.StatusCode, body)
			}
		}(i)
	}
	wg.Wait()
	if _, _, max := a.counts(); max > 3 {
		t.Errorf("saw %d concurrent requests, limit was 3", max)
	}
	if n := mustCounts(a); n != 9 {
		t.Errorf("backend served %d requests, want 9", n)
	}
}

func TestSessionTTLAndLRU(t *testing.T) {
	s := NewSessionStore(20*time.Millisecond, 3, log.New(io.Discard, "", 0))
	for i := 0; i < 5; i++ {
		s.Bind(fmt.Sprintf("k%d", i), "a")
	}
	if n := s.Stats()["sessions"].(int); n != 3 {
		t.Errorf("sessions = %d, want 3 (LRU capped)", n)
	}
	if _, ok := s.Get("k4"); !ok {
		t.Error("newest key should be present")
	}
	if _, ok := s.Get("k0"); ok {
		t.Error("oldest key should have been evicted")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("k4"); ok {
		t.Error("entry should expire after the idle TTL")
	}
}

func TestKeyExtraction(t *testing.T) {
	h := http.Header{}
	h.Set("X-Session-Id", "abc")
	if got := extractKey(h, nil, ""); got.Key != "h:abc" || got.KeySource != "header" {
		t.Errorf("header key = %+v", got)
	}
	h = http.Header{}
	h.Set("session_id", "underscore")
	if got := extractKey(h, nil, ""); got.Key != "h:underscore" {
		t.Errorf("session_id header = %+v", got)
	}
	h = http.Header{}
	h.Set("X-Session-Affinity", "affinity")
	if got := extractKey(h, nil, ""); got.Key != "h:affinity" {
		t.Errorf("x-session-affinity header = %+v", got)
	}
	body := []byte(`{"model":"m","prompt_cache_key":"pc1","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function"}]}`)
	got := extractKey(http.Header{}, body, "application/json")
	if got.Key != "b:prompt_cache_key:pc1" || got.Model != "m" {
		t.Errorf("body key = %+v", got)
	}
	body = []byte(`{"model":"m","metadata":{"user_id":"u42"},"messages":[{"role":"user","content":"hi"}]}`)
	if got := extractKey(http.Header{}, body, "application/json"); got.Key != "b:metadata.user_id:u42" {
		t.Errorf("metadata key = %+v", got)
	}
	// prefix hash: stable across turns, different across sessions
	mk := func(sys, first, latest string) []byte {
		return []byte(fmt.Sprintf(`{"model":"m","messages":[{"role":"system","content":%q},{"role":"user","content":%q},{"role":"assistant","content":"a"},{"role":"user","content":%q}]}`, sys, first, latest))
	}
	a := extractKey(http.Header{}, mk("sys", "hello", "one"), "application/json")
	b := extractKey(http.Header{}, mk("sys", "hello", "two"), "application/json")
	c := extractKey(http.Header{}, mk("sys", "other", "two"), "application/json")
	if a.KeySource != "prefix" {
		t.Errorf("want prefix, got %+v", a)
	}
	if a.Key != b.Key {
		t.Errorf("same conversation hashed differently: %s vs %s", a.Key, b.Key)
	}
	if a.Key == c.Key {
		t.Errorf("different conversations hashed the same")
	}
	// A truncated body (only the scan window is available) must still hash to
	// the same key, because it is the same prefix.
	full := mk("sys", "hello", strings.Repeat("x", 500_000))
	if g := extractKey(http.Header{}, full[:4096], "application/json"); g.Key != a.Key {
		t.Errorf("prefix of a big body hashed to %s, want %s", g.Key, a.Key)
	}
	// A leading image on the first user turn still keys stable.
	img := `{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` +
		strings.Repeat("A", 300_000) + `"}},{"type":"text","text":"what is this"}]}]}`
	g1 := extractKey(http.Header{}, []byte(img), "application/json")
	g2 := extractKey(http.Header{}, []byte(img+`,"seed":7`), "application/json")
	if g1.KeySource != "prefix" || g1.Key != g2.Key {
		t.Errorf("image prefix unstable: %+v vs %+v", g1, g2)
	}
	// Non-JSON bodies fall back to a raw hash.
	if got := extractKey(http.Header{}, []byte("--boundary\r\ncontent"), "multipart/form-data"); got.KeySource != "prefix" {
		t.Errorf("multipart = %+v", got)
	}
	// An empty body is not a session.
	if got := extractKey(http.Header{}, nil, ""); got.Key != "" || got.KeySource != "none" {
		t.Errorf("empty = %+v", got)
	}
	// Responses API bodies carry the conversation in "input" and the system prompt
	// in "instructions". Without those, every turn of a Pi-on-responses session
	// would look like a brand new conversation and stickiness would do nothing.
	rk := func(instr, first, latest string) []byte {
		return []byte(fmt.Sprintf(`{"model":"m","instructions":%q,"input":[{"role":"user","content":[{"type":"input_text","text":%q}]},{"role":"assistant","content":[{"type":"output_text","text":"a"}]},{"role":"user","content":[{"type":"input_text","text":%q}]}]}`, instr, first, latest))
	}
	ra := extractKey(http.Header{}, rk("sys", "hello", "one"), "application/json")
	rb := extractKey(http.Header{}, rk("sys", "hello", "two"), "application/json")
	rc := extractKey(http.Header{}, rk("sys", "other", "two"), "application/json")
	if ra.KeySource != "prefix" || ra.Model != "m" {
		t.Errorf("responses prefix = %+v, want a prefix key for model m", ra)
	}
	if ra.Key != rb.Key {
		t.Errorf("same responses conversation hashed differently: %s vs %s", ra.Key, rb.Key)
	}
	if ra.Key == rc.Key {
		t.Errorf("different responses conversations hashed the same")
	}
	// The bare-string form of input is the whole prompt.
	if bs := extractKey(http.Header{}, []byte(`{"model":"m","input":"just a prompt"}`), "application/json"); bs.KeySource != "prefix" || bs.Key == "" {
		t.Errorf("bare input = %+v", bs)
	}
	// An explicit session field still beats the hash.
	if sid := extractKey(http.Header{}, []byte(`{"model":"m","session_id":"s1","input":[{"role":"user","content":"hi"}]}`), "application/json"); sid.Key != "b:session_id:s1" {
		t.Errorf("responses session_id = %+v", sid)
	}
}

func TestBackendSpecParsing(t *testing.T) {
	cases := []struct {
		spec        string
		name, url   string
		concurrency int
		token       string
		wantErr     bool
	}{
		{spec: "http://h:8088/v1", name: "h", url: "http://h:8088/v1", concurrency: 1},
		{spec: "strata=http://h:8088/v1", name: "strata", url: "http://h:8088/v1", concurrency: 1},
		{spec: "strata=http://h:8088/v1,token=Password1,concurrency=4", name: "strata", url: "http://h:8088/v1", concurrency: 4, token: "Password1"},
		{spec: "name=sushi,url=http://h:8088/v1,slots=1,key=Password1", name: "sushi", url: "http://h:8088/v1", concurrency: 1, token: "Password1"},
		{spec: "a=http://h:8088/v1/,name=b", name: "b", url: "http://h:8088/v1", concurrency: 1},
		{spec: `name=x,url=http://h/a\,b`, name: "x", url: "http://h/a,b", concurrency: 1},
		{spec: "name=x,url=http://h/v1,concurrency=0", wantErr: true},
		{spec: "name=x,url=ftp://h/v1", wantErr: true},
		{spec: "name=x", wantErr: true},
	}
	for _, c := range cases {
		b, err := parseBackend(c.spec, 1)
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %+v", c.spec, b)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.spec, err)
			continue
		}
		if b.Name != c.name || b.Base.String() != c.url || b.Concurrency != c.concurrency || b.Token != c.token {
			t.Errorf("%q -> name=%s url=%s conc=%d token=%s; want name=%s url=%s conc=%d token=%s",
				c.spec, b.Name, b.Base, b.Concurrency, b.Token, c.name, c.url, c.concurrency, c.token)
		}
	}
	if _, err := parseBackends([]string{"a=http://h/v1", "a=http://g/v1"}, 1); err == nil {
		t.Error("duplicate backend names should be rejected")
	}
}

func TestAdminEndpoints(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)
	for _, path := range []string{"/rooter", "/rooter/healthz", "/rooter/stats", "/rooter/metrics", "/rooter/backends", "/rooter/sessions"} {
		res, body := e.do(get(path))
		if res.StatusCode != 200 {
			t.Errorf("%s -> %d %s", path, res.StatusCode, body)
		}
	}
	res, body := e.do(get("/rooter/nope"))
	if res.StatusCode != 404 {
		t.Errorf("unknown admin path -> %d %s", res.StatusCode, body)
	}
	res, body = e.do(get("/rooter/stats"))
	var s map[string]any
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("stats not json: %v (%s)", err, body)
	}
	if bs, ok := s["backends"].([]any); !ok || len(bs) != 1 {
		t.Errorf("stats missing backends: %s", body)
	}
	res, body = e.do(get("/rooter/metrics"))
	if !strings.Contains(body, "rooter_backend_slots_total") {
		t.Error("metrics missing backend gauges")
	}
	_ = res
}

func TestLargeBodyStreamsThroughAndStillRoutes(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) {
		c.ScanWindow = 1024
		c.MaxBody = 2048
	})
	pad := strings.Repeat("y", 50000)
	body := fmt.Sprintf(`{"model":"test-model","messages":[{"role":"user","content":%q}]}`, pad)
	res, out := e.do(withSession(postJSON("/v1/chat/completions", body), "big"))
	if res.StatusCode != 200 {
		t.Fatalf("status %d %s", res.StatusCode, out)
	}
	if got := a.lastBody(); got != body {
		t.Errorf("large body mangled: got %d bytes, want %d", len(got), len(body))
	}
}

func TestContextCancelUnblocksWaiter(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	a.setLatency(2 * time.Second)
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=1"}, func(c *Config) { c.MaxWait = 0 })
	go e.do(withSession(postJSON("/v1/chat/completions", chatBody("hold", "hi")), "hold"))
	time.Sleep(60 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	req := withSession(postJSON("/v1/chat/completions", chatBody("cancel", "hi")), "cancel-me")
	req = req.WithContext(ctx)
	done := make(chan error, 1)
	go func() {
		_, _, err := e.doErr(req)
		done <- err
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Log("waiter completed anyway (slot freed quickly)")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request did not stop waiting for a slot")
	}
}

func TestNoBackendsAtAll(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)
	e.pool.SetHealth(e.pool.Lookup("a"), false)
	res, body := e.do(withSession(postJSON("/v1/chat/completions", chatBody("s", "hi")), "none"))
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d %s, want 503", res.StatusCode, body)
	}
	if !strings.Contains(body, "no healthy backend") {
		t.Errorf("body %s", body)
	}
}

func TestHealthCheckReadsModels(t *testing.T) {
	a := newFake(t, "a", "qwen3.8-flash-next", "Password1")
	e := newEnv(t, []string{"a=" + a.url() + "/v1,token=Password1"}, nil)
	if err := e.pool.Check(e.pool.Lookup("a")); err != nil {
		t.Fatalf("check: %v", err)
	}
	b := e.pool.Lookup("a")
	e.pool.mu.Lock()
	defer e.pool.mu.Unlock()
	if !b.Healthy {
		t.Error("backend should be healthy")
	}
	if !b.Models["qwen3.8-flash-next"] {
		t.Errorf("models = %v", b.Models)
	}
}

func TestOverCapBodyRejectsGracefully(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) { c.ScanWindow = 512; c.MaxBody = 1024 })
	// chunked (no Content-Length) and huge: rooter streams it through
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for i := 0; i < 40; i++ {
			pw.Write([]byte(strings.Repeat("z", 1000)))
		}
	}()
	req, _ := http.NewRequest(http.MethodPost, "http://x/v1/embeddings", pr)
	req.Header.Set("Content-Type", "application/json")
	res, body, err := e.doErr(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("status %d %s", res.StatusCode, body)
	}
}

func TestForgetDropsEveryModelBindingOfASession(t *testing.T) {
	s := NewSessionStore(time.Hour, 100, nil)
	s.Bind(BindKey("h:abc", "model-a"), "a")
	s.Bind(BindKey("h:abc", "model-b"), "b")
	s.Bind(BindKey("h:other", "model-a"), "a")
	if st := s.Stats(); st["sessions"].(int) != 2 || st["bindings"].(int) != 3 {
		t.Errorf("stats = %v/%v, want 2 sessions / 3 bindings", st["sessions"], st["bindings"])
	}

	// The admin page and clients only ever see the prefix-less key, so forgetting
	// by "abc" has to reach both per-model bindings.
	if n := s.UnbindSession("abc"); n != 2 {
		t.Errorf("UnbindSession(abc) dropped %d bindings, want 2", n)
	}
	if _, ok := s.Get(BindKey("h:other", "model-a")); !ok {
		t.Error("unrelated session was collateral damage")
	}
	if n := s.Clear(); n != 1 {
		t.Errorf("Clear dropped %d bindings, want 1", n)
	}
	if n := s.Stats()["sessions"].(int); n != 0 {
		t.Errorf("sessions after Clear = %d, want 0", n)
	}
}

// A Pi provider using api:openai-responses posts /v1/responses, where the
// conversation sits in "input"/"instructions" rather than "messages". Two turns
// of one conversation have to land on the same backend even with no session
// header, or stickiness is dead for that API.
func TestResponsesPathSticksByPrefix(t *testing.T) {
	a := newFake(t, "a", "model-a", "")
	b := newFake(t, "b", "model-a", "")
	e := newEnv(t, []string{"a=" + a.url(), "b=" + b.url()}, nil)

	first := `{"model":"model-a","instructions":"sys","input":[{"role":"user","content":[{"type":"input_text","text":"hello there"}]}]}`
	second := `{"model":"model-a","instructions":"sys","input":[{"role":"user","content":[{"type":"input_text","text":"hello there"}]},{"role":"assistant","content":[{"type":"output_text","text":"hi"}]},{"role":"user","content":[{"type":"input_text","text":"and now?"}]}]}`

	res1, out1 := e.do(postJSON("/v1/responses", first))
	res2, out2 := e.do(postJSON("/v1/responses", second))
	if res1.StatusCode != 200 || res2.StatusCode != 200 {
		t.Fatalf("statuses %d %d (%s %s)", res1.StatusCode, res2.StatusCode, out1, out2)
	}
	if out1 != out2 {
		t.Errorf("turns of one responses conversation landed differently: %s vs %s", out1, out2)
	}
	if st := e.sess.Stats(); st["hits"].(int64) < 1 {
		t.Errorf("second turn was not a sticky hit: %v", st)
	}
	if p := a.lastPath(); p != "/v1/responses" {
		t.Errorf("path rewritten: %q", p)
	}
	if body := a.lastBody(); !strings.Contains(body, "and now?") {
		t.Errorf("body not passed through: %q", body)
	}
}
