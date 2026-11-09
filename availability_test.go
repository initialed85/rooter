package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Everything here is about what rooter says when a box is not answering, because a
// router is judged entirely by that case. Both came out of the live icarus outage on
// 2026-11-09: a Mac that refused connections for five minutes produced one log line,
// a `/rooter/backends` timestamp that had stopped moving, and a 404 telling the client
// its configuration named a model that does not exist. Two of those three were lies
// about rooter's own knowledge, and one of them cost a session nothing.

// eventually waits for an asynchronous thing to happen. The health loop is a ticker,
// so a test that asserts on what it logged has to give it a chance to log it.
func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func TestADownBackendKeepsSayingSo(t *testing.T) {
	var broken atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/models") {
			http.NotFound(w, r)
			return
		}
		if broken.Load() {
			http.Error(w, `{"error":{"message":"there is no llama.cpp here"}}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[{"id":"qwen-tiny"}]}`)
	}))
	defer srv.Close()
	broken.Store(true)

	b, err := parseBackend("grumpy="+srv.URL, 1)
	if err != nil {
		t.Fatalf("parseBackend: %v", err)
	}
	lb := &safeBuf{}
	p := NewPool([]*Backend{b}, PoolOpts{
		HealthInterval: 10 * time.Millisecond,
		HealthTimeout:  2 * time.Second,
	}, srv.Client(), log.New(lb, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.HealthLoop(ctx)

	// The first failure has to be reported at all.
	eventually(t, 3*time.Second, "the first failed probe to be logged", func() bool {
		return strings.Contains(lb.String(), "grumpy unhealthy")
	})
	// And a box that stays down has to keep reporting itself. Silence while a backend
	// fails every probe for five minutes is what made the live outage invisible.
	eventually(t, 5*time.Second, "a repeat line about a box that stayed down", func() bool {
		return strings.Contains(lb.String(), "still unhealthy (10 consecutive checks")
	})

	// `checked` used to move only when a probe passed, so a backend failing
	// continuously looked like a backend nobody was polling. It has to mean "the last
	// time rooter looked", pass or fail.
	before, ok := p.Stats()[0]["checked"].(time.Time)
	if !ok || before.IsZero() {
		t.Fatalf("checked = %v after a failed probe, want a timestamp", p.Stats()[0]["checked"])
	}
	time.Sleep(60 * time.Millisecond)
	after, _ := p.Stats()[0]["checked"].(time.Time)
	if !after.After(before) {
		t.Errorf("checked is %v after more failures and %v before: a failed probe still has to count as a check", after, before)
	}

	st := p.Stats()[0]
	if fails, _ := st["healthFails"].(int); fails < 10 {
		t.Errorf("healthFails = %d, want the consecutive failures counted", fails)
	}
	if ds, ok := st["downSince"].(time.Time); !ok || ds.IsZero() {
		t.Errorf("downSince = %v: an outage has to have a start, or no one can tell how long it has lasted", st["downSince"])
	}
	if st["healthy"].(bool) {
		t.Errorf("a backend failing every probe reports itself healthy")
	}
	if errs, _ := st["errors"].(int64); errs > 2 {
		t.Errorf("errors_total = %d after %v failed probes: an outage counter belongs to healthFails, not to the metric that means failed round trips",
			errs, st["healthFails"])
	}

	// The outage has to be visible in the JSON an operator actually reads.
	snap, err := p.SnapshotJSON()
	if err != nil {
		t.Fatalf("SnapshotJSON: %v", err)
	}
	if !strings.Contains(string(snap), `"healthFails"`) || !strings.Contains(string(snap), `"downSince"`) {
		t.Errorf("/rooter/backends hides the outage: %s", snap)
	}

	// Recovery clears the streak, so the two counters mean "how long has this box been
	// down" rather than "how many things have ever gone wrong anywhere".
	broken.Store(false)
	eventually(t, 3*time.Second, "the recovery to be logged", func() bool {
		return strings.Contains(lb.String(), "grumpy healthy again")
	})
	// An outage should end with a line that says how long it lasted, not just a line
	// saying it stopped.
	if !strings.Contains(lb.String(), "failed check(s), down") {
		t.Errorf("the recovery line does not say how long the box was gone: %s", lb.String())
	}
	st = p.Stats()[0]
	if !st["healthy"].(bool) {
		t.Errorf("healthy = false after probes started passing")
	}
	if fails, _ := st["healthFails"].(int); fails != 0 {
		t.Errorf("healthFails = %d after recovery, want the streak cleared", fails)
	}
	if ds, ok := st["downSince"].(time.Time); ok && !ds.IsZero() {
		t.Errorf("downSince = %v after recovery, want it cleared", ds)
	}
	snap, _ = p.SnapshotJSON()
	if strings.Contains(string(snap), `"healthFails"`) {
		t.Errorf("a healthy backend still carries an outage streak in /rooter/backends: %s", snap)
	}
}

func TestNamingADownBoxIsAnOutageNotAMissingModel(t *testing.T) {
	live := newFake(t, "strata", "qwen-tiny", "")
	gone := newFake(t, "icarus", "qwen-tiny", "")
	goneURL := gone.url()

	e := newEnv(t, []string{"strata=" + live.url(), "icarus=" + goneURL}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus"}
	})
	e.pool.CheckAll()
	// The box dies *after* it was known, which is the shape of a real outage: rooter
	// already has its model list and keeps it, because a list from a box that is down
	// is still the truth about what that box serves.
	gone.srv.Close()
	e.pool.CheckAll()

	if b := e.pool.Lookup("icarus"); b == nil || b.Healthy {
		t.Fatal("a backend nobody can connect to reports itself healthy")
	}

	ask := func(model, session string) (*http.Response, string) {
		return e.do(withSession(postJSON("/v1/chat/completions", fmt.Sprintf(
			`{"model":%q,"messages":[{"role":"user","content":"x"}]}`, model)), session))
	}

	// Asking for a box by name, while that box is down.
	res, body := ask("icarus", "named-down")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("asking for a box that is down got %d: %s; a 404 here reads as a client naming a model that does not exist",
			res.StatusCode, body)
	}
	if !strings.Contains(body, "unhealthy") || !strings.Contains(body, "icarus") {
		t.Errorf("the answer does not name the box that is down: %s", body)
	}
	if strings.Contains(body, "no backend serves model") {
		t.Errorf("a box outage was reported as a missing model: %s", body)
	}
	if res.Header.Get("Retry-After") == "" {
		t.Errorf("an outage answer with no Retry-After invites a client to give up instead of back off")
	}
	if n := live.genCounts(); n != 0 {
		t.Errorf("strata served %d generations for a request that named icarus; a box name is a pin, not a preference", n)
	}

	// The same truth through a retired name, which is where the confusion would be
	// worst: the client's configuration is not wrong, the machine is.
	res, body = ask("sushi", "retired-down")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("a retired name for a down box got %d: %s", res.StatusCode, body)
	}
	if !strings.Contains(body, `asked for as`) || !strings.Contains(body, "sushi") || !strings.Contains(body, "icarus") {
		t.Errorf("the answer does not say the retired name means the box that is down: %s", body)
	}

	// The guard rail: a model that genuinely is not here is still a 404. Making every
	// failure a 503 would teach clients to retry a mistake forever.
	res, body = ask("qwen-not-a-model", "unknown-model")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown model got %d: %s; only the named-box outage was supposed to change status", res.StatusCode, body)
	}
	if !strings.Contains(body, "no backend serves model") {
		t.Errorf("an unknown model stopped being described as one: %s", body)
	}

	// A model both boxes serve is served by the box that is up, and nothing about the
	// outage gets to refuse it.
	res, body = ask("qwen-tiny", "shared-model")
	if res.StatusCode != 200 {
		t.Fatalf("a real model got %d: %s", res.StatusCode, body)
	}
	if n := live.genCounts(); n == 0 {
		t.Errorf("the healthy box refused a model it serves because another box was down")
	}

	// Visibility, in all three places a person might look.
	time.Sleep(150 * time.Millisecond) // long enough that "how long has it been down" is not zero
	_, backends := e.do(get("/rooter/backends"))
	if !strings.Contains(backends, `"healthFails"`) || !strings.Contains(backends, `"downSince"`) {
		t.Errorf("/rooter/backends hides the outage: %s", backends)
	}
	_, index := e.do(get("/rooter"))
	if !strings.Contains(index, "failed check") {
		t.Errorf("the admin index does not mention how long icarus has been failing: %s", index)
	}
	_, metrics := e.do(get("/rooter/metrics"))
	if v := promLine(t, metrics, `rooter_backend_health_fails{backend="icarus"}`); v < 1 {
		t.Errorf("rooter_backend_health_fails for the down box is %v, want at least one failed probe", v)
	}
	if v := promLine(t, metrics, `rooter_backend_down_seconds{backend="icarus"}`); v <= 0 {
		t.Errorf("rooter_backend_down_seconds for the down box is %v, want a duration an alert can threshold", v)
	}
	if v := promLine(t, metrics, `rooter_backend_health_fails{backend="strata"}`); v != 0 {
		t.Errorf("rooter_backend_health_fails for the healthy box is %v, want 0", v)
	}
	if v := promLine(t, metrics, `rooter_backend_down_seconds{backend="strata"}`); v != 0 {
		t.Errorf("rooter_backend_down_seconds for the healthy box is %v, want 0", v)
	}
}

func TestABoxThatNeverAnsweredDoesNotBlindTheModelFilter(t *testing.T) {
	live := newFake(t, "strata", "qwen-tiny", "")
	// A backend nobody has ever reached: its inventory is unknown because it has
	// never told rooter anything, as opposed to a box that died after reporting one.
	e := newEnv(t, []string{"strata=" + live.url(), "phantom=http://127.0.0.1:9/v1"}, nil)
	e.pool.CheckAll()

	b := e.pool.Lookup("phantom")
	if b == nil || b.Healthy {
		t.Fatal("a backend that has never answered reports itself healthy")
	}
	if up := e.pool.Lookup("strata"); len(up.Models) == 0 {
		t.Fatal("the live backend has no model list, so the filter has nothing to work with")
	}

	ask := func(model, session string) (*http.Response, string) {
		return e.do(withSession(postJSON("/v1/chat/completions", fmt.Sprintf(
			`{"model":%q,"messages":[{"role":"user","content":"x"}]}`, model)), session))
	}

	// The trap this pins: an unknown inventory used to count as a reason to stay
	// lenient for everyone, so a dead box made every mistyped model id routable and
	// the live box answered it with whatever it had loaded. That is the silent model
	// swap the filter exists to prevent, arrived at through an outage.
	res, body := ask("qwen-not-a-model", "unknown-while-dead")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown model got %d: %s while a backend was down; a live box answered a model it does not serve",
			res.StatusCode, body)
	}
	if n := live.genCounts(); n != 0 {
		t.Errorf("strata served %d generations for a model it does not list, excused by a dead box's unknown inventory", n)
	}

	// Naming the box that has never answered is still an outage answer, not a
	// missing-model answer: rooter configured it, it is simply not talking.
	res, body = ask("phantom", "named-never-answer")
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("naming a configured backend that has never answered got %d: %s", res.StatusCode, body)
	}
	if !strings.Contains(body, "unhealthy") {
		t.Errorf("the answer does not say the box is down: %s", body)
	}

	// And the model the live box really serves still works, outage or no outage.
	res, body = ask("qwen-tiny", "real-while-dead")
	if res.StatusCode != 200 {
		t.Fatalf("the model a live box serves got %d: %s", res.StatusCode, body)
	}
}

// promLine reads one sample out of the /rooter/metrics text.
func promLine(t *testing.T, metrics, sample string) float64 {
	t.Helper()
	for _, line := range strings.Split(metrics, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, sample+" ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, sample+" "), 64)
			if err != nil {
				t.Fatalf("sample %q has an unparseable value: %q", sample, line)
			}
			return v
		}
	}
	t.Fatalf("metrics do not contain the sample %q:\n%s", sample, metrics)
	return 0
}
