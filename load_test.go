package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// bigGenBody makes a generation request whose prompt is roughly bytes long, so a
// test can assert against the size rooter should be reporting as in-flight work.
func bigGenBody(bytes int) string {
	tail := strings.Repeat("filler ", bytes/7)
	return fmt.Sprintf(`{"model":"test-model","messages":[{"role":"user","content":"%s"}],"stream":true}`, tail)
}

func loadInt(v int) *int { return &v }

func backendsDoc(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("/rooter/backends is not the JSON array it has always been: %v (%s)", err, body)
	}
	return out
}

func field(t *testing.T, doc map[string]any, key string) (float64, bool) {
	t.Helper()
	v, ok := doc[key]
	if !ok {
		return 0, false
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("%s is %#v, not a number", key, v)
	}
	return f, true
}

func docByName(docs []map[string]any, name string) map[string]any {
	for _, d := range docs {
		if d["name"] == name {
			return d
		}
	}
	return nil
}

// near compares a rate or a duration where the point is the order of magnitude and
// the arithmetic, not the last digit.
func near(got, want, tol float64) bool {
	lo, hi := want*(1-tol), want*(1+tol)
	return got >= lo && got <= hi
}

// TestBackendsEndpointShowsWhatEachBoxIsHolding is the read a caller needs before
// it commits a big prompt: busy and concurrency say a slot is taken, they do not
// say how much work is already queued behind that slot, and a 40 KB prompt and a
// 400 KB one are not the same wait.
//
// The cold rate is seeded with the record a real cold call would have produced
// (40,500 tokens evaluated in 16.2 s post-handoff = 2,500 tok/s), because the fake
// upstream answers with no usage object at all. That is deliberate: the arithmetic
// under test is rooter's, and it should be checked against a number whose truth is
// known rather than against a fake's guess.
func TestBackendsEndpointShowsWhatEachBoxIsHolding(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=2"}, func(c *Config) { c.RequestTimeout = 0 })
	a.setLatency(900 * time.Millisecond)

	e.pool.ObserveLoad(&callRecord{
		Backend: "a", TTFTMs: 16200, WaitMs: 0,
		PromptTokens: loadInt(40500), PromptTokensComputed: loadInt(40500),
	})

	body := bigGenBody(110000)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := clientReq(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			res, _, err := e.doErr(req)
			if err != nil {
				t.Errorf("generation: %v", err)
				return
			}
			res.Body.Close()
		}()
	}
	time.Sleep(300 * time.Millisecond) // both are holding a slot and a body by now

	res, raw := e.do(get("/rooter/backends"))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("/rooter/backends answered %d", res.StatusCode)
	}
	doc := docByName(backendsDoc(t, raw), "a")
	if doc == nil {
		t.Fatalf("no backend a in %s", raw)
	}
	if got, _ := field(t, doc, "busy"); got != 2 {
		t.Errorf("busy = %v, want 2 while two generations are running", doc["busy"])
	}
	if got, _ := field(t, doc, "inflight"); got != 2 {
		t.Errorf("inflight = %v, want 2", doc["inflight"])
	}
	if got, _ := field(t, doc, "slotsFree"); got != 0 {
		t.Errorf("slotsFree = %v, want 0", doc["slotsFree"])
	}
	wantBytes := 2 * float64(len(body))
	if got, _ := field(t, doc, "inflightBytes"); !near(got, wantBytes, 0.01) {
		t.Errorf("inflightBytes = %v, want the two bodies in flight (%v)", got, wantBytes)
	}
	if got, _ := field(t, doc, "maxInflightBytes"); !near(got, float64(len(body)), 0.01) {
		t.Errorf("maxInflightBytes = %v, want %v", got, len(body))
	}
	if got, ok := field(t, doc, "oldestInflightMs"); !ok || got <= 0 {
		t.Errorf("oldestInflightMs = %v, want a live age for the work already handed over", doc["oldestInflightMs"])
	}
	if got, _ := field(t, doc, "coldPrefillTokS"); !near(got, 2500, 0.01) {
		t.Errorf("coldPrefillTokS = %v, want the seeded 2500", doc["coldPrefillTokS"])
	}
	if _, ok := doc["estFirstByteQueuedMs"]; ok {
		t.Errorf("an estimate appeared without anybody asking for one: %s", raw)
	}

	// The same endpoint, asked the harder question.
	_, raw = e.do(get("/rooter/backends?promptBytes=200000"))
	doc = docByName(backendsDoc(t, raw), "a")
	if doc == nil {
		t.Fatalf("no backend a in %s", raw)
	}
	own := 200000.0 / BytesPerToken // ~49,383 tokens
	if got, _ := field(t, doc, "ownTokens"); !near(got, own, 0.01) {
		t.Errorf("ownTokens = %v, want %v", doc["ownTokens"], own)
	}
	free, _ := field(t, doc, "estFirstByteFreeMs")
	queued, _ := field(t, doc, "estFirstByteQueuedMs")
	if !near(free, own/2500*1000, 0.02) {
		t.Errorf("estFirstByteFreeMs = %v, want %v at the observed 2500 tok/s", free, own/2500*1000)
	}
	if queued <= free {
		t.Errorf("a box holding two 110 KB prompts estimated %v ms, no more than an idle box at %v ms", queued, free)
	}
	// Each in-flight prompt is ~27,160 tokens, of which a few hundred seconds of
	// 900 ms of running has been spent: the backlog has to be most of both.
	if got, _ := field(t, doc, "queuedTokens"); got < 40000 {
		t.Errorf("queuedTokens = %v, want most of the two 27k-token prompts still ahead of us", got)
	}

	wg.Wait()

	_, raw = e.do(get("/rooter/backends"))
	doc = docByName(backendsDoc(t, raw), "a")
	if got, _ := field(t, doc, "inflight"); got != 0 {
		t.Errorf("inflight = %v after both generations finished: %s", got, raw)
	}
	if got, _ := field(t, doc, "inflightBytes"); got != 0 {
		t.Errorf("inflightBytes = %v after both generations finished", got)
	}
}

// TestAWarmCallDoesNotBendTheColdRate protects the one distinction that makes the
// load numbers usable: a call that restored 50,000 tokens in 50 ms is not evidence
// about how fast this box evaluates a prompt it has never seen, and averaging the
// two produces a rate that describes neither.
func TestAWarmCallDoesNotBendTheColdRate(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)

	cold := func(tokens int, ms float64) *callRecord {
		return &callRecord{Backend: "a", TTFTMs: ms, WaitMs: 0,
			PromptTokens: loadInt(tokens), PromptTokensComputed: loadInt(tokens)}
	}
	e.pool.ObserveLoad(cold(40500, 16200))  // 2,500 tok/s
	e.pool.ObserveLoad(cold(100000, 40000)) // 2,500 tok/s again

	warm := &callRecord{Backend: "a", TTFTMs: 50, WaitMs: 0,
		PromptTokens: loadInt(60000), PromptTokensComputed: loadInt(120), CachedTokens: loadInt(59880)}
	e.pool.ObserveLoad(warm)

	// Read back through the marshalled snapshot rather than the fields themselves:
	// those are guarded by Pool.mu, and this is the rendering a caller actually gets.
	raw, err := e.pool.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := docByName(backendsDoc(t, string(raw)), "a")
	if doc == nil {
		t.Fatalf("no backend a in %s", raw)
	}
	if got, _ := field(t, doc, "coldSamples"); got != 2 {
		t.Fatalf("coldSamples = %v, want 2: a warm call was counted as fresh work", doc["coldSamples"])
	}
	if got, _ := field(t, doc, "coldPrefillTokS"); !near(got, 2500, 0.01) {
		t.Errorf("coldPrefillTokS = %v, want 2500 unchanged by the warm call", doc["coldPrefillTokS"])
	}
	if got, _ := field(t, doc, "restoreSamples"); got != 1 {
		t.Fatalf("restoreSamples = %v, want 1", doc["restoreSamples"])
	}
	if got, _ := field(t, doc, "restoreTokS"); !near(got, 59880/0.05, 0.01) {
		t.Errorf("restoreTokS = %v, want %v", doc["restoreTokS"], 59880/0.05)
	}

	// A prompt below the cold threshold is neither: it is a warm call or a small
	// one, and neither says anything about cold prefill.
	e.pool.ObserveLoad(&callRecord{Backend: "a", TTFTMs: 900, WaitMs: 0,
		PromptTokens: loadInt(3000), PromptTokensComputed: loadInt(1500)})
	raw, _ = e.pool.SnapshotJSON()
	doc = docByName(backendsDoc(t, string(raw)), "a")
	if got, _ := field(t, doc, "coldSamples"); got != 2 {
		t.Errorf("a 1,500-token computed sample was counted as cold work: coldSamples = %v", doc["coldSamples"])
	}
	if got, _ := field(t, doc, "restoreSamples"); got != 1 {
		t.Errorf("a 1,500-token computed sample was counted as a restore: restoreSamples = %v", doc["restoreSamples"])
	}

	est := e.pool.Estimates(200000)
	if _, ok := est["a"]["estFirstByteQueuedMs"]; !ok {
		t.Errorf("no queued estimate on a backend with 2 cold samples: %#v", est)
	}

	// No observation, no claim: a box rooter has never finished a cold call on has
	// no business handing a caller a predicted first-byte time to size a deadline on.
	b := newFake(t, "b", "test-model", "")
	unmeasured := newEnv(t, []string{"b=" + b.url()}, nil)
	if got := unmeasured.pool.Estimates(200000); len(got) != 0 {
		t.Errorf("Estimates produced %v for a backend rooter has never seen evaluate a prompt", got)
	}
	_, body := unmeasured.do(get("/rooter/backends?promptBytes=200000"))
	doc = docByName(backendsDoc(t, body), "b")
	if doc == nil {
		t.Fatalf("no backend b in %s", body)
	}
	if doc["estFirstByteQueuedMs"] != nil {
		t.Errorf("an unmeasured backend was handed an estimate: %s", body)
	}
	if _, ok := doc["inflight"]; !ok {
		t.Errorf("a backend with no traffic omitted inflight, which is the answer a caller needs: %s", body)
	}
}

// TestAbandonedGenerationReleasesItsLoadNote is the leak guard for the accounting
// this endpoint depends on. An abandoned stream unwinds through net/http's silent
// recovery of http.ErrAbortHandler, which is exactly the path that used to skip
// rooter's post-dispatch bookkeeping. A note left behind would keep a healthy box
// looking busy with a prompt it stopped evaluating, and every estimate built from
// it would be fiction.
func TestAbandonedGenerationReleasesItsLoadNote(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, func(c *Config) { c.RequestTimeout = 0 })
	a.setLatency(1200 * time.Millisecond)

	front := httptest.NewServer(e.router)
	defer front.Close()

	body := bigGenBody(120000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequest("POST", front.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	res.Body.Read(buf) // one byte of the stream, then walk away
	res.Body.Close()
	cancel()

	time.Sleep(2500 * time.Millisecond)

	_, raw := e.do(get("/rooter/backends"))
	doc := docByName(backendsDoc(t, raw), "a")
	if got, _ := field(t, doc, "inflight"); got != 0 {
		t.Errorf("an abandoned generation is still recorded as work in flight: %s", raw)
	}
	if got, _ := field(t, doc, "inflightBytes"); got != 0 {
		t.Errorf("an abandoned generation is still holding %v bytes of queue: %s", got, raw)
	}
	if v, _ := e.router.Stats()["aborted"].(int64); v == 0 {
		t.Errorf("the abandonment was not counted at all")
	}
}

// TestTheRunnersOwnPrefillWindowPricesColdWork is the fix for what the first live
// read of this endpoint caught: strata published coldPrefillTokS 793,590.6 off one
// sample, which is not a box evaluating a prompt at 800k tok/s, it is a prefix-cache
// restore arriving as prompt_n with cached=0 and a first byte 50 ms later. Rooter
// prices cold work against the duration the runner measured around the work it did,
// and falls back to its own post-handoff clock only when the runner says nothing.
func TestTheRunnersOwnPrefillWindowPricesColdWork(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)

	prefillMs := 560.0
	e.pool.ObserveLoad(&callRecord{
		Backend: "a", TTFTMs: 50, WaitMs: 0, PrefillMs: &prefillMs,
		PromptTokens: loadInt(40500), PromptTokensComputed: loadInt(40500),
	})

	raw, err := e.pool.SnapshotJSON()
	if err != nil {
		t.Fatal(err)
	}
	doc := docByName(backendsDoc(t, string(raw)), "a")
	got, _ := field(t, doc, "coldPrefillTokS")
	if !near(got, 40500/0.560, 0.01) {
		t.Errorf("coldPrefillTokS = %v, want %v: the runner reported a 560 ms prefill window and rooter priced the work against 50 ms of first-byte time",
			got, 40500/0.560)
	}
	if got > 150000 {
		t.Errorf("coldPrefillTokS = %v, which is a restore wearing a prefill's clothes", got)
	}

	// No prefill window reported: rooter's own clock is the only evidence there is.
	b := newFake(t, "b", "test-model", "")
	untimed := newEnv(t, []string{"b=" + b.url()}, nil)
	untimed.pool.ObserveLoad(&callRecord{Backend: "b", TTFTMs: 16200, WaitMs: 120,
		PromptTokens: loadInt(40500), PromptTokensComputed: loadInt(40500)})
	raw, _ = untimed.pool.SnapshotJSON()
	doc = docByName(backendsDoc(t, string(raw)), "b")
	if got, _ := field(t, doc, "coldPrefillTokS"); !near(got, 40500/16.08, 0.01) {
		t.Errorf("coldPrefillTokS = %v, want 40,500 tokens over 16.08 s of post-handoff time", doc["coldPrefillTokS"])
	}
}

// TestBackendsFieldContractIsWhatCallersDependOn pins the five fields a caller has
// built a decision on - pi-ed's idle-compaction pre-flight reads them to decide
// whether to defer a summarisation - so that a rename or a meaning change fails a
// build rather than turning a shipped fleet rule into guesswork.
//
// The invariant worth pinning is not any single field, it is the arithmetic between
// two of them: busy + slotsFree == concurrency, always, because both read one
// semaphore (free() is cap(sem) - len(sem)). And the trap worth pinning is what
// concurrency means: it is rooter's admission limit from the -backend spec, not the
// runner's slot count. strata answers /props total_slots=1 while home-ops passes
// concurrency=4, so slotsFree on that box is headroom for rooter to hand it work,
// not evidence that the box can run it in parallel.
func TestBackendsFieldContractIsWhatCallersDependOn(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=4"}, func(c *Config) { c.RequestTimeout = 0 })

	_, raw := e.do(get("/rooter/backends"))
	doc := docByName(backendsDoc(t, raw), "a")
	if doc == nil {
		t.Fatalf("no backend a in %s", raw)
	}
	for _, key := range []string{"name", "busy", "concurrency", "healthy", "slotsFree"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("%s is missing from /rooter/backends; a caller is depending on it: %s", key, raw)
		}
	}
	if doc["name"] != "a" {
		t.Errorf("name = %#v, want the backend's own name", doc["name"])
	}
	if _, ok := doc["healthy"].(bool); !ok {
		t.Errorf("healthy = %#v, want a JSON boolean", doc["healthy"])
	}
	busy, _ := field(t, doc, "busy")
	free, _ := field(t, doc, "slotsFree")
	concurrency, _ := field(t, doc, "concurrency")
	if concurrency != 4 {
		t.Fatalf("concurrency = %v, want the 4 from the -backend spec", doc["concurrency"])
	}
	if busy+free != concurrency {
		t.Errorf("busy(%v) + slotsFree(%v) != concurrency(%v): a caller comparing slotsFree > 0 against busy < concurrency is now reading two different boxes",
			busy, free, concurrency)
	}

	// The meaning, not just the shape: rooter reports what it was told to admit, and
	// nothing about how many slots the runner actually runs. A backend declared with
	// no concurrency=N gets 1, so its slotsFree collapses to a 0/1 flag while the box
	// may be running four internally.
	e2 := newEnv(t, []string{"b=" + newFake(t, "b", "test-model", "").url()}, nil)
	_, raw = e2.do(get("/rooter/backends"))
	doc = docByName(backendsDoc(t, raw), "b")
	if got, _ := field(t, doc, "concurrency"); got != 1 {
		t.Errorf("concurrency = %v on a backend given no explicit concurrency, want the pool default 1", doc["concurrency"])
	}
	if got, _ := field(t, doc, "slotsFree"); got != 1 {
		t.Errorf("slotsFree = %v on an idle default-width backend, want 1", doc["slotsFree"])
	}
}

// TestEstimatesAreTheArithmeticAPiDeadlineIsSizedFrom pins the estimate block on
// /rooter/backends, because pi-ed is now converting prompt tokens into a wall-clock
// budget with these numbers (compaction deadline sizing), and two of the three
// bounds disagree by design: estFirstByteFreeMs assumes the box takes the work now,
// estFirstByteQueuedMs assumes it finishes what it is holding first. A deadline sized
// against the free one is a deadline that gets missed, so the ordering of the three is
// the contract - and so is the absence, because a box rooter has never seen prefill a
// large prompt gets no estimate at all rather than a flattering one.
func TestEstimatesAreTheArithmeticAPiDeadlineIsSizedFrom(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url() + ",concurrency=4", "b=" + newFake(t, "b", "test-model", "").url()}, func(c *Config) { c.RequestTimeout = 0 })

	// Seed exactly one cold sample so the EWMA equals a number whose truth is known:
	// 40,500 tokens evaluated in 16.2 s of post-handoff time = 2,500 tok/s.
	e.pool.ObserveLoad(&callRecord{
		Backend: "a", TTFTMs: 16200, WaitMs: 0,
		PromptTokens: loadInt(40500), PromptTokensComputed: loadInt(40500),
	})

	const promptBytes = 200000
	ownTokens := float64(promptBytes) / BytesPerToken // 49,383 tokens

	_, raw := e.do(get("/rooter/backends?promptBytes=" + strconv.Itoa(promptBytes)))
	doc := docByName(backendsDoc(t, raw), "a")
	if doc == nil {
		t.Fatalf("no backend a in %s", raw)
	}
	for _, key := range []string{"coldPrefillTokS", "queuedTokens", "ownTokens",
		"estFirstByteFreeMs", "estFirstByteQueuedMs", "estFirstByteMaxSlotMs"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("%s is missing from /rooter/backends?promptBytes=...; a caller is sizing a deadline from it: %s", key, raw)
		}
	}
	coldTokS, _ := field(t, doc, "coldPrefillTokS")
	if !near(coldTokS, 2500, 0.01) {
		t.Fatalf("coldPrefillTokS = %v, want the seeded 2500", coldTokS)
	}
	own, _ := field(t, doc, "ownTokens")
	if !near(own, ownTokens, 0.001) {
		t.Errorf("ownTokens = %v, want the requested prompt at BytesPerToken (%v)", own, ownTokens)
	}

	// Nothing queued yet: all three bounds collapse onto one number, and that number
	// is the prompt's own cost at the box's cold rate.
	free, _ := field(t, doc, "estFirstByteFreeMs")
	queued, _ := field(t, doc, "estFirstByteQueuedMs")
	maxSlot, _ := field(t, doc, "estFirstByteMaxSlotMs")
	if !near(free, ownTokens/coldTokS*1000, 0.01) {
		t.Errorf("estFirstByteFreeMs = %v, want %v (ownTokens / coldPrefillTokS)", free, ownTokens/coldTokS*1000)
	}
	if queued != free || maxSlot != free {
		t.Errorf("on a box holding nothing every bound is the same wait: free=%v queued=%v maxSlot=%v", free, queued, maxSlot)
	}
	if got, _ := field(t, doc, "queuedTokens"); got != 0 {
		t.Errorf("queuedTokens = %v on a box holding nothing, want 0", got)
	}

	// Now the box is holding two prompts: 10,000 tokens and 20,000 tokens. queued is
	// the sum (the reading a deadline should be sized from), maxSlot is only the
	// longest single one (the reading that holds if the box runs its slots in
	// parallel), and free ignores both. They must never collapse or invert.
	b := e.pool.Lookup("a")
	if b == nil {
		t.Fatal("Lookup(a) found nothing")
	}
	id1 := e.pool.BeginFlight(b, 40500)
	id2 := e.pool.BeginFlight(b, 81000)
	_, raw = e.do(get("/rooter/backends?promptBytes=" + strconv.Itoa(promptBytes)))
	doc = docByName(backendsDoc(t, raw), "a")
	qTok, _ := field(t, doc, "queuedTokens")
	free2, _ := field(t, doc, "estFirstByteFreeMs")
	queued2, _ := field(t, doc, "estFirstByteQueuedMs")
	max2, _ := field(t, doc, "estFirstByteMaxSlotMs")
	// queuedTokens is the backlog alone - the work already handed over - while every
	// est*Ms adds the asked-for prompt on top of it. Conflating the two is the obvious
	// way to read a queued estimate as half its real size, so it is pinned here.
	if !near(qTok, 30000, 0.02) {
		t.Errorf("queuedTokens = %v, want the two bodies already in flight (30,000 tokens), without the prompt being asked about", qTok)
	}
	if !(free2 <= max2 && max2 < queued2) {
		t.Errorf("bounds wrong: free=%v maxSlot=%v queued=%v; want free <= maxSlot < queued when the backlog is more than one request, because a deadline sized on the wrong one is missed by up to the whole backlog", free2, max2, queued2)
	}
	if !near(queued2, (qTok+ownTokens)/coldTokS*1000, 0.02) {
		t.Errorf("estFirstByteQueuedMs = %v, want (queuedTokens + ownTokens) / coldPrefillTokS = %v", queued2, (qTok+ownTokens)/coldTokS*1000)
	}
	if !near(max2, (20000+ownTokens)/coldTokS*1000, 0.02) {
		t.Errorf("estFirstByteMaxSlotMs = %v, want the longest single body (20,000) plus ownTokens at the cold rate = %v", max2, (20000+ownTokens)/coldTokS*1000)
	}
	if !near(free2, free, 0.001) {
		t.Errorf("estFirstByteFreeMs moved from %v to %v: it is the box taking the work now, so it must not absorb the backlog", free, free2)
	}

	// Work the box has already overtaken counts as zero, never as a negative backlog:
	// an hour-old body must not buy the next caller a discount it did not earn.
	e.pool.mu.Lock()
	for i := range b.inFlight {
		b.inFlight[i].Since = time.Now().Add(-time.Hour)
	}
	e.pool.mu.Unlock()
	ests := e.pool.Estimates(promptBytes)
	if aged := ests["a"]; aged == nil {
		t.Fatal("no estimate for a after ageing its backlog out")
	} else if aged["queuedTokens"] != int64(0) {
		t.Errorf("queuedTokens = %v with an hour-old backlog, want 0 rather than a negative credit", aged["queuedTokens"])
	}
	e.pool.EndFlight(b, id1)
	e.pool.EndFlight(b, id2)

	// Nobody asked, nothing published: an estimate answers a promptBytes the caller
	// supplied, it is not a standing claim about the fleet.
	if got := e.pool.Estimates(0); len(got) != 0 {
		t.Errorf("Estimates(0) returned %d entries, want none", len(got))
	}

	// A box rooter has never seen prefill a large prompt gets no estimate at all.
	// Absence is the case a caller has to read as unknown, never as fast.
	_, raw = e.do(get("/rooter/backends?promptBytes=" + strconv.Itoa(promptBytes)))
	doc = docByName(backendsDoc(t, raw), "b")
	if doc == nil {
		t.Fatal("no backend b")
	}
	for _, key := range []string{"coldPrefillTokS", "estFirstByteFreeMs", "estFirstByteQueuedMs", "estFirstByteMaxSlotMs"} {
		if _, ok := doc[key]; ok {
			t.Errorf("unobserved backend b published %s; the promise is that an estimate is a measurement: %v", key, doc)
		}
	}
}
