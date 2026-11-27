package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// The ring's promise to a consumer is that a cursor is a position and a record id
// is a fact. These tests hold it to that, because the thing reading it is a
// telemetry store that will not notice a silently missing row.

func rec(id string) *callRecord {
	return &callRecord{ID: id, Backend: "strata", Model: "m", Status: 200}
}

// newRingRouter is a Router whose ring is the only thing under test, so these
// cases do not need a fake backend to prove a property of the ring.
func newRingRouter(t *testing.T, max int) *Router {
	t.Helper()
	// newEnv already registers a cleanup for the server; this only needs the
	// Router out of it.
	e := newEnv(t, []string{"a=" + newFake(t, "a", "m", "").url()}, nil)
	r := e.router
	r.events = newEventRing(max)
	return r
}

// TestRingStaleIsJudgedAgainstTheRingNotThePage: a consumer that is exactly caught
// up gets an empty page and must not be told it lost data, and a consumer whose
// cursor sits behind the eviction boundary must be told even when there is
// something after it.
func TestRingStaleIsJudgedAgainstTheRingNotThePage(t *testing.T) {
	r := newRingRouter(t, 16)
	for i := 0; i < 40; i++ {
		r.events.push(rec("id" + strconv.Itoa(i)))
	}

	first := r.events.events[0].Seq
	page := getEvents(t, r, "/rooter/events?cursor="+strconv.FormatInt(first-2, 10))
	if !page.Stale {
		t.Fatalf("a cursor behind the oldest retained record (seq %d) must be stale: %+v", first, page)
	}
	if page.Dropped == 0 {
		t.Fatal("a ring that evicted must report dropped_total")
	}

	caught := getEvents(t, r, "/rooter/events?cursor="+strconv.FormatInt(r.events.seq, 10))
	if caught.Stale {
		t.Fatalf("a cursor at the end of the ring is 'nothing new', not 'stale': %+v", caught)
	}
	if len(caught.Events) != 0 {
		t.Fatalf("a caught-up cursor returned events: %+v", caught.Events)
	}

	// A brand new consumer has no cursor at all, which is not the same as having
	// lost it.
	fresh := getEvents(t, r, "/rooter/events")
	if fresh.Stale {
		t.Fatal("a first pull must not be reported as stale")
	}
}

// TestRingCursorDoesNotMoveWithoutNewWork is what lets a harvester insert nothing
// on a poll that changed nothing, without having to compare rows itself.
func TestRingCursorDoesNotMoveWithoutNewWork(t *testing.T) {
	r := newRingRouter(t, 16)
	r.events.push(rec("one"))
	before := getEvents(t, r, "/rooter/events")
	after := getEvents(t, r, "/rooter/events?cursor="+strconv.FormatInt(before.Cursor, 10))
	if after.Cursor != before.Cursor || len(after.Events) != 0 {
		t.Fatalf("a no-change poll moved the cursor or returned rows: %+v then %+v", before, after)
	}
}

// TestPromptTokensDerivedOnlyWhenRooterDidTheArithmetic. The flag is only worth
// having if it is false on the rows that came from a usage block and true on the
// ones that were reconstructed, and never on a row that has neither.
func TestPromptTokensDerivedOnlyWhenRooterDidTheArithmetic(t *testing.T) {
	// A runner that sent a usage block: read, not derived.
	var withUsage timingsDoc
	if err := json.Unmarshal([]byte(`{
		"id":"chatcmpl-1","created":1793700000,
		"usage":{"prompt_tokens":1000,"completion_tokens":10,
			"prompt_tokens_details":{"cached_tokens":900}},
		"timings":{"prompt_n":100,"predicted_n":10,"cache_n":900,"prompt_ms":50,"decode_ms":200}
	}`), &withUsage); err != nil {
		t.Fatal(err)
	}
	c1 := &callRecord{}
	c1.absorb(&withUsage)
	if c1.PromptTokensDerived {
		t.Fatal("prompt_tokens came from usage.prompt_tokens; rooter did not derive it")
	}
	if c1.PromptTokens == nil || *c1.PromptTokens != 1000 {
		t.Fatalf("usage value must win over the reconstruction: %v", c1.PromptTokens)
	}

	// A plain llama.cpp /completion: timings only, so the total is rebuilt.
	var noUsage timingsDoc
	if err := json.Unmarshal([]byte(`{
		"id":"chatcmpl-2","created":1793700001,
		"timings":{"prompt_n":44,"predicted_n":10,"cache_n":88933,"prompt_ms":517,"decode_ms":6271}
	}`), &noUsage); err != nil {
		t.Fatal(err)
	}
	c2 := &callRecord{}
	c2.absorb(&noUsage)
	if !c2.PromptTokensDerived {
		t.Fatal("a reconstructed prompt total must say so")
	}
	if c2.PromptTokens == nil || *c2.PromptTokens != 88977 {
		t.Fatalf("expected 44 + 88933, got %v", c2.PromptTokens)
	}
	if c2.PromptTokensComputed == nil || *c2.PromptTokensComputed != 44 {
		t.Fatalf("prompt_tokens_computed must stay the work done, not the length: %v", c2.PromptTokensComputed)
	}
	// The denominator is only trustworthy if the identity the flag advertises
	// actually holds on the row.
	if *c2.CachedTokens+*c2.PromptTokensComputed != *c2.PromptTokens {
		t.Fatalf("derived prompt does not satisfy cached+computed==prompt: %d+%d != %d",
			*c2.CachedTokens, *c2.PromptTokensComputed, *c2.PromptTokens)
	}

	// Neither signal: nothing to derive, nothing to mark.
	var empty timingsDoc
	c3 := &callRecord{}
	c3.absorb(&empty)
	if c3.PromptTokensDerived || c3.PromptTokens != nil {
		t.Fatalf("a call with no accounting must publish neither: %+v", c3)
	}
}

// TestDerivedFlagIsOnTheWireButNotAdvertisedAsAHeader: it is an audit field for
// the store, not something a client needs to route on.
func TestDerivedFlagIsOnTheWireButNotAdvertisedAsAHeader(t *testing.T) {
	r := newRingRouter(t, 16)
	var doc timingsDoc
	if err := json.Unmarshal([]byte(`{"timings":{"prompt_n":10,"cache_n":90,"predicted_n":3}}`), &doc); err != nil {
		t.Fatal(err)
	}
	c := &callRecord{ID: "rooter-x", Backend: "strata", Model: "m", Status: 200}
	c.absorb(&doc)
	r.observeCall(c)

	page := getEvents(t, r, "/rooter/events")
	if len(page.Events) != 1 {
		t.Fatalf("expected one event, got %+v", page.Events)
	}
	if !page.Events[0].PromptTokensDerived {
		t.Fatal("derived row lost its flag through the ring")
	}
	raw, _ := json.Marshal(page.Events[0])
	if !strings.Contains(string(raw), `"prompt_tokens_derived":true`) {
		t.Fatalf("flag must be visible in the JSON: %s", raw)
	}
}

func getEvents(t *testing.T, r *Router, target string) eventsPage {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	r.eventsJSON(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: %d", target, rec.Code)
	}
	var p eventsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	return p
}

// rawEvents reads the endpoint as a consumer does - as JSON keys, not as Go fields -
// because a rename is exactly the failure a struct-typed test cannot see.
func rawEvents(t *testing.T, r *Router) []map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/rooter/events", nil)
	rr := httptest.NewRecorder()
	r.eventsJSON(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("/rooter/events: %d", rr.Code)
	}
	var envelope struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("envelope: %v", err)
	}
	return envelope.Events
}

// TestEventsPublishTheAccountingNamesAGateIsBuiltOn pins the prompt-accounting field
// names on the /rooter/events wire. pi-ed arms an idle-compaction warm gate on
// prompt_tokens_computed against cache_hit_rate, so the pair has to stay on the wire,
// stay numerically consistent with each other, and - the part that is easy to break by
// accident - keep publishing a hit rate of zero. These fields are pointer-typed with
// omitempty, so a change to how they are filled can make a zero vanish, and a gate
// reading "no cache_hit_rate key" as "fast" would then stop arming at exactly the
// sessions that need it most.
func TestEventsPublishTheAccountingNamesAGateIsBuiltOn(t *testing.T) {
	r := newRingRouter(t, 16)

	warm := rec("warm")
	warm.PromptTokens, warm.PromptTokensComputed, warm.CachedTokens = loadInt(31000), loadInt(12000), loadInt(19000)
	rate := 19000.0 / 31000.0
	warm.CacheHitRate = &rate
	r.events.push(warm)

	cold := rec("cold")
	cold.PromptTokens, cold.PromptTokensComputed, cold.CachedTokens = loadInt(40800), loadInt(40800), loadInt(0)
	zero := 0.0
	cold.CacheHitRate = &zero
	r.events.push(cold)

	none := rec("unmeasured")
	r.events.push(none)

	events := rawEvents(t, r)
	if len(events) != 3 {
		t.Fatalf("want 3 rows on the wire, got %d", len(events))
	}
	for _, key := range []string{"prompt_tokens", "prompt_tokens_computed", "cached_tokens", "cache_hit_rate"} {
		for _, row := range events[:2] {
			if _, ok := row[key]; !ok {
				t.Fatalf("%s is missing from a row a consumer is gating on: %v", key, row)
			}
		}
	}

	// The pair has to agree, because a gate that reads both is only as good as the
	// identity between them.
	for _, row := range events[:2] {
		prompt := row["prompt_tokens"].(float64)
		computed := row["prompt_tokens_computed"].(float64)
		cached := row["cached_tokens"].(float64)
		hit := row["cache_hit_rate"].(float64)
		if cached+computed != prompt {
			t.Errorf("cached(%v) + computed(%v) != prompt(%v) on %v", cached, computed, prompt, row["id"])
		}
		if !near(hit, cached/prompt, 0.001) {
			t.Errorf("cache_hit_rate %v is not cached/prompt (%v) on %v", hit, cached/prompt, row["id"])
		}
	}

	// A zero hit rate is a published fact, not an absent one: that row is the arm case.
	if events[1]["cache_hit_rate"] != float64(0) {
		t.Errorf("the fully cold row published cache_hit_rate %#v, want an explicit 0", events[1]["cache_hit_rate"])
	}

	// And a call rooter never measured publishes none of them, so "unknown" stays
	// distinguishable from "cold".
	for _, key := range []string{"prompt_tokens", "prompt_tokens_computed", "cached_tokens", "cache_hit_rate"} {
		if _, ok := events[2][key]; ok {
			t.Errorf("an unmeasured row published %s: %v", key, events[2])
		}
	}
}
