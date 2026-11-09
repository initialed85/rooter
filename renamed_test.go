package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// A backend name outlives the box it was given. When the Mac stopped being the
// zig-and-mlx box it used to be and became a second llama.cpp, the name "sushi"
// stopped naming anything true - but every client that had written it down kept
// writing it, in models.json entries, in saved /model picks, and in the model field
// of requests already in flight.
//
// So a rename is allowed, and -renamed says what the old name means now. These tests
// pin the two things it must not break: a request for a retired name still reaches a
// real box under a model that box really has, and a retired name is only ever a
// redirect - never a claim about what is loaded, and never a shadow over a real model
// id.

func TestRenamedNameStillRoutesToALiveBox(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "icarus", "qwen-big-iq3", "")
	e := newEnv(t, []string{"strata=" + a.url(), "icarus=" + b.url()}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus"}
	})
	e.pool.CheckAll()

	for i := 0; i < 6; i++ {
		res, body := e.do(withSession(postJSON("/v1/chat/completions",
			`{"model":"sushi","messages":[{"role":"user","content":"x"}]}`), "retired"))
		if res.StatusCode != 200 {
			t.Fatalf("request %d for a retired name got %d: %s", i, res.StatusCode, body)
		}
		// Upstream has to be told the model icarus actually loaded, because a llama.cpp
		// answers whatever it is holding and an alias is about the box, not the weights.
		if got := b.lastBody(); !strings.Contains(got, `"model":"qwen-big-iq3"`) {
			t.Errorf("request %d reached icarus as %q", i, got)
		}
		if got := a.lastBody(); strings.Contains(got, "qwen-big") {
			t.Errorf("request %d leaked to strata: %q", i, got)
		}
		// The client asked for a name that no longer exists; the only honest answer is
		// the box that really replied.
		if got := res.Header.Get("X-Rooter-Backend"); got != "icarus" {
			t.Errorf("request %d reported backend %q, want the box that answered", i, got)
		}
	}
	if n := a.genCounts(); n != 0 {
		t.Errorf("strata took %d generations from an alias for another box", n)
	}
}

// Rooter's own memory is the other place a retired name survives: a session binding
// stores a backend name. This is the shape a binding written before the rename would
// have, and it must still find a box rather than fall through to "no backend can
// serve this request".
func TestStaleBindingNamedAfterARetiredBoxStillSticks(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "icarus", "qwen-big-iq3", "")
	e := newEnv(t, []string{"strata=" + a.url(), "icarus=" + b.url()}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus"}
	})
	e.pool.CheckAll()

	if got := e.pool.Lookup("sushi"); got == nil {
		t.Fatal("Lookup does not resolve a retired name, so a binding made under it is a dead end")
	} else if got.Name != "icarus" {
		t.Errorf("Lookup resolved sushi to %q, want icarus", got.Name)
	}

	e.sess.Bind(BindKey("h:stale", "sushi"), "sushi") // as written before the rename
	_, _ = e.do(withSession(postJSON("/v1/chat/completions",
		`{"model":"sushi","messages":[{"role":"user","content":"x"}]}`), "stale"))
	if n := b.genCounts(); n != 1 {
		t.Errorf("icarus served %d generations, want the one the stale binding pointed at it", n)
	}
	if n := a.genCounts(); n != 0 {
		t.Errorf("strata served %d generations for a name that means icarus", n)
	}
	// And the binding is corrected on the way through, so a retired name is a
	// transitional thing in the request path rather than something the session store
	// has to keep understanding.
	if got, ok := e.sess.Get(BindKey("h:stale", "sushi")); !ok || got != "icarus" {
		t.Errorf("binding is %q (found=%v) after a request; rooter should have rewritten it to the live name", got, ok)
	}
}

// A retired name is the weakest claim there is. If a box really does serve a model by
// that id, the request is routed by inventory and the old name stops being a pin -
// otherwise renaming a box would hijack a model that happens to share its name.
func TestRetiredNameDoesNotShadowARealModel(t *testing.T) {
	a := newFake(t, "strata", "sushi", "") // a model genuinely named "sushi"
	b := newFake(t, "icarus", "qwen-big-iq3", "")
	e := newEnv(t, []string{"strata=" + a.url(), "icarus=" + b.url()}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus"}
	})
	e.pool.CheckAll()

	for i := 0; i < 4; i++ {
		res, body := e.do(withSession(postJSON("/v1/chat/completions",
			`{"model":"sushi","messages":[{"role":"user","content":"x"}]}`), "shadow"))
		if res.StatusCode != 200 {
			t.Fatalf("request %d got %d: %s", i, res.StatusCode, body)
		}
		if got := a.lastBody(); !strings.Contains(got, `"model":"sushi"`) {
			t.Errorf("request %d did not reach the box that serves a model named sushi: %q", i, got)
		}
	}
	if n := b.genCounts(); n != 0 {
		t.Errorf("the retired name hijacked %d generations away from the model that shares it", n)
	}

	// The catalog follows the same rule: one entry for the real model, and no synthetic
	// entry with the same id.
	_, out := e.do(get("/v1/models"))
	entries := catalogEntries(t, out)
	var n int
	for _, m := range entries {
		if m.ID == "sushi" {
			n++
			if m.Rooter.Alias {
				t.Errorf("the synthetic sushi entry survived a real model of the same name: %s", out)
			}
		}
	}
	if n != 1 {
		t.Errorf("catalog has %d entries for id \"sushi\", want exactly the real model: %s", n, out)
	}
}

// A typo in -renamed fails as a mystery rather than as an error: a retired name that
// resolves to nothing behaves exactly like a name nobody configured. Hence the checks.
func TestParseRenamedRejectsWhatCannotWork(t *testing.T) {
	backends := []*Backend{{Name: "strata"}, {Name: "icarus"}}
	for _, tc := range []struct {
		name string
		spec string
	}{
		{"no target", "sushi"},
		{"empty target", "sushi="},
		{"no source", "=icarus"},
		{"target is not a backend", "sushi=icaruz"},
		{"source is a live backend", "icarus=strata"},
		{"target is the source", "sushi=sushi"},
	} {
		if _, err := parseRenamed([]string{tc.spec}, backends); err == nil {
			t.Errorf("%s: -renamed %q was accepted", tc.name, tc.spec)
		}
	}
	// One source, twice, is two answers to the same question.
	if _, err := parseRenamed([]string{"sushi=icarus", "sushi=strata"}, backends); err == nil {
		t.Error("a name was renamed to two different boxes")
	}
	// A duplicate of the same mapping is a repeated flag, not a contradiction...
	if _, err := parseRenamed([]string{"sushi=icarus", "sushi=icarus"}, backends); err == nil {
		t.Error("the same -renamed twice was silently deduplicated")
	}

	// A good one resolves to the real name, however the flag spelled it.
	got, err := parseRenamed([]string{" old-box = ICARUS "}, backends)
	if err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	if len(got) != 1 || got[0].From != "old-box" || got[0].To != "icarus" {
		t.Errorf("got %+v", got)
	}
	// Nothing configured, nothing renamed.
	if got, err := parseRenamed(nil, backends); err != nil || len(got) != 0 {
		t.Errorf("empty -renamed gave %v, %v", got, err)
	}
}

// The redirect has to be visible in the list a client reads, or a retired name looks
// like a model that still exists somewhere.
func TestCatalogMarksARetiredNameAsARedirect(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "icarus", "qwen-big-iq3", "")
	e := newEnv(t, []string{"strata=" + a.url(), "icarus=" + b.url()}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus"}
	})
	e.pool.CheckAll()

	_, out := e.do(get("/v1/models"))
	entries := catalogEntries(t, out)

	at := -1
	for i, m := range entries {
		if m.ID == "sushi" {
			at = i
			if !m.Rooter.Alias {
				t.Errorf("sushi is not marked as an alias: %s", out)
			}
			if m.Rooter.AliasOf != "icarus" {
				t.Errorf("sushi has alias_of %q, want icarus: %s", m.Rooter.AliasOf, out)
			}
			if m.Rooter.DefaultModel != "qwen-big-iq3" {
				t.Errorf("sushi has default_model %q, want the model icarus loaded: %s", m.Rooter.DefaultModel, out)
			}
		}
	}
	if at < 0 {
		t.Fatalf("no sushi entry in the catalog: %s", out)
	}
	// Still ordered so a client reading top-to-bottom lands on real weights: the
	// retired name sits with the aliases, below the models and above the meta names.
	var firstModel, meta = -1, -1
	for i, m := range entries {
		if m.ID == "qwen-big-iq3" && firstModel < 0 {
			firstModel = i
		}
		if m.Rooter.Meta {
			meta = i
		}
	}
	if firstModel < 0 || meta < 0 {
		t.Fatalf("catalog is missing a real model or a meta name: %s", out)
	}
	if !(firstModel < at && at < meta) {
		t.Errorf("ordering: real model at %d, retired name at %d, meta at %d\n%s", firstModel, at, meta, out)
	}

	// And the retired name is not a separate thing in rooter's own accounting: it does
	// not appear as a backend, only as a mapping.
	_, bs := e.do(get("/rooter/backends"))
	if strings.Contains(bs, `"sushi"`) {
		t.Errorf("/rooter/backends invented a backend named sushi: %s", bs)
	}
	_, st := e.do(get("/rooter/stats"))
	if !strings.Contains(st, `"renamed"`) || !strings.Contains(st, `"from": "sushi"`) {
		t.Errorf("/rooter/stats does not say which names are retired: %s", st)
	}
}

// /rooter/forget and every sticky read path address a backend by name, so the retired
// one must not be the name that quietly does nothing.
func TestRenamedNamesAreListedWhereARouterIsRead(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "icarus", "qwen-big-iq3", "")
	e := newEnv(t, []string{"strata=" + a.url(), "icarus=" + b.url()}, func(c *Config) {
		c.RenamedSpecs = []string{"sushi=icarus", "old-box=strata"}
	})
	e.pool.CheckAll()
	names := e.pool.RenamedNames()
	if len(names) != 2 || names[0].From != "old-box" || names[1].From != "sushi" {
		t.Errorf("RenamedNames = %+v, want both, sorted by the retired name", names)
	}
	_, idx := e.do(get("/rooter/"))
	if !strings.Contains(idx, "sushi -> icarus") || !strings.Contains(idx, "old-box -> strata") {
		t.Errorf("the admin index hides the retired names:\n%s", idx)
	}
}

type catalogEntry struct {
	ID     string `json:"id"`
	Rooter struct {
		Alias        bool     `json:"alias"`
		AliasOf      string   `json:"alias_of"`
		Meta         bool     `json:"meta"`
		Backend      string   `json:"backend"`
		Backends     []string `json:"backends"`
		DefaultModel string   `json:"default_model"`
		SlotsTotal   int      `json:"slots_total"`
	} `json:"rooter"`
}

func catalogEntries(t *testing.T, body string) []catalogEntry {
	t.Helper()
	var doc struct {
		Data []catalogEntry `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("bad catalog json: %v\n%s", err, body)
	}
	return doc.Data
}
