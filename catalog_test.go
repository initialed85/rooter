package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// The regression this file pins down: /v1/models was a passthrough, so on the
// live box two calls a second apart returned disjoint lists - one gave strata's
// single model, the next gave sushi's four. Which models "exist" depended on
// which backend happened to be free.
func TestCatalogIsTheUnionNotWhicheverBackendWasFree(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "sushi", "sushi-big-2.6bpw", "")
	e := newEnv(t, []string{"strata=" + a.url(), "sushi=" + b.url()}, nil)
	e.pool.CheckAll()

	var first map[string]bool
	for i := 0; i < 8; i++ {
		res, body := e.do(get("/v1/models"))
		if res.StatusCode != 200 {
			t.Fatalf("status %d", res.StatusCode)
		}
		var doc struct {
			Object string `json:"object"`
			Data   []struct {
				ID      string `json:"id"`
				OwnedBy string `json:"owned_by"`
				Rooter  struct {
					Alias        bool   `json:"alias"`
					Meta         bool   `json:"meta"`
					Backend      string `json:"backend"`
					DefaultModel string `json:"default_model"`
				} `json:"rooter"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatalf("bad json: %v\n%s", err, body)
		}
		if doc.Object != "list" {
			t.Errorf("object=%q", doc.Object)
		}
		ids := map[string]bool{}
		for _, m := range doc.Data {
			ids[m.ID] = true
		}
		for _, want := range []string{"qwen-tiny-iq3", "sushi-big-2.6bpw", "strata", "sushi", "auto"} {
			if !ids[want] {
				t.Errorf("call %d is missing %q; got %v", i, want, keys(ids))
			}
		}
		// The whole point: every call has to agree, because the list is built from
		// what rooter knows rather than from whichever box answered.
		if i == 0 {
			first = ids
			continue
		}
		for k := range first {
			if !ids[k] {
				t.Errorf("call %d dropped %q that call 0 had: %v", i, k, keys(ids))
			}
		}
		for k := range ids {
			if !first[k] {
				t.Errorf("call %d invented %q that call 0 did not have", i, k)
			}
		}
	}

	// The alias entries have to say what they resolve to, or a client picking
	// "sushi" has no idea which model it is going to get.
	_, body := e.do(get("/v1/models"))
	if !strings.Contains(body, `"default_model": "sushi-big-2.6bpw"`) &&
		!strings.Contains(body, `"default_model":"sushi-big-2.6bpw"`) {
		t.Errorf("sushi alias carries no default_model:\n%s", body)
	}
}

// An alias is a hard pin, not a preference: it must never spill onto the other
// box, and the body going upstream must name a model that box really has.
func TestBackendAliasPinsToThatBackendOnly(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "sushi", "sushi-big-2.6bpw", "")
	e := newEnv(t, []string{"strata=" + a.url(), "sushi=" + b.url()}, nil)
	e.pool.CheckAll()

	for i := 0; i < 6; i++ {
		res, body := e.do(postJSON("/v1/chat/completions",
			`{"model":"sushi","messages":[{"role":"user","content":"x"}]}`))
		if res.StatusCode != 200 {
			t.Fatalf("alias request %d got %d: %s", i, res.StatusCode, body)
		}
		if got := b.lastBody(); !strings.Contains(got, `"model":"sushi-big-2.6bpw"`) {
			t.Errorf("alias request %d reached sushi as %q", i, got)
		}
		if got := a.lastBody(); strings.Contains(got, "sushi") {
			t.Errorf("alias request %d leaked to strata: %q", i, got)
		}
	}
	for i := 0; i < 6; i++ {
		res, body := e.do(postJSON("/v1/chat/completions",
			`{"model":"strata","messages":[{"role":"user","content":"x"}]}`))
		if res.StatusCode != 200 {
			t.Fatalf("alias request %d got %d: %s", i, res.StatusCode, body)
		}
		if got := a.lastBody(); !strings.Contains(got, `"model":"qwen-tiny-iq3"`) {
			t.Errorf("alias request %d reached strata as %q", i, got)
		}
	}
}

// A real model id that happens to share a backend's name wins over the alias,
// and the synthetic entry is dropped so the list never carries duplicate ids.
func TestRealModelOutranksAnAliasOfTheSameName(t *testing.T) {
	a := newFake(t, "strata", "strata", "")
	e := newEnv(t, []string{"strata=" + a.url()}, nil)
	e.pool.CheckAll()
	_, body := e.do(get("/v1/models"))
	var doc struct {
		Data []struct {
			ID     string `json:"id"`
			Rooter struct {
				Alias bool `json:"alias"`
			} `json:"rooter"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	for _, m := range doc.Data {
		if m.ID == "strata" && m.Rooter.Alias {
			t.Errorf("the strata alias should have been dropped in favour of the real model: %s", body)
		}
	}
}

func TestRewriteModelFieldTouchesNothingElse(t *testing.T) {
	in := `{"model":"sushi","temperature":0.10,"messages":[{"role":"user","content":"a:b"}],"stream":true}`
	out, ok := rewriteModelField([]byte(in), "sushi-big-2.6bpw")
	if !ok {
		t.Fatal("rewrite refused")
	}
	want := `{"model":"sushi-big-2.6bpw","temperature":0.10,"messages":[{"role":"user","content":"a:b"}],"stream":true}`
	if string(out) != want {
		t.Errorf("rewrote more than the model\n got %s\nwant %s", out, want)
	}
	// Whitespace and key order have to survive: a remarshal would move them.
	spaced := `{ "model" : "sushi" , "n": 1 }`
	out, ok = rewriteModelField([]byte(spaced), "m2")
	if !ok || string(out) != `{ "model" : "m2" , "n": 1 }` {
		t.Errorf("whitespace not preserved: ok=%v %s", ok, out)
	}
	// A "model" nested inside another value is not the top-level one.
	if _, ok := rewriteModelField([]byte(`{"a":{"model":"nope"},"model":"top"}`), "yes"); !ok {
		t.Error("refused a valid body with a nested model")
	}
	nested, _ := rewriteModelField([]byte(`{"a":{"model":"nope"},"model":"top"}`), "yes")
	if !strings.Contains(string(nested), `"model":"nope"`) || !strings.Contains(string(nested), `"model":"yes"`) {
		t.Errorf("nested rewrite went wrong: %s", nested)
	}
	// No model field at all -> refuse, so the caller leaves the body alone.
	if _, ok := rewriteModelField([]byte(`{"messages":[]}`), "x"); ok {
		t.Errorf("rewrote a body with no model field")
	}
	// A non-object body must not be touched either.
	if _, ok := rewriteModelField([]byte(`[1,2,3]`), "x"); ok {
		t.Errorf("rewrote a non-object body")
	}
}

// -no-catalog has to restore the old behaviour exactly, or it is not an escape
// hatch.
func TestNoCatalogFallsBackToPassthrough(t *testing.T) {
	a := newFake(t, "strata", "qwen-tiny-iq3", "")
	b := newFake(t, "sushi", "sushi-big-2.6bpw", "")
	e := newEnv(t, []string{"strata=" + a.url(), "sushi=" + b.url()}, func(c *Config) { c.NoCatalog = true })
	e.pool.CheckAll()
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		_, body := e.do(get("/v1/models"))
		var doc struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &doc); err != nil {
			t.Fatal(err)
		}
		if len(doc.Data) != 1 {
			t.Errorf("passthrough returned %d models, want the 1 a single backend reports: %s", len(doc.Data), body)
		}
		seen[doc.Data[0].ID] = true
	}
	// Each individual call reports exactly one backend's inventory, and the set
	// across calls is the disagreement the catalog exists to remove. No alias and
	// no meta entry survives, because rooter never saw that list.
	if len(seen) != 2 {
		t.Errorf("expected the passthrough to disagree across calls, got %v", keys(seen))
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
