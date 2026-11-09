package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// The bug this file exists for: /v1/models used to be a passthrough, so the
// answer depended on which backend happened to be free. Two calls a second apart
// on the live box returned disjoint lists - one gave strata's single model, the
// next gave sushi's four (that box was sushi, retired 2026-11). A client picking
// from that list is choosing from whichever box answered first, and rooter knows
// the union.
//
// So rooter builds the catalog itself. It stays a router, not a registry: every
// entry is derived from what a backend actually reported, and nothing is invented.

// modelEntry is one line of the advertised catalog. It is an OpenAI model object
// with a rooter namespace bolted on, so clients that only read id/owned_by keep
// working and clients that care can see which box would serve it.
type modelEntry struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created,omitempty"`
	OwnedBy  string `json:"owned_by"`
	Rooter   any    `json:"rooter,omitempty"`
	priority int    // sort key: real models first, then aliases, then meta
}

type modelRooterInfo struct {
	Backend      string   `json:"backend,omitempty"`
	Backends     []string `json:"backends,omitempty"`
	Alias        bool     `json:"alias,omitempty"`
	AliasOf      string   `json:"alias_of,omitempty"` // this id is a retired name; this box is what it now means
	Meta         bool     `json:"meta,omitempty"`
	DefaultModel string   `json:"default_model,omitempty"`
	Healthy      bool     `json:"healthy"`
	SlotsFree    int      `json:"slots_free"`
	SlotsTotal   int      `json:"slots_total"`
}

type modelsCatalog struct {
	Object string       `json:"object"`
	Data   []modelEntry `json:"data"`
}

// Catalog builds the merged model list.
//
// Ordering is deliberate and stable: real models first, then the per-backend
// aliases, then the meta names. A client that shows the list top-to-bottom should
// land on a real model, not on "auto".
func (p *Pool) Catalog() []modelEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	byID := map[string][]*Backend{}
	for _, b := range p.backends {
		for _, id := range b.ModelIDs {
			byID[id] = append(byID[id], b)
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]modelEntry, 0, len(ids)+len(p.backends)+len(p.meta))
	for _, id := range ids {
		names := backendNames(byID[id])
		out = append(out, modelEntry{
			ID:      id,
			Object:  "model",
			OwnedBy: names[0],
			Rooter: modelRooterInfo{
				Backend:    names[0],
				Backends:   names,
				Healthy:    anyHealthy(byID[id]),
				SlotsFree:  freeOf(byID[id]),
				SlotsTotal: sumSlots(byID[id]),
			},
		})
	}

	// Aliases. Skipped when a real model already carries the name, so the list
	// never has two entries with the same id and never shadows a backend's own
	// inventory.
	for _, b := range p.backends {
		if _, taken := byID[b.Name]; taken {
			continue
		}
		out = append(out, modelEntry{
			ID:       b.Name,
			Object:   "model",
			OwnedBy:  "rooter",
			priority: 1,
			Rooter: modelRooterInfo{
				Backend:      b.Name,
				Backends:     []string{b.Name},
				Alias:        true,
				DefaultModel: b.DefaultModel,
				Healthy:      b.Healthy,
				SlotsFree:    b.free(),
				SlotsTotal:   b.Concurrency,
			},
		})
	}

	// Retired names, advertised so a client still asking for one can see in the
	// list itself that it is a redirect, and to which box. Same collision rule as
	// every other synthetic entry: a real model carrying that id wins, and the
	// entry is dropped rather than shadowing it.
	for _, rn := range p.formers {
		if _, taken := byID[rn.From]; taken {
			continue
		}
		b := p.byName[rn.To]
		if b == nil {
			continue
		}
		out = append(out, modelEntry{
			ID:       rn.From,
			Object:   "model",
			OwnedBy:  "rooter",
			priority: 1,
			Rooter: modelRooterInfo{
				Backend:      b.Name,
				Backends:     []string{b.Name},
				Alias:        true,
				AliasOf:      b.Name,
				DefaultModel: b.DefaultModel,
				Healthy:      b.Healthy,
				SlotsFree:    b.free(),
				SlotsTotal:   b.Concurrency,
			},
		})
	}

	// Meta names, same collision rule.
	for _, m := range p.MetaNames() {
		if _, taken := byID[m]; taken {
			continue
		}
		out = append(out, modelEntry{
			ID:       m,
			Object:   "model",
			OwnedBy:  "rooter",
			priority: 2,
			Rooter:   modelRooterInfo{Meta: true, Healthy: p.healthyCountLocked() > 0},
		})
	}

	// Real models first, then aliases, then meta; within a group, by id.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].priority != out[j].priority {
			return out[i].priority < out[j].priority
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func backendNames(bs []*Backend) []string {
	names := make([]string, 0, len(bs))
	for _, b := range bs {
		names = append(names, b.Name)
	}
	sort.Strings(names)
	return names
}

func anyHealthy(bs []*Backend) bool {
	for _, b := range bs {
		if b.Healthy {
			return true
		}
	}
	return false
}

// freeOf adds up the slots currently spare across every backend that serves a
// model. It is the number that answers "can I get an answer right now".
func freeOf(bs []*Backend) int {
	n := 0
	for _, b := range bs {
		n += b.free()
	}
	return n
}

func sumSlots(bs []*Backend) int {
	n := 0
	for _, b := range bs {
		n += b.Concurrency
	}
	return n
}

// isModelsPath reports whether a request is asking for the model list. Both the
// OpenAI path and the bare llama.cpp one are honoured.
func isModelsPath(path string) bool {
	p := strings.TrimSuffix(path, "/")
	switch p {
	case "/v1/models", "/models":
		return true
	}
	return strings.HasSuffix(p, "/v1/models") || strings.HasSuffix(p, "/models")
}

// modelCatalog answers /v1/models from rooter's own knowledge.
//
// ?backend=name narrows the list to what one box serves, which is how to still
// get the old per-backend view without giving up the merged one.
func (r *Router) modelCatalog(w http.ResponseWriter, req *http.Request) {
	entries := r.pool.Catalog()
	if want := req.URL.Query().Get("backend"); want != "" {
		kept := make([]modelEntry, 0, len(entries))
		for _, e := range entries {
			info, _ := e.Rooter.(modelRooterInfo)
			for _, b := range info.Backends {
				if strings.EqualFold(b, want) {
					kept = append(kept, e)
					break
				}
			}
		}
		entries = kept
	}
	obj := modelsCatalog{Object: "list", Data: entries}
	if obj.Data == nil {
		obj.Data = []modelEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	if !strings.Contains(req.Header.Get("Accept"), "application/json") {
		enc.SetIndent("", "  ")
	}
	_ = enc.Encode(obj)
}

// resolveAlias rewrites a request that named a backend instead of a model, so
// the upstream sees a model it genuinely has and echoes a truthful one back.
//
// It returns the backend to pin to and the body to send. The body is unchanged
// unless the model field could be replaced exactly, and a partially buffered
// body is never touched: rewriting one would mean holding all of it, which is
// the buffering this proxy exists to avoid.
func (r *Router) resolveAlias(info requestInfo, body []byte, partial bool, contentType string) (*Backend, []byte, string) {
	b, def := r.pool.Alias(info.Model)
	if b == nil {
		return nil, body, info.Model
	}
	// The box has not told us what it serves yet, or the field could not be
	// replaced cleanly. Pin to it anyway and let it answer for itself: that is
	// what "give me that box" means, and it is strictly better than a 404 for a
	// name rooter itself advertised.
	if def == "" || partial || !isJSONContent(contentType) || len(body) == 0 {
		return b, body, info.Model
	}
	newBody, ok := rewriteModelField(body, def)
	if !ok {
		return b, body, info.Model
	}
	return b, newBody, def
}

// rewriteModelField replaces the value of the top-level "model" key and leaves
// every other byte alone. A full unmarshal/remarshal would reorder keys and
// reformat numbers, and this is the one place rooter edits a client's request:
// it should change exactly one string and nothing else.
func rewriteModelField(body []byte, model string) ([]byte, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, false
	}
	if d, isDelim := tok.(json.Delim); !isDelim || d != '{' {
		return nil, false
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, false
		}
		key, isStr := kt.(string)
		if !isStr {
			return nil, false
		}
		vs := valueStart(body, dec.InputOffset())
		if err := skipValue(dec); err != nil {
			return nil, false
		}
		if key == "model" {
			ve := dec.InputOffset()
			if ve <= vs || ve > int64(len(body)) {
				return nil, false
			}
			repl, err := json.Marshal(model)
			if err != nil {
				return nil, false
			}
			out := make([]byte, 0, len(body)-int(ve-vs)+len(repl))
			out = append(out, body[:vs]...)
			out = append(out, repl...)
			out = append(out, body[ve:]...)
			return out, true
		}
	}
	return nil, false
}

// valueStart walks from just past a key token to the first byte of its value,
// over the ":" and any whitespace between them. InputOffset after a key points
// at the colon, so splicing without this would eat it.
func valueStart(body []byte, off int64) int64 {
	for off < int64(len(body)) && isJSONSpace(body[off]) {
		off++
	}
	if off < int64(len(body)) && body[off] == ':' {
		off++
		for off < int64(len(body)) && isJSONSpace(body[off]) {
			off++
		}
	}
	return off
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// adminCatalog renders the same entries for /rooter/models in the plain text
// style the rest of the admin endpoints use, so the two cannot disagree.
func (r *Router) adminCatalog(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "rooter model catalog (as of %s)\n\n", time.Now().UTC().Format(time.RFC3339))
	for _, e := range r.pool.Catalog() {
		info, _ := e.Rooter.(modelRooterInfo)
		kind := "model"
		switch {
		case info.Alias:
			kind = "alias"
		case info.Meta:
			kind = "meta"
		}
		tail := ""
		if info.DefaultModel != "" {
			tail = " -> " + info.DefaultModel
		}
		if !info.Meta {
			tail += fmt.Sprintf("  slots %d/%d free", info.SlotsFree, info.SlotsTotal)
		}
		if !info.Healthy {
			tail += "  UNHEALTHY"
		}
		fmt.Fprintf(w, "  %-6s %-52s %s%s\n", kind, e.ID, strings.Join(info.Backends, ","), tail)
	}
}
