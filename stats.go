package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// admin serves rooter's own endpoints. They are namespaced under /rooter so
// that every other path stays a clean passthrough to the backends.
func (r *Router) admin(w http.ResponseWriter, req *http.Request) {
	switch strings.Trim(req.URL.Path, "/") {
	case "rooter", "rooter/":
		r.adminIndex(w)
	case "rooter/healthz", "rooter/health":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ok")
	case "rooter/stats":
		b, _ := json.MarshalIndent(r.Stats(), "", "  ")
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	case "rooter/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.prometheus(w)
	case "rooter/backends":
		b, _ := json.MarshalIndent(r.pool.Backends(), "", "  ")
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	case "rooter/sessions":
		b, _ := json.MarshalIndent(r.sessions.Stats(), "", "  ")
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	case "rooter/forget":
		key := req.URL.Query().Get("key")
		n := 0
		if key == "*" {
			n = r.sessions.Clear()
		} else {
			n = r.sessions.UnbindSession(key)
		}
		fmt.Fprintf(w, "forgotten %d binding(s) for %q\n", n, key)
	default:
		errJSON(w, http.StatusNotFound, "not found: "+req.URL.Path)
	}
}

func (r *Router) adminIndex(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "rooter - transparent LLM router (uptime %s)\n\n", time.Since(r.start).Round(time.Second))
	for _, b := range r.pool.Backends() {
		r.pool.mu.Lock()
		models := make([]string, 0, len(b.Models))
		for m := range b.Models {
			models = append(models, m)
		}
		sort.Strings(models)
		fmt.Fprintf(w, "  %-16s %s  slots %d/%d  healthy=%v  models=%s\n",
			b.Name, b.Base, b.busy(), b.Concurrency, b.Healthy, strings.Join(models, ","))
		r.pool.mu.Unlock()
	}
	fmt.Fprintf(w, "\nmeta models (any backend, least loaded, then sticky): %s\n", strings.Join(r.pool.MetaNames(), " "))
	if r.cfg.SpillAfter > 0 {
		fmt.Fprintf(w, "spill after %s waiting on a busy sticky backend\n", r.cfg.SpillAfter)
	}
	if r.auth.enabled() {
		fmt.Fprintf(w, "\napi key required on the front door (%d key(s), %s reachable without one)\n",
			r.auth.keyCount(), strings.Join(r.cfg.PublicPaths, " "))
	} else {
		fmt.Fprintf(w, "\nno api key set - anyone who can reach this address can spend these backends\n")
	}
	fmt.Fprintf(w, "\nendpoints: /rooter/stats /rooter/metrics /rooter/backends /rooter/sessions /rooter/forget?key=K\n")
}

func (r *Router) Stats() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	status := map[string]int64{}
	for code, n := range r.byStatus {
		status[fmt.Sprint(code)] = n
	}
	// avgWait is about generations. waitTotal only ever accumulates for a request
	// that had to win a slot, so dividing it by every request (metadata reads,
	// /rooter/*, 401s) would make a busy router look instant.
	avg := time.Duration(0)
	if r.slotted > 0 {
		avg = r.waitTotal / time.Duration(r.slotted)
	}
	s := map[string]any{
		"uptime":      time.Since(r.start).Round(time.Second).String(),
		"requests":    r.total,
		"generations": r.slotted,
		"inflight":    r.inflight,
		"queued":      r.waiting,
		"rejected":    r.rejected,
		"denied":      r.denied,
		"avgWait":     avg.String(),
		"auth":        r.auth.enabled(),
		"status":      status,
	}
	s["backends"] = r.pool.Stats()
	s["slots"] = r.pool.SlotSummary()
	s["metaModels"] = r.pool.MetaNames()
	if r.cfg.SpillAfter > 0 {
		s["spillAfter"] = r.cfg.SpillAfter.String()
	}
	for k, v := range r.sessions.Stats() {
		s["sessions_"+k] = v
	}
	delete(s, "sessions_ttl")
	delete(s, "sessions_max")
	return s
}

func (p *Pool) Stats() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, 0, len(p.backends))
	for _, b := range p.backends {
		models := make([]string, 0, len(b.Models))
		for m := range b.Models {
			models = append(models, m)
		}
		sort.Strings(models)
		out = append(out, map[string]any{
			"name":        b.Name,
			"url":         b.Base.String(),
			"busy":        b.busy(),
			"concurrency": b.Concurrency,
			"healthy":     b.Healthy,
			"models":      models,
			"requests":    b.Requests,
			"errors":      b.Errors,
			"waitTotal":   b.TotalWait.String(),
			"lastUsed":    b.LastUsed,
			"checked":     b.Checked,
		})
	}
	return out
}

func (p *Pool) SlotSummary() map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	busy, cap := 0, 0
	for _, b := range p.backends {
		busy += b.busy()
		cap += b.Concurrency
	}
	return map[string]any{"busy": busy, "total": cap}
}

func (r *Router) prometheus(w http.ResponseWriter) {
	e := func(name, help, typ string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	e("rooter_requests_total", "Requests received by rooter", "counter")
	fmt.Fprintf(w, "rooter_requests_total %d\n", r.snapshotTotal())
	e("rooter_generation_requests_total", "Requests that had to win a backend slot", "counter")
	fmt.Fprintf(w, "rooter_generation_requests_total %d\n", r.snapshotSlotted())
	e("rooter_auth_required", "Whether an -api-key guards the front door", "gauge")
	fmt.Fprintf(w, "rooter_auth_required %d\n", boolGauge(r.auth.enabled()))
	e("rooter_inflight", "Requests currently being proxied", "gauge")
	e("rooter_queued", "Requests blocked waiting for a free slot", "gauge")
	e("rooter_rejected_total", "Requests rejected because nothing was free", "counter")
	e("rooter_denied_total", "Requests rejected by the front-door API key check", "counter")
	e("rooter_wait_seconds_total", "Time spent waiting for slots", "counter")
	e("rooter_sessions", "Tracked sessions", "gauge")
	st := r.Stats()
	fmt.Fprintf(w, "rooter_inflight %v\nrooter_queued %v\nrooter_rejected_total %v\n", st["inflight"], st["queued"], st["rejected"])
	fmt.Fprintf(w, "rooter_denied_total %v\n", st["denied"])
	fmt.Fprintf(w, "rooter_sessions %v\n", st["sessions_sessions"])
	fmt.Fprintf(w, "rooter_wait_seconds_total %v\n", r.snapshotWait().Seconds())
	e("rooter_backend_slots_busy", "Busy concurrency slots per backend", "gauge")
	e("rooter_backend_slots_total", "Configured concurrency slots per backend", "gauge")
	e("rooter_backend_healthy", "Whether the health check last passed", "gauge")
	e("rooter_backend_requests_total", "Requests proxied to a backend", "counter")
	e("rooter_backend_errors_total", "Failed round trips for a backend", "counter")
	for _, b := range r.pool.Stats() {
		l := fmt.Sprintf(`{backend="%s"}`, b["name"])
		fmt.Fprintf(w, "rooter_backend_slots_busy%s %v\n", l, b["busy"])
		fmt.Fprintf(w, "rooter_backend_slots_total%s %v\n", l, b["concurrency"])
		h := 0
		if b["healthy"].(bool) {
			h = 1
		}
		fmt.Fprintf(w, "rooter_backend_healthy%s %d\n", l, h)
		fmt.Fprintf(w, "rooter_backend_requests_total%s %v\n", l, b["requests"])
		fmt.Fprintf(w, "rooter_backend_errors_total%s %v\n", l, b["errors"])
	}
}

func (r *Router) snapshotTotal() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}

func (r *Router) snapshotSlotted() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.slotted
}

func boolGauge(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (r *Router) snapshotWait() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waitTotal
}
