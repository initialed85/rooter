package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	errBusy        = errors.New("all candidates busy")
	errNoCandidate = errors.New("no backend can serve this request")
	errNoModel     = errors.New("no backend serves the requested model")
	errMaxWait     = errors.New("timed out waiting for a free slot")
)

type PoolOpts struct {
	Poll           time.Duration // safety-net recheck while blocked
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	MetaModels     []string // model names that mean "whoever is freest", not a real model
	Verbose        bool
}

// The built-in meta model names. Neither is a claim about weights: a request for
// one is "give me a GPU", and which box answers is rooter's choice.
//
//	auto   the name to use, and the one Pi's /model should show
//	default OpenAI's own "I don't care" placeholder, kept working
const (
	modelAuto    = "auto"
	modelDefault = "default"
)

// Pool owns the backends and their concurrency slots.
type Pool struct {
	mu       sync.Mutex
	backends []*Backend
	byName   map[string]*Backend
	release  chan struct{} // closed and replaced whenever a slot frees or state changes
	opts     PoolOpts
	meta     map[string]bool // normalized MetaModels, fixed at start
	client   *http.Client
	log      *log.Logger
}

func NewPool(backends []*Backend, opts PoolOpts, client *http.Client, logger *log.Logger) *Pool {
	if opts.Poll <= 0 {
		opts.Poll = time.Second
	}
	p := &Pool{
		backends: backends,
		byName:   map[string]*Backend{},
		release:  make(chan struct{}),
		opts:     opts,
		client:   client,
		log:      logger,
	}
	p.meta = map[string]bool{modelAuto: true, modelDefault: true}
	for _, m := range opts.MetaModels {
		if n := normalizeModel(m); n != "" {
			p.meta[n] = true
		}
	}
	for _, b := range backends {
		b.Healthy = true // optimistic until proven otherwise
		p.byName[b.Name] = b
	}
	return p
}

// notify wakes everyone blocked in Acquire. Call with p.mu held.
func (p *Pool) notify() {
	close(p.release)
	p.release = make(chan struct{})
}

// MetaNames lists the model names that mean "whichever backend is freest".
func (p *Pool) MetaNames() []string {
	out := make([]string, 0, len(p.meta))
	for m := range p.meta {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func (p *Pool) Backends() []*Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

func (p *Pool) Lookup(name string) *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byName[name]
}

// MarkUnhealthy flips a backend over (used when a round trip fails outright).
func (p *Pool) MarkUnhealthy(b *Backend, why string) {
	p.mu.Lock()
	b.Errors++
	if b.Healthy {
		b.Healthy = false
		if p.opts.Verbose || true {
			p.log.Printf("backend %s marked unhealthy: %s", b.Name, why)
		}
		p.notify()
	}
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// selection
// ---------------------------------------------------------------------------

// SelectOpts describes one routing decision.
type SelectOpts struct {
	Prefer     *Backend            // sticky backend for this session, may be nil
	Eligible   func(*Backend) bool // model support / extras; nil == everyone
	MaxWait    time.Duration       // 0 == block forever
	Grace      time.Duration       // how long to keep waiting on an unusable sticky backend
	SpillAfter time.Duration       // after this much waiting, go elsewhere if possible (0 == never)
	NeedSlot   bool                // false => never block, never take a slot
	Started    time.Time           // when this request began waiting; set by Acquire
}

type Outcome struct {
	Backend *Backend
	Waited  time.Duration
	Rebound bool // gave up on the sticky backend
	Spilled bool // ...only because it was busy for too long
}

// Acquire blocks until it can hand back a backend with a slot reserved, or the
// context is cancelled / MaxWait elapses.
func (p *Pool) Acquire(ctx context.Context, o SelectOpts) (*Backend, Outcome, error) {
	start := time.Now()
	var lostSticky time.Time
	var out Outcome
	o.Started = start
	if !o.NeedSlot {
		b := p.pick(ctx, o, &lostSticky)
		if b == nil {
			return nil, out, p.noCandidateErr(o.Eligible)
		}
		out.Backend = b
		out.Rebound = o.Prefer != nil && b != o.Prefer
		out.Waited = time.Since(start)
		p.touch(b, out.Waited)
		return b, out, nil
	}
	var deadline time.Time
	if o.MaxWait > 0 {
		deadline = start.Add(o.MaxWait)
	}
	for {
		b, err := p.try(o, &lostSticky)
		if err == nil {
			out.Backend = b
			out.Waited = time.Since(start)
			out.Rebound = o.Prefer != nil && b != o.Prefer
			out.Spilled = out.Rebound && o.SpillAfter > 0 && out.Waited >= o.SpillAfter
			p.touch(b, out.Waited)
			return b, out, nil
		}
		if errors.Is(err, errNoCandidate) || errors.Is(err, errNoModel) {
			// Nothing to wait for: no backend could ever take this request.
			out.Waited = time.Since(start)
			return nil, out, err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			out.Waited = time.Since(start)
			return nil, out, errMaxWait
		}
		if ctx.Err() != nil {
			out.Waited = time.Since(start)
			return nil, out, ctx.Err()
		}
		if !p.wait(ctx, deadline) {
			out.Waited = time.Since(start)
			return nil, out, ctx.Err()
		}
	}
}

func (p *Pool) touch(b *Backend, waited time.Duration) {
	p.mu.Lock()
	b.Requests++
	b.LastUsed = time.Now()
	b.TotalWait += waited
	p.mu.Unlock()
}

// try makes a single non-blocking selection pass.
func (p *Pool) try(o SelectOpts, lostSticky *time.Time) (*Backend, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	elig := p.eligibleLocked(o.Eligible)
	if len(elig) == 0 {
		// "nothing is healthy" and "nothing serves this model" look the same here
		// but are not the same failure: the first is a 503 worth retrying, the
		// second is a 404 that will never succeed.
		return nil, p.noCandidateErrLocked(o.Eligible)
	}

	// 1. sticky: keep the prefix cache warm on the backend we used before - but
	// only if that backend is still allowed to serve this request. A bound backend
	// that does not have the requested model must not be used: llama.cpp answers a
	// model it does not have with whatever it *does* have loaded, so sticking here
	// silently swaps the model on the client.
	if o.Prefer != nil && p.usableLocked(o.Prefer, now) && inSet(elig, o.Prefer) {
		*lostSticky = time.Time{}
		if !o.NeedSlot {
			return o.Prefer, nil
		}
		if o.Prefer.tryAcquire() {
			return o.Prefer, nil
		}
		// The bound backend is healthy, just full. Normally that is worth queueing
		// for: this session's prefix cache lives there and moving costs a re-prefill.
		// But if the wait outlasts -spill-after and some other eligible backend has
		// a free slot, holding on is no longer affinity, it is a queue.
		if o.SpillAfter > 0 && !o.Started.IsZero() && now.Sub(o.Started) >= o.SpillAfter {
			if rest := withoutBackend(elig, o.Prefer); len(rest) > 0 {
				if free := eligibleFree(rest); len(free) > 0 {
					return free[0], nil
				}
			}
		}
		return nil, errBusy // block; this session belongs here
	}

	// 2. sticky backend is unusable for this request. If it is merely unhealthy,
	//    hold the line briefly so a blip does not shred the cache, then rebind. If
	//    it is healthy but simply cannot serve the requested model, that is not a
	//    blip - rebind straight away, or every model switch would stall for
	//    -sticky-grace.
	if o.Prefer != nil && p.usableLocked(o.Prefer, now) {
		*lostSticky = time.Time{}
	} else if o.Prefer != nil {
		if lostSticky.IsZero() {
			*lostSticky = now
		}
		if o.Grace > 0 && now.Sub(*lostSticky) < o.Grace {
			return nil, errBusy
		}
	}

	// 3. new session (or rebound): least-loaded free backend wins.
	if !o.NeedSlot {
		return leastLoaded(elig), nil
	}
	for _, b := range eligibleFree(elig) {
		if b.tryAcquire() {
			return b, nil
		}
	}
	return nil, errBusy
}

// pick is the non-blocking, no-slot variant used for metadata requests.
func (p *Pool) pick(ctx context.Context, o SelectOpts, lostSticky *time.Time) *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	elig := p.eligibleLocked(o.Eligible)
	if len(elig) == 0 {
		return nil
	}
	// Same rule as try(): stickiness never outranks the model filter.
	if o.Prefer != nil && p.usableLocked(o.Prefer, now) && inSet(elig, o.Prefer) {
		return o.Prefer
	}
	return leastLoaded(elig)
}

// noCandidateErr says *why* there are no candidates, so the caller can answer
// 503 (try again) or 404 (that model does not exist here).
func (p *Pool) noCandidateErr(filter func(*Backend) bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.noCandidateErrLocked(filter)
}

// noCandidateErrLocked is the caller-must-hold-p.mu form; try() reaches for it
// while already holding the lock, and taking it twice would self-deadlock.
func (p *Pool) noCandidateErrLocked(filter func(*Backend) bool) error {
	if filter != nil && p.healthyCountLocked() > 0 {
		return errNoModel
	}
	return errNoCandidate
}

// inSet reports whether b is one of the candidates in list.
func inSet(list []*Backend, b *Backend) bool {
	for _, x := range list {
		if x == b {
			return true
		}
	}
	return false
}

// withoutBackend copies a candidate list minus one entry.
func withoutBackend(list []*Backend, b *Backend) []*Backend {
	out := make([]*Backend, 0, len(list))
	for _, x := range list {
		if x != b {
			out = append(out, x)
		}
	}
	return out
}

func (p *Pool) healthyCountLocked() int {
	n := 0
	for _, b := range p.backends {
		if b.Healthy {
			n++
		}
	}
	return n
}

// eligibleLocked returns candidates that are healthy and can serve the request.
func (p *Pool) eligibleLocked(filter func(*Backend) bool) []*Backend {
	out := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if !b.Healthy {
			continue
		}
		if filter != nil && !filter(b) {
			continue
		}
		out = append(out, b)
	}
	return out
}

func (p *Pool) usableLocked(b *Backend, now time.Time) bool {
	for _, x := range p.backends {
		if x == b {
			return b.Healthy
		}
	}
	return false
}

// eligibleFree orders candidates by "most spare capacity first".
func eligibleFree(elig []*Backend) []*Backend {
	out := make([]*Backend, len(elig))
	copy(out, elig)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if fa, fb := floatFree(a), floatFree(b); fa != fb {
			return fa > fb
		}
		if a.busy() != b.busy() {
			return a.busy() < b.busy()
		}
		if !a.LastUsed.Equal(b.LastUsed) {
			return a.LastUsed.Before(b.LastUsed)
		}
		return a.Name < b.Name
	})
	return out
}

func floatFree(b *Backend) float64 { return float64(b.free()) / float64(b.Concurrency) }

func leastLoaded(elig []*Backend) *Backend {
	return eligibleFree(elig)[0]
}

// wait blocks until something changes, ctx ends, or the poll timer fires.
// It reports whether the caller should loop again (false => give up).
func (p *Pool) wait(ctx context.Context, deadline time.Time) bool {
	p.mu.Lock()
	ch := p.release
	p.mu.Unlock()
	var timer *time.Timer
	if deadline.IsZero() {
		timer = time.NewTimer(p.opts.Poll)
	} else {
		d := time.Until(deadline)
		if d > p.opts.Poll {
			d = p.opts.Poll
		}
		timer = time.NewTimer(d)
	}
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	case <-ctx.Done():
		return false
	}
	return ctx.Err() == nil
}

// Release hands a slot back and wakes the waiters.
func (p *Pool) Release(b *Backend) {
	b.releaseSlot()
	p.mu.Lock()
	p.notify()
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// health
// ---------------------------------------------------------------------------

type modelsDoc struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
	Error json.RawMessage `json:"error"`
}

// Check hits one backend's /models endpoint.
func (p *Pool) Check(b *Backend) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.HealthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.Base.String()+"/models", nil)
	if err != nil {
		return err
	}
	if b.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.Token)
	}
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", res.Status, snippet(body))
	}
	var doc modelsDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("bad /models json: %v", err)
	}
	models := map[string]bool{}
	for _, m := range doc.Data {
		models[m.ID] = true
	}
	p.mu.Lock()
	b.Models = models
	b.Healthy = true
	b.Checked = time.Now()
	p.notify()
	p.mu.Unlock()
	return nil
}

func (p *Pool) SetHealth(b *Backend, healthy bool) {
	p.mu.Lock()
	if b.Healthy != healthy {
		b.Healthy = healthy
		if !healthy {
			b.Errors++
		}
		p.notify()
	}
	p.mu.Unlock()
}

func (p *Pool) CheckAll() {
	var wg sync.WaitGroup
	for _, b := range p.Backends() {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			if err := p.Check(b); err != nil {
				p.mu.Lock()
				first := b.Checked.IsZero()
				b.Healthy = false
				b.Errors++
				p.notify()
				p.mu.Unlock()
				p.log.Printf("backend %s %s: %v", b.Name, map[bool]string{true: "unreachable", false: "still unreachable"}[first], err)
			} else {
				p.log.Printf("backend %s ok (%d slot(s), %s)", b.Name, b.Concurrency, p.modelSummary(b))
			}
		}(b)
	}
	wg.Wait()
}

func (p *Pool) modelSummary(b *Backend) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(b.Models) == 0 {
		return "no models listed"
	}
	names := make([]string, 0, len(b.Models))
	for m := range b.Models {
		names = append(names, m)
	}
	sort.Strings(names)
	if len(names) > 3 {
		names = append(names[:3], "...")
	}
	return strings.Join(names, ", ")
}

func (p *Pool) HealthLoop(ctx context.Context) {
	if p.opts.HealthInterval <= 0 {
		return
	}
	t := time.NewTicker(p.opts.HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, b := range p.Backends() {
				if err := p.Check(b); err != nil {
					p.mu.Lock()
					if b.Healthy {
						p.log.Printf("backend %s unhealthy: %v", b.Name, err)
						b.Healthy = false
						b.Errors++
						p.notify()
					}
					p.mu.Unlock()
				} else {
					p.mu.Lock()
					if !b.Healthy {
						p.log.Printf("backend %s healthy again", b.Name)
						b.Healthy = true
						b.Checked = time.Now()
						p.notify()
					}
					p.mu.Unlock()
				}
			}
		}
	}
}

// ModelFilter returns a predicate restricting selection to the backends that
// claim to serve model. The closure never touches Pool state, so it is safe to
// call while holding p.mu (Pool.try does).
//
// Deliberately lenient: if no backend lists the model (llama.cpp reports
// "default" while accepting anything) or a backend's list has not been read
// yet, that backend stays eligible. A stale model list should cost one request,
// not wedge the router.
func (p *Pool) ModelFilter(model string) func(*Backend) bool {
	model = strings.TrimSpace(model)
	// A meta model is a name the client made up on purpose ("default" built in,
	// anything in -meta-model): it is not a claim about weights, it is a request
	// for whichever backend is freest. Sticking still applies afterwards, so one
	// conversation keeps its cache on one box.
	if model == "" || p.meta[normalizeModel(model)] {
		return nil
	}
	p.mu.Lock()
	serves := false
	unknownLists := 0
	want := normalizeModel(model)
	allowed := make(map[string]bool, len(p.backends))
	for _, b := range p.backends {
		if len(b.Models) == 0 {
			unknownLists++
			continue // never polled, or polled and reported nothing: no basis to exclude it
		}
		for id := range b.Models {
			if normalizeModel(id) == want {
				allowed[b.Name] = true
				serves = true
				break
			}
		}
	}
	p.mu.Unlock()
	if serves {
		return func(b *Backend) bool { return allowed[b.Name] }
	}
	// Nothing claims the model. Refuse it only when every backend's model list is
	// actually known and non-empty, otherwise a backend that has not been polled
	// yet would be hidden behind a hard 404. Refusing at all is the point: a wrong
	// model id has to become a visible error instead of an answer from a different
	// model, which is exactly what llama.cpp does when handed one.
	if unknownLists > 0 {
		return nil
	}
	return func(*Backend) bool { return false }
}

// normalizeModel folds away the quantisation tag llama.cpp clients often append
// to an id ("repo/GGUF:UD-Q6_K_XL"), since /models reports the base id. Only a
// trailing ":tag" after the last separator counts, so scheme colons survive.
func normalizeModel(m string) string {
	m = strings.ToLower(strings.TrimSpace(m))
	if i := strings.LastIndexByte(m, ':'); i > strings.LastIndexByte(m, '/') {
		m = m[:i]
	}
	return m
}

func snippet(b []byte) string {
	s := strings.ReplaceAll(string(b), "\n", " ")
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}
