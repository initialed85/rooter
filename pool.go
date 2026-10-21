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
	errMaxWait     = errors.New("timed out waiting for a free slot")
)

type PoolOpts struct {
	Poll           time.Duration // safety-net recheck while blocked
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	Verbose        bool
}

// Pool owns the backends and their concurrency slots.
type Pool struct {
	mu       sync.Mutex
	backends []*Backend
	byName   map[string]*Backend
	release  chan struct{} // closed and replaced whenever a slot frees or state changes
	opts     PoolOpts
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
	Prefer   *Backend            // sticky backend for this session, may be nil
	Eligible func(*Backend) bool // model support / extras; nil == everyone
	MaxWait  time.Duration       // 0 == block forever
	Grace    time.Duration       // how long to keep waiting on an unusable sticky backend
	NeedSlot bool                // false => never block, never take a slot
}

type Outcome struct {
	Backend *Backend
	Waited  time.Duration
	Rebound bool // gave up on the sticky backend
}

// Acquire blocks until it can hand back a backend with a slot reserved, or the
// context is cancelled / MaxWait elapses.
func (p *Pool) Acquire(ctx context.Context, o SelectOpts) (*Backend, Outcome, error) {
	start := time.Now()
	var lostSticky time.Time
	var out Outcome
	if !o.NeedSlot {
		b := p.pick(ctx, o, &lostSticky)
		if b == nil {
			return nil, out, errNoCandidate
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
			p.touch(b, out.Waited)
			return b, out, nil
		}
		if errors.Is(err, errNoCandidate) {
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
		return nil, errNoCandidate
	}

	// 1. sticky: keep the prefix cache warm on the backend we used before.
	if o.Prefer != nil && p.usableLocked(o.Prefer, now) {
		*lostSticky = time.Time{}
		if !o.NeedSlot {
			return o.Prefer, nil
		}
		if o.Prefer.tryAcquire() {
			return o.Prefer, nil
		}
		return nil, errBusy // block; this session belongs here
	}

	// 2. sticky backend is unusable (down / no longer serves the model). Hold
	//    the line briefly so a blip does not shred the cache, then rebind.
	if o.Prefer != nil {
		if lostSticky.IsZero() {
			*lostSticky = now
		}
		if o.Grace > 0 && now.Sub(*lostSticky) < o.Grace {
			// Only worth waiting if some backend could in principle take us.
			if len(elig) == 0 {
				return nil, errNoCandidate
			}
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
	if o.Prefer != nil && p.usableLocked(o.Prefer, now) {
		return o.Prefer
	}
	elig := p.eligibleLocked(o.Eligible)
	if len(elig) == 0 {
		return nil
	}
	return leastLoaded(elig)
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
	if model == "" || model == "default" {
		return nil
	}
	p.mu.Lock()
	serves := false
	allowed := make(map[string]bool, len(p.backends))
	for _, b := range p.backends {
		if b.Models == nil {
			continue // unknown: do not constrain this backend
		}
		if b.Models[model] {
			allowed[b.Name] = true
			serves = true
		}
	}
	p.mu.Unlock()
	if !serves {
		return nil
	}
	return func(b *Backend) bool { return allowed[b.Name] }
}

func snippet(b []byte) string {
	s := strings.ReplaceAll(string(b), "\n", " ")
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}
