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
	errBackendDown = errors.New("the backend named by the request is unhealthy")
	errMaxWait     = errors.New("timed out waiting for a free slot")
)

type PoolOpts struct {
	Poll           time.Duration // safety-net recheck while blocked
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	MetaModels     []string   // model names that mean "whoever is freest", not a real model
	Renamed        []Renaming // retired backend names that still resolve, to a backend that exists
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
	renamed  map[string]string // lowercased retired name -> the live backend it now means
	formers  []Renaming        // as configured, for the catalog and /rooter/stats
	release  chan struct{}     // closed and replaced whenever a slot frees or state changes
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
	// Retired names are wired to live ones once, at start: nothing adds or removes
	// them afterwards, so every read path can treat the map as fixed.
	p.renamed = map[string]string{}
	for _, rn := range opts.Renamed {
		if b := p.byName[rn.To]; b != nil {
			p.renamed[strings.ToLower(strings.TrimSpace(rn.From))] = b.Name
		}
	}
	p.formers = append([]Renaming(nil), opts.Renamed...)
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

// Backends returns the live backend values. The slice is a copy; the Backends
// themselves are still guarded by Pool.mu, so a caller that wants to *read* their
// state should go through Stats or SnapshotJSON rather than marshalling these.
func (p *Pool) Backends() []*Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

// SnapshotJSON renders every backend with Pool.mu held for the whole render.
// Handing out *Backend values and letting a caller marshal them reads the mutable
// fields Check() writes from outside the lock, which is a race the admin endpoint
// has no need for.
func (p *Pool) SnapshotJSON() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return json.MarshalIndent(p.backends, "", "  ")
}

// Lookup returns a backend by its configured name, or by a name that was retired
// in favour of one. Session bindings store a backend name, so a binding made
// before a rename is read through here rather than being thrown away.
func (p *Pool) Lookup(name string) *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byNameLocked(name)
}

// byNameLocked resolves a backend name to a Backend. Call with p.mu held.
func (p *Pool) byNameLocked(name string) *Backend {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if b := p.byName[name]; b != nil {
		return b
	}
	for _, b := range p.backends {
		if strings.EqualFold(b.Name, name) {
			return b
		}
	}
	if live, ok := p.renamed[strings.ToLower(name)]; ok {
		return p.byName[live]
	}
	return nil
}

// RenamedNames lists the retired names rooter still answers for, as configured.
// Fixed at start, so it needs no lock.
func (p *Pool) RenamedNames() []Renaming {
	out := append([]Renaming(nil), p.formers...)
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	return out
}

// Alias resolves a requested model that is really a backend name. "icarus" is not
// a model, but it is a perfectly good answer to "which box do I want", and it is
// the only such answer a client can discover on its own - so /v1/models advertises
// one entry per backend and a request for it pins to that backend and nothing
// else. A name retired with -renamed resolves here too, which is what makes an
// out-of-date client, or a session bound before the rename, work rather than 404.
//
// Returns the default model to hand upstream, or "" when no backend is named by
// that id. A real model id always wins over an alias: the collision is resolved
// in favour of the backend's own inventory, and the synthetic entry is dropped
// from the catalog to match.
func (p *Pool) Alias(model string) (*Backend, string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.byName[model]
	if b == nil {
		for _, x := range p.backends {
			if strings.EqualFold(x.Name, model) {
				b = x
				break
			}
		}
	}
	if b == nil {
		// Not a name in service today, so it may be a retired one. A retired name is
		// the weakest claim available: if any box really does serve a model by that
		// id, the request is routed by inventory instead of being pinned to wherever
		// the old name used to point.
		live, ok := p.renamed[strings.ToLower(model)]
		if !ok {
			return nil, ""
		}
		for _, x := range p.backends {
			if x.Models[model] {
				return nil, ""
			}
		}
		b = p.byName[live]
		if b == nil {
			return nil, ""
		}
	}
	if b.Models[model] {
		return nil, "" // a real model that happens to share a box's name
	}
	return b, b.DefaultModel
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
	Model      string              // what the client asked for, verbatim; may be a backend name
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
			return nil, out, p.noCandidateErr(o)
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
		if errors.Is(err, errNoCandidate) || errors.Is(err, errNoModel) || errors.Is(err, errBackendDown) {
			// Nothing to wait for: no backend could ever take this request. A named box
			// being down belongs here too - queueing for twenty seconds at the front of
			// a router does not make a machine that is refusing connections come back,
			// and it makes the outage the client already cannot see even later.
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
		return nil, p.noCandidateErrLocked(o)
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
func (p *Pool) noCandidateErr(o SelectOpts) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.noCandidateErrLocked(o)
}

// noCandidateErrLocked is the caller-must-hold-p.mu form; try() reaches for it
// while already holding the lock, and taking it twice would self-deadlock.
func (p *Pool) noCandidateErrLocked(o SelectOpts) error {
	// A backend name is a legitimate thing to ask for, and it is a pin to one
	// machine. When that machine is the only reason there are no candidates, saying
	// "no backend serves model X" is a 404 about a model nobody asked about: the
	// name is not wrong, the box is down, and 503 with a Retry-After is the truth a
	// client can act on. -renamed targets resolve here too, so a retired name reads
	// as what it means now rather than as an unknown model.
	if b := p.byNameLocked(o.Model); b != nil && !b.Healthy {
		return &backendDownErr{
			asked:   strings.TrimSpace(o.Model),
			backend: b.Name,
			fails:   b.HealthFails,
			down:    b.DownSince,
		}
	}
	if o.Eligible != nil && p.healthyCountLocked() > 0 {
		return errNoModel
	}
	return errNoCandidate
}

// backendDownErr is a request that named a box rooter knows about, where that box is
// the reason the request cannot be served. asked and backend differ when the client
// used a name retired with -renamed.
type backendDownErr struct {
	asked   string
	backend string
	fails   int
	down    time.Time
}

func (e *backendDownErr) Error() string {
	who := fmt.Sprintf("backend %q", e.backend)
	if e.asked != "" && e.asked != e.backend {
		who = fmt.Sprintf("backend %q (asked for as %q)", e.backend, e.asked)
	}
	switch {
	case e.fails > 0 && !e.down.IsZero():
		return fmt.Sprintf("%s is unhealthy: %d failed health checks since %s", who, e.fails, e.down.Format(time.RFC3339))
	case e.fails > 0:
		return fmt.Sprintf("%s is unhealthy: %d consecutive failed health checks", who, e.fails)
	default:
		// Marked unhealthy by a failed round trip rather than by a probe: rooter
		// knows the box is not answering, but has not polled it since.
		return fmt.Sprintf("%s is unhealthy", who)
	}
}

func (e *backendDownErr) Unwrap() error { return errBackendDown }

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
	ids := make([]string, 0, len(doc.Data))
	for _, m := range doc.Data {
		if m.ID == "" {
			continue
		}
		if !models[m.ID] {
			models[m.ID] = true
			ids = append(ids, m.ID)
		}
	}
	p.mu.Lock()
	b.Models = models
	// Document order, not sorted: a runner lists the model it actually loaded
	// first, and that is the one a backend-name alias has to resolve to. Sorting
	// here would make a box name mean whichever id happened to sort lowest.
	b.ModelIDs = ids
	b.DefaultModel = ""
	if len(ids) > 0 {
		b.DefaultModel = ids[0]
	}
	b.Healthy = true
	b.Checked = time.Now()
	// A probe that passed ends any outage streak, which is what makes the failure
	// counters below mean "how long has this box been down" rather than "how many
	// times has anything ever gone wrong".
	b.HealthFails = 0
	b.DownSince = time.Time{}
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

// healthLogEvery is how many consecutive failed probes pass before rooter says
// something again about a box that is staying down. The first failure is always
// interesting; the fifth is the same news, and at a 15s interval a line every tenth
// check is one every two and a half minutes - enough to see an outage persisting
// without a wall of identical lines.
const healthLogEvery = 10

type checkFailure struct {
	streak  int           // consecutive failed probes, including this one
	down    time.Duration // how long the box has been failing
	flipped bool          // healthy before this probe, unhealthy now
	speak   bool          // worth a log line
}

// noteFailedProbe records the result of a failed health probe. Call with p.mu held.
//
// Checked advances whether the probe passed or failed, because it means "the last
// time rooter looked". Leaving it frozen on failure is what makes a box that has been
// refused for five minutes look like a box nobody is polling - which is exactly how
// /rooter/backends read during a live outage.
func (p *Pool) noteFailedProbeLocked(b *Backend) checkFailure {
	now := time.Now()
	out := checkFailure{flipped: b.Healthy}
	if b.HealthFails == 0 {
		b.DownSince = now
	}
	b.HealthFails++
	b.Checked = now
	b.Healthy = false
	p.notify()
	out.streak = b.HealthFails
	out.down = now.Sub(b.DownSince)
	out.speak = out.flipped || out.streak == 1 || out.streak%healthLogEvery == 0
	return out
}

// probeState is what rooter believes about a backend at one instant, read as a unit
// so a caller can compare it with what the next probe says.
type probeState struct {
	healthy   bool
	fails     int
	downSince time.Time
}

func (p *Pool) probeState(b *Backend) probeState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return probeState{healthy: b.Healthy, fails: b.HealthFails, downSince: b.DownSince}
}

func (p *Pool) CheckAll() {
	var wg sync.WaitGroup
	for _, b := range p.Backends() {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			if err := p.Check(b); err != nil {
				p.mu.Lock()
				out := p.noteFailedProbeLocked(b)
				b.Errors++
				p.mu.Unlock()
				if out.speak {
					p.log.Printf("backend %s unreachable: %v", b.Name, err)
				}
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
				// Read what rooter currently believes *before* probing. Check() flips
				// Healthy itself when a probe passes, so asking afterwards is how
				// "healthy again" became a line that could never print: a box coming back
				// from an outage said nothing at all.
				before := p.probeState(b)
				err := p.Check(b)
				if err != nil {
					p.mu.Lock()
					out := p.noteFailedProbeLocked(b)
					// errors_total means failed round trips for a backend, and a probe is
					// one; but counting every probe of a box that stays down would grow it
					// four times a minute while nothing is being asked of that box, so it
					// still moves only on the flip. HealthFails is the outage counter.
					if out.flipped {
						b.Errors++
					}
					p.mu.Unlock()
					if out.speak {
						if out.streak == 1 {
							p.log.Printf("backend %s unhealthy: %v", b.Name, err)
						} else {
							p.log.Printf("backend %s still unhealthy (%d consecutive checks, down %s): %v",
								b.Name, out.streak, out.down.Round(time.Second), err)
						}
					}
					continue
				}
				if before.healthy {
					continue // steady state: a passing probe is not news
				}
				// An outage should have a last line as well as a first one. Without it,
				// a box that came back left no trace of how long it had been gone.
				if before.fails > 0 && !before.downSince.IsZero() {
					p.log.Printf("backend %s healthy again (after %d failed check(s), down %s)",
						b.Name, before.fails, time.Since(before.downSince).Round(time.Second))
				} else {
					p.log.Printf("backend %s healthy again", b.Name)
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
// not wedge the router. The one exception is a backend already known to be down:
// it is not a candidate for the request in the first place, so the only thing its
// unknown inventory can do is loosen the filter for the boxes that are up.
func (p *Pool) ModelFilter(model string) func(*Backend) bool {
	model = strings.TrimSpace(model)
	// A meta model is a name the client made up on purpose ("default" built in,
	// anything in -meta-model): it is not a claim about weights, it is a request
	// for whichever backend is freest. Sticking still applies afterwards, so one
	// conversation keeps its cache on one box.
	if model == "" || p.meta[normalizeModel(model)] {
		return nil
	}
	// A backend name is a legitimate thing to ask for: "icarus" means "that box,
	// whatever it has loaded", and unlike a meta model it is a hard pin, not a
	// preference. Restricting eligibility to one backend also makes spill and
	// rebind structurally impossible, since there is no "rest" to spill onto.
	if alias, _ := p.Alias(model); alias != nil {
		name := alias.Name
		return func(b *Backend) bool { return b.Name == name }
	}
	p.mu.Lock()
	serves := false
	unknownLists := 0
	want := normalizeModel(model)
	allowed := make(map[string]bool, len(p.backends))
	for _, b := range p.backends {
		if len(b.Models) == 0 {
			// Never polled, or polled and reported nothing: no basis to exclude it.
			// But a box that has been proven down is not an excuse for the living ones:
			// leaving its unknown inventory in the count would turn every mistyped model
			// id in the fleet into a request a live box answers with whatever it happens
			// to have loaded, which is the silent swap this filter exists to prevent.
			if b.Healthy {
				unknownLists++
			}
			continue
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
