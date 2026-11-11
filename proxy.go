package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Router is the transparent proxy. It picks a backend, holds one of that
// backend's concurrency slots for the whole exchange, and copies bytes both
// ways without touching them.
type Router struct {
	pool     *Pool
	sessions *SessionStore
	cfg      *Config
	log      *log.Logger
	auth     *authGuard
	proxies  map[string]*httputil.ReverseProxy
	start    time.Time
	bootID   string // unique per process, so call ids survive a restart

	// events and models are the per-call record and its aggregate. Both are
	// internally locked, so they sit outside Router.mu.
	events *eventRing
	models *modelStats

	mu        sync.Mutex
	total     int64
	slotted   int64 // requests that had to win a slot: the honest avgWait denominator
	inflight  int64
	waiting   int64
	rejected  int64
	denied    int64
	aborted   int64 // client hung up before the response finished
	seq       int64 // per-call id, monotonic within a process
	waitTotal time.Duration
	byStatus  map[int]int64
	denyNote  map[string]*denyNote
	now       func() time.Time // a test seam for the denial log window
}

func NewRouter(p *Pool, s *SessionStore, cfg *Config, logger *log.Logger) *Router {
	r := &Router{
		pool:     p,
		sessions: s,
		cfg:      cfg,
		log:      logger,
		auth:     newAuthGuard(cfg),
		proxies:  map[string]*httputil.ReverseProxy{},
		start:    time.Now(),
		byStatus: map[int]int64{},
		denyNote: map[string]*denyNote{},
		now:      time.Now,
		bootID:   newBootID(),
		events:   newEventRing(cfg.EventRing),
		models:   newModelStats(),
	}
	if !cfg.NoMeta && cfg.MetaTail <= 0 {
		cfg.MetaTail = 16384
	}
	var tr http.RoundTripper = &http.Transport{
		// Never transparently negotiate gzip: it would buffer and inflate
		// streamed SSE, which is exactly what must not happen.
		DisableCompression:  true,
		MaxIdleConnsPerHost: cfg.TotalSlots() + 4,
		IdleConnTimeout:     90 * time.Second,
		WriteBufferSize:     32 << 10,
		ReadBufferSize:      32 << 10,
		ForceAttemptHTTP2:   false,
	}
	if !cfg.XForwarded {
		tr = &stripForwarded{RoundTripper: tr}
	}
	for _, b := range p.Backends() {
		r.proxies[b.Name] = &httputil.ReverseProxy{
			Transport:     tr,
			FlushInterval: -1, // hand each chunk to the client as it arrives
			Director:      directorFor(b),
			// Errors are captured so the caller can decide whether to retry onto
			// another backend; ReverseProxy's default would just answer 502.
			ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
				if w, ok := rw.(*replyWriter); ok && w.err == nil {
					w.err = err
				}
			},
			ErrorLog: logger,
		}
	}
	return r
}

// directorFor rewrites an outbound request onto one backend. Nothing else
// changes: headers, query string and body pass through untouched.
func directorFor(b *Backend) func(*http.Request) {
	basePath := strings.TrimSuffix(b.Base.EscapedPath(), "/")
	return func(in *http.Request) {
		u := *b.Base
		p := in.URL.EscapedPath()
		// A backend registered as http://host:8088/v1 must be given
		// /v1/chat/completions, not /v1/v1/chat/completions.
		switch {
		case basePath == "":
			u.Path = p
		case p == basePath || strings.HasPrefix(p, basePath+"/"):
			u.Path = p
		default:
			u.Path = basePath + "/" + strings.TrimPrefix(p, "/")
		}
		u.RawPath = ""
		u.RawQuery = in.URL.RawQuery
		in.URL = &u
		in.Host = u.Host
		if b.Token != "" {
			in.Header.Set("Authorization", "Bearer "+b.Token)
		}
	}
}

type stripForwarded struct{ http.RoundTripper }

func (s *stripForwarded) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Del("X-Forwarded-For")
	r.Header.Del("X-Forwarded-Host")
	r.Header.Del("X-Forwarded-Proto")
	return s.RoundTripper.RoundTrip(r)
}

// TotalSlots is the sum of every backend's concurrency, used to size the
// connection pool.
func (c *Config) TotalSlots() int {
	n := 0
	for _, b := range c.backends {
		n += b.Concurrency
	}
	return n
}

func isRooterPath(p string) bool { return p == "/rooter" || strings.HasPrefix(p, "/rooter/") }

// needsSlot reports whether a request should occupy a concurrency slot. Reads
// and cheap metadata/utility endpoints do not; generation does.
func needsSlot(method, path string, free []string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodDelete:
		return false
	}
	for _, f := range free {
		if f == "" {
			continue
		}
		if path == f || strings.HasSuffix(path, f) {
			return false
		}
	}
	return true
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	r.mu.Lock()
	r.total++
	r.mu.Unlock()

	// Front-door auth runs before anything else, including rooter's own
	// endpoints: /rooter/sessions and /rooter/forget are worth scraping, and on a
	// routable address they should not be readable by whoever finds the port.
	if !r.auth.ok(req) {
		r.denial(w, req)
		return
	}
	// The client's rooter credential stops here; backends get their own tokens.
	r.auth.consume(req)

	if isRooterPath(req.URL.Path) {
		r.admin(w, req)
		return
	}

	// The model list is rooter's to answer. As a passthrough it reported whichever
	// box happened to be free, so two calls a second apart disagreed about which
	// models exist at all - and a client that picked from one of them was routed by
	// luck. -no-catalog puts the old behaviour back.
	if !r.cfg.NoCatalog && isModelsPath(req.URL.Path) &&
		(req.Method == http.MethodGet || req.Method == http.MethodHead) {
		r.modelCatalog(w, req)
		r.countStatus(req, http.StatusOK)
		return
	}

	// One pass: headers first, then the buffered body prefix. The body is always
	// scanned when present, because it also carries the requested model, which
	// routing needs even when the session came from a header.
	body, partial, err := r.readBody(req)
	if err != nil {
		errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	info := extractKey(req.Header, body, req.Header.Get("Content-Type"))

	// A backend name is a legitimate thing to ask for, and rooter is the only
	// place that can turn it into a model the box actually has. info.Model stays
	// the alias on purpose: that is what keeps the pinning and the session binding
	// honest, while the body going upstream carries the resolved id so the response
	// echoes a real model rather than a box name.
	if alias, newBody, resolved := r.resolveAlias(info, body, partial, req.Header.Get("Content-Type")); alias != nil {
		body = newBody
		info.Resolved = resolved
		if resolved != info.Model {
			if r.cfg.Verbose {
				r.log.Printf("alias %s -> backend %s model %s", info.Model, alias.Name, resolved)
			}
		}
	}

	slot := needsSlot(req.Method, req.URL.Path, r.cfg.FreePaths)
	// Bindings are per (session, model): one conversation that switches models
	// should stay warm on both backends rather than ping-pong a single binding.
	bindKey := BindKey(info.Key, info.Model)
	var bound *Backend
	if name, ok := r.sessions.Get(bindKey); ok {
		bound = r.pool.Lookup(name)
	}

	ctx := req.Context()
	if slot && r.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.RequestTimeout)
		defer cancel()
	}

	opts := SelectOpts{
		Prefer:     bound,
		Eligible:   r.pool.ModelFilter(info.Model),
		Model:      info.Model,
		MaxWait:    r.cfg.MaxWait,
		Grace:      r.cfg.StickyGrace,
		SpillAfter: r.cfg.SpillAfter,
		NeedSlot:   slot,
	}
	if !slot {
		opts.MaxWait = 0
	}

	if slot {
		r.mu.Lock()
		r.waiting++
		r.slotted++
		r.mu.Unlock()
	}
	held, out, err := r.pool.Acquire(ctx, opts)
	if slot {
		r.mu.Lock()
		r.waiting--
		r.waitTotal += out.Waited
		r.mu.Unlock()
	}
	if err != nil {
		r.mu.Lock()
		// "rejected" is the capacity metric - it should mean "somebody was full".
		// A request for a model nothing serves is a 404, not a lack of slots, and
		// counting it here would make a typo look like overload on the dashboard.
		if statusForErr(err) == http.StatusServiceUnavailable {
			r.rejected++
		}
		r.byStatus[statusForErr(err)]++
		r.mu.Unlock()
		r.noBackend(w, req, info, err, out, start)
		return
	}
	if slot {
		defer func() { r.pool.Release(held) }()
		if bindKey != "" {
			r.sessions.Bind(bindKey, held.Name)
		}
	}

	r.mu.Lock()
	r.inflight++
	r.mu.Unlock()

	// res and aborted are read by the deferred accounting below, so they have to
	// exist before the call that can blow up.
	var res attempt
	aborted := false

	// The record is built here, not in roundTrip, because most of it is known
	// before the backend is even asked: the identity a client needs in order to
	// render a footer while the tokens are still arriving.
	rec := r.newCallRecord(req, info, r.nextID())
	rec.Stream = streamRequested(body)
	rec.Kind = routeKind(slot, out)
	rec.WaitMs = float64(out.Waited) / float64(time.Millisecond)
	if info.Resolved != "" {
		rec.Model = info.Resolved // a backend-name alias: the client asked for the box
	}
	rw := &replyWriter{ResponseWriter: w, started: start, mayBuffer: !r.cfg.NoMeta,
		maxTail: r.cfg.MetaTail, maxBuf: int(r.cfg.MaxBody)}

	// Every way out of this function has to balance the increment above. It did
	// not use to: when a client hangs up mid-response, ReverseProxy panics with
	// http.ErrAbortHandler and net/http recovers that sentinel *silently*, so
	// nothing after dispatch ran. The deferred slot release did run, which is why
	// the box looked healthy - rooter_inflight just climbed forever and every
	// abandoned generation went missing from byStatus. Abandonment is also the
	// one outcome that says something about answer quality, so it gets a counter
	// of its own instead of disappearing.
	//
	// settle also releases a held JSON body, so it has to run on the panic path
	// too: a client waiting on a response rooter decided to buffer would
	// otherwise hang until the server timed it out.
	defer func() {
		if p := recover(); p != nil {
			if p == http.ErrAbortHandler {
				res = attempt{status: statusAborted, err: errAborted, wrote: true, ttfb: res.ttfb}
				aborted = true
			} else {
				r.settle(rw, req, &res, held, info, &out, start, slot, aborted, rec)
				panic(p) // not ours to interpret; let net/http log it and drop the conn
			}
		}
		r.settle(rw, req, &res, held, info, &out, start, slot, aborted, rec)
	}()

	res = r.dispatch(ctx, rw, req, &held, body, partial, info, &out, slot, rec)
}

// settle closes the books on one proxied exchange. It runs on the normal path
// and on the panic path, so it must not assume anything about how far dispatch
// got. Returns the attempt through res so the caller sees the final status.
func (r *Router) settle(w *replyWriter, req *http.Request, res *attempt, b *Backend, info requestInfo, out *Outcome, start time.Time, slot, aborted bool, rec *callRecord) {
	// A caller that hung up before a byte moved is the same outcome as one that hung up
	// mid-stream, and gets counted the same way: dispatch reports it as 499, and without
	// this it would land in byStatus as an abandonment nobody counted.
	if res.status == statusAborted {
		aborted = true
	}
	if res.status == 0 && !aborted {
		res.status = http.StatusBadGateway
		if !res.wrote {
			errJSON(w, res.status, fmt.Sprintf("backend %s failed: %v", b.Name, res.err))
		}
	}
	r.mu.Lock()
	r.inflight--
	r.byStatus[res.status]++
	if aborted {
		r.aborted++
	}
	r.mu.Unlock()

	// Finish the record before releasing a held body, so the object the client
	// sees is the same one rooter keeps.
	r.finishRecord(w, rec, res, b, out, start, aborted)
	r.logLine(req, b, info, *out, *res, start, slot)
}

// statusAborted is nginx's "client closed request". It is never sent on the
// wire - a client that hung up cannot read it - it exists so an abandoned
// generation is a distinct, countable outcome instead of an unrecorded one.
const statusAborted = 499

var errAborted = errors.New("client closed request")

// attempt reports what one round trip did.
type attempt struct {
	status int
	bytes  int64
	err    error
	wrote  bool // any byte of response already handed to the client
	ttfb   time.Duration
}

// dispatch runs the request against *hold, retrying onto another backend when
// the current one failed before a single response byte reached the client.
func (r *Router) dispatch(ctx context.Context, w *replyWriter, req *http.Request, hold **Backend, body []byte, partial bool, info requestInfo, out *Outcome, slot bool, rec *callRecord) attempt {
	maxAttempts := r.cfg.Retries + 1
	if partial {
		maxAttempts = 1 // body only partly buffered: it cannot be replayed
	}
	modelOK := r.pool.ModelFilter(info.Model)
	tried := map[string]bool{}
	var res attempt
	for {
		b := *hold
		tried[b.Name] = true
		rec.Backend = b.Name
		rec.BackendURL = b.Base.String()
		res = r.roundTrip(ctx, w, req, b, body, partial, rec)
		if res.err == nil || res.wrote {
			return res
		}
		// Hanging up is not dying, and confusing the two takes a healthy fleet down.
		//
		// When the caller's context is what ended the round trip - the client timed
		// out, or closed the connection mid-generation - the backend never said
		// anything. Marking it anyway used to be the worst kind of bug: the retry then
		// replayed the request onto the other box with the same dead context, failed
		// instantly, marked that one too, and a router with two perfectly healthy
		// backends answered "no healthy backend serves model auto" to everything still
		// queued behind them. One impatient client could brown out the pool.
		//
		// Slow is a capacity problem, and capacity problems are what the queue, the
		// spill and the wait histogram are for. Whether a box is alive is the health
		// loop's call.
		if done := ctx.Err(); done != nil && errors.Is(res.err, done) {
			return attempt{status: statusAborted, err: errAborted, wrote: res.wrote, bytes: res.bytes, ttfb: res.ttfb}
		}
		r.pool.MarkUnhealthy(b, res.err.Error())
		if len(tried) >= maxAttempts || len(tried) >= len(r.pool.Backends()) {
			return res
		}
		exclude := tried
		nb, o2, aerr := r.pool.Acquire(ctx, SelectOpts{
			Eligible: func(x *Backend) bool {
				if exclude[x.Name] {
					return false
				}
				return modelOK == nil || modelOK(x)
			},
			MaxWait:  r.cfg.RetryWait,
			NeedSlot: slot,
		})
		if aerr != nil {
			return res
		}
		if slot {
			r.pool.Release(b)
		}
		*hold = nb
		out.Backend = nb
		out.Waited += o2.Waited
		out.Rebound = true
		if slot && info.Key != "" {
			r.sessions.Bind(BindKey(info.Key, info.Model), nb.Name)
		}
		if r.cfg.Verbose {
			r.log.Printf("retrying %s %s on %s (after %v)", req.Method, req.URL.Path, nb.Name, res.err)
		}
	}
}

func (r *Router) roundTrip(ctx context.Context, w *replyWriter, req *http.Request, b *Backend, body []byte, partial bool, rec *callRecord) attempt {
	if !r.cfg.NoMeta {
		// These are known before a byte moves, which is exactly why they are
		// headers and not body fields: a client rendering a footer while the
		// tokens are still arriving can have them now.
		rec.Backend = b.Name
		rec.BackendURL = b.Base.String()
		setMetaHeaders(w.Header(), rec)
	}
	px, ok := r.proxies[b.Name]
	if !ok {
		return attempt{status: http.StatusBadGateway, err: fmt.Errorf("no proxy configured for backend %s", b.Name)}
	}
	outreq := req.Clone(ctx)
	switch {
	case len(body) > 0 && partial:
		outreq.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
		outreq.ContentLength = req.ContentLength
	case partial:
		outreq.Body = req.Body
		outreq.ContentLength = req.ContentLength
	case len(body) > 0:
		outreq.Body = io.NopCloser(bytes.NewReader(body))
		outreq.ContentLength = int64(len(body))
	default:
		outreq.Body = nil
		outreq.ContentLength = 0
	}
	if outreq.Header == nil {
		outreq.Header = http.Header{}
	}
	// The writer is the per-request one, not a fresh wrapper per attempt: the
	// buffering state and the tail have to survive a retry onto another backend,
	// and double-wrapping would leave the outer writer believing it wrote nothing.
	w.started = time.Now()
	w.wrote = false
	w.status = 0
	w.tail = w.tail[:0]
	w.buf = w.buf[:0]
	w.buffering = false
	px.ServeHTTP(w, outreq)
	status := w.status
	if status == 0 && w.err == nil {
		status = http.StatusOK
	}
	if status == 0 {
		// Nothing reached the client and the round trip blew up.
		return attempt{status: 0, bytes: w.bytes, err: w.err, wrote: false, ttfb: w.ttfb}
	}
	return attempt{status: status, bytes: w.bytes, err: w.err, wrote: w.wrote, ttfb: w.ttfb}
}

// ---------------------------------------------------------------------------
// response writer
// ---------------------------------------------------------------------------

// replyWriter is the one place rooter sees the response as the client sees it.
//
// It has to do three jobs at once, and the second one is why it is careful:
// report the status and byte count, keep enough of the tail to read the
// per-token numbers the runner puts in its last chunk, and - for a non-streaming
// JSON body only - add rooter's own object to it. A stream is never held back
// and never edited: those bytes are the user's tokens arriving, and buffering
// them to be clever about metadata would be a terrible trade.
type replyWriter struct {
	http.ResponseWriter
	wrote     bool
	status    int
	bytes     int64
	err       error
	ttfb      time.Duration
	started   time.Time
	mayBuffer bool   // this request is one rooter may briefly hold a body for
	buf       []byte // non-streaming JSON body, held so it can be extended
	buffering bool   // currently holding the status and body together
	hook      bodyHook
	tail      []byte // bounded ring of the last bytes written, for timing parsing
	maxTail   int
	maxBuf    int
	// firstContent is how long after rooter started responding the client received
	// a character to read, and contentSeen says whether one ever arrived.
	firstContent time.Duration
	contentSeen  bool
}

func (w *replyWriter) Header() http.Header { return w.ResponseWriter.Header() }

func (w *replyWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = code
	w.ttfb = time.Since(w.started)
	// A 200 JSON body is one object the client cannot act on until it is whole,
	// so it is safe to hold briefly and add a key to. Anything else - an error,
	// a stream, a binary - is passed through untouched.
	// rooter only holds a body when it has been told to add something to it, and
	// that decision is made per request, not here.
	if code == http.StatusOK && w.mayBuffer && w.maxBuf > 0 &&
		strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		w.buffering = true
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

// observe keeps the last bytes of what went to the client, without ever
// allocating more than the ring.
func (w *replyWriter) observe(p []byte) {
	if w.maxTail <= 0 {
		return
	}
	if len(w.tail) == 0 {
		w.tail = make([]byte, 0, w.maxTail)
	}
	w.tail = append(w.tail, p...)
	if over := len(w.tail) - w.maxTail; over > 0 {
		w.tail = append(w.tail[:0], w.tail[over:]...)
	}
	// A stream's first byte and its first word are not the same moment, and the
	// difference is the runner's internal queue. Scanning the ring rather than this
	// one chunk means a marker split across two writes is still seen, and it stops
	// mattering the moment the first real chunk goes past.
	if !w.contentSeen && !w.buffering && len(w.tail) > 0 {
		if at := firstContentOffset(w.tail); at >= 0 {
			w.firstContent = time.Since(w.started)
			w.contentSeen = true
		}
	}
}

// firstContentOffset finds the value of the first non-empty content-like field in a
// chunk and returns its offset, or -1.
//
// A string scan rather than a parse, on purpose: this runs on the copy path of every
// streamed byte, and the thing it is distinguishing is llama.cpp's opening chunk,
// which is the most predictable bytes in the protocol.
//
// The scan tolerates whitespace because llama.cpp writes "key": "value" with a space
// after the colon on some builds and not on others - our two boxes used to disagree
// about it, and today agree. A marker written as `"content":"` matched one runner and
// silently missed the other, and the miss looked exactly like a call that never said
// anything. This is the same lesson streamRequested already learned about the request
// side: tolerate the shape, never assume which box is answering.
//
// reasoning_content is on the list because it is what a thinking model says first and
// it is words the user watches. Live proof: a thinking response on the retired sushi
// box streamed twenty deltas as reasoning_content and its only content field was the
// empty opening one.
func firstContentOffset(p []byte) int {
	for _, key := range []string{`"content"`, `"reasoning_content"`, `"text"`} {
		needle := []byte(key)
		// No accidental overlap: in "reasoning_content" the character before content
		// is an underscore, so the short key cannot match inside it.
		for i := 0; i < len(p); {
			at := bytes.Index(p[i:], needle)
			if at < 0 {
				break
			}
			j := skipSpace(p, i+at+len(needle))
			if j >= len(p) || p[j] != ':' {
				i = j
				continue
			}
			j = skipSpace(p, j+1)
			if j >= len(p) || p[j] != '"' {
				// null, a number, or something rooter is not going to call a word.
				i = j
				continue
			}
			value := j + 1
			if value < len(p) && p[value] != '"' {
				return value
			}
			// An empty string is the opening role delta, not a token.
			i = value
		}
	}
	return -1
}

func skipSpace(p []byte, i int) int {
	for i < len(p) && (p[i] == ' ' || p[i] == '\t' || p[i] == '\n' || p[i] == '\r') {
		i++
	}
	return i
}

func (w *replyWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	w.observe(p)
	if w.buffering {
		if len(w.buf)+len(p) > w.maxBuf {
			// Too big to hold. Give up on being clever; what has already been
			// buffered goes out with the rest.
			w.flushBuf()
			n, err := w.ResponseWriter.Write(p)
			w.bytes += int64(n)
			if err != nil {
				w.err = err
			}
			return n, err
		}
		w.buf = append(w.buf, p...)
		w.bytes += int64(len(p))
		return len(p), nil
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}

// flushBuf writes the held body with the status that was held with it.
//
// Both the status and the length have to wait until here. Releasing the status
// early would send a Content-Length the injected body no longer matches, and
// net/http reacts to that by mangling the response rather than complaining -
// which is a very bad failure mode to discover from a client that simply saw
// nothing.
func (w *replyWriter) flushBuf() {
	if !w.buffering {
		return
	}
	w.buffering = false
	code := w.status
	if code == 0 {
		code = http.StatusOK
	}
	if len(w.buf) > 0 {
		w.Header().Set("Content-Length", strconv.Itoa(len(w.buf)))
	} else {
		// Nothing arrived, so there is no length to promise. Dropping the
		// upstream's header stops net/http declaring bytes it never gets.
		w.Header().Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(code)
	if len(w.buf) == 0 {
		return
	}
	n, err := w.ResponseWriter.Write(w.buf)
	if n > 0 {
		w.bytes += int64(n) // an injected body is longer than the one already counted
	}
	if err != nil {
		w.err = err
	}
	w.buf = w.buf[:0]
}

// finalize releases a held body and its status. It must run on every path,
// including the one where the proxy panicked: a client waiting on a response
// rooter decided to buffer would otherwise hang until the server timed it out.
//
// It is also where the injection happens, because that is the last moment the
// bytes are still rooter's to change.
func (w *replyWriter) finalize() {
	if !w.buffering {
		return
	}
	w.buf = w.hook(w.buf)
	w.flushBuf()
}

// hook is given the complete upstream body and returns what goes to the client.
// Set by finishRecord. It must not fail: returning nil drops the body, so the
// default is identity.
type bodyHook func([]byte) []byte

func (w *replyWriter) Flush() {
	// A flush commits the response header. While a body is being held that is
	// exactly the wrong thing to do: ReverseProxy flushes after every chunk with
	// FlushInterval -1, so honouring it here would write the status and the
	// upstream's Content-Length before the injected body existed, and net/http
	// would mangle the result rather than complain. A JSON response is not a
	// stream; there is nothing the client is waiting to see mid-flush.
	if w.buffering {
		return
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *replyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ---------------------------------------------------------------------------
// bodies
// ---------------------------------------------------------------------------

// readBody buffers the request body when it is small enough to replay, and
// otherwise keeps only the leading -scan-window bytes and streams the rest.
func (r *Router) readBody(req *http.Request) (body []byte, partial bool, err error) {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodDelete:
		return nil, false, nil
	}
	if req.Body == nil {
		return nil, false, nil
	}
	window := int64(r.cfg.ScanWindow)
	if window <= 0 {
		window = 4096
	}
	max := r.cfg.MaxBody
	if max < window {
		max = window
	}
	// Read one byte past the window so a body of exactly -scan-window bytes is
	// not mistaken for a longer one.
	buf := make([]byte, window+1)
	n, readErr := io.ReadFull(req.Body, buf)
	if readErr == nil || readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
		if int64(n) <= window {
			return buf[:n], false, nil // whole body buffered
		}
	} else {
		return buf[:n], true, readErr
	}
	// Everything consumed so far is the prefix; keep it whole so a replayed or
	// streamed body is byte-exact.
	head := buf[:n]
	// Body is longer than the scan window. Hold all of it if it is a sane size,
	// otherwise keep the prefix and stream the remainder through.
	if req.ContentLength > int64(max) {
		return head, true, nil
	}
	whole := append([]byte(nil), buf...)
	rest, rerr := io.ReadAll(io.LimitReader(req.Body, max-int64(len(whole))+1))
	if rerr != nil {
		return head, true, rerr
	}
	whole = append(whole, rest...)
	if int64(len(whole)) > max || (req.ContentLength >= 0 && int64(len(whole)) > req.ContentLength) {
		return head, true, nil
	}
	return whole, false, nil
}

// ---------------------------------------------------------------------------
// errors and logging
// ---------------------------------------------------------------------------

func statusForErr(err error) int {
	switch {
	case err == nil:
		return 200
	case errors.Is(err, errNoModel):
		// A model no backend claims will never succeed, whatever the client
		// retries. llama.cpp would answer it with a different model, silently.
		return http.StatusNotFound
	case errors.Is(err, errBackendDown):
		// The request named a box, and that box is not answering. Nothing about the
		// name is wrong, so this is "try again", not "that model does not exist".
		return http.StatusServiceUnavailable
	case errors.Is(err, errMaxWait), errors.Is(err, errNoCandidate), errors.Is(err, errBusy):
		return http.StatusServiceUnavailable
	default:
		return http.StatusGatewayTimeout
	}
}

// denial answers a request that failed rooter's own front-door check. It takes no
// slot, touches no backend, and never says anything about which key was expected.
func (r *Router) denial(w http.ResponseWriter, req *http.Request) {
	have, _ := presented(req)
	r.mu.Lock()
	r.denied++
	r.byStatus[http.StatusUnauthorized]++
	r.mu.Unlock()
	r.logDenial(req)
	authDenied(w, have == "")
}

func (r *Router) noBackend(w http.ResponseWriter, req *http.Request, info requestInfo, err error, out Outcome, start time.Time) {
	code := statusForErr(err)
	msg := "no backend available"
	switch {
	case errors.Is(err, errBackendDown):
		// Named a box, box is down: say which box, and say it as an outage. A client
		// reading a 404 here would conclude its configuration names a model that does
		// not exist, which is the opposite of what happened.
		var down *backendDownErr
		if errors.As(err, &down) {
			msg = down.Error()
		} else {
			msg = err.Error()
		}
	case errors.Is(err, errNoModel):
		msg = fmt.Sprintf("no backend serves model %q", info.Model)
	case errors.Is(err, errNoCandidate):
		if info.Model != "" {
			msg = fmt.Sprintf("no healthy backend serves model %q", info.Model)
		} else {
			msg = "no healthy backend"
		}
	case errors.Is(err, errMaxWait), errors.Is(err, errBusy):
		msg = fmt.Sprintf("all backends busy (waited %s)", out.Waited.Round(time.Millisecond))
	default: // context.Canceled / DeadlineExceeded
		msg = err.Error()
	}
	if r.cfg.Verbose || !errors.Is(err, context.Canceled) {
		r.log.Printf("%s %s session=%s(%s) queued=%s -> %d: %s (%s)",
			req.Method, req.URL.Path, shortKey(info.Key), info.KeySource,
			out.Waited.Round(time.Millisecond), code, msg, time.Since(start).Round(time.Millisecond))
	}
	if errors.Is(err, context.Canceled) {
		return // client hung up; nothing to say
	}
	if code != http.StatusNotFound {
		// Only a busy/unavailable answer is worth retrying; telling a client to
		// retry a 404 just teaches it to hammer a model that does not exist.
		w.Header().Set("Retry-After", "1")
	}
	errJSON(w, code, msg)
}

// routeKind is why a call went where it went. It is one function so the log
// line, the X-Rooter-Route header and the event record cannot drift apart.
func routeKind(slot bool, out Outcome) string {
	if !slot {
		return "read"
	}
	if out.Rebound {
		if out.Spilled {
			return "spill" // waited too long on a busy sticky backend
		}
		return "rebind"
	}
	return "gen"
}

func (r *Router) logLine(req *http.Request, b *Backend, info requestInfo, out Outcome, res attempt, start time.Time, slot bool) {
	if r.cfg.Quiet && res.status < 400 && res.err == nil {
		return
	}
	kind := routeKind(slot, out)
	model := ""
	if info.Model != "" {
		model = " model=" + info.Model
		if info.Resolved != "" && info.Resolved != info.Model {
			model += "->" + info.Resolved // a backend-name alias
		}
	}
	r.log.Printf("%s %s -> %s [%s key=%s(%s)%s status=%d bytes=%d wait=%s ttfb=%s total=%s%s",
		req.Method, req.URL.Path, b.Name, kind, shortKey(info.Key), info.KeySource, model,
		res.status, res.bytes, out.Waited.Round(time.Millisecond), res.ttfb.Round(time.Millisecond),
		time.Since(start).Round(time.Millisecond), errSuffix(res))
}

func errSuffix(res attempt) string {
	if res.err == nil {
		return ""
	}
	return " err=" + res.err.Error()
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":{"type":"rooter_error","message":%q}}`, msg)
}

// countStatus records a response rooter served itself, so the model catalog and
// the proxied paths land in one histogram instead of the catalog being invisible
// in /rooter/stats and /rooter/metrics.
func (r *Router) countStatus(req *http.Request, code int) {
	r.mu.Lock()
	r.byStatus[code]++
	r.mu.Unlock()
}
