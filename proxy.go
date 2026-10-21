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
	proxies  map[string]*httputil.ReverseProxy
	start    time.Time

	mu        sync.Mutex
	total     int64
	inflight  int64
	waiting   int64
	rejected  int64
	waitTotal time.Duration
	byStatus  map[int]int64
}

func NewRouter(p *Pool, s *SessionStore, cfg *Config, logger *log.Logger) *Router {
	r := &Router{
		pool:     p,
		sessions: s,
		cfg:      cfg,
		log:      logger,
		proxies:  map[string]*httputil.ReverseProxy{},
		start:    time.Now(),
		byStatus: map[int]int64{},
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
	if isRooterPath(req.URL.Path) {
		r.admin(w, req)
		return
	}
	start := time.Now()
	r.mu.Lock()
	r.total++
	r.mu.Unlock()

	// One pass: headers first, then the buffered body prefix. The body is always
	// scanned when present, because it also carries the requested model, which
	// routing needs even when the session came from a header.
	body, partial, err := r.readBody(req)
	if err != nil {
		errJSON(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	info := extractKey(req.Header, body, req.Header.Get("Content-Type"))

	slot := needsSlot(req.Method, req.URL.Path, r.cfg.FreePaths)
	var bound *Backend
	if name, ok := r.sessions.Get(info.Key); ok {
		bound = r.pool.Lookup(name)
	}

	ctx := req.Context()
	if slot && r.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.cfg.RequestTimeout)
		defer cancel()
	}

	opts := SelectOpts{
		Prefer:   bound,
		Eligible: r.pool.ModelFilter(info.Model),
		MaxWait:  r.cfg.MaxWait,
		Grace:    r.cfg.StickyGrace,
		NeedSlot: slot,
	}
	if !slot {
		opts.MaxWait = 0
	}

	if slot {
		r.mu.Lock()
		r.waiting++
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
		r.rejected++
		r.byStatus[statusForErr(err)]++
		r.mu.Unlock()
		r.noBackend(w, req, info, err, out, start)
		return
	}
	if slot {
		defer func() { r.pool.Release(held) }()
		if info.Key != "" {
			r.sessions.Bind(info.Key, held.Name)
		}
	}

	r.mu.Lock()
	r.inflight++
	r.mu.Unlock()
	res := r.dispatch(ctx, w, req, &held, body, partial, info, &out, slot)
	if res.status == 0 {
		res.status = http.StatusBadGateway
		if !res.wrote {
			errJSON(w, res.status, fmt.Sprintf("backend %s failed: %v", held.Name, res.err))
		}
	}
	r.mu.Lock()
	r.inflight--
	r.byStatus[res.status]++
	r.mu.Unlock()
	r.logLine(req, held, info, out, res, start, slot)
}

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
func (r *Router) dispatch(ctx context.Context, w http.ResponseWriter, req *http.Request, hold **Backend, body []byte, partial bool, info requestInfo, out *Outcome, slot bool) attempt {
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
		res = r.roundTrip(ctx, w, req, b, body, partial)
		if res.err == nil || res.wrote {
			return res
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
			r.sessions.Bind(info.Key, nb.Name)
		}
		if r.cfg.Verbose {
			r.log.Printf("retrying %s %s on %s (after %v)", req.Method, req.URL.Path, nb.Name, res.err)
		}
	}
}

func (r *Router) roundTrip(ctx context.Context, w http.ResponseWriter, req *http.Request, b *Backend, body []byte, partial bool) attempt {
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
	rw := &replyWriter{ResponseWriter: w, started: time.Now()}
	px.ServeHTTP(rw, outreq)
	status := rw.status
	if status == 0 && rw.err == nil {
		status = http.StatusOK
	}
	if status == 0 {
		// Nothing reached the client and the round trip blew up.
		return attempt{status: 0, bytes: rw.bytes, err: rw.err, wrote: false, ttfb: rw.ttfb}
	}
	return attempt{status: status, bytes: rw.bytes, err: rw.err, wrote: rw.wrote, ttfb: rw.ttfb}
}

// ---------------------------------------------------------------------------
// response writer
// ---------------------------------------------------------------------------

type replyWriter struct {
	http.ResponseWriter
	wrote   bool
	status  int
	bytes   int64
	err     error
	ttfb    time.Duration
	started time.Time
}

func (w *replyWriter) Header() http.Header { return w.ResponseWriter.Header() }

func (w *replyWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.status = code
	w.ttfb = time.Since(w.started)
	w.ResponseWriter.WriteHeader(code)
}

func (w *replyWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *replyWriter) Flush() {
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
	case errors.Is(err, errMaxWait), errors.Is(err, errNoCandidate), errors.Is(err, errBusy):
		return http.StatusServiceUnavailable
	default:
		return http.StatusGatewayTimeout
	}
}

func (r *Router) noBackend(w http.ResponseWriter, req *http.Request, info requestInfo, err error, out Outcome, start time.Time) {
	code := statusForErr(err)
	msg := "no backend available"
	switch {
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
	w.Header().Set("Retry-After", "1")
	errJSON(w, code, msg)
}

func (r *Router) logLine(req *http.Request, b *Backend, info requestInfo, out Outcome, res attempt, start time.Time, slot bool) {
	if r.cfg.Quiet && res.status < 400 && res.err == nil {
		return
	}
	kind := "read"
	if slot {
		kind = "gen"
	}
	r.log.Printf("%s %s -> %s [%s key=%s(%s) status=%d bytes=%d wait=%s ttfb=%s total=%s%s",
		req.Method, req.URL.Path, b.Name, kind, shortKey(info.Key), info.KeySource,
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
