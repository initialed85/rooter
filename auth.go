package main

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// defaultPublicPaths are reachable without an -api-key. There is exactly one
// reason for them: kubelet's readiness and liveness probes get no credential, and
// a probe that 401s would restart the pod and wipe the session table. They are
// namespaced under /rooter on purpose - a suffix match on "/health" alone would
// also punch a hole in an upstream /v1/health.
var defaultPublicPaths = []string{
	"/rooter/healthz", "/rooter/health",
}

// authGuard is rooter's own front-door credential check. It is deliberately
// narrow: one shared bearer token, compared in constant time, and the credential
// is consumed here rather than passed on upstream. Nothing about it knows which
// backend the request will end up at.
type authGuard struct {
	keys   []string
	public []string
}

func newAuthGuard(cfg *Config) *authGuard {
	public := cfg.PublicPaths
	if public == nil {
		// A Router built without going through the flags (tests, embedding) still
		// gets the probes open: an auth that 401s kubelet's readiness probe restarts
		// the pod and wipes the session table, which is a worse failure than a
		// one-line health endpoint being public.
		public = defaultPublicPaths
	}
	a := &authGuard{public: public}
	for _, k := range cfg.APIKeys {
		if k = strings.TrimSpace(k); k != "" {
			a.keys = append(a.keys, k)
		}
	}
	return a
}

func (a *authGuard) enabled() bool { return a != nil && len(a.keys) > 0 }

func (a *authGuard) keyCount() int { return len(a.keys) }

// isPublic reports whether a path is reachable without a credential.
func (a *authGuard) isPublic(path string) bool {
	for _, p := range a.public {
		if p == "" {
			continue
		}
		if path == p || strings.HasSuffix(path, p) {
			return true
		}
	}
	return false
}

// presented pulls the client's credential out of the request, and says where it
// came from so a denial can tell "sent nothing" from "sent the wrong thing".
// Every spelling that OpenAI-ish clients use is accepted: the SDK bearer token Pi
// sends, x-api-key, and a bare Authorization value for clients that skip the
// scheme.
func presented(req *http.Request) (value, source string) {
	if v := strings.TrimSpace(req.Header.Get("Authorization")); v != "" {
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			return strings.TrimSpace(v[7:]), "bearer"
		}
		return v, "authorization"
	}
	if v := strings.TrimSpace(req.Header.Get("X-Api-Key")); v != "" {
		return v, "x-api-key"
	}
	return "", "none"
}

func (a *authGuard) ok(req *http.Request) bool {
	if !a.enabled() || a.isPublic(req.URL.Path) {
		return true
	}
	got, _ := presented(req)
	if got == "" {
		return false
	}
	for _, k := range a.keys {
		// Constant time per key, so a scanner cannot walk the prefix of a valid
		// one. The loop itself leaks how many keys are configured; that is a
		// count of our own deployments, not a secret.
		if subtle.ConstantTimeCompare([]byte(got), []byte(k)) == 1 {
			return true
		}
	}
	return false
}

// consume drops the client's rooter credential from the request before it is
// proxied. The key that opened rooter is rooter's business: forwarding it would
// hand a caller's credential to whichever llama.cpp box the request lands on,
// where it is at best meaningless and at worst the same string. Backend tokens,
// if any, are injected by the director after this.
func (a *authGuard) consume(req *http.Request) {
	if !a.enabled() {
		return
	}
	req.Header.Del("Authorization")
	req.Header.Del("X-Api-Key")
}

// authDenied is the 401. The body is shaped like an OpenAI error so SDKs report
// "incorrect API key" rather than a parse failure, and the message distinguishes
// nothing about which key was expected.
func authDenied(w http.ResponseWriter, missing bool) {
	msg := "invalid API key"
	if missing {
		msg = "missing API key: send Authorization: Bearer <key> or x-api-key: <key>"
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="rooter", error="invalid_key"`)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusUnauthorized)
	fmt.Fprintf(w, `{"error":{"type":"authentication_error","code":"invalid_api_key","message":%q}}`, msg)
}

// ---------------------------------------------------------------------------
// client identity for denial logging
// ---------------------------------------------------------------------------

// remoteHost is who to blame in a denial log line. Traefik puts the real client
// in X-Forwarded-For; we only use it for our own log, never as an authorisation
// input, so a spoofed header is at worst a misleading log line.
func remoteHost(req *http.Request) string {
	if xff := req.Header.Get("X-Forwarded-For"); xff != "" {
		if first := strings.TrimSpace(strings.Split(xff, ",")[0]); first != "" {
			return first
		}
	}
	addr := req.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// listenIsLoopback reports whether a -listen address is reachable only from this
// machine. Used for one honest line of startup output: an open front door on a
// routable address means anyone who can route to it can spend your GPUs.
func listenIsLoopback(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "" { // ":8089" and "[::]:8089" bind every interface
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// denyLogInterval bounds how often one client can make us write a log line. An
// internet-facing port gets scanned within minutes of appearing, and a denial per
// probe would turn the pod log into a wall of 401s and bury the routing decisions
// that are the reason the log exists.
const denyLogInterval = 30 * time.Second

// denyLogCap bounds the bookkeeping map. Scanners rotate through source ports, so
// the key space is unbounded otherwise.
const denyLogCap = 512

type denyNote struct {
	logged     time.Time
	suppressed int64
}

// logDenial records a rejected request and prints at most one line per client per
// denyLogInterval, summarising whatever it folded away.
func (r *Router) logDenial(req *http.Request) {
	now := r.now()
	ip := remoteHost(req)

	r.mu.Lock()
	n := r.denyNote[ip]
	if n == nil {
		if len(r.denyNote) >= denyLogCap {
			r.pruneDenyNotes(now) // clears the map if pruning is not enough
			n = r.denyNote[ip]
		}
		if n == nil {
			n = &denyNote{}
			r.denyNote[ip] = n
		}
	}
	n.suppressed++
	if !n.logged.IsZero() && now.Sub(n.logged) < denyLogInterval {
		r.mu.Unlock()
		return
	}
	quiet := n.suppressed - 1
	n.logged, n.suppressed = now, 0
	r.mu.Unlock()

	suffix := ""
	if quiet > 0 {
		suffix = fmt.Sprintf(" (+%d more from this client in the last %s)", quiet, denyLogInterval)
	}
	r.log.Printf("401 %s %s from %s%s", req.Method, req.URL.Path, ip, suffix)
}

// pruneDenyNotes drops stale entries; called with r.mu held. If nothing was stale
// the map is cleared outright, which only costs some summarised 401s.
func (r *Router) pruneDenyNotes(now time.Time) {
	for ip, n := range r.denyNote {
		if now.Sub(n.logged) > 10*denyLogInterval {
			delete(r.denyNote, ip)
		}
	}
	if len(r.denyNote) >= denyLogCap {
		r.denyNote = map[string]*denyNote{}
	}
}
