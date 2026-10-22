package main

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"time"
)

// SessionStore maps session key -> backend name. Entries expire after an idle
// TTL and the whole map is capped with LRU eviction so a long-lived router
// cannot grow memory without bound.
type SessionStore struct {
	mu     sync.Mutex
	ttl    time.Duration
	max    int
	ents   map[string]*list.Element
	lru    *list.List // front = most recently used
	log    logLike
	hits   int64
	miss   int64
	rebind int64
}

type logLike interface{ Printf(string, ...any) }

type sessionEnt struct {
	key     string
	backend string
	last    time.Time
}

// BindSep separates a session key from the model it was bound on. A session is
// one conversation, but a conversation that switches models wants to stay warm
// on *both* backends, not ping-pong a single binding - so bindings are per
// (session, model) and the separator is invisible in the key itself.
const BindSep = "\x1f"

// BindKey turns a session key plus the requested model into the storage key for
// the binding. Model is folded away when empty so requests that never name one
// still share a single binding.
func BindKey(key, model string) string {
	if key == "" {
		return ""
	}
	model = strings.TrimSpace(model)
	if model == "" || model == "default" {
		return key
	}
	return key + BindSep + model
}

func NewSessionStore(ttl time.Duration, max int, logger logLike) *SessionStore {
	if max <= 0 {
		max = 100000
	}
	return &SessionStore{ttl: ttl, max: max, ents: map[string]*list.Element{}, lru: list.New(), log: logger}
}

// Get returns the backend a session last used, refreshing its idle TTL.
func (s *SessionStore) Get(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.ents[key]
	if !ok {
		s.miss++
		return "", false
	}
	e := el.Value.(*sessionEnt)
	if s.ttl > 0 && time.Since(e.last) > s.ttl {
		s.dropLocked(el)
		s.miss++
		return "", false
	}
	e.last = time.Now()
	s.lru.MoveToFront(el)
	s.hits++
	return e.backend, true
}

// Bind records (or overwrites) the backend for a session.
func (s *SessionStore) Bind(key, backend string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.ents[key]; ok {
		e := el.Value.(*sessionEnt)
		if e.backend != backend {
			s.rebind++
			if s.log != nil {
				s.log.Printf("session %s rebound %s -> %s", logBindKey(key), e.backend, backend)
			}
			e.backend = backend
		}
		e.last = time.Now()
		s.lru.MoveToFront(el)
		return
	}
	e := &sessionEnt{key: key, backend: backend, last: time.Now()}
	s.ents[key] = s.lru.PushFront(e)
	for len(s.ents) > s.max {
		s.dropLocked(s.lru.Back())
	}
}

// Unbind removes one binding, given its exact storage key.
func (s *SessionStore) Unbind(key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.ents[key]; ok {
		s.dropLocked(el)
	}
}

// UnbindSession forgets a session and every per-model binding hanging off it, so
// /rooter/forget?key=h:abc works without having to know which models that
// session has used. Returns how many bindings were dropped.
func (s *SessionStore) UnbindSession(key string) int {
	if key == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.unbindLocked(key); n > 0 {
		return n
	}
	// Someone pasting an id from a log line or from Pi will not have the h:/p:
	// source tag on it, and guessing wrong only costs a second sweep.
	for _, tag := range []string{"h:", "p:"} {
		if n := s.unbindLocked(tag + key); n > 0 {
			return n
		}
	}
	return 0
}

// unbindLocked drops the exact key plus any "<key><sep><model>" bindings, and
// returns how many went away. s.mu must be held.
func (s *SessionStore) unbindLocked(key string) int {
	n := 0
	for k, el := range s.ents {
		if k == key || strings.HasPrefix(k, key+BindSep) {
			s.dropLocked(el)
			n++
		}
	}
	return n
}

func (s *SessionStore) dropLocked(el *list.Element) {
	if el == nil {
		return
	}
	e := el.Value.(*sessionEnt)
	delete(s.ents, e.key)
	s.lru.Remove(el)
}

// Clear drops every binding. Use it after a backend restart, when every cache on
// the far side is cold and stickiness is only pointing at stale memory.
func (s *SessionStore) Clear() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.ents)
	for _, el := range s.ents {
		s.dropLocked(el)
	}
	return n
}

func (s *SessionStore) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for el := s.lru.Back(); el != nil; {
		prev := el.Prev()
		e := el.Value.(*sessionEnt)
		if s.ttl > 0 && time.Since(e.last) > s.ttl {
			s.dropLocked(el)
			n++
		}
		el = prev
	}
	return n
}

func (s *SessionStore) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	// One conversation can hold several bindings, one per model it has used, so
	// the two numbers are reported apart rather than quietly redefining "sessions".
	sessions := map[string]bool{}
	for k := range s.ents {
		sessions[BaseKey(k)] = true
	}
	return map[string]any{
		"sessions": len(sessions),
		"bindings": len(s.ents),
		"hits":     s.hits,
		"misses":   s.miss,
		"rebinds":  s.rebind,
		"ttl":      s.ttl.String(),
		"max":      s.max,
	}
}

// BaseKey strips the model suffix from a binding key, giving the session it
// belongs to.
func BaseKey(key string) string {
	if i := strings.Index(key, BindSep); i >= 0 {
		return key[:i]
	}
	return key
}

// Janitor sweeps expired sessions in the background.
func (s *SessionStore) Janitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.Sweep(); n > 0 {
				if s.log != nil {
					s.log.Printf("expired %d idle session(s)", n)
				}
			}
		}
	}
}

func shortKey(k string) string {
	if len(k) > 12 {
		return k[:12]
	}
	return k
}

// logBindKey renders a binding key for humans: the separator is unprintable, and
// truncating it would drop the model that the binding is actually keyed on.
func logBindKey(k string) string {
	return strings.ReplaceAll(k, BindSep, " model=")
}
