package main

import (
	"container/list"
	"context"
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
				s.log.Printf("session %s rebound %s -> %s", shortKey(key), e.backend, backend)
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

// Unbind forgets a session so the next request makes a fresh decision.
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

func (s *SessionStore) dropLocked(el *list.Element) {
	if el == nil {
		return
	}
	e := el.Value.(*sessionEnt)
	delete(s.ents, e.key)
	s.lru.Remove(el)
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
	return map[string]any{
		"sessions": len(s.ents),
		"hits":     s.hits,
		"misses":   s.miss,
		"rebinds":  s.rebind,
		"ttl":      s.ttl.String(),
		"max":      s.max,
	}
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
