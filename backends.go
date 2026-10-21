package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Backend is one upstream LLM server. Fields other than the immutable config
// are guarded by Pool.mu.
type Backend struct {
	Name        string
	Base        *url.URL // includes any path prefix, e.g. /v1
	Token       string
	Concurrency int

	// sem holds one token per in-flight request that is using a slot.
	sem chan struct{}

	// mutable state, Pool.mu
	Healthy   bool
	Models    map[string]bool // nil => unknown, treat as serving anything
	Checked   time.Time
	LastUsed  time.Time
	Requests  int64
	Errors    int64
	TotalWait time.Duration
}

func (b *Backend) busy() int { return len(b.sem) }

func (b *Backend) free() int { return cap(b.sem) - len(b.sem) }

func (b *Backend) tryAcquire() bool {
	select {
	case b.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (b *Backend) releaseSlot() {
	select {
	case <-b.sem:
	default:
	}
}

func (b *Backend) MarshalJSON() ([]byte, error) {
	models := make([]string, 0, len(b.Models))
	for m := range b.Models {
		models = append(models, m)
	}
	sortStrings(models)
	return json.Marshal(map[string]any{
		"name":        b.Name,
		"url":         b.Base.String(),
		"concurrency": b.Concurrency,
		"busy":        b.busy(),
		"healthy":     b.Healthy,
		"models":      models,
		"lastUsed":    b.LastUsed,
		"requests":    b.Requests,
		"errors":      b.Errors,
		"totalWait":   b.TotalWait.String(),
		"checked":     b.Checked,
	})
}

// parseBackends turns -backend flag values into Backends.
//
// Accepted shapes:
//
//	http://host:8088/v1
//	myname=http://host:8088/v1
//	name=myname,url=http://host:8088/v1,token=sekrit,concurrency=4
//
// Keys: name, url|base|endpoint, token|key|api-key|apikey,
// concurrency|slots|parallel, priority (unused for now).
// Values containing , or = need to be escaped as \, and \=.
func parseBackends(specs []string, defaultConcurrency int) ([]*Backend, error) {
	out := make([]*Backend, 0, len(specs))
	seen := map[string]bool{}
	for _, spec := range specs {
		b, err := parseBackend(spec, defaultConcurrency)
		if err != nil {
			return nil, err
		}
		if seen[b.Name] {
			return nil, fmt.Errorf("duplicate backend name %q in %q", b.Name, spec)
		}
		seen[b.Name] = true
		out = append(out, b)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no backends configured (use -backend at least once)")
	}
	return out, nil
}

func parseBackend(spec string, defaultConcurrency int) (*Backend, error) {
	fields := map[string]string{}
	positional := []string{}
	rest := spec
	for rest != "" {
		var piece, after string
		var found bool
		piece, after, found = cutUnescaped(rest, ',')
		rest = after
		if strings.TrimSpace(piece) == "" {
			continue
		}
		if piece == "" {
			continue
		}
		k, v, found := cutUnescaped(piece, '=')
		if !found {
			positional = append(positional, unescape(k))
			continue
		}
		fields[normalizeKey(unescape(k))] = unescape(v)
	}

	var b Backend
	b.Token = fields["token"]
	b.Concurrency = defaultConcurrency
	if v := fields["concurrency"]; v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("bad concurrency %q in %q", v, spec)
		}
		b.Concurrency = n
	}

	rawURL := fields["url"]
	name := fields["name"]
	for _, p := range positional {
		if strings.Contains(p, "://") {
			rawURL = p
		} else if name == "" {
			name = p
		} else {
			return nil, fmt.Errorf("unexpected token %q in backend spec %q", p, spec)
		}
	}
	// "sushi=http://host:8088/v1" - an unrecognised key whose value is a URL is
	// read as name=url. An explicit name= still wins.
	if rawURL == "" {
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			v := fields[k]
			if knownKey(k) || !strings.Contains(v, "://") {
				continue
			}
			rawURL = v
			if name == "" {
				name = k
			}
			break
		}
	}
	if rawURL == "" && name != "" && strings.Contains(name, "://") {
		rawURL, name = name, ""
	}
	if rawURL == "" {
		return nil, fmt.Errorf("backend %q has no url", spec)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("bad url in %q: %w", spec, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("backend %q: scheme must be http or https", spec)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("backend %q: missing host", spec)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	b.Base = u
	if name == "" {
		name = uHostname(u)
	}
	b.Name = name
	b.sem = make(chan struct{}, b.Concurrency)
	return &b, nil
}

// normalizeKey maps the accepted spec keys onto a canonical set, and keeps
// unknown keys as-is so that "sushi=http://..." style can be detected.
func normalizeKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	switch k {
	case "url", "base", "endpoint", "uri":
		return "url"
	case "token", "key", "apikey", "api-key", "authorization":
		return "token"
	case "concurrency", "slots", "slot", "parallel", "parallelism", "max-concurrency":
		return "concurrency"
	case "name", "id":
		return "name"
	}
	return k
}

func knownKey(k string) bool {
	switch k {
	case "url", "token", "concurrency", "name":
		return true
	}
	return false
}

func uHostname(u *url.URL) string {
	h := u.Hostname()
	if h == "" {
		return u.Host
	}
	return h
}

// cutUnescaped splits s on the first unescaped instance of sep, without
// touching escapes: a backslash hides the character after it, so a URL that
// contains a comma can be written as \,. The winning token is unescaped by the
// caller once it is final, otherwise \, in a later field would be eaten by an
// earlier split.
func cutUnescaped(s string, sep byte) (before, after string, found bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

func unescape(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}

func sortStrings(s []string) {
	sort.Strings(s)
}
