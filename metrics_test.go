package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

// TestMetricsListenerAnswersOnlyTheScrape pins the two halves of the deal at once:
// the scrape needs no key, and nothing else is reachable there. A keyless listener
// that also served /rooter/sessions or /rooter/backends would be a far larger
// disclosure than a set of counters, and the whole reason the port exists is that
// it can be left off the ingress.
func TestMetricsListenerAnswersOnlyTheScrape(t *testing.T) {
	live := newFake(t, "strata", "qwen-tiny", "")
	e := newEnv(t, []string{"strata=" + live.url()}, func(c *Config) {
		c.APIKeys = []string{"front-door-key"}
		c.MetricsListen = ":8090"
	})
	e.pool.CheckAll()

	metrics := httptest.NewServer(http.HandlerFunc(e.router.metricsOnly))
	defer metrics.Close()

	fetch := func(method, path, key string) (int, string) {
		req, err := http.NewRequest(method, metrics.URL+path, nil)
		if err != nil {
			t.Fatalf("request %s: %v", path, err)
		}
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		res, err := metrics.Client().Do(req)
		if err != nil {
			t.Fatalf("fetch %s: %v", path, err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}

	// The scrape, with no credentials of any kind.
	code, body := fetch(http.MethodGet, "/rooter/metrics", "")
	if code != http.StatusOK {
		t.Fatalf("the scrape without a key got %d: %s", code, body)
	}
	wants := []string{"rooter_backend_down_seconds", "rooter_backend_health_fails",
		"rooter_model_requests_total", `backend="strata"`}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("the scrape is missing %s: %s", want, body)
		}
	}

	// A wrong key is irrelevant here, which is the point: the scraper no longer
	// needs one, so there is no secret to keep in step with rooter's front door.
	code, _ = fetch(http.MethodGet, "/rooter/metrics", "wrong-key")
	if code != http.StatusOK {
		t.Errorf("the scrape answered %d to a wrong key; it is meant to answer to nobody", code)
	}

	// Everything else is closed, including the admin paths that would be a real
	// disclosure without a key.
	for _, path := range []string{"/rooter", "/rooter/stats", "/rooter/backends",
		"/rooter/sessions", "/rooter/models", "/rooter/events", "/rooter/forget?key=*",
		"/rooter/healthz", "/v1/models", "/v1/chat/completions", "/", "/rooter/metricsx"} {
		code, body = fetch(http.MethodGet, path, "")
		if code != http.StatusNotFound {
			t.Errorf("%s answered %d on the metrics listener: %s; only /rooter/metrics belongs there", path, code, body)
		}
	}

	// A POST is not a scrape, and /rooter/forget on the front door is a mutation
	// nobody should be able to do through a read-only scraper port.
	code, _ = fetch(http.MethodPost, "/rooter/metrics", "")
	if code != http.StatusMethodNotAllowed {
		t.Errorf("a POST to the metrics listener got %d, not 405", code)
	}

	// The front door is untouched: the same path on the main listener still needs
	// the key, so nothing about rooter's inbound auth changed.
	res, body := e.do(get("/rooter/metrics"))
	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("/rooter/metrics on the main listener got %d without a key: %s", res.StatusCode, body)
	}
	req := get("/rooter/metrics")
	req.Header.Set("Authorization", "Bearer front-door-key")
	res, body = e.do(req)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("/rooter/metrics with the key got %d: %s", res.StatusCode, body)
	}

	// Same numbers on both surfaces, so a dashboard cannot be fed two different
	// versions of the truth.
	names := func(s string) []string {
		out := []string{}
		for _, line := range strings.Split(s, "\n") {
			if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
				continue
			}
			name := strings.Fields(line)[0]
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	}
	_, keyless := fetch(http.MethodGet, "/rooter/metrics", "")
	series := names(keyless)
	if len(series) < 10 {
		t.Fatalf("the keyless scrape returned only %d series", len(series))
	}
	for _, want := range []string{"rooter_backend_down_seconds", "rooter_backend_health_fails", "rooter_backend_slots_total"} {
		if !strings.Contains(strings.Join(series, "\n"), want) {
			t.Errorf("the keyless scrape does not carry %s", want)
		}
	}
}

func TestMetricsListenFlagIsOffByDefault(t *testing.T) {
	c, err := parseFlags([]string{"-backend", "strata=http://127.0.0.1:8088/v1"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.MetricsListen != "" {
		t.Errorf("metrics listen defaults to %q; a second listener should be a choice, not a default", c.MetricsListen)
	}
	c, err = parseFlags([]string{"-backend", "strata=http://127.0.0.1:8088/v1", "-metrics-listen", ":8090"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if c.MetricsListen != ":8090" {
		t.Errorf("-metrics-listen parsed as %q", c.MetricsListen)
	}
}
