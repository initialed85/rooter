package main

import (
	"encoding/json"
	"testing"
)

// TestPromptNMeansDifferentThingsOnDifferentRunners. Both backends on this network
// publish a field called `prompt_n` and neither means quite the same thing by it,
// which is the kind of fact that only shows up later as a prefill_tok_s that is a
// hundred times too large on exactly the warm-cache rows.
//
// These fixtures are not invented. They are the live payloads of two real calls on
// 2026-11-04, taken from /rooter/events and the engine's own accounting.
func TestPromptNMeansDifferentThingsOnDifferentRunners(t *testing.T) {
	t.Run("sushi counts the cached prefix inside prompt_n", func(t *testing.T) {
		// Live: prompt 49161, of which 48726 were already in the KV cache, evaluated
		// in 1923.964 ms. The runner states 226.096 tok/s, which only 435 tokens over
		// that time can produce.
		c := recordFor(t, `{"usage":{"prompt_tokens":49161,"completion_tokens":466},
			"timings":{"prompt_n":49161,"cached_n":48726,"prompt_ms":1923.964,
				"prompt_per_second":226.096,"predicted_n":466,"predicted_ms":10886.543,
				"predicted_per_second":42.805}}`)
		if got, want := *c.PromptTokens, 49161; got != want {
			t.Fatalf("prompt_tokens = %d, want the prompt length %d", got, want)
		}
		if got, want := *c.PromptTokensComputed, 435; got != want {
			t.Fatalf("prompt_tokens_computed = %d, want the 435 tokens actually evaluated;"+
				" %d would price a 25552 tok/s prefill that this call did not achieve", got, *c.PromptTokens)
		}
		if !c.RunnerPromptNIncludesCache {
			t.Fatal("the normalisation is not recorded on the row")
		}
		if c.PrefillTokS == nil || *c.PrefillTokS > 227 || *c.PrefillTokS < 225 {
			t.Fatalf("prefill_tok_s = %v, want the ~226 the runner itself reported", c.PrefillTokS)
		}
		if c.CacheHitRate == nil || *c.CacheHitRate < 0.99 || *c.CacheHitRate > 1 {
			t.Fatalf("cache_hit_rate = %v, want ~0.991", c.CacheHitRate)
		}
		if !hasExtraFlag(c, "runner_prompt_n_includes_cache") {
			t.Fatal("the normalisation should be visible in `extra` too, for whichever surface is read")
		}
	})

	t.Run("strata reports only the work done", func(t *testing.T) {
		// Live: 44 tokens evaluated out of a 95589 token prompt in 560.5 ms, and the
		// runner states 78.5 tok/s, which is 44/0.5605 exactly.
		c := recordFor(t, `{"usage":{"prompt_tokens":95589,"completion_tokens":810},
			"timings":{"prompt_n":44,"cache_n":95545,"prompt_ms":560.5,
				"prompt_per_second":78.5,"predicted_n":810,"predicted_ms":12101.4,
				"predicted_per_second":66.9}}`)
		if got, want := *c.PromptTokensComputed, 44; got != want {
			t.Fatalf("prompt_tokens_computed = %d, want %d: this runner already excluded the cache", got, want)
		}
		if c.RunnerPromptNIncludesCache {
			t.Fatal("rooter must not subtract a cache this runner already subtracted")
		}
		if got, want := *c.PromptTokens, 95589; got != want {
			t.Fatalf("prompt_tokens = %d, want %d", got, want)
		}
	})

	t.Run("a plain /completion with no usage block still reconstructs the prompt", func(t *testing.T) {
		// Work convention, no usage block: 44 evaluated + 95545 cached = 95589 prompt.
		c := recordFor(t, `{"timings":{"prompt_n":44,"cache_n":95545,"prompt_ms":560.5,
			"prompt_per_second":78.5,"predicted_n":810,"predicted_ms":12101.4}}`)
		if got, want := *c.PromptTokens, 95589; got != want {
			t.Fatalf("prompt_tokens = %d, want 95589 reconstructed from work + cache", got)
		}
		if !c.PromptTokensDerived {
			t.Fatal("a reconstructed total must be labelled as reconstructed")
		}
		// Inflated convention, no usage block: prompt_n is already the whole prompt.
		c = recordFor(t, `{"timings":{"prompt_n":49161,"cached_n":48726,"prompt_ms":1923.964,
			"prompt_per_second":226.096,"predicted_n":466,"predicted_ms":10886.543}}`)
		if got, want := *c.PromptTokens, 49161; got != want {
			t.Fatalf("prompt_tokens = %d, want 49161: this runner's prompt_n WAS the prompt,"+
				" adding the cache back would invent a 97887 token prompt", got)
		}
		if got, want := *c.PromptTokensComputed, 435; got != want {
			t.Fatalf("prompt_tokens_computed = %d, want 435", got)
		}
	})

	t.Run("a cold prompt is the same under both conventions", func(t *testing.T) {
		// With nothing cached the two conventions agree, and rooter must not invent a
		// subtraction of zero that flips the audit flag.
		c := recordFor(t, `{"usage":{"prompt_tokens":93857,"completion_tokens":1689},
			"timings":{"prompt_n":93857,"cache_n":0,"prompt_ms":62011.7,
				"prompt_per_second":1513.5,"predicted_n":1689,"predicted_ms":27010.5}}`)
		if got, want := *c.PromptTokensComputed, 93857; got != want {
			t.Fatalf("prompt_tokens_computed = %d, want %d", got, want)
		}
		if c.RunnerPromptNIncludesCache {
			t.Fatal("nothing was cached, so there is no convention to detect")
		}
	})
}

func recordFor(t *testing.T, body string) *callRecord {
	t.Helper()
	c := &callRecord{}
	c.absorbJSON([]byte(body))
	if c.PromptTokens == nil || c.PromptTokensComputed == nil {
		t.Fatalf("fixture did not produce prompt numbers: %+v", c)
	}
	return c
}

func hasExtraFlag(c *callRecord, key string) bool {
	var m map[string]any
	if err := json.Unmarshal(c.Extra, &m); err != nil {
		return false
	}
	v, ok := m[key]
	return ok && v == true
}
