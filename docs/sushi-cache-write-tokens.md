# sushi should emit `usage.prompt_tokens_details.cache_write_tokens`

**Status: prepared, not applied.** This is a spec with measured grounding, left here for Ed. sushi's
source is not in the fleet workspace — what exists is the release bundle
`/Users/edwardbeech/sushi-macos-arm64/` (`sushi` 17,903,936 bytes, `sushi 1.5.1`, `mlx 0.32.3`,
`mlx-c 56b2d39fc831`, `sushibar`, `guest.json`, `lib/*.dylib`) — and the upstream the binary reports
is **https://github.com/beamivalice/sushi** (that URL is embedded in the binary's strings), so the
edit belongs upstream, not in a patch written from a binary. What is written below is the exact field
path and the exact source of the value, stated against the response object sushi actually emits,
which is the part both of us can verify. I have changed nothing on the box except the prefix-cache
sizing Ed approved. Written 2026-11-27 11:45Z (19:45 AWST), updated 13:15Z after the restart landed.

Evidence that the field does not already exist: `strings` over the binary matches `cache_write` in
the *metrics* names (`prefix_cache_write_bytes_total`, `prefix_cache_write_failed_bytes_total`) and
nowhere in a usage or chunk serialiser. The usage serialiser's literal is
`"usage":{"prompt_tokens":,"prompt_tokens_details":{"cached_tokens":,"completion_tokens_details":{`
— exactly one count inside `prompt_tokens_details`, which is where this one goes.

## The gap, and who is currently papering over it

`pi` prices cache against `cache_write_tokens`. sushi does not emit it, so rooter has been
backfilling it: as of `6181da7` (Woodpecker 23, live on home-dev) rooter injects
`usage.prompt_tokens_details.cache_write_tokens` equal to its own `prompt_tokens_computed`, for
non-streaming bodies only, and only when the runner did not already report the field.

That backfill has hard limits, and they are why the real fix belongs on sushi:

- it is rooter's arithmetic, not the runner's measurement;
- it covers held (non-streamed) bodies only. Streaming responses pass through untouched, because
  reassembling SSE on the path the user's tokens travel in order to publish a statistic was
  refused deliberately;
- it never overwrites a value the runner supplied, so the moment sushi emits the field rooter's
  injection stops applying to sushi automatically. No rooter change is needed after this.

## What sushi already emits (live probe, sushi 1.5.1, 2026-11-27 11:36Z)

Non-streaming `/v1/chat/completions`, `max_tokens: 2`, cold prefix:

```json
"usage": {"prompt_tokens":12,"completion_tokens":2,"total_tokens":14,
          "prompt_tokens_details":{"cached_tokens":0},
          "completion_tokens_details":{"reasoning_tokens":2}},
"timings":{"prompt_n":12,"cached_n":0,"prompt_ms":179.946,"prompt_per_second":66.687,
           "predicted_n":2,"predicted_ms":0.0,"tokenize_ms":0.614}
```

Same request a moment later, warm, as the final chunk of a stream with
`stream_options.include_usage` set:

```json
{"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[],
 "usage":{"prompt_tokens":12,"completion_tokens":2,"total_tokens":14,
          "prompt_tokens_details":{"cached_tokens":11},
          "completion_tokens_details":{"reasoning_tokens":2}},
 "timings":{"prompt_n":12,"cached_n":11,"prompt_ms":30.672,"prompt_per_second":32.603,...}}
```

So sushi already knows both numbers it needs, per request, in the object it already writes: it has
`prompt_n` and `cached_n`, and it already has a `prompt_tokens_details` object to put a third count
beside `cached_tokens`.

## The change

Field path: **`usage.prompt_tokens_details.cache_write_tokens`**, an integer, sitting beside
`cached_tokens`, in both places the usage object appears — the non-streaming response body, and the
final `chat.completion.chunk` (the one with `choices: []`) for streamed requests.

Shape of the edit, in the response object rather than in source:

```diff
 "usage": {
   "prompt_tokens": 41840,
   "completion_tokens": 214,
   "total_tokens": 42054,
   "prompt_tokens_details": {
     "cached_tokens": 38912,
+    "cache_write_tokens": 2928,
     "cache_write_bytes": 39168,
   },
   "completion_tokens_details": { "reasoning_tokens": 214 }
 },
 "timings": { "prompt_n": 41840, "cached_n": 38912, "prompt_ms": 12102.4, ... }
```

Value, from sushi's own per-request numbers, with no new bookkeeping:

```
cache_write_tokens = prompt_n - cached_n
```

which is exactly what rooter currently publishes as `prompt_tokens_computed` and what it currently
back-fills into `cache_write_tokens`. If the two ever disagree, sushi's number is the one that
should win, because sushi measured it.

`cache_write_bytes` above is shown as an *optional* extra, not part of the ask: sushi already sums
it globally as `sushi:prefix_cache_write_bytes_total{kind="kv"|"ssm"|"qsa"|"spec"|"ring"|"meta"}`,
so a per-request version would be useful for the fleet's wear accounting but is a second decision.

### The semantics decision Ed should make explicitly

`prompt_n - cached_n` means **tokens this request had to compute**. Anthropic's
`cache_write_tokens` means **tokens newly written into the prompt cache**. For a cache-eligible
request those are the same number, and "computed" is the one that keeps continuity with everything
rooter already publishes — so default to computed.

They differ when sushi declines to cache something (a prompt below its minimum, or an entry dropped
before it lands). sushi can already tell the two apart: `sushi:prefix_cache_write_failed_bytes_total`
is 0 today, meaning nothing it decided to write was dropped. If Ed prefers the strict
"committed-to-cache" meaning, that is defensible, but then `cache_write_tokens` becomes smaller than
computed tokens on short or rejected prompts, and every consumer pricing off it — including
rooter's own `/rooter/events` `prompt_tokens_computed`, which is pinned by test — has to be told
which meaning it is getting. Say it in the release note either way.

## What a miss actually costs, so the field is worth a release

Measured on sushi, 2026-11-27, from rooter's load accounting (`/rooter/backends`) and sushi's own
`/metrics`:

- sushi cold-prefill EWMA **242 tok/s** (samples gated at `computed >= 2000`; small prompts are
  excluded because fixed overhead dominates — a 12-token cold prefill measured 179.9 ms, i.e. 66.7
  tok/s, which is not a rate worth quoting at any window size);
- sushi hot-restore EWMA **543,806 tok/s** — a warm prefix is roughly four orders of magnitude
  cheaper to have than to recompute, which is the entire reason the field matters;
- a warm session whose prefix is evicted pays its whole window again: **179–248 s** for the 43–60k
  token prompts that post-compaction sessions actually carry at 242 tok/s;
- capacity-eviction misses measured before the restart cost **75–78 s** of prefill each, on a box
  with 16 days 10 hours of uptime. Those were capacity evictions, not restart cost, and that
  attribution stands. After the restart (12:07:08Z) a cold prefill of a 33,761-token prefix took
  **74 s** of wall time, about 456 tok/s including whatever it queued behind — so the 242 tok/s EWMA
  is conservative in the direction a deadline needs;
- prefix-cache tier reality behind those misses, at 11:37Z: hot restores 10,340 against hot
  evictions 1,298; SSD restores **66** against SSD evictions **3,791**; `disk_used_bytes`
  4,201,208,903 against a 4,294,967,296 budget, i.e. 97.8% full. The SSD tier is writing a great
  deal and almost never paying for it.

## Wear note, because the numbers look alarming next to each other

`sushi:prefix_cache_write_bytes_total` sums to **4.40 TB written since Nov 11** (kv 2.20 TB, ssm
1.18 TB, qsa 0.63 TB, spec 0.40 TB, meta 0.0009 TB, ring 0) against 72.4 GB free on the Data volume
holding `~/.sushi/kv-cache`. Roughly 275 GB/day of writes on the volume.

Emitting a field changes none of that: it reports work sushi already does.

### Measured, not assumed: the sizing change is applied and the write rate went up

The sizing Ed approved (entry cap 4 → 12, SSD budget 4 GiB → 8 GiB, `mem_bytes` unchanged) went live
at 12:07:08Z. Over the first 63 minutes on the new settings:

```
hot restores 143 / hot evictions 6      (before: 10,365 / 1,308 over 16.4 d — ratio 7.9 → 23.8)
SSD restores   2 / SSD evictions 18     (before:     66 / 3,799            — ratio 0.017 → 0.11)
requests 152 in 63 min (143/h against 28.4/h before), query-level hit rate 93.6% (was 92.9%)
disk_used oscillating 5.24 – 8.56 GB against the new 8 GiB ceiling, above the old 4.29 GiB budget
   from ~15 minutes in
disk_written 31,069,016,691 in 63 min = 29.4 GB/h = ~706 GB/day  (was ~268 GB/day)
```

So the honest sentence for the wear question is: **the SSD tier writes roughly 2.6× faster than it
did before the sizing change**, `~/.sushi/kv-cache` is now 11 GB, and `/System/Volumes/Data` reads
832 GiB used of 926 GiB — 57 GiB available, 94% full, against 72.4 GB free before. The bigger budget
is a claim on volume space, and the write-rate increase is its price. What the extra budget has
bought is not yet a rate: of the 2 SSD restores in that hour one was my own probe, so the fleet
number for the first hour is **one SSD restore**, and the judgement belongs on tomorrow's counters
(`sushi:prefix_cache_tier_hits_total{tier="ssd"}` against the `{tier="ssd"}` evictions over a
comparable traffic day). Full accounting, including the restart gate and the window chosen, is in
`docs/sushi-prefix-cache-applied-20261127.md`.

## What rooters side of this needs, if sushi emits it

Nothing. `6181da7` refuses to overwrite a runner-supplied `cache_write_tokens`, so sushi emitting
the field retires the backfill for sushi on its own. Rooter will keep injecting it for backends
that still do not emit it, and pi's own rule (treat the field as present-or-unknown, never
absent-or-zero) stays correct under both.
