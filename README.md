# rooter

A play on the word "router"...

`rooter` is a tiny transparent HTTP proxy that sits in front of a couple of
local `llama.cpp`-style servers and routes agent traffic between them. It is
transparent: after it has decided *which* backend a request belongs on, it
copies your request bytes to that backend and copies the response straight back,
including SSE token streams. It does not rewrite messages and does not touch the
sampling parameters.

It can guard its own front door with `-api-key`, which is what lets it sit on a
routable address instead of `127.0.0.1`. That key is rooter's credential only: it
is checked here and consumed here, and the backends get the tokens configured on
their `-backend` specs. See [Front-door API key](#front-door-api-key).

The point is **session stickiness for prefix caching**. A long agent session
sends the same system prompt, the same tool definitions and a growing
conversation on every turn. If turn 7 lands on a different server than turn 6,
that whole prefix has to be re-evaluated. So rooter pins a session to the
backend it is already warm on, and only spreads new sessions across backends
that have free capacity.

## Behaviour

- **New session** — pick the least-loaded backend that has a free concurrency
  slot. If every eligible backend is full, *block* until a slot frees rather
  than bouncing the session onto a cold cache.
- **Existing session** — go back to the same backend, and block if its slots are
  full. Bindings are per *(session, model)*, so one conversation that switches
  between models keeps a warm cache on each backend instead of ping-ponging a
  single binding. A session whose sticky backend is unhealthy waits for
  `-sticky-grace` (20s by default) so a blip does not shred its cache, then
  rebinds elsewhere. `-spill-after` is the same idea for a backend that is not
  broken, only jammed: hold the cache that long, then move the session to a
  backend with a free slot (logged as `spill`). It only fires when another
  *eligible* backend exists, which for an explicit model id means an alias both
  boxes advertise, or a meta model.
- **Model aware** — rooter reads each backend's `/models` and will only send a
  request for `model-X` to a backend that claims it. **The model filter outranks
  stickiness**: a session bound to a backend that does not serve the model it now
  wants is moved, not held. This matters because llama.cpp answers a model it has
  not loaded with the model it *has* — sticking here would be a silent model swap
  that reads like a bad answer. A `:quant` tag on the requested id
  (`repo/GGUF:UD-Q6_K_XL`) is folded away, since `/models` reports the base id.
  If nobody claims the model the request is a `404`; while any backend's list is
  still unknown or empty it does not constrain the choice, so llama.cpp's
  `default` and a cold router keep working.
- **`auto` is the meta model** — the model names `auto` and `default` are not a
  claim about weights, they mean "give me a GPU": the model filter is skipped,
  a fresh session goes to the freest healthy backend, and it is then sticky like
  any other session (bindings stay per *(session, model)*, so `auto` and an
  explicit model id are tracked separately). Add more names with `-meta-model`.
  This is how you use rooter as a balancer instead of a front door. It matters most
  when the boxes hold different files - `auto` pools them, an explicit id pins you to
  whichever boxes claim it - and it is still what you want when both boxes hold the
  same file, which is the case today: `auto` and `qwen3.8-flash-next-iq3_xxs` both
  pool strata and icarus, and a deliberate pin is a backend name (`strata`,
  `icarus`). What you give up either way is the right to be surprised: llama.cpp
  serves whatever it has loaded and echoes back the name you asked for, so an `auto`
  answer can come from a different box - and, if the boxes ever hold different quants
  again, a different quant - on different turns, and only rooter's log or
  `rooter_backend_requests_total` says which.
- **Concurrency is a real limit** — `-backend ...,concurrency=1` means one
  request in flight to that backend, full stop. Good for a box that hangs when
  you overload it.
- **Retries** — if a backend dies *before* sending a single response byte, the
  request is replayed onto another one. Once bytes have reached the client it
  cannot retry, so it lets the failure through.
- **Front-door auth is optional** — with `-api-key`, every request must present
  that key or it is a `401` before a backend is chosen, a slot is taken or a body
  is read. Without it rooter is open to whoever can reach the socket. See
  [Front-door API key](#front-door-api-key).
- **Cheap reads don't take slots** — `GET`s and a few suffixes (`/models`,
  `/health`, `/tokenize`, `/detokenize`, `/apply-template`, `/props`,
  `/version`, `/metrics`; see `-free-path`) pass through without occupying a
  slot, so a busy backend cannot lock you out of metadata.
- When everything is full past `-max-wait`, you get a `503` with
  `{"error":{"type":"rooter_error","message":"all backends busy..."}}` and a
  `Retry-After` header. `-max-wait 0` (the default) waits indefinitely.

### How a "session" is identified

In order of preference:

1. A session header on the request: `x-session-id`, `x-session-affinity`,
   `x-pi-session-id`, `x-conversation-id`, `x-thread-id`, `session-id`,
   `session_id`, `x-openrouter-session-id`, `x-client-session-id`. Pi only sends
   the first two if the model's `compat.sendSessionAffinityHeaders` is on.
2. A session-ish field in the JSON body, in this order of preference:
   `prompt_cache_key`, `session_id`, `cache_key`, `user`, `metadata.user_id`,
   `id`.
3. **The fallback that makes this work with clients that know nothing about
   rooter**: a hash of the conversation *prefix* — the requested model, the
   system prompt and the first user turn (skipping over image parts so base64
   blobs do not matter). That is identical on every turn of a session and
   different between sessions, so stickiness just works. It reads both the
   chat-completions shape (`messages`) and the Responses shape (`instructions`
   plus `input`, as a bare string or an item array), so `api: openai-responses`
   in Pi sticks too.

Only the first `-scan-window` bytes (1 MiB) are read to work this out; bigger
bodies are streamed through and, because they cannot be replayed, are not
retried onto another backend.

The key is then paired with the requested model, because a session that uses two
models on two backends should stay warm on both.

## Building

```shell
go build -o rooter .
go test -race ./...
```

## Usage

```shell
./rooter \
    -listen :8089 \
    -api-key Password1 \
    -backend 'strata=http://192.168.11.128:8088/v1,token=Password1,concurrency=4' \
    -backend 'icarus=http://192.168.137.111:8088/v1,token=Password1,concurrency=4' \
    -renamed 'sushi=icarus'
```

That is the fleet today: two boxes, one runtime (llama.cpp), and the same model id
published by both - `qwen3.8-flash-next-iq3_xxs` at 131072 ctx on each. There is
therefore one model in the catalog naming two machines, and naming a box is the only
way to ask for a particular one. The `-renamed` line is about the name that used to
be there; see "Renaming a backend".

A `-backend` flag per upstream. Spec keys: `name`, `url` (or `base`/`endpoint`),
`token` (or `api-key`), `concurrency` (or `slots`). The name defaults to the
URL's hostname, so `-backend http://host:8088/v1` is legal. `api-key` *inside* a
backend spec is that upstream's token; the standalone `-api-key` flag guards
rooter's own front door. Two different strings, two different directions. `token` is sent as
`Authorization: Bearer <token>`, replacing whatever the client sent; leave it
out and the client's `Authorization` passes through untouched. Escape `,` and
`=` in a value as `\,` and `\=`.

Useful flags:

| flag | default | meaning |
|---|---|---|
| `-session-ttl` | `6h` | forget a session after this long idle |
| `-max-sessions` | `20000` | cap on tracked sessions (LRU evicted) |
| `-max-wait` | `0` | how long to block for a slot before 503 (0 = forever) |
| `-sticky-grace` | `20s` | keep waiting on an unusable sticky backend before rebinding |
| `-retries` / `-retry-wait` | `1` / `5s` | extra attempts on other backends |
| `-health-interval` | `15s` | how often to re-poll `/models` (0 = start only) |
| `-request-timeout` | `0` | hard cap on a generation request, queue included |
| `-meta-model` | `auto`, `default` built in | extra names that mean "whichever backend is freest" |
| `-renamed` | none | a retired backend name that should still resolve, as `OLD=NEW` (repeatable) |
| `-spill-after` | `0` | after waiting this long on a busy sticky backend, take another eligible one (0 = never) |
| `-scan-window` / `-max-body` | `1MiB` / `64MiB` | how much of a body is buffered for classification and replay |
| `-api-key` | none | key clients must present; repeat for more than one; empty = open front door |
| `-public-path` | `/rooter/healthz`, `/rooter/health` | path suffix reachable without `-api-key` |
| `-xff` | off | let `X-Forwarded-For` be added upstream |
| `-verbose` / `-quiet` | | log routing detail / log only failures |

### Renaming a backend

A backend name outlives the box it was attached to. Clients write it into
`models.json`, Pi saves it as a `/model` choice, and rooter's session bindings hold
one, so when a box is renamed or replaced every one of those keeps asking for a name
that no longer means anything - and a router that has simply forgotten it answers
them with a 404, which reads as an outage and is really a rename.

`-renamed 'sushi=icarus'` says what the old name means now:

* a request with `"model": "sushi"` pins to icarus exactly as a request for `icarus`
  would, and goes upstream under the model icarus actually loaded;
* a session binding written under the old name still finds a box, and is rewritten to
  the live name on the way through, so the retired name is transitional rather than
  something the router has to keep understanding;
* `/v1/models` still advertises it, marked `"alias_of": "icarus"`, so a client can see
  from the list itself that it is a redirect, and to where;
* every response names the box that really answered (`X-Rooter-Backend: icarus`) and
  the call record keeps `model_requested: "sushi"`, so the old name stays auditable
  instead of disappearing into a new one;
* a retired name is the *weakest* claim there is: if some box genuinely serves a model
  with that id, the request is routed by inventory and the old name stops being a
  pin. Renaming a box must not let it hijack a model that happens to share its name.

`-renamed` is checked against the backends that exist, because a retired name that
resolves to nothing is indistinguishable from a name nobody configured: it refuses an
unknown target, a source that is a live backend name, and one name renamed to two
boxes. Rooter keeps no fossil box names in its source - **removing the flag is what
finally retires a name.**

### Front-door API key

`-api-key Password1` (repeatable, one accepted key per flag) makes rooter answer
`401` to anything that cannot present it:

```shell
curl -s localhost:8089/v1/chat/completions -d '{"model":"auto","messages":[]}'
# {"error":{"type":"authentication_error","code":"invalid_api_key","message":"missing API key: ..."}}

curl -s localhost:8089/v1/models -H 'Authorization: Bearer Password1'
curl -s localhost:8089/rooter/stats -H 'x-api-key: Password1'
```

The key is accepted as `Authorization: Bearer <key>`, a bare `Authorization:
<key>`, or `x-api-key: <key>` — every spelling an OpenAI-ish client uses in
practice. Comparison is constant time, so a scanner cannot walk the prefix of a
valid key one character at a time.

What it does and does not do:

- **It is a gate, not routing.** The check runs first, before a slot is taken or
  the body is read, so a rejected request costs nothing but a syscall. Denials are
  counted in `/rooter/stats` (`denied`) and `rooter_denied_total`, and each client
  gets one log line per 30s with the rest summarised, so an internet-facing port
  being scanned cannot bury the routing log that is the reason the log exists.
- **The credential stops at rooter.** With `-api-key` set, the client's
  `Authorization`/`x-api-key` is deleted before proxying; the backend then gets
  whatever `token=` its spec carries, or nothing if it has no token. A caller's
  rooter key never reaches a box it has no business on.
- **Health probes stay open.** `-public-path` (default `/rooter/healthz`,
  `/rooter/health`) needs no key, because kubelet presents nothing and a probe
  that 401s would restart the pod and wipe the session table. It matches suffixes
  inside the `/rooter` namespace by default, which is why the defaults are spelled
  out in full and not as bare `/health`. Everything else — `/v1/*`,
  `/rooter/sessions` (every conversation id on the network), `/rooter/forget` (a
  way to shred every warm cache) — needs the key.
- **Repeat it for more than one accepted key**, one per person or device, so a
  leaked one can be dropped without locking everybody else out. Roster, rotation
  and per-user accounting are not rooter's job; if you want them, put something in
  front that does them and leave this as the coarse gate.

With no `-api-key` the front door is open, and rooter says so at startup: an
address that is not loopback logs a `WARNING`. That is fine on a LAN you own and
is the historical default; it is not fine on a port somebody else can route to.

### Looking at it

Everything under `/rooter/` belongs to rooter; every other path is proxied. If
`-api-key` is set, pass it on these too (except the health probes).

- `GET /rooter` — index, including whether the front door is keyed
- `GET /rooter/stats` — JSON: per-backend health/busy/slots/models, queue,
  denial and status counters, session hit/miss/rebind totals
- `GET /rooter/backends`, `GET /rooter/sessions` — the same, split up
- `GET /rooter/metrics` — Prometheus gauges and counters, plus per-`(backend,
  model)` families: `rooter_model_duration_seconds`, `_ttft_seconds`,
  `_prefill_seconds`, `_decode_seconds` histograms, `_requests_total`,
  `_errors_total`, `_aborted_total`, `_tokens_total{kind=prompt|completion|cached}`
  counters and a `_cache_hit_ratio` gauge. Labels are always exactly `backend` and
  `model`; session keys are deliberately never a label, because a per-session
  cardinality on a histogram would blow up every series.
- `GET /rooter/events?cursor=<seq>&limit=<n>` — the finished calls the ring still
  holds, oldest first, as the same records the log line and the metrics are built
  from. The ring is bounded (`-event-ring`, default 2048) and drops the oldest half
  at a time. Store the `cursor` you are handed back and ask for everything after it;
  `id` is the dedup key and the cursor is only a position, so a poll that changed
  nothing returns an empty page and the same cursor. `stale` means the ring no
  longer holds what your cursor pointed at, judged against the oldest surviving
  record rather than against an empty page - a consumer that is perfectly caught up
  also gets an empty page, and calling that data loss would be a flag that trains
  you to ignore it. Ids are `<boot id>/<seq>`, so they cannot be re-issued across a
  restart - which is exactly when a client retries; a changed `boot_id` means your
  cursor is from a life that no longer exists. On a stream each record carries both
  `ttft_ms` and `first_content_ms`; they are different quantities and both are kept,
  for the reason in the measurement note below.
- `GET /rooter/forget?key=p:abc123` — drop a session and every per-model binding
  it holds; a bare id works too (`h:`/`p:` are tried for you), and `key=*` clears
  everything (use it after you restart a backend and its cache is gone)

### What rooter adds to a response

A `model` of `auto` comes back saying `auto`, which tells a client nothing about
what answered. Rooter fixes that on two surfaces, because they are needed at
different times:

- `X-Rooter-Request-Id`, `-Backend`, `-Model`, `-Route`, `-Wait-Ms`,
  `-Streamed` on every proxied response. These are settled before a byte moves, so
  a client rendering a footer while the tokens are still arriving can have them
  now. `X-Rooter-Model` is the *routing decision*: for `auto` it is `auto`, because
  rooter genuinely does not know which model the box will pick until it answers.
- `X-Rooter-Ttft-Ms`, `X-Rooter-Completion-Tokens`, `X-Rooter-Prefill-Tok-S`,
  `X-Rooter-Decode-Tok-S`, `X-Rooter-Cache-Hit-Rate` and a `rooter` object, on
  **non-streaming JSON bodies only**.
  A held body is the one case where a header can still carry a number measured
  during the response, and it is the one body rooter can safely extend. The object
  carries the runner's own per-token numbers, normalised: both boxes report
  timings, and llama.cpp builds have disagreed on the cache field's name (`cache_n`
  versus `cached_n`), so those fold into `cached_tokens`, rates are derived when a
  runner gave counts and a duration but no throughput, and the runner's raw `timings`
  is kept verbatim under `extra` so anything rooter does not model is still
  recoverable.

`prompt_tokens` is the prompt length. `prompt_tokens_computed` is what the runner
actually evaluated: the prompt minus whatever it served from the KV cache.
Confusing the two makes a cache hit look like a tiny prompt, pushes `cache_hit_rate`
above 1, and prices a warm prefill at a speed nobody achieved.

**`prompt_n` does not mean the same thing on every llama.cpp build.** These are two
live payloads of the same field name, taken from `/rooter/events` on 2026-11-04, when
the fleet was two different builds:

| backend   | `prompt_n` | cached   | `prompt_ms` | states `prompt_per_second` | work done |
|-----------|-----------:|---------:|------------:|---------------------------:|----------:|
| strata    |         44 |   95,545 |       560.5 |                       78.5 |        44 |
| sushi     |     49,161 |   48,726 |    1,923.96 |                  226.096 |       435 |

`44 / 0.5605s` is 78.5, so strata's `prompt_n` is already the work done. For sushi,
`49161 / 1.924s` is 25,552 tok/s — nothing it ever said — while `435 / 1.924s` is
226.096 to the digit it printed. Sushi's `prompt_n` was the **prompt length with the
cached prefix still inside it**.

Only the first of those two boxes is still in the fleet, and today both agree:
measured on 2026-11-09 each reported `cache_n` and the work convention (`prompt_n=7`,
`cache_n=9`, `prompt_per_second` reproducing `7 / 0.0989s` on one, `prompt_n=16`,
`cache_n=0` on the other). The normalisation stays, and that is a decision rather
than tidiness:

* it selects on the **runner's own arithmetic**, never on a backend name, so a
  well-behaved box pays nothing for it and a box that changes its mind is caught on
  the first response instead of in a dashboard a week later;
* removing it would take `prompt_tokens_computed`, the reconstruction of
  `prompt_tokens` and the refusal to correct a contradictory `cache_hit_rate` with
  it, which is most of what makes these numbers safe to price against;
* both conventions are still exercised, by fixtures named for the convention rather
  than for a machine that no longer exists.

Neither build was wrong and neither was told about the other, so rooter decides which
convention it is dealing with from the runner's own arithmetic: it publishes a
throughput, and only one of `prompt_n / prompt_ms` or
`(prompt_n - cached) / prompt_ms` reproduces it. Rooter divides the same way the
runner did, which is the only way two boxes can share a column. When it subtracts,
the row carries `runner_prompt_n_includes_cache: true` — and the same key in `extra`
— because a normalisation a reader cannot see is a number they are forced to trust. If
you want the runner's raw figure back, add `cached_tokens` on.

They are not two spellings of one number, and `prompt_tokens` is not always a
measurement. It is read from `usage.prompt_tokens` when the runner sends a usage
block. When it does not - which is what a plain llama.cpp `/completion` does -
rooter reconstructs it from the work done plus the cache, or takes it straight from
`prompt_n` when that was already the full prompt, and marks the row
`prompt_tokens_derived: true`. Both are the full prompt length by construction,
and `cache_hit_rate` is refused rather than corrected when the counts contradict
each other, so either way it is a safe denominator; the flag exists because a
reader who cannot tell a read value from a reconstructed one is trusting arithmetic
they cannot see.

**A stream is never held back and never edited.** Those bytes are the user's
tokens arriving, and buffering them to be clever about metadata is a terrible
trade, so rooter reads a stream's tail and passes it through untouched. That makes
the usage and timing fields `null` on an abandoned call rather than zero, which is
the honest answer: a generation the client hung up on did not use no tokens, it
reported none.

**`ttft_ms` means time to the first upstream byte, not time to the first word.** On a
held body those are the same instant. On a stream they are not, and the gap is not
small: llama.cpp opens every stream with a chunk carrying `content: ""`, so rooter
sees a first byte 40-60ms after the upstream answers while the user waits seconds for
language - measured live at 0.00s for the empty delta and 6.09s for the first token on
one busy box, because llama.cpp queues generations internally after its HTTP response
has already begun streaming. Anything graphing `ttft_ms` from `/rooter/events` is
therefore graphing *how soon the runner started replying*, which is a useful saturation
signal and a misleading responsiveness one. A client that wants time-to-first-word on
a stream has to measure it itself, against the first chunk carrying text.

Rooter measures it, so a client does not have to: `first_content_ms` on a streamed
record is the first chunk carrying words, timed from the same instant as `ttft_ms`, so
subtracting one from the other gives the runner's internal queue. "Words" means
`content`, `reasoning_content` or `text` with a non-empty string value - a thinking
model's first twenty deltas are `reasoning_content`, and treating only `content` as
speech reports the end of the thinking as the first word.

`-no-meta` turns all of this off; `-meta-tail 0` keeps the headers and stops rooter
reading response bodies at all.

## Pointing Pi at it

`~/.pi/agent/models.json` - this is what the deployed `rooter` provider looks
like:

```json
{
  "baseUrl": "https://rooter.dev.initialed85.cc/v1",
  "api": "openai-completions",
  "apiKey": "Password1",
  "compat": {
    "supportsDeveloperRole": true,
    "supportsReasoningEffort": true,
    "sendSessionAffinityHeaders": true,
    "sessionAffinityFormat": "openai"
  },
  "models": [
    {
      "id": "auto",
      "name": "Qwen 3.8 Flash Next (rooter -> either box)",
      "reasoning": true
    },
    {
      "id": "qwen3.8-flash-next-iq3_xxs",
      "name": "Qwen 3.8 Flash Next (rooter -> whichever box is free)",
      "api": "openai-responses"
    },
    {
      "id": "strata",
      "name": "Qwen 3.8 Flash Next (rooter -> strata)"
    },
    {
      "id": "icarus",
      "name": "Qwen 3.8 Flash Next (rooter -> icarus)"
    }
  ]
}
```

`/model rooter/auto` is the interesting one: it pools both boxes, so a new Pi
session lands on whichever has slots free and then stays there for the rest of that
conversation. Since both boxes publish the same id, `qwen3.8-flash-next-iq3_xxs`
pools too - a deliberate pin to one machine is now a request for its name, which is
also what `/v1/models` advertises. Note the id `auto` is not a real model - llama.cpp
will answer it with the weights it has loaded, which is the whole point, and Pi cannot
tell from the response which box replied.

`apiKey` is the **rooter** key, i.e. the value of rooter's `-api-key`. It is not
the backend token and it is not the same string just because both happen to be
`Password1` in this file: change one and the other keeps working, and if they
 disagree you get a `401` from rooter before anything reaches llama.cpp.

`api` can be set per model as well as per provider. Both backends answer
`/v1/chat/completions` and `/v1/responses` (rooter proxies either, and keys
sessions from both shapes), so each model here carries the API it is used to when
reached directly and rooter just sits in front.

Only the fields that matter for routing are spelled out; `contextWindow`,
`maxTokens`, `reasoning`, `thinkingLevelMap` and `input` on each model are copied
from the direct providers unchanged.

`sendSessionAffinityHeaders` is optional - rooter falls back to prefix hashing if
Pi says nothing. It is still worth setting, because an explicit session id keeps a
conversation pinned even when you compact or branch it, whereas the prefix hash
follows the transcript.

Without `-api-key`, rooter has no auth of its own and must be bound to
`127.0.0.1` or a network you trust. With it, rooter can be published: keep the
backend tokens in a Secret, keep the front-door key in a Secret, and treat
`/rooter/healthz` as the only thing a stranger is allowed to see.
