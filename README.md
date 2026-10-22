# rooter

A play on the word "router"...

`rooter` is a tiny transparent HTTP proxy that sits in front of a couple of
local `llama.cpp`-style servers and routes agent traffic between them. It is
transparent: after it has decided *which* backend a request belongs on, it
copies your request bytes to that backend and copies the response straight back,
including SSE token streams. It does not rewrite messages, does not touch the
sampling parameters, and does not need an API key of its own.

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
  rebinds elsewhere.
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
- **Concurrency is a real limit** — `-backend ...,concurrency=1` means one
  request in flight to that backend, full stop. Good for a box that hangs when
  you overload it.
- **Retries** — if a backend dies *before* sending a single response byte, the
  request is replayed onto another one. Once bytes have reached the client it
  cannot retry, so it lets the failure through.
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
    -backend 'strata=http://192.168.11.128:8088/v1,token=Password1,concurrency=4' \
    -backend 'sushi=http://192.168.137.111:8088/v1,token=Password1,concurrency=1'
```

A `-backend` flag per upstream. Spec keys: `name`, `url` (or `base`/`endpoint`),
`token` (or `api-key`), `concurrency` (or `slots`). The name defaults to the
URL's hostname, so `-backend http://host:8088/v1` is legal. `token` is sent as
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
| `-scan-window` / `-max-body` | `1MiB` / `64MiB` | how much of a body is buffered for classification and replay |
| `-xff` | off | let `X-Forwarded-For` be added upstream |
| `-verbose` / `-quiet` | | log routing detail / log only failures |

### Looking at it

Everything under `/rooter/` belongs to rooter; every other path is proxied.

- `GET /rooter` — index
- `GET /rooter/stats` — JSON: per-backend health/busy/slots/models, queue and
  status counters, session hit/miss/rebind totals
- `GET /rooter/backends`, `GET /rooter/sessions` — the same, split up
- `GET /rooter/metrics` — Prometheus gauges and counters
- `GET /rooter/forget?key=p:abc123` — drop one session, or all of them with
  `key=*` (use it after you restart a backend and its cache is gone)

## Pointing Pi at it

`~/.pi/agent/models.json`, on the provider you already use:

```json
{
  "id": "qwen3.8-flash-next",
  "name": "Qwen 3.8 Flash Next (rooter)",
  "baseUrl": "http://127.0.0.1:8089/v1",
  "apiKey": "Password1",
  "api": "openai-completions",
  "compat": { "sendSessionAffinityHeaders": true }
}
```

The `compat` line is optional — rooter falls back to prefix hashing if Pi says
nothing. It is still worth setting, because an explicit session id keeps a
session pinned even when you compact or branch the conversation.

Mind that rooter has **no auth** of its own: bind it to `127.0.0.1` (or a
cluster-internal address) and keep the backend tokens out of the listener.
