# strata-router

one OpenAI-compatible endpoint in front of several [Strata](https://github.com/Niko1221/Strata) engines, with
**conversation-sticky routing** so each conversation keeps its KV cache on the engine that warmed it.

    clients ──> http://localhost:8000/v1 ──┬──> engine A  (home of conversation 1, 2)
                                          └──> engine B  (home of conversation 3, 4)

Zero dependencies, Node 20+, single file.

## why

a Strata engine has a small number of batch slots (`--batch N`) and a hard aggregate ceiling: on an RTX 3090
with a Q2_0 model at 262k context, one request runs at ~114 tok/s, two at ~63 tok/s each (122 tok/s aggregate),
four at ~31 tok/s each (124 tok/s aggregate). More concurrency does **not** buy throughput, it just divides it.

What does buy throughput is cache: a conversation that stays on the engine that already holds its KV reuses the
prefix cache. A conversation that lands on a different engine pays a cold prefill — at ~1,400 tok/s, a 262k
context takes about 190 s. That single event costs more than ten average agent turns
([measurements](docs/measurements.md)).

So the router does one thing: keep conversations on their home engine, and only spill when that engine is down
or stays busy longer than the cold prefill would take.

## run

```bash
STRATA_BACKENDS="a=10.0.0.1:8080,b=10.0.0.2:8080" node router.mjs
```

| env | default | meaning |
|---|---|---|
| `STRATA_BACKENDS` | `127.0.0.1:8080` | comma-separated `host:port` or `name=host:port` (ipv6 as `[::1]:8080`) |
| `PORT` | `8000` | listen port |
| `BIND` | `0.0.0.0` | listen address |
| `POLL_MS` | `2000` | how often to read `GET /metrics` from each engine |
| `POLL_FAILS` | `3` | failed polls in a row (timeouts, resets) before an engine counts as down |
| `MAX_INFLIGHT` | `2` | per-engine cap; the router uses the smaller of this and the engine's reported batch slots (one when it has none) |
| `MAX_QUEUE_WAIT` | `30000` | how long to wait for a free slot before handing the request to an engine's own queue |
| `PREFILL_TOK_S` | `1400` | cold prefill speed until an engine's own is measured; decides how long a conversation waits for its busy home |
| `CONNECT_TIMEOUT` | `10000` | TCP connect timeout to an engine; a failed connect marks it unhealthy until the next good poll |
| `READ_TIMEOUT` | `300000` | how long an engine may stay silent before the router checks on it (see routing rules) |
| `MAX_CONVERSATIONS` | `500` | size of the conversation map (LRU) |
| `PASSTHROUGH` | all | comma-separated POST paths the router relays to the engine; anything else gets 404 |
| `STATE_FILE` | none | keeps conversation homes across restarts (written every minute and on shutdown) |
| `STRATA_API_KEY_FILE` | none | file with the engines' API key, sent on the router's `/metrics` polls (see below) |
| `SHUTDOWN_GRACE` | `30000` | how long answers in flight get to finish after SIGTERM |
| `LOG` | on | set `LOG=off` to silence per-request lines |

a bad value stops the router at startup with a message naming it, rather than routing strangely later.

## endpoints

| path | behaviour |
|---|---|
| `POST /v1/chat/completions` | routed; SSE streamed untouched; client headers (auth included) passed on; response carries `x-strata-backend` |
| `POST /v1/completions` | same routing, keyed on the prompt prefix |
| `POST /v1/messages` | same routing (Anthropic-style payloads) |
| `POST /v1/responses` | same routing (OpenAI Responses-style) |
| `GET /health` | backend health, in-flight, queued |
| `GET /stats` | counters, per-engine state, conversation homes |
| `GET /dashboard` | live dashboard of engines, conversation homes and routing decisions (see [dashboard](#dashboard)) |
| `GET /stats/events?since=<id>` | routing decisions after event `id` (the last 200 are kept), and the requests still waiting or running |
| `GET /stats/conversations` | every tracked conversation, not just the last 50 |
| `GET /v1/models` (and `/models`) | proxied from the first healthy engine (pi's model list needs this to show a provider as running) |
| any other `GET` (`/metrics`, `/status`, `/slots`, ...) | proxied to the first healthy engine, status and body as the engine sent them |
| `POST` to any other engine path (`/v1/vram`, `SAVE` sessions, ...) | proxied to the first healthy engine; set `PASSTHROUGH` to whitelist paths, since the router listens on all interfaces without auth |

## routing rules

- **conversation key** = hash of `model | system prompt | first user message` (first 4k chars; the prompt prefix
  for `/v1/completions`; the raw content when the opener has no text, e.g. a lone image). Forks and branches of
  a conversation share the key, so they inherit the same home engine and reuse the prefix cache.
- **sticky** while the home engine is healthy, has an empty queue and a free batch slot.
- **healthy** until `POLL_FAILS` polls in a row time out or fail, so an engine too busy prefilling to answer
  `/metrics` quickly keeps its conversations. A refused connection, a non-200 `/metrics` or a request that can't
  connect marks it down at once.
- **patient** when the home is busy: wait for its slot as long as a cold prefill of the conversation would take
  elsewhere (last turn's prompt tokens ÷ the fastest other engine's prefill speed, capped at `MAX_QUEUE_WAIT`),
  and hold the home's next free slot against newcomers meanwhile (for at most 2 s of a newcomer's wait, so short
  side requests can't starve behind it). Prefill speed is measured per engine from
  streamed turns with at least 1,000 uncached prompt tokens, starting from `PREFILL_TOK_S`; without usage in the
  answers, the prompt size is estimated from the request. New conversations don't wait.
- **spill** to the least-loaded engine with a free slot after that, or right away when the home is down; the home
  moves with the conversation.
- **queue** up to `MAX_QUEUE_WAIT` when every engine is full, in arrival order, then hand the request to the home
  engine's own queue.
- **fail fast** with 503 once no engine has been healthy for a poll interval, instead of queueing.
- **retry** on another engine, queueing for it like any request, when the engine never started an answer: it
  refused or timed out the connection, reset it before answering, or answered 5xx. Once a response has started,
  an engine failure cuts the client connection, so a truncated answer shows up as an error rather than a short
  reply. With nowhere to retry, the engine's own error is passed on.
- **silence** is not failure: when an engine says nothing for `READ_TIMEOUT`, the request keeps waiting as long as
  the engine still reports work in `/metrics` (long prefills and non-streamed answers can be silent for minutes).
  It is given up only when the engine is idle or down, and then retried elsewhere if nothing reached the client
  yet (504 otherwise). A request whose answer hasn't reached the client moves as soon as polls give its engine
  up, without sitting out `READ_TIMEOUT`; one already streaming is left to finish or time out.
- **cancel**: a client that disconnects aborts the engine request and frees its slot, pipelined requests included.
- **paths** with a trailing or doubled slash route like the canonical ones.

## what the metrics mean

`GET /stats` reports, per engine:

- `held_tokens` — tokens resident in batch slots (`live.slots[].held_tokens`)
- `parked` — conversations parked in the engine's conversation cache (needs `--conversation-cache-mib`)
- `reused/requests` — engine-side prompt cache hit rate
- `tok_s_mean` — rolling decode speed used for the load score
- `prefill_tok_s` — measured cold prefill speed (empty until a long enough streamed turn ran there)
- `poll_fails` — failed polls in a row
- `capacity` — slots the router fills, the smaller of `MAX_INFLIGHT` and the batch slots; `inflight` passes it after an
  overflow
- `claimed` — conversations holding out for this engine as their busy home
- `conversations` — conversations whose home it is

and globally: `cold` (new conversations, i.e. prefills), `migrated` (conversations whose home moved), `queued`
(requests that waited for a slot), `waiting` (waiting right now), `overflow` (handed to a full engine after
`MAX_QUEUE_WAIT`), `retried`, and
`cached_tokens / prompt_tokens` — the ratio you should actually watch. Usage is read from JSON and SSE answers in
chat, messages and responses shapes. A healthy setup has almost as many cold events as conversations, not as
many as turns.

each conversation also carries `last_seen` (ms since the epoch) and `migrations` (how often its home moved).

## dashboard

open `http://localhost:8000/dashboard`. it is part of `router.mjs`, polls the router every 1.5 s and loads nothing
from anywhere else, so it works offline. it shows

- the cache hit ratio and the requests waiting for a slot right now, above the router's counters
- one card per engine: health, slots in use, its own queue, decode and prefill speed, its conversation cache, and
  the conversations whose home it is, with turns, prompt size, last seen and how often each moved
- routing decisions as they finish, newest first, under the requests still waiting or running; hovering a row
  highlights that conversation everywhere on the page

each decision gets one kind, first match wins: `gone` (the client hung up), `failed`, `retry` (an engine failed
before answering), `overflow`, `spill` (the home moved), `queued` (waited for a slot), `cold` (new conversation),
`sticky`. when the router stops answering, the page says so and dims the last data it had.

`GET /stats/events` gives each decision as `kind`, `key`, `turn`, `home` (where the conversation lived before),
`backend`, `wait_ms`, `retry_from`/`retry_error`, `status`/`error`, token counts, `arrived` (ms since the epoch),
`ttft_ms` (streamed answers only) and `ms`, the last two counted from arrival. Like `/stats`, the dashboard data
carries conversation key hashes, never prompt text.

## systemd

```bash
sudo install -Dm644 router.mjs /opt/strata-router/router.mjs
sudo cp systemd/strata-router.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now strata-router
journalctl -u strata-router -f
```

the unit runs as a dynamic user and keeps conversation homes in `/var/lib/strata-router`. With keyed engines, add
`LoadCredential=strata-key:/etc/strata-router/key` and `Environment=STRATA_API_KEY_FILE=%d/strata-key` to it.

on SIGTERM the router saves conversation homes, stops accepting connections (requests on open ones get 503) and
lets the answers in flight reach their clients for up to `SHUTDOWN_GRACE`, then gives clients a second to finish
reading before the connections drop; a second signal exits at once. systemd's default 90 s stop timeout covers
the default grace.

## docker

```bash
docker build -t strata-router .
docker run -d -p 8000:8000 --stop-timeout 40 -v strata-router:/data -e STATE_FILE=/data/homes.json \
  -e STRATA_BACKENDS="a=10.0.0.1:8080,b=10.0.0.2:8080" strata-router
```

`--stop-timeout` matters: docker kills the container 10 s after `docker stop` otherwise, cutting the drain short.
With keyed engines, mount the key file and add `-e STRATA_API_KEY_FILE=/run/secrets/strata-key`.

## test without a GPU

```bash
node test/test.mjs           # stickiness, spill, failover, queueing, failure modes
node test/mock-engines.mjs   # two fake engines on :9101 and :9102, to poke at by hand
```

## pointing a client at it

pi (`~/.pi/agent/models.json`):

```json
{
  "providers": {
    "strata": {
      "name": "Strata router",
      "baseUrl": "http://localhost:8000/v1",
      "api": "openai-completions",
      "apiKey": "local",
      "compat": { "supportsDeveloperRole": false },
      "models": [
        {
          "id": "qwen3.8-flash-next-q2_0",
          "name": "Qwen3.8-Flash-Next Q2_0 (router)",
          "reasoning": true,
          "contextWindow": 262144,
          "maxTokens": 32768,
          "compat": {
            "thinkingFormat": "chat-template",
            "chatTemplateKwargs": { "enable_thinking": { "$var": "thinking.enabled" } }
          }
        }
      ]
    }
  }
}
```

`chatTemplateKwargs` is what carries pi's thinking level into the chat template (Qwen templates read
`enable_thinking`); with `chat-template` alone pi sends no thinking flags. In `settings.json`:
`"defaultProvider": "strata"`. opencode uses the same idea with
`"npm": "@ai-sdk/openai-compatible"` and `"options": { "baseURL": "http://localhost:8000/v1" }`. With keyed
engines, `apiKey` is the engines' key: the router passes it through.

keep the direct engine endpoints as a second provider so you can A/B a conversation against the router.

## notes on engine configuration

- an engine with an API key (Strata wants one whenever it listens beyond 127.0.0.1) answers `/metrics` only with
  that key, so put it in a file and point `STRATA_API_KEY_FILE` at it, or the router sees the engine as down. All
  engines share the one key. Clients still send the key on their own requests; the router never adds it, so it
  can't hand keyed engines to anyone who reaches the router
- Strata refuses host names it doesn't know (DNS rebinding protection) with `403 Host ... is not allowed`. List
  engines by IP address in `STRATA_BACKENDS`, or add the names to each engine's `STRATA_ALLOWED_HOSTS`; an API key
  also turns the check off
- an engine that has unloaded its model (`state: unloaded` in its `/metrics`) is picked last, and its conversations
  move: their KV is gone anyway, and loading the model takes about 90 s
- the router fills each engine up to the smaller of `MAX_INFLIGHT` and the engine's reported batch slots; raise
  `MAX_INFLIGHT` when you run `--batch` above 2
- set `PREFILL_TOK_S` to your measured cold prefill speed: it decides when waiting for a busy home beats moving
- conversation parking (`--conversation-cache-mib 32768 --conversation-cache-slots 8`) is what makes spill and
  restore cheap; without it an evicted conversation must be fully re-prefilled
- parking lives in host RAM: a parked 262k conversation is roughly 5 GB at int8, live KV is ~13.7 KB/token
- thinking consumes the output budget: with reasoning on and a small `max_tokens` the engine can spend the whole
  budget on reasoning and return no answer (`the reply reached max tokens while still thinking`). Give agent calls
  a generous `max_tokens` or set `reasoning_budget_tokens`
