# strata-router

one OpenAI-compatible endpoint in front of several [Strata](https://github.com/strata-ai/strata) engines, with
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
context takes about 190 s. That single event is forty times more expensive than everything else in a turn.

So the router does one thing: keep conversations on their home engine, and only spill when that engine has no
free slot or is unhealthy.

## run

```bash
STRATA_BACKENDS="a=192.168.77.117:8080,b=192.168.77.198:8080" node router.mjs
```

| env | default | meaning |
|---|---|---|
| `STRATA_BACKENDS` | `127.0.0.1:8080` | comma-separated `host:port` or `name=host:port` |
| `PORT` | `8000` | listen port |
| `BIND` | `0.0.0.0` | listen address |
| `POLL_MS` | `2000` | how often to read `GET /metrics` from each engine |
| `MAX_INFLIGHT` | `2` | per-engine cap; match the engine's `--batch N` |
| `MAX_QUEUE_WAIT` | `30000` | how long to wait for a free slot before letting the engine queue it |
| `CONNECT_TIMEOUT` | `10000` | connect timeout to an engine |
| `READ_TIMEOUT` | `300000` | per-request read timeout (long prefills need it) |
| `MAX_CONVERSATIONS` | `500` | size of the conversation map (LRU) |
| `LOG` | on | set `LOG=off` to silence per-request lines |

## endpoints

| path | behaviour |
|---|---|
| `POST /v1/chat/completions` | routed; SSE streamed untouched; response carries `x-strata-backend` |
| `POST /v1/messages` | same routing (Anthropic-style payloads) |
| `POST /v1/responses` | same routing (OpenAI Responses-style) |
| `GET /health` | backend health, in-flight, queued |
| `GET /stats` | counters, per-engine state, conversation homes |
| `GET /v1/models` | proxied from the first healthy engine |

## routing rules

- **conversation key** = `model | system prompt | first user message` (first 4k chars). Forks and branches of a
  conversation share the key, so they inherit the same home engine and reuse the prefix cache.
- **sticky** while the home engine is healthy, has an empty queue and a free batch slot.
- **spill** to the least-loaded healthy engine otherwise; the home moves with the conversation.
- **queue** up to `MAX_QUEUE_WAIT` when every engine is full, then fall through to the engine's own queue.
- **retry** once on another engine only for connect errors, timeouts or 5xx **before the first token**; a
  failure mid-stream is never retried, because the client already saw partial output.

## what the metrics mean

`GET /stats` reports, per engine:

- `held_tokens` — tokens resident in batch slots (`live.slots[].held_tokens`)
- `parked` — conversations parked in the engine's conversation cache (needs `--conversation-cache-mib`)
- `reused/requests` — engine-side prompt cache hit rate
- `tok_s_mean` — rolling decode speed used for the load score

and globally: `cold` (new conversations, i.e. prefills), `migrated` (spill events), `queued`, `overflow`, and
`cached_tokens / prompt_tokens` — the ratio you should actually watch. A healthy setup has almost as many cold
events as conversations, not as many as turns.

## systemd

```bash
sudo cp systemd/strata-router.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now strata-router
journalctl -u strata-router -f
```

## docker

```bash
docker build -t strata-router .
docker run -d -p 8000:8000 -e STRATA_BACKENDS="a=10.0.0.1:8080,b=10.0.0.2:8080" strata-router
```

## test without a GPU

```bash
node test/mock-engines.mjs   # two fake engines
node test/test.mjs           # stickiness, spill, failover, queueing
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
          "compat": { "thinkingFormat": "chat-template" }
        }
      ]
    }
  }
}
```

and in `settings.json`: `"defaultProvider": "strata"`, plus a per-model thinking level if the engine
supports the chat-template thinking flags. opencode uses the same idea with
`"npm": "@ai-sdk/openai-compatible"` and `"options": { "baseURL": "http://localhost:8000/v1" }`.

keep the direct engine endpoints as a second provider so you can A/B a conversation against the router.

## notes on engine configuration

- set `MAX_INFLIGHT` equal to the engine's `--batch` value, otherwise the router under- or over-fills it
- conversation parking (`--conversation-cache-mib 32768 --conversation-cache-slots 8`) is what makes spill and
  restore cheap; without it an evicted conversation must be fully re-prefilled
- parking lives in host RAM: a parked 262k conversation is roughly 5 GB at int8, live KV is ~13.7 KB/token
- thinking consumes the output budget: with reasoning on and a small `max_tokens` the engine can spend the whole
  budget on reasoning and return no answer (`the reply reached max tokens while still thinking`). Give agent calls
  a generous `max_tokens` or set `reasoning_budget_tokens`
