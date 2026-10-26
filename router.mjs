#!/usr/bin/env node
// strata-router: one OpenAI-compatible endpoint in front of several strata engines.
// conversations stay pinned to the engine that holds their KV; they spill only when that
// engine is down or stays busy longer than a cold prefill elsewhere would take.
// node >= 20, no dependencies.

import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import { setTimeout as sleep } from 'node:timers/promises';

const DEFAULT_BACKENDS = process.env.STRATA_BACKENDS || '127.0.0.1:8080';
const problems = [];

const config = {
  backends: parseBackends(DEFAULT_BACKENDS),
  port: num('PORT', 8000, { min: 1, max: 65535 }),
  pollMs: num('POLL_MS', 2000, { min: 10 }),
  pollFails: num('POLL_FAILS', 3, { min: 1 }),
  maxInflight: num('MAX_INFLIGHT', 2, { min: 1 }),
  queueWaitMs: num('MAX_QUEUE_WAIT', 30000),
  connectTimeoutMs: num('CONNECT_TIMEOUT', 10000, { min: 1 }),
  readTimeoutMs: num('READ_TIMEOUT', 300000, { min: 1 }),
  maxConversations: num('MAX_CONVERSATIONS', 500, { min: 1 }),
  prefillTokS: num('PREFILL_TOK_S', 1400, { min: 1, integer: false }),
  shortPrompt: num('SHORT_PROMPT_TOKENS', 2048),
  slowFirstTokenMs: num('SLOW_FIRST_TOKEN', 10000, { min: 1 }),
  shutdownGraceMs: num('SHUTDOWN_GRACE', 30000),
  passthrough: new Set((process.env.PASSTHROUGH || '').split(',').map((s) => s.trim()).filter(Boolean)),
  stateFile: process.env.STATE_FILE || '',
  apiKey: readKey(process.env.STRATA_API_KEY_FILE),
  verbose: process.env.LOG !== 'off',
};

for (const p of config.passthrough) if (!p.startsWith('/')) problems.push(`PASSTHROUGH entry "${p}": expected a path like /v1/vram`);
if (config.stateFile) {
  try {
    fs.accessSync(dirname(config.stateFile), fs.constants.W_OK);
  } catch (err) {
    problems.push(`STATE_FILE=${config.stateFile}: directory not writable (${err.code})`);
  }
}
if (problems.length) {
  for (const p of problems) console.error(`strata-router: ${p}`);
  process.exit(1);
}

// node timers fire after 1 ms when given more than 2^31-1 ms
function num(name, fallback, { min = 0, max = 2 ** 31 - 1, integer = true } = {}) {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const n = Number(raw);
  if (Number.isFinite(n) && n >= min && n <= max && (!integer || Number.isInteger(n))) return n;
  problems.push(`${name}=${raw}: expected ${integer ? 'an integer' : 'a number'} from ${min} to ${max}`);
  return fallback;
}

// strata wants its api key on /metrics too; clients still send their own on requests
function readKey(file) {
  if (!file) return '';
  try {
    const key = fs.readFileSync(file, 'utf8').trim();
    if (key) return key;
    problems.push(`STRATA_API_KEY_FILE=${file}: the file is empty`);
  } catch (err) {
    problems.push(`STRATA_API_KEY_FILE=${file}: ${err.code || err.message}`);
  }
  return '';
}

function parseBackends(spec) {
  const entries = spec.split(',').map((s) => s.trim()).filter(Boolean);
  if (!entries.length) problems.push('STRATA_BACKENDS: no engines configured');
  const backends = [];
  for (const entry of entries) {
    // "name=host:port" or "host:port", ipv6 as [::1]:port; names end up in a response header, so ascii only
    const m = entry.match(/^(?:([\x21-\x3c\x3e-\x7e]+)=)?(\[[0-9a-fA-F:.]+\]|[\w.-]+)(?::(\d{1,5}))?$/);
    const port = Number(m?.[3] || 8080);
    if (!m || port < 1 || port > 65535) {
      problems.push(`STRATA_BACKENDS entry "${entry}": expected host:port or name=host:port`);
      continue;
    }
    const host = m[2].replace(/^\[(.*)\]$/, '$1');
    backends.push({
      name: m[1] || host,
      host,
      port,
      healthy: false,
      fails: 0,
      state: null,
      slots: [],
      running: 0,
      queued: 0,
      inflight: 0,
      pollPeak: 0,
      direct: 0,
      directSeen: 0,
      busy: 0,
      busyAt: 0,
      flights: new Set(),
      batch: 2,
      tok_s_mean: null,
      prefill_tok_s: null,
      held_tokens: 0,
      vram_free_mib: 0,
      cache: null,
      current: null,
      hardware: null,
      history: null,
      recent: null,
      version: null,
      since: null,
      restarts: 0,
      slow: 0,
      routed: 0,
      lastError: null,
      lastPoll: 0,
    });
  }
  const names = backends.map((b) => b.name);
  for (const name of new Set(names.filter((n, i) => names.indexOf(n) !== i))) {
    problems.push(`STRATA_BACKENDS: more than one engine named "${name}"; name them, e.g. a=host:8080,b=host:8081`);
  }
  return backends;
}

const addr = (b) => (b.host.includes(':') ? `[${b.host}]:${b.port}` : `${b.host}:${b.port}`);

const ROUTED = new Set(['/v1/chat/completions', '/v1/completions', '/v1/messages', '/v1/responses']);
// router data sits under /stats so it can't shadow an engine GET path, which gets relayed
const LOCAL = new Set(['/', '/health', '/stats', '/stats/events', '/stats/conversations', '/dashboard']);
const HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'te', 'trailer', 'upgrade', 'proxy-authenticate', 'proxy-authorization']);
// expect would stall the upstream request; accept-encoding would compress answers the router has to read
const NOT_FORWARDED = new Set(['host', 'content-length', 'expect', 'accept-encoding']);
const MAX_BODY = 20_000_000;
const CLAIM_HOLD_MS = 2000;
// a short request can go ahead of longer ones that have waited less than this
const SHORT_PASS_MS = 5000;
// how far back "busy lately" looks: the share of slots in use, halving in weight every 10 minutes
const BUSY_HALF_LIFE_MS = 600000;
// a fresh connection per request, so a stale keep-alive socket can't fail (and spill) a turn
const agent = new http.Agent({ keepAlive: false });

const convs = new Map(); // conversation key -> { backend, turns, prompt_tokens, first, model, last, migrations }, least recently used first
const MAX_EVENTS = 200;
const events = []; // the last MAX_EVENTS finished requests, oldest first
const live = new Set(); // requests waiting for a slot or being answered
let lastEvent = 0;
const stats = {
  requests: 0,
  cold: 0,
  migrated: 0,
  queued: 0,
  overflow: 0,
  retried: 0,
  errors: 0,
  slow: 0,
  restarts: 0,
  prompt_tokens: 0,
  completion_tokens: 0,
  cached_tokens: 0,
  started: new Date().toISOString(),
};

const log = (msg) => { if (config.verbose) console.log(new Date().toISOString().slice(11, 23), msg); };
// a refused localhost connect is an AggregateError with an empty message
const errText = (err) => String(err?.message || err?.code || err);

function getJson(b, path, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const headers = config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {};
    const req = http.request({ host: b.host, port: b.port, path, method: 'GET', headers, timeout, agent }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const hint = res.statusCode === 401 ? (config.apiKey ? ' (wrong API key?)' : ' (the engine wants an API key: STRATA_API_KEY_FILE)') : '';
        if (res.statusCode !== 200) return reject(Object.assign(new Error(`${path} answered ${res.statusCode}${hint}`), { definite: true }));
        try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch (err) { reject(err); }
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('poll timeout')));
    req.on('close', () => { if (!req.res?.complete) reject(new Error('poll connection closed')); });
    req.end();
  });
}

let lastHealthyAt = -Infinity;

// strata reports running and slots only with batch slots; its state is "reading" while it prefills
const busy = (b) => ['reading', 'generating'].includes(b.state) || b.running > 0 || b.queued > 0 || b.slots.some((s) => s?.state && s.state !== 'idle');

async function pollOne(b) {
  b.pollPeak = b.inflight;
  try {
    const m = await getJson(b, '/metrics');
    const live = m.live || {};
    b.healthy = true;
    b.fails = 0;
    b.state = live.state || null;
    b.lastError = null;
    b.lastPoll = Date.now();
    b.slots = Array.isArray(live.slots) ? live.slots : [];
    b.running = live.running || 0;
    b.queued = live.queued || 0;
    // without batch slots strata answers one request at a time and queues the rest
    b.batch = Math.max(1, Number(m.engine?.batch_slots) || 1);
    // requests sent to the engine directly take slots too; a request the router finished while the poll was out
    // counts as its own, and a direct one has to show twice in a row, so one the engine is still wrapping up doesn't
    const running = Number(live.running ?? (['reading', 'generating'].includes(b.state) ? 1 : 0)) || 0;
    const seen = Math.max(0, running - b.pollPeak);
    b.direct = Math.min(seen, b.directSeen);
    b.directSeen = seen;
    const at = performance.now();
    if (b.busyAt) b.busy += (Math.min(1, running / b.batch) - b.busy) * (1 - 0.5 ** ((at - b.busyAt) / BUSY_HALF_LIFE_MS));
    b.busyAt = at;
    b.tok_s_mean = live.tok_s_mean ?? null;
    b.held_tokens = b.slots.reduce((s, x) => s + (x?.held_tokens || 0), 0);
    b.vram_free_mib = m.engine?.vram_free_mib ?? 0;
    b.version = m.engine?.version ?? null;
    // strata's top-level progress is the newest request's, the only one when it runs without batch slots
    b.current = Object.fromEntries(['phase', 'prompt_tokens', 'prompt_read', 'prompt_total', 'generated', 'max_tokens', 'tok_s', 'elapsed_s'].map((k) => [k, live[k] ?? null]));
    b.hardware = numbers(m.hardware, HARDWARE);
    b.history = { gpu_util: series(m.history?.gpu_util), tok_s: series(m.history?.tok_s) };
    b.recent = recentStats(m.requests);
    const cache = m.conversation_cache || null;
    const since = m.totals?.since ?? null;
    // a new server start, or cache counters starting over (strata restarts a stuck engine itself)
    const restarted = (b.since != null && since != null && since !== b.since) || (b.cache && cache && cacheEvents(cache) < cacheEvents(b.cache));
    b.since = since;
    b.cache = cache;
    if (restarted) forgetHomes(b);
    lastHealthyAt = performance.now();
  } catch (err) {
    b.fails++;
    b.lastError = errText(err);
    // an answer or a refused connection is final; a timeout may just be an engine busy prefilling
    if (err.definite || err.code === 'ECONNREFUSED' || b.fails >= config.pollFails) {
      b.healthy = false;
      // requests still waiting for a first token can't tell a long prefill from a dead engine
      for (const flight of b.flights) flight.engineDown();
    }
  }
  wake();
}

const cacheEvents = (c) => (c.parks || 0) + (c.restores || 0);

const HARDWARE = ['gpu_util', 'gpu_mem_used', 'gpu_mem_total', 'gpu_temp', 'gpu_power', 'gpu_power_limit', 'gpu_pcie_gen', 'gpu_pcie_gen_max', 'gpu_pcie_width', 'gpu_pcie_rx_mb', 'gpu_pcie_tx_mb', 'cpu', 'ram_used', 'ram_total'];
const numbers = (o, keys) => (o && typeof o === 'object' ? Object.fromEntries(keys.filter((k) => Number.isFinite(o[k])).map((k) => [k, o[k]])) : null);
const series = (a) => (Array.isArray(a) ? a.slice(-60).map((v) => (Number.isFinite(v) ? v : null)) : null);
const median = (a) => {
  const x = a.filter(Number.isFinite).sort((p, q) => p - q);
  return x.length ? x[Math.floor(x.length / 2)] : null;
};

// the engine's own account of its last requests: decode and prompt speed, expert cache hits, drafts kept
function recentStats(reqs) {
  if (!Array.isArray(reqs) || !reqs.length) return null;
  const offered = reqs.reduce((n, r) => n + (r.drafts_offered || 0), 0);
  const accepted = reqs.reduce((n, r) => n + (r.drafts_accepted || 0), 0);
  return {
    n: reqs.length,
    decode_tok_s: median(reqs.filter((r) => r.output_tokens > 20).map((r) => r.decode_tok_s)),
    prefill_tok_s: median(reqs.filter((r) => r.prompt_read >= 1000 && r.prompt_ms > 0).map((r) => r.prompt_read / (r.prompt_ms / 1000))),
    expert_hit: median(reqs.map((r) => r.hit_rate)),
    pcie_share: median(reqs.map((r) => r.pcie_share)),
    draft_accept: offered ? accepted / offered : null,
  };
}

// what the engine says it is working on, each matched to the router's request that started at about the same time
function activity(b) {
  const busy = b.slots.filter((x) => x?.state && x.state !== 'idle');
  const rows = busy.length
    ? busy.map((x) => ({ slot: x.slot ?? null, state: x.state, prompt_tokens: x.prompt_tokens ?? null, prompt_read: x.prompt_read ?? null, prompt_total: x.prompt_tokens ?? null, generated: x.generated ?? null, max_tokens: null, tok_s: x.tok_s ?? null, elapsed_s: x.elapsed_s ?? null }))
    : ['reading', 'generating'].includes(b.state) ? [{ slot: null, state: b.state, ...b.current }] : [];
  const mine = [...live].filter((e) => e.backend === b.name && e.started);
  for (const r of rows) {
    if (r.elapsed_s == null) continue;
    const at = b.lastPoll - r.elapsed_s * 1000;
    let near = null;
    for (const e of mine) if (Math.abs(e.started - at) < 5000 && (!near || Math.abs(e.started - at) < Math.abs(near.started - at))) near = e;
    if (!near) continue;
    r.key = near.key;
    mine.splice(mine.indexOf(near), 1);
  }
  return rows;
}

// a restarted engine has an empty cache: its conversations are as cold there as anywhere, so nobody waits for it
function forgetHomes(b) {
  let n = 0;
  for (const c of convs.values()) {
    if (c.backend !== b.name) continue;
    c.backend = null;
    n++;
  }
  b.restarts++;
  stats.restarts++;
  log(`${b.name} restarted: ${n} conversation(s) lost their cache there`);
}

// one loop per engine, so a dead one can't hold up the others' state
async function pollLoop(b) {
  for (;;) {
    await sleep(config.pollMs);
    await pollOne(b);
  }
}

// requests waiting for a slot, in arrival order; claim is the busy home a patient one holds out for
const queue = new Set();
const shortWaiting = new Set();

// whoever is woken first takes a freed slot: short requests, and anyone past SHORT_PASS_MS, before the rest
function wake() {
  const now = performance.now();
  const later = [];
  for (const w of queue) {
    if (w.short || now - w.t0 >= SHORT_PASS_MS) w.wake();
    else later.push(w);
  }
  for (const w of later) w.wake();
}

function claimed(b) {
  let n = 0;
  for (const w of queue) if (w.claim === b && !w.isGone()) n++;
  return n;
}

// free slots we are allowed to use, minus the requests already in flight
function freeSlots(b) {
  return Math.max(0, Math.min(b.batch - b.direct, config.maxInflight) - b.inflight);
}

function capacity(b) {
  return b.healthy && b.queued === 0 && freeSlots(b) > 0;
}

// where a request without a usable home goes: an engine with its model loaded (loading takes about 90 s), the most
// free slots, no queue, then the one busy least lately so both boxes work about as long, then the faster prompt reader
const rank = (b) => [b.state === 'unloaded' ? 1 : 0, -freeSlots(b), b.queued, b.busy, -(b.recent?.prefill_tok_s || b.prefill_tok_s || 0)];

function before(x, y) {
  const p = rank(x), q = rank(y);
  for (let i = 0; i < p.length; i++) if (p[i] !== q[i]) return p[i] < q[i];
  return false;
}

const best = (list) => list.reduce((a, b) => (before(b, a) ? b : a));

function reserve(b) {
  b.inflight++;
  b.pollPeak = Math.max(b.pollPeak, b.inflight);
  b.routed++;
  return b;
}

function release(b) {
  b.inflight--;
  wake();
}

function textOf(content) {
  if (typeof content === 'string') return content;
  if (typeof content === 'number') return String(content);
  if (Array.isArray(content)) return content.map((c) => textOf(c?.text ?? c)).filter(Boolean).join(' ');
  return '';
}

// fork-safe key: system + first user message, so a branch of a conversation
// inherits the same home engine and reuses the prefix cache
function conversationKey(body) {
  const msgs = Array.isArray(body.messages) ? body.messages : Array.isArray(body.input) ? body.input : [];
  let system = textOf(body.system) || textOf(body.instructions);
  if (!system && ['system', 'developer'].includes(msgs[0]?.role)) system = textOf(msgs[0].content);
  const opener = msgs.find((m) => m?.role === 'user');
  const text = textOf(opener?.content) || textOf(body.input) || textOf(body.prompt);
  // an opener without text, say a lone image, is told apart by its raw content
  const first = text ? text.slice(0, 4000) : JSON.stringify(opener?.content ?? null);
  return createHash('sha256').update(`${body.model || 'default'}|${system}|${first}`).digest('hex').slice(0, 16);
}

function remember(key, conv) {
  convs.delete(key);
  convs.set(key, conv);
  while (convs.size > config.maxConversations) convs.delete(convs.keys().next().value);
}

// how long a busy home is worth waiting for: past this, a cold prefill elsewhere is cheaper
function patience(conv, home) {
  if (!home?.healthy) return 0;
  const rates = config.backends.filter((b) => b !== home && b.healthy).map((b) => b.prefill_tok_s || config.prefillTokS);
  return Math.min(config.queueWaitMs, (conv.prompt_tokens / Math.max(config.prefillTokS / 10, ...rates)) * 1000);
}

// streamed turns with a mostly uncached prompt show how fast an engine really prefills
function learnPrefill(b, u, firstTokenMs) {
  const fresh = u.prompt - u.cached;
  if (!(fresh >= 1000 && u.cached <= fresh && firstTokenMs >= 20)) return;
  const rate = Math.min(config.prefillTokS * 10, Math.max(config.prefillTokS / 10, fresh / (firstTokenMs / 1000)));
  b.prefill_tok_s = Math.round(b.prefill_tok_s ? 0.7 * b.prefill_tok_s + 0.3 * rate : rate);
}

// the backend it returns already has a slot reserved for the caller
async function pick(conv, isGone, exclude = null, short = false) {
  const t0 = performance.now();
  const me = { claim: null, isGone, t0, short, wake() {} };
  let waited = false;
  try {
    for (;;) {
      if (isGone() || draining) return {};
      const elapsed = performance.now() - t0;
      // a home that unloaded its model has lost the conversation's KV anyway
      const home = config.backends.find((b) => b.name === conv.backend && b !== exclude && b.state !== 'unloaded');
      if (home && capacity(home)) return { b: reserve(home), waited };

      const healthy = config.backends.filter((b) => b.healthy && b !== exclude);
      // a retry has nowhere to go; otherwise give the next poll a chance before failing
      if (!healthy.length && (exclude || performance.now() - lastHealthyAt >= Math.min(config.queueWaitMs, config.pollMs + 1000))) return { waited };

      // a short request waiting behind long ones takes the next freed slot, as long as it does not
      // starve: a long request only steps aside for SHORT_PASS_MS, then arrival order rules again
      if (!me.short && elapsed < SHORT_PASS_MS && shortWaiting.size) {
        waited = true;
        queue.add(me);
        await sleep(200);
        continue;
      }

      const hold = patience(conv, home);
      me.claim = elapsed < hold ? home : null;
      if (!me.claim) {
        // claims shield a waiting conversation's home from newcomers, but only briefly: past that, arrival order
        const open = healthy.filter((b) => capacity(b) && (elapsed >= CLAIM_HOLD_MS || freeSlots(b) > claimed(b)));
        if (open.length) return { b: reserve(best(open)), waited };
      }
      // everything saturated for too long: let an engine queue it, home first
      if (healthy.length && elapsed >= config.queueWaitMs) {
        return { b: reserve(home?.healthy ? home : best(healthy)), waited, overflow: true };
      }
      waited = true;
      queue.add(me);
      if (me.short) shortWaiting.add(me);
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, Math.max(1, Math.min(1000, (me.claim ? hold : config.queueWaitMs) - elapsed)));
        me.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
    }
  } finally {
    shortWaiting.delete(me);
    // a claim going away can open its slot to the others
    if (queue.delete(me) && me.claim) wake();
  }
}

// openai counts cache hits inside prompt_tokens, anthropic reports them beside input_tokens
function normUsage(u) {
  const cached = u.prompt_tokens_details?.cached_tokens ?? u.input_tokens_details?.cached_tokens ?? u.cache_read_input_tokens ?? 0;
  const anthropic = 'cache_read_input_tokens' in u || 'cache_creation_input_tokens' in u;
  const extra = anthropic ? (u.cache_read_input_tokens || 0) + (u.cache_creation_input_tokens || 0) : 0;
  return {
    prompt: u.prompt_tokens ?? (u.input_tokens || 0) + extra,
    completion: u.completion_tokens ?? u.output_tokens ?? 0,
    cached,
  };
}

// an event carrying generated text, reasoning or tool calls; role-only openers don't count
function generated(o) {
  const c = o?.choices?.[0];
  if (c) return Boolean(c.text || c.delta?.content || c.delta?.reasoning_content || c.delta?.reasoning || c.delta?.tool_calls?.length);
  return typeof o?.type === 'string' && /[._]delta$/.test(o.type) && o.type !== 'message_delta';
}

function usageTap(sse) {
  const dec = new StringDecoder('utf8');
  let buf = '';
  let size = 0;
  let usage = null;
  const take = (o) => {
    for (const u of [o?.usage, o?.message?.usage, o?.response?.usage]) {
      if (u && typeof u === 'object') usage = { ...usage, ...u };
    }
  };
  const line = (l) => {
    const data = l.startsWith('data:') ? l.slice(5).trim() : '';
    if (data === '[DONE]') tap.complete = true;
    if (!data || data === '[DONE]') return;
    let o;
    try {
      o = JSON.parse(data);
    } catch {
      return;
    }
    if (!tap.firstTokenAt && generated(o)) tap.firstTokenAt = performance.now();
    if (o?.type === 'message_stop' || o?.type === 'response.completed') tap.complete = true;
    take(o);
  };
  const tap = {
    firstTokenAt: 0,
    // the stream's last event went by: the answer is whole even if the connection isn't closed yet
    complete: false,
    write(chunk) {
      size += chunk.length;
      if (!sse) {
        if (size <= MAX_BODY) buf += dec.write(chunk);
        return;
      }
      buf += dec.write(chunk);
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        line(buf.slice(0, i));
        buf = buf.slice(i + 1);
      }
      if (buf.length > 1_000_000) buf = '';
    },
    end() {
      if (sse) line(buf + dec.end());
      else if (size <= MAX_BODY) {
        try { take(JSON.parse(buf + dec.end())); } catch {}
      }
      return usage && normUsage(usage);
    },
  };
  return tap;
}

// the fixed hop-by-hop headers plus any a message lists in its own connection header
function hopByHop(headers) {
  const names = new Set(HOP);
  for (const name of String(headers.connection || '').split(',')) if (name.trim()) names.add(name.trim().toLowerCase());
  return names;
}

function passHeaders(headers, b) {
  const drop = hopByHop(headers);
  const out = {};
  for (const [k, v] of Object.entries(headers)) if (!drop.has(k)) out[k] = v;
  out['x-strata-backend'] = b.name;
  return out;
}

function upstreamHeaders(req, payload) {
  const drop = hopByHop(req.headers);
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) if (!drop.has(k) && !NOT_FORWARDED.has(k)) headers[k] = v;
  headers.accept ??= 'application/json';
  if (payload) {
    headers['content-type'] ??= 'application/json';
    headers['content-length'] = payload.length;
  }
  return headers;
}

// aborts when the client hangs up; a pipelined request only notices through its socket
function watchClient(req, res) {
  const ac = new AbortController();
  const hangup = () => {
    if (!res.writableFinished) ac.abort();
  };
  if (res.destroyed || req.socket.destroyed) ac.abort();
  res.on('close', hangup);
  req.socket.on('close', hangup);
  ac.signal.addEventListener('abort', wake);
  return {
    signal: ac.signal,
    stop() {
      res.off('close', hangup);
      req.socket.off('close', hangup);
    },
  };
}

// retryable means the engine never started an answer; hold5xx buffers an engine 5xx instead of relaying it
function forward(b, req, url, payload, res, { hold5xx = false, signal }) {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve({ ok: false, retryable: false, aborted: true, error: 'client went away', elapsed: 0 });
    const t0 = performance.now();
    let settled = false;
    let connected = false;
    let timedOut = false;
    let responded = false;
    let tap = null;
    let connectTimer;

    // only an answer that hasn't reached the client can move; a started one waits for READ_TIMEOUT
    const flight = {
      engineDown() {
        if (!res.headersSent) up.destroy(new Error('engine stopped answering polls'));
      },
    };
    const done = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(connectTimer);
      b.flights.delete(flight);
      signal.removeEventListener('abort', onClientGone);
      resolve({ elapsed: Math.round(performance.now() - t0), ...result });
    };
    const fail = (err) => {
      if (settled) return;
      const error = errText(err);
      const gone = signal.aborted;
      if (!connected && !gone) {
        b.healthy = false;
        b.lastError = error;
      }
      // a half-sent answer has to look broken to the client, not complete
      if (res.headersSent && !res.writableEnded) res.destroy();
      done({ ok: false, retryable: !res.headersSent && !gone, timedOut, aborted: gone, error });
    };
    const onClientGone = () => {
      // the openai sdk hangs up as soon as it reads [DONE], before the engine has closed the stream
      if (tap?.complete) {
        done({ ok: true, status: up.res.statusCode, usage: tap.end(), firstTokenMs: tap.firstTokenAt && tap.firstTokenAt - t0 });
        return up.destroy();
      }
      up.destroy(new Error('client went away'));
    };
    // silence is fine while the engine reports work: long prefills and non-streamed answers say nothing for minutes
    const onSilence = (socket) => {
      if (connected && b.healthy && busy(b)) return socket.setTimeout(config.readTimeoutMs);
      timedOut = true;
      up.destroy(new Error('read timeout'));
    };

    const options = { host: b.host, port: b.port, path: url, method: req.method, headers: upstreamHeaders(req, payload), agent };
    const up = http.request(options, (r) => {
      responded = true;
      r.on('error', fail);
      if (hold5xx && r.statusCode >= 500) {
        const chunks = [];
        let size = 0;
        r.on('data', (c) => {
          if ((size += c.length) > 1_000_000) up.destroy(new Error(`upstream ${r.statusCode} with an oversized error body`));
          else chunks.push(c);
        });
        r.on('end', () => done({ ok: false, retryable: !signal.aborted, error: `upstream ${r.statusCode}`, status: r.statusCode, headers: r.headers, body: Buffer.concat(chunks) }));
        return;
      }
      try {
        res.writeHead(r.statusCode, passHeaders(r.headers, b));
      } catch (err) {
        fail(err);
        return up.destroy();
      }
      tap = usageTap(/event-stream/.test(r.headers['content-type'] || ''));
      r.on('data', (c) => {
        tap.write(c);
        if (!res.write(c)) {
          r.pause();
          res.once('drain', () => r.resume());
        }
      });
      r.on('end', () => {
        res.end();
        done({ ok: true, status: r.statusCode, usage: tap.end(), firstTokenMs: tap.firstTokenAt && tap.firstTokenAt - t0 });
      });
    });
    up.on('socket', (socket) => {
      socket.setTimeout(config.readTimeoutMs);
      socket.on('timeout', () => onSilence(socket));
      if (settled || !socket.connecting) {
        connected = true;
        return;
      }
      connectTimer = setTimeout(() => up.destroy(new Error('connect timeout')), config.connectTimeoutMs);
      socket.once('connect', () => {
        connected = true;
        clearTimeout(connectTimer);
      });
    });
    up.on('error', fail);
    up.on('close', () => { if (!up.res?.complete) fail(new Error('engine closed the connection')); });
    b.flights.add(flight);
    signal.addEventListener('abort', onClientGone);
    up.end(payload || undefined);
  });
}

function send(res, status, obj) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

function readBody(req, res, cb) {
  const chunks = [];
  let size = 0;
  req.on('data', (c) => {
    if ((size += c.length) > MAX_BODY) {
      if (!res.headersSent) send(res, 413, { error: 'payload too large' });
      req.destroy();
    } else chunks.push(c);
  });
  req.on('end', () => cb(Buffer.concat(chunks)));
}

// key hashes and numbers only: an event never carries prompt text
function track(key, conv) {
  const ev = {
    kind: null,
    key,
    turn: conv.turns,
    home: conv.backend,
    backend: null,
    arrived: conv.last,
    started: null,
    queued: false,
    wait_ms: 0,
    overflow: false,
    retry_from: null,
    retry_error: null,
    error: null,
    ttft_ms: null,
    engine_ttft_ms: null,
    expected_ttft_ms: null,
    slow: false,
  };
  live.add(ev);
  return ev;
}

// what went wrong outranks how the request was placed
function kindOf(e) {
  if (e.error || !e.status || e.status >= 400) return 'failed';
  if (e.retry_from) return 'retry';
  if (e.overflow) return 'overflow';
  if (e.home && e.home !== e.backend) return 'spill';
  if (e.queued) return 'queued';
  return e.home ? 'sticky' : 'cold';
}

function record(ev, res, out, t0) {
  live.delete(ev);
  const u = out?.ok ? out.usage : null;
  Object.assign(ev, {
    wait_ms: Math.round(ev.wait_ms),
    status: res.headersSent ? res.statusCode : null,
    error: ev.error ?? (out?.ok === false ? out.error : null),
    prompt_tokens: u?.prompt ?? null,
    cached_tokens: u?.cached ?? null,
    completion_tokens: u?.completion ?? null,
    ms: Math.round(performance.now() - t0),
  });
  ev.kind ??= kindOf(ev);
  events.push({ id: ++lastEvent, ...ev });
  if (events.length > MAX_EVENTS) events.shift();
}

async function handleChat(req, res, raw, url) {
  let body;
  try {
    body = JSON.parse(raw.toString('utf8'));
  } catch {
    return send(res, 400, { error: 'invalid json' });
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) return send(res, 400, { error: 'expected a json object' });

  const key = conversationKey(body);
  let conv = convs.get(key);
  if (!conv) {
    conv = { backend: null, turns: 0, prompt_tokens: 0, first: new Date().toISOString(), model: body.model, migrations: 0 };
    stats.cold++;
  }
  conv.turns++;
  conv.last = Date.now();
  remember(key, conv);

  const t0 = performance.now();
  const client = watchClient(req, res);
  const gone = () => client.signal.aborted;
  const ev = track(key, conv);
  const short = raw.length / 4 < config.shortPrompt;
  const slot = async (exclude) => {
    const t = performance.now();
    const picked = await pick(conv, gone, exclude, short);
    ev.wait_ms += performance.now() - t;
    ev.queued ||= Boolean(picked.waited);
    ev.overflow ||= Boolean(picked.overflow);
    if (picked.b) {
      ev.backend = picked.b.name;
      ev.started = Date.now();
    }
    return picked;
  };
  let out = null;
  try {
    const picked = await slot();
    if (!picked.b) {
      if (gone()) {
        ev.kind = 'gone';
        return;
      }
      ev.error = draining ? 'router is shutting down' : 'no healthy backend';
      if (draining) return send(res, 503, { error: ev.error });
      return send(res, 503, {
        error: ev.error,
        backends: config.backends.map((x) => ({ name: x.name, healthy: x.healthy, error: x.lastError })),
      });
    }
    stats.requests++;
    if (picked.waited) stats.queued++;
    if (picked.overflow) stats.overflow++;

    const attempt = async (b, hold5xx) => {
      const sent = performance.now();
      let result;
      try {
        result = await forward(b, req, url, raw, res, { hold5xx, signal: client.signal });
      } finally {
        release(b);
      }
      if (!result.ok && !result.aborted) {
        stats.errors++;
        log(`fail ${b.name} ${result.error} (${result.elapsed}ms)`);
      }
      if (result.firstTokenMs) ev.ttft_ms = Math.round(sent + result.firstTokenMs - t0);
      return result;
    };

    const spare = config.backends.length > 1;
    let served = picked.b;
    let overflow = picked.overflow;
    out = await attempt(served, spare);
    // the engine never started an answer: queue again for a different one
    if (!out.ok && out.retryable && spare) {
      const again = await slot(served);
      if (again.b) {
        stats.retried++;
        ev.retry_from = served.name;
        ev.retry_error = out.error;
        served = again.b;
        overflow = again.overflow;
        out = await attempt(served, false);
      }
    }
    if (out.aborted || (!res.headersSent && gone())) ev.kind = 'gone';

    // the engine took the prompt, so that's where its KV lives now
    if (out.ok ? out.status < 400 : res.headersSent) {
      if (conv.backend && conv.backend !== served.name) {
        stats.migrated++;
        conv.migrations++;
      }
      ev.home = conv.backend;
      conv.backend = served.name;
    }

    if (out.ok) {
      const u = out.usage;
      if (out.firstTokenMs) {
        ev.engine_ttft_ms = Math.round(out.firstTokenMs);
        const fresh = u ? u.prompt - u.cached : raw.length / 4;
        ev.expected_ttft_ms = Math.round((fresh / (served.prefill_tok_s || config.prefillTokS)) * 1000);
        // after an overflow the time to first token includes the engine's own queue
        ev.slow = !overflow && ev.engine_ttft_ms > config.slowFirstTokenMs && ev.engine_ttft_ms > 3 * ev.expected_ttft_ms;
        if (ev.slow) {
          stats.slow++;
          served.slow++;
          log(`slow first token ${served.name} key=${key} ${ev.engine_ttft_ms}ms, its prompt needs about ${ev.expected_ttft_ms}ms`);
        }
      }
      if (u) {
        stats.prompt_tokens += u.prompt;
        stats.completion_tokens += u.completion;
        stats.cached_tokens += u.cached;
        if (out.firstTokenMs && !overflow && !ev.slow) learnPrefill(served, u, out.firstTokenMs);
      }
      // without usage, guess the prompt from the request size so patience still works
      conv.prompt_tokens = u?.prompt || Math.round(raw.length / 4);
      const status = out.status === 200 ? '' : ` status=${out.status}`;
      log(`${served.name} key=${key} turns=${conv.turns}${status} prompt=${u?.prompt ?? '-'} cached=${u?.cached ?? '-'} out=${u?.completion ?? '-'} ${out.elapsed}ms`);
    } else if (out.aborted) {
      log(`gone ${served.name} key=${key} after ${out.elapsed}ms`);
    } else if (!res.headersSent) {
      if (out.status) {
        res.writeHead(out.status, passHeaders(out.headers, served));
        res.end(out.body);
      } else {
        send(res, out.timedOut ? 504 : 502, { error: out.error, backend: served.name });
      }
    }
  } finally {
    client.stop();
    record(ev, res, out, t0);
  }
}

async function relay(req, res, url, raw) {
  const b = config.backends.find((x) => x.healthy);
  if (!b) return send(res, 503, { error: 'no healthy backend' });
  const client = watchClient(req, res);
  try {
    const out = await forward(b, req, url, req.method === 'GET' ? null : raw, res, { signal: client.signal });
    if (!out.ok && !out.aborted && !res.headersSent) send(res, out.timedOut ? 504 : 502, { error: out.error, backend: b.name });
  } finally {
    client.stop();
  }
}

const convView = ([key, c]) => ({ key, backend: c.backend, turns: c.turns, prompt_tokens: c.prompt_tokens, last_seen: c.last, migrations: c.migrations });

function handleGet(res, path, query) {
  if (path === '/dashboard') return page(res);
  if (path === '/stats/events') {
    const since = Number(new URLSearchParams(query).get('since')) || 0;
    return send(res, 200, {
      started: stats.started,
      now: Date.now(),
      last: lastEvent,
      // a cursor past the last event comes from before a restart
      events: events.filter((e) => e.id > (since > lastEvent ? 0 : since)),
      live: [...live].map(({ key, turn, home, backend, arrived, started }) => ({ key, turn, home, backend, arrived, started })),
    });
  }
  if (path === '/stats/conversations') return send(res, 200, { now: Date.now(), conversations: [...convs].map(convView) });
  if (path === '/stats') {
    const homes = new Map();
    for (const c of convs.values()) homes.set(c.backend, (homes.get(c.backend) || 0) + 1);
    return send(res, 200, {
      stats,
      now: Date.now(),
      backends: config.backends.map((b) => ({
        name: b.name,
        host: addr(b),
        healthy: b.healthy,
        state: b.state,
        poll_fails: b.fails,
        inflight: b.inflight,
        routed: b.routed,
        batch: b.batch,
        running: b.running,
        queued: b.queued,
        held_tokens: b.held_tokens,
        tok_s_mean: b.tok_s_mean,
        prefill_tok_s: b.prefill_tok_s,
        vram_free_mib: b.vram_free_mib,
        parked: b.cache?.parked ?? 0,
        cache_budget_mib: b.cache?.budget_mib ?? 0,
        cache_slots: b.cache?.slots ?? null,
        reused: b.cache?.requests_reused ?? 0,
        requests: b.cache?.requests ?? 0,
        reused_tokens: b.cache?.reused_tokens ?? null,
        prompt_tokens: b.cache?.prompt_tokens ?? null,
        parks: b.cache?.parks ?? null,
        evictions: b.cache?.evictions ?? null,
        version: b.version,
        started: b.since ? Math.round(b.since * 1000) : null,
        phase: b.current?.phase ?? null,
        activity: activity(b),
        slots: b.slots.map((x) => ({ slot: x?.slot ?? null, state: x?.state ?? null, held_tokens: x?.held_tokens ?? 0 })),
        recent: b.recent,
        hardware: b.hardware,
        history: b.history,
        restarts: b.restarts,
        slow: b.slow,
        direct: b.direct,
        busy: Math.round(b.busy * 1000) / 1000,
        lastError: b.lastError,
        capacity: Math.min(b.batch, config.maxInflight),
        claimed: claimed(b),
        conversations: homes.get(b.name) || 0,
      })),
      waiting: queue.size,
      settings: { max_queue_wait_ms: config.queueWaitMs, claim_hold_ms: CLAIM_HOLD_MS, short_prompt_tokens: config.shortPrompt, short_pass_ms: SHORT_PASS_MS, slow_first_token_ms: config.slowFirstTokenMs },
      conversations: [...convs].slice(-50).map(convView),
    });
  }
  send(res, 200, {
    ok: config.backends.some((b) => b.healthy),
    backends: config.backends.map((b) => ({ name: b.name, host: addr(b), healthy: b.healthy, inflight: b.inflight, queued: b.queued, running: b.running })),
  });
}

function page(res) {
  res.writeHead(200, {
    'content-type': 'text/html; charset=utf-8',
    'cache-control': 'no-cache',
    // the page may only talk to the router: no cdn, fonts or other hosts
    'content-security-policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:",
  });
  res.end(DASHBOARD);
}

function loadState() {
  if (!config.stateFile) return;
  try {
    const names = new Set(config.backends.map((b) => b.name));
    for (const [key, c] of JSON.parse(fs.readFileSync(config.stateFile, 'utf8'))) {
      if (names.has(c?.backend)) remember(key, { backend: c.backend, turns: c.turns || 0, prompt_tokens: c.prompt_tokens || 0, first: c.first, model: c.model, last: c.last || null, migrations: c.migrations || 0 });
    }
    log(`restored ${convs.size} conversation homes from ${config.stateFile}`);
  } catch (err) {
    if (err.code !== 'ENOENT') console.error(`state file ${config.stateFile}: ${err.message}`);
  }
}

function saveState() {
  if (!config.stateFile) return;
  try {
    fs.writeFileSync(`${config.stateFile}.tmp`, JSON.stringify([...convs]));
    fs.renameSync(`${config.stateFile}.tmp`, config.stateFile);
  } catch (err) {
    console.error(`state file ${config.stateFile}: ${err.message}`);
  }
}

let draining = false;
let active = 0;

const server = http.createServer((req, res) => {
  if (draining) {
    res.setHeader('connection', 'close');
    return send(res, 503, { error: 'router is shutting down' });
  }
  const url = req.url || '/';
  const q = url.indexOf('?');
  // a trailing or doubled slash still reaches the routed paths
  const path = (q < 0 ? url : url.slice(0, q)).replace(/\/{2,}/g, '/').replace(/(.)\/$/, '$1');
  if (req.method === 'GET' && LOCAL.has(path)) return handleGet(res, path, q < 0 ? '' : url.slice(q + 1));

  const routed = req.method === 'POST' && ROUTED.has(path);
  if (!routed && req.method !== 'GET') {
    if (req.method !== 'POST' || LOCAL.has(path)) return send(res, 405, { error: 'method not allowed' });
    if (config.passthrough.size && !config.passthrough.has(path)) return send(res, 404, { error: 'not forwarded (see PASSTHROUGH)' });
  }

  readBody(req, res, (raw) => {
    // counted until the client has the whole answer, so a drain never cuts one short
    active++;
    let counted = true;
    const settle = () => {
      if (!counted) return;
      counted = false;
      req.socket.off('close', settle);
      active--;
      if (draining && !active) drained();
    };
    // each request in flight watches its socket twice; pipelining clients stack those up
    req.socket.setMaxListeners(0);
    res.once('close', settle);
    req.socket.once('close', settle);
    const target = q < 0 ? path : path + url.slice(q);
    const work = routed ? handleChat(req, res, raw, target) : relay(req, res, path === '/models' ? '/v1/models' : target, raw);
    work.catch((err) => {
      console.error('strata-router: unexpected error', err);
      if (!res.headersSent) send(res, 500, { error: 'internal error' });
      else res.destroy();
    });
  });
});

server.on('error', (err) => {
  if (server.listening) return console.error('strata-router:', err.message);
  console.error(`strata-router: cannot listen on ${process.env.BIND || '0.0.0.0'}:${config.port}: ${err.message}`);
  process.exit(1);
});

loadState();
await Promise.all(config.backends.map(pollOne));
for (const b of config.backends) pollLoop(b);
if (config.stateFile) setInterval(saveState, 60_000).unref();

server.listen(config.port, process.env.BIND || '0.0.0.0', () => {
  log(`strata-router on :${config.port} -> ${config.backends.map((b) => `${b.name}=${addr(b)}`).join(', ')}`);
});

// stop taking requests, let the ones in flight finish; a second signal stops waiting
function shutdown() {
  if (draining) return exit();
  draining = true;
  saveState();
  // stop listening but keep connections: http's own close() also destroys those whose answer is still flushing
  net.Server.prototype.close.call(server);
  wake();
  if (active) log(`draining ${active} request(s) for up to ${config.shutdownGraceMs}ms`);
  setTimeout(exit, config.shutdownGraceMs);
  if (!active) drained();
}

// a client still reading an answer when its keep-alive connection drops can report it as cut off (undici does)
function drained() {
  setTimeout(exit, Math.min(1000, config.shutdownGraceMs));
}

function exit() {
  saveState();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// a raw template literal: the page must not contain backticks or dollar-brace
const DASHBOARD = String.raw`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>strata-router</title>
<link rel="icon" href="data:,">
<style>
:root {
  color-scheme: light dark;
  --page: light-dark(#f3f3f0, #0c0c0d);
  --surface: light-dark(#ffffff, #161618);
  --sunken: light-dark(#f6f6f3, #1f1f22);
  --ink: light-dark(#121212, #f3f3f1);
  --ink-2: light-dark(#56554f, #b4b3aa);
  --muted: light-dark(#8b8a84, #7a7972);
  --grid: light-dark(#e7e6e0, #2b2b2e);
  --ring: light-dark(rgba(18, 18, 18, .08), rgba(255, 255, 255, .08));
  --accent: light-dark(#2a78d6, #4b95ee);
  --track: light-dark(#dde9f9, #15345c);
  --good: light-dark(#119c50, #34c172);
  --warning: light-dark(#c98f00, #f0b43a);
  --serious: light-dark(#dd6a39, #f28b5e);
  --critical: light-dark(#cf3636, #f05a5a);
  --wait: light-dark(#c4c3bb, #55555a);
  --solo: light-dark(#bdbcb4, #5d5d62);
}
:root[data-theme=light] { color-scheme: light; }
:root[data-theme=dark] { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--ink); font: 13px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; font-variant-numeric: tabular-nums; }
header { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 16px; padding: 16px 22px 12px; }
h1, h2, h3, h4, p { margin: 0; }
h1 { font-size: 17px; font-weight: 650; letter-spacing: -.01em; }
h2 { font-size: 13px; font-weight: 600; }
h3 { font-size: 15px; font-weight: 600; }
h4 { font-size: 12px; font-weight: 600; color: var(--ink-2); }
.label, .meta, .hint, th, .note td { color: var(--ink-2); font-weight: 400; }
.label { font-size: 12px; display: flex; align-items: center; gap: 5px; }
.muted { color: var(--muted); }
button { font: inherit; font-size: 12px; color: var(--ink-2); background: var(--surface); border: 1px solid var(--ring); border-radius: 7px; padding: 3px 10px; cursor: pointer; }
button:hover { color: var(--ink); }
.seg { display: inline-flex; border: 1px solid var(--ring); border-radius: 7px; overflow: hidden; }
.seg button { border: 0; border-radius: 0; background: none; padding: 3px 9px; white-space: nowrap; }
.seg button[aria-pressed=true] { background: var(--track); color: var(--ink); }
#theme { margin-left: auto; }
#banner { margin: 0 22px 12px; padding: 9px 12px; border: 1px solid var(--critical); border-radius: 10px; background: var(--surface); }
.sprite { position: absolute; width: 0; height: 0; overflow: hidden; }
.ic { width: 12px; height: 12px; fill: currentColor; vertical-align: -1px; flex: none; }
.good { color: var(--good); }
.warning { color: var(--warning); }
.serious { color: var(--serious); }
.critical { color: var(--critical); }
.neutral { color: var(--ink-2); }
.accent { color: var(--accent); }
.card { background: var(--surface); border: 1px solid var(--ring); border-radius: 12px; padding: 14px 16px; box-shadow: 0 1px 2px rgba(0, 0, 0, .04); }
.dim { transition: opacity .3s; }
.stale .dim { opacity: .45; }
.kpis { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; padding: 0 22px 12px; }
.kpi { display: flex; flex-direction: column; min-height: 132px; }
.val { font-size: 30px; font-weight: 650; line-height: 1.15; letter-spacing: -.02em; margin-top: 6px; white-space: nowrap; }
.val small { font-size: 13px; font-weight: 500; color: var(--ink-2); letter-spacing: 0; margin-left: 4px; }
.kpi .hint { margin-top: auto; padding-top: 4px; }
.spark { display: block; width: 100%; height: 30px; margin: 6px 0 0; overflow: visible; }
.spark .line { fill: none; stroke: var(--accent); stroke-width: 1.5; stroke-linejoin: round; vector-effect: non-scaling-stroke; }
.spark.mini { height: 16px; margin: 3px 0 0; }
.spark .area { fill: var(--track); opacity: .7; }
.spark .col { fill: var(--accent); opacity: .75; }
.meter { display: block; height: 8px; border-radius: 4px; background: var(--track); overflow: hidden; margin: 12px 0 4px; }
.meter i { display: block; height: 100%; background: var(--accent); border-radius: 4px; }
.meter.sm { height: 4px; margin: 5px 0 1px; }
.chips { display: flex; flex-wrap: wrap; gap: 8px; padding: 0 22px 12px; }
.chip { display: inline-flex; align-items: center; gap: 6px; background: var(--surface); border: 1px solid var(--ring); border-radius: 999px; padding: 4px 12px; color: var(--ink-2); }
.chip b { color: var(--ink); font-weight: 600; }
.chip.zero { opacity: .55; }
.timeline { margin: 0 22px 12px; }
.timeline .head { margin-bottom: 10px; }
.tl { display: block; width: 100%; overflow: visible; }
.tl text { font-size: 11px; fill: var(--ink-2); }
.tl .lane-name { font-size: 12px; font-weight: 600; fill: var(--ink); }
.tl .gridline { stroke: var(--grid); }
.tl .lane-bg { fill: var(--sunken); }
.tl .now { stroke: var(--accent); stroke-dasharray: 2 3; }
.tl .waitbar { fill: var(--wait); }
.tl g { transition: opacity .15s; }
.legend { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 16px; margin-top: 10px; }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.swatch { display: inline-block; width: 9px; height: 9px; border-radius: 3px; flex: none; }
.swatch.line { height: 3px; width: 14px; border-radius: 2px; background: var(--wait); }
.swatch.outline { background: none; border: 1.5px solid var(--serious); }
main { display: grid; grid-template-columns: minmax(0, 1fr) minmax(540px, 700px); gap: 12px; padding: 0 22px 22px; align-items: start; }
.engines { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 12px; align-items: start; }
.head { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
.title { display: flex; align-items: center; gap: 8px; min-width: 0; }
.pill { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; padding: 2px 9px; border-radius: 999px; background: var(--sunken); border: 1px solid var(--ring); white-space: nowrap; }
.tag { font-size: 11px; color: var(--ink-2); background: var(--sunken); border: 1px solid var(--ring); border-radius: 5px; padding: 0 6px; white-space: nowrap; }
.badges { display: flex; gap: 5px; min-width: 0; overflow: hidden; justify-content: flex-end; }
.badges .pill { flex: none; }
.one { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.down .tiles, .down .slots, .down .convs { opacity: .55; }
.meta.err { color: var(--critical); }
.slots { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); gap: 6px; margin: 12px 0 6px; }
.slot { display: grid; grid-template-rows: 18px 18px 8px; align-content: start; height: 62px; min-width: 0; overflow: hidden; padding: 7px 9px; border-radius: 8px; border: 1px dashed var(--grid); color: var(--muted); font-size: 12px; }
.slot.busy { border: 1px solid var(--ring); background: var(--sunken); color: var(--ink); }
.slot .top { display: flex; align-items: center; gap: 6px; min-width: 0; }
.slot .key { overflow: hidden; text-overflow: ellipsis; }
.slot .meta { margin-left: auto; white-space: nowrap; }
.slot .what { color: var(--ink-2); font-size: 11px; line-height: 18px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.slot .meter.sm { margin-top: 3px; height: 4px; }
.hw { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px 14px; margin: 12px 0 0; padding: 10px 0 0; border-top: 1px solid var(--grid); font-size: 12px; }
.hw .label { font-size: 11px; }
.hw b { display: block; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.tiles { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; margin: 10px 0 14px; }
.tile { background: var(--sunken); border-radius: 8px; padding: 8px 10px; min-width: 0; }
.tile b { display: block; font-size: 16px; font-weight: 600; margin-top: 1px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.tile .hint { display: block; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.convs { border-top: 1px solid var(--grid); padding-top: 12px; }
.convs h4 { margin-bottom: 6px; }
.convs button { margin-top: 8px; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 4px 5px; border-bottom: 1px solid var(--grid); white-space: nowrap; text-align: left; }
th { font-size: 11px; font-weight: 500; position: sticky; top: 0; background: var(--surface); }
.r { text-align: right; }
.key { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
td.key .swatch { margin-right: 6px; }
.bar { display: inline-block; height: 6px; margin-right: 6px; border-radius: 0 3px 3px 0; background: var(--accent); vertical-align: 1px; }
.feed { position: sticky; top: 12px; display: flex; flex-direction: column; max-height: calc(100vh - 24px); }
.feed h2 { margin-bottom: 10px; }
.mix { display: flex; flex-wrap: wrap; gap: 4px 14px; margin-bottom: 10px; }
.mix .zero { opacity: .5; }
.scroll { overflow-y: auto; overflow-x: hidden; min-height: 0; margin: 0 -10px; padding: 0 4px; }
.feed { container-type: inline-size; }
.feed table { font-size: 12px; table-layout: fixed; }
.feed th, .feed td { overflow: hidden; text-overflow: ellipsis; }
.feed .note td { overflow: visible; }
.c-kind { width: 76px; } .c-key { width: 88px; } .c-turn { width: 38px; } .c-num { width: 52px; } .c-time { width: 66px; } .c-fin { width: 62px; }
@container (max-width: 600px) { .c-fin { display: none; } }
@container (max-width: 500px) { .c-cached, .c-turn { display: none; } }
.has-note td { border-bottom: 0; }
.tip { position: fixed; z-index: 10; max-width: 360px; padding: 7px 10px; border-radius: 8px; background: var(--ink); color: var(--surface); font-size: 12px; line-height: 1.45; white-space: pre-line; pointer-events: none; box-shadow: 0 4px 16px rgba(0, 0, 0, .18); }
.help { margin: -4px 0 10px; }
.help summary { cursor: pointer; color: var(--accent); font-size: 12px; width: max-content; }
.help dl { display: grid; grid-template-columns: max-content 1fr; gap: 6px 12px; margin: 8px 0 4px; font-size: 12px; }
.help dt { display: flex; align-items: center; gap: 5px; font-weight: 600; }
.help dd { margin: 0; color: var(--ink-2); }
.note td { padding-top: 0; white-space: normal; }
@keyframes pulse { 50% { opacity: .35; } }
.pulse { animation: pulse 1.4s ease-in-out infinite; }
@media (prefers-reduced-motion: reduce) { .pulse { animation: none; } }
@media (max-width: 1250px) { main { grid-template-columns: minmax(0, 1fr); } .feed { position: static; max-height: none; } .scroll { max-height: 70vh; } }
@media (max-width: 900px) { .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 560px) { .kpis { grid-template-columns: minmax(0, 1fr); } .tiles { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
</style>
<style id="hl"></style>
</head>
<body>
<svg class="sprite" id="sprite" aria-hidden="true">
<symbol id="i-sticky" viewBox="0 0 12 12"><circle cx="6" cy="6" r="4.5"/></symbol>
<symbol id="i-cold" viewBox="0 0 12 12"><circle cx="6" cy="6" r="3.7" fill="none" stroke="currentColor" stroke-width="1.6"/></symbol>
<symbol id="i-queued" viewBox="0 0 12 12"><path d="M2.5 1h7v2.2L7.2 6l2.3 2.8V11h-7V8.8L4.8 6 2.5 3.2z"/></symbol>
<symbol id="i-spill" viewBox="0 0 12 12"><path d="M1 4.4h5V1.4L11 6l-5 4.6V7.6H1z"/></symbol>
<symbol id="i-overflow" viewBox="0 0 12 12"><path d="M1 1.5h10v2.2H1zm0 3.4h10v2.2H1zm0 3.4h10v2.2H1z"/></symbol>
<symbol id="i-retry" viewBox="0 0 12 12"><path d="M6 1.5A4.5 4.5 0 1 0 10.5 6H8.7A2.7 2.7 0 1 1 6 3.3v1.3l2.6-2.2L6 .2z"/></symbol>
<symbol id="i-failed" viewBox="0 0 12 12"><path d="M2.2 3.6l1.4-1.4L6 4.6l2.4-2.4 1.4 1.4L7.4 6l2.4 2.4-1.4 1.4L6 7.4 3.6 9.8 2.2 8.4 4.6 6z"/></symbol>
<symbol id="i-gone" viewBox="0 0 12 12"><path d="M1.5 5h9v2h-9z"/></symbol>
<symbol id="i-warn" viewBox="0 0 12 12"><path d="M6 1l5.5 10H.5z"/></symbol>
<symbol id="i-bolt" viewBox="0 0 12 12"><path d="M7 .5L2 7h3.3L4.5 11.5 10 5H6.6z"/></symbol>
<symbol id="i-clock" viewBox="0 0 12 12"><path d="M6 .8a5.2 5.2 0 1 0 0 10.4A5.2 5.2 0 0 0 6 .8zm0 1.6a3.6 3.6 0 1 1 0 7.2 3.6 3.6 0 0 1 0-7.2zM5.3 3.3v3.2l2.5 1.5.7-1.1-1.9-1.1V3.3z"/></symbol>
<symbol id="i-cache" viewBox="0 0 12 12"><path d="M6 1C3.2 1 1 1.9 1 3v6c0 1.1 2.2 2 5 2s5-.9 5-2V3c0-1.1-2.2-2-5-2zm0 1.4c2.3 0 3.6.6 3.6.8S8.3 4 6 4 2.4 3.4 2.4 3.2 3.7 2.4 6 2.4z"/></symbol>
</svg>
<template id="icon"><svg class="ic" viewBox="0 0 12 12" aria-hidden="true"><use href="#i-sticky"/></svg></template>
<header>
<h1>strata-router</h1>
<span id="state"></span>
<span class="meta" id="uptime"></span>
<button id="theme" type="button"></button>
</header>
<div id="banner" hidden></div>
<noscript><p class="card">This page needs JavaScript; GET /stats has the same numbers.</p></noscript>
<section class="kpis dim">
<div class="card kpi" data-tip="Prompt tokens an engine already had cached, out of all the prompt tokens in answers the router relayed. The higher, the less prompt the engines had to read again.">
<div class="label" id="hit-label">Cache hit ratio</div>
<div class="val" id="hit">–</div>
<span class="meter"><i id="hit-bar"></i></span>
<p class="hint" id="hit-sub"></p>
</div>
<div class="card kpi" data-tip="From a request reaching the router to the first token of its answer, waiting for a slot included. 'At the engine' leaves the wait out.">
<div class="label" id="ttft-label">Time to first token</div>
<div class="val" id="ttft">–</div>
<svg class="spark" id="ttft-spark" aria-hidden="true"></svg>
<p class="hint" id="ttft-sub"></p>
</div>
<div class="card kpi" data-tip="Requests waiting in the router for a free engine slot right now. The chart shows how long recent requests waited.">
<div class="label" id="waiting-label">Waiting for a slot</div>
<div class="val" id="waiting">–</div>
<svg class="spark" id="wait-spark" aria-hidden="true"></svg>
<p class="hint" id="waiting-sub"></p>
</div>
<div class="card kpi" data-tip="Requests finished per minute, and answer tokens generated per second, over the last 15 minutes. The bars are requests per minute.">
<div class="label" id="tput-label">Throughput</div>
<div class="val" id="tput">–</div>
<svg class="spark" id="tput-spark" aria-hidden="true"></svg>
<p class="hint" id="tput-sub"></p>
</div>
</section>
<section class="chips dim" id="counters"></section>
<section class="card timeline dim">
<div class="head"><h2>Where requests ran <span class="hint" id="tl-sub"></span></h2><div class="seg" id="window" role="group" aria-label="Time window"></div></div>
<div id="tl-box"></div>
<div class="legend hint">
<span><i class="swatch" id="legend-conv"></i>one request on that engine, colored by conversation</span>
<span><i class="swatch" style="background: var(--solo)"></i>single-turn conversation</span>
<span><i class="swatch line"></i>waiting for a slot</span>
<span><i class="swatch outline"></i>spill, retry, failure or slow first token</span>
</div>
</section>
<main class="dim">
<section class="engines" id="engines"></section>
<aside class="card feed">
<h2>Routing decisions <span class="hint" id="feed-n"></span></h2>
<div class="mix" id="mix"></div>
<details class="help"><summary>What do these mean?</summary><dl id="help-list"></dl></details>
<div class="scroll">
<table>
<colgroup><col class="c-kind"><col class="c-key"><col class="c-turn"><col><col class="c-num"><col class="c-num c-cached"><col class="c-num"><col class="c-time"><col class="c-num"><col class="c-fin"></colgroup>
<thead><tr><th>Decision</th><th>Conversation</th><th class="r c-turn">Turn</th><th>Engine</th><th class="r">Prompt</th><th class="r c-cached">Cached</th><th class="r">Wait</th><th class="r">1st token</th><th class="r">Total</th><th class="r c-fin">Finished</th></tr></thead>
<tbody id="live"></tbody>
<tbody id="rows"></tbody>
</table>
<p class="hint" id="empty">No requests yet. Each routed request shows up here when it finishes.</p>
</div>
</aside>
</main>
<script>
const POLL = 1500;
const KINDS = {
  sticky: ['good', 'Went straight to its home engine, which still had this conversation cached, so only the new part of the prompt was read.'],
  cold: ['neutral', 'A new conversation, or one whose home engine restarted and lost its cache: the engine read the whole prompt from scratch.'],
  queued: ['warning', 'Every slot it could use was busy, so it waited in the router until one freed up (the Wait column), then ran on its home engine.'],
  spill: ['warning', 'Its home engine was down, or stayed busy longer than reading the whole prompt on another engine would take, so it moved. The new engine read the prompt from scratch and is its home from now on.'],
  overflow: ['serious', 'Every engine stayed full for MAX_QUEUE_WAIT, so the router stopped holding it back and handed it to an engine’s own queue (its home’s, if healthy) to wait there for a slot.'],
  retry: ['serious', 'The first engine failed before it started answering (refused or reset the connection, timed out, or answered 5xx), so another engine took the request.'],
  failed: ['critical', 'Ended with an error; the line under the row says what went wrong.'],
  gone: ['muted', 'The client disconnected before the answer was complete, so the router cancelled the request at the engine.'],
};
const FLAGS = {
  slow: ['warn', 'serious', 'The engine took far longer to start answering than the uncached part of the prompt needs, so it may have stalled.'],
  restarts: ['retry', 'warning', 'An engine restarted, so its cache is empty: its conversations start cold and go wherever there is room.'],
};
const COUNTERS = [
  ['requests', 'Requests', null, 'Requests the router placed on an engine since it started.'],
  ['cold', 'New conversations', 'cold', 'Conversations the router saw for the first time: each needs one full prompt read.'],
  ['migrated', 'Migrated', 'spill', 'Times a conversation’s home moved to another engine (spills, and retries or overflows that ended up elsewhere). Each move costs one full prompt read.'],
  ['queued', 'Queued', 'queued', 'Requests that waited in the router for a free slot.'],
  ['overflow', 'Overflow', 'overflow'],
  ['retried', 'Retried', 'retry', 'Requests another engine took after the first one failed before answering.'],
  ['errors', 'Errors', 'failed', 'Engine failures, including the ones a retry recovered from.'],
  ['slow', 'Slow first token', 'slow'],
  ['restarts', 'Engine restarts', 'restarts'],
  ['completion_tokens', 'Tokens generated', null, 'Answer tokens the engines reported.'],
];
const WINDOWS = [['2 min', 12e4], ['10 min', 6e5], ['1 h', 36e5]];
const THEMES = ['auto', 'light', 'dark'];
const SHOWN = 10;
const $ = (id) => document.getElementById(id);
const SVG = $('sprite').namespaceURI;
const base = location.pathname.replace(/\/+dashboard\/*$/, '');
const tpl = $('icon').content.firstElementChild;
const compact = new Intl.NumberFormat('en', { notation: 'compact', maximumSignificantDigits: 3 });
const whole = new Intl.NumberFormat('en');
const count = (n) => (n == null ? '–' : n < 1e4 ? whole.format(n) : compact.format(n));
const toks = (n) => (n == null ? '–' : compact.format(n));
const rate = (n) => (n == null ? '–' : whole.format(Math.round(n)) + ' tok/s');
const pct = (r) => (r == null ? '–' : Math.round(100 * r) + '%');
const mib = (n) => (n >= 1024 ? +(n / 1024).toFixed(1) + ' GiB' : n + ' MiB');
const clock = (t) => new Date(t).toLocaleTimeString([], { hour12: false });
const route = (e) => (e.home && e.backend && e.home !== e.backend ? e.home + ' → ' + e.backend : e.backend || '–');
const now = () => Date.now() + skew;
const expanded = new Set();
let feed = [], live = [], cursor = 0, started = null, lastOk = 0, failing = false, skew = 0, data = null, drawn = '', theme = 'auto', hovered = '', span = 6e5;
let turnsOf = new Map();
let settings = {}, helpFor = '';

const explain = (k) => KINDS[k][1].replace('MAX_QUEUE_WAIT', settings.max_queue_wait_ms ? dur(settings.max_queue_wait_ms) + ' (MAX_QUEUE_WAIT)' : 'MAX_QUEUE_WAIT');

function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  for (const k in props) {
    const v = props[k];
    if (v == null || v === false) continue;
    if (k === 'style') Object.assign(el.style, v);
    else if (k === 'onclick') el.onclick = v;
    else el.setAttribute(k, v);
  }
  el.append(...kids.flat(2).filter((x) => x != null && x !== false));
  return el;
}

function s(tag, props, ...kids) {
  const el = document.createElementNS(SVG, tag);
  for (const k in props) {
    const v = props[k];
    if (v == null || v === false) continue;
    if (k === 'style') Object.assign(el.style, v);
    else el.setAttribute(k, v);
  }
  el.append(...kids.flat(2).filter((x) => x != null && x !== false));
  return el;
}

// replaceChildren would print false and null
const fill = (el, ...kids) => el.replaceChildren(...kids.flat(2).filter((x) => x != null && x !== false));

function icon(name, tone) {
  const svg = tpl.cloneNode(true);
  svg.firstElementChild.setAttribute('href', '#i-' + name);
  svg.classList.add(...tone.split(' '));
  return svg;
}

const q = (a, p) => {
  if (!a.length) return null;
  const x = a.slice().sort((m, n) => m - n);
  return x[Math.min(x.length - 1, Math.floor(p * x.length))];
};

// a stable hue per conversation, so its turns can be followed across engines
function color(key) {
  if ((turnsOf.get(key) || 1) < 2) return 'var(--solo)';
  return 'oklch(0.68 0.15 ' + (parseInt(key.slice(0, 4), 16) % 360) + ')';
}
const swatch = (key) => h('i', { class: 'swatch', style: { background: color(key) } });

function dur(ms) {
  if (ms == null) return '–';
  if (ms < 1000) return Math.round(ms) + ' ms';
  if (ms < 1e4) return (ms / 1000).toFixed(1) + ' s';
  const sec = Math.round(ms / 1000), m = Math.floor(sec / 60), hr = Math.floor(m / 60), d = Math.floor(hr / 24);
  return d ? d + ' d ' + (hr % 24) + ' h' : hr ? hr + ' h ' + (m % 60) + ' m' : m ? m + ' m ' + (sec % 60) + ' s' : sec + ' s';
}

function ago(t) {
  const sec = Math.round((now() - t) / 1000);
  if (sec < 2) return 'now';
  return (sec < 60 ? sec + ' s' : sec < 3600 ? Math.floor(sec / 60) + ' m' : sec < 86400 ? Math.floor(sec / 3600) + ' h' : Math.floor(sec / 86400) + ' d') + ' ago';
}

async function get(path) {
  const res = await fetch(base + path, { cache: 'no-store', signal: AbortSignal.timeout(4000) });
  if (!res.ok) throw new Error(path + ' answered ' + res.status);
  return res.json();
}

async function tick() {
  try {
    const [st, c, first] = await Promise.all([get('/stats'), get('/stats/conversations'), get('/stats/events?since=' + cursor)]);
    let e = first;
    if (started && e.started !== started) {
      feed = [];
      e = await get('/stats/events?since=0');
    }
    started = e.started;
    feed = feed.concat(e.events).slice(-200);
    cursor = e.last;
    live = e.live;
    skew = e.now - Date.now();
    data = [st, c.conversations];
    lastOk = Date.now();
    failing = false;
  } catch {
    failing = true;
  }
  try {
    if (!failing) render();
  } finally {
    status();
    setTimeout(tick, POLL);
  }
}

// a line with a soft area under it, or columns (null: no data for that one); a line clips past its 95th percentile
function spark(el, values, columns, max) {
  const W = el.clientWidth || 200, H = 30;
  el.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  if (!values.length) return el.replaceChildren();
  const top = max || Math.max(1, ...(columns ? values.filter((v) => v != null) : [q(values, 0.95) * 1.15]));
  const y = (v) => H - 1 - (Math.min(v, top) / top) * (H - 3);
  if (columns) {
    const w = W / values.length;
    return fill(el, values.map((v, i) => v != null && s('rect', { class: 'col', x: i * w + 1, y: y(v), width: Math.max(1, w - 2), height: Math.max(v ? 1.5 : 0, H - 1 - y(v)), rx: 1.5 })));
  }
  const step = values.length > 1 ? W / (values.length - 1) : W;
  const pts = values.map((v, i) => (i * step).toFixed(1) + ',' + y(v).toFixed(1));
  el.replaceChildren(s('path', { class: 'area', d: 'M0,' + H + ' L' + pts.join(' L') + ' L' + ((values.length - 1) * step).toFixed(1) + ',' + H + ' Z' }), s('polyline', { class: 'line', points: pts.join(' ') }));
}

function render() {
  const [st, convs] = data;
  const t = st.stats;
  if (JSON.stringify(st.settings || {}) !== helpFor) {
    helpFor = JSON.stringify(st.settings || {});
    settings = st.settings || {};
    $('help-list').replaceChildren(...Object.keys(KINDS).flatMap((k) => [h('dt', null, icon(k, KINDS[k][0]), k), h('dd', null, explain(k))]));
  }
  turnsOf = new Map(convs.map((c) => [c.key, c.turns]));
  for (const e of feed) turnsOf.set(e.key, Math.max(turnsOf.get(e.key) || 0, e.turn));
  for (const x of live) turnsOf.set(x.key, Math.max(turnsOf.get(x.key) || 0, x.turn));
  $('legend-conv').style.background = 'oklch(0.68 0.15 250)';

  const ratio = t.prompt_tokens ? t.cached_tokens / t.prompt_tokens : null;
  $('hit-label').replaceChildren(icon('cache', 'accent'), 'Cache hit ratio');
  $('hit').textContent = ratio == null ? '–' : (100 * ratio).toFixed(1) + '%';
  $('hit-bar').style.width = 100 * (ratio || 0) + '%';
  $('hit-sub').textContent = ratio == null ? 'No token usage reported yet' : toks(t.cached_tokens) + ' of ' + toks(t.prompt_tokens) + ' prompt tokens were already in an engine cache';

  const ok = feed.filter((e) => e.status === 200 && !e.error);
  const ttfts = ok.filter((e) => e.ttft_ms != null);
  const engineSide = ttfts.map((e) => e.engine_ttft_ms).filter((x) => x != null);
  const p50 = q(ttfts.map((e) => e.ttft_ms), 0.5);
  fill($('ttft-label'), icon('bolt', 'accent'), 'Time to first token', ttfts.length > 0 && h('span', { class: 'muted' }, '· last ' + ttfts.length));
  fill($('ttft'), dur(p50), p50 != null && h('small', null, 'median'));
  spark($('ttft-spark'), ttfts.slice(-60).map((e) => e.ttft_ms));
  $('ttft-sub').textContent = !ttfts.length ? 'Measured on streamed answers' : '90th percentile ' + dur(q(ttfts.map((e) => e.ttft_ms), 0.9)) + (engineSide.length ? ' · at the engine ' + dur(q(engineSide, 0.5)) : '');

  const holding = st.backends.reduce((n, b) => n + b.claimed, 0);
  const waits = ok.filter((e) => e.queued).map((e) => e.wait_ms);
  $('waiting-label').replaceChildren(icon('queued', st.waiting ? 'warning' : 'muted'), 'Waiting for a slot');
  fill($('waiting'), whole.format(st.waiting), h('small', null, 'now'));
  spark($('wait-spark'), ok.slice(-60).map((e) => e.wait_ms));
  $('waiting-sub').textContent = st.waiting ? (holding ? holding + ' holding out for a busy home engine' : 'Every engine is full') : waits.length ? waits.length + ' of the last ' + ok.length + ' waited, median ' + dur(q(waits, 0.5)) : 'Every request found a free slot';

  // the window starts at the router's start, or at the oldest event once the ring is full
  const end = now(), mins = 15;
  const from = Math.max(end - mins * 6e4, Date.parse(t.started), feed.length >= 200 ? feed[0].arrived + feed[0].ms : 0);
  const covered = end - from;
  const recent = ok.filter((e) => e.arrived + e.ms > from);
  const shown = Math.max(1, Math.min(mins, Math.ceil(covered / 6e4)));
  const perMin = Array.from({ length: mins }, (_, i) => (i < mins - shown ? null : recent.filter((e) => Math.floor((end - e.arrived - e.ms) / 6e4) === mins - 1 - i).length));
  const gen = recent.reduce((n, e) => n + (e.completion_tokens || 0), 0);
  $('tput-label').replaceChildren(icon('clock', 'accent'), 'Throughput', h('span', { class: 'muted' }, '· last ' + mins + ' min'));
  fill($('tput'), covered > 1000 ? (recent.length / (covered / 6e4)).toFixed(1) : '–', h('small', null, 'requests/min'));
  spark($('tput-spark'), perMin, true);
  $('tput-sub').textContent = covered > 1000 ? whole.format(Math.round(gen / (covered / 1000))) + ' tok/s generated · ' + recent.length + ' requests' : 'Nothing finished yet';

  $('counters').replaceChildren(...COUNTERS.map(([k, label, kind, about]) => {
    const flag = kind && (FLAGS[kind] || [kind, KINDS[kind][0], explain(kind)]);
    return h('span', { class: t[k] ? 'chip' : 'chip zero', 'data-tip': about || (flag && flag[2]) }, flag && icon(flag[0], t[k] ? flag[1] : 'muted'), label, h('b', null, count(t[k])));
  }));
  $('uptime').textContent = 'Up ' + dur(now() - Date.parse(t.started)) + ' · ' + st.backends.filter((b) => b.healthy).length + ' of ' + st.backends.length + ' engines healthy';

  timeline(st);

  const active = new Set(live.map((x) => x.key));
  const serving = new Map(live.filter((x) => x.backend).map((x) => [x.key, x.backend]));
  const homes = new Map(st.backends.map((b) => [b.name, []]));
  const homeless = [];
  // a first turn in progress already builds its kv on the engine serving it
  for (const c of convs) (homes.get(c.backend || serving.get(c.key)) || homeless).push(c);
  const top = Math.max(1, ...convs.map((c) => c.prompt_tokens));
  const cards = st.backends.map((b) => engine(b, homes.get(b.name), active, top));
  if (homeless.length) cards.push(h('article', { class: 'card' }, h('h3', null, 'No home engine'), convList('', homeless, active, top)));
  $('engines').replaceChildren(...cards);

  $('live').replaceChildren(...live.slice().reverse().map(liveRow));
  $('empty').hidden = feed.length + live.length > 0;
  if (drawn === started + cursor) return;
  drawn = started + cursor;
  const tally = {};
  for (const e of feed) tally[e.kind] = (tally[e.kind] || 0) + 1;
  $('feed-n').textContent = feed.length ? '· last ' + feed.length + ' · hover a row to trace its conversation' : '';
  $('mix').replaceChildren(...Object.keys(KINDS).map((k) => h('span', { class: tally[k] ? null : 'zero', 'data-tip': explain(k) }, icon(k, KINDS[k][0]), ' ' + k + ' ', h('b', null, tally[k] || 0))));
  $('rows').replaceChildren(...feed.slice().reverse().flatMap(row));
}

function tickStep(ms) {
  for (const step of [1e4, 15e3, 3e4, 6e4, 12e4, 3e5, 6e5, 9e5, 18e5]) if (ms / step <= 8) return step;
  return 36e5;
}

// one lane per engine, each request a bar from its slot to its last byte; overlapping requests stack
function timeline(st) {
  const box = $('tl-box');
  const W = Math.max(420, box.clientWidth), LW = 128, PAD = 6, BAR = 9, GAP = 3, AXIS = 20;
  const end = now(), start = end - span;
  const X = (t) => LW + ((Math.max(start, Math.min(end, t)) - start) / span) * (W - LW - PAD);
  const lanes = new Map(st.backends.map((b) => [b.name, []]));
  const waiting = [];
  for (const e of feed) {
    const t1 = e.arrived + e.ms;
    if (t1 < start || !lanes.has(e.backend)) continue;
    lanes.get(e.backend).push({ key: e.key, t0: e.arrived, ts: e.started || e.arrived + (e.wait_ms || 0), t1, e });
  }
  for (const x of live) {
    if (lanes.has(x.backend)) lanes.get(x.backend).push({ key: x.key, t0: x.arrived, ts: x.started || x.arrived, t1: end, x });
    else if (!x.backend) waiting.push({ key: x.key, t0: x.arrived, ts: end, t1: end, x });
  }
  lanes.set('', waiting);

  const kids = [];
  let y = 0, n = 0;
  const laneKids = [];
  for (const [name, items] of lanes) {
    const b = st.backends.find((x) => x.name === name);
    items.sort((m, k) => m.t0 - k.t0);
    // a fixed number of rows per lane, so the page below doesn't move; past that, bars share the row that frees first
    const rows = b ? Math.max(2, b.batch) + 2 : 2;
    const ends = [];
    for (const it of items) {
      let r = ends.findIndex((t) => t <= it.t0);
      if (r < 0 && ends.length < rows) r = ends.push(0) - 1;
      if (r < 0) r = ends.indexOf(Math.min(...ends));
      ends[r] = Math.max(ends[r], it.t1);
      it.row = r;
    }
    const lh = rows * (BAR + GAP) + GAP + 8;
    laneKids.push(s('rect', { class: 'lane-bg', x: LW, y, width: W - LW - PAD, height: Math.max(lh, 34), rx: 6 }));
    laneKids.push(s('text', { class: 'lane-name', x: 0, y: y + 15 }, name || 'Waiting'));
    laneKids.push(s('text', { x: 0, y: y + 30 }, b ? (b.healthy ? Math.min(b.batch, b.inflight + (b.direct || 0)) + ' of ' + b.batch + ' slots busy' : 'down') : items.length + ' without a slot'));
    for (const it of items) {
      if (it.t1 < start) continue;
      n++;
      const top = y + 4 + GAP + it.row * (BAR + GAP);
      const x0 = X(it.t0), xs = X(it.ts), x1 = X(it.t1);
      const e = it.e;
      const tone = e && (e.slow ? 'serious' : ['spill', 'retry', 'overflow', 'failed'].includes(e.kind) ? KINDS[e.kind][0] : null);
      kids.push(s('g', { 'data-key': it.key, 'data-tip': tip(it), class: it.x && it.x.backend ? 'pulse' : null },
        xs - x0 > 1 && s('rect', { class: 'waitbar', x: x0, y: top + BAR / 2 - 1.5, width: xs - x0, height: 3, rx: 1.5 }),
        name && s('rect', { x: xs, y: top, width: Math.max(3, x1 - xs), height: BAR, rx: 2.5, style: { fill: color(it.key), stroke: tone ? 'var(--' + tone + ')' : null, strokeWidth: tone ? 1.5 : null } }),
      ));
    }
    y += Math.max(lh, 34) + 6;
  }
  const H = y + AXIS - 6;
  const step = tickStep(span);
  for (let t = Math.ceil(start / step) * step; t <= end; t += step) {
    kids.unshift(s('line', { class: 'gridline', x1: X(t), x2: X(t), y1: 0, y2: H - AXIS + 2 }));
    kids.push(s('text', { x: X(t), y: H - 4, 'text-anchor': 'middle' }, new Date(t).toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit', second: step < 6e4 ? '2-digit' : undefined })));
  }
  kids.push(s('line', { class: 'now', x1: X(end), x2: X(end), y1: 0, y2: H - AXIS + 2 }));
  const svg = s('svg', { class: 'tl', width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, role: 'img', 'aria-label': 'Requests per engine over the last ' + dur(span) }, laneKids, kids);
  box.replaceChildren(svg);
  $('tl-sub').textContent = '· ' + n + ' request' + (n === 1 ? '' : 's') + ' in the last ' + dur(span) + ' · hover one to trace its conversation';
}

function tip(it) {
  const e = it.e, x = it.x;
  const head = it.key.slice(0, 8) + ' · turn ' + (e || x).turn;
  if (x) return head + (x.backend ? ' · running on ' + x.backend + ' for ' + dur(now() - (x.started || x.arrived)) : ' · waiting ' + dur(now() - x.arrived) + (x.home ? ' for ' + x.home : ''));
  const parts = [head + ' · ' + e.kind + ' · ' + route(e)];
  if (e.queued) parts.push('waited ' + dur(e.wait_ms));
  if (e.ttft_ms != null) parts.push('first token ' + dur(e.ttft_ms));
  parts.push('total ' + dur(e.ms));
  if (e.prompt_tokens) parts.push(toks(e.prompt_tokens) + ' prompt tokens, ' + Math.round((100 * e.cached_tokens) / e.prompt_tokens) + '% cached');
  if (e.slow) parts.push('slow: ' + dur(e.engine_ttft_ms) + ' at the engine, about ' + dur(e.expected_ttft_ms) + ' expected');
  if (e.error) parts.push(e.error);
  return parts.join('\n');
}

function decodeOf(b) {
  if (b.tok_s_mean) return b.tok_s_mean;
  const r = feed.filter((e) => e.backend === b.name && e.completion_tokens > 20 && e.ttft_ms != null && e.ms > e.ttft_ms).map((e) => e.completion_tokens / ((e.ms - e.ttft_ms) / 1000));
  return q(r.slice(-30), 0.5);
}

function tile(label, value, sub, extra, about) {
  return h('div', { class: 'tile', 'data-tip': about }, h('span', { class: 'label' }, label), h('b', null, value), extra, h('span', { class: 'hint' }, sub));
}

// one slot as the engine reports it: reading the prompt or answering, for the conversation the router matched
function work(a, direct) {
  const reading = a.state === 'reading';
  const total = a.prompt_total || a.prompt_tokens;
  const what = reading
    ? 'reading ' + (a.prompt_read != null && total ? toks(a.prompt_read) + ' / ' + toks(total) : toks(a.prompt_tokens) + ' tokens')
    : 'writing ' + whole.format(a.generated || 0) + ' tok' + (a.tok_s ? ' · ' + whole.format(Math.round(a.tok_s)) + ' tok/s' : '');
  const full = reading ? 'Reading a ' + toks(a.prompt_tokens) + '-token prompt' : 'Answering: ' + whole.format(a.generated || 0) + ' tokens so far' + (a.tok_s ? ', ' + rate(a.tok_s) : '');
  const progress = reading ? (a.prompt_read != null && total ? a.prompt_read / total : null) : a.max_tokens ? (a.generated || 0) / a.max_tokens : null;
  const who = a.key ? 'Conversation ' + a.key : direct ? 'Sent to the engine directly, not through the router, which counts this slot as taken.' : 'A request the router couldn’t match to one it sent.';
  return slotBox('slot busy', a.key, who + '\n' + full + (progress != null ? ' (' + Math.round(100 * progress) + '%)' : ''),
    a.key ? [swatch(a.key), h('span', { class: 'key' }, a.key.slice(0, 8))] : direct ? [icon('bolt', 'warning'), h('span', null, 'direct')] : h('span', null, 'request'),
    a.elapsed_s != null ? dur(a.elapsed_s * 1000) : '', what, progress);
}

// every slot box has the same three lines, so the card doesn't jump as slots fill and empty
function slotBox(cls, key, tip, label, age, what, progress) {
  return h('div', { class: cls, 'data-key': key || null, 'data-tip': tip },
    h('div', { class: 'top' }, label, h('span', { class: 'meta' }, age)),
    h('div', { class: 'what' }, what),
    h('span', { class: 'meter sm', style: { visibility: progress == null ? 'hidden' : null } }, h('i', { style: { width: Math.min(100, 100 * (progress || 0)) + '%' } })));
}

const gib = (bytes) => (bytes / 2 ** 30).toFixed(1);
const mbs = (mb) => (mb >= 1000 ? (mb / 1000).toFixed(1) + ' GB/s' : mb >= 1 ? Math.round(mb) + ' MB/s' : Math.round(mb * 1000) + ' KB/s');

function hwCell(label, value, share, series, about) {
  let chart = null;
  if (series && series.length > 1) {
    chart = s('svg', { class: 'spark mini', preserveAspectRatio: 'none', 'aria-hidden': 'true' });
    spark(chart, series.map((v) => v || 0), false, 100);
  }
  return h('div', { 'data-tip': about }, h('span', { class: 'label' }, label), h('b', null, value), chart, !chart && share != null && h('span', { class: 'meter sm' }, h('i', { style: { width: Math.min(100, 100 * share) + '%' } })));
}

function hardware(b) {
  const w = b.hardware;
  if (!w || !Object.keys(w).length) return null;
  const cells = [];
  if (w.gpu_util != null) cells.push(hwCell('GPU · last minute', Math.round(w.gpu_util) + '%', w.gpu_util / 100, b.history && b.history.gpu_util, 'GPU load now; the chart is the last minute.'));
  if (w.gpu_mem_total) cells.push(hwCell('VRAM', gib(w.gpu_mem_used) + ' of ' + gib(w.gpu_mem_total) + ' GiB', w.gpu_mem_used / w.gpu_mem_total, null, 'GPU memory in use. Strata fills most of it with its expert cache on purpose.'));
  if (w.gpu_power != null) cells.push(hwCell('Power', Math.round(w.gpu_power) + ' W' + (w.gpu_temp != null ? ' · ' + Math.round(w.gpu_temp) + ' °C' : ''), w.gpu_power_limit ? w.gpu_power / w.gpu_power_limit : null, null, 'GPU power draw and temperature; the bar is the share of its power limit.'));
  if (w.gpu_pcie_width != null) cells.push(hwCell('PCIe · gen ' + w.gpu_pcie_gen + ' x' + w.gpu_pcie_width, w.gpu_pcie_rx_mb != null ? mbs(w.gpu_pcie_rx_mb) + ' in' : '–', null, null, 'The GPU’s PCIe link right now (it drops to a lower generation when idle) and the data coming in, mostly experts the GPU didn’t have.'));
  if (w.cpu != null) cells.push(hwCell('CPU', Math.round(w.cpu) + '%', w.cpu / 100, null, 'Host CPU load.'));
  if (w.ram_total) cells.push(hwCell('RAM', gib(w.ram_used) + ' of ' + gib(w.ram_total) + ' GiB', w.ram_used / w.ram_total, null, 'Host memory in use: the experts the GPU doesn’t hold and the parked conversations live here.'));
  return h('div', { class: 'hw' }, cells);
}

// a line that never wraps: what doesn't fit is cut, and the whole text is in its tooltip
const oneLine = (cls, text) => h('div', { class: cls + ' one', 'data-tip': text }, text);

function engine(b, list, active, top) {
  const [ic, tone, word] = !b.healthy ? ['failed', 'critical', 'Down'] : b.state === 'unloaded' ? ['warn', 'warning', 'Model unloaded'] : b.poll_fails ? ['warn', 'warning', b.poll_fails + ' failed poll' + (b.poll_fails > 1 ? 's' : '')] : ['sticky', 'good', 'Healthy'];
  const rec = b.recent || {};
  const acts = b.activity || [];
  const matched = new Set(acts.map((a) => a.key).filter(Boolean));
  // sent by the router but not in the engine's last poll yet
  const unseen = live.filter((x) => x.backend === b.name && !matched.has(x.key));
  const idle = (b.slots || []).filter((x) => x.state === 'idle');
  let direct = b.direct || 0;
  const busy = acts.map((a) => work(a, !a.key && direct-- > 0)).concat(unseen.map((x) => slotBox('slot busy', x.key, 'Conversation ' + x.key + '\nSent by the router; the engine’s next report will show what it is doing.',
    [swatch(x.key), h('span', { class: 'key' }, x.key.slice(0, 8))], dur(now() - (x.started || x.arrived)), 'starting', null)));
  // one box per engine slot, always; more requests than slots only show in the line below
  const n = Math.max(1, b.batch);
  const boxes = busy.slice(0, n);
  const more = busy.length - boxes.length;
  for (let i = 0; boxes.length < n; i++) {
    const held = idle[i] && idle[i].held_tokens;
    boxes.push(slotBox('slot', null, held > 0 ? 'Free. It still holds the KV of the last conversation it served, so that conversation’s next turn can start at once.' : 'Free.', 'idle slot', '', held > 0 ? 'keeps ' + toks(held) + ' tokens' : 'free', null));
  }
  const reuse = b.prompt_tokens ? b.reused_tokens / b.prompt_tokens : b.requests ? b.reused / b.requests : null;
  const meta = ' · ' + [b.started && 'up ' + dur(now() - b.started), count(b.routed) + ' routed', b.host].filter(Boolean).join(' · ');
  const badges = [
    b.restarts > 0 && h('span', { class: 'pill', 'data-tip': FLAGS.restarts[2] }, icon('retry', 'warning'), b.restarts + '×'),
    b.slow > 0 && h('span', { class: 'pill', 'data-tip': b.slow + ' slow first token' + (b.slow > 1 ? 's' : '') + '. ' + FLAGS.slow[2] }, icon('warn', 'serious'), b.slow + ' slow'),
    b.direct > 0 && h('span', { class: 'pill', 'data-tip': 'Requests sent to this engine directly, not through the router. Their slots count as taken, so routed requests wait or go elsewhere instead of queueing inside the engine.' }, icon('bolt', 'warning'), b.direct + ' direct'),
    b.claimed > 0 && h('span', { class: 'pill', 'data-tip': 'Conversations waiting for this engine because it holds their cache; for a moment they keep its next free slot from newcomers.' }, icon('queued', 'warning'), b.claimed + ' waiting'),
  ].filter(Boolean);
  return h('article', { class: b.healthy ? 'card' : 'card down' },
    h('div', { class: 'head' }, h('div', { class: 'title' }, h('h3', null, b.name), b.version && h('span', { class: 'tag', 'data-tip': 'Engine version' }, b.version)),
      h('div', { class: 'badges' }, badges, h('span', { class: 'pill', 'data-tip': b.lastError || null }, icon(ic, tone), word))),
    b.lastError
      ? oneLine('meta err', b.lastError)
      : h('div', { class: 'meta one' }, h('span', { 'data-tip': 'Share of its slots in use lately, direct requests included, weighted toward the last 10 minutes. A new conversation goes to the engine with the most free slots, and between equals to the one busy least lately, so both boxes work about as long.' }, 'busy ' + pct(b.busy) + ' lately'), meta),
    h('div', { class: 'slots' }, boxes),
    oneLine('hint', [b.phase || b.state, b.running + ' running', more > 0 && more + ' more than its slots', b.direct > 0 && b.direct + ' sent directly', b.queued + ' queued in the engine', 'batch ' + b.batch].filter(Boolean).join(' · ')),
    hardware(b),
    h('div', { class: 'tiles' },
      tile('Decode', rate(rec.decode_tok_s ?? decodeOf(b)), rec.draft_accept != null ? pct(rec.draft_accept) + ' of drafts kept' : 'recent answers', null,
        'How fast this engine writes answers: the median of its recent requests. Drafts kept: the share of the tokens its draft head guessed ahead that the model accepted; the more it keeps, the faster it writes.'),
      tile('Prefill', rate(rec.prefill_tok_s ?? b.prefill_tok_s), rec.prefill_tok_s ? 'recent prompts' : b.prefill_tok_s ? 'learned' : 'not measured yet', null,
        'How fast it reads prompt tokens it doesn’t have cached: the median of its recent prompts, or what the router measured.'),
      tile('Cache reuse', pct(reuse), reuse == null ? 'no lookups yet' : b.prompt_tokens ? 'of ' + toks(b.prompt_tokens) + ' tokens' : 'of ' + count(b.requests) + ' requests', reuse != null && h('span', { class: 'meter sm' }, h('i', { style: { width: 100 * reuse + '%' } })),
        'Share of all prompt tokens the engine’s conversation cache already held, so it didn’t have to read them again.'),
      tile('Parked', b.cache_budget_mib ? b.parked + (b.cache_slots ? ' of ' + b.cache_slots : '') : '–', b.cache_budget_mib ? mib(b.cache_budget_mib) + ' budget' : 'no conversation cache', null,
        'Conversations the engine parked in RAM, out of its slots, so their next turn resumes without reading the prompt again.'),
      tile('Evictions', b.evictions == null ? '–' : count(b.evictions), b.parks == null ? 'not reported' : count(b.parks) + ' parks', null,
        'Parked conversations dropped to make room for newer ones: their next turn reads the whole prompt again. Parks: how often the engine saved one.'),
      tile('Expert cache', pct(rec.expert_hit), rec.pcie_share != null ? (100 * rec.pcie_share).toFixed(1) + '% over PCIe' : 'hit rate, recent answers', null,
        'Share of the experts the model needed that were already in GPU memory; the rest came from RAM over PCIe, which is slower.')),
    convList(b.name, list, active, top));
}

function convList(name, list, active, top) {
  list.sort((a, b) => (b.last_seen || 0) - (a.last_seen || 0));
  const n = list.length;
  const multi = list.filter((c) => c.turns > 1).length;
  const all = expanded.has(name);
  const noun = count(n) + (n > 1 ? ' conversations' : ' conversation');
  const title = !n ? 'No conversation lives here yet' : name ? noun + (n > 1 ? ' live here' : ' lives here') + (multi && multi < n ? ' · ' + multi + ' with several turns' : '') : noun + ' waiting for a home';
  return h('div', { class: 'convs' },
    h('h4', null, title),
    n > 0 && h('table', null,
      h('thead', null, h('tr', null, h('th', null, 'Conversation'), h('th', { class: 'r' }, 'Turns'), h('th', null, 'Prompt'), h('th', { class: 'r' }, 'Last seen'), h('th', { class: 'r' }, 'Moved'))),
      h('tbody', null, (all ? list : list.slice(0, SHOWN)).map((c) => convRow(c, active.has(c.key), top)))),
    n > SHOWN && h('button', { type: 'button', onclick: () => { if (all) expanded.delete(name); else expanded.add(name); render(); } }, all ? 'Show the ' + SHOWN + ' most recent' : 'Show all ' + n));
}

function convRow(c, on, top) {
  return h('tr', { 'data-key': c.key },
    h('td', { class: 'key', title: c.key }, swatch(c.key), c.key.slice(0, 8), on && [' ', icon('sticky', 'accent pulse')]),
    h('td', { class: 'r' }, c.turns),
    h('td', null, c.prompt_tokens ? [h('span', { class: 'bar', style: { width: Math.max(2, (56 * c.prompt_tokens) / top) + 'px', background: color(c.key) } }), toks(c.prompt_tokens)] : '–'),
    h('td', { class: 'r' }, on ? 'now' : c.last_seen ? ago(c.last_seen) : '–'),
    h('td', { class: c.migrations ? 'r' : 'r muted' }, c.migrations ? c.migrations + '×' : '–'));
}

function row(e) {
  const kind = KINDS[e.kind] ? e.kind : 'failed';
  const tone = KINDS[kind][0];
  const notes = [];
  if (e.retry_from) notes.push(e.retry_from + ' failed first: ' + e.retry_error);
  if (kind === 'failed') notes.push((e.backend ? e.backend + ': ' : '') + (e.error || 'answered ' + e.status));
  if (e.slow) notes.push('slow first token: ' + dur(e.engine_ttft_ms) + ' at the engine, where its uncached prompt needs about ' + dur(e.expected_ttft_ms));
  const note = notes.join(' · ');
  const tr = h('tr', { class: note && 'has-note', 'data-key': e.key },
    h('td', { 'data-tip': explain(kind) }, icon(kind, tone), ' ' + (kind === 'failed' && e.status ? 'failed ' + e.status : kind)),
    h('td', { class: 'key', title: e.key }, swatch(e.key), e.key.slice(0, 8)),
    h('td', { class: 'r c-turn' }, e.turn),
    h('td', { 'data-tip': route(e) }, route(e)),
    h('td', { class: 'r' }, toks(e.prompt_tokens)),
    h('td', { class: 'r c-cached' }, e.prompt_tokens ? Math.round((100 * e.cached_tokens) / e.prompt_tokens) + '%' : '–'),
    h('td', { class: 'r' }, e.queued ? dur(e.wait_ms) : '–'),
    h('td', { class: 'r' }, e.slow && [icon('warn', 'serious'), ' '], dur(e.ttft_ms)),
    h('td', { class: 'r' }, dur(e.ms)),
    h('td', { class: 'r muted c-fin' }, clock(e.arrived + e.ms)));
  return note ? [tr, h('tr', { class: 'note', 'data-key': e.key }, h('td'), h('td', { colspan: 9 }, note))] : [tr];
}

function liveRow(x) {
  const waiting = !x.backend;
  const age = now() - x.arrived;
  return h('tr', { 'data-key': x.key },
    h('td', null, waiting ? icon('queued', 'warning') : icon('sticky', 'accent pulse'), waiting ? ' waiting' : ' running'),
    h('td', { class: 'key', title: x.key }, swatch(x.key), x.key.slice(0, 8)),
    h('td', { class: 'r c-turn' }, x.turn),
    h('td', null, waiting ? (x.home ? 'home ' + x.home : 'new') : route(x)),
    h('td', { class: 'r' }, '–'),
    h('td', { class: 'r c-cached' }, '–'),
    h('td', { class: 'r' }, waiting ? dur(age) : x.started ? dur(x.started - x.arrived) : '–'),
    h('td', { class: 'r' }, '–'),
    h('td', { class: 'r' }, dur(age)),
    h('td', { class: 'r muted c-fin' }, '…'));
}

function status() {
  const age = Date.now() - lastOk;
  const down = failing && (!lastOk || age > 2 * POLL);
  const stale = !down && lastOk > 0 && age > 2 * POLL + 500;
  const [ic, tone, word] = down ? ['failed', 'critical', 'Router unreachable'] : stale ? ['warn', 'warning', 'Stale'] : lastOk ? ['sticky', 'good', 'Live'] : ['queued', 'muted', 'Connecting'];
  fill($('state'), icon(ic, tone), ' ', h('b', null, word), lastOk > 0 && h('span', { class: 'meta' }, ' · updated ' + (age < 2000 ? 'just now' : age < 6e4 ? Math.round(age / 1000) + ' s ago' : dur(age) + ' ago')));
  document.body.classList.toggle('stale', down || stale);
  document.title = (down ? 'unreachable · ' : data && data[0].waiting ? data[0].waiting + ' waiting · ' : '') + 'strata-router';
  $('banner').hidden = !down;
  $('banner').textContent = lastOk ? 'The router stopped answering. Showing its last data, from ' + clock(lastOk) + '. Retrying every ' + POLL / 1000 + ' s.' : 'Cannot reach the router. Retrying every ' + POLL / 1000 + ' s.';
}

function setTheme(t) {
  theme = THEMES.includes(t) ? t : 'auto';
  if (theme === 'auto') delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  $('theme').textContent = 'Theme: ' + theme;
}

function setWindow(ms) {
  span = WINDOWS.some((w) => w[1] === ms) ? ms : 6e5;
  $('window').replaceChildren(...WINDOWS.map(([label, w]) => h('button', { type: 'button', 'aria-pressed': String(w === span), onclick: () => {
    setWindow(w);
    try { localStorage.setItem('strata-window', String(w)); } catch {}
    if (data) render();
  } }, label)));
}

const tipBox = document.body.appendChild(h('div', { class: 'tip', role: 'tooltip', hidden: true }));
let tipFor = null;

// an instant tooltip for anything with data-tip, kept inside the window
function showTip(el) {
  tipFor = el;
  tipBox.hidden = !el;
  if (!el) return;
  tipBox.textContent = el.getAttribute('data-tip');
  const r = el.getBoundingClientRect(), t = tipBox.getBoundingClientRect();
  const below = r.bottom + 8 + t.height < innerHeight;
  tipBox.style.top = (below ? r.bottom + 8 : Math.max(8, r.top - 8 - t.height)) + 'px';
  tipBox.style.left = Math.max(8, Math.min(innerWidth - t.width - 8, r.left)) + 'px';
}

document.addEventListener('mouseover', (ev) => {
  const el = ev.target.closest('[data-tip]');
  if (el !== tipFor) showTip(el);
});
document.addEventListener('mouseout', (ev) => { if (!ev.relatedTarget) showTip(null); });
addEventListener('scroll', () => showTip(null), true);
document.addEventListener('mouseover', (ev) => {
  const key = (ev.target.closest('[data-key]')?.dataset.key || '').replace(/[^0-9a-f]/g, '');
  if (key === hovered) return;
  hovered = key;
  $('hl').textContent = key ? 'tr[data-key="' + key + '"] td { background: var(--track); } .tl g[data-key]:not([data-key="' + key + '"]) { opacity: .15; } .slot[data-key="' + key + '"] { outline: 2px solid var(--accent); }' : '';
});
$('theme').onclick = () => {
  setTheme(THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length]);
  try { localStorage.setItem('strata-theme', theme); } catch {}
};
addEventListener('resize', () => { if (data) render(); });
try { theme = new URLSearchParams(location.search).get('theme') || localStorage.getItem('strata-theme') || 'auto'; } catch {}
try { span = Number(new URLSearchParams(location.search).get('window')) || Number(localStorage.getItem('strata-window')) || 6e5; } catch {}
setTheme(theme);
setWindow(span);
status();
setInterval(status, 1000);
tick();
</script>
</body>
</html>
`;
