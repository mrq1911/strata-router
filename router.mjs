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
      flights: new Set(),
      batch: 2,
      tok_s_mean: null,
      prefill_tok_s: null,
      held_tokens: 0,
      vram_free_mib: 0,
      cache: null,
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
    b.tok_s_mean = live.tok_s_mean ?? null;
    b.held_tokens = b.slots.reduce((s, x) => s + (x?.held_tokens || 0), 0);
    b.vram_free_mib = m.engine?.vram_free_mib ?? 0;
    b.cache = m.conversation_cache || null;
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

// one loop per engine, so a dead one can't hold up the others' state
async function pollLoop(b) {
  for (;;) {
    await sleep(config.pollMs);
    await pollOne(b);
  }
}

// requests waiting for a slot, in arrival order; claim is the busy home a patient one holds out for
const queue = new Set();

function wake() {
  for (const w of queue) w.wake();
}

function claimed(b) {
  let n = 0;
  for (const w of queue) if (w.claim === b && !w.isGone()) n++;
  return n;
}

// free slots we are allowed to use, minus the requests already in flight
function freeSlots(b) {
  return Math.max(0, Math.min(b.batch, config.maxInflight) - b.inflight);
}

function capacity(b) {
  return b.healthy && b.queued === 0 && freeSlots(b) > 0;
}

// an unloaded engine has to load its model first (about 90 s), so it comes last
function score(b) {
  return freeSlots(b) * (b.tok_s_mean || 60) - b.queued * 50 - b.inflight * 30 - (b.state === 'unloaded' ? 1e6 : 0);
}

const best = (list) => list.reduce((a, b) => (score(b) > score(a) ? b : a));

function reserve(b) {
  b.inflight++;
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
async function pick(conv, isGone, exclude = null) {
  const t0 = performance.now();
  const me = { claim: null, isGone, wake() {} };
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
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, Math.max(1, Math.min(1000, (me.claim ? hold : config.queueWaitMs) - elapsed)));
        me.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
    }
  } finally {
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
    if (!data || data === '[DONE]') return;
    let o;
    try {
      o = JSON.parse(data);
    } catch {
      return;
    }
    if (!tap.firstTokenAt && generated(o)) tap.firstTokenAt = performance.now();
    take(o);
  };
  const tap = {
    firstTokenAt: 0,
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
    const onClientGone = () => up.destroy(new Error('client went away'));
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
    queued: false,
    wait_ms: 0,
    overflow: false,
    retry_from: null,
    retry_error: null,
    error: null,
    ttft_ms: null,
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
  const slot = async (exclude) => {
    const t = performance.now();
    const picked = await pick(conv, gone, exclude);
    ev.wait_ms += performance.now() - t;
    ev.queued ||= Boolean(picked.waited);
    ev.overflow ||= Boolean(picked.overflow);
    if (picked.b) ev.backend = picked.b.name;
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
      if (u) {
        stats.prompt_tokens += u.prompt;
        stats.completion_tokens += u.completion;
        stats.cached_tokens += u.cached;
        // after an overflow the time to first token includes the engine's own queue
        if (out.firstTokenMs && !overflow) learnPrefill(served, u, out.firstTokenMs);
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
      live: [...live].map(({ key, turn, home, backend, arrived }) => ({ key, turn, home, backend, arrived })),
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
        reused: b.cache?.requests_reused ?? 0,
        requests: b.cache?.requests ?? 0,
        lastError: b.lastError,
        capacity: Math.min(b.batch, config.maxInflight),
        claimed: claimed(b),
        conversations: homes.get(b.name) || 0,
      })),
      waiting: queue.size,
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
  --page: light-dark(#f9f9f7, #0d0d0d);
  --surface: light-dark(#fcfcfb, #1a1a19);
  --ink: light-dark(#0b0b0b, #ffffff);
  --ink-2: light-dark(#52514e, #c3c2b7);
  --muted: #898781;
  --grid: light-dark(#e1e0d9, #2c2c2a);
  --ring: light-dark(rgba(11, 11, 11, .1), rgba(255, 255, 255, .1));
  --accent: light-dark(#2a78d6, #3987e5);
  --track: light-dark(#cde2fb, #0d366b);
  --good: #0ca30c;
  --warning: #fab219;
  --serious: #ec835a;
  --critical: #d03b3b;
}
:root[data-theme=light] { color-scheme: light; }
:root[data-theme=dark] { color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--ink); font: 13px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
header { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 18px; padding: 14px 20px 12px; }
h1, h2, h3, h4, p { margin: 0; }
h1 { font-size: 16px; font-weight: 600; }
h2, h4 { font-size: 13px; font-weight: 600; }
h3 { font-size: 15px; font-weight: 600; }
.label, .meta, .hint, dt, th, .note td { color: var(--ink-2); font-weight: 400; }
.muted { color: var(--muted); }
button { font: inherit; font-size: 12px; color: var(--ink-2); background: none; border: 1px solid var(--ring); border-radius: 6px; padding: 2px 9px; cursor: pointer; }
#theme { margin-left: auto; }
#banner { margin: 0 20px 12px; padding: 8px 12px; border: 1px solid var(--critical); border-radius: 8px; background: var(--surface); }
.sprite { position: absolute; width: 0; height: 0; overflow: hidden; }
.ic { width: 12px; height: 12px; fill: currentColor; vertical-align: -1px; }
.good { color: var(--good); }
.warning { color: var(--warning); }
.serious { color: var(--serious); }
.critical { color: var(--critical); }
.neutral { color: var(--ink-2); }
.accent { color: var(--accent); }
.card { background: var(--surface); border: 1px solid var(--ring); border-radius: 10px; padding: 14px 16px; }
.dim { transition: opacity .3s; }
.stale .dim { opacity: .45; }
.kpis { display: grid; grid-template-columns: minmax(240px, 1.2fr) minmax(190px, .7fr) minmax(0, 3fr); gap: 12px; padding: 0 20px 12px; }
.big { font-size: 52px; font-weight: 600; line-height: 1.1; margin: 4px 0 10px; }
.val { font-size: 32px; font-weight: 600; line-height: 1.1; margin: 6px 0; }
.hint { margin-top: 6px; }
.counters { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px 18px; align-content: center; }
.counters b { display: block; font-size: 20px; font-weight: 600; }
.meter { display: block; height: 8px; border-radius: 4px; background: var(--track); overflow: hidden; }
.meter i { display: block; height: 100%; background: var(--accent); }
.meter.sm { display: inline-block; width: 64px; height: 6px; margin-right: 6px; vertical-align: 1px; }
main { display: grid; grid-template-columns: minmax(0, 1fr) minmax(500px, 620px); gap: 12px; padding: 0 20px 20px; align-items: start; }
.engines { display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 12px; align-items: start; }
.head { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
.down dl, .down .convs { opacity: .6; }
.err { color: var(--ink-2); overflow-wrap: anywhere; margin-top: 4px; }
dl { display: grid; grid-template-columns: 62px 1fr; gap: 5px 10px; margin: 12px 0 14px; }
dd { margin: 0; }
.cells { display: inline-flex; flex-wrap: wrap; gap: 2px; margin-right: 6px; vertical-align: -1px; }
.cells i { width: 14px; height: 10px; border-radius: 2px; background: var(--track); }
.cells .on { background: var(--accent); }
.cells .over { background: var(--serious); }
.convs { border-top: 1px solid var(--grid); padding-top: 12px; }
.convs h4 { margin-bottom: 6px; }
.convs button { margin-top: 8px; }
table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { padding: 3px 6px; border-bottom: 1px solid var(--grid); white-space: nowrap; text-align: left; }
th { font-size: 12px; position: sticky; top: 0; background: var(--surface); }
.r { text-align: right; }
.key { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.dot { display: inline-block; width: 16px; }
.bar { display: inline-block; height: 6px; margin-right: 6px; border-radius: 0 3px 3px 0; background: var(--accent); vertical-align: 1px; }
.feed { position: sticky; top: 12px; display: flex; flex-direction: column; max-height: calc(100vh - 24px); }
.feed h2 { margin-bottom: 10px; }
.mix { display: flex; flex-wrap: wrap; gap: 4px 14px; margin-bottom: 10px; }
.mix .zero { opacity: .5; }
.scroll { overflow: auto; min-height: 0; margin: 0 -10px; padding: 0 4px; }
.feed table { font-size: 12px; }
.route { max-width: 130px; overflow: hidden; text-overflow: ellipsis; }
.has-note td { border-bottom: 0; }
.note td { padding-top: 0; white-space: normal; }
@keyframes pulse { 50% { opacity: .3; } }
.pulse { animation: pulse 1.4s ease-in-out infinite; }
@media (prefers-reduced-motion: reduce) { .pulse { animation: none; } }
@media (max-width: 1200px) { main { grid-template-columns: minmax(0, 1fr); } .feed { position: static; max-height: none; } .scroll { max-height: 70vh; } }
@media (max-width: 760px) { .kpis { grid-template-columns: minmax(0, 1fr); } .counters { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
</style>
<style id="hl"></style>
</head>
<body>
<svg class="sprite" aria-hidden="true">
<symbol id="i-sticky" viewBox="0 0 12 12"><circle cx="6" cy="6" r="4.5"/></symbol>
<symbol id="i-cold" viewBox="0 0 12 12"><circle cx="6" cy="6" r="3.7" fill="none" stroke="currentColor" stroke-width="1.6"/></symbol>
<symbol id="i-queued" viewBox="0 0 12 12"><path d="M2.5 1h7v2.2L7.2 6l2.3 2.8V11h-7V8.8L4.8 6 2.5 3.2z"/></symbol>
<symbol id="i-spill" viewBox="0 0 12 12"><path d="M1 4.4h5V1.4L11 6l-5 4.6V7.6H1z"/></symbol>
<symbol id="i-overflow" viewBox="0 0 12 12"><path d="M1 1.5h10v2.2H1zm0 3.4h10v2.2H1zm0 3.4h10v2.2H1z"/></symbol>
<symbol id="i-retry" viewBox="0 0 12 12"><path d="M6 1.5A4.5 4.5 0 1 0 10.5 6H8.7A2.7 2.7 0 1 1 6 3.3v1.3l2.6-2.2L6 .2z"/></symbol>
<symbol id="i-failed" viewBox="0 0 12 12"><path d="M2.2 3.6l1.4-1.4L6 4.6l2.4-2.4 1.4 1.4L7.4 6l2.4 2.4-1.4 1.4L6 7.4 3.6 9.8 2.2 8.4 4.6 6z"/></symbol>
<symbol id="i-gone" viewBox="0 0 12 12"><path d="M1.5 5h9v2h-9z"/></symbol>
<symbol id="i-warn" viewBox="0 0 12 12"><path d="M6 1l5.5 10H.5z"/></symbol>
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
<div class="card">
<div class="label">Cache hit ratio · cached ÷ prompt tokens</div>
<div class="big" id="hit">–</div>
<span class="meter"><i id="hit-bar"></i></span>
<p class="hint" id="hit-sub"></p>
</div>
<div class="card">
<div class="label" id="waiting-label">Waiting for a slot now</div>
<div class="val" id="waiting">–</div>
<p class="hint" id="waiting-sub"></p>
</div>
<div class="card counters" id="counters"></div>
</section>
<main class="dim">
<section class="engines" id="engines"></section>
<aside class="card feed">
<h2>Routing decisions <span class="hint" id="feed-n"></span></h2>
<div class="mix" id="mix"></div>
<div class="scroll">
<table>
<thead><tr><th>Decision</th><th>Conversation</th><th class="r">Turn</th><th>Engine</th><th class="r">Prompt</th><th class="r">Cached</th><th class="r">Wait</th><th class="r">1st token</th><th class="r">Total</th><th class="r">Finished</th></tr></thead>
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
  sticky: ['good', 'Served by its home engine, which holds its KV cache'],
  cold: ['neutral', 'New conversation, prefilled from scratch'],
  queued: ['warning', 'Waited in the queue for a free slot'],
  spill: ['warning', 'Its home stayed busy longer than a cold prefill takes, or was down, so the conversation moved'],
  overflow: ['serious', 'Waited MAX_QUEUE_WAIT, then went into an engine queue'],
  retry: ['serious', 'An engine failed before answering, so another one took the request'],
  failed: ['critical', 'Answered with an error'],
  gone: ['muted', 'The client disconnected first'],
};
const COUNTERS = [
  ['requests', 'Requests'],
  ['cold', 'New conversations', 'cold'],
  ['migrated', 'Migrated', 'spill'],
  ['queued', 'Queued', 'queued'],
  ['overflow', 'Overflow', 'overflow'],
  ['retried', 'Retried', 'retry'],
  ['errors', 'Errors', 'failed'],
  ['completion_tokens', 'Tokens generated'],
];
const THEMES = ['auto', 'light', 'dark'];
const SHOWN = 12;
const $ = (id) => document.getElementById(id);
const base = location.pathname.replace(/\/+dashboard\/*$/, '');
const tpl = $('icon').content.firstElementChild;
const compact = new Intl.NumberFormat('en', { notation: 'compact', maximumSignificantDigits: 3 });
const whole = new Intl.NumberFormat('en');
const count = (n) => (n == null ? '–' : n < 1e4 ? whole.format(n) : compact.format(n));
const toks = (n) => (n == null ? '–' : compact.format(n));
const rate = (n) => (n == null ? '–' : whole.format(Math.round(n)) + ' tok/s');
const mib = (n) => (n >= 1024 ? +(n / 1024).toFixed(1) + ' GiB' : n + ' MiB');
const clock = (t) => new Date(t).toLocaleTimeString([], { hour12: false });
const route = (e) => (e.home && e.backend && e.home !== e.backend ? e.home + ' → ' + e.backend : e.backend || '–');
const now = () => Date.now() + skew;
const expanded = new Set();
let feed = [], live = [], cursor = 0, started = null, lastOk = 0, failing = false, skew = 0, data = null, drawn = '', theme = 'auto', hovered = '';

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

function icon(name, tone) {
  const svg = tpl.cloneNode(true);
  svg.firstElementChild.setAttribute('href', '#i-' + name);
  svg.classList.add(...tone.split(' '));
  return svg;
}

function dur(ms) {
  if (ms == null) return '–';
  if (ms < 1000) return Math.round(ms) + ' ms';
  if (ms < 1e4) return (ms / 1000).toFixed(1) + ' s';
  const s = Math.round(ms / 1000), m = Math.floor(s / 60), hr = Math.floor(m / 60), d = Math.floor(hr / 24);
  return d ? d + ' d ' + (hr % 24) + ' h' : hr ? hr + ' h ' + (m % 60) + ' m' : m ? m + ' m ' + (s % 60) + ' s' : s + ' s';
}

function ago(t) {
  const s = Math.round((now() - t) / 1000);
  if (s < 2) return 'now';
  return (s < 60 ? s + ' s' : s < 3600 ? Math.floor(s / 60) + ' m' : s < 86400 ? Math.floor(s / 3600) + ' h' : Math.floor(s / 86400) + ' d') + ' ago';
}

async function get(path) {
  const res = await fetch(base + path, { cache: 'no-store', signal: AbortSignal.timeout(4000) });
  if (!res.ok) throw new Error(path + ' answered ' + res.status);
  return res.json();
}

async function tick() {
  try {
    const [s, c, first] = await Promise.all([get('/stats'), get('/stats/conversations'), get('/stats/events?since=' + cursor)]);
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
    data = [s, c.conversations];
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

function render() {
  const [s, convs] = data;
  const st = s.stats;
  const ratio = st.prompt_tokens ? st.cached_tokens / st.prompt_tokens : null;
  $('hit').textContent = ratio == null ? '–' : (100 * ratio).toFixed(1) + '%';
  $('hit-bar').style.width = 100 * (ratio || 0) + '%';
  $('hit-sub').textContent = ratio == null ? 'No token usage reported yet' : toks(st.cached_tokens) + ' of ' + toks(st.prompt_tokens) + ' prompt tokens were already in an engine cache';
  const holding = s.backends.reduce((n, b) => n + b.claimed, 0);
  $('waiting-label').replaceChildren(icon('queued', s.waiting ? 'warning' : 'muted'), ' Waiting for a slot now');
  $('waiting').textContent = whole.format(s.waiting);
  $('waiting-sub').textContent = !s.waiting ? 'Every request has a slot' : holding ? holding + ' holding out for a busy home engine' : 'Every engine is full';
  $('counters').replaceChildren(...COUNTERS.map(([k, label, kind]) => h('div', { title: kind && KINDS[kind][1] }, h('span', { class: 'label' }, kind && [icon(kind, st[k] ? KINDS[kind][0] : 'muted'), ' '], label), h('b', null, count(st[k])))));
  $('uptime').textContent = 'Up ' + dur(now() - Date.parse(st.started)) + ' · ' + s.backends.filter((b) => b.healthy).length + ' of ' + s.backends.length + ' engines healthy';

  const active = new Set(live.map((x) => x.key));
  const serving = new Map(live.filter((x) => x.backend).map((x) => [x.key, x.backend]));
  const homes = new Map(s.backends.map((b) => [b.name, []]));
  const homeless = [];
  // a first turn in progress already builds its kv on the engine serving it
  for (const c of convs) (homes.get(c.backend || serving.get(c.key)) || homeless).push(c);
  const top = Math.max(1, ...convs.map((c) => c.prompt_tokens));
  const cards = s.backends.map((b) => engine(b, homes.get(b.name), active, top));
  if (homeless.length) cards.push(h('article', { class: 'card' }, h('h3', null, 'No home yet'), convList('', homeless, active, top)));
  $('engines').replaceChildren(...cards);

  $('live').replaceChildren(...live.slice().reverse().map(liveRow));
  $('empty').hidden = feed.length + live.length > 0;
  if (drawn === started + cursor) return;
  drawn = started + cursor;
  const tally = {};
  for (const e of feed) tally[e.kind] = (tally[e.kind] || 0) + 1;
  $('feed-n').textContent = feed.length ? '· last ' + feed.length + ' · hover a row to trace its conversation' : '';
  $('mix').replaceChildren(...Object.keys(KINDS).map((k) => h('span', { class: tally[k] ? null : 'zero', title: KINDS[k][1] }, icon(k, KINDS[k][0]), ' ' + k + ' ', h('b', null, tally[k] || 0))));
  $('rows').replaceChildren(...feed.slice().reverse().flatMap(row));
}

function engine(b, list, active, top) {
  const [ic, tone, word] = !b.healthy ? ['failed', 'critical', 'Down'] : b.state === 'unloaded' ? ['warn', 'warning', 'Model unloaded'] : b.poll_fails ? ['warn', 'warning', b.poll_fails + ' failed poll' + (b.poll_fails > 1 ? 's' : '')] : ['sticky', 'good', 'Healthy'];
  const here = live.filter((x) => x.backend === b.name);
  const over = b.inflight - b.capacity;
  const cells = Array.from({ length: Math.max(b.capacity, b.inflight) }, (_, i) => h('i', { class: i >= b.capacity ? 'over' : i < b.inflight ? 'on' : null, title: here[i] && 'Conversation ' + here[i].key.slice(0, 8) }));
  const reuse = b.requests ? b.reused / b.requests : null;
  return h('article', { class: b.healthy ? 'card' : 'card down' },
    h('div', { class: 'head' }, h('h3', null, b.name), h('span', null, icon(ic, tone), ' ' + word)),
    h('div', { class: 'meta' }, b.host + ' · ' + count(b.routed) + ' routed'),
    b.lastError && h('div', { class: 'err' }, b.lastError),
    h('dl', null,
      h('dt', null, 'Slots'), h('dd', null, h('span', { class: 'cells' }, cells), b.inflight + ' of ' + b.capacity + ' in use', over > 0 && [' · ', icon('overflow', 'serious'), ' ' + over + ' over capacity'], b.claimed ? ' · ' + b.claimed + ' waiting for it' : ''),
      h('dt', null, 'Engine'), h('dd', null, b.queued > 0 && [icon('queued', 'warning'), ' '], (b.state ? b.state + ' · ' : '') + b.running + ' running · ' + b.queued + ' queued · batch ' + b.batch),
      h('dt', null, 'Speed'), h('dd', null, rate(b.tok_s_mean) + ' decode · ' + (b.prefill_tok_s ? rate(b.prefill_tok_s) + ' prefill' : 'prefill not measured yet')),
      h('dt', null, 'Cache'), h('dd', null, b.cache_budget_mib ? b.parked + ' parked · ' + mib(b.cache_budget_mib) + ' budget' : 'No conversation cache reported'),
      h('dt', null, 'Reuse'), h('dd', null, reuse == null ? 'No cache lookups yet' : [h('span', { class: 'meter sm' }, h('i', { style: { width: 100 * reuse + '%' } })), Math.round(100 * reuse) + '% of ' + count(b.requests) + ' requests']),
      h('dt', null, 'Memory'), h('dd', null, mib(b.vram_free_mib) + ' VRAM free · ' + toks(b.held_tokens) + ' tokens held')),
    convList(b.name, list, active, top));
}

function convList(name, list, active, top) {
  list.sort((a, b) => (b.last_seen || 0) - (a.last_seen || 0));
  const n = list.length;
  const all = expanded.has(name);
  const noun = count(n) + (n > 1 ? ' conversations' : ' conversation');
  const title = !n ? 'No conversation lives here yet' : name ? noun + (n > 1 ? ' live here' : ' lives here') : noun + ' with no finished turn';
  return h('div', { class: 'convs' },
    h('h4', null, title),
    n > 0 && h('table', null,
      h('thead', null, h('tr', null, h('th', null, 'Conversation'), h('th', { class: 'r' }, 'Turns'), h('th', null, 'Prompt'), h('th', { class: 'r' }, 'Last seen'), h('th', { class: 'r' }, 'Moved'))),
      h('tbody', null, (all ? list : list.slice(0, SHOWN)).map((c) => convRow(c, active.has(c.key), top)))),
    n > SHOWN && h('button', { type: 'button', onclick: () => { if (all) expanded.delete(name); else expanded.add(name); render(); } }, all ? 'Show the ' + SHOWN + ' most recent' : 'Show all ' + n));
}

function convRow(c, on, top) {
  return h('tr', { 'data-key': c.key },
    h('td', { class: 'key', title: c.key }, h('span', { class: 'dot' }, on && icon('sticky', 'accent pulse')), c.key.slice(0, 8)),
    h('td', { class: 'r' }, c.turns),
    h('td', null, c.prompt_tokens ? [h('span', { class: 'bar', style: { width: Math.max(2, (72 * c.prompt_tokens) / top) + 'px' } }), toks(c.prompt_tokens)] : '–'),
    h('td', { class: 'r' }, on ? 'now' : c.last_seen ? ago(c.last_seen) : '–'),
    h('td', { class: c.migrations ? 'r' : 'r muted' }, c.migrations ? c.migrations + '×' : '–'));
}

function row(e) {
  const kind = KINDS[e.kind] ? e.kind : 'failed';
  const [tone, what] = KINDS[kind];
  const notes = [];
  if (e.retry_from) notes.push(e.retry_from + ' failed first: ' + e.retry_error);
  if (kind === 'failed') notes.push((e.backend ? e.backend + ': ' : '') + (e.error || 'answered ' + e.status));
  const note = notes.join(' · ');
  const tr = h('tr', { class: note && 'has-note', title: what, 'data-key': e.key },
    h('td', null, icon(kind, tone), ' ' + (kind === 'failed' && e.status ? 'failed ' + e.status : kind)),
    h('td', { class: 'key', title: e.key }, e.key.slice(0, 8)),
    h('td', { class: 'r' }, e.turn),
    h('td', { class: 'route', title: route(e) }, route(e)),
    h('td', { class: 'r' }, toks(e.prompt_tokens)),
    h('td', { class: 'r' }, e.prompt_tokens ? Math.round((100 * e.cached_tokens) / e.prompt_tokens) + '%' : '–'),
    h('td', { class: 'r' }, e.queued ? dur(e.wait_ms) : '–'),
    h('td', { class: 'r' }, dur(e.ttft_ms)),
    h('td', { class: 'r' }, dur(e.ms)),
    h('td', { class: 'r muted' }, clock(e.arrived + e.ms)));
  return note ? [tr, h('tr', { class: 'note', 'data-key': e.key }, h('td'), h('td', { colspan: 9 }, note))] : [tr];
}

function liveRow(x) {
  const waiting = !x.backend;
  const age = now() - x.arrived;
  return h('tr', { 'data-key': x.key },
    h('td', null, waiting ? icon('queued', 'warning') : icon('sticky', 'accent pulse'), waiting ? ' waiting' : ' running'),
    h('td', { class: 'key', title: x.key }, x.key.slice(0, 8)),
    h('td', { class: 'r' }, x.turn),
    h('td', { class: 'route' }, waiting ? (x.home ? 'home ' + x.home : 'new') : route(x)),
    h('td', { class: 'r' }, '–'),
    h('td', { class: 'r' }, '–'),
    h('td', { class: 'r' }, waiting ? dur(age) : '–'),
    h('td', { class: 'r' }, '–'),
    h('td', { class: 'r' }, dur(age)),
    h('td', { class: 'r muted' }, '…'));
}

function status() {
  const age = Date.now() - lastOk;
  const down = failing && (!lastOk || age > 2 * POLL);
  const stale = !down && lastOk > 0 && age > 2 * POLL + 500;
  const [ic, tone, word] = down ? ['failed', 'critical', 'Router unreachable'] : stale ? ['warn', 'warning', 'Stale'] : lastOk ? ['sticky', 'good', 'Live'] : ['queued', 'muted', 'Connecting'];
  $('state').replaceChildren(icon(ic, tone), ' ', h('b', null, word), lastOk > 0 && h('span', { class: 'meta' }, ' · updated ' + (age < 2000 ? 'just now' : age < 6e4 ? Math.round(age / 1000) + ' s ago' : dur(age) + ' ago')));
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

document.addEventListener('mouseover', (ev) => {
  const key = (ev.target.closest('[data-key]')?.dataset.key || '').replace(/[^0-9a-f]/g, '');
  if (key === hovered) return;
  hovered = key;
  $('hl').textContent = key ? 'tr[data-key="' + key + '"] td { background: var(--track); }' : '';
});
$('theme').onclick = () => {
  setTheme(THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length]);
  try { localStorage.setItem('strata-theme', theme); } catch {}
};
try { theme = new URLSearchParams(location.search).get('theme') || localStorage.getItem('strata-theme') || 'auto'; } catch {}
setTheme(theme);
status();
setInterval(status, 1000);
tick();
</script>
</body>
</html>
`;
