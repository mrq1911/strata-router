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
const LOCAL = new Set(['/', '/health', '/stats']);
const HOP = new Set(['connection', 'keep-alive', 'transfer-encoding', 'te', 'trailer', 'upgrade', 'proxy-authenticate', 'proxy-authorization']);
// expect would stall the upstream request; accept-encoding would compress answers the router has to read
const NOT_FORWARDED = new Set(['host', 'content-length', 'expect', 'accept-encoding']);
const MAX_BODY = 20_000_000;
// a fresh connection per request, so a stale keep-alive socket can't fail (and spill) a turn
const agent = new http.Agent({ keepAlive: false });

const convs = new Map(); // conversation key -> { backend, turns, prompt_tokens, first, model }, least recently used first
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
    const req = http.request({ host: b.host, port: b.port, path, method: 'GET', timeout, agent }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(Object.assign(new Error(`${path} answered ${res.statusCode}`), { definite: true }));
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

const busy = (b) => b.running > 0 || b.queued > 0 || b.slots.some((s) => s?.state && s.state !== 'idle');

async function pollOne(b) {
  try {
    const m = await getJson(b, '/metrics');
    const live = m.live || {};
    b.healthy = true;
    b.fails = 0;
    b.lastError = null;
    b.lastPoll = Date.now();
    b.slots = Array.isArray(live.slots) ? live.slots : [];
    b.running = live.running || 0;
    b.queued = live.queued || 0;
    b.batch = m.engine?.batch_slots ?? 2;
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

function score(b) {
  return freeSlots(b) * (b.tok_s_mean || 60) - b.queued * 50 - b.inflight * 30;
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
      const home = config.backends.find((b) => b.name === conv.backend && b !== exclude);
      if (home && capacity(home)) return { b: reserve(home), waited };

      const healthy = config.backends.filter((b) => b.healthy && b !== exclude);
      // a retry has nowhere to go; otherwise give the next poll a chance before failing
      if (!healthy.length && (exclude || performance.now() - lastHealthyAt >= Math.min(config.queueWaitMs, config.pollMs + 1000))) return { waited };

      const hold = patience(conv, home);
      me.claim = elapsed < hold ? home : null;
      if (!me.claim) {
        const open = healthy.filter((b) => capacity(b) && freeSlots(b) > claimed(b));
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
    conv = { backend: null, turns: 0, prompt_tokens: 0, first: new Date().toISOString(), model: body.model };
    stats.cold++;
  }
  conv.turns++;
  remember(key, conv);

  const client = watchClient(req, res);
  const gone = () => client.signal.aborted;
  try {
    const picked = await pick(conv, gone);
    if (!picked.b) {
      if (gone()) return;
      if (draining) return send(res, 503, { error: 'router is shutting down' });
      return send(res, 503, {
        error: 'no healthy backend',
        backends: config.backends.map((x) => ({ name: x.name, healthy: x.healthy, error: x.lastError })),
      });
    }
    stats.requests++;
    if (picked.waited) stats.queued++;
    if (picked.overflow) stats.overflow++;

    const attempt = async (b, hold5xx) => {
      let out;
      try {
        out = await forward(b, req, url, raw, res, { hold5xx, signal: client.signal });
      } finally {
        release(b);
      }
      if (!out.ok && !out.aborted) {
        stats.errors++;
        log(`fail ${b.name} ${out.error} (${out.elapsed}ms)`);
      }
      return out;
    };

    const spare = config.backends.length > 1;
    let served = picked.b;
    let overflow = picked.overflow;
    let out = await attempt(served, spare);
    // the engine never started an answer: queue again for a different one
    if (!out.ok && out.retryable && spare) {
      const again = await pick(conv, gone, served);
      if (again.b) {
        stats.retried++;
        served = again.b;
        overflow = again.overflow;
        out = await attempt(served, false);
      }
    }

    // the engine took the prompt, so that's where its KV lives now
    if (out.ok ? out.status < 400 : res.headersSent) {
      if (conv.backend && conv.backend !== served.name) stats.migrated++;
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

function handleGet(res, path) {
  if (path === '/stats') {
    return send(res, 200, {
      stats,
      backends: config.backends.map((b) => ({
        name: b.name,
        host: addr(b),
        healthy: b.healthy,
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
      })),
      waiting: queue.size,
      conversations: [...convs].slice(-50).map(([key, c]) => ({ key, backend: c.backend, turns: c.turns, prompt_tokens: c.prompt_tokens })),
    });
  }
  send(res, 200, {
    ok: config.backends.some((b) => b.healthy),
    backends: config.backends.map((b) => ({ name: b.name, host: addr(b), healthy: b.healthy, inflight: b.inflight, queued: b.queued, running: b.running })),
  });
}

function loadState() {
  if (!config.stateFile) return;
  try {
    const names = new Set(config.backends.map((b) => b.name));
    for (const [key, c] of JSON.parse(fs.readFileSync(config.stateFile, 'utf8'))) {
      if (names.has(c?.backend)) remember(key, { backend: c.backend, turns: c.turns || 0, prompt_tokens: c.prompt_tokens || 0, first: c.first, model: c.model });
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
  if (req.method === 'GET' && LOCAL.has(path)) return handleGet(res, path);

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
