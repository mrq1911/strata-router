#!/usr/bin/env node
// strata-router: one OpenAI-compatible endpoint in front of several strata engines.
// conversations stay pinned to the engine that holds their KV; they spill only when that
// engine is down or stays busy longer than a cold prefill elsewhere would take.
// node >= 20, no dependencies.

import http from 'node:http';
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
    // "name=host:port" or "host:port", ipv6 as [::1]:port
    const m = entry.match(/^(?:([^=\s]+)=)?(\[[0-9a-fA-F:.]+\]|[^\s:=[\]]+)(?::(\d{1,5}))?$/);
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

async function pollOne(b) {
  try {
    const m = await getJson(b, '/metrics');
    const live = m.live || {};
    b.healthy = true;
    b.fails = 0;
    b.lastError = null;
    b.lastPoll = Date.now();
    b.slots = live.slots || [];
    b.running = live.running || 0;
    b.queued = live.queued || 0;
    b.batch = m.engine?.batch_slots ?? 2;
    b.tok_s_mean = live.tok_s_mean ?? null;
    b.held_tokens = b.slots.reduce((s, x) => s + (x.held_tokens || 0), 0);
    b.vram_free_mib = m.engine?.vram_free_mib ?? 0;
    b.cache = m.conversation_cache || null;
  } catch (err) {
    b.lastError = String(err.message || err);
    // an answer or a refused connection is final; a timeout may just be an engine busy prefilling
    b.fails++;
    if (err.definite || err.code === 'ECONNREFUSED' || b.fails >= config.pollFails) b.healthy = false;
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

// requests waiting for a slot re-check whenever one frees up or a poll lands
const waiters = new Set();

function wake() {
  for (const w of waiters) w();
}

function nextChange(ms) {
  return new Promise((resolve) => {
    const done = () => { clearTimeout(timer); waiters.delete(done); resolve(); };
    const timer = setTimeout(done, Math.max(1, ms));
    waiters.add(done);
  });
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
  const first = textOf(msgs.find((m) => m?.role === 'user')?.content) || textOf(body.input) || textOf(body.prompt);
  return createHash('sha256').update(`${body.model || 'default'}|${system}|${first.slice(0, 4000)}`).digest('hex').slice(0, 16);
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
  if (!(fresh >= 1000 && firstTokenMs >= 20)) return;
  const rate = Math.min(config.prefillTokS * 10, Math.max(config.prefillTokS / 10, fresh / (firstTokenMs / 1000)));
  b.prefill_tok_s = Math.round(b.prefill_tok_s ? 0.7 * b.prefill_tok_s + 0.3 * rate : rate);
}

// resolves to a backend with a slot already reserved, or none when nothing can take the request
async function pick(conv, isGone) {
  const t0 = Date.now();
  let waited = false;
  for (;;) {
    if (isGone() || draining) return {};
    const elapsed = Date.now() - t0;
    const home = config.backends.find((b) => b.name === conv.backend);
    if (home && capacity(home)) return { b: reserve(home), waited };

    const healthy = config.backends.filter((b) => b.healthy);
    if (!healthy.length && elapsed >= config.pollMs + 1000) return { waited };

    const hold = patience(conv, home);
    if (elapsed >= hold) {
      const open = healthy.filter(capacity);
      if (open.length) return { b: reserve(best(open)), waited };
    }
    // everything saturated for too long: let an engine queue it, home first
    if (healthy.length && elapsed >= config.queueWaitMs) {
      return { b: reserve(home?.healthy ? home : best(healthy)), waited, overflow: true };
    }
    waited = true;
    await nextChange(Math.min(1000, (elapsed < hold ? hold : config.queueWaitMs) - elapsed));
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
    if (!tap.firstTokenAt && generated(o)) tap.firstTokenAt = Date.now();
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

function passHeaders(headers, b) {
  const out = {};
  for (const [k, v] of Object.entries(headers)) if (!HOP.has(k)) out[k] = v;
  out['x-strata-backend'] = b.name;
  return out;
}

// retryable means nothing reached the client yet; hold5xx buffers an engine 5xx instead of relaying it
function forward(b, req, url, payload, res, hold5xx = false) {
  return new Promise((resolve) => {
    if (res.destroyed) return resolve({ ok: false, retryable: false, aborted: true, error: 'client went away', elapsed: 0 });
    const t0 = Date.now();
    let settled = false;
    let connected = false;
    let gone = false;
    let connectTimer;

    const done = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(connectTimer);
      res.off('close', onClientClose);
      resolve({ elapsed: Date.now() - t0, ...result });
    };
    const fail = (err) => {
      if (settled) return;
      const error = String(err?.message || err);
      if (!connected && !gone) {
        b.healthy = false;
        b.lastError = error;
      }
      // a half-sent answer has to look broken to the client, not complete
      if (res.headersSent && !res.writableEnded) res.destroy();
      done({ ok: false, retryable: !res.headersSent && !gone, aborted: gone, error });
    };
    const onClientClose = () => {
      if (res.writableFinished) return;
      gone = true;
      up.destroy(new Error('client went away'));
    };

    const headers = { accept: req.headers.accept || 'application/json' };
    if (payload) {
      headers['content-type'] = req.headers['content-type'] || 'application/json';
      headers['content-length'] = payload.length;
    }

    const up = http.request({ host: b.host, port: b.port, path: url, method: req.method, headers, agent, timeout: config.readTimeoutMs }, (r) => {
      r.on('error', fail);
      if (hold5xx && r.statusCode >= 500) {
        const chunks = [];
        r.on('data', (c) => chunks.push(c));
        r.on('end', () => done({ ok: false, retryable: !gone, error: `upstream ${r.statusCode}`, status: r.statusCode, headers: r.headers, body: Buffer.concat(chunks) }));
        return;
      }
      try {
        res.writeHead(r.statusCode, passHeaders(r.headers, b));
      } catch (err) {
        r.resume();
        return fail(err);
      }
      const tap = usageTap(/event-stream/.test(r.headers['content-type'] || ''));
      r.on('data', (c) => {
        res.write(c);
        tap.write(c);
      });
      r.on('end', () => {
        res.end();
        done({ ok: true, status: r.statusCode, usage: tap.end(), firstTokenMs: tap.firstTokenAt && tap.firstTokenAt - t0 });
      });
    });
    up.on('socket', (s) => {
      if (settled || !s.connecting) {
        connected = true;
        return;
      }
      connectTimer = setTimeout(() => up.destroy(new Error('connect timeout')), config.connectTimeoutMs);
      s.once('connect', () => {
        connected = true;
        clearTimeout(connectTimer);
      });
    });
    up.on('timeout', () => up.destroy(new Error('read timeout')));
    up.on('error', fail);
    up.on('close', () => { if (!up.res?.complete) fail(new Error('engine closed the connection')); });
    res.on('close', onClientClose);
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

async function handleChat(req, res, raw) {
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

  let gone = false;
  res.on('close', () => {
    if (res.writableFinished) return;
    gone = true;
    wake();
  });

  const picked = await pick(conv, () => gone || res.destroyed);
  if (!picked.b) {
    if (gone) return;
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
      out = await forward(b, req, req.url, raw, res, hold5xx);
    } finally {
      release(b);
    }
    if (!out.ok && !out.aborted) {
      stats.errors++;
      log(`fail ${b.name} ${out.error} (${out.elapsed}ms)`);
    }
    return out;
  };

  let served = picked.b;
  let out = await attempt(served, true);
  if (!out.ok && out.retryable) {
    const open = config.backends.filter((x) => x !== served && capacity(x));
    if (open.length) {
      stats.retried++;
      served = reserve(best(open));
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
      conv.prompt_tokens = u.prompt;
      if (out.firstTokenMs) learnPrefill(served, u, out.firstTokenMs);
    }
    const status = out.status === 200 ? '' : ` status=${out.status}`;
    log(`${served.name} key=${key} turns=${conv.turns}${status} prompt=${u?.prompt ?? '-'} cached=${u?.cached ?? '-'} out=${u?.completion ?? '-'} ${out.elapsed}ms`);
  } else if (out.aborted) {
    log(`gone ${served.name} key=${key} after ${out.elapsed}ms`);
  } else if (!res.headersSent) {
    if (out.status) {
      res.writeHead(out.status, passHeaders(out.headers, served));
      res.end(out.body);
    } else {
      send(res, 502, { error: out.error, backend: served.name });
    }
  }
}

async function relay(req, res, url, raw) {
  const b = config.backends.find((x) => x.healthy);
  if (!b) return send(res, 503, { error: 'no healthy backend' });
  const out = await forward(b, req, url, req.method === 'GET' ? null : raw, res);
  if (!out.ok && !out.aborted && !res.headersSent) send(res, 502, { error: out.error, backend: b.name });
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
  const path = (req.url || '/').split('?')[0];
  if (req.method === 'GET' && LOCAL.has(path)) return handleGet(res, path);

  const routed = req.method === 'POST' && ROUTED.has(path);
  if (!routed && req.method !== 'GET') {
    if (req.method !== 'POST' || LOCAL.has(path)) return send(res, 405, { error: 'method not allowed' });
    if (config.passthrough.size && !config.passthrough.has(path)) return send(res, 404, { error: 'not forwarded (see PASSTHROUGH)' });
  }

  readBody(req, res, (raw) => {
    active++;
    const work = routed ? handleChat(req, res, raw) : relay(req, res, path === '/models' ? '/v1/models' : req.url, raw);
    work
      .catch((err) => {
        console.error('strata-router: unexpected error', err);
        if (!res.headersSent) send(res, 500, { error: 'internal error' });
        else res.destroy();
      })
      .finally(() => {
        active--;
        if (draining && !active) exit();
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
  server.close();
  wake();
  if (!active) return exit();
  log(`draining ${active} request(s) for up to ${config.shutdownGraceMs}ms`);
  setTimeout(exit, config.shutdownGraceMs);
}

function exit() {
  saveState();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
