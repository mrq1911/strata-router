#!/usr/bin/env node
// strata-router: one OpenAI-compatible endpoint in front of several strata engines.
// conversations stay pinned to the engine that holds their KV; spill only when that
// engine has no free batch slot or is unhealthy. node >= 20, no dependencies.

import http from 'node:http';

const DEFAULT_BACKENDS = process.env.STRATA_BACKENDS || '127.0.0.1:8080';

const config = {
  backends: parseBackends(DEFAULT_BACKENDS),
  port: Number(process.env.PORT || 8000),
  pollMs: Number(process.env.POLL_MS || 2000),
  maxInflight: Number(process.env.MAX_INFLIGHT || 2),
  queueWaitMs: Number(process.env.MAX_QUEUE_WAIT || 30000),
  connectTimeoutMs: Number(process.env.CONNECT_TIMEOUT || 10000),
  readTimeoutMs: Number(process.env.READ_TIMEOUT || 300000),
  maxConversations: Number(process.env.MAX_CONVERSATIONS || 500),
  verbose: process.env.LOG !== 'off',
};

function parseBackends(spec) {
  return spec
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((entry) => {
      // "name=host:port" or "host:port"
      const [name, rest] = entry.includes('=') ? entry.split('=', 2) : [null, entry];
      const [host, port] = rest.split(':');
      return {
        name: name || host,
        host,
        port: Number(port || 8080),
        healthy: false,
        slots: [],
        running: 0,
        queued: 0,
        inflight: 0,
        batch: 2,
        tok_s_mean: null,
        held_tokens: 0,
        vram_free_mib: 0,
        cache: null,
        routed: 0,
        lastError: null,
        lastPoll: 0,
      };
    });
}

const home = new Map(); // conversation key -> backend name
const meta = new Map(); // conversation key -> { turns, prompt, first }
const stats = {
  requests: 0,
  cold: 0,
  migrated: 0,
  queued: 0,
  overflow: 0,
  errors: 0,
  prompt_tokens: 0,
  completion_tokens: 0,
  cached_tokens: 0,
  started: new Date().toISOString(),
};

const log = (msg) => { if (config.verbose) console.log(new Date().toISOString().slice(11, 23), msg); };

function getJson(b, path, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const req = http.request({ host: b.host, port: b.port, path, method: 'GET', timeout }, (res) => {
      let buf = '';
      res.on('data', (c) => (buf += c));
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); } catch (err) { reject(err); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('poll timeout')));
    req.end();
  });
}

async function poll() {
  for (const b of config.backends) {
    try {
      const m = await getJson(b, '/metrics');
      const live = m.live || {};
      b.healthy = true;
      b.lastError = null;
      b.lastPoll = Date.now();
      b.slots = live.slots || [];
      b.running = live.running || 0;
      b.queued = live.queued || 0;
      b.batch = m.engine?.batch_slots ?? 2;
      b.tok_s_mean = live.tok_s_mean ?? null;
      b.held_tokens = (live.slots || []).reduce((s, x) => s + (x.held_tokens || 0), 0);
      b.vram_free_mib = m.engine?.vram_free_mib ?? 0;
      b.cache = m.conversation_cache || null;
    } catch (err) {
      b.healthy = false;
      b.lastError = String(err.message || err);
    }
  }
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

function textOf(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) return content.map((c) => (typeof c === 'string' ? c : c.text || '')).join(' ');
  return '';
}

// fork-safe key: system + first user message, so a branch of a conversation
// inherits the same home engine and reuses the prefix cache
function conversationKey(body) {
  const msgs = Array.isArray(body.messages) ? body.messages : [];
  let system = '';
  if (typeof body.system === 'string') system = body.system;
  else if (typeof body.instructions === 'string') system = body.instructions;
  else if (msgs[0] && ['system', 'developer'].includes(msgs[0].role)) system = textOf(msgs[0].content);

  let first = '';
  for (const m of msgs) {
    if (m.role === 'user') { first = textOf(m.content); break; }
  }
  if (!first && Array.isArray(body.input)) first = textOf(body.input);
  return `${body.model || 'default'}|${system}|${first.slice(0, 4000)}`;
}

async function pick(key) {
  const owner = home.get(key);
  const ownerB = config.backends.find((b) => b.name === owner);
  if (ownerB && capacity(ownerB)) return ownerB;

  const open = config.backends.filter(capacity);
  if (open.length) {
    const chosen = open.sort((a, b) => score(b) - score(a))[0];
    if (ownerB && chosen.name !== ownerB) stats.migrated++;
    return chosen;
  }

  // everything saturated: wait for a slot, then let the engine's own queue take it
  const deadline = Date.now() + config.queueWaitMs;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 400));
    const open = config.backends.filter(capacity);
    if (open.length) {
      stats.queued++;
      return open.sort((a, b) => score(b) - score(a))[0];
    }
  }
  stats.overflow++;
  return config.backends.filter((b) => b.healthy).sort((a, b) => score(b) - score(a))[0] || null;
}

function usageFrom(body) {
  const i = body.indexOf('"usage"');
  if (i < 0) return null;
  const j = body.indexOf('{', i);
  if (j < 0) return null;
  let depth = 0, end = -1;
  for (let k = j; k < body.length; k++) {
    if (body[k] === '{') depth++;
    else if (body[k] === '}') { depth--; if (depth === 0) { end = k; break; } }
  }
  if (end < 0) return null;
  try {
    const u = JSON.parse(body.slice(j, end + 1));
    u.cached = u.prompt_tokens_details?.cached_tokens ?? 0;
    return u;
  } catch (err) { return null; }
}

function forward(b, path, payload, res) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    let firstByte = false;
    let body = '';

    const req = http.request(
      {
        host: b.host,
        port: b.port,
        path,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload),
          accept: 'application/json',
        },
        timeout: config.readTimeoutMs,
      },
      (ur) => {
        if (ur.statusCode >= 500 && !firstByte) {
          ur.resume();
          resolve({ ok: false, error: `upstream ${ur.statusCode}`, elapsed: Date.now() - t0 });
          return;
        }
        res.writeHead(ur.statusCode, {
          'content-type': ur.headers['content-type'] || 'application/json',
          'x-strata-backend': b.name,
        });
        ur.on('data', (chunk) => {
          firstByte = true;
          res.write(chunk);
          if (body.length < 1_000_000) body += chunk.toString();
        });
        ur.on('end', () => { res.end(); resolve({ ok: true, elapsed: Date.now() - t0, usage: usageFrom(body) }); });
        ur.on('error', (err) => {
          if (!firstByte) resolve({ ok: false, error: String(err.message || err), elapsed: Date.now() - t0 });
          else res.end();
        });
      }
    );
    req.on('error', (err) => resolve({ ok: false, error: String(err.message || err), elapsed: Date.now() - t0 }));
    req.on('timeout', () => req.destroy(new Error('read timeout')));
    req.write(payload);
    req.end();
  });
}

async function handleChat(path, raw, res) {
  let parsed;
  try { parsed = JSON.parse(raw); } catch (err) {
    res.writeHead(400, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'invalid json' }));
    return;
  }

  const key = conversationKey(parsed);
  const m = meta.get(key) || { turns: 0, first: new Date().toISOString(), model: parsed.model };
  if (!meta.has(key)) { stats.cold++; }
  m.turns++;
  meta.set(key, m);
  if (meta.size > config.maxConversations) {
    const oldest = meta.keys().next().value;
    meta.delete(oldest);
    home.delete(oldest);
  }

  const b = await pick(key);
  if (!b) {
    res.writeHead(503, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      error: 'no healthy backend',
      backends: config.backends.map((x) => ({ name: x.name, healthy: x.healthy, error: x.lastError })),
    }));
    return;
  }

  home.set(key, b.name);
  b.inflight++;
  stats.requests++;
  b.routed++;

  let out = await forward(b, path, raw, res);
  if (!out.ok) {
    stats.errors++;
    log(`fail ${b.name} ${out.error} (${out.elapsed}ms)`);
    const alt = config.backends.find((x) => x.name !== b.name && x.healthy && capacity(x));
    if (alt) {
      alt.inflight++;
      alt.routed++;
      home.set(key, alt.name);
      stats.migrated++;
      out = await forward(alt, path, raw, res);
      if (out.ok) log(`re-routed ${b.name} -> ${alt.name} ${out.elapsed}ms`);
      alt.inflight--;
    }
  }

  b.inflight--;

  if (out.ok) {
    const u = out.usage || {};
    stats.prompt_tokens += u.prompt_tokens || 0;
    stats.completion_tokens += u.completion_tokens || 0;
    stats.cached_tokens += u.cached || 0;
    log(`${b.name} key=${key.slice(0, 16)} turns=${m.turns} prompt=${u.prompt_tokens ?? '-'} cached=${u.cached ?? 0} out=${u.completion_tokens ?? '-'} ${out.elapsed}ms`);
  } else {
    log(`error ${b.name} ${out.error}`);
    if (!res.headersSent) {
      res.writeHead(502, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: out.error, backend: b.name }));
    }
  }
}

function handleGet(res, path) {
  if (path === '/' || path === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: config.backends.some((b) => b.healthy),
      backends: config.backends.map((b) => ({ name: b.name, host: `${b.host}:${b.port}`, healthy: b.healthy, inflight: b.inflight, queued: b.queued, running: b.running })),
    }));
    return;
  }
  if (path === '/stats') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      stats,
      backends: config.backends.map((b) => ({
        name: b.name,
        host: `${b.host}:${b.port}`,
        healthy: b.healthy,
        inflight: b.inflight,
        routed: b.routed,
        batch: b.batch,
        running: b.running,
        queued: b.queued,
        held_tokens: b.held_tokens,
        tok_s_mean: b.tok_s_mean,
        vram_free_mib: b.vram_free_mib,
        parked: b.cache?.parked ?? 0,
        cache_budget_mib: b.cache?.budget_mib ?? 0,
        reused: b.cache?.requests_reused ?? 0,
        requests: b.cache?.requests ?? 0,
        lastError: b.lastError,
      })),
      conversations: [...home.entries()].slice(-50).map(([k, v]) => ({ key: k.slice(0, 16), backend: v, turns: meta.get(k)?.turns ?? 0 })),
    }));
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
}

const CHAT_PATHS = new Set(['/v1/chat/completions', '/v1/messages', '/v1/responses']);

const server = http.createServer((req, res) => {
  const path = req.url || '/';

  if (req.method === 'POST' && CHAT_PATHS.has(path)) {
    let raw = '';
    req.on('data', (c) => { raw += c; if (raw.length > 20_000_000) { res.writeHead(413); res.end('payload too large'); req.destroy(); } });
    req.on('end', () => handleChat(path, raw, res));
    return;
  }

  if (req.method === 'GET') { handleGet(res, path); return; }

  res.writeHead(405, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'method not allowed' }));
});

poll();
setInterval(poll, config.pollMs);
server.listen(config.port, process.env.BIND || '0.0.0.0', () => {
  log(`strata-router on :${config.port} -> ${config.backends.map((b) => `${b.name}=${b.host}:${b.port}`).join(', ')}`);
});

process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000);
});
