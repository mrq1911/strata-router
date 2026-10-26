// fake strata engines: same API surface, deterministic latency, no GPU.
import http from 'node:http';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';

const USAGE = { prompt: 1000, cached: 600, completion: 10 };

const json = (res, status, obj) => {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
};
const event = (res, data) => res.write(`data: ${typeof data === 'string' ? data : JSON.stringify(data)}\n\n`);

const authorized = (engine, req) => {
  if (!engine.apiKey) return true;
  const auth = req.headers.authorization || '';
  return (auth.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : req.headers['x-api-key']) === engine.apiKey;
};
const unauthorized = (res) => json(res, 401, { error: { type: 'authentication_error', message: 'missing or wrong API key' } });

export function makeEngine(name, port, batch = 2, latencyMs = 300) {
  const engine = {
    name,
    port,
    mode: 'ok',
    queued: 0,
    metricsStatus: 200,
    metricsHangup: false,
    reportIdle: false,
    noBatch: false,
    unloaded: false,
    apiKey: '',
    prefillMs: 0,
    tailMs: 0,
    bigBytes: 8 << 20,
    usage: { ...USAGE },
    log: [],
    posts: 0,
    aborted: 0,
    completed: 0,
    since: Date.now() / 1000,
    parks: 0,
    // a new server start, or with sameServer the engine alone, which only zeroes the cache counters
    restart({ sameServer = false } = {}) {
      if (!sameServer) engine.since += 1;
      engine.parks = 0;
    },
  };
  const inflight = new Map();

  engine.server = http.createServer((req, res) => {
    if (req.url === '/v1/models' || req.url === '/models') {
      json(res, 200, { object: 'list', data: [{ id: 'fake', object: 'model', status: { value: 'loaded' } }] });
      return;
    }

    if (req.url === '/metrics') {
      if (engine.metricsHangup) return req.socket.destroy();
      if (!authorized(engine, req)) return unauthorized(res);
      if (engine.metricsStatus !== 200) return json(res, engine.metricsStatus, { error: 'model loading' });
      const running = engine.reportIdle ? 0 : inflight.size;
      const live = { state: engine.unloaded ? 'unloaded' : running ? 'generating' : 'idle', queued: engine.queued, tok_s_mean: 60 };
      if (!engine.noBatch) {
        const slots = [];
        const since = [...inflight.values()];
        for (let i = 0; i < batch; i++) {
          const busy = i < running ? { prompt_tokens: 1000, generated: 5, tok_s: 60, elapsed_s: (Date.now() - since[i]) / 1000 } : {};
          slots.push({ slot: i, state: i < running ? 'decoding' : 'idle', held_tokens: i < running ? 1000 : 0, ...busy });
        }
        Object.assign(live, { running, slots, parallel: batch });
      }
      const p = engine.parks;
      json(res, 200, {
        engine: { model: 'fake', version: '0.0-mock', ...(engine.noBatch ? {} : { batch_slots: batch }), vram_free_mib: 1000 },
        live,
        conversation_cache: {
          enabled: true, budget_mib: 32768, slots: 8, parked: Math.min(8, p), parks: p, restores: 0, evictions: Math.max(0, p - 8),
          requests: p, requests_reused: Math.floor(p / 2), reused_tokens: 600 * p, prompt_tokens: 1000 * p,
        },
        totals: { since: engine.since, requests: engine.posts },
        hardware: { gpu_util: 50, gpu_mem_used: 20e9, gpu_mem_total: 24e9, cpu: 10, ram_used: 50e9, ram_total: 64e9 },
        history: { gpu_util: [10, 50], tok_s: [0, 60] },
        requests: p ? [{ output_tokens: 50, decode_tok_s: 100, prompt_read: 2000, prompt_ms: 1000, hit_rate: 0.9, pcie_share: 0.01, drafts_offered: 10, drafts_accepted: 6 }] : [],
      });
      return;
    }

    if (req.method !== 'POST') return json(res, 404, { error: 'engine 404' });
    if (!authorized(engine, req)) return unauthorized(res);

    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      engine.posts++;
      const raw = Buffer.concat(chunks);
      let body = {};
      try { body = JSON.parse(raw) || {}; } catch {}
      const first = Array.isArray(body.messages) ? body.messages.find((m) => m?.role === 'user')?.content : undefined;
      engine.log.push({ url: req.url, first, headers: req.headers });
      const id = Math.random().toString(36).slice(2);
      inflight.set(id, Date.now());
      res.on('close', () => {
        inflight.delete(id);
        engine.parks++;
      });
      generate(engine, req, res, body, raw, Number.isFinite(body.latency_ms) ? body.latency_ms : latencyMs);
    });
  });

  engine.server.listen(port, '127.0.0.1');
  return engine;
}

function generate(engine, req, res, body, raw, latencyMs) {
  const { mode } = engine;
  if (mode === 'hang') return;
  if (mode === '500') return json(res, 500, { error: 'engine exploded' });
  if (mode === '500big') return json(res, 500, { error: 'x'.repeat(2 << 20) });

  if (mode === 'drop' || mode === 'reset') {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    event(res, { choices: [{ delta: { content: 'partial' } }] });
    setTimeout(() => (mode === 'reset' ? res.socket.resetAndDestroy() : res.socket.destroy()), 50);
    return;
  }

  if (mode === 'drop-headers') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.flushHeaders();
    setTimeout(() => res.socket.destroy(), 50);
    return;
  }

  if (mode === 'big') {
    res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': engine.bigBytes });
    res.end(Buffer.alloc(engine.bigBytes, 'x'));
    return;
  }

  if (mode === 'slow') {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    let n = 0;
    const timer = setInterval(() => {
      event(res, { choices: [{ delta: { content: `t${n}` } }] });
      if (++n < 20) return;
      clearInterval(timer);
      res.end();
      engine.completed++;
    }, 50);
    res.on('close', () => {
      if (n >= 20) return;
      clearInterval(timer);
      engine.aborted++;
    });
    return;
  }

  setTimeout(() => reply(engine, req, res, body, raw), latencyMs);
}

function reply(engine, req, res, body, raw) {
  const text = `answer from ${engine.name}`;
  const { prompt, cached, completion } = engine.usage || {};
  const usage = engine.usage && { prompt_tokens: prompt, completion_tokens: completion, prompt_tokens_details: { cached_tokens: cached } };

  if (req.url.startsWith('/v1/messages')) {
    return json(res, 200, {
      type: 'message',
      role: 'assistant',
      content: [{ type: 'text', text }],
      usage: engine.usage && { input_tokens: prompt - cached, cache_read_input_tokens: cached, cache_creation_input_tokens: 0, output_tokens: completion },
    });
  }

  if (req.url.startsWith('/v1/responses')) {
    return json(res, 200, {
      object: 'response',
      output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text }] }],
      usage: engine.usage && { input_tokens: prompt, input_tokens_details: { cached_tokens: cached }, output_tokens: completion },
    });
  }

  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    event(res, { object: 'chat.completion.chunk', choices: [{ index: 0, delta: { role: 'assistant', content: '' } }], usage: null });
    setTimeout(() => {
      for (const content of ['answer from ', engine.name]) event(res, { object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content } }], usage: null });
      if (usage) event(res, { object: 'chat.completion.chunk', choices: [], usage });
      event(res, '[DONE]');
      setTimeout(() => res.end(), engine.tailMs);
    }, engine.prefillMs);
    return;
  }

  json(res, 200, {
    id: 'chatcmpl-' + Math.random().toString(36).slice(2),
    object: 'chat.completion',
    model: 'fake',
    choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }],
    usage,
    sha256: createHash('sha256').update(raw).digest('hex'),
  });
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  for (const e of [makeEngine('engineA', 9101), makeEngine('engineB', 9102)]) {
    e.server.on('listening', () => console.log(`fake engine ${e.name} on ${e.port}`));
  }
  console.log('STRATA_BACKENDS=a=127.0.0.1:9101,b=127.0.0.1:9102 node router.mjs');
}
