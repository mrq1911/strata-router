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

export function makeEngine(name, port, batch = 2, latencyMs = 300) {
  const engine = { name, port, mode: 'ok', queued: 0, metricsStatus: 200, posts: 0, aborted: 0, completed: 0 };
  const inflight = new Set();

  engine.server = http.createServer((req, res) => {
    if (req.url === '/v1/models' || req.url === '/models') {
      json(res, 200, { object: 'list', data: [{ id: 'fake', object: 'model', status: { value: 'loaded' } }] });
      return;
    }

    if (req.url === '/metrics') {
      if (engine.metricsStatus !== 200) return json(res, engine.metricsStatus, { error: 'model loading' });
      const slots = [];
      for (let i = 0; i < batch; i++) {
        const busy = [...inflight][i];
        slots.push({ slot: i, state: busy ? 'decoding' : 'idle', held_tokens: busy ? 1000 : 0 });
      }
      json(res, 200, {
        engine: { model: 'fake', batch_slots: batch, vram_free_mib: 1000 },
        live: { state: inflight.size ? 'busy' : 'idle', queued: engine.queued, running: inflight.size, slots, parallel: batch, tok_s_mean: 60 },
        conversation_cache: { enabled: true, budget_mib: 32768, parked: 0, requests: 10, requests_reused: 5 },
      });
      return;
    }

    if (req.method !== 'POST') return json(res, 404, { error: 'engine 404' });

    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      engine.posts++;
      const raw = Buffer.concat(chunks);
      let body = {};
      try { body = JSON.parse(raw) || {}; } catch {}
      const id = Math.random().toString(36).slice(2);
      inflight.add(id);
      res.on('close', () => inflight.delete(id));
      generate(engine, req, res, body, raw, latencyMs);
    });
  });

  engine.server.listen(port, '127.0.0.1');
  return engine;
}

function generate(engine, req, res, body, raw, latencyMs) {
  const { mode } = engine;
  if (mode === '500') return json(res, 500, { error: 'engine exploded' });

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
  const { prompt, cached, completion } = USAGE;

  if (req.url.startsWith('/v1/messages')) {
    return json(res, 200, {
      type: 'message',
      role: 'assistant',
      content: [{ type: 'text', text }],
      usage: { input_tokens: prompt - cached, cache_read_input_tokens: cached, cache_creation_input_tokens: 0, output_tokens: completion },
    });
  }

  if (req.url.startsWith('/v1/responses')) {
    return json(res, 200, {
      object: 'response',
      output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text }] }],
      usage: { input_tokens: prompt, input_tokens_details: { cached_tokens: cached }, output_tokens: completion },
    });
  }

  const usage = { prompt_tokens: prompt, completion_tokens: completion, prompt_tokens_details: { cached_tokens: cached } };
  if (body.stream) {
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    for (const content of ['answer from ', engine.name]) event(res, { object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content } }], usage: null });
    event(res, { object: 'chat.completion.chunk', choices: [], usage });
    event(res, '[DONE]');
    res.end();
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
