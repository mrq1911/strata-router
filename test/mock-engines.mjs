// two fake strata engines: same API surface, deterministic latency, no GPU.
import http from 'node:http';

export function makeEngine(name, port, batch = 2, latencyMs = 300) {
  const inflight = new Set();
  const parked = 0;

  const server = http.createServer((req, res) => {
    if (req.url === '/v1/models' || req.url === '/models') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ object: 'list', data: [{ id: 'fake', object: 'model', status: { value: 'loaded' } }] }));
      return;
    }

    if (req.url === '/metrics') {
      const slots = [];
      for (let i = 0; i < batch; i++) {
        const busy = [...inflight][i];
        slots.push({ slot: i, state: busy ? 'decoding' : 'idle', held_tokens: busy ? 1000 : 0 });
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        engine: { model: 'fake', batch_slots: batch, vram_free_mib: 1000 },
        live: { state: inflight.size ? 'busy' : 'idle', queued: 0, running: inflight.size, slots, parallel: batch, tok_s_mean: 60 },
        conversation_cache: { enabled: true, budget_mib: 32768, parked, requests: 10, requests_reused: 5 },
      }));
      return;
    }

    if (req.url === '/v1/chat/completions' && req.method === 'POST') {
      let raw = '';
      req.on('data', (c) => (raw += c));
      req.on('end', () => {
        const id = Math.random().toString(36).slice(2);
        inflight.add(id);
        setTimeout(() => {
          inflight.delete(id);
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            id: 'chatcmpl-' + id,
            object: 'chat.completion',
            model: 'fake',
            choices: [{ index: 0, message: { role: 'assistant', content: `answer from ${name}` }, finish_reason: 'stop' }],
            usage: { prompt_tokens: raw.length, completion_tokens: 10, prompt_tokens_details: { cached_tokens: Math.min(50, raw.length) } },
          }));
        }, latencyMs);
      });
      return;
    }

    res.writeHead(404);
    res.end('{}');
  });

  server.listen(port, '127.0.0.1', () => console.log(`fake engine ${name} on ${port}`));
  return { name, port, server };
}
