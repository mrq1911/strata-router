// fake sglang server: /server_info, /v1/loads and chat that streams several tokens per chunk, as speculative
// decoding does, with the usage chunk only when the request asks for it
import http from 'node:http';

export function makeSglang(port, { slots = 1, promptMs = 200, chunks = 8, perChunk = 3 } = {}) {
  const engine = {
    port,
    failing: false,
    promptMs,
    chunks,
    perChunk,
    slots,
    prompt: 900,
    loadWeight: 100 + Math.random(),
    active: [],
    waiting: 0,
    lastRequest: null,
    server: null,
  };

  const json = (res, status, obj) => {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(obj));
  };

  // a new process measures new load times, which is all /server_info has to tell a restart by
  engine.restart = () => {
    engine.loadWeight = 100 + Math.random();
    engine.active = [];
  };

  engine.server = http.createServer((req, res) => {
    const path = req.url.split('?')[0];
    if (engine.failing) return json(res, 503, { error: { message: 'fake engine is failing' } });

    if (path === '/health') return res.end();

    if (path === '/server_info') {
      return json(res, 200, {
        served_model_name: 'qwen3.8-flash-next-exl3',
        model_path: '/models/fake',
        version: '0.5.20',
        context_length: 262144,
        max_running_requests: engine.slots,
        quantization: 'exl3',
        speculative_algorithm: 'EAGLE',
        startup_time: { load_weight: engine.loadWeight, cuda_graph: { target_verify: 36.5 } },
      });
    }

    if (path === '/v1/loads') {
      return json(res, 200, {
        version: '0.5.20',
        loads: [{
          num_running_reqs: engine.active.length,
          num_waiting_reqs: engine.waiting,
          num_used_tokens: engine.active.reduce((n, a) => n + a.held, 0),
          max_total_num_tokens: 263168,
          max_running_requests: engine.slots,
          gen_throughput: engine.active.length ? 75.5 : 0,
          speculative: { accept_length: 2.2 },
        }],
      });
    }

    if (path === '/v1/models') {
      return json(res, 200, { object: 'list', data: [{ id: 'qwen3.8-flash-next-exl3', object: 'model', owned_by: 'sglang' }] });
    }

    if (path === '/v1/chat/completions' && req.method === 'POST') {
      let body = '';
      req.on('data', (c) => (body += c));
      req.on('end', async () => {
        let parsed = {};
        try { parsed = JSON.parse(body); } catch {}
        // the qwen3.8 template raises on any other level and sglang answers 400
        const effort = parsed.reasoning_effort ?? parsed.chat_template_kwargs?.reasoning_effort;
        if (effort !== undefined && !['low', 'medium', 'xhigh'].includes(effort)) {
          return json(res, 400, { object: 'error', message: `Unexpected reasoning effort ${effort}. Supported types are xhigh (default), medium, and low.`, code: 400 });
        }
        engine.lastRequest = parsed;
        const stream = parsed.stream === true;
        const completion = engine.chunks * engine.perChunk;
        const me = { held: engine.prompt };
        if (engine.active.length < engine.slots) engine.active.push(me);
        else engine.waiting++;
        if (stream) res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        await new Promise((r) => setTimeout(r, engine.promptMs));
        for (let i = 0; i < engine.chunks; i++) {
          await new Promise((r) => setTimeout(r, 30));
          me.held += engine.perChunk;
          const chunk = { id: 'fake', object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content: 'x'.repeat(engine.perChunk) }, finish_reason: null }] };
          if (stream) res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        }
        const usage = { prompt_tokens: engine.prompt, completion_tokens: completion, total_tokens: engine.prompt + completion, prompt_tokens_details: null };
        if (stream) {
          res.write(`data: ${JSON.stringify({ id: 'fake', object: 'chat.completion.chunk', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
          if (parsed.stream_options?.include_usage) res.write(`data: ${JSON.stringify({ id: 'fake', object: 'chat.completion.chunk', choices: [], usage })}\n\n`);
          res.write('data: [DONE]\n\n');
          res.end();
        } else {
          json(res, 200, { id: 'fake', object: 'chat.completion', model: parsed.model, choices: [{ index: 0, message: { role: 'assistant', content: 'x'.repeat(completion) }, finish_reason: 'stop' }], usage });
        }
        const i = engine.active.indexOf(me);
        if (i >= 0) engine.active.splice(i, 1);
        else engine.waiting = Math.max(0, engine.waiting - 1);
      });
      return;
    }

    json(res, 404, { error: { message: `no such endpoint: ${path}` } });
  });

  engine.server.listen(port, '127.0.0.1');
  return engine;
}
