// fake avalon (llama.cpp fork) server: /props, /slots, prometheus /metrics and streaming chat,
// with the counters the shim reads. no strata in it, that is the point.
import http from 'node:http';
import { randomBytes } from 'node:crypto';

const num = (v, fallback) => (Number.isFinite(Number(v)) ? Number(v) : fallback);

export function makeAvalon(port, { slots = 1, promptMs = 200, tokens = 8, metricsEndpoint = true, n_ctx = 262144 } = {}) {
  const engine = {
    port,
    marker: randomBytes(16).toString('hex'),
    started: Math.floor(Date.now() / 1000),
    metricsEndpoint,
    sleeping: false,
    failing: false,
    promptMs,
    tokens,
    slots,
    usage: { prompt: 900, cached: 0, completion: tokens },
    draftRate: 2,
    totals: { prompt: 0, cached: 0, predicted: 0, promptSeconds: 0, predictSeconds: 0, decode: 0, drafts: 0, accepted: 0, perPos: [0, 0], n_tokens_max: 0 },
    active: [],
    held: [],
    log: [],
    server: null,
  };

  const json = (res, status, obj) => {
    res.writeHead(status, { 'content-type': 'application/json' });
    res.end(JSON.stringify(obj));
  };

  const nextMarker = () => {
    engine.marker = randomBytes(16).toString('hex');
    engine.started = Math.floor(Date.now() / 1000);
    engine.totals = { prompt: 0, cached: 0, predicted: 0, promptSeconds: 0, predictSeconds: 0, decode: 0, drafts: 0, accepted: 0, perPos: [0, 0], n_tokens_max: 0 };
    engine.active = [];
    engine.held = [];
  };

  engine.restart = () => nextMarker();

  engine.server = http.createServer((req, res) => {
    const path = req.url.split('?')[0];
    if (engine.failing) return json(res, 500, { error: { type: 'server_error', message: 'fake engine is failing' } });

    if (path === '/health') return json(res, 200, { status: 'ok' });

    if (path === '/props') {
      return json(res, 200, {
        total_slots: engine.slots,
        model_alias: 'qwen3.8-flash-next',
        model_ftype: 'IQ3_S - 3.0 bpw',
        model_path: '/models/fake.gguf',
        modalities: { vision: false, video: false, audio: false },
        media_marker: `<__media_${engine.marker}__>`,
        endpoint_slots: true,
        endpoint_props: false,
        endpoint_metrics: engine.metricsEndpoint,
        build_info: 'b1-abcdef0',
        is_sleeping: engine.sleeping,
        cors_proxy_enabled: false,
      });
    }

    if (path === '/slots') {
      // like llama.cpp: an idle slot keeps reporting the prompt it still holds
      return json(res, 200, Array.from({ length: engine.slots }, (_, i) => {
        const a = engine.active[i] || null;
        const held = a || engine.held[i] || null;
        return {
          id: i,
          n_ctx,
          speculative: true,
          is_processing: Boolean(a),
          id_task: a ? 1 : -1,
          n_prompt_tokens: held ? held.prompt : 0,
          n_prompt_tokens_processed: a ? Math.min(held.prompt, Math.floor(held.prompt * a.progress)) : (held ? held.prompt : 0),
          n_prompt_tokens_cache: held ? held.cached : 0,
          params: { seed: 1, temperature: 1 },
          next_token: { has_next_token: Boolean(a), n_remain: a ? engine.tokens - a.generated : -1, n_decoded: a ? a.generated : 0 },
        };
      }));
    }

    if (path === '/metrics') {
      if (!engine.metricsEndpoint) return json(res, 501, { error: { code: 501, message: 'This server does not support metrics endpoint. Start it with `--metrics`' } });
      const t = engine.totals;
      res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4', 'process-start-time-unix': String(engine.started) });
      return res.end([
        '# HELP llamacpp:prompt_tokens_total Number of prompt tokens processed, excluding cached tokens',
        '# TYPE llamacpp:prompt_tokens_total counter',
        `llamacpp:prompt_tokens_total ${t.prompt}`,
        `llamacpp:prompt_tokens_cached_total ${t.cached}`,
        `llamacpp:prompt_seconds_total ${t.promptSeconds}`,
        `llamacpp:tokens_predicted_total ${t.predicted}`,
        `llamacpp:tokens_predicted_seconds_total ${t.predictSeconds}`,
        `llamacpp:n_decode_total ${t.decode}`,
        `llamacpp:n_tokens_max ${t.n_tokens_max}`,
        `llamacpp:spec_decode_num_draft_tokens_total ${t.drafts}`,
        `llamacpp:spec_decode_num_accepted_tokens_total ${t.accepted}`,
        `llamacpp:spec_decode_num_drafts_total ${t.drafts ? Math.round(t.drafts / engine.draftRate) : 0}`,
        `llamacpp:prompt_tokens_seconds ${t.promptSeconds ? Math.round(t.prompt / t.promptSeconds) : 0}`,
        `llamacpp:predicted_tokens_seconds ${t.predictSeconds ? Math.round(t.predicted / t.predictSeconds) : 0}`,
        `llamacpp:requests_processing ${engine.active.length}`,
        'llamacpp:requests_deferred 0',
        `llamacpp:spec_decode_num_accepted_tokens_per_pos_total{position="0"} ${t.perPos[0]}`,
        `llamacpp:spec_decode_num_accepted_tokens_per_pos_total{position="1"} ${t.perPos[1]}`,
      ].join('\n') + '\n');
    }

    if (path === '/v1/models') {
      return json(res, 200, { object: 'list', data: [{ id: 'qwen3.8-flash-next', object: 'model', owned_by: 'llamacpp' }] });
    }

    if (path === '/v1/chat/completions' && req.method === 'POST') {
      let body = '';
      req.on('data', (c) => (body += c));
      req.on('end', async () => {
        let parsed = {};
        try { parsed = JSON.parse(body); } catch {}
        // like avalon's qwen3.8 template: anything outside these levels raises
        const effort = parsed.reasoning_effort ?? parsed.chat_template_kwargs?.reasoning_effort;
        if (effort !== undefined && !['low', 'medium', 'xhigh'].includes(effort)) {
          return json(res, 500, { error: { code: 500, message: `Unexpected reasoning effort ${effort}. Supported types are xhigh (default), medium, and low.` } });
        }
        engine.lastRequest = parsed;
        const stream = parsed.stream === true;
        const u = engine.usage;
        const me = { prompt: u.prompt, cached: u.cached, generated: 0, progress: 0 };
        if (engine.active.length < engine.slots) engine.active.push(me);
        const t0 = Date.now();
        if (stream) {
          res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        }
        // the prompt is read before the first token, and the slot says so while it is
        me.progress = 0.5;
        await new Promise((r) => setTimeout(r, engine.promptMs));
        me.progress = 1;
        for (let i = 0; i < engine.tokens; i++) {
          await new Promise((r) => setTimeout(r, Math.max(1, Math.round(1000 / 60))));
          me.generated = i + 1;
          me.progress = 1;
          const chunk = { id: 'chatcmpl-fake', object: 'chat.completion.chunk', choices: [{ index: 0, delta: { content: 'x' }, finish_reason: null }] };
          if (stream) res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        }
        const promptSeconds = engine.promptMs / 1000;
        const predictSeconds = engine.tokens / 60;
        engine.totals.prompt += u.prompt - u.cached;
        engine.totals.cached += u.cached;
        engine.totals.predicted += u.completion;
        engine.totals.promptSeconds += promptSeconds;
        engine.totals.predictSeconds += predictSeconds;
        engine.totals.decode += 1;
        engine.totals.drafts += u.completion * engine.draftRate;
        engine.totals.accepted += Math.round(u.completion * engine.draftRate * 0.8);
        engine.totals.perPos[0] += Math.round(u.completion * 0.8);
        engine.totals.perPos[1] += Math.round(u.completion * 0.6);
        engine.totals.n_tokens_max = Math.max(engine.totals.n_tokens_max, u.prompt + u.completion);
        const usage = {
          prompt_tokens: u.prompt,
          completion_tokens: u.completion,
          total_tokens: u.prompt + u.completion,
          prompt_tokens_details: { cached_tokens: u.cached },
        };
        const timings = {
          cache_n: u.cached,
          prompt_n: u.prompt,
          prompt_ms: engine.promptMs,
          prompt_per_token_ms: engine.promptMs / u.prompt,
          prompt_per_second: u.prompt / (engine.promptMs / 1000),
          predicted_n: u.completion,
          predicted_ms: engine.tokens * 16,
          predicted_per_token_ms: engine.tokens * 16 / u.completion,
          predicted_per_second: u.completion / (engine.tokens * 16 / 1000),
          draft_n: u.completion * engine.draftRate,
          draft_n_accepted: Math.round(u.completion * engine.draftRate * 0.8),
        };
        if (stream) {
          res.write(`data: ${JSON.stringify({ choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage, timings })}\n\n`);
          res.write('data: [DONE]\n\n');
          res.end();
        } else {
          json(res, 200, { id: 'chatcmpl-fake', object: 'chat.completion', model: 'qwen3.8-flash-next', choices: [{ index: 0, message: { role: 'assistant', content: 'x'.repeat(engine.tokens) }, finish_reason: 'stop' }], usage, timings });
        }
        const i2 = engine.active.indexOf(me);
        if (i2 >= 0) {
          engine.active.splice(i2, 1);
          engine.held[i2] = me;
        }
        engine.log.push({ at: Date.now(), prompt: u.prompt, ms: Date.now() - t0 });
      });
      return;
    }

    json(res, 404, { error: { message: `no such endpoint: ${path}` } });
  });

  engine.server.listen(port, '127.0.0.1');
  return engine;
}
