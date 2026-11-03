#!/usr/bin/env node
// sglang-shim: makes an SGLang server look like a Strata engine to strata-router.
// it fronts one engine and relays everything untouched except GET /metrics, which it answers itself:
// SGLang's /metrics is prometheus text (and absent without --enable-metrics); /server_info and /v1/loads say the rest.
// node >= 20, no dependencies.

import http from 'node:http';
import { execFile } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { StringDecoder } from 'node:string_decoder';
import { setTimeout as sleep } from 'node:timers/promises';

const problems = [];

const config = {
  engine: parseHostPort(process.env.SGLANG || '127.0.0.1:30000', 'SGLANG'),
  port: num('PORT', 30001, { min: 1, max: 65535 }),
  bind: process.env.BIND || '0.0.0.0',
  readTimeoutMs: num('READ_TIMEOUT', 300000, { min: 1 }),
  freshMs: num('FRESH_MS', 600, { min: 0 }),
  sampleMs: num('SAMPLE_MS', 2000, { min: 200 }),
  infoMs: num('INFO_MS', 10000, { min: 500 }),
  recentKept: num('RECENT_KEPT', 50, { min: 1 }),
  gpuIndex: num('GPU_INDEX', 0),
  apiKey: readKey(process.env.SGLANG_API_KEY_FILE),
  smi: process.env.NVIDIA_SMI || '/usr/bin/nvidia-smi',
  verbose: process.env.LOG !== 'off',
};

if (problems.length) {
  for (const p of problems) console.error(`sglang-shim: ${p}`);
  process.exit(1);
}

function num(name, fallback, { min = 0, max = 2 ** 31 - 1, integer = true } = {}) {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const n = Number(raw);
  if (Number.isFinite(n) && n >= min && n <= max && (!integer || Number.isInteger(n))) return n;
  problems.push(`${name}=${raw}: expected ${integer ? 'an integer' : 'a number'} from ${min} to ${max}`);
  return fallback;
}

function readKey(path) {
  if (!path) return '';
  try {
    return readFileSync(path, 'utf8').trim();
  } catch (err) {
    problems.push(`SGLANG_API_KEY_FILE=${path}: not readable (${err.code})`);
    return '';
  }
}

function parseHostPort(spec, name) {
  const m = /^(?:([^=]+)=)?(\[[0-9a-f:.]+\]|[^:]+):(\d{1,5})$/.exec(spec.trim());
  if (!m) {
    problems.push(`${name}=${spec}: expected host:port`);
    return { host: '127.0.0.1', port: 30000 };
  }
  return { host: m[2].replace(/^\[|\]$/g, ''), port: Number(m[3]) };
}

const log = (...a) => config.verbose && console.log(new Date().toISOString().slice(11, 19), 'sglang-shim:', ...a);

const agent = new http.Agent({ keepAlive: true, maxSockets: 32 });
const engineUrl = `${config.engine.host}:${config.engine.port}`;

const state = {
  // the engine's start as the shim saw it, unix seconds. a change is what tells the router its cache is gone
  since: null,
  marker: null,
  info: null,
  infoAt: 0,
  load: null,
  engineAt: 0,
  engineError: null,
  hardware: null,
  hardwareStatic: null,
  history: { gpu_util: [], tok_s: [] },
  inflight: [],
  effort_mapped: [],
  usage_added: 0,
  recent: [],
  lastHeld: 0,
  totals: { requests: 0, prompt_tokens: 0, reused: 0, output_tokens: 0, prompt_ms: 0, decode_ms: 0, drafts_offered: 0, drafts_accepted: 0 },
  cache: { requests: 0, requests_reused: 0, reused_tokens: 0, prompt_tokens: 0, last_reused: 0, last_prompt: 0 },
  refresh: null,
};

const CHAT = new Set(['/v1/chat/completions', '/chat/completions', '/v1/completions', '/completions', '/v1/responses', '/responses', '/v1/messages']);
// stream_options only exists on these
const OPENAI = new Set(['/v1/chat/completions', '/chat/completions', '/v1/completions', '/completions']);
// the qwen3.8 chat template only knows xhigh/medium/low and raises on anything else (SGLang answers 400), while
// strata and the openai clients speak none/minimal/low/medium/high/max
const EFFORT = { none: null, off: null, minimal: 'low', low: 'low', medium: 'medium', high: 'xhigh', xhigh: 'xhigh', max: 'xhigh' };
const HOP = new Set(['connection', 'keep-alive', 'proxy-authorization', 'proxy-authenticate', 'te', 'trailer', 'transfer-encoding', 'upgrade', 'host', 'accept-encoding']);

function getEngine(path, { timeout = 5000 } = {}) {
  return new Promise((resolve, reject) => {
    const headers = config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {};
    const req = http.request({ host: config.engine.host, port: config.engine.port, path, method: 'GET', headers, timeout, agent }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode !== 200) return reject(Object.assign(new Error(`${path} answered ${res.statusCode}`), { definite: true }));
        try { resolve(JSON.parse(body)); } catch (err) { reject(err); }
      });
      req.on('error', reject);
      req.on('timeout', () => req.destroy(new Error(`${path} timed out`)));
    });
    req.on('error', reject);
    req.end();
  });
}

// /server_info only every INFO_MS: its startup_time holds this process's measured load and graph times, which
// differ on every start, so it is the restart marker. /v1/loads is cheap and says what the scheduler is doing
async function refreshEngine(force = false) {
  if (!force && state.refresh && Date.now() - state.engineAt < config.freshMs) return state.refresh;
  state.refresh = (async () => {
    try {
      if (force || !state.info || Date.now() - state.infoAt > config.infoMs) {
        const info = await getEngine('/server_info');
        const marker = JSON.stringify([info.startup_time ?? null, info.version ?? null, info.served_model_name ?? null]);
        if (marker !== state.marker) {
          state.marker = marker;
          state.since = Date.now() / 1000;
          log(`engine ${engineUrl} is ${info.served_model_name} (${info.quantization || 'unquantized'}), sglang ${info.version}${info.speculative_algorithm ? `, ${info.speculative_algorithm} drafts` : ''}`);
        }
        state.info = info;
        state.infoAt = Date.now();
      }
      const loads = await getEngine('/v1/loads');
      state.load = Array.isArray(loads?.loads) ? loads.loads[0] || null : null;
      state.engineAt = Date.now();
      state.engineError = null;
    } catch (err) {
      state.engineError = err.message;
      // a restarted engine must be looked at again before its new start is believed
      state.info = null;
      throw err;
    }
  })();
  return state.refresh;
}

const nvidiaSmi = () => new Promise((resolve) => {
  const fields = 'index,name,utilization.gpu,memory.used,memory.total,memory.free,temperature.gpu,power.draw,power.limit,pcie.link.gen.current,pcie.link.gen.max,pcie.link.width.current';
  execFile(config.smi, ['--query-gpu=' + fields, '--format=csv,noheader,nounits'], { timeout: 1500 }, (err, out) => {
    if (err) return resolve(null);
    const row = out.trim().split('\n')[config.gpuIndex]?.split(',').map((s) => s.trim());
    if (!row) return resolve(null);
    const n = (v) => (Number.isFinite(Number(v)) ? Number(v) : null);
    const gib = (mib) => (mib == null ? null : Math.round(mib * 1048576));
    return resolve({
      name: row[1],
      gpu_util: n(row[2]),
      gpu_mem_used: gib(n(row[3])),
      gpu_mem_total: gib(n(row[4])),
      vram_free_mib: n(row[5]),
      gpu_temp: n(row[6]),
      gpu_power: n(row[7]),
      gpu_power_limit: n(row[8]),
      gpu_pcie_gen: n(row[9]),
      gpu_pcie_gen_max: n(row[10]),
      gpu_pcie_width: n(row[11]),
    });
  });
});

let cpuPrev = null;
function cpuPercent() {
  let total = 0;
  let idle = 0;
  for (const line of readFileSync('/proc/stat', 'utf8').split('\n')) {
    if (!line.startsWith('cpu ')) continue;
    const v = line.trim().split(/\s+/).slice(1).map(Number);
    total += v.reduce((a, b) => a + b, 0);
    idle += v[3] + (v[4] || 0);
  }
  const prev = cpuPrev;
  cpuPrev = { total, idle };
  if (!prev || total <= prev.total) return null;
  return ((total - prev.total) - (idle - prev.idle)) / (total - prev.total) * 100;
}

function meminfo() {
  const kb = {};
  for (const line of readFileSync('/proc/meminfo', 'utf8').split('\n')) {
    const m = /^(\w+):\s+(\d+)/.exec(line);
    if (m) kb[m[1]] = Number(m[2]) * 1024;
  }
  return { ram_used: (kb.MemTotal || 0) - (kb.MemAvailable || 0), ram_total: kb.MemTotal || 0 };
}

async function sampleLoop() {
  for (;;) {
    const gpu = await nvidiaSmi();
    const cpu = { cpu: cpuPercent(), ...meminfo() };
    if (gpu) {
      state.hardwareStatic = { gpu_pcie_gen_max: gpu.gpu_pcie_gen_max, gpu_power_limit: gpu.gpu_power_limit };
      state.hardware = { ...gpu, ...cpu };
    } else {
      state.hardware = cpu;
    }
    await refreshEngine().catch(() => {});
    const tok_s = state.load?.num_running_reqs ? state.load.gen_throughput || 0 : 0;
    state.history.gpu_util.push(gpu?.gpu_util ?? null);
    state.history.tok_s.push(tok_s || null);
    state.history.gpu_util = state.history.gpu_util.slice(-60);
    state.history.tok_s = state.history.tok_s.slice(-60);
    await sleep(config.sampleMs);
  }
}

// one record per chat request the shim relays: the engine keeps none of this
function startRecord(req) {
  const path = req.url.split('?')[0].replace(/\/+$/, '') || '/';
  if (req.method !== 'POST' || !CHAT.has(path)) return null;
  const rec = {
    id: createHash('sha1').update(`${Date.now()}${Math.random()}`).digest('hex').slice(0, 12),
    t0: performance.now(),
    started: Date.now(),
    firstTokenAt: 0,
    lastTokenAt: 0,
    chunks: 0,
    usage: null,
    finish: null,
  };
  state.inflight.push(rec);
  return rec;
}

function generated(o) {
  const c = o?.choices?.[0];
  if (c) return Boolean(c.text || c.delta?.content || c.delta?.reasoning_content || c.delta?.reasoning || c.delta?.tool_calls?.length);
  return typeof o?.type === 'string' && /[._]delta$/.test(o.type) && o.type !== 'message_delta';
}

const FINAL = new Set(['message_stop', 'error', 'response.completed', 'response.incomplete', 'response.failed']);

// the tap writes into the request record as the answer arrives, so liveBlock can say what the slot is doing
// while it runs and finishRecord can report what it cost
function usageTap(rec, sse) {
  const dec = new StringDecoder('utf8');
  let buf = '';
  const take = (o) => {
    for (const u of [o?.usage, o?.message?.usage, o?.response?.usage]) if (u && typeof u === 'object') rec.usage = { ...rec.usage, ...u };
    const f = o?.choices?.[0]?.finish_reason ?? o?.response?.incomplete_details?.reason ?? (FINAL.has(o?.type) ? o.type.replace(/[._]/g, ' ') : null);
    if (f) rec.finish = f;
    if (generated(o)) {
      if (!rec.firstTokenAt) rec.firstTokenAt = performance.now();
      // speculative decoding streams several tokens per chunk: the usage block is the count, chunks only the pace
      rec.chunks++;
      rec.lastTokenAt = performance.now();
    }
  };
  const line = (l) => {
    const data = l.startsWith('data:') ? l.slice(5).trim() : '';
    if (!data || data === '[DONE]') return;
    try { take(JSON.parse(data)); } catch {}
  };
  // the bytes go on untouched; the tap only reads them
  return {
    consume(chunk) {
      if (!sse) {
        if (buf.length < 4_000_000) buf += dec.write(chunk);
        return chunk;
      }
      buf += dec.write(chunk);
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        line(buf.slice(0, i));
        buf = buf.slice(i + 1);
      }
      if (buf.length > 1_000_000) buf = '';
      return chunk;
    },
    end() {
      if (sse) line(buf + dec.end());
      else try { take(JSON.parse(buf + dec.end())); } catch {}
      return '';
    },
  };
}

// openai counts cache hits inside prompt_tokens, anthropic reports them beside input_tokens
function normUsage(u) {
  const cached = u.prompt_tokens_details?.cached_tokens ?? u.input_tokens_details?.cached_tokens ?? u.cache_read_input_tokens ?? null;
  const anthropic = 'cache_read_input_tokens' in u || 'cache_creation_input_tokens' in u;
  const extra = anthropic ? (u.cache_read_input_tokens || 0) + (u.cache_creation_input_tokens || 0) : 0;
  return { prompt: u.prompt_tokens ?? (u.input_tokens || 0) + extra, completion: u.completion_tokens ?? u.output_tokens ?? 0, cached };
}

// a request that ended: strata's per-request record, so the router can learn this engine's speeds
function finishRecord(rec, status) {
  const i = state.inflight.indexOf(rec);
  if (i >= 0) state.inflight.splice(i, 1);
  if (!rec) return;
  const u = rec.usage ? normUsage(rec.usage) : null;
  const prompt = u?.prompt ?? null;
  // SGLang reports cached prompt tokens only with --enable-cache-report
  const cached = u?.cached ?? null;
  const completion = u?.completion ?? null;
  const promptMs = rec.firstTokenAt ? rec.firstTokenAt - rec.t0 : null;
  const decodeMs = rec.firstTokenAt && rec.lastTokenAt > rec.firstTokenAt ? rec.lastTokenAt - rec.firstTokenAt : null;
  const r = {
    id: rec.id,
    time: rec.started / 1000,
    duration_s: Math.round((performance.now() - rec.t0) * 100) / 100000,
    finish: status >= 400 ? 'error' : rec.finish || (completion != null ? 'stop' : null),
    prompt_tokens: prompt,
    reused: cached,
    output_tokens: completion,
    first_engine_token_s: promptMs != null ? Math.round(promptMs) / 1000 : null,
    prompt_total: prompt,
    prompt_read: prompt != null ? prompt - (cached || 0) : null,
    prompt_ms: promptMs != null ? Math.round(promptMs * 10) / 10 : null,
    decode_ms: decodeMs != null ? Math.round(decodeMs * 10) / 10 : null,
    // the first chunk's tokens arrived with the prompt, so they are not decode time
    decode_tok_s: decodeMs && completion > 1 ? Math.round((completion - 1) / (decodeMs / 1000) * 10) / 10 : null,
    hit_rate: null,
    drafts_offered: null,
    drafts_accepted: null,
  };
  state.recent.unshift(r);
  if (state.recent.length > config.recentKept) state.recent.length = config.recentKept;
  if (status < 400) {
    state.totals.requests++;
    state.cache.requests++;
    if (prompt != null) {
      state.totals.prompt_tokens += prompt;
      state.totals.reused += cached || 0;
      state.totals.output_tokens += completion || 0;
      state.cache.prompt_tokens += prompt;
      state.cache.reused_tokens += cached || 0;
      state.cache.last_reused = cached || 0;
      state.cache.last_prompt = prompt;
      if (cached > 0) state.cache.requests_reused++;
      state.lastHeld = prompt + (completion || 0);
    }
    if (promptMs) state.totals.prompt_ms += promptMs;
    if (decodeMs) state.totals.decode_ms += decodeMs;
  }
}

// strata's live block: SGLang's scheduler counts plus what the shim knows it sent there. with more requests in
// flight than max_running_requests, the extras wait in SGLang's queue and are reported as queued
function liveBlock() {
  const recs = state.inflight.slice().sort((a, b) => a.t0 - b.t0);
  const L = state.load || {};
  const parallel = Math.max(1, Number(state.info?.max_running_requests) || 1);
  const running = Math.min(parallel, Number.isFinite(L.num_running_reqs) ? L.num_running_reqs : recs.length);
  const queued = Math.max(Number(L.num_waiting_reqs) || 0, recs.length - running);
  const slots = [];
  for (let i = 0; i < parallel; i++) {
    if (i >= running) {
      slots.push({ slot: i, state: 'idle', held_tokens: i === 0 ? state.lastHeld : 0 });
      continue;
    }
    const r = recs[i] || null;
    const reading = r ? !r.firstTokenAt : false;
    const elapsed = r ? (performance.now() - (r.firstTokenAt || r.t0)) / 1000 : null;
    slots.push({
      slot: i,
      state: reading ? 'reading' : 'generating',
      held_tokens: running === 1 ? Number(L.num_used_tokens) || 0 : 0,
      prompt_tokens: null,
      prompt_read: null,
      generated: null,
      tok_s: !reading && running === 1 && L.gen_throughput ? Math.round(L.gen_throughput * 10) / 10 : null,
      elapsed_s: elapsed != null ? Math.round(elapsed * 10) / 10 : null,
    });
  }
  const busySlots = slots.filter((s) => s.state !== 'idle');
  const first = recs[0] || null;
  const slot0 = busySlots[0] || {};
  const decodeMean = state.recent.map((r) => r.decode_tok_s).filter(Number.isFinite);
  return {
    state: running ? (busySlots.some((s) => s.state === 'reading') ? 'reading' : 'generating') : 'idle',
    queued,
    running,
    parallel,
    slots,
    phase: running ? (slot0.state === 'reading' ? 'prompt' : 'generate') : null,
    prompt_tokens: null,
    prompt_read: null,
    prompt_total: null,
    generated: null,
    max_tokens: null,
    elapsed_s: slot0.elapsed_s ?? (first ? Math.round((performance.now() - first.t0) * 10) / 10000 : null),
    tok_s: slot0.tok_s ?? null,
    tok_s_mean: decodeMean.length ? Math.round(decodeMean.reduce((a, b) => a + b, 0) / decodeMean.length * 10) / 10 : null,
    prefill_tok_s_mean: null,
    tok_s_window_s: null,
    outside_slots: 0,
    waiting: queued,
    admitting: 0,
  };
}

function metricsBlock() {
  const live = liveBlock();
  const info = state.info || {};
  const L = state.load || {};
  return {
    engine: {
      model: info.served_model_name ?? null,
      model_ftype: info.quantization ?? null,
      max_context: info.context_length ?? null,
      context: info.context_length ?? null,
      images: false,
      version: info.version ? `sglang ${info.version}` : null,
      batch_slots: live.parallel,
      vram_free_mib: state.hardware?.vram_free_mib ?? null,
      drafting: info.speculative_algorithm ? String(info.speculative_algorithm).toLowerCase() : null,
      accept_length: L.speculative?.accept_length ?? null,
      metrics_endpoint: false,
      engine: info.version ?? null,
    },
    live,
    requests: state.recent,
    requests_kept: state.recent.length,
    // SGLang's radix cache keeps prefixes but reports no conversation cache: the shim's own counters below are what
    // it can honestly report
    conversation_cache: {
      enabled: false,
      slots: null,
      parked: 0,
      parks: null,
      restores: null,
      evictions: null,
      requests: state.cache.requests,
      requests_reused: state.cache.requests_reused,
      reused_tokens: state.cache.reused_tokens,
      prompt_tokens: state.cache.prompt_tokens,
      last_reused: state.cache.last_reused,
      last_prompt: state.cache.last_prompt,
    },
    totals: {
      since: state.since,
      requests: state.totals.requests,
      prompt_tokens: state.totals.prompt_tokens,
      reused: state.totals.reused,
      output_tokens: state.totals.output_tokens,
      prompt_ms: state.totals.prompt_ms,
      decode_ms: state.totals.decode_ms,
      drafts_offered: state.totals.drafts_offered,
      drafts_accepted: state.totals.drafts_accepted,
      n_tokens_max: L.max_total_num_tokens ?? null,
    },
    hardware: state.hardware,
    hardware_static: state.hardwareStatic,
    history: state.history,
    shim: { engine: engineUrl, engine_error: state.engineError, inflight: state.inflight.length, effort_mapped: state.effort_mapped, usage_added: state.usage_added },
    time: Date.now() / 1000,
  };
}

function send(res, status, obj) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

async function metrics(req, res) {
  try {
    await refreshEngine();
  } catch (err) {
    // the engine is not answering: tell the router so it fails over instead of routing into nothing
    return send(res, 503, { error: { type: 'engine_unreachable', message: `${engineUrl}: ${err.message}` } });
  }
  send(res, 200, metricsBlock());
}

function upstreamHeaders(req) {
  const out = {};
  for (const [k, v] of Object.entries(req.headers)) if (!HOP.has(k.toLowerCase())) out[k] = v;
  out.host = engineUrl;
  out.accept ??= 'application/json';
  if (config.apiKey && !out.authorization) out.authorization = `Bearer ${config.apiKey}`;
  return out;
}

function passHeaders(headers) {
  const drop = new Set(HOP);
  for (const name of String(headers.connection || '').split(',')) if (name.trim()) drop.add(name.trim().toLowerCase());
  return Object.fromEntries(Object.entries(headers).filter(([k]) => !drop.has(k.toLowerCase())));
}

// the chat body as the engine gets it: effort levels the template accepts, and a usage chunk on streamed answers
function rewrite(body, path) {
  let o;
  try {
    o = JSON.parse(body);
  } catch {
    return { body, mapped: null, usage: false };
  }
  if (!o || typeof o !== 'object') return { body, mapped: null, usage: false };
  let changed = false;
  let mapped = null;
  const kw = o.chat_template_kwargs;
  const levels = [];
  if (typeof o.reasoning_effort === 'string') levels.push(['reasoning_effort', o.reasoning_effort]);
  if (kw && typeof kw.reasoning_effort === 'string') levels.push(['chat_template_kwargs.reasoning_effort', kw.reasoning_effort]);
  for (const [where, value] of levels) {
    if (!Object.hasOwn(EFFORT, value.toLowerCase())) continue;
    const target = EFFORT[value.toLowerCase()];
    if (target === null) {
      o.chat_template_kwargs = { ...(kw || {}), enable_thinking: false };
      if (where === 'reasoning_effort') delete o.reasoning_effort;
      else delete o.chat_template_kwargs.reasoning_effort;
    } else if (target === value) {
      continue;
    } else if (where === 'reasoning_effort') {
      o.reasoning_effort = target;
    } else {
      kw.reasoning_effort = target;
    }
    changed = true;
    mapped = `${value} -> ${target === null ? 'enable_thinking=false' : target}`;
    break;
  }
  // strata ends every stream with usage and avalon-shim adds it, so clients and the router count the same way here
  let usage = false;
  if (OPENAI.has(path) && o.stream === true && !o.stream_options?.include_usage) {
    o.stream_options = { ...(o.stream_options || {}), include_usage: true };
    usage = changed = true;
  }
  return { body: changed ? JSON.stringify(o) : body, mapped, usage };
}

function relay(req, res) {
  const rec = startRecord(req);
  const path = req.url.split('?')[0].replace(/\/+$/, '') || '/';
  let tap = null;
  let ended = false;
  let up = null;
  const fail = (err) => {
    if (ended || res.writableEnded) return;
    ended = true;
    if (!res.headersSent) send(res, 502, { error: { type: 'shim_error', message: `${engineUrl}: ${err.message}` } });
    else res.destroy();
    if (rec) finishRecord(rec, 502);
    if (up) up.destroy();
  };
  const forward = (r) => {
    r.on('error', fail);
    try {
      res.writeHead(r.statusCode, passHeaders(r.headers));
    } catch (err) {
      return up.destroy(err);
    }
    tap = rec ? usageTap(rec, /event-stream/.test(r.headers['content-type'] || '')) : null;
    r.on('data', (c) => {
      const out = tap ? tap.consume(c) : c;
      if (out.length && !res.write(out)) {
        r.pause();
        res.once('drain', () => r.resume());
      }
    });
    r.on('end', () => {
      ended = true;
      if (tap) { const tail = tap.end(); if (tail) res.write(tail); }
      res.end();
      if (rec) finishRecord(rec, r.statusCode);
    });
  };
  // a client that hangs up takes the engine request with it, so the slot is not left to a dead conversation
  const gone = () => {
    if (ended || res.writableFinished) return;
    fail(new Error('client went away'));
  };
  res.on('close', gone);
  req.on('aborted', gone);

  if (req.method === 'POST' && CHAT.has(path) && /json/.test(req.headers['content-type'] || '')) {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      chunks.push(c);
      size += c.length;
      if (size > 64 * 1024 * 1024) {
        req.destroy();
        fail(new Error('request body too large'));
      }
    });
    req.on('end', () => {
      const fixed = rewrite(Buffer.concat(chunks).toString('utf8'), path);
      if (fixed.mapped) {
        state.effort_mapped.push(fixed.mapped);
        if (state.effort_mapped.length > 20) state.effort_mapped.shift();
      }
      if (fixed.usage) state.usage_added++;
      const headers = upstreamHeaders(req);
      headers['content-length'] = Buffer.byteLength(fixed.body);
      up = http.request({ host: config.engine.host, port: config.engine.port, path: req.url, method: req.method, headers, timeout: config.readTimeoutMs, agent }, forward);
      up.on('socket', (s) => s.setTimeout(config.readTimeoutMs));
      up.on('error', fail);
      up.end(fixed.body);
    });
    return;
  }

  up = http.request(
    { host: config.engine.host, port: config.engine.port, path: req.url, method: req.method, headers: upstreamHeaders(req), timeout: config.readTimeoutMs, agent },
    forward,
  );
  up.on('socket', (s) => s.setTimeout(config.readTimeoutMs));
  up.on('error', fail);
  req.pipe(up);
}

const server = http.createServer(async (req, res) => {
  const path = req.url.split('?')[0].replace(/\/+$/, '') || '/';
  if (path === '/metrics' && req.method === 'GET') return metrics(req, res);
  if (path === '/shim' && req.method === 'GET') {
    return send(res, 200, { config: { engine: engineUrl, port: config.port, read_timeout_ms: config.readTimeoutMs }, ...metricsBlock() });
  }
  return relay(req, res);
});

server.on('clientError', (err, socket) => socket.destroy());
server.listen(config.port, config.bind, () => {
  log(`fronting ${engineUrl} on http://${config.bind}:${config.port}`);
  if (!config.verbose) console.error = () => {};
  refreshEngine(true).catch((err) => log(`engine not up yet: ${err.message}`));
});
sampleLoop().catch((err) => log(`sampling stopped: ${err.message}`));

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    log(`${sig}: closing`);
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref();
  });
}
