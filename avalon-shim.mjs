#!/usr/bin/env node
// avalon-shim: makes an Avalon (llama.cpp fork) server look like a Strata engine to strata-router.
// it fronts one engine and relays everything untouched except GET /metrics, which it answers itself:
// the router reads engine state there, and Avalon only has prometheus text plus /slots.
// node >= 20, no dependencies.

import http from 'node:http';
import { execFile } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { StringDecoder } from 'node:string_decoder';
import { setTimeout as sleep } from 'node:timers/promises';

const problems = [];

const config = {
  engine: parseHostPort(process.env.AVALON || '127.0.0.1:8080', 'AVALON'),
  port: num('PORT', 8081, { min: 1, max: 65535 }),
  bind: process.env.BIND || '0.0.0.0',
  readTimeoutMs: num('READ_TIMEOUT', 300000, { min: 1 }),
  freshMs: num('FRESH_MS', 600, { min: 0 }),
  sampleMs: num('SAMPLE_MS', 2000, { min: 200 }),
  propsMs: num('PROPS_MS', 10000, { min: 500 }),
  recentKept: num('RECENT_KEPT', 50, { min: 1 }),
  gpuIndex: num('GPU_INDEX', 0),
  apiKey: readKey(process.env.AVALON_API_KEY_FILE),
  smi: process.env.NVIDIA_SMI || '/usr/bin/nvidia-smi',
  verbose: process.env.LOG !== 'off',
};

if (problems.length) {
  for (const p of problems) console.error(`avalon-shim: ${p}`);
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
    problems.push(`AVALON_API_KEY_FILE=${path}: not readable (${err.code})`);
    return '';
  }
}

function parseHostPort(spec, name) {
  const m = /^(?:([^=]+)=)?(\[[0-9a-f:.]+\]|[^:]+):(\d{1,5})$/.exec(spec.trim());
  if (!m) {
    problems.push(`${name}=${spec}: expected host:port`);
    return { host: '127.0.0.1', port: 8080 };
  }
  return { host: m[2].replace(/^\[|\]$/g, ''), port: Number(m[3]) };
}

const log = (...a) => config.verbose && console.log(new Date().toISOString().slice(11, 19), 'avalon-shim:', ...a);

const agent = new http.Agent({ keepAlive: true, maxSockets: 32 });
const engineUrl = `${config.engine.host}:${config.engine.port}`;

// what the shim knows about the engine; everything here is derived, nothing is configured
const state = {
  // the engine's own start, unix seconds. a change is what tells the router its cache is gone
  since: null,
  marker: null,
  props: null,
  propsAt: 0,
  prom: {},
  promStart: null,
  slots: [],
  engineAt: 0,
  engineError: null,
  metricsEndpoint: false,
  hardware: null,
  hardwareStatic: null,
  history: { gpu_util: [], tok_s: [] },
  inflight: [],
  effort_mapped: [],
  recent: [],
  totals: { requests: 0, prompt_tokens: 0, reused: 0, output_tokens: 0, prompt_ms: 0, decode_ms: 0, drafts_offered: 0, drafts_accepted: 0 },
  cache: { requests: 0, requests_reused: 0, reused_tokens: 0, prompt_tokens: 0, last_reused: 0, last_prompt: 0 },
  refresh: null,
};

const CHAT = new Set(['/v1/chat/completions', '/chat/completions', '/v1/completions', '/completions', '/v1/responses', '/responses', '/v1/messages']);
// avalon's qwen3.8 template only knows xhigh/medium/low and raises on anything else, while strata and the
// openai clients speak none/minimal/low/medium/high/max. Without this the router's 500s come from the
// chat template, not the engine.
const EFFORT = { none: null, off: null, minimal: 'low', low: 'low', medium: 'medium', high: 'xhigh', xhigh: 'xhigh', max: 'xhigh' };
const HOP = new Set(['connection', 'keep-alive', 'proxy-authorization', 'proxy-authenticate', 'te', 'trailer', 'transfer-encoding', 'upgrade', 'host', 'accept-encoding']);

function getEngine(path, { json = true, timeout = 5000 } = {}) {
  return new Promise((resolve, reject) => {
    const headers = config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {};
    const req = http.request({ host: config.engine.host, port: config.engine.port, path, method: 'GET', headers, timeout, agent }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode !== 200) return reject(Object.assign(new Error(`${path} answered ${res.statusCode}`), { definite: true }));
        if (!json) return resolve({ body, headers: res.headers });
        try { resolve(JSON.parse(body)); } catch (err) { reject(err); }
      });
      req.on('error', reject);
      req.on('timeout', () => req.destroy(new Error(`${path} timed out`)));
    });
    req.on('error', reject);
    req.end();
  });
}

// llama.cpp's prometheus text: "llamacpp:name value", with labeled series summed into the plain name
function parseProm(text) {
  const out = {};
  for (const line of text.split('\n')) {
    const m = /^llamacpp:([a-z_]+)(?:\{[^}]*\})?\s+(\S+)$/.exec(line);
    if (!m) continue;
    const v = Number(m[2]);
    if (Number.isFinite(v)) out[m[1]] = (out[m[1]] || 0) + v;
  }
  return out;
}

// the engine's state: props for what it is, /slots for what it is doing, /metrics for the counters.
// props only every PROPS_MS; its media_marker is random per process and so says when the engine restarted
async function refreshEngine(force = false) {
  if (!force && state.refresh && Date.now() - state.engineAt < config.freshMs) return state.refresh;
  state.refresh = (async () => {
    try {
      if (force || state.since == null || !state.props || Date.now() - state.propsAt > config.propsMs) {
        const props = await getEngine('/props');
        if (props.media_marker !== state.marker) {
          state.marker = props.media_marker;
          state.propsAt = Date.now();
          state.since = state.promStart ?? Math.floor(Date.now() / 1000);
          state.props = props;
          log(`engine ${engineUrl} is ${props.model_alias} (${props.model_ftype}), ${props.build_info}`);
        } else {
          state.propsAt = Date.now();
        }
      }
      const [slots, metrics] = await Promise.all([
        getEngine('/slots'),
        getEngine('/metrics', { json: false }).catch((err) => {
          if (!err.definite) throw err;
          state.metricsEndpoint = false;
          return null;
        }),
      ]);
      state.slots = Array.isArray(slots) ? slots : [];
      if (metrics) {
        state.metricsEndpoint = true;
        state.prom = parseProm(metrics.body);
        // llama.cpp's process-start header is its own clock, not unix time, so it is only useful as a
        // change detector; the shim's own watch is what the router sees as the engine's start
        if (metrics.headers['process-start-time-unix'] !== state.promHeader) {
          state.promHeader = metrics.headers['process-start-time-unix'];
          state.since = Date.now() / 1000;
        }
      }
      state.engineAt = Date.now();
      state.engineError = null;
    } catch (err) {
      state.engineError = err.message;
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
    const tok_s = state.prom.predicted_tokens_seconds || 0;
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
    tokens: 0,
    usage: null,
    finish: null,
    timings: null,
    // fallback when the answer carries no per-request timings: the prometheus counters either side of it
    counters: [state.prom.spec_decode_num_draft_tokens_total || 0, state.prom.spec_decode_num_accepted_tokens_total || 0],
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
    // avalon puts prompt and generation timings on the finished answer, streamed or not
    for (const t of [o?.timings, o?.choices?.[0]?.timings]) if (t && typeof t === 'object') rec.timings = { ...rec.timings, ...t };
    const f = o?.choices?.[0]?.finish_reason ?? o?.response?.incomplete_details?.reason ?? (FINAL.has(o?.type) ? o.type.replace(/[._]/g, ' ') : null);
    if (f) rec.finish = f;
    if (!rec.usage && o.timings) rec.usage = usageFromTimings(o.timings);
    if (generated(o)) {
      if (!rec.firstTokenAt) rec.firstTokenAt = performance.now();
      // one chunk per streamed token, so the running speed is an estimate when spec-decode writes several at once
      rec.tokens++;
      rec.lastTokenAt = performance.now();
    }
  };
  const line = (l) => {
    const data = l.startsWith('data:') ? l.slice(5).trim() : '';
    if (!data || data === '[DONE]') return;
    try { take(JSON.parse(data)); } catch {}
  };
  return {
    // the bytes to send onward: usage is added to the chunk carrying timings, everything else passes through
    consume(chunk) {
      if (!sse) {
        if (buf.length < 4_000_000) buf += dec.write(chunk);
        return chunk;
      }
      buf += dec.write(chunk);
      let out = '';
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const l = buf.slice(0, i);
        line(l);
        out += injectUsage(l) + '\n';
        buf = buf.slice(i + 1);
      }
      if (buf.length > 1_000_000) buf = '';
      return out;
    },
    end() {
      if (sse) {
        const l = buf + dec.end();
        line(l);
        return injectUsage(l) + '\n';
      }
      try { take(JSON.parse(buf + dec.end())); } catch {}
      return '';
    },
  };
}

// llama.cpp streams timings but no usage; put the usage block on the chunk that carries them so clients
// and the router count tokens the same way they do for a non-streamed answer
function usageFromTimings(t) {
  const prompt = (t.prompt_n || 0) + (t.cache_n || 0);
  const completion = t.predicted_n ?? 0;
  return {
    prompt_tokens: prompt,
    completion_tokens: completion,
    total_tokens: prompt + completion,
    prompt_tokens_details: { cached_tokens: t.cache_n || 0 },
  };
}

function injectUsage(line) {
  if (!line.startsWith('data:')) return line;
  const body = line.slice(5).trim();
  if (!body || body === '[DONE]' || body.includes('"usage"') || !body.includes('"timings"')) return line;
  let o;
  try { o = JSON.parse(body); } catch { return line; }
  if (!o.timings) return line;
  o.usage = usageFromTimings(o.timings);
  return 'data: ' + JSON.stringify(o);
}

// openai counts cache hits inside prompt_tokens, anthropic reports them beside input_tokens
function normUsage(u) {
  const cached = u.prompt_tokens_details?.cached_tokens ?? u.input_tokens_details?.cached_tokens ?? u.cache_read_input_tokens ?? 0;
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
  const t = rec.timings || {};
  const prompt = u?.prompt ?? t.prompt_n ?? null;
  const cached = t.cache_n ?? u?.cached ?? null;
  const completion = u?.completion ?? t.predicted_n ?? null;
  const promptMs = t.prompt_ms ?? (rec.firstTokenAt ? rec.firstTokenAt - rec.t0 : null);
  // streamed answers carry timings only at the end; the tap's own token timings are the fallback
  const decodeMs = t.predicted_ms ?? (rec.firstTokenAt && rec.lastTokenAt > rec.firstTokenAt ? rec.lastTokenAt - rec.firstTokenAt : null);
  const draftsOffered = t.draft_n ?? null;
  const draftsAccepted = t.draft_n_accepted ?? null;
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
    prompt_read: prompt != null && cached != null ? prompt - cached : null,
    prompt_ms: promptMs != null ? Math.round(promptMs * 10) / 10 : null,
    decode_ms: decodeMs != null ? Math.round(decodeMs * 10) / 10 : null,
    decode_tok_s: decodeMs && completion ? Math.round(completion / (decodeMs / 1000) * 10) / 10 : null,
    hit_rate: null,
    drafts_offered: draftsOffered,
    drafts_accepted: draftsAccepted,
  };
  state.recent.unshift(r);
  if (state.recent.length > config.recentKept) state.recent.length = config.recentKept;
  state.totals.since = state.since;
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
    }
    if (promptMs) state.totals.prompt_ms += promptMs;
    if (decodeMs) state.totals.decode_ms += decodeMs;
    if (draftsOffered != null) {
      state.totals.drafts_offered += draftsOffered;
      state.totals.drafts_accepted += draftsAccepted || 0;
    } else {
      // no per-request timings: the drafts this request caused are the counters that moved while it ran
      r.drafts_offered = Math.max(0, (state.prom.spec_decode_num_draft_tokens_total || 0) - rec.counters[0]);
      r.drafts_accepted = Math.max(0, (state.prom.spec_decode_num_accepted_tokens_total || 0) - rec.counters[1]);
      state.totals.drafts_offered += r.drafts_offered;
      state.totals.drafts_accepted += r.drafts_accepted;
    }
  }
}

// strata's live block: what each slot is doing, from /slots plus what the shim knows it sent there.
// with more requests in flight than slots, the extras sit in the engine's own queue and are reported as queued
function liveBlock() {
  const recs = state.inflight.slice().sort((a, b) => a.t0 - b.t0);
  let k = 0;
  const slots = state.slots.map((s) => {
    const held = (s.n_prompt_tokens || 0) + (s.next_token?.n_decoded || 0);
    if (!s.is_processing) return { slot: s.id, state: 'idle', held_tokens: held };
    const r = recs[k++] || null;
    const reading = r ? !r.firstTokenAt : false;
    const elapsed = r ? (performance.now() - (r.firstTokenAt || r.t0)) / 1000 : null;
    return {
      slot: s.id,
      state: reading ? 'reading' : 'generating',
      held_tokens: held,
      prompt_tokens: s.n_prompt_tokens || null,
      prompt_read: s.n_prompt_tokens_processed || (r ? null : null),
      generated: s.next_token?.n_decoded ?? r?.tokens ?? null,
      tok_s: r && r.lastTokenAt > r.firstTokenAt ? Math.round(r.tokens / ((r.lastTokenAt - r.firstTokenAt) / 1000) * 10) / 10 : null,
      elapsed_s: elapsed != null ? Math.round(elapsed * 10) / 10 : null,
    };
  });
  const busySlots = slots.filter((s) => s.state !== 'idle');
  const running = busySlots.length;
  const queued = Math.max(0, (state.metricsEndpoint ? state.prom.requests_deferred || 0 : 0), recs.length - running);
  const first = recs[0] || null;
  const slot0 = busySlots[0] || {};
  const decodeMean = state.recent.map((r) => r.decode_tok_s).filter(Number.isFinite);
  return {
    state: state.props?.is_sleeping ? 'unloaded' : running ? (busySlots.some((s) => s.state === 'reading') ? 'reading' : 'generating') : 'idle',
    queued,
    running,
    parallel: Math.max(1, state.slots.length),
    slots,
    phase: running ? (slot0.state === 'reading' ? 'prompt' : 'generate') : null,
    prompt_tokens: slot0.prompt_tokens ?? first?.usage?.prompt_tokens ?? null,
    prompt_read: slot0.prompt_read ?? null,
    prompt_total: slot0.prompt_tokens ?? null,
    generated: slot0.generated ?? null,
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
  const p = state.prom;
  const held = live.slots.reduce((n, s) => n + (s.held_tokens || 0), 0);
  return {
    engine: {
      model: state.props?.model_alias ?? null,
      model_ftype: state.props?.model_ftype ?? null,
      max_context: state.slots[0]?.n_ctx ?? null,
      context: state.slots[0]?.n_ctx ?? null,
      images: state.props?.modalities?.vision ?? false,
      version: state.props ? `avalon ${state.props.build_info}` : null,
      batch_slots: Math.max(1, state.slots.length),
      vram_free_mib: state.hardware?.vram_free_mib ?? null,
      drafting: (p.spec_decode_num_drafts_total || 0) > 0 ? 'mtp' : null,
      metrics_endpoint: state.metricsEndpoint,
      engine: state.props?.build_info ?? null,
    },
    live,
    requests: state.recent,
    requests_kept: state.recent.length,
    // avalon keeps a prompt cache per slot but has no conversation cache: the router's own counters
    // below are what it can honestly report
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
      engine_prompt_tokens: p.prompt_tokens_total ?? null,
      engine_prompt_cached: p.prompt_tokens_cached_total ?? null,
      n_tokens_max: p.n_tokens_max ?? null,
    },
    hardware: state.hardware,
    hardware_static: state.hardwareStatic,
    history: state.history,
    shim: { engine: engineUrl, engine_error: state.engineError, metrics_endpoint: state.metricsEndpoint, inflight: state.inflight.length, effort_mapped: state.effort_mapped },
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
  // a keyed engine needs the key on every request, including ones a client did not send it on
  if (config.apiKey && !out.authorization) out.authorization = `Bearer ${config.apiKey}`;
  return out;
}

function passHeaders(headers) {
  const drop = new Set(HOP);
  for (const name of String(headers.connection || '').split(',')) if (name.trim()) drop.add(name.trim().toLowerCase());
  return Object.fromEntries(Object.entries(headers).filter(([k]) => !drop.has(k.toLowerCase())));
}

function effortOf(body) {
  let o;
  try {
    o = JSON.parse(body);
  } catch {
    return { body, mapped: null };
  }
  if (!o || typeof o !== 'object') return { body, mapped: null };
  const levels = [];
  if (typeof o.reasoning_effort === 'string') levels.push(['reasoning_effort', o.reasoning_effort]);
  const kw = o.chat_template_kwargs;
  if (kw && typeof kw.reasoning_effort === 'string') levels.push(['chat_template_kwargs.reasoning_effort', kw.reasoning_effort]);
  for (const [where, value] of levels) {
    if (!Object.hasOwn(EFFORT, value.toLowerCase())) continue;
    const target = EFFORT[value.toLowerCase()];
    if (where === 'reasoning_effort') {
      if (target === null) {
        o.chat_template_kwargs = { ...(kw || {}), enable_thinking: false };
        delete o.reasoning_effort;
      } else if (target !== value) {
        o.reasoning_effort = target;
      }
    } else if (target !== value) {
      kw.reasoning_effort = target;
    }
    return { body: JSON.stringify(o), mapped: `${value} -> ${target === null ? 'enable_thinking=false' : target}` };
  }
  return { body, mapped: null };
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
      if (!res.write(out)) {
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

  // the chat bodies are buffered so avalon's effort levels can be rewritten
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
      const fixed = effortOf(Buffer.concat(chunks).toString('utf8'));
      if (fixed.mapped) {
        state.effort_mapped.push(fixed.mapped);
        if (state.effort_mapped.length > 20) state.effort_mapped.shift();
      }
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
