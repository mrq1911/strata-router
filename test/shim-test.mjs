// shim tests: does strata-router see a plain Avalon server as a working engine, and does it route on that
import { setTimeout as sleep } from 'node:timers/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { makeAvalon } from './fake-avalon.mjs';
import { makeEngine } from './mock-engines.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const SMI = join(here, 'fixtures', 'fake-nvidia-smi.sh');

let nextPort = 19300;
const children = [];
const servers = [];

let failures = 0;
const check = (label, cond, detail = '') => {
  console.log(`${cond ? 'ok  ' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);
  if (!cond) failures++;
};

async function avalon(opts = {}) {
  const e = makeAvalon(nextPort++, opts);
  servers.push(e);
  await once(e.server, 'listening');
  return e;
}

function mock(name) {
  const e = makeEngine(name, nextPort++, 2, 120);
  servers.push(e);
  return once(e.server, 'listening').then(() => e);
}

async function shim(engine, env = {}) {
  const port = nextPort++;
  const child = spawn(process.execPath, [join(here, '..', 'avalon-shim.mjs')], {
    env: {
      ...process.env,
      LOG: 'off',
      AVALON: `127.0.0.1:${engine.port}`,
      PORT: String(port),
      BIND: '127.0.0.1',
      SAMPLE_MS: '200',
      FRESH_MS: '0',
      PROPS_MS: '500',
      NVIDIA_SMI: SMI,
      ...env,
    },
    stdio: ['ignore', 'inherit', 'pipe'],
  });
  children.push(child);
  const s = { url: `http://127.0.0.1:${port}`, port, child, stderr: '' };
  child.stderr.on('data', (c) => (s.stderr += c));
  for (let i = 0; i < 100; i++) {
    try {
      await fetch(`${s.url}/health`);
      break;
    } catch {
      await sleep(50);
    }
  }
  return s;
}

async function router(backends, env = {}) {
  const port = nextPort++;
  const child = spawn(process.execPath, [join(here, '..', 'router.mjs')], {
    env: {
      ...process.env,
      LOG: 'off',
      STRATA_BACKENDS: backends.map((b) => `${b.name}=127.0.0.1:${b.port}`).join(','),
      PORT: String(port),
      POLL_MS: '100',
      ...env,
    },
    stdio: ['ignore', 'inherit', 'pipe'],
  });
  children.push(child);
  const r = { url: `http://127.0.0.1:${port}`, port, child };
  child.stderr.on('data', (c) => process.stderr.write(c));
  for (let i = 0; i < 100; i++) {
    try {
      await fetch(`${r.url}/health`);
      break;
    } catch {
      await sleep(50);
    }
  }
  return r;
}

const getJson = async (url) => (await fetch(url)).json();
const backend = async (r, name) => (await getJson(`${r.url}/stats`)).backends.find((b) => b.name === name);

async function chat(r, messages, extra = {}) {
  const res = await fetch(`${r.url}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'qwen3.8-flash-next', messages, max_tokens: 10, ...extra }),
  });
  const d = await res.json().catch(() => ({}));
  return { status: res.status, backend: res.headers.get('x-strata-backend'), text: d.choices?.[0]?.message?.content, error: d.error };
}

// what agents actually do: a streamed answer, so prefill and decode can be told apart
async function stream(r, messages, extra = {}) {
  const res = await fetch(`${r.url}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'qwen3.8-flash-next', messages, stream: true, max_tokens: 10, ...extra }),
  });
  const body = await res.text();
  return { status: res.status, backend: res.headers.get('x-strata-backend'), text: body.includes('[DONE]') ? body.match(/content":"([^"]*)/g)?.join('') : '', error: body.includes('"error"') ? body : null };
}

async function scenario(name, fn) {
  try {
    await fn();
  } catch (err) {
    check(`${name} threw`, false, err.stack);
  }
}

const turns = (first, n) => [
  { role: 'user', content: first },
  ...Array.from({ length: n }, (_, i) => [{ role: 'assistant', content: 'x' }, { role: 'user', content: `turn ${i}` }]).flat(),
];

await scenario('what the shim reports', async () => {
  const A = await avalon();
  const S = await shim(A);
  await sleep(300);
  const m = await getJson(`${S.url}/metrics`);
  check('engine block from /props', m.engine.model === 'qwen3.8-flash-next' && m.engine.version === 'avalon b1-abcdef0' && m.engine.max_context === 262144, JSON.stringify(m.engine));
  check('one batch slot, as avalon runs it', m.engine.batch_slots === 1, `batch_slots=${m.engine.batch_slots}`);
  check('engine start watched for restart detection', Math.abs(m.totals.since * 1000 - Date.now()) < 10000, `${m.totals.since}`);
  check('idle engine reports an idle slot', m.live.state === 'idle' && m.live.running === 0 && m.live.slots[0].state === 'idle', JSON.stringify(m.live));
  check('gpu readings from nvidia-smi', m.hardware.gpu_util === 42 && m.hardware.vram_free_mib === 2576 && m.hardware.gpu_mem_used === 22000 * 1048576, JSON.stringify(m.hardware));
  check('history sampled for the dashboard', m.history.gpu_util.length > 0 && m.history.gpu_util.every((v) => v === 42), `n=${m.history.gpu_util.length}`);
  check('no parks without a conversation cache', m.conversation_cache.parks === null && m.conversation_cache.requests === 0, JSON.stringify(m.conversation_cache));
  check('other paths relay to the engine', (await getJson(`${S.url}/slots`))[0].n_ctx === 262144 && (await getJson(`${S.url}/v1/models`)).data[0].id === 'qwen3.8-flash-next');
});

await scenario('reasoning effort translated for avalon', async () => {
  const A = await avalon();
  const S = await shim(A);
  for (const [sent, want] of [['high', 'xhigh'], ['minimal', 'low'], ['max', 'xhigh'], ['medium', 'medium'], ['low', 'low'], ['xhigh', 'xhigh']]) {
    const res = await fetch(`${S.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: 'hi' }], reasoning_effort: sent, max_tokens: 5 }),
    });
    const ok = res.status === 200 && A.lastRequest?.reasoning_effort === want;
    check(`effort ${sent} reaches avalon as ${want}`, ok, `${res.status} got ${A.lastRequest?.reasoning_effort}`);
  }
  const off = await fetch(`${S.url}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: 'hi' }], reasoning_effort: 'none', max_tokens: 5 }),
  });
  check('effort none turns thinking off', off.status === 200 && A.lastRequest?.chat_template_kwargs?.enable_thinking === false && !A.lastRequest?.reasoning_effort, `${off.status} ${JSON.stringify(A.lastRequest)}`);
  const kw = await fetch(`${S.url}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: 'hi' }], chat_template_kwargs: { reasoning_effort: 'high', preserve_thinking: true }, max_tokens: 5 }),
  });
  check('effort inside chat_template_kwargs translated too', kw.status === 200 && A.lastRequest?.chat_template_kwargs?.reasoning_effort === 'xhigh' && A.lastRequest?.chat_template_kwargs?.preserve_thinking === true, `${kw.status} ${JSON.stringify(A.lastRequest?.chat_template_kwargs)}`);
  const m = await getJson(`${S.url}/shim`);
  check('the shim says what it rewrote', m.shim.effort_mapped.length >= 7, JSON.stringify(m.shim.effort_mapped));
});

await scenario('the router routes avalon', async () => {
  const A = await avalon({ tokens: 24 });
  A.usage = { prompt: 1200, cached: 0, completion: 24 };
  const S = await shim(A);
  const r = await router([{ name: 'avalon', port: S.port }]);
  await sleep(300);

  const b0 = await backend(r, 'avalon');
  check('router marks the shimmed engine healthy', b0.healthy === true && b0.poll_fails === 0, `${b0.healthy} fails=${b0.poll_fails} err=${b0.lastError}`);
  check('router took batch slots, version and vram from the shim', b0.batch === 1 && b0.version === 'avalon b1-abcdef0' && b0.vram_free_mib === 2576, `${b0.batch} ${b0.version} ${b0.vram_free_mib}`);
  check('router carries gpu readings through', b0.hardware?.gpu_util === 42 && b0.history?.gpu_util?.length > 0, JSON.stringify(b0.hardware?.gpu_util));
  check('model list proxied through the shim', (await getJson(`${r.url}/v1/models`)).data?.[0]?.id === 'qwen3.8-flash-next');

  const first = await stream(r, turns('avalon conversation', 0));
  check('routed to avalon', first.backend === 'avalon' && first.text, `${first.backend} ${first.error ?? first.text}`);
  check('second turn stays on avalon', (await chat(r, turns('avalon conversation', 1))).backend === 'avalon');

  A.usage = { prompt: 1300, cached: 1200, completion: 24 };
  const running = stream(r, turns('avalon conversation', 2));
  await sleep(120);
  const busy = await backend(r, 'avalon');
  check('busy slot reported as work in progress', busy.running === 1 && ['reading', 'generating'].includes(busy.activity?.[0]?.state), JSON.stringify(busy.activity));
  check('the router matched that slot to the conversation', typeof busy.activity?.[0]?.key === 'string', JSON.stringify(busy.activity?.[0]));
  await running;

  await sleep(300);
  const b = await backend(r, 'avalon');
  check('engine speeds learned from the shim', b.recent?.decode_tok_s > 20 && b.recent?.prefill_tok_s > 1000, JSON.stringify(b.recent));
  check('draft acceptance from avalon counters', b.recent?.draft_accept > 0.7 && b.recent?.draft_accept <= 1, `${b.recent?.draft_accept}`);
  check('prompt cache hits reported', b.reused === 1 && b.reused_tokens === 1200 && b.prompt_tokens >= 2500, `reused=${b.reused}/${b.requests} tokens=${b.reused_tokens}/${b.prompt_tokens}`);
  check('held tokens from avalon slots', b.held_tokens >= 1300 && b.slots?.[0]?.held_tokens >= 1300, `${b.held_tokens} ${JSON.stringify(b.slots)}`);
  check('the router start follows the engine, not a bogus clock', Math.abs(b.started - Date.now()) < 10000, `${b.started}`);
});

await scenario('one avalon slot against a two slot strata', async () => {
  const A = await avalon({ promptMs: 500 });
  const S = await shim(A);
  const M = await mock('strata');
  const r = await router([{ name: 'avalon', port: S.port }, { name: 'strata', port: M.port }]);
  const one = await chat(r, turns('a new conversation', 0));
  check('a new conversation prefers the engine with more free slots', one.backend === 'strata', one.backend);

  const both = await Promise.all([chat(r, turns('one at a time', 0)), chat(r, turns('the other one', 0)), chat(r, turns('and a third', 0))]);
  check('all three served', both.every((x) => x.text), both.map((x) => x.error?.message ?? x.backend).join('/'));
  check('they spread over both engines', new Set(both.map((x) => x.backend)).size === 2, both.map((x) => x.backend).join('/'));
  check('avalon never ran two at once', A.active.length <= 1, `active=${A.active.length}`);
});

await scenario('the shim follows the engine down and back', async () => {
  const A = await avalon();
  const S = await shim(A);
  const M = await mock('strata');
  const r = await router([{ name: 'avalon', port: S.port }, { name: 'strata', port: M.port }], { MAX_INFLIGHT: '1' });
  await sleep(200);
  const home = await chat(r, turns('a conversation on avalon', 1));
  check('conversation at home on avalon', home.backend === 'avalon', home.backend);

  A.failing = true;
  await sleep(300);
  const down = await backend(r, 'avalon');
  check('a failing engine reads as down', down.healthy === false, `healthy=${down.healthy} err=${down.lastError}`);
  const moved = await chat(r, turns('a conversation on avalon', 2));
  check('spill to the other engine', moved.backend === 'strata' && !moved.error, `${moved.backend} ${moved.error?.message ?? ''}`);

  A.failing = false;
  A.restart();
  await sleep(400);
  const back = await backend(r, 'avalon');
  const st = await getJson(`${r.url}/stats`);
  check('restarted engine is healthy again', back.healthy === true, `fails=${back.poll_fails}`);
  check('the router saw the engine lose its cache', back.restarts >= 1 && st.stats.restarts >= 1, `restarts=${back.restarts}/${st.stats.restarts}`);
  check('no conversation keeps its home on the restarted engine', !st.conversations.some((c) => c.backend === 'avalon'), JSON.stringify(st.conversations));
});

await scenario('an engine without --metrics', async () => {
  const A = await avalon({ metricsEndpoint: false, tokens: 24 });
  A.usage = { prompt: 1200, cached: 0, completion: 24 };
  const S = await shim(A);
  const m = await getJson(`${S.url}/metrics`);
  check('shim still answers metrics', m.live.state === 'idle' && m.engine.metrics_endpoint === false, JSON.stringify({ state: m.live.state, ep: m.engine.metrics_endpoint }));
  check('start time derived from the engine, not prometheus', typeof m.totals.since === 'number' && m.totals.since > 0, `${m.totals.since}`);
  const r = await router([{ name: 'avalon', port: S.port }]);
  check('router still routes it', (await backend(r, 'avalon')).healthy === true);
  const c = await stream(r, turns('no metrics engine', 0));
  check('and gets an answer', c.backend === 'avalon' && c.text, `${c.backend} ${c.error?.message ?? c.text}`);
  await sleep(200);
  const b = await backend(r, 'avalon');
  check('answer timings carry speeds and drafts even without /metrics', b.recent?.decode_tok_s > 20 && b.recent?.draft_accept > 0.7, JSON.stringify(b.recent));
});

for (const c of children) c.kill('SIGKILL');
for (const s of servers) {
  s.server.closeAllConnections?.();
  s.server.close();
}
await sleep(200);
console.log(failures ? `${failures} test(s) failed` : 'all tests passed');
process.exit(failures ? 1 : 0);
