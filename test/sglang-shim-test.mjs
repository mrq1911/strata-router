// sglang shim tests: does strata-router see a plain SGLang server as a working engine, and does it route on that
import { setTimeout as sleep } from 'node:timers/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { makeSglang } from './fake-sglang.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const SMI = join(here, 'fixtures', 'fake-nvidia-smi.sh');

let nextPort = 19500;
const children = [];
const servers = [];

let failures = 0;
const check = (label, cond, detail = '') => {
  console.log(`${cond ? 'ok  ' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);
  if (!cond) failures++;
};

async function sglang(opts = {}) {
  const e = makeSglang(nextPort++, opts);
  servers.push(e);
  await once(e.server, 'listening');
  return e;
}

async function spawnUp(script, env) {
  const port = nextPort++;
  const child = spawn(process.execPath, [join(here, '..', script)], {
    env: { ...process.env, LOG: 'off', PORT: String(port), ...env },
    stdio: ['ignore', 'inherit', 'pipe'],
  });
  children.push(child);
  child.stderr.on('data', (c) => process.stderr.write(c));
  const s = { url: `http://127.0.0.1:${port}`, port, child };
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

const shim = (engine, env = {}) => spawnUp('sglang-shim.mjs', {
  SGLANG: `127.0.0.1:${engine.port}`, BIND: '127.0.0.1', SAMPLE_MS: '200', FRESH_MS: '0', INFO_MS: '500', NVIDIA_SMI: SMI, ...env,
});
const router = (backends, env = {}) => spawnUp('router.mjs', {
  STRATA_BACKENDS: backends.map((b) => `${b.name}=127.0.0.1:${b.port}`).join(','), POLL_MS: '100', ...env,
});

const getJson = async (url) => (await fetch(url)).json();
const backend = async (r, name) => (await getJson(`${r.url}/stats`)).backends.find((b) => b.name === name);

async function post(url, body) {
  const res = await fetch(`${url}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: 'hi' }], max_tokens: 10, ...body }),
  });
  return { status: res.status, backend: res.headers.get('x-strata-backend'), text: await res.text() };
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
  const E = await sglang();
  const S = await shim(E);
  await sleep(300);
  const m = await getJson(`${S.url}/metrics`);
  check('engine block from /server_info', m.engine.model === 'qwen3.8-flash-next-exl3' && m.engine.version === 'sglang 0.5.20' && m.engine.max_context === 262144 && m.engine.drafting === 'eagle', JSON.stringify(m.engine));
  check('batch slots from max_running_requests', m.engine.batch_slots === 1 && m.live.parallel === 1, `batch_slots=${m.engine.batch_slots}`);
  check('engine start watched for restart detection', Math.abs(m.totals.since * 1000 - Date.now()) < 10000, `${m.totals.since}`);
  check('idle engine reports an idle slot', m.live.state === 'idle' && m.live.running === 0 && m.live.slots[0].state === 'idle', JSON.stringify(m.live));
  check('gpu readings from nvidia-smi', m.hardware.gpu_util === 42 && m.hardware.vram_free_mib === 2576, JSON.stringify(m.hardware));
  check('other paths relay to the engine', (await getJson(`${S.url}/v1/models`)).data[0].id === 'qwen3.8-flash-next-exl3');
});

await scenario('reasoning effort translated for the qwen3.8 template', async () => {
  const E = await sglang({ chunks: 1 });
  const S = await shim(E);
  for (const [sent, want] of [['high', 'xhigh'], ['minimal', 'low'], ['max', 'xhigh'], ['medium', 'medium'], ['low', 'low'], ['xhigh', 'xhigh']]) {
    const r = await post(S.url, { reasoning_effort: sent });
    check(`effort ${sent} reaches sglang as ${want}`, r.status === 200 && E.lastRequest?.reasoning_effort === want, `${r.status} got ${E.lastRequest?.reasoning_effort}`);
  }
  const off = await post(S.url, { reasoning_effort: 'none' });
  check('effort none turns thinking off', off.status === 200 && E.lastRequest?.chat_template_kwargs?.enable_thinking === false && !('reasoning_effort' in E.lastRequest), `${off.status} ${JSON.stringify(E.lastRequest)}`);
  const kw = await post(S.url, { chat_template_kwargs: { reasoning_effort: 'high', preserve_thinking: true } });
  check('effort inside chat_template_kwargs translated too', kw.status === 200 && E.lastRequest?.chat_template_kwargs?.reasoning_effort === 'xhigh' && E.lastRequest.chat_template_kwargs.preserve_thinking === true, `${kw.status} ${JSON.stringify(E.lastRequest?.chat_template_kwargs)}`);
  const kwOff = await post(S.url, { chat_template_kwargs: { reasoning_effort: 'none' } });
  check('none inside chat_template_kwargs leaves no effort behind', kwOff.status === 200 && E.lastRequest?.chat_template_kwargs?.enable_thinking === false && !('reasoning_effort' in E.lastRequest.chat_template_kwargs), `${kwOff.status} ${JSON.stringify(E.lastRequest?.chat_template_kwargs)}`);
});

await scenario('usage on streamed answers', async () => {
  const E = await sglang({ chunks: 8, perChunk: 3 });
  const S = await shim(E);
  const plain = await post(S.url, { stream: true });
  check('the shim asks sglang for usage', E.lastRequest?.stream_options?.include_usage === true);
  check('and the stream ends with it, as strata\'s do', plain.status === 200 && plain.text.includes('"completion_tokens":24') && plain.text.includes('[DONE]'), plain.text.slice(-200));
  const asked = await post(S.url, { stream: true, stream_options: { include_usage: true } });
  check('a client that asked for usage gets it once', asked.text.split('"usage"').length === 2 && asked.text.includes('"completion_tokens":24'));
  const m = await getJson(`${S.url}/metrics`);
  const r = m.requests[1];
  check('tokens counted from usage, not chunks', r.output_tokens === 24 && r.prompt_tokens === 900, JSON.stringify(r));
  check('decode speed from those tokens', r.decode_tok_s > 50, `${r.decode_tok_s}`);
  check('the shim says it added the usage', m.shim.usage_added === 1, `${m.shim.usage_added}`);
});

await scenario('the router routes sglang', async () => {
  const E = await sglang({ promptMs: 300 });
  const S = await shim(E);
  const r = await router([{ name: 'sglang', port: S.port }]);
  await sleep(300);
  const b0 = await backend(r, 'sglang');
  check('router marks the shimmed engine healthy', b0.healthy === true, `${b0.healthy} err=${b0.lastError}`);
  check('router took batch slots and version from the shim', b0.batch === 1 && b0.version === 'sglang 0.5.20', `${b0.batch} ${b0.version}`);

  const first = await post(r.url, { messages: turns('sglang conversation', 0), stream: true });
  check('routed to sglang', first.status === 200 && first.backend === 'sglang' && first.text.includes('[DONE]'), `${first.status} ${first.backend}`);
  check('streams through the router end with usage', first.text.includes('"completion_tokens":24'));

  const running = post(r.url, { messages: turns('sglang conversation', 1), stream: true });
  await sleep(150);
  const busy = await backend(r, 'sglang');
  check('busy slot reported as work in progress', busy.running === 1 && ['reading', 'generating'].includes(busy.activity?.[0]?.state), JSON.stringify(busy.activity));
  check('second turn stays on sglang', (await running).backend === 'sglang');

  await sleep(300);
  const b = await backend(r, 'sglang');
  check('engine speeds learned from the shim', b.recent?.decode_tok_s > 20, JSON.stringify(b.recent));
});

await scenario('the shim follows the engine down and back', async () => {
  const E = await sglang();
  const S = await shim(E);
  const r = await router([{ name: 'sglang', port: S.port }]);
  await sleep(200);
  await post(r.url, { messages: turns('a conversation on sglang', 1) });
  E.failing = true;
  await sleep(300);
  const down = await backend(r, 'sglang');
  check('a failing engine reads as down', down.healthy === false, `healthy=${down.healthy}`);
  E.failing = false;
  E.restart();
  await sleep(800);
  const back = await backend(r, 'sglang');
  const st = await getJson(`${r.url}/stats`);
  check('restarted engine is healthy again', back.healthy === true, `err=${back.lastError}`);
  check('the router saw the engine lose its cache', back.restarts >= 1, `restarts=${back.restarts}`);
  check('no conversation keeps its home on the restarted engine', !st.conversations.some((c) => c.backend === 'sglang'), JSON.stringify(st.conversations));
});

for (const c of children) c.kill('SIGKILL');
for (const s of servers) {
  s.server.closeAllConnections?.();
  s.server.close();
}
await sleep(200);
console.log(failures ? `${failures} test(s) failed` : 'all tests passed');
process.exit(failures ? 1 : 0);
