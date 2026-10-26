import { setTimeout as sleep } from 'node:timers/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { makeEngine } from './mock-engines.mjs';

let nextPort = 19100;
const engines = [];
const routers = [];

let failures = 0;
const check = (label, cond, detail = '') => {
  console.log(`${cond ? 'ok  ' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);
  if (!cond) failures++;
};

async function engine(name, latencyMs = 300) {
  const e = makeEngine(name, nextPort++, 2, latencyMs);
  engines.push(e);
  await once(e.server, 'listening');
  return e;
}

async function router(backends, env = {}) {
  const port = nextPort++;
  const child = spawn(process.execPath, ['router.mjs'], {
    env: {
      ...process.env,
      LOG: process.env.LOG || 'off',
      STRATA_BACKENDS: backends.map((e) => `${e.name}=127.0.0.1:${e.port}`).join(','),
      PORT: String(port),
      POLL_MS: '100',
      ...env,
    },
    stdio: 'inherit',
  });
  routers.push(child);
  const url = `http://127.0.0.1:${port}`;
  for (let i = 0; i < 100 && child.exitCode === null; i++) {
    try {
      await fetch(`${url}/health`);
      break;
    } catch {
      await sleep(50);
    }
  }
  return { url, child };
}

const post = (r, path, body, init = {}) =>
  fetch(`${r.url}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: typeof body === 'string' ? body : JSON.stringify(body),
    ...init,
  });

const get = async (r, path) => (await fetch(`${r.url}${path}`)).json();

async function chat(r, messages) {
  const res = await post(r, '/v1/chat/completions', { model: 'fake', messages, max_tokens: 10 });
  const d = await res.json().catch(() => ({}));
  return { status: res.status, backend: res.headers.get('x-strata-backend'), text: d.choices?.[0]?.message?.content, error: d.error, sha256: d.sha256 };
}

async function stream(r, content) {
  try {
    const res = await post(r, '/v1/chat/completions', { model: 'fake', stream: true, messages: [{ role: 'user', content }] });
    await res.text();
    return 'clean';
  } catch {
    return 'cut';
  }
}

const turns = (first, n) => [
  { role: 'user', content: first },
  ...Array.from({ length: n }, (_, i) => [{ role: 'assistant', content: 'x' }, { role: 'user', content: `turn ${i}` }]).flat(),
];

async function scenario(name, fn) {
  try {
    await fn();
  } catch (err) {
    check(`${name} threw`, false, err.stack);
  }
}

await scenario('routing', async () => {
  const A = await engine('A');
  const B = await engine('B');
  const r = await router([A, B]);

  // 1. the model list is proxied (clients use it to show a provider as running)
  const models = await get(r, '/v1/models');
  check('/v1/models proxied', Array.isArray(models.data) && models.data.length > 0, JSON.stringify(models.data));

  // 2. a conversation stays on one engine
  const conv = [{ role: 'system', content: ' terse ' }, { role: 'user', content: 'sticky conversation' }];
  const homes = new Set();
  for (let i = 0; i < 5; i++) {
    homes.add((await chat(r, conv)).backend);
    conv.push({ role: 'assistant', content: 'x' }, { role: 'user', content: `turn ${i}` });
  }
  check('conversation stays on one engine', homes.size === 1, [...homes].join('/'));

  // 3. a fork of that conversation inherits the same engine
  const fork = await chat(r, conv.slice(0, 4).concat([{ role: 'user', content: 'branch here' }]));
  check('fork routed to the same engine', [...homes][0] === fork.backend, fork.backend);

  // 4. burst of 4 new conversations spreads 2 per engine
  const burst = await Promise.all(Array.from({ length: 4 }, (_, i) => chat(r, [{ role: 'user', content: `burst ${i}` }])));
  const counts = {};
  for (const x of burst) counts[x.backend] = (counts[x.backend] || 0) + 1;
  check('burst respects 2 slots per engine', Object.values(counts).every((c) => c <= 2), JSON.stringify(counts));

  // 5. kill engine A, requests still succeed
  A.server.closeAllConnections();
  A.server.close();
  await sleep(500);
  const dead = await chat(r, [{ role: 'user', content: 'after A died' }]);
  check('failover to the surviving engine', dead.backend === 'B' && !dead.error, `${dead.backend} ${dead.error ?? ''}`);
  const health = await get(r, '/health');
  check('dead engine reported unhealthy', health.backends.find((b) => b.name === 'A').healthy === false);

  // 6. queueing when everything is full
  const flood = await Promise.all(Array.from({ length: 6 }, (_, i) => chat(r, [{ role: 'user', content: `flood ${i}` }])));
  check('all flood requests served despite saturation', flood.every((x) => x.text), `${flood.length} responses`);
  const st = (await get(r, '/stats')).stats;
  check('stats counted queue waits', st.queued > 0, `queued=${st.queued} overflow=${st.overflow}`);
});

await scenario('malformed', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  const bodies = ['null', '[]', '"x"', 'nope', '{"messages":[null]}', '{"messages":[{"role":"user","content":[null]}]}', '{"input":[null,{"role":"user"}]}'];
  const statuses = [];
  for (const body of bodies) statuses.push((await post(r, '/v1/chat/completions', body)).status);
  check('malformed bodies answered, router still up', statuses.join() === '400,400,400,400,200,200,200' && r.child.exitCode === null, statuses.join());
});

await scenario('utf-8', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  const body = JSON.stringify({ model: 'fake', messages: [{ role: 'user', content: '│ ├─ └─ → ř ž 日本 '.repeat(20000) }] });
  const d = await (await post(r, '/v1/chat/completions', body)).json();
  check('multi-byte prompt reaches the engine byte for byte', d.sha256 === createHash('sha256').update(body).digest('hex'), `${Buffer.byteLength(body)} bytes`);
});

await scenario('mid-stream drop', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  A.mode = 'drop';
  const outcomes = [await stream(r, 'drop 1'), await stream(r, 'drop 2')];
  A.mode = 'ok';
  await sleep(100);
  const inflight = (await get(r, '/health')).backends[0].inflight;
  const t0 = Date.now();
  const next = await chat(r, turns('after the drops', 0));
  const ms = Date.now() - t0;
  check('engine dying mid-stream cuts the client off', outcomes.join() === 'cut,cut', outcomes.join());
  check('engine dying mid-stream frees its slot', inflight === 0 && next.status === 200 && ms < 1000, `inflight=${inflight} next=${next.status} in ${ms}ms`);
});

await scenario('no retry once started', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B]);
  for (const mode of ['drop-headers', 'reset']) {
    A.mode = mode;
    const before = B.posts;
    const outcome = await stream(r, `${mode} test`);
    await sleep(100);
    check(`${mode}: client cut off, not retried, router up`, outcome === 'cut' && B.posts === before && r.child.exitCode === null, `${outcome}, B got ${B.posts - before}`);
  }
});

await scenario('5xx', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B]);
  A.mode = '500';
  const moved = await chat(r, turns('retry me', 0));
  const st = (await get(r, '/stats')).stats;
  check('engine 5xx before any output is retried on the other engine', moved.status === 200 && moved.backend === 'B' && st.retried === 1, `${moved.status} ${moved.backend} retried=${st.retried}`);

  const solo = await router([A]);
  const passed = await chat(solo, turns('nowhere to go', 0));
  check('with nowhere to retry, the engine error is passed on', passed.status === 500 && passed.error === 'engine exploded', `${passed.status} ${JSON.stringify(passed.error)}`);
});

await scenario('client abort', async () => {
  const A = await engine('A');
  const r = await router([A]);
  A.mode = 'slow';
  const ac = new AbortController();
  const res = await post(r, '/v1/chat/completions', { model: 'fake', stream: true, messages: [{ role: 'user', content: 'abort me' }] }, { signal: ac.signal });
  await res.body.getReader().read();
  ac.abort();
  await sleep(300);
  const inflight = (await get(r, '/health')).backends[0].inflight;
  check('client abort reaches the engine and frees the slot', A.aborted === 1 && A.completed === 0 && inflight === 0, `aborted=${A.aborted} completed=${A.completed} inflight=${inflight}`);
});

await scenario('lru', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { MAX_CONVERSATIONS: '3' });
  let k = 0;
  await chat(r, turns('X', k++));
  for (const c of ['c1', 'c2', 'c3']) {
    await chat(r, turns('X', k++));
    await chat(r, turns(c, 0));
  }
  await chat(r, turns('X', k++));
  const { cold } = (await get(r, '/stats')).stats;
  check('conversation map evicts the least recently used', cold === 4, `cold=${cold}`);
});

await scenario('keys', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  const send = async (path, body) => (await post(r, path, { model: 'fake', ...body })).text();
  await send('/v1/responses', { input: 'apples' });
  await send('/v1/responses', { input: 'pears' });
  await send('/v1/responses', { input: [{ role: 'user', content: 'plums' }] });
  await send('/v1/responses', { input: [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'figs' }] }] });
  await send('/v1/messages', { system: [{ type: 'text', text: 'be terse' }], messages: [{ role: 'user', content: 'figs' }] });
  const { cold } = (await get(r, '/stats')).stats;
  check('responses and messages conversations get their own keys', cold === 5, `cold=${cold}`);
});

await scenario('usage', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  await stream(r, 'streamed');
  await (await post(r, '/v1/messages', { model: 'fake', max_tokens: 10, messages: [{ role: 'user', content: 'anthropic' }] })).text();
  await (await post(r, '/v1/responses', { model: 'fake', input: 'responses' })).text();
  const st = (await get(r, '/stats')).stats;
  check('usage counted for streams, messages and responses', st.prompt_tokens === 3000 && st.cached_tokens === 1800 && st.completion_tokens === 30, `prompt=${st.prompt_tokens} cached=${st.cached_tokens} out=${st.completion_tokens}`);
});

await scenario('passthrough', async () => {
  const A = await engine('A', 20);
  const open = await router([A]);
  A.mode = '500';
  const relayed = await post(open, '/v1/vram', {}, { signal: AbortSignal.timeout(3000) });
  check('engine POST paths relayed, errors included', relayed.status === 500 && A.posts === 1, String(relayed.status));
  A.mode = 'ok';

  const locked = await router([A], { PASSTHROUGH: '/v1/vram' });
  const refused = await post(locked, '/v1/admin/reset', {});
  check('PASSTHROUGH limits which POST paths reach the engine', refused.status === 404 && A.posts === 1, `${refused.status}, engine posts=${A.posts}`);

  const slots = await fetch(`${open.url}/slots`);
  const models = await fetch(`${open.url}/models`);
  check('GET passthrough keeps the engine status', slots.status === 404 && models.status === 200, `${slots.status}/${models.status}`);

  const before = (await get(open, '/stats')).stats.requests;
  const q = await post(open, '/v1/chat/completions?api-version=1', { model: 'fake', messages: [{ role: 'user', content: 'q' }] });
  const after = (await get(open, '/stats')).stats.requests;
  check('chat path with a query string is still routed', q.status === 200 && after === before + 1, `${q.status}, routed +${after - before}`);
});

await scenario('health', async () => {
  const A = await engine('A', 20);
  A.metricsStatus = 503;
  const r = await router([A]);
  check('engine answering 503 on /metrics is unhealthy', (await get(r, '/health')).backends[0].healthy === false);
  const t0 = Date.now();
  const res = await chat(r, turns('anyone there', 0));
  const ms = Date.now() - t0;
  check('nothing healthy: 503 without sitting out MAX_QUEUE_WAIT', res.status === 503 && ms < 2000, `${res.status} in ${ms}ms`);
});

await scenario('patience', async () => {
  const A = await engine('A', 50);
  const B = await engine('B', 50);
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const first = await chat(r, turns('long conversation', 0));

  A.queued = 1;
  await sleep(300);
  setTimeout(() => (A.queued = 0), 400);
  const t0 = Date.now();
  const [stay, fresh] = await Promise.all([
    chat(r, turns('long conversation', 1)).then((x) => ({ ...x, ms: Date.now() - t0 })),
    sleep(50).then(() => chat(r, turns('brand new', 0))),
  ]);
  check('busy home: conversation waits for its engine instead of spilling', first.backend === 'A' && stay.backend === 'A' && stay.ms >= 300, `${first.backend} -> ${stay.backend} after ${stay.ms}ms`);
  check('busy home: a new conversation goes straight to the idle engine', fresh.backend === 'B', fresh.backend);

  A.queued = 1;
  await sleep(300);
  const t1 = Date.now();
  const spill = await chat(r, turns('long conversation', 2));
  const ms = Date.now() - t1;
  const { migrated } = (await get(r, '/stats')).stats;
  check('home busy longer than a cold prefill: spill and move', spill.backend === 'B' && ms >= 900 && migrated === 1, `${spill.backend} after ${ms}ms, migrated=${migrated}`);
});

await scenario('state file', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const dir = mkdtempSync(join(tmpdir(), 'strata-router-'));
  try {
    const STATE_FILE = join(dir, 'homes.json');
    const before = await router([A, B], { STATE_FILE });
    const first = await chat(before, turns('survive a restart', 0));
    before.child.kill('SIGTERM');
    await once(before.child, 'exit');
    const after = await router([B, A], { STATE_FILE });
    const next = await chat(after, turns('survive a restart', 1));
    check('STATE_FILE keeps conversation homes across restarts', first.backend === 'A' && next.backend === 'A', `${first.backend} -> ${next.backend}`);
    after.child.kill('SIGTERM');
    await once(after.child, 'exit');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

for (const child of routers) child.kill();
for (const e of engines) {
  e.server.closeAllConnections();
  e.server.close();
}
console.log(failures ? `${failures} test(s) failed` : 'all tests passed');
process.exit(failures ? 1 : 0);
