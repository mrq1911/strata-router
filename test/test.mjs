import { setTimeout as sleep } from 'node:timers/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import http from 'node:http';
import net from 'node:net';
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
    stdio: ['ignore', 'inherit', 'pipe'],
  });
  routers.push(child);
  const r = { url: `http://127.0.0.1:${port}`, child, stderr: '' };
  child.stderr.on('data', (c) => {
    r.stderr += c;
    process.stderr.write(c);
  });
  for (let i = 0; i < 100 && child.exitCode === null; i++) {
    try {
      await fetch(`${r.url}/health`);
      break;
    } catch {
      await sleep(50);
    }
  }
  return r;
}

const post = (r, path, body, init = {}) =>
  fetch(`${r.url}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: typeof body === 'string' ? body : JSON.stringify(body),
    ...init,
  });

const get = async (r, path) => (await fetch(`${r.url}${path}`)).json();

async function chat(r, messages, extra = {}) {
  const res = await post(r, '/v1/chat/completions', { model: 'fake', messages, max_tokens: 10, ...extra });
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
    A.mode = B.mode = mode;
    const before = A.posts + B.posts;
    const outcome = await stream(r, `${mode} test`);
    await sleep(100);
    check(`${mode}: client cut off, not retried, router up`, outcome === 'cut' && A.posts + B.posts === before + 1 && r.child.exitCode === null, `${outcome}, engines got ${A.posts + B.posts - before}`);
  }
  A.mode = B.mode = 'ok';
});

await scenario('5xx', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B]);
  A.mode = '500';
  const moved = await chat(r, turns('retry me', 0));
  const st = (await get(r, '/stats')).stats;
  check('engine 5xx before any output is retried on the other engine', moved.status === 200 && moved.backend === 'B' && st.retried === 1, `${moved.status} ${moved.backend} retried=${st.retried}`);

  A.mode = '500big';
  const big = await chat(r, turns('oversized error', 0));
  check('an oversized engine error body is dropped and the request still moves', big.status === 200 && big.backend === 'B', `${big.status} ${big.backend}`);
  A.mode = '500';

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
  const image = (data) => [{ type: 'image_url', image_url: { url: `data:image/png;base64,${data}` } }];
  await send('/v1/chat/completions', { messages: [{ role: 'user', content: image('AAAA') }] });
  await send('/v1/chat/completions', { messages: [{ role: 'user', content: image('BBBB') }] });
  const { cold } = (await get(r, '/stats')).stats;
  check('responses, messages and image-only conversations get their own keys', cold === 7, `cold=${cold}`);
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

  for (const odd of ['/v1/chat/completions/', '//v1/chat/completions']) {
    await (await post(open, odd, { model: 'fake', messages: [{ role: 'user', content: 'odd path' }] })).text();
  }
  const oddRouted = (await get(open, '/stats')).stats.requests - after;
  const oddUrls = A.log.slice(-2).map((x) => x.url);
  check('trailing and doubled slashes still route', oddRouted === 2 && oddUrls.every((u) => u === '/v1/chat/completions'), `routed +${oddRouted}, engine saw ${oddUrls.join(' ')}`);
});

await scenario('headers', async () => {
  const A = await engine('A', 20);
  const r = await router([A]);
  const headers = { 'content-type': 'application/json', authorization: 'Bearer local', 'x-api-key': 'k', 'anthropic-version': '2023-06-01', 'accept-encoding': 'gzip' };
  await (await post(r, '/v1/messages', { model: 'fake', max_tokens: 10, messages: [{ role: 'user', content: 'keys' }] }, { headers })).text();
  const seen = A.log.at(-1).headers;
  check('end-to-end client headers reach the engine', seen.authorization === 'Bearer local' && seen['x-api-key'] === 'k' && seen['anthropic-version'] === '2023-06-01' && !seen['accept-encoding'], JSON.stringify({ authorization: seen.authorization, 'x-api-key': seen['x-api-key'], 'accept-encoding': seen['accept-encoding'] }));

  const status = await new Promise((resolve, reject) => {
    const req = http.request(`${r.url}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', connection: 'keep-alive, x-hop', 'x-hop': 'local only' },
    }, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
    req.end(JSON.stringify({ model: 'fake', messages: [{ role: 'user', content: 'hop' }] }));
  });
  check('headers named in connection stay hop-by-hop', status === 200 && !A.log.at(-1).headers['x-hop'], `status ${status}, x-hop=${A.log.at(-1).headers['x-hop']}`);
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
  await sleep(1200);
  const t1 = Date.now();
  const later = await chat(r, turns('still nobody', 0));
  const ms1 = Date.now() - t1;
  check('engines down for a while: 503 at once', later.status === 503 && ms1 < 300, `${later.status} in ${ms1}ms`);
});

await scenario('poll hysteresis', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { POLL_FAILS: '5' });
  const home = await chat(r, turns('flaky metrics', 0));
  A.metricsHangup = true;
  await sleep(120);
  const during = (await get(r, '/health')).backends[0].healthy;
  const stay = await chat(r, turns('flaky metrics', 1));
  A.metricsHangup = false;
  check('a failed poll or two keeps the engine and its conversations', home.backend === 'A' && during && stay.backend === 'A', `${home.backend} -> ${stay.backend}, healthy during the blip: ${during}`);

  await sleep(250);
  A.metricsHangup = true;
  const t0 = Date.now();
  let down = false;
  while (!down && Date.now() - t0 < 3000) {
    await sleep(25);
    down = (await get(r, '/health')).backends[0].healthy === false;
  }
  const ms = Date.now() - t0;
  A.metricsHangup = false;
  check('POLL_FAILS failed polls in a row mark it down', down && ms >= 300, `down after ${ms}ms`);
});

await scenario('learned prefill', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  for (const e of [A, B]) Object.assign(e, { prefillMs: 500, usage: { prompt: 1500, cached: 0, completion: 10 } });
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  await Promise.all([stream(r, 'measure one'), stream(r, 'measure two')]);
  const rates = (await get(r, '/stats')).backends.map((b) => b.prefill_tok_s);
  check('prefill speed measured per engine, role-only opener ignored', rates.every((x) => x > 2000 && x < 3200), rates.join('/'));

  const home = (await chat(r, turns('measure one', 1))).backend;
  const busy = home === 'A' ? A : B;
  busy.queued = 1;
  await sleep(300);
  const t0 = Date.now();
  const moved = await chat(r, turns('measure one', 2));
  const ms = Date.now() - t0;
  busy.queued = 0;
  check('patience follows the measured speed, not PREFILL_TOK_S', moved.backend !== home && ms >= 400 && ms < 1200, `${home} -> ${moved.backend} after ${ms}ms`);
});

await scenario('drain', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { SHUTDOWN_GRACE: '5000' });
  A.mode = 'slow';
  const res = await post(r, '/v1/chat/completions', { model: 'fake', stream: true, messages: [{ role: 'user', content: 'finish me' }] });
  const reader = res.body.getReader();
  await reader.read();
  const t0 = Date.now();
  const exit = once(r.child, 'exit');
  r.child.kill('SIGTERM');
  await sleep(100);
  const late = await post(r, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content: 'too late' }] }).then(
    (x) => x.status,
    (e) => e.cause?.code || e.message,
  );
  let text = '';
  const decoder = new TextDecoder();
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) text += decoder.decode(chunk.value, { stream: true });
  const [code] = await exit;
  const ms = Date.now() - t0;
  check('SIGTERM lets the answer in flight finish', A.completed === 1 && text.includes('t19'), `completed=${A.completed}`);
  check('... turns new work away and exits once idle', late !== 200 && code === 0 && ms < 5000, `late=${late}, exit ${code} after ${ms}ms`);
});

await scenario('settings', async () => {
  const cases = [
    { MAX_INFLIGHT: 'two' },
    { STRATA_BACKENDS: '127.0.0.1:9,127.0.0.1:10' },
    { STRATA_BACKENDS: 'a=host:99999' },
    { PASSTHROUGH: 'v1/vram' },
    { SHUTDOWN_GRACE: '99999999999' },
  ];
  for (const env of cases) {
    const child = spawn(process.execPath, ['router.mjs'], {
      env: { ...process.env, STRATA_BACKENDS: '127.0.0.1:9', PORT: String(nextPort++), ...env },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', (c) => (stderr += c));
    const timer = setTimeout(() => child.kill(), 3000);
    const [code] = await once(child, 'exit');
    clearTimeout(timer);
    const [name, value] = Object.entries(env)[0];
    check(`bad ${name}=${value} stops the router`, code === 1 && stderr.includes(name), stderr.trim());
  }
  const v6 = await router([], { STRATA_BACKENDS: 'v6=[::1]:9' });
  const { host } = (await get(v6, '/health')).backends[0];
  check('ipv6 engines parse', host === '[::1]:9', host);
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

await scenario('fifo', async () => {
  const E = await engine('E', 10);
  const r = await router([E], { MAX_INFLIGHT: '1', POLL_MS: '60000', MAX_QUEUE_WAIT: '20000' });
  const holder = chat(r, turns('L', 0), { latency_ms: 2300 });
  await sleep(100);
  const a = chat(r, turns('A', 0));
  await sleep(500);
  const b = chat(r, turns('B', 0));
  await Promise.all([holder, a, b]);
  const order = E.log.map((x) => x.first).join(',');
  check('requests waiting for a slot are served in arrival order', order === 'L,A,B', order);
});

await scenario('home claim', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { MAX_INFLIGHT: '1', POLL_MS: '60000', PREFILL_TOK_S: '2000', MAX_QUEUE_WAIT: '10000' });
  await chat(r, turns('patient', 0));
  const holdA = chat(r, turns('hold A', 0), { latency_ms: 400 });
  await sleep(30);
  const holdB = chat(r, turns('hold B', 0), { latency_ms: 750 });
  await sleep(30);
  const newcomer = chat(r, turns('newcomer', 0), { latency_ms: 450 });
  await sleep(30);
  const patient = chat(r, turns('patient', 1));
  const [n, p] = await Promise.all([newcomer, patient, holdA, holdB]);
  const servedA = A.log.map((x) => x.first);
  const patientFirst = n.backend === 'B' || servedA.lastIndexOf('patient') < servedA.indexOf('newcomer');
  check('a conversation waiting for its home keeps that slot from newcomers', p.backend === 'A' && patientFirst, `patient -> ${p.backend}, newcomer -> ${n.backend}, A served ${servedA.join(',')}`);
});

await scenario('dead home', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { MAX_INFLIGHT: '1', POLL_MS: '60000' });
  const home = await chat(r, turns('home dies', 0));
  const holdA = chat(r, turns('hold A', 0), { latency_ms: 200 });
  await sleep(30);
  const holdB = chat(r, turns('hold B', 0), { latency_ms: 800 });
  await holdA;
  A.server.closeAllConnections();
  A.server.close();
  const t0 = Date.now();
  const moved = await chat(r, turns('home dies', 1));
  const ms = Date.now() - t0;
  await holdB;
  check('home refusing connections: the request queues for another engine instead of failing', home.backend === 'A' && moved.status === 200 && moved.backend === 'B', `${moved.status} from ${moved.backend} after ${ms}ms`);
});

await scenario('read timeout', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { READ_TIMEOUT: '300' });
  const quiet = await chat(r, turns('long silent prefill', 0), { latency_ms: 1000 });
  check('silence past READ_TIMEOUT is fine while the engine reports work', quiet.status === 200 && quiet.backend === 'A' && B.posts === 0, `${quiet.status} from ${quiet.backend}, B got ${B.posts}`);

  A.mode = 'hang';
  A.reportIdle = true;
  await sleep(250);
  const lost = await chat(r, turns('lost on the engine', 0));
  A.mode = 'ok';
  A.reportIdle = false;
  check('silence from an engine with nothing running: given up and moved', lost.status === 200 && lost.backend === 'B', `${lost.status} from ${lost.backend}`);

  const C = await engine('C', 20);
  const D = await engine('D', 20);
  const r2 = await router([C, D], { POLL_FAILS: '2', READ_TIMEOUT: '30000' });
  C.mode = 'hang';
  const t0 = Date.now();
  const pending = chat(r2, turns('engine vanishes', 0));
  await sleep(150);
  C.metricsHangup = true;
  const vanished = await pending;
  const ms = Date.now() - t0;
  check('engine stops answering polls mid-prefill: moved without waiting out READ_TIMEOUT', vanished.status === 200 && vanished.backend === 'D' && ms < 3000, `${vanished.status} from ${vanished.backend} after ${ms}ms`);

  const E = await engine('E', 20);
  const F = await engine('F', 20);
  const r3 = await router([E, F], { POLL_FAILS: '2' });
  E.prefillMs = 1500;
  const started = stream(r3, 'already streaming');
  await sleep(150);
  E.metricsHangup = true;
  const outcome = await started;
  E.metricsHangup = false;
  check('a stream that already started is not cut when polls give its engine up', outcome === 'clean', outcome);
});

await scenario('drain slow client', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { SHUTDOWN_GRACE: '10000' });
  A.mode = 'big';
  const res = await post(r, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content: 'big answer' }] });
  const reader = res.body.getReader();
  const exit = once(r.child, 'exit');
  let got = 0;
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) {
    if (got < 1 << 20 && got + chunk.value.length >= 1 << 20) r.child.kill('SIGTERM');
    got += chunk.value.length;
    await sleep(1);
  }
  const [code] = await exit;
  check('a drain waits until a slow client has the whole answer', got === A.bigBytes && code === 0, `${got} of ${A.bigBytes} bytes, exit ${code}`);
});

await scenario('state on kill', async () => {
  const A = await engine('A', 20);
  const dir = mkdtempSync(join(tmpdir(), 'strata-router-'));
  try {
    const STATE_FILE = join(dir, 'homes.json');
    const r = await router([A], { STATE_FILE, SHUTDOWN_GRACE: '10000' });
    await chat(r, turns('remember me', 0));
    A.mode = 'slow';
    const busyStream = stream(r, 'keeps the drain going');
    await sleep(100);
    const exit = once(r.child, 'exit');
    r.child.kill('SIGTERM');
    await sleep(100);
    r.child.kill('SIGKILL');
    await exit;
    await busyStream;
    const saved = existsSync(STATE_FILE) ? JSON.parse(readFileSync(STATE_FILE, 'utf8')).conversations.length : 0;
    check('homes are saved when the drain starts, so a kill mid-drain keeps them', saved >= 1, `${saved} saved`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

await scenario('pipelined hang-up', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { MAX_INFLIGHT: '1', POLL_MS: '60000' });
  const holder = chat(r, turns('hold the slot', 0), { latency_ms: 600 });
  await sleep(50);
  const request = (content) => {
    const body = JSON.stringify({ model: 'fake', messages: [{ role: 'user', content }] });
    return `POST /v1/chat/completions HTTP/1.1\r\nhost: router\r\ncontent-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;
  };
  const socket = net.connect(Number(new URL(r.url).port), '127.0.0.1');
  await once(socket, 'connect');
  socket.write(Array.from({ length: 6 }, (_, i) => request(`pipelined ${i}`)).join(''));
  await sleep(100);
  socket.destroy();
  await holder;
  await sleep(300);
  const inflight = (await get(r, '/health')).backends[0].inflight;
  const reached = A.log.filter((x) => String(x.first).startsWith('pipelined')).length;
  check('pipelined requests from a client that hung up never reach the engine', reached === 0 && inflight === 0, `reached=${reached} inflight=${inflight}`);
  check('six pipelined requests without a listener warning', !r.stderr.includes('MaxListenersExceededWarning'), r.stderr.trim().split('\n')[0] || 'no warnings');
});

await scenario('retry with nowhere to go', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B]);
  B.server.closeAllConnections();
  B.server.close();
  await sleep(300);
  A.mode = '500';
  const t0 = Date.now();
  const res = await post(r, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content: 'nowhere left' }] }, { signal: AbortSignal.timeout(5000) });
  const body = await res.json();
  const ms = Date.now() - t0;
  check('retry with the only other engine down: the engine error comes back instead of a hang', res.status === 500 && body.error === 'engine exploded' && ms < 2000, `${res.status} in ${ms}ms`);
});

await scenario('no usage', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  A.usage = null;
  B.usage = null;
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const opener = 'x'.repeat(4000);
  const first = await chat(r, turns(opener, 0));
  A.queued = 1;
  await sleep(300);
  setTimeout(() => (A.queued = 0), 300);
  const next = await chat(r, turns(opener, 1));
  check('no usage from the engine: patience comes from the request size', first.backend === 'A' && next.backend === 'A', `${first.backend} -> ${next.backend}`);
});

await scenario('api key', async () => {
  const A = await engine('A', 20);
  A.apiKey = 'sesame';
  const bare = await router([A]);
  const stat = (await get(bare, '/stats')).backends[0];
  check('keyed engine without STRATA_API_KEY_FILE: down, and the error says why', stat.healthy === false && /STRATA_API_KEY_FILE/.test(stat.lastError), stat.lastError);

  const dir = mkdtempSync(join(tmpdir(), 'strata-router-'));
  try {
    const keyFile = join(dir, 'key');
    writeFileSync(keyFile, 'sesame\n');
    const keyed = await router([A], { STRATA_API_KEY_FILE: keyFile });
    const healthy = (await get(keyed, '/health')).backends[0].healthy;
    const withKey = await post(keyed, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content: 'key' }] }, { headers: { 'content-type': 'application/json', authorization: 'Bearer sesame' } });
    const withoutKey = await post(keyed, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content: 'no key' }] });
    check('STRATA_API_KEY_FILE: polls get through, clients still need their own key', healthy && withKey.status === 200 && withoutKey.status === 401, `healthy=${healthy}, with key ${withKey.status}, without ${withoutKey.status}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

await scenario('strata state', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  A.noBatch = true;
  B.noBatch = true;
  const r = await router([A, B], { READ_TIMEOUT: '300' });
  const quiet = await chat(r, turns('single-slot prefill', 0), { latency_ms: 1000 });
  check('an engine without batch slots counts as busy from its state', quiet.status === 200 && quiet.backend === 'A' && B.posts === 0, `${quiet.status} from ${quiet.backend}, B got ${B.posts}`);

  A.unloaded = true;
  await sleep(300);
  const fresh = await chat(r, turns('new while A is unloaded', 0));
  const moved = await chat(r, turns('single-slot prefill', 1));
  A.unloaded = false;
  check('unloaded engines come last and lose their conversations', fresh.backend === 'B' && moved.backend === 'B', `new -> ${fresh.backend}, A's conversation -> ${moved.backend}`);

  const solo = await router([A]);
  const before = (await get(solo, '/stats')).stats.queued;
  await Promise.all([chat(solo, turns('solo one', 0), { latency_ms: 300 }), chat(solo, turns('solo two', 0), { latency_ms: 300 })]);
  const waited = (await get(solo, '/stats')).stats.queued - before;
  check('an engine without batch slots takes one request at a time', waited === 1, `${waited} waited for the slot`);
});

await scenario('dashboard and events', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { PREFILL_TOK_S: '20000' });

  const page = await fetch(`${r.url}/dashboard`);
  const html = await page.text();
  check('/dashboard serves a self-contained html page', page.status === 200 && /^text\/html/.test(page.headers.get('content-type')) && html.startsWith('<!doctype html>') && !/https?:\/\//.test(html), `${page.status} ${page.headers.get('content-type')}, ${html.length} bytes`);
  const icon = await fetch(`${r.url}/favicon.ico`);
  const svg = await icon.text();
  check('/favicon.ico serves the icon the page links to', icon.status === 200 && svg.startsWith('<svg') && html.includes('rel="icon" href="/favicon.ico"'), `${icon.status} ${icon.headers.get('content-type')}, ${svg.length} bytes`);

  const t0 = Date.now();
  const first = await chat(r, turns('route me', 0));
  await chat(r, turns('route me', 1));
  const [home, other] = first.backend === 'A' ? [A, B] : [B, A];
  home.queued = 1;
  await sleep(300);
  await chat(r, turns('route me', 2));
  home.queued = 0;
  await sleep(300);
  other.mode = '500';
  await chat(r, turns('route me', 3));
  other.mode = 'ok';
  const { events, last } = await get(r, '/stats/events');
  const [, sticky, spill, retry] = events;
  check('events: a new conversation, a sticky turn, a spill and a retry', events.map((e) => e.kind).join() === 'cold,sticky,spill,retry', events.map((e) => e.kind).join());
  check('events: a spill names the old home, a retry the engine that failed', spill?.home === home.name && spill.backend === other.name && spill.queued && retry?.retry_from === other.name && retry.retry_error === 'upstream 500' && retry.backend === home.name, JSON.stringify({ spill, retry }));
  check('events: tokens and times, but no prompt text', sticky?.prompt_tokens === 1000 && sticky.cached_tokens === 600 && sticky.completion_tokens === 10 && sticky.arrived >= t0 && sticky.ms >= 0 && !JSON.stringify(events).includes('route me'), JSON.stringify(sticky));

  const newer = await get(r, `/stats/events?since=${events[1].id}`);
  const restarted = await get(r, `/stats/events?since=${last + 100}`);
  check('/stats/events?since= sends only newer events, all of them to a cursor from before a restart', newer.events.map((e) => e.kind).join() === 'spill,retry' && restarted.events.length === events.length, `${newer.events.length} newer, ${restarted.events.length} for a stale cursor`);

  const { conversations } = await get(r, '/stats/conversations');
  const conv = conversations.find((c) => c.turns === 4);
  check('conversations carry their last-seen time and migration count', conv?.backend === home.name && conv.migrations === 2 && conv.last_seen >= t0 && conv.last_seen <= Date.now(), JSON.stringify(conv));
  const st = await get(r, '/stats');
  check('/stats adds conversation and engine fields', st.conversations[0].migrations === 2 && st.conversations[0].last_seen === conv.last_seen && st.backends.every((b) => b.capacity === 2 && b.claimed === 0) && st.backends.find((b) => b.name === home.name).conversations === 1, JSON.stringify(st.backends.map(({ name, capacity, claimed, conversations: n }) => ({ name, capacity, claimed, n }))));

  A.mode = B.mode = '500';
  await chat(r, turns('nobody answers', 0));
  A.mode = B.mode = 'ok';
  const pending = chat(r, turns('in flight', 0), { latency_ms: 800 });
  await sleep(200);
  const during = (await get(r, '/stats/events')).live;
  await pending;
  A.mode = B.mode = 'slow';
  const ac = new AbortController();
  const res = await post(r, '/v1/chat/completions', { model: 'fake', stream: true, messages: [{ role: 'user', content: 'leaving early' }] }, { signal: ac.signal });
  await res.body.getReader().read();
  ac.abort();
  await sleep(300);
  A.mode = B.mode = 'ok';
  const tail = (await get(r, '/stats/events')).events.slice(-3);
  check('events: failed with its status, in flight under live, client gone', tail.map((e) => e.kind).join() === 'failed,cold,gone' && tail[0].status === 500 && tail[0].retry_from && during.length === 1 && during[0].backend && during[0].key === tail[1].key, JSON.stringify({ tail: tail.map((e) => [e.kind, e.status, e.error]), during }));

  const Z = await engine('Z', 0);
  const rz = await router([Z]);
  for (let i = 0; i < 205; i++) await chat(rz, turns('ring', 0));
  const ring = await get(rz, '/stats/events');
  check('events keep only the last 200', ring.events.length === 200 && ring.last === 205 && ring.events[0].id === 6, `${ring.events.length} kept, ids ${ring.events[0]?.id}..${ring.last}`);
});

await scenario('claim hold', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { MAX_INFLIGHT: '1', POLL_MS: '60000', PREFILL_TOK_S: '100' });
  for (const p of ['P1', 'P2', 'P3', 'P4']) await chat(r, turns(p, 0));
  const pending = [chat(r, turns('L', 0), { latency_ms: 1500 })];
  await sleep(50);
  pending.push(chat(r, turns('N', 0)));
  for (const p of ['P1', 'P2', 'P3', 'P4']) {
    await sleep(50);
    pending.push(chat(r, turns(p, 1), { latency_ms: 900 }));
  }
  await Promise.all(pending);
  const order = A.log.slice(4).map((x) => x.first);
  check('a newcomer waits out claims for at most CLAIM_HOLD_MS, then takes its turn', order.indexOf('N') < order.indexOf('P3'), order.join(','));
});

await scenario('engine restart', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const first = await chat(r, turns('warm', 0));
  await chat(r, turns('warm', 1));
  await sleep(300);
  const calm = (await get(r, '/stats')).stats.restarts;
  const [home, other] = first.backend === 'A' ? [A, B] : [B, A];
  home.queued = 1;
  home.restart();
  await sleep(300);
  const t0 = Date.now();
  const moved = await chat(r, turns('warm', 2));
  const ms = Date.now() - t0;
  home.queued = 0;
  const st = await get(r, '/stats');
  const ev = (await get(r, '/stats/events')).events.at(-1);
  check('a restarted home lost the cache: no waiting for it, the turn goes where there is room', calm === 0 && moved.backend === other.name && ms < 500 && ev.kind === 'cold' && st.stats.restarts === 1 && st.backends.find((b) => b.name === home.name).restarts === 1, `restarts before ${calm}, ${moved.backend} after ${ms}ms as ${ev.kind}, restarts ${st.stats.restarts}`);

  await sleep(300);
  other.queued = 1;
  other.restart({ sameServer: true });
  await sleep(300);
  const t1 = Date.now();
  const back = await chat(r, turns('warm', 3));
  const ms1 = Date.now() - t1;
  other.queued = 0;
  const restarts = (await get(r, '/stats')).stats.restarts;
  check('an engine restarted inside the same server counts too (its cache counters start over)', back.backend === home.name && ms1 < 500 && restarts === 2, `${back.backend} after ${ms1}ms, restarts ${restarts}`);
});

await scenario('short first', async () => {
  const long = (name) => turns(`${name} ${'x'.repeat(9000)}`, 0);
  const order = (e) => e.log.map((x) => String(x.first).split(' ')[0]).join(',');
  const run = async (env, holdMs, queue) => {
    const E = await engine('E', 10);
    const r = await router([E], { MAX_INFLIGHT: '1', POLL_MS: '60000', MAX_QUEUE_WAIT: '20000', ...env });
    const pending = [chat(r, turns('H', 0), { latency_ms: holdMs })];
    for (const [at, messages] of queue) {
      await sleep(at);
      pending.push(chat(r, messages));
    }
    await Promise.all(pending);
    return order(E);
  };
  const [jump, fifo, bounded] = await Promise.all([
    run({}, 1000, [[100, long('L')], [100, turns('S', 0)]]),
    run({ SHORT_PROMPT_TOKENS: '0' }, 1000, [[100, long('L')], [100, turns('S', 0)]]),
    run({}, 5600, [[100, long('L1')], [1400, long('L2')], [500, turns('S', 0)]]),
  ]);
  check('a short request takes a freed slot before a long one queued ahead of it', jump === 'H,S,L', jump);
  check('SHORT_PROMPT_TOKENS=0: plain arrival order', fifo === 'H,L,S', fifo);
  check('a long request that already waited SHORT_PASS_MS is not passed any more', bounded === 'H,L1,S,L2', bounded);
});

await scenario('hang-up after [DONE]', async () => {
  const A = await engine('A', 20);
  A.tailMs = 300;
  const r = await router([A]);
  const res = await post(r, '/v1/chat/completions', { model: 'fake', stream: true, messages: [{ role: 'user', content: 'sdk style' }] });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let text = '';
  while (!text.includes('[DONE]')) {
    const { value, done } = await reader.read();
    if (done) break;
    text += dec.decode(value, { stream: true });
  }
  await reader.cancel();
  await sleep(500);
  const [ev] = (await get(r, '/stats/events')).events;
  const { stats } = await get(r, '/stats');
  check('a client hanging up right after [DONE] got a whole answer, not a cancelled one', ev?.kind === 'cold' && ev.status === 200 && ev.prompt_tokens === 1000 && ev.ttft_ms != null && stats.prompt_tokens === 1000, JSON.stringify({ kind: ev?.kind, error: ev?.error, prompt: ev?.prompt_tokens, ttft: ev?.ttft_ms }));
});

await scenario('direct use', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const first = await chat(r, turns('lives on A', 0));
  const [home, other] = first.backend === 'A' ? [A, B] : [B, A];
  const direct = (content) => post({ url: `http://127.0.0.1:${home.port}` }, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content }], latency_ms: 3000 }).then((x) => x.text());
  const bypass = [direct('straight to the engine 1'), direct('straight to the engine 2')];
  await sleep(400);
  const seen = (await get(r, '/stats')).backends.find((b) => b.name === home.name).direct;
  const fresh = await chat(r, turns('new while it is busy', 0));
  const t0 = Date.now();
  const sticky = await chat(r, turns('lives on A', 1));
  const ms = Date.now() - t0;
  await Promise.all(bypass);
  await sleep(400);
  const after = (await get(r, '/stats')).backends.find((b) => b.name === home.name).direct;
  check('requests sent to an engine directly take its slots: newcomers go elsewhere', seen === 2 && fresh.backend === other.name && after === 0, `direct ${seen} then ${after}, new conversation -> ${fresh.backend}`);
  check('a conversation whose home is used directly waits out its patience and spills instead of queueing inside it', sticky.backend === other.name && ms >= 900, `${sticky.backend} after ${ms}ms`);
});

await scenario('balance', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B]);
  const idle = await chat(r, turns('both idle', 0));
  const direct = (content) => post({ url: `http://127.0.0.1:${A.port}` }, '/v1/chat/completions', { model: 'fake', messages: [{ role: 'user', content }], latency_ms: 1500 }).then((x) => x.text());
  await Promise.all([direct('keep A busy 1'), direct('keep A busy 2')]);
  await sleep(300);
  const busy = Object.fromEntries((await get(r, '/stats')).backends.map((b) => [b.name, b.busy]));
  const next = await chat(r, turns('both idle again', 0));
  check('with both engines idle, a new conversation goes to the one busy least lately', idle.backend === 'A' && busy.A > busy.B && next.backend === 'B', `first -> ${idle.backend}, busy ${JSON.stringify(busy)}, next -> ${next.backend}`);
});

await scenario('slow first token', async () => {
  const A = await engine('A', 20);
  const r = await router([A], { SLOW_FIRST_TOKEN: '200', PREFILL_TOK_S: '20000' });
  await stream(r, 'quick');
  A.prefillMs = 400;
  await stream(r, 'stalled');
  const [quick, stalled] = (await get(r, '/stats/events')).events;
  const st = await get(r, '/stats');
  check('a first token far slower than the uncached prompt needs is flagged', !quick.slow && stalled.slow && stalled.engine_ttft_ms >= 400 && stalled.expected_ttft_ms === 20 && st.stats.slow === 1 && st.backends[0].slow === 1, JSON.stringify({ quick: [quick.slow, quick.engine_ttft_ms], stalled: [stalled.slow, stalled.engine_ttft_ms, stalled.expected_ttft_ms] }));
  A.reportRecent = true;
  await sleep(300);
  const pending = chat(r, turns('busy now', 0), { latency_ms: 800 });
  await sleep(400);
  const [busy, inflight] = [(await get(r, '/stats')).backends[0], (await get(r, '/stats/events')).live];
  await pending;
  check('/stats shows what the engine is doing, for which conversation, and its hardware and recent speed', busy.activity.length === 1 && busy.activity[0].key === inflight[0]?.key && busy.activity[0].state === 'decoding' && busy.hardware.gpu_util === 50 && busy.history.gpu_util.length === 2 && busy.recent.decode_tok_s === 100 && busy.recent.prefill_tok_s === 2000 && busy.recent.draft_accept === 0.6, JSON.stringify({ activity: busy.activity, recent: busy.recent }));
  check('/stats shows engine version and cache counters, and the router settings the dashboard explains', st.settings.max_queue_wait_ms === 30000 && st.backends[0].version === '0.0-mock' && st.backends[0].parks >= 1 && st.backends[0].prompt_tokens >= 1000 && st.backends[0].started > 0, JSON.stringify(st.backends[0]));
});

// a client that stops reading its answer, and hangs up after hangupMs unless that is null
function stalled(r, body, hangupMs) {
  const url = new URL(r.url);
  const req = http.request({ host: url.hostname, port: url.port, path: '/v1/chat/completions', method: 'POST', headers: { 'content-type': 'application/json' } });
  req.on('response', (res) => {
    res.pause();
    if (hangupMs != null) setTimeout(() => req.destroy(), hangupMs);
  });
  req.on('error', () => {});
  req.end(JSON.stringify(body));
  return req;
}

await scenario('stuck clients', async () => {
  const A = await engine('A', 0);
  A.mode = 'big';
  const r = await router([A], { READ_TIMEOUT: '600' });
  // the engine finishes while the router waits on the client, then the client hangs up; which sizes do that
  // depends on socket buffers, so try a range
  for (let kb = 2048; kb <= 3686; kb += 102) {
    A.bigBytes = kb * 1024;
    stalled(r, { model: 'fake', messages: [{ role: 'user', content: `stuck ${kb}` }] }, 300);
    await sleep(450);
  }
  await sleep(500);
  const leaked = (await get(r, '/health')).backends[0].inflight;
  A.bigBytes = 8 << 20;
  const hung = stalled(r, { model: 'fake', messages: [{ role: 'user', content: 'hung' }] }, null);
  await sleep(300);
  const during = (await get(r, '/health')).backends[0].inflight;
  await sleep(1200);
  const after = (await get(r, '/health')).backends[0].inflight;
  const ev = (await get(r, '/stats/events')).events.at(-1);
  hung.destroy();
  check('a client hanging up while the router waits on it frees the slot, wherever the engine got to', leaked === 0, `${leaked} slot(s) still taken`);
  check('a client that stops reading holds its slot for READ_TIMEOUT, then counts as gone', during === 1 && after === 0 && ev.kind === 'gone' && ev.error === 'client stopped reading', `${during} -> ${after}, ${ev.kind} ${ev.error}`);
});

await scenario('step aside', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { MAX_INFLIGHT: '1', PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const first = await chat(r, turns('short one', 0));
  const [home, other] = first.backend === 'A' ? [A, B] : [B, A];
  home.queued = 1;
  await sleep(300);
  const waiting = chat(r, turns('short one', 1));
  await sleep(100);
  const t0 = Date.now();
  const long = await chat(r, turns(`long newcomer ${'x'.repeat(12000)}`, 0));
  const ms = Date.now() - t0;
  home.queued = 0;
  await waiting;
  check('a long request doesn\'t wait for a short one that waits for another engine', long.backend === other.name && ms < 400, `${long.backend} after ${ms}ms`);
});

await scenario('patience by size', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  A.usageFromSize = B.usageFromSize = true;
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const branch = (last) => [{ role: 'user', content: 'branchy' }, { role: 'assistant', content: 'x' }, { role: 'user', content: last }];
  const first = await chat(r, branch('y'.repeat(16000)));
  const home = first.backend === 'A' ? A : B;
  await chat(r, branch('a short fork'));
  home.queued = 1;
  await sleep(300);
  setTimeout(() => (home.queued = 0), 2500);
  const t0 = Date.now();
  const next = await chat(r, branch('y'.repeat(17000)));
  const ms = Date.now() - t0;
  check('a short fork doesn\'t cut how long the next long turn waits for its home', next.backend === first.backend && ms >= 2000, `${next.backend} after ${ms}ms`);
});

await scenario('unloaded engines', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  const r = await router([A, B], { PREFILL_TOK_S: '1000', MAX_QUEUE_WAIT: '10000' });
  const first = await chat(r, turns('lives here', 0));
  const [home, other] = first.backend === 'A' ? [A, B] : [B, A];
  home.unloaded = true;
  await sleep(300);
  home.unloaded = false;
  await sleep(300);
  await chat(r, turns('lives here', 1));
  const ev = (await get(r, '/stats/events')).events.at(-1);
  const restarts = (await get(r, '/stats')).stats.restarts;
  check('an engine that unloads its model loses its conversations\' homes', ev.kind === 'cold' && restarts === 1, `${ev.kind}, restarts ${restarts}`);

  home.unloaded = true;
  other.queued = 1;
  await sleep(300);
  setTimeout(() => (other.queued = 0), 1000);
  const t0 = Date.now();
  const fresh = await chat(r, turns('nothing loaded is free', 0));
  const ms = Date.now() - t0;
  home.unloaded = false;
  check('an engine with its model unloaded takes no work while a loaded one is about to free up', fresh.backend === other.name && ms >= 800, `${fresh.backend} after ${ms}ms`);
});

await scenario('restart while away', async () => {
  const A = await engine('A', 20);
  const dir = mkdtempSync(join(tmpdir(), 'strata-router-'));
  try {
    const STATE_FILE = join(dir, 'homes.json');
    const before = await router([A], { STATE_FILE });
    await chat(before, turns('across restarts', 0));
    before.child.kill('SIGTERM');
    await once(before.child, 'exit');
    A.restart();
    const after = await router([A], { STATE_FILE });
    await chat(after, turns('across restarts', 1));
    const ev = (await get(after, '/stats/events')).events.at(-1);
    const restarts = (await get(after, '/stats')).stats.restarts;
    check('an engine that restarted while the router was down has lost the homes the router saved', ev.kind === 'cold' && restarts === 1, `${ev.kind}, restarts ${restarts}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

await scenario('slow engine learns', async () => {
  const A = await engine('A', 20);
  A.usage = { prompt: 5000, cached: 0, completion: 10 };
  A.prefillMs = 1000;
  const r = await router([A], { PREFILL_TOK_S: '20000', SLOW_FIRST_TOKEN: '100' });
  await stream(r, 'slow one');
  await stream(r, 'slow two');
  const [one, two] = (await get(r, '/stats/events')).events;
  const learned = (await get(r, '/stats')).backends[0].prefill_tok_s;
  check('an engine far slower than PREFILL_TOK_S still learns its own speed, and then isn\'t flagged', one.slow && learned > 0 && learned < 10000 && !two.slow, `learned ${learned}, slow ${one.slow} then ${two.slow}`);
});

await scenario('hang-up after the last event', async () => {
  const A = await engine('A', 20);
  A.tailMs = 300;
  const r = await router([A]);
  for (const [path, body, last] of [
    ['/v1/messages', { model: 'fake', max_tokens: 10, stream: true, messages: [{ role: 'user', content: 'anthropic sdk style' }] }, '"type":"message_stop"'],
    ['/v1/responses', { model: 'fake', stream: true, input: 'responses sdk style' }, '"type":"response.completed"'],
  ]) {
    const res = await post(r, path, body);
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let text = '';
    while (!text.includes(last)) {
      const { value, done } = await reader.read();
      if (done) break;
      text += dec.decode(value, { stream: true });
    }
    await reader.cancel();
  }
  await sleep(500);
  const ev = (await get(r, '/stats/events')).events;
  check('anthropic and responses streams: a hang-up after the last event is a whole answer, usage and first token included', ev.length === 2 && ev.every((e) => e.kind === 'cold' && e.prompt_tokens === 1000 && e.ttft_ms != null), JSON.stringify(ev.map((e) => [e.kind, e.prompt_tokens, e.ttft_ms])));
});

await scenario('engine view', async () => {
  const A = await engine('A', 20);
  const B = await engine('B', 20);
  B.noBatch = true;
  A.badRequests = true;
  const r = await router([A, B]);
  await sleep(400);
  const odd = (await get(r, '/stats')).backends.find((b) => b.name === 'A');
  check('an odd field in /metrics doesn\'t fail the poll', odd.healthy && odd.poll_fails === 0 && odd.recent === null, JSON.stringify({ healthy: odd.healthy, fails: odd.poll_fails, recent: odd.recent }));

  A.queued = 1;
  await sleep(300);
  const pending = chat(r, turns('one at a time', 0), { latency_ms: 800 });
  await sleep(400);
  const solo = (await get(r, '/stats')).backends.find((b) => b.name === 'B');
  await pending;
  A.queued = 0;
  check('an engine without batch slots reports the request it runs', solo.running === 1, `running ${solo.running}`);

  const busy = chat(r, turns('busy when it goes quiet', 0), { latency_ms: 1500 });
  await sleep(300);
  A.metricsHangup = B.metricsHangup = true;
  await sleep(700);
  const down = (await get(r, '/stats')).backends;
  A.metricsHangup = B.metricsHangup = false;
  await busy;
  check('an engine that stopped answering polls shows no stale work', down.every((b) => !b.healthy && b.running === 0 && b.activity.length === 0), JSON.stringify(down.map((b) => [b.name, b.healthy, b.running, b.activity.length])));
});

for (const child of routers) child.kill('SIGKILL');
for (const e of engines) {
  e.server.closeAllConnections();
  e.server.close();
}
console.log(failures ? `${failures} test(s) failed` : 'all tests passed');
process.exit(failures ? 1 : 0);
