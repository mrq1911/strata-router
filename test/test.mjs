import { setTimeout as sleep } from 'node:timers/promises';
import { spawn } from 'node:child_process';
import { makeEngine } from './mock-engines.mjs';

const A = makeEngine('engineA', 9101, 2, 300);
const B = makeEngine('engineB', 9102, 2, 300);

const router = spawn(process.execPath, ['router.mjs'], {
  env: { ...process.env, STRATA_BACKENDS: 'A=127.0.0.1:9101,B=127.0.0.1:9102', PORT: '9000', POLL_MS: '500' },
  stdio: 'inherit',
});
await sleep(1200);

const R = 'http://127.0.0.1:9000';
const chat = async (messages) => {
  const res = await fetch(`${R}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: 'fake', messages, max_tokens: 10 }),
  });
  const d = await res.json();
  return { backend: res.headers.get('x-strata-backend'), text: d.choices?.[0]?.message?.content, error: d.error };
};

let failures = 0;
const check = (label, cond, detail = '') => {
  console.log(`${cond ? 'ok  ' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);
  if (!cond) failures++;
};

// 1. the model list is proxied (clients use it to show a provider as running)
const models = await (await fetch(`${R}/v1/models`)).json();
check('/v1/models proxied', Array.isArray(models.data) && models.data.length > 0, JSON.stringify(models.data));

// 2. a conversation stays on one engine
const conv = [{ role: 'system', content: ' terse ' }, { role: 'user', content: 'sticky conversation' }];
const homes = new Set();
for (let i = 0; i < 5; i++) {
  const r = await chat(conv);
  homes.add(r.backend);
  conv.push({ role: 'assistant', content: 'x' });
  conv.push({ role: 'user', content: `turn ${i}` });
}
check('conversation stays on one engine', homes.size === 1, [...homes].join('/'));

// 3. a fork of that conversation inherits the same engine
const fork = await chat(conv.slice(0, 4).concat([{ role: 'user', content: 'branch here' }]));
check('fork routed to the same engine', [...homes][0] === fork.backend, fork.backend);

// 4. burst of 4 new conversations spreads 2 per engine
const burst = await Promise.all(Array.from({ length: 4 }, (_, i) => chat([{ role: 'user', content: `burst ${i}` }])));
const counts = {};
for (const r of burst) counts[r.backend] = (counts[r.backend] || 0) + 1;
check('burst respects 2 slots per engine', Object.values(counts).every((c) => c <= 2), JSON.stringify(counts));

// 5. kill engine A, requests still succeed
A.server.closeAllConnections();
A.server.close();
await sleep(1500);
const dead = await chat([{ role: 'user', content: 'after A died' }]);
check('failover to the surviving engine', dead.backend === 'B' && !dead.error, `${dead.backend} ${dead.error ?? ''}`);
const health = await (await fetch(`${R}/health`)).json();
check('dead engine reported unhealthy', health.backends.find((b) => b.name === 'A').healthy === false);

// 6. queueing when everything is full
const flood = await Promise.all(Array.from({ length: 6 }, (_, i) => chat([{ role: 'user', content: `flood ${i}` }])));
check('all flood requests served despite saturation', flood.every((r) => r.text), `${flood.length} responses`);
const st = await (await fetch(`${R}/stats`)).json();
check('stats counted queue waits', st.stats.queued > 0, `queued=${st.stats.queued} overflow=${st.stats.overflow}`);

router.kill();
A.server.closeAllConnections();
B.server.closeAllConnections();
A.server.close();
B.server.close();
console.log(failures ? `${failures} test(s) failed` : 'all tests passed');
process.exit(failures ? 1 : 0);
