// realistic soak: growing context, tools, streaming, repeated rounds
const BASE = process.env.BASE || 'http://127.0.0.1:8080';
const tool = [{ type: 'function', function: { name: 'read_file', description: 'Read a file', parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } } }];
const filler = (n) => Array.from({ length: n }, (_, i) => `chunk ${i}: shipment ${i * 13 % 991} dock ${i % 7} archive ${i * 7 % 997}`).join(' ');
const sizes = [400, 1500, 3000, 6000, 8000, 1200];

async function health() {
  try { return (await fetch(BASE + '/health', { signal: AbortSignal.timeout(8000) })).ok; } catch { return false; }
}

for (let n = 0; n < (Number(process.env.ROUNDS) || 12); n++) {
  const chunks = sizes[n % sizes.length];
  const t0 = Date.now();
  let timings = null, status = 0;
  try {
    const res = await fetch(BASE + '/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: filler(chunks) + '\n\nSummarise the notes and check notes.txt with the tool.' }], tools: tool, max_tokens: 200, stream: n % 2 === 0 }),
    });
    status = res.status;
    const text = await res.text();
    for (const line of text.split('\n')) {
      if (!line.startsWith('data:')) continue;
      try { const o = JSON.parse(line.slice(5)); if (o.timings) timings = o.timings; } catch {}
    }
    if (!timings && status === 200) timings = JSON.parse(text).timings || null;
  } catch (err) {
    console.log(`round ${n} (${chunks} chunks): fetch error ${err.message}`);
  }
  const t = timings || {};
  console.log(`round ${n} (${chunks} chunks): ${status} ${Date.now() - t0}ms prefill ${t.prompt_n ? (t.prompt_n / (t.prompt_ms / 1000)).toFixed(0) : '-'} t/s decode ${t.predicted_ms ? (t.predicted_n / (t.predicted_ms / 1000)).toFixed(1) : '-'} t/s cache ${t.cache_n ?? 0} draft ${t.draft_n_accepted ?? 0}/${t.draft_n ?? 0}`);
  if (!await health()) { console.log('engine died at round ' + n); break; }
}
console.log('soak done');
