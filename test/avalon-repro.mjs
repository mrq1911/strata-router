// one request that reproduces the cuda abort: a ~150k token prompt with tools
const BASE = process.env.BASE || 'http://127.0.0.1:8080';
const tool = [{ type: 'function', function: { name: 'read_file', description: 'Read a file', parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } } }];
const filler = Array.from({ length: Number(process.env.CHUNKS || 8000) }, (_, i) => `chunk ${i}: shipment ${i * 13 % 991} dock ${i % 7} archive ${i * 7 % 997}`).join(' ');

const res = await fetch(BASE + '/v1/chat/completions', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ model: 'qwen3.8-flash-next', messages: [{ role: 'user', content: filler + '\n\nSummarise the notes and check notes.txt with the tool.' }], tools: tool, max_tokens: 200, stream: true }),
});
const text = await res.text();
let timings = null;
for (const line of text.split('\n')) {
  if (!line.startsWith('data:')) continue;
  try { const o = JSON.parse(line.slice(5)); if (o.timings) timings = o.timings; } catch {}
}
if (!timings) {
  console.log('FAILED: status ' + res.status + ' no timings, body ' + text.slice(0, 200));
  process.exit(1);
}
console.log(`ok: prompt ${timings.prompt_n} cached ${timings.cache_n} prefill ${(timings.prompt_n / (timings.prompt_ms / 1000)).toFixed(0)} t/s decode ${(timings.predicted_n / (timings.predicted_ms / 1000)).toFixed(1)} t/s draft ${timings.draft_n_accepted}/${timings.draft_n}`);
