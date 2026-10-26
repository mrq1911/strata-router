import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const d = JSON.parse(readFileSync(process.argv[2] || join(homedir(), '.pi/agent/models.json'), 'utf8'));
for (const [pid, p] of Object.entries(d.providers)) {
  for (const m of p.models || []) {
    if (p.api !== 'openai-completions' && p.api !== 'openai-responses') continue;
    const url = (p.baseUrl || '').replace(/\/+$/, '') + '/models';
    try {
      const t0 = Date.now();
      const r = await fetch(url, { headers: { Authorization: `Bearer ${p.apiKey || 'local'}` }, signal: AbortSignal.timeout(1000) });
      const b = await r.json().catch(() => undefined);
      const ids = Array.isArray(b?.data) ? b.data.map((x) => String(x.id)) : undefined;
      const state = ids === undefined ? 'no state' : ids.some((id) => id === m.id || id.endsWith(`/${m.id}`)) ? 'running' : 'idle';
      console.log(`${pid}/${m.id}  ${url}  ${r.status} ${Date.now() - t0}ms  -> ${state}`);
    } catch (e) {
      console.log(`${pid}/${m.id}  ${url}  -> ${e.name === 'TimeoutError' ? 'slow/unknown' : 'offline'}`);
    }
  }
}
