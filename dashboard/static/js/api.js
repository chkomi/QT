/**
 * API fetch wrapper — OKX-focused dashboard v2
 */
const B = '';

async function f(p) {
  const r = await fetch(B + p);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export const api = {
  positions:  ()           => f('/api/positions'),
  health:     ()           => f('/api/health'),
  portfolio:  ()           => f('/api/portfolio'),
  trades:     (n = 20)     => f(`/api/trades?limit=${n}`),
  equity:     (ex = 'okx') => f(`/api/equity-history/${ex}`),
  candles:    (m, i = 'day', c = 200) => f(`/api/candles/okx/${m}?interval=${i}&count=${c}`),
  logs:       (n = 200)    => f(`/api/logs?lines=${n}`),
};
