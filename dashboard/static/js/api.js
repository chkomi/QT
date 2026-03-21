/**
 * API fetch 래퍼 — 모든 /api/* 호출 담당
 */

const BASE = '';  // same origin

async function apiFetch(path) {
  const res = await fetch(BASE + path);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${path}`);
  return res.json();
}

export const api = {
  health:       ()                              => apiFetch('/api/health'),
  portfolio:    ()                              => apiFetch('/api/portfolio'),
  risk:         ()                              => apiFetch('/api/risk'),
  signals:      ()                              => apiFetch('/api/signals'),
  trades:       (limit=50, exchange='', market='') => {
    const params = new URLSearchParams({ limit });
    if (exchange) params.set('exchange', exchange);
    if (market)   params.set('market',   market);
    return apiFetch(`/api/trades?${params}`);
  },
  candles:      (exchange, market, interval='day', count=200) =>
    apiFetch(`/api/candles/${exchange}/${encodeURIComponent(market)}?interval=${interval}&count=${count}`),
  equityHistory: (exchange) => apiFetch(`/api/equity-history/${exchange}`),
  logs:          (lines=200) => apiFetch(`/api/logs?lines=${lines}`),
};
