/**
 * API fetch 래퍼 — 정적 JSON 파일 기반 (GitHub Pages용)
 * 데이터는 GitHub Actions(거래소) + 로컬 push(거래내역)로 갱신됩니다.
 */

async function apiFetch(path) {
  const res = await fetch(path + '?_=' + Date.now());  // 캐시 방지
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${path}`);
  return res.json();
}

export const api = {
  health:    () => apiFetch('./data/health.json'),
  portfolio: () => apiFetch('./data/portfolio.json'),
  risk:      () => apiFetch('./data/risk.json'),
  signals:   () => apiFetch('./data/signals.json'),

  trades: (limit = 50, exchange = '', market = '') => {
    return apiFetch('./data/trades.json').then(data => {
      let trades = data.trades || [];
      if (exchange) trades = trades.filter(t => t.exchange === exchange);
      if (market)   trades = trades.filter(t => t.market   === market);
      return { trades: trades.slice(0, limit), total: trades.length };
    });
  },

  candles: (exchange, market, interval = 'day', count = 200) => {
    const key = `${exchange}_${market}_${interval}`;
    return apiFetch(`./data/candles_${key}.json`);
  },

  equityHistory: (exchange) => apiFetch(`./data/equity_${exchange}.json`),
};
