/**
 * TradingView Lightweight Charts — OKX only
 */
const THEME = {
  bg: '#161b22', text: '#7d8590', grid: '#21262d',
  up: '#3fb950', down: '#f85149',
};

let _candleChart = null, _candleSeries = null;
let _equityChart = null, _equitySeries = null;

function makeChart(el, opts = {}) {
  return LightweightCharts.createChart(el, {
    layout: { background: { color: THEME.bg }, textColor: THEME.text },
    grid: { vertLines: { color: THEME.grid }, horzLines: { color: THEME.grid } },
    crosshair: { mode: 0 },
    rightPriceScale: { borderColor: THEME.grid },
    timeScale: { borderColor: THEME.grid, timeVisible: true },
    ...opts,
  });
}

export function updateCandleChart(el, data) {
  if (!data || !data.candles || data.candles.length === 0) return;

  if (_candleChart) { _candleChart.remove(); _candleChart = null; }

  _candleChart = makeChart(el);
  _candleSeries = _candleChart.addCandlestickSeries({
    upColor: THEME.up, downColor: THEME.down,
    borderUpColor: THEME.up, borderDownColor: THEME.down,
    wickUpColor: THEME.up, wickDownColor: THEME.down,
  });

  const candles = data.candles.map(c => ({
    time: Math.floor(new Date(c.time || c.datetime).getTime() / 1000),
    open: c.open, high: c.high, low: c.low, close: c.close,
  })).sort((a, b) => a.time - b.time);

  _candleSeries.setData(candles);

  // MA200
  if (data.ma200) {
    const ma = _candleChart.addLineSeries({ color: '#f0883e', lineWidth: 1, priceLineVisible: false });
    const pts = data.ma200.filter(v => v.value > 0).map(v => ({
      time: Math.floor(new Date(v.time || v.datetime).getTime() / 1000),
      value: v.value,
    })).sort((a, b) => a.time - b.time);
    if (pts.length) ma.setData(pts);
  }

  _candleChart.timeScale().fitContent();
}

export function updateEquityChart(el, data) {
  if (!data || data.length === 0) return;

  if (_equityChart) { _equityChart.remove(); _equityChart = null; }

  _equityChart = makeChart(el, { height: 180 });
  _equitySeries = _equityChart.addAreaSeries({
    lineColor: '#58a6ff', topColor: 'rgba(88,166,255,0.3)',
    bottomColor: 'rgba(88,166,255,0.02)', lineWidth: 2,
    priceLineVisible: false,
  });

  const pts = data.map(d => ({
    time: typeof d.time === 'number' ? d.time : Math.floor(new Date(d.time).getTime() / 1000),
    value: d.equity || d.value || 0,
  })).sort((a, b) => a.time - b.time);

  _equitySeries.setData(pts);
  _equityChart.timeScale().fitContent();
}
