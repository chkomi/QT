/**
 * TradingView Lightweight Charts v4 — 캔들 차트 + 인디케이터 + 거래 마커
 */

// ── 차트 색상 ──────────────────────────────────────────────────
const CHART_THEME = {
  background:  '#161b22',
  text:        '#7d8590',
  grid:        '#21262d',
  border:      '#30363d',
  upColor:     '#26a69a',
  downColor:   '#ef5350',
  wickUp:      '#26a69a',
  wickDown:    '#ef5350',
};

// ── 마커 스타일 ────────────────────────────────────────────────
const MARKER_STYLES = {
  long_entry:        { position: 'belowBar', color: '#26a69a', shape: 'arrowUp',   text: '▲롱',  size: 1 },
  long_close:        { position: 'aboveBar', color: '#ef5350', shape: 'arrowDown', text: '청산',  size: 1 },
  stop_loss:         { position: 'aboveBar', color: '#ff1744', shape: 'arrowDown', text: '손절',  size: 1 },
  take_profit:       { position: 'aboveBar', color: '#00e676', shape: 'arrowDown', text: '익절',  size: 1 },
  short_entry:       { position: 'aboveBar', color: '#ef5350', shape: 'arrowDown', text: '▼숏',  size: 1 },
  short_stop_loss:   { position: 'belowBar', color: '#ff1744', shape: 'arrowUp',   text: '숏SL', size: 1 },
  short_take_profit: { position: 'belowBar', color: '#00e676', shape: 'arrowUp',   text: '숏TP', size: 1 },
};

// ── 일봉 캔들 시간 스냅 ────────────────────────────────────────
// TradingView는 markers의 time이 candle time과 정확히 일치해야 함
function snapToCandle(tradeTs, candleTimes) {
  const d      = new Date(tradeTs * 1000);
  const dayUTC = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
  let best = candleTimes[0];
  for (const t of candleTimes) {
    if (t <= dayUTC) best = t;
    else break;
  }
  return best;
}

// ── 차트 인스턴스 저장소 ────────────────────────────────────────
const chartInstances = {};

/**
 * 차트 초기화 또는 재사용
 * @param {string} containerId  DOM id
 * @param {string} key          고유 키 (exchange_market)
 */
function getOrCreateChart(containerId, key) {
  const container = document.getElementById(containerId);
  if (!container) return null;

  if (chartInstances[key]) {
    return chartInstances[key];
  }

  const chart = LightweightCharts.createChart(container, {
    width:  container.clientWidth,
    height: container.clientHeight || 380,
    layout: {
      background: { type: 'solid', color: CHART_THEME.background },
      textColor:  CHART_THEME.text,
    },
    grid: {
      vertLines:   { color: CHART_THEME.grid },
      horzLines:   { color: CHART_THEME.grid },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
    rightPriceScale: {
      borderColor: CHART_THEME.border,
    },
    timeScale: {
      borderColor:       CHART_THEME.border,
      timeVisible:       false,
      secondsVisible:    false,
    },
    handleScroll:   true,
    handleScale:    true,
  });

  // 반응형 리사이즈
  const ro = new ResizeObserver(() => {
    chart.applyOptions({ width: container.clientWidth });
  });
  ro.observe(container);

  chartInstances[key] = { chart, ro, candleSeries: null, indicatorSeries: {} };
  return chartInstances[key];
}

/**
 * 캔들 + 인디케이터 + 마커 업데이트
 * @param {string}  containerId
 * @param {string}  key
 * @param {object}  candleData  API /api/candles 응답
 * @param {Array}   trades      /api/trades 응답에서 필터링된 거래 배열
 * @param {boolean} isFutures   true이면 숏 마커도 표시
 */
export function updateChart(containerId, key, candleData, trades = [], isFutures = false) {
  const inst = getOrCreateChart(containerId, key);
  if (!inst) return;

  const { chart } = inst;
  const candles    = candleData.candles    || [];
  const indicators = candleData.indicators || {};

  if (candles.length === 0) return;

  // ── 캔들 시리즈 ──
  if (!inst.candleSeries) {
    inst.candleSeries = chart.addCandlestickSeries({
      upColor:        CHART_THEME.upColor,
      downColor:      CHART_THEME.downColor,
      borderUpColor:  CHART_THEME.upColor,
      borderDownColor:CHART_THEME.downColor,
      wickUpColor:    CHART_THEME.wickUp,
      wickDownColor:  CHART_THEME.wickDown,
    });
  }
  inst.candleSeries.setData(candles);

  // ── 인디케이터 시리즈 ──
  const indConfigs = {
    ma200:        { color: 'rgba(255,255,255,0.65)', lineWidth: 1, lineStyle: 0, title: 'MA200' },
    target_long:  { color: 'rgba(38,166,154,0.55)',  lineWidth: 1, lineStyle: 1, title: 'TargetL' },
    target_short: { color: 'rgba(239,83,80,0.55)',   lineWidth: 1, lineStyle: 1, title: 'TargetS' },
    vp_vah:       { color: 'rgba(255,167,38,0.45)',  lineWidth: 1, lineStyle: 2, title: 'VP VAH' },
    vp_val:       { color: 'rgba(171,71,188,0.45)',  lineWidth: 1, lineStyle: 2, title: 'VP VAL' },
    vp_poc:       { color: 'rgba(255,240,64,0.40)',  lineWidth: 1, lineStyle: 2, title: 'VP POC' },
  };

  for (const [key2, cfg] of Object.entries(indConfigs)) {
    const data = indicators[key2];
    if (!data || data.length === 0) continue;

    if (!inst.indicatorSeries[key2]) {
      inst.indicatorSeries[key2] = chart.addLineSeries({
        color:            cfg.color,
        lineWidth:        cfg.lineWidth,
        lineStyle:        cfg.lineStyle,
        title:            cfg.title,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    }
    inst.indicatorSeries[key2].setData(data);
  }

  // ── 거래 마커 ──
  if (trades.length > 0) {
    const candleTimes = candles.map(c => c.time);
    const markers = [];

    for (const trade of trades) {
      const style = MARKER_STYLES[trade.type];
      if (!style) continue;

      // 선물 전용 마커는 isFutures 차트에서만, 현물은 non-futures에서만
      const isShortType = trade.type.startsWith('short');
      if (isFutures  && !isShortType) continue;
      if (!isFutures && isShortType)  continue;

      const snappedTime = snapToCandle(trade.timestamp_unix, candleTimes);

      let text = style.text;
      if (trade.pnl_pct !== null && trade.pnl_pct !== undefined) {
        const sign = trade.pnl_pct >= 0 ? '+' : '';
        text += ` ${sign}${trade.pnl_pct.toFixed(2)}%`;
      }

      markers.push({
        time:     snappedTime,
        position: style.position,
        color:    style.color,
        shape:    style.shape,
        text,
        size:     style.size,
      });
    }

    // TradingView 요구: markers는 time 오름차순 정렬
    markers.sort((a, b) => a.time - b.time);
    inst.candleSeries.setMarkers(markers);
  }

  // 최신 캔들로 스크롤
  chart.timeScale().scrollToPosition(5, false);
}

/**
 * 자산 곡선 (line chart)
 */
export function updateEquityChart(containerId, key, equityData) {
  const container = document.getElementById(containerId);
  if (!container || !equityData || equityData.length === 0) return;

  if (chartInstances[key]) {
    chartInstances[key].chart.remove();
    delete chartInstances[key];
  }

  const chart = LightweightCharts.createChart(container, {
    width:  container.clientWidth,
    height: container.clientHeight || 180,
    layout: {
      background: { type: 'solid', color: CHART_THEME.background },
      textColor:  CHART_THEME.text,
    },
    grid: {
      vertLines: { color: CHART_THEME.grid },
      horzLines: { color: CHART_THEME.grid },
    },
    rightPriceScale: { borderColor: CHART_THEME.border },
    timeScale: {
      borderColor:    CHART_THEME.border,
      timeVisible:    true,
      secondsVisible: false,
    },
    handleScroll: false,
    handleScale:  false,
  });

  const series = chart.addAreaSeries({
    lineColor:        '#388bfd',
    topColor:         'rgba(56,139,253,0.25)',
    bottomColor:      'rgba(56,139,253,0.02)',
    lineWidth:        2,
    priceLineVisible: false,
    lastValueVisible: true,
  });

  series.setData(equityData.map(d => ({ time: d.time, value: d.equity })));
  chart.timeScale().fitContent();

  chartInstances[key] = { chart, ro: null, candleSeries: series, indicatorSeries: {} };
}

/**
 * 모든 차트 크기 리셋 (창 리사이즈 시)
 */
export function resizeAll() {
  for (const inst of Object.values(chartInstances)) {
    if (inst.chart && inst.ro) {
      // ResizeObserver가 이미 처리
    }
  }
}
