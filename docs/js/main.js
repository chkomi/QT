/**
 * 대시보드 메인 — 부트스트랩 + 자동 갱신 + UI 업데이트
 */
import { api }                                from './api.js';
import { updateChart, updateEquityChart }     from './charts.js';

// ── 상태 ────────────────────────────────────────────────────────
let _state = {
  portfolio:    null,
  risk:         null,
  signals:      null,
  trades:       [],
  health:       null,
  selectedCoin: 'KRW-BTC',   // 차트 탭
  refreshCount: 0,
  isRefreshing: false,
};

const REFRESH_MS   = 30_000;   // 30초
const CHART_EVERY  = 5;        // 5 사이클 = 150초
const EQUITY_EVERY = 10;       // 10 사이클 = 300초

// ── 포맷 유틸 ───────────────────────────────────────────────────
const fmtKRW   = v  => v == null ? '—' : Number(v).toLocaleString('ko-KR') + '원';
const fmtUSDT  = v  => v == null ? '—' : Number(v).toLocaleString('en-US', {minimumFractionDigits:2,maximumFractionDigits:2}) + ' USDT';
const fmtPct   = v  => v == null ? '—' : (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%';
const fmtNum   = (v, d=4) => v == null ? '—' : Number(v).toLocaleString('en-US', {minimumFractionDigits:0,maximumFractionDigits:d});
const fmtPrice = (v, cur) => cur === 'USDT' ? fmtNum(v,2) : fmtNum(v,0);

function pnlClass(v) {
  if (v == null || v === 0) return 'pnl-neutral';
  return v > 0 ? 'pnl-positive' : 'pnl-negative';
}

function colorClass(v) {
  if (v == null || v === 0) return '';
  return v > 0 ? 'green' : 'red';
}

const SIGNAL_LABEL = {
  '0':  { text: '신호 없음', cls: 'badge-neutral' },
  '1':  { text: '롱 진입 ▲', cls: 'badge-signal' },
  '-1': { text: '롱 청산',   cls: 'badge-bear'   },
  '2':  { text: '숏 진입 ▼', cls: 'badge-bear'   },
  '-2': { text: '숏 청산',   cls: 'badge-bull'   },
};

const TYPE_KR = {
  long_entry:        '롱 진입',
  long_close:        '롱 청산',
  stop_loss:         '손  절',
  take_profit:       '익  절',
  short_entry:       '숏 진입',
  short_stop_loss:   '숏 손절',
  short_take_profit: '숏 익절',
};

// ── DOM 참조 ────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

// ── 헤더 ────────────────────────────────────────────────────────
function renderHeader(health) {
  const botBadge = $('bot-status-badge');
  if (!botBadge) return;
  if (health?.bot_running) {
    botBadge.innerHTML = '<span class="dot dot-green"></span> 실행 중';
    botBadge.className = 'badge badge-live';
  } else {
    botBadge.innerHTML = '<span class="dot dot-grey"></span> 오프라인';
    botBadge.className = 'badge badge-offline';
  }
  const lastRun = $('last-strategy-run');
  if (lastRun && health?.last_strategy_run) {
    const d = new Date(health.last_strategy_run);
    lastRun.textContent = `마지막 전략: ${d.toLocaleString('ko-KR')}`;
  }
}

function setLastUpdateTime() {
  const el = $('last-update');
  if (el) el.textContent = '갱신: ' + new Date().toLocaleTimeString('ko-KR');
}

// ── 포트폴리오 ──────────────────────────────────────────────────
function renderPortfolio(portfolio) {
  if (!portfolio) return;
  const total = portfolio.total_krw_equiv;

  const totalEl = $('portfolio-total');
  if (totalEl) totalEl.textContent = fmtKRW(total);

  const exRows = $('exchange-rows');
  if (!exRows) return;
  exRows.innerHTML = '';

  for (const [name, ex] of Object.entries(portfolio.exchanges || {})) {
    if (!ex.enabled) continue;
    const val = ex.quote_currency === 'USDT'
      ? fmtUSDT(ex.total_equity)
      : fmtKRW(ex.total_equity);
    exRows.insertAdjacentHTML('beforeend', `
      <div class="exchange-row">
        <div>
          <span class="exchange-name">${name}</span>
          <span class="badge ${ex.paper_trading ? 'badge-paper' : 'badge-live'}" style="margin-left:6px">
            ${ex.paper_trading ? '페이퍼' : '실전'}
          </span>
        </div>
        <div>
          <span class="exchange-value">${val}</span>
        </div>
      </div>
    `);
  }
}

// ── 리스크 패널 ─────────────────────────────────────────────────
function renderRisk(risk) {
  if (!risk) return;
  const container = $('risk-rows');
  if (!container) return;
  container.innerHTML = '';

  for (const [exName, r] of Object.entries(risk)) {
    if (r.error) continue;
    const cur     = r.quote_currency || 'KRW';
    const equity  = cur === 'USDT' ? fmtUSDT(r.current_equity) : fmtKRW(r.current_equity);
    const limit   = r.daily_loss_limit_pct * 100;  // -20 etc.

    // SL/TP 가격 칩
    let priceChips = '';
    for (const [mkt, slPrice] of Object.entries(r.stop_loss_prices || {})) {
      if (slPrice > 0) {
        priceChips += `<span class="risk-price-chip chip-sl">
          <span class="chip-label">SL</span> ${mkt.split('-')[1]}: ${fmtNum(slPrice,0)}
        </span>`;
      }
    }
    for (const [mkt, tpPrice] of Object.entries(r.take_profit_prices || {})) {
      if (tpPrice > 0) {
        priceChips += `<span class="risk-price-chip chip-tp">
          <span class="chip-label">TP</span> ${mkt.split('-')[1]}: ${fmtNum(tpPrice,0)}
        </span>`;
      }
    }

    container.insertAdjacentHTML('beforeend', `
      <div class="risk-row">
        <div class="risk-row-header">
          <span class="risk-ex-name">${exName}</span>
          <span class="risk-equity">${equity}</span>
          <span class="badge badge-neutral" style="font-size:10px">
            SL ${(r.stop_loss_pct*100).toFixed(0)}%
          </span>
          <span class="badge badge-neutral" style="font-size:10px">
            TP +${(r.take_profit_pct*100).toFixed(0)}%
          </span>
          <span class="badge badge-neutral" style="font-size:10px; color:#ffa726">
            한도 ${limit.toFixed(0)}%
          </span>
        </div>
        ${priceChips ? `<div class="risk-prices">${priceChips}</div>` : ''}
      </div>
    `);
  }
}

// ── 종목 현황 그리드 (20종목) ────────────────────────────────────
function renderSignals(signals) {
  if (!signals) return;
  const container = $('market-grid');
  if (!container) return;
  container.innerHTML = '';

  // OKX 기준 모든 종목 수집 (신호 있는 것만)
  const rows = [];
  for (const [exName, markets] of Object.entries(signals)) {
    if (exName.toLowerCase() !== 'okx') continue;
    for (const [mkt, s] of Object.entries(markets)) {
      rows.push({ exName, mkt, s });
    }
  }

  // 신호 있는 종목 먼저, 그 다음 알파벳 순
  rows.sort((a, b) => {
    const aHas = a.s.signal !== 0 ? -1 : 0;
    const bHas = b.s.signal !== 0 ? -1 : 0;
    if (aHas !== bHas) return aHas - bHas;
    return a.mkt.localeCompare(b.mkt);
  });

  const countEl = $('market-count-badge');
  if (countEl) countEl.textContent = `${rows.length}종목`;

  // 차트 탭도 갱신
  refreshChartTabs(rows.map(r => r.mkt));

  rows.forEach(({ mkt, s }) => {
    const coin  = mkt.split('-')[1];
    const sl    = SIGNAL_LABEL[String(s.signal)] || SIGNAL_LABEL['0'];
    const isUp  = s.trend === 'uptrend';
    const hasSig = s.signal !== 0;
    const volSurge = s.vol_surge;

    const trendHtml = isUp
      ? `<span class="mini-badge trend-up">▲ 상승</span>`
      : `<span class="mini-badge trend-dn">▼ 하락</span>`;
    const volHtml = volSurge
      ? `<span class="mini-badge vol-surge">⚡Vol</span>` : '';
    const sigHtml = hasSig
      ? `<span class="mini-badge ${sl.cls}">${sl.text}</span>` : '';
    const ma200Html = s.ma200_gap != null
      ? `<span class="mkt-gap ${s.ma200_gap >= 0 ? 'gap-pos' : 'gap-neg'}">MA200 ${s.ma200_gap >= 0 ? '+' : ''}${Number(s.ma200_gap).toFixed(1)}%</span>` : '';

    container.insertAdjacentHTML('beforeend', `
      <div class="market-cell ${hasSig ? 'market-cell-active' : ''}"
           onclick="selectChartCoin('${mkt}')" style="cursor:pointer">
        <div class="mkt-top">
          <span class="mkt-name">${coin}</span>
          ${trendHtml}${volHtml}
        </div>
        <div class="mkt-bottom">
          ${ma200Html}
          ${sigHtml || `<span class="mini-badge badge-neutral">대기</span>`}
        </div>
      </div>
    `);
  });

  // Upbit 신호도 포지션 카드 옆 별도 표시 (없으면 skip)
  const upbitSignals = signals['upbit'];
  if (upbitSignals) {
    const upbitWrap = $('upbit-signal-row');
    if (!upbitWrap) return;
    upbitWrap.innerHTML = '';
    for (const [mkt, s] of Object.entries(upbitSignals)) {
      const coin = mkt.split('-')[1];
      const sl = SIGNAL_LABEL[String(s.signal)] || SIGNAL_LABEL['0'];
      const isUp = s.trend === 'uptrend';
      upbitWrap.insertAdjacentHTML('beforeend', `
        <span class="upbit-sig-item">
          <b>${coin}</b>
          <span class="mini-badge ${isUp ? 'trend-up' : 'trend-dn'}">${isUp ? '▲' : '▼'}</span>
          <span class="mini-badge ${sl.cls}">${sl.text}</span>
        </span>
      `);
    }
  }
}

// 차트 탭 동적 갱신
function refreshChartTabs(markets) {
  const tabsEl = $('chart-tabs');
  if (!tabsEl || tabsEl.dataset.initialized === '1') return;
  tabsEl.dataset.initialized = '1';
  tabsEl.innerHTML = '';

  const allMarkets = markets.length > 0 ? markets
    : ['KRW-BTC','KRW-ETH','KRW-SOL','KRW-XRP'];

  allMarkets.forEach((mkt, i) => {
    const coin = mkt.split('-')[1];
    const btn = document.createElement('button');
    btn.className = 'chart-tab' + (i === 0 ? ' active' : '');
    btn.dataset.coin = mkt;
    btn.textContent = coin;
    btn.addEventListener('click', () => {
      document.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      _state.selectedCoin = mkt;
      refreshCharts();
    });
    tabsEl.appendChild(btn);
  });

  if (allMarkets.length > 0 && !allMarkets.includes(_state.selectedCoin)) {
    _state.selectedCoin = allMarkets[0];
  }
}

// 종목 클릭 → 차트 탭 이동
window.selectChartCoin = function(mkt) {
  const tab = document.querySelector(`.chart-tab[data-coin="${mkt}"]`);
  if (tab) {
    tab.click();
    $('chart-tabs')?.closest('.card')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  } else {
    _state.selectedCoin = mkt;
    refreshCharts();
  }
};

// ── 포지션 카드 (활성 포지션만 표시) ───────────────────────────
function renderPositionCards(portfolio) {
  if (!portfolio) return;
  const container = $('position-cards');
  if (!container) return;
  container.innerHTML = '';

  let activeCount = 0;

  for (const [exName, ex] of Object.entries(portfolio.exchanges || {})) {
    if (!ex.enabled) continue;
    const cur = ex.quote_currency;

    // 현물 포지션 — held인 것만
    for (const [mkt, pos] of Object.entries(ex.positions || {})) {
      if (!pos.held) continue;
      activeCount++;
      const coin     = mkt.split('-')[1];
      const pnlPct   = pos.unrealized_pnl_pct;
      const priceDisplay = fmtPrice(pos.current_price, cur);
      const entryDisplay = fmtPrice(pos.entry_price, cur);
      const pnlDisplay   = `<span class="${colorClass(pnlPct)}">${fmtPct(pnlPct)}</span>`;

      container.insertAdjacentHTML('beforeend', `
        <div class="position-card active">
          <div class="pos-header">
            <span class="pos-ex">${exName}</span>
            <span class="pos-mkt">${coin}</span>
            <span class="pos-type"><span class="badge badge-bull">롱</span></span>
          </div>
          <div class="pos-body">
            <div class="pos-row"><span class="pos-label">진입가</span><span class="pos-val">${entryDisplay}</span></div>
            <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${priceDisplay}</span></div>
            <div class="pos-row"><span class="pos-label">수량</span><span class="pos-val">${Number(pos.volume).toFixed(6)}</span></div>
            <div class="pos-row"><span class="pos-label">미실현</span><span class="pos-val">${pnlDisplay}</span></div>
          </div>
        </div>
      `);
    }

    // OKX 선물 포지션 — held인 것만
    for (const [mkt, fut] of Object.entries(ex.futures || {})) {
      const side = fut.side;
      const held = side !== null && fut.volume > 0;
      if (!held) continue;
      activeCount++;
      const coin   = mkt.split('-')[1];
      const pnlPct = fut.unrealized_pnl_pct;
      const cardCls = side === 'short' ? 'active-short' : 'active';
      const sideBadge = side === 'short'
        ? '<span class="badge badge-bear">숏</span>'
        : '<span class="badge badge-bull">롱</span>';

      container.insertAdjacentHTML('beforeend', `
        <div class="position-card ${cardCls}">
          <div class="pos-header">
            <span class="pos-ex">${exName} 선물</span>
            <span class="pos-mkt">${coin}</span>
            <span class="pos-type">${sideBadge}</span>
          </div>
          <div class="pos-body">
            <div class="pos-row"><span class="pos-label">진입가</span><span class="pos-val">${fmtNum(fut.entry_price,2)}</span></div>
            <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${fmtNum(fut.current_price,2)}</span></div>
            <div class="pos-row"><span class="pos-label">수량</span><span class="pos-val">${Number(fut.volume).toFixed(4)}</span></div>
            <div class="pos-row"><span class="pos-label">미실현</span>
              <span class="pos-val ${colorClass(pnlPct)}">${fmtPct(pnlPct)}</span></div>
          </div>
        </div>
      `);
    }
  }

  if (activeCount === 0) {
    container.innerHTML = '<div class="pos-empty-state">현재 보유 포지션이 없습니다</div>';
  }
}

// ── 차트 탭 초기화 (정적 탭 → 이벤트 연결만) ───────────────────
function initChartTabs() {
  document.querySelectorAll('.chart-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      _state.selectedCoin = tab.dataset.coin;
      refreshCharts();
    });
  });
}

// Upbit은 BTC/ETH만 지원 (KRW 현물)
const UPBIT_MARKETS = ['KRW-BTC', 'KRW-ETH'];

async function refreshCharts() {
  const coin     = _state.selectedCoin;
  const trades   = _state.trades;
  const upbitSupported = UPBIT_MARKETS.includes(coin);

  // Upbit 차트: SOL/XRP는 숨기고 OKX만 전체 너비로
  const upbitPanel = $('chart-upbit');
  const chartGrid  = upbitPanel?.closest('.chart-grid');
  if (upbitPanel) {
    upbitPanel.style.display = upbitSupported ? '' : 'none';
  }
  if (chartGrid) {
    chartGrid.style.gridTemplateColumns = upbitSupported ? '' : '1fr';
  }

  // Upbit 차트
  if (upbitSupported) {
    try {
      const upbitData = await api.candles('upbit', coin);
      updateChart('chart-upbit-body', `upbit_${coin}`, upbitData,
        trades.filter(t => t.exchange === 'upbit' && t.market === coin), false);
      updateChartHeader('chart-upbit', 'upbit', coin, upbitData, 'KRW');
    } catch (e) { console.error('Upbit 차트 오류:', e); }
  }

  // OKX 선물 차트
  try {
    $('chart-okx')?.classList.remove('hidden');
    const okxData = await api.candles('okx', coin);
    updateChart('chart-okx-body', `okx_${coin}`, okxData,
      trades.filter(t => t.exchange === 'okx' && t.market === coin), true);
    updateChartHeader('chart-okx', 'okx', coin, okxData, 'USDT');
  } catch (e) { console.error('OKX 차트 오류:', e); }
}

function updateChartHeader(panelId, exName, market, candleData, cur) {
  const panel   = $(panelId);
  if (!panel) return;
  const candles = candleData?.candles;
  if (!candles || candles.length === 0) return;

  const last    = candles[candles.length - 1];
  const prev    = candles.length > 1 ? candles[candles.length - 2] : null;
  const change  = prev ? ((last.close - prev.close) / prev.close * 100) : 0;

  const priceEl = panel.querySelector('.chart-price');
  const pnlEl   = panel.querySelector('.chart-pnl');
  if (priceEl) priceEl.textContent = fmtPrice(last.close, cur);
  if (pnlEl) {
    pnlEl.textContent  = fmtPct(change);
    pnlEl.className    = `chart-pnl ${change >= 0 ? 'pnl-positive' : 'pnl-negative'}`;
  }
}

// ── 자산 곡선 ────────────────────────────────────────────────────
async function refreshEquityCharts() {
  const exchanges = ['upbit', 'okx'];
  for (const ex of exchanges) {
    try {
      const data = await api.equityHistory(ex);
      if (data && data.length > 0) {
        updateEquityChart(`equity-${ex}-body`, `equity_${ex}`, data);
      }
    } catch (e) { /* silent */ }
  }
}

// ── 거래 이력 테이블 ─────────────────────────────────────────────
function renderTradeTable(tradesData) {
  const tbody = $('trades-tbody');
  if (!tbody) return;

  if (!tradesData || tradesData.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-state">거래 이력 없음</td></tr>';
    return;
  }

  const rowClass = {
    long_entry:        'row-buy',
    long_close:        'row-sell',
    stop_loss:         'row-sl',
    take_profit:       'row-tp',
    short_entry:       'row-short',
    short_stop_loss:   'row-sl',
    short_take_profit: 'row-tp',
  };

  tbody.innerHTML = tradesData.map(t => {
    const d       = new Date(t.timestamp);
    const timeStr = d.toLocaleString('ko-KR', {month:'2-digit',day:'2-digit',
                      hour:'2-digit',minute:'2-digit'});
    const pnlHtml = t.pnl_pct != null
      ? `<span class="${t.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg'}">${fmtPct(t.pnl_pct)}</span>`
      : '—';
    const amtStr  = t.amount != null
      ? `${fmtNum(t.amount,0)} ${t.currency || ''}` : '—';
    const typeKr  = TYPE_KR[t.type] || t.type;

    return `<tr class="${rowClass[t.type] || ''}">
      <td>${timeStr}</td>
      <td><span style="text-transform:uppercase;font-weight:600">${t.exchange}</span></td>
      <td>${t.market}</td>
      <td><span class="type-chip type-${t.type}">${typeKr}</span></td>
      <td class="right">${amtStr}</td>
      <td class="right">${pnlHtml}</td>
    </tr>`;
  }).join('');
}

// ── 필터 ────────────────────────────────────────────────────────
function initTradeFilters() {
  const filterEx  = $('filter-exchange');
  const filterMkt = $('filter-market');

  async function applyFilter() {
    const ex  = filterEx?.value  || '';
    const mkt = filterMkt?.value || '';
    try {
      const result = await api.trades(50, ex, mkt);
      _state.trades = result.trades;
      renderTradeTable(result.trades);
    } catch(e) { console.error(e); }
  }

  filterEx?.addEventListener('change',  applyFilter);
  filterMkt?.addEventListener('change', applyFilter);
}

// ── 새로고침 버튼 ────────────────────────────────────────────────
function initRefreshBtn() {
  const btn = $('btn-refresh');
  btn?.addEventListener('click', () => {
    if (!_state.isRefreshing) refreshAll(true);
  });
}

function setRefreshLoading(loading) {
  const btn = $('btn-refresh');
  if (!btn) return;
  btn.classList.toggle('loading', loading);
}

// ── 메인 갱신 루프 ───────────────────────────────────────────────
async function refreshAll(force = false) {
  if (_state.isRefreshing) return;
  _state.isRefreshing = true;
  setRefreshLoading(true);

  try {
    // 항상 갱신
    const [health, portfolio, risk, signals, tradesResult] = await Promise.all([
      api.health(),
      api.portfolio(),
      api.risk(),
      api.signals(),
      api.trades(50),
    ]);

    _state.health    = health;
    _state.portfolio = portfolio;
    _state.risk      = risk;
    _state.signals   = signals;
    _state.trades    = tradesResult.trades;

    renderHeader(health);
    renderPortfolio(portfolio);
    renderRisk(risk);
    renderSignals(signals);
    renderPositionCards(portfolio);
    renderTradeTable(tradesResult.trades);
    setLastUpdateTime();

    // 차트는 첫 로드 또는 5 사이클마다
    if (force || _state.refreshCount === 0 || _state.refreshCount % CHART_EVERY === 0) {
      await refreshCharts();
    }

    // 자산 곡선은 10 사이클마다
    if (force || _state.refreshCount === 0 || _state.refreshCount % EQUITY_EVERY === 0) {
      await refreshEquityCharts();
    }

    _state.refreshCount++;
  } catch (e) {
    console.error('갱신 오류:', e);
  } finally {
    _state.isRefreshing = false;
    setRefreshLoading(false);
  }
}

// ── 초기화 ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initChartTabs();
  initRefreshBtn();
  initTradeFilters();

  refreshAll(true);
  setInterval(() => refreshAll(), REFRESH_MS);
});
