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

// ── 신호 행 ─────────────────────────────────────────────────────
function renderSignals(signals) {
  if (!signals) return;
  const container = $('signal-rows');
  if (!container) return;
  container.innerHTML = '';

  for (const [exName, markets] of Object.entries(signals)) {
    for (const [mkt, s] of Object.entries(markets)) {
      const coin = mkt.split('-')[1];
      const sl   = SIGNAL_LABEL[String(s.signal)] || SIGNAL_LABEL['0'];
      const trend = s.trend === 'uptrend'
        ? '<span class="badge badge-bull">▲ 상승</span>'
        : '<span class="badge badge-bear">▼ 하락</span>';
      const volChip = s.vol_surge
        ? '<span class="badge badge-signal" style="font-size:9px">Vol급증</span>' : '';

      container.insertAdjacentHTML('beforeend', `
        <div class="signal-item">
          <div>
            <div><span class="signal-market">${coin}</span>
              <span class="signal-ex" style="margin-left:4px">${exName}</span></div>
            <div style="margin-top:3px">${trend} ${volChip}</div>
          </div>
          <div style="margin-left:auto;text-align:right">
            <div><span class="badge ${sl.cls}">${sl.text}</span></div>
            <div class="signal-vp" style="margin-top:3px">
              POC ${fmtNum(s.vp_poc,0)}
            </div>
          </div>
        </div>
      `);
    }
  }
}

// ── 포지션 카드 ─────────────────────────────────────────────────
function renderPositionCards(portfolio) {
  if (!portfolio) return;
  const container = $('position-cards');
  if (!container) return;
  container.innerHTML = '';

  for (const [exName, ex] of Object.entries(portfolio.exchanges || {})) {
    if (!ex.enabled) continue;
    const cur = ex.quote_currency;

    // 현물 포지션
    for (const [mkt, pos] of Object.entries(ex.positions || {})) {
      const coin     = mkt.split('-')[1];
      const held     = pos.held;
      const pnlPct   = pos.unrealized_pnl_pct;
      const pnlKey   = `unrealized_pnl_${cur.toLowerCase()}`;
      const pnlQuote = pos[pnlKey] || 0;

      let priceDisplay = fmtPrice(pos.current_price, cur);
      let entryDisplay = held ? fmtPrice(pos.entry_price, cur) : '—';
      let pnlDisplay   = held ? `<span class="${colorClass(pnlPct)}">${fmtPct(pnlPct)}</span>` : '—';

      container.insertAdjacentHTML('beforeend', `
        <div class="position-card ${held ? 'active' : 'inactive'}">
          <div class="pos-header">
            <span class="pos-ex">${exName}</span>
            <span class="pos-mkt">${coin}</span>
            <span class="pos-type">
              ${held ? '<span class="badge badge-bull">롱</span>' : '<span class="badge badge-neutral">대기</span>'}
            </span>
          </div>
          <div class="pos-body">
            ${held ? `
              <div class="pos-row"><span class="pos-label">진입가</span><span class="pos-val">${entryDisplay}</span></div>
              <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${priceDisplay}</span></div>
              <div class="pos-row"><span class="pos-label">수량</span><span class="pos-val">${Number(pos.volume).toFixed(6)}</span></div>
              <div class="pos-row"><span class="pos-label">미실현</span><span class="pos-val">${pnlDisplay}</span></div>
            ` : `
              <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${priceDisplay}</span></div>
              <div class="pos-no-position">포지션 없음</div>
            `}
          </div>
        </div>
      `);
    }

    // OKX 선물 포지션
    for (const [mkt, fut] of Object.entries(ex.futures || {})) {
      const coin  = mkt.split('-')[1];
      const side  = fut.side;
      const held  = side !== null && fut.volume > 0;
      const pnlPct = fut.unrealized_pnl_pct;

      container.insertAdjacentHTML('beforeend', `
        <div class="position-card ${held && side==='short' ? 'active-short' : held ? 'active' : 'inactive'}">
          <div class="pos-header">
            <span class="pos-ex">${exName} 선물</span>
            <span class="pos-mkt">${coin}</span>
            <span class="pos-type">
              ${held && side==='short' ? '<span class="badge badge-bear">숏</span>'
              : held && side==='long'  ? '<span class="badge badge-bull">롱</span>'
              : '<span class="badge badge-neutral">대기</span>'}
            </span>
          </div>
          <div class="pos-body">
            ${held ? `
              <div class="pos-row"><span class="pos-label">진입가</span><span class="pos-val">${fmtNum(fut.entry_price,2)}</span></div>
              <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${fmtNum(fut.current_price,2)}</span></div>
              <div class="pos-row"><span class="pos-label">수량</span><span class="pos-val">${Number(fut.volume).toFixed(4)}</span></div>
              <div class="pos-row"><span class="pos-label">미실현</span>
                <span class="pos-val ${colorClass(pnlPct)}">${fmtPct(pnlPct)}</span></div>
            ` : `
              <div class="pos-row"><span class="pos-label">현재가</span><span class="pos-val">${fmtNum(fut.current_price,2)}</span></div>
              <div class="pos-no-position">포지션 없음</div>
            `}
          </div>
        </div>
      `);
    }
  }
}

// ── 차트 탭 ─────────────────────────────────────────────────────
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

async function refreshCharts() {
  const coin     = _state.selectedCoin;
  const trades   = _state.trades;

  // Upbit 차트
  try {
    $('chart-upbit')?.classList.remove('hidden');
    const upbitData = await api.candles('upbit', coin);
    updateChart('chart-upbit-body', `upbit_${coin}`, upbitData,
      trades.filter(t => t.exchange === 'upbit' && t.market === coin), false);
    updateChartHeader('chart-upbit', 'upbit', coin, upbitData, 'KRW');
  } catch (e) { console.error('Upbit 차트 오류:', e); }

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
