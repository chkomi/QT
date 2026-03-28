/**
 * Quant Bot Dashboard v2 — OKX-focused
 */
import { api } from './api.js';
import { updateCandleChart, updateEquityChart } from './charts.js';

// State
let _coin = 'KRW-BTC';
let _cycle = 0;
let _tradesExpanded = false;

// ── Boot ──────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Tab clicks
  document.getElementById('tabs').addEventListener('click', e => {
    const btn = e.target.closest('.tab');
    if (!btn) return;
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    _coin = btn.dataset.c;
    refreshChart();
  });

  // Refresh button
  document.getElementById('btn-refresh').addEventListener('click', () => refreshAll(true));

  // Trades expand
  document.getElementById('btn-more').addEventListener('click', () => {
    _tradesExpanded = !_tradesExpanded;
    document.getElementById('btn-more').textContent = _tradesExpanded ? 'less' : 'more';
    refreshTrades();
  });

  // Log toggle
  document.getElementById('log-toggle').addEventListener('click', () => {
    document.getElementById('sec-log').classList.toggle('collapsed');
    const caret = document.querySelector('#log-toggle .caret');
    caret.innerHTML = document.getElementById('sec-log').classList.contains('collapsed') ? '&#9654;' : '&#9660;';
  });

  refreshAll(true);
  setInterval(() => refreshAll(false), 15000);  // 15s core data
  setInterval(refreshLogs, 10000);              // 10s logs
});

// ── Refresh All ───────────────────────────────────────
async function refreshAll(force = false) {
  _cycle++;
  try {
    const [pos, health] = await Promise.all([
      api.positions(),
      api.health(),
    ]);
    renderHero(pos, health);
    renderPositions(pos);

    if (force || _cycle % 4 === 0) refreshChart();
    if (force || _cycle % 6 === 0) refreshEquity();
    if (force || _cycle % 2 === 0) refreshTrades();
  } catch (e) {
    console.error('refresh error:', e);
  }
}

// ── Hero ──────────────────────────────────────────────
function renderHero(pos, health) {
  const amt = pos.total_equity_usdt || 0;
  document.getElementById('hero-amount').textContent = `$${amt.toLocaleString('en', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  const upbit = pos.upbit_equity_krw || 0;
  document.getElementById('hero-upbit').textContent = `Upbit ₩${Math.round(upbit).toLocaleString()}`;

  // Bot status
  const badge = document.getElementById('hero-bot');
  if (health && health.bot_running) {
    badge.className = 'badge badge-on';
    badge.textContent = 'LIVE';
  } else {
    badge.className = 'badge badge-off';
    badge.textContent = 'OFF';
  }

  document.getElementById('hero-time').textContent = new Date().toLocaleTimeString('ko', { hour: '2-digit', minute: '2-digit' });
}

// ── Positions ─────────────────────────────────────────
function renderPositions(pos) {
  document.getElementById('pos-count').textContent = `${pos.open_count}/${pos.max_positions}`;

  const grid = document.getElementById('pos-grid');
  if (!pos.positions || pos.positions.length === 0) {
    grid.innerHTML = '<div class="empty">No open positions</div>';
    return;
  }

  grid.innerHTML = pos.positions.map(p => {
    const isLong = p.direction === 'long';
    const dir = isLong ? 'LONG' : 'SHORT';
    const dirClass = isLong ? 'long' : 'short';
    const pnlClass = p.unrealized_pnl_usdt >= 0 ? 'profit' : 'loss';
    const pnlSign = p.unrealized_pnl_usdt >= 0 ? '+' : '';
    const coin = p.market.replace('KRW-', '');
    const progress = Math.round(p.sl_tp_progress * 100);
    const holdPct = p.max_holding_hours > 0 ? Math.min(100, Math.round(p.holding_hours / p.max_holding_hours * 100)) : 0;

    return `
      <div class="pos-card ${dirClass}">
        <div class="pos-header">
          <span class="pos-coin">${coin}</span>
          <span class="pos-dir ${dirClass}">${dir}</span>
          <span class="pos-tier">${p.tier}</span>
          <span class="pos-lev">${p.leverage}x</span>
          <span class="pos-conf">C${p.confluence_score}</span>
        </div>
        <div class="pos-prices">
          <span>Entry ${formatPrice(p.entry_price)}</span>
          <span>Now ${formatPrice(p.current_price)}</span>
        </div>
        <div class="pos-pnl ${pnlClass}">
          ${pnlSign}$${Math.abs(p.unrealized_pnl_usdt).toFixed(2)}
          (${pnlSign}${p.unrealized_pnl_pct.toFixed(2)}%)
        </div>
        <div class="pos-bar-row">
          <span class="bar-label">SL</span>
          <div class="pos-bar">
            <div class="pos-bar-fill ${dirClass}" style="width:${progress}%"></div>
            <div class="pos-bar-thumb" style="left:${progress}%"></div>
          </div>
          <span class="bar-label">TP</span>
        </div>
        <div class="pos-meta">
          <span>${p.holding_hours.toFixed(1)}h${p.max_holding_hours > 0 ? ' / ' + p.max_holding_hours + 'h' : ''}</span>
          <span>$${p.invest_usdt.toFixed(0)}</span>
        </div>
      </div>`;
  }).join('');
}

function formatPrice(p) {
  if (!p) return '-';
  if (p >= 1000) return p.toLocaleString('en', { maximumFractionDigits: 0 });
  if (p >= 1) return p.toFixed(2);
  return p.toFixed(4);
}

// ── Chart ─────────────────────────────────────────────
async function refreshChart() {
  try {
    const data = await api.candles(_coin, 'day', 120);
    updateCandleChart(document.getElementById('candle-chart'), data);
  } catch (e) {
    console.error('chart error:', e);
  }
}

// ── Equity ────────────────────────────────────────────
async function refreshEquity() {
  try {
    const data = await api.equity('okx');
    updateEquityChart(document.getElementById('equity-chart'), data);
  } catch (e) {
    console.error('equity error:', e);
  }
}

// ── Trades ────────────────────────────────────────────
async function refreshTrades() {
  try {
    const limit = _tradesExpanded ? 50 : 10;
    const { trades } = await api.trades(limit);
    const el = document.getElementById('trades-list');

    if (!trades || trades.length === 0) {
      el.innerHTML = '<div class="empty">No trades yet</div>';
      return;
    }

    el.innerHTML = trades.map(t => {
      const pnl = t.pnl_pct != null ? `${t.pnl_pct >= 0 ? '+' : ''}${t.pnl_pct.toFixed(2)}%` : '';
      const cls = t.type === 'buy' || t.type === 'long_entry' ? 'trade-buy' :
                  t.type === 'stop_loss' ? 'trade-sl' :
                  t.type === 'take_profit' ? 'trade-tp' : 'trade-sell';
      const coin = (t.market || '').replace('KRW-', '');
      const time = (t.timestamp || '').slice(5, 16).replace('T', ' ');
      const tier = t.tier || '';
      const side = t.side || t.type || '';

      return `<div class="trade-row ${cls}">
        <span class="t-time">${time}</span>
        <span class="t-coin">${coin}</span>
        <span class="t-side">${side}</span>
        <span class="t-tier">${tier}</span>
        <span class="t-pnl ${pnl.startsWith('+') ? 'profit' : pnl.startsWith('-') ? 'loss' : ''}">${pnl}</span>
      </div>`;
    }).join('');
  } catch (e) {
    console.error('trades error:', e);
  }
}

// ── Logs ──────────────────────────────────────────────
async function refreshLogs() {
  if (document.getElementById('sec-log').classList.contains('collapsed')) return;
  try {
    const { logs } = await api.logs(100);
    const el = document.getElementById('log-box');
    // Only show WARNING + ERROR by default
    const filtered = logs.filter(l => l.level === 'WARNING' || l.level === 'ERROR' || l.msg.includes('[v2]'));
    el.innerHTML = filtered.slice(-50).map(l => {
      const cls = l.level === 'ERROR' ? 'log-err' : l.level === 'WARNING' ? 'log-warn' : 'log-info';
      return `<div class="${cls}">${l.ts.slice(11, 19)} ${l.msg}</div>`;
    }).join('');
    el.scrollTop = el.scrollHeight;
  } catch (e) { /* silent */ }
}
