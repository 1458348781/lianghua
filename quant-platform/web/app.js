const state = {
  strategies: [],
  localSymbols: [],
  selected: [],
  result: null,
  signals: [],
  candidatePool: null,
  watchlist: [],
  positions: [],
  closedPositions: [],
  watchTimer: null,
  klineTimer: null,
  positionTimer: null,
};

const $ = (id) => document.getElementById(id);

const DIVERGENCE_PRESETS = {
  stable: {
    label: "稳健版：最差年份更强",
    params: {
      max_positions: 2,
      hold_days: 2,
      stop_loss: -0.025,
      min_price: 3,
      max_price: 500,
      min_turnover: 3,
      max_turnover: 24.4,
      day1_min_volume_ratio: 0.85,
      day1_max_volume_ratio: 5.59,
      range_min_amplitude_30: 0.108,
      range_min_return_20: 0.039,
      day2_min_pct_chg: -1.6,
      day2_max_pct_chg: 8.4,
      day2_max_volume_ratio: 2.15,
      day2_min_close_position: 0.51,
      day2_max_upper_shadow: 0.075,
      day2_min_close_vs_day1_close: 0.954,
      entry_min_open_gap_pct_chg: 1.2,
      entry_max_open_gap_pct_chg: 6.3,
      entry_min_high_from_open_pct_chg: 2.7,
    },
  },
  aggressive: {
    label: "进攻版：综合评分第一",
    params: {
      max_positions: 2,
      hold_days: 2,
      stop_loss: -0.025,
      min_price: 3,
      max_price: 500,
      min_turnover: 3,
      max_turnover: 24.4,
      day1_min_volume_ratio: 0.85,
      day1_max_volume_ratio: 5.59,
      range_min_amplitude_30: 0.108,
      range_min_return_20: 0.039,
      day2_min_pct_chg: -1.6,
      day2_max_pct_chg: 8.4,
      day2_max_volume_ratio: 2.15,
      day2_min_close_position: 0.51,
      day2_max_upper_shadow: 0.075,
      day2_min_close_vs_day1_close: 0.954,
      entry_min_open_gap_pct_chg: 1.2,
      entry_max_open_gap_pct_chg: 6.3,
      entry_min_high_from_open_pct_chg: 2.7,
    },
  },
};

const MARKET_UNIVERSE_LABELS = {
  all: "全部本地股票",
  chinext: "创业板",
  star: "科创板",
  main: "主板及其他",
  selected: "已选股票",
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "请求失败");
  return data;
}

const debounce = (fn, wait = 260) => {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), wait);
  };
};

function formatPercent(value) {
  return `${(Number(value || 0) * 100).toFixed(2)}%`;
}

function formatPercentNumber(value) {
  if (value === undefined || value === null || value === "") return "";
  return `${Number(value || 0).toFixed(2)}%`;
}

function formatProbability(value) {
  if (value === undefined || value === null || value === "") return "";
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function formatMoney(value) {
  return Number(value || 0).toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

async function init() {
  applyDefaultDates();
  bindEvents();
  $("klinePeriod").value = "minute";
  $("watchSummary").textContent = "使用当前分歧战法参数，支持按全部、创业板、科创板、主板及其他分别盯盘。";
  await Promise.all([loadHealth(), loadStrategies()]);
  await loadCandidatePool();
  await loadSymbols();
  await searchStocks("");
  refreshPositions();
  startKlineAutoRefresh();
  startPositionAutoRefresh();
  toggleWatchAutoRefresh();
}

function applyDefaultDates() {
  const end = new Date();
  const start = new Date(end);
  start.setMonth(start.getMonth() - 1);
  $("startDate").value = formatDateInput(start);
  $("endDate").value = formatDateInput(end);
}

function formatDateInput(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function bindEvents() {
  $("refreshSymbols").addEventListener("click", async () => {
    await loadSymbols();
    await searchStocks($("stockSearch").value);
  });
  $("stockSearch").addEventListener("input", debounce((event) => searchStocks(event.target.value)));
  $("clearSelected").addEventListener("click", () => {
    state.selected = [];
    renderSelected();
  });
  $("strategyName").addEventListener("change", renderParams);
  $("divergencePreset").addEventListener("change", applyDivergencePreset);
  $("runBtn").addEventListener("click", () => runBacktest(false));
  $("runAllBtn").addEventListener("click", () => runBacktest(true));
  $("scanBtn").addEventListener("click", scanSignals);
  $("watchRefresh").addEventListener("click", refreshWatchlist);
  $("watchAutoRefresh").addEventListener("change", toggleWatchAutoRefresh);
  $("watchInterval").addEventListener("change", () => {
    if ($("watchAutoRefresh").checked) {
      toggleWatchAutoRefresh();
    }
  });
  $("positionRefresh").addEventListener("click", refreshPositions);
  $("positionAdd").addEventListener("click", addPosition);
  $("klineSymbol").addEventListener("change", renderSelectedKline);
  $("klinePeriod").addEventListener("change", renderSelectedKline);
  $("startDate").addEventListener("change", renderSelectedKline);
  $("endDate").addEventListener("change", renderSelectedKline);
  document.querySelectorAll("nav a").forEach((link) => {
    link.addEventListener("click", () => {
      document.querySelectorAll("nav a").forEach((item) => item.classList.remove("active"));
      link.classList.add("active");
    });
  });
}

function startKlineAutoRefresh() {
  if (state.klineTimer) clearInterval(state.klineTimer);
  state.klineTimer = setInterval(() => {
    if (($("klinePeriod").value || "minute") !== "minute") return;
    if (!$("klineSymbol").value && !state.selected[0]?.symbol) return;
    renderSelectedKline();
  }, 5000);
}

function startPositionAutoRefresh() {
  if (state.positionTimer) clearInterval(state.positionTimer);
  state.positionTimer = setInterval(() => refreshPositions({ silent: true }), 1000);
}

async function loadHealth() {
  try {
    const health = await api("/api/health");
    $("health").textContent = health.ok ? "服务正常" : "服务异常";
  } catch {
    $("health").textContent = "服务异常";
  }
}

async function loadStrategies() {
  const data = await api("/api/strategies");
  state.strategies = data.strategies;
  $("strategyName").innerHTML = data.strategies
    .map((strategy) => `<option value="${strategy.name}">${strategy.label}</option>`)
    .join("");
  $("strategyName").value = data.strategies.some((item) => item.name === "divergence_tactic")
    ? "divergence_tactic"
    : data.strategies.some((item) => item.name === "divergence_flow")
      ? "divergence_flow"
    : data.strategies[0]?.name;
  renderDivergencePresets();
  renderParams();
}

async function loadCandidatePool() {
  try {
    const data = await api("/api/candidate-pool");
    state.candidatePool = data.pool || null;
    const candidates = data.candidates || [];
    if (!state.selected.length && candidates.length) {
      const first = candidates[0];
      addStock({ symbol: first.symbol, name: first.name || first.symbol });
      $("klineSymbol").value = first.symbol;
      $("klinePeriod").value = "minute";
      renderSelectedKline();
    }
  } catch (error) {
    state.candidatePool = null;
    console.warn("candidate pool load failed", error);
  }
}

async function loadSymbols() {
  const data = await api("/api/symbols");
  state.localSymbols = data.symbols;
  $("localCount").textContent = `${data.symbols.length} 只`;
  const symbolRows = $("symbolRows");
  if (symbolRows) {
    symbolRows.innerHTML =
      data.symbols
        .slice(0, 300)
        .map(
          (item) => `
            <tr class="symbol-row" data-symbol="${item.symbol}" data-name="${item.name || ""}">
              <td>${item.symbol}</td>
              <td>${item.name || ""}</td>
              <td>${item.start_date || ""}</td>
              <td>${item.end_date || ""}</td>
              <td>${item.rows || 0}</td>
              <td>${item.sources || ""}</td>
            </tr>
          `,
        )
        .join("") || `<tr><td colspan="6">暂无本地行情。</td></tr>`;
    document.querySelectorAll(".symbol-row").forEach((row) => {
      row.addEventListener("click", () => {
        addStock({ symbol: row.dataset.symbol, name: row.dataset.name || row.dataset.symbol });
        location.hash = "kline";
      });
    });
  }
  if (!state.selected.length) {
    const firstReady = data.symbols.find((item) => Number(item.rows || 0) > 0);
    if (firstReady) addStock({ symbol: firstReady.symbol, name: firstReady.name || firstReady.symbol });
  }
}

async function searchStocks(query) {
  const data = await api(`/api/stocks/search?q=${encodeURIComponent(query)}&limit=40`);
  $("searchResults").innerHTML =
    data.stocks
      .map(
        (stock) => `
          <button class="stock-row" data-symbol="${stock.symbol}" data-name="${stock.name || ""}" data-rows="${stock.rows || 0}">
            <span><b>${stock.name || stock.symbol}</b><em>${stock.symbol}</em></span>
            <small>${stock.start_date || "未下载"} · ${stock.rows || 0}行</small>
          </button>
        `,
      )
      .join("") || `<div class="empty">没搜到，本地库可能还没有下载这只股票。</div>`;
  document.querySelectorAll(".stock-row").forEach((button) => {
    button.addEventListener("click", () =>
      addStock({ symbol: button.dataset.symbol, name: button.dataset.name || button.dataset.symbol }),
    );
  });
}

function addStock(stock) {
  if (!state.selected.some((item) => item.symbol === stock.symbol)) {
    state.selected.push(stock);
    renderSelected();
  }
}

function removeStock(symbol) {
  state.selected = state.selected.filter((item) => item.symbol !== symbol);
  renderSelected();
}

function renderSelected() {
  $("selectedStocks").innerHTML =
    state.selected
      .map(
        (stock) => `
          <span class="chip">
            ${stock.name || stock.symbol}<em>${stock.symbol}</em>
            <button title="移除" data-remove="${stock.symbol}">×</button>
          </span>
        `,
      )
      .join("") || `<span class="hint">从上方搜索结果点选股票。</span>`;
  document.querySelectorAll("[data-remove]").forEach((button) => {
    button.addEventListener("click", () => removeStock(button.dataset.remove));
  });
  $("klineSymbol").innerHTML = state.selected
    .map((stock) => `<option value="${stock.symbol}">${stock.name || stock.symbol} · ${stock.symbol}</option>`)
    .join("");
  if (state.selected.length) renderSelectedKline();
}

function renderParams() {
  const name = $("strategyName").value;
  const isShortStrategy = ["divergence_tactic", "divergence_flow", "gap_t_tactic"].includes(name);
  document.querySelectorAll(".ma-param").forEach((node) => node.classList.toggle("hidden", name !== "moving_average"));
  document.querySelectorAll(".momentum-param").forEach((node) => node.classList.toggle("hidden", name !== "momentum"));
  document.querySelectorAll(".short-param").forEach((node) => node.classList.toggle("hidden", !isShortStrategy));
  document
    .querySelectorAll(".divergence-param")
    .forEach((node) => node.classList.toggle("hidden", name !== "divergence_tactic"));
  document.querySelectorAll(".flow-param").forEach((node) => node.classList.toggle("hidden", name !== "divergence_flow"));
  document.querySelectorAll(".gap-t-param").forEach((node) => node.classList.toggle("hidden", name !== "gap_t_tactic"));
  if (name === "divergence_tactic") {
    applyDivergencePreset();
  } else if (name === "divergence_flow") {
    applyFlowDefaults();
  } else if (name === "gap_t_tactic") {
    applyGapTDefaults();
  }
}

function renderDivergencePresets() {
  $("divergencePreset").innerHTML = Object.entries(DIVERGENCE_PRESETS)
    .map(([key, preset]) => `<option value="${key}">${preset.label}</option>`)
    .join("");
  $("divergencePreset").value = "stable";
}

function applyDivergencePreset() {
  const preset = DIVERGENCE_PRESETS[$("divergencePreset").value] || DIVERGENCE_PRESETS.stable;
  const params = preset.params;
  $("maxPositions").value = params.max_positions;
  $("holdDays").value = params.hold_days || 3;
  $("stopLoss").value = params.stop_loss;
  $("entryMinOpenGapPctChg").value = params.entry_min_open_gap_pct_chg;
  $("entryMaxOpenGapPctChg").value = params.entry_max_open_gap_pct_chg;
  $("entryMinHighFromOpenPctChg").value = params.entry_min_high_from_open_pct_chg;
  $("day2MaxVolumeRatio").value = params.day2_max_volume_ratio;
  $("day2MinClosePosition").value = params.day2_min_close_position;
  $("day2MaxUpperShadow").value = params.day2_max_upper_shadow;
  $("day2MinCloseVsDay1Close").value = params.day2_min_close_vs_day1_close;
}

function applyFlowDefaults() {
  $("maxPositions").value = 3;
  $("holdDays").value = 3;
  $("stopLoss").value = -0.03;
  $("flowDay2HighLimit").value = 8;
  $("flowDay2LowLimit").value = -3;
  $("flowDay2VolumeRatio").value = 2;
  $("flowSidewaysDays").value = 22;
  $("flowPullbackMa").value = 5;
  $("flowPullbackNear").value = 1;
  $("flowMaxWaitDays").value = 8;
}

function applyGapTDefaults() {
  $("maxPositions").value = 3;
  $("holdDays").value = 5;
  $("stopLoss").value = -0.03;
  $("gapSupportWindow").value = 5;
  $("gapSupportNear").value = 1;
  $("gapSupportConfirm").value = 1;
  $("gapLongLowerRatio").value = 3;
  $("gapHighLookback").value = 20;
  $("gapUpperShadowSell").value = 0.08;
  $("gapVolumeSellRatio").value = 1.8;
}

function collectBacktestPayload(useAll = false) {
  const strategyName = $("strategyName").value;
  let params;
  if (strategyName === "momentum") {
    params = { lookback: Number($("lookback").value), top_k: Number($("topK").value) };
  } else if (strategyName === "divergence_tactic") {
    params = collectDivergence2Params();
  } else if (strategyName === "divergence_flow") {
    params = collectFlowParams();
  } else if (strategyName === "gap_t_tactic") {
    params = collectGapTParams();
  } else {
    params = {
      short_window: Number($("shortWindow").value),
      long_window: Number($("longWindow").value),
      position_ratio: 1,
    };
  }
  return {
    strategy_name: strategyName,
    universe: useAll ? $("marketUniverse").value : "selected",
    symbols: useAll ? [] : state.selected.map((item) => item.symbol),
    start_date: $("startDate").value,
    end_date: $("endDate").value,
    initial_cash: Number($("initialCash").value),
    commission_rate: Number($("commissionRate").value),
    slippage_rate: Number($("slippageRate").value),
    stamp_tax_rate: Number($("stampTaxRate").value),
    min_rows: 30,
    params,
  };
}

function collectDivergence2Params() {
  const preset = DIVERGENCE_PRESETS[$("divergencePreset").value] || DIVERGENCE_PRESETS.stable;
  return {
    ...preset.params,
    max_positions: Number($("maxPositions").value),
    hold_days: Number($("holdDays").value),
    stop_loss: Number($("stopLoss").value),
    entry_min_open_gap_pct_chg: Number($("entryMinOpenGapPctChg").value),
    entry_max_open_gap_pct_chg: Number($("entryMaxOpenGapPctChg").value),
    entry_min_high_from_open_pct_chg: Number($("entryMinHighFromOpenPctChg").value),
    day2_max_volume_ratio: Number($("day2MaxVolumeRatio").value),
    day2_min_close_position: Number($("day2MinClosePosition").value),
    day2_max_upper_shadow: Number($("day2MaxUpperShadow").value),
    day2_min_close_vs_day1_close: Number($("day2MinCloseVsDay1Close").value),
  };
}

function collectFlowParams() {
  return {
    max_positions: Number($("maxPositions").value),
    hold_days: Number($("holdDays").value),
    stop_loss: Number($("stopLoss").value),
    min_price: 3,
    max_price: 500,
    day2_high_limit_pct_chg: Number($("flowDay2HighLimit").value),
    day2_low_limit_pct_chg: Number($("flowDay2LowLimit").value),
    day2_volume_limit_ratio: Number($("flowDay2VolumeRatio").value),
    pre_sideways_days: Number($("flowSidewaysDays").value),
    pullback_ma_window: Number($("flowPullbackMa").value),
    pullback_near_pct_chg: Number($("flowPullbackNear").value),
    max_wait_days: Number($("flowMaxWaitDays").value),
  };
}

function collectGapTParams() {
  return {
    max_positions: Number($("maxPositions").value),
    hold_days: Number($("holdDays").value),
    stop_loss: Number($("stopLoss").value),
    min_price: 3,
    max_price: 500,
    support_window: Number($("gapSupportWindow").value),
    support_near_pct_chg: Number($("gapSupportNear").value),
    support_confirm_pct_chg: Number($("gapSupportConfirm").value),
    long_lower_shadow_ratio: Number($("gapLongLowerRatio").value),
    high_position_lookback: Number($("gapHighLookback").value),
    upper_shadow_sell: Number($("gapUpperShadowSell").value),
    volume_sell_ratio: Number($("gapVolumeSellRatio").value),
  };
}

function collectDivergenceParams() {
  return collectDivergence2Params();
}

async function runBacktest(forceAll = false) {
  const useAll = forceAll || $("allLocalStocks").checked;
  if (!useAll && !state.selected.length) {
    $("runStatus").textContent = "请先选择股票。";
    return;
  }
  const universeLabel = MARKET_UNIVERSE_LABELS[$("marketUniverse").value] || "全部本地股票";
  $("runStatus").textContent = useAll ? `${universeLabel}回算中，数据多时需要等一会...` : "回测中...";
  $("runBtn").disabled = true;
  $("runAllBtn").disabled = true;
  $("scanBtn").disabled = true;
  try {
    const result = await api("/api/backtests", {
      method: "POST",
      body: JSON.stringify(collectBacktestPayload(useAll)),
    });
    state.result = result;
    renderResult(result);
    $("runStatus").textContent = "回测完成";
    location.hash = "result";
  } catch (error) {
    $("runStatus").textContent = error.message;
  } finally {
    $("runBtn").disabled = false;
    $("runAllBtn").disabled = false;
    $("scanBtn").disabled = false;
  }
}

async function scanSignals() {
  const useAll = $("allLocalStocks").checked || !state.selected.length;
  const universeLabel = MARKET_UNIVERSE_LABELS[$("marketUniverse").value] || "全部本地股票";
  $("runStatus").textContent = useAll ? `正在扫描${universeLabel}...` : "正在扫描已选股票...";
  $("scanBtn").disabled = true;
  try {
    const data = await api("/api/strategy-signals", {
      method: "POST",
      body: JSON.stringify({ ...collectBacktestPayload(useAll), limit: 1000 }),
    });
    state.signals = data.signals || [];
    renderSignals(data);
    $("runStatus").textContent = `扫描完成，找到 ${state.signals.length} 条`;
  } catch (error) {
    $("runStatus").textContent = error.message;
  } finally {
    $("scanBtn").disabled = false;
  }
}

function renderSignals(data) {
  const strategy = state.strategies.find((item) => item.name === data.strategy_name);
  const universeLabel = data.universe_label || MARKET_UNIVERSE_LABELS[data.universe] || "已选股票";
  $("signalSummary").textContent = `${strategy?.label || data.strategy_name} · ${universeLabel} · 扫描 ${data.scanned_symbols || 0} 只 · ${data.start_date} 至 ${data.end_date} · 命中 ${data.signals.length} 条`;
  $("signalRows").innerHTML =
    data.signals
      .map(
        (signal) => `
          <tr class="signal-row" data-symbol="${signal.symbol}" data-name="${signal.name || ""}">
            <td>${signal.signal_date || ""}</td>
            <td>${signal.symbol}</td>
            <td>${signal.name || ""}</td>
            <td class="${Number(signal.day1_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(signal.day1_pct_chg)}</td>
            <td class="${Number(signal.day2_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(signal.day2_pct_chg)}</td>
            <td>${signal.buy_price ? Number(signal.buy_price).toFixed(3) : signal.entry_open ? Number(signal.entry_open).toFixed(3) : ""}</td>
            <td class="${Number(signal.trade_worth_probability || 0) >= 0.55 ? "gain" : ""}" title="${signal.ml_score_source || ""}">${formatProbability(signal.trade_worth_probability)}</td>
            <td>${signal.reason || ""}</td>
          </tr>
        `,
      )
      .join("") || `<tr><td colspan="8">当前日期范围没有找到符合策略的股票。</td></tr>`;
  document.querySelectorAll(".signal-row").forEach((row) => {
    row.addEventListener("click", () => {
      addStock({ symbol: row.dataset.symbol, name: row.dataset.name || row.dataset.symbol });
      $("klineSymbol").value = row.dataset.symbol;
      renderSelectedKline();
      location.hash = "kline";
    });
  });
}

async function refreshWatchlist() {
  $("watchRefresh").disabled = true;
  $("watchMeta").textContent = "刷新中";
  $("watchSummary").textContent = "正在运行实时信号引擎 v1...";
  try {
    const data = await api("/api/realtime/signal-engine", {
      method: "POST",
      body: JSON.stringify({ params: collectDivergenceParams(), limit: 500, universe: $("marketUniverse").value }),
    });
    state.watchlist = data.items || [];
    renderWatchlist(data);
  } catch (error) {
    $("watchMeta").textContent = "刷新失败";
    $("watchSummary").textContent = error.message;
  } finally {
    $("watchRefresh").disabled = false;
  }
}

function toggleWatchAutoRefresh() {
  if (state.watchTimer) {
    clearInterval(state.watchTimer);
    state.watchTimer = null;
  }
  if (!$("watchAutoRefresh").checked) return;
  refreshWatchlist();
  const intervalMs = Math.max(1, Number($("watchInterval").value || 1)) * 1000;
  state.watchTimer = setInterval(refreshWatchlist, intervalMs);
}

function renderWatchlist(data) {
  $("watchMeta").textContent = data.quote_datetime || "无行情时间";
  const universeLabel = data.universe_label || MARKET_UNIVERSE_LABELS[data.universe] || "全部本地股票";
  const counts = data.counts || {};
  $("watchSubtitle").textContent = `实时信号引擎 v1 · 股票池：${universeLabel} · 最近行情：${data.quote_datetime || "未知"}`;
  $("watchSummary").textContent =
    data.message ||
    `扫描 ${data.scanned_symbols || 0} 只，T/T+1候选 ${data.setup_symbols || 0} 只；尾盘可买 ${counts.tail_ready || 0}，盘中突破 ${counts.triggered || 0}，候选中 ${counts.candidate || 0}，已失效 ${counts.invalid || 0}；耗时 ${data.elapsed_seconds || "-"} 秒`;
  $("watchRows").innerHTML =
    state.watchlist
      .map(
        (item) => `
          <tr class="watch-row" data-symbol="${item.symbol}" data-name="${item.name || ""}">
            <td>${item.quote_time || ""}</td>
            <td><span class="status-badge status-${item.status || "candidate"}">${item.status_label || ""}</span></td>
            <td>${item.symbol}</td>
            <td>${item.name || ""}</td>
            <td>${Number(item.price || 0).toFixed(3)}</td>
            <td class="${Number(item.pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(item.pct_chg)}</td>
            <td>${Number(item.open || 0).toFixed(3)}</td>
            <td class="${Number(item.day2_low_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(item.day2_low_pct_chg)}</td>
            <td class="${Number(item.day2_high_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(item.day2_high_pct_chg)}</td>
            <td>${item.day1_date || ""}</td>
            <td class="${Number(item.day1_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(item.day1_pct_chg)}</td>
            <td>${item.day2_date || ""}</td>
            <td class="${Number(item.day2_pct_chg || 0) < 0 ? "loss" : "gain"}">${formatPercentNumber(item.day2_pct_chg)}</td>
            <td>${item.buy_price ? Number(item.buy_price).toFixed(3) : ""}</td>
            <td class="${Number(item.trade_worth_probability || 0) >= 0.55 ? "gain" : ""}" title="${item.ml_score_source || ""}">${formatProbability(item.trade_worth_probability)}</td>
            <td title="${(item.reasons || []).join("；")}">${item.action || item.reason || ""}</td>
          </tr>
        `,
      )
      .join("") || `<tr><td colspan="16">当前没有进入 T/T+1 候选链路的股票。</td></tr>`;
  document.querySelectorAll(".watch-row").forEach((row) => {
    row.addEventListener("click", () => {
      addStock({ symbol: row.dataset.symbol, name: row.dataset.name || row.dataset.symbol });
      $("klineSymbol").value = row.dataset.symbol;
      renderSelectedKline();
      location.hash = "kline";
    });
  });
}

async function refreshPositions(options = {}) {
  const silent = Boolean(options.silent);
  if (!silent) {
    $("positionRefresh").disabled = true;
    $("positionMeta").textContent = "刷新中";
  }
  try {
    const data = await api("/api/positions");
    state.positions = data.positions || [];
    state.closedPositions = data.closed_positions || [];
    renderPositions(data);
  } catch (error) {
    if (!silent) $("positionMeta").textContent = "刷新失败";
    $("positionSummary").textContent = error.message;
  } finally {
    if (!silent) $("positionRefresh").disabled = false;
  }
}

async function addPosition() {
  const payload = {
    symbol: $("positionSymbol").value.trim(),
    name: $("positionName").value.trim(),
    entry_price: $("positionEntryPrice").value,
    quantity: $("positionQuantity").value,
    amount: $("positionAmount").value,
    source: "manual",
  };
  $("positionAdd").disabled = true;
  try {
    const data = await api("/api/positions", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    $("positionSymbol").value = "";
    $("positionName").value = "";
    $("positionEntryPrice").value = "";
    $("positionQuantity").value = "";
    $("positionAmount").value = "";
    state.positions = data.positions || [];
    state.closedPositions = data.closed_positions || [];
    renderPositions(data);
  } catch (error) {
    $("positionSummary").textContent = error.message;
  } finally {
    $("positionAdd").disabled = false;
  }
}

async function closePosition(symbol) {
  if (!window.confirm(`确认清仓 ${symbol}？将优先使用实时现价作为清仓价。`)) return;
  try {
    const data = await api("/api/positions/close", {
      method: "POST",
      body: JSON.stringify({ symbol }),
    });
    state.positions = data.positions || [];
    state.closedPositions = data.closed_positions || [];
    renderPositions(data);
  } catch (error) {
    $("positionSummary").textContent = error.message;
  }
}

function renderPositions(data) {
  const totals = data.totals || {};
  const totalPnl = totals.total_pnl ?? totals.pnl ?? 0;
  const totalReturn = totals.total_return ?? totalPnl / (Number(totals.initial_capital || 1000000) || 1000000);
  $("positionMeta").textContent = data.updated_at || "已刷新";
  $("positionSummary").innerHTML = `
    <div class="metric"><span>持仓数量</span><strong>${totals.count || 0}</strong></div>
    <div class="metric"><span>总本金</span><strong>${formatMoney(totals.initial_capital ?? 1000000)}</strong></div>
    <div class="metric"><span>买入金额</span><strong>${formatMoney(totals.entry_amount)}</strong></div>
    <div class="metric"><span>剩余本金</span><strong>${formatMoney(totals.cash_available)}</strong></div>
    <div class="metric"><span>当前市值</span><strong>${formatMoney(totals.market_value)}</strong></div>
    <div class="metric"><span>浮动收益</span><strong class="${Number(totals.pnl || 0) < 0 ? "loss" : "gain"}">${formatMoney(totals.pnl)}</strong></div>
    <div class="metric"><span>总盈亏</span><strong class="${Number(totalPnl) < 0 ? "loss" : "gain"}">${formatMoney(totalPnl)}</strong></div>
    <div class="metric"><span>总收益率</span><strong class="${Number(totalReturn) < 0 ? "loss" : "gain"}">${formatPercent(totalReturn)}</strong></div>
  `;
  $("positionRows").innerHTML =
    state.positions
      .map(
        (item) => `
          <tr class="position-row" data-symbol="${item.symbol}" data-name="${item.name || ""}">
            <td>${item.symbol}</td>
            <td>${item.name || ""}</td>
            <td>${item.entry_date || ""}</td>
            <td>${Number(item.entry_price || 0).toFixed(3)}</td>
            <td>${formatQuantity(item.quantity)}</td>
            <td>${formatMoney(item.entry_amount)}</td>
            <td>${Number(item.current_price || 0).toFixed(3)}</td>
            <td>${formatMoney(item.market_value)}</td>
            <td class="${Number(item.pnl || 0) < 0 ? "loss" : "gain"}">${formatMoney(item.pnl)}</td>
            <td class="${Number(item.pnl_pct || 0) < 0 ? "loss" : "gain"}">${formatPercent(item.pnl_pct)}</td>
            <td>${item.source || ""}</td>
            <td>${[item.quote_date, item.quote_time].filter(Boolean).join(" ")}</td>
            <td><button class="mini danger" data-close-symbol="${item.symbol}">清仓</button></td>
          </tr>
        `,
      )
      .join("") || `<tr><td colspan="13">暂无持仓。实时盯盘自动登记或手工登记后会显示在这里。</td></tr>`;
  $("closedPositionRows").innerHTML =
    state.closedPositions
      .slice()
      .reverse()
      .map(
        (item) => `
          <tr>
            <td>${item.symbol}</td>
            <td>${item.name || ""}</td>
            <td>${item.entry_date || ""}</td>
            <td>${Number(item.entry_price || 0).toFixed(3)}</td>
            <td>${item.exit_date || ""}</td>
            <td>${Number(item.exit_price || 0).toFixed(3)}</td>
            <td>${formatQuantity(item.quantity)}</td>
            <td>${formatMoney(item.entry_amount)}</td>
            <td>${formatMoney(item.exit_amount)}</td>
            <td class="${Number(item.realized_pnl || 0) < 0 ? "loss" : "gain"}">${formatMoney(item.realized_pnl)}</td>
            <td class="${Number(item.realized_return || 0) < 0 ? "loss" : "gain"}">${formatPercent(item.realized_return)}</td>
            <td>${item.close_reason || ""}</td>
          </tr>
        `,
      )
      .join("") || `<tr><td colspan="12">暂无清仓记录。</td></tr>`;
  document.querySelectorAll("[data-close-symbol]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      closePosition(button.dataset.closeSymbol);
    });
  });
  document.querySelectorAll(".position-row").forEach((row) => {
    row.addEventListener("click", () => {
      addStock({ symbol: row.dataset.symbol, name: row.dataset.name || row.dataset.symbol });
      $("klineSymbol").value = row.dataset.symbol;
      renderSelectedKline();
      location.hash = "kline";
    });
  });
}

function formatQuantity(value) {
  const number = Number(value || 0);
  if (!number) return "";
  return Number.isInteger(number) ? String(number) : number.toFixed(2);
}

function renderResult(result) {
  $("backtestId").textContent = `#${result.id}`;
  const strategy = state.strategies.find((item) => item.name === result.config.strategy_name);
  const universe =
    result.config.universe === "selected"
      ? `${result.config.symbols.length} 只股票`
      : `${result.config.universe_label || MARKET_UNIVERSE_LABELS[result.config.universe] || "全部本地股票"}（${result.config.universe_symbol_count || result.config.symbols.length} 只）`;
  const executionMode = result.config.execution_mode === "minute" ? "分钟级回算" : "日线回算";
  $("resultSubtitle").textContent = `${strategy?.label || result.config.strategy_name} · ${executionMode} · ${universe} · ${result.config.start_date} 至 ${result.config.end_date}`;
  renderMetrics(result.metrics);
  renderTrades(result.trades);
  renderEquity(result.equity_curve);
  renderDrawdown(result.equity_curve);
  renderMonthly(result.monthly_returns);
}

function renderMetrics(metrics) {
  const cards = [
    ["账户累计收益", formatPercent(metrics.cumulative_return), metrics.cumulative_return],
    ["最佳单笔价格涨跌", formatPercent(metrics.best_price_return), metrics.best_price_return],
    ["最佳单笔扣费收益", formatPercent(metrics.best_trade_return), metrics.best_trade_return],
    ["平均单笔扣费收益", formatPercent(metrics.avg_trade_return), metrics.avg_trade_return],
    ["年化收益", formatPercent(metrics.annual_return), metrics.annual_return],
    ["最大回撤", formatPercent(metrics.max_drawdown), metrics.max_drawdown],
    ["夏普", Number(metrics.sharpe || 0).toFixed(2), metrics.sharpe],
    ["胜率", formatPercent(metrics.win_rate), metrics.win_rate],
    ["交易次数", metrics.trade_count || 0, metrics.trade_count],
    ["期末资产", formatMoney(metrics.final_value), metrics.final_value],
  ];
  $("metrics").innerHTML = cards
    .map(([label, value, raw]) => `<div class="metric"><span>${label}</span><strong class="${Number(raw) < 0 ? "loss" : "gain"}">${value}</strong></div>`)
    .join("");
}

function renderTrades(trades) {
  $("tradeRows").innerHTML =
    trades
      .slice()
      .reverse()
      .map((trade) => {
        const pnl = Number(trade.pnl || 0);
        return `
          <tr>
            <td>${trade.trade_date}</td>
            <td>${trade.symbol}</td>
            <td>${trade.name || ""}</td>
            <td>${trade.side === "buy" ? "买入" : "卖出"}</td>
            <td>${trade.quantity}</td>
            <td>${Number(trade.price).toFixed(3)}</td>
            <td>${trade.reason || ""}</td>
            <td class="${pnl < 0 ? "loss" : "gain"}">${trade.side === "sell" ? formatMoney(pnl) : ""}</td>
            <td class="${Number(trade.price_return || 0) < 0 ? "loss" : "gain"}">${trade.side === "sell" ? formatPercent(trade.price_return) : ""}</td>
            <td class="${pnl < 0 ? "loss" : "gain"}">${trade.side === "sell" ? formatPercent(trade.pnl_pct) : ""}</td>
          </tr>
        `;
      })
      .join("") || `<tr><td colspan="10">暂无交易。</td></tr>`;
}

function renderEquity(rows) {
  const chart = echarts.init($("equityChart"));
  chart.setOption({
    title: { text: "资金曲线", left: 16, top: 12, textStyle: { fontSize: 15 } },
    tooltip: { trigger: "axis" },
    grid: { left: 58, right: 22, top: 54, bottom: 40 },
    xAxis: { type: "category", data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { type: "value", scale: true },
    series: [
      {
        type: "line",
        name: "净值",
        data: rows.map((row) => row.net_value),
        smooth: true,
        showSymbol: false,
        lineStyle: { color: "#16735f", width: 3 },
        areaStyle: { opacity: 0.12 },
      },
    ],
  });
}

function renderDrawdown(rows) {
  const chart = echarts.init($("drawdownChart"));
  chart.setOption({
    title: { text: "回撤", left: 16, top: 12, textStyle: { fontSize: 15 } },
    tooltip: { trigger: "axis", valueFormatter: formatPercent },
    grid: { left: 58, right: 22, top: 54, bottom: 40 },
    xAxis: { type: "category", data: rows.map((row) => row.trade_date), boundaryGap: false },
    yAxis: { type: "value", axisLabel: { formatter: (value) => `${Math.round(value * 100)}%` } },
    series: [
      {
        type: "line",
        name: "回撤",
        data: rows.map((row) => row.drawdown),
        smooth: true,
        showSymbol: false,
        lineStyle: { color: "#b24731", width: 2 },
        areaStyle: { opacity: 0.16 },
      },
    ],
  });
}

function renderMonthly(rows) {
  const chart = echarts.init($("monthlyChart"));
  chart.setOption({
    tooltip: { trigger: "axis", valueFormatter: formatPercent },
    grid: { left: 48, right: 16, top: 18, bottom: 32 },
    xAxis: { type: "category", data: rows.map((row) => row.month) },
    yAxis: { type: "value", axisLabel: { formatter: (value) => `${Math.round(value * 100)}%` } },
    series: [
      {
        type: "bar",
        data: rows.map((row) => row.return),
        itemStyle: { color: (params) => (params.value >= 0 ? "#16735f" : "#b24731") },
      },
    ],
  });
}

async function renderSelectedKline() {
  const symbol = $("klineSymbol").value || state.selected[0]?.symbol;
  if (!symbol) return;
  try {
    const data = await api(
      `/api/market-data/daily?symbol=${encodeURIComponent(symbol)}&start_date=${$("startDate").value}&end_date=${$("endDate").value}`,
    );
    const rows = data.data;
    if (!rows.length) {
      $("klineSubtitle").textContent = `${symbol} 在当前日期范围内没有本地行情`;
      renderKline([]);
      return;
    }
    $("klineSubtitle").textContent = `${symbol} · ${rows[0].trade_date} 至 ${rows[rows.length - 1].trade_date}`;
    renderKline(rows);
  } catch (error) {
    $("klineSubtitle").textContent = error.message;
  }
}

function renderKline(rows) {
  if (!window.echarts) {
    $("klineChart").innerHTML = `<div class="chart-empty">图表库没有加载成功，请刷新页面。</div>`;
    return;
  }
  const pctLabel = (value) => `${Number(value || 0).toFixed(2)}%`;
  const chart = echarts.init($("klineChart"));
  if (!rows.length) {
    chart.clear();
    chart.setOption({
      title: { text: "暂无 K 线数据", left: "center", top: "center", textStyle: { color: "#69756e", fontSize: 16 } },
    });
    return;
  }
  chart.setOption({
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params) => {
        const index = params[0]?.dataIndex ?? 0;
        const row = rows[index];
        if (!row) return "";
        return [
          `<b>${row.trade_date}</b>`,
          `开：${Number(row.open).toFixed(2)} 高：${Number(row.high).toFixed(2)}`,
          `低：${Number(row.low).toFixed(2)} 收：${Number(row.close).toFixed(2)}`,
          `涨跌幅：${pctLabel(row.pct_chg)}`,
          `换手率：${pctLabel(row.turnover)}`,
          `成交量：${Number(row.volume || 0).toLocaleString("zh-CN")}`,
        ].join("<br/>");
      },
    },
    grid: [
      { left: 56, right: 24, top: 28, height: "54%" },
      { left: 56, right: 24, top: "68%", height: "12%" },
      { left: 56, right: 24, bottom: 30, height: "12%" },
    ],
    xAxis: [
      { type: "category", data: rows.map((row) => row.trade_date), boundaryGap: false },
      { type: "category", data: rows.map((row) => row.trade_date), gridIndex: 1, boundaryGap: false, axisLabel: { show: false } },
      { type: "category", data: rows.map((row) => row.trade_date), gridIndex: 2, boundaryGap: false, axisLabel: { show: false } },
    ],
    yAxis: [
      { scale: true },
      { gridIndex: 1 },
      { gridIndex: 2, axisLabel: { formatter: (value) => `${value}%` } },
    ],
    dataZoom: [{ type: "inside", xAxisIndex: [0, 1, 2] }, { type: "slider", xAxisIndex: [0, 1, 2], bottom: 0, height: 18 }],
    series: [
      {
        name: "K线",
        type: "candlestick",
        data: rows.map((row) => [row.open, row.close, row.low, row.high]),
        itemStyle: { color: "#b24731", color0: "#16735f", borderColor: "#b24731", borderColor0: "#16735f" },
      },
      {
        name: "成交量",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: rows.map((row) => row.volume),
        itemStyle: { color: "#8d988f" },
      },
      {
        name: "涨跌幅",
        type: "bar",
        xAxisIndex: 2,
        yAxisIndex: 2,
        data: rows.map((row) => Number(row.pct_chg || 0)),
        itemStyle: { color: (params) => (params.value >= 0 ? "#b24731" : "#16735f") },
      },
    ],
  });
}

async function renderSelectedKline() {
  const symbol = $("klineSymbol").value || state.selected[0]?.symbol;
  if (!symbol) return;
  const period = $("klinePeriod").value || "minute";
  const endpoint = period === "minute" ? "minute" : "daily";
  const today = formatDateInput(new Date());
  const startDate = period === "minute" ? today : $("startDate").value;
  const endDate = period === "minute" ? today : $("endDate").value;
  try {
    const data = await api(
      `/api/market-data/${endpoint}?symbol=${encodeURIComponent(symbol)}&start_date=${startDate}&end_date=${endDate}`,
    );
    const rows = data.data || [];
    if (!rows.length) {
      $("klineSubtitle").textContent = `${symbol} 在当前日期范围内没有本地${period === "minute" ? "分时" : "日线"}行情`;
      renderKline([], period);
      return;
    }
    const first = period === "minute" ? rows[0].trade_time : rows[0].trade_date;
    const last = period === "minute" ? rows[rows.length - 1].trade_time : rows[rows.length - 1].trade_date;
    $("klineSubtitle").textContent = `${symbol} · ${period === "minute" ? "分时" : "日线"} · ${first} 至 ${last}`;
    const latest = rows[rows.length - 1];
    const latestPctText = formatPercentNumber(latest.pct_chg);
    $("klineSubtitle").textContent = `${symbol} · ${period === "minute" ? "分时" : "日线"} · ${first} 至 ${last} · 涨跌幅 ${formatPercentNumber(latest.pct_chg)}`;
    $("klineSubtitle").textContent = `${symbol} \u00b7 ${period === "minute" ? "\u5206\u65f6" : "\u65e5\u7ebf"} \u00b7 ${first} \u81f3 ${last} \u00b7 \u6da8\u8dcc\u5e45 ${latestPctText}`;
    renderKline(rows, period);
  } catch (error) {
    $("klineSubtitle").textContent = error.message;
  }
}

function renderKline(rows, period = "daily") {
  if (!window.echarts) {
    $("klineChart").innerHTML = `<div class="chart-empty">图表库没有加载成功，请刷新页面。</div>`;
    return;
  }
  const chart = echarts.init($("klineChart"));
  if (!rows.length) {
    chart.clear();
    chart.setOption({
      title: { text: "暂无行情数据", left: "center", top: "center", textStyle: { color: "#69756e", fontSize: 16 } },
    });
    return;
  }
  const labels = rows.map((row) => (period === "minute" ? row.trade_time.slice(5, 16) : row.trade_date));
  chart.setOption({
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params) => {
        const row = rows[params[0]?.dataIndex ?? 0];
        if (!row) return "";
        return [
          `<b>${period === "minute" ? row.trade_time : row.trade_date}</b> \u6da8\u8dcc\u5e45 ${formatPercentNumber(row.pct_chg)}`,
          `开：${Number(row.open).toFixed(2)} 高：${Number(row.high).toFixed(2)}`,
          `低：${Number(row.low).toFixed(2)} 收：${Number(row.close).toFixed(2)}`,
          `成交量：${Number(row.volume || 0).toLocaleString("zh-CN")}`,
        ].join("<br/>");
      },
    },
    grid: [
      { left: 56, right: 24, top: 28, height: "62%" },
      { left: 56, right: 24, top: "76%", height: "12%" },
    ],
    xAxis: [
      { type: "category", data: labels, boundaryGap: false },
      { type: "category", data: labels, gridIndex: 1, boundaryGap: false, axisLabel: { show: false } },
    ],
    yAxis: [{ scale: true }, { gridIndex: 1 }],
    dataZoom: [{ type: "inside", xAxisIndex: [0, 1] }, { type: "slider", xAxisIndex: [0, 1], bottom: 0, height: 18 }],
    series: [
      {
        name: period === "minute" ? "分时" : "K线",
        type: "candlestick",
        data: rows.map((row) => [row.open, row.close, row.low, row.high]),
        itemStyle: { color: "#b24731", color0: "#16735f", borderColor: "#b24731", borderColor0: "#16735f" },
      },
      {
        name: "成交量",
        type: "bar",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: rows.map((row) => row.volume),
        itemStyle: { color: "#8d988f" },
      },
    ],
  });
}

window.addEventListener("resize", () => {
  ["equityChart", "drawdownChart", "monthlyChart", "klineChart"].forEach((id) => {
    const instance = echarts.getInstanceByDom($(id));
    if (instance) instance.resize();
  });
});

init();
