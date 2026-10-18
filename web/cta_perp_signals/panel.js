/* Each strategy has one position per symbol; B/S mark entry and exit transitions. */
"use strict";
const CtaPanel = (() => {
  const MINUTE = 60000;
  const strategyOf = (record) =>
    record.snapshot?.strategy_name || record.strategy_name || "";
  const published = (record) => !!record.publication?.target;
  const candidateDirection = (record) =>
    Number.isFinite(record.snapshot?.signal?.score)
      ? Math.sign(record.snapshot.signal.score)
      : 0;
  const modelBucket = (record) => record.model_ts_ms - MINUTE;
  const executionBucket = (record) =>
    Math.floor(record.event_ts_us / 1000 / MINUTE) * MINUTE;
  function actions(record) {
    if (record.simulation)
      return (record.simulation.actions || []).filter(
        (a) => a.kind === "open" || a.kind === "close",
      );
    const target = record.publication?.target;
    if (!target || !Number.isFinite(target.previous_qty)) return [];
    const previous = target.previous_qty,
      qty = target.qty === "clear" ? 0 : target.qty;
    if (!Number.isFinite(qty) || qty === previous) return [];
    const ts_ms = target.published_ts_us / 1000;
    if (previous === 0 && qty !== 0)
      return [
        { kind: "open", side: Math.sign(qty), qty: Math.abs(qty), ts_ms },
      ];
    if (previous !== 0 && qty === 0)
      return [
        {
          kind: "close",
          side: Math.sign(previous),
          qty: Math.abs(previous),
          ts_ms,
        },
      ];
    return [];
  }
  const direction = (record) => {
    const action = actions(record).at(-1);
    return action ? (action.kind === "close" ? -action.side : action.side) : 0;
  };
  function nqGate(record) {
    const entry = record.snapshot?.entry || {},
      signal = record.snapshot?.signal || {},
      side = candidateDirection(record),
      enabled = entry.nq_change_enabled,
      ready = signal.filter_ready;
    const valid = (q) => Number.isFinite(q) && q >= 0 && q <= 1;
    const branch = (name, compare) => {
      const quantile = signal[`nq_${name}_quantile`],
        threshold = entry[`nq_${name}_quantile`];
      return {
        value: signal[`nq_${name}_value`],
        quantile,
        threshold,
        passed:
          enabled === true &&
          ready === true &&
          valid(quantile) &&
          valid(threshold)
            ? compare(quantile, threshold)
            : null,
      };
    };
    const long = branch("long", (q, t) => q >= t),
      short = branch("short", (q, t) => q <= t);
    let mode = "unknown",
      text = "数据不完整";
    if (actions(record).some((a) => a.kind === "close")) {
      mode = "exit";
      text = "退出无需 NQ";
    } else if (enabled === false) {
      mode = "disabled";
      text = "已关闭";
    } else if (enabled !== true) text = "配置未记录";
    else if (ready === false) {
      mode = "warming";
      text = "未就绪";
    } else if (!Number.isFinite(signal.score) || signal.score_ready === false) {
      mode = "factor_unready";
      text = "等待因子";
    } else if (side === 0) {
      mode = "neutral";
      text = "无开仓方向";
    } else if ((side > 0 ? long : short).passed !== null) {
      mode = (side > 0 ? long : short).passed ? "passed" : "blocked";
      text = mode === "passed" ? "通过" : "拦截";
    }
    return { enabled, ready, side, mode, text, long, short };
  }
  function preferred(records) {
    return (
      records.slice().sort((a, b) => b.event_ts_us - a.event_ts_us)[0] || null
    );
  }
  function buildSeries(data) {
    const names = new Set(data.signals.map(strategyOf).filter(Boolean));
    const strategy = data.strategy || (names.size === 1 ? [...names][0] : "");
    const records = strategy
      ? data.signals.filter((r) => strategyOf(r) === strategy)
      : [];
    const candles = new Map(data.klines.map((row) => [row.open_time_ms, row]));
    const observations = new Map(),
      states = new Map(),
      seen = new Set(),
      events = [];
    const context = (data.simulation?.context || []).filter(
      (r) => strategyOf(r) === strategy,
    );
    let initialFlag = preferred(context)?.snapshot.position_flag;
    for (const record of records
      .slice()
      .sort((a, b) => a.event_ts_us - b.event_ts_us)) {
      const bucket = modelBucket(record);
      if (!observations.has(bucket)) observations.set(bucket, []);
      observations.get(bucket).push(record);
      const flag =
        record.simulation?.position_flag ?? record.snapshot.position_flag;
      if ([-1, 0, 1].includes(flag)) states.set(executionBucket(record), flag);
      for (const action of actions(record)) {
        if (
          ![-1, 1].includes(action.side) ||
          !Number.isFinite(action.ts_ms) ||
          !(action.qty > 0)
        )
          continue;
        const key = `${strategyOf(record)}:${record.symbol}:${action.kind}:${action.ts_ms}:${action.trade_id || ""}`;
        if (seen.has(key)) continue;
        seen.add(key);
        events.push({
          ...action,
          record,
          buy: action.kind === "close" ? action.side < 0 : action.side > 0,
        });
      }
    }
    events.sort((a, b) => a.ts_ms - b.ts_ms || (a.kind === "close" ? -1 : 1));
    const times = [];
    for (
      let ts = data.start_ms;
      ts <= Math.floor(data.end_ms / MINUTE) * MINUTE;
      ts += MINUTE
    )
      times.push(ts);
    const long = [],
      short = [],
      closes = [],
      markers = [];
    for (const action of events) {
      const ts = Math.floor(action.ts_ms / MINUTE) * MINUTE,
        candle = candles.get(ts);
      if (!candle) continue;
      const point = {
        value: [
          ts,
          Number.isFinite(action.price) ? action.price : candle.close,
        ],
        record: action.record,
        action,
        symbolSize: action.kind === "open" ? 12 : 10,
        label: {
          formatter: action.buy ? "B" : "S",
          position: action.buy ? "bottom" : "top",
          color: action.buy ? "#4ad6ab" : "#f48792",
        },
        itemStyle: {
          color: action.buy ? "#4ad6ab" : "#f48792",
          borderColor: "#e7edf7",
          borderWidth: 1,
        },
      };
      markers.push(point);
      if (action.kind === "close") closes.push(point);
      else (action.side > 0 ? long : short).push(point);
    }
    const byId = new Map(records.map((r) => [r.signal_id, r]));
    const trades = [
      ...(data.simulation?.trades || []).map((t) => ({
        ...t,
        status: "closed",
      })),
      ...(data.simulation?.open_positions || []).map((t) => ({
        ...t,
        status: "open",
      })),
    ]
      .filter((t) => t.strategy_name === strategy && t.symbol === data.symbol)
      .sort((a, b) => b.entry_ts_ms - a.entry_ts_ms);
    const pairs = trades.filter(
      (t) =>
        t.status === "closed" &&
        candles.has(Math.floor(t.entry_ts_ms / MINUTE) * MINUTE) &&
        candles.has(Math.floor(t.exit_ts_ms / MINUTE) * MINUTE),
    );
    let currentFlag = [-1, 0, 1].includes(initialFlag) ? initialFlag : "-";
    const position = times.map((ts) => {
      if (states.has(ts)) currentFlag = states.get(ts);
      return currentFlag;
    });
    const quantile = (ts, name) => {
      const record = preferred(observations.get(ts) || []),
        value = record?.snapshot.signal?.[name];
      return Number.isFinite(value) ? value : "-";
    };
    return {
      strategy,
      times,
      candles,
      observations,
      records,
      events,
      markers,
      long,
      short,
      closes,
      pairs,
      trades,
      byId,
      position,
      nqLong: times.map((ts) => quantile(ts, "nq_long_quantile")),
      nqShort: times.map((ts) => quantile(ts, "nq_short_quantile")),
      nqLongThreshold: times.map(
        (ts) =>
          preferred(observations.get(ts) || [])?.snapshot.entry
            ?.nq_long_quantile ?? "-",
      ),
      nqShortThreshold: times.map(
        (ts) =>
          preferred(observations.get(ts) || [])?.snapshot.entry
            ?.nq_short_quantile ?? "-",
      ),
      ohlc: times.map((ts) => {
        const row = candles.get(ts);
        return row
          ? [row.open, row.close, row.low, row.high]
          : ["-", "-", "-", "-"];
      }),
    };
  }
  return {
    MINUTE,
    strategyOf,
    published,
    actions,
    direction,
    candidateDirection,
    nqGate,
    modelBucket,
    executionBucket,
    preferred,
    buildSeries,
  };
})();
if (typeof module !== "undefined") module.exports = CtaPanel;
if (typeof document !== "undefined")
  (() => {
    const el = (id) => document.getElementById(id);
    const esc = (value) =>
      String(value ?? "—").replace(
        /[&<>"']/g,
        (c) =>
          ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          })[c],
      );
    const fmt = (value, digits = 3) =>
      value == null || !Number.isFinite(Number(value))
        ? "—"
        : Number(value).toLocaleString("en-US", {
            maximumFractionDigits: digits,
          });
    const pct = (value) =>
      Number.isFinite(value) ? `${fmt(value * 100, 1)}%` : "—";
    const time = (ms, full = false) =>
      Number.isFinite(ms)
        ? new Date(ms)
            .toISOString()
            .slice(full ? 0 : 11, 19)
            .replace("T", " ")
        : "—";
    const tone = (value) =>
      value > 0 ? "positive" : value < 0 ? "negative" : "neutral";
    const positionText = (flag) =>
      flag === 1
        ? "满仓多"
        : flag === -1
          ? "满仓空"
          : flag === 0
            ? "空仓"
            : "等待数据";
    const actionText = (a) =>
      a.kind === "open"
        ? a.side > 0
          ? "B 开多"
          : "S 开空"
        : a.side > 0
          ? "S 平多"
          : "B 平空";
    const state = {
      overview: null,
      symbol: null,
      strategy: "",
      data: null,
      series: null,
      selected: null,
      trade: null,
      hours: 6,
      zoom: null,
      etag: null,
      generation: 0,
      controller: null,
      inFlight: false,
    };
    const chart =
      typeof echarts !== "undefined" ? echarts.init(el("chart")) : null;
    function error(message) {
      el("error").textContent = message;
      el("error").hidden = !message;
    }
    async function json(url, headers = {}, signal) {
      const response = await fetch(url, { headers, signal, cache: "no-store" });
      if (response.status === 304) return { unchanged: true };
      const value = await response.json();
      if (!response.ok)
        throw new Error(value.error || `HTTP ${response.status}`);
      return { value, etag: response.headers.get("ETag") };
    }
    const choices = (symbol) =>
      state.overview?.factor_strategies?.[symbol] ||
      Object.keys(
        state.overview?.simulation?.summaries?.[symbol]?.strategies || {},
      );
    const strategyLabel = name => name === "symbol_mean" ? "因子算术平均" : name;
    const factorStats = (symbol, factor) =>
      state.overview?.simulation?.summaries?.[symbol]?.strategies?.[factor];
    function watchlist() {
      el("symbols").innerHTML = (state.overview?.symbols || [])
        .map((row) => {
          const factors = choices(row.symbol);
          return `<button class="symbol ${row.symbol === state.symbol ? "active" : ""}" data-symbol="${esc(row.symbol)}"><span><strong>${esc(row.symbol.replace(/USDT$/, ""))}</strong><small>${factors.includes("symbol_mean") ? "综合方向" : factors.length + " 个独立因子"}</small></span><span class="count">${factors.length}</span></button>`;
        })
        .join("");
      el("symbols")
        .querySelectorAll("button")
        .forEach((button) =>
          button.addEventListener("click", () =>
            selectSymbol(button.dataset.symbol),
          ),
        );
    }
    function factors() {
      const available = choices(state.symbol);
      el("strategy-filter").innerHTML = available.length
        ? available
            .map((name) => `<option value="${esc(name)}">${esc(strategyLabel(name))}</option>`)
            .join("")
        : '<option value="">暂无可用因子</option>';
      el("strategy-filter").value = state.strategy;
      el("strategy-filter").disabled = !available.length;
      el("factor-list").innerHTML = available
        .map((name) => {
          const stats = factorStats(state.symbol, name),
            pnl = (stats?.realized_pnl || 0) + (stats?.unrealized_pnl || 0);
          return `<button class="factor-choice ${name === state.strategy ? "active" : ""}" data-factor="${esc(name)}"><b>${esc(strategyLabel(name))}</b><span>${positionText(stats?.position_flag)} · ${stats?.closed_trades || 0} 笔已平</span><strong class="${tone(pnl)}">${fmt(pnl, 2)} <small>USDT</small></strong></button>`;
        })
        .join("");
      el("factor-list")
        .querySelectorAll("button")
        .forEach((button) =>
          button.addEventListener("click", () =>
            selectFactor(button.dataset.factor),
          ),
        );
    }
    function summary() {
      const data = state.data,
        sim = data.simulation,
        stats = sim?.summary,
        last = data.klines.at(-1),
        first = data.klines[0];
      const record = data.signals.at(-1),
        signal = record?.snapshot.signal;
      el("demo").hidden = !data.signals.some(
        (row) => row.snapshot.demo === true,
      );
      el("market-symbol").textContent = data.symbol.replace(/USDT$/, " ");
      el("factor-name").textContent = strategyLabel(state.strategy);
      el("price").textContent = fmt(last?.close, 8);
      const change = last && first ? (last.close / first.open - 1) * 100 : null;
      el("change").textContent =
        `${data.closed_bars === data.expected_closed_bars ? "24h" : "已采集区间"} ${change == null ? "—" : `${change >= 0 ? "+" : ""}${fmt(change, 2)}%`}`;
      el("change").className = `change ${tone(change || 0)}`;
      el("closed-trades").textContent = fmt(stats?.closed_trades, 0);
      el("win-rate").textContent = `胜率 ${pct(stats?.win_rate)} · 只计已平仓`;
      el("position-state").textContent = positionText(stats?.position_flag);
      el("position-qty").textContent = stats
        ? `${fmt(stats.position_qty, 8)} qty`
        : "等待仓位回放";
      const pnl = stats ? stats.realized_pnl + stats.unrealized_pnl : null;
      el("pnl").textContent = fmt(pnl, 3);
      el("pnl").className = tone(pnl || 0);
      el("pnl-note").textContent =
        `已实现 ${fmt(stats?.realized_pnl, 3)} / 浮动 ${fmt(stats?.unrealized_pnl, 3)}`;
      el("factor-quantile").textContent = pct(signal?.decision_value ?? signal?.quantile);
      el("factor-quantile").className = tone(signal?.score || 0);
      el("nq-status").textContent = record
        ? `NQ ${CtaPanel.nqGate(record).text}`
        : "等待 NQ";
      el("coverage").textContent =
        `K 线 ${data.closed_bars}/${data.expected_closed_bars}${data.missing_minutes.length ? ` · 缺 ${data.missing_minutes.length} 根` : ""}`;
      el("replay-summary").textContent = sim?.error
        ? `回放错误：${sim.error}`
        : !sim?.ready
          ? "等待因子历史与仓位回放。"
          : `UTC ${time(sim.start_ms, true)} 起空仓 · 每因子独立满仓 · ${sim.gross_results ? "毛收益，未计手续费" : "已计 taker 手续费"} · 无额外滑点 · 尚未平仓不计胜率`;
      el("chart-empty").hidden = data.klines.length > 0 && sim?.ready === true;
      el("chart-empty").textContent = data.feed?.error
        ? `K 线暂不可用：${data.feed.error}`
        : !sim?.ready
          ? "等待该因子的历史回放…"
          : "正在补齐分钟 K 线…";
      el("updated").textContent =
        `更新 ${time(data.server_ts_ms)} UTC · 历史模拟`;
    }
    function tooltip(params) {
      const rows = Array.isArray(params) ? params : [params],
        marker = rows.find((row) => row.data?.action);
      if (marker) {
        const action = marker.data.action;
        return `<b>${actionText(action)}</b><br>${esc(strategyLabel(state.strategy))} · ${time(action.ts_ms)} UTC<br>价格 ${fmt(action.price, 8)} · 数量 ${fmt(action.qty, 8)}${action.kind === "close" ? `<br>本笔 PnL ${fmt(action.pnl, 4)} USDT` : ""}<br>点击查看本次交易与 NQ`;
      }
      const candlePoint = rows.find((row) => row.seriesType === "candlestick");
      if (candlePoint) {
        const candle = state.series.candles.get(
          state.series.times[candlePoint.dataIndex],
        );
        if (!candle) return "该分钟 K 线缺失";
        return `<b>${time(candle.open_time_ms)} UTC${candle.closed ? "" : " · 形成中"}</b><br>开 ${fmt(candle.open, 8)}　高 ${fmt(candle.high, 8)}<br>低 ${fmt(candle.low, 8)}　收 ${fmt(candle.close, 8)}<br>点击查看该分钟因子与 NQ 快照`;
      }
      return rows
        .map(
          (row) =>
            `${esc(row.seriesName)}: ${typeof row.value === "number" ? pct(row.value) : esc(row.value)}`,
        )
        .join("<br>");
    }
    function renderChart() {
      const series = (state.series = CtaPanel.buildSeries(state.data)),
        index = (ts) =>
          Math.floor((ts - state.data.start_ms) / CtaPanel.MINUTE);
      const scatter = (points) =>
        points.map((point) => ({
          ...point,
          value: [index(point.value[0]), point.value[1]],
        }));
      const labels = series.times.map((ts) => time(ts).slice(0, 5));
      const axis = {
        type: "category",
        data: labels,
        boundaryGap: true,
        axisLine: { lineStyle: { color: "#314056" } },
        axisTick: { show: false },
        axisLabel: { color: "#8496af", fontSize: 10 },
        splitLine: { show: false },
      };
      const zoom = state.zoom || {
        start: Math.max(0, 100 - (state.hours / 24) * 100),
        end: 100,
      };
      const label = {
        show: true,
        distance: 5,
        fontSize: 11,
        fontWeight: 700,
        backgroundColor: "#0a111bee",
        padding: [2, 4],
        borderRadius: 3,
      };
      const marks = (name, points, symbol, rotate = 0) => ({
        name,
        type: "scatter",
        data: scatter(points),
        symbol,
        symbolRotate: rotate,
        label,
        labelLayout: { hideOverlap: true },
        tooltip: { trigger: "item", formatter: tooltip },
        z: 12,
      });
      const selectedPair = state.trade;
      const pairLines = series.pairs.map((trade) => [
        {
          coord: [index(trade.entry_ts_ms), trade.average_entry_price],
          lineStyle: {
            color: trade.pnl >= 0 ? "#4ad6ab" : "#f48792",
            opacity:
              selectedPair && trade.trade_id !== selectedPair ? 0.16 : 0.65,
            width: trade.trade_id === selectedPair ? 3 : 1.2,
            type: "dashed",
          },
        },
        { coord: [index(trade.exit_ts_ms), trade.exit_price] },
      ]);
      const nqLine = (name, data, color, dashed = false) => ({
        name,
        type: "line",
        xAxisIndex: 2,
        yAxisIndex: 2,
        data,
        showSymbol: false,
        connectNulls: false,
        lineStyle: {
          color,
          width: dashed ? 1 : 1.6,
          type: dashed ? "dashed" : "solid",
        },
        z: dashed ? 1 : 3,
      });
      chart.setOption(
        {
          animation: false,
          backgroundColor: "transparent",
          textStyle: { fontFamily: "system-ui" },
          grid: [
            { left: 57, right: 20, top: 25, height: "52%" },
            { left: 57, right: 20, top: "64%", height: "9%" },
            { left: 57, right: 20, top: "80%", height: "11%" },
          ],
          title: [
            {
              text: "持仓",
              left: 10,
              top: "64%",
              textStyle: { fontSize: 10, color: "#8496af", fontWeight: 400 },
            },
            {
              text: "NQ",
              left: 10,
              top: "80%",
              textStyle: { fontSize: 10, color: "#8496af", fontWeight: 400 },
            },
          ],
          tooltip: {
            trigger: "axis",
            axisPointer: { type: "cross" },
            backgroundColor: "#101a29",
            borderColor: "#3a4e6c",
            textStyle: { color: "#e7edf7", fontSize: 11 },
            formatter: tooltip,
            confine: true,
          },
          axisPointer: {
            link: [{ xAxisIndex: "all" }],
            label: { backgroundColor: "#3c506c" },
          },
          xAxis: [
            { ...axis, axisLabel: { show: false } },
            { ...axis, gridIndex: 1, axisLabel: { show: false } },
            { ...axis, gridIndex: 2 },
          ],
          yAxis: [
            {
              scale: true,
              axisLabel: { color: "#8496af", fontSize: 10 },
              splitLine: { lineStyle: { color: "#233144" } },
              axisPointer: { label: { precision: 6 } },
            },
            {
              gridIndex: 1,
              min: -1,
              max: 1,
              interval: 1,
              axisLabel: {
                color: "#8496af",
                fontSize: 9,
                formatter: (v) => (v === 1 ? "多" : v === -1 ? "空" : "0"),
              },
              splitLine: { lineStyle: { color: "#233144" } },
            },
            {
              gridIndex: 2,
              min: 0,
              max: 1,
              interval: 0.5,
              axisLabel: {
                color: "#8496af",
                fontSize: 9,
                formatter: (v) => `${v * 100}%`,
              },
              splitLine: { lineStyle: { color: "#233144" } },
            },
          ],
          dataZoom: [
            { type: "inside", xAxisIndex: [0, 1, 2], ...zoom },
            {
              type: "slider",
              xAxisIndex: [0, 1, 2],
              ...zoom,
              bottom: 5,
              height: 17,
              borderColor: "#314056",
              textStyle: { color: "#8496af" },
              fillerColor: "#8aaeff18",
              handleStyle: { color: "#8aaeff" },
              showDetail: false,
            },
          ],
          series: [
            {
              name: "K线",
              type: "candlestick",
              data: series.ohlc,
              itemStyle: {
                color: "#4ad6ab",
                color0: "#f48792",
                borderColor: "#4ad6ab",
                borderColor0: "#f48792",
              },
              markLine: {
                silent: true,
                symbol: ["none", "none"],
                label: { show: false },
                data: pairLines,
              },
            },
            marks("B 开多", series.long, "triangle"),
            marks("S 开空", series.short, "triangle", 180),
            marks("平仓 B/S", series.closes, "circle"),
            {
              name: "持仓状态",
              type: "line",
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: series.position,
              step: "end",
              showSymbol: false,
              connectNulls: false,
              lineStyle: { color: "#8aaeff", width: 1.6 },
              areaStyle: { color: "#8aaeff20" },
            },
            nqLine("BUY_NQ 分位", series.nqLong, "#4ad6ab"),
            nqLine("SELL_NQ 分位", series.nqShort, "#f48792"),
            nqLine("多头 NQ 阈值", series.nqLongThreshold, "#9dcbb9", true),
            nqLine("空头 NQ 阈值", series.nqShortThreshold, "#c49da8", true),
          ],
        },
        true,
      );
    }
    function nqInspector(record) {
      const gate = CtaPanel.nqGate(record),
        row = (name, data, side, operator) => {
          const result =
            gate.enabled === false
              ? "不参与过滤"
              : gate.ready === false
                ? "未就绪"
                : data.passed === null
                  ? "数据不完整"
                  : data.passed
                    ? "通过"
                    : "拦截";
          return `<div class="nq-row ${gate.side === side ? "selected" : ""}" data-side="${side > 0 ? "long" : "short"}"><div><b>${name}${gate.side === side ? " · 本次方向" : ""}</b><span>${pct(data.value)}</span></div><small>分位 ${pct(data.quantile)} ${operator} 阈值 ${pct(data.threshold)}</small><strong class="${data.passed === true ? "positive" : data.passed === false ? "negative" : "neutral"}">${result}</strong></div>`;
        };
      return `<section class="nq-section"><div class="section-heading"><b>NQ change</b><span class="nq-state ${gate.mode === "passed" ? "positive" : gate.mode === "blocked" ? "negative" : "neutral"}" data-mode="${gate.mode}">${gate.text}</span></div>
      <p class="hint">只过滤开仓 · 使用当时的阈值</p>${row("BUY_NQ", gate.long, 1, "≥")}${row("SELL_NQ", gate.short, -1, "≤")}
      <details class="nq-method"><summary>计算方式</summary><p>BUY_NQ = (P − 2h 最低 mid) / 最低 mid<br>SELL_NQ = (P − 2h 最高 mid) / 最高 mid</p><p>P 是本合约分钟最后一个有效盘口 mid。极值和分位窗口均为 120 个分钟槽位，至少 60 个有效 NQ 样本后就绪。分位采用相同值的中间排名。</p><p>50% 是历史分位阈值。NQ 放行开仓，因子退出不受 NQ 限制。</p></details></section>`;
    }
    function inspector(record, action = null) {
      if (!record) {
        el("inspector").innerHTML =
          '<div class="hint">点击 B/S 点、交易记录或 K 线，查看当前策略的交易与 NQ。</div>';
        return;
      }
      const snapshot = record.snapshot,
        signal = snapshot.signal || {},
        flag = snapshot.position_flag;
      const execution = snapshot.execution || {},
        entry = snapshot.entry || {},
        cfg = snapshot.strategy_config || {};
      const selectedTrade = state.series.trades.find(
        (t) => t.trade_id === state.trade,
      );
      el("record-state").textContent = action ? actionText(action) : "因子观察";
      const pair = selectedTrade
        ? `<section class="trade-detail"><div class="section-heading"><b>本笔${selectedTrade.side > 0 ? "多仓" : "空仓"}</b><span>${selectedTrade.status === "open" ? "尚未平仓" : "已闭合"}</span></div>
      <p>${selectedTrade.side > 0 ? "B 开多 → S 平多" : "S 开空 → B 平空"}</p><div class="detail-grid"><div><small>开仓 UTC</small><b>${time(selectedTrade.entry_ts_ms)}</b></div><div><small>平仓 UTC</small><b>${time(selectedTrade.exit_ts_ms)}</b></div>
      <div><small>开仓价</small><b>${fmt(selectedTrade.average_entry_price, 8)}</b></div><div><small>${selectedTrade.status === "open" ? "当前标记价" : "平仓价"}</small><b>${fmt(selectedTrade.exit_price ?? selectedTrade.mark_price, 8)}</b></div>
      <div><small>原持仓数量</small><b>${fmt(selectedTrade.qty, 8)}</b></div><div><small>${selectedTrade.status === "open" ? "浮动 PnL" : "本笔 PnL"}</small><b class="${tone(selectedTrade.pnl ?? selectedTrade.unrealized_pnl)}">${fmt(selectedTrade.pnl ?? selectedTrade.unrealized_pnl, 4)}</b></div></div></section>`
        : "";
      el("inspector").innerHTML =
        `<div class="signal-top"><b>${esc(strategyLabel(state.strategy))}</b><span>${time(record.model_ts_ms, true)} UTC</span></div>${pair}
      <div class="detail-grid"><div><small>该分钟持仓</small><b>${positionText(flag)}</b></div><div><small>开平判断值 · ${esc(snapshot.position_algorithm || "v0")}</small><b>${pct(signal.decision_value ?? signal.quantile)}</b></div>
      <div><small>开多 / 开空</small><b>${pct(entry.factor_long_quantile)} / ${pct(entry.factor_short_quantile)}</b></div><div><small>退多 / 退空</small><b>${pct(execution.factor_exit_quantile_long)} / ${pct(execution.factor_exit_quantile_short)}</b></div>
      <div><small>因子数量</small><b>${signal.factor_votes?.length || 0}</b></div><div><small>单份金额</small><b>${fmt(snapshot.max_pos_u ?? cfg.max_pos_u_overrides?.[record.symbol] ?? cfg.max_pos_u, 2)} USDT</b></div></div>
      ${(signal.factor_votes || []).map(v => `<div class="factor-snapshot"><b>${esc(v.factor)}</b><span>sign ${esc(v.sign)}</span><span>原分位 ${pct(v.quantile)}</span><span>修正分位 x ${pct(v.corrected_quantile)}</span><span>判断值 ${pct(v.decision_value ?? v.corrected_quantile)}</span><span>占比 ${pct(v.normalized_weight)}</span></div>`).join("")}
      <p class="hint">同向重复触发保持原仓位，平仓使用原数量。</p>${nqInspector(record)}`;
    }
    function selectRecord(record, action = null) {
      state.selected = record?.signal_id || null;
      state.trade = action?.trade_id || null;
      inspector(record, action);
      renderChart();
      trades();
      observations();
    }
    function selectTrade(id) {
      const trade = state.series.trades.find((t) => t.trade_id === id);
      if (!trade) return;
      state.trade = id;
      const record =
        state.series.byId.get(trade.entry_signal_id) ||
        state.series.byId.get(trade.exit_signal_id);
      state.selected = record?.signal_id || null;
      inspector(
        record,
        record ? CtaPanel.actions(record).find((a) => a.trade_id === id) : null,
      );
      renderChart();
      trades();
      observations();
    }
    function trades() {
      const rows = state.series.trades;
      el("trade-count").textContent =
        `${rows.filter((t) => t.status === "closed").length} 笔已平 · ${rows.filter((t) => t.status === "open").length} 笔持仓`;
      el("trade-rows").innerHTML = rows
        .map((trade) => {
          const isLong = trade.side > 0,
            open = trade.status === "open",
            pnl = open ? trade.unrealized_pnl : trade.pnl;
          const entryRecord = state.series.byId.get(trade.entry_signal_id);
          return `<tr data-trade="${esc(trade.trade_id)}" class="${state.trade === trade.trade_id ? "selected" : ""}" tabindex="0"><td><span class="side-badge ${isLong ? "positive" : "negative"}">${isLong ? "多仓" : "空仓"}</span></td>
        <td><b class="${isLong ? "positive" : "negative"}">${isLong ? "B" : "S"}</b> ${time(trade.entry_ts_ms)}<small>${fmt(trade.average_entry_price, 8)}</small></td>
        <td>${open ? '<span class="pending">待平仓</span>' : `<b class="${isLong ? "negative" : "positive"}">${isLong ? "S" : "B"}</b> ${time(trade.exit_ts_ms)}`}<small>${open ? "—" : fmt(trade.exit_price, 8)}</small></td>
        <td>${fmt(trade.qty, 8)}</td><td class="${tone(pnl)}">${fmt(pnl, 4)}<small>${open ? "浮动" : "已实现"} USDT</small></td><td>${entryRecord ? CtaPanel.nqGate(entryRecord).text : "窗口外快照"}</td></tr>`;
        })
        .join("");
      el("trades-empty").hidden = rows.length > 0;
      el("trades-empty").textContent =
        "该因子尚未开仓。可点击 K 线查看因子分位与 NQ 是否放行。";
      el("trade-rows")
        .querySelectorAll("tr")
        .forEach((row) => {
          const choose = () => selectTrade(row.dataset.trade);
          row.addEventListener("click", choose);
          row.addEventListener("keydown", (event) => {
            if (["Enter", " "].includes(event.key)) {
              event.preventDefault();
              choose();
            }
          });
        });
    }
    function observations() {
      const rows = state.data.signals.slice().reverse().slice(0, 120);
      el("observation-count").textContent =
        `${state.data.signals.length} 个分钟快照`;
      el("observation-rows").innerHTML = rows
        .map((record) => {
          const signal = record.snapshot.signal || {},
            gate = CtaPanel.nqGate(record),
            event = CtaPanel.actions(record).at(-1);
          return `<tr data-id="${esc(record.signal_id)}" class="${state.selected === record.signal_id ? "selected" : ""}"><td>${time(record.model_ts_ms)}</td>
        <td>${event ? `<span class="${tone(CtaPanel.direction(record))}">${actionText(event)}</span>` : positionText(record.snapshot.position_flag)}</td>
        <td>${pct(signal.decision_value ?? signal.quantile)}</td><td class="nq-timeline" data-mode="${gate.mode}">${gate.text}</td></tr>`;
        })
        .join("");
      el("observation-rows")
        .querySelectorAll("tr")
        .forEach((row) =>
          row.addEventListener("click", () => {
            const record = state.series.byId.get(row.dataset.id);
            selectRecord(record, CtaPanel.actions(record).at(-1));
          }),
        );
    }
    function render() {
      summary();
      factors();
      renderChart();
      trades();
      observations();
      const record =
        state.data.signals.find((r) => r.signal_id === state.selected) ||
        state.data.signals.at(-1);
      const action =
        record &&
        CtaPanel.actions(record).find((a) => a.trade_id === state.trade);
      inspector(record, action);
    }
    async function history(generation) {
      if (!state.strategy) return;
      const response = await json(
        `../api/signal-history?symbol=${encodeURIComponent(state.symbol)}&hours=24&strategy=${encodeURIComponent(state.strategy)}`,
        state.etag ? { "If-None-Match": state.etag } : {},
        state.controller.signal,
      );
      if (generation !== state.generation || response.unchanged) return;
      state.etag = response.etag;
      state.data = response.value;
      state.data.strategy = state.strategy;
      state.data.signals = (state.data.simulation?.records || []).filter(
        (r) => CtaPanel.strategyOf(r) === state.strategy,
      );
      render();
    }
    async function loadSelection() {
      state.controller?.abort();
      state.controller = new AbortController();
      const generation = ++state.generation;
      state.etag = null;
      state.selected = null;
      state.trade = null;
      state.data = null;
      state.series = null;
      chart.clear();
      el("trade-rows").innerHTML = "";
      el("observation-rows").innerHTML = "";
      el("trades-empty").hidden = false;
      el("trades-empty").textContent = "读取该因子的交易记录…";
      for (const id of [
        "closed-trades",
        "position-state",
        "pnl",
        "factor-quantile",
      ])
        el(id).textContent = "—";
      el("record-state").textContent = "等待选择";
      inspector(null);
      el("factor-name").textContent = strategyLabel(state.strategy) || "暂无策略";
      watchlist();
      factors();
      if (!state.strategy) {
        chart.clear();
        el("chart-empty").hidden = false;
        el("chart-empty").textContent = "该币尚未配置因子。";
        return;
      }
      el("chart-empty").hidden = false;
      el("chart-empty").textContent = "读取独立因子交易…";
      try {
        await history(generation);
        error("");
      } catch (err) {
        if (err.name !== "AbortError" && generation === state.generation)
          error(`读取因子历史失败：${err.message}`);
      }
    }
    async function selectSymbol(symbol) {
      if (state.symbol === symbol) return;
      const previousFactor = state.strategy;
      state.symbol = symbol;
      state.zoom = null;
      const available = choices(symbol);
      state.strategy = available.includes(previousFactor)
        ? previousFactor
        : available[0] || "";
      el("market-symbol").textContent = symbol.replace(/USDT$/, " ");
      await loadSelection();
    }
    async function selectFactor(factor) {
      if (state.strategy === factor || !choices(state.symbol).includes(factor))
        return;
      state.strategy = factor;
      await loadSelection();
    }
    async function refresh() {
      if (state.inFlight) return;
      state.inFlight = true;
      const generation = state.generation;
      try {
        state.overview = (await json("../api/signal-panel")).value;
        watchlist();
        factors();
        const online =
          !state.overview.journal_error &&
          state.overview.signal_journal_healthy === true &&
          state.overview.signal_status_age_ms < 90000;
        const trading = state.overview.trading_enabled;
        el("service-state").textContent =
          `${online ? "信号在线" : "历史查看"} · ${trading === true ? "实盘已启用" : trading === false ? "实盘关闭" : "交易状态未知"}`;
        el("service-state").className =
          `live ${online && trading === false ? "" : "warn"}`;
        error(
          state.overview.journal_error
            ? `历史读取失败：${state.overview.journal_error}`
            : state.overview.simulation?.error
              ? `回放错误：${state.overview.simulation.error}`
              : "",
        );
        if (!state.symbol) {
          const first =
            state.overview.symbols.find(
              (row) => row.active && choices(row.symbol).length,
            ) || state.overview.symbols[0];
          if (first) await selectSymbol(first.symbol);
        } else if (!choices(state.symbol).includes(state.strategy)) {
          state.strategy = choices(state.symbol)[0] || "";
          await loadSelection();
        } else if (generation === state.generation) await history(generation);
      } catch (err) {
        if (err.name !== "AbortError") error(`服务连接失败：${err.message}`);
      } finally {
        state.inFlight = false;
      }
    }
    el("strategy-filter").addEventListener("change", () =>
      selectFactor(el("strategy-filter").value),
    );
    document.querySelectorAll("[data-hours]").forEach((button) =>
      button.addEventListener("click", () => {
        state.hours = Number(button.dataset.hours);
        state.zoom = null;
        document
          .querySelectorAll("[data-hours]")
          .forEach((item) => item.classList.toggle("active", item === button));
        if (state.data) renderChart();
      }),
    );
    if (!chart) {
      error("图表资源未加载。");
      return;
    }
    chart.on("datazoom", (event) => {
      const item = event.batch?.[0] || event;
      state.zoom = { start: item.start, end: item.end };
    });
    chart.on("click", (event) => {
      if (event.data?.record)
        selectRecord(event.data.record, event.data.action);
      else if (event.seriesType === "candlestick")
        selectRecord(
          CtaPanel.preferred(
            state.series.observations.get(
              state.series.times[event.dataIndex],
            ) || [],
          ),
        );
    });
    chart.getZr().on("click", (event) => {
      if (event.target || !state.data) return;
      const point = [event.offsetX, event.offsetY];
      if (chart.containPixel({ gridIndex: 0 }, point)) {
        const index = Math.round(
          chart.convertFromPixel({ xAxisIndex: 0 }, point[0]),
        );
        if (Number.isFinite(index))
          selectRecord(
            CtaPanel.preferred(
              state.series.observations.get(state.series.times[index]) || [],
            ),
          );
      }
    });
    new ResizeObserver(() => chart.resize()).observe(el("chart"));
    refresh();
    setInterval(refresh, 5000);
  })();
