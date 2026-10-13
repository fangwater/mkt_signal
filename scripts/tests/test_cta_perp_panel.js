"use strict";
const assert = require("node:assert/strict");
const panel = require("../../web/cta_perp_signals/panel.js");
const start = 1800000000000,
  minute = panel.MINUTE;
function record(i, flag, score = 1, factor = "A") {
  const ts = start + i * minute;
  return {
    signal_id: `${factor}:${i}`,
    symbol: "FILUSDT",
    strategy_name: factor,
    model_ts_ms: ts,
    event_ts_us: (ts + 1) * 1000,
    snapshot: {
      strategy_name: factor,
      position_flag: flag,
      position_qty: flag * 10,
      entry: {
        nq_change_enabled: true,
        nq_long_quantile: 0.5,
        nq_short_quantile: 0.5,
      },
      signal: {
        score,
        score_ready: true,
        filter_ready: true,
        nq_long_value: 0.02,
        nq_short_value: -0.03,
        nq_long_quantile: 0.8,
        nq_short_quantile: 0.2,
      },
    },
    publication: { levels: [] },
    simulation: { position_flag: flag, actions: [] },
  };
}
function action(row, kind, side, tradeId) {
  row.simulation.actions.push({
    kind,
    side,
    trade_id: tradeId,
    ts_ms: row.event_ts_us / 1000,
    price: 100 + Number(row.signal_id.split(":").at(-1)),
    qty: 10,
  });
  return row;
}
const rows = [
  action(record(0, 1), "open", 1, "long"),
  record(1, 1),
  record(2, 1),
  action(record(3, 0, -1), "close", 1, "long"),
  action(record(4, -1, -1), "open", -1, "short"),
  record(5, -1, -1),
  action(record(6, 0, 1), "close", -1, "short"),
  action(record(7, 1), "open", 1, "unclosed"),
];
const trade = (id, side, enter, exit) => ({
  trade_id: id,
  strategy_name: "A",
  symbol: "FILUSDT",
  side,
  entry_ts_ms: start + enter * minute + 1,
  exit_ts_ms: start + exit * minute + 1,
  average_entry_price: 100 + enter,
  exit_price: 100 + exit,
  qty: 10,
  pnl: side * (exit - enter) * 10,
  entry_signal_id: `A:${enter}`,
  exit_signal_id: `A:${exit}`,
});
const data = {
  symbol: "FILUSDT",
  strategy: "A",
  start_ms: start,
  end_ms: start + 8 * minute,
  klines: [...Array(8).keys()].map((i) => ({
    open_time_ms: start + i * minute,
    open: 100,
    close: 101,
    low: 99,
    high: 110,
    volume: 20,
  })),
  signals: rows,
  simulation: {
    trades: [trade("long", 1, 0, 3), trade("short", -1, 4, 6)],
    open_positions: [
      {
        trade_id: "unclosed",
        strategy_name: "A",
        symbol: "FILUSDT",
        side: 1,
        entry_ts_ms: start + 7 * minute + 1,
        qty: 10,
      },
    ],
  },
};
const series = panel.buildSeries(data);
assert.equal(series.long.length, 2);
assert.equal(series.short.length, 1);
assert.equal(series.closes.length, 2);
assert.equal(series.events.length, 5, "holds cannot create extra B/S points");
assert.equal(series.pairs.length, 2, "only closed trades get a B/S connector");
assert.equal(
  series.trades.length,
  3,
  "an open position remains visible without a fabricated close",
);
assert.equal(series.closes[0].label.formatter, "S", "a long closes by selling");
assert.equal(series.closes[1].label.formatter, "B", "a short closes by buying");
assert.equal(
  series.closes[0].value[1],
  103,
  "closing markers use the actual simulated exit price",
);
assert.equal(
  series.long[0].value[0],
  start,
  "entry markers belong to the execution minute",
);
assert.equal(
  panel.modelBucket(rows[0]),
  start - minute,
  "factor snapshots belong to the preceding closed candle",
);
assert.deepEqual(
  series.position.slice(0, 8),
  [1, 1, 1, 0, -1, -1, 0, 1],
  "position changes draw at execution time",
);
assert.equal(
  series.position[8],
  1,
  "the held position carries forward without repeating an entry",
);
assert.equal(series.ohlc[8][0], "-", "missing K lines remain gaps");
assert.equal(
  panel.direction(rows[1]),
  0,
  "a repeated long candidate is an observation, not BUY",
);
assert.equal(
  panel.candidateDirection(rows[1]),
  1,
  "the held candidate still has an NQ direction",
);
const duplicate = panel.buildSeries({
  ...data,
  signals: [...rows, structuredClone(rows[0])],
});
assert.equal(
  duplicate.events.length,
  5,
  "duplicate event delivery cannot repeat an opening marker",
);
const withOtherFactor = panel.buildSeries({
  ...data,
  signals: [...rows, action(record(0, -1, -1, "B"), "open", -1, "other")],
});
assert.equal(
  withOtherFactor.events.length,
  5,
  "a selected factor cannot display another factor's trades",
);
const noSelection = panel.buildSeries({
  ...data,
  strategy: "",
  signals: [...rows, record(0, -1, -1, "B")],
});
assert.equal(
  noSelection.markers.length,
  0,
  "multiple factors must never silently become an aggregate chart",
);
assert.deepEqual(noSelection.position.slice(0, 2), ["-", "-"]);
const late = panel.buildSeries({
  ...data,
  start_ms: start + minute,
  signals: rows.slice(1),
  simulation: { ...data.simulation, context: [rows[0]] },
});
assert.equal(
  late.position[0],
  1,
  "a position opened before the displayed window remains held",
);
assert.equal(
  late.long.length,
  1,
  "window context cannot draw an extra opening",
);
const gap = panel.buildSeries({
  ...data,
  klines: data.klines.filter((c) => c.open_time_ms !== start + 3 * minute),
});
assert.equal(
  gap.closes.length,
  1,
  "an absent candle cannot be replaced by an invented price point",
);
assert.equal(gap.pairs.length, 1);
// NQ checks use frozen per-record thresholds and do not block any close.
const nq = structuredClone(rows[1]);
nq.snapshot.signal.nq_long_quantile = 0.5;
assert.equal(panel.nqGate(nq).mode, "passed", "long includes the 50% boundary");
nq.snapshot.signal.score = -1;
nq.snapshot.signal.nq_short_quantile = 0.5;
assert.equal(
  panel.nqGate(nq).mode,
  "passed",
  "short includes the 50% boundary",
);
nq.snapshot.signal.nq_short_quantile = 0.7;
assert.equal(panel.nqGate(nq).mode, "blocked");
nq.snapshot.signal.score = 1;
assert.equal(
  panel.nqGate(nq).mode,
  "passed",
  "the other NQ side cannot veto an entry",
);
const frozen = structuredClone(nq);
nq.snapshot.entry.nq_long_quantile = 0.8;
assert.equal(panel.nqGate(nq).mode, "blocked");
assert.equal(
  panel.nqGate(frozen).mode,
  "passed",
  "later settings do not rewrite the old NQ gate",
);
nq.snapshot.signal.filter_ready = false;
assert.equal(panel.nqGate(nq).mode, "warming");
nq.snapshot.entry.nq_change_enabled = false;
assert.equal(panel.nqGate(nq).mode, "disabled");
nq.snapshot.entry.nq_change_enabled = true;
nq.snapshot.signal.filter_ready = true;
for (const invalid of [null, undefined, NaN, Infinity, "0.9", -0.1, 1.1]) {
  nq.snapshot.signal.nq_long_quantile = invalid;
  assert.equal(panel.nqGate(nq).mode, "unknown");
}
const close = structuredClone(rows[3]);
close.snapshot.signal.filter_ready = false;
assert.equal(
  panel.nqGate(close).mode,
  "exit",
  "closing the original qty never waits for NQ",
);
nq.snapshot.signal.nq_long_quantile = 0.9;
nq.snapshot.signal.score = 0;
assert.equal(
  panel.nqGate(nq).mode,
  "neutral",
  "NQ alone cannot generate a trade",
);
// Actual target changes are distinct from candidate directions and config republishing.
const target = record(0, 1);
delete target.simulation;
target.publication.target = {
  qty: 10,
  previous_qty: 0,
  published_ts_us: (start + 1) * 1000,
};
assert.equal(panel.direction(target), 1);
const hold = structuredClone(target);
hold.publication.target.previous_qty = 10;
assert.equal(
  panel.direction(hold),
  0,
  "republishing an unchanged qty has no trading direction",
);
const clear = structuredClone(target);
clear.publication.target = {
  qty: "clear",
  previous_qty: 10,
  published_ts_us: (start + minute + 1) * 1000,
};
assert.equal(panel.direction(clear), -1);
assert.equal(panel.nqGate(clear).mode, "exit");
const oldAggregate = structuredClone(target);
delete oldAggregate.publication.target;
oldAggregate.publication.levels = [{ published_ts_us: (start + 1) * 1000 }];
assert.equal(
  panel.direction(oldAggregate),
  0,
  "old aggregate observations cannot invent a factor transaction",
);
console.log(
  "Single-factor B/S pairs, fixed positions, unclosed trades, causal markers, isolation and NQ gates passed",
);
