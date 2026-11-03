use super::config::{Calculation, Config, Interval, Market, MissingPolicy, Parameters, Plan};
use super::normalize::NormalizeState;
use anyhow::{Context, Result};
use mkt_parsers::msg::{mkt_msg::Level, trade_flow_feature_msg::TradeFlowFeatureMsg};
use period_pbs::{
    pb::{IncrementOrderBookInfo, PeriodMessage, TradeInfo},
    period::normalize_timestamp_ms,
};
use rolling_common::exact_rolling_window::ExactRollingWindow;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::factor_pub::{
    depth_book::DepthFactorId,
    fusion_factor_pub::{app::BaselineReplayState, SymbolFactorPlan},
    trade_flow_feature_pub::local_baseline::{
        BaselineBar, LocalBaselineAggregator, MultiMinuteBaseline,
    },
};
use crate::{common::amount_threshold::AmountThreshold, depth_pub::orderbook::OrderBook};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub raw: Option<f64>,
    pub value: Option<f64>,
    pub quantile: Option<f64>,
    pub median: Option<f64>,
    pub ready: bool,
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

/// One unversioned factor output. Vector factors (NQ) and scalar factors use
/// exactly the same field representation; no account or entry-rule context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FactorPoint {
    pub factor_id: String,
    pub ts_ms: i64,
    pub fields: BTreeMap<String, Field>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum FactorEvent {
    Point {
        generation: String,
        point: FactorPoint,
    },
    Removed {
        generation: String,
        factor_id: String,
    },
}
impl FactorPoint {
    pub fn ready(&self) -> bool {
        !self.fields.is_empty() && self.fields.values().all(|v| v.ready)
    }
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

pub use super::catalog::requires_amount_thresholds;

struct Transform {
    normalize: NormalizeState,
    ranks: ExactRollingWindow,
    min_periods: usize,
}
impl Transform {
    fn new(params: &Parameters) -> Self {
        Self {
            normalize: NormalizeState::new(
                params.normalize_window,
                params.normalize_min_periods,
                params.clip_zscore,
            ),
            ranks: ExactRollingWindow::new(params.rank_window),
            min_periods: params.rank_min_periods,
        }
    }
    fn observe(&mut self, raw: Option<f64>, standardize: bool) -> Field {
        let normalized = if standardize {
            self.normalize.observe(raw.unwrap_or(f64::NAN))
        } else {
            raw
        };
        // An invalid current raw observation cannot become a tradable carried
        // z-score, even though causal normalization retains its previous value.
        let value = raw.and(normalized).and_then(finite);
        self.ranks.observe_slot(value.unwrap_or(f64::NAN));
        let quantile = self.ranks.percentile_rank_last();
        Field {
            raw,
            value,
            quantile,
            median: self.ranks.quantile_linear(0.5),
            ready: value.is_some() && quantile.is_some() && self.ranks.len() >= self.min_periods,
        }
    }
}

struct Nq {
    params: Parameters,
    closes: VecDeque<Option<f64>>,
    long: ExactRollingWindow,
    short: ExactRollingWindow,
    previous: BTreeMap<String, Field>,
}
impl Nq {
    fn new(params: &Parameters) -> Self {
        Self {
            params: params.clone(),
            closes: VecDeque::new(),
            long: ExactRollingWindow::new(params.rank_window),
            short: ExactRollingWindow::new(params.rank_window),
            previous: BTreeMap::from([
                ("long".into(), Field::default()),
                ("short".into(), Field::default()),
            ]),
        }
    }
    fn observe(&mut self, mid: Option<f64>) -> BTreeMap<String, Field> {
        // The observation timestamp identifies the decision bar. Its NQ uses
        // only the distribution and close from the preceding bar, including
        // an explicit unknown value after a missing source bar.
        let current = self.observe_current(mid);
        std::mem::replace(&mut self.previous, current)
    }
    fn observe_current(&mut self, mid: Option<f64>) -> BTreeMap<String, Field> {
        let valid = mid.filter(|p| p.is_finite() && *p > 0.0);
        if valid.is_none() && self.params.missing_policy == MissingPolicy::ValidSamples {
            return BTreeMap::from([
                ("long".into(), Field::default()),
                ("short".into(), Field::default()),
            ]);
        }
        self.closes.push_back(valid);
        while self.closes.len() > self.params.lookback {
            self.closes.pop_front();
        }
        let low = self
            .closes
            .iter()
            .flatten()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let high = self
            .closes
            .iter()
            .flatten()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let buy = valid.and_then(|p| finite((p - low) / low));
        let sell = valid.and_then(|p| finite((p - high) / high));
        self.long.observe_slot(buy.unwrap_or(f64::NAN));
        self.short.observe_slot(sell.unwrap_or(f64::NAN));
        let field = |raw: Option<f64>, window: &ExactRollingWindow| {
            let quantile = window.percentile_rank_last();
            Field {
                raw,
                value: raw,
                quantile,
                median: window.quantile_linear(0.5),
                ready: raw.is_some()
                    && quantile.is_some()
                    && window.len() >= self.params.rank_min_periods,
            }
        };
        BTreeMap::from([
            ("long".into(), field(buy, &self.long)),
            ("short".into(), field(sell, &self.short)),
        ])
    }
}

struct Spread {
    watermarks: [i64; 2],
    pending: BTreeMap<i64, [Option<Option<f64>>; 2]>,
    transform: Transform,
}
impl Spread {
    fn new(parameters: &Parameters) -> Self {
        Self {
            watermarks: [0; 2],
            pending: BTreeMap::new(),
            transform: Transform::new(parameters),
        }
    }
    fn observe(&mut self, side: usize, ts: i64, mid: Option<f64>) -> Vec<(i64, Field)> {
        if ts <= self.watermarks[side] {
            return Vec::new();
        }
        self.watermarks[side] = ts;
        self.pending.entry(ts).or_insert([None, None])[side] = Some(mid);
        let watermark = self.watermarks[0].min(self.watermarks[1]);
        let mut output = Vec::new();
        while self
            .pending
            .first_key_value()
            .is_some_and(|(ts, _)| *ts <= watermark)
        {
            let (ts, pair) = self.pending.pop_first().unwrap();
            let raw = pair[0]
                .flatten()
                .zip(pair[1].flatten())
                .filter(|(a, b)| *a > 0.0 && *b > 0.0)
                .and_then(|(a, b)| finite((a - b) / a));
            output.push((ts, self.transform.observe(raw, false)));
        }
        // The later market must not accumulate unlimited state during an outage
        // of its counterpart. Missing values stay unknown when it recovers.
        let cutoff = self.watermarks[0].max(self.watermarks[1]) - 7 * 86_400_000;
        self.pending.retain(|ts, _| *ts >= cutoff);
        output
    }
}

enum Special {
    Nq(Nq),
    Spread(Spread),
}

type SeriesKey = (Market, String);
type GroupKey = (Market, String, Interval);

struct QuoteClock {
    interval: Interval,
    start: Option<i64>,
    last: Option<(i64, f64)>,
}
impl QuoteClock {
    fn new(interval: Interval) -> Self {
        Self {
            interval,
            start: None,
            last: None,
        }
    }
    fn observe(&mut self, ts: i64, mid: Option<f64>) -> Vec<(i64, Option<f64>)> {
        let start = ts.div_euclid(self.interval.millis()) * self.interval.millis();
        let current = self.start.get_or_insert(start);
        if start < *current {
            return Vec::new();
        }
        let mut output = Vec::new();
        while *current < start {
            let end = *current + self.interval.millis();
            let close = self
                .last
                .filter(|(t, _)| *t >= end - 60_000 && *t < end)
                .map(|(_, p)| p);
            output.push((end, close));
            *current = end;
            self.last = None;
        }
        self.last = mid.map(|p| (ts, p));
        output
    }
}

struct Series {
    baseline: Option<LocalBaselineAggregator>,
    book: OrderBook,
    update_id: i64,
    last_book_ts: i64,
    book_initialized: bool,
    clocks: BTreeMap<Interval, QuoteClock>,
    threshold: Option<AmountThreshold>,
}
impl Series {
    fn on_book(&mut self, book: &IncrementOrderBookInfo) -> Vec<(Interval, i64, Option<f64>)> {
        let ts = normalize_timestamp_ms(book.timestamp);
        if ts <= 0 || ts < self.last_book_ts {
            return Vec::new();
        }
        self.last_book_ts = ts;
        let bids = book
            .bids
            .iter()
            .map(|p| (p.price, p.amount))
            .collect::<Vec<_>>();
        let asks = book
            .asks
            .iter()
            .map(|p| (p.price, p.amount))
            .collect::<Vec<_>>();
        if book.is_snapshot {
            self.book = OrderBook::new();
            self.book_initialized = true;
        }
        self.update_id = self.update_id.saturating_add(1);
        self.book
            .apply_update(&bids, &asks, self.update_id, ts * 1000);
        if !self.book.is_valid() {
            self.book.prune_crossed_by_best_update_id();
        }
        let mid = self
            .book
            .best_bid_price()
            .zip(self.book.best_ask_price())
            .filter(|(b, a)| {
                self.book_initialized && b.is_finite() && a.is_finite() && *b > 0.0 && a >= b
            })
            .map(|(b, a)| (b + a) * 0.5);
        // Close the previous bucket before using this book's price.
        let output = self
            .clocks
            .iter_mut()
            .flat_map(|(interval, clock)| {
                clock
                    .observe(ts, mid)
                    .into_iter()
                    .map(|(ts, p)| (*interval, ts, p))
                    .collect::<Vec<_>>()
            })
            .collect();
        if let Some(agg) = &mut self.baseline {
            let bids = bids
                .into_iter()
                .map(|(p, a)| Level::from_values(p, a))
                .collect::<Vec<_>>();
            let asks = asks
                .into_iter()
                .map(|(p, a)| Level::from_values(p, a))
                .collect::<Vec<_>>();
            agg.on_retained_book(ts * 1000, book.is_snapshot, &bids, &asks);
        }
        output
    }
    fn on_trade(&mut self, trade: &TradeInfo) {
        let Some(agg) = &mut self.baseline else {
            return;
        };
        let buy = match trade.side.to_ascii_lowercase().as_str() {
            "b" | "buy" => true,
            "s" | "sell" => false,
            _ => return,
        };
        let ts = normalize_timestamp_ms(trade.timestamp) * 1000;
        if ts <= 0 {
            return;
        }
        if let Some(threshold) = self.threshold {
            agg.on_trade_with_threshold(ts, buy, trade.price, trade.amount, threshold);
        } else {
            agg.on_trade(ts, buy, trade.price, trade.amount);
        }
    }
}

struct FormulaGroup {
    interval: Interval,
    resampler: Option<MultiMinuteBaseline>,
    evaluator: BaselineReplayState,
    plan: SymbolFactorPlan,
    names: Vec<String>,
    transforms: Vec<Transform>,
    needs_threshold: Vec<bool>,
    last_ts: i64,
}
impl FormulaGroup {
    fn retain(&mut self, plan: &Plan) -> Result<()> {
        let keep = self
            .names
            .iter()
            .map(|name| plan.calculations.contains_key(name))
            .collect::<Vec<_>>();
        let factors = self
            .plan
            .factor_names()
            .enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(_, name)| name.to_owned())
            .collect::<Vec<_>>();
        self.names = std::mem::take(&mut self.names)
            .into_iter()
            .enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(_, v)| v)
            .collect();
        self.transforms = std::mem::take(&mut self.transforms)
            .into_iter()
            .enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(_, v)| v)
            .collect();
        self.needs_threshold = std::mem::take(&mut self.needs_threshold)
            .into_iter()
            .enumerate()
            .filter(|(i, _)| keep[*i])
            .map(|(_, v)| v)
            .collect();
        self.plan = SymbolFactorPlan::from_factor_names("cta-factor-calc", factors)?;
        Ok(())
    }
    fn new(calculations: &[&Calculation]) -> Result<Self> {
        let interval = calculations[0].id.interval;
        let names = calculations.iter().map(|c| c.id.name()).collect();
        let plan = SymbolFactorPlan::from_factor_names(
            "cta-factor-calc",
            calculations.iter().map(|c| c.id.factor.clone()).collect(),
        )?;
        Ok(Self {
            interval,
            resampler: (interval != Interval::Minute)
                .then(|| MultiMinuteBaseline::new(interval.millis())),
            evaluator: BaselineReplayState::default(),
            plan,
            names,
            transforms: calculations
                .iter()
                .map(|c| Transform::new(&c.parameters))
                .collect(),
            needs_threshold: calculations
                .iter()
                .map(|c| requires_amount_thresholds(&c.id.factor))
                .collect(),
            last_ts: 0,
        })
    }
    fn observe(
        &mut self,
        symbol: &str,
        market: Market,
        minute: &BaselineBar,
        threshold_ready: bool,
    ) -> Result<Vec<FactorPoint>> {
        let bars = self
            .resampler
            .as_mut()
            .map_or_else(|| vec![minute.clone()], |r| r.push(minute));
        let mut output = Vec::new();
        for mut bar in bars {
            let end = bar.start_ms + self.interval.millis();
            if end <= self.last_ts {
                continue;
            }
            self.last_ts = end;
            bar.start_ms = end;
            let payload = bar.to_trade_flow_feature_payload(symbol, market.venue().to_u8())?;
            let msg = TradeFlowFeatureMsg::from_bytes(payload.as_ref())?;
            let baseline_ready = [0, 1, 2, 3, 23, 24, 25]
                .iter()
                .all(|i| msg.values[*i].is_finite() && msg.values[*i] > 0.0);
            if baseline_ready {
                self.evaluator.push(msg)?;
            }
            let raw = if baseline_ready {
                self.evaluator.factor_values_with_tp_vpi_006_min_periods(
                    &self.plan,
                    if self.interval == Interval::Minute {
                        360
                    } else {
                        280
                    },
                )
            } else {
                vec![f64::NAN; self.names.len()]
            };
            for (i, name) in self.plan.factor_names().enumerate() {
                let value = if let Some(depth) = DepthFactorId::from_name(name) {
                    bar.full_depth.as_ref().map(|book| book.factor(depth))
                } else {
                    finite(raw[i])
                }
                .and_then(finite);
                let value = value.filter(|_| !self.needs_threshold[i] || threshold_ready);
                let field = self.transforms[i].observe(value, true);
                output.push(FactorPoint {
                    factor_id: self.names[i].clone(),
                    ts_ms: end,
                    fields: BTreeMap::from([("value".into(), field)]),
                });
            }
        }
        Ok(output)
    }
}

pub struct Engine {
    pub plan: Plan,
    series: BTreeMap<SeriesKey, Series>,
    formula: BTreeMap<GroupKey, FormulaGroup>,
    special: BTreeMap<String, Special>,
    quote_jobs: BTreeMap<GroupKey, Vec<String>>,
    pub latest: BTreeMap<String, FactorPoint>,
    /// One preceding spread observation is needed by the entry filter. This
    /// history belongs to the replay generation, including during handover.
    pub previous_spreads: BTreeMap<String, FactorPoint>,
}
impl Engine {
    pub fn new(plan: Plan, config: &Config) -> Result<Self> {
        let mut engine = Self {
            plan,
            series: BTreeMap::new(),
            formula: BTreeMap::new(),
            special: BTreeMap::new(),
            quote_jobs: BTreeMap::new(),
            latest: BTreeMap::new(),
            previous_spreads: BTreeMap::new(),
        };
        let mut groups: BTreeMap<GroupKey, Vec<&Calculation>> = BTreeMap::new();
        for (name, calculation) in &engine.plan.calculations {
            let id = &calculation.id;
            for market in id.markets() {
                let key = (market, id.symbol.clone());
                let series = engine.series.entry(key).or_insert_with(|| {
                    let threshold = config
                        .amount_thresholds
                        .get(&format!("{}:{}", market.name(), id.symbol))
                        .map(|p| AmountThreshold {
                            medium_notional_threshold: p[0],
                            large_notional_threshold: p[1],
                        });
                    Series {
                        baseline: None,
                        book: OrderBook::new(),
                        update_id: 0,
                        last_book_ts: 0,
                        book_initialized: false,
                        clocks: BTreeMap::new(),
                        threshold,
                    }
                });
                if id.is_formula() {
                    let agg = series
                        .baseline
                        .get_or_insert_with(LocalBaselineAggregator::new);
                    if DepthFactorId::from_name(&id.factor).is_some() {
                        agg.enable_full_depth();
                    }
                } else {
                    series
                        .clocks
                        .entry(id.interval)
                        .or_insert_with(|| QuoteClock::new(id.interval));
                    engine
                        .quote_jobs
                        .entry((market, id.symbol.clone(), id.interval))
                        .or_default()
                        .push(name.clone());
                }
            }
            if id.is_formula() {
                groups
                    .entry((id.market, id.symbol.clone(), id.interval))
                    .or_default()
                    .push(calculation);
            } else if id.factor == "nq" {
                engine
                    .special
                    .insert(name.clone(), Special::Nq(Nq::new(&calculation.parameters)));
            } else {
                engine.special.insert(
                    name.clone(),
                    Special::Spread(Spread::new(&calculation.parameters)),
                );
            }
        }
        for (key, calculations) in groups {
            engine
                .formula
                .insert(key, FormulaGroup::new(&calculations)?);
        }
        Ok(engine)
    }
    pub fn on_period(
        &mut self,
        market: Market,
        period: &PeriodMessage,
    ) -> Result<Vec<FactorPoint>> {
        let mut points = Vec::new();
        for info in &period.symbol_infos {
            let symbol = normalize_symbol_for_internal(&info.symbol);
            let Some(series) = self.series.get_mut(&(market, symbol.clone())) else {
                continue;
            };
            enum Event<'a> {
                Book(&'a IncrementOrderBookInfo),
                Trade(&'a TradeInfo),
            }
            let mut events = info
                .incs
                .iter()
                .map(Event::Book)
                .chain(info.trades.iter().map(Event::Trade))
                .collect::<Vec<_>>();
            events.sort_by_key(|e| {
                normalize_timestamp_ms(match e {
                    Event::Book(b) => b.timestamp,
                    Event::Trade(t) => t.timestamp,
                })
            });
            let mut quotes = Vec::new();
            for event in events {
                match event {
                    Event::Book(book) => quotes.extend(series.on_book(book)),
                    Event::Trade(trade) => series.on_trade(trade),
                }
            }
            let threshold_ready = series.threshold.is_some();
            let minutes = if let Some(agg) = &mut series.baseline {
                agg.drain_ten_second_bars();
                agg.drain_sixty_second_bars()
            } else {
                Vec::new()
            };
            for (interval, ts, mid) in quotes {
                for name in self
                    .quote_jobs
                    .get(&(market, symbol.clone(), interval))
                    .into_iter()
                    .flatten()
                {
                    let id = &self.plan.calculations[name].id;
                    match self.special.get_mut(name).context("missing factor state")? {
                        Special::Nq(state) => points.push(FactorPoint {
                            factor_id: name.clone(),
                            ts_ms: ts,
                            fields: state.observe(mid),
                        }),
                        Special::Spread(state) => {
                            for (ts, field) in
                                state.observe(usize::from(market != id.market), ts, mid)
                            {
                                points.push(FactorPoint {
                                    factor_id: name.clone(),
                                    ts_ms: ts,
                                    fields: BTreeMap::from([("value".into(), field)]),
                                });
                            }
                        }
                    }
                }
            }
            for minute in minutes {
                for ((group_market, group_symbol, _), group) in self
                    .formula
                    .iter_mut()
                    .filter(|((m, s, _), _)| *m == market && s == &symbol)
                {
                    points.extend(group.observe(
                        group_symbol,
                        *group_market,
                        &minute,
                        threshold_ready,
                    )?);
                }
            }
        }
        points.sort_by(|a, b| (a.ts_ms, &a.factor_id).cmp(&(b.ts_ms, &b.factor_id)));
        points.retain(|p| {
            self.plan.calculations.contains_key(&p.factor_id)
                && self
                    .latest
                    .get(&p.factor_id)
                    .is_none_or(|old| p.ts_ms > old.ts_ms)
        });
        for point in &points {
            if point.factor_id.ends_with("-spread") {
                if let Some(previous) = self
                    .latest
                    .get(&point.factor_id)
                    .filter(|p| p.ts_ms < point.ts_ms)
                {
                    self.previous_spreads
                        .insert(point.factor_id.clone(), previous.clone());
                }
            }
            self.latest.insert(point.factor_id.clone(), point.clone());
        }
        Ok(points)
    }
    pub fn retain_plan(&mut self, plan: Plan) -> Result<()> {
        self.special
            .retain(|name, _| plan.calculations.contains_key(name));
        self.latest
            .retain(|name, _| plan.calculations.contains_key(name));
        self.previous_spreads
            .retain(|name, _| plan.calculations.contains_key(name));
        for names in self.quote_jobs.values_mut() {
            names.retain(|name| plan.calculations.contains_key(name));
        }
        // Formula groups retain their rolling history; deleted formulas stop
        // being evaluated even when other factors still use this input series.
        self.formula.retain(|_, group| {
            group
                .names
                .iter()
                .any(|name| plan.calculations.contains_key(name))
        });
        for group in self.formula.values_mut() {
            group.retain(&plan)?;
        }
        self.quote_jobs.retain(|_, names| !names.is_empty());
        let needed = plan
            .calculations
            .values()
            .flat_map(|c| c.id.markets().map(move |m| (m, c.id.symbol.clone())))
            .collect::<BTreeSet<_>>();
        self.series.retain(|key, _| needed.contains(key));
        for ((market, symbol), series) in &mut self.series {
            series.clocks.retain(|interval, _| {
                self.quote_jobs
                    .contains_key(&(*market, symbol.clone(), *interval))
            });
            if !self
                .formula
                .keys()
                .any(|(m, s, _)| m == market && s == symbol)
            {
                series.baseline = None;
            }
        }
        self.plan = plan;
        Ok(())
    }
}
