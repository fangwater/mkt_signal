use super::config::Config;
use crate::cta_factor_calc::{
    config::{Interval, Parameters},
    engine::{Field, Nq, Transform},
};
use crate::cta_perp::{
    config::{CtaPerpConfig, PositionAlgorithm},
    factors::WeightedBar,
    interval::BarInterval,
    positions::next_position_flag,
};
use crate::factor_pub::cn_features::{
    FuturesDepth5, FuturesFactorPlan, FuturesFusionInput, FuturesFusionState, FuturesTradeBar,
    FUTURES_TRADE_FIELD_COUNT,
};
use anyhow::{ensure, Context, Result};
use cn_market_proto::{Book5, MinuteBar};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

pub(super) fn parameters(interval: Interval, nq: bool) -> Parameters {
    let mut params = Parameters::for_interval(interval, nq);
    if interval == Interval::Hour && !nq {
        params.normalize_window = 120;
        params.normalize_min_periods = 60;
    }
    params
}

pub(super) fn hourly_entry(q: f64, long: f64, short: f64) -> f64 {
    if q > long {
        1.
    } else if q < short {
        -1.
    } else {
        0.
    }
}

pub(super) fn hourly_exit(current: i8, q: f64, long: f64, short: f64) -> bool {
    (current > 0 && q <= long) || (current < 0 && q >= short)
}

#[derive(Clone)]
pub struct Bar {
    pub start_ms: i64,
    pub end_ms: i64,
    pub trading_day: u32,
    pub minutes: usize,
    pub trade: [f64; FUTURES_TRADE_FIELD_COUNT],
    pub book: Book5,
    pub mid: f64,
    additive_hfq: bool,
}
/// Aggregate native minutes. Hourly bars retain the existing CN segment
/// convention; shorter bars align to clock boundaries as in Prep.
pub struct Bars {
    rows: Vec<MinuteBar>,
    interval: Interval,
    started: bool,
    additive_hfq: bool,
}
impl Default for Bars {
    fn default() -> Self {
        Self::new(Interval::Hour)
    }
}
impl Bars {
    pub fn new(interval: Interval) -> Self {
        Self {
            rows: Vec::new(),
            interval,
            started: false,
            additive_hfq: false,
        }
    }
    pub fn last_minute_ms(&self) -> Option<i64> {
        self.rows.last().map(|r| r.open_time_ms)
    }
    pub fn push(&mut self, row: MinuteBar, multiple: f64) -> Result<Vec<Bar>> {
        self.push_with_basis(row, multiple, false)
    }
    pub fn push_hfq(&mut self, row: MinuteBar, multiple: f64) -> Result<Vec<Bar>> {
        self.push_with_basis(row, multiple, true)
    }
    fn push_with_basis(
        &mut self,
        row: MinuteBar,
        multiple: f64,
        additive_hfq: bool,
    ) -> Result<Vec<Bar>> {
        let mut out = Vec::new();
        let valid_price = |value: f64| value.is_finite() && (additive_hfq || value > 0.);
        ensure!(
            row.mid_price.is_some_and(valid_price)
                || (self.interval == Interval::Hour && valid_price(row.close)),
            "minute is missing a valid mid_price"
        );
        if !self.started && self.interval != Interval::Hour {
            if row.open_time_ms.rem_euclid(self.interval.millis()) != 0 {
                return Ok(out);
            }
            self.started = true;
        }
        if let Some(last) = self.rows.last() {
            ensure!(
                row.open_time_ms > last.open_time_ms,
                "minute timestamps must increase"
            );
            if row.open_time_ms - last.open_time_ms != 60_000
                || row.trading_day != last.trading_day
                || additive_hfq != self.additive_hfq
                || (self.interval != Interval::Hour
                    && row.open_time_ms.div_euclid(self.interval.millis())
                        != last.open_time_ms.div_euclid(self.interval.millis()))
            {
                out.push(self.finish(multiple)?.context("missing hourly tail")?);
            }
        }
        self.additive_hfq = additive_hfq;
        self.rows.push(row);
        if self.rows.len() == (self.interval.millis() / 60_000) as usize {
            out.push(self.finish(multiple)?.context("missing complete bar")?);
        }
        Ok(out)
    }
    pub fn finish(&mut self, multiple: f64) -> Result<Option<Bar>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let rows = std::mem::take(&mut self.rows);
        let first = &rows[0];
        let last = rows.last().context("empty hour")?;
        ensure!(
            multiple.is_finite() && multiple > 0.,
            "verified multiplier required"
        );
        let mut v = [0.; FUTURES_TRADE_FIELD_COUNT];
        let traded: Vec<_> = rows.iter().filter(|r| r.volume_lots > 0.).collect();
        v[0] = traded.first().map_or(last.open, |r| r.open);
        v[1] = if traded.is_empty() {
            last.snapshot_high
        } else {
            f64::NEG_INFINITY
        };
        v[2] = if traded.is_empty() {
            last.snapshot_low
        } else {
            f64::INFINITY
        };
        v[3] = traded.last().map_or(last.close, |r| r.close);
        for r in &rows {
            ensure!(
                r.open_time_ms % 60_000 == 0
                    && r.volume_lots >= 0.
                    && (r.volume_lots == 0.
                        || [r.open, r.snapshot_high, r.snapshot_low, r.close]
                            .iter()
                            .all(|v| v.is_finite() && (self.additive_hfq || *v > 0.))),
                "invalid traded minute"
            );
            if r.volume_lots > 0. {
                v[1] = v[1].max(r.snapshot_high);
                v[2] = v[2].min(r.snapshot_low);
            }
            let s = r
                .size_buckets
                .as_ref()
                .context("gRPC minute has no required size buckets")?;
            let sums = [
                (4, r.volume_lots),
                (5, r.turnover_cny),
                (7, f64::from(r.snapshot_trades)),
                (8, r.buy_snapshot_trades),
                (9, r.sell_snapshot_trades),
                (10, r.buy_turnover_cny),
                (11, r.sell_turnover_cny),
                (12, r.buy_volume_lots),
                (13, r.sell_volume_lots),
                (14, s.large_turnover_cny),
                (15, s.medium_turnover_cny),
                (16, s.small_turnover_cny),
                (17, s.large_buy_cny),
                (18, s.large_sell_cny),
                (19, s.medium_buy_cny),
                (20, s.medium_sell_cny),
                (21, s.small_buy_cny),
                (22, s.small_sell_cny),
                (26, r.net_buy_turnover_cny),
                (27, r.net_buy_volume_lots),
                (29, s.net_buy_large_cny),
                (30, s.net_buy_medium_cny),
                (31, s.net_buy_small_cny),
            ];
            for (i, x) in sums {
                ensure!(x.is_finite(), "nonfinite baseline field");
                v[i] += x;
            }
            ensure!(
                (s.large_turnover_cny + s.medium_turnover_cny + s.small_turnover_cny
                    - r.turnover_cny)
                    .abs()
                    <= r.turnover_cny.abs() * 1e-8 + 1e-6,
                "size buckets do not cover turnover"
            );
        }
        v[6] = if v[7] > 0. { v[5] / v[7] } else { 0. };
        let quotes: [fn(&MinuteBar) -> (f64, f64); 3] = [
            |row| (row.vwap, row.volume_lots),
            |row| (row.buy_vwap.unwrap_or(f64::NAN), row.buy_volume_lots),
            |row| (row.sell_vwap.unwrap_or(f64::NAN), row.sell_volume_lots),
        ];
        for ((i, amount, volume), quote) in [(23, 5, 4), (24, 10, 12), (25, 11, 13)]
            .into_iter()
            .zip(quotes)
        {
            v[i] = if v[volume] > 0. {
                if self.additive_hfq {
                    rows.iter()
                        .map(quote)
                        .filter(|(_, lots)| *lots > 0.)
                        .map(|(price, lots)| price * lots)
                        .sum::<f64>()
                        / v[volume]
                } else {
                    v[amount] / v[volume] / multiple
                }
            } else {
                quote(last).0
            };
        }
        v[28] = if v[5] > 0. { v[26] / v[5] } else { f64::NAN };
        let book = last
            .book
            .clone()
            .context("gRPC minute has no native five-level book")?;
        ensure!(
            book.bids.len() == 5 && book.asks.len() == 5,
            "native book must have exactly five levels"
        );
        let mid = last
            .mid_price
            .filter(|v| v.is_finite() && (self.additive_hfq || *v > 0.))
            .or_else(|| {
                (self.interval == Interval::Hour
                    && last.close.is_finite()
                    && (self.additive_hfq || last.close > 0.))
                    .then_some(last.close)
            })
            .context("minute is missing a valid mid_price")?;
        Ok(Some(Bar {
            start_ms: first.open_time_ms,
            end_ms: last.open_time_ms + 60_000,
            trading_day: last.trading_day,
            minutes: rows.len(),
            trade: v,
            book,
            mid,
            additive_hfq: self.additive_hfq,
        }))
    }
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    pub contract: String,
    pub last_decision_ms: i64,
    #[serde(default = "one_lot")]
    pub lots_per_rule: i32,
    pub flags: BTreeMap<String, i8>,
    pub retired_contracts: Vec<String>,
}
fn one_lot() -> i32 {
    1
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            contract: String::new(),
            last_decision_ms: 0,
            lots_per_rule: 1,
            flags: BTreeMap::new(),
            retired_contracts: Vec::new(),
        }
    }
}
#[derive(Clone, Serialize)]
pub struct RuleState {
    pub factor: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub raw: Option<f64>,
    pub value: Option<f64>,
    pub quantile: Option<f64>,
    pub ready: bool,
    pub flag: i8,
    pub target_lots: i32,
}

pub struct Engine {
    cfg: Config,
    plan: Option<FuturesFactorPlan>,
    formula_indices: Vec<usize>,
    formula: FuturesFusionState,
    transforms: Vec<Transform>,
    nq: Nq,
    pub ledger: Ledger,
    pub last_decision_day: Option<u32>,
    pub latest: Vec<RuleState>,
    pub last_end_ms: i64,
    pub hours: usize,
    observations: VecDeque<(Bar, String, f64)>,
}
impl Engine {
    pub fn new(cfg: &Config, ledger: Ledger) -> Result<Self> {
        ensure!(
            (1..=100).contains(&ledger.lots_per_rule)
                && ledger.flags.values().all(|v| matches!(v, -1..=1)),
            "invalid persisted rule position"
        );
        let interval = cfg.interval_kind()?;
        let factor_names = cfg.factor_names();
        let formula_indices = cfg
            .rules
            .iter()
            .map(|r| {
                factor_names
                    .binary_search(&r.formula_name())
                    .map_err(|_| anyhow::anyhow!("formula missing from plan: {}", r.factor))
            })
            .collect::<Result<_>>()?;
        let plan = if factor_names.is_empty() {
            ensure!(!cfg.enabled, "empty frequency must be disabled");
            None
        } else {
            Some(FuturesFactorPlan::from_factor_names(factor_names)?)
        };
        Ok(Self {
            cfg: cfg.clone(),
            plan,
            formula_indices,
            formula: FuturesFusionState::default(),
            transforms: cfg
                .rules
                .iter()
                .map(|_| Transform::new(&parameters(interval, false), interval))
                .collect(),
            nq: Nq::new(&parameters(interval, true)),
            ledger,
            last_decision_day: None,
            latest: Vec::new(),
            last_end_ms: 0,
            hours: 0,
            observations: VecDeque::new(),
        })
    }
    pub fn reconfigure(&self, cfg: &Config) -> Result<Self> {
        let mut next = Self::new(cfg, self.ledger.clone())?;
        for (hour, contract, multiple) in &self.observations {
            next.observe(hour, contract, *multiple, false)?;
        }
        ensure!(
            next.ready() || (!cfg.enabled && (cfg.rules.is_empty() || cfg.interval != "1h")),
            "changed factors are not ready on the factual hourly history"
        );
        ensure!(
            next.ledger == self.ledger,
            "configuration warm-up changed factual positions"
        );
        next.last_decision_day = self.last_decision_day;
        Ok(next)
    }
    pub fn ready(&self) -> bool {
        !self.cfg.rules.is_empty()
            && self.latest.len() == self.cfg.rules.len()
            && self.latest.iter().all(|r| r.ready)
    }
    pub fn observe(
        &mut self,
        hour: &Bar,
        contract: &str,
        multiple: f64,
        live: bool,
    ) -> Result<bool> {
        if hour.end_ms <= self.last_end_ms {
            return Ok(false);
        }
        ensure!(
            hour.minutes > 0 && hour.minutes <= 60 && hour.end_ms > hour.start_ms,
            "invalid closed native hourly bounds"
        );
        let array = |levels: &[cn_market_proto::BookLevel], price: bool| -> Vec<f64> {
            levels
                .iter()
                .map(|l| if price { l.price } else { l.volume_lots }.unwrap_or(f64::NAN))
                .collect()
        };
        let depth_constructor = if hour.additive_hfq {
            FuturesDepth5::from_additive_hfq_slices
        } else {
            FuturesDepth5::from_slices
        };
        let depth = depth_constructor(
            &array(&hour.book.bids, true),
            &array(&hour.book.bids, false),
            &array(&hour.book.asks, true),
            &array(&hour.book.asks, false),
        )?;
        let input = FuturesFusionInput {
            ts_ms: hour.end_ms,
            symbol: self.cfg.symbol.clone(),
            trading_day: hour.trading_day,
            trade: FuturesTradeBar { values: hour.trade },
            depth: Some(depth),
            quality_flags: 0,
            volume_multiple: multiple,
            volume_multiple_verified: true,
        };
        if hour.additive_hfq {
            self.formula.push_additive_hfq_continuous(input)?;
        } else {
            self.formula.push_continuous(input)?;
        }
        let raw = match &self.plan {
            Some(plan) => self.formula.factor_values(plan)?,
            None => Vec::new(),
        };
        let nq = self.nq.observe(Some(hour.mid));
        self.last_end_ms = hour.end_ms;
        self.hours += 1;
        let mut points = Vec::new();
        let mut all_ready = true;
        for ((rule, transform), &formula_index) in self
            .cfg
            .rules
            .iter()
            .zip(&mut self.transforms)
            .zip(&self.formula_indices)
        {
            let raw = raw[formula_index];
            let f: Field = if self.cfg.interval == "1h" {
                transform.observe_with_forward_fill(raw)
            } else {
                transform.observe(raw, true)
            };
            let gates = if self.cfg.entry.nq_change_enabled {
                rule.gates()?
            } else {
                None
            };
            let ready = f.ready
                && (self.cfg.interval == "1h"
                    || gates.is_none_or(|_| nq.values().all(|v| v.ready)));
            all_ready &= ready;
            points.push((rule.clone(), f, gates, ready));
        }
        // Preheat never advances a position or decision clock. Apply an entire
        // frequency atomically only after every configured dependency is ready.
        let publish =
            self.cfg.enabled && live && all_ready && hour.end_ms > self.ledger.last_decision_ms;
        if publish {
            self.ledger.lots_per_rule = self.cfg.lots_per_rule;
            let keep_attribution = self.cfg.publish_per_factor()
                && self
                    .cfg
                    .publication_settings()
                    .is_some_and(|p| p.enabled_for(&self.cfg.symbol));
            self.ledger.flags.retain(|factor, flag| {
                if self.cfg.rules.iter().any(|r| r.key() == factor) {
                    return true;
                }
                // Manager still requires an explicit zero for a retired member.
                *flag = 0;
                keep_attribution
            });
        }
        if publish && self.ledger.contract != contract {
            if !self.ledger.contract.is_empty()
                && !self
                    .ledger
                    .retired_contracts
                    .contains(&self.ledger.contract)
            {
                self.ledger
                    .retired_contracts
                    .push(self.ledger.contract.clone());
            }
            self.ledger.contract = contract.into();
        }
        self.latest.clear();
        for (rule, f, gates, ready) in points {
            let current = *self.ledger.flags.get(rule.key()).unwrap_or(&0);
            let flag = if publish {
                let q = f.quantile.context("ready factor missing rank")?;
                let q = if rule.sign > 0 { q } else { 1. - q };
                let eps = 4. * f64::EPSILON;
                let direction = if self.cfg.interval == "1h" {
                    hourly_entry(
                        q,
                        self.cfg.entry.factor_long_quantile,
                        self.cfg.entry.factor_short_quantile,
                    )
                } else if q - self.cfg.entry.factor_long_quantile > eps {
                    1.
                } else if self.cfg.entry.factor_short_quantile - q > eps {
                    -1.
                } else {
                    0.
                };
                let mut entry = self.cfg.entry.clone();
                entry.nq_change_enabled = gates.is_some();
                if let Some((long, short)) = gates {
                    entry.nq_long_quantile = long;
                    entry.nq_short_quantile = short;
                }
                let signal = CtaPerpConfig {
                    bar_interval: match self.cfg.interval_kind()? {
                        Interval::Minute => BarInterval::OneMinute,
                        Interval::FiveMinutes => BarInterval::FiveMinutes,
                        Interval::FifteenMinutes => BarInterval::FifteenMinutes,
                        Interval::Hour => BarInterval::OneHour,
                    },
                    enabled: true,
                    max_pos_u: 1.,
                    position_algorithm: PositionAlgorithm::V0,
                    symbol_overrides: BTreeMap::new(),
                    factor_rules: Vec::new(),
                    strict_nq: false,
                    linear_nq: true,
                    entry,
                    execution: self.cfg.exits.clone(),
                };
                let mut input = trade_signal::model_output_hub::ModelOutputScoreLookupResult {
                    service_name: self.cfg.name.clone(),
                    symbol_key: self.cfg.symbol.clone(),
                    subscribed: true,
                    score: f.value,
                    score_quantile: f.quantile,
                    score_ready: f.ready,
                    score_long_threshold: None,
                    score_short_threshold: None,
                    filter_long_value: None,
                    filter_short_value: None,
                    filter_long_threshold: None,
                    filter_short_threshold: None,
                    filter_long_quantile: None,
                    filter_short_quantile: None,
                    filter_ready: false,
                    score_ts_ms: hour.end_ms,
                    note: String::new(),
                };
                input.filter_ready = nq.values().all(|v| v.ready);
                input.filter_long_value = nq["long"].value;
                input.filter_short_value = nq["short"].value;
                input.filter_long_quantile = nq["long"].quantile;
                input.filter_short_quantile = nq["short"].quantile;
                input.filter_long_threshold = nq["long"].median;
                input.filter_short_threshold = nq["short"].median;
                let bar = WeightedBar {
                    model_ts_ms: hour.end_ms,
                    value: direction,
                    coefficient: 1.,
                    quantile: q,
                    decision_value: q,
                    votes: Vec::new(),
                    nq: input,
                };
                if self.cfg.interval == "1h"
                    && hourly_exit(
                        current,
                        q,
                        self.cfg.exits.factor_exit_quantile_long,
                        self.cfg.exits.factor_exit_quantile_short,
                    )
                {
                    next_position_flag(0, &bar, &signal)
                } else {
                    next_position_flag(current, &bar, &signal)
                }
            } else {
                current
            };
            if publish {
                self.ledger.flags.insert(rule.key().to_owned(), flag);
            }
            self.latest.push(RuleState {
                factor: rule.factor,
                id: rule.id,
                raw: f.raw,
                value: f.value,
                quantile: f.quantile,
                ready,
                flag,
                target_lots: i32::from(flag) * self.ledger.lots_per_rule,
            });
        }
        if publish {
            self.ledger.last_decision_ms = hour.end_ms;
            self.last_decision_day = Some(hour.trading_day);
        }
        self.observations
            .push_back((hour.clone(), contract.into(), multiple));
        if self.observations.len() > 16_384 {
            self.observations.pop_front();
        }
        Ok(publish)
    }
    pub fn publication_positions(
        &self,
    ) -> Result<BTreeMap<String, super::publication::StrategyPosition>> {
        let group = self.cfg.strategy_group()?;
        let native = |lots: i64| {
            let mut targets = super::publication::Targets::new();
            for contract in &self.ledger.retired_contracts {
                targets.insert(format!("{}:{contract}", self.cfg.exchange), 0);
            }
            if !self.ledger.contract.is_empty() {
                targets.insert(
                    format!("{}:{}", self.cfg.exchange, self.ledger.contract),
                    lots,
                );
            }
            super::publication::StrategyPosition {
                decision_ms: self.ledger.last_decision_ms,
                targets,
            }
        };
        if self.cfg.publish_per_factor() {
            let names: std::collections::BTreeSet<_> = self
                .cfg
                .rules
                .iter()
                .map(|r| r.key().to_owned())
                .chain(self.ledger.flags.keys().cloned())
                .collect();
            Ok(names
                .into_iter()
                .map(|factor| {
                    let lots = i64::from(*self.ledger.flags.get(&factor).unwrap_or(&0))
                        * i64::from(self.ledger.lots_per_rule);
                    (
                        format!("{group}-{factor}-{}", self.cfg.interval),
                        native(lots),
                    )
                })
                .collect())
        } else if self.cfg.rules.is_empty() && self.ledger.flags.is_empty() {
            Ok(BTreeMap::new())
        } else {
            let lots = self
                .ledger
                .flags
                .values()
                .map(|v| i64::from(*v) * i64::from(self.ledger.lots_per_rule))
                .sum();
            Ok(BTreeMap::from([(
                format!("{group}-{}", self.cfg.interval),
                native(lots),
            )]))
        }
    }
    pub fn targets(&self) -> serde_json::Value {
        let mut targets: Vec<_> = self
            .ledger
            .retired_contracts
            .iter()
            .filter(|c| *c != &self.ledger.contract)
            .map(|c| serde_json::json!({"exchange":self.cfg.exchange,"symbol":c,"target_lots":0}))
            .collect();
        if !self.ledger.contract.is_empty() {
            let lots: i32 = self
                .ledger
                .flags
                .values()
                .map(|f| i32::from(*f) * self.ledger.lots_per_rule)
                .sum();
            targets.push(serde_json::json!({"exchange":self.cfg.exchange,"symbol":self.ledger.contract,"target_lots":lots}));
        }
        serde_json::json!({"strategy_name":self.cfg.name,"interval":self.cfg.interval,"position_unit":"lot","decision_ms":self.ledger.last_decision_ms,"targets":targets})
    }
}
