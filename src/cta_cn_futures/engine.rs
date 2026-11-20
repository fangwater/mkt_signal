use super::config::{Config, Rule};
use crate::cta_factor_calc::{
    config::{Interval, Parameters},
    engine::{Field, Nq, Transform},
};
use crate::cta_perp::{
    config::{CtaPerpConfig, PositionAlgorithm},
    factors::{mean_bar, SymbolFactors, WeightedBar},
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
use trade_signal::model_output_hub::ModelOutputScoreLookupResult;

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
    #[serde(default)]
    pub position_algorithm: PositionAlgorithm,
    #[serde(default)]
    pub target_direction: Option<i8>,
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
            position_algorithm: PositionAlgorithm::V0,
            target_direction: None,
        }
    }
}
impl Ledger {
    fn target_lots(&self) -> i64 {
        self.target_direction.map(i64::from).unwrap_or_else(|| {
            // Keep the last published quantity from an older ledger until a live bar.
            self.flags
                .values()
                .map(|&flag| i64::from(flag))
                .sum::<i64>()
                * i64::from(self.lots_per_rule)
        })
    }
}

struct ObservedRule {
    rule: Rule,
    field: Field,
    gates: Option<(f64, f64)>,
    ready: bool,
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
    signal: CtaPerpConfig,
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
                && ledger.flags.values().all(|v| matches!(v, -1..=1))
                && ledger.target_direction.is_none_or(|v| matches!(v, -1..=1))
                && (ledger.position_algorithm == PositionAlgorithm::V0
                    || ledger.target_direction.is_some()),
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
            signal: CtaPerpConfig {
                bar_interval: match interval {
                    Interval::Minute => BarInterval::OneMinute,
                    Interval::FiveMinutes => BarInterval::FiveMinutes,
                    Interval::FifteenMinutes => BarInterval::FifteenMinutes,
                    Interval::Hour => BarInterval::OneHour,
                },
                position_algorithm: cfg.position_algorithm,
                linear_nq: true,
                entry: cfg.entry.clone(),
                execution: cfg.exits.clone(),
                enabled: cfg.enabled,
                max_pos_u: 1.,
                symbol_overrides: BTreeMap::new(),
                factor_rules: Vec::new(),
                strict_nq: false,
            },
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
            let gates = if !self.cfg.entry.nq_change_enabled {
                None
            } else if self.cfg.position_algorithm == PositionAlgorithm::V1 {
                Some((
                    self.cfg.entry.nq_long_quantile,
                    self.cfg.entry.nq_short_quantile,
                ))
            } else {
                rule.gates()?
            };
            let ready = f.ready
                && (self.cfg.interval == "1h"
                    || gates.is_none_or(|_| nq.values().all(|v| v.ready)));
            all_ready &= ready;
            points.push(ObservedRule {
                rule: rule.clone(),
                field: f,
                gates,
                ready,
            });
        }
        // Preheat never advances a position or decision clock.
        let publish =
            self.cfg.enabled && live && all_ready && hour.end_ms > self.ledger.last_decision_ms;
        if publish {
            let (direction, flags) = self.decide(&points, &nq, hour.end_ms)?;
            if self.ledger.contract != contract {
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
            self.ledger.flags = flags;
            self.ledger.lots_per_rule = 1;
            self.ledger.position_algorithm = self.cfg.position_algorithm;
            self.ledger.target_direction = Some(direction);
            self.ledger.last_decision_ms = hour.end_ms;
            self.last_decision_day = Some(hour.trading_day);
        }
        self.latest = points
            .into_iter()
            .map(
                |ObservedRule {
                     rule, field, ready, ..
                 }| {
                    let flag = *self.ledger.flags.get(rule.key()).unwrap_or(&0);
                    RuleState {
                        factor: rule.factor,
                        id: rule.id,
                        raw: field.raw,
                        value: field.value,
                        quantile: field.quantile,
                        ready,
                        flag,
                        target_lots: i32::from(flag),
                    }
                },
            )
            .collect();
        self.observations
            .push_back((hour.clone(), contract.into(), multiple));
        if self.observations.len() > 16_384 {
            self.observations.pop_front();
        }
        Ok(publish)
    }
    fn decide(
        &self,
        points: &[ObservedRule],
        nq: &BTreeMap<String, Field>,
        end_ms: i64,
    ) -> Result<(i8, BTreeMap<String, i8>)> {
        let inputs = points.iter().map(|point| ModelOutputScoreLookupResult {
            service_name: self.cfg.name.clone(),
            symbol_key: self.cfg.symbol.clone(),
            subscribed: true,
            score: point.field.value,
            score_quantile: point.field.quantile,
            score_ready: point.field.ready,
            score_long_threshold: None,
            score_short_threshold: None,
            filter_long_value: nq["long"].value,
            filter_short_value: nq["short"].value,
            filter_long_threshold: nq["long"].median,
            filter_short_threshold: nq["short"].median,
            filter_long_quantile: nq["long"].quantile,
            filter_short_quantile: nq["short"].quantile,
            filter_ready: nq.values().all(|v| v.ready),
            score_ts_ms: end_ms,
            note: String::new(),
        });
        if self.cfg.position_algorithm == PositionAlgorithm::V1 {
            let row = SymbolFactors {
                factors: points.iter().map(|p| p.rule.key().to_owned()).collect(),
                weights: points.iter().map(|p| f64::from(p.rule.sign)).collect(),
            };
            let inputs: Vec<_> = inputs.collect();
            let bar = mean_bar(&row, &inputs, &self.signal.entry)
                .context("cannot aggregate ready domestic factor scores")?;
            let current = self.ledger.target_lots().signum() as i8;
            let direction = next_position_flag(current, &bar, &self.signal);
            let flags = bar
                .votes
                .into_iter()
                .map(|vote| (vote.factor, vote.vote))
                .collect();
            return Ok((direction, flags));
        }

        let mut flags = BTreeMap::new();
        let mut signal = self.signal.clone();
        for (point, input) in points.iter().zip(inputs) {
            let raw_q = point.field.quantile.context("ready factor missing rank")?;
            let q = if point.rule.sign > 0 {
                raw_q
            } else {
                1. - raw_q
            };
            let entry = &mut signal.entry;
            entry.nq_change_enabled = point.gates.is_some();
            if let Some((long, short)) = point.gates {
                entry.nq_long_quantile = long;
                entry.nq_short_quantile = short;
            }
            let hourly = signal.bar_interval == BarInterval::OneHour;
            let value = if hourly {
                hourly_entry(q, entry.factor_long_quantile, entry.factor_short_quantile)
            } else if q - entry.factor_long_quantile > 4. * f64::EPSILON {
                1.
            } else if entry.factor_short_quantile - q > 4. * f64::EPSILON {
                -1.
            } else {
                0.
            };
            let bar = WeightedBar {
                model_ts_ms: end_ms,
                value,
                coefficient: 1.,
                quantile: q,
                decision_value: q,
                votes: Vec::new(),
                nq: input,
            };
            let mut current = if self.ledger.position_algorithm == PositionAlgorithm::V0 {
                self.ledger
                    .flags
                    .get(point.rule.key())
                    .copied()
                    .unwrap_or(0)
            } else {
                // The old aggregate direction has no independent factor position.
                0
            };
            if hourly
                && hourly_exit(
                    current,
                    q,
                    signal.execution.factor_exit_quantile_long,
                    signal.execution.factor_exit_quantile_short,
                )
            {
                current = 0;
            }
            flags.insert(
                point.rule.key().to_owned(),
                next_position_flag(current, &bar, &signal),
            );
        }
        let direction = flags
            .values()
            .map(|&flag| i32::from(flag))
            .sum::<i32>()
            .signum() as i8;
        Ok((direction, flags))
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
        if self.cfg.rules.is_empty()
            && self.ledger.flags.is_empty()
            && self.ledger.contract.is_empty()
        {
            return Ok(BTreeMap::new());
        }
        Ok(BTreeMap::from([(
            format!("{group}-{}", self.cfg.interval),
            native(self.ledger.target_lots()),
        )]))
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
            let lots = self.ledger.target_lots();
            targets.push(serde_json::json!({"exchange":self.cfg.exchange,"symbol":self.ledger.contract,"target_lots":lots}));
        }
        serde_json::json!({"strategy_name":self.cfg.name,"interval":self.cfg.interval,"position_unit":"lot","decision_ms":self.ledger.last_decision_ms,"targets":targets})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_lot_direction_uses_independent_votes_or_the_mean_with_strict_thresholds() {
        let mut cfg: Config = serde_json::from_str(include_str!(
            "../../tests/fixtures/cta_cn_futures_legacy.json"
        ))
        .unwrap();
        cfg.rules.truncate(2);
        cfg.entry.nq_change_enabled = false;
        let points = |quantiles: [f64; 2]| {
            cfg.rules
                .iter()
                .zip(quantiles)
                .map(|(rule, q)| {
                    let mut rule = rule.clone();
                    rule.sign = 1;
                    ObservedRule {
                        rule,
                        field: Field {
                            value: Some(q),
                            quantile: Some(q),
                            ready: true,
                            ..Field::default()
                        },
                        gates: None,
                        ready: true,
                    }
                })
                .collect::<Vec<_>>()
        };
        let nq = BTreeMap::from([
            ("long".into(), Field::default()),
            ("short".into(), Field::default()),
        ]);
        let mut v0 = Engine::new(&cfg, Ledger::default()).unwrap();
        for (q, expected) in [([0.99, 0.99], 1), ([0.01, 0.01], -1), ([0.99, 0.01], 0)] {
            assert_eq!(v0.decide(&points(q), &nq, 1).unwrap().0, expected);
        }

        let mut v1_cfg = cfg.clone();
        v1_cfg.position_algorithm = PositionAlgorithm::V1;
        v1_cfg.entry.factor_long_quantile = 0.5;
        v1_cfg.entry.factor_short_quantile = -0.5;
        v1_cfg.exits.factor_exit_quantile_long = -0.2;
        v1_cfg.exits.factor_exit_quantile_short = 0.2;
        v1_cfg.validate().unwrap();
        let mut v1 = Engine::new(&v1_cfg, Ledger::default()).unwrap();
        for (current, q, expected) in [
            (0, [0.99, 0.51], 0),
            (0, [0.99, 0.52], 1),
            (0, [0.01, 0.49], 0),
            (0, [0.01, 0.48], -1),
            (1, [0.4, 0.4], 1),
            (1, [0.39, 0.4], 0),
            (-1, [0.6, 0.6], -1),
            (-1, [0.61, 0.6], 0),
        ] {
            v1.ledger.target_direction = Some(current);
            assert_eq!(v1.decide(&points(q), &nq, 1).unwrap().0, expected);
        }
        v0.ledger.position_algorithm = PositionAlgorithm::V1;
        v0.ledger.target_direction = Some(1);
        v0.ledger.flags.insert(cfg.rules[0].key().into(), 1);
        assert_eq!(v0.decide(&points([0.5, 0.5]), &nq, 1).unwrap().0, 0);
    }
}
