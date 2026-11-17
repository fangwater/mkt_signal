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

#[derive(Clone)]
pub struct Hour {
    pub start_ms: i64,
    pub end_ms: i64,
    pub trading_day: u32,
    pub minutes: usize,
    pub trade: [f64; FUTURES_TRADE_FIELD_COUNT],
    pub book: Book5,
    pub mid: f64,
    pub close_approximation: bool,
}
/// The published CN hourly convention: consecutive source minutes, groups of
/// 60 starting at the segment's first minute; retain a segment's short tail.
#[derive(Default)]
pub struct Hours {
    rows: Vec<MinuteBar>,
}
impl Hours {
    pub fn last_minute_ms(&self) -> Option<i64> {
        self.rows.last().map(|r| r.open_time_ms)
    }
    pub fn push(&mut self, row: MinuteBar, multiple: f64) -> Result<Vec<Hour>> {
        let mut out = Vec::new();
        if let Some(last) = self.rows.last() {
            ensure!(
                row.open_time_ms > last.open_time_ms,
                "minute timestamps must increase"
            );
            if row.open_time_ms - last.open_time_ms != 60_000 || row.trading_day != last.trading_day
            {
                out.push(self.finish(multiple)?.context("missing hourly tail")?);
            }
        }
        self.rows.push(row);
        if self.rows.len() == 60 {
            out.push(self.finish(multiple)?.context("missing full hour")?);
        }
        Ok(out)
    }
    pub fn finish(&mut self, multiple: f64) -> Result<Option<Hour>> {
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
                            .all(|v| v.is_finite() && *v > 0.)),
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
        for (i, amount, volume, carry) in [
            (23, 5, 4, last.vwap),
            (24, 10, 12, last.buy_vwap.unwrap_or(f64::NAN)),
            (25, 11, 13, last.sell_vwap.unwrap_or(f64::NAN)),
        ] {
            v[i] = if v[volume] > 0. {
                v[amount] / v[volume] / multiple
            } else {
                carry
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
        let mid = last.mid_price.filter(|v| v.is_finite() && *v > 0.);
        Ok(Some(Hour {
            start_ms: first.open_time_ms,
            end_ms: last.open_time_ms + 60_000,
            trading_day: last.trading_day,
            minutes: rows.len(),
            trade: v,
            book,
            mid: mid.unwrap_or(last.close),
            close_approximation: mid.is_none(),
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
    pub raw: Option<f64>,
    pub value: Option<f64>,
    pub quantile: Option<f64>,
    pub ready: bool,
    pub flag: i8,
    pub target_lots: i32,
}

pub struct Engine {
    cfg: Config,
    plan: FuturesFactorPlan,
    formula: FuturesFusionState,
    transforms: Vec<Transform>,
    nq: Nq,
    pub ledger: Ledger,
    pub latest: Vec<RuleState>,
    pub last_end_ms: i64,
    pub hours: usize,
    pub approximated_mid_hours: usize,
    observations: VecDeque<(Hour, String, f64)>,
}
impl Engine {
    pub fn new(cfg: &Config, ledger: Ledger) -> Result<Self> {
        ensure!(
            (1..=100).contains(&ledger.lots_per_rule)
                && ledger.flags.values().all(|v| matches!(v, -1..=1)),
            "invalid persisted rule position"
        );
        let plan = FuturesFactorPlan::from_factor_names(
            cfg.rules
                .iter()
                .map(|r| format!("cn_features_{}", r.factor))
                .collect(),
        )?;
        Ok(Self {
            cfg: cfg.clone(),
            plan,
            formula: FuturesFusionState::default(),
            transforms: cfg
                .rules
                .iter()
                .map(|_| {
                    Transform::new(
                        &Parameters::for_interval(Interval::Hour, false),
                        Interval::Hour,
                    )
                })
                .collect(),
            nq: Nq::new(&Parameters::for_interval(Interval::Hour, true)),
            ledger,
            latest: Vec::new(),
            last_end_ms: 0,
            hours: 0,
            approximated_mid_hours: 0,
            observations: VecDeque::new(),
        })
    }
    pub fn reconfigure(&self, cfg: &Config) -> Result<Self> {
        let mut next = Self::new(cfg, self.ledger.clone())?;
        for (hour, contract, multiple) in &self.observations {
            next.observe(hour, contract, *multiple, false)?;
        }
        ensure!(
            next.ready(),
            "changed factors are not ready on the factual hourly history"
        );
        ensure!(
            next.ledger == self.ledger,
            "configuration warm-up changed factual positions"
        );
        Ok(next)
    }
    pub fn ready(&self) -> bool {
        self.latest.len() == self.cfg.rules.len() && self.latest.iter().all(|r| r.ready)
    }
    pub fn observe(
        &mut self,
        hour: &Hour,
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
        let depth = FuturesDepth5::from_slices(
            &array(&hour.book.bids, true),
            &array(&hour.book.bids, false),
            &array(&hour.book.asks, true),
            &array(&hour.book.asks, false),
        )?;
        self.formula.push_continuous(FuturesFusionInput {
            ts_ms: hour.end_ms,
            symbol: contract.into(),
            trading_day: hour.trading_day,
            trade: FuturesTradeBar { values: hour.trade },
            depth: Some(depth),
            quality_flags: 0,
            volume_multiple: multiple,
            volume_multiple_verified: true,
        })?;
        let raw = self.formula.factor_values(&self.plan)?;
        let nq = self.nq.observe(Some(hour.mid));
        self.last_end_ms = hour.end_ms;
        self.hours += 1;
        self.approximated_mid_hours += usize::from(hour.close_approximation);
        let mut points = Vec::new();
        let mut all_ready = true;
        for ((rule, transform), raw) in self.cfg.rules.iter().zip(&mut self.transforms).zip(raw) {
            let f: Field = transform.observe(raw, true);
            let gates = if self.cfg.entry.nq_change_enabled {
                rule.gates()?
            } else {
                None
            };
            let ready = f.ready && gates.is_none_or(|_| nq.values().all(|v| v.ready));
            all_ready &= ready;
            points.push((rule.clone(), f, gates, ready));
        }
        // Preheat never advances a position or decision clock. Apply an entire
        // frequency atomically only after every configured dependency is ready.
        let publish =
            self.cfg.enabled && live && all_ready && hour.end_ms > self.ledger.last_decision_ms;
        if publish {
            self.ledger.lots_per_rule = self.cfg.lots_per_rule;
            self.ledger
                .flags
                .retain(|factor, _| self.cfg.rules.iter().any(|r| &r.factor == factor));
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
            let current = *self.ledger.flags.get(&rule.factor).unwrap_or(&0);
            let flag = if publish {
                let q = f.quantile.context("ready factor missing rank")?;
                let q = if rule.sign > 0 { q } else { 1. - q };
                let eps = 4. * f64::EPSILON;
                let direction = if q - self.cfg.entry.factor_long_quantile > eps {
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
                    bar_interval: BarInterval::OneHour,
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
                next_position_flag(current, &bar, &signal)
            } else {
                current
            };
            if publish {
                self.ledger.flags.insert(rule.factor.clone(), flag);
            }
            self.latest.push(RuleState {
                factor: rule.factor,
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
        }
        self.observations
            .push_back((hour.clone(), contract.into(), multiple));
        if self.observations.len() > 16_384 {
            self.observations.pop_front();
        }
        Ok(publish)
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
        serde_json::json!({"strategy_name":self.cfg.name,"interval":"1h","position_unit":"lot","decision_ms":self.ledger.last_decision_ms,"targets":targets})
    }
}
