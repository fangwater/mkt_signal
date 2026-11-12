use anyhow::{ensure, Context, Result};
use mkt_signal::cta_factor_calc::{
    config::{
        Calculation, Combination, FactorId, Interval, Market, Parameters, Plan, SymbolGroups,
    },
    engine::Field,
};
use mkt_signal::cta_perp::{
    config::{CtaPerpConfig, EntryConfig, ExecutionConfig, PositionAlgorithm},
    factors::WeightedBar,
    positions::{full_qty, next_position_flag},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use trade_signal::model_output_hub::ModelOutputScoreLookupResult;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub source_path: String,
    pub source_sha256: String,
    pub combination: Combination,
}

pub struct Choice {
    pub strategy: String,
    pub factor: String,
    pub weight: f64,
    pub nq: String,
}

impl Profile {
    pub fn name(&self) -> &str {
        &self.combination.name
    }
    pub fn interval(&self) -> Interval {
        self.combination.interval
    }
    pub fn factor_id(&self, symbol: &str, factor: &str) -> String {
        format!(
            "binance-futures-{}-{}-{}",
            symbol.to_ascii_lowercase(),
            self.interval().name(),
            factor.to_ascii_lowercase()
        )
    }
    pub fn rules(&self) -> Result<BTreeMap<String, Vec<Choice>>> {
        let config = self.combination.signal_config()?;
        let mut rules = BTreeMap::<String, Vec<Choice>>::new();
        for rule in &config.factor_rules {
            rules.entry(rule.venue_symbol()).or_default().push(Choice {
                strategy: config.rule_strategy(rule),
                factor: rule.factor.clone(),
                weight: f64::from(rule.sign),
                nq: rule.nq.clone(),
            });
        }
        Ok(rules)
    }
    pub fn choice(&self, symbol: &str, strategy: &str) -> Result<Choice> {
        self.rules()?
            .remove(symbol)
            .unwrap_or_default()
            .into_iter()
            .find(|rule| rule.strategy == strategy)
            .context("rule is not in this Prep profile")
    }
    pub fn defaults(&self, symbol: &str, strategy: &str) -> Result<CtaPerpConfig> {
        let mut config = self
            .combination
            .signal_config()?
            .for_strategy(symbol, strategy);
        // Every form submission is a standalone simulation, never a live switch.
        config.enabled = false;
        Ok(config)
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub fetched_at_ms: i64,
    pub source_host: String,
    pub profiles: Vec<Profile>,
    pub symbol_groups: SymbolGroups,
    pub amount_thresholds: BTreeMap<String, [f64; 2]>,
    pub threshold_metadata: BTreeMap<String, String>,
}

impl Bootstrap {
    pub fn plan(&self) -> Result<Plan> {
        ensure!(
            !self.profiles.is_empty(),
            "no current Prep profiles were imported"
        );
        let mut combinations = Vec::new();
        for profile in &self.profiles {
            profile
                .combination
                .signal_config()?
                .validate()
                .with_context(|| format!("profile {}", profile.name()))?;
            ensure!(
                profile.combination.signal_config()?.position_algorithm == PositionAlgorithm::V0,
                "profile {} requires independent-factor v0",
                profile.name()
            );
            ensure!(
                profile.combination.venue == Market::BinanceFutures
                    && profile.combination.reference_venue.is_none(),
                "memory replay requires a Prep combination"
            );
            combinations.push(profile.combination.clone());
        }
        let mut plan = Plan::merge_with_symbol_groups(combinations, self.symbol_groups.clone())?;
        // NQ remains selectable in simulation even for rules initially gated off.
        // These are ordinary current factor calculations, not a second codec.
        for profile in &self.profiles {
            for symbol in profile.rules()?.keys() {
                let name = profile.factor_id(symbol, "nq");
                let id = FactorId::parse(&name)?;
                let parameters = profile
                    .combination
                    .parameters
                    .get(&name)
                    .cloned()
                    .unwrap_or_else(|| Parameters::for_id(&id));
                parameters.validate(&id)?;
                let calculation = plan
                    .calculations
                    .entry(name)
                    .or_insert_with(|| Calculation {
                        id,
                        parameters: parameters.clone(),
                        combinations: Default::default(),
                    });
                ensure!(
                    calculation.parameters == parameters,
                    "conflicting shared NQ definition"
                );
                calculation.combinations.insert(profile.name().to_string());
            }
        }
        Ok(plan)
    }
}

#[derive(Clone)]
pub struct Point {
    pub ts_ms: i64,
    pub available_ms: i64,
    pub value: Field,
    pub short: Option<Field>,
}

#[derive(Clone, Copy)]
pub struct Fill {
    pub ts_ms: i64,
    pub price: f64,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Candle {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
}

impl Candle {
    pub fn vwap(&self) -> Option<f64> {
        let price = self.turnover / self.volume;
        (self.volume.is_finite()
            && self.volume > 0.0
            && self.turnover.is_finite()
            && self.turnover > 0.0
            && price.is_finite()
            && price > 0.0)
            .then_some(price)
    }
}

#[derive(Default)]
pub struct Memory {
    pub points: BTreeMap<String, VecDeque<Point>>,
    pub candles: BTreeMap<(String, i64), VecDeque<Candle>>,
    pub kafka_records: u64,
    pub revision: u64,
    pub latest_data_ms: i64,
    pub caught_up: bool,
    pub partitions: BTreeMap<String, (i64, i64, i64)>,
    pub error: Option<String>,
    pub last_received_ms: i64,
}

impl Memory {
    pub fn prune(&mut self, cutoff: i64) {
        for rows in self.points.values_mut() {
            while rows.front().is_some_and(|r| r.ts_ms < cutoff) {
                rows.pop_front();
            }
        }
        for rows in self.candles.values_mut() {
            while rows.front().is_some_and(|r| r.ts_ms < cutoff) {
                rows.pop_front();
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRequest {
    pub profile: String,
    pub symbol: String,
    pub factor: String,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub entry: EntryConfig,
    pub execution: ExecutionConfig,
    pub max_pos_u: f64,
    pub fee_rate: f64,
}

#[derive(Clone)]
pub struct Observation {
    pub point: Point,
    pub nq: Option<Point>,
    pub fill: Option<Fill>,
}

pub struct Input {
    pub observations: Vec<Observation>,
    pub revision: u64,
    pub kafka_records: u64,
    pub caught_up: bool,
    pub requested_end_ms: i64,
}

pub fn prepare(memory: &Memory, profile: &Profile, request: &ReplayRequest) -> Result<Input> {
    let choice = profile.choice(&request.symbol, &request.factor)?;
    ensure!(
        request.fee_rate.is_finite() && (0.0..=0.01).contains(&request.fee_rate),
        "fee_rate must be in 0..0.01"
    );
    let config = effective_config(profile, request)?;
    config.validate()?;
    let start = request.start_ms.unwrap_or(0);
    let end = request.end_ms.unwrap_or(memory.latest_data_ms);
    ensure!(start >= 0 && end > start, "invalid replay time range");
    let empty = VecDeque::new();
    let points = memory
        .points
        .get(&profile.factor_id(&request.symbol, &choice.factor))
        .unwrap_or(&empty);
    let nqs = memory
        .points
        .get(&profile.factor_id(&request.symbol, "nq"))
        .unwrap_or(&empty);
    let empty_candles = VecDeque::new();
    let interval = profile.interval().millis();
    let prices = memory
        .candles
        .get(&(request.symbol.clone(), interval))
        .unwrap_or(&empty_candles);
    let mut observations = Vec::new();
    let first = points.partition_point(|point| point.ts_ms < start);
    for point in points
        .iter()
        .skip(first)
        .take_while(|point| point.ts_ms <= end)
    {
        if point.available_ms > end || point.ts_ms + interval > end {
            continue;
        }
        let nq = nqs
            .binary_search_by_key(&point.ts_ms, |r| r.ts_ms)
            .ok()
            .map(|i| nqs[i].clone())
            .filter(|nq| nq.available_ms <= end);
        // A signal at closed-bar boundary t fills at VWAP of [t,t+interval).
        // Only complete subsequent bars are eligible, never the signal bar.
        let fill = prices
            .binary_search_by_key(&point.ts_ms, |bar| bar.ts_ms)
            .ok()
            .and_then(|index| prices[index].vwap())
            .map(|price| Fill {
                ts_ms: point.ts_ms + interval,
                price,
            });
        observations.push(Observation {
            point: point.clone(),
            nq,
            fill,
        });
    }
    Ok(Input {
        observations,
        revision: memory.revision,
        kafka_records: memory.kafka_records,
        caught_up: memory.caught_up,
        requested_end_ms: end,
    })
}

fn effective_config(profile: &Profile, request: &ReplayRequest) -> Result<CtaPerpConfig> {
    let mut config = profile.defaults(&request.symbol, &request.factor)?;
    config.entry = request.entry.clone();
    config.execution = request.execution.clone();
    config.max_pos_u = request.max_pos_u;
    Ok(config)
}

fn lookup(nq: Option<&Point>) -> ModelOutputScoreLookupResult {
    let long = nq.map(|r| &r.value);
    let short = nq.and_then(|r| r.short.as_ref());
    ModelOutputScoreLookupResult {
        service_name: String::new(),
        symbol_key: String::new(),
        subscribed: true,
        score: None,
        score_quantile: None,
        score_ready: false,
        score_long_threshold: None,
        score_short_threshold: None,
        filter_long_value: long.and_then(|v| v.value),
        filter_long_threshold: long.and_then(|v| v.median),
        filter_short_value: short.and_then(|v| v.value),
        filter_short_threshold: short.and_then(|v| v.median),
        filter_long_quantile: long.and_then(|v| v.quantile),
        filter_short_quantile: short.and_then(|v| v.quantile),
        filter_ready: long.is_some_and(|v| v.ready) && short.is_some_and(|v| v.ready),
        score_ts_ms: nq.map_or(0, |r| r.ts_ms),
        note: String::new(),
    }
}

#[derive(Serialize)]
pub struct Decision {
    pub ts_ms: i64,
    pub signal_ts_ms: i64,
    pub quantile: Option<f64>,
    pub decision_value: Option<f64>,
    pub nq_long: Option<f64>,
    pub nq_short: Option<f64>,
    pub flag: i8,
    pub qty: f64,
    pub equity: f64,
    pub action: Option<&'static str>,
    pub execution_ms: Option<i64>,
    pub execution_price: Option<f64>,
}

#[derive(Clone, Serialize)]
pub struct Trade {
    pub side: i8,
    pub entry_ms: i64,
    pub exit_ms: Option<i64>,
    pub entry_bar_ms: i64,
    pub exit_bar_ms: Option<i64>,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub qty: f64,
    pub pnl: f64,
    pub fees: f64,
}

#[derive(Serialize)]
pub struct ReplayResult {
    pub parameters: ReplayRequest,
    pub data_revision: u64,
    pub kafka_records: u64,
    pub caught_up: bool,
    pub cutoff_ms: i64,
    pub first_ms: Option<i64>,
    pub last_ms: Option<i64>,
    pub ready_bars: usize,
    pub warmup_bars: usize,
    pub missing_vwap: usize,
    pub closed_trades: usize,
    pub win_rate: Option<f64>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub max_drawdown: f64,
    pub trades: Vec<Trade>,
    pub decisions: Vec<Decision>,
    pub compute_ms: f64,
}

pub fn simulate(profile: &Profile, request: ReplayRequest, input: Input) -> Result<ReplayResult> {
    let config = effective_config(profile, &request)?;
    config.validate()?;
    ensure!(
        request.fee_rate.is_finite() && (0.0..=0.01).contains(&request.fee_rate),
        "invalid fee_rate"
    );
    let weight = profile.choice(&request.symbol, &request.factor)?.weight;
    let (mut flag, mut qty, mut realized, mut fees, mut peak, mut drawdown) =
        (0, 0.0_f64, 0.0, 0.0, 0.0_f64, 0.0_f64);
    let (mut ready, mut warmup, mut missing) = (0, 0, 0);
    let mut open: Option<Trade> = None;
    let mut trades = Vec::new();
    let mut decisions = Vec::new();
    let mut last_mark = None;
    for row in input.observations {
        let nq = lookup(row.nq.as_ref());
        let q = row
            .point
            .value
            .quantile
            .filter(|q| q.is_finite() && (0.0..=1.0).contains(q));
        let value = q.map(|q| {
            config
                .position_algorithm
                .decision_value(if weight > 0.0 { q } else { 1.0 - q })
        });
        let valid = row.point.value.ready && value.is_some();
        if valid {
            ready += 1;
        } else {
            warmup += 1;
        }
        let mut action = None;
        let mut execution_ms = None;
        let mut execution_price = None;
        if let Some(fill) = row.fill {
            last_mark = Some(fill.price);
            if valid {
                let value = value.unwrap();
                let direction = if value - config.entry.factor_long_quantile > 4.0 * f64::EPSILON {
                    1.0
                } else if config.entry.factor_short_quantile - value > 4.0 * f64::EPSILON {
                    -1.0
                } else {
                    0.0
                };
                let bar = WeightedBar {
                    model_ts_ms: row.point.ts_ms,
                    value: direction,
                    coefficient: 1.0,
                    quantile: q.unwrap(),
                    decision_value: value,
                    votes: Vec::new(),
                    nq: nq.clone(),
                };
                let next = next_position_flag(flag, &bar, &config);
                if next != flag {
                    let price = fill.price;
                    execution_ms = Some(fill.ts_ms);
                    execution_price = Some(price);
                    if next == 0 {
                        let mut trade = open.take().context("missing simulated entry")?;
                        let exit_fee = qty.abs() * price * request.fee_rate;
                        fees += exit_fee;
                        realized += qty * (price - trade.entry_price) - exit_fee;
                        trade.exit_ms = Some(fill.ts_ms);
                        trade.exit_bar_ms = Some(row.point.ts_ms);
                        trade.exit_price = Some(price);
                        trade.fees += exit_fee;
                        trade.pnl = qty * (price - trade.entry_price) - trade.fees;
                        trades.push(trade);
                        action = Some(if flag > 0 {
                            "close_long"
                        } else {
                            "close_short"
                        });
                        qty = 0.0;
                    } else {
                        qty = full_qty(config.max_pos_u, next, price)?;
                        let entry_fee = qty.abs() * price * request.fee_rate;
                        realized -= entry_fee;
                        fees += entry_fee;
                        open = Some(Trade {
                            side: next,
                            entry_ms: fill.ts_ms,
                            exit_ms: None,
                            entry_bar_ms: row.point.ts_ms,
                            exit_bar_ms: None,
                            entry_price: price,
                            exit_price: None,
                            qty,
                            pnl: 0.0,
                            fees: entry_fee,
                        });
                        action = Some(if next > 0 { "open_long" } else { "open_short" });
                    }
                    flag = next;
                }
            }
        } else {
            missing += 1;
        }
        let unrealized = open
            .as_ref()
            .zip(last_mark)
            .map_or(0.0, |(trade, mark)| qty * (mark - trade.entry_price));
        let equity = realized + unrealized;
        peak = peak.max(equity);
        drawdown = drawdown.max(peak - equity);
        decisions.push(Decision {
            ts_ms: row.point.ts_ms + profile.interval().millis(),
            signal_ts_ms: row.point.ts_ms,
            quantile: q,
            decision_value: value,
            nq_long: nq.filter_long_quantile,
            nq_short: nq.filter_short_quantile,
            flag,
            qty,
            equity,
            action,
            execution_ms,
            execution_price,
        });
    }
    let unrealized = open
        .as_ref()
        .zip(last_mark)
        .map_or(0.0, |(trade, mark)| qty * (mark - trade.entry_price));
    let closed = trades.len();
    let wins = trades.iter().filter(|t| t.pnl > 0.0).count();
    if let Some(mut trade) = open {
        trade.pnl = unrealized - trade.fees;
        trades.push(trade);
    }
    Ok(ReplayResult {
        parameters: request,
        data_revision: input.revision,
        kafka_records: input.kafka_records,
        caught_up: input.caught_up,
        cutoff_ms: input.requested_end_ms,
        first_ms: decisions.first().map(|r| r.ts_ms),
        last_ms: decisions.last().map(|r| r.ts_ms),
        ready_bars: ready,
        warmup_bars: warmup,
        missing_vwap: missing,
        closed_trades: closed,
        win_rate: (closed > 0).then(|| wins as f64 / closed as f64),
        realized_pnl: realized,
        unrealized_pnl: unrealized,
        fees,
        max_drawdown: drawdown,
        trades,
        decisions,
        compute_ms: 0.0,
    })
}
