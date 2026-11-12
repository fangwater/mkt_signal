//! Aggregate all independent rules; currency quantities are never netted together.
use super::model::{self, Bootstrap, Memory};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Instant,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Thresholds {
    pub long_entry: f64,
    pub short_entry: f64,
    pub long_exit: f64,
    pub short_exit: f64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRequest {
    pub profiles: Vec<String>,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub thresholds: Option<Thresholds>,
    pub max_pos_u: Option<f64>,
    pub fee_rate: f64,
}

#[derive(Clone, Copy, Serialize)]
pub struct CurvePoint {
    pub ts_ms: i64,
    pub pnl: f64,
}

#[derive(Clone, Default, Serialize)]
pub struct Summary {
    pub pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub closed_trades: usize,
    pub wins: usize,
    pub win_rate: Option<f64>,
    pub max_drawdown: f64,
    pub rules: usize,
    pub ready_rules: usize,
    pub ready_bars: usize,
    pub warmup_bars: usize,
    pub missing_vwap: usize,
    pub long_rules: usize,
    pub short_rules: usize,
}

impl Summary {
    pub fn add(&mut self, other: &Self) {
        self.realized_pnl += other.realized_pnl;
        self.unrealized_pnl += other.unrealized_pnl;
        self.fees += other.fees;
        self.closed_trades += other.closed_trades;
        self.wins += other.wins;
        self.rules += other.rules;
        self.ready_rules += other.ready_rules;
        self.ready_bars += other.ready_bars;
        self.warmup_bars += other.warmup_bars;
        self.missing_vwap += other.missing_vwap;
        self.long_rules += other.long_rules;
        self.short_rules += other.short_rules;
    }
    fn finish(&mut self, curve: &[CurvePoint]) {
        self.pnl = self.realized_pnl + self.unrealized_pnl;
        self.win_rate =
            (self.closed_trades > 0).then(|| self.wins as f64 / self.closed_trades as f64);
        let mut peak = 0.0_f64;
        self.max_drawdown = 0.0;
        for point in curve {
            peak = peak.max(point.pnl);
            self.max_drawdown = self.max_drawdown.max(peak - point.pnl);
        }
    }
}

#[derive(Serialize)]
pub struct StrategyResult {
    pub profile: String,
    pub strategy_name: String,
    pub strategy_group: String,
    pub symbol_group: String,
    pub interval: String,
    pub summary: Summary,
    pub curve: Vec<CurvePoint>,
}

#[derive(Serialize)]
pub struct GroupResult {
    pub strategy_group: String,
    pub summary: Summary,
    pub curve: Vec<CurvePoint>,
}

#[derive(Serialize)]
pub struct PortfolioResult {
    pub parameters: PortfolioRequest,
    pub cutoff_ms: i64,
    pub data_revision_start: u64,
    pub data_revision_end: u64,
    pub caught_up: bool,
    pub data_error: Option<String>,
    pub price_method: &'static str,
    pub summary: Summary,
    pub curve: Vec<CurvePoint>,
    pub groups: Vec<GroupResult>,
    pub strategies: Vec<StrategyResult>,
    pub compute_ms: f64,
}

fn add_curve(events: &mut BTreeMap<i64, f64>, curve: impl IntoIterator<Item = (i64, f64)>) {
    let mut previous = 0.0;
    for (ts, pnl) in curve {
        *events.entry(ts).or_default() += pnl - previous;
        previous = pnl;
    }
}

fn finish_curve(events: BTreeMap<i64, f64>) -> Vec<CurvePoint> {
    let mut pnl = 0.0;
    events
        .into_iter()
        .map(|(ts_ms, delta)| {
            pnl += delta;
            CurvePoint { ts_ms, pnl }
        })
        .collect()
}

pub fn rule_request(
    profile: &model::Profile,
    symbol: &str,
    strategy: &str,
    request: &PortfolioRequest,
    cutoff: i64,
) -> Result<model::ReplayRequest> {
    let mut config = profile.defaults(symbol, strategy)?;
    if let Some(thresholds) = &request.thresholds {
        config.entry.factor_long_quantile = thresholds.long_entry;
        config.entry.factor_short_quantile = thresholds.short_entry;
        config.execution.factor_exit_quantile_long = thresholds.long_exit;
        config.execution.factor_exit_quantile_short = thresholds.short_exit;
    }
    if let Some(amount) = request.max_pos_u {
        config.max_pos_u = amount;
    }
    Ok(model::ReplayRequest {
        profile: profile.name().to_string(),
        symbol: symbol.into(),
        factor: strategy.into(),
        start_ms: request.start_ms,
        end_ms: Some(cutoff),
        entry: config.entry,
        execution: config.execution,
        max_pos_u: config.max_pos_u,
        fee_rate: request.fee_rate,
    })
}

pub fn calculate(
    bootstrap: &Bootstrap,
    memory: &Arc<RwLock<Memory>>,
    mut request: PortfolioRequest,
) -> Result<PortfolioResult> {
    let started = Instant::now();
    ensure!(
        request.fee_rate.is_finite() && (0.0..=0.01).contains(&request.fee_rate),
        "fee_rate must be in 0..0.01"
    );
    if let Some(amount) = request.max_pos_u {
        ensure!(
            amount.is_finite() && amount > 0.0,
            "invalid amount override"
        );
    }
    if let Some(t) = &request.thresholds {
        ensure!(
            [t.long_entry, t.short_entry, t.long_exit, t.short_exit]
                .into_iter()
                .all(|v| v.is_finite() && (0.0..=1.0).contains(&v))
                && t.long_entry > t.short_entry
                && t.long_exit < t.long_entry
                && t.short_exit > t.short_entry,
            "invalid entry/exit thresholds"
        );
    }
    let selected = request.profiles.iter().cloned().collect::<BTreeSet<_>>();
    ensure!(
        selected.len() == request.profiles.len(),
        "duplicate profile"
    );
    ensure!(
        selected
            .iter()
            .all(|name| bootstrap.profiles.iter().any(|p| p.name() == name)),
        "unknown profile"
    );
    let (cutoff, revision, caught_up, error) = {
        let data = memory.read().unwrap();
        (
            request.end_ms.unwrap_or(data.latest_data_ms),
            data.revision,
            data.caught_up,
            data.error.clone(),
        )
    };
    ensure!(
        cutoff > request.start_ms.unwrap_or(0) && request.start_ms.unwrap_or(0) >= 0,
        "invalid replay time range"
    );
    request.end_ms = Some(cutoff);
    let mut strategies = Vec::new();
    for profile in bootstrap
        .profiles
        .iter()
        .filter(|p| selected.is_empty() || selected.contains(p.name()))
    {
        let mut summary = Summary::default();
        let mut events = BTreeMap::new();
        for (symbol, rules) in profile.rules()? {
            for rule in rules {
                let single = rule_request(profile, &symbol, &rule.strategy, &request, cutoff)?;
                let input = model::prepare(&memory.read().unwrap(), profile, &single)?;
                let result =
                    model::simulate(profile, single, input).context("simulate strategy rule")?;
                let last = result.decisions.last();
                let part = Summary {
                    realized_pnl: result.realized_pnl,
                    unrealized_pnl: result.unrealized_pnl,
                    fees: result.fees,
                    closed_trades: result.closed_trades,
                    wins: result
                        .trades
                        .iter()
                        .filter(|t| t.exit_ms.is_some() && t.pnl > 0.0)
                        .count(),
                    rules: 1,
                    ready_rules: usize::from(result.ready_bars > 0),
                    ready_bars: result.ready_bars,
                    warmup_bars: result.warmup_bars,
                    missing_vwap: result.missing_vwap,
                    long_rules: usize::from(last.is_some_and(|r| r.flag > 0)),
                    short_rules: usize::from(last.is_some_and(|r| r.flag < 0)),
                    ..Summary::default()
                };
                summary.add(&part);
                add_curve(
                    &mut events,
                    result.decisions.iter().map(|row| (row.ts_ms, row.equity)),
                );
            }
        }
        let curve = finish_curve(events);
        summary.finish(&curve);
        strategies.push(StrategyResult {
            profile: profile.name().to_string(),
            strategy_name: profile.combination.manager_strategy_name(),
            strategy_group: profile.combination.strategy_group.clone(),
            symbol_group: profile
                .combination
                .symbol_group
                .clone()
                .unwrap_or_else(|| "all".into()),
            interval: profile
                .combination
                .signal_config()?
                .bar_interval
                .as_str()
                .into(),
            summary,
            curve,
        });
    }
    let mut groups = Vec::new();
    let names = strategies
        .iter()
        .map(|s| s.strategy_group.clone())
        .collect::<BTreeSet<_>>();
    for strategy_group in names {
        let mut summary = Summary::default();
        let mut events = BTreeMap::new();
        for strategy in strategies
            .iter()
            .filter(|s| s.strategy_group == strategy_group)
        {
            summary.add(&strategy.summary);
            add_curve(&mut events, strategy.curve.iter().map(|p| (p.ts_ms, p.pnl)));
        }
        let curve = finish_curve(events);
        summary.finish(&curve);
        groups.push(GroupResult {
            strategy_group,
            summary,
            curve,
        });
    }
    let mut summary = Summary::default();
    let mut events = BTreeMap::new();
    for group in &groups {
        summary.add(&group.summary);
        add_curve(&mut events, group.curve.iter().map(|p| (p.ts_ms, p.pnl)));
    }
    let curve = finish_curve(events);
    summary.finish(&curve);
    let revision_end = memory.read().unwrap().revision;
    Ok(PortfolioResult {
        parameters: request,
        cutoff_ms: cutoff,
        data_revision_start: revision,
        data_revision_end: revision_end,
        caught_up,
        data_error: error,
        price_method: "next_closed_bar_vwap",
        summary,
        curve,
        groups,
        strategies,
        compute_ms: started.elapsed().as_secs_f64() * 1000.0,
    })
}
