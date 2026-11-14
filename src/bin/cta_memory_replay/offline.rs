//! Batch counterfactuals from existing native factor history and minute candles.
use super::{
    model::{self, Bootstrap, Candle, Memory, Point},
    portfolio::{self, PortfolioRequest, PortfolioResult},
};
use anyhow::{ensure, Context, Result};
use mkt_signal::cta_factor_calc::{config::Parameters, engine::FactorPoint};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Deserialize)]
pub struct PriceBar {
    pub symbol: String,
    pub interval_ms: i64,
    pub candle: Candle,
}

#[derive(Deserialize)]
pub struct Job {
    pub label: String,
    pub request: PortfolioRequest,
}

#[derive(Deserialize)]
pub struct Input {
    pub source_generation: String,
    pub source_plan: serde_json::Value,
    pub points: Vec<FactorPoint>,
    pub candles: Vec<PriceBar>,
    pub jobs: Vec<Job>,
    pub rule_details_from_ms: Option<i64>,
}

#[derive(Serialize)]
pub struct RuleResult {
    pub profile: String,
    pub strategy_group: String,
    pub symbol: String,
    pub factor: String,
    pub strategy: String,
    pub nq: String,
    pub weight: f64,
    pub result: model::ReplayResult,
}

#[derive(Serialize)]
pub struct JobResult {
    pub label: String,
    pub result: PortfolioResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_details: Option<Vec<RuleResult>>,
}

#[derive(Serialize)]
pub struct Output {
    pub source_generation: String,
    pub point_count: usize,
    pub candle_count: usize,
    pub results: Vec<JobResult>,
}

pub fn run(bootstrap: &Bootstrap, path: &Path) -> Result<Output> {
    let input: Input = serde_json::from_reader(std::io::BufReader::new(std::fs::File::open(path)?))
        .context("read native historical replay input")?;
    ensure!(
        input.rule_details_from_ms.is_none_or(|t| t >= 0),
        "invalid rule details start time"
    );
    let plan = bootstrap.plan()?;
    let mut needed = BTreeSet::new();
    for job in &input.jobs {
        for profile in bootstrap.profiles.iter().filter(|p| {
            job.request.profiles.is_empty() || job.request.profiles.iter().any(|n| n == p.name())
        }) {
            for (symbol, rules) in profile.rules()? {
                for rule in rules {
                    needed.insert(profile.factor_id(&symbol, &rule.factor));
                    if profile
                        .defaults(&symbol, &rule.strategy)?
                        .entry
                        .nq_change_enabled
                    {
                        needed.insert(profile.factor_id(&symbol, "nq"));
                    }
                }
            }
        }
    }
    for id in &needed {
        let source: Parameters = serde_json::from_value(
            input
                .source_plan
                .get("calculations")
                .context("historical plan has no calculations")?
                .get(id)
                .and_then(|row| row.get("parameters"))
                .with_context(|| format!("historical plan is missing {id}"))?
                .clone(),
        )
        .with_context(|| format!("invalid historical parameters for {id}"))?;
        ensure!(
            source == plan.calculations[id].parameters,
            "historical factor definition differs: {id}"
        );
    }
    let mut data = Memory {
        caught_up: true,
        ..Memory::default()
    };
    let point_count = input.points.len();
    let candle_count = input.candles.len();
    for point in input.points {
        if !needed.contains(&point.factor_id) {
            continue;
        }
        let value = point
            .fields
            .get("value")
            .or_else(|| point.fields.get("long"))
            .cloned()
            .unwrap_or_default();
        data.latest_data_ms = data.latest_data_ms.max(point.ts_ms);
        data.points
            .entry(point.factor_id)
            .or_default()
            .push_back(Point {
                ts_ms: point.ts_ms,
                available_ms: point.ts_ms,
                value,
                short: point.fields.get("short").cloned(),
            });
    }
    for rows in data.points.values_mut() {
        let contiguous = rows.make_contiguous();
        contiguous.sort_unstable_by_key(|p| p.ts_ms);
        ensure!(
            contiguous.windows(2).all(|w| w[0].ts_ms < w[1].ts_ms),
            "duplicate historical factor point"
        );
    }
    for bar in input.candles {
        ensure!(
            bar.interval_ms > 0 && bar.candle.ts_ms % bar.interval_ms == 0,
            "invalid historical candle time"
        );
        data.latest_data_ms = data.latest_data_ms.max(bar.candle.ts_ms + bar.interval_ms);
        data.candles
            .entry((bar.symbol, bar.interval_ms))
            .or_default()
            .push_back(bar.candle);
    }
    for rows in data.candles.values_mut() {
        let contiguous = rows.make_contiguous();
        contiguous.sort_unstable_by_key(|p| p.ts_ms);
        ensure!(
            contiguous.windows(2).all(|w| w[0].ts_ms < w[1].ts_ms),
            "duplicate historical candle"
        );
    }
    let data = Arc::new(RwLock::new(data));
    let mut results = Vec::new();
    for job in input.jobs {
        let result = portfolio::calculate(bootstrap, &data, job.request.clone())?;
        let mut rule_details = None;
        if let Some(start) = input.rule_details_from_ms {
            let mut details = Vec::new();
            for profile in bootstrap.profiles.iter().filter(|p| {
                job.request.profiles.is_empty()
                    || job.request.profiles.iter().any(|n| n == p.name())
            }) {
                for (symbol, rules) in profile.rules()? {
                    for rule in rules {
                        let request = portfolio::rule_request(
                            profile,
                            &symbol,
                            &rule.strategy,
                            &job.request,
                            result.cutoff_ms,
                        )?;
                        let input = model::prepare(&data.read().unwrap(), profile, &request)?;
                        let mut result = model::simulate(profile, request, input)?;
                        result.decisions.retain(|d| d.ts_ms >= start);
                        result
                            .trades
                            .retain(|t| t.exit_ms.is_none_or(|end| end >= start));
                        details.push(RuleResult {
                            profile: profile.name().into(),
                            strategy_group: profile.combination.strategy_group.clone(),
                            symbol: symbol.clone(),
                            factor: rule.factor,
                            strategy: rule.strategy,
                            nq: rule.nq,
                            weight: rule.weight,
                            result,
                        });
                    }
                }
            }
            rule_details = Some(details);
        }
        results.push(JobResult {
            label: job.label,
            result,
            rule_details,
        });
    }
    Ok(Output {
        source_generation: input.source_generation,
        point_count,
        candle_count,
        results,
    })
}
