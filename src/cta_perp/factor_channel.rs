use super::config::{CtaPerpConfig, EntryConfig, PositionAlgorithm, SYMBOL_MEAN_STRATEGY};
use super::factors::{
    aggregate_bar, mean_bar, model_bar_is_fresh, SymbolFactorTable, SymbolFactors, WeightedBar,
};
use crate::cta_factor_calc::{
    client::FactorFeed,
    config::{Combination, Worker},
    engine::FactorPoint,
};
use anyhow::Result;
use runtime_common::time_util::get_timestamp_us;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};
use trade_signal::model_output_hub::ModelOutputScoreLookupResult;

/// Signal inputs come exclusively from the merged factor process and the same
/// combination document that owns signal controls. No per-strategy Redis table.
pub struct WeightedFactorChannel {
    combination: Combination,
    table: SymbolFactorTable,
    feed: Option<FactorFeed>,
    last_connect: Instant,
    last_ts: HashMap<String, i64>,
}

pub struct FactorObservation {
    pub symbol: String,
    pub model_ts_ms: i64,
    pub bar: Option<WeightedBar>,
}

pub struct PositionObservation {
    pub symbol: String,
    pub factor: String,
    pub model_ts_ms: i64,
    pub bar: Option<WeightedBar>,
}

impl WeightedFactorChannel {
    pub fn new(combination: Combination) -> Result<Self> {
        let table = combination.signal_table()?;
        Ok(Self {
            combination,
            table,
            feed: None,
            last_connect: Instant::now() - Duration::from_secs(10),
            last_ts: HashMap::new(),
        })
    }
    pub fn apply_combination(&mut self, next: Combination) -> Result<bool> {
        let table = next.signal_table()?;
        let changed = table != self.table;
        self.table = table;
        self.combination = next;
        Ok(changed)
    }
    pub fn retire(&mut self) {
        self.table.clear();
    }
    pub fn key(&self) -> &str {
        &self.combination.name
    }
    pub fn combination(&self) -> &Combination {
        &self.combination
    }
    pub fn table(&self) -> &SymbolFactorTable {
        &self.table
    }

    pub fn poll_positions(&mut self, config: &CtaPerpConfig) -> (bool, Vec<PositionObservation>) {
        if self.table.is_empty() {
            return (false, Vec::new());
        }
        if self.feed.is_none() && self.last_connect.elapsed() >= Duration::from_secs(5) {
            self.last_connect = Instant::now();
            match FactorFeed::new(Worker::Perp) {
                Ok(feed) => self.feed = Some(feed),
                Err(err) => log::warn!("shared CTA factor feed unavailable: {err:#}"),
            }
        }
        let Some(feed) = &mut self.feed else {
            return (false, Vec::new());
        };
        if let Err(err) = feed.poll() {
            log::warn!("shared CTA factor feed failed: {err:#}");
            self.feed = None;
            return (false, Vec::new());
        }
        let now = get_timestamp_us();
        let mut updates = Vec::new();
        for (symbol, row) in &self.table {
            if config.position_algorithm == PositionAlgorithm::V1 {
                let effective = config.for_symbol(symbol);
                let inputs = row
                    .factors
                    .iter()
                    .map(|factor| {
                        shared_input(feed.latest(), symbol, factor, &effective, now / 1000)
                    })
                    .collect::<Option<Vec<_>>>();
                if let Some(inputs) = inputs {
                    if let Some(observation) = next_mean_observation(
                        symbol.clone(),
                        row,
                        &inputs,
                        &effective.entry,
                        &mut self.last_ts,
                        now,
                        config.bar_interval.max_model_age_ms(),
                    ) {
                        updates.push(observation);
                    }
                }
            } else {
                for binding in config.position_factors(symbol, row) {
                    let effective = config.for_strategy(symbol, &binding.strategy);
                    let Some(input) = shared_input(
                        feed.latest(),
                        symbol,
                        &binding.factor,
                        &effective,
                        now / 1000,
                    ) else {
                        continue;
                    };
                    let key = format!("{}:{symbol}", binding.strategy);
                    let ts = input.score_ts_ms;
                    if self.last_ts.get(&key).is_some_and(|old| ts <= *old)
                        || !model_bar_is_fresh(ts, now, config.bar_interval.max_model_age_ms())
                    {
                        continue;
                    }
                    self.last_ts.insert(key, ts);
                    let single = SymbolFactors {
                        factors: vec![binding.factor],
                        weights: vec![binding.weight.signum()],
                    };
                    updates.push(PositionObservation {
                        symbol: symbol.clone(),
                        factor: binding.strategy,
                        model_ts_ms: ts,
                        bar: aggregate_bar(
                            &single,
                            &[input],
                            &effective.entry,
                            effective.position_algorithm,
                        ),
                    });
                }
            }
        }
        (false, updates)
    }
}

pub(crate) fn shared_input(
    points: &BTreeMap<String, FactorPoint>,
    symbol: &str,
    factor: &str,
    config: &CtaPerpConfig,
    now_ms: i64,
) -> Option<ModelOutputScoreLookupResult> {
    let id = |name: &str| {
        format!(
            "binance-futures-{}-{}-{}",
            symbol.to_ascii_lowercase(),
            config.bar_interval.as_str().replace('m', "min"),
            name.to_ascii_lowercase()
        )
    };
    let point = points.get(&id(factor))?;
    if point.ts_ms > now_ms {
        return None;
    }
    let value = point.fields.get("value")?;
    let nq = points.get(&id("nq")).filter(|n| n.ts_ms == point.ts_ms);
    // Wait for both independently published inputs for this closed bar. An old
    // NQ must never consume the current factor's one decision opportunity.
    if config.entry.nq_change_enabled && nq.is_none() {
        return None;
    }
    let long = nq.and_then(|n| n.fields.get("long"));
    let short = nq.and_then(|n| n.fields.get("short"));
    Some(ModelOutputScoreLookupResult {
        service_name: format!("factor_output/{}", Worker::Perp.name()),
        symbol_key: symbol.into(),
        subscribed: true,
        score: value.value,
        score_quantile: value.quantile,
        score_ready: value.ready,
        score_long_threshold: None,
        score_short_threshold: None,
        filter_long_value: long.and_then(|f| f.value),
        filter_short_value: short.and_then(|f| f.value),
        filter_long_threshold: long.and_then(|f| f.median),
        filter_short_threshold: short.and_then(|f| f.median),
        filter_long_quantile: long.and_then(|f| f.quantile),
        filter_short_quantile: short.and_then(|f| f.quantile),
        filter_ready: long.is_some_and(|f| f.ready) && short.is_some_and(|f| f.ready),
        score_ts_ms: point.ts_ms,
        note: String::new(),
    })
}

fn next_mean_observation(
    symbol: String,
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
    last_ts: &mut HashMap<String, i64>,
    now_us: i64,
    max_age_ms: u64,
) -> Option<PositionObservation> {
    let mut timestamps = inputs
        .iter()
        .zip(&row.weights)
        .filter(|(_, weight)| **weight != 0.0)
        .map(|(input, _)| input.score_ts_ms);
    let model_ts_ms = timestamps.next()?;
    let key = format!("{SYMBOL_MEAN_STRATEGY}:{symbol}");
    if model_ts_ms <= 0
        || timestamps.any(|ts| ts != model_ts_ms)
        || last_ts.get(&key).is_some_and(|ts| model_ts_ms <= *ts)
        || !model_bar_is_fresh(model_ts_ms, now_us, max_age_ms)
    {
        return None;
    }
    last_ts.insert(key, model_ts_ms);
    Some(PositionObservation {
        symbol,
        factor: SYMBOL_MEAN_STRATEGY.into(),
        model_ts_ms,
        bar: mean_bar(row, inputs, entry),
    })
}

// An aligned but unready bar still carries exit settings so recovered positions
// can retain trailing protection without using an invalid factor percentile.
#[cfg(test)]
fn next_observation(
    symbol: String,
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
    last_ts: &mut HashMap<String, i64>,
    now_us: i64,
    max_age_ms: u64,
) -> Option<FactorObservation> {
    let mut timestamps = inputs
        .iter()
        .zip(&row.weights)
        .filter(|(_, weight)| **weight != 0.0)
        .map(|(input, _)| input.score_ts_ms);
    let model_ts_ms = timestamps.next()?;
    if model_ts_ms <= 0
        || timestamps.any(|ts| ts != model_ts_ms)
        || last_ts.get(&symbol).is_some_and(|ts| model_ts_ms <= *ts)
        || !model_bar_is_fresh(model_ts_ms, now_us, max_age_ms)
    {
        return None;
    }
    last_ts.insert(symbol.clone(), model_ts_ms);
    Some(FactorObservation {
        symbol,
        model_ts_ms,
        bar: aggregate_bar(row, inputs, entry, PositionAlgorithm::V0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_perp::config::CTA_PERP_MAX_MODEL_AGE_MS;
    #[test]
    fn shared_inputs_wait_for_same_bar_nq_and_keep_linear_median() {
        use crate::cta_factor_calc::engine::Field;
        let combination = Combination::load(std::path::Path::new(
            "config/cta_prep_binance_futures/binance-futures_5min.json",
        ))
        .unwrap();
        let mut config = combination.signal_config().unwrap();
        assert!(config.linear_nq);
        let factor = FactorPoint {
            factor_id: "binance-futures-ethusdt-5min-factor_036".into(),
            ts_ms: 600_000,
            fields: BTreeMap::from([(
                "value".into(),
                Field {
                    raw: Some(2.0),
                    value: Some(1.0),
                    quantile: Some(0.99),
                    median: Some(0.0),
                    ready: true,
                },
            )]),
        };
        let mut points = BTreeMap::from([(factor.factor_id.clone(), factor)]);
        assert!(shared_input(&points, "ETHUSDT", "factor_036", &config, 601_000).is_none());
        let mut nq = FactorPoint {
            factor_id: "binance-futures-ethusdt-5min-nq".into(),
            ts_ms: 300_000,
            fields: BTreeMap::from([
                (
                    "long".into(),
                    Field {
                        raw: Some(0.1),
                        value: Some(0.1),
                        quantile: Some(0.2),
                        median: Some(0.1),
                        ready: true,
                    },
                ),
                (
                    "short".into(),
                    Field {
                        raw: Some(-0.1),
                        value: Some(-0.1),
                        quantile: Some(0.8),
                        median: Some(-0.1),
                        ready: true,
                    },
                ),
            ]),
        };
        points.insert(nq.factor_id.clone(), nq.clone());
        assert!(shared_input(&points, "ETHUSDT", "factor_036", &config, 601_000).is_none());
        nq.ts_ms = 600_000;
        points.insert(nq.factor_id.clone(), nq);
        let input = shared_input(&points, "ETHUSDT", "factor_036", &config, 601_000).unwrap();
        assert_eq!(input.filter_long_threshold, Some(0.1));
        let row = SymbolFactors {
            factors: vec!["factor_036".into()],
            weights: vec![1.0],
        };
        let bar = aggregate_bar(&row, &[input], &config.entry, config.position_algorithm).unwrap();
        assert_eq!(
            super::super::positions::next_position_flag(
                0,
                &bar,
                &config.for_strategy("ETHUSDT", "factor_036")
            ),
            1
        );
        config.entry.nq_change_enabled = false;
        points.remove("binance-futures-ethusdt-5min-nq");
        assert!(shared_input(&points, "ETHUSDT", "factor_036", &config, 601_000).is_some());
        assert!(shared_input(&points, "ETHUSDT", "factor_036", &config, 599_999).is_none());
    }
    fn input(ts: i64) -> ModelOutputScoreLookupResult {
        ModelOutputScoreLookupResult {
            service_name: String::new(),
            symbol_key: "ETHUSDT".into(),
            subscribed: true,
            score: Some(1.0),
            score_quantile: Some(0.95),
            score_ready: true,
            score_long_threshold: None,
            score_short_threshold: None,
            filter_long_value: None,
            filter_long_threshold: None,
            filter_short_value: None,
            filter_short_threshold: None,
            filter_long_quantile: None,
            filter_short_quantile: None,
            filter_ready: false,
            score_ts_ms: ts,
            note: String::new(),
        }
    }
    #[test]
    fn mean_waits_for_aligned_factors_and_emits_one_symbol_strategy() {
        let row = SymbolFactors {
            factors: vec!["factor_036".into(), "factor_051".into()],
            weights: vec![10.0, 1.0],
        };
        let mut last = HashMap::new();
        let entry = EntryConfig {
            factor_long_quantile: 0.8,
            ..Default::default()
        };
        assert!(next_mean_observation(
            "ETHUSDT".into(),
            &row,
            &[input(120_000), input(60_000)],
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
        assert!(last.is_empty());
        let bar = next_mean_observation(
            "ETHUSDT".into(),
            &row,
            &[input(120_000), input(120_000)],
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .unwrap();
        assert_eq!(bar.factor, SYMBOL_MEAN_STRATEGY);
        assert_eq!(bar.bar.unwrap().value, 1.0);
        assert!(next_mean_observation(
            "ETHUSDT".into(),
            &row,
            &[input(120_000), input(120_000)],
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
        let mut unready = input(180_000);
        unready.score_ready = false;
        assert!(next_mean_observation(
            "ETHUSDT".into(),
            &row,
            &[input(180_000), unready],
            &entry,
            &mut last,
            181_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .unwrap()
        .bar
        .is_none());
    }

    #[test]
    fn partial_arrival_does_not_consume_bar_and_complete_bar_is_emitted_once() {
        let row = SymbolFactors {
            factors: vec!["factor_036".into(), "factor_051".into()],
            weights: vec![2.0, 1.0],
        };
        let mut last = HashMap::new();
        let entry = EntryConfig::default();
        assert!(next_observation(
            "ETHUSDT".into(),
            &row,
            &[input(120_000), input(60_000)],
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
        assert!(last.is_empty());
        let inputs = [input(120_000), input(120_000)];
        assert_eq!(
            next_observation(
                "ETHUSDT".into(),
                &row,
                &inputs,
                &entry,
                &mut last,
                121_000_000,
                CTA_PERP_MAX_MODEL_AGE_MS,
            )
            .unwrap()
            .bar
            .unwrap()
            .coefficient,
            1.0
        );
        assert!(next_observation(
            "ETHUSDT".into(),
            &row,
            &inputs,
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
        // A hot weight/sign edit does not replay an already consumed model bar.
        let modified = SymbolFactors {
            weights: vec![-1.0, -1.0],
            ..row
        };
        assert!(next_observation(
            "ETHUSDT".into(),
            &modified,
            &inputs,
            &entry,
            &mut last,
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
    }
    #[test]
    fn signed_weights_wait_for_every_nonzero_factor_and_emit_negative_only_rows() {
        for weights in [vec![-2.0, -1.0, 0.0], vec![2.0, -1.0, 0.0]] {
            let row = SymbolFactors {
                factors: vec![
                    "factor_036".into(),
                    "factor_051".into(),
                    "factor_170".into(),
                ],
                weights,
            };
            let mut last = HashMap::new();
            let entry = EntryConfig::default();
            assert!(next_observation(
                "ETHUSDT".into(),
                &row,
                &[input(120_000), input(60_000), input(0)],
                &entry,
                &mut last,
                121_000_000,
                CTA_PERP_MAX_MODEL_AGE_MS,
            )
            .is_none());
            assert!(last.is_empty());
            let observation = next_observation(
                "ETHUSDT".into(),
                &row,
                &[input(120_000), input(120_000), input(0)],
                &entry,
                &mut last,
                121_000_000,
                CTA_PERP_MAX_MODEL_AGE_MS,
            )
            .expect("all nonzero signed weights have a complete bar");
            let bar = observation.bar.expect("signed combination ready");
            assert_eq!(bar.votes.len(), 2);
            assert_eq!(bar.model_ts_ms, 120_000);
            assert!(
                (bar.value
                    - if row.weights[0] < 0.0 {
                        -1.0
                    } else {
                        1.0 / 3.0
                    })
                .abs()
                    < 1e-12
            );
            assert_eq!(last["ETHUSDT"], 120_000);
        }
    }

    #[test]
    fn unready_observation_preserves_exit_configuration_path_but_has_no_signal() {
        let row = SymbolFactors {
            factors: vec!["factor_036".into()],
            weights: vec![1.0],
        };
        let mut unready = input(120_000);
        unready.score_ready = false;
        let result = next_observation(
            "ETHUSDT".into(),
            &row,
            &[unready],
            &EntryConfig::default(),
            &mut HashMap::new(),
            121_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .unwrap();
        assert_eq!(result.model_ts_ms, 120_000);
        assert!(result.bar.is_none());
        assert!(next_observation(
            "ETHUSDT".into(),
            &row,
            &[input(120_000)],
            &EntryConfig::default(),
            &mut HashMap::new(),
            241_000_000,
            CTA_PERP_MAX_MODEL_AGE_MS,
        )
        .is_none());
    }
}
