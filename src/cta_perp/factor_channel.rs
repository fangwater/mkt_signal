use super::config::{
    CtaPerpConfig, EntryConfig, PositionAlgorithm, CTA_PERP_MAX_MODEL_AGE_MS, SYMBOL_MEAN_STRATEGY,
};
use super::factors::{
    aggregate_bar, mean_bar, model_bar_is_fresh, model_service, table_services,
    FactorConfigWatcher, SymbolFactorTable, SymbolFactors, WeightedBar,
};
use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use trade_signal::model_output_hub::{ModelOutputHub, ModelOutputScoreLookupResult};

pub struct WeightedFactorChannel {
    watcher: FactorConfigWatcher,
    table: SymbolFactorTable,
    retain_retired: bool,
    node: Node<ipc::Service>,
    hub: ModelOutputHub,
    last_ts: HashMap<String, i64>,
    last_subscribe: Instant,
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
    pub async fn new(key: String, node_name: &str, retain_retired: bool) -> Result<Self> {
        let watcher = FactorConfigWatcher::new(key).await?;
        let table = watcher.current().as_ref().clone();
        let node = NodeBuilder::new()
            .name(&NodeName::new(node_name)?)
            .create::<ipc::Service>()?;
        let mut hub = ModelOutputHub::new_with_max_subscribers(TradingVenue::BinanceFutures, 32);
        hub.update_services(&node, table_services(&table));
        Ok(Self {
            watcher,
            table,
            retain_retired,
            node,
            hub,
            last_ts: HashMap::new(),
            last_subscribe: Instant::now(),
        })
    }

    pub fn key(&self) -> &str {
        &self.watcher.key
    }
    pub fn table(&self) -> &SymbolFactorTable {
        &self.table
    }

    /// v0 consumes each factor independently; v1 waits for an aligned symbol bar.
    pub fn poll_positions(&mut self, config: &CtaPerpConfig) -> (bool, Vec<PositionObservation>) {
        let changed = if let Some(next) = self.watcher.take_change() {
            self.table = next.as_ref().clone();
            self.hub
                .update_services(&self.node, table_services(&self.table));
            self.last_subscribe = Instant::now();
            true
        } else {
            false
        };
        if self.last_subscribe.elapsed() >= Duration::from_secs(10) {
            self.hub
                .update_services(&self.node, table_services(&self.table));
            self.last_subscribe = Instant::now();
        }
        let touched: HashSet<String> = self
            .hub
            .poll_updates()
            .into_iter()
            .map(|event| event.symbol_key)
            .collect();
        let mut updates = Vec::new();
        for symbol in touched {
            let effective = config.for_symbol(&symbol);
            let Some(row) = self.table.get(&symbol) else {
                continue;
            };
            if config.position_algorithm == PositionAlgorithm::V1 {
                let inputs: Vec<_> = row
                    .factors
                    .iter()
                    .map(|factor| {
                        self.hub.cached_score(
                            &model_service(factor),
                            &symbol,
                            TradingVenue::BinanceFutures,
                        )
                    })
                    .collect();
                if let Some(observation) = next_mean_observation(
                    symbol,
                    row,
                    &inputs,
                    &effective.entry,
                    &mut self.last_ts,
                    get_timestamp_us(),
                ) {
                    updates.push(observation);
                }
                continue;
            }
            for (factor, weight) in row
                .factors
                .iter()
                .zip(&row.weights)
                .filter(|(_, w)| **w != 0.0)
            {
                let input = self.hub.cached_score(
                    &model_service(factor),
                    &symbol,
                    TradingVenue::BinanceFutures,
                );
                let key = format!("{factor}:{symbol}");
                let ts = input.score_ts_ms;
                if ts <= 0
                    || self.last_ts.get(&key).is_some_and(|old| ts <= *old)
                    || !model_bar_is_fresh(ts, get_timestamp_us(), CTA_PERP_MAX_MODEL_AGE_MS)
                {
                    continue;
                }
                self.last_ts.insert(key, ts);
                let single = SymbolFactors {
                    factors: vec![factor.clone()],
                    weights: vec![weight.signum()],
                };
                updates.push(PositionObservation {
                    symbol: symbol.clone(),
                    factor: factor.clone(),
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
        (changed, updates)
    }

    pub fn poll(&mut self, entry: &EntryConfig) -> (bool, Vec<FactorObservation>) {
        let changed = if let Some(next) = self.watcher.take_change() {
            if self.retain_retired {
                self.table.extend(next.as_ref().clone());
            } else {
                self.table = next.as_ref().clone();
            }
            self.hub
                .update_services(&self.node, table_services(&self.table));
            self.last_subscribe = Instant::now();
            true
        } else {
            false
        };
        if self.last_subscribe.elapsed() >= Duration::from_secs(10) {
            self.hub
                .update_services(&self.node, table_services(&self.table));
            self.last_subscribe = Instant::now();
        }
        let touched: HashSet<String> = self
            .hub
            .poll_updates()
            .into_iter()
            .map(|event| event.symbol_key)
            .collect();
        let mut updates = Vec::new();
        for symbol in touched {
            let Some(row) = self.table.get(&symbol) else {
                continue;
            };
            let inputs: Vec<_> = row
                .factors
                .iter()
                .map(|factor| {
                    self.hub.cached_score(
                        &model_service(factor),
                        &symbol,
                        TradingVenue::BinanceFutures,
                    )
                })
                .collect();
            if let Some(observation) = next_observation(
                symbol,
                row,
                &inputs,
                entry,
                &mut self.last_ts,
                get_timestamp_us(),
            ) {
                updates.push(observation);
            }
        }
        (changed, updates)
    }
}

fn next_mean_observation(
    symbol: String,
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
    last_ts: &mut HashMap<String, i64>,
    now_us: i64,
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
        || !model_bar_is_fresh(model_ts_ms, now_us, CTA_PERP_MAX_MODEL_AGE_MS)
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
fn next_observation(
    symbol: String,
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
    last_ts: &mut HashMap<String, i64>,
    now_us: i64,
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
        || !model_bar_is_fresh(model_ts_ms, now_us, CTA_PERP_MAX_MODEL_AGE_MS)
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
            121_000_000
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
            121_000_000
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
            181_000_000
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
            121_000_000
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
                121_000_000
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
            121_000_000
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
            121_000_000
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
            241_000_000
        )
        .is_none());
    }
}
