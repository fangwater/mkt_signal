use crate::cta_perp::config::{EntryConfig, PositionAlgorithm};
use crate::factor_pub::fusion_factor_pub::app::BaselineReplayState;
use crate::factor_pub::fusion_factor_pub::SymbolFactorPlan;
use anyhow::{ensure, Context, Result};
use order_common::Side;
use runtime_common::redis_client::{RedisClient, RedisSettings};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use trade_signal::model_output_hub::ModelOutputScoreLookupResult;

pub type SymbolFactorTable = BTreeMap<String, SymbolFactors>;
pub const WEIGHTED_RULE_NAME: &str = "weighted";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FactorSign {
    #[serde(rename = "+")]
    Positive,
    #[serde(rename = "-")]
    Negative,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(try_from = "BTreeMap<String, f64>", into = "BTreeMap<String, f64>")]
pub struct SymbolFactors {
    pub factors: Vec<String>,
    pub weights: Vec<f64>,
}

impl TryFrom<BTreeMap<String, f64>> for SymbolFactors {
    type Error = anyhow::Error;
    fn try_from(values: BTreeMap<String, f64>) -> Result<Self> {
        ensure!(
            !values.is_empty() && values.len() <= 32,
            "symbol requires 1..=32 factor weights"
        );
        let (factors, weights) = values.into_iter().unzip();
        Ok(Self { factors, weights })
    }
}

impl From<SymbolFactors> for BTreeMap<String, f64> {
    fn from(row: SymbolFactors) -> Self {
        row.factors.into_iter().zip(row.weights).collect()
    }
}

pub fn factor_config_key(env: &str) -> String {
    format!("{env}:cta_symbol_factors")
}

pub fn runtime_factor_config_key() -> Result<String> {
    let cwd = std::env::current_dir().context("resolve CTA environment directory")?;
    let env = cwd.file_name().and_then(|name| name.to_str()).unwrap_or("");
    ensure!(
        env.starts_with("binance-cta-perp-"),
        "CTA perp requires a binance-cta-perp-<tag> working directory"
    );
    Ok(factor_config_key(env))
}

pub fn model_service(factor: &str) -> String {
    super::model_service::model_service_path("binance-futures", factor)
}

pub fn parse_factor_table(raw: &str) -> Result<SymbolFactorTable> {
    let mut table: SymbolFactorTable =
        serde_json::from_str(raw).context("parse CTA symbol factor table")?;
    ensure!(
        table.len() <= 256,
        "CTA factor table supports at most 256 symbols"
    );
    let mut canonical = BTreeMap::new();
    for (raw_symbol, mut row) in std::mem::take(&mut table) {
        let symbol = raw_symbol.trim().to_ascii_uppercase();
        ensure!(
            !symbol.is_empty()
                && symbol.len() <= 32
                && symbol.bytes().all(|c| c.is_ascii_alphanumeric()),
            "invalid CTA symbol {raw_symbol:?}"
        );
        let n = row.factors.len();
        ensure!(
            n > 0 && n <= 32 && row.weights.len() == n,
            "{symbol}: factor weights must contain 1..=32 entries"
        );
        let mut names = HashSet::new();
        for factor in &mut row.factors {
            *factor = factor.trim().to_ascii_lowercase();
            if factor.starts_with("td_") || factor.starts_with("tp_") {
                *factor = factor.to_ascii_uppercase();
            }
            ensure!(
                names.insert(factor.clone()),
                "{symbol}: duplicate factor {factor}"
            );
        }
        ensure!(
            row.weights.iter().all(|w| w.is_finite()) && row.weights.iter().any(|w| *w != 0.0),
            "{symbol}: signed weights must be finite and have a positive absolute total"
        );
        let plan = SymbolFactorPlan::from_factor_names(&symbol, row.factors.clone())?;
        BaselineReplayState::validate_factor_plan(&plan)
            .with_context(|| format!("{symbol}: validate factors"))?;
        ensure!(
            canonical.insert(symbol.clone(), row).is_none(),
            "duplicate normalized symbol {symbol}"
        );
    }
    ensure!(
        table_services(&canonical).len() <= 32,
        "CTA factor table requires more than 32 model services"
    );
    Ok(canonical)
}

impl SymbolFactors {
    /// Scale by the maximum first so finite weights cannot overflow their sum.
    pub fn normalized_weights(&self) -> Vec<f64> {
        let max = self.weights.iter().map(|w| w.abs()).fold(0.0_f64, f64::max);
        let total: f64 = self.weights.iter().map(|w| w.abs() / max).sum();
        self.weights
            .iter()
            .map(|w| (w.abs() / max) / total)
            .collect()
    }
}

pub fn table_services(table: &SymbolFactorTable) -> Vec<String> {
    let mut services: Vec<String> = table
        .values()
        .flat_map(|row| {
            row.factors
                .iter()
                .zip(&row.weights)
                .filter(|(_, weight)| **weight != 0.0)
                .map(|(factor, _)| model_service(factor))
        })
        .collect();
    services.sort();
    services.dedup();
    services
}

#[derive(Debug, Clone, Serialize)]
pub struct FactorVote {
    pub factor: String,
    pub sign: FactorSign,
    pub weight: f64,
    pub normalized_weight: f64,
    pub score: f64,
    pub corrected_score: f64,
    pub quantile: f64,
    pub corrected_quantile: f64,
    pub decision_value: f64,
    pub vote: i8,
}

#[derive(Debug, Clone)]
pub struct WeightedBar {
    pub model_ts_ms: i64,
    pub value: f64,
    pub coefficient: f64,
    pub quantile: f64,
    pub decision_value: f64,
    pub votes: Vec<FactorVote>,
    pub nq: ModelOutputScoreLookupResult,
}

impl WeightedBar {
    pub fn side(&self) -> Option<Side> {
        if self.value > 0.0 {
            Some(Side::Buy)
        } else if self.value < 0.0 {
            Some(Side::Sell)
        } else {
            None
        }
    }
}

pub fn aggregate_bar(
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
    algorithm: PositionAlgorithm,
) -> Option<WeightedBar> {
    if inputs.len() != row.factors.len()
        || row.weights.len() != inputs.len()
        || !row.weights.iter().all(|w| w.is_finite())
        || !row.weights.iter().any(|w| *w != 0.0)
    {
        return None;
    }
    let weights = row.normalized_weights();
    let mut first: Option<usize> = None;
    let mut value = 0.0;
    let mut quantile = 0.0;
    let mut votes = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        if row.weights[i] == 0.0 {
            continue;
        }
        if !input.subscribed
            || !input.score_ready
            || input.score_ts_ms <= 0
            || !input.score.is_some_and(f64::is_finite)
        {
            return None;
        }
        let q = input
            .score_quantile
            .filter(|q| q.is_finite() && (0.0..=1.0).contains(q))?;
        match first {
            Some(index) if inputs[index].score_ts_ms != input.score_ts_ms => return None,
            None => first = Some(i),
            _ => {}
        }
        let corrected = if row.weights[i] > 0.0 { q } else { 1.0 - q };
        let decision_value = algorithm.decision_value(corrected);
        // Reflecting 0.9 can produce 0.09999999999999998. Keep threshold ties
        // neutral under sign correction instead of inventing a boundary vote.
        let quantile_epsilon = 4.0 * f64::EPSILON;
        let vote = if decision_value - entry.factor_long_quantile > quantile_epsilon {
            1
        } else if entry.factor_short_quantile - decision_value > quantile_epsilon {
            -1
        } else {
            0
        };
        value += weights[i] * f64::from(vote);
        quantile += weights[i] * corrected;
        votes.push(FactorVote {
            factor: row.factors[i].clone(),
            sign: if row.weights[i] > 0.0 {
                FactorSign::Positive
            } else {
                FactorSign::Negative
            },
            weight: row.weights[i],
            normalized_weight: weights[i],
            score: input.score?,
            corrected_score: if row.weights[i] > 0.0 {
                input.score?
            } else {
                -input.score?
            },
            quantile: q,
            corrected_quantile: corrected,
            decision_value,
            vote,
        });
    }
    let first = first?;
    // Suppress floating-point residues from offsetting normalized votes.
    if value.abs() <= 1e-12 {
        value = 0.0;
    }
    let value = value.clamp(-1.0, 1.0);
    Some(WeightedBar {
        model_ts_ms: inputs[first].score_ts_ms,
        value,
        coefficient: value.abs(),
        quantile: quantile.clamp(0.0, 1.0),
        decision_value: algorithm.decision_value(quantile.clamp(0.0, 1.0)),
        votes,
        nq: inputs[first].clone(),
    })
}

pub fn model_bar_is_fresh(ts_ms: i64, now_us: i64, max_age_ms: u64) -> bool {
    ts_ms > 0
        && ts_ms.saturating_mul(1_000) <= now_us.saturating_add(5_000_000)
        && now_us.saturating_sub(ts_ms.saturating_mul(1_000))
            <= (max_age_ms as i64).saturating_mul(1_000)
}

/// One symbol decision from the arithmetic mean of all active factor values.
/// Magnitudes in the factor table do not change the equal weighting.
pub fn mean_bar(
    row: &SymbolFactors,
    inputs: &[ModelOutputScoreLookupResult],
    entry: &EntryConfig,
) -> Option<WeightedBar> {
    if !row.weights.iter().all(|w| w.is_finite()) {
        return None;
    }
    let equal = SymbolFactors {
        factors: row.factors.clone(),
        weights: row
            .weights
            .iter()
            .map(|w| if *w == 0.0 { 0.0 } else { w.signum() })
            .collect(),
    };
    let mut bar = aggregate_bar(&equal, inputs, entry, PositionAlgorithm::V1)?;
    bar.decision_value = bar
        .votes
        .iter()
        .map(|vote| vote.decision_value)
        .sum::<f64>()
        / bar.votes.len() as f64;
    bar.value = if bar.decision_value - entry.factor_long_quantile > 4.0 * f64::EPSILON {
        1.0
    } else if entry.factor_short_quantile - bar.decision_value > 4.0 * f64::EPSILON {
        -1.0
    } else {
        0.0
    };
    bar.coefficient = bar.value.abs();
    Some(bar)
}

/// Redis polling runs outside the trading reactor. One GET reads the entire table.
pub struct FactorConfigWatcher {
    receiver: watch::Receiver<Arc<SymbolFactorTable>>,
    task: JoinHandle<()>,
    pub key: String,
}

impl FactorConfigWatcher {
    pub async fn new(key: String) -> Result<Self> {
        let mut settings = RedisSettings::default();
        if let Ok(host) = std::env::var("REDIS_HOST") {
            settings.host = host;
        }
        if let Ok(port) = std::env::var("REDIS_PORT") {
            settings.port = port.parse().context("invalid REDIS_PORT")?;
        }
        if let Ok(db) = std::env::var("REDIS_DB") {
            settings.db = db.parse().context("invalid REDIS_DB")?;
        }
        settings.username = std::env::var("REDIS_USERNAME").ok();
        settings.password = std::env::var("REDIS_PASSWORD").ok();
        Self::with_settings(key, settings).await
    }

    async fn with_settings(key: String, settings: RedisSettings) -> Result<Self> {
        let mut redis = tokio::time::timeout(Duration::from_secs(3), async {
            RedisClient::connect(settings)
                .await
                .map_err(|_| anyhow::anyhow!("connect CTA factor Redis failed"))
        })
        .await
        .context("CTA factor Redis connect timed out")??;
        let raw = tokio::time::timeout(Duration::from_secs(1), redis.get_string(&key))
            .await
            .context("CTA factor Redis read timed out")??;
        let table = parse_factor_table(
            &raw.with_context(|| format!("missing CTA factor configuration {key}"))?,
        )?;
        let (sender, receiver) = watch::channel(Arc::new(table));
        let poll_key = key.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut last_warning = None;
            loop {
                interval.tick().await;
                let next = async {
                    let raw = redis.get_string(&poll_key).await?;
                    // DEL disables new entries; pre-trade retains retired symbols for exits.
                    parse_factor_table(raw.as_deref().unwrap_or("{}"))
                };
                match tokio::time::timeout(Duration::from_secs(1), next).await {
                    Ok(Ok(table)) => {
                        let changed = sender.borrow().as_ref() != &table;
                        if changed && sender.send(Arc::new(table)).is_err() {
                            break;
                        }
                    }
                    error => {
                        let now = std::time::Instant::now();
                        if last_warning.is_none_or(|last: std::time::Instant| {
                            now.duration_since(last) >= Duration::from_secs(30)
                        }) {
                            match error {
                                Ok(Err(err)) => log::warn!(
                                    "CTA factor config reload rejected key={poll_key}: {err:#}"
                                ),
                                _ => {
                                    log::warn!("CTA factor config reload timed out key={poll_key}")
                                }
                            }
                            last_warning = Some(now);
                        }
                    }
                }
            }
        });
        Ok(Self {
            receiver,
            task,
            key,
        })
    }

    pub fn current(&self) -> Arc<SymbolFactorTable> {
        self.receiver.borrow().clone()
    }
    pub fn take_change(&mut self) -> Option<Arc<SymbolFactorTable>> {
        self.receiver
            .has_changed()
            .ok()
            .filter(|changed| *changed)?;
        Some(self.receiver.borrow_and_update().clone())
    }
}

impl Drop for FactorConfigWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(weights: Vec<f64>, signs: Vec<FactorSign>) -> SymbolFactors {
        SymbolFactors {
            factors: (0..weights.len())
                .map(|i| format!("factor_{i:03}"))
                .collect(),
            weights: weights
                .into_iter()
                .zip(signs)
                .map(|(w, s)| if s == FactorSign::Negative { -w } else { w })
                .collect(),
        }
    }

    fn input(q: f64) -> ModelOutputScoreLookupResult {
        ModelOutputScoreLookupResult {
            service_name: "model_output/cta_perp-binance-futures-1m-factor_036".into(),
            symbol_key: "ETHUSDT".into(),
            subscribed: true,
            score: Some(2.0),
            score_quantile: Some(q),
            score_ready: true,
            score_long_threshold: Some(1.0),
            score_short_threshold: Some(-1.0),
            filter_long_value: Some(0.2),
            filter_long_threshold: Some(0.1),
            filter_short_value: Some(-0.2),
            filter_short_threshold: Some(-0.1),
            filter_long_quantile: Some(0.75),
            filter_short_quantile: Some(0.25),
            filter_ready: true,
            score_ts_ms: 60_000,
            note: "ok".into(),
        }
    }

    fn bar(weights: Vec<f64>, signs: Vec<FactorSign>, qs: &[f64]) -> WeightedBar {
        aggregate_bar(
            &row(weights, signs),
            &qs.iter().map(|q| input(*q)).collect::<Vec<_>>(),
            &EntryConfig::default(),
            PositionAlgorithm::V0,
        )
        .unwrap()
    }

    #[test]
    fn symbol_mean_compares_values_before_making_one_decision() {
        let row = row(vec![20.0, 1.0], vec![FactorSign::Positive; 2]);
        let entry = EntryConfig {
            factor_long_quantile: 0.8,
            factor_short_quantile: -0.8,
            ..Default::default()
        };
        let bar = mean_bar(&row, &[input(0.99), input(0.79)], &entry).unwrap();
        assert!((bar.decision_value - 0.78).abs() < 1e-12);
        assert_eq!(bar.value, 0.0); // One factor votes long, but the mean stays below entry.
        assert_eq!(bar.votes[0].vote, 1);
        assert!(bar.votes.iter().all(|v| v.normalized_weight == 0.5));
        let bar = mean_bar(&row, &[input(0.95), input(0.96)], &entry).unwrap();
        assert_eq!(bar.value, 1.0);
        assert!((bar.decision_value - 0.91).abs() < 1e-12);
        assert_eq!(
            mean_bar(&row, &[input(0.9), input(0.9)], &entry)
                .unwrap()
                .value,
            0.0
        );
    }

    #[test]
    fn symbol_mean_keeps_sign_and_requires_every_enabled_factor() {
        let row = row(
            vec![20.0, 1.0, 0.0],
            vec![
                FactorSign::Negative,
                FactorSign::Positive,
                FactorSign::Positive,
            ],
        );
        let entry = EntryConfig::default();
        let mut inputs = [input(0.01), input(0.99), input(f64::NAN)];
        let bar = mean_bar(&row, &inputs, &entry).unwrap();
        assert!((bar.decision_value - 0.98).abs() < 1e-12);
        assert_eq!(bar.votes.len(), 2);
        assert_eq!(bar.value, 1.0);
        inputs[1].score_ready = false;
        assert!(mean_bar(&row, &inputs, &entry).is_none());
        inputs[1].score_ready = true;
        inputs[1].score_ts_ms += 60_000;
        assert!(mean_bar(&row, &inputs, &entry).is_none());
    }

    #[test]
    fn opposing_votes_produce_net_normalized_weight() {
        let bar = bar(vec![2.0, 1.0], vec![FactorSign::Positive; 2], &[0.95, 0.05]);
        assert_eq!(bar.side(), Some(Side::Buy));
        assert!((bar.coefficient - 1.0 / 3.0).abs() < 1e-12);
        assert!((bar.quantile - 0.65).abs() < 1e-12);
        assert_eq!(
            bar.votes.iter().map(|v| v.vote).collect::<Vec<_>>(),
            vec![1, -1]
        );
    }

    #[test]
    fn sign_correction_happens_before_voting_and_exit_percentile() {
        let bar = bar(
            vec![2.0, 1.0],
            vec![FactorSign::Negative, FactorSign::Positive],
            &[0.95, 0.05],
        );
        assert_eq!(bar.side(), Some(Side::Sell));
        assert_eq!(bar.coefficient, 1.0);
        assert!((bar.quantile - 0.05).abs() < 1e-12);
        assert!((bar.votes[0].corrected_quantile - 0.05).abs() < 1e-12);
    }

    #[test]
    fn neutral_votes_remain_in_denominator_and_boundary_is_strict() {
        let observation = bar(
            vec![2.0, 1.0, 1.0],
            vec![FactorSign::Positive; 3],
            &[0.95, 0.9, 0.1],
        );
        assert_eq!(observation.coefficient, 0.5);
        assert_eq!(
            observation.votes.iter().map(|v| v.vote).collect::<Vec<_>>(),
            vec![1, 0, 0]
        );
        let reflected = bar(vec![1.0, 1.0], vec![FactorSign::Negative; 2], &[0.1, 0.9]);
        assert_eq!(reflected.coefficient, 0.0);
    }

    #[test]
    fn offsetting_votes_are_flat_and_weight_scaling_is_invariant() {
        let flat = bar(vec![1.0, 1.0], vec![FactorSign::Positive; 2], &[0.95, 0.05]);
        assert_eq!(flat.side(), None);
        assert_eq!(flat.coefficient, 0.0);
        let huge = bar(
            vec![f64::MAX, f64::MAX / 2.0],
            vec![FactorSign::Positive; 2],
            &[0.95, 0.05],
        );
        assert!((huge.coefficient - 1.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn missing_or_invalid_active_factor_never_renormalizes_remaining_votes() {
        let row = row(vec![2.0, 1.0], vec![FactorSign::Positive; 2]);
        for invalid in 0..6 {
            let mut inputs = vec![input(0.95), input(0.05)];
            match invalid {
                0 => inputs[1].subscribed = false,
                1 => inputs[1].score_ready = false,
                2 => inputs[1].score = Some(f64::NAN),
                3 => inputs[1].score_quantile = None,
                4 => inputs[1].score_quantile = Some(1.01),
                _ => inputs[1].score_quantile = Some(f64::INFINITY),
            }
            assert!(aggregate_bar(
                &row,
                &inputs,
                &EntryConfig::default(),
                PositionAlgorithm::V0
            )
            .is_none());
        }
    }

    #[test]
    fn basket_waits_for_all_factors_from_the_same_closed_bar() {
        let row = row(vec![2.0, 1.0], vec![FactorSign::Positive; 2]);
        let mut inputs = vec![input(0.95), input(0.05)];
        inputs[0].score_ts_ms = 120_000;
        assert!(aggregate_bar(
            &row,
            &inputs,
            &EntryConfig::default(),
            PositionAlgorithm::V0
        )
        .is_none());
        inputs[1].score_ts_ms = 120_000;
        assert_eq!(
            aggregate_bar(
                &row,
                &inputs,
                &EntryConfig::default(),
                PositionAlgorithm::V0
            )
            .unwrap()
            .model_ts_ms,
            120_000
        );
    }

    #[test]
    fn zero_weight_factor_does_not_require_data_or_a_subscription() {
        let row = row(vec![1.0, 0.0], vec![FactorSign::Positive; 2]);
        let mut missing = input(f64::NAN);
        missing.subscribed = false;
        missing.score_ts_ms = 0;
        let bar = aggregate_bar(
            &row,
            &[input(0.95), missing],
            &EntryConfig::default(),
            PositionAlgorithm::V0,
        )
        .unwrap();
        assert_eq!(bar.coefficient, 1.0);
        assert_eq!(bar.votes.len(), 1);
        let table = BTreeMap::from([("ETHUSDT".into(), row)]);
        assert_eq!(table_services(&table).len(), 1);
    }

    #[test]
    fn all_vote_combinations_have_bounded_coefficients_and_percentiles() {
        for q1 in [0.0, 0.05, 0.5, 0.95, 1.0] {
            for q2 in [0.0, 0.05, 0.5, 0.95, 1.0] {
                for sign in [FactorSign::Positive, FactorSign::Negative] {
                    let bar = bar(vec![7.0, 3.0], vec![sign; 2], &[q1, q2]);
                    assert!((0.0..=1.0).contains(&bar.coefficient));
                    assert!((0.0..=1.0).contains(&bar.quantile));
                    assert_eq!(bar.coefficient, bar.value.abs());
                }
            }
        }
    }

    #[test]
    fn factor_table_validates_the_whole_document_and_actual_evaluators() {
        let table = parse_factor_table(r#"{" ethusdt ":{"FACTOR_036":2,"td_ti_002":-1}}"#).unwrap();
        assert_eq!(table["ETHUSDT"].factors, vec!["factor_036", "TD_TI_002"]);
        assert!(parse_factor_table("{}").unwrap().is_empty());
        for invalid in [
            r#"{"ETHUSDT":{"factor_999":1}}"#,
            r#"{"ETHUSDT":{"factor_036":0}}"#,
            r#"{"ETHUSDT":{"factor_036":"-1"}}"#,
            r#"{"ETHUSDT":{}}"#,
            r#"{"ETHUSDT":{"factor_036":true}}"#,
            r#"{"ETHUSDT":{"factor_036":1,"FACTOR_036":1}}"#,
            r#"{"ETH-USDT":{"factor_036":1}}"#,
        ] {
            assert!(parse_factor_table(invalid).is_err(), "accepted {invalid}");
        }
        let duplicate =
            serde_json::json!({"ETHUSDT": table["ETHUSDT"], "ethusdt": table["ETHUSDT"]});
        assert!(parse_factor_table(&duplicate.to_string()).is_err());
        for (raw, symbols, bindings) in [
            (
                include_str!("../../config/cta_perp_symbol_factors.json"),
                9,
                24,
            ),
            (
                include_str!("../../config/cta_perp_rx03/cta_perp_symbol_factors.json"),
                14,
                34,
            ),
        ] {
            let table = parse_factor_table(raw).unwrap();
            assert_eq!(table.len(), symbols);
            assert_eq!(
                table.values().map(|row| row.factors.len()).sum::<usize>(),
                bindings
            );
        }
    }

    #[tokio::test]
    async fn redis_reload_is_atomic_rejects_invalid_data_and_observes_deletion() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;
        use tokio::sync::RwLock;
        let original = r#"{"ETHUSDT":{"factor_036":1}}"#;
        let raw = Arc::new(RwLock::new(Some(original.to_string())));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_raw = raw.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            loop {
                let mut line = String::new();
                if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                    break;
                }
                let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                let mut args = Vec::new();
                for _ in 0..count {
                    line.clear();
                    read.read_line(&mut line).await.unwrap();
                    let len: usize = line.trim().strip_prefix('$').unwrap().parse().unwrap();
                    let mut bytes = vec![0; len + 2];
                    read.read_exact(&mut bytes).await.unwrap();
                    args.push(String::from_utf8(bytes[..len].to_vec()).unwrap());
                }
                let response = if args[0].eq_ignore_ascii_case("GET") {
                    assert_eq!(args[1], "test:cta_symbol_factors");
                    match server_raw.read().await.as_ref() {
                        Some(value) => format!("${}\r\n{value}\r\n", value.len()),
                        None => "$-1\r\n".into(),
                    }
                } else {
                    "+OK\r\n".into()
                };
                if write.write_all(response.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
        let settings = RedisSettings {
            port,
            ..RedisSettings::default()
        };
        let mut watcher =
            FactorConfigWatcher::with_settings("test:cta_symbol_factors".into(), settings)
                .await
                .unwrap();
        let mut observer = watcher.receiver.clone();
        assert_eq!(watcher.current()["ETHUSDT"].weights, vec![1.0]);
        *raw.write().await = Some("invalid JSON".into());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(watcher.take_change().is_none());
        assert_eq!(watcher.current()["ETHUSDT"].weights, vec![1.0]);
        *raw.write().await = Some(original.replace(":1", ":-2"));
        tokio::time::timeout(Duration::from_secs(3), observer.changed())
            .await
            .unwrap()
            .unwrap();
        let updated = watcher.take_change().unwrap();
        assert_eq!(updated["ETHUSDT"].weights, vec![-2.0]);
        *raw.write().await = None;
        tokio::time::timeout(Duration::from_secs(3), observer.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(watcher.take_change().unwrap().is_empty());
        server.abort();
    }
}
