//! Explicit, silent reconstruction from a single factor-publisher replay.
use super::config::{CtaPerpConfig, PositionAlgorithm, SYMBOL_MEAN_STRATEGY};
use super::factors::{aggregate_bar, mean_bar, SymbolFactorTable, SymbolFactors, WeightedBar};
use super::positions::{full_qty, next_position_flag};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use trade_signal::model_output_hub::ModelOutputScoreLookupResult;

pub const REQUEST_PATH: &str = "run/cta_perp_recovery_request.json";
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrategyPosition {
    pub flag: i8,
    pub qty: f64,
    #[serde(default)]
    pub entry_notional: f64,
}
pub type Positions = BTreeMap<String, BTreeMap<String, StrategyPosition>>;

/// The same transition and fixed-entry sizing are used by replay and live signals.
pub fn advance_position(
    position: &mut StrategyPosition,
    bar: &WeightedBar,
    config: &CtaPerpConfig,
    bid: f64,
    ask: f64,
) -> Result<()> {
    let flag = next_position_flag(position.flag, bar, config);
    if flag == position.flag {
        return Ok(());
    }
    if flag == 0 {
        *position = StrategyPosition::default();
    } else {
        ensure!(
            bid.is_finite() && ask.is_finite() && bid > 0.0 && ask >= bid,
            "invalid replay sizing BBO"
        );
        position.qty = full_qty(config.max_pos_u, flag, if flag > 0 { ask } else { bid })?;
        position.entry_notional = config.max_pos_u;
        position.flag = flag;
    }
    Ok(())
}

#[derive(Deserialize)]
struct Request {
    history_path: PathBuf,
    generation: String,
}

pub struct PositionRecovery {
    request: Request,
    offsets: BTreeMap<PathBuf, u64>,
    pub positions: Positions,
    pub bars: BTreeMap<String, BTreeMap<String, WeightedBar>>,
    latest: BTreeMap<String, i64>,
    config: Value,
    table: SymbolFactorTable,
    pub records: u64,
}
impl PositionRecovery {
    pub fn load(
        config: &CtaPerpConfig,
        table: &SymbolFactorTable,
        previous: &Positions,
    ) -> Result<Option<Self>> {
        if !Path::new(REQUEST_PATH).exists() {
            return Ok(None);
        }
        let request: Request = serde_json::from_slice(&fs::read(REQUEST_PATH)?)?;
        ensure!(
            !request.generation.is_empty(),
            "recovery requires a publisher generation"
        );
        let positions = previous
            .iter()
            .map(|(name, rows)| {
                (
                    name.clone(),
                    rows.keys()
                        .map(|symbol| (symbol.clone(), StrategyPosition::default()))
                        .collect(),
                )
            })
            .collect();
        Ok(Some(Self {
            request,
            positions,
            offsets: Default::default(),
            bars: Default::default(),
            latest: Default::default(),
            config: serde_json::to_value(config)?,
            table: table.clone(),
            records: 0,
        }))
    }

    pub fn poll(
        &mut self,
        config: &CtaPerpConfig,
        table: &SymbolFactorTable,
        now_ms: i64,
    ) -> Result<bool> {
        ensure!(serde_json::to_value(config)? == self.config && table == &self.table,
            "configuration changed during position recovery; restart recovery with the final configuration");
        let status: Value =
            serde_json::from_slice(&fs::read(self.request.history_path.join("recovery.json"))?)?;
        ensure!(
            status["generation"].as_str() == Some(&self.request.generation),
            "factor publisher restarted during position recovery"
        );
        let mut paths = fs::read_dir(&self.request.history_path)?
            .map(|r| r.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.retain(|p| p.extension().is_some_and(|x| x == "jsonl"));
        paths.sort();
        let mut count = 0;
        let mut incomplete = false;
        for path in paths {
            let offset = *self.offsets.get(&path).unwrap_or(&0);
            let file = File::open(&path)?;
            ensure!(
                file.metadata()?.len() >= offset,
                "factor history was truncated during recovery"
            );
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(offset))?;
            loop {
                let mut line = String::new();
                let len = reader.read_line(&mut line)?;
                if len == 0 {
                    break;
                }
                if !line.ends_with('\n') {
                    incomplete = true;
                    break;
                }
                let record: Value =
                    serde_json::from_str(&line).context("decode recovery observation")?;
                if record["generation"].as_str() == Some(&self.request.generation) {
                    self.consume(&record, config)?;
                }
                *self.offsets.entry(path.clone()).or_default() += len as u64;
                count += 1;
                if count >= 2000 {
                    return Ok(false);
                }
            }
        }
        if incomplete
            || status["caught_up"] != true
            || now_ms - status["updated_ms"].as_i64().unwrap_or(0) > 5000
        {
            return Ok(false);
        }
        for (symbol, row) in table {
            let head = status["latest"][symbol].as_i64().unwrap_or(0);
            if head <= 0
                || head > now_ms
                || now_ms - head > 120_000
                || self.latest.get(symbol).copied().unwrap_or(0) < head
            {
                return Ok(false);
            }
            let names: Vec<&str> = if config.position_algorithm == PositionAlgorithm::V1 {
                vec![SYMBOL_MEAN_STRATEGY]
            } else {
                row.factors
                    .iter()
                    .zip(&row.weights)
                    .filter(|(_, w)| **w != 0.0)
                    .map(|(f, _)| f.as_str())
                    .collect()
            };
            for name in names {
                if self
                    .bars
                    .get(name)
                    .and_then(|rows| rows.get(symbol))
                    .is_none_or(|bar| bar.model_ts_ms < head)
                {
                    return Ok(false);
                }
            }
        }
        Ok(!table.is_empty())
    }

    fn consume(&mut self, record: &Value, config: &CtaPerpConfig) -> Result<()> {
        let symbol = record["symbol"].as_str().context("replay symbol missing")?;
        let Some(row) = self.table.get(symbol) else {
            return Ok(());
        };
        let ts = record["model_ts_ms"]
            .as_i64()
            .context("replay timestamp missing")?;
        if self.latest.get(symbol).is_some_and(|last| *last >= ts) {
            return Ok(());
        }
        let nq = &record["nq"];
        let effective = config.for_symbol(symbol);
        let inputs: Vec<_> = row
            .factors
            .iter()
            .map(|factor| {
                let input = &record["inputs"][factor];
                ModelOutputScoreLookupResult {
                    service_name: factor.clone(),
                    symbol_key: symbol.to_string(),
                    subscribed: true,
                    score: input["score"].as_f64(),
                    score_quantile: input["quantile"].as_f64(),
                    score_ready: input["ready"] == true,
                    score_long_threshold: None,
                    score_short_threshold: None,
                    filter_long_value: nq["long_value"].as_f64(),
                    filter_short_value: nq["short_value"].as_f64(),
                    filter_long_quantile: nq["long_quantile"].as_f64(),
                    filter_short_quantile: nq["short_quantile"].as_f64(),
                    filter_long_threshold: None,
                    filter_short_threshold: None,
                    filter_ready: nq["ready"] == true,
                    score_ts_ms: ts,
                    note: String::new(),
                }
            })
            .collect();
        let bars: Vec<_> = if config.position_algorithm == PositionAlgorithm::V1 {
            vec![(
                SYMBOL_MEAN_STRATEGY.to_string(),
                mean_bar(row, &inputs, &effective.entry),
            )]
        } else {
            row.factors
                .iter()
                .zip(&row.weights)
                .zip(&inputs)
                .filter(|((_, w), _)| **w != 0.0)
                .map(|((factor, weight), input)| {
                    (
                        factor.clone(),
                        aggregate_bar(
                            &SymbolFactors {
                                factors: vec![factor.clone()],
                                weights: vec![*weight],
                            },
                            std::slice::from_ref(input),
                            &effective.entry,
                            PositionAlgorithm::V0,
                        ),
                    )
                })
                .collect()
        };
        for (name, bar) in bars {
            let Some(bar) = bar else {
                continue;
            };
            let execution_ts = nq["execution_ts_ms"].as_i64().unwrap_or(0);
            ensure!(
                (ts..=ts + 120_000).contains(&execution_ts),
                "missing historical execution BBO symbol={symbol} ts={ts}"
            );
            let bid = nq["execution_bid"].as_f64().context("missing replay bid")?;
            let ask = nq["execution_ask"].as_f64().context("missing replay ask")?;
            let p = self
                .positions
                .entry(name.clone())
                .or_default()
                .entry(symbol.to_string())
                .or_default();
            advance_position(p, &bar, &effective, bid, ask)?;
            self.bars
                .entry(name)
                .or_default()
                .insert(symbol.to_string(), bar);
        }
        self.latest.insert(symbol.to_string(), ts);
        self.records += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn config() -> CtaPerpConfig {
        let mut c = CtaPerpConfig::load("config/cta_perp.json").unwrap();
        c.position_algorithm = PositionAlgorithm::V1;
        c.max_pos_u = 300.0;
        c.entry.nq_change_enabled = false;
        c
    }
    fn record(generation: &str, ts: i64, q: f64, ask: f64) -> Value {
        json!({"generation":generation,"symbol":"ETHUSDT","model_ts_ms":ts,
            "inputs":{"factor_036":{"ready":true,"score":1.0,"quantile":q},
                "factor_051":{"ready":true,"score":1.0,"quantile":q}},
            "nq":{"ready":true,"execution_ts_ms":ts+5,"execution_bid":ask-1.0,"execution_ask":ask}})
    }
    fn recovery(path: &Path, config: &CtaPerpConfig) -> PositionRecovery {
        PositionRecovery {
            request: Request {
                history_path: path.to_path_buf(),
                generation: "new".into(),
            },
            offsets: Default::default(),
            positions: Default::default(),
            bars: Default::default(),
            latest: Default::default(),
            config: serde_json::to_value(config).unwrap(),
            table: serde_json::from_value(json!({"ETHUSDT":{"factor_036":1.0,"factor_051":10.0}}))
                .unwrap(),
            records: 0,
        }
    }
    #[test]
    fn replay_preserves_entry_quantity_then_clears_and_deduplicates() {
        let c = config();
        let mut r = recovery(Path::new("."), &c);
        r.consume(&record("new", 60_000, 0.99, 101.0), &c).unwrap();
        let qty = r.positions[SYMBOL_MEAN_STRATEGY]["ETHUSDT"].qty;
        assert_eq!(qty, 300.0 / 101.0);
        r.consume(&record("new", 120_000, 0.99, 201.0), &c).unwrap();
        assert_eq!(r.positions[SYMBOL_MEAN_STRATEGY]["ETHUSDT"].qty, qty);
        r.consume(&record("new", 180_000, 0.01, 201.0), &c).unwrap();
        assert_eq!(r.positions[SYMBOL_MEAN_STRATEGY]["ETHUSDT"].qty, 0.0);
        r.consume(&record("new", 120_000, 0.99, 201.0), &c).unwrap();
        assert_eq!(r.positions[SYMBOL_MEAN_STRATEGY]["ETHUSDT"].qty, 0.0);
        assert_eq!(r.records, 3);
    }
    #[test]
    fn recovery_barrier_requires_complete_fresh_current_generation_and_unchanged_config() {
        let path = std::env::temp_dir().join(format!("cta-recovery-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        let c = config();
        let mut r = recovery(&path, &c);
        let table = r.table.clone();
        let status = json!({"generation":"new","caught_up":false,"updated_ms":65_000,"latest":{"ETHUSDT":60_000}});
        fs::write(path.join("recovery.json"), status.to_string()).unwrap();
        let old = record("old", 60_000, 0.01, 201.0).to_string() + "\n";
        let current = record("new", 60_000, 0.99, 101.0).to_string();
        fs::write(path.join("20261015.jsonl"), old.clone() + &current).unwrap();
        assert!(!r.poll(&c, &table, 65_000).unwrap());
        assert_eq!(r.records, 0);
        fs::write(path.join("20261015.jsonl"), old + &current + "\n").unwrap();
        assert!(!r.poll(&c, &table, 65_000).unwrap());
        assert_eq!(r.records, 1);
        let mut status = status;
        status["caught_up"] = json!(true);
        fs::write(path.join("recovery.json"), status.to_string()).unwrap();
        assert!(r.poll(&c, &table, 65_000).unwrap());
        assert!(!r.poll(&c, &table, 200_000).unwrap());
        let mut changed = c.clone();
        changed.max_pos_u = 600.0;
        assert!(r.poll(&changed, &table, 65_000).is_err());
        status["generation"] = json!("unexpected");
        fs::write(path.join("recovery.json"), status.to_string()).unwrap();
        assert!(r.poll(&c, &table, 65_000).is_err());
        fs::remove_dir_all(path).unwrap();
    }
}
