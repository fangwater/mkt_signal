//! Native hourly baseline archives; replay these inputs into the existing engine.
use super::config::{Interval, Market};
use crate::factor_pub::trade_flow_feature_pub::local_baseline::BaselineBar;
use anyhow::{ensure, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::Path,
};

pub const HOUR_MS: i64 = 3_600_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyInput {
    pub bar: BaselineBar,
    /// NQ's hourly BBO mid, or the explicitly permitted historical close.
    pub nq_price: Option<f64>,
    /// True only when size fields were classified with factual month thresholds.
    pub classified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFile {
    pub file: String,
    pub rows: usize,
    pub sha256: String,
    pub first_end_ms: i64,
    pub last_end_ms: i64,
    pub source_book_events: u64,
    pub source_trades: u64,
    pub thresholds: BTreeMap<String, [f64; 2]>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub market: Market,
    pub interval: Interval,
    pub start_ms: i64,
    pub end_ms: i64,
    pub source: String,
    pub symbols: BTreeMap<String, SymbolFile>,
}

pub struct HourlyHistory {
    pub manifest: Manifest,
    pub rows: BTreeMap<String, Vec<HourlyInput>>,
}

#[derive(Debug, Default, Serialize)]
pub struct SeedSummary {
    pub cutoff_ms: i64,
    pub formula_rows: BTreeMap<String, usize>,
    pub ready_formulas: BTreeMap<String, bool>,
    pub nq_rows: BTreeMap<String, usize>,
    pub missing_symbols: Vec<String>,
}

pub fn cutoff_ms(first_period_end_ms: i64) -> Result<i64> {
    ensure!(first_period_end_ms > 0, "invalid first Kafka period end");
    Ok((first_period_end_ms - 1).div_euclid(HOUR_MS) * HOUR_MS)
}

pub fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

pub fn write_symbol(root: &Path, symbol: &str, rows: &[HourlyInput]) -> Result<SymbolFile> {
    ensure!(!rows.is_empty(), "no hourly observations for {symbol}");
    let file = format!("{symbol}.bin.gz");
    let path = root.join(&file);
    let temporary = root.join(format!(".{file}.tmp"));
    let mut encoder = GzEncoder::new(File::create(&temporary)?, Compression::fast());
    bincode::serialize_into(&mut encoder, rows).context("serialize native hourly baseline")?;
    let mut output = encoder.finish()?;
    output.flush()?;
    output.sync_all()?;
    std::fs::rename(temporary, &path)?;
    Ok(SymbolFile {
        file,
        rows: rows.len(),
        sha256: sha256(&path)?,
        first_end_ms: rows[0].bar.start_ms + HOUR_MS,
        last_end_ms: rows.last().unwrap().bar.start_ms + HOUR_MS,
        source_book_events: 0,
        source_trades: 0,
        thresholds: BTreeMap::new(),
    })
}

impl HourlyHistory {
    pub fn load(root: &Path) -> Result<Self> {
        let manifest: Manifest =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)
                .context("read hourly history manifest")?;
        ensure!(
            manifest.market == Market::BinanceFutures && manifest.interval == Interval::Hour,
            "hourly history must be Binance futures 1h"
        );
        ensure!(
            manifest.start_ms < manifest.end_ms
                && manifest.start_ms % HOUR_MS == 0
                && manifest.end_ms % HOUR_MS == 0,
            "invalid hourly history bounds"
        );
        let mut rows = BTreeMap::new();
        for (symbol, meta) in &manifest.symbols {
            ensure!(
                symbol.ends_with("USDT")
                    && symbol.bytes().all(|b| b.is_ascii_alphanumeric())
                    && meta.file == format!("{symbol}.bin.gz"),
                "invalid hourly history symbol path"
            );
            let path = root.join(&meta.file);
            ensure!(
                std::fs::metadata(&path)?.len() <= 64 * 1024 * 1024,
                "hourly history file too large: {symbol}"
            );
            ensure!(
                sha256(&path)? == meta.sha256,
                "hourly history checksum mismatch: {symbol}"
            );
            let decoder = GzDecoder::new(File::open(&path)?);
            let mut limited = decoder.take(128 * 1024 * 1024);
            let inputs: Vec<HourlyInput> = bincode::deserialize_from(&mut limited)
                .with_context(|| format!("decode hourly history {symbol}"))?;
            ensure!(
                limited.read(&mut [0u8; 1])? == 0,
                "trailing data in hourly history {symbol}"
            );
            ensure!(
                inputs.len() == meta.rows && inputs.len() <= 100_000,
                "hourly history row count mismatch: {symbol}"
            );
            ensure!(
                inputs
                    .first()
                    .is_some_and(|r| r.bar.start_ms + HOUR_MS == meta.first_end_ms)
                    && inputs
                        .last()
                        .is_some_and(|r| r.bar.start_ms + HOUR_MS == meta.last_end_ms),
                "hourly history endpoint mismatch: {symbol}"
            );
            let mut previous = None;
            for input in &inputs {
                let ts = input.bar.start_ms;
                ensure!(
                    ts >= manifest.start_ms
                        && ts < manifest.end_ms
                        && ts % HOUR_MS == 0
                        && previous.is_none_or(|old| ts > old),
                    "invalid hourly history time: {symbol}"
                );
                previous = Some(ts);
            }
            rows.insert(symbol.clone(), inputs);
        }
        Ok(Self { manifest, rows })
    }
}
