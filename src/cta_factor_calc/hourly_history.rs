//! Native hourly baseline archives; replay these inputs into the existing engine.
use super::config::{Interval, Market, Plan};
use crate::factor_pub::trade_flow_feature_pub::local_baseline::BaselineBar;
use anyhow::{ensure, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::Path,
};

pub const HOUR_MS: i64 = 3_600_000;

pub fn requires_book(id: &super::config::FactorId) -> bool {
    id.factor == "nq"
        || id.factor == "spread"
        || id.factor.starts_with("depth_")
        || id.factor.starts_with("factor_") && !id.factor.starts_with("factor_trades_")
}

/// Inspect actual retained records without committing offsets or touching a worker.
pub fn retained_kafka_start(
    config: &period_pbs::kafka::KafkaConsumerConfig,
    topics: Vec<String>,
) -> Result<BTreeMap<String, i64>> {
    use period_pbs::kafka::{decode_period_payload, RawKafkaConsumer};
    use std::time::{Duration, Instant};
    let mut config = config.clone();
    config.topics = topics;
    config.enable_auto_commit = false;
    config.group_id = format!("cta-hourly-continuity-{}", uuid::Uuid::new_v4());
    config.client_id = config.group_id.clone();
    let consumer = RawKafkaConsumer::new_unassigned(&config)?;
    let marks = consumer.query_topic_watermarks(
        &config.topics,
        config.metadata_timeout_ms,
        config.watermark_timeout_ms,
    )?;
    ensure!(
        marks.iter().all(|m| m.high > m.low),
        "empty retained Kafka partition"
    );
    let mut remaining = marks
        .iter()
        .map(|m| format!("{}:{}", m.topic, m.partition))
        .collect::<BTreeSet<_>>();
    consumer.assign_offsets(
        &marks
            .iter()
            .map(|m| (m.topic.clone(), m.partition, m.low))
            .collect::<Vec<_>>(),
    )?;
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut starts = BTreeMap::new();
    while !remaining.is_empty() {
        ensure!(
            Instant::now() < deadline,
            "timed out reading first retained Kafka periods"
        );
        let Some(record) = consumer.poll(1000) else {
            continue;
        };
        let record = record?;
        let key = format!("{}:{}", record.topic, record.partition);
        if remaining.remove(&key) {
            let (_, _, period) =
                decode_period_payload(&record.payload, config.payload_compression)?;
            starts.insert(key, period.ts);
        }
    }
    Ok(starts)
}

pub fn validate_kafka_start(
    histories: &BTreeMap<Market, HourlyHistory>,
    starts: &BTreeMap<String, i64>,
) -> Result<()> {
    for (market, history) in histories {
        let prefix = format!("{}:", market.topic());
        ensure!(
            starts.keys().any(|key| key.starts_with(&prefix)),
            "missing retained {} Kafka start",
            market.name()
        );
        for (_, &start) in starts.iter().filter(|(key, _)| key.starts_with(&prefix)) {
            ensure!(cutoff_ms(start)? <= history.manifest.end_ms,
                "{} Tardis history ends at {}, before retained Kafka source hour {}; refresh the archive",
                market.name(), history.manifest.end_ms, cutoff_ms(start)?);
        }
    }
    Ok(())
}

pub struct HourlyPrice {
    pub end_ms: i64,
    pub mid: f64,
}

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
    pub ready_nq: BTreeMap<String, bool>,
    pub ready_spreads: BTreeMap<String, bool>,
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
            manifest.interval == Interval::Hour,
            "native history must contain 1h inputs"
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

/// One current layout: root/<market>/manifest.json and venue-scoped symbol files.
/// Every hourly dependency must have matching native Tardis history, including
/// both markets of a spread. No REST-candle or live-only bootstrap is accepted.
pub fn load_for_plan(root: Option<&Path>, plan: &Plan) -> Result<BTreeMap<Market, HourlyHistory>> {
    let mut required = BTreeMap::<Market, BTreeMap<String, bool>>::new();
    for calculation in plan
        .calculations
        .values()
        .filter(|c| c.id.interval == Interval::Hour)
    {
        for market in calculation.id.markets() {
            let book = required
                .entry(market)
                .or_default()
                .entry(calculation.id.symbol.clone())
                .or_default();
            *book |= requires_book(&calculation.id);
        }
    }
    if required.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root =
        root.context("1h factors require a locally built Tardis hourly history directory")?;
    let mut histories = BTreeMap::new();
    let mut cutoff = None;
    for (market, symbols) in required {
        let history = HourlyHistory::load(&root.join(market.name()))
            .with_context(|| format!("load mandatory {} Tardis hourly history", market.name()))?;
        ensure!(
            history.manifest.market == market,
            "hourly history market mismatch: {}",
            market.name()
        );
        ensure!(
            history.manifest.source.starts_with("Tardis "),
            "hourly history must identify its Tardis raw source"
        );
        ensure!(
            cutoff.is_none_or(|end| end == history.manifest.end_ms),
            "hourly market archives must have one common cutoff"
        );
        cutoff = Some(history.manifest.end_ms);
        for (symbol, book) in symbols {
            let meta = history
                .manifest
                .symbols
                .get(&symbol)
                .with_context(|| format!("missing {} {symbol} hourly inputs", market.name()))?;
            ensure!(
                meta.last_end_ms == history.manifest.end_ms,
                "{} {symbol} hourly inputs end before the archive cutoff",
                market.name()
            );
            ensure!(
                !book || meta.source_book_events > 0,
                "{} {symbol} hourly history requires factual Tardis L2 inputs",
                market.name()
            );
        }
        histories.insert(market, history);
    }
    Ok(histories)
}
