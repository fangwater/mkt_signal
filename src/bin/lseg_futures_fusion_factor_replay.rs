//! Replay 1-minute LSEG/CME baseline factors from the native LSEG parquet layer.
//!
//! Trade bars come from `baseline_data_1m_drop_special`; ten-level books come
//! from `level2_1s`. Both inputs remain fixed-expiry contract series. No
//! dominant-contract stitching, cross-session fill, or synthetic 11th-20th
//! book levels are introduced here.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use futures::executor::block_on;
use log::info;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use mkt_signal::factor_pub::lseg_features::{
    LsegDepth10, LsegFactorPlan, LsegFeatureState, LsegFusionInput, LsegTradeBar,
};
use polars::prelude::{
    DataFrame, Float64Chunked, NamedFrom, ParquetReader, ParquetWriter, SerReader, Series,
    StringChunked,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

const FEATURE_SET: &str = "lseg_features";
const LSEG_DEPTH_LEVELS: usize = 10;
const MINUTE_SECONDS: i64 = 60;
const CLOSED_BAR_LAST_SECOND: i64 = MINUTE_SECONDS - 1;
const DEPTH_PARQUET_BATCH_ROWS: usize = 65_536;

#[derive(Parser, Debug)]
#[command(name = "lseg_futures_fusion_factor_replay")]
#[command(about = "Compute 1-minute LSEG/CME fusion factors from native 10-level parquet")]
struct Args {
    #[arg(long, default_value = "config/lseg_futures_fusion_factor_replay.toml")]
    config: PathBuf,

    /// Parse and validate configuration without reading parquet inputs.
    #[arg(long)]
    validate_config_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    feature_set: String,
    trade_root: PathBuf,
    depth_root: PathBuf,
    output_root: PathBuf,
    start_date: String,
    end_date: String,
    #[serde(default)]
    products: Vec<String>,
    #[serde(default)]
    contracts: Vec<String>,
    factors: Vec<String>,
    #[serde(default = "default_replay_workers")]
    replay_workers: usize,
    #[serde(default)]
    overwrite: bool,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug)]
struct ValidatedConfig {
    start_date: NaiveDate,
    end_date: NaiveDate,
    products: HashSet<String>,
    contracts: HashSet<String>,
    plan: LsegFactorPlan,
    factor_names: Vec<String>,
    replay_workers: usize,
}

#[derive(Debug)]
struct DayJob {
    relative_path: PathBuf,
    trade_path: PathBuf,
    depth_path: PathBuf,
}

#[derive(Debug)]
struct TradeRow {
    contract_id: String,
    ric: String,
    ts: i64,
    values: [f64; TRADE_FLOW_FEATURE_DIM],
}

#[derive(Debug, Clone)]
struct DepthBook {
    bid_prices: [f64; LSEG_DEPTH_LEVELS],
    bid_amounts: [f64; LSEG_DEPTH_LEVELS],
    ask_prices: [f64; LSEG_DEPTH_LEVELS],
    ask_amounts: [f64; LSEG_DEPTH_LEVELS],
}

#[derive(Debug)]
struct DepthRow {
    ts: i64,
    book: DepthBook,
}

#[derive(Default)]
struct ContractReplayState {
    features: LsegFeatureState,
    last_source_ts: Option<i64>,
}

#[derive(Default)]
struct ReplayStats {
    source_rows: u64,
    written_rows: u64,
    skipped_missing_depth: u64,
    skipped_invalid_bbo: u64,
    segment_resets: u64,
}

impl ReplayStats {
    fn add(&mut self, other: Self) {
        self.source_rows = self.source_rows.saturating_add(other.source_rows);
        self.written_rows = self.written_rows.saturating_add(other.written_rows);
        self.skipped_missing_depth = self
            .skipped_missing_depth
            .saturating_add(other.skipped_missing_depth);
        self.skipped_invalid_bbo = self
            .skipped_invalid_bbo
            .saturating_add(other.skipped_invalid_bbo);
        self.segment_resets = self.segment_resets.saturating_add(other.segment_resets);
    }
}

#[derive(Debug)]
struct OutputRow {
    contract_id: String,
    ric: String,
    ts: i64,
    depth_ts: i64,
    factors: Vec<Option<f64>>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let config: Config = toml::from_str(&content)
        .with_context(|| format!("parse config {}", args.config.display()))?;
    let validated = validate_config(&config)?;
    if args.validate_config_only {
        println!(
            "validated lseg futures config: feature_set={} dates={}..{} products={} contracts={} factors={} workers={} dry_run={}",
            config.feature_set,
            validated.start_date,
            validated.end_date,
            validated.products.len(),
            validated.contracts.len(),
            validated.factor_names.len(),
            validated.replay_workers,
            config.dry_run,
        );
        return Ok(());
    }
    replay(&config, validated)
}

fn validate_config(config: &Config) -> Result<ValidatedConfig> {
    if config.feature_set.trim() != FEATURE_SET {
        bail!(
            "feature_set must be {FEATURE_SET}, got {}",
            config.feature_set
        );
    }
    if !config.trade_root.is_dir() {
        bail!(
            "trade_root is not a directory: {}",
            config.trade_root.display()
        );
    }
    if !config.depth_root.is_dir() {
        bail!(
            "depth_root is not a directory: {}",
            config.depth_root.display()
        );
    }
    let start_date = parse_date(&config.start_date, "start_date")?;
    let end_date = parse_date(&config.end_date, "end_date")?;
    if start_date > end_date {
        bail!("start_date must not be after end_date");
    }
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }

    let products = normalize_products(&config.products)?;
    let contracts = normalize_contracts(&config.contracts)?;
    let plan = LsegFactorPlan::from_factor_names(config.factors.clone())?;
    let factor_names = plan.factor_names().map(ToOwned::to_owned).collect();
    Ok(ValidatedConfig {
        start_date,
        end_date,
        products,
        contracts,
        plan,
        factor_names,
        replay_workers: config.replay_workers,
    })
}

fn default_replay_workers() -> usize {
    8
}

fn parse_date(value: &str, field: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("{field} must use YYYY-MM-DD, got {value:?}"))
}

fn normalize_products(raw: &[String]) -> Result<HashSet<String>> {
    let mut products = HashSet::with_capacity(raw.len());
    for value in raw {
        let normalized = value.trim().to_ascii_uppercase();
        let mut parts = normalized.split('/');
        let exchange = parts.next().unwrap_or_default();
        let product = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || !valid_component(exchange)
            || !valid_component(product)
            || exchange.is_empty()
            || product.is_empty()
        {
            bail!(
                "product must have the form EXCHANGE/PRODUCT using ASCII letters, digits, or underscores: {value:?}"
            );
        }
        products.insert(format!("{exchange}/{product}"));
    }
    Ok(products)
}

fn valid_component(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn normalize_contracts(raw: &[String]) -> Result<HashSet<String>> {
    let mut contracts = HashSet::with_capacity(raw.len());
    for value in raw {
        let contract = value.trim().to_ascii_uppercase();
        if contract.is_empty()
            || !contract
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b':' || byte == b'-')
        {
            bail!("invalid LSEG contract_id: {value:?}");
        }
        contracts.insert(contract);
    }
    Ok(contracts)
}

fn replay(config: &Config, validated: ValidatedConfig) -> Result<()> {
    let jobs = discover_jobs(config, &validated)?;
    if jobs.is_empty() {
        bail!(
            "no LSEG trade parquet files in {} for {}..{}",
            config.trade_root.display(),
            validated.start_date,
            validated.end_date
        );
    }
    if !config.dry_run {
        for job in &jobs {
            let output = config.output_root.join(&job.relative_path);
            if output.exists() && !config.overwrite {
                bail!(
                    "output exists and overwrite=false: {}; no files were written",
                    output.display()
                );
            }
        }
    }

    info!(
        "Starting LSEG futures fusion replay: jobs={} dates={}..{} factors={} products={} contracts={} workers={} trade_root={} depth_root={} output_root={} dry_run={}",
        jobs.len(),
        validated.start_date,
        validated.end_date,
        validated.factor_names.len(),
        validated.products.len(),
        validated.contracts.len(),
        validated.replay_workers,
        config.trade_root.display(),
        config.depth_root.display(),
        config.output_root.display(),
        config.dry_run,
    );

    let mut state_lanes: Vec<HashMap<String, ContractReplayState>> = (0..validated.replay_workers)
        .map(|_| HashMap::new())
        .collect();
    let mut stats = ReplayStats::default();
    for (index, job) in jobs.iter().enumerate() {
        let output = process_day(job, &validated, &mut state_lanes, &mut stats)?;
        if !config.dry_run {
            write_output(
                &config.output_root.join(&job.relative_path),
                &output,
                &validated.factor_names,
            )?;
        }
        info!(
            "LSEG futures fusion progress: job={}/{} source={} output={} path={}",
            index + 1,
            jobs.len(),
            stats.source_rows,
            stats.written_rows,
            job.relative_path.display(),
        );
    }
    info!(
        "LSEG features replay complete: source_rows={} output_rows={} skipped_missing_depth={} skipped_invalid_bbo={} segment_resets={} contracts={}",
        stats.source_rows,
        stats.written_rows,
        stats.skipped_missing_depth,
        stats.skipped_invalid_bbo,
        stats.segment_resets,
        state_lanes.iter().map(HashMap::len).sum::<usize>(),
    );
    Ok(())
}

fn discover_jobs(config: &Config, validated: &ValidatedConfig) -> Result<Vec<DayJob>> {
    let mut product_dirs = Vec::new();
    for exchange in sorted_dirs(&config.trade_root)? {
        let exchange_name = file_name(&exchange)?;
        for product in sorted_dirs(&exchange)? {
            let product_name = file_name(&product)?;
            let key = format!("{exchange_name}/{product_name}");
            if !validated.products.is_empty() && !validated.products.contains(&key) {
                continue;
            }
            product_dirs.push((key, product));
        }
    }

    let mut jobs = Vec::new();
    for (_, product_dir) in product_dirs {
        for path in sorted_parquet_files(&product_dir)? {
            let date = parquet_trade_date(&path)?;
            if date < validated.start_date || date > validated.end_date {
                continue;
            }
            let relative_path = path
                .strip_prefix(&config.trade_root)
                .with_context(|| {
                    format!(
                        "trade file {} is outside trade_root {}",
                        path.display(),
                        config.trade_root.display()
                    )
                })?
                .to_path_buf();
            let depth_path = config.depth_root.join(&relative_path);
            if !depth_path.is_file() {
                bail!(
                    "missing paired 10-level depth file for {}: {}",
                    path.display(),
                    depth_path.display()
                );
            }
            jobs.push(DayJob {
                relative_path,
                trade_path: path,
                depth_path,
            });
        }
    }
    jobs.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(jobs)
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read directory {}", root.display()))? {
        let entry = entry.with_context(|| format!("read entry below {}", root.display()))?;
        let path = entry.path();
        if path.is_dir() {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn sorted_parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read directory {}", root.display()))? {
        let entry = entry.with_context(|| format!("read entry below {}", root.display()))?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("parquet") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .with_context(|| format!("path has no UTF-8 filename: {}", path.display()))
}

fn parquet_trade_date(path: &Path) -> Result<NaiveDate> {
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .with_context(|| format!("parquet path has no UTF-8 filename: {}", path.display()))?;
    NaiveDate::parse_from_str(stem, "%Y%m%d").with_context(|| {
        format!(
            "parquet filename must use YYYYMMDD.parquet: {}",
            path.display()
        )
    })
}

fn process_day(
    job: &DayJob,
    validated: &ValidatedConfig,
    state_lanes: &mut [HashMap<String, ContractReplayState>],
    stats: &mut ReplayStats,
) -> Result<Vec<OutputRow>> {
    let mut trades = read_trade_rows(&job.trade_path, &validated.contracts)?;
    if trades.is_empty() {
        return Ok(Vec::new());
    }
    let depths = read_depth_rows(&job.depth_path, &validated.contracts)?;
    trades.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.contract_id.cmp(&right.contract_id))
            .then_with(|| left.ric.cmp(&right.ric))
    });

    let mut lane_rows: Vec<Vec<TradeRow>> = (0..state_lanes.len()).map(|_| Vec::new()).collect();
    for trade in trades {
        let lane = contract_lane(&trade.contract_id, state_lanes.len());
        lane_rows[lane].push(trade);
    }
    let results: Vec<(Vec<OutputRow>, ReplayStats)> = state_lanes
        .par_iter_mut()
        .zip(lane_rows.into_par_iter())
        .map(|(states, trades)| {
            process_contract_lane(trades, &depths, validated, states, &job.trade_path)
        })
        .collect::<Result<_>>()?;

    let mut output = Vec::new();
    for (rows, lane_stats) in results {
        stats.add(lane_stats);
        output.extend(rows);
    }
    output.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| left.contract_id.cmp(&right.contract_id))
            .then_with(|| left.ric.cmp(&right.ric))
    });
    Ok(output)
}

fn process_contract_lane(
    trades: Vec<TradeRow>,
    depths: &HashMap<String, Vec<DepthRow>>,
    validated: &ValidatedConfig,
    states: &mut HashMap<String, ContractReplayState>,
    source_path: &Path,
) -> Result<(Vec<OutputRow>, ReplayStats)> {
    let mut stats = ReplayStats::default();
    let mut seen_rows = HashSet::with_capacity(trades.len());
    let mut output = Vec::with_capacity(trades.len());
    for trade in trades {
        stats.source_rows = stats.source_rows.saturating_add(1);
        let key = (trade.contract_id.clone(), trade.ts);
        if !seen_rows.insert(key) {
            bail!(
                "duplicate LSEG trade bar: contract_id={} ts={} path={}",
                trade.contract_id,
                trade.ts,
                source_path.display()
            );
        }
        let depth_ts = close_book_timestamp(trade.ts)?;
        let Some(book) = depth_at(&depths, &trade.contract_id, depth_ts) else {
            stats.skipped_missing_depth = stats.skipped_missing_depth.saturating_add(1);
            continue;
        };
        if !valid_best_book(book) {
            stats.skipped_invalid_bbo = stats.skipped_invalid_bbo.saturating_add(1);
            continue;
        }

        let state = states.entry(trade.contract_id.clone()).or_default();
        let segment_break = state
            .last_source_ts
            .is_some_and(|previous| trade.ts != previous + MINUTE_SECONDS);
        if segment_break {
            stats.segment_resets = stats.segment_resets.saturating_add(1);
        }
        let depth = LsegDepth10::from_slices(
            &book.bid_prices,
            &book.bid_amounts,
            &book.ask_prices,
            &book.ask_amounts,
        )
        .context("build native ten-level LSEG depth")?;
        let ts_ms = trade
            .ts
            .checked_mul(1_000)
            .with_context(|| format!("trade timestamp overflows milliseconds: {}", trade.ts))?;
        let input = LsegFusionInput {
            ts_ms,
            symbol: trade.contract_id.clone(),
            trade: LsegTradeBar::from_slice(&trade.values).context("build LSEG trade-flow bar")?,
            depth,
            segment_break,
        };
        state.features.push(input).with_context(|| {
            format!(
                "push LSEG features row contract_id={} ts={}",
                trade.contract_id, trade.ts
            )
        })?;
        state.last_source_ts = Some(trade.ts);
        let factors = state
            .features
            .factor_values(&validated.plan)
            .context("compute LSEG feature values")?;
        output.push(OutputRow {
            contract_id: trade.contract_id,
            ric: trade.ric,
            ts: trade.ts,
            depth_ts,
            factors,
        });
        stats.written_rows = stats.written_rows.saturating_add(1);
    }
    Ok((output, stats))
}

fn contract_lane(contract_id: &str, lanes: usize) -> usize {
    debug_assert!(lanes > 0);
    let hash = contract_id
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    (hash as usize) % lanes
}

fn close_book_timestamp(bar_ts: i64) -> Result<i64> {
    if bar_ts.rem_euclid(MINUTE_SECONDS) != 0 {
        bail!("LSEG trade bar timestamp is not a minute boundary: {bar_ts}");
    }
    bar_ts
        .checked_add(CLOSED_BAR_LAST_SECOND)
        .context("LSEG close-of-bar depth timestamp overflow")
}

fn valid_best_book(book: &DepthBook) -> bool {
    book.bid_prices[0].is_finite()
        && book.bid_amounts[0].is_finite()
        && book.bid_amounts[0] >= 0.0
        && book.ask_prices[0].is_finite()
        && book.ask_amounts[0].is_finite()
        && book.ask_amounts[0] >= 0.0
}

fn depth_at<'a>(
    depths: &'a HashMap<String, Vec<DepthRow>>,
    contract_id: &str,
    ts: i64,
) -> Option<&'a DepthBook> {
    let rows = depths.get(contract_id)?;
    rows.binary_search_by_key(&ts, |row| row.ts)
        .ok()
        .map(|index| &rows[index].book)
}

fn read_trade_rows(path: &Path, contracts: &HashSet<String>) -> Result<Vec<TradeRow>> {
    let mut columns = vec![
        "contract_id".to_string(),
        "ric".to_string(),
        "ts".to_string(),
    ];
    columns.extend(
        TRADE_FLOW_FEATURE_FIELD_NAMES
            .iter()
            .map(|name| (*name).to_string()),
    );
    let dataframe = read_parquet(path, columns)?;
    let contract_id = string_column(&dataframe, "contract_id")?;
    let ric = string_column(&dataframe, "ric")?;
    let ts = i64_column(&dataframe, "ts")?;
    let value_columns: Vec<&Float64Chunked> = TRADE_FLOW_FEATURE_FIELD_NAMES
        .iter()
        .map(|field| f64_column(&dataframe, field))
        .collect::<Result<_>>()?;
    let mut rows = Vec::with_capacity(dataframe.height());
    for index in 0..dataframe.height() {
        let contract = required_str(contract_id, index, "contract_id", path)?;
        if !contracts.is_empty() && !contracts.contains(contract) {
            continue;
        }
        let values =
            std::array::from_fn(|field| value_columns[field].get(index).unwrap_or(f64::NAN));
        rows.push(TradeRow {
            contract_id: contract.to_string(),
            ric: required_str(ric, index, "ric", path)?.to_string(),
            ts: required_i64(ts, index, "ts", path)?,
            values,
        });
    }
    Ok(rows)
}

fn read_depth_rows(
    path: &Path,
    contracts: &HashSet<String>,
) -> Result<HashMap<String, Vec<DepthRow>>> {
    let mut columns = vec!["contract_id".to_string(), "ts".to_string()];
    for side in ["bid", "ask"] {
        for level in 0..LSEG_DEPTH_LEVELS {
            columns.push(format!("{side}{level}p"));
            columns.push(format!("{side}{level}v"));
        }
    }
    let file = File::open(path).with_context(|| format!("open parquet {}", path.display()))?;
    let mut reader = ParquetReader::new(file)
        .with_columns(Some(columns))
        .set_low_memory(true)
        .batched(DEPTH_PARQUET_BATCH_ROWS)
        .with_context(|| format!("open batched parquet reader {}", path.display()))?;

    let mut by_contract: HashMap<String, Vec<DepthRow>> = HashMap::new();
    while let Some(batches) = block_on(reader.next_batches(1))
        .with_context(|| format!("read depth parquet batch {}", path.display()))?
    {
        for dataframe in batches {
            append_depth_batch(&dataframe, path, contracts, &mut by_contract)?;
        }
    }
    finalize_depth_rows(path, &mut by_contract)?;
    Ok(by_contract)
}

fn append_depth_batch(
    dataframe: &DataFrame,
    path: &Path,
    contracts: &HashSet<String>,
    by_contract: &mut HashMap<String, Vec<DepthRow>>,
) -> Result<()> {
    let contract_id = string_column(&dataframe, "contract_id")?;
    let ts = i64_column(&dataframe, "ts")?;
    let bid_prices: Vec<&Float64Chunked> = (0..LSEG_DEPTH_LEVELS)
        .map(|level| f64_column(&dataframe, &format!("bid{level}p")))
        .collect::<Result<_>>()?;
    let bid_amounts: Vec<&Float64Chunked> = (0..LSEG_DEPTH_LEVELS)
        .map(|level| f64_column(&dataframe, &format!("bid{level}v")))
        .collect::<Result<_>>()?;
    let ask_prices: Vec<&Float64Chunked> = (0..LSEG_DEPTH_LEVELS)
        .map(|level| f64_column(&dataframe, &format!("ask{level}p")))
        .collect::<Result<_>>()?;
    let ask_amounts: Vec<&Float64Chunked> = (0..LSEG_DEPTH_LEVELS)
        .map(|level| f64_column(&dataframe, &format!("ask{level}v")))
        .collect::<Result<_>>()?;

    for index in 0..dataframe.height() {
        let row_ts = required_i64(ts, index, "ts", path)?;
        if row_ts.rem_euclid(MINUTE_SECONDS) != CLOSED_BAR_LAST_SECOND {
            continue;
        }
        let contract = required_str(contract_id, index, "contract_id", path)?;
        if !contracts.is_empty() && !contracts.contains(contract) {
            continue;
        }
        let bid_prices =
            std::array::from_fn(|level| bid_prices[level].get(index).unwrap_or(f64::NAN));
        let bid_amounts =
            std::array::from_fn(|level| bid_amounts[level].get(index).unwrap_or(f64::NAN));
        let ask_prices =
            std::array::from_fn(|level| ask_prices[level].get(index).unwrap_or(f64::NAN));
        let ask_amounts =
            std::array::from_fn(|level| ask_amounts[level].get(index).unwrap_or(f64::NAN));
        by_contract
            .entry(contract.to_string())
            .or_default()
            .push(DepthRow {
                ts: row_ts,
                book: DepthBook {
                    bid_prices,
                    bid_amounts,
                    ask_prices,
                    ask_amounts,
                },
            });
    }
    Ok(())
}

fn finalize_depth_rows(
    path: &Path,
    by_contract: &mut HashMap<String, Vec<DepthRow>>,
) -> Result<()> {
    for (contract, rows) in by_contract {
        rows.sort_by_key(|row| row.ts);
        if rows.windows(2).any(|pair| pair[0].ts == pair[1].ts) {
            bail!(
                "duplicate LSEG depth second: contract_id={} path={}",
                contract,
                path.display()
            );
        }
    }
    Ok(())
}

fn read_parquet(path: &Path, columns: Vec<String>) -> Result<DataFrame> {
    let file = File::open(path).with_context(|| format!("open parquet {}", path.display()))?;
    ParquetReader::new(file)
        .with_columns(Some(columns))
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read parquet {}", path.display()))
}

fn string_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .str()
        .with_context(|| format!("parquet column {name} must be Utf8"))
}

fn i64_column<'a>(
    dataframe: &'a DataFrame,
    name: &str,
) -> Result<&'a polars::prelude::Int64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .i64()
        .with_context(|| format!("parquet column {name} must be Int64"))
}

fn f64_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing parquet column {name}"))?
        .f64()
        .with_context(|| format!("parquet column {name} must be Float64"))
}

fn required_str<'a>(
    column: &'a StringChunked,
    index: usize,
    name: &str,
    path: &Path,
) -> Result<&'a str> {
    column
        .get(index)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("null or empty {name} at row {index} in {}", path.display()))
}

fn required_i64(
    column: &polars::prelude::Int64Chunked,
    index: usize,
    name: &str,
    path: &Path,
) -> Result<i64> {
    column
        .get(index)
        .with_context(|| format!("null {name} at row {index} in {}", path.display()))
}

fn write_output(path: &Path, rows: &[OutputRow], factor_names: &[String]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let mut contract_id = Vec::with_capacity(rows.len());
    let mut ric = Vec::with_capacity(rows.len());
    let mut ts = Vec::with_capacity(rows.len());
    let mut depth_ts = Vec::with_capacity(rows.len());
    let mut factor_columns: BTreeMap<&str, Vec<Option<f64>>> = factor_names
        .iter()
        .map(|name| (name.as_str(), Vec::with_capacity(rows.len())))
        .collect();
    for row in rows {
        if row.factors.len() != factor_names.len() {
            bail!(
                "factor output width mismatch: got={} expected={}",
                row.factors.len(),
                factor_names.len()
            );
        }
        contract_id.push(row.contract_id.clone());
        ric.push(row.ric.clone());
        ts.push(row.ts);
        depth_ts.push(row.depth_ts);
        for (name, value) in factor_names.iter().zip(&row.factors) {
            factor_columns
                .get_mut(name.as_str())
                .expect("factor column initialized from factor names")
                .push(*value);
        }
    }
    let mut series = vec![
        Series::new("contract_id".into(), contract_id),
        Series::new("ric".into(), ric),
        Series::new("ts".into(), ts),
        Series::new("depth_ts".into(), depth_ts),
    ];
    for name in factor_names {
        series.push(Series::new(
            name.as_str().into(),
            factor_columns
                .remove(name.as_str())
                .expect("factor column exists"),
        ));
    }
    let mut dataframe = DataFrame::new(series).context("build LSEG fusion output dataframe")?;
    let temporary = path.with_extension("parquet.tmp");
    let file = File::create(&temporary)
        .with_context(|| format!("create output parquet {}", temporary.display()))?;
    ParquetWriter::new(file)
        .finish(&mut dataframe)
        .with_context(|| format!("write output parquet {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_book_uses_the_final_second_of_the_closed_minute() {
        assert_eq!(close_book_timestamp(1_704_754_800).unwrap(), 1_704_754_859);
        assert!(close_book_timestamp(1_704_754_801).is_err());
    }

    #[test]
    fn depth_batch_retains_only_minute_close_snapshots() {
        let mut columns = vec![
            Series::new("contract_id".into(), vec!["CME:ES:2024-03"; 3]),
            Series::new(
                "ts".into(),
                vec![1_704_754_858_i64, 1_704_754_859, 1_704_754_919],
            ),
        ];
        for side in ["bid", "ask"] {
            for level in 0..LSEG_DEPTH_LEVELS {
                let price = if side == "bid" {
                    100.0 - level as f64 * 0.1
                } else {
                    100.1 + level as f64 * 0.1
                };
                columns.push(Series::new(
                    format!("{side}{level}p").into(),
                    vec![price; 3],
                ));
                columns.push(Series::new(
                    format!("{side}{level}v").into(),
                    vec![10.0 + level as f64; 3],
                ));
            }
        }
        let dataframe = DataFrame::new(columns).unwrap();
        let mut rows = HashMap::new();
        append_depth_batch(
            &dataframe,
            Path::new("depth-test.parquet"),
            &HashSet::new(),
            &mut rows,
        )
        .unwrap();
        let selected = rows.get("CME:ES:2024-03").unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].ts, 1_704_754_859);
        assert_eq!(selected[1].ts, 1_704_754_919);
    }
}
