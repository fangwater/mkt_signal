//! Replay fusion baseline factors from ClickHouse baseline bars into ClickHouse.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use log::info;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use mkt_signal::factor_pub::fusion_factor_pub::app::{BaselineReplayState, MAX_SYMBOL_HISTORY};
use order_common::TradingVenue;
use rayon::prelude::*;
use serde::Deserialize;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEPTH_VALUE_COUNT: usize = 80;
const INPUT_VALUE_COUNT: usize = TRADE_FLOW_FEATURE_DIM + DEPTH_VALUE_COUNT;
const PROGRESS_ROWS: u64 = 100_000;

#[derive(Parser, Debug)]
#[command(name = "db_fusion_factor_replay")]
#[command(about = "Compute configured baseline factors from ClickHouse baseline bars")]
struct Args {
    #[arg(long, default_value = "config/db_fusion_factor_replay.toml")]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    venue: String,
    symbols: Vec<String>,
    start_date: String,
    end_date: String,
    factors: Vec<String>,
    #[serde(default = "default_workers")]
    replay_workers: usize,
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Deserialize)]
struct ClickHouseConfig {
    url: String,
    input_database: String,
    input_trade_table: String,
    input_depth_table: String,
    output_database: String,
    output_table: String,
    #[serde(default = "default_batch_rows")]
    batch_rows: usize,
}

fn default_workers() -> usize {
    1
}

fn default_batch_rows() -> usize {
    10_000
}

struct InputRow {
    ts_ms: i64,
    symbol: String,
    values: Vec<f64>,
}

struct OutputRow {
    ts_ms: i64,
    symbol: String,
    replay_version: u64,
    values: Vec<f64>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read config {}", args.config.display()))?;
    let config: Config = toml::from_str(&content)
        .with_context(|| format!("parse config {}", args.config.display()))?;
    replay(&config)
}

fn replay(config: &Config) -> Result<()> {
    let venue = TradingVenue::from_str(&config.venue, true)
        .map_err(|err| anyhow!("unsupported replay venue '{}': {err}", config.venue))?;
    let symbols = normalize_symbols(&config.symbols)?;
    let factors = normalize_factors(&config.factors)?;
    let (start_ms, end_ms) = date_bounds(&config.start_date, &config.end_date)?;
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    if config.clickhouse.batch_rows == 0 {
        bail!("clickhouse.batch_rows must be > 0");
    }
    validate_identifier(&config.clickhouse.input_database)?;
    validate_identifier(&config.clickhouse.input_trade_table)?;
    validate_identifier(&config.clickhouse.input_depth_table)?;
    validate_identifier(&config.clickhouse.output_database)?;
    validate_identifier(&config.clickhouse.output_table)?;

    ensure_output_table(&config.clickhouse, &factors)?;
    let replay_version = Utc::now().timestamp_millis().try_into().map_err(|_| {
        anyhow!("system clock is before the Unix epoch; cannot create replay version")
    })?;
    let workers = config.replay_workers.min(symbols.len());
    let started_at = Instant::now();
    info!(
        "Starting database fusion factor replay: venue={} symbols={} factors={} workers={} dates={}..{} input={}.{}+{} output={}.{} replay_version={}",
        venue.data_pub_slug(),
        symbols.len(),
        factors.join(","),
        workers,
        config.start_date,
        config.end_date,
        config.clickhouse.input_database,
        config.clickhouse.input_trade_table,
        config.clickhouse.input_depth_table,
        config.clickhouse.output_database,
        config.clickhouse.output_table,
        replay_version,
    );
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .context("build database fusion replay Rayon pool")?
        .install(|| {
            symbols.par_iter().try_for_each(|symbol| {
                replay_symbol(
                    config,
                    venue,
                    symbol,
                    start_ms,
                    end_ms,
                    &factors,
                    replay_version,
                )
            })
        })?;
    info!(
        "Database fusion factor replay complete: symbols={} factors={} elapsed={:.2?}",
        symbols.len(),
        factors.len(),
        started_at.elapsed(),
    );
    Ok(())
}

fn normalize_symbols(raw: &[String]) -> Result<Vec<String>> {
    let mut symbols = Vec::with_capacity(raw.len());
    for raw_symbol in raw {
        let symbol = raw_symbol.trim().to_ascii_uppercase();
        if symbol.is_empty()
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("invalid symbol: {raw_symbol}");
        }
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    if symbols.is_empty() {
        bail!("at least one symbol is required");
    }
    Ok(symbols)
}

fn normalize_factors(raw: &[String]) -> Result<Vec<String>> {
    let mut factors = Vec::with_capacity(raw.len());
    for raw_name in raw {
        let name = raw_name.trim().to_ascii_lowercase();
        validate_identifier(&name)?;
        if !factor_engine::baseline::is_supported_baseline(&name) {
            bail!("unsupported baseline factor: {raw_name}");
        }
        if !factors.contains(&name) {
            factors.push(name);
        }
    }
    if factors.is_empty() {
        bail!("at least one factor is required");
    }
    Ok(factors)
}

fn date_bounds(start: &str, end: &str) -> Result<(i64, i64)> {
    let start = NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .with_context(|| format!("parse start_date {start}"))?;
    let end = NaiveDate::parse_from_str(end, "%Y-%m-%d")
        .with_context(|| format!("parse end_date {end}"))?;
    if start > end {
        bail!("start_date must not be after end_date");
    }
    let start_dt = start
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("build start timestamp"))?;
    let end_dt = end
        .succ_opt()
        .ok_or_else(|| anyhow!("end_date overflow"))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("build end timestamp"))?;
    Ok((
        DateTime::<Utc>::from_naive_utc_and_offset(start_dt, Utc).timestamp_millis(),
        DateTime::<Utc>::from_naive_utc_and_offset(end_dt, Utc).timestamp_millis(),
    ))
}

fn replay_symbol(
    config: &Config,
    venue: TradingVenue,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    factors: &[String],
    replay_version: u64,
) -> Result<()> {
    let started_at = Instant::now();
    let client = clickhouse_client()?;
    let warmup_query = prior_rows_query(&config.clickhouse, symbol, start_ms);
    let mut warmup_response = client
        .post(config.clickhouse.url.trim_end_matches('/'))
        .query(&[("query", warmup_query)])
        .send()
        .with_context(|| format!("read warm-up baseline rows for {symbol}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse warm-up query failed for {symbol}"))?;
    let mut warmup_rows = Vec::with_capacity(MAX_SYMBOL_HISTORY);
    while let Some(row) = read_input_row(&mut warmup_response)? {
        warmup_rows.push(row);
    }

    let query = input_query(&config.clickhouse, symbol, start_ms, end_ms);
    let mut response = client
        .post(config.clickhouse.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .send()
        .with_context(|| format!("read baseline rows for {symbol}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse baseline query failed for {symbol}"))?;

    let mut state = BaselineReplayState::default();
    for row in warmup_rows.into_iter().rev() {
        push_input_row(&mut state, venue, symbol, &row)?;
    }
    let mut batch = Vec::with_capacity(config.clickhouse.batch_rows);
    let mut read_rows = 0u64;
    let mut written_rows = 0u64;
    while let Some(row) = read_input_row(&mut response)? {
        push_input_row(&mut state, venue, symbol, &row)?;
        read_rows = read_rows.saturating_add(1);
        batch.push(OutputRow {
            ts_ms: row.ts_ms,
            symbol: symbol.to_string(),
            replay_version,
            values: state.baseline_values(factors),
        });
        if batch.len() >= config.clickhouse.batch_rows {
            written_rows = written_rows.saturating_add(flush_output_batch(
                &client,
                &config.clickhouse,
                factors,
                &mut batch,
            )?);
        }
        if read_rows % PROGRESS_ROWS == 0 {
            info!(
                "Database fusion replay progress: symbol={} rows_read={} rows_written={} elapsed={:.2?}",
                symbol, read_rows, written_rows, started_at.elapsed(),
            );
        }
    }
    if read_rows == 0 {
        bail!(
            "no baseline rows for symbol={} in requested range {}..{}",
            symbol,
            config.start_date,
            config.end_date
        );
    }
    written_rows = written_rows.saturating_add(flush_output_batch(
        &client,
        &config.clickhouse,
        factors,
        &mut batch,
    )?);
    info!(
        "Database fusion replay complete: symbol={} rows_read={} rows_written={} elapsed={:.2?}",
        symbol,
        read_rows,
        written_rows,
        started_at.elapsed(),
    );
    Ok(())
}

fn push_input_row(
    state: &mut BaselineReplayState,
    venue: TradingVenue,
    expected_symbol: &str,
    row: &InputRow,
) -> Result<()> {
    if row.symbol != expected_symbol {
        bail!(
            "source query returned unexpected symbol {} for {expected_symbol}",
            row.symbol
        );
    }
    let message = TradeFlowFeatureMsg::from_indexed_values(
        row.symbol.clone(),
        venue.to_u8(),
        row.ts_ms,
        &row.values,
    )
    .context("build trade-flow feature message from baseline row")?;
    state.push(message)
}

fn input_query(config: &ClickHouseConfig, symbol: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "SELECT toUnixTimestamp64Milli(t.ts), t.symbol, {} FROM {}.{} AS t INNER JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}' AND t.ts >= fromUnixTimestamp64Milli({}) AND t.ts < fromUnixTimestamp64Milli({}) ORDER BY t.ts FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        start_ms,
        end_ms,
    )
}

fn prior_rows_query(config: &ClickHouseConfig, symbol: &str, start_ms: i64) -> String {
    format!(
        "SELECT toUnixTimestamp64Milli(t.ts), t.symbol, {} FROM {}.{} AS t INNER JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}' AND t.ts < fromUnixTimestamp64Milli({}) ORDER BY t.ts DESC LIMIT {} FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        start_ms,
        MAX_SYMBOL_HISTORY,
    )
}

fn input_columns_sql() -> String {
    let mut columns: Vec<String> = TRADE_FLOW_FEATURE_FIELD_NAMES
        .iter()
        .map(|name| format!("t.{name}"))
        .collect();
    for side in ["bid", "ask"] {
        for level in 0..20 {
            columns.push(format!("d.{side}_{level:02}_price"));
            columns.push(format!("d.{side}_{level:02}_amount"));
        }
    }
    columns.join(", ")
}

fn read_input_row(reader: &mut impl Read) -> Result<Option<InputRow>> {
    let Some(ts_ms) = read_i64_or_eof(reader)? else {
        return Ok(None);
    };
    let symbol = read_string(reader)?;
    let mut values = Vec::with_capacity(INPUT_VALUE_COUNT);
    for _ in 0..INPUT_VALUE_COUNT {
        values.push(read_f64(reader)?);
    }
    Ok(Some(InputRow {
        ts_ms,
        symbol,
        values,
    }))
}

fn read_i64_or_eof(reader: &mut impl Read) -> Result<Option<i64>> {
    let mut bytes = [0u8; 8];
    match reader.read(&mut bytes[..1]) {
        Ok(0) => Ok(None),
        Ok(_) => {
            reader.read_exact(&mut bytes[1..])?;
            Ok(Some(i64::from_le_bytes(bytes)))
        }
        Err(err) => Err(err.into()),
    }
}

fn read_f64(reader: &mut impl Read) -> Result<f64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(f64::from_le_bytes(bytes))
}

fn read_string(reader: &mut impl Read) -> Result<String> {
    let len = read_var_uint(reader)? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).context("decode RowBinary symbol")
}

fn read_var_uint(reader: &mut impl Read) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        value |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("RowBinary varuint is too long")
}

fn ensure_output_table(config: &ClickHouseConfig, factors: &[String]) -> Result<()> {
    let client = clickhouse_client()?;
    clickhouse_execute(
        &client,
        &config.url,
        &format!("CREATE DATABASE IF NOT EXISTS {}", config.output_database),
    )?;
    let columns = output_columns_sql(factors);
    clickhouse_execute(
        &client,
        &config.url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({columns}) ENGINE = ReplacingMergeTree(replay_version) ORDER BY (symbol, ts)",
            config.output_database, config.output_table
        ),
    )?;
    ensure_config_covers_output_factors(&client, config, factors)?;
    for factor in factors {
        clickhouse_execute(
            &client,
            &config.url,
            &format!(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} Float64",
                config.output_database, config.output_table, factor
            ),
        )?;
    }
    Ok(())
}

fn ensure_config_covers_output_factors(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: &[String],
) -> Result<()> {
    let query = format!(
        "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' AND name NOT IN ('ts', 'symbol', 'replay_version') ORDER BY name FORMAT TabSeparatedRaw",
        config.output_database, config.output_table
    );
    let response = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .send()
        .context("read database fusion output columns")?
        .error_for_status()
        .context("database fusion output column query failed")?
        .text()
        .context("read database fusion output columns response")?;
    let missing: Vec<&str> = response
        .lines()
        .filter(|name| !factors.iter().any(|factor| factor == name))
        .collect();
    if !missing.is_empty() {
        bail!(
            "output table contains factor columns absent from config.factors: {}; include all existing factors before replaying so their values are retained",
            missing.join(",")
        );
    }
    Ok(())
}

fn output_columns_sql(factors: &[String]) -> String {
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
        "replay_version UInt64".to_string(),
    ];
    columns.extend(factors.iter().map(|name| format!("{name} Float64")));
    columns.join(", ")
}

fn flush_output_batch(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: &[String],
    batch: &mut Vec<OutputRow>,
) -> Result<u64> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut body = Vec::with_capacity(batch.len() * (40 + factors.len() * 8));
    for row in batch.iter() {
        body.extend_from_slice(&row.ts_ms.to_le_bytes());
        append_var_uint(&mut body, row.symbol.len() as u64);
        body.extend_from_slice(row.symbol.as_bytes());
        body.extend_from_slice(&row.replay_version.to_le_bytes());
        if row.values.len() != factors.len() {
            bail!("baseline output width mismatch: {}", row.values.len());
        }
        for value in &row.values {
            body.extend_from_slice(&value.to_le_bytes());
        }
    }
    let query = format!(
        "INSERT INTO {}.{} (ts, symbol, replay_version, {}) FORMAT RowBinary",
        config.output_database,
        config.output_table,
        factors.join(", "),
    );
    client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .context("insert database fusion factor batch")?
        .error_for_status()
        .context("database fusion factor insert failed")?;
    let rows = batch.len() as u64;
    batch.clear();
    Ok(rows)
}

fn clickhouse_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()
        .context("build ClickHouse HTTP client")
}

fn clickhouse_execute(client: &reqwest::blocking::Client, url: &str, query: &str) -> Result<()> {
    client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .send()
        .with_context(|| format!("ClickHouse query failed: {query}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse query returned error: {query}"))?;
    Ok(())
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid ClickHouse identifier: {value}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reads_rowbinary_baseline_row() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&123_i64.to_le_bytes());
        append_var_uint(&mut bytes, 7);
        bytes.extend_from_slice(b"BTCUSDT");
        for index in 0..INPUT_VALUE_COUNT {
            bytes.extend_from_slice(&(index as f64).to_le_bytes());
        }
        let row = read_input_row(&mut Cursor::new(bytes))
            .expect("read row")
            .expect("row");
        assert_eq!(row.ts_ms, 123);
        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.values.len(), INPUT_VALUE_COUNT);
        assert_eq!(
            row.values[INPUT_VALUE_COUNT - 1],
            (INPUT_VALUE_COUNT - 1) as f64
        );
    }

    #[test]
    fn output_schema_matches_selected_baselines() {
        let columns = output_columns_sql(&["baseline_042".to_string(), "baseline_118".to_string()]);
        assert_eq!(columns.matches(" Float64").count(), 2);
        assert!(columns.contains("replay_version UInt64"));
        assert!(columns.contains("baseline_042 Float64"));
        assert!(columns.contains("baseline_118 Float64"));
        assert!(!columns.contains("baseline_001 Float64"));
    }

    #[test]
    fn accepts_selected_supported_baselines() {
        let factors = normalize_factors(&[
            "baseline_042".to_string(),
            "BASELINE_118".to_string(),
            "baseline_042".to_string(),
        ])
        .expect("valid selected factors");
        assert_eq!(factors, ["baseline_042", "baseline_118"]);
        assert!(normalize_factors(&["factor_001".to_string()]).is_err());
    }

    #[test]
    fn date_bounds_are_inclusive() {
        let (start, end) = date_bounds("2026-06-15", "2026-07-15").expect("dates");
        assert_eq!(end - start, 31 * 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn replay_config_template_parses() {
        let config: Config =
            toml::from_str(include_str!("../../config/db_fusion_factor_replay.toml"))
                .expect("replay config template");
        assert_eq!(config.venue, "binance-futures");
        assert_eq!(
            config.symbols,
            ["XRPUSDT", "DOGEUSDT", "SOLUSDT", "ETHUSDT", "BTCUSDT", "BNBUSDT"]
        );
        assert_eq!(config.start_date, "2024-12-01");
        assert_eq!(config.end_date, "2024-12-31");
        assert_eq!(config.factors, ["baseline_118"]);
        assert_eq!(config.replay_workers, 6);
        assert_eq!(
            config.clickhouse.input_trade_table,
            "baseline_binance_futures_5s_trade"
        );
        assert_eq!(
            config.clickhouse.input_depth_table,
            "baseline_binance_futures_5s_depth"
        );
        assert_eq!(
            config.clickhouse.output_table,
            "fusion_factor_binance_futures_5s"
        );

        let config: Config = toml::from_str(include_str!(
            "../../config/db_fusion_factor_replay_60s.toml"
        ))
        .expect("60s replay config template");
        assert_eq!(
            config.clickhouse.input_trade_table,
            "baseline_binance_futures_60s_trade"
        );
        assert_eq!(
            config.clickhouse.input_depth_table,
            "baseline_binance_futures_60s_depth"
        );
        assert_eq!(
            config.clickhouse.output_table,
            "fusion_factor_binance_futures_60s"
        );
    }
}
