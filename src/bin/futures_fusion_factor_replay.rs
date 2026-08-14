//! Replay domestic-futures baseline bars through the independent five-level fusion engine.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate, Utc};
use clap::Parser;
use log::info;
#[cfg(test)]
use mkt_signal::factor_pub::cn_features::SUPPORTED_FUTURES_FACTOR_COUNT;
use mkt_signal::factor_pub::cn_features::{
    FuturesDepth5, FuturesFactorPlan, FuturesFusionInput, FuturesFusionState, FuturesTradeBar,
    FUTURES_DEPTH_LEVELS, FUTURES_TRADE_FIELD_COUNT, FUTURES_TRADE_FIELD_NAMES,
    MAX_FUTURES_HISTORY,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DOMESTIC_EXCHANGES: [&str; 6] = ["ccfx", "xdce", "xgfe", "xsge", "xsie", "xzce"];
const PROGRESS_ROWS: u64 = 100_000;
const FEATURE_SET: &str = "cn_features";

#[derive(Parser, Debug)]
#[command(name = "cn_features_replay")]
#[command(about = "Compute domestic-futures factors from native five-level baseline bars")]
struct Args {
    #[arg(long, default_value = "config/futures_fusion_factor_replay_xzce.toml")]
    config: PathBuf,
    /// Parse and validate the futures-specific config without touching ClickHouse.
    #[arg(long)]
    validate_config_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    exchange: String,
    feature_set: String,
    #[serde(default)]
    symbols: Vec<String>,
    start_date: String,
    end_date: String,
    factors: Vec<String>,
    #[serde(default = "default_workers")]
    replay_workers: usize,
    dry_run: bool,
    clickhouse: ClickHouseConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug)]
struct InputRow {
    input: FuturesFusionInput,
}

#[derive(Debug)]
struct OutputRow {
    ts_ms: i64,
    symbol: String,
    trading_day: u32,
    source_quality_flags: u32,
    source_volume_multiple_verified: bool,
    replay_version: u64,
    values: Vec<Option<f64>>,
}

fn default_workers() -> usize {
    1
}

fn default_batch_rows() -> usize {
    10_000
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
            "validated cn_features config: feature_set={} exchange={} symbols={} factors={} depth_levels={} dry_run={}",
            config.feature_set,
            validated.exchange,
            config.symbols.len(),
            validated.factor_plan.len(),
            FUTURES_DEPTH_LEVELS,
            config.dry_run,
        );
        return Ok(());
    }
    replay(&config, validated)
}

struct ValidatedConfig {
    exchange: String,
    start_trading_day: u32,
    end_trading_day: u32,
    factor_plan: FuturesFactorPlan,
}

fn validate_config(config: &Config) -> Result<ValidatedConfig> {
    if config.feature_set.trim() != FEATURE_SET {
        bail!(
            "feature_set must be {FEATURE_SET}, got {}",
            config.feature_set
        );
    }
    let exchange = normalize_exchange(&config.exchange)?;
    let start_trading_day = parse_trading_day(&config.start_date, "start_date")?;
    let end_trading_day = parse_trading_day(&config.end_date, "end_date")?;
    if start_trading_day > end_trading_day {
        bail!("start_date must not be after end_date");
    }
    if config.replay_workers == 0 {
        bail!("replay_workers must be > 0");
    }
    if config.clickhouse.batch_rows == 0 {
        bail!("clickhouse.batch_rows must be > 0");
    }
    if config.clickhouse.url.trim().is_empty() {
        bail!("clickhouse.url must not be empty");
    }
    for identifier in [
        &config.clickhouse.input_database,
        &config.clickhouse.input_trade_table,
        &config.clickhouse.input_depth_table,
        &config.clickhouse.output_database,
        &config.clickhouse.output_table,
    ] {
        validate_identifier(identifier)?;
    }
    let _ = normalize_symbols(&config.symbols)?;
    let factor_plan = FuturesFactorPlan::from_factor_names(config.factors.clone())?;
    for factor in factor_plan.factor_names() {
        validate_identifier(factor)?;
    }
    Ok(ValidatedConfig {
        exchange,
        start_trading_day,
        end_trading_day,
        factor_plan,
    })
}

fn replay(config: &Config, validated: ValidatedConfig) -> Result<()> {
    let client = clickhouse_client()?;
    let mut symbols = normalize_symbols(&config.symbols)?;
    if symbols.is_empty() {
        symbols = discover_symbols(
            &client,
            &config.clickhouse,
            validated.start_trading_day,
            validated.end_trading_day,
        )?;
    }
    if symbols.is_empty() {
        bail!(
            "no domestic-futures symbols found for trading days {}..{}",
            validated.start_trading_day,
            validated.end_trading_day
        );
    }
    if !config.dry_run {
        ensure_output_table(
            &client,
            &config.clickhouse,
            validated.factor_plan.factor_names(),
        )?;
    }

    let replay_version = u64::try_from(Utc::now().timestamp_millis())
        .context("system clock is before the Unix epoch")?;
    let worker_count = config.replay_workers.min(symbols.len());
    let started_at = Instant::now();
    info!(
        "Starting futures fusion replay: exchange={} symbols={} factors={} workers={} trading_days={}..{} input={}.{}+{} output={}.{} dry_run={}",
        validated.exchange,
        symbols.len(),
        validated.factor_plan.len(),
        worker_count,
        validated.start_trading_day,
        validated.end_trading_day,
        config.clickhouse.input_database,
        config.clickhouse.input_trade_table,
        config.clickhouse.input_depth_table,
        config.clickhouse.output_database,
        config.clickhouse.output_table,
        config.dry_run,
    );

    rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build()
        .context("build futures fusion replay Rayon pool")?
        .install(|| {
            symbols.par_iter().try_for_each(|symbol| {
                replay_symbol(
                    config,
                    symbol,
                    validated.start_trading_day,
                    validated.end_trading_day,
                    &validated.factor_plan,
                    replay_version,
                )
            })
        })?;

    info!(
        "Futures fusion replay complete: exchange={} symbols={} elapsed={:.2?}",
        validated.exchange,
        symbols.len(),
        started_at.elapsed(),
    );
    Ok(())
}

fn replay_symbol(
    config: &Config,
    symbol: &str,
    start_trading_day: u32,
    end_trading_day: u32,
    factor_plan: &FuturesFactorPlan,
    replay_version: u64,
) -> Result<()> {
    let client = clickhouse_client()?;
    let warmup_query = prior_rows_query(&config.clickhouse, symbol, start_trading_day);
    let mut warmup_response = clickhouse_rowbinary_query(
        &client,
        &config.clickhouse.url,
        warmup_query,
        "read futures warm-up rows",
    )?;
    let mut warmup = Vec::with_capacity(MAX_FUTURES_HISTORY);
    while let Some(row) = read_input_row(&mut warmup_response)? {
        warmup.push(row);
    }

    let query = input_query(
        &config.clickhouse,
        symbol,
        start_trading_day,
        end_trading_day,
    );
    let mut response = clickhouse_rowbinary_query(
        &client,
        &config.clickhouse.url,
        query,
        "read futures baseline rows",
    )?;

    let started_at = Instant::now();
    let mut state = FuturesFusionState::default();
    for row in warmup.into_iter().rev() {
        validate_expected_symbol(symbol, &row)?;
        state.push(row.input)?;
    }

    let mut batch = Vec::with_capacity(config.clickhouse.batch_rows);
    let mut read_rows = 0u64;
    let mut written_rows = 0u64;
    let factor_names: Vec<&str> = factor_plan.factor_names().collect();
    while let Some(row) = read_input_row(&mut response)? {
        read_rows = read_rows.saturating_add(1);
        validate_expected_symbol(symbol, &row)?;
        let ts_ms = row.input.ts_ms;
        let trading_day = row.input.trading_day;
        let source_quality_flags = row.input.quality_flags;
        let source_volume_multiple_verified = row.input.volume_multiple_verified;
        state.push(row.input)?;
        let values = state.factor_values(factor_plan)?;
        if values.len() != factor_names.len() {
            bail!(
                "futures factor width mismatch: symbol={symbol} ts_ms={ts_ms} names={} values={}",
                factor_names.len(),
                values.len()
            );
        }
        for (name, value) in factor_names.iter().zip(&values) {
            if value.is_some_and(|value| !value.is_finite()) {
                bail!(
                    "non-finite futures factor: symbol={symbol} ts_ms={ts_ms} factor={name} value={value:?}"
                );
            }
        }

        if !config.dry_run {
            batch.push(OutputRow {
                ts_ms,
                symbol: symbol.to_string(),
                trading_day,
                source_quality_flags,
                source_volume_multiple_verified,
                replay_version,
                values,
            });
            if batch.len() >= config.clickhouse.batch_rows {
                written_rows = written_rows.saturating_add(flush_output_batch(
                    &client,
                    &config.clickhouse,
                    &factor_names,
                    &mut batch,
                )?);
            }
        }
        if read_rows % PROGRESS_ROWS == 0 {
            info!(
                "Futures fusion progress: symbol={} read_rows={} written_rows={} elapsed={:.2?}",
                symbol,
                read_rows,
                written_rows,
                started_at.elapsed(),
            );
        }
    }
    if !config.dry_run {
        written_rows = written_rows.saturating_add(flush_output_batch(
            &client,
            &config.clickhouse,
            &factor_names,
            &mut batch,
        )?);
    }
    info!(
        "Futures fusion symbol complete: symbol={} read_rows={} written_rows={} dry_run={} elapsed={:.2?}",
        symbol,
        read_rows,
        written_rows,
        config.dry_run,
        started_at.elapsed(),
    );
    Ok(())
}

fn validate_expected_symbol(expected: &str, row: &InputRow) -> Result<()> {
    if row.input.symbol != expected {
        bail!(
            "source query returned unexpected symbol {} for {expected}",
            row.input.symbol
        );
    }
    Ok(())
}

fn normalize_exchange(raw: &str) -> Result<String> {
    let exchange = raw.trim().to_ascii_lowercase();
    if !DOMESTIC_EXCHANGES.contains(&exchange.as_str()) {
        bail!(
            "unsupported domestic futures exchange {raw}; expected one of {}",
            DOMESTIC_EXCHANGES.join(", ")
        );
    }
    Ok(exchange)
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
            bail!("invalid futures symbol: {raw_symbol}");
        }
        if !symbols.contains(&symbol) {
            symbols.push(symbol);
        }
    }
    Ok(symbols)
}

fn parse_trading_day(raw: &str, name: &str) -> Result<u32> {
    let date = NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("parse {name} {raw}"))?;
    Ok((date.year() as u32) * 10_000 + date.month() * 100 + date.day())
}

fn discover_symbols(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    start_trading_day: u32,
    end_trading_day: u32,
) -> Result<Vec<String>> {
    let query = format!(
        "SELECT DISTINCT t.symbol FROM {}.{} AS t WHERE t.trading_day >= {} AND t.trading_day <= {} ORDER BY t.symbol FORMAT TabSeparatedRaw",
        config.input_database,
        config.input_trade_table,
        start_trading_day,
        end_trading_day,
    );
    let response = client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .context("discover futures replay symbols")?
        .error_for_status()
        .context("ClickHouse futures symbol query failed")?
        .text()
        .context("read futures symbol query response")?;
    normalize_symbols(
        &response
            .lines()
            .map(str::to_string)
            .collect::<Vec<String>>(),
    )
}

fn input_columns_sql() -> String {
    let mut columns = vec![
        "toUnixTimestamp64Milli(t.ts)".to_string(),
        "t.symbol".to_string(),
        "t.trading_day".to_string(),
    ];
    columns.extend(
        FUTURES_TRADE_FIELD_NAMES
            .iter()
            .map(|name| format!("t.{name}")),
    );
    columns.extend([
        "t.quality_flags".to_string(),
        "t.volume_multiple".to_string(),
        "t.volume_multiple_verified".to_string(),
        "d.bid_prices".to_string(),
        "d.bid_amounts".to_string(),
        "d.ask_prices".to_string(),
        "d.ask_amounts".to_string(),
    ]);
    columns.join(", ")
}

fn input_query(
    config: &ClickHouseConfig,
    symbol: &str,
    start_trading_day: u32,
    end_trading_day: u32,
) -> String {
    format!(
        "SELECT {} FROM {}.{} AS t LEFT JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}' AND t.trading_day >= {} AND t.trading_day <= {} ORDER BY t.ts FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        start_trading_day,
        end_trading_day,
    )
}

fn prior_rows_query(config: &ClickHouseConfig, symbol: &str, start_trading_day: u32) -> String {
    format!(
        "SELECT {} FROM {}.{} AS t LEFT JOIN {}.{} AS d USING (symbol, ts) WHERE t.symbol = '{}' AND t.trading_day < {} ORDER BY t.ts DESC LIMIT {} FORMAT RowBinary",
        input_columns_sql(),
        config.input_database,
        config.input_trade_table,
        config.input_database,
        config.input_depth_table,
        symbol,
        start_trading_day,
        MAX_FUTURES_HISTORY,
    )
}

fn read_input_row(reader: &mut impl Read) -> Result<Option<InputRow>> {
    let Some(ts_ms) = read_i64_or_eof(reader)? else {
        return Ok(None);
    };
    let symbol = read_string(reader)?;
    let trading_day = read_u32(reader)?;
    let mut trade_values = [0.0; FUTURES_TRADE_FIELD_COUNT];
    for value in &mut trade_values {
        *value = read_f64(reader)?;
    }
    let quality_flags = read_u32(reader)?;
    let volume_multiple = read_f64(reader)?;
    let volume_multiple_verified = read_u8(reader)? != 0;
    let bid_prices = read_native_depth_array(reader, "bid_prices")?;
    let bid_amounts = read_native_depth_array(reader, "bid_amounts")?;
    let ask_prices = read_native_depth_array(reader, "ask_prices")?;
    let ask_amounts = read_native_depth_array(reader, "ask_amounts")?;
    let lengths = [
        bid_prices.len(),
        bid_amounts.len(),
        ask_prices.len(),
        ask_amounts.len(),
    ];
    let depth = if lengths.iter().all(|length| *length == 0) {
        None
    } else if lengths.iter().all(|length| *length == FUTURES_DEPTH_LEVELS) {
        Some(FuturesDepth5::from_slices(
            &bid_prices,
            &bid_amounts,
            &ask_prices,
            &ask_amounts,
        )?)
    } else {
        bail!(
            "native depth arrays must be all empty or all exactly {FUTURES_DEPTH_LEVELS} levels, got {lengths:?}"
        );
    };

    let input = FuturesFusionInput {
        ts_ms,
        symbol,
        trading_day,
        trade: FuturesTradeBar::from_slice(&trade_values)?,
        depth,
        quality_flags,
        volume_multiple,
        volume_multiple_verified,
    };
    input.validate()?;
    Ok(Some(InputRow { input }))
}

fn read_native_depth_array(reader: &mut impl Read, name: &str) -> Result<Vec<f64>> {
    let len = usize::try_from(read_var_uint(reader)?)
        .with_context(|| format!("RowBinary {name} length exceeds usize"))?;
    if len != 0 && len != FUTURES_DEPTH_LEVELS {
        bail!(
            "RowBinary {name} must be empty or contain exactly {FUTURES_DEPTH_LEVELS} native levels, got {len}"
        );
    }
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_f64(reader)?);
    }
    Ok(values)
}

fn read_i64_or_eof(reader: &mut impl Read) -> Result<Option<i64>> {
    let mut bytes = [0u8; 8];
    match reader.read(&mut bytes[..1]) {
        Ok(0) => Ok(None),
        Ok(_) => {
            reader.read_exact(&mut bytes[1..])?;
            Ok(Some(i64::from_le_bytes(bytes)))
        }
        Err(error) => Err(error.into()),
    }
}

fn read_string(reader: &mut impl Read) -> Result<String> {
    let len =
        usize::try_from(read_var_uint(reader)?).context("RowBinary string length overflow")?;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).context("RowBinary symbol is not UTF-8")
}

fn read_var_uint(reader: &mut impl Read) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = read_u8(reader)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("RowBinary varuint exceeds 10 bytes")
}

fn read_u8(reader: &mut impl Read) -> Result<u8> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_f64(reader: &mut impl Read) -> Result<f64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(f64::from_le_bytes(bytes))
}

fn clickhouse_rowbinary_query(
    client: &reqwest::blocking::Client,
    url: &str,
    query: String,
    context: &'static str,
) -> Result<reqwest::blocking::Response> {
    client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .context(context)?
        .error_for_status()
        .with_context(|| format!("{context}: ClickHouse returned an error"))
}

fn ensure_output_table<'a>(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: impl Iterator<Item = &'a str>,
) -> Result<()> {
    clickhouse_execute(
        client,
        &config.url,
        &format!("CREATE DATABASE IF NOT EXISTS {}", config.output_database),
    )?;
    let factor_names: Vec<&str> = factors.collect();
    let mut columns = vec![
        "ts DateTime64(3, 'UTC') CODEC(Delta, ZSTD)".to_string(),
        "symbol String".to_string(),
        "trading_day UInt32".to_string(),
        "source_quality_flags UInt32".to_string(),
        "source_volume_multiple_verified UInt8".to_string(),
        "replay_version UInt64".to_string(),
    ];
    columns.extend(
        factor_names
            .iter()
            .map(|name| format!("{name} Nullable(Float64)")),
    );
    clickhouse_execute(
        client,
        &config.url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {}.{} ({}) ENGINE = ReplacingMergeTree(replay_version) PARTITION BY trading_day ORDER BY (symbol, trading_day, ts)",
            config.output_database,
            config.output_table,
            columns.join(", "),
        ),
    )?;
    for factor in factor_names {
        clickhouse_execute(
            client,
            &config.url,
            &format!(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} Nullable(Float64)",
                config.output_database, config.output_table, factor
            ),
        )?;
    }
    Ok(())
}

fn flush_output_batch(
    client: &reqwest::blocking::Client,
    config: &ClickHouseConfig,
    factors: &[&str],
    batch: &mut Vec<OutputRow>,
) -> Result<u64> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut body = Vec::with_capacity(batch.len() * (48 + factors.len() * 9));
    for row in batch.iter() {
        if row.values.len() != factors.len() {
            bail!(
                "futures output width mismatch: values={} factors={}",
                row.values.len(),
                factors.len()
            );
        }
        body.extend_from_slice(&row.ts_ms.to_le_bytes());
        append_var_uint(&mut body, row.symbol.len() as u64);
        body.extend_from_slice(row.symbol.as_bytes());
        body.extend_from_slice(&row.trading_day.to_le_bytes());
        body.extend_from_slice(&row.source_quality_flags.to_le_bytes());
        body.push(u8::from(row.source_volume_multiple_verified));
        body.extend_from_slice(&row.replay_version.to_le_bytes());
        for value in &row.values {
            append_nullable_f64(&mut body, *value);
        }
    }
    let mut columns = vec![
        "ts",
        "symbol",
        "trading_day",
        "source_quality_flags",
        "source_volume_multiple_verified",
        "replay_version",
    ];
    columns.extend(factors.iter().copied());
    let query = format!(
        "INSERT INTO {}.{} ({}) FORMAT RowBinary",
        config.output_database,
        config.output_table,
        columns.join(", "),
    );
    client
        .post(config.url.trim_end_matches('/'))
        .query(&[("query", query)])
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .context("insert futures fusion factor batch")?
        .error_for_status()
        .context("ClickHouse futures fusion insert failed")?;
    let rows = batch.len() as u64;
    batch.clear();
    Ok(rows)
}

fn append_nullable_f64(output: &mut Vec<u8>, value: Option<f64>) {
    match value {
        Some(value) => {
            output.push(0);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(1),
    }
}

fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn clickhouse_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(300))
        .build()
        .context("build ClickHouse HTTP client")
}

fn clickhouse_execute(client: &reqwest::blocking::Client, url: &str, query: &str) -> Result<()> {
    client
        .post(url.trim_end_matches('/'))
        .query(&[("query", query)])
        .body(Vec::new())
        .send()
        .with_context(|| format!("ClickHouse query failed: {query}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse query returned error: {query}"))?;
    Ok(())
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

    fn append_input_row(output: &mut Vec<u8>, depth_levels: usize) {
        output.extend_from_slice(&1_762_143_600_000_i64.to_le_bytes());
        append_var_uint(output, 6);
        output.extend_from_slice(b"AP2601");
        output.extend_from_slice(&20251103_u32.to_le_bytes());
        for _ in 0..FUTURES_TRADE_FIELD_COUNT {
            output.extend_from_slice(&0.0f64.to_le_bytes());
        }
        output.extend_from_slice(&0_u32.to_le_bytes());
        output.extend_from_slice(&1.0f64.to_le_bytes());
        output.push(0);
        for (base, step) in [(100.0, -1.0), (1.0, 1.0), (101.0, 1.0), (2.0, 1.0)] {
            append_var_uint(output, depth_levels as u64);
            for level in 0..depth_levels {
                output.extend_from_slice(&(base + step * level as f64).to_le_bytes());
            }
        }
    }

    #[test]
    fn rowbinary_input_keeps_exactly_five_levels() {
        let mut bytes = Vec::new();
        append_input_row(&mut bytes, FUTURES_DEPTH_LEVELS);
        let row = read_input_row(&mut Cursor::new(bytes))
            .unwrap()
            .expect("row");
        assert_eq!(row.input.depth.as_ref().unwrap().bid_prices.len(), 5);
        assert_eq!(row.input.depth.as_ref().unwrap().ask_amounts.len(), 5);
        assert_eq!(row.input.depth.as_ref().unwrap().bid_prices[4], 96.0);
    }

    #[test]
    fn rowbinary_input_rejects_non_five_depth_without_padding() {
        for levels in [4, 6, 20] {
            let mut bytes = Vec::new();
            append_input_row(&mut bytes, levels);
            let error = read_input_row(&mut Cursor::new(bytes)).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("empty or contain exactly 5 native levels"),
                "levels={levels} error={error:#}"
            );
        }
    }

    #[test]
    fn input_query_is_futures_specific_and_has_no_wide_depth_columns() {
        let config = ClickHouseConfig {
            url: "http://localhost".to_string(),
            input_database: "baseline".to_string(),
            input_trade_table: "baseline_xzce_future_5s_trade".to_string(),
            input_depth_table: "baseline_xzce_future_5s_depth".to_string(),
            output_database: "cn_features".to_string(),
            output_table: "cn_features_xzce_5s".to_string(),
            batch_rows: 100,
        };
        let query = input_query(&config, "AP2601", 20251103, 20251103);
        assert!(query.contains("LEFT JOIN"));
        assert!(query.contains("d.bid_prices"));
        assert!(query.contains("t.trading_day >= 20251103"));
        assert!(!query.contains("bid_00_price"));
        assert!(!query.contains("bid_19_price"));
    }

    #[test]
    fn output_uses_nullable_futures_values() {
        let mut bytes = Vec::new();
        append_nullable_f64(&mut bytes, Some(1.5));
        append_nullable_f64(&mut bytes, None);
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[9], 1);
    }

    #[test]
    fn all_exchange_configs_parse_and_remain_dry_run() {
        let configs = [
            include_str!("../../config/futures_fusion_factor_replay_ccfx.toml"),
            include_str!("../../config/futures_fusion_factor_replay_xdce.toml"),
            include_str!("../../config/futures_fusion_factor_replay_xgfe.toml"),
            include_str!("../../config/futures_fusion_factor_replay_xsge.toml"),
            include_str!("../../config/futures_fusion_factor_replay_xsie.toml"),
            include_str!("../../config/futures_fusion_factor_replay_xzce.toml"),
        ];
        for content in configs {
            let config: Config = toml::from_str(content).unwrap();
            let validated = validate_config(&config).unwrap();
            assert!(DOMESTIC_EXCHANGES.contains(&validated.exchange.as_str()));
            assert!(config.dry_run);
            assert_eq!(validated.factor_plan.len(), SUPPORTED_FUTURES_FACTOR_COUNT);
        }
    }
}
