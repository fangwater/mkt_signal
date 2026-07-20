//! Merge hourly trade-notional KLL sketches from ClickHouse for one symbol and time range.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use mkt_parsers::msg::trade_notional_kll_msg::TradeNotionalKllMsg;
use rolling_common::kll_quantile::FrozenKllSketch;
use serde::Serialize;
use std::io::{ErrorKind, Read};

const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:18123";
const DEFAULT_DATABASE: &str = "baseline";
const DEFAULT_VENUE: &str = "binance-futures";
const HOUR_MS: i64 = 3_600_000;

#[derive(Parser, Debug)]
#[command(name = "trade_notional_kll_aggregate")]
#[command(about = "Merge hourly trade-notional KLL sketches from ClickHouse")]
struct Args {
    /// Market symbol stored in the hourly KLL table.
    #[arg(long)]
    symbol: String,
    /// Inclusive UTC Unix timestamp in milliseconds.
    #[arg(long)]
    start_ms: i64,
    /// Exclusive UTC Unix timestamp in milliseconds.
    #[arg(long)]
    end_ms: i64,
    #[arg(long, default_value = DEFAULT_CLICKHOUSE_URL)]
    clickhouse_url: String,
    #[arg(long, default_value = DEFAULT_DATABASE)]
    database: String,
    /// Used to derive the table name when --table is omitted.
    #[arg(long, default_value = DEFAULT_VENUE)]
    venue: String,
    /// Override the derived table name.
    #[arg(long)]
    table: Option<String>,
}

#[derive(Debug)]
struct HourlyRow {
    hour_start_ms: i64,
    symbol: String,
    sample_count: u64,
    level_capacity: u32,
    payload: Vec<u8>,
}

#[derive(Serialize)]
struct AggregateOutput {
    symbol: String,
    venue: u8,
    start_ms: i64,
    end_ms: i64,
    source_hourly_rows: usize,
    first_hour_start_ms: i64,
    last_hour_start_ms: i64,
    sample_count: usize,
    level_capacity: usize,
    levels: Vec<Vec<f64>>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let output = aggregate(&args)?;
    println!(
        "{}",
        serde_json::to_string(&output).context("serialize merged KLL JSON")?
    );
    Ok(())
}

fn aggregate(args: &Args) -> Result<AggregateOutput> {
    if args.start_ms >= args.end_ms {
        bail!("start_ms must be before end_ms");
    }
    if args.start_ms.rem_euclid(HOUR_MS) != 0 || args.end_ms.rem_euclid(HOUR_MS) != 0 {
        bail!("start_ms and end_ms must be aligned to whole UTC hours");
    }
    let symbol = normalize_symbol(&args.symbol)?;
    validate_identifier(&args.database)?;
    let table = args
        .table
        .clone()
        .unwrap_or_else(|| default_table_name(&args.venue));
    validate_identifier(&table)?;

    let query = hourly_kll_query(&args.database, &table, &symbol, args.start_ms, args.end_ms);
    let client = clickhouse_client()?;
    let mut response = client
        .post(args.clickhouse_url.trim_end_matches('/'))
        .query(&[("query", query.as_str())])
        .send()
        .with_context(|| format!("read hourly KLL rows for {symbol}"))?
        .error_for_status()
        .with_context(|| format!("ClickHouse hourly KLL query failed for {symbol}"))?;

    let mut sketches = Vec::new();
    let mut venue = None;
    let mut first_hour_start_ms = None;
    let mut last_hour_start_ms = None;
    let mut previous_hour_start_ms = None;
    while let Some(row) = read_hourly_row(&mut response)? {
        validate_row(&row, &symbol, args.start_ms, args.end_ms)?;
        if previous_hour_start_ms == Some(row.hour_start_ms) {
            bail!(
                "duplicate hourly KLL row for symbol={} hour_start_ms={}",
                row.symbol,
                row.hour_start_ms
            );
        }
        let message = TradeNotionalKllMsg::from_bytes(&row.payload).with_context(|| {
            format!(
                "decode hourly KLL payload for symbol={} hour_start_ms={}",
                row.symbol, row.hour_start_ms
            )
        })?;
        if message.symbol != row.symbol
            || message.hour_start_ms != row.hour_start_ms
            || message.sketch.sample_count != row.sample_count as usize
            || message.sketch.level_capacity != row.level_capacity as usize
        {
            bail!(
                "hourly KLL metadata does not match payload for symbol={} hour_start_ms={}",
                row.symbol,
                row.hour_start_ms
            );
        }
        if let Some(expected_venue) = venue {
            if message.venue != expected_venue {
                bail!(
                    "hourly KLL payload venue changed from {} to {} within the requested range",
                    expected_venue,
                    message.venue
                );
            }
        } else {
            venue = Some(message.venue);
        }
        first_hour_start_ms.get_or_insert(row.hour_start_ms);
        last_hour_start_ms = Some(row.hour_start_ms);
        previous_hour_start_ms = Some(row.hour_start_ms);
        sketches.push(message.sketch);
    }

    let source_hourly_rows = sketches.len();
    let merged = FrozenKllSketch::merge_all(sketches.iter())
        .context("merge hourly KLL sketches")?
        .ok_or_else(|| {
            anyhow!("no hourly KLL rows found for symbol={symbol} in requested range")
        })?;
    Ok(AggregateOutput {
        symbol,
        venue: venue.ok_or_else(|| anyhow!("merged KLL has no venue"))?,
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        source_hourly_rows,
        first_hour_start_ms: first_hour_start_ms
            .ok_or_else(|| anyhow!("merged KLL has no first hour"))?,
        last_hour_start_ms: last_hour_start_ms
            .ok_or_else(|| anyhow!("merged KLL has no last hour"))?,
        sample_count: merged.sample_count,
        level_capacity: merged.level_capacity,
        levels: merged.levels,
    })
}

fn default_table_name(venue: &str) -> String {
    format!("trade_notional_kll_{}_hourly", venue.replace('-', "_"))
}

fn hourly_kll_query(
    database: &str,
    table: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> String {
    format!(
        "SELECT toUnixTimestamp64Milli(hour_start), symbol, sample_count, level_capacity, payload FROM {database}.{table} WHERE symbol = '{symbol}' AND hour_start >= fromUnixTimestamp64Milli({start_ms}) AND hour_start < fromUnixTimestamp64Milli({end_ms}) ORDER BY hour_start FORMAT RowBinary"
    )
}

fn validate_row(row: &HourlyRow, symbol: &str, start_ms: i64, end_ms: i64) -> Result<()> {
    if row.symbol != symbol {
        bail!("source query returned unexpected symbol {}", row.symbol);
    }
    if row.hour_start_ms < start_ms || row.hour_start_ms >= end_ms {
        bail!(
            "source query returned out-of-range hour {}",
            row.hour_start_ms
        );
    }
    if row.level_capacity == 0 {
        bail!("hourly KLL level_capacity must be positive");
    }
    Ok(())
}

fn read_hourly_row(reader: &mut impl Read) -> Result<Option<HourlyRow>> {
    let Some(hour_start_ms) = read_i64_or_eof(reader)? else {
        return Ok(None);
    };
    Ok(Some(HourlyRow {
        hour_start_ms,
        symbol: read_string(reader)?,
        sample_count: read_u64(reader)?,
        level_capacity: read_u32(reader)?,
        payload: read_bytes(reader)?,
    }))
}

fn read_i64_or_eof(reader: &mut impl Read) -> Result<Option<i64>> {
    let mut bytes = [0_u8; 8];
    match reader.read_exact(&mut bytes) {
        Ok(()) => Ok(Some(i64::from_le_bytes(bytes))),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error).context("read RowBinary hour_start"),
    }
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    reader
        .read_exact(&mut bytes)
        .context("read RowBinary sample_count")?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    reader
        .read_exact(&mut bytes)
        .context("read RowBinary level_capacity")?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_string(reader: &mut impl Read) -> Result<String> {
    let bytes = read_bytes(reader)?;
    String::from_utf8(bytes).context("decode RowBinary string")
}

fn read_bytes(reader: &mut impl Read) -> Result<Vec<u8>> {
    let len = read_var_uint(reader)?;
    let len = usize::try_from(len).context("RowBinary string length exceeds usize")?;
    let mut bytes = vec![0_u8; len];
    reader
        .read_exact(&mut bytes)
        .context("read RowBinary string body")?;
    Ok(bytes)
}

fn read_var_uint(reader: &mut impl Read) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0_u8; 1];
        reader
            .read_exact(&mut byte)
            .context("read RowBinary varuint")?;
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("RowBinary varuint is too long")
}

fn clickhouse_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("build ClickHouse HTTP client")
}

fn normalize_symbol(raw: &str) -> Result<String> {
    let symbol = raw.trim().to_ascii_uppercase();
    if symbol.is_empty()
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("invalid symbol: {raw}");
    }
    Ok(symbol)
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
    use rolling_common::kll_quantile::StreamingKllSketch;
    use std::io::Cursor;

    #[test]
    fn query_is_symbol_and_range_scoped() {
        let query = hourly_kll_query(
            "baseline",
            "trade_notional_kll_binance_futures_hourly",
            "BTCUSDT",
            100,
            200,
        );
        assert!(query.contains("symbol = 'BTCUSDT'"));
        assert!(query.contains("fromUnixTimestamp64Milli(100)"));
        assert!(query.contains("fromUnixTimestamp64Milli(200)"));
    }

    #[test]
    fn reads_hourly_rowbinary_row() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&123_i64.to_le_bytes());
        append_var_uint(&mut bytes, 7);
        bytes.extend_from_slice(b"BTCUSDT");
        bytes.extend_from_slice(&42_u64.to_le_bytes());
        bytes.extend_from_slice(&512_u32.to_le_bytes());
        append_var_uint(&mut bytes, 3);
        bytes.extend_from_slice(&[1, 2, 3]);
        let row = read_hourly_row(&mut Cursor::new(bytes))
            .expect("read row")
            .expect("row");
        assert_eq!(row.hour_start_ms, 123);
        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.sample_count, 42);
        assert_eq!(row.level_capacity, 512);
        assert_eq!(row.payload, [1, 2, 3]);
    }

    #[test]
    fn merged_rows_preserve_all_hourly_samples() {
        let mut first = StreamingKllSketch::new();
        first.insert(10.0);
        first.insert(20.0);
        let mut second = StreamingKllSketch::new();
        second.insert(30.0);
        let merged = FrozenKllSketch::merge_all([&first.freeze(), &second.freeze()])
            .expect("merge")
            .expect("merged sketch");
        assert_eq!(merged.sample_count, 3);
        assert_eq!(
            merged.quantiles(&[0.0, 0.5, 1.0]),
            vec![Some(10.0), Some(20.0), Some(30.0)]
        );
    }

    fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
