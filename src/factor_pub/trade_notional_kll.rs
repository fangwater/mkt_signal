//! ClickHouse-backed hourly trade-notional KLL loading and aggregation.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use mkt_parsers::msg::trade_notional_kll_msg::TradeNotionalKllMsg;
use rolling_common::kll_quantile::FrozenKllSketch;
use std::io::Read;
use std::time::Duration;

pub const HOUR_MS: i64 = 3_600_000;

#[derive(Debug, Clone)]
pub struct MergedTradeNotionalKll {
    pub symbol: String,
    pub venue: u8,
    pub start_ms: i64,
    pub end_ms: i64,
    pub source_hourly_rows: usize,
    pub first_hour_start_ms: i64,
    pub last_hour_start_ms: i64,
    pub sketch: FrozenKllSketch,
}

struct HourlyRow {
    hour_start_ms: i64,
    symbol: String,
    sample_count: u64,
    level_capacity: u32,
    payload: Vec<u8>,
}

pub fn load_merged_hourly_kll(
    clickhouse_url: &str,
    database: &str,
    table: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<MergedTradeNotionalKll> {
    if start_ms >= end_ms {
        bail!("start_ms must be before end_ms");
    }
    if start_ms.rem_euclid(HOUR_MS) != 0 || end_ms.rem_euclid(HOUR_MS) != 0 {
        bail!("start_ms and end_ms must be aligned to whole UTC hours");
    }
    validate_identifier(database)?;
    validate_identifier(table)?;
    let symbol = normalize_symbol(symbol)?;
    let query = hourly_kll_query(database, table, &symbol, start_ms, end_ms);
    let client = clickhouse_client()?;
    let mut response = client
        .post(clickhouse_url.trim_end_matches('/'))
        .query(&[("query", query.as_str())])
        .body(Vec::new())
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
        validate_row(&row, &symbol, start_ms, end_ms)?;
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
    let sketch = FrozenKllSketch::merge_all(sketches.iter())
        .context("merge hourly KLL sketches")?
        .ok_or_else(|| {
            anyhow!("no hourly KLL rows found for symbol={symbol} in requested range")
        })?;
    Ok(MergedTradeNotionalKll {
        symbol,
        venue: venue.ok_or_else(|| anyhow!("merged KLL has no venue"))?,
        start_ms,
        end_ms,
        source_hourly_rows,
        first_hour_start_ms: first_hour_start_ms
            .ok_or_else(|| anyhow!("merged KLL has no first hour"))?,
        last_hour_start_ms: last_hour_start_ms
            .ok_or_else(|| anyhow!("merged KLL has no last hour"))?,
        sketch,
    })
}

pub fn order_size_thresholds(
    sketch: &FrozenKllSketch,
    medium_quantile: f32,
    large_quantile: f32,
) -> Result<(f64, f64)> {
    validate_order_size_quantiles(medium_quantile, large_quantile)?;
    let values = sketch.quantiles(&[medium_quantile, large_quantile]);
    let medium = values
        .first()
        .copied()
        .flatten()
        .context("KLL medium quantile is unavailable")?;
    let large = values
        .get(1)
        .copied()
        .flatten()
        .context("KLL large quantile is unavailable")?;
    if !medium.is_finite() || !large.is_finite() || medium <= 0.0 || medium > large {
        bail!(
            "invalid KLL order-size thresholds: medium={} large={}",
            medium,
            large
        );
    }
    Ok((medium, large))
}

pub fn validate_order_size_quantiles(medium_quantile: f32, large_quantile: f32) -> Result<()> {
    if !medium_quantile.is_finite()
        || !large_quantile.is_finite()
        || !(0.0..=1.0).contains(&medium_quantile)
        || !(0.0..=1.0).contains(&large_quantile)
        || medium_quantile > large_quantile
    {
        bail!(
            "order-size quantiles must satisfy 0 <= medium <= large <= 1, got medium={} large={}",
            medium_quantile,
            large_quantile
        );
    }
    Ok(())
}

pub fn utc_month_bounds(year_month: &str) -> Result<(i64, i64)> {
    let month_start = parse_year_month(year_month)?;
    let next_month = next_month(month_start)?;
    Ok((utc_midnight_ms(month_start)?, utc_midnight_ms(next_month)?))
}

pub fn previous_month(year_month: &str) -> Result<String> {
    let current = parse_year_month(year_month)?;
    let (year, month) = if current.month() == 1 {
        (current.year() - 1, 12)
    } else {
        (current.year(), current.month() - 1)
    };
    Ok(format!("{year:04}-{month:02}"))
}

fn parse_year_month(year_month: &str) -> Result<NaiveDate> {
    if year_month.len() != 7 {
        bail!("month must use YYYY-MM, got '{year_month}'");
    }
    NaiveDate::parse_from_str(&format!("{year_month}-01"), "%Y-%m-%d")
        .with_context(|| format!("parse month {year_month}"))
}

fn next_month(current: NaiveDate) -> Result<NaiveDate> {
    let (year, month) = if current.month() == 12 {
        (current.year() + 1, 1)
    } else {
        (current.year(), current.month() + 1)
    };
    NaiveDate::from_ymd_opt(year, month, 1).context("next month exceeds supported date range")
}

fn utc_midnight_ms(date: NaiveDate) -> Result<i64> {
    let midnight = date
        .and_hms_opt(0, 0, 0)
        .context("build UTC month boundary")?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(midnight, Utc).timestamp_millis())
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
    let first = reader
        .read(&mut bytes[..1])
        .context("read RowBinary hour_start")?;
    if first == 0 {
        return Ok(None);
    }
    reader
        .read_exact(&mut bytes[1..])
        .context("read complete RowBinary hour_start")?;
    Ok(Some(i64::from_le_bytes(bytes)))
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
        .timeout(Duration::from_secs(60))
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
    fn month_helpers_handle_year_boundary() {
        assert_eq!(previous_month("2026-01").unwrap(), "2025-12");
        let (start, end) = utc_month_bounds("2024-02").unwrap();
        assert_eq!(end - start, 29 * 24 * HOUR_MS);
        assert!(utc_month_bounds("2024-13").is_err());
    }

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
    fn rejects_truncated_hour_start() {
        assert!(read_hourly_row(&mut Cursor::new(vec![1, 2, 3])).is_err());
    }

    #[test]
    fn extracts_order_size_quantiles() {
        let mut sketch = StreamingKllSketch::new();
        for value in 1..=100 {
            sketch.insert(value as f64);
        }
        let (medium, large) = order_size_thresholds(&sketch.freeze(), 0.5, 0.9).unwrap();
        assert_eq!(medium, 50.5);
        assert!((large - 90.1).abs() < 1e-5);
        assert!(order_size_thresholds(&sketch.freeze(), 0.9, 0.5).is_err());
    }

    fn append_var_uint(output: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            output.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
