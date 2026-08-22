//! Merge hourly trade-notional KLL sketches for one symbol and extract thresholds.

use anyhow::{bail, Context, Result};
use clap::Parser;
use mkt_signal::factor_pub::trade_notional_kll::{
    load_merged_hourly_kll, order_size_thresholds, utc_month_bounds,
};
use serde::Serialize;

const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:18123";
const DEFAULT_DATABASE: &str = "baseline";
const DEFAULT_VENUE: &str = "binance-futures";

#[derive(Parser, Debug)]
#[command(name = "trade_notional_kll_aggregate")]
#[command(about = "Merge hourly trade-notional KLL sketches from ClickHouse")]
struct Args {
    /// Market symbol stored in the hourly KLL table.
    #[arg(long)]
    symbol: String,
    /// UTC natural month to merge, in YYYY-MM format.
    #[arg(long)]
    month: Option<String>,
    /// Inclusive UTC Unix timestamp in milliseconds. Use together with --end-ms.
    #[arg(long)]
    start_ms: Option<i64>,
    /// Exclusive UTC Unix timestamp in milliseconds. Use together with --start-ms.
    #[arg(long)]
    end_ms: Option<i64>,
    /// Quantile used as the small/medium boundary.
    #[arg(long, default_value_t = 0.5)]
    medium_quantile: f32,
    /// Quantile used as the medium/large boundary.
    #[arg(long, default_value_t = 0.9)]
    large_quantile: f32,
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

#[derive(Serialize)]
struct AggregateOutput {
    symbol: String,
    venue: u8,
    source_month: Option<String>,
    start_ms: i64,
    end_ms: i64,
    source_hourly_rows: usize,
    first_hour_start_ms: i64,
    last_hour_start_ms: i64,
    sample_count: usize,
    level_capacity: usize,
    medium_quantile: f32,
    large_quantile: f32,
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
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
    let (start_ms, end_ms) = resolve_time_range(args)?;
    let table = args
        .table
        .clone()
        .unwrap_or_else(|| default_table_name(&args.venue));
    let merged = load_merged_hourly_kll(
        &args.clickhouse_url,
        &args.database,
        &table,
        &args.symbol,
        start_ms,
        end_ms,
    )?
    .with_context(|| {
        format!(
            "no hourly KLL rows found for symbol={} in requested range",
            args.symbol
        )
    })?;
    let (medium_notional_threshold, large_notional_threshold) =
        order_size_thresholds(&merged.sketch, args.medium_quantile, args.large_quantile)?;

    Ok(AggregateOutput {
        symbol: merged.symbol,
        venue: merged.venue,
        source_month: args.month.clone(),
        start_ms: merged.start_ms,
        end_ms: merged.end_ms,
        source_hourly_rows: merged.source_hourly_rows,
        first_hour_start_ms: merged.first_hour_start_ms,
        last_hour_start_ms: merged.last_hour_start_ms,
        sample_count: merged.sketch.sample_count,
        level_capacity: merged.sketch.level_capacity,
        medium_quantile: args.medium_quantile,
        large_quantile: args.large_quantile,
        medium_notional_threshold,
        large_notional_threshold,
        levels: merged.sketch.levels,
    })
}

fn resolve_time_range(args: &Args) -> Result<(i64, i64)> {
    match (&args.month, args.start_ms, args.end_ms) {
        (Some(month), None, None) => utc_month_bounds(month),
        (None, Some(start_ms), Some(end_ms)) => {
            if start_ms >= end_ms {
                bail!("start_ms must be before end_ms");
            }
            Ok((start_ms, end_ms))
        }
        (Some(_), _, _) => bail!("--month cannot be combined with --start-ms or --end-ms"),
        (None, _, _) => bail!("provide either --month or both --start-ms and --end-ms"),
    }
}

fn default_table_name(venue: &str) -> String {
    format!("trade_notional_kll_{}_hourly", venue.replace('-', "_"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse_args(extra: &[&str]) -> Args {
        let mut args = vec!["trade_notional_kll_aggregate", "--symbol", "BTCUSDT"];
        args.extend_from_slice(extra);
        Args::parse_from(args)
    }

    #[test]
    fn resolves_utc_natural_month() {
        let args = parse_args(&["--month", "2024-02"]);
        let (start_ms, end_ms) = resolve_time_range(&args).unwrap();
        assert_eq!(end_ms - start_ms, 29 * 24 * 3_600_000);
    }

    #[test]
    fn preserves_explicit_millisecond_range() {
        let args = parse_args(&["--start-ms", "0", "--end-ms", "3600000"]);
        assert_eq!(resolve_time_range(&args).unwrap(), (0, 3_600_000));
    }

    #[test]
    fn rejects_mixed_or_incomplete_ranges() {
        assert!(
            resolve_time_range(&parse_args(&["--month", "2026-02", "--start-ms", "0",])).is_err()
        );
        assert!(resolve_time_range(&parse_args(&["--start-ms", "0"])).is_err());
    }

    #[test]
    fn derives_hourly_table_from_venue() {
        assert_eq!(
            default_table_name("binance-futures"),
            "trade_notional_kll_binance_futures_hourly"
        );
    }
}
