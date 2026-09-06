use anyhow::{ensure, Context, Result};
use clap::{Parser, ValueEnum};
use persist_common::rapidx_execution::ExecutionRecord;
use persist_manager::rapidx_execution::CF_RAPIDX_EXECUTION;
use persist_manager::{RocksDbStore, DEFAULT_DB_PATH};
use std::io::{BufWriter, Write};

/// Read-only JSONL export of execution observations, not cumulative order fills.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = DEFAULT_DB_PATH)]
    db_path: String,
    /// Select a source in a persist sync center; omit for a local source DB.
    #[arg(long)]
    source_id: Option<String>,
    #[arg(long)]
    portfolio: String,
    #[arg(long, value_parser = ["BINANCE", "OKX"])]
    exchange: String,
    /// Inclusive venue execution timestamp in microseconds.
    #[arg(long)]
    start_us: i64,
    /// Exclusive venue execution timestamp in microseconds.
    #[arg(long)]
    end_us: i64,
    /// WS and REST are separate observations; never sum both quantities/fees.
    #[arg(long, value_enum, default_value = "all")]
    observation: Observation,
}

#[derive(Clone, Copy, ValueEnum)]
enum Observation {
    All,
    Ws,
    Rest,
}

fn matches(record: &ExecutionRecord, args: &Args) -> Result<bool> {
    record.validate()?;
    let time_us = record
        .execution
        .timestamp_ms
        .checked_mul(1_000)
        .context("execution time overflows microseconds")?;
    Ok(record.portfolio == args.portfolio
        && record.exchange == args.exchange
        && time_us >= args.start_us
        && time_us < args.end_us
        && match args.observation {
            Observation::All => true,
            Observation::Ws => !record.execution.rest,
            Observation::Rest => record.execution.rest,
        })
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.start_us >= 0 && args.end_us > args.start_us,
        "invalid [start-us, end-us) window"
    );
    ensure!(
        !args.portfolio.is_empty()
            && args.portfolio.len() <= 64
            && args.portfolio.bytes().all(|b| b.is_ascii_digit()),
        "invalid portfolio identity"
    );
    let cf = match &args.source_id {
        Some(source) => {
            ensure!(!source.is_empty(), "empty sync source ID");
            persist_manager::sync::center_source_cf_name(source, CF_RAPIDX_EXECUTION)
        }
        None => CF_RAPIDX_EXECUTION.into(),
    };
    let store = RocksDbStore::open_read_only(&args.db_path, &[&cf])?;
    let mut output = BufWriter::new(std::io::stdout().lock());
    let mut count = 0_u64;
    // Stable keys are 32 bytes; this exclusive sentinel includes the maximal key.
    store.scan_range_batches(&cf, b"", &[0xff; 33], 512, |rows| {
        for (key, value) in rows {
            let record: ExecutionRecord =
                serde_json::from_slice(&value).context("decode stored RapidX execution")?;
            ensure!(
                key.as_slice() == record.stable_key()?,
                "stored execution identity mismatch"
            );
            if matches(&record, &args)? {
                output.write_all(&record.to_json_bytes()?)?;
                output.write_all(b"\n")?;
                count += 1;
            }
        }
        Ok(())
    })?;
    output.flush()?;
    eprintln!("Exported {count} execution observations; WS and REST are not additive.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist_common::rapidx_execution::ExecutionEvidence;
    use serde_json::json;

    #[test]
    fn export_filters_scope_source_and_half_open_microsecond_window() {
        let mut record = ExecutionRecord {
            portfolio: "123".into(), exchange: "OKX".into(),
            execution: ExecutionEvidence::parse(&json!({
                "portfolioId":"123", "exchangeType":"OKX", "businessType":"SPOT",
                "sym":"OKX_SPOT_BTC_USDT", "transactionId":"external-fill", "orderId":"external-order",
                "clientOrderId":"", "side":"BUY", "quantity":"1", "price":"10",
                "createAt":"1000", "rpnl":"0", "tradingFee":"-0.01", "tradingFeeCoin":"USDT"
            }), "123", "OKX", false).unwrap(),
        };
        let mut args = Args::parse_from([
            "export",
            "--portfolio",
            "123",
            "--exchange",
            "OKX",
            "--start-us",
            "1000000",
            "--end-us",
            "1001000",
        ]);
        assert!(matches(&record, &args).unwrap());
        args.observation = Observation::Rest;
        assert!(!matches(&record, &args).unwrap());
        args.observation = Observation::Ws;
        assert!(matches(&record, &args).unwrap());
        record.execution.timestamp_ms = 1001;
        assert!(!matches(&record, &args).unwrap());
        record.execution.timestamp_ms = 999;
        assert!(!matches(&record, &args).unwrap());
        record.execution.timestamp_ms = 1000;
        args.portfolio = "456".into();
        assert!(!matches(&record, &args).unwrap());
    }
}
