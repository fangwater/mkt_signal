use anyhow::{ensure, Context, Result};
use clap::{Parser, ValueEnum};
use persist_common::rapidx_execution::ExecutionRecord;
use persist_manager::rapidx_execution::CF_RAPIDX_EXECUTION;
use persist_manager::rapidx_reconcile::Reconciler;
use persist_manager::{RocksDbStore, DEFAULT_DB_PATH};
use std::io::{BufRead, BufReader, Read};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

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
    /// Merge observations into unique executions and per-order fee totals.
    #[arg(long)]
    reconcile: bool,
    /// Return success despite unallocatable REST-only cumulative rebates.
    #[arg(long, requires = "reconcile")]
    allow_incomplete_fees: bool,
    /// Bound scoped execution identities retained while reconciling.
    #[arg(long, default_value_t = 100_000)]
    max_executions: usize,
    /// Read liquidation-order evidence from account journal JSONL files.
    #[arg(long, requires = "reconcile")]
    account_journal: Vec<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
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
    export(&args, BufWriter::new(std::io::stdout().lock()))
}

fn export(args: &Args, mut output: impl Write) -> Result<()> {
    ensure!(
        !args.reconcile || args.observation == Observation::All,
        "--reconcile requires --observation all to detect cross-source conflicts"
    );
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
    let mut count = 0_u64;
    let mut reconciler = args
        .reconcile
        .then(|| Reconciler::new(args.max_executions))
        .transpose()?;
    if let Some(reconciler) = &mut reconciler {
        for path in &args.account_journal {
            read_liquidations(path, reconciler, &args.portfolio, &args.exchange)?;
        }
    }
    // Stable keys are 32 bytes; this exclusive sentinel includes the maximal key.
    store.scan_range_batches(&cf, b"", &[0xff; 33], 512, |rows| {
        for (key, value) in rows {
            let record: ExecutionRecord =
                serde_json::from_slice(&value).context("decode stored RapidX execution")?;
            ensure!(
                key.as_slice() == record.stable_key()?,
                "stored execution identity mismatch"
            );
            if let Some(reconciler) = &mut reconciler {
                if record.portfolio == args.portfolio && record.exchange == args.exchange {
                    // Retain the counterpart even if its conflicting timestamp
                    // is outside the requested window. finish applies the window.
                    reconciler.insert(record)?;
                }
            } else if matches(&record, args)? {
                output.write_all(&record.to_json_bytes()?)?;
                output.write_all(b"\n")?;
                count += 1;
            }
        }
        Ok(())
    })?;
    if let Some(reconciler) = reconciler {
        let report = reconciler.finish(args.start_us, args.end_us)?;
        serde_json::to_writer(&mut output, &report)?;
        output.write_all(b"\n")?;
        output.flush()?;
        ensure!(
            report.unmatched_liquidations.is_empty(),
            "{} liquidation orders have no stored execution; report is incomplete",
            report.unmatched_liquidations.len()
        );
        ensure!(
            args.allow_incomplete_fees || report.unresolved_fee_executions == 0,
            "{} executions have unresolved cumulative rebates; report is incomplete",
            report.unresolved_fee_executions
        );
        eprintln!("Reconciled {} unique executions; quantities are not strategy positions and reported PNL is not net PNL.", report.execution_count);
        return Ok(());
    }
    output.flush()?;
    eprintln!("Exported {count} execution observations; WS and REST are not additive.");
    Ok(())
}

fn read_liquidations(
    path: &std::path::Path,
    reconciler: &mut Reconciler,
    portfolio: &str,
    exchange: &str,
) -> Result<()> {
    let mut reader = BufReader::new(
        std::fs::File::open(path)
            .with_context(|| format!("open account journal {}", path.display()))?,
    );
    let mut line = Vec::new();
    const MAX_LINE: usize = 8 * 1024 * 1024;
    loop {
        line.clear();
        let count = reader
            .by_ref()
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut line)?;
        ensure!(count <= MAX_LINE, "account journal line exceeds 8 MiB");
        if count == 0 {
            break;
        }
        ensure!(line.last() == Some(&b'\n'),
            "unfinished account journal tail; retry after the writer completes or use a closed snapshot");
        let row: serde_json::Value =
            serde_json::from_slice(&line).context("invalid complete journal line")?;
        ensure!(row.is_object(), "journal record must be object");
        if let Some(message) = row.get("message") {
            reconciler.observe_liquidation(message, portfolio, exchange)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist_common::rapidx_execution::ExecutionEvidence;
    use serde_json::json;

    fn stored_record(rest: bool) -> ExecutionRecord {
        ExecutionRecord {
            portfolio: "123".into(), exchange: "OKX".into(),
            execution: ExecutionEvidence::parse(&json!({
                "portfolioId":"123", "exchangeType":"OKX", "businessType":"SPOT",
                "sym":"OKX_SPOT_BTC_USDT", "transactionId":"external-fill", "orderId":"external-order",
                "clientOrderId":"", "side":"BUY", "quantity":"0.1", "price":"10",
                "createAt":"1000", "rpnl":"0", "tradingFee":"-0.01", "tradingFeeCoin":"USDT",
                "fee":"0", "feeCoin":"", "rebate":"0.25", "rebateCoin":"USDT"
            }), "123", "OKX", rest).unwrap(),
        }
    }

    fn database(records: &[ExecutionRecord]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rapidx-reconcile-export-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store =
            RocksDbStore::open(path.to_str().unwrap(), &[CF_RAPIDX_EXECUTION], true).unwrap();
        for record in records {
            store
                .put(
                    CF_RAPIDX_EXECUTION,
                    &record.stable_key().unwrap(),
                    &record.to_json_bytes().unwrap(),
                )
                .unwrap();
        }
        drop(store);
        path
    }

    fn reconciliation_args(path: &std::path::Path) -> Args {
        Args::parse_from([
            "export",
            "--db-path",
            path.to_str().unwrap(),
            "--portfolio",
            "123",
            "--exchange",
            "OKX",
            "--start-us",
            "1000000",
            "--end-us",
            "1001000",
            "--reconcile",
        ])
    }

    #[test]
    fn persisted_ws_and_rest_export_one_execution_and_exact_rebate() {
        let path = database(&[stored_record(false), stored_record(true)]);
        let args = reconciliation_args(&path);
        let mut output = Vec::new();
        export(&args, &mut output).unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(report["execution_count"], 1);
        assert_eq!(report["orders"][0]["quantity"], "0.1");
        assert_eq!(report["orders"][0]["fees"]["USDT"]["rebated"], "0.01");
        assert_eq!(report["executions"][0]["rest_reported_rebate"], "0.25");
        assert_eq!(report["strategy_attribution"], "unassigned");
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn unresolved_rebate_writes_report_but_fails_without_explicit_override() {
        let path = database(&[stored_record(true)]);
        let mut args = reconciliation_args(&path);
        let mut output = Vec::new();
        assert!(export(&args, &mut output).is_err());
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(report["unresolved_fee_executions"], 1);
        assert_eq!(report["orders"][0]["fees_complete"], false);
        args.allow_incomplete_fees = true;
        export(&args, &mut Vec::new()).unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn conflicting_counterpart_outside_window_prevents_report_output() {
        let mut rest = stored_record(true);
        rest.execution.timestamp_ms = 1001;
        let path = database(&[stored_record(false), rest]);
        let mut args = reconciliation_args(&path);
        args.allow_incomplete_fees = true;
        let mut output = Vec::new();
        assert!(export(&args, &mut output).is_err());
        assert!(output.is_empty());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn account_journal_links_liquidation_and_rejects_complete_corruption() {
        let mut fill = stored_record(false);
        fill.execution.symbol = "OKX_PERP_BTC_USDT".into();
        fill.execution.signed_fee = Some("0.01".into());
        let path = database(&[fill]);
        let journal_path = path.join("account.jsonl");
        let message = json!({"message":{"channel":"LiquidationPosition", "data":{
            "portfolioId":"123", "exchangeType":"OKX", "orderId":"external-order",
            "liquidationEventId":"liq-1", "positionId":"pos-1", "sym":"OKX_PERP_BTC_USDT",
            "positionSide":"SHORT", "reduceQty":"0.1", "closedPnl":"-3", "liqFee":"0.5",
            "totalTradingFee":"0.01", "createAt":900, "updateAt":1100
        }}});
        let mut bytes = serde_json::to_vec(&message).unwrap();
        bytes.push(b'\n');
        std::fs::write(&journal_path, &bytes).unwrap();
        let mut args = reconciliation_args(&path);
        args.account_journal.push(journal_path.clone());
        let mut output = Vec::new();
        export(&args, &mut output).unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(
            report["orders"][0]["forced_close_reason"],
            "exchange_forced_close:liquidation"
        );
        assert_eq!(
            report["orders"][0]["liquidation"]["liquidation_fee_usdt"],
            "0.5"
        );
        assert_eq!(report["orders"][0]["fees"]["USDT"]["charged"], "0.01");
        bytes.extend_from_slice(b"{\"torn\":");
        std::fs::write(&journal_path, &bytes).unwrap();
        output.clear();
        assert!(export(&args, &mut output).is_err());
        assert!(output.is_empty());
        bytes.push(b'\n');
        std::fs::write(&journal_path, &bytes).unwrap();
        assert!(export(&args, &mut output).is_err());
        assert!(output.is_empty());
        std::fs::remove_dir_all(path).unwrap();
    }

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
