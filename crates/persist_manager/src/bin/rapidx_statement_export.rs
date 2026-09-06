use anyhow::{ensure, Context, Result};
use clap::Parser;
use persist_common::rapidx_statement::StatementRecord;
use persist_manager::rapidx_execution::CF_RAPIDX_STATEMENT;
use persist_manager::rapidx_statement::StatementLedger;
use persist_manager::{RocksDbStore, DEFAULT_DB_PATH};
use std::io::{BufWriter, Write};

/// Read-only ledger report; settlement amounts and debt changes are not net PNL.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = DEFAULT_DB_PATH)]
    db_path: String,
    #[arg(long)]
    source_id: Option<String>,
    #[arg(long)]
    portfolio: String,
    #[arg(long, value_parser = ["BINANCE", "OKX"])]
    exchange: String,
    #[arg(long)]
    start_us: i64,
    #[arg(long)]
    end_us: i64,
    #[arg(long, default_value_t = 100_000)]
    max_statements: usize,
}

fn export(args: &Args, mut output: impl Write) -> Result<()> {
    ensure!(
        !args.portfolio.is_empty()
            && args.portfolio.len() <= 64
            && args.portfolio.bytes().all(|b| b.is_ascii_digit()),
        "invalid portfolio identity"
    );
    let mut ledger = StatementLedger::new(args.start_us, args.end_us, args.max_statements)?;
    let cf = match &args.source_id {
        Some(source) => {
            ensure!(!source.is_empty(), "empty sync source ID");
            persist_manager::sync::center_source_cf_name(source, CF_RAPIDX_STATEMENT)
        }
        None => CF_RAPIDX_STATEMENT.into(),
    };
    let store = RocksDbStore::open_read_only(&args.db_path, &[&cf])?;
    store.scan_range_batches(&cf, b"", &[0xff; 33], 512, |rows| {
        for (key, value) in rows {
            let row: StatementRecord =
                serde_json::from_slice(&value).context("decode persisted statement")?;
            ensure!(
                key.as_slice() == row.stable_key()?,
                "stored statement key mismatch"
            );
            if row.portfolio == args.portfolio && row.exchange == args.exchange {
                ledger.insert(row)?;
            }
        }
        Ok(())
    })?;
    let report = ledger.finish()?;
    serde_json::to_writer(&mut output, &report)?;
    output.write_all(b"\n")?;
    output.flush()?;
    eprintln!(
        "Exported {} statements; reported settlement and debt changes are separate facts.",
        report.statement_count
    );
    Ok(())
}

fn main() -> Result<()> {
    export(&Args::parse(), BufWriter::new(std::io::stdout().lock()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_export_filters_scope_and_window_and_rejects_corruption() {
        let path = std::env::temp_dir().join(format!(
            "rapidx_statement_export_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store =
            RocksDbStore::open(path.to_str().unwrap(), &[CF_RAPIDX_STATEMENT], false).unwrap();
        let row = StatementRecord::parse(
            &serde_json::json!({
                "portfolioId":123,"exchangeType":"BINANCE","statementId":"s1","requestId":"",
                "coin":"USDT","sym":"","statementType":"DEDUCT_INTEREST","businessType":"SPOT",
                "createAt":1000,"beforeAvailable":"0","afterAvailable":"0","beforeOverdraw":"0",
                "afterOverdraw":"0.01","beforeBorrow":"0","afterBorrow":"0","deltaAmount":"0"
            }),
            "123",
            "BINANCE",
        )
        .unwrap();
        for (id, portfolio, timestamp_us) in [
            ("s1", "123", 1_000_000),
            ("s2", "456", 1_000_000),
            ("s3", "123", 2_000_000),
        ] {
            let mut item = row.clone();
            item.statement_id = id.into();
            item.portfolio = portfolio.into();
            item.timestamp_us = timestamp_us;
            store
                .put(
                    CF_RAPIDX_STATEMENT,
                    &item.stable_key().unwrap(),
                    &item.to_json_bytes().unwrap(),
                )
                .unwrap();
        }
        drop(store);
        let args = Args {
            db_path: path.to_str().unwrap().into(),
            source_id: None,
            portfolio: "123".into(),
            exchange: "BINANCE".into(),
            start_us: 1_000_000,
            end_us: 2_000_000,
            max_statements: 10,
        };
        let mut output = Vec::new();
        export(&args, &mut output).unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(report["statement_count"], 1);
        assert_eq!(report["totals"][0]["reported_settlement_amount"], "0");
        assert_eq!(report["totals"][0]["overdraw_change"], "0.01");
        let store =
            RocksDbStore::open(path.to_str().unwrap(), &[CF_RAPIDX_STATEMENT], false).unwrap();
        assert_eq!(
            store
                .scan(CF_RAPIDX_STATEMENT, None, false, None)
                .unwrap()
                .len(),
            3
        );
        store
            .put(CF_RAPIDX_STATEMENT, &[0; 32], &row.to_json_bytes().unwrap())
            .unwrap();
        drop(store);
        output.clear();
        assert!(export(&args, &mut output).is_err());
        assert!(output.is_empty());
        std::fs::remove_dir_all(path).unwrap();
    }
}
