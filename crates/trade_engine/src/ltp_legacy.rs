//! Explicit, bounded one-time migration. Live monitoring never opens these files.
use crate::{
    ltp_account::parse_account_push,
    ltp_delivery::Delivery,
    ltp_outbox::{Fact, Identity, LegacyCursor},
};
use anyhow::{ensure, Context, Result};
use persist_common::{
    rapidx_execution::{ExecutionEvidence, ExecutionRecord},
    rapidx_role::ExecutionRoleRecord,
    rapidx_statement::StatementRecord,
};
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::Path,
    time::Duration,
};

pub async fn import(dir: &Path, delivery: &mut Delivery) -> Result<()> {
    let paths = paths(dir)?;
    let portfolio = delivery.store.state().portfolio.clone();
    let exchange = delivery.store.state().exchange.clone();
    let mut count = 0u64;
    for path in paths {
        let path = std::fs::canonicalize(path)?;
        let mut file = File::open(&path)?;
        let metadata = file.metadata()?;
        let old = delivery
            .store
            .state()
            .legacy_cursors
            .get(&path)
            .filter(|c| {
                c.device == metadata.dev()
                    && c.inode == metadata.ino()
                    && c.offset <= metadata.len()
            });
        let mut offset = old.map_or(0, |c| c.offset);
        file.seek(SeekFrom::Start(offset))?;
        // Pin this pass. A first pass can run beside the old monitor; the final
        // pass after stopping it reads only the appended tail.
        let mut reader = BufReader::new(file.take(metadata.len() - offset));
        let mut facts = vec![];
        let mut identities = vec![];
        let mut end: Option<i64> = None;
        let mut since_cursor = 0usize;
        loop {
            let Some((n, row)) = read_row(&mut reader)? else {
                break;
            };
            let (new_facts, new_identities, new_end) = decode(&row, &portfolio, &exchange)?;
            facts.extend(new_facts);
            identities.extend(new_identities);
            if let Some(new_end) = new_end {
                end = Some(end.map_or(new_end, |old| old.max(new_end)));
            }
            count += 1;
            offset += n as u64;
            since_cursor += n;
            if facts.len() >= 128 || identities.len() >= 256 || since_cursor >= 16 * 1024 * 1024 {
                flush(delivery, &mut facts, &mut identities, end.take()).await?;
                delivery.poll().await?;
            }
            if since_cursor >= 16 * 1024 * 1024 {
                delivery
                    .store
                    .import_cursor(
                        path.clone(),
                        LegacyCursor {
                            device: metadata.dev(),
                            inode: metadata.ino(),
                            offset,
                        },
                    )
                    .await?;
                since_cursor = 0;
                log::info!(
                    "RapidX legacy import rows={count} offset={offset} observed_bytes={}",
                    metadata.len()
                );
                tokio::task::yield_now().await;
            }
        }
        flush(delivery, &mut facts, &mut identities, end).await?;
        delivery
            .store
            .import_cursor(
                path,
                LegacyCursor {
                    device: metadata.dev(),
                    inode: metadata.ino(),
                    offset,
                },
            )
            .await?;
        log::info!(
            "RapidX legacy import scanned rows={count} pending={}",
            delivery.store.state().pending.len()
        );
    }
    Ok(())
}

fn paths(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut paths = std::fs::read_dir(dir)?
        .map(|row| row.map(|e| e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|p| p.extension().is_some_and(|v| v == "jsonl"));
    paths.sort();
    ensure!(!paths.is_empty(), "no legacy RapidX journals found");
    Ok(paths)
}

fn read_row(reader: &mut impl BufRead) -> Result<Option<(usize, Value)>> {
    let mut line = Vec::new();
    let n = reader
        .by_ref()
        .take(8 * 1024 * 1024 + 1)
        .read_until(b'\n', &mut line)?;
    ensure!(n <= 8 * 1024 * 1024, "legacy journal line exceeds limit");
    if n == 0 || !line.ends_with(b"\n") {
        return Ok(None);
    }
    Ok(Some((
        n,
        serde_json::from_slice(&line).context("corrupt complete legacy journal row")?,
    )))
}

/// Full read-only preflight, without an IPC publisher, outbox or exchange request.
pub fn validate(dir: &Path, portfolio: &str, exchange: &str) -> Result<()> {
    let (mut rows, mut executions, mut roles, mut statements, mut identities) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    for path in paths(dir)? {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let mut reader = BufReader::new(file.take(length));
        while let Some((_, row)) = read_row(&mut reader)? {
            let (facts, ids, _) = decode(&row, portfolio, exchange)?;
            rows += 1;
            identities += ids.len() as u64;
            for fact in facts {
                match fact {
                    Fact::Execution(_) => executions += 1,
                    Fact::Role(_) => roles += 1,
                    Fact::Statement(_) => statements += 1,
                }
            }
        }
    }
    log::info!("RapidX legacy validation rows={rows} executions={executions} roles={roles} statements={statements} identity_observations={identities}");
    Ok(())
}

async fn flush(
    delivery: &mut Delivery,
    facts: &mut Vec<Fact>,
    identities: &mut Vec<Identity>,
    end: Option<i64>,
) -> Result<()> {
    let end = end.map(|e| {
        delivery
            .store
            .state()
            .statement_history_end_ms
            .map_or(e, |old| old.max(e))
    });
    tokio::time::timeout(
        Duration::from_secs(120),
        delivery.save(std::mem::take(facts), std::mem::take(identities), end),
    )
    .await
    .context("legacy import persistence stalled; retry from retained archives")??;
    Ok(())
}

fn decode(
    row: &Value,
    portfolio: &str,
    exchange: &str,
) -> Result<(Vec<Fact>, Vec<Identity>, Option<i64>)> {
    ensure!(row.is_object(), "legacy journal row is not an object");
    let mut facts = Vec::new();
    let mut identities = Vec::new();
    if let Some(evidence) = row.get("execution") {
        scope(row, portfolio, exchange)?;
        let execution: ExecutionEvidence = serde_json::from_value(evidence.clone())?;
        facts.push(Fact::Execution(ExecutionRecord {
            portfolio: portfolio.into(),
            exchange: exchange.into(),
            execution,
        }));
    }
    if let Some(statement) = row
        .get("statement")
        .or_else(|| row.get("statement_balance_reobservation"))
    {
        scope(row, portfolio, exchange)?;
        let statement: StatementRecord = serde_json::from_value(statement.clone())?;
        ensure!(
            statement.portfolio == portfolio && statement.exchange == exchange,
            "legacy statement scope mismatch"
        );
        facts.push(Fact::Statement(statement));
    }
    if let Some(message) = row.get("message") {
        match message["channel"].as_str() {
            Some("Trades" | "HistoricalExecution") => {
                let data = &message["data"];
                // The old raw logger ran before venue filtering.
                if data["portfolioId"] == portfolio && data["exchangeType"] == exchange {
                    let record = ExecutionRecord {
                        portfolio: portfolio.into(),
                        exchange: exchange.into(),
                        execution: ExecutionEvidence::parse(
                            data,
                            portfolio,
                            exchange,
                            message["channel"] == "HistoricalExecution",
                        )?,
                    };
                    facts.push(Fact::Execution(record.clone()));
                    if let Some(role) = ExecutionRoleRecord::from_row(record, data)? {
                        facts.push(Fact::Role(role));
                    }
                }
            }
            Some("Assets" | "Positions") => {
                for event in parse_account_push(&message.to_string(), portfolio, exchange)? {
                    if let Some(identity) = Identity::from_event(&event)? {
                        identities.push(identity);
                    }
                }
            }
            _ => {}
        }
    }
    let end = row
        .get("statement_history_end_ms")
        .map(|end| -> Result<i64> {
            scope(row, portfolio, exchange)?;
            end.as_i64()
                .filter(|end| *end > 0)
                .context("invalid legacy statement checkpoint")
        })
        .transpose()?;
    for fact in &facts {
        fact.validate()?;
    }
    Ok((facts, identities, end))
}
fn scope(row: &Value, portfolio: &str, exchange: &str) -> Result<()> {
    ensure!(
        row["portfolio"] == portfolio && row["exchange"] == exchange,
        "legacy journal scope mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn trade() -> Value {
        serde_json::json!({"message":{"channel":"Trades","data":{"portfolioId":"123","exchangeType":"BINANCE",
            "businessType":"PERP","sym":"BINANCE_PERP_BTC_USDT","transactionId":"tx","orderId":"o",
            "clientOrderId":"7","side":"BUY","quantity":"1","price":"2","createAt":"1000",
            "rpnl":"0","tradingFee":"-0.001","tradingFeeCoin":"USDT","execType":"MAKER"}}})
    }

    #[test]
    fn legacy_import_keeps_validated_role_signed_fee_and_scope_without_inventing_fills() {
        let (facts, identities, end) = decode(&trade(), "123", "BINANCE").unwrap();
        assert_eq!(facts.len(), 2);
        assert!(identities.is_empty() && end.is_none());
        let Fact::Execution(execution) = &facts[0] else {
            panic!("missing execution");
        };
        assert_eq!(execution.execution.signed_fee.as_deref(), Some("-0.001"));
        let Fact::Role(role) = &facts[1] else {
            panic!("missing role");
        };
        assert_eq!(&role.record, execution);
        assert_eq!(role.role, persist_common::rapidx_role::ExecutionRole::Maker);
        assert!(decode(&trade(), "456", "BINANCE").unwrap().0.is_empty());
        let mut unknown = trade();
        unknown["message"]["data"]["execType"] = "UNKNOWN".into();
        assert_eq!(decode(&unknown, "123", "BINANCE").unwrap().0.len(), 1);
        let mut corrupt = trade();
        corrupt["message"]["data"]["quantity"] = "NaN".into();
        assert!(decode(&corrupt, "123", "BINANCE").is_err());
        let explicit = serde_json::json!({"portfolio":"456","exchange":"BINANCE","execution":execution.execution});
        assert!(decode(&explicit, "123", "BINANCE").is_err());
    }

    #[test]
    fn read_only_preflight_preserves_archives_and_only_ignores_unterminated_tail() {
        let dir = std::env::temp_dir().join(format!("rapidx-preflight-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("archive.jsonl");
        let bytes = format!("{}\n{{\"torn\":", trade());
        std::fs::write(&path, &bytes).unwrap();
        validate(&dir, "123", "BINANCE").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), bytes);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert!(validate(&dir, "123", "BINANCE").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
