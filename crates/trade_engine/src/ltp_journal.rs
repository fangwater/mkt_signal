use anyhow::{ensure, Context, Result};
use persist_common::rapidx_execution::ExecutionRecord;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub use persist_common::rapidx_execution::ExecutionEvidence;

pub struct AccountJournal {
    file: File,
    portfolio: String,
    exchange: String,
    executions: HashMap<(String, bool), ExecutionEvidence>,
    delivery_pending: VecDeque<(String, bool)>,
    healthy: bool,
    pub history_end_ms: Option<i64>,
}

impl AccountJournal {
    pub fn open(
        dir: &Path,
        portfolio: &str,
        exchange: &str,
        mut recover_message: impl FnMut(&Value) -> Result<()>,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut files = std::fs::read_dir(dir)?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        files.retain(|path| path.extension().and_then(|s| s.to_str()) == Some("jsonl"));
        files.sort();
        let mut executions = HashMap::new();
        let mut history_end_ms: Option<i64> = None;
        for path in files {
            read_records(BufReader::new(File::open(&path)?), |record| {
                if let Some(message) = record.get("message") {
                    recover_message(message)?;
                }
                if let Some(evidence) = record.get("execution") {
                    ensure!(
                        record["portfolio"] == portfolio && record["exchange"] == exchange,
                        "journal execution scope mismatch"
                    );
                    let evidence: ExecutionEvidence = serde_json::from_value(evidence.clone())?;
                    let key = (evidence.transaction_id.clone(), evidence.rest);
                    if let Some(old) = executions.insert(key, evidence.clone()) {
                        ensure!(old == evidence, "conflicting execution evidence in journal");
                    }
                }
                if let Some(end) = record.get("history_end_ms") {
                    ensure!(
                        record["portfolio"] == portfolio && record["exchange"] == exchange,
                        "journal checkpoint scope mismatch"
                    );
                    let end = end
                        .as_i64()
                        .filter(|n| *n > 0)
                        .context("invalid history checkpoint")?;
                    history_end_ms = Some(history_end_ms.map_or(end, |old| old.max(end)));
                }
                Ok(())
            })
            .with_context(|| format!("recover RapidX journal {}", path.display()))?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join(format!(
                "{}-{}.jsonl",
                chrono::Utc::now().format("%Y%m%dT%H%M%S%.6f"),
                uuid::Uuid::new_v4()
            )))?;
        File::open(dir)?.sync_all()?;
        // ACKs are intentionally not used as a local deletion cursor. Replaying
        // every durable fact on restart also repairs a restarted downstream.
        let delivery_pending = executions.keys().cloned().collect();
        Ok(Self {
            file,
            portfolio: portfolio.into(),
            exchange: exchange.into(),
            executions,
            delivery_pending,
            healthy: true,
            history_end_ms,
        })
    }

    pub fn record_message(&mut self, message: &Value, source: &str) -> Result<()> {
        self.append(&json!({"received_us":chrono::Utc::now().timestamp_micros(), "source":source,"message":message}))?;
        self.sync()?;
        Ok(())
    }

    pub fn record_execution(&mut self, row: &Value, rest: bool) -> Result<bool> {
        let evidence = ExecutionEvidence::parse(row, &self.portfolio, &self.exchange, rest)?;
        let key = (evidence.transaction_id.clone(), rest);
        if let Some(old) = self.executions.get(&key) {
            ensure!(*old == evidence, "conflicting duplicate RapidX execution");
            return Ok(false);
        }
        self.append(
            &json!({"portfolio":self.portfolio,"exchange":self.exchange,"execution":evidence}),
        )?;
        self.sync()?;
        self.delivery_pending.push_back(key.clone());
        self.executions.insert(key, evidence);
        Ok(true)
    }

    pub fn complete_history(&mut self, end_ms: i64) -> Result<()> {
        ensure!(
            end_ms > 0 && self.history_end_ms.is_none_or(|old| end_ms >= old),
            "history checkpoint must not regress"
        );
        self.append(
            &json!({"portfolio":self.portfolio,"exchange":self.exchange,"history_end_ms":end_ms}),
        )?;
        self.sync()?;
        self.history_end_ms = Some(end_ms);
        Ok(())
    }

    pub fn record_history(&mut self, rows: &[Value]) -> Result<()> {
        // Validate the entire batch before writing. One sync covers its evidence;
        // the caller appends the checkpoint only after this returns successfully.
        let evidence = rows
            .iter()
            .map(|row| ExecutionEvidence::parse(row, &self.portfolio, &self.exchange, true))
            .collect::<Result<Vec<_>>>()?;
        let mut batch_ids = std::collections::HashSet::new();
        for item in &evidence {
            ensure!(
                batch_ids.insert(&item.transaction_id),
                "duplicate execution in history batch"
            );
            if let Some(old) = self.executions.get(&(item.transaction_id.clone(), true)) {
                ensure!(old == item, "conflicting historical RapidX execution");
            }
        }
        let mut added = Vec::new();
        for (row, item) in rows.iter().zip(evidence) {
            self.append(&json!({"received_us":chrono::Utc::now().timestamp_micros(), "source":"rest_history", "message":{"channel":"HistoricalExecution","data":row}}))?;
            let key = (item.transaction_id.clone(), true);
            if !self.executions.contains_key(&key) {
                self.append(
                    &json!({"portfolio":self.portfolio,"exchange":self.exchange,"execution":item}),
                )?;
                added.push(key.clone());
                self.executions.insert(key, item);
            }
        }
        self.sync()?;
        self.delivery_pending.extend(added);
        Ok(())
    }

    pub fn execution_delivery_pending(&self) -> usize {
        self.delivery_pending.len()
    }

    pub fn next_execution_record(&self) -> Result<Option<ExecutionRecord>> {
        self.ensure_healthy()?;
        self.delivery_pending
            .front()
            .map(|key| {
                let record = ExecutionRecord {
                    portfolio: self.portfolio.clone(),
                    exchange: self.exchange.clone(),
                    execution: self
                        .executions
                        .get(key)
                        .context("missing journal execution")?
                        .clone(),
                };
                record.validate()?;
                Ok(record)
            })
            .transpose()
    }

    /// Called only after the bounded sender has accepted this durable record.
    pub fn execution_record_enqueued(&mut self) {
        self.delivery_pending.pop_front();
    }

    fn append(&mut self, record: &Value) -> Result<()> {
        self.ensure_healthy()?;
        let mut bytes = serde_json::to_vec(record)?;
        bytes.push(b'\n');
        if let Err(error) = self.file.write_all(&bytes) {
            self.healthy = false;
            return Err(error)
                .context("write RapidX journal; restart required before further appends");
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        if let Err(error) = self.file.sync_data() {
            self.healthy = false;
            return Err(error).context("sync RapidX journal; durability unavailable");
        }
        Ok(())
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        ensure!(
            self.healthy,
            "RapidX journal is unavailable; refusing further account processing"
        );
        Ok(())
    }
}

/// Only a torn final line is recoverable. Interior corruption must never silently
/// discard account identities or advance a historical recovery checkpoint.
fn read_records(
    mut reader: impl BufRead,
    mut visit: impl FnMut(&Value) -> Result<()>,
) -> Result<()> {
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if line.last() != Some(&b'\n') {
            return Ok(());
        }
        let record: Value =
            serde_json::from_slice(&line).context("corrupt complete journal record")?;
        ensure!(record.is_object(), "journal record must be object");
        visit(&record)?;
    }
}

pub fn write_snapshot(path: &Path, snapshot: &impl Serialize) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer(&mut file, snapshot)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(path.parent().context("snapshot needs parent directory")?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn trade() -> Value {
        json!({"portfolioId":"123","exchangeType":"OKX","businessType":"SPOT","sym":"OKX_SPOT_BTC_USDT",
            "transactionId":"external_fill","orderId":"external_order","clientOrderId":"manual",
            "side":"BUY","quantity":"0.01","price":"50000","createAt":"1000","rpnl":"0",
            "fee":"0.00001","feeCoin":"BTC","rebate":"0.02","rebateCoin":"USDT",
            "tradingFee":"-0.02","tradingFeeCoin":"USDT"})
    }
    #[test]
    fn separate_fee_currencies_and_external_identity_are_preserved() {
        let parsed = ExecutionEvidence::parse(&trade(), "123", "OKX", true).unwrap();
        assert_eq!(parsed.client_order_id, "manual");
        assert_eq!(parsed.fee_currency.as_deref(), Some("BTC"));
        assert_eq!(parsed.rebate_currency.as_deref(), Some("USDT"));
        assert!(parsed.signed_fee.is_none());
        let ws = ExecutionEvidence::parse(&trade(), "123", "OKX", false).unwrap();
        assert_eq!(ws.signed_fee.as_deref(), Some("-0.02"));
        assert!(ws.fee.is_none());
    }
    #[test]
    fn malformed_execution_does_not_become_a_fill() {
        let mut row = trade();
        row["price"] = json!("NaN");
        assert!(ExecutionEvidence::parse(&row, "123", "OKX", true).is_err());
        assert!(ExecutionEvidence::parse(&trade(), "999", "OKX", true).is_err());
    }
    #[test]
    fn only_unterminated_tail_is_ignored() {
        let mut count = 0;
        read_records(&b"{\"ok\":1}\n{\"torn\":"[..], |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert!(read_records(&b"{\"ok\":1}\nINVALID\n"[..], |_| Ok(())).is_err());
    }
    #[test]
    fn checkpoint_and_duplicate_evidence_survive_restart() {
        let dir = std::env::temp_dir().join(format!("rapidx-journal-{}", uuid::Uuid::new_v4()));
        let mut journal = AccountJournal::open(&dir, "123", "OKX", |_| Ok(())).unwrap();
        assert!(journal.record_execution(&trade(), true).unwrap());
        assert_eq!(journal.execution_delivery_pending(), 1);
        let record = journal.next_execution_record().unwrap().unwrap();
        assert_eq!(record.execution.transaction_id, "external_fill");
        journal.execution_record_enqueued();
        assert!(journal.next_execution_record().unwrap().is_none());
        journal.complete_history(1000).unwrap();
        drop(journal);
        let mut recovered = AccountJournal::open(&dir, "123", "OKX", |_| Ok(())).unwrap();
        assert_eq!(recovered.history_end_ms, Some(1000));
        assert_eq!(recovered.execution_delivery_pending(), 1);
        assert_eq!(recovered.next_execution_record().unwrap().unwrap(), record);
        assert!(!recovered.record_execution(&trade(), true).unwrap());
        assert!(recovered.record_execution(&trade(), false).unwrap());
        assert_eq!(recovered.execution_delivery_pending(), 2);
        assert!(recovered.complete_history(999).is_err());
        drop(recovered);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn invalid_history_batch_cannot_advance_progress_or_write_partial_evidence() {
        let dir = std::env::temp_dir().join(format!("rapidx-batch-{}", uuid::Uuid::new_v4()));
        let mut journal = AccountJournal::open(&dir, "123", "OKX", |_| Ok(())).unwrap();
        let mut invalid = trade();
        invalid["quantity"] = json!("NaN");
        assert!(journal.record_history(&[trade(), invalid]).is_err());
        assert!(journal.executions.is_empty());
        assert_eq!(journal.execution_delivery_pending(), 0);
        assert!(journal.history_end_ms.is_none());
        assert!(journal.record_history(&[trade(), trade()]).is_err());
        assert!(journal.executions.is_empty());
        journal.record_history(&[trade()]).unwrap();
        assert_eq!(journal.executions.len(), 1);
        assert_eq!(journal.execution_delivery_pending(), 1);
        drop(journal);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_journal_write_poisoning_prevents_a_later_checkpoint() {
        let dir = std::env::temp_dir().join(format!("rapidx-full-{}", uuid::Uuid::new_v4()));
        let mut journal = AccountJournal::open(&dir, "123", "OKX", |_| Ok(())).unwrap();
        journal.file = OpenOptions::new().write(true).open("/dev/full").unwrap();
        assert!(journal
            .record_message(&json!({"channel":"Assets"}), "fixture")
            .is_err());
        assert!(journal.ensure_healthy().is_err());
        assert!(journal.complete_history(1000).is_err());
        assert!(journal.history_end_ms.is_none());
        drop(journal);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
