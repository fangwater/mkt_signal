//! Small recovery metadata and an ACK-pruned durable fact outbox.
//! No balances, account snapshots, raw pushes or acknowledged history live here.
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventMsg, BasicAccountEventType, BasicAccountScope,
    BasicBalanceMsg, BasicPositionMsg,
};
use persist_common::{
    rapidx_execution::ExecutionRecord, rapidx_fact::RapidXFact, rapidx_role::ExecutionRoleRecord,
    rapidx_statement::StatementRecord,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

pub const MAX_PENDING: usize = 4096;
const MAX_LINE: usize = 80 * 1024 * 1024;
const COMPACT_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Fact {
    Execution(ExecutionRecord),
    Role(ExecutionRoleRecord),
    Statement(StatementRecord),
}

impl Fact {
    pub fn validate(&self) -> Result<()> {
        self.key().map(|_| ())
    }
    pub fn ack(&self) -> Result<Vec<u8>> {
        Ok(match self {
            Self::Execution(r) => r.ack()?.to_vec(),
            Self::Role(r) => r.ack()?.to_vec(),
            Self::Statement(r) => r.ack()?.to_vec(),
        })
    }
    fn key(&self) -> Result<String> {
        let (channel, key) = match self {
            Self::Execution(r) => {
                r.to_ipc_payload()?;
                (ExecutionRecord::RECORD_CHANNEL, r.stable_key()?)
            }
            Self::Role(r) => {
                r.to_ipc_payload()?;
                (ExecutionRoleRecord::RECORD_CHANNEL, r.stable_key()?)
            }
            Self::Statement(r) => {
                r.to_ipc_payload()?;
                (StatementRecord::RECORD_CHANNEL, r.stable_key()?)
            }
        };
        let content = if matches!(self, Self::Statement(_)) {
            format!(":{}", hex::encode(self.ack()?))
        } else {
            String::new()
        };
        Ok(format!("{channel}:{}{content}", hex::encode(key)))
    }
    fn scope(&self) -> (&str, &str) {
        match self {
            Self::Execution(r) => (&r.portfolio, &r.exchange),
            Self::Role(r) => (&r.record.portfolio, &r.record.exchange),
            Self::Statement(r) => (&r.portfolio, &r.exchange),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Identity {
    kind: u32,
    scope: u32,
    symbol: String,
    side: char,
}
impl Identity {
    pub fn from_event(event: &[u8]) -> Result<Option<Self>> {
        let Some((kind, scope, data)) = split_basic_account_event(event) else {
            anyhow::bail!("invalid account identity event");
        };
        let (symbol, side) = match kind {
            BasicAccountEventType::BalanceUpdate => {
                (BasicBalanceMsg::from_bytes(data)?.symbol, '\0')
            }
            BasicAccountEventType::PositionUpdate => {
                let p = BasicPositionMsg::from_bytes(data)?;
                (p.inst_id, p.position_side)
            }
            _ => return Ok(None),
        };
        Ok(Some(Self {
            kind: kind as u32,
            scope: scope as u32,
            symbol,
            side,
        }))
    }
    pub fn event(&self) -> Result<Bytes> {
        ensure!(!self.symbol.is_empty(), "empty recovery identity");
        let scope = BasicAccountScope::from_u32(self.scope);
        ensure!(
            matches!(
                scope,
                BasicAccountScope::BinanceUnified | BasicAccountScope::OkexUnified
            ),
            "invalid recovery scope"
        );
        let (kind, bytes) = if self.kind == BasicAccountEventType::BalanceUpdate as u32 {
            (
                BasicAccountEventType::BalanceUpdate,
                BasicBalanceMsg::create(0, self.symbol.clone(), 0.0).to_bytes(),
            )
        } else {
            ensure!(
                self.kind == BasicAccountEventType::PositionUpdate as u32,
                "invalid recovery identity kind"
            );
            (
                BasicAccountEventType::PositionUpdate,
                BasicPositionMsg::create(0, self.symbol.clone(), self.side, 0.0).to_bytes(),
            )
        };
        Ok(BasicAccountEventMsg::create(kind, scope, bytes).to_bytes())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryState {
    pub portfolio: String,
    pub exchange: String,
    pub identities: BTreeSet<Identity>,
    pub statement_history_end_ms: Option<i64>,
    pub execution_history_end_ms: i64,
    pub pending: BTreeMap<String, Fact>,
    pub legacy_cursors: BTreeMap<PathBuf, LegacyCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyCursor {
    pub device: u64,
    pub inode: u64,
    pub offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum Record {
    State(RecoveryState),
    Change {
        facts: Vec<Fact>,
        identities: Vec<Identity>,
        statement_end: Option<i64>,
    },
    Ack(Vec<Vec<u8>>),
    ImportCursor(PathBuf, LegacyCursor),
    ExecutionCheckpoint(i64),
}

struct Disk {
    path: PathBuf,
    file: File,
    _lock: File,
    state: RecoveryState,
    retired: usize,
    appended_bytes: u64,
    healthy: bool,
}

/// All filesystem work runs on a blocking worker, never the current-thread WS reactor.
pub struct DeliveryStore {
    disk: Option<Disk>,
}
impl DeliveryStore {
    pub fn open(dir: &Path, portfolio: &str, exchange: &str) -> Result<Self> {
        ensure!(
            !portfolio.is_empty()
                && portfolio.bytes().all(|b| b.is_ascii_digit())
                && matches!(exchange, "BINANCE" | "OKX"),
            "invalid outbox scope"
        );
        let mut created = vec![];
        let mut parent = dir;
        while !parent.exists() {
            created.push(parent.to_owned());
            parent = parent
                .parent()
                .context("delivery directory has no existing ancestor")?;
        }
        std::fs::create_dir_all(dir)?;
        for child in created {
            File::open(child.parent().context("delivery parent missing")?)?.sync_all()?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(dir.join("delivery.lock"))?;
        // Prevent an importer and a live monitor from mutating the same outbox.
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "RapidX delivery state already in use"
        );
        let path = dir.join("delivery.wal");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .mode(0o600)
            .open(&path)?;
        let mut state = RecoveryState {
            portfolio: portfolio.into(),
            exchange: exchange.into(),
            identities: Default::default(),
            statement_history_end_ms: None,
            execution_history_end_ms: chrono::Utc::now().timestamp_millis(),
            pending: Default::default(),
            legacy_cursors: Default::default(),
        };
        let mut valid_len = 0;
        let mut reader = BufReader::new(file.try_clone()?);
        loop {
            let mut bytes = Vec::new();
            let n = reader
                .by_ref()
                .take(MAX_LINE as u64 + 1)
                .read_until(b'\n', &mut bytes)?;
            ensure!(n <= MAX_LINE, "oversized RapidX delivery record");
            if n == 0 || !bytes.ends_with(b"\n") {
                break;
            }
            apply(
                &mut state,
                &serde_json::from_slice(&bytes).context("corrupt RapidX delivery record")?,
            )?;
            valid_len += n as u64;
        }
        ensure!(
            state.portfolio == portfolio && state.exchange == exchange,
            "outbox scope mismatch"
        );
        validate(&state)?;
        file.set_len(valid_len)?;
        file.seek(std::io::SeekFrom::End(0))?;
        let mut disk = Disk {
            path,
            file,
            _lock: lock,
            state,
            retired: 0,
            appended_bytes: 0,
            healthy: true,
        };
        // Also seals a previously torn tail and bounds cold recovery work.
        disk.compact()?;
        Ok(Self { disk: Some(disk) })
    }
    pub fn state(&self) -> &RecoveryState {
        &self
            .disk
            .as_ref()
            .expect("delivery operation awaited")
            .state
    }
    pub fn has_capacity(&self) -> bool {
        self.state().pending.len() + 2 <= MAX_PENDING
    }
    pub fn ensure_healthy(&self) -> Result<()> {
        ensure!(
            self.disk.as_ref().is_some_and(|disk| disk.healthy),
            "RapidX delivery durability unavailable; restart required"
        );
        Ok(())
    }
    pub async fn change(
        &mut self,
        facts: Vec<Fact>,
        identities: Vec<Identity>,
        statement_end: Option<i64>,
    ) -> Result<()> {
        self.ensure_healthy()?;
        let s = self.state();
        let identities: Vec<_> = identities
            .into_iter()
            .filter(|i| !s.identities.contains(i))
            .collect();
        if facts.is_empty()
            && identities.is_empty()
            && statement_end.is_none_or(|end| s.statement_history_end_ms == Some(end))
        {
            return Ok(());
        }
        self.write(
            Record::Change {
                facts,
                identities,
                statement_end,
            },
            true,
        )
        .await
    }
    pub async fn acknowledge(&mut self, acks: Vec<Vec<u8>>) -> Result<()> {
        if acks.is_empty() {
            return Ok(());
        }
        // Losing a cleanup ACK on crash only causes safe idempotent replay.
        self.write(Record::Ack(acks), false).await
    }
    pub async fn execution_checkpoint(&mut self, end: i64) -> Result<()> {
        self.write(Record::ExecutionCheckpoint(end), true).await
    }
    pub async fn import_cursor(&mut self, path: PathBuf, cursor: LegacyCursor) -> Result<()> {
        self.write(Record::ImportCursor(path, cursor), true).await
    }
    async fn write(&mut self, record: Record, sync: bool) -> Result<()> {
        let mut disk = self.disk.take().context("delivery worker unavailable")?;
        let (disk, result) = tokio::task::spawn_blocking(move || {
            let result = disk.write(record, sync);
            (disk, result)
        })
        .await
        .context("RapidX delivery worker failed")?;
        self.disk = Some(disk);
        result
    }
}

fn validate(state: &RecoveryState) -> Result<()> {
    ensure!(
        state.execution_history_end_ms > 0
            && state.statement_history_end_ms.is_none_or(|end| end > 0)
            && state.legacy_cursors.len() <= 16384,
        "invalid RapidX recovery checkpoint"
    );
    ensure!(
        state.pending.len() <= MAX_PENDING && state.identities.len() <= 16384,
        "RapidX recovery capacity exceeded"
    );
    for identity in &state.identities {
        identity.event()?;
        ensure!(
            identity.scope == if state.exchange == "BINANCE" { 1 } else { 10 },
            "recovery identity exchange mismatch"
        );
    }
    for (key, fact) in &state.pending {
        ensure!(
            fact.scope() == (state.portfolio.as_str(), state.exchange.as_str())
                && *key == fact.key()?,
            "outbox fact scope/key mismatch"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist_common::rapidx_execution::ExecutionEvidence;

    fn dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rapidx-delivery-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        dir
    }
    fn fact(id: &str) -> Fact {
        let row = serde_json::json!({"portfolioId":"123","exchangeType":"BINANCE","businessType":"PERP",
            "sym":"BINANCE_PERP_BTC_USDT","transactionId":id,"orderId":"o","clientOrderId":"7",
            "side":"BUY","quantity":"1","price":"2","createAt":"1000","rpnl":"0","tradingFee":"0","tradingFeeCoin":""});
        Fact::Execution(ExecutionRecord {
            portfolio: "123".into(),
            exchange: "BINANCE".into(),
            execution: ExecutionEvidence::parse(&row, "123", "BINANCE", false).unwrap(),
        })
    }

    #[tokio::test]
    async fn exact_ack_crash_recovery_and_compaction_preserve_only_pending_facts() {
        let dir = dir();
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        assert!(DeliveryStore::open(&dir, "123", "BINANCE").is_err());
        let first = fact("first");
        let second = fact("second");
        store
            .change(vec![first.clone(), second.clone()], vec![], Some(1000))
            .await
            .unwrap();
        let mut wrong = first.ack().unwrap();
        wrong[63] ^= 1;
        store.acknowledge(vec![wrong]).await.unwrap();
        assert_eq!(store.state().pending.len(), 2);
        store.acknowledge(vec![first.ack().unwrap()]).await.unwrap();
        drop(store);
        let mut file = OpenOptions::new()
            .append(true)
            .open(dir.join("delivery.wal"))
            .unwrap();
        file.write_all(b"{\"Change\":").unwrap();
        drop(file);
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        assert_eq!(
            store.state().pending.values().cloned().collect::<Vec<_>>(),
            vec![second.clone()]
        );
        assert_eq!(store.state().statement_history_end_ms, Some(1000));
        let facts: Vec<_> = (0..128).map(|i| fact(&format!("tx-{i}"))).collect();
        store.change(facts.clone(), vec![], None).await.unwrap();
        store
            .acknowledge(facts.iter().map(|f| f.ack().unwrap()).collect())
            .await
            .unwrap();
        assert_eq!(store.disk.as_ref().unwrap().retired, 0);
        assert_eq!(store.state().pending.len(), 1);
        store
            .acknowledge(vec![second.ack().unwrap()])
            .await
            .unwrap();
        drop(store);
        let store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        assert!(store.state().pending.is_empty());
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn realtime_identity_updates_do_not_write_balances_or_repeat_disk_writes() {
        let dir = dir();
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        let identity = Identity {
            kind: 4002,
            scope: 1,
            symbol: "USDT".into(),
            side: '\0',
        };
        store
            .change(vec![], vec![identity.clone()], None)
            .await
            .unwrap();
        let before = std::fs::read(dir.join("delivery.wal")).unwrap();
        for _ in 0..1000 {
            store
                .change(vec![], vec![identity.clone()], None)
                .await
                .unwrap();
            store.acknowledge(vec![]).await.unwrap();
        }
        assert_eq!(std::fs::read(dir.join("delivery.wal")).unwrap(), before);
        let event = store
            .state()
            .identities
            .iter()
            .next()
            .unwrap()
            .event()
            .unwrap();
        let (_, _, body) = split_basic_account_event(&event).unwrap();
        let balance = BasicBalanceMsg::from_bytes(body).unwrap();
        assert_eq!((balance.timestamp, balance.wallet), (0, 0.0));
        assert!(!String::from_utf8(before).unwrap().contains("wallet"));
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn restart_metadata_clears_omitted_positions_and_borrow_without_restoring_values() {
        let dir = dir();
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        let scope = BasicAccountScope::BinanceUnified;
        let balance = BasicAccountEventMsg::create(
            BasicAccountEventType::BalanceUpdate,
            scope,
            BasicBalanceMsg::create(100, "BTC".into(), 2.0).to_bytes(),
        )
        .to_bytes();
        let position = BasicAccountEventMsg::create(
            BasicAccountEventType::PositionUpdate,
            scope,
            BasicPositionMsg::create(100, "BTCUSDT".into(), 'B', -3.0).to_bytes(),
        )
        .to_bytes();
        store
            .change(
                vec![],
                vec![
                    Identity::from_event(&balance).unwrap().unwrap(),
                    Identity::from_event(&position).unwrap().unwrap(),
                ],
                None,
            )
            .await
            .unwrap();
        drop(store);
        let store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        let mut assets = crate::ltp_snapshot::AccountSnapshotState::default();
        let mut positions = crate::ltp_account::PositionSnapshotState::default();
        for identity in &store.state().identities {
            let event = identity.event().unwrap();
            assets.observe(&event).unwrap();
            positions.reconcile(&mut vec![event], false, 0).unwrap();
        }
        let zeroes = assets
            .reconcile_assets_snapshot(scope, true, &[], 200)
            .unwrap();
        assert_eq!(zeroes.len(), 2);
        let mut zero_positions = vec![];
        positions.reconcile(&mut zero_positions, true, 200).unwrap();
        assert_eq!(zero_positions.len(), 2);
        let (_, _, body) = split_basic_account_event(&zero_positions[0]).unwrap();
        let position = BasicPositionMsg::from_bytes(body).unwrap();
        assert_eq!((position.timestamp, position.position_amount), (200, 0.0));
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn conflicting_scope_capacity_checkpoint_and_disk_failure_are_fail_closed() {
        let dir = dir();
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        let first = fact("first");
        store
            .change(vec![first.clone()], vec![], Some(1000))
            .await
            .unwrap();
        let before = std::fs::read(dir.join("delivery.wal")).unwrap();
        let mut bad = first.clone();
        if let Fact::Execution(record) = &mut bad {
            record.execution.price = "3".into();
        }
        assert!(store.change(vec![bad], vec![], None).await.is_err());
        let mut bad = fact("foreign");
        if let Fact::Execution(record) = &mut bad {
            record.portfolio = "456".into();
        }
        assert!(store.change(vec![bad], vec![], None).await.is_err());
        assert!(store.change(vec![], vec![], Some(999)).await.is_err());
        assert_eq!(std::fs::read(dir.join("delivery.wal")).unwrap(), before);
        store.disk.as_mut().unwrap().file = File::open(dir.join("delivery.wal")).unwrap();
        assert!(store
            .change(vec![fact("next")], vec![], None)
            .await
            .is_err());
        assert_eq!(store.state().pending.len(), 1);
        assert!(store
            .change(vec![fact("again")], vec![], None)
            .await
            .is_err());
        drop(store);
        let mut store = DeliveryStore::open(&dir, "123", "BINANCE").unwrap();
        let facts = (0..MAX_PENDING)
            .map(|i| fact(&format!("full-{i}")))
            .collect();
        assert!(store.change(facts, vec![], Some(2000)).await.is_err());
        assert_eq!(store.state().statement_history_end_ms, Some(1000));
        drop(store);
        assert!(DeliveryStore::open(&dir, "456", "BINANCE").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

fn apply(state: &mut RecoveryState, record: &Record) -> Result<()> {
    match record {
        Record::State(snapshot) => {
            ensure!(
                snapshot.portfolio == state.portfolio && snapshot.exchange == state.exchange,
                "recovery snapshot scope mismatch"
            );
            *state = snapshot.clone();
        }
        Record::Change {
            facts,
            identities,
            statement_end,
        } => {
            for fact in facts {
                let key = fact.key()?;
                if let Some(old) = state.pending.get(&key) {
                    ensure!(old == fact, "conflicting pending RapidX fact");
                } else {
                    state.pending.insert(key, fact.clone());
                }
            }
            state.identities.extend(identities.iter().cloned());
            if let Some(end) = statement_end {
                ensure!(
                    *end > 0 && state.statement_history_end_ms.is_none_or(|old| *end >= old),
                    "statement checkpoint regression"
                );
                state.statement_history_end_ms = Some(*end);
            }
        }
        Record::ExecutionCheckpoint(end) => {
            ensure!(
                *end >= state.execution_history_end_ms,
                "execution checkpoint regression"
            );
            state.execution_history_end_ms = *end;
        }
        Record::ImportCursor(path, cursor) => {
            state.legacy_cursors.insert(path.clone(), cursor.clone());
            ensure!(
                state.legacy_cursors.len() <= 16384,
                "legacy import cursor limit exceeded"
            );
        }
        Record::Ack(acks) => {
            let acks: BTreeSet<_> = acks.iter().collect();
            state
                .pending
                .retain(|_, fact| fact.ack().map_or(true, |ack| !acks.contains(&ack)));
        }
    }
    validate(state)
}

impl Disk {
    fn write(&mut self, record: Record, sync: bool) -> Result<()> {
        ensure!(
            self.healthy,
            "RapidX delivery durability unavailable; restart required"
        );
        let mut next = self.state.clone();
        apply(&mut next, &record)?;
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        ensure!(bytes.len() <= MAX_LINE, "delivery record too large");
        let result = (|| -> Result<()> {
            self.file.write_all(&bytes)?;
            if sync {
                self.file.sync_data()?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.healthy = false;
        }
        result.context("write RapidX pending facts")?;
        self.retired += self.state.pending.len().saturating_sub(next.pending.len());
        self.state = next;
        self.appended_bytes += bytes.len() as u64;
        if self.retired >= 128 || self.appended_bytes >= COMPACT_BYTES {
            if let Err(error) = self.compact() {
                self.healthy = false;
                return Err(error);
            }
        }
        Ok(())
    }
    fn compact(&mut self) -> Result<()> {
        let temp = self.path.with_extension("pending");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&temp)?;
        let mut bytes = serde_json::to_vec(&Record::State(self.state.clone()))?;
        bytes.push(b'\n');
        ensure!(bytes.len() <= MAX_LINE, "delivery snapshot too large");
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, &self.path)?;
        File::open(self.path.parent().context("delivery parent missing")?)?.sync_all()?;
        self.file = file;
        self.retired = 0;
        self.appended_bytes = 0;
        Ok(())
    }
}
