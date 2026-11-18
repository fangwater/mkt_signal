//! Exec-owned durable normalization. Manager never writes these stores.
use crate::storage::RocksDbStore;
use anyhow::{ensure, Context, Result};
use persist_common::{
    rapidx_execution::ExecutionRecord,
    rapidx_fact::RapidXFact,
    rapidx_role::ExecutionRoleRecord,
    uniform_fill::{
        self,
        normalize::{self, Lifecycle, MAX_ORDER_BYTES, MAX_ORDER_ROWS},
        NormalizedUniformMarker, StoredUniformOrder, NORMALIZED_UNIFORM_MARKER_KEY,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const STATE_CF: &str = "rapidx_uniform_state";
pub const EXECUTION_OBSERVATIONS_CF: &str = "rapidx_execution_observations";
const UNIFORM: &str = "uniform_orders";
const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct Output {
    physical: String,
    digest: [u8; 32],
}

fn tagged(tag: u8, rest: &[u8]) -> Vec<u8> {
    [vec![tag], rest.to_vec()].concat()
}
fn hash_order(row: &StoredUniformOrder, physical_key: &[u8]) -> Result<[u8; 32]> {
    if row.record.client_order_id <= 0
        || row.record.ttype == persist_common::UNIFORM_ORDER_TYPE_INTERNAL_CROSS
    {
        return Ok(Sha256::digest(physical_key).into());
    }
    Ok(Sha256::digest(serde_json::to_vec(&normalize::order_key(row))?).into())
}
fn execution_hash(record: &ExecutionRecord) -> Result<Option<[u8; 32]>> {
    // External/manual identities remain durable raw facts without invented CTA ownership.
    if record
        .execution
        .client_order_id
        .parse::<i64>()
        .ok()
        .is_none_or(|v| v <= 0)
    {
        return Ok(None);
    }
    Ok(Some(
        Sha256::digest(serde_json::to_vec(&normalize::execution_key(record)?)?).into(),
    ))
}
fn identity(store: &RocksDbStore) -> Result<String> {
    Ok(std::fs::read_to_string(store.path().join("IDENTITY"))?
        .trim()
        .to_owned())
}

pub fn marker(store: &RocksDbStore) -> Result<Option<NormalizedUniformMarker>> {
    let value = store.get("default", NORMALIZED_UNIFORM_MARKER_KEY)?;
    value
        .map(|value| {
            let m: NormalizedUniformMarker = serde_json::from_slice(&value)?;
            ensure!(
                m.db_identity == identity(store)?,
                "normalized uniform source identity changed"
            );
            ensure!(
                matches!(m.exchange.as_str(), "BINANCE" | "OKX") && !m.portfolio.is_empty(),
                "invalid uniform scope"
            );
            Ok(m)
        })
        .transpose()
}

fn required_marker(store: &RocksDbStore) -> Result<NormalizedUniformMarker> {
    let marker = marker(store)?.context("uniform normalization has not been initialized")?;
    ensure!(
        marker.complete,
        "explicit uniform history repair is incomplete"
    );
    Ok(marker)
}

fn load_group(store: &RocksDbStore, tag: u8, hash: &[u8; 32]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let start = tagged(tag, hash);
    let mut end = start.clone();
    let mut index = end.len();
    while index > 0 {
        index -= 1;
        if end[index] != 255 {
            end[index] += 1;
            end.truncate(index + 1);
            break;
        }
    }
    let rows = store.scan_range(STATE_CF, &start, &end, Some(MAX_ORDER_ROWS + 1))?;
    ensure!(
        rows.len() <= MAX_ORDER_ROWS
            && rows.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() <= MAX_ORDER_BYTES,
        "uniform order group exceeds bounded projection size"
    );
    Ok(rows)
}

fn allocation_key(store: &RocksDbStore) -> Result<i64> {
    let cursor = store
        .get(STATE_CF, b"last_key")?
        .map(|b| String::from_utf8(b))
        .transpose()?
        .map(|s| s.parse::<i64>())
        .transpose()?
        .unwrap_or(0);
    let head = store
        .scan(UNIFORM, None, true, Some(1))?
        .into_iter()
        .next()
        .map(|(k, _)| String::from_utf8(k))
        .transpose()?
        .map(|s| s.parse::<i64>())
        .transpose()?
        .unwrap_or(0);
    Ok(cursor.max(head))
}

fn scope(m: &NormalizedUniformMarker, execution: &ExecutionRecord) -> Result<()> {
    execution.validate()?;
    ensure!(
        execution.portfolio == m.portfolio && execution.exchange == m.exchange,
        "RapidX uniform fact belongs to another portfolio or exchange"
    );
    Ok(())
}

type Write = (String, Vec<u8>, Vec<u8>);

pub(crate) fn validate_fact<R: RapidXFact>(store: &RocksDbStore, record: &R) -> Result<()> {
    let Some(m) = marker(store)? else {
        return Ok(());
    };
    ensure!(m.complete, "uniform history repair is incomplete");
    match R::COLUMN_FAMILY {
        "rapidx_executions" => scope(
            &m,
            &serde_json::from_slice::<ExecutionRecord>(&record.to_json_bytes()?)?,
        ),
        "rapidx_execution_roles" => scope(
            &m,
            &serde_json::from_slice::<ExecutionRoleRecord>(&record.to_json_bytes()?)?.record,
        ),
        _ => Ok(()),
    }
}
fn any_group(store: &RocksDbStore, tag: u8, hash: &[u8; 32]) -> Result<bool> {
    let start = tagged(tag, hash);
    let mut end = start.clone();
    while end.last() == Some(&255) {
        end.pop();
    }
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    Ok(!store
        .scan_range(STATE_CF, &start, &end, Some(1))?
        .is_empty())
}
fn pending(store: &RocksDbStore, hash: &[u8; 32], writes: &mut Vec<Write>) -> Result<()> {
    if store.get(STATE_CF, &tagged(b'Q', hash))?.is_none() {
        let mut m = marker(store)?.context("missing uniform marker")?;
        m.pending_orders = m
            .pending_orders
            .checked_add(1)
            .context("uniform pending count overflow")?;
        writes.push((STATE_CF.into(), tagged(b'Q', hash), vec![]));
        writes.push((
            "default".into(),
            NORMALIZED_UNIFORM_MARKER_KEY.to_vec(),
            serde_json::to_vec(&m)?,
        ));
    }
    Ok(())
}

pub(crate) fn enqueue_fact<R: RapidXFact>(
    store: &RocksDbStore,
    record: &R,
    sync_enabled: bool,
) -> Result<()> {
    let Some(m) = marker(store)? else {
        return Ok(());
    };
    ensure!(m.complete, "uniform history repair is incomplete");
    ensure!(
        !sync_enabled,
        "uniform normalization requires local owner-only persistence"
    );
    match R::COLUMN_FAMILY {
        "rapidx_executions" => {
            let record: ExecutionRecord = serde_json::from_slice(&record.to_json_bytes()?)?;
            index_execution(store, &m, &record)?;
        }
        "rapidx_execution_roles" => {
            let role: ExecutionRoleRecord = serde_json::from_slice(&record.to_json_bytes()?)?;
            index_role(store, &m, &role)?;
        }
        _ => {}
    }
    Ok(())
}

fn index_execution(
    store: &RocksDbStore,
    m: &NormalizedUniformMarker,
    record: &ExecutionRecord,
) -> Result<()> {
    scope(m, record)?;
    let Some(hash) = execution_hash(record)? else {
        return Ok(());
    };
    let key = [tagged(b'X', &hash), record.stable_key()?.to_vec()].concat();
    let bytes = record.to_json_bytes()?;
    if store.get(STATE_CF, &key)?.as_deref() == Some(&bytes) {
        return Ok(());
    }
    if let Some(old) = store.get(STATE_CF, &key)? {
        let old: ExecutionRecord = serde_json::from_slice(&old)?;
        ensure!(
            record.accepts_existing(&old.to_json_bytes()?)?,
            "conflicting indexed execution"
        );
        if old.execution.liquidity.is_some() && record.execution.liquidity.is_none() {
            return Ok(());
        }
    }
    let mut writes = vec![
        (STATE_CF.into(), key, bytes),
        (STATE_CF.into(), tagged(b'D', &hash), vec![]),
    ];
    if any_group(store, b'L', &hash)? {
        pending(store, &hash, &mut writes)?;
    }
    store.put_many_sync(&writes)
}

fn index_role(
    store: &RocksDbStore,
    m: &NormalizedUniformMarker,
    role: &ExecutionRoleRecord,
) -> Result<()> {
    scope(m, &role.record)?;
    role.validate()?;
    let Some(hash) = execution_hash(&role.record)? else {
        return Ok(());
    };
    let key = [tagged(b'R', &hash), role.stable_key()?.to_vec()].concat();
    let bytes = role.to_json_bytes()?;
    if let Some(old) = store.get(STATE_CF, &key)? {
        ensure!(old == bytes, "conflicting indexed role");
        return Ok(());
    }
    let mut writes = vec![
        (STATE_CF.into(), key, bytes),
        (STATE_CF.into(), tagged(b'D', &hash), vec![]),
    ];
    if any_group(store, b'L', &hash)? {
        pending(store, &hash, &mut writes)?;
    }
    store.put_many_sync(&writes)
}

fn lifecycle_writes(
    store: &RocksDbStore,
    physical_key: &[u8],
    payload: &[u8],
) -> Result<Option<Vec<Write>>> {
    ensure!(
        payload.len() <= MAX_RECORD_BYTES,
        "uniform lifecycle record exceeds bound"
    );
    let row = uniform_fill::decode(payload)?;
    ensure!(
        row.record.amount_update.is_finite() && row.record.amount_update >= 0.0,
        "invalid lifecycle quantity"
    );
    ensure!(
        physical_key.len() == 20 && physical_key.iter().all(u8::is_ascii_digit),
        "invalid uniform receive key"
    );
    let hash = hash_order(&row, physical_key)?;
    let key = [tagged(b'L', &hash), physical_key.to_vec()].concat();
    if let Some(old) = store.get(STATE_CF, &key)? {
        ensure!(old == payload, "original lifecycle record changed");
        return Ok(None);
    }
    let mut writes = vec![
        (STATE_CF.into(), key, payload.to_vec()),
        (STATE_CF.into(), tagged(b'D', &hash), vec![]),
    ];
    if row.record.amount_update > 0.0 || any_group(store, b'X', &hash)? {
        pending(store, &hash, &mut writes)?;
    }
    Ok(Some(writes))
}

pub(crate) fn persist_lifecycle(
    store: &RocksDbStore,
    physical_key: &[u8],
    payload: &[u8],
    sync_enabled: bool,
) -> Result<bool> {
    let Some(m) = marker(store)? else {
        return Ok(false);
    };
    ensure!(m.complete, "uniform repair incomplete; refuse mixed input");
    ensure!(
        !sync_enabled,
        "uniform normalization requires local owner-only persistence"
    );
    let venue = uniform_fill::decode(payload)?.record.venue;
    ensure!(
        matches!(
            (m.exchange.as_str(), venue),
            ("BINANCE", 0 | 1) | ("OKX", 2 | 3)
        ),
        "uniform lifecycle venue mismatch"
    );
    let Some(mut writes) = lifecycle_writes(store, physical_key, payload)? else {
        return Ok(true);
    };
    let mut zero = uniform_fill::decode(payload)?;
    zero.record.amount_update = 0.0;
    zero.record.fill_liquidity = persist_common::FillLiquidity::Unknown;
    let next = allocation_key(store)?
        .checked_add(1)
        .context("uniform key overflow")?
        .max(zero.received_at_us);
    let key = format!("{next:020}").into_bytes();
    writes.extend([
        (UNIFORM.into(), key, uniform_fill::encode(&zero)?.to_vec()),
        (
            STATE_CF.into(),
            b"last_key".to_vec(),
            next.to_string().into_bytes(),
        ),
    ]);
    // Keep the original, dirty/pending receipt, lifecycle metadata and key
    // allocation together. A retry cannot mistake a half-written row for a
    // completely accepted lifecycle after an I/O failure or process restart.
    store.put_many_sync(&writes)?;
    Ok(true)
}

pub fn project_one(store: &RocksDbStore, hash: &[u8; 32]) -> Result<usize> {
    project_order(store, hash, true)
}

fn project_order(store: &RocksDbStore, hash: &[u8; 32], durable: bool) -> Result<usize> {
    let mut m = marker(store)?.context("missing uniform marker")?;
    let raw = load_group(store, b'L', hash)?;
    if raw.is_empty() {
        store.delete(STATE_CF, &tagged(b'D', hash))?;
        return Ok(0);
    }
    let rows = raw
        .into_iter()
        .map(|(key, payload)| {
            Ok(Lifecycle {
                key: String::from_utf8(key[33..].to_vec())?,
                value: uniform_fill::decode(&payload)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let executions = load_group(store, b'X', hash)?
        .into_iter()
        .map(|(_, v)| serde_json::from_slice::<ExecutionRecord>(&v))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let roles = load_group(store, b'R', hash)?
        .into_iter()
        .map(|(_, v)| serde_json::from_slice::<ExecutionRoleRecord>(&v))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let normalized = if store.get(STATE_CF, &tagged(b'H', hash))?.is_some() {
        normalize::normalize(&rows, &executions, &roles, &m.portfolio, &m.exchange)?
    } else {
        normalize::normalize_new(&rows, &executions, &roles, &m.portfolio, &m.exchange)?
    };
    let mut previous: BTreeMap<Vec<u8>, Output> = load_group(store, b'P', hash)?
        .into_iter()
        .map(|(k, v)| Ok((k, serde_json::from_slice(&v)?)))
        .collect::<Result<_>>()?;
    let mut last = allocation_key(store)?;
    let mut writes = Vec::new();
    let mut deletes = Vec::new();
    let mut changed = false;
    let mut logical_keys = BTreeSet::new();
    for mut row in normalized {
        if !row.role_present {
            row.provenance
                .as_mut()
                .context("missing normalized provenance")?
                .legacy_report_role = store
                .get(STATE_CF, &tagged(b'T', hash))?
                .map(|bytes| -> Result<u8> {
                    ensure!(
                        bytes.len() == 1 && bytes[0] <= 2,
                        "invalid legacy role receipt"
                    );
                    Ok(bytes[0])
                })
                .transpose()?;
        }
        let logical = &row
            .provenance
            .as_ref()
            .context("missing normalized provenance")?
            .logical_record_key;
        ensure!(
            logical_keys.insert(logical.clone()),
            "duplicate normalized identity"
        );
        let pkey = [
            tagged(b'P', hash),
            Sha256::digest(logical.as_bytes()).to_vec(),
        ]
        .concat();
        let old = previous.remove(&pkey);
        let physical = match &old {
            Some(old) => old.physical.clone(),
            None => {
                last = last.checked_add(1).context("uniform key overflow")?;
                format!("{last:020}")
            }
        };
        row.received_at_us = physical.parse()?;
        let payload = uniform_fill::encode(&row)?.to_vec();
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        if old.as_ref().is_some_and(|old| old.digest == digest) {
            continue;
        }
        changed |= old.is_some();
        writes.push((UNIFORM.into(), physical.as_bytes().to_vec(), payload));
        writes.push((
            STATE_CF.into(),
            pkey,
            serde_json::to_vec(&Output { physical, digest })?,
        ));
    }
    for (key, old) in previous {
        let bytes = store
            .get(UNIFORM, old.physical.as_bytes())?
            .context("missing old uniform projection")?;
        let mut row = uniform_fill::decode(&bytes)?;
        row.record.amount_update = 0.0;
        writes.push((
            UNIFORM.into(),
            old.physical.into_bytes(),
            uniform_fill::encode(&row)?.to_vec(),
        ));
        deletes.push((STATE_CF.into(), key));
        changed = true;
    }
    if changed {
        m.rewrite_epoch = m
            .rewrite_epoch
            .checked_add(1)
            .context("uniform epoch overflow")?;
    }
    if store.get(STATE_CF, &tagged(b'Q', hash))?.is_some() {
        m.pending_orders = m
            .pending_orders
            .checked_sub(1)
            .context("uniform pending receipt mismatch")?;
        deletes.push((STATE_CF.into(), tagged(b'Q', hash)));
    }
    writes.push((
        "default".into(),
        NORMALIZED_UNIFORM_MARKER_KEY.to_vec(),
        serde_json::to_vec(&m)?,
    ));
    writes.push((
        STATE_CF.into(),
        b"last_key".to_vec(),
        last.to_string().into_bytes(),
    ));
    deletes.push((STATE_CF.into(), tagged(b'D', hash)));
    let count = writes.len();
    store.replace_many(&writes, &deletes, durable)?;
    Ok(count)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairReport {
    pub db_identity: String,
    pub portfolio: String,
    pub exchange: String,
    pub original_rows: u64,
    pub original_sha256: String,
    pub evidence_sha256: String,
    pub projection_rows: u64,
    pub projection_sha256: String,
    pub normalized_rows: u64,
    pub normalized_sha256: String,
    pub normalized_fills: u64,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn evidence_digest(store: &RocksDbStore) -> Result<String> {
    let mut hash = Sha256::new();
    for cf in [
        "rapidx_executions",
        EXECUTION_OBSERVATIONS_CF,
        "rapidx_execution_roles",
        "trade_updates",
        "trade_updates_unmatched",
    ] {
        hash.update((cf.len() as u64).to_be_bytes());
        hash.update(cf.as_bytes());
        if store.has_column_family(cf) {
            store.scan_range_batches(cf, b"", &[255; 128], 128, |rows| {
                for (key, value) in rows {
                    hash.update((key.len() as u64).to_be_bytes());
                    hash.update(&key);
                    hash.update((value.len() as u64).to_be_bytes());
                    hash.update(&value);
                }
                Ok(())
            })?;
        }
    }
    Ok(hex(&hash.finalize()))
}

pub fn verify(store: &RocksDbStore) -> Result<(u64, String, u64)> {
    ensure!(
        required_marker(store)?.pending_orders == 0,
        "uniform fills are still pending"
    );
    digest(store, UNIFORM)
}

fn digest(store: &RocksDbStore, cf: &str) -> Result<(u64, String, u64)> {
    let mut hash = Sha256::new();
    let mut count = 0;
    let mut fills = 0;
    store.scan_range_batches(cf, b"", &[255; 64], 128, |rows| {
        for (key, value) in rows {
            hash.update((key.len() as u64).to_be_bytes());
            hash.update(&key);
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(&value);
            count += 1;
            if cf == UNIFORM {
                let row = uniform_fill::decode(&value)?;
                ensure!(
                    row.record.amount_update.is_finite() && row.record.amount_update >= 0.0,
                    "invalid normalized quantity"
                );
                if row.record.amount_update > 0.0 {
                    ensure!(
                        row.record.price.is_finite()
                            && row.record.price > 0.0
                            && row.provenance.is_some(),
                        "normalized fill is not self-contained"
                    );
                    fills += 1;
                }
            }
        }
        Ok(())
    })?;
    Ok((count, hex(&hash.finalize()), fills))
}

/// Build and verify the entire result in disposable scratch before touching a
/// source record. The caller must hold the source writer lock for maintenance.
pub fn build_repair_plan(
    input: &RocksDbStore,
    scratch: &RocksDbStore,
    portfolio: &str,
    exchange: &str,
) -> Result<RepairReport> {
    require_local_store(input)?;
    ensure!(
        matches!(exchange, "BINANCE" | "OKX")
            && !portfolio.is_empty()
            && portfolio.bytes().all(|c| c.is_ascii_digit()),
        "invalid repair scope"
    );
    ensure!(marker(input)?.is_none(),"source is already normalized; use its original verified backup to resume an interrupted repair");
    let m = NormalizedUniformMarker {
        db_identity: identity(scratch)?,
        portfolio: portfolio.into(),
        exchange: exchange.into(),
        complete: false,
        rewrite_epoch: 0,
        pending_orders: 0,
    };
    scratch.put(
        "default",
        NORMALIZED_UNIFORM_MARKER_KEY,
        &serde_json::to_vec(&m)?,
    )?;
    let mut original_hash = Sha256::new();
    let mut original_rows = 0;
    input.scan_range_batches(UNIFORM, b"", b"\xff", 128, |rows| {
        let mut writes = Vec::new();
        for (key, value) in rows {
            ensure!(
                key.len() == 20
                    && key.iter().all(u8::is_ascii_digit)
                    && value.len() <= MAX_RECORD_BYTES,
                "invalid source uniform record"
            );
            original_hash.update((key.len() as u64).to_be_bytes());
            original_hash.update(&key);
            original_hash.update((value.len() as u64).to_be_bytes());
            original_hash.update(&value);
            original_rows += 1;
            let mut row = uniform_fill::decode(&value)?;
            let hash = hash_order(&row, &key)?;
            writes.push((
                STATE_CF.into(),
                [tagged(b'L', &hash), key.clone()].concat(),
                value.clone(),
            ));
            writes.push((STATE_CF.into(), tagged(b'D', &hash), vec![]));
            writes.push((STATE_CF.into(), tagged(b'H', &hash), vec![]));
            row.record.amount_update = 0.0;
            writes.push((UNIFORM.into(), key, uniform_fill::encode(&row)?.to_vec()));
        }
        scratch.put_many(&writes)
    })?;
    for cf in [
        "rapidx_executions",
        EXECUTION_OBSERVATIONS_CF,
        "rapidx_execution_roles",
    ] {
        if !input.has_column_family(cf) {
            continue;
        }
        input.scan_range_batches(cf, b"", &[255; 128], 128, |rows| {
            let mut writes = Vec::new();
            for (key, value) in rows {
                ensure!(
                    value.len() <= MAX_RECORD_BYTES,
                    "execution evidence exceeds bound"
                );
                let (record, role) = if cf == "rapidx_execution_roles" {
                    let role: ExecutionRoleRecord = serde_json::from_slice(&value)?;
                    role.validate()?;
                    ensure!(
                        role.stable_key()?.as_slice() == key,
                        "role storage identity mismatch"
                    );
                    (role.record.clone(), Some(role))
                } else {
                    let record: ExecutionRecord = serde_json::from_slice(&value)?;
                    record.validate()?;
                    if cf == "rapidx_executions" {
                        ensure!(
                            record.stable_key()?.as_slice() == key,
                            "execution storage identity mismatch"
                        );
                    } else {
                        ensure!(
                            record.ack()?.as_slice() == key,
                            "execution observation identity mismatch"
                        );
                    }
                    (record, None)
                };
                scope(&m, &record)?;
                let Some(hash) = execution_hash(&record)? else {
                    continue;
                };
                if let Some(role) = role {
                    writes.push((
                        STATE_CF.into(),
                        [tagged(b'R', &hash), role.stable_key()?.to_vec()].concat(),
                        role.to_json_bytes()?,
                    ));
                } else {
                    let key = [tagged(b'X', &hash), record.stable_key()?.to_vec()].concat();
                    let old = writes
                        .iter()
                        .rev()
                        .find(|(_, k, _)| *k == key)
                        .map(|(_, _, v)| v.clone())
                        .or(scratch.get(STATE_CF, &key)?);
                    if let Some(old) = old {
                        ensure!(
                            record.accepts_existing(&old)?,
                            "conflicting execution observation"
                        );
                        let old: ExecutionRecord = serde_json::from_slice(&old)?;
                        if old.execution.liquidity.is_some() && record.execution.liquidity.is_none()
                        {
                            continue;
                        }
                    }
                    writes.push((STATE_CF.into(), key, record.to_json_bytes()?));
                }
                writes.push((STATE_CF.into(), tagged(b'D', &hash), vec![]));
            }
            scratch.put_many(&writes)
        })?;
    }
    for cf in ["trade_updates", "trade_updates_unmatched"] {
        if !input.has_column_family(cf) {
            continue;
        }
        input.scan_range_batches(cf, b"", b"\xff", 128, |rows| {
            for (_, payload) in rows {
                if let Some((symbol, venue, client, role)) = uniform_fill::legacy_report(&payload)?
                {
                    let hash: [u8; 32] =
                        Sha256::digest(serde_json::to_vec(&(symbol, venue, client))?).into();
                    let key = tagged(b'T', &hash);
                    let code = match scratch.get(STATE_CF, &key)? {
                        Some(old) if old != [role as u8] => 0,
                        _ => role as u8,
                    };
                    scratch.put(STATE_CF, &key, &[code])?;
                }
            }
            Ok(())
        })?;
    }
    scratch.scan_range_batches(STATE_CF, b"D", b"E", 64, |rows| {
        for (key, _) in rows {
            project_order(scratch, key[1..].try_into()?, false)?;
        }
        Ok(())
    })?;
    let (normalized_rows, normalized_sha256, normalized_fills) = digest(scratch, UNIFORM)?;
    let (projection_rows, projection_sha256, _) = digest(scratch, STATE_CF)?;
    Ok(RepairReport {
        db_identity: identity(input)?,
        portfolio: portfolio.into(),
        exchange: exchange.into(),
        original_rows,
        original_sha256: hex(&original_hash.finalize()),
        evidence_sha256: evidence_digest(input)?,
        projection_rows,
        projection_sha256,
        normalized_rows,
        normalized_sha256,
        normalized_fills,
    })
}

/// Explicit owner-side apply. A native closed checkpoint is mandatory, and the
/// original source bytes are verified before the first overwrite. No runtime
/// startup scans or implicit repair are permitted.
pub fn apply_repair_plan(
    store: &RocksDbStore,
    plan: &RocksDbStore,
    report: &RepairReport,
    backup: &std::path::Path,
) -> Result<()> {
    require_local_store(store)?;
    ensure!(
        report.db_identity == identity(store)?,
        "repair source identity changed"
    );
    ensure!(
        report.evidence_sha256 == evidence_digest(store)?,
        "source execution evidence changed after preview"
    );
    let expected = digest(plan, UNIFORM)?;
    ensure!(
        expected
            == (
                report.normalized_rows,
                report.normalized_sha256.clone(),
                report.normalized_fills
            ),
        "repair plan changed"
    );
    ensure!(
        digest(plan, STATE_CF)? == (report.projection_rows, report.projection_sha256.clone(), 0),
        "repair projection state changed"
    );
    let old = marker(store)?;
    if let Some(old) = &old {
        ensure!(!old.complete, "source already normalized");
    }
    let mut raw_hash = Sha256::new();
    let mut raw_count = 0;
    if old.is_none() {
        store.scan_range_batches(UNIFORM, b"", b"\xff", 128, |rows| {
            for (key, value) in rows {
                raw_hash.update((key.len() as u64).to_be_bytes());
                raw_hash.update(&key);
                raw_hash.update((value.len() as u64).to_be_bytes());
                raw_hash.update(&value);
                raw_count += 1;
            }
            Ok(())
        })?;
        ensure!(
            raw_count == report.original_rows
                && hex(&raw_hash.finalize()) == report.original_sha256,
            "source changed after repair preview"
        );
        ensure!(!backup.exists(), "repair backup path already exists");
        store.checkpoint(backup)?;
        std::fs::write(
            backup.join("UNIFORM_REPAIR.json"),
            serde_json::to_vec(report)?,
        )?;
        std::fs::File::open(backup.join("UNIFORM_REPAIR.json"))?.sync_all()?;
        std::fs::File::open(backup)?.sync_all()?;
    } else {
        let saved: RepairReport =
            serde_json::from_slice(&std::fs::read(backup.join("UNIFORM_REPAIR.json"))?)?;
        ensure!(
            saved == *report,
            "interrupted repair backup does not match this plan"
        );
    }
    let selected = [
        "uniform_orders",
        "rapidx_executions",
        EXECUTION_OBSERVATIONS_CF,
        "rapidx_execution_roles",
        "trade_updates",
        "trade_updates_unmatched",
    ]
    .into_iter()
    .filter(|cf| store.has_column_family(cf))
    .collect::<Vec<_>>();
    let backed_up = RocksDbStore::open_read_only_with_tuning(
        backup.to_str().context("non-UTF8 backup path")?,
        &selected,
        &crate::storage::RocksDbTuning {
            max_open_files: Some(-1),
            ..Default::default()
        },
    )?;
    ensure!(
        identity(&backed_up)? == report.db_identity
            && evidence_digest(&backed_up)? == report.evidence_sha256,
        "backup execution evidence does not match source"
    );
    let mut backup_hash = Sha256::new();
    let mut backup_count = 0;
    backed_up.scan_range_batches(UNIFORM, b"", b"\xff", 128, |rows| {
        for (key, value) in rows {
            backup_hash.update((key.len() as u64).to_be_bytes());
            backup_hash.update(&key);
            backup_hash.update((value.len() as u64).to_be_bytes());
            backup_hash.update(&value);
            backup_count += 1;
        }
        Ok(())
    })?;
    ensure!(
        backup_count == report.original_rows
            && hex(&backup_hash.finalize()) == report.original_sha256,
        "backup uniform history is incomplete"
    );
    let mut m = NormalizedUniformMarker {
        db_identity: report.db_identity.clone(),
        portfolio: report.portfolio.clone(),
        exchange: report.exchange.clone(),
        complete: false,
        rewrite_epoch: old.map_or(1, |m| m.rewrite_epoch),
        pending_orders: 0,
    };
    store.put_sync(
        "default",
        NORMALIZED_UNIFORM_MARKER_KEY,
        &serde_json::to_vec(&m)?,
    )?;
    // Copy exact original lifecycle audit and identity indexes first. Projection
    // data and per-order receipts remain one format and are restart-resumable.
    for cf in [STATE_CF, UNIFORM] {
        plan.scan_range_batches(cf, b"", &[255; 128], 128, |rows| {
            store.put_many_sync(
                &rows
                    .into_iter()
                    .map(|(k, v)| (cf.into(), k, v))
                    .collect::<Vec<_>>(),
            )
        })?;
    }
    ensure!(
        digest(store, UNIFORM)? == expected,
        "applied uniform records do not match verified plan"
    );
    m.complete = true;
    store.put_sync(
        "default",
        NORMALIZED_UNIFORM_MARKER_KEY,
        &serde_json::to_vec(&m)?,
    )?;
    Ok(())
}

fn require_local_store(store: &RocksDbStore) -> Result<()> {
    for cf in ["sync_meta", "sync_outbox"] {
        ensure!(
            !store.has_column_family(cf) || store.scan(cf, None, false, Some(1))?.is_empty(),
            "uniform repair refuses a source with replication history: {cf}"
        );
    }
    Ok(())
}

/// The owner polls at most one bounded order between normal persistence passes.
pub(crate) fn poll(store: &RocksDbStore) -> Result<usize> {
    if marker(store)?.is_none() {
        return Ok(0);
    }
    required_marker(store)?;
    // A late/missing execution must not keep unrelated orders behind its hash.
    let cursor = store.get(STATE_CF, b"poll_cursor")?;
    let after = cursor.map(|mut key| {
        key.push(0);
        key
    });
    let mut dirty = store.scan_range(STATE_CF, after.as_deref().unwrap_or(b"D"), b"E", Some(1))?;
    if dirty.is_empty() {
        dirty = store.scan_range(STATE_CF, b"D", b"E", Some(1))?;
    }
    match dirty.first() {
        None => Ok(0),
        Some((key, value)) => {
            store.put(STATE_CF, b"poll_cursor", key)?;
            let now = crate::runtime_common::get_timestamp_us();
            if value.len() == 8 && i64::from_le_bytes(value.as_slice().try_into()?) > now {
                return Ok(0);
            }
            match project_one(store, key[1..].try_into()?) {
                Ok(count) => Ok(count),
                Err(error) => {
                    store.put(STATE_CF, key, &now.saturating_add(2_000_000).to_le_bytes())?;
                    Err(error)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
