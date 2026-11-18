use super::*;
use persist_common::{rapidx_execution::ExecutionEvidence, FillLiquidity};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Fixture {
    source: RocksDbStore,
    plan: RocksDbStore,
    directory: Directory,
}
fn fixture() -> Fixture {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "rapidx_uniform_{}_{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let mut cfs = crate::required_column_families();
    cfs.push("sync_meta");
    Fixture {
        source: RocksDbStore::open(root.join("source").to_str().unwrap(), &cfs, false).unwrap(),
        plan: RocksDbStore::open(root.join("plan").to_str().unwrap(), &cfs, false).unwrap(),
        directory: Directory(root),
    }
}
fn lifecycle(price: f64, quantity: f64) -> Vec<u8> {
    let mut row = uniform_fill::decode(include_bytes!(
        "../../../persist_common/src/testdata/rapidx_uniform.bin"
    ))
    .unwrap();
    row.received_at_us = 90_000;
    row.provenance = None;
    row.record.price = price;
    row.record.amount_update = quantity;
    row.record.fill_liquidity = FillLiquidity::Unknown;
    row.role_present = true;
    uniform_fill::encode(&row).unwrap().to_vec()
}
fn execution(
    tx: &str,
    price: f64,
    quantity: f64,
    role: Option<&str>,
    rest: bool,
) -> ExecutionRecord {
    let mut json = serde_json::json!({"portfolioId":"123","exchangeType":"BINANCE","businessType":"MARGIN",
        "sym":"BINANCE_MARGIN_FIL_USDT","transactionId":tx,"orderId":"88","clientOrderId":"7",
        "side":"SELL","quantity":quantity.to_string(),"price":price.to_string(),"createAt":"100",
        "rpnl":"0","tradingFee":"0","tradingFeeCoin":"","fee":"0","feeCoin":"","rebate":"0","rebateCoin":""});
    if let Some(role) = role {
        json["execType"] = role.into();
    }
    ExecutionRecord {
        portfolio: "123".into(),
        exchange: "BINANCE".into(),
        execution: ExecutionEvidence::parse(&json, "123", "BINANCE", rest).unwrap(),
    }
}
fn fills(store: &RocksDbStore) -> Vec<StoredUniformOrder> {
    store
        .scan(UNIFORM, None, false, None)
        .unwrap()
        .into_iter()
        .map(|(_, b)| uniform_fill::decode(&b).unwrap())
        .filter(|r| r.record.amount_update > 0.0)
        .collect()
}
fn initialize(f: &Fixture) {
    let report = build_repair_plan(&f.source, &f.plan, "123", "BINANCE").unwrap();
    apply_repair_plan(&f.source, &f.plan, &report, &f.directory.0.join("backup")).unwrap();
}

#[test]
fn repair_keeps_verified_originals_and_replaces_quantity_without_double_counting() {
    let f = fixture();
    let raw = lifecycle(0.0, 3.0);
    let original_key = format!("{:020}", 90_000);
    f.source
        .put(UNIFORM, original_key.as_bytes(), &raw)
        .unwrap();
    for e in [
        execution("a", 100.0, 1.0, Some("MAKER"), false),
        execution("a", 100.0, 1.0, Some("MAKER"), true),
        execution("b", 103.0, 2.0, Some("TAKER"), true),
    ] {
        f.source
            .put(
                "rapidx_executions",
                &e.stable_key().unwrap(),
                &e.to_json_bytes().unwrap(),
            )
            .unwrap();
    }
    let before = f.source.scan(UNIFORM, None, false, None).unwrap();
    let report = build_repair_plan(&f.source, &f.plan, "123", "BINANCE").unwrap();
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap(), before);
    assert_eq!(report.normalized_fills, 2);
    let backup = f.directory.0.join("backup");
    apply_repair_plan(&f.source, &f.plan, &report, &backup).unwrap();
    let output = fills(&f.source);
    assert_eq!(
        output.iter().map(|r| r.record.amount_update).sum::<f64>(),
        3.0
    );
    assert_eq!(
        output
            .iter()
            .map(|r| r.record.price * r.record.amount_update)
            .sum::<f64>(),
        306.0
    );
    assert_eq!(
        uniform_fill::decode(
            &f.source
                .get(UNIFORM, original_key.as_bytes())
                .unwrap()
                .unwrap()
        )
        .unwrap()
        .record
        .amount_update,
        0.0
    );
    let backed_up = RocksDbStore::open_read_only_with_tuning(
        backup.to_str().unwrap(),
        &[UNIFORM],
        &Default::default(),
    )
    .unwrap();
    assert_eq!(
        backed_up
            .get(UNIFORM, original_key.as_bytes())
            .unwrap()
            .unwrap(),
        raw
    );
    assert_eq!(verify(&f.source).unwrap().2, 2);
    assert!(marker(&f.source).unwrap().unwrap().complete);
    let output_before = f.source.scan(UNIFORM, None, false, None).unwrap();
    assert!(apply_repair_plan(&f.source, &f.plan, &report, &backup).is_err());
    assert_eq!(
        f.source.scan(UNIFORM, None, false, None).unwrap(),
        output_before
    );
}

#[test]
fn new_orders_wait_for_executions_and_keep_duplicate_ws_rest_idempotent() {
    let f = fixture();
    initialize(&f);
    persist_lifecycle(
        &f.source,
        format!("{:020}", 90_000).as_bytes(),
        &lifecycle(999.0, 1.0),
        false,
    )
    .unwrap();
    assert_eq!(marker(&f.source).unwrap().unwrap().pending_orders, 1);
    assert!(poll(&f.source).is_err());
    assert!(fills(&f.source).is_empty());
    let ws = execution("a", 100.0, 1.0, Some("MAKER"), false);
    enqueue_fact(&f.source, &ws, false).unwrap();
    poll(&f.source).unwrap();
    assert_eq!(marker(&f.source).unwrap().unwrap().pending_orders, 0);
    assert_eq!(fills(&f.source)[0].record.price, 100.0);
    let before = f.source.scan(UNIFORM, None, false, None).unwrap();
    enqueue_fact(&f.source, &ws, false).unwrap();
    enqueue_fact(
        &f.source,
        &execution("a", 100.0, 1.0, Some("MAKER"), true),
        false,
    )
    .unwrap();
    poll(&f.source).unwrap();
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap(), before);
    assert_eq!(verify(&f.source).unwrap().2, 1);
}

#[test]
fn late_role_changes_one_fill_and_advances_epoch_without_adding_quantity() {
    let f = fixture();
    initialize(&f);
    persist_lifecycle(
        &f.source,
        format!("{:020}", 90_000).as_bytes(),
        &lifecycle(0.0, 0.0),
        false,
    )
    .unwrap();
    enqueue_fact(&f.source, &execution("a", 100.0, 1.0, None, true), false).unwrap();
    poll(&f.source).unwrap();
    let epoch = marker(&f.source).unwrap().unwrap().rewrite_epoch;
    assert_eq!(
        fills(&f.source)[0].record.fill_liquidity,
        FillLiquidity::Unknown
    );
    enqueue_fact(
        &f.source,
        &execution("a", 100.0, 1.0, Some("MAKER"), false),
        false,
    )
    .unwrap();
    poll(&f.source).unwrap();
    assert_eq!(fills(&f.source).len(), 1);
    assert_eq!(
        fills(&f.source)[0].record.fill_liquidity,
        FillLiquidity::Maker
    );
    assert_eq!(marker(&f.source).unwrap().unwrap().rewrite_epoch, epoch + 1);
}

#[test]
fn conflicts_or_incomplete_repair_do_not_overwrite_good_uniform_rows() {
    let f = fixture();
    let raw = lifecycle(0.0, 3.0);
    f.source
        .put(UNIFORM, format!("{:020}", 90_000).as_bytes(), &raw)
        .unwrap();
    let e = execution("a", 100.0, 1.0, Some("MAKER"), false);
    f.source
        .put(
            "rapidx_executions",
            &e.stable_key().unwrap(),
            &e.to_json_bytes().unwrap(),
        )
        .unwrap();
    let before = f.source.scan(UNIFORM, None, false, None).unwrap();
    assert!(build_repair_plan(&f.source, &f.plan, "123", "BINANCE").is_err());
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap(), before);
    assert!(marker(&f.source).unwrap().is_none());
}

#[test]
fn persisted_dirty_receipt_and_pending_gate_survive_owner_restart() {
    let f = fixture();
    initialize(&f);
    persist_lifecycle(
        &f.source,
        format!("{:020}", 90_000).as_bytes(),
        &lifecycle(999.0, 1.0),
        false,
    )
    .unwrap();
    let Fixture {
        source,
        plan,
        directory,
    } = f;
    drop(source);
    let reopened = RocksDbStore::open_with_existing_cfs_and_tuning(
        directory.0.join("source").to_str().unwrap(),
        &crate::required_column_families()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        false,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(marker(&reopened).unwrap().unwrap().pending_orders, 1);
    enqueue_fact(
        &reopened,
        &execution("a", 100.0, 1.0, Some("MAKER"), false),
        false,
    )
    .unwrap();
    poll(&reopened).unwrap();
    assert_eq!(verify(&reopened).unwrap().2, 1);
    drop(reopened);
    drop(plan);
    drop(directory);
}

#[test]
fn interrupted_history_repair_resumes_from_the_verified_backup() {
    let f = fixture();
    let key = format!("{:020}", 90_000);
    let raw = lifecycle(100.0, 1.0);
    f.source.put(UNIFORM, key.as_bytes(), &raw).unwrap();
    let report = build_repair_plan(&f.source, &f.plan, "123", "BINANCE").unwrap();
    let backup = f.directory.0.join("backup");
    apply_repair_plan(&f.source, &f.plan, &report, &backup).unwrap();
    let expected = f.source.scan(UNIFORM, None, false, None).unwrap();
    let mut m = marker(&f.source).unwrap().unwrap();
    m.complete = false;
    f.source
        .put_sync(
            "default",
            NORMALIZED_UNIFORM_MARKER_KEY,
            &serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
    f.source.put(UNIFORM, key.as_bytes(), &raw).unwrap();
    assert!(verify(&f.source).is_err());
    assert!(persist_lifecycle(&f.source, key.as_bytes(), &raw, false).is_err());
    let original = RocksDbStore::open_read_only_with_tuning(
        backup.to_str().unwrap(),
        &[UNIFORM],
        &Default::default(),
    )
    .unwrap();
    let rebuilt = RocksDbStore::open(
        f.directory.0.join("rebuilt").to_str().unwrap(),
        &crate::required_column_families(),
        false,
    )
    .unwrap();
    let resumed = build_repair_plan(&original, &rebuilt, "123", "BINANCE").unwrap();
    assert_eq!(resumed, report);
    apply_repair_plan(&f.source, &rebuilt, &resumed, &backup).unwrap();
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap(), expected);
    assert_eq!(verify(&f.source).unwrap().2, 1);
}

#[test]
fn changed_projection_plan_is_rejected_before_source_writes() {
    let f = fixture();
    f.source
        .put(
            UNIFORM,
            format!("{:020}", 90_000).as_bytes(),
            &lifecycle(100.0, 1.0),
        )
        .unwrap();
    let report = build_repair_plan(&f.source, &f.plan, "123", "BINANCE").unwrap();
    let before = f.source.scan(UNIFORM, None, false, None).unwrap();
    f.plan.put(STATE_CF, b"invalid", b"changed").unwrap();
    let backup = f.directory.0.join("backup");
    assert!(apply_repair_plan(&f.source, &f.plan, &report, &backup).is_err());
    assert!(!backup.exists());
    assert!(marker(&f.source).unwrap().is_none());
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap(), before);
}

#[test]
fn historical_repair_refuses_existing_replication_state() {
    let f = fixture();
    f.source.put("sync_meta", b"sequence", b"1").unwrap();
    assert!(build_repair_plan(&f.source, &f.plan, "123", "BINANCE").is_err());
    assert!(marker(&f.source).unwrap().is_none());
}

#[test]
fn failed_lifecycle_admission_does_not_leave_a_pending_receipt_without_its_uniform_row() {
    let f = fixture();
    initialize(&f);
    f.source
        .put(STATE_CF, b"last_key", i64::MAX.to_string().as_bytes())
        .unwrap();
    let payload = lifecycle(100.0, 1.0);
    let key = format!("{:020}", 90_000);
    assert!(persist_lifecycle(&f.source, key.as_bytes(), &payload, false).is_err());
    assert_eq!(marker(&f.source).unwrap().unwrap().pending_orders, 0);
    assert!(f
        .source
        .scan_range(STATE_CF, b"L", b"M", None)
        .unwrap()
        .is_empty());
    assert!(f
        .source
        .scan(UNIFORM, None, false, None)
        .unwrap()
        .is_empty());
    f.source.delete(STATE_CF, b"last_key").unwrap();
    persist_lifecycle(&f.source, key.as_bytes(), &payload, false).unwrap();
    assert_eq!(marker(&f.source).unwrap().unwrap().pending_orders, 1);
    assert_eq!(f.source.scan(UNIFORM, None, false, None).unwrap().len(), 1);
}
