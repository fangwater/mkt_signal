use anyhow::Result;
use chrono::NaiveDate;
use cn_futures_l2::codec::{
    decode_depth, decode_key, decode_oi, decode_queue, decode_trade, KIND_DEPTH, KIND_OI,
    KIND_QUEUE, KIND_TRADE, STATUS_DONE,
};
use cn_futures_l2::db::{claim_day, open_rocksdb, read_day_status, run_replay, ReplayArgs};
use cn_futures_l2::session::Exchange;
use cn_futures_l2::universe::EXCLUDED_RESEARCH_PRODUCTS;
use rocksdb::{IteratorMode, ReadOptions};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dual_layout")
}

fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

struct Scanned {
    trades: Vec<cn_futures_l2::TradeRecord>,
    depths: Vec<cn_futures_l2::DepthRecord>,
    open_ints: Vec<cn_futures_l2::OiRecord>,
    queues: Vec<cn_futures_l2::QueueRecord>,
}

fn scan_cf(db: &cn_futures_l2::db::L2Db, name: &str) -> Result<Scanned> {
    let cf = db
        .cf_handle(name)
        .ok_or_else(|| anyhow::anyhow!("missing cf {name}"))?;
    let mut scanned = Scanned {
        trades: Vec::new(),
        depths: Vec::new(),
        open_ints: Vec::new(),
        queues: Vec::new(),
    };
    let mut seen_keys = HashSet::new();
    let iter = db.iterator_cf_opt(&cf, ReadOptions::default(), IteratorMode::Start);
    for item in iter {
        let (key, value) = item?;
        assert!(seen_keys.insert(key.to_vec()), "duplicate key");
        let (kind, instrument, ts, seq) = decode_key(&key)?;
        match kind {
            KIND_TRADE => {
                let trade = decode_trade(&value)?;
                assert_eq!(trade.instrument, instrument);
                assert_eq!(trade.ts_utc_ns, ts);
                scanned.trades.push(trade);
            }
            KIND_DEPTH => {
                assert_eq!(seq, 0);
                let depth = decode_depth(&value)?;
                assert_eq!(depth.instrument, instrument);
                assert_eq!(depth.ts_utc_ns, ts);
                scanned.depths.push(depth);
            }
            KIND_OI => {
                let oi = decode_oi(&value)?;
                assert_eq!(oi.instrument, instrument);
                assert_eq!(oi.ts_utc_ns, ts);
                scanned.open_ints.push(oi);
            }
            KIND_QUEUE => {
                let queue = decode_queue(&value)?;
                assert_eq!(queue.instrument, instrument);
                assert_eq!(queue.ts_utc_ns, ts);
                scanned.queues.push(queue);
            }
            other => anyhow::bail!("unexpected kind {other}"),
        }
    }
    Ok(scanned)
}

#[test]
fn fixture_writes_trades_and_last_depth_without_auction() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let rocksdb_dir = tmp.path().join("cn_l2");
    let root = fixture_root();
    let args = ReplayArgs {
        l2_root: Some(root.join("comm_l2")),
        msg_root: Some(root.join("msg")),
        exchanges: vec![Exchange::Xsge],
        start: day(2026, 2, 10),
        end: day(2026, 2, 17),
        rocksdb_dir: rocksdb_dir.clone(),
        workers: 2,
        overlap_cut: None,
        lookback_days: 16,
    };
    let (stats, n_days) = run_replay(args)?;
    assert!(stats.trades > 0);
    assert!(stats.depths > 0);
    assert!(stats.open_ints > 0);
    assert_eq!(stats.queues, 0);
    assert_eq!(n_days, 8);

    {
        let db = open_rocksdb(&rocksdb_dir)?;
        assert_eq!(
            read_day_status(&db, Exchange::Xsge, day(2026, 2, 10))?,
            Some(STATUS_DONE.to_vec())
        );
        assert_eq!(
            read_day_status(&db, Exchange::Xsge, day(2026, 2, 16))?,
            Some(STATUS_DONE.to_vec())
        );

        let scanned = scan_cf(&db, "p:2026:RB")?;
        assert!(!scanned.trades.is_empty());
        assert!(!scanned.depths.is_empty());
        assert!(!scanned.open_ints.is_empty());
        assert!(scanned.queues.is_empty());
        assert!(scanned.trades.iter().all(|trade| trade.volume > 0.0));
        assert!(scanned.trades.iter().all(|trade| trade.price > 0.0));
        assert!(scanned.trades.iter().all(|trade| trade.aggressor <= 2));
        let mut depth_keys = HashSet::new();
        for depth in &scanned.depths {
            assert!(
                depth_keys.insert((depth.instrument.clone(), depth.ts_utc_ns)),
                "more than one depth per contract-second"
            );
            assert!(depth.bid_prices[0].is_some());
            assert!(depth.ask_prices[0].is_some());
            assert!(depth.bid_prices[0] <= depth.ask_prices[0]);
        }
        assert!(scanned.open_ints.iter().any(|oi| oi.instrument == "rb2605"));
        assert!(scanned
            .trades
            .iter()
            .any(|trade| trade.instrument == "rb2605"));
        assert!(!scanned
            .trades
            .iter()
            .any(|trade| trade.instrument.contains("efp")));
        for excluded in EXCLUDED_RESEARCH_PRODUCTS {
            let names = db.cf_handle(&format!("p:2026:{excluded}"));
            assert!(names.is_none(), "excluded product {excluded} was written");
        }
    }

    let rerun = ReplayArgs {
        l2_root: Some(root.join("comm_l2")),
        msg_root: Some(root.join("msg")),
        exchanges: vec![Exchange::Xsge],
        start: day(2026, 2, 10),
        end: day(2026, 2, 17),
        rocksdb_dir,
        workers: 2,
        overlap_cut: None,
        lookback_days: 16,
    };
    let (again, again_days) = run_replay(rerun)?;
    assert_eq!(again.trades, 0);
    assert_eq!(again.depths, 0);
    assert_eq!(again.open_ints, 0);
    assert_eq!(again.queues, 0);
    assert_eq!(again_days, 0);
    Ok(())
}

#[test]
fn leftover_writing_blocks_only_that_range() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let rocksdb_dir = tmp.path().join("cn_l2");
    let db = open_rocksdb(&rocksdb_dir)?;
    claim_day(&db, Exchange::Xsge, day(2026, 2, 10))?;
    drop(db);

    let err = run_replay(ReplayArgs {
        l2_root: Some(fixture_root().join("comm_l2")),
        msg_root: None,
        exchanges: vec![Exchange::Xsge],
        start: day(2026, 2, 10),
        end: day(2026, 2, 10),
        rocksdb_dir,
        workers: 1,
        overlap_cut: None,
        lookback_days: 1,
    })
    .unwrap_err();
    assert!(err.to_string().contains("writing"));
    Ok(())
}

#[test]
fn xdce_writes_every_queue_row() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let rocksdb_dir = tmp.path().join("cn_l2");
    let root = fixture_root();
    let args = ReplayArgs {
        l2_root: Some(root.join("comm_l2")),
        msg_root: None,
        exchanges: vec![Exchange::Xdce],
        start: day(2026, 2, 10),
        end: day(2026, 2, 10),
        rocksdb_dir: rocksdb_dir.clone(),
        workers: 1,
        overlap_cut: None,
        lookback_days: 1,
    };
    let (stats, n_days) = run_replay(args)?;
    assert_eq!(n_days, 1);
    assert_eq!(stats.queues, 2);
    assert!(stats.open_ints >= 2);

    let db = open_rocksdb(&rocksdb_dir)?;
    let scanned = scan_cf(&db, "p:2026:I")?;
    assert_eq!(scanned.queues.len(), 2);
    assert_eq!(scanned.queues[0].instrument, "i2609");
    assert_eq!(scanned.queues[0].bid_qty[0], Some(1.0));
    assert_eq!(scanned.queues[1].bid_qty[0], Some(1.0));
    assert_eq!(scanned.queues[1].bid_qty[1], Some(1.0));
    assert_ne!(scanned.queues[0].ts_utc_ns, scanned.queues[1].ts_utc_ns);
    Ok(())
}

#[test]
fn refuses_cme_rocksdb_path() {
    let err = cn_futures_l2::db::refuse_cme_rocksdb(Path::new(
        "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb",
    ))
    .unwrap_err();
    assert!(err.to_string().contains("CME"));
}
