//! Read complete source prefixes for explicit reference sessions and listed RICs.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use clap::Parser;
use cme_tas_replay::causal_volumes::{Accumulator, Frozen, Request, Trade};
use cme_tas_replay::product::{encode_all_key, product_cf_name, quote_last_merge};
use cme_tas_replay::{
    decode_cme_trade, decode_period_status, PeriodStatus, CF_REPLAY_META, KIND_CME_TRADE,
    MISSING_EXCH_HMS_NS, MISSING_PRICE, PERIOD_META_PREFIX, RIC_LEN,
};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    /// JSON array of explicit reviewed freeze requests.
    #[arg(long)]
    requests: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn opts() -> Options {
    let mut o = Options::default();
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    o.set_max_open_files(256);
    o.set_skip_stats_update_on_db_open(true);
    o
}

fn utc_year(ns: u64) -> Result<i32> {
    Ok(
        DateTime::<Utc>::from_timestamp((ns / 1_000_000_000) as i64, 0)
            .context("source timestamp")?
            .year(),
    )
}

fn midnight(date: NaiveDate) -> Result<u64> {
    let seconds = date
        .and_hms_opt(0, 0, 0)
        .context("midnight")?
        .and_utc()
        .timestamp();
    ensure!(seconds >= 0, "negative source date");
    (seconds as u64)
        .checked_mul(1_000_000_000)
        .context("date overflow")
}

fn run(a: Args) -> Result<()> {
    ensure!(!a.output.exists(), "output already exists");
    let raw_requests = fs::read(&a.requests)?;
    let requests: Vec<Request> = serde_json::from_slice(&raw_requests)?;
    ensure!(!requests.is_empty(), "empty requests");
    let mut required = BTreeMap::new();
    let mut names = BTreeSet::from(["default".to_string(), CF_REPLAY_META.to_string()]);
    let mut unique = BTreeSet::new();
    for r in &requests {
        let start = r.validate()?;
        ensure!(
            unique.insert((&r.product, r.trading_day, r.freeze_ns)),
            "duplicate freeze request"
        );
        for year in utc_year(start)?..=utc_year(r.freeze_ns - 1)? {
            let lo =
                midnight(NaiveDate::from_ymd_opt(year, 1, 1).context("year start")?)?.max(start);
            let hi = midnight(NaiveDate::from_ymd_opt(year + 1, 1, 1).context("year end")?)?
                .min(r.freeze_ns);
            let bounds = required.entry(year).or_insert((lo, hi));
            bounds.0 = bounds.0.min(lo);
            bounds.1 = bounds.1.max(hi);
            names.insert(product_cf_name(year.try_into()?, &r.product)?);
        }
    }
    let db = DB::open_cf_descriptors_read_only(
        &opts(),
        &a.rocksdb_dir,
        names.iter().map(|n| ColumnFamilyDescriptor::new(n, opts())),
        false,
    )?;
    let meta = db.cf_handle(CF_REPLAY_META).context("missing metadata")?;
    let mut periods = BTreeMap::new();
    for item in db.iterator_cf(&meta, IteratorMode::Start) {
        let (key, value) = item?;
        let Some(bytes) = key.strip_prefix(PERIOD_META_PREFIX.as_bytes()) else {
            continue;
        };
        let name = std::str::from_utf8(bytes)?;
        let Some((lo, hi)) = name.split_once('_') else {
            continue;
        };
        let first: NaiveDate = lo.parse()?;
        let Some(&(needed_lo, needed_hi)) = required.get(&first.year()) else {
            continue;
        };
        let last: NaiveDate = hi.parse()?;
        ensure!(
            decode_period_status(&value)? == PeriodStatus::Done,
            "source period incomplete: {name}"
        );
        ensure!(
            midnight(first)? <= needed_lo && midnight(last)? >= needed_hi,
            "source period does not cover requested prefix: {name}"
        );
        ensure!(
            periods.insert(first.year(), name.to_string()).is_none(),
            "ambiguous source year"
        );
    }
    ensure!(periods.len() == required.len(), "missing done source year");
    let mut results = Vec::<Frozen>::new();
    for r in &requests {
        let start = r.validate()?;
        let mut acc = Accumulator::new(r.clone())?;
        let rics = r
            .contracts
            .iter()
            .map(|c| c.ric.as_str())
            .collect::<BTreeSet<_>>();
        for ric in rics {
            for year in utc_year(start)?..=utc_year(r.freeze_ns - 1)? {
                let name = product_cf_name(year.try_into()?, &r.product)?;
                let cf = db.cf_handle(&name).context("missing product/year")?;
                let seek = encode_all_key(KIND_CME_TRADE, ric, start, 0, 0)?;
                let mut it = db.raw_iterator_cf(&cf);
                it.seek(seek);
                while it.valid() {
                    let key = it.key().context("source key")?;
                    if !key.starts_with(&seek[..1 + RIC_LEN]) {
                        break;
                    }
                    ensure!(key.len() == 31, "invalid source key length");
                    let published = u64::from_be_bytes(key[17..25].try_into()?);
                    if published >= r.freeze_ns {
                        break;
                    }
                    let rec = decode_cme_trade(it.value().context("source value")?)?;
                    ensure!(
                        published >= start
                            && published == rec.ts_utc_ns
                            && rec.ric == ric
                            && utc_year(published)? == year,
                        "source key/value/year mismatch"
                    );
                    acc.add(&Trade {
                        ric: rec.ric,
                        published_ns: published,
                        exchange_hms_ns: (rec.exch_hms_ns != MISSING_EXCH_HMS_NS)
                            .then_some(rec.exch_hms_ns),
                        price_e9: (rec.price != MISSING_PRICE).then_some(rec.price),
                        volume: rec.volume as u64,
                        part: u16::from_be_bytes(key[25..27].try_into()?),
                        seq: u32::from_be_bytes(key[27..31].try_into()?),
                    })?;
                    it.next();
                }
                it.status()?;
            }
            acc.finish_ric(ric)?;
        }
        results.push(acc.finish());
    }
    let blocked = results.iter().filter(|r| !r.batch.complete).count();
    let uncertain = results
        .iter()
        .filter(|r| r.batch.complete && r.batch.rows.iter().any(|q| q.volume.is_none()))
        .count();
    let output = serde_json::json!({
        "requests_sha256":format!("{:x}",Sha256::digest(&raw_requests)),
        "source_database":a.rocksdb_dir,"source_periods":periods,
        "scope":"source-prefix computation from supplied calendar/identity evidence; not independent historical evidence certification",
        "volumes":results.iter().map(|r|&r.batch).collect::<Vec<_>>(),
        "audits":results.iter().map(|r|serde_json::json!({"product":r.batch.product,
            "trading_day":r.batch.trading_day,"freeze_ns":r.batch.available_at_ns,
            "audit":r.audit,"blocked_reasons":r.blocked_reasons})).collect::<Vec<_>>(),
        "blocked_batches":blocked,
        "uncertain_batches":uncertain,
    });
    if let Some(parent) = a.output.parent() {
        fs::create_dir_all(parent)?;
    }
    let partial = a.output.with_extension("json.partial");
    let mut writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)?;
    serde_json::to_writer_pretty(&mut writer, &output)?;
    writer.write_all(b"\n")?;
    writer.sync_all()?;
    drop(writer);
    fs::rename(partial, &a.output)?;
    eprintln!(
        "freeze_complete batches={} blocked={blocked} uncertain={uncertain}",
        results.len()
    );
    ensure!(
        blocked == 0,
        "incomplete batches written for audit; do not publish a main series"
    );
    Ok(())
}

fn main() -> Result<()> {
    run(Args::parse())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rocksdb_prefix_keeps_late_reports_out_and_proves_empty_ric() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("source");
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/causal_volumes.json"))?;
        let case = &cases[1];
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, &source, [CF_REPLAY_META, "p:2021:TY"])?;
        db.put_cf(
            &db.cf_handle(CF_REPLAY_META).unwrap(),
            cme_tas_replay::period_meta_key("2021-01-01_2022-01-01")?,
            cme_tas_replay::encode_period_status(PeriodStatus::Done),
        )?;
        let events: Vec<Trade> = serde_json::from_value(case["events"].clone())?;
        for e in events {
            let mut t = cme_tas_replay::ali_h26_fixture();
            t.ric = e.ric;
            t.ts_utc_ns = e.published_ns;
            t.exch_hms_ns = e.exchange_hms_ns.unwrap_or(MISSING_EXCH_HMS_NS);
            // The Trade codec rejects absent price/volume; zero price exercises
            // the same non-printable exclusion against a legal stored record.
            t.price = e.price_e9.unwrap_or(0);
            t.volume = if e.price_e9.is_none() {
                1
            } else {
                e.volume.try_into()?
            };
            db.put_cf(
                &db.cf_handle("p:2021:TY").unwrap(),
                encode_all_key(KIND_CME_TRADE, &t.ric, t.ts_utc_ns, e.part, e.seq)?,
                cme_tas_replay::encode_cme_trade(&t)?,
            )?;
        }
        drop(db);
        let input = tmp.path().join("requests.json");
        fs::write(&input, serde_json::to_vec(&vec![&case["request"]])?)?;
        let output = tmp.path().join("volumes.json");
        run(Args {
            rocksdb_dir: source,
            requests: input,
            output: output.clone(),
        })?;
        let actual: serde_json::Value = serde_json::from_slice(&fs::read(output)?)?;
        assert_eq!(actual["volumes"][0], case["expected"]["batch"]);
        assert_eq!(actual["audits"][0]["audit"], case["expected"]["audit"]);
        assert_eq!(actual["blocked_batches"], 0);
        Ok(())
    }

    #[test]
    fn cross_year_requires_both_done_partitions_before_emitting_zero() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let source = tmp.path().join("source");
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/causal_volumes.json"))?;
        let mut request: Request = serde_json::from_value(cases[0]["request"].clone())?;
        let boundary = midnight("2022-01-01".parse()?)?;
        request.trading_day = "2022-01-03".parse()?;
        request.segments = vec![(boundary - 60_000_000_000, boundary + 60_000_000_000)];
        request.freeze_ns = boundary + 300_000_000_000;
        request.decision_ns = boundary + 360_000_000_000;
        let input = tmp.path().join("requests.json");
        fs::write(&input, serde_json::to_vec(&vec![request])?)?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(
            &options,
            &source,
            [CF_REPLAY_META, "p:2021:TY", "p:2022:TY"],
        )?;
        for (year, published, volume) in [
            (2021, boundary - 1_000_000_000, 3),
            (2022, boundary + 1_000_000_000, 4),
        ] {
            let mut t = cme_tas_replay::ali_h26_fixture();
            t.ric = "TYH2".into();
            t.ts_utc_ns = published;
            t.exch_hms_ns = MISSING_EXCH_HMS_NS;
            t.volume = volume;
            let name = product_cf_name(year, "TY")?;
            db.put_cf(
                &db.cf_handle(&name).unwrap(),
                encode_all_key(KIND_CME_TRADE, &t.ric, published, 0, 0)?,
                cme_tas_replay::encode_cme_trade(&t)?,
            )?;
        }
        db.put_cf(
            &db.cf_handle(CF_REPLAY_META).unwrap(),
            cme_tas_replay::period_meta_key("2021-01-01_2022-01-01")?,
            cme_tas_replay::encode_period_status(PeriodStatus::Done),
        )?;
        drop(db);
        let output = tmp.path().join("out.json");
        assert!(run(Args {
            rocksdb_dir: source.clone(),
            requests: input.clone(),
            output: output.clone()
        })
        .is_err());
        assert!(!output.exists());
        let db = DB::open_cf(
            &options,
            &source,
            [CF_REPLAY_META, "p:2021:TY", "p:2022:TY"],
        )?;
        db.put_cf(
            &db.cf_handle(CF_REPLAY_META).unwrap(),
            cme_tas_replay::period_meta_key("2022-01-01_2023-01-01")?,
            cme_tas_replay::encode_period_status(PeriodStatus::Done),
        )?;
        drop(db);
        run(Args {
            rocksdb_dir: source,
            requests: input,
            output: output.clone(),
        })?;
        let actual: serde_json::Value = serde_json::from_slice(&fs::read(output)?)?;
        assert_eq!(
            actual["volumes"][0]["rows"][0],
            serde_json::json!({"ric":"TYH2","volume":7,"volume_lower":7,"volume_upper":7})
        );
        assert_eq!(actual["volumes"][0]["rows"][1]["volume"], 0);
        assert_eq!(actual["volumes"][0]["complete"], true);
        Ok(())
    }
}
