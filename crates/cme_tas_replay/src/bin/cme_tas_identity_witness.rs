//! Read-only exact-RIC witnesses from publication-keyed, unaggregated messages.
//! This proves neither first listing nor tradability nor candidate completeness.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Datelike, Utc};
use clap::Parser;
use cme_tas_replay::product::{encode_all_key, product_cf_name, quote_last_merge};
use cme_tas_replay::{
    decode_cme_settlement, decode_cme_status, decode_cme_trade, decode_period_status,
    decode_reference_change, decode_symbology_change, PeriodStatus, CF_REPLAY_META,
    KIND_CME_SETTLEMENT, KIND_CME_STATUS, KIND_CME_TRADE, KIND_REFERENCE_CHANGE,
    KIND_SYMBOLOGY_CHANGE, PERIOD_META_PREFIX, RIC_LEN,
};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs::OpenOptions, io::Write, path::PathBuf};

const KINDS: [u8; 5] = [
    KIND_CME_TRADE,
    KIND_SYMBOLOGY_CHANGE,
    KIND_CME_SETTLEMENT,
    KIND_CME_STATUS,
    KIND_REFERENCE_CHANGE,
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    requests: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Request {
    product: String,
    ric: String,
    year: u16,
    start_ns: u64,
    cutoff_ns: u64,
}

#[derive(Debug, Serialize)]
struct Witness {
    kind: u8,
    ric: String,
    published_ns: u64,
    part: u16,
    seq: u32,
    key_hex: String,
    value_hex: String,
}

#[derive(Serialize)]
struct Evidence {
    request: Request,
    period: String,
    searched_kinds: [u8; 5],
    witnesses: Vec<Witness>,
}

fn opts() -> Options {
    let mut o = Options::default();
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    o.set_max_open_files(128);
    o.set_skip_stats_update_on_db_open(true);
    o
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn validate(r: &Request) -> Result<()> {
    ensure!(r.start_ns < r.cutoff_ns, "empty request");
    ensure!(
        cme_roll_metadata::exchange(&r.product).is_some(),
        "unsupported product"
    );
    encode_all_key(KIND_CME_TRADE, &r.ric, r.start_ns, 0, 0)?;
    for ns in [r.start_ns, r.cutoff_ns - 1] {
        let seconds = i64::try_from(ns / 1_000_000_000)?;
        let time = DateTime::<Utc>::from_timestamp(seconds, 0).context("invalid timestamp")?;
        ensure!(
            time.year() == i32::from(r.year),
            "request crosses source year"
        );
    }
    Ok(())
}

fn typed_identity(kind: u8, value: &[u8]) -> Result<(String, u64)> {
    Ok(match kind {
        KIND_CME_TRADE => {
            let r = decode_cme_trade(value)?;
            (r.ric, r.ts_utc_ns)
        }
        KIND_SYMBOLOGY_CHANGE => {
            let r = decode_symbology_change(value)?;
            (r.ric, r.ts_utc_ns)
        }
        KIND_CME_SETTLEMENT => {
            let r = decode_cme_settlement(value)?;
            (r.ric, r.ts_utc_ns)
        }
        KIND_CME_STATUS => {
            let r = decode_cme_status(value)?;
            (r.ric, r.ts_utc_ns)
        }
        KIND_REFERENCE_CHANGE => {
            let r = decode_reference_change(value)?;
            (r.ric, r.ts_utc_ns)
        }
        _ => anyhow::bail!("unsupported raw kind"),
    })
}

fn scan(db: &DB, r: &Request) -> Result<Evidence> {
    validate(r)?;
    let meta = db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta")?;
    let mut matching = Vec::new();
    for item in db.iterator_cf(&meta, IteratorMode::Start) {
        let (key, value) = item?;
        if let Some(raw) = key.strip_prefix(PERIOD_META_PREFIX.as_bytes()) {
            let period = std::str::from_utf8(raw)?;
            if period.starts_with(&format!("{}-", r.year)) {
                ensure!(
                    decode_period_status(&value)? == PeriodStatus::Done,
                    "source period incomplete"
                );
                matching.push(period.to_owned());
            }
        }
    }
    ensure!(matching.len() == 1, "missing or ambiguous done source year");
    let name = product_cf_name(r.year, &r.product)?;
    let cf = db.cf_handle(&name).context("missing product partition")?;
    let mut witnesses = Vec::new();
    for kind in KINDS {
        let start = encode_all_key(kind, &r.ric, r.start_ns, 0, 0)?;
        let mut iter = db.raw_iterator_cf(&cf);
        iter.seek(start);
        if iter.valid() {
            let key = iter.key().context("missing key")?;
            if key.starts_with(&start[..1 + RIC_LEN]) {
                ensure!(key.len() == start.len(), "unexpected key length");
                let key_ns = u64::from_be_bytes(key[17..25].try_into()?);
                ensure!(key_ns >= r.start_ns, "seek before start");
                if key_ns < r.cutoff_ns {
                    let value = iter.value().context("missing value")?;
                    let (ric, published_ns) = typed_identity(kind, value)?;
                    ensure!(ric == r.ric && published_ns == key_ns, "key/value mismatch");
                    witnesses.push(Witness {
                        kind,
                        ric,
                        published_ns,
                        part: u16::from_be_bytes(key[25..27].try_into()?),
                        seq: u32::from_be_bytes(key[27..31].try_into()?),
                        key_hex: hex(key),
                        value_hex: hex(value),
                    });
                }
            }
        }
        iter.status()?;
    }
    Ok(Evidence {
        request: r.clone(),
        period: matching.remove(0),
        searched_kinds: KINDS,
        witnesses,
    })
}

fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(!a.output.exists(), "output already exists");
    let requests: Vec<Request> = serde_json::from_slice(&std::fs::read(&a.requests)?)?;
    ensure!(!requests.is_empty(), "empty requests");
    let mut names = BTreeSet::from(["default".to_owned(), CF_REPLAY_META.to_owned()]);
    let mut unique = BTreeSet::new();
    for r in &requests {
        validate(r)?;
        ensure!(
            unique.insert((
                r.product.clone(),
                r.ric.clone(),
                r.year,
                r.start_ns,
                r.cutoff_ns
            )),
            "duplicate request"
        );
        names.insert(product_cf_name(r.year, &r.product)?);
    }
    let db = DB::open_cf_descriptors_read_only(
        &opts(),
        &a.rocksdb_dir,
        names.iter().map(|n| ColumnFamilyDescriptor::new(n, opts())),
        false,
    )?;
    let rows = requests
        .iter()
        .map(|r| scan(&db, r))
        .collect::<Result<Vec<_>>>()?;
    let raw = serde_json::to_vec_pretty(&rows)?;
    if let Some(parent) = a.output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let partial = a.output.with_extension("json.partial");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)?;
    file.write_all(&raw)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(partial, &a.output)?;
    eprintln!(
        "requests={} witnesses={}",
        rows.len(),
        rows.iter().map(|r| r.witnesses.len()).sum::<usize>()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::{
        encode_cme_status, encode_period_status, period_meta_key, SlimStatus, MISSING_EXCH_HMS_NS,
    };

    #[test]
    fn exact_ric_publication_cutoff_and_period_are_required() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut options = opts();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let name = product_cf_name(2021, "JY")?;
        let db = DB::open_cf_descriptors(
            &options,
            dir.path(),
            ["default", CF_REPLAY_META, &name].map(|n| ColumnFamilyDescriptor::new(n, opts())),
        )?;
        let base = 1_609_459_200_000_000_000;
        let mut request = Request {
            product: "JY".into(),
            ric: "JYH1".into(),
            year: 2021,
            start_ns: base,
            cutoff_ns: base + 100,
        };
        let cf = db.cf_handle(&name).unwrap();
        for (ric, timestamp) in [("JYG1", base + 1), ("JYH1", base + 100), ("JYJ1", base + 2)] {
            let row = SlimStatus {
                ric: ric.into(),
                ts_utc_ns: timestamp,
                exch_hms_ns: MISSING_EXCH_HMS_NS,
                qualifiers: "H[TRD_STATUS]".into(),
            };
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_STATUS, ric, timestamp, 2, 3)?,
                encode_cme_status(&row)?,
            )?;
        }
        assert!(scan(&db, &request).is_err());
        let meta = db.cf_handle(CF_REPLAY_META).unwrap();
        let key = period_meta_key("2021-01-01_2022-01-01")?;
        db.put_cf(&meta, &key, encode_period_status(PeriodStatus::Writing))?;
        assert!(scan(&db, &request).is_err());
        db.put_cf(&meta, &key, encode_period_status(PeriodStatus::Done))?;
        assert!(scan(&db, &request)?.witnesses.is_empty());
        request.cutoff_ns += 1;
        let rows = scan(&db, &request)?.witnesses;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].published_ns, rows[0].part, rows[0].seq),
            (base + 100, 2, 3)
        );
        request.ric = "JYI1".into();
        assert!(scan(&db, &request)?.witnesses.is_empty());
        Ok(())
    }
}
