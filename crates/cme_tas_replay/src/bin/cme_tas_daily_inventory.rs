//! Read-only inventory of new-root TAS trades and timestamped reference changes.
//! Deliberately does NOT certify sessions, historical availability, or zero days.
use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use clap::Parser;
use cme_tas_replay::daily_inventory::{add, event_time, trading_day, Event};
use cme_tas_replay::product::{encode_all_key, product_cf_name, quote_last_merge};
use cme_tas_replay::{
    decode_cme_trade, decode_period_status, decode_reference_change, PeriodStatus, CF_REPLAY_META,
    KIND_CME_TRADE, KIND_REFERENCE_CHANGE, MISSING_EXCH_HMS_NS, MISSING_PRICE, PERIOD_META_PREFIX,
};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    path::PathBuf,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    start: NaiveDate,
    #[arg(long)]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',')]
    products: Vec<String>,
}

fn opts() -> Options {
    let mut o = Options::default();
    o.set_merge_operator_associative("quote_last", quote_last_merge);
    o.set_max_open_files(256);
    o.set_skip_stats_update_on_db_open(true);
    o
}

#[derive(Serialize)]
struct Reference {
    product: String,
    ric: String,
    published_ns: u64,
    change_type: u8,
    old_value: String,
    new_value: String,
}

fn run(a: Args) -> Result<()> {
    if a.end < a.start || a.products.is_empty() {
        bail!("invalid range or empty products");
    }
    let products = a.products.iter().collect::<std::collections::BTreeSet<_>>();
    if products.len() != a.products.len() {
        bail!("duplicate product");
    }
    for p in &products {
        if !["URO", "JY", "HG", "SI", "TU", "FV", "TY", "TN", "US", "AUL"].contains(&p.as_str()) {
            bail!("unsupported inventory product {p}");
        }
    }
    fs::create_dir_all(&a.output_dir)?;
    let meta_db = DB::open_cf_descriptors_read_only(
        &opts(),
        &a.rocksdb_dir,
        ["default", CF_REPLAY_META].map(|n| ColumnFamilyDescriptor::new(n, opts())),
        false,
    )?;
    let meta = meta_db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta")?;
    let mut periods = BTreeMap::new();
    for item in meta_db.iterator_cf(&meta, IteratorMode::Start) {
        let (k, v) = item?;
        let Some(raw) = k.strip_prefix(PERIOD_META_PREFIX.as_bytes()) else {
            continue;
        };
        let period = std::str::from_utf8(raw)?;
        if decode_period_status(&v)? != PeriodStatus::Done {
            bail!("period writing: {period}");
        }
        let year: i32 = period.get(..4).context("bad period")?.parse()?;
        if periods.insert(year, period.to_string()).is_some() {
            bail!("ambiguous year {year}");
        }
    }
    drop(meta_db);
    // Source-time padding only bounds the scan; labels use recovered exchange time.
    let lo = (a.start - Duration::days(2))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp() as u64
        * 1_000_000_000;
    let hi = (a.end + Duration::days(2))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp() as u64
        * 1_000_000_000;
    for year in a.start.year()..=a.end.year() {
        let period = periods
            .get(&year)
            .with_context(|| format!("no done period for {year}"))?;
        for product in &products {
            let name = product_cf_name(year as u16, product)?;
            let path = a.output_dir.join(format!("{year}_{product}.csv"));
            let partial = path.with_extension("csv.partial");
            if path.exists() {
                bail!("output exists: {}", path.display());
            }
            let db = DB::open_cf_descriptors_read_only(
                &opts(),
                &a.rocksdb_dir,
                ["default", &name].map(|n| ColumnFamilyDescriptor::new(n, opts())),
                false,
            )?;
            let cf = db.cf_handle(&name).context("missing product CF")?;
            let mut totals = BTreeMap::new();
            let mut iter = db.raw_iterator_cf(&cf);
            iter.seek([KIND_CME_TRADE]);
            let mut scanned = 0u64;
            while iter.valid() {
                let key = iter.key().context("iterator key")?;
                if key[0] != KIND_CME_TRADE {
                    break;
                }
                let rec = decode_cme_trade(iter.value().context("iterator value")?)?;
                if rec.ts_utc_ns < lo {
                    iter.seek(encode_all_key(KIND_CME_TRADE, &rec.ric, lo, 0, 0)?);
                    continue;
                }
                if rec.ts_utc_ns >= hi {
                    let mut next = key[..17].to_vec();
                    let i = next
                        .iter()
                        .rposition(|b| *b != 255)
                        .context("prefix overflow")?;
                    next[i] += 1;
                    next.truncate(i + 1);
                    iter.seek(next);
                    continue;
                }
                scanned += 1;
                let e = Event {
                    ric: rec.ric,
                    published_ns: rec.ts_utc_ns,
                    exchange_hms_ns: (rec.exch_hms_ns != MISSING_EXCH_HMS_NS)
                        .then_some(rec.exch_hms_ns),
                    price_e9: (rec.price != MISSING_PRICE).then_some(rec.price),
                    volume: rec.volume as u64,
                };
                let label = trading_day(event_time(&e)?)?;
                if label >= a.start && label <= a.end {
                    add(&mut totals, &e)?;
                }
                iter.next();
            }
            iter.status()?;
            let mut csv = csv::Writer::from_writer(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&partial)?,
            );
            for row in totals.values() {
                csv.serialize(row)?;
            }
            csv.flush()?;
            drop(csv);
            fs::rename(&partial, &path)?;
            let refpath = a.output_dir.join(format!("{year}_{product}_reference.csv"));
            let mut refs = csv::Writer::from_writer(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(refpath)?,
            );
            let mut refcount = 0u64;
            for item in db.iterator_cf(
                &cf,
                IteratorMode::From(&[KIND_REFERENCE_CHANGE], Direction::Forward),
            ) {
                let (key, val) = item?;
                if key[0] != KIND_REFERENCE_CHANGE {
                    break;
                }
                let r = decode_reference_change(&val)?;
                refs.serialize(Reference {
                    product: product.to_string(),
                    ric: r.ric,
                    published_ns: r.ts_utc_ns,
                    change_type: r.change_type,
                    old_value: r.old_value,
                    new_value: r.new_value,
                })?;
                refcount += 1;
            }
            refs.flush()?;
            let receipt = serde_json::json!({"period":period,"product":product,"scanned":scanned,
                "contract_days":totals.len(),"reference_changes":refcount,
                "start":a.start,"end":a.end,"session_validated":false,
                "availability_certified":false,"scope":"raw daily inventory, not formal volume"});
            fs::write(
                a.output_dir.join(format!("{year}_{product}.json")),
                serde_json::to_vec_pretty(&receipt)?,
            )?;
            eprintln!("{receipt}");
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    run(Args::parse())
}
