//! Read-only contract enumeration and deterministic minute spot checks.
use anyhow::{bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_1min::{decode_ll2_minute, ll2_latest_merge};
use cme_tas_replay::product::quote_last_merge;
use cme_tas_replay::{decode_ric, period_meta_key, MISSING_PRICE};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    tas_db: PathBuf,
    #[arg(long)]
    raw_db: PathBuf,
    #[arg(long)]
    normalized_db: PathBuf,
    #[arg(long, num_args=1..)]
    years: Vec<u16>,
    #[arg(long)]
    output: PathBuf,
    /// Enumerate TAS only; use an existing endpoint audit for the LL2 join.
    #[arg(long)]
    trade_only: bool,
    /// Exclusive UTC end date for one partial-year source period.
    #[arg(long)]
    period_end: Option<String>,
    #[arg(long)]
    descriptions: bool,
}
fn open(path: &Path, kind: &str) -> Result<DB> {
    eprintln!("opening {kind}: {}", path.display());
    let mut o = Options::default();
    o.set_max_open_files(65536);
    let desc = DB::list_cf(&o, path)?.into_iter().map(|n| {
        let mut c = Options::default();
        if n.starts_with("p:") {
            match kind {
                "tas" => c.set_merge_operator_associative("quote_last", quote_last_merge),
                "normalized" => c.set_merge_operator_associative("ll2_latest", ll2_latest_merge),
                _ => (),
            }
        }
        ColumnFamilyDescriptor::new(n, c)
    });
    Ok(DB::open_cf_descriptors_read_only(&o, path, desc, false)?)
}
fn done(db: &DB, year: u16, end: &str) -> Result<bool> {
    let cf = db.cf_handle("replay_meta").context("replay_meta")?;
    Ok(db
        .get_cf(&cf, period_meta_key(&format!("{year}-01-01_{end}"))?)?
        .as_deref()
        == Some(b"done"))
}
fn valid_levels(v: &cme_tas_replay::ll2_1min::Ll2Minute, known: u64) -> usize {
    (0..10)
        .take_while(|&l| {
            let mask = 27u64 << (l * 6);
            known & mask == mask
                && v.bid_prices[l] != MISSING_PRICE
                && v.ask_prices[l] != MISSING_PRICE
                && v.bid_prices[l] > 0
                && v.ask_prices[l] >= v.bid_prices[l]
                && v.bid_sizes[l] >= 0
                && v.ask_sizes[l] >= 0
        })
        .count()
}
fn point(
    db: &DB,
    name: &str,
    prefix: &[u8],
    lo: u64,
    hi: u64,
    last: bool,
    raw: bool,
) -> Result<Value> {
    let Some(cf) = db.cf_handle(name) else {
        return Ok(Value::Null);
    };
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(prefix);
    key[16..].copy_from_slice(&(if last { hi - 1 } else { lo }).to_be_bytes());
    let mut it = db.raw_iterator_cf(&cf);
    if last {
        it.seek_for_prev(key);
    } else {
        it.seek(key);
    }
    it.status()?;
    let Some(k) = it.key() else {
        return Ok(Value::Null);
    };
    if k.len() != 24 || k[..16] != *prefix {
        return Ok(Value::Null);
    }
    let ts = u64::from_be_bytes(k[16..].try_into()?);
    if ts < lo || ts >= hi {
        return Ok(Value::Null);
    }
    let v = decode_ll2_minute(it.value().context("value")?)?;
    let known = if raw {
        let q = db.cf_handle(&format!("q:{name}")).context("quality cf")?;
        u64::from_le_bytes(
            db.get_cf(&q, k)?
                .context("quality value")?
                .as_slice()
                .try_into()?,
        )
    } else {
        (1u64 << 61) - 1
    };
    Ok(json!({"bucket_ns":ts,"source_ns":v.source_ts_utc_ns,"valid_levels":valid_levels(&v,known)}))
}
fn main() -> Result<()> {
    run(Args::parse())
}
fn descriptions(
    db: &DB,
    name: &str,
    ric_prefix: &[u8],
    start: u64,
    end: u64,
) -> Result<Vec<Value>> {
    let cf = db.cf_handle(name).context("TAS CF")?;
    let mut key = [0u8; 31];
    key[0] = 14;
    key[1..17].copy_from_slice(ric_prefix);
    key[17..25].copy_from_slice(&start.to_be_bytes());
    let mut it = db.raw_iterator_cf(&cf);
    it.seek(key);
    let mut result = Vec::new();
    for _ in 0..32 {
        it.status()?;
        let Some(k) = it.key() else { break };
        if k.len() != 31 || k[..17] != key[..17] || u64::from_be_bytes(k[17..25].try_into()?) >= end
        {
            break;
        }
        let v = cme_tas_replay::decode_reference_change(it.value().context("reference value")?)?;
        if v.change_type == cme_tas_replay::CHANGE_TYPE_DESCRIPTION {
            result.push(json!({"ts_ns":v.ts_utc_ns,"old":v.old_value,"new":v.new_value}));
        }
        it.next();
    }
    Ok(result)
}
fn run(a: Args) -> Result<()> {
    if a.period_end.is_some() && a.years.len() != 1 {
        bail!("period-end requires exactly one year");
    }
    let tas = open(&a.tas_db, "tas")?;
    let norm = if a.trade_only {
        None
    } else {
        Some(open(&a.normalized_db, "normalized")?)
    };
    let raw = if a.trade_only {
        None
    } else {
        Some(open(&a.raw_db, "raw")?)
    };
    let mut rows = Vec::new();
    for year in &a.years {
        let end_date = a
            .period_end
            .clone()
            .unwrap_or_else(|| format!("{}-01-01", year + 1));
        let end_day = chrono::NaiveDate::parse_from_str(&end_date, "%Y-%m-%d")?;
        let first_day = chrono::NaiveDate::from_ymd_opt(i32::from(*year), 1, 1).unwrap();
        let next_year = chrono::NaiveDate::from_ymd_opt(i32::from(*year) + 1, 1, 1).unwrap();
        if end_day <= first_day || end_day > next_year {
            bail!("period-end must be within the selected year");
        }
        if !done(&tas, *year, &end_date)? {
            bail!("TAS year {year} not done");
        }
        let raw_done = raw
            .as_ref()
            .map(|db| done(db, *year, &end_date))
            .transpose()?
            .unwrap_or(false);
        let norm_done = norm
            .as_ref()
            .map(|db| done(db, *year, &end_date))
            .transpose()?
            .unwrap_or(false);
        let start = chrono::NaiveDate::from_ymd_opt(i32::from(*year), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp() as u64
            * 1_000_000_000;
        let end =
            end_day.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as u64 * 1_000_000_000;
        let mut names = DB::list_cf(&Options::default(), &a.tas_db)?;
        names.sort();
        for name in names
            .into_iter()
            .filter(|n| n.starts_with(&format!("p:{year}:")))
        {
            let cf = tas.cf_handle(&name).context("tas cf")?;
            let mut it = tas.raw_iterator_cf(&cf);
            it.seek([1u8]);
            loop {
                it.status()?;
                let Some(k) = it.key() else { break };
                if k.first() != Some(&1) {
                    break;
                }
                if k.len() != 31 {
                    bail!("invalid trade key");
                }
                let prefix = k[..17].to_vec();
                let ric = decode_ric(&prefix[1..])?;
                // Seek the beginning and end of this RIC's trade block; never walk its tape.
                let mut low = [0u8; 31];
                low[..17].copy_from_slice(&prefix);
                low[17..25].copy_from_slice(&start.to_be_bytes());
                it.seek(low);
                it.status()?;
                let first = it
                    .key()
                    .filter(|k| k[..17] == prefix)
                    .map(|k| u64::from_be_bytes(k[17..25].try_into().unwrap()));
                let mut upper = [255u8; 31];
                upper[..17].copy_from_slice(&prefix);
                upper[17..25].copy_from_slice(&(end - 1).to_be_bytes());
                it.seek_for_prev(upper);
                it.status()?;
                let last = it
                    .key()
                    .filter(|k| k[..17] == prefix)
                    .map(|k| u64::from_be_bytes(k[17..25].try_into().unwrap()));
                if let (Some(first), Some(last)) = (first, last) {
                    if first <= last && first < end && last >= start {
                        let mut mid = low;
                        mid[17..25].copy_from_slice(&(first + (last - first) / 2).to_be_bytes());
                        it.seek(mid);
                        it.status()?;
                        let middle = u64::from_be_bytes(
                            it.key().context("middle trade")?[17..25].try_into()?,
                        );
                        let raw_name = format!("s15:{name}");
                        let raw_name = if raw
                            .as_ref()
                            .is_some_and(|db| db.cf_handle(&raw_name).is_some())
                        {
                            raw_name
                        } else {
                            name.clone()
                        };
                        let endpoints = |db: Option<&DB>,
                                         n: &str,
                                         is_raw: bool,
                                         complete: bool|
                         -> Result<Value> {
                            if !complete {
                                return Ok(Value::Null);
                            }
                            let db = db.context("LL2 database")?;
                            Ok(
                                json!({"first":point(db,n,&prefix[1..],start,end,false,is_raw)?,"last":point(db,n,&prefix[1..],start,end,true,is_raw)?}),
                            )
                        };
                        let mut checks = Vec::new();
                        let mut times = vec![first, middle, last];
                        times.sort();
                        times.dedup();
                        for t in times {
                            let minute = t / 60_000_000_000 * 60_000_000_000;
                            checks.push(json!({"trade_ns":t,"minute_ns":minute,
                                "raw":if raw_done{point(raw.as_ref().context("raw DB")?,&raw_name,&prefix[1..],minute,minute+60_000_000_000,true,true)?}else{Value::Null},
                                "normalized":if norm_done{point(norm.as_ref().context("normalized DB")?,&name,&prefix[1..],minute,minute+60_000_000_000,true,false)?}else{Value::Null}}));
                        }
                        rows.push(json!({"year":year,"product_cf":name,"ric":ric,"is_index":ric.starts_with('.'),
                            "first_trade_ns":first,"last_trade_ns":last,"raw_done":raw_done,"normalized_done":norm_done,
                            "raw":endpoints(raw.as_ref(),&raw_name,true,raw_done)?,"normalized":endpoints(norm.as_ref(),&name,false,norm_done)?,"minute_spot_checks":checks,
                            "descriptions":if a.descriptions{descriptions(&tas,&name,&prefix[1..],start,end)?}else{Vec::new()}}));
                    }
                }
                // The maximal timestamp/part/seq is outside any real trade; next RIC in one seek.
                let mut next = [255u8; 31];
                next[..17].copy_from_slice(&prefix);
                it.seek(next);
                if it.key() == Some(next.as_slice()) {
                    it.next();
                }
            }
            eprintln!("audited {name}; contracts={}", rows.len());
        }
    }
    if let Some(p) = a.output.parent() {
        fs::create_dir_all(p)?;
    }
    fs::write(
        &a.output,
        serde_json::to_vec_pretty(
            &json!({"measured_at":chrono::Utc::now().to_rfc3339(),"tas_db":a.tas_db,"raw_db":a.raw_db,"normalized_db":a.normalized_db,"trade_only":a.trade_only,"period_end":a.period_end,"rows":rows}),
        )?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::encode_ric;
    use cme_tas_replay::ll2_1min::{encode_ll2_minute, Ll2Minute};

    #[test]
    fn enumerates_trade_ric_blocks_and_excludes_quote_only_products() -> Result<()> {
        use cme_tas_replay::product::encode_all_key;
        let dir = tempfile::tempdir()?;
        let mut o = Options::default();
        o.create_if_missing(true);
        o.create_missing_column_families(true);
        let tas = dir.path().join("tas");
        let raw = dir.path().join("raw");
        let norm = dir.path().join("norm");
        let start = 1_735_689_600_000_000_000u64;
        for path in [&tas, &raw, &norm] {
            let db = DB::open_cf(&o, path, ["p:2025:ES", "p:2025:IGN", "replay_meta"])?;
            db.put_cf(
                &db.cf_handle("replay_meta").unwrap(),
                period_meta_key("2025-01-01_2026-01-01")?,
                b"done",
            )?;
            if path == &tas {
                let cf = db.cf_handle("p:2025:ES").unwrap();
                for n in 1..=3 {
                    db.put_cf(
                        &cf,
                        encode_all_key(1, "ESH25", start + n * 1_000_000_000, 0, 0)?,
                        [0u8; 80],
                    )?;
                }
                db.put_cf(
                    &cf,
                    encode_all_key(1, "ESM25", start + 4_000_000_000, 0, 0)?,
                    [0u8; 80],
                )?;
                db.put_cf(&cf, encode_all_key(3, "ESU25", start, 0, 0)?, [0u8; 64])?;
                db.put_cf(
                    &db.cf_handle("p:2025:IGN").unwrap(),
                    encode_all_key(3, "IGNH25", start, 0, 0)?,
                    [0u8; 64],
                )?;
            }
        }
        let output = dir.path().join("result.json");
        run(Args {
            tas_db: tas,
            raw_db: raw,
            normalized_db: norm,
            years: vec![2025],
            output: output.clone(),
            trade_only: false,
            period_end: None,
            descriptions: false,
        })?;
        let result: Value = serde_json::from_slice(&fs::read(output)?)?;
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["ric"], "ESH25");
        assert_eq!(rows[1]["ric"], "ESM25");
        assert_eq!(rows[0]["first_trade_ns"], start + 1_000_000_000);
        assert_eq!(rows[0]["last_trade_ns"], start + 3_000_000_000);
        assert_eq!(
            rows[0]["minute_spot_checks"][1]["trade_ns"],
            start + 2_000_000_000
        );
        assert!(rows[1]["raw"]["first"].is_null());
        Ok(())
    }

    #[test]
    fn minute_uses_last_invalidation_and_never_another_ric_or_minute() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut o = Options::default();
        o.create_if_missing(true);
        o.create_missing_column_families(true);
        let db = DB::open_cf(&o, dir.path(), ["s15:p:2025:ES", "q:s15:p:2025:ES"])?;
        let name = "s15:p:2025:ES";
        let cf = db.cf_handle(name).unwrap();
        let q = db.cf_handle(&format!("q:{name}")).unwrap();
        let ric = encode_ric("ESH25")?;
        let v = Ll2Minute {
            source_ts_utc_ns: 60_000_000_001,
            source_order: 1,
            exch_time_ns: 0,
            gmt_offset_minutes: 0,
            bid_prices: [100; 10],
            ask_prices: [101; 10],
            bid_sizes: [1; 10],
            ask_sizes: [1; 10],
            bid_counts: [1; 10],
            ask_counts: [1; 10],
        };
        let mut key = [0u8; 24];
        key[..16].copy_from_slice(&ric);
        key[16..].copy_from_slice(&60_000_000_000u64.to_be_bytes());
        db.put_cf(&cf, key, encode_ll2_minute(&v))?;
        db.put_cf(&q, key, ((1u64 << 61) - 1).to_le_bytes())?;
        assert_eq!(
            point(&db, name, &ric, 60_000_000_000, 120_000_000_000, true, true)?["valid_levels"],
            10
        );
        key[16..].copy_from_slice(&105_000_000_000u64.to_be_bytes());
        db.put_cf(&cf, key, encode_ll2_minute(&v))?;
        db.put_cf(&q, key, 0u64.to_le_bytes())?;
        assert_eq!(
            point(&db, name, &ric, 60_000_000_000, 120_000_000_000, true, true)?["valid_levels"],
            0
        );
        assert!(point(
            &db,
            name,
            &ric,
            120_000_000_000,
            180_000_000_000,
            true,
            true
        )?
        .is_null());
        assert!(point(
            &db,
            name,
            &encode_ric("ESM25")?,
            60_000_000_000,
            120_000_000_000,
            true,
            true
        )?
        .is_null());
        Ok(())
    }
}
