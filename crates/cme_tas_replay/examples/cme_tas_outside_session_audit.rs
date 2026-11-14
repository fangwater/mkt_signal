//! Read-only audit of one indexed TAS window and a supplied open interval.
use anyhow::{bail, Context, Result};
use cme_tas_replay::product::{encode_all_key, exch_event_time_ns, quote_last_merge};
use cme_tas_replay::{decode_cme_trade, KIND_CME_TRADE, MISSING_EXCH_HMS_NS};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};

fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    if a.len() != 9 {
        bail!("usage: db cf ric_csv window_start_ns window_end_ns open_start_ns open_end_ns output_json");
    }
    let lo: u64 = a[4].parse()?;
    let hi: u64 = a[5].parse()?;
    let open_lo: u64 = a[6].parse()?;
    let open_hi: u64 = a[7].parse()?;
    let mut options = Options::default();
    options.set_max_open_files(512);
    options.set_skip_stats_update_on_db_open(true);
    let descriptors = DB::list_cf(&options, &a[1])?.into_iter().map(|name| {
        let mut o = Options::default();
        o.set_merge_operator_associative("quote_last", quote_last_merge);
        ColumnFamilyDescriptor::new(name, o)
    });
    let db = DB::open_cf_descriptors_read_only(&options, &a[1], descriptors, false)?;
    let cf = db.cf_handle(&a[2]).context("selected CF")?;
    let mut excluded = Vec::new();
    for ric in a[3].split(',') {
        let key = encode_all_key(
            KIND_CME_TRADE,
            ric,
            lo.saturating_sub(43_200_000_000_000),
            0,
            0,
        )?;
        for item in db.iterator_cf(cf, IteratorMode::From(&key, Direction::Forward)) {
            let (k, v) = item?;
            if k[..17] != key[..17] {
                break;
            }
            let source = u64::from_be_bytes(k[17..25].try_into()?);
            if source > hi + 43_200_000_000_000 {
                break;
            }
            let t = decode_cme_trade(&v)?;
            let event = if t.exch_hms_ns == MISSING_EXCH_HMS_NS {
                t.ts_utc_ns
            } else {
                exch_event_time_ns(t.ts_utc_ns, t.exch_hms_ns)?
            };
            if event >= lo && event < hi && !(event >= open_lo && event < open_hi) {
                excluded.push(serde_json::json!({"ric":ric,"source_ts_utc_ns":source,"event_ts_utc_ns":event,"exch_hms_ns":t.exch_hms_ns,"price_e9":t.price,"volume":t.volume,"aggressor":t.aggressor,"key_hex":k.iter().map(|b|format!("{b:02x}")).collect::<String>()}));
            }
        }
    }
    let report = serde_json::json!({"database":a[1],"cf":a[2],"rics":a[3],"window_ns":[lo,hi],"open_interval_ns":[open_lo,open_hi],"outside_session_trades":excluded});
    std::fs::write(&a[8], serde_json::to_string_pretty(&report)? + "\n")?;
    println!("{report}");
    Ok(())
}
