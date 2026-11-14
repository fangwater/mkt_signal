//! Audit whether typed LSEG trade corrections can be linked to RAW trades.

use anyhow::{bail, Context, Result};
use clap::Parser;
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use usstock_lseg_raw_replay::event_codec::{
    decode_correction, decode_trade, MSG_CANCEL, MSG_PREVIOUS_DAY, MSG_TRADE, MSG_TRADE_RESTATEMENT,
};
use usstock_lseg_raw_replay::quote_codec::{decode_key, encode_key};

#[derive(Debug, Parser)]
#[command(about = "Audit typed LSEG RAW cancellation/restatement linkage for one RIC")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    /// Print all RICs present in the RAW RocksDB and exit.
    #[arg(long)]
    list_rics: bool,
    #[arg(long)]
    ric: Option<String>,
}

#[derive(Default, Serialize)]
struct Counts {
    total: u64,
    with_trade_id: u64,
    with_sequence: u64,
    matched_trade_id: u64,
    matched_trade_id_and_sequence: u64,
    matched_trade_id_sequence_price_size: u64,
}

#[derive(Default, Serialize)]
struct Audit {
    ric: String,
    trades_with_trade_id: u64,
    cancel: Counts,
    previous_day: Counts,
    trade_restatement_messages: u64,
}

fn rows_of_kind<'a>(
    db: &'a DB,
    cf_name: &'a str,
    kind: u8,
) -> Result<impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a> {
    let cf = db
        .cf_handle(cf_name)
        .context("missing selected column family")?;
    let start = encode_key(kind, 0, 0);
    Ok(db
        .iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward))
        .map(move |row| {
            let (key, value) = row?;
            if decode_key(&key)?.0 != kind {
                bail!("end of requested message type")
            }
            Ok((key.to_vec(), value.to_vec()))
        })
        .take_while(|row| match row {
            Ok(_) => true,
            Err(error) if error.to_string() == "end of requested message type" => false,
            Err(_) => true,
        }))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let options = Options::default();
    let all = DB::list_cf(&options, &args.rocksdb_dir)?;
    if args.list_rics {
        let mut rics = all
            .iter()
            .filter_map(|name| name.strip_prefix("i:"))
            .collect::<Vec<_>>();
        rics.sort_unstable();
        for ric in rics {
            println!("{ric}");
        }
        return Ok(());
    }
    let ric = args
        .ric
        .context("--ric is required unless --list-rics is set")?;
    let instrument = format!("i:{ric}");
    let prefix = format!("v:{ric}:");
    let selected = all
        .into_iter()
        .filter(|name| name == "default" || name == &instrument || name.starts_with(&prefix))
        .collect::<Vec<_>>();
    if !selected.iter().any(|name| name == &instrument) {
        bail!("RAW RocksDB has no instrument column family {instrument}");
    }
    let db = DB::open_cf_for_read_only(&options, &args.rocksdb_dir, &selected, false)?;
    let mut audit = Audit {
        ric,
        ..Audit::default()
    };

    for name in &selected {
        if name == "default" {
            continue;
        }
        let mut trades = BTreeMap::<u64, Vec<(u64, i64, u64)>>::new();
        for row in rows_of_kind(&db, name, MSG_TRADE)? {
            let (_, value) = row?;
            let trade = decode_trade(&value)?;
            if trade.trade_id != u64::MAX {
                audit.trades_with_trade_id += 1;
                trades.entry(trade.trade_id).or_default().push((
                    trade.sequence,
                    trade.price,
                    trade.size,
                ));
            }
        }
        for (kind, counts) in [
            (MSG_CANCEL, &mut audit.cancel),
            (MSG_PREVIOUS_DAY, &mut audit.previous_day),
        ] {
            for row in rows_of_kind(&db, name, kind)? {
                let (_, value) = row?;
                let correction = decode_correction(&value)?;
                counts.total += 1;
                if correction.trade_id == u64::MAX {
                    continue;
                }
                counts.with_trade_id += 1;
                if correction.sequence != u64::MAX {
                    counts.with_sequence += 1;
                }
                let Some(candidates) = trades.get(&correction.trade_id) else {
                    continue;
                };
                counts.matched_trade_id += 1;
                if candidates
                    .iter()
                    .any(|(sequence, _, _)| *sequence == correction.sequence)
                {
                    counts.matched_trade_id_and_sequence += 1;
                }
                if candidates.iter().any(|(sequence, price, size)| {
                    *sequence == correction.sequence
                        && *price == correction.price
                        && *size == correction.size
                }) {
                    counts.matched_trade_id_sequence_price_size += 1;
                }
            }
        }
        audit.trade_restatement_messages +=
            rows_of_kind(&db, name, MSG_TRADE_RESTATEMENT)?.count() as u64;
    }
    println!("{}", serde_json::to_string_pretty(&audit)?);
    Ok(())
}
