//! Export verified raw CME settlement updates from RocksDB as CSV.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::{
    decode_cme_settlement, decode_ric, key_ts_utc_ns, CF_CME_SETTLEMENT, KEY_LEN, RIC_LEN,
};
use rocksdb::{IteratorMode, Options, DB};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

const DEFAULT_ROCKSDB_DIR: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb";
const DEFAULT_MAX_OPEN_FILES: i32 = 256;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_settlement_export")]
#[command(about = "Export raw cme_settlement records from a RocksDB secondary")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    secondary_dir: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = DEFAULT_MAX_OPEN_FILES)]
    max_open_files: i32,
    #[arg(long)]
    overwrite: bool,
}

fn open_secondary(args: &Args) -> Result<DB> {
    if args.max_open_files <= 0 {
        bail!("max_open_files must be positive");
    }
    if !args.rocksdb_dir.is_dir() {
        bail!(
            "rocksdb_dir {} is not a directory",
            args.rocksdb_dir.display()
        );
    }
    if let Some(parent) = args.secondary_dir.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create secondary parent {}", parent.display()))?;
    }
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    options.set_max_open_files(args.max_open_files);
    let names = DB::list_cf(&options, &args.rocksdb_dir)
        .with_context(|| format!("list column families {}", args.rocksdb_dir.display()))?;
    if !names.iter().any(|name| name == CF_CME_SETTLEMENT) {
        bail!("rocksdb has no {CF_CME_SETTLEMENT} column family");
    }
    let db = DB::open_cf_as_secondary(&options, &args.rocksdb_dir, &args.secondary_dir, &names)
        .with_context(|| {
            format!(
                "open RocksDB secondary {} from {}",
                args.secondary_dir.display(),
                args.rocksdb_dir.display()
            )
        })?;
    db.try_catch_up_with_primary()
        .context("catch up RocksDB secondary")?;
    Ok(db)
}

fn open_output(args: &Args) -> Result<BufWriter<File>> {
    if let Some(parent) = args.out.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output parent {}", parent.display()))?;
    }
    let mut options = OpenOptions::new();
    options.write(true);
    if args.overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let file = options
        .open(&args.out)
        .with_context(|| format!("open output {}", args.out.display()))?;
    Ok(BufWriter::with_capacity(4 * 1024 * 1024, file))
}

fn source_date_text(value: u32) -> Result<String> {
    if value == 0 {
        return Ok(String::new());
    }
    let year = value / 10_000;
    let month = (value / 100) % 100;
    let day = value % 100;
    if !(1900..=2100).contains(&year) || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        bail!("invalid settlement source date {value}");
    }
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let db = open_secondary(&args)?;
    let cf = db
        .cf_handle(CF_CME_SETTLEMENT)
        .ok_or_else(|| anyhow!("missing {CF_CME_SETTLEMENT} handle"))?;
    let mut output = open_output(&args)?;
    writeln!(
        output,
        "ric,source_date,source_date_yyyymmdd,published_ts_utc_ns,part,seq,price_e9"
    )?;

    let mut count = 0_u64;
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = item.context("iterate cme_settlement")?;
        if key.len() != KEY_LEN {
            bail!("cme_settlement key length {} is not {KEY_LEN}", key.len());
        }
        let record = decode_cme_settlement(&value).context("decode cme_settlement")?;
        let key_ric = decode_ric(&key[..RIC_LEN])?;
        let key_ts = key_ts_utc_ns(&key)?;
        if record.ric != key_ric || record.ts_utc_ns != key_ts {
            bail!(
                "cme_settlement key/value mismatch key=({key_ric},{key_ts}) value=({}, {})",
                record.ric,
                record.ts_utc_ns
            );
        }
        let part = u16::from_be_bytes(key[24..26].try_into().unwrap());
        let seq = u32::from_be_bytes(key[26..30].try_into().unwrap());
        writeln!(
            output,
            "{},{},{},{},{},{},{}",
            record.ric,
            source_date_text(record.source_date_yyyymmdd)?,
            record.source_date_yyyymmdd,
            record.ts_utc_ns,
            part,
            seq,
            record.price
        )?;
        count += 1;
    }
    output.flush().context("flush settlement export")?;
    eprintln!(
        "exported {count} cme_settlement rows to {}",
        args.out.display()
    );
    Ok(())
}
