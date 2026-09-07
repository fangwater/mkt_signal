use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::product::{product_cf_name, quote_last_merge};
use cme_tas_replay::{
    decode_cme_status, decode_period_status, PeriodStatus, CF_REPLAY_META, KIND_CME_STATUS,
    PERIOD_META_PREFIX,
};
use rayon::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_export_status")]
#[command(about = "Export TAS Mkt. Condition records from selected product-year column families")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    #[arg(long)]
    year: Option<u16>,
    #[arg(long)]
    start_ns: Option<u64>,
    #[arg(long)]
    end_ns: Option<u64>,
    #[arg(long)]
    list_partitions: bool,
    #[arg(long)]
    phase_actions_only: bool,
    #[arg(long = "product", required = true)]
    products: Vec<String>,
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", quote_last_merge);
    options
}

fn clean_tsv(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\t' | '\r' | '\n' => ' ',
            other => other,
        })
        .collect()
}

fn has_phase_action(qualifiers: &str) -> bool {
    if !qualifiers.contains("[TRD_TYPE]") {
        return false;
    }
    let mut close_type = false;
    let mut pre_open_type = false;
    let mut open_type_15 = false;
    let mut open_type_17 = false;
    let mut close_phase = false;
    let mut pre_open_phase = false;
    let mut trading_phase = false;
    for part in qualifiers.split(';') {
        let Some((raw_value, raw_name)) = part.rsplit_once('[') else {
            continue;
        };
        let Some(name) = raw_name.strip_suffix(']') else {
            continue;
        };
        let value = raw_value.trim().trim_matches('"').trim();
        match (name, value) {
            ("TRD_TYPE", "4") => close_type = true,
            ("TRD_TYPE", "21") => pre_open_type = true,
            ("TRD_TYPE", "15") => open_type_15 = true,
            ("TRD_TYPE", "17") => open_type_17 = true,
            ("INST_PHASE", "C") => close_phase = true,
            ("INST_PHASE", "O") => pre_open_phase = true,
            ("INST_PHASE", "T") => trading_phase = true,
            _ => {}
        }
    }
    (close_type && close_phase)
        || (pre_open_type && pre_open_phase)
        || open_type_15
        || (open_type_17 && trading_phase)
}

fn raw_status_has_trade_type(value: &[u8]) -> Result<bool> {
    let qualifiers = value
        .get(36..)
        .context("status value is too short to contain qualifiers")?;
    let used_length = qualifiers
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(qualifiers.len());
    Ok(qualifiers[..used_length]
        .windows(b"[TRD_TYPE]".len())
        .any(|window| window == b"[TRD_TYPE]"))
}

fn raw_status_timestamp(value: &[u8]) -> Result<u64> {
    let bytes = value
        .get(20..28)
        .context("status value is too short to contain a timestamp")?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

struct ProductScan {
    product: String,
    scanned: u64,
    in_range: u64,
    rows: Vec<String>,
}

fn scan_phase_actions(
    db: &DB,
    year: u16,
    product: &str,
    start_ns: u64,
    end_ns: u64,
) -> Result<ProductScan> {
    let name = product_cf_name(year, product)?;
    let cf = db
        .cf_handle(&name)
        .ok_or_else(|| anyhow!("missing column family {name}"))?;
    let mut scanned = 0_u64;
    let mut in_range = 0_u64;
    let mut rows = Vec::new();
    for item in db.iterator_cf(
        &cf,
        IteratorMode::From(&[KIND_CME_STATUS], Direction::Forward),
    ) {
        let (key, value) = item.with_context(|| format!("iterate {name}"))?;
        if key.first().copied() != Some(KIND_CME_STATUS) {
            break;
        }
        scanned += 1;
        let timestamp = raw_status_timestamp(&value)?;
        if timestamp < start_ns || timestamp >= end_ns {
            continue;
        }
        in_range += 1;
        if !raw_status_has_trade_type(&value)? {
            continue;
        }
        let record = decode_cme_status(&value)?;
        if !has_phase_action(&record.qualifiers) {
            continue;
        }
        rows.push(format!(
            "{}\t{}\t{}\t{}\t{}\n",
            product,
            record.ric,
            record.ts_utc_ns,
            record.exch_hms_ns,
            clean_tsv(&record.qualifiers)
        ));
    }
    Ok(ProductScan {
        product: product.to_string(),
        scanned,
        in_range,
        rows,
    })
}

fn selected_products(args: &Args) -> Result<Vec<String>> {
    let mut products = args
        .products
        .iter()
        .map(|product| product.trim().to_ascii_uppercase())
        .collect::<Vec<_>>();
    products.sort();
    products.dedup();
    if products.iter().any(String::is_empty) {
        bail!("product must not be empty");
    }
    Ok(products)
}

fn parse_partition_name(name: &str) -> Option<(u16, &str)> {
    let remainder = name.strip_prefix("p:")?;
    let (raw_year, product) = remainder.split_once(':')?;
    if product.is_empty() || product.contains(':') {
        return None;
    }
    Some((raw_year.parse().ok()?, product))
}

fn list_partitions(args: &Args, products: &[String]) -> Result<()> {
    let selected = products.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let names = DB::list_cf(&Options::default(), &args.rocksdb_dir)
        .with_context(|| format!("list column families in {}", args.rocksdb_dir.display()))?;
    if !names.iter().any(|name| name == CF_REPLAY_META) {
        bail!("column family {CF_REPLAY_META} is missing");
    }
    let descriptors = ["default", CF_REPLAY_META]
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect::<Vec<_>>();
    let db = DB::open_cf_descriptors_read_only(
        &Options::default(),
        &args.rocksdb_dir,
        descriptors,
        false,
    )
    .with_context(|| format!("open {} watermarks read-only", args.rocksdb_dir.display()))?;
    let meta = db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta column family after open")?;
    let mut done_periods = BTreeMap::new();
    for item in db.iterator_cf(&meta, IteratorMode::Start) {
        let (key, value) = item.context("scan replay period watermarks")?;
        let Some(period_bytes) = key.strip_prefix(PERIOD_META_PREFIX.as_bytes()) else {
            continue;
        };
        let period = std::str::from_utf8(period_bytes).context("period watermark is not UTF-8")?;
        let raw_year = period
            .get(..4)
            .with_context(|| format!("period watermark has no year: {period:?}"))?;
        let year: u16 = raw_year
            .parse()
            .with_context(|| format!("period watermark year is invalid: {period:?}"))?;
        match decode_period_status(&value)? {
            PeriodStatus::Done => {
                if let Some(previous) = done_periods.insert(year, period.to_string()) {
                    bail!("multiple done periods for year {year}: {previous:?}, {period:?}");
                }
            }
            PeriodStatus::Writing => {
                bail!("refuse partition discovery while period {period:?} is writing")
            }
        }
    }
    let mut partitions = names
        .iter()
        .filter_map(|name| parse_partition_name(name))
        .filter(|(year, product)| selected.contains(product) && done_periods.contains_key(year))
        .map(|(year, product)| (year, product, done_periods[&year].as_str()))
        .collect::<Vec<_>>();
    partitions.sort_unstable_by_key(|(year, product, _)| (*year, *product));

    let stdout = io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    writeln!(output, "year\tproduct\tperiod")?;
    for (year, product, period) in partitions {
        writeln!(output, "{year}\t{product}\t{period}")?;
    }
    output.flush()?;
    Ok(())
}

fn export_status(args: &Args, products: &[String]) -> Result<()> {
    let year = args
        .year
        .context("--year is required unless --list-partitions is used")?;
    let start_ns = args
        .start_ns
        .context("--start-ns is required unless --list-partitions is used")?;
    let end_ns = args
        .end_ns
        .context("--end-ns is required unless --list-partitions is used")?;
    if start_ns >= end_ns {
        bail!("start_ns must be less than end_ns");
    }

    let mut names = vec!["default".to_string()];
    for product in products {
        names.push(product_cf_name(year, product)?);
    }
    let descriptors = names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect::<Vec<_>>();
    let db = DB::open_cf_descriptors_read_only(
        &Options::default(),
        &args.rocksdb_dir,
        descriptors,
        false,
    )
    .with_context(|| format!("open {} read-only", args.rocksdb_dir.display()))?;

    let stdout = io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    writeln!(output, "product\tric\tts_utc_ns\texch_hms_ns\tqualifiers")?;
    output.flush()?;
    if args.phase_actions_only {
        let scans = products
            .par_iter()
            .map(|product| scan_phase_actions(&db, year, product, start_ns, end_ns))
            .collect::<Result<Vec<_>>>()?;
        for scan in scans {
            for row in &scan.rows {
                output.write_all(row.as_bytes())?;
            }
            eprintln!(
                "product={} year={year} scanned={} exported={} in_range={}",
                scan.product,
                scan.scanned,
                scan.rows.len(),
                scan.in_range
            );
        }
        output.flush()?;
        return Ok(());
    }
    for product in products {
        let name = product_cf_name(year, product)?;
        let cf = db
            .cf_handle(&name)
            .ok_or_else(|| anyhow!("missing column family {name}"))?;
        let mut scanned = 0_u64;
        let mut in_range = 0_u64;
        let mut exported = 0_u64;
        for item in db.iterator_cf(
            &cf,
            IteratorMode::From(&[KIND_CME_STATUS], Direction::Forward),
        ) {
            let (key, value) = item.with_context(|| format!("iterate {name}"))?;
            if key.first().copied() != Some(KIND_CME_STATUS) {
                break;
            }
            scanned += 1;
            let record = decode_cme_status(&value)?;
            if record.ts_utc_ns < start_ns || record.ts_utc_ns >= end_ns {
                continue;
            }
            in_range += 1;
            writeln!(
                output,
                "{}\t{}\t{}\t{}\t{}",
                product,
                record.ric,
                record.ts_utc_ns,
                record.exch_hms_ns,
                clean_tsv(&record.qualifiers)
            )?;
            exported += 1;
        }
        eprintln!(
            "product={product} year={} scanned={scanned} exported={exported} in_range={in_range}",
            year
        );
    }
    output.flush()?;
    Ok(())
}

fn run(args: &Args) -> Result<()> {
    let products = selected_products(args)?;
    if args.list_partitions {
        list_partitions(args, &products)
    } else {
        export_status(args, &products)
    }
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("cme_tas_export_status failed: {error:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{clean_tsv, has_phase_action, parse_partition_name};

    #[test]
    fn tsv_control_characters_are_replaced() {
        assert_eq!(clean_tsv("a\tb\nc\rd"), "a b c d");
    }

    #[test]
    fn product_partition_names_are_parsed_strictly() {
        assert_eq!(parse_partition_name("p:2025:ES"), Some((2025, "ES")));
        assert_eq!(parse_partition_name("default"), None);
        assert_eq!(parse_partition_name("p:2025:ES:extra"), None);
    }

    #[test]
    fn phase_action_filter_matches_supported_qualifiers() {
        assert!(has_phase_action("4[TRD_TYPE];C  [INST_PHASE]"));
        assert!(has_phase_action("21[TRD_TYPE];\" O \"[INST_PHASE]"));
        assert!(has_phase_action("15[TRD_TYPE]"));
        assert!(has_phase_action("17[TRD_TYPE];T[INST_PHASE]"));
        assert!(!has_phase_action("17[TRD_TYPE];C[INST_PHASE]"));
        assert!(!has_phase_action("N[ORD_ENT_ST];5[SECUR_ST]"));
    }

    #[test]
    fn raw_trade_type_filter_only_reads_the_used_qualifier_slot() {
        let mut value = vec![0_u8; 220];
        let trade_type = b"4[TRD_TYPE]";
        value[36..36 + trade_type.len()].copy_from_slice(trade_type);
        assert!(super::raw_status_has_trade_type(&value).unwrap());
        value[36..].fill(0);
        let market_status = b"I[MKT_ST_IND]";
        value[36..36 + market_status.len()].copy_from_slice(market_status);
        assert!(!super::raw_status_has_trade_type(&value).unwrap());
    }
}
