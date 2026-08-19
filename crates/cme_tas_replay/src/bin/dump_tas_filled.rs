//! Dump TAS source rows that have a named column filled.
//!
//! Later gzip parts have no header. This binary reads the 294-column
//! header from part 0, then streams the requested part with MultiGzDecoder.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ColumnRules;
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "dump_tas_filled")]
struct Args {
    #[arg(long)]
    data_root: PathBuf,
    #[arg(long)]
    period: String,
    #[arg(long, default_value_t = 1)]
    part_index: u16,
    #[arg(long)]
    column: Option<String>,
    #[arg(long)]
    ric: Option<String>,
    #[arg(long)]
    date_time: Option<String>,
    #[arg(long, default_value_t = 3)]
    max_hits: usize,
    #[arg(long, default_value = "../preprocess/lseg/tas_column_rules.json")]
    column_rules: PathBuf,
    #[arg(long)]
    out_json: Option<PathBuf>,
}

fn period_dir(data_root: &Path, period: &str) -> PathBuf {
    data_root.join(format!(
        "shanghai_evolution_futures_time_and_sales_ric_list_0_tas_{period}"
    ))
}

fn part_path(dir: &Path, part_no: u16) -> PathBuf {
    dir.join(format!("merged-Data-part-{part_no:06}.csv.gz"))
}

fn header_map(part0: &Path, rules: &ColumnRules) -> Result<(Vec<String>, BTreeMap<String, usize>)> {
    let file = File::open(part0).with_context(|| format!("open {}", part0.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(decoder);
    let headers = reader
        .headers()
        .context("read TAS header from part 0")?
        .clone();
    if headers.get(0).map(str::trim) != Some("#RIC") {
        bail!(
            "part 0 first row is {:?}, expected TAS header starting with #RIC",
            headers.get(0)
        );
    }
    let mut names = Vec::with_capacity(headers.len());
    let mut by_name = BTreeMap::new();
    for (index, name) in headers.iter().enumerate() {
        rules.group_of(name)?;
        names.push(name.to_string());
        by_name.insert(name.to_string(), index);
    }
    Ok((names, by_name))
}

fn cell<'a>(record: &'a StringRecord, by_name: &BTreeMap<String, usize>, name: &str) -> &'a str {
    by_name
        .get(name)
        .and_then(|&idx| record.get(idx))
        .map(str::trim)
        .unwrap_or("")
}

fn nonempty_object(record: &StringRecord, names: &[String], rules: &ColumnRules) -> Result<Map<String, Value>> {
    let mut object = Map::new();
    for (idx, name) in names.iter().enumerate() {
        let value = record.get(idx).map(str::trim).unwrap_or("");
        if value.is_empty() {
            continue;
        }
        let group = rules.group_of(name)?;
        object.insert(
            name.clone(),
            json!({
                "value": value,
                "group": group,
            }),
        );
    }
    Ok(object)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rules = ColumnRules::load(&args.column_rules)?;
    let dir = period_dir(&args.data_root, &args.period);
    let part0 = part_path(&dir, 0);
    let part = part_path(&dir, args.part_index);
    let (names, by_name) = header_map(&part0, &rules)?;
    if let Some(column) = args.column.as_deref() {
        if !by_name.contains_key(column) {
            bail!("column {column:?} is not in the TAS header");
        }
    }
    if args.column.is_none() && args.date_time.is_none() && args.ric.is_none() {
        bail!("need --column, --ric, or --date-time");
    }
    let file = File::open(&part).with_context(|| format!("open {}", part.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);

    println!(
        "dump_tas_filled part={} column={:?} ric={:?} date_time={:?} max_hits={}",
        part.display(),
        args.column,
        args.ric,
        args.date_time,
        args.max_hits
    );

    let mut source_rows = 0u64;
    let mut hits = Vec::new();
    for record in reader.records() {
        source_rows += 1;
        let record = record.with_context(|| format!("read TAS row {source_rows} from {}", part.display()))?;
        if source_rows % 5_000_000 == 0 {
            eprintln!(
                "scanned={source_rows} last_ric={:?} hits={}",
                cell(&record, &by_name, "#RIC"),
                hits.len()
            );
        }
        if let Some(wanted) = args.ric.as_deref() {
            if cell(&record, &by_name, "#RIC") != wanted {
                continue;
            }
        }
        if let Some(wanted) = args.date_time.as_deref() {
            if cell(&record, &by_name, "Date-Time") != wanted {
                continue;
            }
        }
        if let Some(column) = args.column.as_deref() {
            if cell(&record, &by_name, column).is_empty() {
                continue;
            }
        }
        let filled = nonempty_object(&record, &names, &rules)?;
        let hit = json!({
            "part": args.part_index,
            "source_row": source_rows,
            "ric": cell(&record, &by_name, "#RIC"),
            "date_time": cell(&record, &by_name, "Date-Time"),
            "type": cell(&record, &by_name, "Type"),
            "column": args.column,
            "filled": filled,
        });
        println!("{}", serde_json::to_string_pretty(&hit)?);
        hits.push(hit);
        if hits.len() >= args.max_hits {
            break;
        }
    }
    if hits.is_empty() {
        return Err(anyhow!(
            "no filled {:?} after {source_rows} rows in {}",
            args.column,
            part.display()
        ));
    }
    println!("dump_tas_filled hits={} scanned={source_rows}", hits.len());
    if let Some(path) = args.out_json {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "part": args.part_index,
                "column": args.column,
                "ric": args.ric,
                "scanned": source_rows,
                "hits": hits,
            }))?,
        )?;
        println!("wrote {}", path.display());
    }
    Ok(())
}
