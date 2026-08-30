use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::shard::{period_dir_name, TasShardManifest};
use csv::ByteRecord;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_reference_scan")]
#[command(about = "Enumerate Reference Change types in TAS zstd shards")]
struct Args {
    #[arg(long)]
    shard_root: PathBuf,
    #[arg(long, required = true)]
    period: Vec<String>,
    #[arg(long, default_value_t = 32)]
    workers: usize,
}

#[derive(Default)]
struct TypeStats {
    rows: u64,
    old_empty: u64,
    new_empty: u64,
    both_empty: u64,
    max_old_len: usize,
    max_new_len: usize,
    samples: Vec<String>,
}

impl TypeStats {
    fn merge_from(&mut self, other: Self) {
        self.rows += other.rows;
        self.old_empty += other.old_empty;
        self.new_empty += other.new_empty;
        self.both_empty += other.both_empty;
        self.max_old_len = self.max_old_len.max(other.max_old_len);
        self.max_new_len = self.max_new_len.max(other.max_new_len);
        for sample in other.samples {
            if self.samples.len() >= 3 {
                break;
            }
            self.samples.push(sample);
        }
    }
}

#[derive(Default)]
struct ScanResult {
    source_rows: u64,
    reference_rows: u64,
    types: BTreeMap<String, TypeStats>,
}

impl ScanResult {
    fn merge_from(&mut self, other: Self) {
        self.source_rows += other.source_rows;
        self.reference_rows += other.reference_rows;
        for (name, stats) in other.types {
            self.types.entry(name).or_default().merge_from(stats);
        }
    }
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

fn plain_csv_cell(line: &[u8], wanted: usize) -> Option<&[u8]> {
    let mut field = 0usize;
    let mut start = 0usize;
    for (index, byte) in line.iter().copied().enumerate() {
        if byte == b'"' {
            return None;
        }
        if byte == b',' {
            if field == wanted {
                return Some(trim_ascii(&line[start..index]));
            }
            field += 1;
            start = index + 1;
        }
    }
    (field == wanted).then(|| trim_ascii(&line[start..]))
}

fn parse_record(line: &[u8]) -> Result<ByteRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(line);
    match reader.byte_records().next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(err)) => Err(err.into()),
        None => bail!("empty TAS CSV row"),
    }
}

fn required_index(headers: &ByteRecord, name: &[u8]) -> Result<usize> {
    headers
        .iter()
        .position(|field| trim_ascii(field) == name)
        .ok_or_else(|| {
            anyhow!(
                "TAS shard header is missing {:?}",
                String::from_utf8_lossy(name)
            )
        })
}

fn display_cell(record: &ByteRecord, index: usize) -> String {
    String::from_utf8_lossy(trim_ascii(record.get(index).unwrap_or_default())).into_owned()
}

fn scan_shard(path: &Path) -> Result<ScanResult> {
    let file = File::open(path).with_context(|| format!("open TAS shard {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("open zstd decoder for {}", path.display()))?;
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 {
        bail!("empty TAS shard {}", path.display());
    }
    let headers = parse_record(&line)?;
    let ric_idx = required_index(&headers, b"#RIC")?;
    let date_time_idx = required_index(&headers, b"Date-Time")?;
    let type_idx = required_index(&headers, b"Type")?;
    let change_type_idx = required_index(&headers, b"Change Type")?;
    let old_value_idx = required_index(&headers, b"Old Value")?;
    let new_value_idx = required_index(&headers, b"New Value")?;

    let mut result = ScanResult::default();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        result.source_rows += 1;
        let is_reference = match plain_csv_cell(&line, type_idx) {
            Some(event_type) => event_type == b"Reference Change",
            None => {
                let record = parse_record(&line)?;
                trim_ascii(record.get(type_idx).unwrap_or_default()) == b"Reference Change"
            }
        };
        if !is_reference {
            continue;
        }
        let record = parse_record(&line)?;
        let change_type = display_cell(&record, change_type_idx);
        if change_type.is_empty() {
            bail!(
                "Reference Change has empty Change Type in {}",
                path.display()
            );
        }
        let old_value = trim_ascii(record.get(old_value_idx).unwrap_or_default());
        let new_value = trim_ascii(record.get(new_value_idx).unwrap_or_default());
        let stats = result.types.entry(change_type).or_default();
        stats.rows += 1;
        stats.old_empty += u64::from(old_value.is_empty());
        stats.new_empty += u64::from(new_value.is_empty());
        stats.both_empty += u64::from(old_value.is_empty() && new_value.is_empty());
        stats.max_old_len = stats.max_old_len.max(old_value.len());
        stats.max_new_len = stats.max_new_len.max(new_value.len());
        if stats.samples.len() < 3 {
            stats.samples.push(format!(
                "ric={} ts={} old={:?} new={:?}",
                display_cell(&record, ric_idx),
                display_cell(&record, date_time_idx),
                String::from_utf8_lossy(old_value),
                String::from_utf8_lossy(new_value)
            ));
        }
        result.reference_rows += 1;
    }
    Ok(result)
}

fn run(args: &Args) -> Result<()> {
    if args.workers == 0 {
        bail!("workers must be >= 1");
    }
    let mut shards = Vec::new();
    let mut expected_rows = 0u64;
    for period in &args.period {
        let dir = args.shard_root.join(period_dir_name(period));
        let manifest = TasShardManifest::load(&dir)?;
        manifest.validate(period, true)?;
        expected_rows += manifest
            .sources
            .iter()
            .map(|source| source.data_rows)
            .sum::<u64>();
        shards.extend(manifest.shards.iter().map(|shard| dir.join(&shard.file)));
    }
    let completed = AtomicUsize::new(0);
    let pool = ThreadPoolBuilder::new()
        .num_threads(args.workers.min(shards.len()).max(1))
        .thread_name(|id| format!("cme-tas-reference-scan-{id}"))
        .build()
        .context("build TAS reference scan pool")?;
    let results = pool.install(|| {
        shards
            .par_iter()
            .map(|path| {
                let result = scan_shard(path);
                let count = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if count % 100 == 0 || count == shards.len() {
                    eprintln!("cme_tas_reference_scan shards={count}/{}", shards.len());
                }
                result
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let mut total = ScanResult::default();
    for result in results {
        total.merge_from(result);
    }
    if total.source_rows != expected_rows {
        bail!(
            "scanned {} source rows, manifests expect {expected_rows}",
            total.source_rows
        );
    }
    println!(
        "cme_tas_reference_scan source_rows={} reference_rows={} types={}",
        total.source_rows,
        total.reference_rows,
        total.types.len()
    );
    for (name, stats) in total.types {
        println!(
            "type={name:?} rows={} old_empty={} new_empty={} both_empty={} max_old_len={} max_new_len={} samples={:?}",
            stats.rows,
            stats.old_empty,
            stats.new_empty,
            stats.both_empty,
            stats.max_old_len,
            stats.max_new_len,
            stats.samples
        );
    }
    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(err) = run(&args) {
        eprintln!("cme_tas_reference_scan failed: {err:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_cell_stops_before_quoted_later_fields() {
        let row = b"CCU2,Market Price,2010-09-30T23:46:22Z,-4,Reference Change,,,,\"later,comma\"";
        assert_eq!(plain_csv_cell(row, 4), Some(b"Reference Change".as_slice()));
    }

    #[test]
    fn plain_cell_falls_back_for_a_quoted_prefix() {
        assert_eq!(plain_csv_cell(b"A,\"Market, Price\",ts,-4,Trade", 4), None);
    }
}
