//! Extract a valid same-RIC, same-time normalised TAS CSV gzip.
//!
//! Input records are delimited with a CSV quote-aware scanner, rather than
//! physical line filtering: source fields can be quoted and contain newlines.

use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use clap::Parser;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "usstock_extract_normalised")]
#[command(about = "Extract one RIC and UTC time window from normalised TAS")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, required = true, num_args = 1..)]
    ric: Vec<String>,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
}

#[derive(Debug, Serialize)]
struct Output {
    input: PathBuf,
    output: PathBuf,
    rics: Vec<String>,
    start: String,
    end: String,
    source_records_read: u64,
    target_records_before_start: BTreeMap<String, u64>,
    target_records_written: BTreeMap<String, u64>,
    target_rics_seen: Vec<String>,
    target_rics_completed: Vec<String>,
    stopped_on_ric: String,
}

fn parse_utc_ns(raw: &str) -> Result<i64> {
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse UTC timestamp {raw:?}"))?;
    parsed
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("UTC timestamp out of nanosecond range {raw:?}"))
}

/// Read one RFC-4180 record while preserving its raw bytes.
///
/// The source has unquoted `#RIC`, `Domain`, and `Date-Time` as its first
/// three fields, but later fields can include quoted newlines.  Full parsing
/// of all 294 fields before the target RIC is unnecessarily expensive; this
/// scanner only detects logical record boundaries and passes selected records
/// through unchanged.
fn read_csv_record<R: BufRead>(reader: &mut R, record: &mut Vec<u8>) -> Result<bool> {
    record.clear();
    let mut in_quotes = false;
    loop {
        let offset = record.len();
        let read = reader
            .read_until(b'\n', record)
            .context("read normalised physical CSV line")?;
        if read == 0 {
            if record.is_empty() {
                return Ok(false);
            }
            if in_quotes {
                bail!("unterminated quoted CSV record at end of source");
            }
            return Ok(true);
        }
        let mut index = offset;
        while index < record.len() {
            if record[index] != b'"' {
                index += 1;
                continue;
            }
            if in_quotes && record.get(index + 1) == Some(&b'"') {
                index += 2;
                continue;
            }
            in_quotes = !in_quotes;
            index += 1;
        }
        if !in_quotes {
            return Ok(true);
        }
    }
}

fn first_three_fields(record: &[u8]) -> Result<(&[u8], &[u8], &[u8])> {
    let mut fields = record.splitn(4, |byte| *byte == b',');
    let ric = fields
        .next()
        .ok_or_else(|| anyhow!("normalised record missing #RIC"))?;
    let domain = fields
        .next()
        .ok_or_else(|| anyhow!("normalised record missing Domain"))?;
    let timestamp = fields
        .next()
        .ok_or_else(|| anyhow!("normalised record missing Date-Time"))?;
    if ric.starts_with(b"\"") || domain.starts_with(b"\"") || timestamp.starts_with(b"\"") {
        bail!("first three normalised fields unexpectedly quoted")
    }
    Ok((ric, domain, timestamp))
}

fn run(args: Args) -> Result<Output> {
    let wanted: BTreeSet<String> = args.ric.into_iter().collect();
    if wanted.is_empty() || wanted.iter().any(|ric| ric.is_empty()) {
        bail!("--ric must contain one or more nonempty RICs");
    }
    let start_ns = parse_utc_ns(&args.start)?;
    let end_ns = parse_utc_ns(&args.end)?;
    if end_ns <= start_ns {
        bail!("--end must be after --start");
    }
    let input =
        File::open(&args.input).with_context(|| format!("open input {}", args.input.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::new(input));
    let mut reader = BufReader::new(decoder);
    let mut record = Vec::new();
    if !read_csv_record(&mut reader, &mut record)? {
        bail!("empty normalised source {}", args.input.display());
    }
    if !record.starts_with(b"#RIC,Domain,Date-Time,") {
        bail!("unexpected normalised CSV header")
    }

    let output_file = File::create(&args.output)
        .with_context(|| format!("create output {}", args.output.display()))?;
    let mut writer = GzEncoder::new(BufWriter::new(output_file), Compression::default());
    writer
        .write_all(&record)
        .context("write selected normalised header")?;

    let mut out = Output {
        input: args.input.clone(),
        output: args.output.clone(),
        rics: wanted.iter().cloned().collect(),
        start: args.start,
        end: args.end,
        source_records_read: 0,
        target_records_before_start: BTreeMap::new(),
        target_records_written: BTreeMap::new(),
        target_rics_seen: Vec::new(),
        target_rics_completed: Vec::new(),
        stopped_on_ric: String::new(),
    };
    while read_csv_record(&mut reader, &mut record)? {
        out.source_records_read += 1;
        let (raw_ric, _, raw_timestamp) = first_three_fields(&record)?;
        let row_ric = std::str::from_utf8(raw_ric).context("decode normalised #RIC")?;
        if !wanted.contains(row_ric) {
            // This extraction is RIC-grouped.  Once every requested RIC has
            // been seen, the first following group marks the end of the
            // requested range and avoids scanning the remainder of the file.
            if out.target_rics_completed.len() == wanted.len() {
                out.stopped_on_ric = row_ric.to_string();
                break;
            }
            continue;
        }
        if !out.target_rics_seen.iter().any(|seen| seen == row_ric) {
            out.target_rics_seen.push(row_ric.to_string());
        }
        if out
            .target_rics_completed
            .iter()
            .any(|completed| completed == row_ric)
        {
            continue;
        }
        let timestamp = std::str::from_utf8(raw_timestamp)
            .with_context(|| format!("decode Date-Time for {row_ric}"))?;
        let ts_ns = parse_utc_ns(timestamp)?;
        if ts_ns >= end_ns {
            out.target_rics_completed.push(row_ric.to_string());
            continue;
        }
        if ts_ns < start_ns {
            *out.target_records_before_start
                .entry(row_ric.to_string())
                .or_default() += 1;
            continue;
        }
        writer
            .write_all(&record)
            .context("write selected normalised CSV record")?;
        *out.target_records_written
            .entry(row_ric.to_string())
            .or_default() += 1;
    }
    writer.finish().context("finish selected gzip")?;
    if out.target_rics_seen.len() != wanted.len() {
        let missing: Vec<&str> = wanted
            .iter()
            .filter(|ric| !out.target_rics_seen.iter().any(|seen| seen == *ric))
            .map(String::as_str)
            .collect();
        bail!(
            "RICs {missing:?} were not found in {}",
            args.input.display()
        );
    }
    Ok(out)
}

fn main() -> Result<()> {
    let output = run(Args::parse())?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::read_csv_record;
    use std::io::{BufReader, Cursor};

    #[test]
    fn preserves_quoted_embedded_newline_as_one_record() {
        let source = b"#RIC,Domain,Date-Time,Text\nARKG.BAT,Market Price,2026-07-01T00:00:00Z,\"first\nsecond \"\"quoted\"\" line\"\nNEXT,Market Price,2026-07-01T00:00:01Z,x\n";
        let mut reader = BufReader::new(Cursor::new(source));
        let mut record = Vec::new();
        assert!(read_csv_record(&mut reader, &mut record).unwrap());
        assert_eq!(record, b"#RIC,Domain,Date-Time,Text\n");
        assert!(read_csv_record(&mut reader, &mut record).unwrap());
        assert_eq!(
            record,
            b"ARKG.BAT,Market Price,2026-07-01T00:00:00Z,\"first\nsecond \"\"quoted\"\" line\"\n"
        );
        assert!(read_csv_record(&mut reader, &mut record).unwrap());
        assert_eq!(record, b"NEXT,Market Price,2026-07-01T00:00:01Z,x\n");
        assert!(!read_csv_record(&mut reader, &mut record).unwrap());
    }
}
