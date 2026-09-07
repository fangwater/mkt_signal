use anyhow::{bail, Context, Result};
use clap::Parser;
use csv::StringRecord;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Instant;

const EXPECTED_HEADER: [&str; 14] = [
    "#RIC",
    "Domain",
    "Date-Time",
    "GMT Offset",
    "Type",
    "MsgClass/FID number",
    "UpdateType/Action",
    "FID Name",
    "FID Value",
    "FID Enum String",
    "PE Code",
    "Template Number",
    "Key/Msg Sequence Number",
    "Number of FIDs",
];

#[derive(Debug, Parser)]
#[command(about = "Read-only structural census of an LSEG raw MBP CSV")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    max_messages: Option<u64>,
    #[arg(long, default_value_t = 1_000_000)]
    progress_every: u64,
}

#[derive(Debug, Clone, Serialize)]
struct Example {
    ric: String,
    date_time: String,
    source_record: u64,
}

#[derive(Debug, Default, Serialize)]
struct Occurrences {
    count: u64,
    first: Option<Example>,
}

impl Occurrences {
    fn observe(&mut self, example: &Example) {
        self.count += 1;
        if self.first.is_none() {
            self.first = Some(example.clone());
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct MessageOccurrences {
    count: u64,
    total_entries: u64,
    min_entries: Option<u64>,
    max_entries: u64,
    first: Option<Example>,
}

impl MessageOccurrences {
    fn observe(&mut self, entries: u64, example: &Example) {
        self.count += 1;
        self.total_entries += entries;
        self.min_entries = Some(self.min_entries.map_or(entries, |old| old.min(entries)));
        self.max_entries = self.max_entries.max(entries);
        if self.first.is_none() {
            self.first = Some(example.clone());
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct FidOccurrences {
    count: u64,
    nonempty_value: u64,
    nonempty_enum: u64,
    max_value_bytes: usize,
    max_enum_bytes: usize,
    first: Option<Example>,
}

#[derive(Debug, Default, Serialize)]
struct Anomalies {
    orphan_children: u64,
    unexpected_child_types: BTreeMap<String, u64>,
    repeated_summary_rows: u64,
    missing_summary_rows: u64,
    summary_declared_fid_mismatch: u64,
    entry_declared_fid_mismatch: u64,
    invalid_declared_fid_counts: u64,
}

#[derive(Debug, Default, Serialize)]
struct Audit {
    input: String,
    input_bytes: u64,
    capped: bool,
    physical_records_including_header: u64,
    logical_messages: u64,
    map_entries: u64,
    messages_by_ric: BTreeMap<String, u64>,
    msg_classes: BTreeMap<String, u64>,
    map_actions: BTreeMap<String, u64>,
    gmt_offsets: BTreeMap<String, u64>,
    pe_codes: BTreeMap<String, u64>,
    template_numbers: BTreeMap<String, u64>,
    entries_per_message: BTreeMap<u64, u64>,
    outer_templates: BTreeMap<String, Occurrences>,
    summary_templates: BTreeMap<String, Occurrences>,
    map_entry_templates: BTreeMap<String, Occurrences>,
    message_templates: BTreeMap<String, MessageOccurrences>,
    fid_catalog: BTreeMap<String, FidOccurrences>,
    anomalies: Anomalies,
}

#[derive(Debug)]
struct Message {
    example: Example,
    outer_template: String,
    summary_seen: bool,
    summary_declared: Option<u64>,
    summary_fids: Vec<(String, String)>,
    entry_templates: BTreeSet<String>,
    entry_count: u64,
}

#[derive(Debug)]
struct Entry {
    action: String,
    declared: Option<u64>,
    fids: Vec<(String, String)>,
}

fn field(record: &StringRecord, index: usize) -> &str {
    record.get(index).unwrap_or("")
}

fn bump(map: &mut BTreeMap<String, u64>, key: &str) {
    *map.entry(key.to_string()).or_default() += 1;
}

fn parse_declared(value: &str, audit: &mut Audit) -> Option<u64> {
    match value.parse::<u64>() {
        Ok(value) => Some(value),
        Err(_) => {
            audit.anomalies.invalid_declared_fid_counts += 1;
            None
        }
    }
}

fn fid_signature(fids: &[(String, String)]) -> String {
    if fids.is_empty() {
        return "<empty>".to_string();
    }
    fids.iter()
        .map(|(number, name)| format!("{number}:{name}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn observe_fid(
    audit: &mut Audit,
    scope: &str,
    record: &StringRecord,
    example: &Example,
) -> (String, String) {
    let number = field(record, 5).to_string();
    let name = field(record, 7).to_string();
    let value = field(record, 8);
    let enum_value = field(record, 9);
    let key = format!("{scope}|{number}:{name}");
    let stats = audit.fid_catalog.entry(key).or_default();
    stats.count += 1;
    stats.nonempty_value += u64::from(!value.is_empty());
    stats.nonempty_enum += u64::from(!enum_value.is_empty());
    stats.max_value_bytes = stats.max_value_bytes.max(value.len());
    stats.max_enum_bytes = stats.max_enum_bytes.max(enum_value.len());
    if stats.first.is_none() {
        stats.first = Some(example.clone());
    }
    (number, name)
}

fn finish_entry(audit: &mut Audit, message: &mut Message, entry: Entry) {
    if entry
        .declared
        .is_some_and(|declared| declared != entry.fids.len() as u64)
    {
        audit.anomalies.entry_declared_fid_mismatch += 1;
    }
    let signature = format!(
        "action={}|declared={}|fids={}",
        entry.action,
        entry
            .declared
            .map_or_else(|| "invalid".to_string(), |value| value.to_string()),
        fid_signature(&entry.fids)
    );
    audit
        .map_entry_templates
        .entry(signature.clone())
        .or_default()
        .observe(&message.example);
    message.entry_templates.insert(signature);
    message.entry_count += 1;
    audit.map_entries += 1;
}

fn finish_message(audit: &mut Audit, message: Message) {
    if !message.summary_seen {
        audit.anomalies.missing_summary_rows += 1;
    }
    if message
        .summary_declared
        .is_some_and(|declared| declared != message.summary_fids.len() as u64)
    {
        audit.anomalies.summary_declared_fid_mismatch += 1;
    }
    let summary_signature = format!(
        "declared={}|fids={}",
        message.summary_declared.map_or_else(
            || "missing_or_invalid".to_string(),
            |value| value.to_string()
        ),
        fid_signature(&message.summary_fids)
    );
    audit
        .summary_templates
        .entry(summary_signature.clone())
        .or_default()
        .observe(&message.example);
    let entry_signatures = if message.entry_templates.is_empty() {
        "<none>".to_string()
    } else {
        message
            .entry_templates
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(";")
    };
    let signature = format!(
        "outer=[{}]|summary=[{}]|entry_types=[{}]",
        message.outer_template, summary_signature, entry_signatures
    );
    audit
        .message_templates
        .entry(signature)
        .or_default()
        .observe(message.entry_count, &message.example);
    *audit
        .entries_per_message
        .entry(message.entry_count)
        .or_default() += 1;
}

fn validate_header(header: &StringRecord) -> Result<()> {
    if header.len() != EXPECTED_HEADER.len()
        || header
            .iter()
            .zip(EXPECTED_HEADER)
            .any(|(actual, expected)| actual != expected)
    {
        bail!("unexpected raw MBP header: {header:?}");
    }
    Ok(())
}

fn audit_reader<R: Read>(
    reader: R,
    input: &Path,
    input_bytes: u64,
    max_messages: Option<u64>,
    progress_every: u64,
) -> Result<Audit> {
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::with_capacity(16 * 1024 * 1024, reader));
    let header = csv.records().next().context("input is empty")??;
    validate_header(&header)?;
    let mut audit = Audit {
        input: input.display().to_string(),
        input_bytes,
        physical_records_including_header: 1,
        ..Audit::default()
    };
    let mut current_message: Option<Message> = None;
    let mut current_entry: Option<Entry> = None;
    let started = Instant::now();

    for result in csv.records() {
        let record = result.with_context(|| format!("parse {}", input.display()))?;
        audit.physical_records_including_header += 1;
        let source_record = audit.physical_records_including_header;
        if !field(&record, 0).is_empty() {
            if let Some(entry) = current_entry.take() {
                let message = current_message
                    .as_mut()
                    .context("entry exists without an outer message")?;
                finish_entry(&mut audit, message, entry);
            }
            if let Some(message) = current_message.take() {
                finish_message(&mut audit, message);
                if max_messages.is_some_and(|limit| audit.logical_messages >= limit) {
                    audit.capped = true;
                    break;
                }
            }
            let example = Example {
                ric: field(&record, 0).to_string(),
                date_time: field(&record, 2).to_string(),
                source_record,
            };
            let outer_template = format!(
                "domain={}|type={}|class={}|pe={}|template={}|declared={}",
                field(&record, 1),
                field(&record, 4),
                field(&record, 5),
                field(&record, 10),
                field(&record, 11),
                field(&record, 13)
            );
            audit
                .outer_templates
                .entry(outer_template.clone())
                .or_default()
                .observe(&example);
            audit.logical_messages += 1;
            bump(&mut audit.messages_by_ric, &example.ric);
            bump(&mut audit.msg_classes, field(&record, 5));
            bump(&mut audit.gmt_offsets, field(&record, 3));
            bump(&mut audit.pe_codes, field(&record, 10));
            bump(&mut audit.template_numbers, field(&record, 11));
            if progress_every > 0 && audit.logical_messages.is_multiple_of(progress_every) {
                eprintln!(
                    "messages={} physical_records={} elapsed_seconds={:.1}",
                    audit.logical_messages,
                    audit.physical_records_including_header,
                    started.elapsed().as_secs_f64()
                );
            }
            current_message = Some(Message {
                example,
                outer_template,
                summary_seen: false,
                summary_declared: None,
                summary_fids: Vec::new(),
                entry_templates: BTreeSet::new(),
                entry_count: 0,
            });
            continue;
        }

        let Some(message) = current_message.as_mut() else {
            audit.anomalies.orphan_children += 1;
            continue;
        };
        match field(&record, 4) {
            "Summary" => {
                if let Some(entry) = current_entry.take() {
                    finish_entry(&mut audit, message, entry);
                }
                if message.summary_seen {
                    audit.anomalies.repeated_summary_rows += 1;
                }
                message.summary_seen = true;
                message.summary_declared = parse_declared(field(&record, 13), &mut audit);
            }
            "MapEntry" => {
                if let Some(entry) = current_entry.take() {
                    finish_entry(&mut audit, message, entry);
                }
                let action = field(&record, 6).to_string();
                bump(&mut audit.map_actions, &action);
                current_entry = Some(Entry {
                    action,
                    declared: parse_declared(field(&record, 13), &mut audit),
                    fids: Vec::new(),
                });
            }
            "FID" => {
                if let Some(entry) = current_entry.as_mut() {
                    entry.fids.push(observe_fid(
                        &mut audit,
                        "MapEntry",
                        &record,
                        &message.example,
                    ));
                } else {
                    message.summary_fids.push(observe_fid(
                        &mut audit,
                        "Summary",
                        &record,
                        &message.example,
                    ));
                }
            }
            other => {
                *audit
                    .anomalies
                    .unexpected_child_types
                    .entry(other.to_string())
                    .or_default() += 1;
            }
        }
    }

    if !audit.capped {
        if let Some(entry) = current_entry.take() {
            let message = current_message
                .as_mut()
                .context("entry exists without an outer message")?;
            finish_entry(&mut audit, message, entry);
        }
        if let Some(message) = current_message.take() {
            finish_message(&mut audit, message);
        }
    }
    Ok(audit)
}

fn audit_gzip(args: &Args) -> Result<Audit> {
    let mut child = Command::new("/usr/bin/gzip")
        .args(["-dc", "--"])
        .arg(&args.input)
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn gzip for {}", args.input.display()))?;
    let stdout = child.stdout.take().context("gzip stdout is unavailable")?;
    let result = audit_reader(
        stdout,
        &args.input,
        fs::metadata(&args.input)?.len(),
        args.max_messages,
        args.progress_every,
    );
    match result {
        Ok(audit) => {
            finish_gzip(child, audit.capped)?;
            Ok(audit)
        }
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

fn finish_gzip(mut child: Child, capped: bool) -> Result<()> {
    if capped {
        let _ = child.kill();
    }
    let status = child.wait().context("wait for gzip")?;
    if !capped && !status.success() {
        bail!("gzip exited with {status}");
    }
    Ok(())
}

fn write_report(path: &Path, audit: &Audit) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let partial = path.with_extension("json.partial");
    let mut file = File::create(&partial)
        .with_context(|| format!("create audit output {}", partial.display()))?;
    serde_json::to_writer_pretty(&mut file, audit)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&partial, path)
        .with_context(|| format!("publish audit output {}", path.display()))?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let audit = if args.input.extension().is_some_and(|value| value == "gz") {
        audit_gzip(&args)?
    } else {
        let file =
            File::open(&args.input).with_context(|| format!("open {}", args.input.display()))?;
        audit_reader(
            file,
            &args.input,
            fs::metadata(&args.input)?.len(),
            args.max_messages,
            args.progress_every,
        )?
    };
    write_report(&args.output, &audit)?;
    eprintln!(
        "complete messages={} entries={} outer_templates={} summary_templates={} entry_templates={} message_templates={} output={}",
        audit.logical_messages,
        audit.map_entries,
        audit.outer_templates.len(),
        audit.summary_templates.len(),
        audit.map_entry_templates.len(),
        audit.message_templates.len(),
        args.output.display()
    );
    Ok(())
}
