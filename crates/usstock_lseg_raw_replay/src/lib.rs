use anyhow::{bail, Context, Result};
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use log::info;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use zstd::stream::write::Encoder as ZstdEncoder;

pub mod event_codec;
pub mod quote_codec;
pub mod quote_replay;
pub mod raw;

pub const MANIFEST_FILE: &str = "manifest.json";
pub const MANIFEST_SCHEMA: &str = "lseg-usstock-raw-shards";
const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const OUTPUT_BUFFER_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub data_root: PathBuf,
    pub output_root: PathBuf,
    pub periods: Vec<String>,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_rows_per_shard")]
    pub rows_per_shard: u64,
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    #[serde(default = "default_progress_every")]
    pub progress_every: u64,
}

fn default_workers() -> usize {
    4
}

fn default_rows_per_shard() -> u64 {
    250_000_000
}

fn default_zstd_level() -> i32 {
    1
}

fn default_progress_every() -> u64 {
    10_000_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageBoundary {
    pub ric: String,
    pub date_time: String,
    pub message_class: String,
    pub source_sequence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    pub file: String,
    pub original_part: u16,
    pub shard_index: u32,
    pub physical_rows: u64,
    pub logical_messages: u64,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub first_message: MessageBoundary,
    pub last_message: MessageBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEntry {
    pub file: String,
    pub original_part: u16,
    pub compressed_bytes: u64,
    pub physical_rows: u64,
    pub logical_messages: u64,
    pub data_bytes: u64,
    pub shard_count: u32,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: String,
    pub period: String,
    pub complete: bool,
    pub header: String,
    pub rows_per_shard: u64,
    pub zstd_level: i32,
    pub sources: Vec<SourceEntry>,
    pub shards: Vec<ShardEntry>,
}

impl Manifest {
    pub fn validate(&self, expected_period: &str, require_complete: bool) -> Result<()> {
        if self.schema != MANIFEST_SCHEMA {
            bail!("unsupported RAW shard schema {:?}", self.schema);
        }
        if self.period != expected_period {
            bail!(
                "RAW shard period {:?} does not match {:?}",
                self.period,
                expected_period
            );
        }
        if self.rows_per_shard == 0 || self.sources.is_empty() || self.shards.is_empty() {
            bail!("RAW shard manifest has empty sources/shards or zero rows_per_shard");
        }
        if self.complete != self.sources.iter().all(|source| source.complete) {
            bail!("RAW shard completion disagrees with source completion");
        }
        if require_complete && !self.complete {
            bail!("RAW shard period is incomplete");
        }

        let sources = self
            .sources
            .iter()
            .map(|source| (source.original_part, source))
            .collect::<BTreeMap<_, _>>();
        if sources.len() != self.sources.len() {
            bail!("RAW shard manifest repeats a source part");
        }
        let mut totals = BTreeMap::<u16, (u64, u64, u64, u32)>::new();
        for shard in &self.shards {
            if shard.file != shard_file_name(shard.original_part, shard.shard_index) {
                bail!("invalid RAW shard file name {:?}", shard.file);
            }
            if shard.physical_rows == 0
                || shard.logical_messages == 0
                || shard.compressed_bytes == 0
            {
                bail!("RAW shard {:?} is empty", shard.file);
            }
            if !sources.contains_key(&shard.original_part) {
                bail!("RAW shard {:?} references an unknown source", shard.file);
            }
            let total = totals.entry(shard.original_part).or_default();
            if shard.shard_index != total.3 {
                bail!(
                    "RAW shard part {} index {} is not contiguous from {}",
                    shard.original_part,
                    shard.shard_index,
                    total.3
                );
            }
            total.0 = total
                .0
                .checked_add(shard.physical_rows)
                .context("row overflow")?;
            total.1 = total
                .1
                .checked_add(shard.logical_messages)
                .context("message overflow")?;
            total.2 = total
                .2
                .checked_add(shard.data_bytes)
                .context("byte overflow")?;
            total.3 += 1;
        }
        for source in &self.sources {
            let actual = totals
                .get(&source.original_part)
                .copied()
                .unwrap_or_default();
            let expected = (
                source.physical_rows,
                source.logical_messages,
                source.data_bytes,
                source.shard_count,
            );
            if actual != expected {
                bail!(
                    "RAW source part {} totals {:?} do not match {:?}",
                    source.original_part,
                    actual,
                    expected
                );
            }
        }
        Ok(())
    }

    pub fn write(&self, directory: &Path) -> Result<()> {
        let final_path = directory.join(MANIFEST_FILE);
        let partial_path = directory.join(format!("{MANIFEST_FILE}.partial"));
        let file = File::create(&partial_path)
            .with_context(|| format!("create {}", partial_path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self)?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        drop(writer);
        fs::rename(&partial_path, &final_path).with_context(|| {
            format!(
                "publish {} -> {}",
                partial_path.display(),
                final_path.display()
            )
        })?;
        Ok(())
    }
}

#[derive(Clone)]
struct SourceJob {
    period: String,
    source_path: PathBuf,
    output_dir: PathBuf,
    original_part: u16,
    header: Arc<Vec<u8>>,
}

struct SourceResult {
    period: String,
    source: SourceEntry,
    shards: Vec<ShardEntry>,
}

struct CurrentMessage {
    boundary: MessageBoundary,
    expected_children: u32,
    observed_children: u32,
}

impl CurrentMessage {
    fn validate(&self) -> Result<()> {
        if self.expected_children != self.observed_children {
            bail!(
                "message {:?} declares {} FIDs but has {} child rows",
                self.boundary,
                self.expected_children,
                self.observed_children
            );
        }
        Ok(())
    }
}

struct OpenShard {
    final_path: PathBuf,
    partial_path: PathBuf,
    original_part: u16,
    shard_index: u32,
    encoder: ZstdEncoder<'static, BufWriter<File>>,
    physical_rows: u64,
    logical_messages: u64,
    data_bytes: u64,
    first_message: MessageBoundary,
}

impl OpenShard {
    fn create(
        output_dir: &Path,
        original_part: u16,
        shard_index: u32,
        header: &[u8],
        first_message: MessageBoundary,
        zstd_level: i32,
    ) -> Result<Self> {
        let file_name = shard_file_name(original_part, shard_index);
        let final_path = output_dir.join(&file_name);
        let partial_path = output_dir.join(format!("{file_name}.partial"));
        let writer = BufWriter::with_capacity(
            OUTPUT_BUFFER_BYTES,
            File::create(&partial_path)
                .with_context(|| format!("create {}", partial_path.display()))?,
        );
        let mut encoder = ZstdEncoder::new(writer, zstd_level)?;
        encoder.include_checksum(true)?;
        encoder.write_all(header)?;
        if !header.ends_with(b"\n") {
            encoder.write_all(b"\n")?;
        }
        Ok(Self {
            final_path,
            partial_path,
            original_part,
            shard_index,
            encoder,
            physical_rows: 0,
            logical_messages: 0,
            data_bytes: 0,
            first_message,
        })
    }

    fn write_row(&mut self, row: &[u8], is_outer: bool) -> Result<()> {
        self.encoder.write_all(row)?;
        self.physical_rows += 1;
        self.logical_messages += u64::from(is_outer);
        self.data_bytes += row.len() as u64;
        Ok(())
    }

    fn finish(self, last_message: MessageBoundary) -> Result<ShardEntry> {
        let mut writer = self.encoder.finish()?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        drop(writer);
        fs::rename(&self.partial_path, &self.final_path).with_context(|| {
            format!(
                "publish {} -> {}",
                self.partial_path.display(),
                self.final_path.display()
            )
        })?;
        let compressed_bytes = fs::metadata(&self.final_path)?.len();
        Ok(ShardEntry {
            file: self
                .final_path
                .file_name()
                .and_then(|name| name.to_str())
                .context("non-UTF8 shard file name")?
                .to_string(),
            original_part: self.original_part,
            shard_index: self.shard_index,
            physical_rows: self.physical_rows,
            logical_messages: self.logical_messages,
            data_bytes: self.data_bytes,
            compressed_bytes,
            first_message: self.first_message,
            last_message,
        })
    }
}

pub fn load_config(path: &Path) -> Result<Config> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let config: Config =
        toml::from_str(&content).with_context(|| format!("parse config {}", path.display()))?;
    if config.periods.is_empty() || config.workers == 0 || config.rows_per_shard == 0 {
        bail!("periods must be nonempty and workers/rows_per_shard must be positive");
    }
    Ok(config)
}

pub fn period_dir_name(period: &str) -> String {
    format!("shanghai_evolution_equities_raw_ric_list_0_raw_{period}")
}

pub fn shard_file_name(original_part: u16, shard_index: u32) -> String {
    format!("merged-Data-part-{original_part:06}-shard-{shard_index:06}.csv.zst")
}

fn trim_line_ending(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line = &line[..line.len() - 1];
    }
    line
}

fn parse_record(line: &[u8]) -> Result<StringRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(trim_line_ending(line));
    reader
        .records()
        .next()
        .context("empty CSV row")?
        .map_err(Into::into)
}

fn parse_outer(line: &[u8]) -> Result<CurrentMessage> {
    let row = parse_record(line)?;
    if row.len() < 14
        || row.get(0).unwrap_or("").is_empty()
        || row.get(1) != Some("Market Price")
        || row.get(2).unwrap_or("").is_empty()
        || row.get(4) != Some("Raw")
        || row.get(5).unwrap_or("").is_empty()
    {
        bail!("invalid RAW outer row: {row:?}");
    }
    let expected_children = row
        .get(13)
        .unwrap_or("")
        .parse::<u32>()
        .context("parse outer Number of FIDs")?;
    Ok(CurrentMessage {
        boundary: MessageBoundary {
            ric: row[0].to_string(),
            date_time: row[2].to_string(),
            message_class: row[5].to_string(),
            source_sequence: row.get(12).unwrap_or("").to_string(),
        },
        expected_children,
        observed_children: 0,
    })
}

fn validate_header(header: &[u8]) -> Result<()> {
    let row = parse_record(header)?;
    let expected = [
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
    if row.iter().collect::<Vec<_>>() != expected {
        bail!("unexpected RAW CSV header: {row:?}");
    }
    Ok(())
}

fn read_header(part_zero: &Path) -> Result<Vec<u8>> {
    let file = File::open(part_zero)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut header = Vec::new();
    if reader.read_until(b'\n', &mut header)? == 0 {
        bail!("empty RAW part zero {}", part_zero.display());
    }
    validate_header(&header)?;
    Ok(header)
}

fn discover_parts(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = fs::read_dir(directory)
        .with_context(|| format!("read {}", directory.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz")
        })
        .collect::<Vec<_>>();
    paths.sort();
    if paths.is_empty() {
        bail!("no RAW gzip parts under {}", directory.display());
    }
    Ok(paths)
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("non-UTF8 RAW source name")?;
    name.strip_prefix("merged-Data-part-")
        .and_then(|value| value.strip_suffix(".csv.gz"))
        .context("unrecognized RAW part name")?
        .parse()
        .with_context(|| format!("parse part number from {name}"))
}

fn finish_current(current: &mut Option<CurrentMessage>) -> Result<MessageBoundary> {
    let message = current.take().context("missing current RAW message")?;
    message.validate()?;
    Ok(message.boundary)
}

fn shard_source(job: &SourceJob, config: &Config, row_limit: Option<u64>) -> Result<SourceResult> {
    let started = Instant::now();
    let source_bytes = fs::metadata(&job.source_path)?.len();
    let file = File::open(&job.source_path)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut line = Vec::new();
    let mut pending_line = None;
    if reader.read_until(b'\n', &mut line)? == 0 {
        bail!("empty RAW source {}", job.source_path.display());
    }
    if trim_line_ending(&line) != trim_line_ending(&job.header) {
        pending_line = Some(std::mem::take(&mut line));
    }

    let mut shards = Vec::new();
    let mut open: Option<OpenShard> = None;
    let mut current = None;
    let mut last_completed = None;
    let mut physical_rows = 0_u64;
    let mut logical_messages = 0_u64;
    let mut data_bytes = 0_u64;
    let mut complete = true;

    loop {
        line = match pending_line.take() {
            Some(value) => value,
            None => {
                let mut value = Vec::new();
                if reader.read_until(b'\n', &mut value)? == 0 {
                    break;
                }
                value
            }
        };
        let is_outer = !line.starts_with(b",");
        if is_outer {
            if current.is_some() {
                last_completed = Some(finish_current(&mut current)?);
            }
            if row_limit.is_some_and(|limit| physical_rows >= limit) {
                complete = false;
                break;
            }
            if open
                .as_ref()
                .is_some_and(|writer| writer.physical_rows >= config.rows_per_shard)
            {
                let finished = open.take().expect("checked open shard").finish(
                    last_completed
                        .clone()
                        .context("missing last shard message")?,
                )?;
                info!(
                    "RAW shard finished period={} part={} shard={} rows={} messages={} compressed_bytes={}",
                    job.period,
                    job.original_part,
                    finished.shard_index,
                    finished.physical_rows,
                    finished.logical_messages,
                    finished.compressed_bytes
                );
                shards.push(finished);
            }
            let message = parse_outer(&line).with_context(|| {
                format!(
                    "parse outer period={} part={} physical_row={}",
                    job.period,
                    job.original_part,
                    physical_rows + 1
                )
            })?;
            if open.is_none() {
                open = Some(OpenShard::create(
                    &job.output_dir,
                    job.original_part,
                    shards.len() as u32,
                    &job.header,
                    message.boundary.clone(),
                    config.zstd_level,
                )?);
            }
            current = Some(message);
            logical_messages += 1;
        } else {
            if !line.starts_with(b",,,,FID,") {
                bail!(
                    "unsupported RAW child period={} part={} physical_row={}",
                    job.period,
                    job.original_part,
                    physical_rows + 1
                );
            }
            let message = current.as_mut().context("RAW child before outer row")?;
            message.observed_children += 1;
            if message.observed_children > message.expected_children {
                message.validate()?;
            }
        }
        open.as_mut()
            .context("RAW row has no open shard")?
            .write_row(&line, is_outer)?;
        physical_rows += 1;
        data_bytes += line.len() as u64;
        if config.progress_every > 0 && physical_rows.is_multiple_of(config.progress_every) {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            info!(
                "RAW shard progress period={} part={} rows={} messages={} rows_per_s={:.0} elapsed_s={:.1}",
                job.period,
                job.original_part,
                physical_rows,
                logical_messages,
                physical_rows as f64 / elapsed,
                elapsed
            );
        }
    }

    if current.is_some() {
        last_completed = Some(finish_current(&mut current)?);
    }
    if let Some(writer) = open {
        let finished = writer.finish(last_completed.context("source has no complete message")?)?;
        info!(
            "RAW shard finished period={} part={} shard={} rows={} messages={} compressed_bytes={}",
            job.period,
            job.original_part,
            finished.shard_index,
            finished.physical_rows,
            finished.logical_messages,
            finished.compressed_bytes
        );
        shards.push(finished);
    }
    if shards.is_empty() {
        bail!("RAW source {} has no data", job.source_path.display());
    }
    if complete {
        let decoder = reader.into_inner();
        let mut source = decoder.into_inner().into_inner();
        let position = source.stream_position()?;
        if position < source_bytes {
            bail!(
                "gzip ended at byte {position} of {source_bytes} in {}",
                job.source_path.display()
            );
        }
    }
    info!(
        "RAW source finished period={} part={} rows={} messages={} shards={} complete={} elapsed_s={:.1}",
        job.period,
        job.original_part,
        physical_rows,
        logical_messages,
        shards.len(),
        complete,
        started.elapsed().as_secs_f64()
    );
    Ok(SourceResult {
        period: job.period.clone(),
        source: SourceEntry {
            file: job
                .source_path
                .file_name()
                .and_then(|name| name.to_str())
                .context("non-UTF8 RAW source file")?
                .to_string(),
            original_part: job.original_part,
            compressed_bytes: source_bytes,
            physical_rows,
            logical_messages,
            data_bytes,
            shard_count: shards.len() as u32,
            complete,
        },
        shards,
    })
}

pub fn run(config: &Config, row_limit: Option<u64>) -> Result<()> {
    fs::create_dir_all(&config.output_root)?;
    let mut headers = BTreeMap::<String, Arc<Vec<u8>>>::new();
    let mut final_dirs = BTreeMap::<String, PathBuf>::new();
    let mut building_dirs = BTreeMap::<String, PathBuf>::new();
    let mut jobs = Vec::new();
    for period in &config.periods {
        if period.is_empty()
            || !period
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'-' || byte == b'_')
        {
            bail!("invalid period {period:?}");
        }
        let source_dir = config.data_root.join(period_dir_name(period));
        let parts = discover_parts(&source_dir)?;
        let part_zero = parts
            .iter()
            .find(|path| part_number(path).ok() == Some(0))
            .context("RAW period has no part zero")?;
        let header = Arc::new(read_header(part_zero)?);
        let final_dir = config.output_root.join(period_dir_name(period));
        let building_dir = final_dir.with_file_name(format!(
            "{}.building",
            final_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("raw-shards")
        ));
        if final_dir.exists() || building_dir.exists() {
            bail!(
                "RAW shard output already exists: {} or {}",
                final_dir.display(),
                building_dir.display()
            );
        }
        fs::create_dir_all(&building_dir)?;
        for source_path in parts {
            jobs.push(SourceJob {
                period: period.clone(),
                original_part: part_number(&source_path)?,
                source_path,
                output_dir: building_dir.clone(),
                header: Arc::clone(&header),
            });
        }
        headers.insert(period.clone(), header);
        final_dirs.insert(period.clone(), final_dir);
        building_dirs.insert(period.clone(), building_dir);
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(config.workers.min(jobs.len()).max(1))
        .thread_name(|index| format!("usstock-raw-shard-{index}"))
        .build()?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| shard_source(job, config, row_limit))
            .collect::<Result<Vec<_>>>()
    })?;

    for period in &config.periods {
        let mut sources = results
            .iter()
            .filter(|result| &result.period == period)
            .map(|result| result.source.clone())
            .collect::<Vec<_>>();
        let mut shards = results
            .iter()
            .filter(|result| &result.period == period)
            .flat_map(|result| result.shards.clone())
            .collect::<Vec<_>>();
        sources.sort_by_key(|source| source.original_part);
        shards.sort_by_key(|shard| (shard.original_part, shard.shard_index));
        let manifest = Manifest {
            schema: MANIFEST_SCHEMA.to_string(),
            period: period.clone(),
            complete: sources.iter().all(|source| source.complete),
            header: String::from_utf8(
                trim_line_ending(headers.get(period).context("missing period header")?).to_vec(),
            )?,
            rows_per_shard: config.rows_per_shard,
            zstd_level: config.zstd_level,
            sources,
            shards,
        };
        manifest.validate(period, row_limit.is_none())?;
        manifest.write(
            building_dirs
                .get(period)
                .context("missing building directory")?,
        )?;
    }
    for period in &config.periods {
        fs::rename(
            building_dirs
                .get(period)
                .context("missing building directory")?,
            final_dirs.get(period).context("missing final directory")?,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn shards_only_between_complete_outer_messages() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("merged-Data-part-000000.csv.gz");
        let header = b"#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs\n";
        let rows = [
            b"AAA.O,Market Price,2021-01-01T00:00:00.000000001Z,-5,Raw,UPDATE,QUOTE,,,,74,,1,2\n"
                .as_slice(),
            b",,,,FID,22,,BID,10.1,\n".as_slice(),
            b",,,,FID,25,,ASK,10.2,\n".as_slice(),
            b"AAA.O,Market Price,2021-01-01T00:00:00.000000002Z,-5,Raw,UPDATE,TRADE,,,,74,,2,1\n"
                .as_slice(),
            b",,,,FID,6,,TRDPRC_1,10.2,\n".as_slice(),
        ];
        let mut gzip = GzEncoder::new(File::create(&source).unwrap(), Compression::fast());
        gzip.write_all(header).unwrap();
        gzip.write_all(&rows.concat()).unwrap();
        gzip.finish().unwrap();
        let output = temp.path().join("output");
        fs::create_dir(&output).unwrap();
        let config = Config {
            data_root: temp.path().to_path_buf(),
            output_root: temp.path().join("unused"),
            periods: vec![],
            workers: 1,
            rows_per_shard: 2,
            zstd_level: 1,
            progress_every: 0,
        };
        let job = SourceJob {
            period: "2021-01-01_2022-01-01".to_string(),
            source_path: source,
            output_dir: output.clone(),
            original_part: 0,
            header: Arc::new(header.to_vec()),
        };
        let result = shard_source(&job, &config, None).unwrap();
        assert_eq!(result.source.physical_rows, 5);
        assert_eq!(result.source.logical_messages, 2);
        assert_eq!(result.shards.len(), 2);
        assert_eq!(result.shards[0].physical_rows, 3);
        assert_eq!(result.shards[1].physical_rows, 2);

        let mut recovered = Vec::new();
        for shard in &result.shards {
            let file = File::open(output.join(&shard.file)).unwrap();
            let mut decoder = zstd::stream::read::Decoder::new(file).unwrap();
            let mut bytes = Vec::new();
            decoder.read_to_end(&mut bytes).unwrap();
            let header_end = bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
            recovered.extend_from_slice(&bytes[header_end..]);
        }
        assert_eq!(recovered, rows.concat());
    }
}
