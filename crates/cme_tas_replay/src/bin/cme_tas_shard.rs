use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::shard::{
    period_dir_name, shard_file_name, CsvGroup, TasShardEntry, TasShardManifest, TasShardSource,
    SHARD_MANIFEST_VERSION,
};
use cme_tas_replay::validate_period;
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use log::info;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use zstd::stream::write::Encoder as ZstdEncoder;

const INPUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const OUTPUT_BUFFER_BYTES: usize = 4 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_shard")]
#[command(about = "Split sequential TAS gzip streams into independent zstd CSV shards")]
struct Args {
    #[arg(long, default_value = "config/cme_tas_shard.toml")]
    config: PathBuf,
    #[arg(long)]
    max_source_rows: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardConfig {
    data_root: PathBuf,
    output_root: PathBuf,
    periods: Vec<String>,
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default = "default_rows_per_shard")]
    rows_per_shard: u64,
    #[serde(default = "default_zstd_level")]
    zstd_level: i32,
    #[serde(default = "default_progress_every")]
    progress_every: u64,
    #[serde(default)]
    max_source_rows: Option<u64>,
}

fn default_workers() -> usize {
    4
}

fn default_rows_per_shard() -> u64 {
    25_000_000
}

fn default_zstd_level() -> i32 {
    1
}

fn default_progress_every() -> u64 {
    10_000_000
}

#[derive(Clone, Copy)]
struct GroupColumns {
    ric: usize,
    date_time: usize,
}

struct SourceJob {
    period: String,
    source_path: PathBuf,
    output_dir: PathBuf,
    original_part: u16,
    header_bytes: Arc<Vec<u8>>,
    group_columns: GroupColumns,
}

struct SourceResult {
    period: String,
    source: TasShardSource,
    shards: Vec<TasShardEntry>,
}

struct OpenShard {
    final_path: PathBuf,
    partial_path: PathBuf,
    original_part: u16,
    shard_index: u32,
    encoder: ZstdEncoder<'static, BufWriter<File>>,
    rows: u64,
    data_bytes: u64,
    first_group: CsvGroup,
}

impl OpenShard {
    fn create(
        output_dir: &Path,
        original_part: u16,
        shard_index: u32,
        header_bytes: &[u8],
        first_group: CsvGroup,
        zstd_level: i32,
    ) -> Result<Self> {
        let file_name = shard_file_name(original_part, shard_index);
        let final_path = output_dir.join(&file_name);
        let partial_path = output_dir.join(format!("{file_name}.partial"));
        let file = File::create(&partial_path)
            .with_context(|| format!("create TAS shard {}", partial_path.display()))?;
        let writer = BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, file);
        let mut encoder = ZstdEncoder::new(writer, zstd_level)
            .with_context(|| format!("create zstd encoder for {}", partial_path.display()))?;
        encoder
            .include_checksum(true)
            .with_context(|| format!("enable zstd checksum for {}", partial_path.display()))?;
        encoder
            .write_all(header_bytes)
            .with_context(|| format!("write TAS header to {}", partial_path.display()))?;
        if !header_bytes.ends_with(b"\n") {
            encoder.write_all(b"\n")?;
        }
        Ok(Self {
            final_path,
            partial_path,
            original_part,
            shard_index,
            encoder,
            rows: 0,
            data_bytes: 0,
            first_group,
        })
    }

    fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.encoder
            .write_all(row)
            .with_context(|| format!("write TAS shard {}", self.partial_path.display()))?;
        self.rows = self
            .rows
            .checked_add(1)
            .context("TAS shard row count overflow")?;
        self.data_bytes = self
            .data_bytes
            .checked_add(row.len() as u64)
            .context("TAS shard byte count overflow")?;
        Ok(())
    }

    fn finish(self, last_group: CsvGroup) -> Result<TasShardEntry> {
        let mut writer = self
            .encoder
            .finish()
            .with_context(|| format!("finish zstd shard {}", self.partial_path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flush zstd shard {}", self.partial_path.display()))?;
        writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync zstd shard {}", self.partial_path.display()))?;
        drop(writer);
        fs::rename(&self.partial_path, &self.final_path).with_context(|| {
            format!(
                "publish TAS shard {} -> {}",
                self.partial_path.display(),
                self.final_path.display()
            )
        })?;
        let compressed_bytes = fs::metadata(&self.final_path)
            .with_context(|| format!("stat TAS shard {}", self.final_path.display()))?
            .len();
        Ok(TasShardEntry {
            file: self
                .final_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("non-UTF8 TAS shard path {}", self.final_path.display()))?
                .to_string(),
            original_part: self.original_part,
            shard_index: self.shard_index,
            rows: self.rows,
            data_bytes: self.data_bytes,
            compressed_bytes,
            first_group: self.first_group,
            last_group,
        })
    }
}

fn period_source_dir(config: &ShardConfig, period: &str) -> PathBuf {
    config.data_root.join(period_dir_name(period))
}

fn period_output_dir(config: &ShardConfig, period: &str) -> PathBuf {
    config.output_root.join(period_dir_name(period))
}

fn staging_dir(final_dir: &Path) -> PathBuf {
    let name = final_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tas_shards");
    final_dir.with_file_name(format!("{name}.building"))
}

fn discover_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read TAS period {}", dir.display()))? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz") {
            parts.push(path);
        }
    }
    parts.sort();
    if parts.is_empty() {
        bail!("no merged-Data-part-*.csv.gz under {}", dir.display());
    }
    Ok(parts)
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("part path {} has no UTF-8 file name", path.display()))?;
    let digits = name
        .strip_prefix("merged-Data-part-")
        .and_then(|rest| rest.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized TAS part name {name}"))?;
    digits
        .parse()
        .with_context(|| format!("parse TAS part number in {name}"))
}

fn trim_line_ending(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line = &line[..line.len() - 1];
    }
    line
}

fn parse_csv_record(line: &[u8]) -> Result<StringRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(trim_line_ending(line));
    match reader.records().next() {
        Some(Ok(record)) => Ok(record),
        Some(Err(err)) => Err(err.into()),
        None => bail!("empty TAS CSV row"),
    }
}

fn group_columns(header_bytes: &[u8]) -> Result<GroupColumns> {
    let headers = parse_csv_record(header_bytes)?;
    if headers.get(0).map(str::trim) != Some("#RIC") {
        bail!("TAS header does not start with #RIC");
    }
    let find = |name: &str| {
        headers
            .iter()
            .position(|field| field.trim() == name)
            .ok_or_else(|| anyhow!("TAS header is missing {name:?}"))
    };
    Ok(GroupColumns {
        ric: find("#RIC")?,
        date_time: find("Date-Time")?,
    })
}

fn parse_group(line: &[u8], columns: GroupColumns) -> Result<CsvGroup> {
    let record = parse_csv_record(line)?;
    let ric = record
        .get(columns.ric)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("TAS shard boundary row has empty #RIC"))?;
    let date_time = record
        .get(columns.date_time)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("TAS shard boundary row has empty Date-Time"))?;
    Ok(CsvGroup {
        ric: ric.to_string(),
        date_time: date_time.to_string(),
    })
}

fn read_header(part_zero: &Path) -> Result<Vec<u8>> {
    let file = File::open(part_zero)
        .with_context(|| format!("open TAS part 0 header {}", part_zero.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut header = Vec::new();
    let read = reader
        .read_until(b'\n', &mut header)
        .with_context(|| format!("read TAS header from {}", part_zero.display()))?;
    if read == 0 {
        bail!("TAS part 0 is empty: {}", part_zero.display());
    }
    group_columns(&header)?;
    Ok(header)
}

fn shard_source(job: &SourceJob, config: &ShardConfig) -> Result<SourceResult> {
    let started = Instant::now();
    let source_bytes = fs::metadata(&job.source_path)
        .with_context(|| format!("stat TAS source {}", job.source_path.display()))?
        .len();
    let file = File::open(&job.source_path)
        .with_context(|| format!("open TAS source {}", job.source_path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);

    if job.original_part == 0 {
        let mut source_header = Vec::new();
        if reader.read_until(b'\n', &mut source_header)? == 0 {
            bail!("TAS part 0 is empty: {}", job.source_path.display());
        }
        if trim_line_ending(&source_header) != trim_line_ending(&job.header_bytes) {
            bail!("TAS part 0 header changed in {}", job.source_path.display());
        }
    }

    info!(
        "cme_tas_shard start period={} part={} source={} rows_per_shard={} max_source_rows={:?}",
        job.period,
        job.original_part,
        job.source_path.display(),
        config.rows_per_shard,
        config.max_source_rows
    );

    let mut shards = Vec::new();
    let mut open: Option<OpenShard> = None;
    let mut pending_cut_group: Option<CsvGroup> = None;
    let mut line = Vec::new();
    let mut last_line = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut complete = true;

    loop {
        if config
            .max_source_rows
            .is_some_and(|limit| total_rows >= limit)
        {
            complete = false;
            break;
        }
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read TAS source {}", job.source_path.display()))?;
        if read == 0 {
            break;
        }

        let mut parsed_group = None;
        if let Some(cut_group) = pending_cut_group.as_ref() {
            let group = parse_group(&line, job.group_columns)?;
            if &group != cut_group {
                let finished = open
                    .take()
                    .expect("pending TAS cut requires an open shard")
                    .finish(cut_group.clone())?;
                info!(
                    "cme_tas_shard finished period={} part={} shard={} rows={} data_bytes={} compressed_bytes={}",
                    job.period,
                    job.original_part,
                    finished.shard_index,
                    finished.rows,
                    finished.data_bytes,
                    finished.compressed_bytes
                );
                shards.push(finished);
                pending_cut_group = None;
            }
            parsed_group = Some(group);
        }

        if open.is_none() {
            let first_group = match parsed_group.take() {
                Some(group) => group,
                None => parse_group(&line, job.group_columns)?,
            };
            open = Some(OpenShard::create(
                &job.output_dir,
                job.original_part,
                shards.len() as u32,
                &job.header_bytes,
                first_group,
                config.zstd_level,
            )?);
        }

        let writer = open.as_mut().expect("TAS shard was opened above");
        writer.write_row(&line)?;
        total_rows = total_rows
            .checked_add(1)
            .context("TAS source row count overflow")?;
        total_bytes = total_bytes
            .checked_add(line.len() as u64)
            .context("TAS source byte count overflow")?;

        if writer.rows >= config.rows_per_shard && pending_cut_group.is_none() {
            pending_cut_group = Some(match parsed_group.take() {
                Some(group) => group,
                None => parse_group(&line, job.group_columns)?,
            });
        }
        if config.progress_every > 0 && total_rows % config.progress_every == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            info!(
                "cme_tas_shard progress period={} part={} rows={} data_gib={:.2} rows_per_s={:.0} elapsed_s={:.1}",
                job.period,
                job.original_part,
                total_rows,
                total_bytes as f64 / 1024f64.powi(3),
                total_rows as f64 / elapsed,
                elapsed
            );
        }
        std::mem::swap(&mut line, &mut last_line);
    }

    if let Some(writer) = open.take() {
        let last_group = match pending_cut_group {
            Some(group) => group,
            None => parse_group(&last_line, job.group_columns)?,
        };
        let finished = writer.finish(last_group)?;
        info!(
            "cme_tas_shard finished period={} part={} shard={} rows={} data_bytes={} compressed_bytes={}",
            job.period,
            job.original_part,
            finished.shard_index,
            finished.rows,
            finished.data_bytes,
            finished.compressed_bytes
        );
        shards.push(finished);
    }
    if shards.is_empty() {
        bail!("TAS source has no data rows: {}", job.source_path.display());
    }

    if complete {
        let decoder = reader.into_inner();
        let mut source = decoder.into_inner().into_inner();
        let position = source
            .stream_position()
            .with_context(|| format!("tell TAS source {}", job.source_path.display()))?;
        if position < source_bytes {
            bail!(
                "gzip ended at byte {position} of {source_bytes} in {}; concatenated member was not consumed",
                job.source_path.display()
            );
        }
    }

    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    info!(
        "cme_tas_shard source_done period={} part={} rows={} data_bytes={} shards={} complete={} rows_per_s={:.0} elapsed_s={:.1}",
        job.period,
        job.original_part,
        total_rows,
        total_bytes,
        shards.len(),
        complete,
        total_rows as f64 / elapsed,
        elapsed
    );
    Ok(SourceResult {
        period: job.period.clone(),
        source: TasShardSource {
            file: job
                .source_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("non-UTF8 TAS source path {}", job.source_path.display()))?
                .to_string(),
            original_part: job.original_part,
            compressed_bytes: source_bytes,
            data_rows: total_rows,
            data_bytes: total_bytes,
            shard_count: shards.len() as u32,
            complete,
        },
        shards,
    })
}

fn run(config: &ShardConfig) -> Result<()> {
    if config.periods.is_empty() {
        bail!("periods must not be empty");
    }
    if config.workers == 0 || config.rows_per_shard == 0 {
        bail!("workers and rows_per_shard must be >= 1");
    }
    if config.max_source_rows == Some(0) {
        bail!("max_source_rows must be >= 1 when set");
    }
    fs::create_dir_all(&config.output_root).with_context(|| {
        format!(
            "create TAS shard output root {}",
            config.output_root.display()
        )
    })?;

    let mut headers: BTreeMap<String, Arc<Vec<u8>>> = BTreeMap::new();
    let mut final_dirs = BTreeMap::new();
    let mut staging_dirs = BTreeMap::new();
    let mut jobs = Vec::new();
    for period in &config.periods {
        validate_period(period)?;
        let source_dir = period_source_dir(config, period);
        let parts = discover_parts(&source_dir)?;
        let part_zero = parts
            .iter()
            .find(|path| part_number(path).ok() == Some(0))
            .ok_or_else(|| anyhow!("TAS period {period} has no part 0 header"))?;
        let header = Arc::new(read_header(part_zero)?);
        let columns = group_columns(&header)?;
        let final_dir = period_output_dir(config, period);
        let building = staging_dir(&final_dir);
        if final_dir.exists() {
            bail!(
                "TAS shard output {} already exists; refuse to overwrite",
                final_dir.display()
            );
        }
        if building.exists() {
            bail!(
                "leftover TAS shard staging {} exists; inspect and remove it first",
                building.display()
            );
        }
        fs::create_dir_all(&building)
            .with_context(|| format!("create TAS shard staging {}", building.display()))?;
        for source_path in parts {
            jobs.push(SourceJob {
                period: period.clone(),
                original_part: part_number(&source_path)?,
                source_path,
                output_dir: building.clone(),
                header_bytes: Arc::clone(&header),
                group_columns: columns,
            });
        }
        headers.insert(period.clone(), header);
        final_dirs.insert(period.clone(), final_dir);
        staging_dirs.insert(period.clone(), building);
    }

    let started = Instant::now();
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.workers.min(jobs.len()).max(1))
        .thread_name(|id| format!("cme-tas-shard-{id}"))
        .build()
        .context("build TAS shard worker pool")?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| shard_source(job, config))
            .collect::<Result<Vec<_>>>()
    })?;

    for period in &config.periods {
        let mut sources = Vec::new();
        let mut shards = Vec::new();
        for result in results.iter().filter(|result| &result.period == period) {
            sources.push(result.source.clone());
            shards.extend(result.shards.iter().cloned());
        }
        sources.sort_by_key(|source| source.original_part);
        shards.sort_by_key(|shard| (shard.original_part, shard.shard_index));
        let complete = sources.iter().all(|source| source.complete);
        let header = String::from_utf8(
            trim_line_ending(
                headers
                    .get(period)
                    .expect("TAS period header was loaded above"),
            )
            .to_vec(),
        )
        .context("TAS header is not UTF-8")?;
        let manifest = TasShardManifest {
            format_version: SHARD_MANIFEST_VERSION,
            period: period.clone(),
            complete,
            header,
            rows_per_shard: config.rows_per_shard,
            zstd_level: config.zstd_level,
            sources,
            shards,
        };
        manifest.validate(period, false)?;
        let building = staging_dirs
            .get(period)
            .expect("TAS period staging dir was created above");
        manifest.write(building)?;
    }

    for period in &config.periods {
        let building = staging_dirs
            .get(period)
            .expect("TAS period staging dir was created above");
        let final_dir = final_dirs
            .get(period)
            .expect("TAS period final dir was created above");
        fs::rename(building, final_dir).with_context(|| {
            format!(
                "publish TAS shard period {} -> {}",
                building.display(),
                final_dir.display()
            )
        })?;
    }
    info!(
        "cme_tas_shard finished periods={:?} sources={} elapsed_s={:.1}",
        config.periods,
        jobs.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config).unwrap_or_else(|err| {
        panic!("read TAS shard config {}: {err}", args.config.display());
    });
    let mut config: ShardConfig = toml::from_str(&content).unwrap_or_else(|err| {
        panic!("parse TAS shard config {}: {err}", args.config.display());
    });
    if args.max_source_rows.is_some() {
        config.max_source_rows = args.max_source_rows;
    }
    if let Err(err) = run(&config) {
        eprintln!("cme_tas_shard failed: {err:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Read;
    use tempfile::TempDir;

    #[test]
    fn sharder_keeps_equal_ric_timestamp_rows_together() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("merged-Data-part-000000.csv.gz");
        let header = b"#RIC,Date-Time,Type\n";
        let rows = [
            b"A,2020-01-01T00:00:00.000000000Z,Trade\n".as_slice(),
            b"A,2020-01-01T00:00:01.000000000Z,Trade\n".as_slice(),
            b"A,2020-01-01T00:00:01.000000000Z,Quote\n".as_slice(),
            b"A,2020-01-01T00:00:02.000000000Z,Trade\n".as_slice(),
        ];
        let file = File::create(&source).unwrap();
        let mut gzip = GzEncoder::new(file, Compression::fast());
        gzip.write_all(header).unwrap();
        for row in rows {
            gzip.write_all(row).unwrap();
        }
        gzip.finish().unwrap();

        let output = temp.path().join("output");
        fs::create_dir(&output).unwrap();
        let config = ShardConfig {
            data_root: temp.path().to_path_buf(),
            output_root: temp.path().join("unused"),
            periods: vec![],
            workers: 1,
            rows_per_shard: 2,
            zstd_level: 1,
            progress_every: 0,
            max_source_rows: None,
        };
        let job = SourceJob {
            period: "2010-01-01_2011-01-01".to_string(),
            source_path: source,
            output_dir: output.clone(),
            original_part: 0,
            header_bytes: Arc::new(header.to_vec()),
            group_columns: GroupColumns {
                ric: 0,
                date_time: 1,
            },
        };
        let result = shard_source(&job, &config).unwrap();
        assert_eq!(result.source.data_rows, 4);
        assert_eq!(result.shards.len(), 2);
        assert_eq!(result.shards[0].rows, 3);
        assert_ne!(result.shards[0].last_group, result.shards[1].first_group);

        let mut recovered = Vec::new();
        for shard in &result.shards {
            let file = File::open(output.join(&shard.file)).unwrap();
            let mut decoder = zstd::stream::read::Decoder::new(file).unwrap();
            let mut content = Vec::new();
            decoder.read_to_end(&mut content).unwrap();
            let header_end = content.iter().position(|byte| *byte == b'\n').unwrap() + 1;
            recovered.extend_from_slice(&content[header_end..]);
        }
        assert_eq!(recovered, rows.concat());
    }
}
