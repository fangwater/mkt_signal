//! Split sequential Normalized LL2 gzip streams into independent zstd shards.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_shard::{
    ll2_period_dir_name, ll2_shard_file_name, Ll2ShardEntry, Ll2ShardGroup, Ll2ShardManifest,
    Ll2ShardSource, LL2_SHARD_MANIFEST_VERSION,
};
use cme_tas_replay::ll2_source::{
    parse_normalized_ll2_group, strip_line_ending, validate_normalized_ll2_header,
};
use cme_tas_replay::validate_period;
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
#[command(name = "cme_ll2_shard")]
#[command(about = "Split Normalized LL2 gzip streams at RIC/second-safe boundaries")]
struct Args {
    #[arg(long, default_value = "config/cme_ll2_shard.toml")]
    config: PathBuf,
    #[arg(long)]
    period: Option<String>,
    #[arg(long)]
    output_root: Option<PathBuf>,
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
    16
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

struct SourceJob {
    period: String,
    source_path: PathBuf,
    output_dir: PathBuf,
    original_part: u16,
    header_bytes: Arc<Vec<u8>>,
}

struct SourceResult {
    period: String,
    source: Ll2ShardSource,
    shards: Vec<Ll2ShardEntry>,
}

struct OpenShard {
    final_path: PathBuf,
    partial_path: PathBuf,
    original_part: u16,
    shard_index: u32,
    encoder: ZstdEncoder<'static, BufWriter<File>>,
    rows: u64,
    data_bytes: u64,
    first_group: Ll2ShardGroup,
}

impl OpenShard {
    fn create(
        output_dir: &Path,
        original_part: u16,
        shard_index: u32,
        header_bytes: &[u8],
        first_group: Ll2ShardGroup,
        zstd_level: i32,
    ) -> Result<Self> {
        let file_name = ll2_shard_file_name(original_part, shard_index);
        let final_path = output_dir.join(&file_name);
        let partial_path = output_dir.join(format!("{file_name}.partial"));
        let file = File::create(&partial_path)
            .with_context(|| format!("create LL2 shard {}", partial_path.display()))?;
        let writer = BufWriter::with_capacity(OUTPUT_BUFFER_BYTES, file);
        let mut encoder = ZstdEncoder::new(writer, zstd_level)
            .with_context(|| format!("create zstd encoder for {}", partial_path.display()))?;
        encoder
            .include_checksum(true)
            .with_context(|| format!("enable zstd checksum for {}", partial_path.display()))?;
        encoder.write_all(header_bytes)?;
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
            .with_context(|| format!("write LL2 shard {}", self.partial_path.display()))?;
        self.rows = self.rows.checked_add(1).context("LL2 shard row overflow")?;
        self.data_bytes = self
            .data_bytes
            .checked_add(row.len() as u64)
            .context("LL2 shard byte overflow")?;
        Ok(())
    }

    fn finish(self, last_group: Ll2ShardGroup) -> Result<Ll2ShardEntry> {
        let mut writer = self
            .encoder
            .finish()
            .with_context(|| format!("finish LL2 shard {}", self.partial_path.display()))?;
        writer.flush()?;
        writer.get_ref().sync_data()?;
        drop(writer);
        fs::rename(&self.partial_path, &self.final_path).with_context(|| {
            format!(
                "publish LL2 shard {} -> {}",
                self.partial_path.display(),
                self.final_path.display()
            )
        })?;
        let compressed_bytes = fs::metadata(&self.final_path)?.len();
        Ok(Ll2ShardEntry {
            file: self
                .final_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("non-UTF8 LL2 shard path {}", self.final_path.display()))?
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

fn source_period_dir(config: &ShardConfig, period: &str) -> PathBuf {
    config.data_root.join(ll2_period_dir_name(period))
}

fn output_period_dir(config: &ShardConfig, period: &str) -> PathBuf {
    config.output_root.join(ll2_period_dir_name(period))
}

fn staging_dir(final_dir: &Path) -> PathBuf {
    let name = final_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ll2_shards");
    final_dir.with_file_name(format!("{name}.building"))
}

fn discover_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = fs::read_dir(dir)
        .with_context(|| format!("read LL2 period {}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz")
                })
        })
        .collect::<Vec<_>>();
    parts.sort();
    if parts.is_empty() {
        bail!("no LL2 gzip parts under {}", dir.display());
    }
    Ok(parts)
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("LL2 part path {} has no UTF-8 name", path.display()))?;
    name.strip_prefix("merged-Data-part-")
        .and_then(|rest| rest.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized LL2 part name {name}"))?
        .parse()
        .with_context(|| format!("parse LL2 part number in {name}"))
}

fn read_header(part_zero: &Path) -> Result<Vec<u8>> {
    let file = File::open(part_zero)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    let mut header = Vec::new();
    if reader.read_until(b'\n', &mut header)? == 0 {
        bail!("LL2 part 0 is empty: {}", part_zero.display());
    }
    validate_normalized_ll2_header(&header)?;
    Ok(header)
}

fn group(line: &[u8]) -> Result<Ll2ShardGroup> {
    let value = parse_normalized_ll2_group(line)?;
    Ok(Ll2ShardGroup {
        ric: value.ric,
        second_utc_ns: value.second_utc_ns,
    })
}

fn shard_source(job: &SourceJob, config: &ShardConfig) -> Result<SourceResult> {
    let started = Instant::now();
    let source_bytes = fs::metadata(&job.source_path)?.len();
    let file = File::open(&job.source_path)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(INPUT_BUFFER_BYTES, file));
    let mut reader = BufReader::with_capacity(INPUT_BUFFER_BYTES, decoder);
    if job.original_part == 0 {
        let mut header = Vec::new();
        if reader.read_until(b'\n', &mut header)? == 0 {
            bail!("LL2 part 0 is empty: {}", job.source_path.display());
        }
        if strip_line_ending(&header) != strip_line_ending(&job.header_bytes) {
            bail!("LL2 part 0 header changed in {}", job.source_path.display());
        }
    }

    info!(
        "cme_ll2_shard start period={} part={} rows_per_shard={} source={}",
        job.period,
        job.original_part,
        config.rows_per_shard,
        job.source_path.display()
    );
    let mut shards = Vec::new();
    let mut open: Option<OpenShard> = None;
    let mut pending_cut_group: Option<Ll2ShardGroup> = None;
    let mut line = Vec::with_capacity(1024);
    let mut last_line = Vec::with_capacity(1024);
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
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if strip_line_ending(&line).is_empty() {
            bail!("empty LL2 source row in {}", job.source_path.display());
        }

        let mut parsed_group = None;
        if let Some(cut_group) = pending_cut_group.as_ref() {
            let current_group = group(&line)?;
            if &current_group != cut_group {
                let finished = open
                    .take()
                    .expect("pending LL2 cut requires open shard")
                    .finish(cut_group.clone())?;
                info!(
                    "cme_ll2_shard shard_done period={} part={} shard={} rows={} compressed_bytes={}",
                    job.period,
                    job.original_part,
                    finished.shard_index,
                    finished.rows,
                    finished.compressed_bytes
                );
                shards.push(finished);
                pending_cut_group = None;
            }
            parsed_group = Some(current_group);
        }

        if open.is_none() {
            let first_group = parsed_group
                .take()
                .map(Ok)
                .unwrap_or_else(|| group(&line))?;
            open = Some(OpenShard::create(
                &job.output_dir,
                job.original_part,
                shards.len() as u32,
                &job.header_bytes,
                first_group,
                config.zstd_level,
            )?);
        }

        let writer = open.as_mut().expect("LL2 shard opened");
        writer.write_row(&line)?;
        total_rows = total_rows
            .checked_add(1)
            .context("LL2 source row overflow")?;
        total_bytes = total_bytes
            .checked_add(line.len() as u64)
            .context("LL2 source byte overflow")?;
        if writer.rows >= config.rows_per_shard && pending_cut_group.is_none() {
            pending_cut_group = Some(
                parsed_group
                    .take()
                    .map(Ok)
                    .unwrap_or_else(|| group(&line))?,
            );
        }
        if config.progress_every > 0 && total_rows % config.progress_every == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            info!(
                "cme_ll2_shard progress period={} part={} rows={} rows_per_s={:.0} elapsed_s={:.1}",
                job.period,
                job.original_part,
                total_rows,
                total_rows as f64 / elapsed,
                elapsed
            );
        }
        std::mem::swap(&mut line, &mut last_line);
    }

    if let Some(writer) = open.take() {
        let last_group = match pending_cut_group {
            Some(value) => value,
            None => group(&last_line)?,
        };
        shards.push(writer.finish(last_group)?);
    }
    if shards.is_empty() {
        bail!("LL2 source has no rows: {}", job.source_path.display());
    }
    if complete {
        let decoder = reader.into_inner();
        let mut source = decoder.into_inner().into_inner();
        let position = source.stream_position()?;
        if position < source_bytes {
            bail!(
                "LL2 gzip ended at byte {position} of {source_bytes} in {}",
                job.source_path.display()
            );
        }
    }
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    info!(
        "cme_ll2_shard source_done period={} part={} rows={} shards={} complete={} rows_per_s={:.0} elapsed_s={:.1}",
        job.period,
        job.original_part,
        total_rows,
        shards.len(),
        complete,
        total_rows as f64 / elapsed,
        elapsed
    );
    Ok(SourceResult {
        period: job.period.clone(),
        source: Ll2ShardSource {
            file: job
                .source_path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("non-UTF8 LL2 source path"))?
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
    if config.periods.is_empty() || config.workers == 0 || config.rows_per_shard == 0 {
        bail!("periods must be nonempty and workers/rows_per_shard must be >= 1");
    }
    fs::create_dir_all(&config.output_root)?;
    let mut headers = BTreeMap::<String, Arc<Vec<u8>>>::new();
    let mut final_dirs = BTreeMap::new();
    let mut staging_dirs = BTreeMap::new();
    let mut jobs = Vec::new();
    for period in &config.periods {
        validate_period(period)?;
        let parts = discover_parts(&source_period_dir(config, period))?;
        let part_zero = parts
            .iter()
            .find(|path| part_number(path).ok() == Some(0))
            .ok_or_else(|| anyhow!("LL2 period {period} has no part 0"))?;
        let header = Arc::new(read_header(part_zero)?);
        let final_dir = output_period_dir(config, period);
        let building = staging_dir(&final_dir);
        if final_dir.exists() || building.exists() {
            bail!(
                "LL2 shard output or staging already exists: {} / {}",
                final_dir.display(),
                building.display()
            );
        }
        fs::create_dir_all(&building)?;
        for source_path in parts {
            jobs.push(SourceJob {
                period: period.clone(),
                original_part: part_number(&source_path)?,
                source_path,
                output_dir: building.clone(),
                header_bytes: Arc::clone(&header),
            });
        }
        headers.insert(period.clone(), header);
        final_dirs.insert(period.clone(), final_dir);
        staging_dirs.insert(period.clone(), building);
    }

    let started = Instant::now();
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.workers.min(jobs.len()).max(1))
        .thread_name(|id| format!("cme-ll2-shard-{id}"))
        .build()?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| shard_source(job, config))
            .collect::<Result<Vec<_>>>()
    })?;

    for period in &config.periods {
        let mut sources = Vec::new();
        let mut shards = Vec::new();
        for result in results.iter().filter(|value| &value.period == period) {
            sources.push(result.source.clone());
            shards.extend(result.shards.iter().cloned());
        }
        sources.sort_by_key(|source| source.original_part);
        shards.sort_by_key(|shard| (shard.original_part, shard.shard_index));
        let complete = sources.iter().all(|source| source.complete);
        let header = String::from_utf8(
            strip_line_ending(headers.get(period).expect("LL2 header loaded")).to_vec(),
        )?;
        let manifest = Ll2ShardManifest {
            format_version: LL2_SHARD_MANIFEST_VERSION,
            period: period.clone(),
            complete,
            header,
            rows_per_shard: config.rows_per_shard,
            zstd_level: config.zstd_level,
            sources,
            shards,
        };
        manifest.validate(period, false)?;
        manifest.write(staging_dirs.get(period).expect("LL2 staging created"))?;
    }
    for period in &config.periods {
        fs::rename(
            staging_dirs.get(period).expect("LL2 staging created"),
            final_dirs.get(period).expect("LL2 final planned"),
        )?;
    }
    info!(
        "cme_ll2_shard complete periods={} sources={} elapsed_s={:.1}",
        config.periods.len(),
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
    let result = (|| -> Result<()> {
        let text = fs::read_to_string(&args.config)?;
        let mut config: ShardConfig = toml::from_str(&text)?;
        if let Some(period) = args.period {
            config.periods = vec![period];
        }
        if let Some(output_root) = args.output_root {
            config.output_root = output_root;
        }
        if args.max_source_rows.is_some() {
            config.max_source_rows = args.max_source_rows;
        }
        run(&config)
    })();
    if let Err(error) = result {
        eprintln!("cme_ll2_shard failed: {error:#}");
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

    fn header() -> Vec<u8> {
        let mut fields = vec![
            "#RIC".to_string(),
            "Domain".to_string(),
            "Date-Time".to_string(),
            "GMT Offset".to_string(),
            "Type".to_string(),
        ];
        for level in 1..=10 {
            fields.extend([
                format!("L{level}-BidPrice"),
                format!("L{level}-BidSize"),
                format!("L{level}-BuyNo"),
                format!("L{level}-AskPrice"),
                format!("L{level}-AskSize"),
                format!("L{level}-SellNo"),
            ]);
        }
        fields.push("Exch Time".into());
        format!("{}\n", fields.join(",")).into_bytes()
    }

    fn row(ts: &str) -> Vec<u8> {
        format!("A,Market Price,{ts},-6,Normalized LL2\n").into_bytes()
    }

    #[test]
    fn keeps_equal_ric_second_together() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("merged-Data-part-000000.csv.gz");
        let rows = [
            row("2026-01-01T00:00:00.100000000Z"),
            row("2026-01-01T00:00:01.100000000Z"),
            row("2026-01-01T00:00:01.900000000Z"),
            row("2026-01-01T00:00:02.100000000Z"),
        ];
        let file = File::create(&source).unwrap();
        let mut gzip = GzEncoder::new(file, Compression::fast());
        gzip.write_all(&header()).unwrap();
        for value in &rows {
            gzip.write_all(value).unwrap();
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
            period: "2026-01-01_2026-06-01".into(),
            source_path: source,
            output_dir: output.clone(),
            original_part: 0,
            header_bytes: Arc::new(header()),
        };
        let result = shard_source(&job, &config).unwrap();
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
