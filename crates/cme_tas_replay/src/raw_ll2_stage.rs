//! Parallel gzip -> compact zstd shards, followed by causal part-state stitching.
use crate::decode_ric;
use crate::ll2_1min::{decode_ll2_minute, encode_ll2_minute};
use crate::raw_ll2::{
    inherit_unknown, validate_quality, Census, Checkpoint, Extractor, RawMessage, RawScanner,
    RawSnapshot,
};
use crate::raw_ll2_io::{
    available_package, client, expected_counts, input, package, source_part_index, Config,
    Inventory, Package, SourceFile,
};
use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const MAGIC: &[u8; 16] = b"RAWLL2-STAGE0001";
const ROW_BYTES: usize = 472;
pub const REFRESH_POLICY: &str = "discard_prior_knowledge_apply_explicit_fields";
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub name: String,
    pub rows: u64,
    pub bytes: u64,
}
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Part {
    pub refresh_policy: String,
    pub part: usize,
    pub source: SourceFile,
    pub frequency: u32,
    #[serde(default)]
    pub stored_frequencies: Vec<u32>,
    pub census: Census,
    pub checkpoints: BTreeMap<String, Checkpoint>,
    pub first_ts: BTreeMap<String, u64>,
    pub first_depth: BTreeMap<String, u64>,
    pub leading_fids: Vec<(String, String)>,
    pub pending: Option<RawMessage>,
    pub shards: Vec<Shard>,
    pub elapsed_seconds: f64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub period: String,
    pub frequency: u32,
    pub parts: Vec<Part>,
    pub census: Census,
    pub expected: BTreeMap<String, u64>,
}
pub struct Plan {
    pub seeds: Vec<BTreeMap<String, Checkpoint>>,
    pub boundary: Vec<RawSnapshot>,
    pub contested: BTreeSet<(u32, [u8; 24])>,
    pub census: Census,
}
pub fn free_bytes(path: &Path) -> Result<u64> {
    let s = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs writes the structure on success and does not retain the path.
    let rc = unsafe { libc::statvfs(s.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}
fn guard(config: &Config, path: &Path, initial: bool) -> Result<()> {
    let required = config.min_free_bytes + if initial { config.disk_budget_bytes } else { 0 };
    let free = free_bytes(path)?;
    if free < required {
        bail!(
            "insufficient disk: free={free}, required={required}, path={}",
            path.display()
        );
    }
    Ok(())
}
fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("json.partial");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}
struct Sink {
    root: PathBuf,
    rows_per_shard: u64,
    shards: Vec<Shard>,
    encoder: Option<zstd::stream::write::Encoder<'static, BufWriter<File>>>,
    rows: u64,
}
impl Sink {
    fn emit(&mut self, s: RawSnapshot) -> Result<()> {
        if self.encoder.is_none() {
            let path = self
                .root
                .join(format!("shard-{:06}.zst.partial", self.shards.len()));
            let mut e = zstd::stream::write::Encoder::new(
                BufWriter::with_capacity(1024 * 1024, File::create(path)?),
                3,
            )?;
            e.include_checksum(true)?;
            e.write_all(MAGIC)?;
            self.encoder = Some(e);
        }
        let mut row = [0u8; ROW_BYTES];
        row[..24].copy_from_slice(&s.key()?);
        row[24..28].copy_from_slice(&s.seconds.to_le_bytes());
        row[28] = u8::from(!s.inherit_prior);
        row[32..464].copy_from_slice(&encode_ll2_minute(&s.value));
        row[464..].copy_from_slice(&s.known.to_le_bytes());
        self.encoder.as_mut().unwrap().write_all(&row)?;
        self.rows += 1;
        if self.rows == self.rows_per_shard {
            self.close()?;
        }
        Ok(())
    }
    fn close(&mut self) -> Result<()> {
        if let Some(e) = self.encoder.take() {
            let mut file = e.finish()?;
            file.flush()?;
            file.get_ref().sync_all()?;
            let name = format!("shard-{:06}.zst", self.shards.len());
            fs::rename(
                self.root.join(format!("{name}.partial")),
                self.root.join(&name),
            )?;
            self.shards.push(Shard {
                name: name.clone(),
                rows: self.rows,
                bytes: fs::metadata(self.root.join(name))?.len(),
            });
            self.rows = 0;
        }
        Ok(())
    }
}
pub fn part_root(root: &Path, part: usize) -> PathBuf {
    root.join(format!("source-{part:06}"))
}
fn stage_part(
    config: &Config,
    p: &Package,
    file: &SourceFile,
    part: usize,
    root: &Path,
    cancelled: &AtomicBool,
) -> Result<Part> {
    let root = part_root(root, part);
    fs::create_dir_all(&root)?;
    let manifest = root.join("manifest.json");
    if manifest.exists() {
        let old: Part = serde_json::from_reader(File::open(&manifest)?)?;
        if old.part != part
            || old.source != *file
            || old.frequency != config.frequency
            || old.stored_frequencies != [config.frequency]
            || old.refresh_policy != REFRESH_POLICY
        {
            bail!(
                "staging source metadata/config changed: {}",
                manifest.display()
            );
        }
        for s in &old.shards {
            if fs::metadata(root.join(&s.name))?.len() != s.bytes {
                bail!("incomplete shard {}", s.name);
            }
        }
        eprintln!(
            "raw_stage_reuse part={part} messages={} shards={}",
            old.census.messages,
            old.shards.len()
        );
        return Ok(old);
    }
    // Only remove this worker's unfinished, derived directory. Sources are elsewhere.
    for entry in fs::read_dir(&root)? {
        let path = entry?.path();
        if path.is_file() {
            fs::remove_file(path)?;
        } else {
            bail!("unexpected nested stage directory");
        }
    }
    guard(config, &root, false)?;
    let started = Instant::now();
    let c = client()?;
    let mut scan = RawScanner::default();
    let mut extract = Extractor::new(config.frequency)?;
    let mut sink = Sink {
        root: root.clone(),
        rows_per_shard: config.rows_per_shard,
        shards: Vec::new(),
        encoder: None,
        rows: 0,
    };
    let mut first_ts = BTreeMap::new();
    let mut first_depth = BTreeMap::new();
    let (start, end) = period_window(&p.period)?;
    eprintln!(
        "raw_stage_start part={part} source={} bytes={}",
        file.name, file.remote_bytes
    );
    scan.scan_independent(
        input(config, p, file, &c)?,
        part == 0,
        None,
        &mut |mut m| {
            if m.order % 65536 == 0 && cancelled.load(Ordering::Relaxed) {
                bail!("cancelled after another part failed");
            }
            if m.ts < start || m.ts >= end {
                bail!("Raw message outside period");
            }
            if m.order >= 1 << 48 || part >= 1 << 16 {
                bail!("partition source order overflow");
            }
            if m.order % 5_000_000 == 0 {
                guard(config, &root, false)?;
                eprintln!(
                    "raw_stage_progress part={part} messages={} seconds={:.1} ric={}",
                    m.order,
                    started.elapsed().as_secs_f64(),
                    m.ric
                );
            }
            m.order |= (part as u64) << 48;
            first_ts.entry(m.ric.clone()).or_insert(m.ts);
            let ric = m.ric.clone();
            let ts = m.ts;
            let n = extract.depth_messages;
            if let Err(error) = extract.apply(&m, &mut |s| sink.emit(s)) {
                let _ = atomic_json(&root.join("invalid_message.json"), &m);
                return Err(error).with_context(|| {
                    format!("Raw {} ts={} source_order={}", m.ric, m.ts, m.order)
                });
            }
            if extract.depth_messages > n {
                first_depth.entry(ric).or_insert(ts);
            }
            Ok(())
        },
    )?;
    let mut pending = scan.take_pending();
    if let Some(m) = &mut pending {
        m.order |= (part as u64) << 48;
    }
    extract.finish(&mut |s| sink.emit(s))?;
    sink.close()?;
    scan.census.depth_messages = extract.depth_messages;
    let result = Part {
        refresh_policy: REFRESH_POLICY.into(),
        part,
        source: file.clone(),
        frequency: config.frequency,
        stored_frequencies: vec![config.frequency],
        census: scan.census,
        checkpoints: extract.checkpoints(),
        first_ts,
        first_depth,
        leading_fids: scan.leading_fids,
        pending,
        shards: sink.shards,
        elapsed_seconds: started.elapsed().as_secs_f64(),
    };
    atomic_json(&manifest, &result)?;
    eprintln!(
        "raw_stage_done part={part} messages={} shards={} seconds={:.1}",
        result.census.messages,
        result.shards.len(),
        result.elapsed_seconds
    );
    Ok(result)
}
pub fn period_window(period: &str) -> Result<(u64, u64)> {
    let (a, b) = period.split_once('_').context("invalid period")?;
    let parse =
        |s: &str| crate::ll2_source::parse_ll2_datetime_ns(format!("{s}T00:00:00Z").as_bytes());
    let a = parse(a)?;
    let b = parse(b)?;
    if a >= b {
        bail!("invalid period bounds");
    }
    Ok((a, b))
}
pub fn plan(manifest: &Manifest) -> Result<Plan> {
    Extractor::new(manifest.frequency)?;
    let mut states = BTreeMap::<String, Checkpoint>::new();
    let mut seeds = Vec::new();
    let mut boundary = Vec::new();
    let mut contested = BTreeSet::new();
    let mut census = Census::default();
    let mut pending: Option<RawMessage> = None;
    let frequencies = [manifest.frequency];
    let (start, end) = period_window(&manifest.period)?;
    for (i, part) in manifest.parts.iter().enumerate() {
        if part.part != i
            || part.frequency != manifest.frequency
            || part.stored_frequencies != [manifest.frequency]
            || part.refresh_policy != REFRESH_POLICY
        {
            bail!("invalid part sequence/frequency");
        }
        if let Some(mut m) = pending.take() {
            m.fields.extend(part.leading_fids.clone());
            if m.fields.len() > m.declared {
                bail!("cross-part FID overflow");
            }
            if m.fields.len() < m.declared {
                if !part.first_ts.is_empty() || part.pending.is_some() {
                    bail!("new header before cross-part message completes");
                }
                pending = Some(m);
                seeds.push(states.clone());
                census.append(&part.census);
                continue;
            }
            if m.ts < start || m.ts >= end {
                bail!("boundary message outside period");
            }
            let mut e = Extractor::new(manifest.frequency)?;
            if let Some(s) = states.get(&m.ric) {
                e.seed(m.ric.clone(), s.clone())?;
            }
            census.observe(&m);
            e.apply(&m, &mut |s| {
                boundary.push(s);
                Ok(())
            })?;
            e.finish(&mut |s| {
                boundary.push(s);
                Ok(())
            })?;
            census.depth_messages += e.depth_messages;
            states.extend(e.checkpoints());
        } else if !part.leading_fids.is_empty() {
            bail!("orphan leading FIDs");
        }
        for (ric, ts) in &part.first_ts {
            if states.get(ric).is_some_and(|s| *ts < s.last_ts) {
                bail!("time reversal across parts: {ric}");
            }
        }
        for (ric, ts) in &part.first_depth {
            if let Some(prev) = states.get(ric) {
                for seconds in &frequencies {
                    let span = u64::from(*seconds) * 1_000_000_000;
                    if prev.value.source_ts_utc_ns != 0
                        && prev.value.source_ts_utc_ns / span == *ts / span
                    {
                        let s = RawSnapshot {
                            ric: ric.clone(),
                            seconds: *seconds,
                            bucket_ns: ts / span * span,
                            value: prev.value.clone(),
                            known: prev.known,
                            inherit_prior: false,
                        };
                        contested.insert((*seconds, s.key()?));
                    }
                }
            }
        }
        seeds.push(states.clone());
        for (ric, local) in &part.checkpoints {
            let mut corrected = local.clone();
            if let Some(prior) = states.get(ric).filter(|_| corrected.inherit_prior) {
                inherit_unknown(&mut corrected.value, &mut corrected.known, prior);
                if corrected.value.source_ts_utc_ns == 0 {
                    corrected.value.source_ts_utc_ns = prior.value.source_ts_utc_ns;
                    corrected.value.source_order = prior.value.source_order;
                    corrected.value.gmt_offset_minutes = prior.value.gmt_offset_minutes;
                }
            }
            states.insert(ric.clone(), corrected);
        }
        census.append(&part.census);
        pending = part.pending.clone();
    }
    if pending.is_some() {
        bail!("last Raw part has truncated message");
    }
    for b in &boundary {
        contested.insert((b.seconds, b.key()?));
    }
    let expected: BTreeMap<_, _> = manifest
        .expected
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(k, n)| (k.clone(), *n))
        .collect();
    if census.messages_by_ric != expected {
        bail!("Raw message census differs from Report Count");
    }
    Ok(Plan {
        seeds,
        boundary,
        contested,
        census,
    })
}
pub fn build(config: &Config) -> Result<()> {
    build_mode(config, false)
}
/// Stage finalized gzip files at their original part indices without publishing a period manifest.
pub fn build_available(config: &Config) -> Result<()> {
    build_mode(config, true)
}
fn build_mode(config: &Config, available_only: bool) -> Result<()> {
    if config.partition_workers == 0 || config.rows_per_shard == 0 {
        bail!("workers and rows_per_shard must be positive");
    }
    let root = config.shard_root.as_ref().context("shard_root required")?;
    if root == &config.rocksdb_dir
        || root.starts_with(&config.rocksdb_dir)
        || config
            .data_root
            .as_ref()
            .is_some_and(|s| root.starts_with(s))
    {
        bail!("staging must be separate from source and database");
    }
    fs::create_dir_all(root)?;
    guard(config, root, true)?;
    let inv: Inventory = serde_json::from_reader(File::open(&config.inventory)?)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.partition_workers)
        .build()?;
    for period in &config.periods {
        let p = if available_only {
            available_package(&inv, config, period)?
        } else {
            package(&inv, config, period)?
        };
        let root = root.join(&p.directory);
        fs::create_dir_all(&root)?;
        let mut files: Vec<_> = p
            .files
            .iter()
            .filter(|f| f.name.starts_with("merged-Data-part-") && f.name.ends_with(".csv.gz"))
            .collect();
        files.sort_by_key(|f| &f.name);
        let expected = if available_only {
            BTreeMap::new()
        } else {
            expected_counts(config, p, &client()?)?
        };
        let cancelled = AtomicBool::new(false);
        let results: Vec<Result<Part>> = pool.install(|| {
            files
                .par_iter()
                .map(|f| {
                    // Gaps must not renumber subsequent files: these IDs encode source order.
                    let i = source_part_index(f)?;
                    let result = stage_part(config, p, f, i, &root, &cancelled)
                        .with_context(|| format!("stage part {i}"));
                    if let Err(e) = &result {
                        eprintln!("raw_stage_failed part={i} error={e:#}");
                        let _ =
                            fs::write(part_root(&root, i).join("error.txt"), format!("{e:#}\n"));
                        cancelled.store(true, Ordering::Relaxed);
                    }
                    result
                })
                .collect()
        });
        let parts: Vec<Part> = results.into_iter().collect::<Result<_>>()?;
        if available_only {
            atomic_json(
                &root.join("available_parts.json"),
                &serde_json::json!({
                    "period":period,"frequency":config.frequency,"status":"available_parts_staged",
                    "complete":false,"source_part_indices":parts.iter().map(|p|p.part).collect::<Vec<_>>(),
                    "missing_part_indices":p.missing_part_indices_through_last,"partial_files":p.partial_files,
                    "shards":parts.iter().map(|p|p.shards.len()).sum::<usize>()
                }),
            )?;
            eprintln!(
                "raw_stage_available_done period={period} parts={} complete=false",
                parts.len()
            );
            continue;
        }
        let mut manifest = Manifest {
            period: period.clone(),
            frequency: config.frequency,
            parts,
            census: Census::default(),
            expected,
        };
        manifest.census = plan(&manifest)?.census;
        atomic_json(&root.join("manifest.json"), &manifest)?;
        eprintln!(
            "raw_stage_period_done period={period} messages={} shards={}",
            manifest.census.messages,
            manifest.parts.iter().map(|p| p.shards.len()).sum::<usize>()
        );
    }
    Ok(())
}
pub fn load(config: &Config, period: &str) -> Result<(Manifest, PathBuf)> {
    let root = config
        .shard_root
        .as_ref()
        .context("shard_root")?
        .join(format!(
            "shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_{period}"
        ));
    let manifest: Manifest = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
    if manifest.period != period || manifest.frequency != config.frequency {
        bail!("staging period/frequency mismatch");
    }
    let inv: Inventory = serde_json::from_reader(File::open(&config.inventory)?)?;
    let p = package(&inv, config, period)?;
    let mut files: Vec<_> = p
        .files
        .iter()
        .filter(|f| f.name.starts_with("merged-Data-part-") && f.name.ends_with(".csv.gz"))
        .collect();
    files.sort_by_key(|f| &f.name);
    if files.len() != manifest.parts.len()
        || manifest.parts.iter().any(|p| {
            p.refresh_policy != REFRESH_POLICY || p.stored_frequencies != [config.frequency]
        })
        || files
            .iter()
            .zip(&manifest.parts)
            .any(|(f, p)| **f != p.source)
    {
        bail!("staging inventory mismatch");
    }
    Ok((manifest, root))
}
pub fn read_shard<F: FnMut(RawSnapshot) -> Result<()>>(
    root: &Path,
    part: &Part,
    shard: &Shard,
    seed: &BTreeMap<String, Checkpoint>,
    emit: &mut F,
) -> Result<()> {
    let path = part_root(root, part.part).join(&shard.name);
    if shard.name
        != format!(
            "shard-{:06}.zst",
            part.shards
                .iter()
                .position(|s| s.name == shard.name)
                .context("unknown shard")?
        )
        || fs::metadata(&path)?.len() != shard.bytes
    {
        bail!("invalid shard name/size");
    }
    let mut reader = zstd::stream::read::Decoder::new(BufReader::new(File::open(path)?))?;
    let mut magic = [0; 16];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("invalid Raw stage header");
    }
    let mut row = [0u8; ROW_BYTES];
    for _ in 0..shard.rows {
        reader.read_exact(&mut row)?;
        let seconds = u32::from_le_bytes(row[24..28].try_into()?);
        if ![15, 60].contains(&seconds)
            || seconds != part.frequency
            || part.stored_frequencies != [part.frequency]
            || row[28] > 1
            || row[29..32] != [0; 3]
        {
            bail!("invalid stage frequency/padding");
        }
        let mut s = RawSnapshot {
            ric: decode_ric(&row[..16])?,
            seconds,
            bucket_ns: u64::from_be_bytes(row[16..24].try_into()?),
            value: decode_ll2_minute(&row[32..464])?,
            known: u64::from_le_bytes(row[464..472].try_into()?),
            inherit_prior: row[28] == 0,
        };
        validate_quality(&s.value, s.known)?;
        let span = u64::from(seconds) * 1_000_000_000;
        if s.value.source_order >> 48 != part.part as u64
            || s.value.source_ts_utc_ns < s.bucket_ns
            || s.value.source_ts_utc_ns >= s.bucket_ns + span
        {
            bail!("invalid shard source order/timestamp");
        }
        s.key()?;
        if let Some(prior) = seed.get(&s.ric).filter(|_| s.inherit_prior) {
            inherit_unknown(&mut s.value, &mut s.known, prior);
        }
        validate_quality(&s.value, s.known)?;
        emit(s)?;
    }
    if reader.read(&mut [0])? != 0 {
        bail!("extra stage rows");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    #[test]
    fn available_parts_keep_sparse_source_ids_and_rejoin_when_missing_file_arrives() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let period = "2024-01-01_2025-01-01";
        let directory =
            format!("shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_{period}");
        let config = Config {
            inventory: tmp.path().join("inventory.json"),
            remote_root: "http://fixture/rawLL2".into(),
            data_root: Some(tmp.path().join("source")),
            shard_root: Some(tmp.path().join("stage")),
            rocksdb_dir: tmp.path().join("db"),
            periods: vec![period.into()],
            frequency: 15,
            rows_per_shard: 2,
            partition_workers: 3,
            min_free_bytes: 0,
            disk_budget_bytes: 0,
            ..Config::default()
        };
        let source = config.data_root.as_ref().unwrap().join(&directory);
        fs::create_dir_all(&source)?;
        let input = include_bytes!("../tests/fixtures/raw_ll2_small.csv");
        let rows: Vec<_> = input.split_inclusive(|b| *b == b'\n').collect();
        let cuts = [0, rows.len() / 3, rows.len() * 2 / 3, rows.len()];
        let mut files = Vec::new();
        for i in 0..3 {
            let name = format!("merged-Data-part-{i:06}.csv.gz");
            let path = source.join(&name);
            let mut gz = GzEncoder::new(File::create(&path)?, Compression::fast());
            for row in &rows[cuts[i]..cuts[i + 1]] {
                gz.write_all(row)?;
            }
            gz.finish()?;
            files.push(SourceFile {
                url: format!("{}/{directory}/{name}", config.remote_root),
                name,
                remote_bytes: fs::metadata(path)?.len(),
                etag: None,
            });
        }
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_small.expected.json"
        ))?;
        let report = source.join("merged-Report.csv.gz");
        let mut gz = GzEncoder::new(File::create(&report)?, Compression::fast());
        writeln!(gz, "#RIC,Count")?;
        for (ric, count) in expected["census"]["messages_by_ric"].as_object().unwrap() {
            writeln!(gz, "{ric},{count}")?;
        }
        gz.finish()?;
        let report_file = SourceFile {
            name: "merged-Report.csv.gz".into(),
            url: format!("{}/{directory}/merged-Report.csv.gz", config.remote_root),
            remote_bytes: fs::metadata(&report)?.len(),
            etag: None,
        };
        let write_inventory = |complete: bool| -> Result<()> {
            let mut selected = vec![files[0].clone(), files[2].clone(), report_file.clone()];
            if complete {
                selected.push(files[1].clone());
            }
            atomic_json(
                &config.inventory,
                &serde_json::json!({
                    "remote_root":config.remote_root,"packages":[{
                        "directory":directory,"period":period,"files":selected,
                        "missing_part_indices_through_last":if complete {vec![]} else {vec![1]},
                        "partial_files":if complete {vec![]} else {vec!["merged-Data-part-000001.csv.gz.part"]},
                        "report_present":true
                    }]
                }),
            )
        };
        write_inventory(false)?;
        assert!(build(&config).is_err());
        build_available(&config)?;
        let root = config.shard_root.as_ref().unwrap().join(&directory);
        assert!(!root.join("manifest.json").exists());
        assert!(!part_root(&root, 1).exists());
        assert!(load(&config, period).is_err());
        let old = fs::read(part_root(&root, 2).join("manifest.json"))?;
        let staged: Part = serde_json::from_slice(&old)?;
        assert_eq!(staged.part, 2);
        write_inventory(true)?;
        build(&config)?;
        assert_eq!(old, fs::read(part_root(&root, 2).join("manifest.json"))?);
        let (manifest, _) = load(&config, period)?;
        assert_eq!(
            manifest.parts.iter().map(|p| p.part).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(serde_json::to_value(&manifest.census)?, expected["census"]);
        // A legacy dual-frequency part must not be silently reused as 15s only.
        let mut legacy: serde_json::Value = serde_json::from_slice(&old)?;
        legacy.as_object_mut().unwrap().remove("stored_frequencies");
        atomic_json(&part_root(&root, 2).join("manifest.json"), &legacy)?;
        assert!(build(&config).is_err());
        let mut legacy_manifest = manifest;
        legacy_manifest.parts[2].stored_frequencies.clear();
        assert!(plan(&legacy_manifest).is_err());
        Ok(())
    }
    fn staged(parts: &[&[u8]]) -> Result<serde_json::Value> {
        let tmp = tempfile::tempdir()?;
        let config = Config {
            data_root: Some(tmp.path().join("source")),
            frequency: 15,
            rows_per_shard: 2,
            ..Config::default()
        };
        let all = parts
            .iter()
            .flat_map(|p| p.iter().copied())
            .collect::<Vec<_>>();
        let mut rows = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(&all[..]);
        let first = rows.records().next().context("fixture first message")??;
        let year = first[2][..4].parse::<u16>()?;
        let mut package = Package {
            directory: "fixture".into(),
            period: format!("{year}-01-01_{}-01-01", year + 1),
            files: vec![],
            missing_part_indices_through_last: vec![],
            partial_files: vec![],
            report_present: true,
        };
        fs::create_dir_all(config.data_root.as_ref().unwrap().join(&package.directory))?;
        let mut scan = RawScanner::default();
        for (i, part) in parts.iter().enumerate() {
            scan.scan(*part, i == 0, None, &mut |_| Ok(()))?;
        }
        scan.finish()?;
        for (i, part) in parts.iter().enumerate() {
            let name = format!("merged-Data-part-{i:06}.csv.gz");
            let path = config
                .data_root
                .as_ref()
                .unwrap()
                .join(&package.directory)
                .join(&name);
            let mut gz = GzEncoder::new(File::create(&path)?, Compression::fast());
            gz.write_all(part)?;
            gz.finish()?;
            package.files.push(SourceFile {
                name,
                url: "fixture".into(),
                remote_bytes: fs::metadata(path)?.len(),
                etag: None,
            });
        }
        let root = tmp.path().join("stage");
        fs::create_dir_all(&root)?;
        let parts: Vec<_> = package
            .files
            .par_iter()
            .enumerate()
            .map(|(i, f)| stage_part(&config, &package, f, i, &root, &AtomicBool::new(false)))
            .collect::<Result<_>>()?;
        let manifest = Manifest {
            period: package.period,
            frequency: 15,
            parts,
            census: Census::default(),
            expected: scan.census.messages_by_ric,
        };
        let plan = plan(&manifest)?;
        let mut snapshots = BTreeMap::<(u32, [u8; 24]), RawSnapshot>::new();
        let mut emit = |s: RawSnapshot| -> Result<()> {
            let index = (s.seconds, s.key()?);
            if snapshots.get(&index).is_none_or(|old| {
                (old.value.source_ts_utc_ns, old.value.source_order)
                    < (s.value.source_ts_utc_ns, s.value.source_order)
            }) {
                snapshots.insert(index, s);
            }
            Ok(())
        };
        for s in plan.boundary {
            emit(s)?;
        }
        // Reverse ingestion order makes boundary latest-wins independent of workers.
        for p in manifest.parts.iter().rev() {
            for s in p.shards.iter().rev() {
                read_shard(&root, p, s, &plan.seeds[p.part], &mut emit)?;
            }
        }
        let mut out = Vec::new();
        for (_, mut s) in snapshots {
            s.value.source_order = 0;
            out.push(s.json()?);
        }
        Ok(serde_json::json!({"census":plan.census,"snapshots":out}))
    }
    #[test]
    fn parallel_shards_match_python_at_every_csv_record_boundary() -> Result<()> {
        let input = include_bytes!("../tests/fixtures/raw_ll2_small.csv");
        let mut expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_small.expected.json"
        ))?;
        for s in expected["snapshots"].as_array_mut().unwrap() {
            let bytes = s["value_hex"].as_str().unwrap().to_owned();
            // source_order at value bytes 12..20. Independent partitions use part-prefix order.
            s["value_hex"] = serde_json::Value::String(format!(
                "{}0000000000000000{}",
                &bytes[..24],
                &bytes[40..]
            ));
        }
        assert_eq!(staged(&[input])?, expected);
        for cut in input
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'\n')
            .map(|(i, _)| i + 1)
            .filter(|i| *i < input.len())
        {
            assert_eq!(
                staged(&[&input[..cut], &input[cut..]])?,
                expected,
                "cut={cut}"
            );
        }
        // One CSV row per part, including messages spanning multiple source parts.
        let rows: Vec<_> = input.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(staged(&rows)?, expected);
        Ok(())
    }
    #[test]
    fn refresh_images_survive_parallel_fid_and_part_edges() -> Result<()> {
        let input = include_bytes!("../tests/fixtures/raw_ll2_refresh.csv");
        let mut expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_refresh.expected.json"
        ))?;
        for s in expected["snapshots"].as_array_mut().unwrap() {
            let bytes = s["value_hex"].as_str().unwrap().to_owned();
            s["value_hex"] = serde_json::Value::String(format!(
                "{}0000000000000000{}",
                &bytes[..24],
                &bytes[40..]
            ));
        }
        assert_eq!(staged(&[input])?, expected);
        let rows: Vec<_> = input.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(staged(&rows)?, expected);
        Ok(())
    }

    #[test]
    fn partial_refresh_blocks_prior_part_inheritance_at_every_record_edge() -> Result<()> {
        let input = include_bytes!("../tests/fixtures/raw_ll2_partial_refresh.csv");
        let mut expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_partial_refresh.expected.json"
        ))?;
        for s in expected["snapshots"].as_array_mut().unwrap() {
            let bytes = s["value_hex"].as_str().unwrap().to_owned();
            s["value_hex"] = serde_json::Value::String(format!(
                "{}0000000000000000{}",
                &bytes[..24],
                &bytes[40..]
            ));
        }
        assert_eq!(staged(&[input])?, expected);
        for cut in input
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'\n')
            .map(|(i, _)| i + 1)
            .filter(|i| *i < input.len())
        {
            assert_eq!(
                staged(&[&input[..cut], &input[cut..]])?,
                expected,
                "cut={cut}"
            );
        }
        let rows: Vec<_> = input.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(staged(&rows)?, expected);
        Ok(())
    }

    #[test]
    fn status_notifications_survive_part_boundaries_without_new_buckets() -> Result<()> {
        let input = include_bytes!("../tests/fixtures/raw_ll2_status.csv");
        let mut expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_status.expected.json"
        ))?;
        for s in expected["snapshots"].as_array_mut().unwrap() {
            let bytes = s["value_hex"].as_str().unwrap().to_owned();
            s["value_hex"] = serde_json::Value::String(format!(
                "{}0000000000000000{}",
                &bytes[..24],
                &bytes[40..]
            ));
        }
        let rows: Vec<_> = input.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(staged(&rows)?, expected);
        Ok(())
    }

    #[test]
    fn incomplete_or_orphan_part_edges_fail_closed() {
        let input = include_bytes!("../tests/fixtures/raw_ll2_small.csv");
        let cut = input.windows(10).position(|s| s == b",,,,FID,40").unwrap();
        assert!(staged(&[&input[..cut]]).is_err());
        assert!(staged(&[&input[cut..]]).is_err());
    }
}
