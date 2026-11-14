pub mod codec;
pub mod model;
pub mod source;

use anyhow::{anyhow, bail, Context, Result};
use rocksdb::{
    ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::Deserialize;
use source::{validate_header, Census};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub use codec::{decode_key, decode_message, encode_key, encode_message};
pub use model::{
    LevelAction, LevelDelta, LogicalMessage, MessageClass, Side, SummaryDelta, SummaryField,
};
pub use source::{parse_utc_ns, scan_csv, Census as ReplayCensus};

const CF_META: &str = "replay_meta";
const CF_PREFIX: &str = "mbp:";
const SCHEMA_KEY: &[u8] = b"schema";
const SCHEMA_ID: &[u8] = b"mbp-event";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub data_root: PathBuf,
    pub rocksdb_root: PathBuf,
    pub periods: Vec<String>,
    #[serde(default = "default_batch_messages")]
    pub batch_messages: usize,
    #[serde(default = "default_progress_every")]
    pub progress_every: u64,
}

fn default_batch_messages() -> usize {
    10_000
}
fn default_progress_every() -> u64 {
    1_000_000
}

pub fn load_config(path: &Path) -> Result<Config> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let config: Config =
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    if config.periods.is_empty() {
        bail!("config periods must not be empty");
    }
    if config.batch_messages == 0 {
        bail!("batch_messages must be positive");
    }
    Ok(config)
}

pub fn period_dir_name(period: &str) -> String {
    format!("shanghai_evolution_equities_mbp_ric_list_0_mbp_{period}")
}

fn data_period_dir(config: &Config, period: &str) -> PathBuf {
    config.data_root.join(period_dir_name(period))
}
fn data_path(config: &Config, period: &str) -> PathBuf {
    data_period_dir(config, period).join("merged-Data.csv")
}
fn report_path(config: &Config, period: &str) -> PathBuf {
    data_period_dir(config, period).join("merged-Report.csv")
}
fn final_db_path(config: &Config, period: &str) -> PathBuf {
    config.rocksdb_root.join(period)
}
fn building_db_path(config: &Config, period: &str) -> PathBuf {
    config.rocksdb_root.join(format!("{period}.building"))
}

fn report_counts(path: &Path) -> Result<BTreeMap<String, u64>> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("open {}", path.display()))?;
    let headers = reader.headers()?.clone();
    let ric_index = headers
        .iter()
        .position(|name| name == "#RIC")
        .context("report has no #RIC")?;
    let count_index = headers
        .iter()
        .position(|name| name == "Count")
        .context("report has no Count")?;
    let mut counts = BTreeMap::new();
    for result in reader.records() {
        let record = result?;
        let ric = record.get(ric_index).unwrap_or("").trim();
        if ric.is_empty() {
            continue;
        }
        let count = record
            .get(count_index)
            .unwrap_or("")
            .trim()
            .parse::<u64>()
            .with_context(|| format!("parse Count for {ric}"))?;
        if counts.insert(ric.to_string(), count).is_some() {
            bail!("duplicate report RIC {ric}");
        }
    }
    if counts.is_empty() {
        bail!("report {} has no RIC rows", path.display());
    }
    Ok(counts)
}

fn read_and_validate_header(path: &Path) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(BufReader::new(file));
    let header = reader.records().next().context("data CSV is empty")??;
    validate_header(&header)
}

pub fn preflight(config: &Config, periods: &[String]) -> Result<()> {
    for period in periods {
        validate_period(period)?;
        let dir = data_period_dir(config, period);
        let marker = dir.join("decompress.complete");
        if !marker.is_file() {
            bail!("decompression marker is missing: {}", marker.display());
        }
        let data = data_path(config, period);
        let report = report_path(config, period);
        if !data.is_file() || !report.is_file() {
            bail!("period {period} is missing decompressed data or report");
        }
        read_and_validate_header(&data)?;
        let counts = report_counts(&report)?;
        println!(
            "preflight period={period} data_bytes={} report_rics={} report_messages={}",
            fs::metadata(&data)?.len(),
            counts.len(),
            counts.values().sum::<u64>()
        );
    }
    Ok(())
}

fn validate_period(period: &str) -> Result<()> {
    if period.is_empty()
        || !period
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        bail!("invalid period {period:?}");
    }
    Ok(())
}

fn db_options() -> Options {
    let mut o = Options::default();
    o.create_if_missing(true);
    o.create_missing_column_families(true);
    o.set_max_open_files(4096);
    o.set_compression_type(DBCompressionType::Lz4);
    o.set_bytes_per_sync(64 * 1024 * 1024);
    o.increase_parallelism(16);
    o.set_max_background_jobs(16);
    o.set_db_write_buffer_size(4 * 1024 * 1024 * 1024);
    o
}

fn cf_options() -> Options {
    let mut o = Options::default();
    o.set_compression_type(DBCompressionType::Lz4);
    o.set_write_buffer_size(256 * 1024 * 1024);
    o.set_max_write_buffer_number(4);
    o.set_min_write_buffer_number_to_merge(1);
    o
}

fn open_db(path: &Path, rics: &BTreeSet<String>, existing: bool) -> Result<DB> {
    let names = if existing {
        DB::list_cf(&Options::default(), path)
            .with_context(|| format!("list column families in {}", path.display()))?
    } else {
        let mut names = vec!["default".to_string(), CF_META.to_string()];
        names.extend(rics.iter().map(|ric| format!("{CF_PREFIX}{ric}")));
        names
    };
    let expected = rics
        .iter()
        .map(|ric| format!("{CF_PREFIX}{ric}"))
        .collect::<BTreeSet<_>>();
    let actual = names
        .iter()
        .filter(|name| name.starts_with(CF_PREFIX))
        .cloned()
        .collect::<BTreeSet<_>>();
    if existing && actual != expected {
        bail!("building RocksDB column families do not match current Report");
    }
    for name in &names {
        if name != "default" && name != CF_META && !name.starts_with(CF_PREFIX) {
            bail!("unsupported column family {name:?} in MBP RocksDB");
        }
    }
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()));
    DB::open_cf_descriptors(&db_options(), path, descriptors)
        .with_context(|| format!("open RocksDB {}", path.display()))
}

fn validate_census(census: &Census, expected: &BTreeMap<String, u64>) -> Result<()> {
    if &census.messages_by_ric != expected {
        let diffs = expected
            .iter()
            .filter_map(|(ric, count)| {
                let actual = census.messages_by_ric.get(ric).copied().unwrap_or(0);
                (actual != *count).then(|| format!("{ric}: expected={count} actual={actual}"))
            })
            .take(20)
            .collect::<Vec<_>>();
        bail!("Report Count mismatch: {}", diffs.join(", "));
    }
    Ok(())
}

pub fn replay_period(config: &Config, period: &str) -> Result<Census> {
    validate_period(period)?;
    let final_path = final_db_path(config, period);
    let building_path = building_db_path(config, period);
    if final_path.exists() {
        eprintln!(
            "period={period} final database exists; verifying before skip path={}",
            final_path.display()
        );
        return verify_period(config, period);
    }
    fs::create_dir_all(&config.rocksdb_root)?;
    let expected = report_counts(&report_path(config, period))?;
    let rics = expected.keys().cloned().collect::<BTreeSet<_>>();
    let existing = building_path.is_dir();
    if existing {
        eprintln!("period={period} deterministically replaying incomplete database from source start path={}", building_path.display());
    }
    let db = open_db(&building_path, &rics, existing)?;
    let meta = db.cf_handle(CF_META).context("missing replay_meta CF")?;
    if existing {
        let schema = db.get_cf(meta, SCHEMA_KEY)?.context(
            "building RocksDB has no schema marker; refuse to mix an incompatible database",
        )?;
        if schema.as_slice() != SCHEMA_ID {
            bail!("building RocksDB uses an incompatible schema");
        }
    } else {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        db.put_cf_opt(meta, SCHEMA_KEY, SCHEMA_ID, &options)?;
    }

    let mut batch = WriteBatch::default();
    let mut pending = 0_usize;
    let mut written = 0_u64;
    let started = Instant::now();
    let census = scan_csv(&data_path(config, period), period, None, |message| {
        let cf_name = format!("{CF_PREFIX}{}", message.ric);
        let cf = db
            .cf_handle(&cf_name)
            .ok_or_else(|| anyhow!("source RIC is absent from Report: {}", message.ric))?;
        batch.put_cf(cf, encode_key(message), encode_message(message)?);
        pending += 1;
        written += 1;
        if pending >= config.batch_messages {
            let mut options = WriteOptions::default();
            options.disable_wal(true);
            db.write_opt(std::mem::take(&mut batch), &options)?;
            pending = 0;
        }
        if config.progress_every > 0 && written.is_multiple_of(config.progress_every) {
            eprintln!(
                "period={period} messages={written} source_row={} elapsed_seconds={:.1}",
                message.source_row,
                started.elapsed().as_secs_f64()
            );
        }
        Ok(())
    })?;
    if !batch.is_empty() {
        let mut options = WriteOptions::default();
        options.disable_wal(true);
        db.write_opt(batch, &options)?;
    }
    validate_census(&census, &expected)?;
    let mut sync = WriteOptions::default();
    sync.set_sync(true);
    db.put_cf_opt(meta, b"census", serde_json::to_vec(&census)?, &sync)?;
    for ric in &rics {
        let name = format!("{CF_PREFIX}{ric}");
        db.flush_cf(db.cf_handle(&name).context("missing RIC CF during flush")?)?;
    }
    db.flush_cf(meta)?;
    drop(db);
    fs::rename(&building_path, &final_path).with_context(|| {
        format!(
            "publish {} -> {}",
            building_path.display(),
            final_path.display()
        )
    })?;
    Ok(census)
}

pub fn verify_period(config: &Config, period: &str) -> Result<Census> {
    validate_period(period)?;
    let path = final_db_path(config, period);
    if !path.is_dir() {
        bail!(
            "published period RocksDB does not exist: {}",
            path.display()
        );
    }
    let names = DB::list_cf(&Options::default(), &path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    let descriptors = names
        .iter()
        .cloned()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()));
    let db = DB::open_cf_descriptors(&db_options(), &path, descriptors)?;
    let meta = db.cf_handle(CF_META).context("missing replay_meta CF")?;
    let schema = db
        .get_cf(meta, SCHEMA_KEY)?
        .context("missing schema marker")?;
    if schema.as_slice() != SCHEMA_ID {
        bail!("published RocksDB uses an incompatible schema");
    }
    let census: Census =
        serde_json::from_slice(&db.get_cf(meta, b"census")?.context("missing census")?)?;
    let mut counts = BTreeMap::new();
    let mut decoded_messages = 0_u64;
    let mut decoded_entries = 0_u64;
    let mut decoded_actions = BTreeMap::<String, u64>::new();
    let mut decoded_classes = BTreeMap::<String, u64>::new();
    let mut decoded_update_types = BTreeMap::<String, u64>::new();
    let mut decoded_summary_masks = BTreeMap::<String, u64>::new();
    let mut decoded_without_summary = 0_u64;
    let mut decoded_book_images = 0_u64;
    let mut decoded_refresh_without_entries = 0_u64;
    let mut decoded_add_existing = 0_u64;
    let mut decoded_update_missing = 0_u64;
    let mut decoded_delete_missing = 0_u64;
    let mut decoded_final_depth = BTreeMap::<String, u64>::new();
    for name in names.iter().filter(|name| name.starts_with(CF_PREFIX)) {
        let ric = name.trim_start_matches(CF_PREFIX).to_string();
        let cf = db.cf_handle(name).context("missing RIC column family")?;
        let mut count = 0_u64;
        let mut previous_key: Option<Box<[u8]>> = None;
        let mut book = HashSet::<(Side, i64)>::new();
        for item in db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item?;
            if previous_key
                .as_deref()
                .is_some_and(|previous| previous >= key.as_ref())
            {
                bail!("{name} keys are not strictly ordered");
            }
            let (key_ts, key_row) = decode_key(&key)?;
            let message = decode_message(&ric, &value)?;
            if (message.ts_utc_ns, message.source_row) != (key_ts, key_row) {
                bail!("{name} key/value source identity mismatch");
            }
            count += 1;
            decoded_messages += 1;
            decoded_entries += message.entries.len() as u64;
            *decoded_classes
                .entry(message.message_class.as_str().to_string())
                .or_default() += 1;
            *decoded_update_types
                .entry(message.update_type.clone())
                .or_default() += 1;
            let summary_mask = message.summary.as_ref().map_or(0_u32, |summary| {
                summary.fields.iter().fold(0_u32, |mask, field| {
                    mask | (1_u32 << model::summary_index(field.fid).expect("decoded Summary FID"))
                })
            });
            *decoded_summary_masks
                .entry(format!("{summary_mask:08x}"))
                .or_default() += 1;
            if message.summary.is_none() {
                decoded_without_summary += 1;
            }
            if message.message_class == MessageClass::Refresh {
                if message.entries.is_empty() {
                    decoded_refresh_without_entries += 1;
                } else {
                    decoded_book_images += 1;
                    book.clear();
                }
            }
            for entry in &message.entries {
                *decoded_actions
                    .entry(entry.action.as_str().to_string())
                    .or_default() += 1;
                let level_key = (entry.side, entry.price_e9);
                match entry.action {
                    LevelAction::Add => {
                        if !book.insert(level_key) {
                            decoded_add_existing += 1;
                        }
                    }
                    LevelAction::Update => {
                        if !book.contains(&level_key) {
                            decoded_update_missing += 1;
                        }
                        book.insert(level_key);
                    }
                    LevelAction::Delete => {
                        if !book.remove(&level_key) {
                            decoded_delete_missing += 1;
                        }
                    }
                }
            }
            previous_key = Some(key);
        }
        decoded_final_depth.insert(ric.clone(), book.len() as u64);
        counts.insert(ric, count);
    }
    if counts != census.messages_by_ric
        || decoded_messages != census.messages
        || decoded_entries != census.map_entries
        || decoded_actions != census.map_actions
        || decoded_classes != census.msg_classes
        || decoded_update_types != census.update_types
        || decoded_summary_masks != census.summary_masks
        || decoded_without_summary != census.messages_without_summary
        || decoded_book_images != census.book_images
        || decoded_refresh_without_entries != census.refresh_without_entries
        || decoded_add_existing != census.add_existing
        || decoded_update_missing != census.update_missing
        || decoded_delete_missing != census.delete_missing
        || decoded_final_depth != census.final_depth_by_ric
    {
        bail!("decoded RocksDB census does not match stored source census");
    }
    validate_census(&census, &report_counts(&report_path(config, period))?)?;
    Ok(census)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbp_small.csv")
    }

    #[test]
    fn timestamp_keeps_nanoseconds() {
        assert_eq!(parse_utc_ns("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            parse_utc_ns("2026-07-01T00:00:00.000000123Z").unwrap(),
            1_782_864_000_000_000_123
        );
    }

    #[test]
    fn fixture_codec_round_trips_all_typed_messages() {
        let golden: serde_json::Value = serde_json::from_slice(
            &fs::read(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbp_small_golden.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let mut messages = Vec::new();
        let census = scan_csv(&fixture(), "fixture", None, |message| {
            let encoded = encode_message(message)?;
            assert_eq!(decode_message(&message.ric, &encoded)?, *message);
            assert_eq!(
                decode_key(&encode_key(message))?,
                (message.ts_utc_ns, message.source_row)
            );
            messages.push(message.clone());
            Ok(())
        })
        .unwrap();
        let to_hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        for (index, message) in messages.iter().enumerate() {
            assert_eq!(to_hex(&encode_key(message)), golden[index]["key"]);
            assert_eq!(
                to_hex(&encode_message(message).unwrap()),
                golden[index]["value"]
            );
        }
        assert_eq!(messages.len(), 5);
        assert_eq!(census.book_images, 1);
        assert_eq!(census.refresh_without_entries, 1);
        assert_eq!(census.messages_without_summary, 2);
        assert_eq!(census.final_depth_by_ric["AAA.BAT"], 2);
        assert_eq!(census.final_depth_by_ric["BBB.BAT"], 0);
    }

    #[test]
    fn replay_publishes_and_verifies_rocksdb() {
        let temp = tempdir().unwrap();
        let period = "2026-01-01_2026-01-02";
        let data_root = temp.path().join("csv");
        let period_dir = data_root.join(period_dir_name(period));
        fs::create_dir_all(&period_dir).unwrap();
        fs::copy(fixture(), period_dir.join("merged-Data.csv")).unwrap();
        fs::write(
            period_dir.join("merged-Report.csv"),
            "#RIC,Domain,Start,End,Status,Count\nAAA.BAT,Market By Price,,,Active,3\nBBB.BAT,Market By Price,,,Active,2\n",
        )
        .unwrap();
        fs::write(period_dir.join("decompress.complete"), "test\n").unwrap();
        let config = Config {
            data_root,
            rocksdb_root: temp.path().join("rocksdb"),
            periods: vec![period.to_string()],
            batch_messages: 2,
            progress_every: 0,
        };
        preflight(&config, &config.periods).unwrap();
        let replayed = replay_period(&config, period).unwrap();
        assert_eq!(replayed.messages, 5);
        assert!(!building_db_path(&config, period).exists());
        assert!(final_db_path(&config, period).is_dir());
        assert_eq!(verify_period(&config, period).unwrap(), replayed);
        let path = final_db_path(&config, period);
        let names = DB::list_cf(&Options::default(), &path).unwrap();
        let descriptors = names
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, cf_options()));
        let db = DB::open_cf_descriptors(&db_options(), path, descriptors).unwrap();
        let meta = db.cf_handle(CF_META).unwrap();
        assert_eq!(
            db.get_cf(meta, SCHEMA_KEY).unwrap().as_deref(),
            Some(SCHEMA_ID)
        );
    }
}
