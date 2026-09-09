pub mod codec;
pub mod source;

use anyhow::{bail, Context, Result};
use rocksdb::{
    ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::Deserialize;
use source::{scan_gzip, scan_gzip_limited, Census};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const CF_META: &str = "replay_meta";
const CF_PREFIX: &str = "rawll2:";
const SCHEMA: &[u8] = b"lseg-usstock-rawll2-event";

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
    let config: Config = toml::from_str(&fs::read_to_string(path)?)
        .with_context(|| format!("parse {}", path.display()))?;
    if config.periods.is_empty() || config.batch_messages == 0 {
        bail!("periods and batch_messages must be nonempty");
    }
    Ok(config)
}

pub fn period_dir_name(period: &str) -> String {
    format!("shanghai_evolution_equities_raw_market_depth_ric_list_0_rl2_{period}")
}

fn period_dir(config: &Config, period: &str) -> PathBuf {
    config.data_root.join(period_dir_name(period))
}
fn data_path(config: &Config, period: &str) -> PathBuf {
    period_dir(config, period).join("merged-Data.csv.gz")
}
fn final_path(config: &Config, period: &str) -> PathBuf {
    config.rocksdb_root.join(period)
}
fn building_path(config: &Config, period: &str) -> PathBuf {
    config.rocksdb_root.join(format!("{period}.building"))
}

fn rics_from_notes(directory: &Path) -> Result<Option<BTreeSet<String>>> {
    let Some(notes) = fs::read_dir(directory)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".notes.txt"))
        })
    else {
        return Ok(None);
    };
    let text = fs::read_to_string(&notes)?;
    let mut rics = BTreeSet::new();
    for line in text.lines() {
        let Some((_, tail)) = line.split_once("expanded to 1 RIC:") else {
            continue;
        };
        let ric = tail.trim().trim_end_matches('.').trim();
        if ric.is_empty() || !ric.is_ascii() {
            bail!("invalid expanded RIC in {}: {line:?}", notes.display());
        }
        if !rics.insert(ric.to_owned()) {
            bail!("duplicate expanded RIC {ric} in {}", notes.display());
        }
    }
    if rics.is_empty() {
        bail!("no expanded RICs found in {}", notes.display());
    }
    Ok(Some(rics))
}

fn rics_for_replay(config: &Config, period: &str) -> Result<BTreeSet<String>> {
    if let Some(rics) = rics_from_notes(&period_dir(config, period))? {
        return Ok(rics);
    }
    let census = scan_gzip(&data_path(config, period), |_| Ok(()))?;
    if census.messages_by_ric.is_empty() {
        bail!("rawLL2 source {period} has no messages");
    }
    Ok(census.messages_by_ric.into_keys().collect())
}

pub fn preflight(config: &Config, periods: &[String]) -> Result<()> {
    for period in periods {
        let directory = period_dir(config, period);
        let data = data_path(config, period);
        if !data.is_file() {
            bail!("missing rawLL2 gzip {}", data.display());
        }
        if fs::read_dir(&directory)?
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "part")
            })
        {
            bail!(
                "rawLL2 period {} contains an unfinished .part file",
                directory.display()
            );
        }
        let coverage = rics_from_notes(&directory)?;
        println!(
            "preflight period={period} gzip_bytes={} notes_rics={}",
            fs::metadata(data)?.len(),
            coverage.map_or_else(
                || "source-census-required".to_owned(),
                |rics| rics.len().to_string()
            )
        );
    }
    Ok(())
}

fn options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_max_open_files(4096);
    options.set_max_background_jobs(16);
    options.increase_parallelism(16);
    options
}

fn open_db(path: &Path, rics: &BTreeSet<String>, existing: bool) -> Result<DB> {
    let names = if existing {
        DB::list_cf(&Options::default(), path)?
    } else {
        let mut names = vec!["default".to_owned(), CF_META.to_owned()];
        names.extend(rics.iter().map(|ric| format!("{CF_PREFIX}{ric}")));
        names
    };
    let actual = names
        .iter()
        .filter(|name| name.starts_with(CF_PREFIX))
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected = rics
        .iter()
        .map(|ric| format!("{CF_PREFIX}{ric}"))
        .collect::<BTreeSet<_>>();
    if actual != expected {
        bail!("rawLL2 RocksDB column families do not match notes coverage");
    }
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
    Ok(DB::open_cf_descriptors(&options(), path, descriptors)?)
}

fn put_meta(db: &DB, census: &Census) -> Result<()> {
    let meta = db
        .cf_handle(CF_META)
        .context("missing rawLL2 metadata CF")?;
    let mut batch = WriteBatch::default();
    batch.put_cf(&meta, b"schema", SCHEMA);
    batch.put_cf(&meta, b"census", serde_json::to_vec(census)?);
    let mut write = WriteOptions::default();
    write.set_sync(true);
    db.write_opt(batch, &write)?;
    Ok(())
}

pub fn replay_period(config: &Config, period: &str, max_messages: Option<u64>) -> Result<Census> {
    if let Some(limit) = max_messages {
        return scan_gzip_limited(&data_path(config, period), limit, |_| Ok(()));
    }
    let final_db = final_path(config, period);
    let building = building_path(config, period);
    if final_db.exists() {
        return verify_period(config, period);
    }
    if building.exists() {
        bail!("incomplete rawLL2 output exists: {}", building.display());
    }
    let rics = rics_for_replay(config, period)?;
    fs::create_dir_all(&config.rocksdb_root)?;
    let db = open_db(&building, &rics, false)?;
    let mut batch = WriteBatch::default();
    let mut batched = 0usize;
    let census = scan_gzip(&data_path(config, period), |message| {
        let cf = db
            .cf_handle(&format!("{CF_PREFIX}{}", message.ric))
            .with_context(|| format!("RIC {} is absent from notes coverage", message.ric))?;
        batch.put_cf(
            &cf,
            codec::encode_key(message),
            codec::encode_message(message)?,
        );
        batched += 1;
        if batched >= config.batch_messages {
            db.write(std::mem::take(&mut batch))?;
            batched = 0;
        }
        Ok(())
    })?;
    if batched != 0 {
        db.write(batch)?;
    }
    put_meta(&db, &census)?;
    db.flush()?;
    drop(db);
    fs::rename(&building, &final_db).with_context(|| format!("publish {}", final_db.display()))?;
    Ok(census)
}

pub fn verify_period(config: &Config, period: &str) -> Result<Census> {
    let final_db = final_path(config, period);
    let rics = rics_for_replay(config, period)?;
    let db = open_db(&final_db, &rics, true)?;
    let meta = db
        .cf_handle(CF_META)
        .context("missing rawLL2 metadata CF")?;
    if db.get_cf(&meta, b"schema")?.as_deref() != Some(SCHEMA) {
        bail!("rawLL2 schema mismatch");
    }
    let stored: Census = serde_json::from_slice(
        &db.get_cf(&meta, b"census")?
            .context("rawLL2 metadata has no census")?,
    )?;
    let source = scan_gzip(&data_path(config, period), |_| Ok(()))?;
    if source != stored {
        bail!("rawLL2 source census differs from published metadata");
    }
    let mut actual = Census::default();
    for ric in rics {
        let cf = db.cf_handle(&format!("{CF_PREFIX}{ric}")).unwrap();
        for record in db.iterator_cf(&cf, IteratorMode::Start) {
            let (key, value) = record?;
            let (ts, row) = codec::decode_key(&key)?;
            let message = codec::decode_message(&value, &ric, ts)?;
            if message.source_row != row {
                bail!("rawLL2 key/source row mismatch for {ric}");
            }
            actual.observe(&message);
        }
    }
    if actual != stored {
        bail!("rawLL2 RocksDB census differs from source census");
    }
    Ok(stored)
}
