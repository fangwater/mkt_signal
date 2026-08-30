//! Manifest contract for independently replayable Normalized LL2 zstd shards.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub const LL2_SHARD_MANIFEST_VERSION: u32 = 1;
pub const LL2_SHARD_MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ll2ShardGroup {
    pub ric: String,
    pub second_utc_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ll2ShardEntry {
    pub file: String,
    pub original_part: u16,
    pub shard_index: u32,
    pub rows: u64,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub first_group: Ll2ShardGroup,
    pub last_group: Ll2ShardGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ll2ShardSource {
    pub file: String,
    pub original_part: u16,
    pub compressed_bytes: u64,
    pub data_rows: u64,
    pub data_bytes: u64,
    pub shard_count: u32,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ll2ShardManifest {
    pub format_version: u32,
    pub period: String,
    pub complete: bool,
    pub header: String,
    pub rows_per_shard: u64,
    pub zstd_level: i32,
    pub sources: Vec<Ll2ShardSource>,
    pub shards: Vec<Ll2ShardEntry>,
}

pub fn ll2_period_dir_name(period: &str) -> String {
    format!("shanghai_evolution_futures_market_depth_ric_list_0_ll2_{period}")
}

pub fn ll2_shard_file_name(original_part: u16, shard_index: u32) -> String {
    format!("merged-Data-part-{original_part:06}-shard-{shard_index:06}.csv.zst")
}

pub fn ll2_manifest_path(period_dir: &Path) -> PathBuf {
    period_dir.join(LL2_SHARD_MANIFEST_FILE)
}

impl Ll2ShardManifest {
    pub fn load(period_dir: &Path) -> Result<Self> {
        let path = ll2_manifest_path(period_dir);
        let file = File::open(&path)
            .with_context(|| format!("open LL2 shard manifest {}", path.display()))?;
        serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse LL2 shard manifest {}", path.display()))
    }

    pub fn write(&self, period_dir: &Path) -> Result<()> {
        let path = ll2_manifest_path(period_dir);
        let partial = period_dir.join(format!("{LL2_SHARD_MANIFEST_FILE}.partial"));
        let file = File::create(&partial)
            .with_context(|| format!("create LL2 shard manifest {}", partial.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self)
            .with_context(|| format!("write LL2 shard manifest {}", partial.display()))?;
        writer
            .flush()
            .with_context(|| format!("flush LL2 shard manifest {}", partial.display()))?;
        writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync LL2 shard manifest {}", partial.display()))?;
        drop(writer);
        std::fs::rename(&partial, &path).with_context(|| {
            format!(
                "publish LL2 shard manifest {} -> {}",
                partial.display(),
                path.display()
            )
        })
    }

    pub fn validate(&self, expected_period: &str, require_complete: bool) -> Result<()> {
        if self.format_version != LL2_SHARD_MANIFEST_VERSION {
            bail!(
                "unsupported LL2 shard manifest version {}, expected {LL2_SHARD_MANIFEST_VERSION}",
                self.format_version
            );
        }
        if self.period != expected_period {
            bail!(
                "LL2 shard manifest period {:?} does not match requested {expected_period:?}",
                self.period
            );
        }
        if require_complete && !self.complete {
            bail!("LL2 shard manifest for {expected_period} is incomplete");
        }
        if self.header.is_empty() || self.header.contains(['\n', '\r']) {
            bail!("LL2 shard manifest has an empty or multiline header");
        }
        if self.rows_per_shard == 0 {
            bail!("LL2 shard manifest rows_per_shard must be >= 1");
        }
        if self.sources.is_empty() || self.shards.is_empty() {
            bail!("LL2 shard manifest has no sources or shards");
        }
        if self.complete != self.sources.iter().all(|source| source.complete) {
            bail!("LL2 shard manifest complete flag disagrees with its sources");
        }

        let mut sources = BTreeMap::new();
        for source in &self.sources {
            if source.file.is_empty() {
                bail!("LL2 shard source has an empty file name");
            }
            if sources.insert(source.original_part, source).is_some() {
                bail!(
                    "LL2 shard manifest repeats original part {}",
                    source.original_part
                );
            }
            if require_complete && !source.complete {
                bail!(
                    "LL2 shard source part {} is incomplete",
                    source.original_part
                );
            }
        }

        let mut totals: BTreeMap<u16, (u64, u64, u32)> = BTreeMap::new();
        let mut previous: Option<&Ll2ShardEntry> = None;
        for shard in &self.shards {
            if shard.file != ll2_shard_file_name(shard.original_part, shard.shard_index) {
                bail!("invalid LL2 shard file name {:?}", shard.file);
            }
            if shard.rows == 0 || shard.compressed_bytes == 0 {
                bail!("LL2 shard {:?} is empty", shard.file);
            }
            if !sources.contains_key(&shard.original_part) {
                bail!(
                    "LL2 shard {:?} references missing original part {}",
                    shard.file,
                    shard.original_part
                );
            }
            let total = totals.entry(shard.original_part).or_insert((0, 0, 0));
            if shard.shard_index != total.2 {
                bail!(
                    "LL2 shard part {} index {} is not contiguous from {}",
                    shard.original_part,
                    shard.shard_index,
                    total.2
                );
            }
            if let Some(prev) = previous {
                if prev.original_part == shard.original_part && prev.last_group == shard.first_group
                {
                    bail!(
                        "LL2 shard boundary splits RIC/second group {:?} between {:?} and {:?}",
                        shard.first_group,
                        prev.file,
                        shard.file
                    );
                }
            }
            total.0 = total
                .0
                .checked_add(shard.rows)
                .context("LL2 shard row overflow")?;
            total.1 = total
                .1
                .checked_add(shard.data_bytes)
                .context("LL2 shard byte overflow")?;
            total.2 += 1;
            previous = Some(shard);
        }

        for source in &self.sources {
            let actual = totals
                .get(&source.original_part)
                .copied()
                .unwrap_or_default();
            let expected = (source.data_rows, source.data_bytes, source.shard_count);
            if actual != expected {
                bail!(
                    "LL2 shard totals for part {} are {:?}, expected {:?}",
                    source.original_part,
                    actual,
                    expected
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Ll2ShardManifest {
        Ll2ShardManifest {
            format_version: LL2_SHARD_MANIFEST_VERSION,
            period: "2026-01-01_2026-06-01".into(),
            complete: true,
            header: "#RIC,Domain,Date-Time".into(),
            rows_per_shard: 2,
            zstd_level: 1,
            sources: vec![Ll2ShardSource {
                file: "merged-Data-part-000000.csv.gz".into(),
                original_part: 0,
                compressed_bytes: 100,
                data_rows: 3,
                data_bytes: 30,
                shard_count: 2,
                complete: true,
            }],
            shards: vec![
                Ll2ShardEntry {
                    file: ll2_shard_file_name(0, 0),
                    original_part: 0,
                    shard_index: 0,
                    rows: 2,
                    data_bytes: 20,
                    compressed_bytes: 10,
                    first_group: Ll2ShardGroup {
                        ric: "A".into(),
                        second_utc_ns: 1,
                    },
                    last_group: Ll2ShardGroup {
                        ric: "A".into(),
                        second_utc_ns: 2,
                    },
                },
                Ll2ShardEntry {
                    file: ll2_shard_file_name(0, 1),
                    original_part: 0,
                    shard_index: 1,
                    rows: 1,
                    data_bytes: 10,
                    compressed_bytes: 8,
                    first_group: Ll2ShardGroup {
                        ric: "A".into(),
                        second_utc_ns: 3,
                    },
                    last_group: Ll2ShardGroup {
                        ric: "A".into(),
                        second_utc_ns: 3,
                    },
                },
            ],
        }
    }

    #[test]
    fn validates_safe_boundaries() {
        manifest().validate("2026-01-01_2026-06-01", true).unwrap();
    }

    #[test]
    fn rejects_split_second_group() {
        let mut value = manifest();
        value.shards[1].first_group = value.shards[0].last_group.clone();
        assert!(value
            .validate("2026-01-01_2026-06-01", true)
            .unwrap_err()
            .to_string()
            .contains("splits RIC/second"));
    }
}
