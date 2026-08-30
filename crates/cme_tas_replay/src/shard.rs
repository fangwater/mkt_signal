use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

pub const SHARD_MANIFEST_VERSION: u32 = 1;
pub const SHARD_MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvGroup {
    pub ric: String,
    pub date_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TasShardEntry {
    pub file: String,
    pub original_part: u16,
    pub shard_index: u32,
    pub rows: u64,
    pub data_bytes: u64,
    pub compressed_bytes: u64,
    pub first_group: CsvGroup,
    pub last_group: CsvGroup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TasShardSource {
    pub file: String,
    pub original_part: u16,
    pub compressed_bytes: u64,
    pub data_rows: u64,
    pub data_bytes: u64,
    pub shard_count: u32,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TasShardManifest {
    pub format_version: u32,
    pub period: String,
    pub complete: bool,
    pub header: String,
    pub rows_per_shard: u64,
    pub zstd_level: i32,
    pub sources: Vec<TasShardSource>,
    pub shards: Vec<TasShardEntry>,
}

pub fn period_dir_name(period: &str) -> String {
    format!("shanghai_evolution_futures_time_and_sales_ric_list_0_tas_{period}")
}

pub fn shard_file_name(original_part: u16, shard_index: u32) -> String {
    format!("merged-Data-part-{original_part:06}-shard-{shard_index:06}.csv.zst")
}

pub fn manifest_path(period_dir: &Path) -> PathBuf {
    period_dir.join(SHARD_MANIFEST_FILE)
}

impl TasShardManifest {
    pub fn load(period_dir: &Path) -> Result<Self> {
        let path = manifest_path(period_dir);
        let file = File::open(&path)
            .with_context(|| format!("open TAS shard manifest {}", path.display()))?;
        let manifest = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse TAS shard manifest {}", path.display()))?;
        Ok(manifest)
    }

    pub fn write(&self, period_dir: &Path) -> Result<()> {
        let path = manifest_path(period_dir);
        let partial = period_dir.join(format!("{SHARD_MANIFEST_FILE}.partial"));
        let file = File::create(&partial)
            .with_context(|| format!("create TAS shard manifest {}", partial.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self)
            .with_context(|| format!("write TAS shard manifest {}", partial.display()))?;
        use std::io::Write;
        writer
            .flush()
            .with_context(|| format!("flush TAS shard manifest {}", partial.display()))?;
        writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("sync TAS shard manifest {}", partial.display()))?;
        drop(writer);
        std::fs::rename(&partial, &path).with_context(|| {
            format!(
                "publish TAS shard manifest {} -> {}",
                partial.display(),
                path.display()
            )
        })?;
        Ok(())
    }

    pub fn validate(&self, expected_period: &str, require_complete: bool) -> Result<()> {
        if self.format_version != SHARD_MANIFEST_VERSION {
            bail!(
                "unsupported TAS shard manifest version {}, expected {SHARD_MANIFEST_VERSION}",
                self.format_version
            );
        }
        if self.period != expected_period {
            bail!(
                "TAS shard manifest period {:?} does not match requested {expected_period:?}",
                self.period
            );
        }
        if require_complete && !self.complete {
            bail!("TAS shard manifest for {expected_period} is incomplete");
        }
        if self.header.is_empty() || self.header.contains('\n') || self.header.contains('\r') {
            bail!("TAS shard manifest has an empty or multiline header");
        }
        if self.rows_per_shard == 0 {
            bail!("TAS shard manifest rows_per_shard must be >= 1");
        }
        if self.sources.is_empty() || self.shards.is_empty() {
            bail!("TAS shard manifest has no sources or shards");
        }
        if self.complete != self.sources.iter().all(|source| source.complete) {
            bail!("TAS shard manifest complete flag disagrees with its sources");
        }

        let mut sources = BTreeMap::new();
        for source in &self.sources {
            if sources.insert(source.original_part, source).is_some() {
                bail!(
                    "TAS shard manifest repeats original part {}",
                    source.original_part
                );
            }
            if require_complete && !source.complete {
                bail!(
                    "TAS shard source part {} is incomplete",
                    source.original_part
                );
            }
        }

        let mut totals: BTreeMap<u16, (u64, u64, u32)> = BTreeMap::new();
        let mut previous: Option<&TasShardEntry> = None;
        for shard in &self.shards {
            if shard.file != shard_file_name(shard.original_part, shard.shard_index) {
                bail!("invalid TAS shard file name {:?}", shard.file);
            }
            if shard.rows == 0 {
                bail!("TAS shard {:?} has zero rows", shard.file);
            }
            if shard.compressed_bytes == 0 {
                bail!("TAS shard {:?} has zero compressed bytes", shard.file);
            }
            let source = sources.get(&shard.original_part).ok_or_else(|| {
                anyhow::anyhow!(
                    "TAS shard {:?} references missing original part {}",
                    shard.file,
                    shard.original_part
                )
            })?;
            let total = totals.entry(shard.original_part).or_insert((0, 0, 0));
            if shard.shard_index != total.2 {
                bail!(
                    "TAS shard part {} index {} is not contiguous from {}",
                    shard.original_part,
                    shard.shard_index,
                    total.2
                );
            }
            if let Some(prev) = previous {
                if prev.original_part == shard.original_part && prev.last_group == shard.first_group
                {
                    bail!(
                        "TAS shard boundary splits group {:?} between {:?} and {:?}",
                        shard.first_group,
                        prev.file,
                        shard.file
                    );
                }
            }
            total.0 = total
                .0
                .checked_add(shard.rows)
                .context("TAS shard row total overflow")?;
            total.1 = total
                .1
                .checked_add(shard.data_bytes)
                .context("TAS shard byte total overflow")?;
            total.2 += 1;
            previous = Some(shard);

            if source.file.is_empty() {
                bail!("TAS shard source has an empty file name");
            }
        }

        for source in &self.sources {
            let (rows, bytes, count) = totals
                .get(&source.original_part)
                .copied()
                .unwrap_or_default();
            if (rows, bytes, count) != (source.data_rows, source.data_bytes, source.shard_count) {
                bail!(
                    "TAS shard totals for part {} are rows={rows} bytes={bytes} count={count}, expected rows={} bytes={} count={}",
                    source.original_part,
                    source.data_rows,
                    source.data_bytes,
                    source.shard_count
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> TasShardManifest {
        TasShardManifest {
            format_version: SHARD_MANIFEST_VERSION,
            period: "2010-01-01_2011-01-01".to_string(),
            complete: true,
            header: "#RIC,Date-Time".to_string(),
            rows_per_shard: 2,
            zstd_level: 1,
            sources: vec![TasShardSource {
                file: "merged-Data-part-000000.csv.gz".to_string(),
                original_part: 0,
                compressed_bytes: 123,
                data_rows: 3,
                data_bytes: 30,
                shard_count: 2,
                complete: true,
            }],
            shards: vec![
                TasShardEntry {
                    file: shard_file_name(0, 0),
                    original_part: 0,
                    shard_index: 0,
                    rows: 2,
                    data_bytes: 20,
                    compressed_bytes: 10,
                    first_group: CsvGroup {
                        ric: "A".to_string(),
                        date_time: "1".to_string(),
                    },
                    last_group: CsvGroup {
                        ric: "A".to_string(),
                        date_time: "2".to_string(),
                    },
                },
                TasShardEntry {
                    file: shard_file_name(0, 1),
                    original_part: 0,
                    shard_index: 1,
                    rows: 1,
                    data_bytes: 10,
                    compressed_bytes: 8,
                    first_group: CsvGroup {
                        ric: "A".to_string(),
                        date_time: "3".to_string(),
                    },
                    last_group: CsvGroup {
                        ric: "A".to_string(),
                        date_time: "3".to_string(),
                    },
                },
            ],
        }
    }

    #[test]
    fn manifest_validates_contiguous_safe_shards() {
        manifest().validate("2010-01-01_2011-01-01", true).unwrap();
    }

    #[test]
    fn manifest_rejects_a_split_timestamp_group() {
        let mut value = manifest();
        value.shards[1].first_group = value.shards[0].last_group.clone();
        assert!(value
            .validate("2010-01-01_2011-01-01", true)
            .unwrap_err()
            .to_string()
            .contains("splits group"));
    }
}
