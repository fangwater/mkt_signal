use anyhow::{bail, Context, Result};
use flate2::read::MultiGzDecoder;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::time::Duration;
#[derive(Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub inventory: PathBuf,
    pub remote_root: String,
    pub data_root: Option<PathBuf>,
    pub rocksdb_dir: PathBuf,
    pub periods: Vec<String>,
    #[serde(default = "default_frequency")]
    pub frequency: u32,
    #[serde(default)]
    pub shard_root: Option<PathBuf>,
    #[serde(default = "default_partition_workers")]
    pub partition_workers: usize,
    #[serde(default = "default_workers")]
    pub workers: usize,
    #[serde(default = "default_rows")]
    pub rows_per_shard: u64,
    #[serde(default = "default_reserve")]
    pub min_free_bytes: u64,
    #[serde(default = "default_budget")]
    pub disk_budget_bytes: u64,
}
fn default_frequency() -> u32 {
    60
}
#[derive(Deserialize)]
pub struct Inventory {
    pub remote_root: String,
    pub packages: Vec<Package>,
}
#[derive(Deserialize, Clone)]
pub struct Package {
    pub directory: String,
    pub period: String,
    pub files: Vec<SourceFile>,
    pub missing_part_indices_through_last: Vec<u16>,
    pub partial_files: Vec<String>,
    pub report_present: bool,
}
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceFile {
    pub name: String,
    pub url: String,
    pub remote_bytes: u64,
    pub etag: Option<String>,
}

pub fn client() -> Result<Client> {
    Ok(Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .build()?)
}
pub fn checked_response(client: &Client, file: &SourceFile) -> Result<reqwest::blocking::Response> {
    let response = client.get(&file.url).send()?.error_for_status()?;
    if response
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<u64>().ok())
        != Some(file.remote_bytes)
        || file.etag.as_deref() != response.headers().get("etag").and_then(|v| v.to_str().ok())
    {
        bail!(
            "source metadata changed: {}; refresh inventory before replay",
            file.url
        );
    }
    Ok(response)
}
pub fn input(
    config: &Config,
    package: &Package,
    file: &SourceFile,
    client: &Client,
) -> Result<Box<dyn Read>> {
    let reader: Box<dyn Read> = if let Some(root) = &config.data_root {
        let path = root.join(&package.directory).join(&file.name);
        if fs::metadata(&path)?.len() != file.remote_bytes {
            bail!("source size mismatch: {}", path.display());
        }
        Box::new(File::open(path)?)
    } else {
        Box::new(checked_response(client, file)?)
    };
    Ok(Box::new(MultiGzDecoder::new(BufReader::with_capacity(
        4 * 1024 * 1024,
        reader,
    ))))
}
pub fn package<'a>(inventory: &'a Inventory, config: &Config, period: &str) -> Result<&'a Package> {
    checked_package(inventory, config, period, true)
}
/// Only for independently staging finalized source files; never authorizes replay.
pub fn available_package<'a>(
    inventory: &'a Inventory,
    config: &Config,
    period: &str,
) -> Result<&'a Package> {
    checked_package(inventory, config, period, false)
}
pub fn source_part_index(file: &SourceFile) -> Result<usize> {
    let digits = file
        .name
        .strip_prefix("merged-Data-part-")
        .and_then(|s| s.strip_suffix(".csv.gz"))
        .context("invalid finalized Raw source part name")?;
    if digits.len() != 6 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        bail!("invalid Raw source part number");
    }
    let index: usize = digits.parse()?;
    if index >= 1 << 16 {
        bail!("Raw source part number overflows source order");
    }
    Ok(index)
}
fn checked_package<'a>(
    inventory: &'a Inventory,
    config: &Config,
    period: &str,
    require_complete: bool,
) -> Result<&'a Package> {
    if inventory.remote_root.trim_end_matches('/') != config.remote_root.trim_end_matches('/') {
        bail!("inventory remote_root does not match config");
    }
    let package = inventory
        .packages
        .iter()
        .find(|p| p.period == period)
        .context("period absent from inventory")?;
    if require_complete
        && (!package.report_present
            || !package.partial_files.is_empty()
            || !package.missing_part_indices_through_last.is_empty())
    {
        bail!("Raw period {period} is incomplete: missing part / Report or .part present");
    }
    if package.directory
        != format!("shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_{period}")
    {
        bail!("unexpected Raw source package directory");
    }
    let mut files: Vec<_> = package
        .files
        .iter()
        .filter(|f| f.name.starts_with("merged-Data-part-") && f.name.ends_with(".csv.gz"))
        .collect();
    files.sort_by_key(|f| &f.name);
    if files.is_empty() {
        bail!("no Raw source parts");
    }
    for (i, file) in files.iter().enumerate() {
        let part = source_part_index(file)?;
        if (require_complete && part != i)
            || (i > 0 && files[i - 1].name == file.name)
            || file.url
                != format!(
                    "{}/{}/{}",
                    config.remote_root.trim_end_matches('/'),
                    package.directory,
                    file.name
                )
        {
            bail!("Raw source parts must match remote root and have unique valid indices; replay requires contiguous parts from 0");
        }
    }
    Ok(package)
}
pub fn expected_counts(
    config: &Config,
    p: &Package,
    client: &Client,
) -> Result<BTreeMap<String, u64>> {
    let file = p
        .files
        .iter()
        .find(|f| f.name == "merged-Report.csv.gz")
        .context("missing Report file")?;
    let mut csv = csv::Reader::from_reader(input(config, p, file, client)?);
    let header = csv.headers()?.clone();
    let ric_index = header
        .iter()
        .position(|s| s == "#RIC")
        .context("Report #RIC")?;
    let count_index = header
        .iter()
        .position(|s| s == "Count")
        .context("Report Count")?;
    let mut expected = BTreeMap::new();
    for row in csv.records() {
        let row = row?;
        let ric = row.get(ric_index).context("Report RIC")?.to_owned();
        let count = row
            .get(count_index)
            .context("Report count")?
            .parse::<u64>()?;
        if expected.insert(ric, count).is_some() {
            bail!("duplicate Report RIC");
        }
    }
    Ok(expected)
}
fn default_partition_workers() -> usize {
    15
}
fn default_workers() -> usize {
    64
}
fn default_rows() -> u64 {
    250_000
}
fn default_reserve() -> u64 {
    1_000_000_000_000
}
fn default_budget() -> u64 {
    3_000_000_000_000
}
