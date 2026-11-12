//! FR and MM parameter archives; the source Redis is read only.

use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, NaiveDateTime, Utc};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use mkt_signal::common::delist_accounts::AccountSpec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

pub enum Outcome {
    Created(PathBuf),
    AlreadyExists,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConfigKey {
    name: String,
    kind: &'static str,
    shared: bool,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    key: String,
    kind: String,
    shared: bool,
    ttl_ms: i64,
    value: serde_json::Value,
    dump_file: Option<String>,
    sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    site: String,
    environment: String,
    exchange: String,
    captured_utc: String,
    keys: Vec<Entry>,
    files: Vec<FileEntry>,
}

#[derive(Serialize, Deserialize)]
struct FileEntry {
    path: String,
    archive_file: String,
    original_mode: u32,
    size: u64,
    sha256: String,
}

struct CapturedFile {
    path: String,
    mode: u32,
    bytes: Vec<u8>,
}

const MAX_FILE_SIZE: u64 = 32 * 1024 * 1024;
const MAX_ENV_SIZE: u64 = 64 * 1024 * 1024;

fn config_keys(account: &AccountSpec) -> Result<Vec<ConfigKey>> {
    if !matches!(account.kind.as_str(), "funding_rate" | "market_making")
        || !["binance", "okex", "bybit", "bitget", "gate"].contains(&account.exchange.as_str())
    {
        bail!("unsupported parameter backup account kind or exchange");
    }
    if account.slug.is_empty()
        || account.slug.len() > 128
        || !account
            .slug
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        bail!("invalid parameter environment identifier");
    }
    let env = &account.slug;
    if account.kind == "market_making" {
        let venue = format!("{}-futures", account.exchange);
        let mut keys = vec![
            ConfigKey {
                name: format!("mm_trade_symbols:{venue}"),
                kind: "string",
                shared: true,
            },
            ConfigKey {
                name: format!("mm_strategy_params_{venue}"),
                kind: "hash",
                shared: true,
            },
            ConfigKey {
                name: format!("{env}:{venue}:{venue}:pre_trade_risk_params"),
                kind: "hash",
                shared: false,
            },
        ];
        for suffix in [
            "amount_u",
            "max_pos_u",
            "open_offset_lower",
            "hedge_price_offset_limits",
            "hedge_price_offset_limit_upper",
            "hedge_price_offset_limit_lower",
        ] {
            keys.push(ConfigKey {
                name: format!("{env}:{venue}:mm:{suffix}"),
                kind: "string",
                shared: false,
            });
        }
        return Ok(keys);
    }
    let open = format!("{}-margin", account.exchange);
    let hedge = format!("{}-futures", account.exchange);
    let pair = format!("{open}_{hedge}");
    let mut keys = Vec::new();
    for name in [
        "dump_symbols",
        "pos_dump_symbols",
        "trade_symbols",
        "fwd_trade_symbols",
        "bwd_trade_symbols",
        "unimmr_close_symbols",
    ] {
        keys.push(ConfigKey {
            name: format!("{env}:fr_{name}:{pair}"),
            kind: "string",
            shared: false,
        });
    }
    keys.push(ConfigKey {
        name: format!("{env}:{open}:{hedge}:pre_trade_risk_params"),
        kind: "hash",
        shared: false,
    });
    keys.push(ConfigKey {
        name: format!("{env}:funding_rate_thresholds_{pair}"),
        kind: "hash",
        shared: false,
    });
    for name in [
        "amount_u_overrides",
        "max_pos_u_overrides",
        "taker_decsion_model_overrides",
        "fr_open_spread_limit_overrides",
        "hedge_price_offset_limits",
        "hedge_price_offset_limit_upper",
        "hedge_price_offset_limit_lower",
        "open_offset_lower_overrides",
    ] {
        keys.push(ConfigKey {
            name: format!("{env}:{open}:{hedge}:{name}"),
            kind: "string",
            shared: false,
        });
    }
    for prefix in [
        "fr_strategy_params",
        "fr_spread_threshold_mapping",
        "fr_spread_thresholds",
        "rolling_metrics_params",
    ] {
        keys.push(ConfigKey {
            name: format!("{prefix}_{pair}"),
            kind: "hash",
            shared: true,
        });
    }
    Ok(keys)
}

fn mm_model_keys(strategy: &serde_json::Value) -> Result<Vec<ConfigKey>> {
    let mut names = BTreeSet::new();
    for field in ["return_model_service", "environment_model_service"] {
        let Some(value) = strategy.get(field) else {
            continue;
        };
        let model = value
            .as_str()
            .context("MM model service must be a string")?
            .trim();
        if model.is_empty() || model == "-" {
            continue;
        }
        if model.len() > 256
            || !model.as_bytes()[0].is_ascii_alphanumeric()
            || !model
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            bail!("invalid MM model service name");
        }
        names.insert(format!("model_score_rolling_params_{model}"));
    }
    Ok(names
        .into_iter()
        .map(|name| ConfigKey {
            name,
            kind: "hash",
            shared: true,
        })
        .collect())
}

/// Values only: no DUMP, TTL changes, scans or writes on the source Redis.
pub async fn parameter_values(
    url: &str,
    account: &AccountSpec,
) -> Result<BTreeMap<String, serde_json::Value>> {
    use mkt_signal::common::parameter_audit::normalise_redis;
    let mut conn = redis::Client::open(url)?
        .get_multiplexed_async_connection()
        .await?;
    let mut keys = config_keys(account)?;
    if account.kind == "market_making" {
        let strategy: BTreeMap<String, String> = redis::cmd("HGETALL")
            .arg(format!("mm_strategy_params_{}-futures", account.exchange))
            .query_async(&mut conn)
            .await?;
        keys.extend(mm_model_keys(&serde_json::to_value(strategy)?)?);
    }
    let mut pipe = redis::pipe();
    pipe.atomic();
    for key in &keys {
        pipe.cmd("TYPE").arg(&key.name);
        pipe.cmd(if key.kind == "hash" { "HGETALL" } else { "GET" })
            .arg(&key.name);
    }
    let values: Vec<redis::Value> = pipe.query_async(&mut conn).await?;
    anyhow::ensure!(
        values.len() == keys.len() * 2,
        "incomplete parameter snapshot"
    );
    keys.into_iter()
        .enumerate()
        .map(|(i, key)| {
            let kind: String = redis::from_redis_value(&values[i * 2])?;
            let value = match kind.as_str() {
                "none" => serde_json::Value::Null,
                "hash" => serde_json::to_value(
                    redis::from_redis_value::<BTreeMap<String, String>>(&values[i * 2 + 1])?,
                )?,
                "string" => {
                    serde_json::to_value(redis::from_redis_value::<String>(&values[i * 2 + 1])?)?
                }
                _ => bail!("unexpected parameter type"),
            };
            Ok((key.name, normalise_redis(&kind, value)))
        })
        .collect()
}

pub fn allowed_parameter_key(account: &AccountSpec, key: &str) -> bool {
    config_keys(account).is_ok_and(|keys| keys.iter().any(|entry| entry.name == key))
        || (account.kind == "market_making"
            && key
                .strip_prefix("model_score_rolling_params_")
                .is_some_and(|name| {
                    !name.is_empty()
                        && name.len() <= 256
                        && name.as_bytes()[0].is_ascii_alphanumeric()
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
                }))
}

fn check_required_keys(account: &AccountSpec, entries: &[Entry]) -> Result<()> {
    let has = |suffix: &str| {
        entries
            .iter()
            .any(|entry| entry.key.ends_with(suffix) && entry.dump_file.is_some())
    };
    if account.kind == "market_making" {
        let venue = format!("{}-futures", account.exchange);
        if !has(":pre_trade_risk_params")
            || !has(&format!("mm_strategy_params_{venue}"))
            || !has(&format!("mm_trade_symbols:{venue}"))
        {
            bail!("MM configuration incomplete: risk parameters, strategy parameters and symbol list are required");
        }
    } else {
        let strategy = entries
            .iter()
            .any(|entry| entry.key.starts_with("fr_strategy_params_") && entry.dump_file.is_some());
        let lists = entries.iter().any(|entry| {
            (entry.key.contains(":fr_dump_symbols:")
                || entry.key.contains(":fr_fwd_trade_symbols:")
                || entry.key.contains(":fr_bwd_trade_symbols:"))
                && entry.dump_file.is_some()
        });
        if !has(":pre_trade_risk_params") || !strategy || !lists {
            bail!("parameter configuration incomplete: risk parameters, strategy parameters and at least one symbol list are required");
        }
    }
    Ok(())
}

pub async fn backup_today(
    root: &Path,
    site: &str,
    url: &str,
    account: &AccountSpec,
    keep: usize,
    env_root: &Path,
    sg_ssh_host: &str,
    preserve_for_upload: bool,
) -> Result<Outcome> {
    backup_snapshot(
        root,
        site,
        url,
        account,
        keep,
        env_root,
        sg_ssh_host,
        preserve_for_upload,
        false,
    )
    .await
}

pub async fn backup_manual(
    root: &Path,
    site: &str,
    url: &str,
    account: &AccountSpec,
    keep: usize,
    env_root: &Path,
    sg_ssh_host: &str,
    preserve_for_upload: bool,
) -> Result<Outcome> {
    backup_snapshot(
        root,
        site,
        url,
        account,
        keep,
        env_root,
        sg_ssh_host,
        preserve_for_upload,
        true,
    )
    .await
}

pub fn archive_stem(manual: bool) -> String {
    let now = Utc::now();
    if manual {
        now.format("%Y-%m-%dT%H%M%S%9fZ").to_string()
    } else {
        now.format("%Y-%m-%d").to_string()
    }
}

pub fn parse_archive_stem(stem: &str) -> Option<NaiveDateTime> {
    if stem.len() == 10 {
        let date = NaiveDate::parse_from_str(stem, "%Y-%m-%d").ok()?;
        if date.format("%Y-%m-%d").to_string() != stem {
            return None;
        }
        return date.and_hms_opt(0, 0, 0);
    }
    let dt = NaiveDateTime::parse_from_str(stem, "%Y-%m-%dT%H%M%S%9fZ").ok()?;
    (dt.format("%Y-%m-%dT%H%M%S%9fZ").to_string() == stem).then_some(dt)
}

async fn backup_snapshot(
    root: &Path,
    site: &str,
    url: &str,
    account: &AccountSpec,
    keep: usize,
    env_root: &Path,
    sg_ssh_host: &str,
    preserve_for_upload: bool,
    manual: bool,
) -> Result<Outcome> {
    if !matches!(site, "jp" | "sg") || keep == 0 {
        bail!("invalid backup site or retention");
    }
    let keys = config_keys(account)?;
    let dir = root.join(site).join(&account.slug);
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .context("create parameter backup directory")?;
    for path in [root.to_path_buf(), root.join(site), dir.clone()] {
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            bail!("parameter backup directories must be private directories (mode 0700)");
        }
    }
    let date = Utc::now().date_naive();
    let stem = archive_stem(manual);
    let path = dir.join(format!("{stem}.tar.gz"));
    if path.exists() {
        if manual {
            bail!("manual backup name already exists; retry");
        }
        validate_archive(&path, site, account, &keys)?;
        if !preserve_for_upload {
            prune(&dir, keep, date)?;
        }
        return Ok(Outcome::AlreadyExists);
    }
    let (mut manifest, dumps) =
        tokio::time::timeout(Duration::from_secs(20), capture(site, url, account, &keys))
            .await
            .context("parameter Redis backup timed out")??;
    let files = match site {
        "jp" => capture_local_files(&env_root.join(&account.slug))?,
        "sg" => capture_sg_files(sg_ssh_host, &account.slug).await?,
        _ => unreachable!(),
    };
    manifest.files = files
        .iter()
        .map(|file| FileEntry {
            path: file.path.clone(),
            archive_file: format!("files/{}", file.path),
            original_mode: file.mode,
            size: file.bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&file.bytes)),
        })
        .collect();
    let temp = dir.join(format!(
        ".{date}-{}-{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .context("create temporary parameter backup")?;
        let gzip = GzEncoder::new(file, Compression::default());
        let mut tar = tar::Builder::new(gzip);
        let json =
            serde_json::to_vec_pretty(&manifest).context("encode parameter config manifest")?;
        append(&mut tar, "config.json", &json)?;
        for (name, bytes) in dumps {
            append(&mut tar, &name, &bytes)?;
        }
        for file in &files {
            append(&mut tar, &format!("files/{}", file.path), &file.bytes)?;
        }
        let gzip = tar.into_inner().context("finish parameter backup tar")?;
        let mut file = gzip.finish().context("finish parameter backup gzip")?;
        file.flush()?;
        file.sync_all()?;
        validate_archive(&temp, site, account, &keys)?;
        match fs::hard_link(&temp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && !manual => {
                validate_archive(&path, site, account, &keys)?;
                return Ok(Outcome::AlreadyExists);
            }
            Err(e) => return Err(e).context("publish parameter backup"),
        }
        File::open(&dir)?.sync_all()?;
        if !preserve_for_upload {
            prune(&dir, keep, date)?;
        }
        Ok(Outcome::Created(path))
    })();
    let _ = fs::remove_file(&temp);
    result
}

async fn capture(
    site: &str,
    url: &str,
    account: &AccountSpec,
    keys: &[ConfigKey],
) -> Result<(Manifest, Vec<(String, Vec<u8>)>)> {
    let client = redis::Client::open(url).context("open parameter Redis")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect parameter Redis")?;
    let base_key_count = keys.len();
    let mut capture_keys = keys.to_vec();
    if account.kind == "market_making" {
        let strategy_key = format!("mm_strategy_params_{}-futures", account.exchange);
        let strategy: BTreeMap<String, String> = redis::cmd("HGETALL")
            .arg(&strategy_key)
            .query_async(&mut conn)
            .await
            .context("read MM model dependency names")?;
        capture_keys.extend(mm_model_keys(&serde_json::to_value(strategy)?)?);
    }
    let keys = &capture_keys;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for key in keys {
        pipe.cmd("DUMP").arg(&key.name).cmd("PTTL").arg(&key.name);
        pipe.cmd(if key.kind == "hash" { "HGETALL" } else { "GET" })
            .arg(&key.name);
    }
    let values: Vec<redis::Value> = pipe
        .query_async(&mut conn)
        .await
        .context("read parameter configuration transaction")?;
    if values.len() != keys.len() * 3 {
        bail!("incomplete parameter configuration response");
    }
    let mut entries = Vec::new();
    let mut dumps = Vec::new();
    for (idx, key) in keys.iter().enumerate() {
        let dump: Option<Vec<u8>> =
            redis::from_redis_value(&values[idx * 3]).context("decode parameter Redis dump")?;
        let ttl_ms: i64 =
            redis::from_redis_value(&values[idx * 3 + 1]).context("decode parameter Redis TTL")?;
        let value = if dump.is_none() {
            serde_json::Value::Null
        } else if key.kind == "hash" {
            let map: BTreeMap<String, String> = redis::from_redis_value(&values[idx * 3 + 2])
                .context("decode parameter Redis hash")?;
            serde_json::to_value(map)?
        } else {
            let text: String = redis::from_redis_value(&values[idx * 3 + 2])
                .context("decode parameter Redis string")?;
            serde_json::Value::String(text)
        };
        let (dump_file, sha256) = if let Some(bytes) = dump {
            let name = format!("keys/{idx:03}.dump");
            let hash = format!("{:x}", Sha256::digest(&bytes));
            dumps.push((name.clone(), bytes));
            (Some(name), Some(hash))
        } else {
            (None, None)
        };
        entries.push(Entry {
            key: key.name.clone(),
            kind: key.kind.to_string(),
            shared: key.shared,
            ttl_ms,
            value,
            dump_file,
            sha256,
        });
    }
    check_required_keys(account, &entries)?;
    if account.kind == "market_making" {
        let strategy = entries
            .iter()
            .find(|entry| entry.key == format!("mm_strategy_params_{}-futures", account.exchange))
            .context("MM strategy capture missing")?;
        let expected = mm_model_keys(&strategy.value)?;
        if capture_keys[base_key_count..] != expected {
            bail!("MM model dependency names changed during backup; retry");
        }
    }
    Ok((
        Manifest {
            site: site.to_string(),
            environment: account.slug.clone(),
            exchange: account.exchange.clone(),
            captured_utc: Utc::now().to_rfc3339(),
            keys: entries,
            files: Vec::new(),
        },
        dumps,
    ))
}

fn is_backup_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        let lower = name.to_string_lossy().to_ascii_lowercase();
        [".bak", ".bk", ".backup"]
            .iter()
            .any(|suffix| lower.ends_with(suffix) || lower.contains(&format!("{suffix}.")))
    })
}

fn valid_file_path(path: &str) -> bool {
    let path = Path::new(path);
    if is_backup_path(path)
        || !path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return false;
    }
    matches!(
        path.to_str(),
        Some("env.sh" | "trade_engine.toml" | "trade engine.toml")
    ) || path
        .strip_prefix("config")
        .is_ok_and(|rest| !rest.as_os_str().is_empty())
}

fn check_files(files: &[CapturedFile]) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut total = 0u64;
    for file in files {
        if !valid_file_path(&file.path) || !names.insert(file.path.as_str()) {
            bail!("invalid or duplicate parameter configuration file path");
        }
        total = total
            .checked_add(file.bytes.len() as u64)
            .context("parameter configuration size overflow")?;
        if file.bytes.len() as u64 > MAX_FILE_SIZE || total > MAX_ENV_SIZE || names.len() > 1024 {
            bail!("parameter configuration files exceed backup limits");
        }
    }
    if !names.contains("env.sh")
        || !(names.contains("trade_engine.toml") || names.contains("trade engine.toml"))
        || !names.iter().any(|name| name.starts_with("config/"))
    {
        bail!(
            "parameter environment is missing env.sh, trade engine configuration, or config files"
        );
    }
    Ok(())
}

fn capture_local_files(env: &Path) -> Result<Vec<CapturedFile>> {
    if !fs::symlink_metadata(env)
        .context("inspect parameter environment directory")?
        .file_type()
        .is_dir()
    {
        bail!("parameter environment directory is missing or a symlink");
    }
    let mut files = Vec::new();
    capture_local_file(env, Path::new("env.sh"), &mut files)?;
    for name in ["trade_engine.toml", "trade engine.toml"] {
        if fs::symlink_metadata(env.join(name)).is_ok() {
            capture_local_file(env, Path::new(name), &mut files)?;
        }
    }
    walk_config(env, Path::new("config"), &mut files, 0)?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    check_files(&files)?;
    Ok(files)
}

fn walk_config(
    env: &Path,
    relative: &Path,
    files: &mut Vec<CapturedFile>,
    depth: usize,
) -> Result<()> {
    if depth > 16 {
        bail!("parameter configuration directory nesting exceeds limit");
    }
    let metadata = fs::symlink_metadata(env.join(relative))
        .context("inspect parameter configuration directory")?;
    if !metadata.file_type().is_dir() {
        bail!("parameter configuration directory is missing or a symlink");
    }
    for entry in
        fs::read_dir(env.join(relative)).context("list parameter configuration directory")?
    {
        let entry = entry?;
        let name = relative.join(entry.file_name());
        if is_backup_path(&name) {
            continue;
        }
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk_config(env, &name, files, depth + 1)?;
        } else if kind.is_file() {
            capture_local_file(env, &name, files)?;
        } else {
            bail!("parameter configuration contains a symlink or unsupported file type");
        }
    }
    Ok(())
}

fn capture_local_file(env: &Path, relative: &Path, files: &mut Vec<CapturedFile>) -> Result<()> {
    let path = relative
        .to_str()
        .context("parameter configuration path is not UTF-8")?;
    if !valid_file_path(path) {
        bail!("invalid parameter configuration path");
    }
    let full_path = env.join(relative);
    let before =
        fs::symlink_metadata(&full_path).context("inspect parameter configuration file")?;
    if !before.file_type().is_file() || before.len() > MAX_FILE_SIZE {
        bail!("parameter configuration file is not a regular file or is too large");
    }
    let mut reader = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&full_path)
        .context("open parameter configuration file")?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut reader)
        .take(MAX_FILE_SIZE + 1)
        .read_to_end(&mut bytes)?;
    let after = reader.metadata()?;
    if bytes.len() as u64 > MAX_FILE_SIZE
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
    {
        bail!("parameter configuration changed while reading");
    }
    files.push(CapturedFile {
        path: path.to_string(),
        mode: before.permissions().mode() & 0o777,
        bytes,
    });
    check_file_limits(files)?;
    Ok(())
}

fn check_file_limits(files: &[CapturedFile]) -> Result<()> {
    if files.len() > 1024 || files.iter().map(|f| f.bytes.len() as u64).sum::<u64>() > MAX_ENV_SIZE
    {
        bail!("parameter configuration files exceed backup limits");
    }
    Ok(())
}

async fn capture_sg_files(host: &str, env: &str) -> Result<Vec<CapturedFile>> {
    if host.is_empty()
        || host.starts_with('-')
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'@' | b':'))
    {
        bail!("invalid SG backup SSH host");
    }
    // env is validated by config_keys before interpolation; no config content is passed in argv.
    let remote = format!("set -eu; cd \"$HOME/{env}\"; set -- env.sh; for f in trade_engine.toml 'trade engine.toml'; do if [ -f \"$f\" ] || [ -L \"$f\" ]; then set -- \"$@\" \"$f\"; fi; done; [ \"$#\" -gt 1 ]; [ -d config ]; exec tar --exclude='*.[bB][aA][kK]' --exclude='*.[bB][aA][kK].*' --exclude='*.[bB][kK]' --exclude='*.[bB][kK].*' --exclude='*.[bB][aA][cC][kK][uU][pP]' --exclude='*.[bB][aA][cC][kK][uU][pP].*' -cf - \"$@\" config");
    let mut child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            host,
            &remote,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start SG configuration read")?;
    let stdout = child
        .stdout
        .take()
        .context("SG configuration stream unavailable")?;
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let mut data = Vec::new();
        stdout
            .take(MAX_ENV_SIZE + 4 * 1024 * 1024 + 1)
            .read_to_end(&mut data)
            .await?;
        if data.len() as u64 > MAX_ENV_SIZE + 4 * 1024 * 1024 {
            bail!("SG configuration stream exceeds backup limit");
        }
        let status = child
            .wait()
            .await
            .context("wait for SG configuration read")?;
        if !status.success() {
            bail!("SG configuration read failed ({status})");
        }
        parse_remote_tar(&data)
    })
    .await
    .context("SG configuration read timed out")?;
    result
}

fn parse_remote_tar(data: &[u8]) -> Result<Vec<CapturedFile>> {
    let mut tar = tar::Archive::new(data);
    let mut files = Vec::new();
    for entry in tar.entries().context("parse SG configuration stream")? {
        let mut entry = entry?;
        let kind = entry.header().entry_type();
        let path = entry.path()?.into_owned();
        if is_backup_path(&path) {
            continue;
        }
        if kind.is_dir() {
            continue;
        }
        if !kind.is_file() || entry.size() > MAX_FILE_SIZE {
            bail!("SG configuration contains a symlink, unsupported file type, or oversized file");
        }
        let name = path
            .to_str()
            .context("SG configuration path is not UTF-8")?
            .to_string();
        if !valid_file_path(&name) {
            bail!("invalid SG configuration file path");
        }
        let mode = entry.header().mode()? & 0o777;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        files.push(CapturedFile {
            path: name,
            mode,
            bytes,
        });
        check_file_limits(&files)?;
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    check_files(&files)?;
    Ok(files)
}

fn append(tar: &mut tar::Builder<GzEncoder<File>>, name: &str, bytes: &[u8]) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    tar.append_data(&mut header, name, bytes)
        .context("write parameter archive entry")
}

fn validate_archive(
    path: &Path,
    site: &str,
    account: &AccountSpec,
    keys: &[ConfigKey],
) -> Result<()> {
    let gzip = GzDecoder::new(File::open(path).context("open parameter backup")?);
    let mut tar = tar::Archive::new(gzip);
    let mut files = BTreeMap::new();
    let mut total = 0u64;
    for entry in tar.entries().context("read parameter backup tar")? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() || entry.size() > 32 * 1024 * 1024 {
            bail!("invalid parameter archive entry");
        }
        total = total
            .checked_add(entry.size())
            .context("parameter archive size overflow")?;
        if total > 128 * 1024 * 1024 || files.len() >= 2048 {
            bail!("parameter archive exceeds validation limits");
        }
        let name = entry.path()?.to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        if files.insert(name, bytes).is_some() {
            bail!("duplicate parameter archive entry");
        }
    }
    std::io::copy(&mut tar.into_inner(), &mut std::io::sink())
        .context("check parameter gzip checksum")?;
    let manifest: Manifest = serde_json::from_slice(
        &files
            .remove("config.json")
            .context("parameter backup missing config.json")?,
    )
    .context("decode parameter backup manifest")?;
    let mut expected_keys = keys.to_vec();
    if account.kind == "market_making" {
        let strategy = manifest
            .keys
            .iter()
            .find(|entry| entry.key == format!("mm_strategy_params_{}-futures", account.exchange))
            .context("MM archive strategy parameters missing")?;
        expected_keys.extend(mm_model_keys(&strategy.value)?);
    }
    let keys = &expected_keys;
    check_required_keys(account, &manifest.keys)?;
    if manifest.site != site
        || manifest.environment != account.slug
        || manifest.exchange != account.exchange
        || manifest.keys.len() != keys.len()
    {
        bail!("parameter backup metadata mismatch");
    }
    let mut names = BTreeSet::new();
    for (entry, expected) in manifest.keys.iter().zip(keys) {
        if entry.key != expected.name
            || entry.kind != expected.kind
            || entry.shared != expected.shared
        {
            bail!("parameter backup key set mismatch");
        }
        match (&entry.dump_file, &entry.sha256) {
            (Some(name), Some(hash)) => {
                if !names.insert(name.clone()) {
                    bail!("duplicate parameter backup dump");
                }
                let bytes = files
                    .remove(name)
                    .context("parameter backup dump missing")?;
                if format!("{:x}", Sha256::digest(&bytes)) != *hash {
                    bail!("parameter backup dump checksum mismatch");
                }
            }
            (None, None) => {}
            _ => bail!("invalid parameter backup dump metadata"),
        }
    }
    let mut config_files = Vec::new();
    for entry in &manifest.files {
        if !valid_file_path(&entry.path)
            || entry.archive_file != format!("files/{}", entry.path)
            || entry.original_mode > 0o777
        {
            bail!("invalid parameter configuration file metadata");
        }
        let bytes = files
            .remove(&entry.archive_file)
            .context("parameter configuration file missing from archive")?;
        if bytes.len() as u64 != entry.size
            || format!("{:x}", Sha256::digest(&bytes)) != entry.sha256
        {
            bail!("parameter configuration file checksum mismatch");
        }
        config_files.push(CapturedFile {
            path: entry.path.clone(),
            mode: entry.original_mode,
            bytes,
        });
    }
    check_files(&config_files)?;
    if !files.is_empty() || names.is_empty() {
        bail!("invalid or empty parameter backup");
    }
    Ok(())
}

pub fn validate_for_account(path: &Path, site: &str, account: &AccountSpec) -> Result<()> {
    if matches!(account.kind.as_str(), "cta_perp" | "cta_spread") {
        super::cta_backup::validate_archive(path, site, account)
    } else {
        validate_archive(path, site, account, &config_keys(account)?)
    }
}

pub fn archive_paths(root: &Path, site: &str, account: &AccountSpec) -> Result<Vec<PathBuf>> {
    if matches!(account.kind.as_str(), "cta_perp" | "cta_spread") {
        if site != "jp" || super::cta_backup::worker(&account.slug).is_none() {
            bail!("invalid CTA backup identity");
        }
    } else {
        config_keys(account)?;
    }
    if !matches!(site, "jp" | "sg") {
        bail!("invalid backup site");
    }
    let dir = root.join(site).join(&account.slug);
    let now = Utc::now().naive_utc();
    let mut archives = Vec::new();
    for entry in fs::read_dir(dir).context("list local parameter backups")? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(stamp) = name
            .to_str()
            .and_then(|s| s.strip_suffix(".tar.gz"))
            .and_then(parse_archive_stem)
        else {
            continue;
        };
        if stamp <= now {
            archives.push((stamp, entry.path()));
        }
    }
    archives.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(archives.into_iter().map(|(_, path)| path).collect())
}

fn prune(dir: &Path, keep: usize, today: NaiveDate) -> Result<()> {
    let mut archives = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(stamp) = name
            .to_str()
            .and_then(|s| s.strip_suffix(".tar.gz"))
            .and_then(parse_archive_stem)
        else {
            continue;
        };
        if stamp.date() <= today && stamp <= Utc::now().naive_utc() {
            archives.push((
                stamp,
                entry.path(),
                name.to_str()
                    .is_some_and(|s| s.len() > "YYYY-MM-DD.tar.gz".len()),
            ));
        }
    }
    archives.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    let (mut daily, mut manual) = (0usize, 0usize);
    for (_, path, is_manual) in archives {
        let count = if is_manual { &mut manual } else { &mut daily };
        if *count < keep {
            *count += 1;
        } else {
            fs::remove_file(path).context("remove expired parameter archive")?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn test_redis_available() -> bool {
    let available = std::process::Command::new("redis-server")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !available {
        eprintln!(
            "skipping Redis integration test: redis-server must be installed and runnable on PATH"
        );
    }
    available
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_signal::common::delist_accounts::RedisSite;

    #[test]
    fn manual_versions_have_canonical_sortable_timestamps() {
        let daily = archive_stem(false);
        let manual = archive_stem(true);
        assert!(parse_archive_stem(&daily).is_some());
        assert!(parse_archive_stem(&manual).is_some(), "{manual}");
        assert!(parse_archive_stem(&manual) > parse_archive_stem(&daily));
        assert!(parse_archive_stem("2026-11-02T999999999999999Z").is_none());
        assert!(parse_archive_stem("2026-11-02T123456000000000Z-extra").is_none());
    }
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

    struct RedisProcess(Child);
    impl Drop for RedisProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    async fn start_redis(dir: &Path) -> (RedisProcess, String) {
        let socket = dir.join("redis.sock");
        let child = Command::new("redis-server")
            .args([
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
            ])
            .arg(&socket)
            .arg("--dir")
            .arg(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let process = RedisProcess(child);
        let url = format!("redis+unix://{}", socket.display());
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(client) = redis::Client::open(url.as_str()) {
                if client.get_multiplexed_async_connection().await.is_ok() {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "test Redis did not start");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (process, url)
    }

    fn account(name: &str) -> AccountSpec {
        AccountSpec {
            slug: name.into(),
            alias: name.into(),
            exchange: "binance".into(),
            kind: "funding_rate".into(),
            host: "jp".into(),
            site: RedisSite::Jp,
            config_url: String::new(),
        }
    }

    #[tokio::test]
    async fn mm_archives_restore_scoped_and_shared_parameters_without_running_processes() {
        if !test_redis_available() {
            return;
        }
        let root = std::env::temp_dir().join(format!("mm-backup-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let (_redis, url) = start_redis(&root).await;
        let mut spec = account("okex_mm_83777");
        spec.kind = "market_making".into();
        spec.exchange = "okex".into();
        let env_root = root.join("envs");
        let env = env_root.join(&spec.slug);
        fs::create_dir_all(env.join("config")).unwrap();
        for (path, data) in [
            ("env.sh", "TRADE_ENGINE_READ_ONLY=1\n"),
            ("trade_engine.toml", "local_ips=[]\n"),
            ("config/mm_config_server.env", "PORT=18112\n"),
            ("config/ignored.bak", "old credential snapshot"),
        ] {
            fs::write(env.join(path), data).unwrap();
        }
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let risk = "okex_mm_83777:okex-futures:okex-futures:pre_trade_risk_params";
        let model = "model_score_rolling_params_okex-futures-test";
        redis::cmd("HSET")
            .arg(risk)
            .arg("max_pos_u")
            .arg("123")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        redis::cmd("HSET")
            .arg("mm_strategy_params_okex-futures")
            .arg("return_model_service")
            .arg("okex-futures-test")
            .arg("environment_model_service")
            .arg("-")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        redis::cmd("HSET")
            .arg(model)
            .arg("rolling_window")
            .arg("17")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        for (key, value) in [
            ("mm_trade_symbols:okex-futures", "[\"BTCUSDT\"]"),
            (
                "okex_mm_83777:okex-futures:mm:max_pos_u",
                "{\"BTCUSDT\":500}",
            ),
            ("another_mm:okex-futures:mm:max_pos_u", "unrelated"),
        ] {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .query_async::<()>(&mut conn)
                .await
                .unwrap();
        }
        let backups = root.join("backups");
        let path = match backup_today(&backups, "jp", &url, &spec, 30, &env_root, "sg", false)
            .await
            .unwrap()
        {
            Outcome::Created(path) => path,
            _ => panic!("first MM daily archive must be created"),
        };
        validate_for_account(&path, "jp", &spec).unwrap();
        let initial = fs::read(&path).unwrap();
        let mut archive = tar::Archive::new(GzDecoder::new(File::open(&path).unwrap()));
        let mut payloads = BTreeMap::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            payloads.insert(name, bytes);
        }
        let manifest: Manifest = serde_json::from_slice(&payloads["config.json"]).unwrap();
        assert_eq!(manifest.keys.len(), 10);
        assert!(!manifest
            .files
            .iter()
            .any(|entry| entry.path.ends_with(".bak")));
        assert!(!manifest
            .keys
            .iter()
            .any(|entry| entry.key.starts_with("another_mm:")));
        assert!(manifest
            .keys
            .iter()
            .any(|entry| entry.key == model && entry.shared));
        assert!(manifest.keys.iter().any(|entry| entry.key
            == "okex_mm_83777:okex-futures:mm:amount_u"
            && entry.dump_file.is_none()));
        for key in [risk, model] {
            let entry = manifest.keys.iter().find(|entry| entry.key == key).unwrap();
            redis::cmd("RESTORE")
                .arg(format!("restored:{key}"))
                .arg(0)
                .arg(&payloads[entry.dump_file.as_ref().unwrap()])
                .arg("REPLACE")
                .query_async::<()>(&mut conn)
                .await
                .unwrap();
        }
        let restored: String = redis::cmd("HGET")
            .arg(format!("restored:{risk}"))
            .arg("max_pos_u")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(restored, "123");
        let restored: String = redis::cmd("HGET")
            .arg(format!("restored:{model}"))
            .arg("rolling_window")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(restored, "17");
        assert!(matches!(
            backup_today(&backups, "jp", &url, &spec, 30, &env_root, "sg", false)
                .await
                .unwrap(),
            Outcome::AlreadyExists
        ));
        assert!(matches!(
            backup_manual(&backups, "jp", &url, &spec, 30, &env_root, "sg", false)
                .await
                .unwrap(),
            Outcome::Created(_)
        ));
        redis::cmd("DEL")
            .arg(risk)
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        assert!(
            backup_manual(&backups, "jp", &url, &spec, 30, &env_root, "sg", false)
                .await
                .is_err()
        );
        assert_eq!(fs::read(path).unwrap(), initial);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mm_model_dependencies_are_deduplicated_and_reject_invalid_names() {
        let keys = mm_model_keys(&serde_json::json!({
            "return_model_service": " model.test-1 ", "environment_model_service": "model.test-1"
        }))
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert!(mm_model_keys(&serde_json::json!({"return_model_service":"../other"})).is_err());
        assert!(mm_model_keys(&serde_json::json!({"return_model_service":17})).is_err());
    }

    #[tokio::test]
    async fn individual_archives_restore_fr_config_and_preserve_history_on_failure() {
        if !test_redis_available() {
            return;
        }
        let root = std::env::temp_dir().join(format!(
            "fr-backup-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let (server, url) = start_redis(&root).await;
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let first = account("binance_fr_arb01");
        let second = account("binance_fr_arb02");
        let env_root = root.join("envs");
        for a in [&first, &second] {
            let dir = env_root.join(&a.slug);
            fs::create_dir_all(dir.join("config/nested")).unwrap();
            fs::write(dir.join("env.sh"), format!("ACCOUNT={}\n", a.slug)).unwrap();
            fs::write(dir.join("trade_engine.toml"), "local_ips = []\n").unwrap();
            fs::write(dir.join("config/fr_config_server.env"), "PORT=123\n").unwrap();
            fs::write(dir.join("config/nested/viz.toml"), "[viz]\n").unwrap();
            fs::write(dir.join("config/viz.toml.bak.20261101"), "old\n").unwrap();
            fs::write(dir.join("config/nested/viz.toml.BK"), "old\n").unwrap();
        }
        let first_keys = config_keys(&first).unwrap();
        let second_keys = config_keys(&second).unwrap();
        let strategy = first_keys
            .iter()
            .find(|k| k.name.starts_with("fr_strategy_params_"))
            .unwrap();
        redis::cmd("HSET")
            .arg(&strategy.name)
            .arg("order_amount")
            .arg("42")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
        for (keyset, symbol) in [(&first_keys, "BTCUSDT"), (&second_keys, "ETHUSDT")] {
            let risk = keyset
                .iter()
                .find(|k| k.name.ends_with(":pre_trade_risk_params"))
                .unwrap();
            let list = keyset
                .iter()
                .find(|k| k.name.contains(":fr_fwd_trade_symbols:"))
                .unwrap();
            redis::cmd("HSET")
                .arg(&risk.name)
                .arg("max_pos_u")
                .arg("500")
                .query_async::<i64>(&mut conn)
                .await
                .unwrap();
            redis::cmd("SET")
                .arg(&list.name)
                .arg(format!("[\"{symbol}\"]"))
                .query_async::<()>(&mut conn)
                .await
                .unwrap();
        }
        redis::cmd("SET")
            .arg("unrelated:config")
            .arg("secret")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        let backups = root.join("backups");
        let first_path = match backup_today(&backups, "jp", &url, &first, 1, &env_root, "sg", false)
            .await
            .unwrap()
        {
            Outcome::Created(path) => path,
            Outcome::AlreadyExists => panic!("first archive already exists"),
        };
        let second_path =
            match backup_today(&backups, "jp", &url, &second, 1, &env_root, "sg", false)
                .await
                .unwrap()
            {
                Outcome::Created(path) => path,
                Outcome::AlreadyExists => panic!("second archive already exists"),
            };
        assert_ne!(first_path, second_path);
        assert_eq!(
            fs::metadata(&first_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let mut archive = tar::Archive::new(GzDecoder::new(File::open(&first_path).unwrap()));
        let mut files = BTreeMap::new();
        for item in archive.entries().unwrap() {
            let mut item = item.unwrap();
            let name = item.path().unwrap().to_string_lossy().into_owned();
            let mut data = Vec::new();
            item.read_to_end(&mut data).unwrap();
            files.insert(name, data);
        }
        let manifest: Manifest = serde_json::from_slice(files.get("config.json").unwrap()).unwrap();
        assert_eq!(
            files.get("files/env.sh").unwrap(),
            b"ACCOUNT=binance_fr_arb01\n"
        );
        assert_eq!(
            files.get("files/trade_engine.toml").unwrap(),
            b"local_ips = []\n"
        );
        assert_eq!(
            files.get("files/config/nested/viz.toml").unwrap(),
            b"[viz]\n"
        );
        assert!(!files.contains_key("files/config/viz.toml.bak.20261101"));
        assert!(!files.contains_key("files/config/nested/viz.toml.BK"));
        assert_eq!(manifest.files.len(), 4);
        assert!(manifest
            .keys
            .iter()
            .all(|e| !e.key.starts_with(&second.slug) && e.key != "unrelated:config"));
        assert!(manifest
            .keys
            .iter()
            .any(|e| e.key == strategy.name && e.shared && e.value["order_amount"] == "42"));
        let restore_dir = root.join("restore");
        fs::create_dir(&restore_dir).unwrap();
        let (restore, restore_url) = start_redis(&restore_dir).await;
        let mut target = redis::Client::open(restore_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for entry in &manifest.keys {
            if let Some(name) = &entry.dump_file {
                redis::cmd("RESTORE")
                    .arg(&entry.key)
                    .arg(0)
                    .arg(files.get(name).unwrap())
                    .query_async::<()>(&mut target)
                    .await
                    .unwrap();
            }
        }
        let amount: String = redis::cmd("HGET")
            .arg(&strategy.name)
            .arg("order_amount")
            .query_async(&mut target)
            .await
            .unwrap();
        assert_eq!(amount, "42");
        let list = first_keys
            .iter()
            .find(|k| k.name.contains(":fr_fwd_trade_symbols:"))
            .unwrap();
        let symbols: String = redis::cmd("GET")
            .arg(&list.name)
            .query_async(&mut target)
            .await
            .unwrap();
        assert_eq!(symbols, "[\"BTCUSDT\"]");
        let prior = first_path.parent().unwrap().join("2020-01-01.tar.gz");
        if Command::new("rclone")
            .arg("version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
        {
            let cloud = root.join("cloud");
            fs::create_dir(&cloud).unwrap();
            let rclone_config = root.join("rclone.conf");
            fs::write(
                &rclone_config,
                format!("[localtest]\ntype = alias\nremote = {}\n", cloud.display()),
            )
            .unwrap();
            fs::set_permissions(&rclone_config, fs::Permissions::from_mode(0o600)).unwrap();
            let config = crate::upload::UploadConfig {
                target: "localtest:bucket/fr-config-backups".into(),
                config_path: rclone_config.clone(),
            };
            assert_eq!(
                crate::upload::sync_archives(&config, &backups, "jp", &first, 1)
                    .await
                    .unwrap(),
                1
            );
            let remote_dir = cloud.join("bucket/fr-config-backups/jp").join(&first.slug);
            let remote_files: Vec<_> = fs::read_dir(&remote_dir)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            assert_eq!(remote_files.len(), 1);
            assert_eq!(
                fs::read(&remote_files[0]).unwrap(),
                fs::read(&first_path).unwrap()
            );
            assert_eq!(
                crate::upload::sync_archives(&config, &backups, "jp", &first, 1)
                    .await
                    .unwrap(),
                0
            );
            let remote_current = remote_files[0].clone();
            let backup_bytes = fs::read(&first_path).unwrap();
            let digest = format!("{:x}", Sha256::digest(&backup_bytes));
            let start = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
            for day in 0..34 {
                let date = start + chrono::Duration::days(day);
                fs::write(
                    remote_dir.join(format!("{date}-{digest}.tar.gz")),
                    &backup_bytes,
                )
                .unwrap();
            }
            let unrelated = remote_dir.join("do-not-delete.txt");
            fs::write(&unrelated, b"keep").unwrap();
            let other_dir = cloud.join("bucket/fr-config-backups/jp").join(&second.slug);
            fs::create_dir(&other_dir).unwrap();
            let other = other_dir.join(format!("2020-01-01-{digest}.tar.gz"));
            fs::write(&other, &backup_bytes).unwrap();
            fs::write(&remote_current, b"corrupt").unwrap();
            assert!(
                crate::upload::prune_remote(&config, &backups, "jp", &first, 30)
                    .await
                    .is_err()
            );
            assert_eq!(
                fs::read_dir(&remote_dir).unwrap().count(),
                36,
                "failed read-back must prevent deletion"
            );
            fs::write(&remote_current, &backup_bytes).unwrap();
            assert_eq!(
                crate::upload::prune_remote(&config, &backups, "jp", &first, 30)
                    .await
                    .unwrap(),
                5
            );
            assert_eq!(fs::read_dir(&remote_dir).unwrap().count(), 31);
            assert!(
                unrelated.exists() && other.exists(),
                "other objects must remain untouched"
            );
            fs::copy(&first_path, &prior).unwrap();
            assert!(matches!(
                backup_today(&backups, "jp", &url, &first, 1, &env_root, "sg", true)
                    .await
                    .unwrap(),
                Outcome::AlreadyExists
            ));
            assert!(
                prior.exists(),
                "local retention must wait for COS verification"
            );
            let bad_config = crate::upload::UploadConfig {
                target: "missing:bucket/fr-config-backups".into(),
                config_path: rclone_config,
            };
            assert!(
                crate::upload::sync_archives(&bad_config, &backups, "jp", &first, 1)
                    .await
                    .is_err()
            );
            assert!(
                prior.exists(),
                "failed upload must not delete old local backup"
            );
            let remote_before: BTreeSet<_> = fs::read_dir(&remote_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert_eq!(
                crate::upload::sync_archives(&config, &backups, "jp", &first, 1)
                    .await
                    .unwrap(),
                1
            );
            assert!(!prior.exists());
            let remote_after: BTreeSet<_> = fs::read_dir(&remote_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert!(
                remote_before.is_subset(&remote_after)
                    && remote_after.len() == remote_before.len() + 1,
                "upload must add the archive without deleting remote history"
            );
            fs::write(
                env_root.join(&first.slug).join("env.sh"),
                "ACCOUNT=updated\n",
            )
            .unwrap();
            let manual =
                match backup_manual(&backups, "jp", &url, &first, 30, &env_root, "sg", true)
                    .await
                    .unwrap()
                {
                    Outcome::Created(path) => path,
                    Outcome::AlreadyExists => panic!("manual archive must be a new version"),
                };
            assert_ne!(manual, first_path);
            validate_for_account(&manual, "jp", &first).unwrap();
            let mut tar = tar::Archive::new(GzDecoder::new(File::open(&manual).unwrap()));
            let mut captured = None;
            for entry in tar.entries().unwrap() {
                let mut entry = entry.unwrap();
                if entry.path().unwrap().as_ref() == Path::new("files/env.sh") {
                    let mut bytes = Vec::new();
                    entry.read_to_end(&mut bytes).unwrap();
                    captured = Some(bytes);
                }
            }
            assert_eq!(captured.as_deref(), Some(b"ACCOUNT=updated\n".as_slice()));
            assert_eq!(
                crate::upload::sync_archives(&config, &backups, "jp", &first, 1)
                    .await
                    .unwrap(),
                1
            );
            assert!(
                crate::upload::prune_remote(&config, &backups, "jp", &first, 1)
                    .await
                    .is_ok()
            );
            assert!(
                manual.exists(),
                "manual backup remains the newest retained version"
            );
            assert!(
                first_path.exists(),
                "manual backup must not evict the daily backup"
            );
            assert!(
                remote_current.exists(),
                "remote daily backup must remain retained"
            );
            fs::remove_file(&manual).unwrap();
        }
        fs::write(&prior, b"old").unwrap();
        assert!(matches!(
            backup_today(&backups, "jp", &url, &first, 1, &env_root, "sg", false)
                .await
                .unwrap(),
            Outcome::AlreadyExists
        ));
        assert!(!prior.exists());
        let third = account("binance_fr_arb03");
        let old_dir = backups.join("jp").join(&third.slug);
        fs::create_dir_all(&old_dir).unwrap();
        fs::set_permissions(&old_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let old = old_dir.join("2020-01-01.tar.gz");
        fs::write(&old, b"old").unwrap();
        let third_keys = config_keys(&third).unwrap();
        let risk = third_keys
            .iter()
            .find(|k| k.name.ends_with(":pre_trade_risk_params"))
            .unwrap();
        let list = third_keys
            .iter()
            .find(|k| k.name.contains(":fr_fwd_trade_symbols:"))
            .unwrap();
        redis::cmd("HSET")
            .arg(&risk.name)
            .arg("max_pos_u")
            .arg("500")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg(&list.name)
            .arg("[]")
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        assert!(
            backup_today(&backups, "jp", &url, &third, 1, &env_root, "sg", false)
                .await
                .is_err()
        );
        assert!(old.exists());
        std::os::unix::fs::symlink(
            "/etc/passwd",
            env_root.join(&second.slug).join("config/bad-link"),
        )
        .unwrap();
        assert!(capture_local_files(&env_root.join(&second.slug)).is_err());
        fs::write(&first_path, b"corrupted").unwrap();
        assert!(
            backup_today(&backups, "jp", &url, &first, 1, &env_root, "sg", false)
                .await
                .is_err()
        );
        drop(restore);
        drop(server);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sg_tar_accepts_complete_files_and_rejects_links() {
        fn tar_stream(link: bool) -> Vec<u8> {
            let mut builder = tar::Builder::new(Vec::new());
            for (name, bytes) in [
                ("env.sh", b"ENV=1\n".as_slice()),
                ("trade_engine.toml", b"x=1\n".as_slice()),
                ("config/viz.toml", b"port=1\n".as_slice()),
                ("config/viz.toml.bak.20261101", b"old\n".as_slice()),
            ] {
                let mut h = tar::Header::new_gnu();
                h.set_size(bytes.len() as u64);
                h.set_mode(0o600);
                h.set_cksum();
                builder.append_data(&mut h, name, bytes).unwrap();
            }
            if link {
                let mut h = tar::Header::new_gnu();
                h.set_entry_type(tar::EntryType::Symlink);
                h.set_size(0);
                h.set_mode(0o777);
                h.set_link_name("/etc/passwd").unwrap();
                h.set_cksum();
                builder.append_data(&mut h, "config/link", &[][..]).unwrap();
            }
            builder.into_inner().unwrap()
        }
        let files = parse_remote_tar(&tar_stream(false)).unwrap();
        assert_eq!(files.len(), 3);
        assert!(files
            .iter()
            .any(|f| f.path == "config/viz.toml" && f.bytes == b"port=1\n"));
        assert!(parse_remote_tar(&tar_stream(true)).is_err());
    }
}
