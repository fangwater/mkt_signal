//! Read-only snapshots of the current shared CTA Prep and Spread workers.

use super::{backup, cta_factors};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use mkt_signal::common::delist_accounts::AccountSpec;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tokio_postgres::{IsolationLevel, NoTls, Transaction};

const MAX_FILE: u64 = 32 * 1024 * 1024;
const MAX_TOTAL: u64 = 128 * 1024 * 1024;
const MAX_ENTRIES: usize = 8192;

#[derive(Serialize, Deserialize)]
struct Manifest {
    site: String,
    environment: String,
    captured_utc: String,
    exec_sources: Vec<String>,
    warnings: Vec<String>,
    files: Vec<FileEntry>,
    redis: Vec<RedisEntry>,
    factor_process: FactorArchive,
}
#[derive(Serialize, Deserialize)]
struct FactorArchive {
    name: String,
    combinations: Vec<String>,
    files: Vec<String>,
}
#[derive(Serialize, Deserialize)]
struct FileEntry {
    path: String,
    size: u64,
    mode: u32,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
struct RedisEntry {
    key: String,
    ttl_ms: i64,
    dump_path: Option<String>,
}
struct Blob {
    path: String,
    mode: u32,
    bytes: Vec<u8>,
}

pub struct Created {
    pub outcome: backup::Outcome,
    pub warnings: Vec<String>,
}

fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
fn safe_path(p: &str) -> bool {
    !p.is_empty()
        && Path::new(p)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

fn is_backup(path: &Path) -> bool {
    path.components().any(|part| match part {
        Component::Normal(name) => {
            let lower = name.to_string_lossy().to_ascii_lowercase();
            [".bk", ".bak", ".backup"]
                .iter()
                .any(|s| lower.ends_with(s) || lower.contains(&format!("{s}.")))
        }
        _ => true,
    })
}
fn push(blobs: &mut Vec<Blob>, path: String, mode: u32, bytes: Vec<u8>) -> Result<()> {
    if !safe_path(&path)
        || bytes.len() as u64 > MAX_FILE
        || blobs.len() >= MAX_ENTRIES
        || blobs.iter().any(|b| b.path == path)
    {
        bail!("invalid, duplicate or oversized CTA archive entry");
    }
    let size = blobs.iter().map(|b| b.bytes.len() as u64).sum::<u64>();
    if size.saturating_add(bytes.len() as u64) > MAX_TOTAL {
        bail!("CTA archive exceeds size limit");
    }
    blobs.push(Blob { path, mode, bytes });
    Ok(())
}
fn read_file(base: &Path, relative: &Path, archive: String, blobs: &mut Vec<Blob>) -> Result<()> {
    if !relative
        .components()
        .all(|c| matches!(c, Component::Normal(_)))
        || is_backup(relative)
    {
        bail!("invalid CTA file path");
    }
    let full = base.join(relative);
    let before = fs::symlink_metadata(&full).context("inspect CTA configuration file")?;
    if !before.file_type().is_file() || before.len() > MAX_FILE {
        bail!("CTA configuration is not a regular file or is too large");
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&full)
        .context("open CTA configuration file")?;
    let mut data = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(MAX_FILE + 1)
        .read_to_end(&mut data)?;
    let after = file.metadata()?;
    if data.len() as u64 > MAX_FILE
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
    {
        bail!("CTA configuration changed while reading");
    }
    push(blobs, archive, before.permissions().mode() & 0o777, data)
}
fn walk(
    base: &Path,
    relative: &Path,
    archive_prefix: &str,
    blobs: &mut Vec<Blob>,
    depth: usize,
) -> Result<()> {
    if depth > 16
        || !fs::symlink_metadata(base.join(relative))?
            .file_type()
            .is_dir()
    {
        bail!("invalid CTA config directory");
    }
    for entry in fs::read_dir(base.join(relative))? {
        let entry = entry?;
        let next = relative.join(entry.file_name());
        if is_backup(&next) {
            continue;
        }
        let name = next.to_str().context("CTA config path is not UTF-8")?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(base, &next, archive_prefix, blobs, depth + 1)?;
        } else if kind.is_file() {
            let archive = format!("{archive_prefix}/{name}");
            if let Some(previous) = blobs.iter().position(|blob| blob.path == archive) {
                let mut checked = Vec::new();
                read_file(base, &next, archive, &mut checked)?;
                if checked[0].bytes != blobs[previous].bytes {
                    bail!("CTA configuration changed while reading");
                }
            } else {
                read_file(base, &next, archive, blobs)?;
            }
        } else {
            bail!("CTA config contains a symlink or unsupported file");
        }
    }
    Ok(())
}
fn capture_env(base: &Path, archive_prefix: &str, exec: bool, blobs: &mut Vec<Blob>) -> Result<()> {
    if !fs::symlink_metadata(base)
        .context("inspect CTA environment")?
        .file_type()
        .is_dir()
    {
        bail!("CTA environment is missing or a symlink");
    }
    read_file(
        base,
        Path::new("env.sh"),
        format!("{archive_prefix}/env.sh"),
        blobs,
    )?;
    if exec {
        read_file(
            base,
            Path::new("trade_engine.toml"),
            format!("{archive_prefix}/trade_engine.toml"),
            blobs,
        )?;
    }
    walk(base, Path::new("config"), archive_prefix, blobs, 0)?;
    for name in ["release.json", "release.sha256", "SHA256SUMS"] {
        if base.join(name).is_file() {
            read_file(
                base,
                Path::new(name),
                format!("{archive_prefix}/{name}"),
                blobs,
            )?;
        }
    }
    Ok(())
}

pub fn worker(name: &str) -> Option<cta_factors::Worker> {
    [cta_factors::Worker::Perp, cta_factors::Worker::Spread]
        .into_iter()
        .find(|worker| worker.name() == name)
}

pub fn account(worker: cta_factors::Worker) -> AccountSpec {
    AccountSpec {
        slug: worker.name().into(),
        alias: worker.name().into(),
        exchange: if worker == cta_factors::Worker::Perp {
            "binance"
        } else {
            "okex"
        }
        .into(),
        kind: if worker == cta_factors::Worker::Perp {
            "cta_perp"
        } else {
            "cta_spread"
        }
        .into(),
        host: "jp".into(),
        site: mkt_signal::common::delist_accounts::RedisSite::Jp,
        config_url: cta_factors::config_url(worker, None),
    }
}

fn strategy_names(snapshot: &cta_factors::Snapshot) -> Result<BTreeSet<String>> {
    let mut names: BTreeSet<String> = snapshot
        .plan
        .combinations
        .values()
        .filter(|combination| combination.signal.is_some())
        .map(|combination| combination.manager_strategy_name())
        .collect();
    // Retiring tables still own factual position ledgers and Manager bindings.
    if let Some(tables) = snapshot
        .runtime
        .as_ref()
        .and_then(|state| state["position_tables"]["tables"].as_array())
    {
        for table in tables {
            names.insert(
                table["strategy_name"]
                    .as_str()
                    .context("worker position table has no strategy name")?
                    .into(),
            );
        }
    }
    if names.iter().any(|name| !identifier(name)) {
        bail!("invalid worker Manager strategy name");
    }
    Ok(names)
}

fn capture_factors(
    factor_env: &Path,
    snapshot: cta_factors::Snapshot,
    worker: cta_factors::Worker,
    blobs: &mut Vec<Blob>,
    warnings: &mut Vec<String>,
) -> Result<FactorArchive> {
    warnings.extend(cta_factors::view(Ok(&snapshot), worker).errors);
    let combinations = snapshot.plan.combinations.keys().cloned().collect();
    let prefix = format!("files/factor/{}", worker.name());
    let mut files = Vec::new();
    for (relative, data) in snapshot.files {
        let path = format!(
            "{prefix}/{}",
            relative.to_str().context("factor path is not UTF-8")?
        );
        push(blobs, path.clone(), 0o600, data)?;
        files.push(path);
    }
    // env.sh may be absent on a configuration-only worker, but present metadata
    // must be read securely. The full config directory includes auxiliary inputs.
    walk(factor_env, Path::new("config"), &prefix, blobs, 0)?;
    for name in ["env.sh", "release.json", "release.sha256", "SHA256SUMS"] {
        match fs::symlink_metadata(factor_env.join(name)) {
            Ok(_) => {
                let path = format!("{prefix}/{name}");
                read_file(factor_env, Path::new(name), path.clone(), blobs)?;
                files.push(path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).context("inspect factor environment metadata"),
        }
    }
    let path = format!("{prefix}/merged_plan.json");
    push(
        blobs,
        path.clone(),
        0o600,
        serde_json::to_vec_pretty(&snapshot.plan)?,
    )?;
    files.push(path);
    files.extend(
        blobs
            .iter()
            .filter(|b| b.path.starts_with(&format!("{prefix}/")))
            .map(|b| b.path.clone()),
    );
    files.sort();
    files.dedup();
    Ok(FactorArchive {
        name: worker.name().into(),
        combinations,
        files,
    })
}

async fn rows(tx: &Transaction<'_>, sql: &str, keys: &[String]) -> Result<Vec<Value>> {
    Ok(tx
        .query(sql, &[&keys])
        .await?
        .into_iter()
        .map(|r| r.get::<_, Value>(0))
        .collect())
}
fn strings(rows: &[Value], field: &str) -> Result<BTreeSet<String>> {
    rows.iter()
        .map(|v| {
            v.get(field)
                .and_then(Value::as_str)
                .map(str::to_string)
                .with_context(|| format!("Manager row missing {field}"))
        })
        .collect()
}
struct ManagerCapture {
    catalog: Value,
    sources: Vec<String>,
}
async fn capture_manager(url: &str, names: &BTreeSet<String>) -> Result<ManagerCapture> {
    let (mut client, connection) =
        tokio::time::timeout(Duration::from_secs(5), tokio_postgres::connect(url, NoTls))
            .await
            .context("Manager connect timeout")?
            .map_err(|_| anyhow::anyhow!("Manager connection failed"))?;
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = tokio::time::timeout(Duration::from_secs(20),async {
        let tx=client.build_transaction().isolation_level(IsolationLevel::RepeatableRead).read_only(true).start().await?;
        let signal_names: Vec<String>=names.iter().cloned().collect();
        let virtuals=rows(&tx,"SELECT to_jsonb(v) FROM cta_virtual_accounts v WHERE EXISTS (SELECT 1 FROM cta_virtual_account_bindings b WHERE b.virtual_id=v.virtual_id AND b.position_strategy_name=ANY($1)) ORDER BY v.virtual_id",&signal_names).await?;
        let virtual_ids: Vec<String>=strings(&virtuals,"virtual_id")?.into_iter().collect();
        let virtual_bindings=rows(&tx,"SELECT to_jsonb(b) FROM cta_virtual_account_bindings b WHERE b.virtual_id=ANY($1) ORDER BY b.virtual_id,b.binding_name",&virtual_ids).await?;
        let follows=rows(&tx,"SELECT to_jsonb(f) FROM cta_account_follows f WHERE f.virtual_id=ANY($1) ORDER BY f.source_id,f.virtual_id",&virtual_ids).await?;
        let mut strategy_names=names.clone(); strategy_names.extend(strings(&virtual_bindings,"position_strategy_name")?);
        let strategy_vec: Vec<String>=strategy_names.into_iter().collect();
        let bindings=rows(&tx,"SELECT to_jsonb(b) FROM cta_account_strategy_bindings b WHERE b.position_strategy_name=ANY($1) ORDER BY b.source_id,b.binding_name",&strategy_vec).await?;
        let mut sources=strings(&bindings,"source_id")?; sources.extend(strings(&follows,"source_id")?);
        if sources.iter().any(|s| !identifier(s)) { bail!("invalid Manager Exec source identifier"); }
        let source_vec: Vec<String>=sources.into_iter().collect();
        let source_rows=rows(&tx,"SELECT to_jsonb(s) FROM cta_order_sources s WHERE s.source_id=ANY($1) ORDER BY s.source_id",&source_vec).await?;
        if source_rows.len()!=source_vec.len() { bail!("Manager Exec source catalog incomplete"); }
        let positions=rows(&tx,"SELECT to_jsonb(p) FROM cta_position_strategies p WHERE p.strategy_name=ANY($1) ORDER BY p.strategy_name",&strategy_vec).await?;
        let mut order_names=strings(&bindings,"order_strategy_name")?; order_names.extend(strings(&virtual_bindings,"order_strategy_name")?);
        for pos in &positions { if let Some(overrides)=pos.get("symbol_order_strategy_overrides").and_then(Value::as_object) { for v in overrides.values() { if let Some(name)=v.as_str() { order_names.insert(name.to_string()); } } } }
        let order_vec:Vec<String>=order_names.into_iter().collect();
        let orders=rows(&tx,"SELECT to_jsonb(o) FROM cta_order_strategies o WHERE o.strategy_name=ANY($1) ORDER BY o.strategy_name",&order_vec).await?;
        if orders.len()!=order_vec.len() { bail!("Manager order templates are incomplete"); }
        let leverages=rows(&tx,"SELECT to_jsonb(l) FROM cta_account_symbol_leverages l WHERE l.source_id=ANY($1) ORDER BY l.source_id,l.symbol",&source_vec).await?;
        let virtual_managers=rows(&tx,"SELECT to_jsonb(g) FROM cta_virtual_account_managers g WHERE g.virtual_id=ANY($1) ORDER BY g.virtual_id,g.user_id",&virtual_ids).await?;
        let strategy_grants=rows(&tx,"SELECT to_jsonb(g) FROM cta_position_strategy_grants g WHERE g.strategy_name=ANY($1) ORDER BY g.strategy_name,g.user_id",&strategy_vec).await?;
        let strategy_managers=rows(&tx,"SELECT to_jsonb(g) FROM cta_position_strategy_managers g WHERE g.strategy_name=ANY($1) ORDER BY g.strategy_name,g.user_id",&strategy_vec).await?;
        let source_grants=rows(&tx,"SELECT to_jsonb(g) FROM cta_user_source_permissions g WHERE g.source_id=ANY($1) ORDER BY g.source_id,g.user_id",&source_vec).await?;
        let mut users=BTreeSet::new();
        for row in virtuals.iter().chain(positions.iter()).chain(orders.iter()) { if let Some(id)=row.get("created_by_user_id").and_then(Value::as_i64) { users.insert(id); } }
        for row in virtual_managers.iter().chain(strategy_grants.iter()).chain(strategy_managers.iter()).chain(source_grants.iter()) { if let Some(id)=row.get("user_id").and_then(Value::as_i64) { users.insert(id); } }
        let user_ids:Vec<i64>=users.into_iter().collect();
        let user_rows:Vec<Value>=tx.query("SELECT jsonb_build_object('user_id',user_id,'username',username,'role',role,'disabled',disabled) FROM cta_users WHERE user_id=ANY($1) ORDER BY user_id",&[&user_ids]).await?.into_iter().map(|r|r.get(0)).collect();
        tx.commit().await?;
        Ok(ManagerCapture { sources:source_vec,catalog:json!({"virtual_accounts":virtuals,"virtual_bindings":virtual_bindings,"follows":follows,"account_bindings":bindings,"sources":source_rows,"position_strategies":positions,"order_strategies":orders,"account_leverages":leverages,"virtual_managers":virtual_managers,"strategy_grants":strategy_grants,"strategy_managers":strategy_managers,"source_grants":source_grants,"users":user_rows}) })
    }).await.context("Manager snapshot timed out")?;
    task.abort();
    result.context("read Manager configuration snapshot")
}

struct ExecEnvironment {
    directory: PathBuf,
    markets: Vec<String>,
}

fn source_envs(root: &Path, ids: &[String]) -> Result<BTreeMap<String, ExecEnvironment>> {
    let path = root.join("crypto_cta_manager/config/cta-manager.toml");
    let bytes = fs::read(&path).context("read Manager source configuration")?;
    if bytes.len() > MAX_FILE as usize {
        bail!("Manager configuration too large");
    }
    let cfg: toml::Value = toml::from_str(std::str::from_utf8(&bytes)?)
        .map_err(|_| anyhow::anyhow!("invalid Manager source configuration"))?;
    let sources = cfg
        .get("sources")
        .and_then(toml::Value::as_array)
        .context("Manager sources missing")?;
    let root_canonical = fs::canonicalize(root)?;
    let mut result = BTreeMap::new();
    for id in ids {
        let matching: Vec<_> = sources
            .iter()
            .filter(|s| s.get("id").and_then(toml::Value::as_str) == Some(id))
            .collect();
        if matching.len() != 1 {
            bail!("Manager source {id} missing or duplicated");
        }
        let source = matching[0];
        let env_path = if let Some(path) = source.get("env_path").and_then(toml::Value::as_str) {
            PathBuf::from(path)
                .parent()
                .context("invalid Exec env path")?
                .to_path_buf()
        } else {
            let path = Path::new(
                source
                    .get("rocksdb_path")
                    .and_then(toml::Value::as_str)
                    .context("Exec source path missing")?,
            );
            if path.file_name().and_then(|s| s.to_str()) != Some("persist_manager")
                || path
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|s| s.to_str())
                    != Some("data")
            {
                bail!("invalid Exec source storage path");
            }
            path.parent()
                .and_then(Path::parent)
                .context("invalid Exec source storage parent")?
                .to_path_buf()
        };
        if !env_path.is_absolute() {
            bail!("Exec environment path is not absolute");
        }
        let env = fs::canonicalize(&env_path).context("resolve Exec environment")?;
        if env.parent() != Some(root_canonical.as_path())
            || !identifier(env.file_name().and_then(|x| x.to_str()).unwrap_or(""))
        {
            bail!("Exec environment is outside the configured root");
        }
        if fs::symlink_metadata(&env_path)?.file_type().is_symlink() {
            bail!("Exec environment is a symlink");
        }
        let venue = source
            .get("venue")
            .and_then(toml::Value::as_str)
            .context("Exec source venue missing")?;
        let markets = match venue {
            "binance-futures" => vec!["binance-futures"],
            "binance-margin" => vec!["binance-margin"],
            "okex-futures" => vec!["okex-futures", "okex-margin"],
            "okex-margin" => vec!["okex-margin"],
            _ => bail!("CTA backup does not support the configured Exec market"),
        }
        .into_iter()
        .map(str::to_owned)
        .collect();
        result.insert(
            id.clone(),
            ExecEnvironment {
                directory: env,
                markets,
            },
        );
    }
    Ok(result)
}

fn wanted_redis_key(env: &str, key: &str, markets: &[String]) -> bool {
    let Some(rest) = key.strip_prefix(&format!("{env}:")) else {
        return false;
    };
    for market in markets {
        let Some(suffix) = rest.strip_prefix(&format!("{market}:")) else {
            continue;
        };
        return suffix == "pre_trade_risk_params"
            || suffix == "exec:max_pos_u"
            || suffix == "batch_exec_state:position_allocations"
            || suffix == "chase_exec_state:position_allocations"
            || suffix == "batch_exec_state:leverage_initialized"
            || suffix == "chase_exec_state:leverage_initialized"
            || suffix.starts_with("batch_exec:")
            || suffix.starts_with("chase_exec:")
            || suffix.starts_with("exec_switch:");
    }
    false
}

async fn scan_exec_keys(
    conn: &mut redis::aio::MultiplexedConnection,
    envs: &BTreeMap<String, ExecEnvironment>,
) -> Result<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for source in envs.values() {
        let env = source
            .directory
            .file_name()
            .and_then(|s| s.to_str())
            .context("Exec environment name missing")?;
        if !identifier(env) {
            bail!("invalid Exec Redis namespace");
        }
        let mut cursor = 0u64;
        loop {
            let (next, found): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{env}:*"))
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut *conn)
                .await
                .context("scan Exec config keys")?;
            for key in found {
                if wanted_redis_key(env, &key, &source.markets) {
                    keys.insert(key);
                }
            }
            if keys.len() > 4096 {
                bail!("too many CTA Redis config keys");
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
    }
    Ok(keys)
}
async fn capture_redis(
    url: &str,
    envs: &BTreeMap<String, ExecEnvironment>,
    blobs: &mut Vec<Blob>,
) -> Result<(Vec<RedisEntry>, Vec<String>)> {
    let client = redis::Client::open(url).context("open CTA Redis")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect CTA Redis")?;
    let mut keys = BTreeSet::new();
    let mut expected = BTreeSet::new();
    for source in envs.values() {
        let env = source
            .directory
            .file_name()
            .and_then(|s| s.to_str())
            .context("Exec environment name missing")?;
        if !identifier(env) {
            bail!("invalid Exec Redis namespace");
        }
        for market in &source.markets {
            for suffix in [
                "pre_trade_risk_params",
                "exec:max_pos_u",
                "batch_exec:strategy_names",
                "batch_exec:removed_strategy_names",
                "chase_exec:strategy_names",
                "chase_exec:removed_strategy_names",
                "batch_exec_state:position_allocations",
                "chase_exec_state:position_allocations",
                "batch_exec_state:leverage_initialized",
                "chase_exec_state:leverage_initialized",
                "exec_switch:strategy_names",
            ] {
                let key = format!("{env}:{market}:{suffix}");
                keys.insert(key.clone());
                if market.ends_with("-futures") && suffix == "pre_trade_risk_params" {
                    expected.insert(key);
                }
            }
        }
    }
    let scanned = scan_exec_keys(&mut conn, envs).await?;
    keys.extend(scanned.iter().cloned());
    let mut pipe = redis::pipe();
    pipe.atomic();
    for key in &keys {
        pipe.cmd("DUMP").arg(key).cmd("PTTL").arg(key);
    }
    let results: Vec<redis::Value> = pipe
        .query_async(&mut conn)
        .await
        .context("read CTA Redis configuration transaction")?;
    if results.len() != keys.len() * 2 {
        bail!("invalid CTA Redis transaction result");
    }
    if scan_exec_keys(&mut conn, envs).await? != scanned {
        bail!("Exec Redis config key set changed during backup; retry");
    }
    let mut entries = Vec::new();
    let mut missing = Vec::new();
    for (index, key) in keys.into_iter().enumerate() {
        let dump: Option<Vec<u8>> =
            redis::from_redis_value(&results[index * 2]).context("decode CTA Redis dump")?;
        let ttl: i64 =
            redis::from_redis_value(&results[index * 2 + 1]).context("decode CTA Redis TTL")?;
        let dump_path = match dump {
            Some(bytes) => {
                let path = format!("redis/{index:04}.dump");
                push(blobs, path.clone(), 0o600, bytes)?;
                Some(path)
            }
            None => {
                if expected.contains(&key) {
                    missing.push(key.clone());
                }
                None
            }
        };
        entries.push(RedisEntry {
            key,
            ttl_ms: ttl,
            dump_path,
        });
    }
    Ok((entries, missing))
}

fn write_tar(path: &Path, manifest: &Manifest, blobs: &[Blob]) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    let mut tar = tar::Builder::new(GzEncoder::new(file, Compression::default()));
    let manifest = serde_json::to_vec_pretty(manifest)?;
    for (name, bytes) in std::iter::once(("manifest.json", manifest.as_slice()))
        .chain(blobs.iter().map(|b| (b.path.as_str(), b.bytes.as_slice())))
    {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        tar.append_data(&mut header, name, bytes)?;
    }
    let gzip = tar.into_inner()?;
    let mut file = gzip.finish()?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

pub fn validate_archive(path: &Path, site: &str, account: &AccountSpec) -> Result<()> {
    let worker = worker(&account.slug).context("unknown CTA archive worker")?;
    if site != "jp" || account.kind != self::account(worker).kind {
        bail!("invalid CTA archive identity");
    }
    let mut tar = tar::Archive::new(GzDecoder::new(File::open(path)?));
    let mut files = BTreeMap::new();
    let mut size = 0u64;
    for entry in tar.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() || entry.size() > MAX_FILE {
            bail!("invalid CTA archive member");
        }
        size = size
            .checked_add(entry.size())
            .context("CTA archive size overflow")?;
        if size > MAX_TOTAL || files.len() > MAX_ENTRIES {
            bail!("CTA archive exceeds limits");
        }
        let name = entry
            .path()?
            .to_str()
            .context("invalid CTA archive member name")?
            .to_string();
        if !safe_path(&name) {
            bail!("unsafe CTA archive member name");
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        if files.insert(name, bytes).is_some() {
            bail!("duplicate CTA archive member");
        }
    }
    std::io::copy(&mut tar.into_inner(), &mut std::io::sink())
        .context("check CTA gzip checksum")?;
    let manifest: Manifest = serde_json::from_slice(
        &files
            .remove("manifest.json")
            .context("CTA manifest missing")?,
    )?;
    if manifest.site != site || manifest.environment != account.slug {
        bail!("CTA archive metadata mismatch");
    }
    let prefix = format!("files/factor/{}", worker.name());
    let merged: Value = serde_json::from_slice(
        files
            .get(&format!("{prefix}/merged_plan.json"))
            .context("CTA merged plan missing")?,
    )?;
    let combinations: BTreeMap<String, mkt_signal::cta_factor_calc::config::Combination> =
        serde_json::from_value(merged["combinations"].clone())?;
    let groups = serde_json::from_value(merged.get("symbol_groups").cloned().unwrap_or(json!({})))?;
    let plan = mkt_signal::cta_factor_calc::config::Plan::merge_with_symbol_groups(
        combinations.into_values(),
        groups,
    )?;
    worker.validate_plan(&plan)?;
    let mut recorded = BTreeSet::new();
    for entry in &manifest.files {
        if !safe_path(&entry.path) || entry.mode > 0o777 || !recorded.insert(entry.path.clone()) {
            bail!("invalid CTA archive file metadata");
        }
        let bytes = files
            .remove(&entry.path)
            .context("CTA archive file missing")?;
        if bytes.len() as u64 != entry.size
            || format!("{:x}", Sha256::digest(&bytes)) != entry.sha256
        {
            bail!("CTA archive file checksum mismatch");
        }
    }
    for name in ["manager/catalog.json", "files/manager/cta-manager.toml"] {
        if !recorded.contains(name) {
            bail!("required CTA configuration missing from archive");
        }
    }
    let factor = &manifest.factor_process;
    let expected: Vec<_> = plan.combinations.keys().cloned().collect();
    if factor.name != worker.name()
        || factor.combinations != expected
        || !factor
            .files
            .contains(&format!("{prefix}/{}", worker.default_config()))
        || !factor.files.contains(&format!("{prefix}/merged_plan.json"))
        || factor.combinations.iter().any(|name| {
            !identifier(name)
                || !factor
                    .files
                    .iter()
                    .any(|path| path.ends_with(&format!("/{name}.json")))
        })
    {
        bail!("invalid shared factor archive identity or inventory");
    }
    let mut unique = BTreeSet::new();
    for path in &factor.files {
        if !path.starts_with(&format!("{prefix}/"))
            || !recorded.contains(path)
            || !unique.insert(path)
        {
            bail!("shared factor archive member missing or duplicated");
        }
    }
    for source in &manifest.exec_sources {
        if !identifier(source)
            || !recorded.contains(&format!("files/exec/{source}/env.sh"))
            || !recorded.contains(&format!("files/exec/{source}/trade_engine.toml"))
        {
            bail!("required Exec configuration missing from CTA archive");
        }
    }
    let mut redis_names = BTreeSet::new();
    for r in &manifest.redis {
        if !redis_names.insert(&r.key) {
            bail!("duplicate CTA Redis key");
        }
        if let Some(name) = &r.dump_path {
            if !recorded.contains(name) || !name.starts_with("redis/") {
                bail!("CTA Redis dump missing");
            }
        }
    }
    if !files.is_empty() {
        bail!("CTA archive inventory mismatch");
    }
    Ok(())
}

fn archive_warnings(path: &Path) -> Result<Vec<String>> {
    let mut tar = tar::Archive::new(GzDecoder::new(File::open(path)?));
    for entry in tar.entries()? {
        let mut entry = entry?;
        if entry.path()?.as_ref() == Path::new("manifest.json") {
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes)?;
            let mut m: Manifest = serde_json::from_slice(&bytes)?;
            m.warnings.sort();
            m.warnings.dedup();
            return Ok(m.warnings);
        }
    }
    bail!("CTA backup manifest missing")
}

pub fn validated_warnings(path: &Path, account: &AccountSpec) -> Result<Vec<String>> {
    validate_archive(path, "jp", account)?;
    archive_warnings(path)
}

pub async fn backup_today(
    root: &Path,
    env_root: &Path,
    redis_url: &str,
    manager_url: &str,
    factor_env: &Path,
    worker: cta_factors::Worker,
) -> Result<Created> {
    backup_snapshot(
        root,
        env_root,
        redis_url,
        manager_url,
        factor_env,
        worker,
        false,
    )
    .await
}

pub async fn backup_manual(
    root: &Path,
    env_root: &Path,
    redis_url: &str,
    manager_url: &str,
    factor_env: &Path,
    worker: cta_factors::Worker,
) -> Result<Created> {
    backup_snapshot(
        root,
        env_root,
        redis_url,
        manager_url,
        factor_env,
        worker,
        true,
    )
    .await
}

async fn backup_snapshot(
    root: &Path,
    env_root: &Path,
    redis_url: &str,
    manager_url: &str,
    factor_env: &Path,
    worker: cta_factors::Worker,
    manual: bool,
) -> Result<Created> {
    let account = account(worker);
    let dir = root.join("jp").join(&account.slug);
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .context("create CTA backup directory")?;
    for path in [root.to_path_buf(), root.join("jp"), dir.clone()] {
        let meta = fs::symlink_metadata(path)?;
        if !meta.file_type().is_dir() || meta.permissions().mode() & 0o077 != 0 {
            bail!("CTA backup directories must be private");
        }
    }
    let date = Utc::now().date_naive();
    let path = dir.join(format!("{}.tar.gz", backup::archive_stem(manual)));
    if path.exists() {
        if manual {
            bail!("manual backup name already exists; retry");
        }
        validate_archive(&path, "jp", &account)?;
        return Ok(Created {
            warnings: archive_warnings(&path)?,
            outcome: backup::Outcome::AlreadyExists,
        });
    }
    let snapshot = cta_factors::snapshot(factor_env, worker)?;
    let names = strategy_names(&snapshot)?;
    let configured_names: BTreeSet<_> = snapshot
        .plan
        .combinations
        .values()
        .filter(|combination| combination.signal.is_some())
        .map(|combination| combination.manager_strategy_name())
        .collect();
    let manager = capture_manager(manager_url, &names).await?;
    let envs = source_envs(env_root, &manager.sources)?;
    let mut blobs = Vec::new();
    let mut warnings = Vec::new();
    let recorded_names = strings(
        manager.catalog["position_strategies"]
            .as_array()
            .context("Manager position strategy snapshot missing")?,
        "strategy_name",
    )?;
    let missing = configured_names.difference(&recorded_names).count();
    if missing > 0 {
        warnings.push(format!("Manager 当前缺少 {missing} 个仓位策略的配置"));
    }
    if manager.sources.is_empty() {
        warnings.push("Manager 当前没有关联的 Exec 账户".into());
    }
    let factor_process = capture_factors(factor_env, snapshot, worker, &mut blobs, &mut warnings)?;
    for (source, env) in &envs {
        capture_env(
            &env.directory,
            &format!("files/exec/{source}"),
            true,
            &mut blobs,
        )?;
    }
    read_file(
        env_root,
        Path::new("crypto_cta_manager/config/cta-manager.toml"),
        "files/manager/cta-manager.toml".into(),
        &mut blobs,
    )?;
    push(
        &mut blobs,
        "manager/catalog.json".into(),
        0o600,
        serde_json::to_vec_pretty(&manager.catalog)?,
    )?;
    let (redis, missing) = tokio::time::timeout(
        Duration::from_secs(30),
        capture_redis(redis_url, &envs, &mut blobs),
    )
    .await
    .context("CTA Redis capture timed out")??;
    if !missing.is_empty() {
        warnings.push(format!(
            "以下 Redis 配置键当前不存在：{}",
            missing.join(", ")
        ));
    }
    let files = blobs
        .iter()
        .map(|b| FileEntry {
            path: b.path.clone(),
            size: b.bytes.len() as u64,
            mode: b.mode,
            sha256: format!("{:x}", Sha256::digest(&b.bytes)),
        })
        .collect();
    let manifest = Manifest {
        site: "jp".into(),
        environment: account.slug.clone(),
        captured_utc: Utc::now().to_rfc3339(),
        exec_sources: manager.sources,
        warnings: warnings.clone(),
        files,
        redis,
        factor_process,
    };
    let temp = dir.join(format!(
        ".{date}-{}-{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let result = (|| {
        write_tar(&temp, &manifest, &blobs)?;
        validate_archive(&temp, "jp", &account)?;
        match fs::hard_link(&temp, &path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && !manual => {
                validate_archive(&path, "jp", &account)?;
                return Ok(Created {
                    warnings: archive_warnings(&path)?,
                    outcome: backup::Outcome::AlreadyExists,
                });
            }
            Err(e) => return Err(e).context("publish CTA archive"),
        };
        File::open(&dir)?.sync_all()?;
        Ok(Created {
            warnings,
            outcome: backup::Outcome::Created(path),
        })
    })();
    let _ = fs::remove_file(temp);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

    fn inventory(blobs: &[Blob]) -> Vec<FileEntry> {
        blobs
            .iter()
            .map(|b| FileEntry {
                path: b.path.clone(),
                mode: b.mode,
                size: b.bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&b.bytes)),
            })
            .collect()
    }

    #[test]
    fn spread_archive_round_trip_preserves_all_supported_frequencies() {
        let worker = cta_factors::Worker::Spread;
        let env = cta_factors::test_environment_for(worker);
        let directory = env.0.join("config/cta_factor_spread");
        for entry in fs::read_dir(&directory).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }
        let mut expected = Vec::new();
        for interval in ["1min", "5min", "15min", "1h"] {
            let name = format!("spread_{interval}");
            let combination = json!({
                "name": name,
                "strategy_group": "spread01",
                "venue": "okex-futures",
                "reference_venue": "binance-futures",
                "interval": interval,
                "factor_rules": [{
                    "venue": "binance-futures", "标的": "ETHUSDT",
                    "nq": "关闭", "符号": 1, "因子": "factor_036"
                }],
                "signal": {
                    "enabled": true, "max_pos_u": 200,
                    "entry": {"nq_change_enabled": false}, "execution": {},
                    "spread": {"long_quantile": 0.3, "short_quantile": 0.7}
                }
            });
            fs::write(
                directory.join(format!("{name}.json")),
                serde_json::to_vec(&combination).unwrap(),
            )
            .unwrap();
            expected.push(name);
        }
        expected.sort();
        let snapshot = cta_factors::snapshot(&env.0, worker).unwrap();
        let mut blobs = Vec::new();
        let mut warnings = Vec::new();
        let factor = capture_factors(&env.0, snapshot, worker, &mut blobs, &mut warnings).unwrap();
        assert_eq!(factor.combinations, expected);
        for name in ["manager/catalog.json", "files/manager/cta-manager.toml"] {
            push(&mut blobs, name.into(), 0o600, b"fixture".to_vec()).unwrap();
        }
        let spec = account(worker);
        let manifest = Manifest {
            site: "jp".into(),
            environment: spec.slug.clone(),
            captured_utc: Utc::now().to_rfc3339(),
            exec_sources: vec![],
            warnings,
            files: inventory(&blobs),
            redis: vec![],
            factor_process: factor,
        };
        let root = temp_root();
        let archive = root.join("spread.tar.gz");
        write_tar(&archive, &manifest, &blobs).unwrap();
        validate_archive(&archive, "jp", &spec).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shared_worker_archives_validate_without_signal_directories_and_reject_missing_dependencies()
    {
        for worker in [cta_factors::Worker::Perp, cta_factors::Worker::Spread] {
            let root = temp_root();
            let env = cta_factors::test_environment_for(worker);
            let snapshot = cta_factors::snapshot(&env.0, worker).unwrap();
            let mut blobs = Vec::new();
            let mut warnings = Vec::new();
            let factor =
                capture_factors(&env.0, snapshot, worker, &mut blobs, &mut warnings).unwrap();
            assert_eq!(factor.name, worker.name());
            let spec = account(worker);
            assert!(super::worker("binance-cta-perp-rx01").is_none());
            for name in [
                "manager/catalog.json",
                "files/manager/cta-manager.toml",
                "files/exec/exec01/env.sh",
                "files/exec/exec01/trade_engine.toml",
            ] {
                push(&mut blobs, name.into(), 0o600, b"fixture".to_vec()).unwrap();
            }
            let mut manifest = Manifest {
                site: "jp".into(),
                environment: spec.slug.clone(),
                captured_utc: Utc::now().to_rfc3339(),
                exec_sources: vec!["exec01".into()],
                warnings,
                files: inventory(&blobs),
                redis: vec![],
                factor_process: factor,
            };
            let archive = root.join("good.tar.gz");
            write_tar(&archive, &manifest, &blobs).unwrap();
            validate_archive(&archive, "jp", &spec).unwrap();
            assert!(!blobs.iter().any(|b| b.path.starts_with("files/signal/")));
            let wrong = account(if worker == cta_factors::Worker::Perp {
                cta_factors::Worker::Spread
            } else {
                cta_factors::Worker::Perp
            });
            assert!(validate_archive(&archive, "jp", &wrong).is_err());
            blobs[0].bytes.push(0);
            let corrupt = root.join("corrupt.tar.gz");
            write_tar(&corrupt, &manifest, &blobs).unwrap();
            assert!(validate_archive(&corrupt, "jp", &spec).is_err());
            blobs[0].bytes.pop();
            let removed = manifest
                .factor_process
                .files
                .iter()
                .find(|path| {
                    path.ends_with(&format!(
                        "/{}.json",
                        manifest.factor_process.combinations[0]
                    ))
                })
                .unwrap()
                .clone();
            blobs.retain(|b| b.path != removed);
            manifest.files = inventory(&blobs);
            manifest
                .factor_process
                .files
                .retain(|path| path != &removed);
            let missing = root.join("missing.tar.gz");
            write_tar(&missing, &manifest, &blobs).unwrap();
            assert!(validate_archive(&missing, "jp", &spec).is_err());
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn manager_scope_includes_retiring_worker_ledgers() {
        let env = cta_factors::test_environment();
        let mut snapshot = cta_factors::snapshot(&env.0, cta_factors::Worker::Perp).unwrap();
        let current = strategy_names(&snapshot).unwrap();
        snapshot.runtime = Some(json!({"position_tables":{"tables":[{
            "strategy_name":"cta-retired-prep-pos","retired":true}]}}));
        let names = strategy_names(&snapshot).unwrap();
        assert!(names.is_superset(&current));
        assert!(names.contains("cta-retired-prep-pos"));
        snapshot.runtime =
            Some(json!({"position_tables":{"tables":[{"strategy_name":"../invalid"}]}}));
        assert!(strategy_names(&snapshot).is_err());
    }

    #[test]
    fn exec_config_selection_keeps_okx_owned_markets_separate() {
        let markets = vec!["okex-futures".into(), "okex-margin".into()];
        assert!(wanted_redis_key(
            "exec",
            "exec:okex-futures:batch_exec:alpha",
            &markets
        ));
        assert!(wanted_redis_key(
            "exec",
            "exec:okex-margin:pre_trade_risk_params",
            &markets
        ));
        assert!(!wanted_redis_key(
            "exec",
            "exec:binance-futures:batch_exec:alpha",
            &markets
        ));
        assert!(!wanted_redis_key(
            "exec",
            "other:okex-futures:batch_exec:alpha",
            &markets
        ));
        assert!(!wanted_redis_key(
            "exec",
            "exec:okex-futures:market_rules",
            &markets
        ));
    }

    fn temp_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cta-backup-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        root
    }

    struct Server(Child);
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    async fn redis_server(root: &Path) -> (Server, String) {
        let sock = root.join("redis.sock");
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
            .arg(&sock)
            .arg("--dir")
            .arg(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Server(child);
        let url = format!("redis+unix://{}", sock.display());
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            if redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .is_ok()
            {
                break;
            }
            assert!(Instant::now() < end);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (server, url)
    }
    #[tokio::test]
    async fn redis_capture_restores_config_and_excludes_unrelated_keys() {
        if Command::new("redis-server")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_err()
        {
            eprintln!("skipping: redis-server unavailable");
            return;
        }
        let root = temp_root();
        let (server, url) = redis_server(&root).await;
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for (key, value) in [
            (
                "binance-cta-perp-rx01:cta_symbol_factors",
                "{\"BTCUSDT\":{}}",
            ),
            (
                "exec01:binance-futures:batch_exec:alpha",
                "{\"configured\":true}",
            ),
            (
                "exec01:binance-futures:chase_exec_state:position_allocations",
                "chase",
            ),
            (
                "exec01:binance-futures:batch_exec_state:leverage_initialized",
                "leverage",
            ),
            ("exec01:binance-futures:exec_switch:alpha", "switch"),
            ("exec01:binance-futures:market_rules", "skip"),
            ("other:binance-futures:batch_exec:alpha", "skip"),
        ] {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .query_async::<()>(&mut conn)
                .await
                .unwrap();
        }
        let mut blobs = Vec::new();
        let envs = BTreeMap::from([(
            "exec01".to_string(),
            ExecEnvironment {
                directory: root.join("exec01"),
                markets: vec!["binance-futures".into()],
            },
        )]);
        let (entries, missing) = capture_redis(&url, &envs, &mut blobs).await.unwrap();
        assert!(entries
            .iter()
            .any(|x| x.key == "exec01:binance-futures:batch_exec:alpha" && x.dump_path.is_some()));
        for suffix in [
            "chase_exec_state:position_allocations",
            "batch_exec_state:leverage_initialized",
            "exec_switch:alpha",
        ] {
            assert!(entries
                .iter()
                .any(|x| x.key == format!("exec01:binance-futures:{suffix}")
                    && x.dump_path.is_some()));
        }
        assert!(!entries
            .iter()
            .any(|x| x.key.contains("market_rules") || x.key.starts_with("other:")));
        assert_eq!(
            missing,
            vec!["exec01:binance-futures:pre_trade_risk_params"]
        );
        let item = entries
            .iter()
            .find(|x| x.key == "exec01:binance-futures:batch_exec:alpha")
            .unwrap();
        let data = &blobs
            .iter()
            .find(|b| Some(&b.path) == item.dump_path.as_ref())
            .unwrap()
            .bytes;
        redis::cmd("RESTORE")
            .arg("restore-check")
            .arg(0)
            .arg(data)
            .query_async::<()>(&mut conn)
            .await
            .unwrap();
        let restored: String = redis::cmd("GET")
            .arg("restore-check")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(restored, "{\"configured\":true}");
        drop(conn);
        drop(server);
        fs::remove_dir_all(root).unwrap();
    }
}
