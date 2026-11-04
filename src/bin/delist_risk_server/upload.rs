//! Upload FR archives using a private rclone configuration, then read back.

use super::backup;
use anyhow::{bail, Context, Result};
use mkt_signal::common::delist_accounts::AccountSpec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

#[derive(Deserialize)]
struct RemoteObject {
    #[serde(rename = "Path")]
    path: String,
    #[serde(rename = "ModTime")]
    modified: String,
}

#[derive(Clone)]
pub struct UploadConfig {
    pub target: String,
    pub config_path: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    destination: String,
    sha256: String,
    verified_utc: String,
}

impl UploadConfig {
    pub fn validate(&self) -> Result<()> {
        let (remote, path) = self
            .target
            .split_once(':')
            .context("COS target must be remote:bucket/prefix")?;
        if remote.is_empty()
            || remote.starts_with('-')
            || !remote
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            bail!("invalid rclone remote name");
        }
        if path.is_empty()
            || path.starts_with('/')
            || path.split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || !part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            })
        {
            bail!("invalid COS bucket or prefix");
        }
        let metadata =
            fs::symlink_metadata(&self.config_path).context("inspect private rclone config")?;
        if !metadata.file_type().is_file()
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            bail!("rclone config must be a private regular file owned by the service user");
        }
        Ok(())
    }

    fn identity(&self) -> Result<String> {
        self.validate()?;
        let bytes = fs::read(&self.config_path).context("read private rclone config")?;
        let mut hasher = Sha256::new();
        hasher.update(self.target.as_bytes());
        hasher.update([0]);
        hasher.update(bytes);
        Ok(format!("{:x}", hasher.finalize()))
    }
}

pub async fn sync_archives(
    config: &UploadConfig,
    root: &Path,
    site: &str,
    account: &AccountSpec,
    keep: usize,
) -> Result<usize> {
    if keep == 0 {
        bail!("invalid backup retention");
    }
    let identity = config.identity()?;
    let archives = backup::archive_paths(root, site, account)?;
    if archives.is_empty() {
        bail!("no local FR backups available for COS upload");
    }
    let mut uploaded = 0usize;
    let mut confirmed = Vec::new();
    for path in &archives {
        let sha = hash_file(path).await?;
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("invalid backup filename")?;
        let date = filename
            .strip_suffix(".tar.gz")
            .context("invalid backup suffix")?;
        let destination = format!(
            "{}/{}/{}/{}-{}.tar.gz",
            config.target, site, account.slug, date, sha
        );
        let receipt_path =
            path.with_file_name(format!(".{filename}.{}.uploaded.json", &identity[..16]));
        let verified = read_receipt(&receipt_path)
            .is_some_and(|r| r.destination == destination && r.sha256 == sha);
        if !verified {
            backup::validate_for_account(path, site, account)
                .context("validate local archive before COS upload")?;
            copy_and_verify(config, path, &destination, &sha).await?;
            let receipt = Receipt {
                destination,
                sha256: sha,
                verified_utc: chrono::Utc::now().to_rfc3339(),
            };
            write_receipt(&receipt_path, &receipt)?;
            uploaded += 1;
        }
        confirmed.push((path.clone(), receipt_path));
    }
    // Retention never deletes an archive until every listed archive has a
    // receipt for this destination and its local content hash.
    let mut daily = Vec::new();
    let mut manual = Vec::new();
    for item in confirmed {
        let name = item
            .0
            .file_name()
            .and_then(|s| s.to_str())
            .context("invalid backup filename")?;
        let stem = name
            .strip_suffix(".tar.gz")
            .context("invalid backup suffix")?;
        if stem.len() == 10 {
            daily.push(item);
        } else {
            manual.push(item);
        }
    }
    for group in [daily, manual] {
        let remove_count = group.len().saturating_sub(keep);
        for (path, receipt) in group.into_iter().take(remove_count) {
            fs::remove_file(&path).context("remove uploaded expired backup")?;
            fs::remove_file(&receipt).context("remove expired upload receipt")?;
        }
    }
    Ok(uploaded)
}

fn read_receipt(path: &Path) -> Option<Receipt> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file()
        || metadata.len() > 4096
        || metadata.permissions().mode() & 0o077 != 0
    {
        return None;
    }
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn write_receipt(path: &Path, receipt: &Receipt) -> Result<()> {
    let temp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let result = (|| {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        f.write_all(&serde_json::to_vec(receipt)?)?;
        f.sync_all()?;
        fs::rename(&temp, path)?;
        fs::File::open(path.parent().context("receipt directory missing")?)?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(temp);
    result
}

async fn hash_file(path: &Path) -> Result<String> {
    let mut f = tokio::fs::File::open(path)
        .await
        .context("open local FR backup")?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).await.context("read local FR backup")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

async fn copy_and_verify(
    config: &UploadConfig,
    path: &Path,
    destination: &str,
    expected_sha: &str,
) -> Result<()> {
    let mut copy = Command::new("rclone");
    copy.arg("--config")
        .arg(&config.config_path)
        .args([
            "copyto",
            "--immutable",
            "--checksum",
            "--no-traverse",
            "--s3-no-check-bucket",
            "--s3-upload-cutoff",
            "256Mi",
            "--retries",
            "1",
            "--low-level-retries",
            "1",
            "--timeout",
            "60s",
        ])
        .arg(path)
        .arg(destination)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let status = tokio::time::timeout(Duration::from_secs(180), copy.status())
        .await
        .context("COS upload timed out")?
        .context("start rclone upload")?;
    if !status.success() {
        bail!("COS upload failed ({status}); check rclone configuration and bucket permissions");
    }
    verify_object(config, path, destination, expected_sha).await
}

async fn verify_object(
    config: &UploadConfig,
    path: &Path,
    destination: &str,
    expected_sha: &str,
) -> Result<()> {
    let (remote, bucket_and_key) = destination
        .split_once(':')
        .context("invalid COS object destination")?;
    let (bucket, object_key) = bucket_and_key
        .split_once('/')
        .context("invalid COS object key")?;
    let bucket_root = format!("{remote}:{bucket}");
    let mut child = Command::new("rclone")
        .arg("--config")
        .arg(&config.config_path)
        .args([
            "cat",
            "--files-from-raw",
            "-",
            "--no-traverse",
            "--s3-no-check-bucket",
            "--retries",
            "1",
            "--low-level-retries",
            "1",
            "--timeout",
            "60s",
        ])
        .arg(bucket_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start COS read-back")?;
    let mut stdin = child
        .stdin
        .take()
        .context("COS read-back input unavailable")?;
    stdin
        .write_all(object_key.as_bytes())
        .await
        .context("select COS object")?;
    stdin.write_all(b"\n").await.context("select COS object")?;
    stdin
        .shutdown()
        .await
        .context("finish COS object selection")?;
    drop(stdin);
    let mut stdout = child
        .stdout
        .take()
        .context("COS read-back stream unavailable")?;
    let expected_size = fs::metadata(path)?.len();
    tokio::time::timeout(Duration::from_secs(180), async {
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = stdout
                .read(&mut buf)
                .await
                .context("read uploaded COS object")?;
            if n == 0 {
                break;
            }
            size = size
                .checked_add(n as u64)
                .context("COS object size overflow")?;
            if size > expected_size {
                bail!("COS read-back size mismatch");
            }
            hasher.update(&buf[..n]);
        }
        let status = child.wait().await.context("wait for COS read-back")?;
        if !status.success() {
            bail!("COS read-back failed ({status})");
        }
        if size != expected_size || format!("{:x}", hasher.finalize()) != expected_sha {
            bail!("COS read-back checksum mismatch");
        }
        if hash_file(path).await? != expected_sha {
            bail!("local archive changed during COS upload");
        }
        Ok(())
    })
    .await
    .context("COS read-back timed out")?
}

/// Keep only the most recent `keep` archive objects for this exact site/env.
/// A current-day local receipt and a fresh read-back are required before any
/// remote deletion. Other object names and future-dated archives are ignored.
pub async fn prune_remote(
    config: &UploadConfig,
    root: &Path,
    site: &str,
    account: &AccountSpec,
    keep: usize,
) -> Result<usize> {
    if keep == 0 {
        bail!("invalid COS retention");
    }
    let identity = config.identity()?;
    let now = chrono::Utc::now().naive_utc();
    let today = now.date();
    let archives = backup::archive_paths(root, site, account)?;
    let local = archives
        .iter()
        .rev()
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".tar.gz"))
                .and_then(backup::parse_archive_stem)
                .is_some_and(|stamp| stamp.date() == today)
        })
        .context("current-day local archive missing")?;
    let sha = hash_file(local).await?;
    let filename = local
        .file_name()
        .and_then(|n| n.to_str())
        .context("invalid current-day archive filename")?;
    let stem = filename
        .strip_suffix(".tar.gz")
        .context("invalid current-day archive suffix")?;
    let destination = format!(
        "{}/{}/{}/{}-{}.tar.gz",
        config.target, site, account.slug, stem, sha
    );
    let receipt_path =
        local.with_file_name(format!(".{filename}.{}.uploaded.json", &identity[..16]));
    let receipt =
        read_receipt(&receipt_path).context("current-day verified COS receipt missing")?;
    if receipt.destination != destination || receipt.sha256 != sha {
        bail!("current-day COS receipt mismatch");
    }
    let (remote, bucket_and_prefix) = config
        .target
        .split_once(':')
        .context("invalid COS target")?;
    let (bucket, prefix) = bucket_and_prefix
        .split_once('/')
        .context("COS prefix required for retention")?;
    let bucket_root = format!("{remote}:{bucket}");
    let dir = format!("{prefix}/{site}/{}/", account.slug);
    let objects = list_bucket(config, &bucket_root).await?;
    let mut candidates = Vec::new();
    for object in objects {
        let Some(name) = object.path.strip_prefix(&dir) else {
            continue;
        };
        let Some((stem, digest)) = name
            .strip_suffix(".tar.gz")
            .and_then(|n| n.rsplit_once('-'))
        else {
            continue;
        };
        let Some(stamp) = backup::parse_archive_stem(stem) else {
            continue;
        };
        if !name.is_ascii()
            || digest.len() != 64
            || stamp > now
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            continue;
        }
        let modified = chrono::DateTime::parse_from_rfc3339(&object.modified)
            .context("invalid COS archive modification time")?;
        let manual = stem.len() != 10;
        candidates.push((stamp, modified, object.path, manual));
    }
    candidates.sort_unstable_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
    });
    let mut daily_count = 0usize;
    let mut manual_count = 0usize;
    let mut retained = Vec::new();
    let mut expired = Vec::new();
    for candidate in candidates {
        let count = if candidate.3 {
            &mut manual_count
        } else {
            &mut daily_count
        };
        if *count < keep {
            *count += 1;
            retained.push(candidate);
        } else {
            expired.push(candidate);
        }
    }
    if expired.is_empty() {
        return Ok(0);
    }
    let relative_current = destination
        .strip_prefix(&format!("{bucket_root}/"))
        .context("COS destination outside configured bucket")?;
    if !retained
        .iter()
        .any(|(_, _, path, _)| path == relative_current)
    {
        bail!("current verified COS backup is not among retained archives");
    }
    verify_object(config, local, &destination, &sha)
        .await
        .context("verify current COS backup before retention")?;
    let mut deleted = 0;
    for (_, _, path, _) in expired {
        let full = format!("{bucket_root}/{path}");
        let mut command = Command::new("rclone");
        command
            .arg("--config")
            .arg(&config.config_path)
            .args([
                "deletefile",
                "--s3-no-check-bucket",
                "--retries",
                "1",
                "--low-level-retries",
                "1",
                "--timeout",
                "60s",
            ])
            .arg(full)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let status = tokio::time::timeout(Duration::from_secs(90), command.status())
            .await
            .context("COS retention deletion timed out")?
            .context("start COS retention deletion")?;
        if !status.success() {
            bail!("COS retention deletion failed ({status}); check DeleteObject permission");
        }
        deleted += 1;
    }
    Ok(deleted)
}

async fn list_bucket(config: &UploadConfig, root: &str) -> Result<Vec<RemoteObject>> {
    let mut child = Command::new("rclone")
        .arg("--config")
        .arg(&config.config_path)
        .args([
            "lsjson",
            "--recursive",
            "--fast-list",
            "--files-only",
            "--s3-no-check-bucket",
            "--retries",
            "1",
            "--low-level-retries",
            "1",
            "--timeout",
            "60s",
        ])
        .arg(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start COS listing")?;
    let stdout = child
        .stdout
        .take()
        .context("COS listing stream unavailable")?;
    tokio::time::timeout(Duration::from_secs(120), async {
        let mut data = Vec::new();
        stdout
            .take(16 * 1024 * 1024 + 1)
            .read_to_end(&mut data)
            .await
            .context("read COS listing")?;
        if data.len() > 16 * 1024 * 1024 {
            bail!("COS listing exceeds retention limit");
        }
        let status = child.wait().await.context("wait for COS listing")?;
        if !status.success() {
            bail!("COS listing failed ({status}); check GetBucket permission");
        }
        serde_json::from_slice(&data).context("decode COS listing")
    })
    .await
    .context("COS listing timed out")?
}
