//! Durable, read-only-to-parameters change journal shared with Python config pages.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

pub const MAX_RECORD: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct Busy;
impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("parameter journal is busy; retry")
    }
}
impl std::error::Error for Busy {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub family: String,
    pub environment: String,
    pub source: String,
    pub actor: Option<String>,
    pub source_ip: Option<String>,
    pub session_id: Option<String>,
    pub request_path: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Writer {
    pub pid: u32,
    pub start_ticks: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Change {
    pub before: Value,
    pub after: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub time_us: i64,
    pub storage: String,
    pub writer: Writer,
    #[serde(flatten)]
    pub metadata: Metadata,
    pub objects: BTreeMap<String, Change>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub event: Event,
    pub pending: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Batch {
    pub watermark: u64,
    pub complete: bool,
    pub records: Vec<Record>,
}

pub fn default_root() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("PARAMETER_AUDIT_DIR").filter(|s| !s.is_empty()) {
        return Ok(root.into());
    }
    Ok(
        PathBuf::from(std::env::var_os("HOME").context("HOME is unavailable")?)
            .join(".local/state/mkt_signal/parameter_changes"),
    )
}

pub struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.0.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
fn lock(root: &Path, exclusive: bool) -> Result<Lock> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(root)?;
    let meta = fs::symlink_metadata(root)?;
    ensure!(
        meta.is_dir()
            && !meta.file_type().is_symlink()
            && meta.uid() == unsafe { libc::geteuid() }
            && meta.permissions().mode() & 0o077 == 0,
        "parameter journal must be private and owned by service user"
    );
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(root.join(".lock"))?;
    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(Busy.into());
        }
        return Err(error).context("lock parameter journal");
    }
    Ok(Lock(file))
}
pub fn atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() as u64 <= MAX_RECORD,
        "parameter record exceeds size limit"
    );
    let parent = path.parent().context("journal parent missing")?;
    let temp = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let name: String = key
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric())
                        .flat_map(char::to_lowercase)
                        .collect();
                    let sensitive = [
                        "password",
                        "secret",
                        "credential",
                        "apikey",
                        "privatekey",
                        "webhook",
                    ]
                    .iter()
                    .any(|part| name.contains(part))
                        || matches!(
                            name.as_str(),
                            "token" | "accesstoken" | "authorization" | "cookie"
                        );
                    (
                        key.clone(),
                        if sensitive {
                            Value::String("[redacted]".into())
                        } else {
                            redact(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        _ => value.clone(),
    }
}

pub fn equivalent(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            a == b || a.as_f64().zip(b.as_f64()).is_some_and(|(a, b)| a == b)
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, value)| b.get(key).is_some_and(|other| equivalent(value, other)))
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equivalent(a, b))
        }
        _ => a == b,
    }
}

pub fn apply<T>(
    root: &Path,
    metadata: Metadata,
    storage: &str,
    read: impl Fn() -> Result<BTreeMap<String, Value>>,
    write: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let _guard = lock(root, true)?;
    let before = read()?
        .into_iter()
        .map(|(k, v)| (k, redact(&v)))
        .collect::<BTreeMap<_, _>>();
    let sequence = root.join(".sequence");
    let id = if sequence.exists() {
        serde_json::from_slice::<u64>(&fs::read(&sequence)?)?
    } else {
        0
    };
    let id = id.checked_add(1).context("parameter sequence overflow")?;
    atomic(&sequence, &id)?;
    let stat = fs::read_to_string("/proc/self/stat")?;
    let start_ticks = stat
        .rsplit_once(')')
        .context("invalid process stat")?
        .1
        .split_whitespace()
        .nth(19)
        .context("process start time missing")?
        .into();
    let mut event = Event {
        id,
        time_us: chrono::Utc::now().timestamp_micros(),
        storage: storage.into(),
        writer: Writer {
            pid: std::process::id(),
            start_ticks,
        },
        metadata,
        objects: before
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    Change {
                        before: value.clone(),
                        after: value.clone(),
                    },
                )
            })
            .collect(),
    };
    let pending = root.join(format!("{id:020}.pending"));
    atomic(&pending, &event)?;
    let result = write();
    let after = read()
        .context("parameter save may have completed; audit reconciliation is pending")?
        .into_iter()
        .map(|(k, v)| (k, redact(&v)))
        .collect::<BTreeMap<_, _>>();
    event.objects.clear();
    for key in before.keys().chain(after.keys()) {
        let a = before.get(key).cloned().unwrap_or(Value::Null);
        let b = after.get(key).cloned().unwrap_or(Value::Null);
        if !equivalent(&a, &b) {
            event.objects.insert(
                key.clone(),
                Change {
                    before: a,
                    after: b,
                },
            );
        }
    }
    if result.is_err() {
        event.metadata.source = "failed_config_request".into();
    }
    atomic(&root.join(format!("{id:020}.json")), &event)
        .context("parameter save may have completed; audit acknowledgement is pending")?;
    fs::remove_file(pending)?;
    result
}

pub fn read_batch(root: &Path, cursor: u64) -> Result<Batch> {
    let _guard = lock(root, false)?;
    let watermark = if root.join(".sequence").exists() {
        serde_json::from_slice(&fs::read(root.join(".sequence"))?)?
    } else {
        0
    };
    let mut paths = BTreeMap::new();
    for path in fs::read_dir(root)? {
        let path = path?;
        let name = path.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some((number, extension)) = name.rsplit_once('.') else {
            continue;
        };
        if number.len() != 20 || !matches!(extension, "json" | "pending") {
            continue;
        }
        let Ok(id) = number.parse::<u64>() else {
            continue;
        };
        if id <= cursor {
            continue;
        }
        if extension == "json" || !paths.contains_key(&id) {
            paths.insert(id, (path.path(), extension == "pending"));
        }
    }
    let total = paths.len();
    let mut records = Vec::new();
    let mut bytes = 0;
    for (id, (path, pending)) in paths {
        let meta = fs::symlink_metadata(&path)?;
        ensure!(
            meta.is_file()
                && !meta.file_type().is_symlink()
                && meta.len() <= MAX_RECORD
                && meta.permissions().mode() & 0o077 == 0,
            "invalid private parameter record"
        );
        if records.len() >= 128 || bytes + meta.len() > 32 * 1024 * 1024 {
            break;
        }
        bytes += meta.len();
        let event: Event = serde_json::from_slice(&fs::read(path)?)?;
        ensure!(
            event.id == id
                && event.objects.len() <= 256
                && matches!(
                    event.metadata.family.as_str(),
                    "cta_prep" | "cta_spread" | "funding_rate" | "market_making"
                )
                && matches!(event.storage.as_str(), "redis" | "cta"),
            "invalid parameter event identity"
        );
        records.push(Record { event, pending });
    }
    Ok(Batch {
        watermark,
        complete: records.len() == total,
        records,
    })
}

pub fn normalise_redis(kind: &str, value: Value) -> Value {
    fn compound(value: Value) -> Value {
        if let Value::String(text) = &value {
            if text.trim_start().starts_with(['{', '[']) {
                if let Ok(decoded) = serde_json::from_str(text) {
                    return decoded;
                }
            }
        }
        value
    }
    let value = if kind == "hash" {
        value
            .as_object()
            .map(|map| {
                Value::Object(
                    map.iter()
                        .map(|(k, v)| (k.clone(), compound(v.clone())))
                        .collect(),
                )
            })
            .unwrap_or(value)
    } else {
        compound(value)
    };
    serde_json::json!({"type":kind,"value":value})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_journal_python_reader_noops_and_crash_pending() {
        let root = std::env::temp_dir().join(format!("parameter-journal-{}", uuid::Uuid::new_v4()));
        let value =
            std::cell::RefCell::new(serde_json::json!({"threshold":0.7,"api_key":"secret-value"}));
        let metadata = || Metadata {
            family: "cta_prep".into(),
            environment: "cta-prep-binance-futures".into(),
            source: "config_page".into(),
            actor: None,
            source_ip: None,
            session_id: None,
            request_path: None,
        };
        let read = || {
            Ok(BTreeMap::from([(
                "cta-prep-binance-futures/one.json".into(),
                value.borrow().clone(),
            )]))
        };
        apply(&root, metadata(), "cta", read, || {
            value.borrow_mut()["threshold"] = serde_json::json!(0.3);
            Ok(())
        })
        .unwrap();
        apply(&root, metadata(), "cta", read, || Ok(())).unwrap();
        let batch = read_batch(&root, 0).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert!(batch.records[1].event.objects.is_empty());
        assert!(!serde_json::to_string(&batch)
            .unwrap()
            .contains("secret-value"));
        let output = std::process::Command::new("python3")
            .args(["scripts/parameter_audit.py", "--read", "0"])
            .env("PARAMETER_AUDIT_DIR", &root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let python: Batch = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(python.watermark, 2);
        assert_eq!(python.records.len(), 2);
        let pending = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            apply::<()>(&root, metadata(), "cta", read, || {
                panic!("simulated interrupted writer")
            })
        }));
        assert!(pending.is_err());
        assert!(read_batch(&root, 2).unwrap().records[0].pending);
        assert_eq!(value.borrow()["threshold"], serde_json::json!(0.3));
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn compound_normalisation_and_redaction_preserve_trading_values() {
        let a = normalise_redis(
            "hash",
            serde_json::json!({"scalar":"1","map":"{\"ETHUSDT\":1}","api_key":"secret"}),
        );
        assert_eq!(a["value"]["scalar"], "1");
        assert_eq!(a["value"]["map"]["ETHUSDT"], 1);
        assert_eq!(redact(&a)["value"]["api_key"], "[redacted]");
        assert!(equivalent(&serde_json::json!(1), &serde_json::json!(1.0)));
    }
}
