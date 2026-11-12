use super::model::Record;
use anyhow::{ensure, Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub struct Store {
    directory: PathBuf,
    _lock: File,
}

impl Store {
    pub fn open(directory: &Path) -> Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(directory.join(".lock"))?;
        // The process owns this lock until Store is dropped; a second instance cannot submit.
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "another transfer server owns the audit directory"
        );
        let store = Self {
            directory: directory.into(),
            _lock: lock,
        };
        // Corrupt evidence must stop startup rather than silently enable another submission.
        store.records()?;
        Ok(store)
    }

    fn path(&self, id: &str) -> Result<PathBuf> {
        ensure!(
            !id.is_empty()
                && id.len() <= 64
                && id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_')),
            "划转请求 ID 无效"
        );
        Ok(self.directory.join(format!("{id}.json")))
    }

    pub fn get(&self, id: &str) -> Result<Option<Record>> {
        let path = self.path(id)?;
        match fs::read(&path) {
            Ok(bytes) => {
                Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                    format!("read audit {}", path.display())
                })?))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn records(&self) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            if path.extension().and_then(|v| v.to_str()) == Some("json") {
                records.push(
                    serde_json::from_slice(&fs::read(&path)?)
                        .with_context(|| format!("read audit {}", path.display()))?,
                );
            }
        }
        Ok(records)
    }

    pub fn blocking(&self, source_id: &str) -> Result<Option<Record>> {
        Ok(self
            .records()?
            .into_iter()
            .find(|r| r.request.from_account_id == source_id && r.unresolved()))
    }

    pub fn reserve(&self, record: &Record) -> Result<()> {
        ensure!(
            self.blocking(&record.request.from_account_id)?.is_none(),
            "来源账户仍有未完成或结果未知的划转，请先查询原请求"
        );
        let path = self.path(&record.client_order_id)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    pub fn update(&self, record: &Record) -> Result<()> {
        let path = self.path(&record.client_order_id)?;
        ensure!(path.exists(), "missing original transfer audit");
        let temporary = self
            .directory
            .join(format!(".{}.tmp", record.client_order_id));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        fs::rename(temporary, path)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}
