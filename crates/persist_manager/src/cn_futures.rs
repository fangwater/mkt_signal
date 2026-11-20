//! Domestic store, aligned with crypto `uniform_orders` / `trade_updates`:
//! Exec publishes final records in their crypto store format and this side
//! only appends them under a unique 20-digit microsecond key. No projection,
//! validation or JSON lives in the store.
use crate::{RocksDbStore, RocksDbTuning};
use anyhow::{Context, Result};
use std::{path::Path, sync::Arc};

pub use persist_common::cn_futures::store_cf::{ALL as COLUMN_FAMILIES, ORDERS, TRADES};

/// Appends records; keys stay strictly increasing per column family even when
/// several records share a receipt microsecond.
pub struct NativeLog {
    store: Arc<RocksDbStore>,
    last: [u64; 2],
}

fn index(cf: &str) -> Result<usize> {
    COLUMN_FAMILIES
        .iter()
        .position(|name| *name == cf)
        .with_context(|| format!("unknown domestic column family {cf}"))
}

impl NativeLog {
    pub fn attach(store: Arc<RocksDbStore>) -> Result<Self> {
        let mut last = [0; 2];
        for (slot, cf) in COLUMN_FAMILIES.iter().enumerate() {
            if let Some((key, _)) = store.scan(cf, None, true, Some(1))?.into_iter().next() {
                last[slot] = std::str::from_utf8(&key)?
                    .parse()
                    .context("domestic record key is not a 20-digit timestamp")?;
            }
        }
        Ok(Self { store, last })
    }

    /// Key for a record received at `received_us` in `cf`.
    pub fn key(&mut self, cf: &str, received_us: u64) -> Result<Vec<u8>> {
        let slot = index(cf)?;
        let key = received_us.max(self.last[slot] + 1);
        self.last[slot] = key;
        Ok(format!("{key:020}").into_bytes())
    }

    /// One synced WAL write for an available microbatch.
    pub fn append(&self, writes: &[(String, Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.store.put_many_sync(writes)
    }
}

/// Domestic tuning: low descriptor use beside SDK select-based networking, and a
/// ten-minute WAL so a collector can finish a fixed audit cut after rotation.
pub fn tuning() -> RocksDbTuning {
    RocksDbTuning {
        block_cache_bytes: Some(8 * 1024 * 1024),
        max_open_files: Some(128),
        write_buffer_size_bytes: Some(1024 * 1024),
        db_write_buffer_size_bytes: Some(16 * 1024 * 1024),
        max_write_buffer_number: Some(2),
        wal_ttl_seconds: Some(600),
    }
}

/// Human-readable JSONL rendering of the binary store, for operators only.
/// Stop the trader, or open a RocksDB secondary and use `export_store`.
pub fn export(path: &Path, output: &Path) -> Result<()> {
    let store =
        RocksDbStore::open_read_only(path.to_str().context("non-UTF8 path")?, COLUMN_FAMILIES)?;
    export_store(&store, output)
}
pub fn export_store(store: &RocksDbStore, output: &Path) -> Result<()> {
    use std::io::Write;
    std::fs::create_dir_all(output)?;
    for cf in COLUMN_FAMILIES {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(output.join(format!("{cf}.jsonl")))?;
        // Stream bounded batches; history can grow far beyond memory.
        store.scan_range_batches(cf, b"", &[255], 512, |entries| {
            for (key, value) in entries {
                let line = cn_futures::persist::render_stored(cf, &value)
                    .with_context(|| format!("decode {cf} {}", String::from_utf8_lossy(&key)))?;
                serde_json::to_writer(
                    &mut file,
                    &serde_json::json!({
                        "key": String::from_utf8_lossy(&key),
                        "record": line,
                    }),
                )?;
                file.write_all(b"\n")?;
            }
            Ok(())
        })?;
        file.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) struct Temp(pub std::path::PathBuf);
    impl Temp {
        pub fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "cn-native-log-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
        }
        pub fn open(&self) -> Arc<RocksDbStore> {
            Arc::new(
                RocksDbStore::open_with_existing_cfs_and_tuning(
                    self.0.to_str().unwrap(),
                    &COLUMN_FAMILIES
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>(),
                    true,
                    &tuning(),
                )
                .unwrap(),
            )
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn keys_stay_unique_and_increasing_across_reopen() {
        let path = Temp::new();
        let store = path.open();
        let mut log = NativeLog::attach(store.clone()).unwrap();
        let a = log.key(ORDERS, 100).unwrap();
        let b = log.key(ORDERS, 100).unwrap();
        let c = log.key(TRADES, 100).unwrap();
        assert_eq!(a, b"00000000000000000100");
        assert_eq!(b, b"00000000000000000101");
        assert_eq!(c, b"00000000000000000100");
        log.append(&[(ORDERS.into(), b, b"x".to_vec())]).unwrap();
        drop(log);
        let mut log = NativeLog::attach(store).unwrap();
        assert_eq!(log.key(ORDERS, 50).unwrap(), b"00000000000000000102");
    }
}
