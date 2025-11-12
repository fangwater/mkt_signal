use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rocksdb::{
    ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode, Options, WriteOptions, DB,
};

pub struct RocksDbStore {
    db: Arc<DB>,
    sync_writes: bool,
}

impl RocksDbStore {
    pub fn open(path: &str, cf_names: &[&str], sync_writes: bool) -> Result<Self> {
        let path_ref = Path::new(path);
        if let Some(parent) = path_ref.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create parent directory {:?}", parent))?;
            }
        }

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(DBCompressionType::Lz4);

        let mut descriptors = vec![ColumnFamilyDescriptor::new("default", Options::default())];
        let mut seen = HashSet::new();
        for name in cf_names {
            if seen.insert(*name) {
                descriptors.push(ColumnFamilyDescriptor::new(*name, Options::default()));
            }
        }

        let db = if descriptors.len() == 1 {
            DB::open(&opts, path_ref)?
        } else {
            DB::open_cf_descriptors(&opts, path_ref, descriptors)?
        };

        Ok(Self {
            db: Arc::new(db),
            sync_writes,
        })
    }

    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("column family {} not found", cf_name))?;
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(cf, key, value, &write_opts)
            .with_context(|| format!("failed to write to column family {}", cf_name))
    }

    pub fn get(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("column family {} not found", cf_name))?;
        self.db
            .get_cf(cf, key)
            .with_context(|| format!("failed to read from column family {}", cf_name))
            .map(|opt| opt.map(|v| v.to_vec()))
    }

    pub fn delete(&self, cf_name: &str, key: &[u8]) -> Result<()> {
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("column family {} not found", cf_name))?;
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(self.sync_writes);
        self.db
            .delete_cf_opt(cf, key, &write_opts)
            .with_context(|| format!("failed to delete from column family {}", cf_name))
    }

    pub fn scan(
        &self,
        cf_name: &str,
        start_key: Option<&[u8]>,
        reverse: bool,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("column family {} not found", cf_name))?;

        let mode = match (start_key, reverse) {
            (Some(key), true) => IteratorMode::From(key, Direction::Reverse),
            (Some(key), false) => IteratorMode::From(key, Direction::Forward),
            (None, true) => IteratorMode::End,
            (None, false) => IteratorMode::Start,
        };

        let iter = self.db.iterator_cf(cf, mode);
        let mut entries = Vec::new();
        for (idx, item) in iter.enumerate() {
            if idx >= limit {
                break;
            }
            let (key, value) = item?;
            entries.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
        }
        Ok(entries)
    }
}
