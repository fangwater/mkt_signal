use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode,
    Options, WriteOptions, DB,
};

#[derive(Debug, Clone, Default)]
pub struct RocksDbTuning {
    pub block_cache_bytes: Option<usize>,
    pub write_buffer_size_bytes: Option<usize>,
    pub db_write_buffer_size_bytes: Option<usize>,
    pub max_write_buffer_number: Option<i32>,
}

impl RocksDbTuning {
    fn apply_db_options(&self, opts: &mut Options) {
        if let Some(size) = self.db_write_buffer_size_bytes {
            opts.set_db_write_buffer_size(size);
        }
    }

    fn apply_cf_options(&self, opts: &mut Options) {
        if let Some(n) = self.max_write_buffer_number {
            opts.set_max_write_buffer_number(n);
        }
        if let Some(size) = self.write_buffer_size_bytes {
            opts.set_write_buffer_size(size);
        }

        if let Some(cache_bytes) = self.block_cache_bytes {
            let mut block_opts = BlockBasedOptions::default();
            if cache_bytes == 0 {
                block_opts.disable_cache();
            } else {
                let cache = Cache::new_lru_cache(cache_bytes);
                block_opts.set_block_cache(&cache);
            }
            opts.set_block_based_table_factory(&block_opts);
        }
    }
}

pub struct RocksDbStore {
    db: Arc<DB>,
    sync_writes: bool,
    read_only: bool,
}

impl RocksDbStore {
    #[allow(dead_code)]
    pub fn open(path: &str, cf_names: &[&str], sync_writes: bool) -> Result<Self> {
        Self::open_with_tuning(path, cf_names, sync_writes, &RocksDbTuning::default())
    }

    pub fn open_with_tuning(
        path: &str,
        cf_names: &[&str],
        sync_writes: bool,
        tuning: &RocksDbTuning,
    ) -> Result<Self> {
        let path_ref = Path::new(path);
        if let Some(parent) = path_ref.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create parent directory {:?}", parent))?;
            }
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compression_type(DBCompressionType::Lz4);
        tuning.apply_db_options(&mut db_opts);

        let mut cf_opts = Options::default();
        tuning.apply_cf_options(&mut cf_opts);

        let mut descriptors = vec![ColumnFamilyDescriptor::new("default", cf_opts.clone())];
        let mut seen = HashSet::new();
        for name in cf_names {
            if seen.insert(*name) {
                descriptors.push(ColumnFamilyDescriptor::new(*name, cf_opts.clone()));
            }
        }

        let db = if descriptors.len() == 1 {
            DB::open(&db_opts, path_ref)?
        } else {
            DB::open_cf_descriptors(&db_opts, path_ref, descriptors)?
        };

        Ok(Self {
            db: Arc::new(db),
            sync_writes,
            read_only: false,
        })
    }

    pub fn open_read_only(path: &str, cf_names: &[&str]) -> Result<Self> {
        Self::open_read_only_with_tuning(path, cf_names, &RocksDbTuning::default())
    }

    pub fn open_read_only_with_tuning(
        path: &str,
        cf_names: &[&str],
        tuning: &RocksDbTuning,
    ) -> Result<Self> {
        let path_ref = Path::new(path);
        if !path_ref.exists() {
            return Err(anyhow!("rocksdb path does not exist: {}", path));
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(false);
        db_opts.create_missing_column_families(false);
        db_opts.set_compression_type(DBCompressionType::Lz4);
        tuning.apply_db_options(&mut db_opts);

        let mut cf_opts = Options::default();
        tuning.apply_cf_options(&mut cf_opts);

        let mut cf_list = vec![("default", cf_opts.clone())];
        let mut seen = HashSet::new();
        seen.insert("default");
        for name in cf_names {
            if seen.insert(*name) {
                cf_list.push((*name, cf_opts.clone()));
            }
        }

        let db = DB::open_cf_with_opts_for_read_only(&db_opts, path_ref, cf_list, false)?;

        Ok(Self {
            db: Arc::new(db),
            sync_writes: false,
            read_only: true,
        })
    }

    pub fn put(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(anyhow!("rocksdb store is read-only"));
        }
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
        if self.read_only {
            return Err(anyhow!("rocksdb store is read-only"));
        }
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
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
            if let Some(max) = limit {
                if idx >= max {
                    break;
                }
            }
            let (key, value) = item?;
            entries.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
        }
        Ok(entries)
    }

    pub fn scan_range(
        &self,
        cf_name: &str,
        start_key: &[u8],
        end_key_exclusive: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key >= end_key_exclusive {
            return Ok(Vec::new());
        }

        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("column family {} not found", cf_name))?;

        let iter = self
            .db
            .iterator_cf(cf, IteratorMode::From(start_key, Direction::Forward));
        let mut entries = Vec::new();

        for item in iter {
            let (key, value) = item?;
            if key.as_ref() >= end_key_exclusive {
                break;
            }

            entries.push((key.as_ref().to_vec(), value.as_ref().to_vec()));
            if let Some(max) = limit {
                if entries.len() >= max {
                    break;
                }
            }
        }

        Ok(entries)
    }
}
