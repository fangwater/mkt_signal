//! One-time, scoped archive/reset for the operator-approved Spread01 replacement.
//! Dry-run by default. Run only against a stopped Manager when using --execute.
use anyhow::{ensure, Context, Result};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde_json::{json, Value};
use std::{collections::hash_map::DefaultHasher, hash::Hasher};
use std::{fs::OpenOptions, io::Write, path::Path};

const STRATEGY: &str = "cta-spread01-okx-bn-1min-pos";
// SQL proves this strategy was created on 2026-11-03. Include the prior UTC day.
const FROM_US: i64 = 1_793_577_600_000_000;

fn matches_strategy(raw: &[u8]) -> Result<bool> {
    if !raw
        .windows(STRATEGY.len())
        .any(|part| part == STRATEGY.as_bytes())
    {
        return Ok(false);
    }
    Ok(serde_json::from_slice::<Value>(raw)?
        .pointer("/strategy/strategy_name")
        .and_then(Value::as_str)
        == Some(STRATEGY))
}

fn run(path: &Path, archive: &Path, execute: bool) -> Result<Value> {
    ensure!(
        path.join("CURRENT").is_file(),
        "existing Manager database required"
    );
    let mut options = Options::default();
    options.set_max_open_files(4096);
    options.set_skip_stats_update_on_db_open(true);
    let families = DB::list_cf(&options, path)?;
    ensure!(
        families.iter().any(|f| f == "position_updates"),
        "not a Manager store"
    );
    let db = if execute {
        DB::open_cf(&options, path, &families)?
    } else {
        DB::open_cf_for_read_only(&options, path, ["default", "position_updates"], false)?
    };
    let cf = db
        .cf_handle("position_updates")
        .context("missing position_updates")?;
    eprintln!("opened Manager archive; scan from {FROM_US}");
    let mut removed = Vec::new();
    let mut retained = DefaultHasher::new();
    let mut retained_count = 0u64;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(archive)?;
    let start = FROM_US.to_be_bytes();
    for item in db.iterator_cf(cf, IteratorMode::From(&start, Direction::Forward)) {
        let (key, raw) = item?;
        if matches_strategy(&raw)? {
            let value: Value = serde_json::from_slice(&raw)?;
            let key_hex = key.iter().map(|b| format!("{b:02x}")).collect::<String>();
            serde_json::to_writer(
                &mut output,
                &json!({"cf":"position_updates","key_hex":key_hex,"value":value}),
            )?;
            output.write_all(b"\n")?;
            removed.push(key.to_vec());
        } else {
            retained.write(&key);
            retained.write(&raw);
            retained_count += 1;
        }
    }
    let current_key = format!("position_current:{STRATEGY}");
    let current = db.get(&current_key)?;
    if let Some(raw) = &current {
        serde_json::to_writer(
            &mut output,
            &json!({"cf":"default","key":current_key,"value":serde_json::from_slice::<Value>(raw)?}),
        )?;
        output.write_all(b"\n")?;
    }
    output.sync_all()?;
    if let Some(parent) = archive.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    let digest = format!("{:x}", retained.finish());
    if execute {
        let mut batch = WriteBatch::default();
        for key in &removed {
            batch.delete_cf(cf, key);
        }
        batch.delete(current_key.as_bytes());
        let mut write = WriteOptions::default();
        write.set_sync(true);
        db.write_opt(batch, &write)?;
        let mut after = DefaultHasher::new();
        let mut after_count = 0;
        for item in db.iterator_cf(cf, IteratorMode::From(&start, Direction::Forward)) {
            let (key, raw) = item?;
            ensure!(!matches_strategy(&raw)?, "retired target remains");
            after.write(&key);
            after.write(&raw);
            after_count += 1;
        }
        ensure!(
            after_count == retained_count && format!("{:x}", after.finish()) == digest,
            "unrelated archive changed"
        );
        ensure!(
            db.get(&current_key)?.is_none(),
            "retired current target remains"
        );
    }
    Ok(
        json!({"strategy":STRATEGY,"execute":execute,"archived_targets":removed.len(),"archived_current":current.is_some(),"retained_targets":retained_count,"retained_fingerprint":digest}),
    )
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    ensure!(
        (3..=4).contains(&args.len()) && (args.len() == 3 || args[3] == "--execute"),
        "usage: retire_spread01_history DB ARCHIVE.jsonl [--execute]"
    );
    println!(
        "{}",
        run(Path::new(&args[1]), Path::new(&args[2]), args.len() == 4)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scoped_reset_archives_before_deleting_and_preserves_other_targets() {
        let root = std::env::temp_dir().join(format!(
            "spread01-retirement-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("db");
        std::fs::create_dir_all(&root).unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        {
            let db = DB::open_cf(&options, &path, ["default", "position_updates"]).unwrap();
            let cf = db.cf_handle("position_updates").unwrap();
            for (offset, name) in [(0, STRATEGY), (1, "cta-prep01-bn-1h-pos")] {
                db.put_cf(
                    cf,
                    (FROM_US + offset).to_be_bytes(),
                    serde_json::to_vec(&json!({"strategy":{"strategy_name":name}})).unwrap(),
                )
                .unwrap();
            }
            db.put(format!("position_current:{STRATEGY}"), b"[1,{}]")
                .unwrap();
            db.put("unrelated", b"preserved").unwrap();
        }
        let preview = run(&path, &root.join("preview.jsonl"), false).unwrap();
        assert_eq!(preview["archived_targets"], 1);
        let result = run(&path, &root.join("archive.jsonl"), true).unwrap();
        assert_eq!(result["retained_targets"], 1);
        let db = DB::open_cf_for_read_only(&options, &path, ["default", "position_updates"], false)
            .unwrap();
        assert_eq!(db.get("unrelated").unwrap().unwrap(), b"preserved");
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
