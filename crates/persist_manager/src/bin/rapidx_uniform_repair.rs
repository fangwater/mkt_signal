//! Explicit closed-store maintenance. Never run against a live owner and never
//! start trading, contact an exchange, or mutate a source during preview.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use persist_manager::{rapidx_uniform, RocksDbStore, RocksDbTuning};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    portfolio: String,
    #[arg(long)]
    exchange: String,
    /// New disposable directory outside all live stores and backup directories.
    #[arg(long)]
    scratch: PathBuf,
    /// Native checkpoint destination; required for --apply and interruption recovery.
    #[arg(long)]
    backup: Option<PathBuf>,
    #[arg(long)]
    apply: bool,
    /// Verified, source-scoped RapidX REST execution rows, one object per line.
    #[arg(long)]
    executions_jsonl: Option<PathBuf>,
    /// Require evidence-backed liquidity for every external fill before applying.
    #[arg(long)]
    require_known_liquidity: bool,
    /// Write unresolved external fill identities into a new private file.
    #[arg(long)]
    unknown_report: Option<PathBuf>,
}

fn audit_liquidity(store: &RocksDbStore, args: &Args) -> Result<()> {
    let mut unknown = args
        .unknown_report
        .as_ref()
        .map(|path| -> Result<_> {
            let path = canonical_destination(path)?;
            ensure!(
                !path.starts_with(&args.db.canonicalize()?),
                "unknown report overlaps source"
            );
            Ok(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)?,
            ))
        })
        .transpose()?;
    let coverage = rapidx_uniform::liquidity_coverage(store, |row| {
        if let Some(out) = &mut unknown {
            let r = &row.record;
            serde_json::to_writer(
                &mut *out,
                &serde_json::json!({
                    "symbol": std::str::from_utf8(&r.symbol)?,
                    "venue": r.venue,
                    "client_order_id": r.client_order_id.to_string(),
                    "update_ts_us": r.update_ts,
                    "create_ts_us": r.create_ts,
                    "price": r.price,
                    "amount": r.amount_update,
                    "side": r.side,
                }),
            )?;
            writeln!(out)?;
        }
        Ok(())
    })?;
    if let Some(out) = &mut unknown {
        out.flush()?;
    }
    println!("{}", serde_json::json!({"coverage": coverage}));
    ensure!(
        !args.require_known_liquidity || coverage.external_unknown == 0,
        "unresolved external uniform fill liquidity"
    );
    Ok(())
}

fn selected(path: &Path) -> Result<Vec<String>> {
    Ok(rocksdb::DB::list_cf(&rocksdb::Options::default(), path)?
        .into_iter()
        .filter(|n| {
            matches!(
                n.as_str(),
                "uniform_orders"
                    | "rapidx_executions"
                    | "rapidx_execution_roles"
                    | "rapidx_execution_observations"
                    | "trade_updates"
                    | "trade_updates_unmatched"
                    | "sync_meta"
                    | "sync_outbox"
            )
        })
        .collect())
}

fn read(path: &Path) -> Result<RocksDbStore> {
    let names = selected(path)?;
    RocksDbStore::open_read_only_with_tuning(
        path.to_str().context("non-UTF8 store path")?,
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
        &RocksDbTuning {
            max_open_files: Some(-1),
            ..Default::default()
        },
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    let db = args.db.canonicalize()?;
    let scratch = canonical_destination(&args.scratch)?;
    ensure!(
        !scratch.exists() && !scratch.starts_with(&db) && !db.starts_with(&scratch),
        "scratch must be new and outside the source"
    );
    let backup = args
        .backup
        .as_ref()
        .map(|path| canonical_destination(path))
        .transpose()?;
    if let Some(backup) = &backup {
        ensure!(
            !backup.starts_with(&db)
                && !db.starts_with(backup)
                && !scratch.starts_with(backup)
                && !backup.starts_with(&scratch),
            "backup and scratch must be independent of the live store"
        );
    }
    ensure!(!args.apply || backup.is_some(), "--apply requires --backup");
    // RocksDB uses POSIX record locks, not flock. Fail before opening a reader
    // if any live persist_manager owns this store. This never stops that owner.
    let guard = OpenOptions::new()
        .read(true)
        .write(true)
        .open(db.join("LOCK"))?;
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    ensure!(
        unsafe { libc::fcntl(guard.as_raw_fd(), libc::F_SETLK, &lock) } != -1,
        "source has a live owner; explicit closed-store maintenance is required"
    );
    let source = read(&db)?;
    let marker = rapidx_uniform::marker(&source)?;
    if marker.as_ref().is_some_and(|m| m.complete) {
        let m = marker.unwrap();
        ensure!(
            m.portfolio == args.portfolio && m.exchange == args.exchange,
            "normalized source scope mismatch"
        );
        println!(
            "{}",
            serde_json::json!({"already_complete":true,"verification":rapidx_uniform::verify(&source)?})
        );
        if args.require_known_liquidity || args.unknown_report.is_some() {
            audit_liquidity(&source, &args)?;
        }
        return Ok(());
    }
    let original = if marker.is_some() {
        read(
            backup
                .as_ref()
                .context("incomplete repair requires its original --backup")?,
        )?
    } else {
        read(&db)?
    };
    let names = persist_manager::required_column_families();
    let plan = RocksDbStore::open(
        scratch.to_str().context("non-UTF8 scratch path")?,
        &names,
        false,
    )?;
    let mut evidence = args
        .executions_jsonl
        .as_ref()
        .map(File::open)
        .transpose()?
        .map(BufReader::new);
    let report = rapidx_uniform::build_repair_plan(
        &original,
        &plan,
        &args.portfolio,
        &args.exchange,
        evidence.as_mut().map(|reader| reader as &mut dyn BufRead),
    )?;
    std::fs::write(
        scratch.join("REPAIR_PLAN.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::json!({"apply":args.apply,"plan":report}));
    if args.require_known_liquidity || args.unknown_report.is_some() {
        audit_liquidity(&plan, &args)?;
    }
    drop(original);
    drop(source);
    drop(guard);
    if args.apply {
        let store = RocksDbStore::open_with_existing_cfs_and_tuning(
            db.to_str().context("non-UTF8 source path")?,
            &names.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
            false,
            &RocksDbTuning::default(),
        )?;
        rapidx_uniform::apply_repair_plan(&store, &plan, &report, backup.as_ref().unwrap())?;
        println!(
            "{}",
            serde_json::json!({"completed":true,"verification":rapidx_uniform::verify(&store)?})
        );
    }
    Ok(())
}

fn canonical_destination(path: &Path) -> Result<PathBuf> {
    let path = std::path::absolute(path)?;
    if std::fs::symlink_metadata(&path).is_ok() {
        return Ok(path.canonicalize()?);
    }
    let parent = path
        .parent()
        .context("maintenance destination has no parent")?;
    Ok(parent.canonicalize()?.join(
        path.file_name()
            .context("maintenance destination has no name")?,
    ))
}
