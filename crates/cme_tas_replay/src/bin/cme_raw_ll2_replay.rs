//! Fused Raw LL2 streaming extraction; independent output DB, no source writes.
use anyhow::{bail, Context, Result};
use clap::Parser;
use cme_tas_replay::ll2_1min::{decode_ll2_minute, encode_ll2_minute};
use cme_tas_replay::product::{parse_product, period_year};
use cme_tas_replay::raw_ll2::{raw_cf, validate_quality, Extractor, RawScanner, RawSnapshot};
use cme_tas_replay::raw_ll2_io::{client, expected_counts, input, package, Config, Inventory};
use cme_tas_replay::raw_ll2_stage;
use cme_tas_replay::{decode_ric, period_meta_key, CF_REPLAY_META};
use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/cme_raw_ll2_2026_15s.toml")]
    config: PathBuf,
    #[arg(long)]
    period: Option<String>,
    #[arg(long)]
    rocksdb_dir: Option<PathBuf>,
    #[arg(long)]
    frequency: Option<u32>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    preflight: bool,
    #[arg(long)]
    verify: bool,
    /// Finalize a completed write audit after an interrupted optional verification.
    #[arg(long)]
    finalize_written: bool,
    #[arg(long)]
    max_messages: Option<u64>,
    /// Small complete CSV/gzip fixtures only; watermark is sample, never done.
    #[arg(long, num_args = 1..)]
    sample_input: Vec<PathBuf>,
    #[arg(long)]
    audit: Option<PathBuf>,
    #[arg(long)]
    dump_jsonl: Option<PathBuf>,
}
fn raw_cf_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_write_buffer_size(64 * 1024 * 1024);
    options.set_max_write_buffer_number(4);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_level_zero_file_num_compaction_trigger(8);
    options.set_level_zero_slowdown_writes_trigger(32);
    options.set_level_zero_stop_writes_trigger(48);
    options
}
fn open_output(path: &Path) -> Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    // Match the existing full-product LL2 store; cap shared write buffers.
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_max_open_files(65_536);
    options.increase_parallelism(32);
    options.set_max_background_jobs(32);
    options.set_db_write_buffer_size(8 * 1024 * 1024 * 1024);
    options.set_max_subcompactions(4);
    let existing = path.join("CURRENT").exists();
    let names = if existing {
        DB::list_cf(&options, path)?
    } else {
        vec!["default".into(), CF_REPLAY_META.into()]
    };
    let db = DB::open_cf_descriptors(
        &options,
        path,
        names
            .into_iter()
            .map(|n| ColumnFamilyDescriptor::new(n, raw_cf_options())),
    )?;
    if existing {
        if db.get(b"db_kind")?.as_deref() != Some(b"cme_raw_ll2_v1") {
            bail!("output is not a Raw LL2 DB: {}", path.display());
        }
    } else {
        db.put(b"db_kind", b"cme_raw_ll2_v1")?;
    }
    Ok(db)
}
fn verify(db: &DB, period: &str, frequency: u32, allow_writing: bool) -> Result<Value> {
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    let status = db
        .get_cf(&meta, period_meta_key(period)?)?
        .context("period has no status")?;
    if status != b"done" && status != b"sample" && !(allow_writing && status == b"writing") {
        bail!("Raw period is not done/sample");
    }
    let key = format!("audit:{period}");
    let audit: Value =
        serde_json::from_slice(&db.get_cf(&meta, key.as_bytes())?.context("missing audit")?)?;
    if audit["frequency"].as_u64() != Some(u64::from(frequency)) {
        bail!("frequency differs from period metadata");
    }
    let (window_start, window_end) = period_window(period)?;
    let mut counts = BTreeMap::<String, u64>::new();
    for (name, expected) in audit["snapshot_counts"]
        .as_object()
        .context("snapshot_counts")?
    {
        let cf = db.cf_handle(name).context("missing data CF")?;
        let q = db
            .cf_handle(&format!("q:{name}"))
            .context("missing quality CF")?;
        let seconds = if name.starts_with("s15:") { 15 } else { 60 };
        for item in db.iterator_cf(&cf, IteratorMode::Start) {
            let (key, bytes) = item?;
            if key.len() != 24 {
                bail!("invalid Raw key length");
            }
            let ric = decode_ric(&key[..16])?;
            let ts = u64::from_be_bytes(key[16..].try_into()?);
            if ts < window_start || ts >= window_end {
                continue;
            }
            let value = decode_ll2_minute(&bytes)?;
            let span = seconds * 1_000_000_000;
            if ts % span != 0 || value.source_ts_utc_ns < ts || value.source_ts_utc_ns >= ts + span
            {
                bail!("source timestamp outside Raw bucket");
            }
            let routed = raw_cf(
                period_year(period)?,
                &parse_product(&ric).context("unparsed RIC")?,
                seconds as u32,
            )?;
            if routed != *name {
                bail!("Raw RIC in wrong CF");
            }
            let known = db.get_cf(&q, &key)?.context("missing Raw quality")?;
            if known.len() != 8 || u64::from_le_bytes(known.as_slice().try_into()?) >> 61 != 0 {
                bail!("invalid Raw quality mask");
            }
            validate_quality(&value, u64::from_le_bytes(known.as_slice().try_into()?))?;
            *counts.entry(name.clone()).or_default() += 1;
        }
        if counts.get(name).copied().unwrap_or(0) != expected.as_u64().context("count")? {
            bail!("Raw snapshot census mismatch for {name}");
        }
        let mut quality_count = 0;
        for row in db.iterator_cf(&q, IteratorMode::Start) {
            let (key, _) = row?;
            if key.len() != 24 {
                bail!("invalid quality key length");
            }
            let ts = u64::from_be_bytes(key[16..].try_into()?);
            if window_start <= ts && ts < window_end {
                quality_count += 1;
            }
        }
        if quality_count != expected.as_u64().unwrap() {
            bail!("Raw quality census mismatch");
        }
    }
    Ok(audit)
}
fn period_window(period: &str) -> Result<(u64, u64)> {
    let (start, end) = period.split_once('_').context("invalid period window")?;
    let timestamp = |date: &str| -> Result<u64> {
        cme_tas_replay::ll2_source::parse_ll2_datetime_ns(format!("{date}T00:00:00Z").as_bytes())
    };
    let start = timestamp(start)?;
    let end = timestamp(end)?;
    if start >= end {
        bail!("period end must follow start");
    }
    Ok((start, end))
}
fn replay_staged(db: &mut DB, config: &Config, period: &str) -> Result<Value> {
    let started = Instant::now();
    let (manifest, root) = raw_ll2_stage::load(config, period)?;
    let plan = raw_ll2_stage::plan(&manifest)?;
    if config.workers == 0 {
        bail!("workers must be positive");
    }
    let year = period_year(period)?;
    let (start, end) = period_window(period)?;
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    if db.get_cf(&meta, period_meta_key(period)?)?.is_some() {
        bail!("Raw period already claimed; use fresh DB for rerun");
    }
    if raw_ll2_stage::free_bytes(&config.rocksdb_dir)?
        < config.min_free_bytes + config.disk_budget_bytes
    {
        bail!("insufficient disk for staged replay budget");
    }
    db.put_cf(&meta, period_meta_key(period)?, b"writing")?;
    db.flush_wal(true)?;
    let mut batch = WriteBatch::default();
    let mut counts = BTreeMap::<String, u64>::new();
    let mut processed = 0u64;
    let mut output = |s: RawSnapshot| -> Result<()> {
        if s.value.source_ts_utc_ns < start || s.value.source_ts_utc_ns >= end {
            bail!("staged timestamp outside period");
        }
        let name = raw_cf(
            year,
            &parse_product(&s.ric).context("unparsed Raw RIC")?,
            s.seconds,
        )?;
        let quality = format!("q:{name}");
        if db.cf_handle(&name).is_none() {
            db.create_cf(&name, &raw_cf_options())?;
            db.create_cf(&quality, &raw_cf_options())?;
        }
        let key = s.key()?;
        if plan.contested.contains(&(s.seconds, key)) {
            if !batch.is_empty() {
                db.write(std::mem::take(&mut batch))?;
            }
            if let Some(bytes) = db.get_cf(&db.cf_handle(&name).unwrap(), key)? {
                let old = decode_ll2_minute(&bytes)?;
                if (old.source_ts_utc_ns, old.source_order)
                    >= (s.value.source_ts_utc_ns, s.value.source_order)
                {
                    return Ok(());
                }
            } else {
                *counts.entry(name.clone()).or_default() += 1;
            }
        } else {
            *counts.entry(name.clone()).or_default() += 1;
        }
        batch.put_cf(
            &db.cf_handle(&name).unwrap(),
            key,
            encode_ll2_minute(&s.value),
        );
        batch.put_cf(&db.cf_handle(&quality).unwrap(), key, s.known.to_le_bytes());
        processed += 1;
        if batch.len() >= 16_384 || plan.contested.contains(&(s.seconds, key)) {
            db.write(std::mem::take(&mut batch))?;
        }
        if processed % 5_000_000 == 0 {
            let free = raw_ll2_stage::free_bytes(&config.rocksdb_dir)?;
            if free < config.min_free_bytes {
                bail!("disk reserve reached");
            }
            eprintln!(
                "raw_replay_progress period={period} rows={processed} seconds={:.1} free={free}",
                started.elapsed().as_secs_f64()
            );
        }
        Ok(())
    };
    for s in plan.boundary.iter().cloned() {
        output(s)?;
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()?;
    let (tx, rx) = crossbeam_channel::bounded::<Vec<RawSnapshot>>(config.workers * 2);
    let jobs: Vec<_> = manifest
        .parts
        .iter()
        .flat_map(|p| p.shards.iter().map(move |s| (p, s)))
        .collect();
    std::thread::scope(|scope| -> Result<()> {
        let producer = scope.spawn(|| -> Result<()> {
            let results: Vec<Result<()>> = pool.install(|| {
                jobs.par_iter()
                    .map(|(part, shard)| {
                        let mut buffer = Vec::with_capacity(1024);
                        raw_ll2_stage::read_shard(
                            &root,
                            part,
                            shard,
                            &plan.seeds[part.part],
                            &mut |s| {
                                buffer.push(s);
                                if buffer.len() == 1024 {
                                    tx.send(std::mem::replace(
                                        &mut buffer,
                                        Vec::with_capacity(1024),
                                    ))
                                    .context("Raw writer stopped")?;
                                }
                                Ok(())
                            },
                        )?;
                        if !buffer.is_empty() {
                            tx.send(buffer).context("Raw writer stopped")?;
                        }
                        Ok(())
                    })
                    .collect()
            });
            drop(tx);
            for r in results {
                r?;
            }
            Ok(())
        });
        let write_result = (|| -> Result<()> {
            for chunk in &rx {
                for s in chunk {
                    output(s)?;
                }
            }
            Ok(())
        })();
        drop(rx); // Wake blocked producers on a writer failure.
        let read_result = producer
            .join()
            .map_err(|_| anyhow::anyhow!("Raw shard worker panicked"))?;
        write_result?;
        read_result?;
        Ok(())
    })?;
    drop(output);
    if !batch.is_empty() {
        db.write(batch)?;
    }
    db.flush_wal(true)?;
    let audit = json!({"period":period,"frequency":config.frequency,"stored_frequencies":[config.frequency],"refresh_policy":raw_ll2_stage::REFRESH_POLICY,"source_kind":"rawLL2","complete":true,"write_complete":true,"full_scan_verification":"not_run","sample":false,
        "pipeline":"parallel_gzip_zstd_causal_stitch","sources":manifest.parts.iter().map(|p|&p.source).collect::<Vec<_>>(),
        "census":plan.census,"snapshot_counts":counts,"inventory":config.inventory,"remote_root":config.remote_root,
        "shard_root":root,"contested_keys":plan.contested.len(),"workers":config.workers,"elapsed_seconds":started.elapsed().as_secs_f64()});
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    db.put_cf(
        &meta,
        format!("audit:{period}"),
        serde_json::to_vec(&audit)?,
    )?;
    // Completed source census + successful atomic writes define done.
    // Full database scan is now explicit --verify only, per user preference.
    db.put_cf(&meta, period_meta_key(period)?, b"done")?;
    db.flush_wal(true)?;
    eprintln!(
        "raw_replay_done period={period} seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
    Ok(audit)
}
fn finalize_written(db: &DB, config: &Config, period: &str) -> Result<Value> {
    let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
    let status = db
        .get_cf(&meta, period_meta_key(period)?)?
        .context("period status")?;
    if status != b"writing" && status != b"done" {
        bail!("only a completed production write can be finalized");
    }
    let mut audit: Value = serde_json::from_slice(
        &db.get_cf(&meta, format!("audit:{period}"))?
            .context("missing complete write audit")?,
    )?;
    if audit["complete"] != true
        || audit["sample"] != false
        || audit["period"] != period
        || audit["frequency"] != config.frequency
        || audit["refresh_policy"] != raw_ll2_stage::REFRESH_POLICY
    {
        bail!("write audit does not describe this completed production period");
    }
    let (manifest, _) = raw_ll2_stage::load(config, period)?;
    if audit["census"] != serde_json::to_value(&manifest.census)? {
        bail!("persisted write census differs from completed source manifest");
    }
    for name in audit["snapshot_counts"]
        .as_object()
        .context("snapshot_counts")?
        .keys()
    {
        if db.cf_handle(name).is_none() || db.cf_handle(&format!("q:{name}")).is_none() {
            bail!("completed write is missing data/quality CF {name}");
        }
    }
    audit["write_complete"] = true.into();
    audit["full_scan_verification"] = "skipped_at_user_request".into();
    audit["finalized_at"] = chrono::Utc::now().to_rfc3339().into();
    let mut batch = WriteBatch::default();
    batch.put_cf(
        &meta,
        format!("audit:{period}"),
        serde_json::to_vec(&audit)?,
    );
    batch.put_cf(&meta, period_meta_key(period)?, b"done");
    db.write(batch)?;
    db.flush_wal(true)?;
    // Materialize remaining WAL-backed memtables; no scan or record reread.
    let names = DB::list_cf(&Options::default(), &config.rocksdb_dir)?;
    let handles: Vec<_> = names.iter().map(|n| db.cf_handle(n).unwrap()).collect();
    let refs: Vec<_> = handles.iter().collect();
    db.flush_cfs_opt(&refs, &rocksdb::FlushOptions::default())?;
    eprintln!("raw_write_finalized period={period} full_scan_verification=skipped_at_user_request");
    Ok(audit)
}

fn run() -> Result<()> {
    let args = Args::parse();
    if args.finalize_written
        && (args.verify
            || args.preflight
            || args.dry_run
            || !args.sample_input.is_empty()
            || args.max_messages.is_some()
            || args.dump_jsonl.is_some())
    {
        bail!("finalize-written cannot be combined with input/replay/verify flags");
    }
    if args.preflight && (args.verify || args.dry_run || !args.sample_input.is_empty()) {
        bail!("preflight cannot be combined with verify, dry-run, or sample input");
    }
    if args.verify
        && (!args.sample_input.is_empty()
            || args.max_messages.is_some()
            || args.dump_jsonl.is_some())
    {
        bail!("verify cannot be combined with sample input or scan limits/output");
    }
    let mut config: Config = toml::from_str(&fs::read_to_string(&args.config)?)?;
    if let Some(path) = args.rocksdb_dir {
        config.rocksdb_dir = path;
    }
    if let Some(frequency) = args.frequency {
        config.frequency = frequency;
    }
    Extractor::new(config.frequency)?;
    if args.dry_run && args.verify {
        bail!("verify and dry-run are mutually exclusive");
    }
    let periods = args
        .period
        .map(|p| vec![p])
        .unwrap_or_else(|| config.periods.clone());
    if periods.is_empty() {
        bail!("no periods");
    }
    if !args.sample_input.is_empty() && periods.len() != 1 {
        bail!("sample input needs one period");
    }
    if args.max_messages.is_some() && !args.dry_run {
        bail!("max-messages only allowed with dry-run");
    }
    if args.max_messages == Some(0) {
        bail!("max-messages must be positive");
    }
    if let Some(root) = &config.data_root {
        if config.rocksdb_dir.starts_with(root) {
            bail!("derived DB must be outside source root");
        }
    }
    let client = client()?;
    let inventory: Option<Inventory> =
        if args.sample_input.is_empty() && !args.verify && !args.finalize_written {
            Some(serde_json::from_str(&fs::read_to_string(
                &config.inventory,
            )?)?)
        } else {
            None
        };
    // Preflight every period before creating or claiming any output.
    if let Some(inv) = &inventory {
        for period in &periods {
            let p = package(inv, &config, period)?;
            for file in &p.files {
                if file.name.starts_with("merged-Data-part-") || file.name == "merged-Report.csv.gz"
                {
                    if let Some(root) = &config.data_root {
                        if fs::metadata(root.join(&p.directory).join(&file.name))?.len()
                            != file.remote_bytes
                        {
                            bail!("local Raw file size mismatch: {}", file.name);
                        }
                    } else {
                        let response = client.head(&file.url).send()?.error_for_status()?;
                        if response
                            .headers()
                            .get("content-length")
                            .and_then(|h| h.to_str().ok())
                            .and_then(|h| h.parse::<u64>().ok())
                            != Some(file.remote_bytes)
                            || file.etag.as_deref()
                                != response.headers().get("etag").and_then(|v| v.to_str().ok())
                        {
                            bail!("remote Raw metadata changed: {}", file.url);
                        }
                    }
                }
            }
        }
    }
    if args.preflight {
        println!("Raw LL2 preflight passed: {} periods", periods.len());
        return Ok(());
    }
    let mut db = if args.dry_run {
        None
    } else if args.verify {
        let names = DB::list_cf(&Options::default(), &config.rocksdb_dir)?;
        let db = DB::open_cf_for_read_only(&Options::default(), &config.rocksdb_dir, names, false)?;
        if db.get(b"db_kind")?.as_deref() != Some(b"cme_raw_ll2_v1") {
            bail!("not a Raw LL2 DB");
        }
        Some(db)
    } else {
        Some(open_output(&config.rocksdb_dir)?)
    };
    if args.verify {
        let db = db.as_ref().context("verify requires a DB")?;
        for p in &periods {
            println!("{}", verify(db, p, config.frequency, false)?);
        }
        return Ok(());
    }
    if args.finalize_written {
        let mut audits = Vec::new();
        for p in &periods {
            audits.push(finalize_written(
                db.as_ref().context("output DB")?,
                &config,
                p,
            )?);
        }
        if let Some(path) = args.audit {
            fs::write(path, serde_json::to_vec_pretty(&audits)?)?;
        }
        println!("{}", serde_json::to_string(&audits)?);
        return Ok(());
    }
    if config.shard_root.is_some() && args.sample_input.is_empty() && !args.dry_run {
        if args.dump_jsonl.is_some() {
            bail!("dump-jsonl unsupported for full staged replay");
        }
        let mut audits = Vec::new();
        for period in &periods {
            let audit = replay_staged(db.as_mut().context("output DB")?, &config, period)?;
            println!("{audit}");
            audits.push(audit);
        }
        if let Some(path) = args.audit {
            fs::write(path, serde_json::to_vec_pretty(&audits)?)?;
        }
        return Ok(());
    }
    let mut dump = args
        .dump_jsonl
        .map(|path| File::create(path).map(BufWriter::new))
        .transpose()?;
    let mut audits = Vec::new();
    for period in periods {
        let started = Instant::now();
        let year = period_year(&period)?;
        let (window_start, window_end) = period_window(&period)?;
        if let Some(db) = &db {
            let meta = db.cf_handle(CF_REPLAY_META).context("replay_meta")?;
            if db.get_cf(&meta, period_meta_key(&period)?)?.is_some() {
                bail!("Raw period already claimed; use a fresh output DB for rerun");
            }
            db.put_cf(&meta, period_meta_key(&period)?, b"writing")?;
        }
        let mut scanner = RawScanner::default();
        let mut extractor = Extractor::new(config.frequency)?;
        let mut batch = WriteBatch::default();
        let mut counts = BTreeMap::<String, u64>::new();
        let mut output = |snapshot: RawSnapshot| -> Result<()> {
            let product = parse_product(&snapshot.ric).context("unparsed Raw RIC")?;
            let name = raw_cf(year, &product, snapshot.seconds)?;
            *counts.entry(name.clone()).or_default() += 1;
            if let Some(dump) = &mut dump {
                serde_json::to_writer(&mut *dump, &snapshot.json()?)?;
                writeln!(dump)?;
            }
            if let Some(db) = &mut db {
                let quality = format!("q:{name}");
                if db.cf_handle(&name).is_none() {
                    db.create_cf(&name, &raw_cf_options())?;
                    db.create_cf(&quality, &raw_cf_options())?;
                }
                let key = snapshot.key()?;
                batch.put_cf(
                    &db.cf_handle(&name).unwrap(),
                    key,
                    encode_ll2_minute(&snapshot.value),
                );
                batch.put_cf(
                    &db.cf_handle(&quality).unwrap(),
                    key,
                    snapshot.known.to_le_bytes(),
                );
                if batch.len() >= 16_384 {
                    db.write(std::mem::take(&mut batch))?;
                }
            }
            Ok(())
        };
        let mut limited = false;
        let mut sources = Vec::new();
        let expected = if let Some(inv) = &inventory {
            let p = package(inv, &config, &period)?;
            let expected = expected_counts(&config, p, &client)?;
            let mut files: Vec<_> = p
                .files
                .iter()
                .filter(|f| f.name.starts_with("merged-Data-part-") && f.name.ends_with(".csv.gz"))
                .collect();
            files.sort_by_key(|f| &f.name);
            for (i, file) in files.into_iter().enumerate() {
                eprintln!("raw_source_start period={period} file={}", file.name);
                sources.push(json!({"name": file.name, "url": file.url, "bytes": file.remote_bytes, "etag": file.etag}));
                limited = scanner.scan(
                    input(&config, p, file, &client)?,
                    i == 0,
                    args.max_messages,
                    &mut |m| {
                        if m.ts < window_start || m.ts >= window_end {
                            bail!("Raw source message outside configured period");
                        }
                        if m.order % 10_000_000 == 0 {
                            eprintln!(
                                "raw_progress period={period} messages={} ric={} ts={}",
                                m.order, m.ric, m.ts
                            );
                        }
                        extractor.apply(&m, &mut output)
                    },
                )?;
                if limited {
                    break;
                }
            }
            Some(expected)
        } else {
            for (i, path) in args.sample_input.iter().enumerate() {
                let file = File::open(path)?;
                let reader: Box<dyn Read> = if path.extension().is_some_and(|s| s == "gz") {
                    Box::new(MultiGzDecoder::new(BufReader::new(file)))
                } else {
                    Box::new(file)
                };
                sources.push(json!({"sample_path": path, "bytes": fs::metadata(path)?.len()}));
                limited = scanner.scan(reader, i == 0, args.max_messages, &mut |m| {
                    extractor.apply(&m, &mut output)
                })?;
                if limited {
                    break;
                }
            }
            None
        };
        scanner.finish()?;
        extractor.finish(&mut output)?;
        scanner.census.depth_messages = extractor.depth_messages;
        drop(output);
        if let Some(db) = &db {
            if !batch.is_empty() {
                db.write(std::mem::take(&mut batch))?;
            }
        }
        if !limited {
            if let Some(expected) = &expected {
                let active: BTreeMap<_, _> = expected
                    .iter()
                    .filter(|(_, c)| **c > 0)
                    .map(|(r, c)| (r.clone(), *c))
                    .collect();
                if active != scanner.census.messages_by_ric {
                    bail!("Raw message census differs from Report Count");
                }
            }
        }
        let audit = json!({"period": period, "frequency": config.frequency, "stored_frequencies": [config.frequency], "refresh_policy": raw_ll2_stage::REFRESH_POLICY, "source_kind": "rawLL2", "sources": sources,
            "complete": !limited && expected.is_some(), "write_complete": !limited, "full_scan_verification": "not_run", "sample": !args.sample_input.is_empty(),
            "census": scanner.census, "snapshot_counts": counts, "inventory": config.inventory, "remote_root": config.remote_root, "elapsed_seconds": started.elapsed().as_secs_f64()});
        if let Some(db) = &db {
            db.flush_wal(true)?;
            let meta = db.cf_handle(CF_REPLAY_META).unwrap();
            db.put_cf(
                &meta,
                format!("audit:{period}").as_bytes(),
                serde_json::to_vec(&audit)?,
            )?;
            db.put_cf(
                &meta,
                period_meta_key(&period)?,
                if expected.is_some() {
                    &b"done"[..]
                } else {
                    &b"sample"[..]
                },
            )?;
            db.flush_wal(true)?;
        }
        println!("{audit}");
        audits.push(audit);
    }
    if let Some(mut dump) = dump {
        dump.flush()?;
    }
    if let Some(path) = args.audit {
        fs::write(path, serde_json::to_vec_pretty(&audits)?)?;
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("cme_raw_ll2_replay failed: {e:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::ll2_1min::Ll2Minute;
    use cme_tas_replay::ll2_source::parse_normalized_ll2_line;
    use cme_tas_replay::raw_ll2_io::Package;

    #[test]
    fn staged_database_matches_python_with_concurrent_bucket_collisions() -> Result<()> {
        use flate2::{write::GzEncoder, Compression};
        let bytes = include_bytes!("../../tests/fixtures/raw_ll2_small.csv");
        let cut = bytes.windows(10).position(|s| s == b",,,,FID,40").unwrap();
        for parts in [
            vec![&bytes[..]],
            vec![&bytes[..cut], &bytes[cut..]],
            bytes.split_inclusive(|b| *b == b'\n').collect(),
        ] {
            let mut order_map = BTreeMap::new();
            let mut global_order = 0u64;
            for (part, input) in parts.iter().enumerate() {
                let mut local_order = 0u64;
                let mut csv = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .flexible(true)
                    .from_reader(*input);
                for record in csv.records() {
                    let row = record?;
                    if !row[0].is_empty() && &row[0] != "#RIC" {
                        global_order += 1;
                        local_order += 1;
                        order_map.insert(global_order, ((part as u64) << 48) | local_order);
                    }
                }
            }
            let tmp = tempfile::tempdir()?;
            let root = "http://example.invalid/rawLL2";
            let period = "2024-01-01_2025-01-01";
            let directory =
                format!("shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_{period}");
            let source = tmp.path().join("source");
            fs::create_dir_all(source.join(&directory))?;
            let mut files = Vec::new();
            for (i, input) in parts.iter().enumerate() {
                let name = format!("merged-Data-part-{i:06}.csv.gz");
                let path = source.join(&directory).join(&name);
                let mut gz = GzEncoder::new(File::create(&path)?, Compression::fast());
                gz.write_all(input)?;
                gz.finish()?;
                files.push(json!({"name":name,"url":format!("{root}/{directory}/{name}"),"remote_bytes":fs::metadata(path)?.len(),"etag":null}));
            }
            let name = "merged-Report.csv.gz";
            let path = source.join(&directory).join(name);
            let mut gz = GzEncoder::new(File::create(&path)?, Compression::fast());
            gz.write_all(b"#RIC,Count\nESH24,8\nNQH24,1\n")?;
            gz.finish()?;
            files.push(json!({"name":name,"url":format!("{root}/{directory}/{name}"),"remote_bytes":fs::metadata(path)?.len(),"etag":null}));
            let inventory = tmp.path().join("inventory.json");
            fs::write(
                &inventory,
                serde_json::to_vec(
                    &json!({"remote_root":root,"packages":[{"directory":directory,"period":period,"files":files,"report_present":true,"missing_part_indices_through_last":[],"partial_files":[]}]}),
                )?,
            )?;
            let config = Config {
                inventory,
                remote_root: root.into(),
                data_root: Some(source),
                rocksdb_dir: tmp.path().join("db"),
                shard_root: Some(tmp.path().join("stage")),
                periods: vec![period.into()],
                frequency: 15,
                partition_workers: 4,
                workers: 4,
                rows_per_shard: 2,
                ..Config::default()
            };
            raw_ll2_stage::build(&config)?;
            let mut db = open_output(&config.rocksdb_dir)?;
            let audit = replay_staged(&mut db, &config, period)?;
            assert_eq!(audit["census"]["messages"], 9);
            assert_eq!(audit["stored_frequencies"], json!([15]));
            assert!(DB::list_cf(&Options::default(), &config.rocksdb_dir)?
                .iter()
                .all(|name| !name.starts_with("p:") && !name.starts_with("q:p:")));
            assert_eq!(audit["full_scan_verification"], "not_run");
            let meta = db.cf_handle(CF_REPLAY_META).unwrap();
            db.put_cf(&meta, period_meta_key(period)?, b"writing")?;
            let finalized = finalize_written(&db, &config, period)?;
            assert_eq!(
                finalized["full_scan_verification"],
                "skipped_at_user_request"
            );
            assert_eq!(
                db.get_cf(&meta, period_meta_key(period)?)?.unwrap(),
                b"done"
            );
            let mut incomplete = finalized.clone();
            incomplete["complete"] = false.into();
            db.put_cf(
                &meta,
                format!("audit:{period}"),
                serde_json::to_vec(&incomplete)?,
            )?;
            db.put_cf(&meta, period_meta_key(period)?, b"writing")?;
            assert!(finalize_written(&db, &config, period).is_err());
            assert_eq!(
                db.get_cf(&meta, period_meta_key(period)?)?.unwrap(),
                b"writing"
            );
            db.put_cf(
                &meta,
                format!("audit:{period}"),
                serde_json::to_vec(&finalized)?,
            )?;
            db.put_cf(&meta, period_meta_key(period)?, b"done")?;
            // Force persisted LZ4 SSTs rather than validating only memory/WAL.
            for name in DB::list_cf(&Options::default(), &config.rocksdb_dir)? {
                db.flush_cf(&db.cf_handle(&name).unwrap())?;
            }
            verify(&db, period, 15, false)?;
            let expected: Value = serde_json::from_str(include_str!(
                "../../tests/fixtures/raw_ll2_small.expected.json"
            ))?;
            for s in expected["snapshots"].as_array().unwrap() {
                let ric = s["ric"].as_str().unwrap();
                let seconds = s["seconds"].as_u64().unwrap() as u32;
                let name = raw_cf(2024, &parse_product(ric).unwrap(), seconds)?;
                let key = hex_decode(s["key_hex"].as_str().unwrap());
                let actual = db.get_cf(&db.cf_handle(&name).unwrap(), &key)?.unwrap();
                let mut wanted = hex_decode(s["value_hex"].as_str().unwrap());
                let original_order = u64::from_le_bytes(wanted[12..20].try_into()?);
                wanted[12..20].copy_from_slice(&order_map[&original_order].to_le_bytes());
                assert_eq!(actual, wanted);
                let known = db
                    .get_cf(&db.cf_handle(&format!("q:{name}")).unwrap(), &key)?
                    .unwrap();
                assert_eq!(
                    u64::from_le_bytes(known.as_slice().try_into()?),
                    s["known"].as_u64().unwrap()
                );
            }
        }
        Ok(())
    }
    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn incomplete_2025_inventory_is_rejected_before_any_output() {
        let root = "http://example.invalid/rawLL2";
        let period = "2025-01-01_2026-01-01";
        let inv = Inventory {
            remote_root: root.into(),
            packages: vec![Package {
                directory: format!(
                    "shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_{period}"
                ),
                period: period.into(),
                files: vec![],
                missing_part_indices_through_last: vec![0, 10],
                partial_files: vec!["unfinished.part".into()],
                report_present: false,
            }],
        };
        let cfg = Config {
            inventory: "unused".into(),
            remote_root: root.into(),
            data_root: None,
            rocksdb_dir: "unused".into(),
            periods: vec![period.into()],
            frequency: 60,
            ..Config::default()
        };
        assert!(package(&inv, &cfg, period).is_err());
    }

    #[test]
    fn verify_checks_period_window_quality_and_database_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("raw");
        let mut db = open_output(&path).unwrap();
        let name = "p:2024:ES";
        let quality = format!("q:{name}");
        db.create_cf(name, &Options::default()).unwrap();
        db.create_cf(&quality, &Options::default()).unwrap();
        let mut first_key = None;
        for (period, date) in [
            ("2024-01-01_2024-07-01", "2024-01-02"),
            ("2024-07-01_2025-01-01", "2024-07-02"),
        ] {
            let line = format!("ESH24,Market Price,{date}T00:00:01Z,-6,Normalized LL2,1,2,0,3,4,0");
            let value =
                Ll2Minute::from_source(parse_normalized_ll2_line(line.as_bytes()).unwrap(), 1);
            let s = RawSnapshot {
                ric: "ESH24".into(),
                seconds: 60,
                bucket_ns: value.source_ts_utc_ns / 60_000_000_000 * 60_000_000_000,
                value,
                known: (1 << 61) - 1,
                inherit_prior: false,
            };
            let key = s.key().unwrap();
            first_key.get_or_insert(key);
            db.put_cf(
                &db.cf_handle(name).unwrap(),
                key,
                encode_ll2_minute(&s.value),
            )
            .unwrap();
            db.put_cf(&db.cf_handle(&quality).unwrap(), key, s.known.to_le_bytes())
                .unwrap();
            let meta = db.cf_handle(CF_REPLAY_META).unwrap();
            db.put_cf(&meta, period_meta_key(period).unwrap(), b"done")
                .unwrap();
            db.put_cf(
                &meta,
                format!("audit:{period}").as_bytes(),
                serde_json::to_vec(&json!({"frequency":60,"snapshot_counts":{name:1}})).unwrap(),
            )
            .unwrap();
        }
        assert!(verify(&db, "2024-01-01_2024-07-01", 60, false).is_ok());
        assert!(verify(&db, "2024-07-01_2025-01-01", 60, false).is_ok());
        assert!(verify(&db, "2024-01-01_2024-07-01", 15, false).is_err());
        db.put_cf(
            &db.cf_handle(&quality).unwrap(),
            first_key.unwrap(),
            0u64.to_le_bytes(),
        )
        .unwrap();
        assert!(verify(&db, "2024-01-01_2024-07-01", 60, false).is_err());
        drop(db);
        let other = temp.path().join("other");
        let mut options = Options::default();
        options.create_if_missing(true);
        drop(DB::open(&options, &other).unwrap());
        assert!(open_output(&other).is_err());
    }
}
