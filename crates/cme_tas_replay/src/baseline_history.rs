//! Explicit historical catalog adapter for the contract-minute exporter.
use super::*;
use cme_tas_replay::history_catalog::{Catalog, ProjectedDay};
use cme_tas_replay::session_status;
use sha2::{Digest, Sha256};

pub(super) struct ReviewedHistory {
    pub from: NaiveDate,
    pub until: NaiveDate,
    pub days: BTreeMap<(String, NaiveDate), ProjectedDay>,
    pub multipliers: BTreeMap<String, f64>,
    pub catalog: Catalog,
    pub sha256: String,
    pub status: Option<Vec<session_status::Event>>,
    pub status_sha256: Option<String>,
}

impl ReviewedHistory {
    pub(super) fn status_segments(
        &self,
        product: &str,
        ric: &str,
        planned: &[(i64, i64)],
    ) -> Result<Option<Vec<(i64, i64)>>> {
        let Some(events) = &self.status else {
            return Ok(None);
        };
        let nanos = planned
            .iter()
            .map(|&(lo, last)| {
                Ok((
                    u64::try_from(lo)? * NS_PER_SEC,
                    u64::try_from(last + 60)? * NS_PER_SEC,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(
            session_status::project(&nanos, events, product, ric)?
                .into_iter()
                .map(|(lo, hi)| ((lo / NS_PER_SEC) as i64, (hi / NS_PER_SEC) as i64 - 60))
                .collect(),
        ))
    }
}

pub(super) fn load(args: &Args, products: &[ProductSpec]) -> Result<Option<Arc<ReviewedHistory>>> {
    let Some(path) = &args.history_catalog else {
        return Ok(None);
    };
    let bytes =
        fs::read(path).with_context(|| format!("read historical catalog {}", path.display()))?;
    let catalog: Catalog = serde_json::from_slice(&bytes)?;
    let (status, status_sha256) = if let Some(path) = &args.session_status {
        let data =
            fs::read(path).with_context(|| format!("read session status {}", path.display()))?;
        let events: Vec<session_status::Event> = serde_json::from_slice(&data)?;
        session_status::validate(&events)?;
        (Some(events), Some(format!("{:x}", Sha256::digest(&data))))
    } else {
        (None, None)
    };
    if let Some(required) = &catalog.required_session_status_sha256 {
        if required.len() != 64
            || !required.bytes().all(|b| b.is_ascii_hexdigit())
            || status_sha256.as_deref() != Some(required.as_str())
        {
            bail!("planned history catalog requires its exact --session-status input");
        }
    }
    let from = if args.size_buckets {
        raw_baseline::previous_month_start(args.start)?
    } else {
        args.start
    };
    let roots = products
        .iter()
        .map(|p| p.product.to_string())
        .collect::<Vec<_>>();
    let projected = catalog.project(&roots, from, args.end)?;
    let mut multipliers = BTreeMap::new();
    for p in products {
        let m = catalog
            .multipliers
            .iter()
            .find(|m| m.product == p.product && m.from <= from && m.until >= args.end)
            .context("validated multiplier")?;
        if m.exchange != p.exchange {
            bail!("catalog exchange mismatch for {}", p.product);
        }
        multipliers.insert(p.product.to_string(), m.volume_multiple);
    }
    let days = projected
        .into_iter()
        .map(|r| ((r.product.clone(), r.trading_day), r))
        .collect();
    Ok(Some(Arc::new(ReviewedHistory {
        from,
        until: args.end,
        days,
        multipliers,
        catalog,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        status,
        status_sha256,
    })))
}

pub(super) fn source_years(segments: &[(i64, i64)]) -> Result<BTreeSet<i32>> {
    let mut years = BTreeSet::new();
    for &(start, last_minute) in segments {
        let first = Utc
            .timestamp_opt(start, 0)
            .single()
            .context("segment start")?
            .year();
        let last = Utc
            .timestamp_opt(last_minute + 59, 0)
            .single()
            .context("segment end")?
            .year();
        years.extend(first..=last);
    }
    Ok(years)
}

pub(super) fn list_jobs(args: &Args, history: &ReviewedHistory) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for ((product, day), record) in &history.days {
        if *day < args.start || *day >= args.end {
            continue;
        }
        let spec = product_spec(product)?;
        let stem = day.format("%Y%m%d").to_string();
        let output = args
            .output_root
            .join(spec.exchange)
            .join(product)
            .join(format!("{stem}.parquet"));
        if output.exists() && !args.overwrite {
            bail!("historical output already exists: {}", output.display());
        }
        jobs.push(Job {
            product: product.clone(),
            exchange: spec.exchange.into(),
            day: *day,
            input: args.tas_rocksdb.clone(),
            output,
            audit: args
                .output_root
                .join("_audit")
                .join(product)
                .join(format!("{stem}.json")),
            tas_grids: Some(
                record
                    .contracts
                    .iter()
                    .map(|c| RicGrid {
                        ric: c.ric.clone(),
                        contract_id: c.contract_id.clone(),
                        segments: vec![],
                        status_segments: None,
                    })
                    .collect(),
            ),
        });
    }
    jobs.sort_by(|a, b| a.day.cmp(&b.day).then(a.product.cmp(&b.product)));
    if let Some(max) = args.max_files {
        jobs.truncate(max);
    }
    if jobs.is_empty() {
        bail!("no open historical days selected");
    }
    write_json_atomic(
        &args.output_root.join("_audit/history_catalog.json"),
        &history.catalog,
    )?;
    write_json_atomic(
        &args.output_root.join("_audit/history_catalog_source.json"),
        &serde_json::json!({"path":args.history_catalog,"sha256":history.sha256,
            "from":history.from,"until":history.until,"jobs":jobs.len(),"metadata_database_modified":false,
            "session_status_path":args.session_status,"session_status_sha256":history.status_sha256}),
    )?;
    if let Some(events) = &history.status {
        let source = args.session_status.as_ref().context("status source path")?;
        let bytes = fs::read(source)?;
        if Some(format!("{:x}", Sha256::digest(&bytes))) != history.status_sha256 {
            bail!("session status input changed after loading");
        }
        let output = args.output_root.join("_audit/session_status.json");
        let temporary = output.with_extension("json.tmp");
        fs::write(&temporary, bytes)?;
        fs::rename(&temporary, &output)?;
        // Also validate the loaded representation before any job is published.
        session_status::validate(events)?;
    }
    Ok(jobs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn causal_status_filters_minutes_preserves_started_bar_and_resets_prices() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let base = Utc
            .with_ymd_and_hms(2025, 11, 28, 13, 20, 0)
            .unwrap()
            .timestamp() as u64
            * NS_PER_SEC;
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../tests/fixtures/session_status.json"))?;
        let mut events: Vec<session_status::Event> =
            serde_json::from_value(cases[0]["events"].clone())?;
        for e in &mut events {
            e.known_at_ns += base;
            e.effective_ns += base;
        }
        let status_path = temp.path().join("status.json");
        let status_bytes = serde_json::to_vec(&events)?;
        fs::write(&status_path, &status_bytes)?;
        let digest = format!("{:x}", Sha256::digest(&status_bytes));
        let known = base - 86400 * NS_PER_SEC;
        let source = "SYNTHETIC integration fixture, not historical evidence";
        let catalog = serde_json::json!({
            "days":[{"product":"TY","trading_day":"2025-11-28","known_at_ns":known,"source":source,"segments":[[base,base+600*NS_PER_SEC]]}],
            "contracts":[{"product":"TY","ric":"TYZ25","contract_id":"CBOT:TY:2025-12","from":"2025-11-28","until":"2025-11-29","known_at_ns":known,"source":source}],
            "multipliers":[{"product":"TY","exchange":"CBOT","volume_multiple":1000.0,"from":"2025-11-01","until":"2025-12-01","known_at_ns":known,"source":source}],
            "required_session_status_sha256":digest
        });
        let catalog_path = temp.path().join("catalog.json");
        fs::write(&catalog_path, serde_json::to_vec(&catalog)?)?;
        let output = temp.path().join("out");
        let mut args = Args::parse_from([
            "baseline",
            "--history-catalog",
            catalog_path.to_str().unwrap(),
            "--session-status",
            status_path.to_str().unwrap(),
            "--output-root",
            output.to_str().unwrap(),
            "--start",
            "2025-11-28",
            "--end",
            "2025-11-29",
            "--products",
            "TY",
            "--trade-only",
            "--tas-contract-grid",
            "--trade-time-basis",
            "publication",
        ]);
        let specs = validate_args(&args)?;
        let history = load(&args, &specs)?.unwrap();
        args.session_status = None;
        assert!(
            load(&args, &specs).is_err(),
            "required overlay cannot be omitted"
        );
        args.session_status = Some(status_path.clone());
        fs::write(&status_path, b"[]")?;
        assert!(
            load(&args, &specs).is_err(),
            "required overlay cannot be replaced"
        );
        fs::write(&status_path, &status_bytes)?;
        let calendar = SessionCalendar {
            by_group: BTreeMap::new(),
            coverage: BTreeMap::new(),
            history: Some(history.clone()),
        };
        let jobs = list_jobs(&args, &history)?;
        assert_eq!(
            fs::read(output.join("_audit/session_status.json"))?,
            status_bytes
        );
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, temp.path().join("tas"), ["p:2025:TY"])?;
        let cf = db.cf_handle("p:2025:TY").unwrap();
        let mut trade = cme_tas_replay::ali_h26_fixture();
        trade.ric = "TYZ25".into();
        trade.exch_hms_ns = cme_tas_replay::MISSING_EXCH_HMS_NS;
        for (sec, volume, price) in [(10, 2, 100), (140, 3, 101), (190, 4, 102), (490, 7, 110)] {
            trade.ts_utc_ns = base + sec * NS_PER_SEC;
            trade.volume = volume;
            trade.price = i64::try_from(price * PRICE_SCALE)?;
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_TRADE, &trade.ric, trade.ts_utc_ns, 0, 0)?,
                cme_tas_replay::encode_cme_trade(&trade)?,
            )?;
        }
        trade.price = MISSING_PRICE;
        trade.aggressor = 0;
        for (sec, volume) in [(145, 13), (200, 11)] {
            trade.ts_utc_ns = base + sec * NS_PER_SEC;
            trade.volume = volume;
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_SPECIAL, &trade.ric, trade.ts_utc_ns, 0, 0)?,
                cme_tas_replay::encode_cme_special(&trade)?,
            )?;
        }
        let audit = process_trade_only_job(
            &jobs[0],
            &db,
            &history.multipliers,
            &calendar,
            None,
            None,
            &args,
            &Mutex::new(()),
        )?;
        assert_eq!(
            (audit.output_rows, audit.trade_rows, audit.special_rows),
            (6, 3, 1)
        );
        assert_eq!(
            (
                audit.status_removed_grid_minutes,
                audit.outside_session_trades,
                audit.outside_session_specials
            ),
            (4, 1, 1)
        );
        let frame = ParquetReader::new(File::open(&jobs[0].output)?).finish()?;
        assert_eq!(frame.width(), 84);
        assert_eq!(
            frame
                .column("volume")?
                .f64()?
                .into_iter()
                .flatten()
                .sum::<f64>(),
            12.0
        );
        assert_eq!(
            frame
                .column("special_volume")?
                .f64()?
                .into_iter()
                .flatten()
                .sum::<f64>(),
            13.0
        );
        assert_eq!(frame.column("close")?.f64()?.get(2), Some(101.0));
        assert_eq!(
            frame.column("close")?.f64()?.get(3),
            None,
            "no carry into resumed empty minute"
        );
        assert_eq!(frame.column("close")?.f64()?.get(4), Some(110.0));
        // The identical scanner used by previous-month size buckets also omits
        // the halted-minute event; its source timestamp remains publication time.
        let planned = calendar.segments_for_product(product_spec("TY")?, jobs[0].day)?;
        let grid = RicGrid {
            contract_id: "CBOT:TY:2025-12".into(),
            ric: "TYZ25".into(),
            status_segments: history.status_segments("TY", "TYZ25", &planned)?,
            segments: planned,
        };
        let sample = scan_trades(&db, &cf, &grid, TimeBasis::Publication)?;
        assert_eq!(sample.iter().map(|t| t.volume).sum::<f64>(), 12.0);
        Ok(())
    }

    #[test]
    fn explicit_holiday_catalog_generates_short_year_rics_without_future_endpoints() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/history_catalog.json"))?;
        let catalog = temp.path().join("catalog.json");
        fs::write(&catalog, serde_json::to_vec(&cases[0]["catalog"])?)?;
        let output = temp.path().join("out");
        let args = Args::parse_from([
            "baseline",
            "--history-catalog",
            catalog.to_str().unwrap(),
            "--output-root",
            output.to_str().unwrap(),
            "--start",
            "2021-11-24",
            "--end",
            "2021-11-27",
            "--products",
            "URO,TU",
            "--trade-only",
            "--tas-contract-grid",
            "--trade-time-basis",
            "publication",
        ]);
        let specs = validate_args(&args)?;
        let h = load(&args, &specs)?.unwrap();
        let calendar = SessionCalendar {
            by_group: BTreeMap::new(),
            coverage: BTreeMap::new(),
            history: Some(h.clone()),
        };
        let jobs = list_jobs(&args, &h)?;
        assert_eq!(jobs.len(), 4);
        assert!(jobs.iter().all(|j| j.day.day() != 25));
        let job = jobs
            .iter()
            .find(|j| j.product == "URO" && j.day.day() == 26)
            .unwrap();
        let segments = calendar.segments_for_product(product_spec("URO")?, job.day)?;
        assert_eq!(segments.len(), 2);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, temp.path().join("tas"), ["p:2021:URO"])?;
        let cf = db.cf_handle("p:2021:URO").unwrap();
        let mut t = cme_tas_replay::ali_h26_fixture();
        t.ric = "UROZ1".into();
        t.exch_hms_ns = cme_tas_replay::MISSING_EXCH_HMS_NS;
        t.price = 1_100_000_000;
        t.volume = 2;
        for sec in [segments[0].0 + 10, segments[0].1 + 120, segments[1].0 + 10] {
            t.ts_utc_ns = sec as u64 * NS_PER_SEC;
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_TRADE, &t.ric, t.ts_utc_ns, 0, 0)?,
                cme_tas_replay::encode_cme_trade(&t)?,
            )?;
        }
        let audit = process_trade_only_job(
            job,
            &db,
            &h.multipliers,
            &calendar,
            None,
            None,
            &args,
            &Mutex::new(()),
        )?;
        assert_eq!(audit.trade_rows, 2);
        assert_eq!(audit.outside_session_trades, 1);
        assert_eq!(audit.output_rows, 2295);
        assert!(job.output.exists());
        let frame = ParquetReader::new(File::open(&job.output)?).finish()?;
        assert_eq!(
            frame
                .column("volume")?
                .f64()?
                .into_iter()
                .flatten()
                .sum::<f64>(),
            4.0
        );
        assert_eq!(
            frame
                .column("amount")?
                .f64()?
                .into_iter()
                .flatten()
                .sum::<f64>(),
            550000.0
        );
        assert_eq!(
            frame.column("contract_id")?.str()?.get(0),
            Some("CME:URO:2021-12")
        );
        Ok(())
    }

    #[test]
    fn historical_day_reads_both_source_year_partitions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let open = Utc
            .with_ymd_and_hms(2021, 12, 31, 23, 59, 0)
            .unwrap()
            .timestamp();
        let day = NaiveDate::from_ymd_opt(2022, 1, 3).unwrap();
        let record = ProjectedDay {
            product: "TU".into(),
            trading_day: day,
            segments: vec![(open as u64 * NS_PER_SEC, (open + 120) as u64 * NS_PER_SEC)],
            volume_multiple: 2000.0,
            contracts: vec![cme_tas_replay::history_catalog::Identity {
                ric: "TUH2".into(),
                contract_id: "CBOT:TU:2022-03".into(),
            }],
        };
        let h = Arc::new(ReviewedHistory {
            from: day,
            until: day + Duration::days(1),
            days: BTreeMap::from([(("TU".into(), day), record)]),
            multipliers: BTreeMap::from([("TU".into(), 2000.0)]),
            catalog: Catalog {
                days: vec![],
                contracts: vec![],
                multipliers: vec![],
                required_session_status_sha256: None,
            },
            sha256: "synthetic test".into(),
            status: None,
            status_sha256: None,
        });
        let output = temp.path().join("out");
        let args = Args::parse_from([
            "baseline",
            "--history-catalog",
            "synthetic-test",
            "--output-root",
            output.to_str().unwrap(),
            "--start",
            "2022-01-03",
            "--end",
            "2022-01-04",
            "--products",
            "TU",
            "--trade-only",
            "--tas-contract-grid",
            "--trade-time-basis",
            "publication",
        ]);
        let jobs = list_jobs(&args, &h)?;
        let calendar = SessionCalendar {
            by_group: BTreeMap::new(),
            coverage: BTreeMap::new(),
            history: Some(h.clone()),
        };
        let mut opt = Options::default();
        opt.create_if_missing(true);
        opt.create_missing_column_families(true);
        let db = DB::open_cf(&opt, temp.path().join("tas"), ["p:2021:TU", "p:2022:TU"])?;
        let mut t = cme_tas_replay::ali_h26_fixture();
        t.ric = "TUH2".into();
        t.volume = 3;
        t.exch_hms_ns = cme_tas_replay::MISSING_EXCH_HMS_NS;
        for (year, sec) in [(2021, open + 10), (2022, open + 70)] {
            t.ts_utc_ns = sec as u64 * NS_PER_SEC;
            db.put_cf(
                &db.cf_handle(&format!("p:{year}:TU")).unwrap(),
                encode_all_key(KIND_CME_TRADE, &t.ric, t.ts_utc_ns, 0, 0)?,
                cme_tas_replay::encode_cme_trade(&t)?,
            )?;
        }
        let a = process_trade_only_job(
            &jobs[0],
            &db,
            &h.multipliers,
            &calendar,
            None,
            None,
            &args,
            &Mutex::new(()),
        )?;
        assert_eq!((a.trade_rows, a.output_rows), (2, 2));
        Ok(())
    }
}
