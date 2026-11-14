//! Scoped TAS contract grid, exact Raw overlay, and previous-month size sampling.
//! Shared aggregation/schema remain in baseline_1min; this module is included
//! only by cme_baseline_1min, never by the six-root backtest exporter.
use super::*;
use chrono::{Months, Timelike};
use cme_tas_replay::baseline_1min::{linear_percentile, SizeBuckets, SizeThresholds};
use cme_tas_replay::ll2_1min::Ll2Minute;
use cme_tas_replay::raw_ll2::{raw_cf, validate_quality};
use cme_tas_replay::{decode_ric, encode_ric};

pub(super) fn previous_month_start(day: NaiveDate) -> Result<NaiveDate> {
    day.with_day(1)
        .context("month start")?
        .checked_sub_months(Months::new(1))
        .context("previous month")
}

fn contract_id(ric: &str, spec: ProductSpec) -> Result<String> {
    let suffix = ric
        .strip_prefix(spec.product)
        .context("RIC root mismatch")?;
    if suffix.len() != 3 || !suffix.as_bytes()[1..].iter().all(u8::is_ascii_digit) {
        bail!("not a standard two-digit-year contract RIC: {ric}");
    }
    let month = b"FGHJKMNQUVXZ"
        .iter()
        .position(|&c| c == suffix.as_bytes()[0])
        .context("RIC month")?
        + 1;
    let year = 2000 + suffix[1..].parse::<u16>()?;
    Ok(format!(
        "{}:{}:{year:04}-{month:02}",
        spec.exchange, spec.product
    ))
}

fn trading_day(event_ns: u64) -> Result<NaiveDate> {
    let local = Utc
        .timestamp_opt((event_ns / NS_PER_SEC) as i64, 0)
        .single()
        .context("event timestamp")?
        .with_timezone(&Chicago);
    Ok(local.date_naive() + Duration::days(i64::from(local.hour() >= 17)))
}

// Walk RIC blocks with two endpoint seeks; do not scan the complete trade tape.
fn ric_spans(
    db: &DB,
    cf_name: &str,
    from: NaiveDate,
    until: NaiveDate,
    basis: TimeBasis,
) -> Result<Vec<(String, u64, u64)>> {
    let cf = db.cf_handle(cf_name).context("TAS CF")?;
    let lo = chicago_window(from)?.0.max(0) as u64 * NS_PER_SEC;
    let hi = chicago_window(until)?.0.max(0) as u64 * NS_PER_SEC;
    let mut it = db.raw_iterator_cf(&cf);
    it.seek([KIND_CME_TRADE]);
    let mut spans = Vec::new();
    loop {
        it.status()?;
        let Some(key) = it.key() else { break };
        if key[0] != KIND_CME_TRADE {
            break;
        }
        if key.len() != ALL_KEY_LEN {
            bail!("invalid TAS key");
        }
        let prefix = key[..17].to_vec();
        let ric = decode_ric(&prefix[1..])?;
        let mut lower = [0; ALL_KEY_LEN];
        lower[..17].copy_from_slice(&prefix);
        lower[17..25].copy_from_slice(&lo.to_be_bytes());
        it.seek(lower);
        it.status()?;
        let first = if it
            .key()
            .is_some_and(|k| k.starts_with(&prefix) && key_ts_ns(k).is_ok_and(|t| t < hi))
        {
            let r = decode_cme_trade(it.value().context("first trade")?)?;
            Some(bar_time(r.ts_utc_ns, r.exch_hms_ns, basis)?)
        } else {
            None
        };
        let mut upper = [255; ALL_KEY_LEN];
        upper[..17].copy_from_slice(&prefix);
        upper[17..25].copy_from_slice(&(hi - 1).to_be_bytes());
        it.seek_for_prev(upper);
        it.status()?;
        let last = if it
            .key()
            .is_some_and(|k| k.starts_with(&prefix) && key_ts_ns(k).is_ok_and(|t| t >= lo))
        {
            let r = decode_cme_trade(it.value().context("last trade")?)?;
            Some(bar_time(r.ts_utc_ns, r.exch_hms_ns, basis)?)
        } else {
            None
        };
        if let (Some(first), Some(last)) = (first, last) {
            if first <= last && !ric.starts_with('.') {
                spans.push((ric, first, last));
            }
        }
        let mut next = [255; ALL_KEY_LEN];
        next[..17].copy_from_slice(&prefix);
        it.seek(next);
    }
    Ok(spans)
}

pub(super) fn list_tas_jobs(
    db: &DB,
    raw: Option<&DB>,
    args: &Args,
    products: &[ProductSpec],
    calendar: &SessionCalendar,
) -> Result<Vec<Job>> {
    let mut selected: BTreeMap<(NaiveDate, String), Vec<RicGrid>> = BTreeMap::new();
    let mut census = Vec::new();
    for spec in products {
        for year in args.start.year()..=(args.end - Duration::days(1)).year() {
            let spans = ric_spans(
                db,
                &product_cf_name(year as u16, spec.product)?,
                args.start,
                args.end,
                args.trade_time_basis,
            )?;
            for (ric, first, last) in spans {
                let id = contract_id(&ric, *spec)?;
                let raw_span = raw
                    .map(|db| {
                        raw_activity_span(
                            db,
                            &raw_cf(year as u16, spec.product, 15)?,
                            &ric,
                            args.start,
                            args.end,
                        )
                    })
                    .transpose()?
                    .flatten();
                let grid_first = raw_span.map_or(first, |(lo, _)| first.min(lo));
                let grid_last = raw_span.map_or(last, |(_, hi)| last.max(hi));
                census.push(serde_json::json!({"product":spec.product,"ric":ric,"contract_id":id,"first_trade_ns":first,"last_trade_ns":last,"raw_activity_span_ns":raw_span,"grid_first_ns":grid_first,"grid_last_ns":grid_last}));
                let mut day = trading_day(grid_first)?.max(args.start);
                let last_day = trading_day(grid_last)?.min(args.end - Duration::days(1));
                while day <= last_day {
                    if !calendar.segments_for_product(*spec, day)?.is_empty() {
                        selected
                            .entry((day, spec.product.into()))
                            .or_default()
                            .push(RicGrid {
                                contract_id: id.clone(),
                                ric: ric.clone(),
                                segments: vec![],
                                status_segments: None,
                            });
                    }
                    day += Duration::days(1);
                }
            }
        }
    }
    write_json_atomic(&args.output_root.join("_audit/contracts.json"), &census)?;
    let mut jobs = Vec::new();
    for ((day, product), mut grids) in selected {
        let spec = product_spec(&product)?;
        grids.sort_by(|a, b| a.ric.cmp(&b.ric));
        grids.dedup_by(|a, b| a.ric == b.ric);
        let stem = day.format("%Y%m%d").to_string();
        let output = args
            .output_root
            .join(spec.exchange)
            .join(&product)
            .join(format!("{stem}.parquet"));
        if output.exists() && !args.overwrite {
            continue;
        }
        jobs.push(Job {
            product: product.clone(),
            exchange: spec.exchange.into(),
            day,
            input: args.tas_rocksdb.clone(),
            output,
            audit: args
                .output_root
                .join("_audit")
                .join(product)
                .join(format!("{stem}.json")),
            tas_grids: Some(grids),
        });
    }
    if let Some(limit) = args.max_files {
        jobs.truncate(limit);
    }
    if jobs.is_empty() {
        bail!("no TAS contract-grid jobs selected");
    }
    Ok(jobs)
}

// Two indexed seeks only. Activity bounds define candidate days; each output
// minute still requires its own exact Raw state. No stale book is carried.
fn raw_activity_span(
    db: &DB,
    cf_name: &str,
    ric: &str,
    from: NaiveDate,
    until: NaiveDate,
) -> Result<Option<(u64, u64)>> {
    let cf = db.cf_handle(cf_name).context("Raw activity CF")?;
    let mut lower = [0; 24];
    lower[..16].copy_from_slice(&encode_ric(ric)?);
    let lo = chicago_window(from)?.0.max(0) as u64 * NS_PER_SEC;
    let hi = chicago_window(until)?.0.max(0) as u64 * NS_PER_SEC;
    lower[16..].copy_from_slice(&lo.to_be_bytes());
    let mut it = db.raw_iterator_cf(&cf);
    it.seek(lower);
    it.status()?;
    let first = if it.key().is_some_and(|k| {
        k.len() == 24
            && k[..16] == lower[..16]
            && u64::from_be_bytes(k[16..].try_into().unwrap()) < hi
    }) {
        Some(decode_ll2_minute(it.value().context("Raw first activity")?)?.source_ts_utc_ns)
    } else {
        None
    };
    let mut upper = lower;
    upper[16..].copy_from_slice(&(hi - 1).to_be_bytes());
    it.seek_for_prev(upper);
    it.status()?;
    let last = if it.key().is_some_and(|k| {
        k.len() == 24
            && k[..16] == lower[..16]
            && u64::from_be_bytes(k[16..].try_into().unwrap()) >= lo
    }) {
        Some(decode_ll2_minute(it.value().context("Raw last activity")?)?.source_ts_utc_ns)
    } else {
        None
    };
    Ok(first.zip(last))
}

pub(super) fn job_grids(job: &Job) -> Result<Vec<RicGrid>> {
    if let Some(grids) = &job.tas_grids {
        Ok(grids.clone())
    } else {
        read_contracts(&job.input, &job.product, job.day)
    }
}

pub(super) fn raw_cf_names(args: &Args, products: &[ProductSpec]) -> Result<Vec<String>> {
    let mut names = vec!["default".into(), CF_REPLAY_META.into()];
    for year in args.start.year()..=(args.end - Duration::days(1)).year() {
        for spec in products {
            let cf = raw_cf(year as u16, spec.product, 15)?;
            names.push(format!("q:{cf}"));
            names.push(cf);
        }
    }
    Ok(names)
}

fn raw_book(value: &Ll2Minute, known: u64) -> Result<Book10> {
    validate_quality(value, known)?;
    let mut book = Book10::default();
    for level in 0..10 {
        for (side, price, size, bit) in [
            (
                0,
                value.bid_prices[level],
                value.bid_sizes[level],
                level * 6,
            ),
            (
                1,
                value.ask_prices[level],
                value.ask_sizes[level],
                level * 6 + 3,
            ),
        ] {
            if known & (3u64 << bit) != 3u64 << bit
                || price == MISSING_PRICE
                || size == MISSING_PRICE
                || price <= 0
                || size < 0
            {
                continue;
            }
            let p = scaled_option(price);
            let v = scaled_option(size);
            if side == 0 {
                book.bid_prices[level] = p;
                book.bid_sizes[level] = v;
            } else {
                book.ask_prices[level] = p;
                book.ask_sizes[level] = v;
            }
        }
    }
    Ok(book)
}

pub(super) fn full_depth(book: &Book10) -> bool {
    (0..10).all(|i| matches!((book.bid_prices[i],book.bid_sizes[i],book.ask_prices[i],book.ask_sizes[i]),(Some(bp),Some(bv),Some(ap),Some(av)) if bp>0.0 && ap>=bp && bv>=0.0 && av>=0.0))
}

pub(super) fn scan_raw_books(
    db: &DB,
    cf_name: &str,
    grid: &RicGrid,
) -> Result<BTreeMap<i64, Book10>> {
    let cf = db.cf_handle(cf_name).context("Raw 15s CF")?;
    let q = db
        .cf_handle(&format!("q:{cf_name}"))
        .context("Raw quality CF")?;
    let mut key = [0; 24];
    key[..16].copy_from_slice(&encode_ric(&grid.ric)?);
    key[16..].copy_from_slice(&((grid.segments[0].0 as u64) * NS_PER_SEC).to_be_bytes());
    let end = (grid.segments.last().unwrap().1 as u64 + 60) * NS_PER_SEC;
    let mut chosen: BTreeMap<i64, ((u64, u64), Book10)> = BTreeMap::new();
    for item in db.iterator_cf(&cf, IteratorMode::From(&key, Direction::Forward)) {
        let (k, v) = item?;
        if k.len() != 24 || k[..16] != key[..16] {
            break;
        }
        let bucket = u64::from_be_bytes(k[16..].try_into()?);
        if bucket >= end {
            break;
        }
        if bucket % 15_000_000_000 != 0 {
            bail!("invalid Raw bucket");
        }
        let minute = (bucket / 60_000_000_000 * 60) as i64;
        if !minute_in_segments(minute, &grid.segments) {
            continue;
        }
        let value = decode_ll2_minute(&v)?;
        if value.source_ts_utc_ns / 15_000_000_000 * 15_000_000_000 != bucket {
            bail!("Raw source time outside bucket");
        }
        let mask = db.get_cf(&q, &k)?.context("missing Raw quality")?;
        let known = u64::from_le_bytes(
            mask.as_slice()
                .try_into()
                .context("invalid Raw quality length")?,
        );
        let order = value.ordering_tuple();
        if chosen.get(&minute).is_none_or(|(prior, _)| order > *prior) {
            chosen.insert(minute, (order, raw_book(&value, known)?));
        }
    }
    Ok(chosen.into_iter().map(|(k, (_, v))| (k, v)).collect())
}

pub(super) fn apply_size_buckets(
    rows: &mut [BaselineMinute],
    trades: &[BaselineTrade],
    multiplier: f64,
    thresholds: SizeThresholds,
) -> Result<()> {
    let mut buckets: BTreeMap<i64, SizeBuckets> = BTreeMap::new();
    for t in trades {
        buckets
            .entry((t.event_ns / 60_000_000_000 * 60) as i64)
            .or_default()
            .add(t.price * t.volume * multiplier, t.aggressor, thresholds)?;
    }
    for row in rows {
        if let Some(b) = buckets.get(&row.ts) {
            row.large_order = b.large_order;
            row.medium_order = b.medium_order;
            row.small_order = b.small_order;
            row.large_buy = b.large_buy;
            row.large_sell = b.large_sell;
            row.medium_buy = b.medium_buy;
            row.medium_sell = b.medium_sell;
            row.small_buy = b.small_buy;
            row.small_sell = b.small_sell;
            (row.net_buy_large, row.net_buy_medium, row.net_buy_small) = b.nets();
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct VisibleThresholds {
    pub values: SizeThresholds,
    // Only publication mode proves this bound. Exchange-time samples retain
    // the legacy behavior and must not claim source-time availability.
    pub source_max_published_ns: Option<u64>,
}

impl VisibleThresholds {
    pub fn for_open(self, open_ns: u64, basis: TimeBasis) -> Result<SizeThresholds> {
        if basis == TimeBasis::Publication
            && !self
                .source_max_published_ns
                .is_some_and(|latest| latest < open_ns)
        {
            bail!("size thresholds contain unknown or post-open source information");
        }
        Ok(self.values)
    }
}

type ThresholdMap = BTreeMap<(String, i32, u32), VisibleThresholds>;

pub(super) fn load_size_thresholds(
    db: &DB,
    args: &Args,
    products: &[ProductSpec],
    multipliers: &BTreeMap<String, f64>,
    calendar: &SessionCalendar,
) -> Result<ThresholdMap> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers.min(products.len()))
        .build()?;
    let results:Vec<Result<ThresholdMap>>=pool.install(||products.par_iter().map(|spec| {
        let mut map=BTreeMap::new();
        let mut target=args.start.with_day(1).context("month")?;
        while target<args.end {
            let source=previous_month_start(target)?;
            let mut samples=Vec::new();
            let mut latest_sample_ns=0;
            if let Some(history) = &calendar.history {
                for ((product,day),record) in &history.days {
                    if product != spec.product || *day < source || *day >= target { continue; }
                    let segments=calendar.segments_for_product(*spec,*day)?;
                    for contract in &record.contracts {
                        let grid=RicGrid {contract_id:contract.contract_id.clone(),ric:contract.ric.clone(),segments:segments.clone(),status_segments:history.status_segments(spec.product,&contract.ric,&segments)?};
                        for year in historical::source_years(&segments)? {
                            let cf_name=product_cf_name(year as u16,spec.product)?;
                            let cf=db.cf_handle(&cf_name).context("historical size source CF")?;
                            for t in scan_trades(db,&cf,&grid,args.trade_time_basis)? {
                                latest_sample_ns=latest_sample_ns.max(t.event_ns);
                                samples.push(t.price*t.volume*multipliers[spec.product]);
                            }
                        }
                    }
                }
            } else {
              let cf_name=product_cf_name(source.year() as u16,spec.product)?;
              let cf=db.cf_handle(&cf_name).context("size source CF")?;
              let spans=ric_spans(db,&cf_name,source,target,args.trade_time_basis)?;
              for (ric,_,_) in spans {
                // Reject indices and non-outright instruments before sampling.
                contract_id(&ric,*spec)?;
                let mut day=source;
                while day<target {
                    let segments=calendar.segments_for_product(*spec,day)?;
                    if !segments.is_empty() {
                        let grid=RicGrid {contract_id:String::new(),ric:ric.clone(),segments,status_segments:None};
                        for t in scan_trades(db,&cf,&grid,args.trade_time_basis)? {
                            latest_sample_ns=latest_sample_ns.max(t.event_ns);
                            samples.push(t.price*t.volume*multipliers[spec.product]);
                        }
                    }
                    day+=Duration::days(1);
                }
              }
            }
            samples.sort_unstable_by(f64::total_cmp);
            let p50=linear_percentile(&samples,0.5).context("no previous-month trades; refuse incomplete size buckets")?;
            let p90=linear_percentile(&samples,0.9).context("previous-month P90")?;
            let thresholds=SizeThresholds::new(p50,p90)?;
            let source_max_published_ns=(args.trade_time_basis==TimeBasis::Publication).then_some(latest_sample_ns);
            let audit=serde_json::json!({"product":spec.product,"chicago_trading_month":target.format("%Y-%m").to_string(),"threshold_source_month":source.format("%Y-%m").to_string(),"threshold_method":"previous month linear P50/P90","threshold_sample_trades":samples.len(),"p50":p50,"p90":p90,"implied_in_total":true,"special_excluded":true,"trade_time_basis":args.trade_time_basis,"source_max_published_ns":source_max_published_ns});
            write_json_atomic(&args.output_root.join("_audit/size_buckets").join(spec.product).join(format!("{}.json",target.format("%Y%m"))),&audit)?;
            eprintln!("baseline_size_threshold product={} month={} source={} trades={} p50={p50} p90={p90}",spec.product,target,source,samples.len());
            map.insert((spec.product.into(),target.year(),target.month()),VisibleThresholds { values:thresholds, source_max_published_ns });
            target=target.checked_add_months(Months::new(1)).context("next month")?;
        }
        Ok(map)
    }).collect());
    let mut combined = BTreeMap::new();
    for r in results {
        combined.extend(r?);
    }
    Ok(combined)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn size_thresholds_must_be_known_before_open() -> Result<()> {
        let values = SizeThresholds::new(1.0, 2.0)?;
        for (latest, accepted) in [
            (None, false),
            (Some(99), true),
            (Some(100), false),
            (Some(101), false),
        ] {
            let threshold = VisibleThresholds {
                values,
                source_max_published_ns: latest,
            };
            assert_eq!(
                threshold.for_open(100, TimeBasis::Publication).is_ok(),
                accepted
            );
        }
        Ok(())
    }
    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn overlay_memory(case: &serde_json::Value) -> Result<Vec<serde_json::Value>> {
        let mut selected: BTreeMap<Vec<u8>, ((u64, u64), Book10)> = BTreeMap::new();
        for s in case["snapshots"].as_array().unwrap() {
            let mut key = unhex(s["key_hex"].as_str().unwrap());
            let bucket = u64::from_be_bytes(key[16..].try_into()?);
            key[16..].copy_from_slice(&(bucket / 60_000_000_000 * 60_000_000_000).to_be_bytes());
            let value = decode_ll2_minute(&unhex(s["value_hex"].as_str().unwrap()))?;
            let order = value.ordering_tuple();
            if selected.get(&key).is_none_or(|(prior, _)| order > *prior) {
                selected.insert(
                    key,
                    (order, raw_book(&value, s["known"].as_u64().unwrap())?),
                );
            }
        }
        case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|source| {
                let mut row = source.clone();
                let mut key = encode_ric(row["ric"].as_str().unwrap())?.to_vec();
                key.extend((row["ts"].as_u64().unwrap() * NS_PER_SEC).to_be_bytes());
                if let Some((_, book)) = selected.get(&key) {
                    for i in 0..10 {
                        for (name, v) in [
                            (format!("bid{i}p"), book.bid_prices[i]),
                            (format!("bid{i}v"), book.bid_sizes[i]),
                            (format!("ask{i}p"), book.ask_prices[i]),
                            (format!("ask{i}v"), book.ask_sizes[i]),
                        ] {
                            row[&name] = serde_json::to_value(v)?;
                        }
                    }
                    row["mid_price"] = serde_json::to_value(book.mid())?;
                }
                Ok(row)
            })
            .collect()
    }

    #[test]
    fn raw_overlay_cpu_benchmark_matches_python_fixture() -> Result<()> {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_baseline_overlay.expected.json"
        ))?;
        for case in cases.as_array().unwrap() {
            assert_eq!(
                serde_json::to_value(overlay_memory(case)?)?,
                case["expected"]
            );
        }
        let started = Instant::now();
        for _ in 0..2000 {
            for case in cases.as_array().unwrap() {
                std::hint::black_box(overlay_memory(case)?);
            }
        }
        eprintln!("raw_overlay_cpu_benchmark iterations=2000 cases=4 excludes_db_io elapsed_seconds={:.6}",started.elapsed().as_secs_f64());
        Ok(())
    }

    #[test]
    fn raw_overlay_matches_independent_python_all_fields_and_invalidation() -> Result<()> {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_baseline_overlay.expected.json"
        ))?;
        let clock = Instant::now();
        for _ in 0..1 {
            for case in cases.as_array().unwrap() {
                let temp = tempfile::tempdir()?;
                let mut opt = Options::default();
                opt.create_if_missing(true);
                opt.create_missing_column_families(true);
                let db = DB::open_cf(&opt, temp.path(), ["s15:p:2024:ES", "q:s15:p:2024:ES"])?;
                for s in case["snapshots"].as_array().unwrap() {
                    db.put_cf(
                        &db.cf_handle("s15:p:2024:ES").unwrap(),
                        unhex(s["key_hex"].as_str().unwrap()),
                        unhex(s["value_hex"].as_str().unwrap()),
                    )?;
                    db.put_cf(
                        &db.cf_handle("q:s15:p:2024:ES").unwrap(),
                        unhex(s["key_hex"].as_str().unwrap()),
                        s["known"].as_u64().unwrap().to_le_bytes(),
                    )?;
                }
                for row in case["expected"].as_array().unwrap() {
                    let ts = row["ts"].as_i64().unwrap();
                    let grid = RicGrid {
                        contract_id: String::new(),
                        ric: row["ric"].as_str().unwrap().into(),
                        segments: vec![(ts, ts)],
                        status_segments: None,
                    };
                    let books = scan_raw_books(&db, "s15:p:2024:ES", &grid)?;
                    let book = books.get(&ts).cloned().unwrap_or_default();
                    for i in 0..10 {
                        for (name, v) in [
                            (format!("bid{i}p"), book.bid_prices[i]),
                            (format!("bid{i}v"), book.bid_sizes[i]),
                            (format!("ask{i}p"), book.ask_prices[i]),
                            (format!("ask{i}v"), book.ask_sizes[i]),
                        ] {
                            assert_eq!(
                                serde_json::to_value(v)?,
                                row[&name],
                                "{} {ts} {name}",
                                case["name"]
                            );
                        }
                    }
                    assert_eq!(serde_json::to_value(book.mid())?, row["mid_price"]);
                }
            }
        }
        eprintln!(
            "raw_overlay_regression elapsed_s={:.6}",
            clock.elapsed().as_secs_f64()
        );
        Ok(())
    }

    #[test]
    fn far_contract_keeps_raw_days_before_and_after_its_only_trade() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut opt = Options::default();
        opt.create_if_missing(true);
        opt.create_missing_column_families(true);
        let tas = DB::open_cf(&opt, temp.path().join("tas"), ["p:2026:AUL"])?;
        let raw = DB::open_cf(&opt, temp.path().join("raw"), ["s15:p:2026:AUL"])?;
        let ns = |day: u32| {
            Utc.with_ymd_and_hms(2026, 5, day, 12, 0, 0)
                .unwrap()
                .timestamp() as u64
                * NS_PER_SEC
        };
        let mut trade = cme_tas_replay::ali_h26_fixture();
        trade.ric = "AULZ26".into();
        trade.ts_utc_ns = ns(13);
        trade.exch_hms_ns = cme_tas_replay::MISSING_EXCH_HMS_NS;
        tas.put_cf(
            &tas.cf_handle("p:2026:AUL").unwrap(),
            encode_all_key(KIND_CME_TRADE, "AULZ26", ns(13), 0, 0)?,
            cme_tas_replay::encode_cme_trade(&trade)?,
        )?;
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_small.expected.json"
        ))?;
        let mut value = decode_ll2_minute(&unhex(
            fixture["snapshots"][0]["value_hex"].as_str().unwrap(),
        ))?;
        for day in [1, 29] {
            value.source_ts_utc_ns = ns(day);
            let mut key = [0; 24];
            key[..16].copy_from_slice(&encode_ric("AULZ26")?);
            key[16..].copy_from_slice(&ns(day).to_be_bytes());
            raw.put_cf(
                &raw.cf_handle("s15:p:2026:AUL").unwrap(),
                key,
                cme_tas_replay::ll2_1min::encode_ll2_minute(&value),
            )?;
        }
        let mut intervals = Vec::new();
        for d in 1..=29 {
            let day = NaiveDate::from_ymd_opt(2026, 5, d).unwrap();
            if day.weekday().num_days_from_monday() < 5 {
                let (start, end) = chicago_window(day)?;
                intervals.push(RawInterval {
                    start,
                    end: end - 3600,
                });
            }
        }
        let calendar = SessionCalendar {
            history: None,
            by_group: BTreeMap::from([("interest_rates".into(), intervals)]),
            coverage: BTreeMap::from([(
                "interest_rates".into(),
                (
                    NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 5, 31).unwrap(),
                ),
            )]),
        };
        let args = Args::parse_from([
            "baseline",
            "--start",
            "2026-05-01",
            "--end",
            "2026-06-01",
            "--products",
            "AUL",
            "--raw-ll2",
            "--tas-contract-grid",
            "--output-root",
            temp.path().join("output").to_str().unwrap(),
        ]);
        let jobs = list_tas_jobs(&tas, Some(&raw), &args, &[product_spec("AUL")?], &calendar)?;
        assert_eq!(jobs.len(), 21);
        assert_eq!(jobs[0].day, NaiveDate::from_ymd_opt(2026, 5, 1).unwrap());
        assert_eq!(
            jobs.last().unwrap().day,
            NaiveDate::from_ymd_opt(2026, 5, 29).unwrap()
        );
        assert!(jobs
            .iter()
            .any(|j| j.day == NaiveDate::from_ymd_opt(2026, 5, 20).unwrap()));
        Ok(())
    }

    #[test]
    fn rates_identity_and_chicago_trading_day_do_not_use_summary_aliases() -> Result<()> {
        assert_eq!(
            contract_id("AULH26", product_spec("AUL")?)?,
            "CBOT:AUL:2026-03"
        );
        assert_eq!(
            contract_id("FFZ27", product_spec("FF")?)?,
            "CBOT:FF:2027-12"
        );
        assert!(contract_id("AULH6", product_spec("AUL")?).is_err());
        assert_eq!(
            trading_day(1767308400000000000)?,
            NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()
        );
        Ok(())
    }
}
