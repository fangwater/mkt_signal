//! Build bounded CTA warm-up from official Tardis trades and incremental L2.
use anyhow::{bail, ensure, Context, Result};
use chrono::{Duration, NaiveDate};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use mkt_parsers::msg::mkt_msg::Level;
use mkt_signal::{
    common::amount_threshold::AmountThreshold,
    cta_perp::interval::BarInterval,
    factor_pub::{
        cta_perp_factor_model_1m_pub::{
            cfg::CtaPerpFactorPubConfig,
            seed::{BaselineSeed, BaselineSeedRow},
        },
        trade_flow_feature_pub::local_baseline::{
            BaselineBar, LocalBaselineAggregator, MultiMinuteBaseline,
        },
    },
};
use order_common::TradingVenue;
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    thresholds: PathBuf,
    #[arg(long)]
    source_root: PathBuf,
    #[arg(long)]
    start: NaiveDate,
    #[arg(long)]
    end: NaiveDate,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 6)]
    workers: usize,
    #[arg(long)]
    execute: bool,
}
type Csv = csv::Reader<MultiGzDecoder<BufReader<File>>>;
fn open(path: &Path, header: &str) -> Result<Csv> {
    let mut reader = csv::Reader::from_reader(MultiGzDecoder::new(BufReader::new(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    )));
    ensure!(
        reader.headers()?.iter().eq(header.split(',')),
        "unexpected Tardis columns"
    );
    Ok(reader)
}
fn next(reader: &mut Csv, symbol: &str) -> Result<Option<csv::StringRecord>> {
    let mut row = csv::StringRecord::new();
    if !reader.read_record(&mut row)? {
        return Ok(None);
    }
    ensure!(
        &row[0] == "binance-futures" && &row[1] == symbol,
        "Tardis source identity mismatch"
    );
    Ok(Some(row))
}
fn ts(row: &csv::StringRecord) -> Result<i64> {
    // The Kafka publisher normalizes both event types to milliseconds.
    Ok(row[2].parse::<i64>()?.div_euclid(1000) * 1000)
}
fn source(root: &Path, symbol: &str, kind: &str, day: NaiveDate) -> PathBuf {
    root.join(format!(
        "binance_usd_{}/{kind}/binance-futures_{kind}_{day}_{symbol}.csv.gz",
        symbol.to_lowercase()
    ))
}
fn append(
    rows: &mut Vec<BaselineSeedRow>,
    bar: BaselineBar,
    symbol: &str,
    interval: BarInterval,
) -> Result<()> {
    if [
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.vwap,
        bar.buy_vwap,
        bar.sell_vwap,
    ]
    .iter()
    .all(|v| v.is_finite() && *v > 0.)
    {
        let mut bar = bar;
        bar.start_ms += interval.millis();
        rows.push(BaselineSeedRow {
            payload: bar
                .to_trade_flow_feature_payload(symbol, TradingVenue::BinanceFutures.to_u8())?
                .to_vec(),
            depth: bar.full_depth,
        });
    }
    Ok(())
}
fn drain(
    agg: &mut LocalBaselineAggregator,
    multi: &mut MultiMinuteBaseline,
    rows: &mut Vec<BaselineSeedRow>,
    symbol: &str,
    interval: BarInterval,
) -> Result<()> {
    agg.drain_ten_second_bars();
    for minute in agg.drain_sixty_second_bars() {
        for bar in multi.push(&minute) {
            append(rows, bar, symbol, interval)?;
        }
    }
    Ok(())
}
fn build(args: &Args, symbol: &str, threshold: &Value, interval: BarInterval) -> Result<Value> {
    let started = Instant::now();
    let threshold = AmountThreshold {
        medium_notional_threshold: threshold["medium_notional_threshold"]
            .as_f64()
            .context("missing medium threshold")?,
        large_notional_threshold: threshold["large_notional_threshold"]
            .as_f64()
            .context("missing large threshold")?,
    };
    ensure!(
        threshold.is_online(),
        "invalid real order-size thresholds for {symbol}"
    );
    let mut agg = LocalBaselineAggregator::new();
    agg.enable_full_depth();
    let mut multi = MultiMinuteBaseline::new(interval.millis());
    let mut rows = Vec::new();
    let mut inputs = Vec::new();
    let mut day = args.start;
    let mut trade_rows = 0u64;
    let mut book_rows = 0u64;
    while day <= args.end {
        let tp = source(&args.source_root, symbol, "trades", day);
        let bp = source(&args.source_root, symbol, "incremental_book_L2", day);
        let before = [fs::metadata(&tp)?, fs::metadata(&bp)?];
        let mut trades = open(
            &tp,
            "exchange,symbol,timestamp,local_timestamp,id,side,price,amount",
        )?;
        let mut books = open(
            &bp,
            "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount",
        )?;
        let mut trade = next(&mut trades, symbol)?;
        let mut book = next(&mut books, symbol)?;
        while trade.is_some() || book.is_some() {
            let tt = trade.as_ref().map(ts).transpose()?.unwrap_or(i64::MAX);
            let bt = book.as_ref().map(ts).transpose()?.unwrap_or(i64::MAX);
            if bt <= tt {
                let first = book.take().unwrap();
                let snapshot = match &first[4] {
                    "true" => true,
                    "false" => false,
                    _ => bail!("invalid snapshot flag"),
                };
                let key = (
                    first[2].to_owned(),
                    first[3].to_owned(),
                    first[4].to_owned(),
                );
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                let mut row = first;
                loop {
                    let price = row[6].parse::<f64>()?;
                    let amount = row[7].parse::<f64>()?;
                    ensure!(
                        price.is_finite() && price > 0. && amount.is_finite() && amount >= 0.,
                        "invalid L2 level"
                    );
                    match &row[5] {
                        "bid" => bids.push(Level::from_values(price, amount)),
                        "ask" => asks.push(Level::from_values(price, amount)),
                        _ => bail!("invalid L2 side"),
                    };
                    book_rows += 1;
                    book = next(&mut books, symbol)?;
                    if book
                        .as_ref()
                        .is_some_and(|r| (r[2].to_owned(), r[3].to_owned(), r[4].to_owned()) == key)
                    {
                        row = book.take().unwrap();
                    } else {
                        break;
                    }
                }
                agg.on_book(bt, snapshot, &bids, &asks);
            } else {
                let row = trade.take().unwrap();
                let buy = match &row[5] {
                    "buy" => true,
                    "sell" => false,
                    _ => bail!("invalid trade side"),
                };
                let price = row[6].parse::<f64>()?;
                let amount = row[7].parse::<f64>()?;
                ensure!(
                    price.is_finite() && price > 0. && amount.is_finite() && amount > 0.,
                    "invalid trade"
                );
                agg.on_trade_with_threshold(tt, buy, price, amount, threshold);
                trade_rows += 1;
                trade = next(&mut trades, symbol)?;
            }
            drain(&mut agg, &mut multi, &mut rows, symbol, interval)?;
        }
        for (i, path) in [&tp, &bp].iter().enumerate() {
            let after = fs::metadata(path)?;
            ensure!(
                before[i].len() == after.len() && before[i].modified()? == after.modified()?,
                "source changed during replay"
            );
            inputs.push(json!({"path":path,"bytes":after.len(),"modified":format!("{:?}",after.modified()?)}));
        }
        day += Duration::days(1);
    }
    let end_ms = day
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    agg.flush_until_ms(end_ms);
    drain(&mut agg, &mut multi, &mut rows, symbol, interval)?;
    let path = args.output.join(format!("{symbol}.seed"));
    let count = rows.len();
    BaselineSeed {
        symbol: symbol.into(),
        bar_interval: interval,
        rows,
    }
    .write(&path)?;
    let verified = BaselineSeed::read(
        &path,
        interval,
        symbol,
        TradingVenue::BinanceFutures.to_u8(),
        end_ms,
    )?;
    ensure!(
        mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg::from_bytes(
            &verified.rows.last().unwrap().payload
        )?
        .ts == end_ms,
        "incomplete seed end"
    );
    let result = json!({"symbol":symbol,"bars":count,"trade_rows":trade_rows,"book_rows":book_rows,"late_trades":agg.stats()[0].late_trades,"end_ms":end_ms,"bytes":fs::metadata(&path)?.len(),"sha256":format!("{:x}",Sha256::digest(fs::read(&path)?)),"seconds":started.elapsed().as_secs_f64(),"sources":inputs});
    fs::write(
        args.output.join(format!("{symbol}.json")),
        serde_json::to_vec_pretty(&result)?,
    )?;
    println!(
        "{}",
        json!({"symbol":symbol,"bars":count,"seconds":started.elapsed().as_secs_f64()})
    );
    Ok(result)
}
fn main() -> Result<()> {
    let args = Args::parse();
    let config = CtaPerpFactorPubConfig::load(args.config.to_str().context("config path")?)?;
    ensure!(
        config.bar_interval.uses_linear_nq() && args.start <= args.end && args.workers > 0,
        "invalid bounded seed request"
    );
    let threshold: Value = serde_json::from_slice(&fs::read(&args.thresholds)?)?;
    for symbol in &config.universe.symbols {
        ensure!(
            threshold["thresholds"].get(symbol).is_some(),
            "missing threshold for {symbol}"
        );
        let mut day = args.start;
        while day <= args.end {
            for kind in ["trades", "incremental_book_L2"] {
                ensure!(
                    source(&args.source_root, symbol, kind, day).is_file(),
                    "missing {symbol} {day} {kind}"
                );
            }
            day += Duration::days(1);
        }
    }
    println!(
        "{}",
        json!({"execute":args.execute,"symbols":config.universe.symbols.len(),"start":args.start.to_string(),"end":args.end.to_string()})
    );
    if !args.execute {
        return Ok(());
    }
    fs::create_dir_all(&args.output)?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()?;
    let results: Result<Vec<_>> = config
        .universe
        .symbols
        .par_iter()
        .map(|symbol| {
            build(
                &args,
                symbol,
                &threshold["thresholds"][symbol],
                config.bar_interval,
            )
            .with_context(|| format!("seed {symbol}"))
        })
        .collect();
    let map: BTreeMap<_, _> = results?
        .into_iter()
        .map(|r| (r["symbol"].as_str().unwrap().to_string(), r))
        .collect();
    fs::write(
        args.output.join("manifest.json"),
        serde_json::to_vec_pretty(
            &json!({"start":args.start.to_string(),"end":args.end.to_string(),"bar_interval":config.bar_interval,"symbols":map,"thresholds_sha256":format!("{:x}",Sha256::digest(fs::read(&args.thresholds)?))}),
        )?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn raw_files_use_event_time_atomic_books_and_half_open_bars() {
        let root = std::env::temp_dir().join(format!("cta-tardis-seed-{}", uuid::Uuid::new_v4()));
        let day = NaiveDate::from_ymd_opt(2026, 10, 22).unwrap();
        let start = day
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let write = |kind: &str, text: String| {
            let p = source(&root, "BTCUSDT", kind, day);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            let mut g = flate2::write::GzEncoder::new(
                File::create(p).unwrap(),
                flate2::Compression::fast(),
            );
            g.write_all(text.as_bytes()).unwrap();
            g.finish().unwrap();
        };
        let t = |us: i64| start + us;
        write("trades",format!("exchange,symbol,timestamp,local_timestamp,id,side,price,amount\nbinance-futures,BTCUSDT,{},{},1,buy,100,1\nbinance-futures,BTCUSDT,{},{},2,sell,101,2\nbinance-futures,BTCUSDT,{},{},3,buy,200,3\nbinance-futures,BTCUSDT,{},{},4,sell,201,4\n",t(1_000_000),t(1_100_000),t(2_000_000),t(2_100_000),t(900_000_000),t(901_000_000),t(901_000_000),t(902_000_000)));
        write("incremental_book_L2",format!("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\nbinance-futures,BTCUSDT,{},{},true,bid,99,10\nbinance-futures,BTCUSDT,{},{},true,ask,102,20\nbinance-futures,BTCUSDT,{},{},false,ask,102,0\nbinance-futures,BTCUSDT,{},{},false,ask,202,30\n",t(1_000_000),t(1_001_000),t(1_000_000),t(1_001_000),t(900_000_000),t(900_001_000),t(900_000_000),t(900_001_000)));
        let output = root.join("seed");
        fs::create_dir(&output).unwrap();
        let args = Args {
            config: PathBuf::new(),
            thresholds: PathBuf::new(),
            source_root: root.clone(),
            start: day,
            end: day,
            output: output.clone(),
            workers: 1,
            execute: true,
        };
        let result = build(
            &args,
            "BTCUSDT",
            &json!({"medium_notional_threshold":100.,"large_notional_threshold":200.}),
            BarInterval::FifteenMinutes,
        )
        .unwrap();
        assert_eq!(result["late_trades"], 0);
        let seed = BaselineSeed::read(
            &output.join("BTCUSDT.seed"),
            BarInterval::FifteenMinutes,
            "BTCUSDT",
            TradingVenue::BinanceFutures.to_u8(),
            start / 1000 + 86_400_000,
        )
        .unwrap();
        assert_eq!(seed.rows.len(), 96);
        let first = mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg::from_bytes(
            &seed.rows[0].payload,
        )
        .unwrap();
        assert_eq!(first.ts, start / 1000 + 900_000);
        assert_eq!(&first.values[..6], &[100., 101., 100., 101., 3., 302.]);
        assert_eq!(first.values[14], 202.);
        assert_eq!(first.values[15], 100.);
        assert_eq!(first.values[72], 102.);
        assert_eq!(
            seed.rows[0].depth.as_ref().unwrap().depth_ask_notional_all,
            2040.
        );
        let second = mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg::from_bytes(
            &seed.rows[1].payload,
        )
        .unwrap();
        assert_eq!(second.values[0], 200.);
        assert_eq!(second.values[3], 201.);
        fs::remove_dir_all(root).unwrap();
    }
}
