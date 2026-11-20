//! Product HFQ archive for the startup factor window; native minutes prove Redis continuity.
use super::{config::Config, history::Dominant};
use anyhow::{ensure, Context, Result};
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, GetHfqMinuteBarsRequest, MinuteBar, TimeRange,
};
use std::time::Duration;

const PAGE_MS: i64 = 30 * 86_400_000;

pub struct History {
    pub generation: String,
    pub basis_day: u32,
    pub multiple: f64,
    pub rows: Vec<MinuteBar>,
    pub cutoff: i64,
}

pub async fn fetch(cfg: &Config, main: &Dominant, mut start: i64, end: i64) -> Result<History> {
    let mut client = tokio::time::timeout(
        Duration::from_secs(10),
        CnFuturesMarketClient::connect(cfg.history_grpc.clone()),
    )
    .await??
    .max_decoding_message_size(64 * 1024 * 1024);
    let mut generation = String::new();
    let mut basis_day = 0;
    let mut multiple = 0.;
    let mut gap = 0.;
    let mut rows: Vec<MinuteBar> = Vec::new();
    while start <= end {
        let until = (start + PAGE_MS - 1).min(end);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            client.get_hfq_minute_bars(GetHfqMinuteBarsRequest {
                exchange: cfg.exchange_codes()?.1 as i32,
                product: cfg.symbol.clone(),
                time: Some(TimeRange {
                    start_ms: start,
                    end_ms: until,
                }),
                generation: generation.clone(),
            }),
        )
        .await??
        .into_inner();
        if generation.is_empty() {
            ensure!(
                !result.generation.is_empty()
                    && result.basis_trading_day == main.source_trading_day
                    && result.last_trading_day == result.basis_trading_day,
                "HFQ archive is not current at dominant source day {}",
                main.source_trading_day
            );
            ensure!(
                result.volume_multiple.is_finite() && result.volume_multiple > 0.,
                "HFQ archive has no verified multiplier"
            );
            let archive_contract = super::history::native_contract(cfg, &result.last_contract)?;
            if archive_contract != main.contract {
                ensure!(
                    main.trading_day > result.basis_trading_day
                        && result
                            .scheduled_next
                            .as_deref()
                            .map(|contract| super::history::native_contract(cfg, contract))
                            .transpose()?
                            .as_deref()
                            == Some(main.contract.as_str()),
                    "current dominant is outside the HFQ price basis"
                );
                gap = result
                    .next_gap
                    .filter(|gap| gap.is_finite())
                    .context("scheduled HFQ rollover has no verified price adjustment")?;
            }
            generation = result.generation;
            basis_day = result.basis_trading_day;
            multiple = result.volume_multiple;
        } else {
            ensure!(
                result.generation == generation
                    && result.basis_trading_day == basis_day
                    && result.last_trading_day == basis_day
                    && result.volume_multiple == multiple,
                "HFQ archive changed during warm-up"
            );
        }
        for mut row in result.bars {
            ensure!(
                !row.provisional
                    && row.open_time_ms % 60_000 == 0
                    && (start..=until).contains(&row.open_time_ms)
                    && rows
                        .last()
                        .is_none_or(|last| last.open_time_ms < row.open_time_ms),
                "gRPC returned invalid or unordered HFQ minutes"
            );
            if gap != 0. {
                shift_prices(&mut row, gap);
            }
            rows.push(row);
        }
        start = until + 1;
    }
    let cutoff = rows
        .last()
        .filter(|last| last.trading_day == basis_day)
        .context("HFQ archive has no minutes from its final trading day")?
        .open_time_ms;
    Ok(History {
        generation,
        basis_day,
        multiple,
        rows,
        cutoff,
    })
}

fn shift_prices(row: &mut MinuteBar, gap: f64) {
    for price in [
        &mut row.open,
        &mut row.high,
        &mut row.low,
        &mut row.close,
        &mut row.snapshot_high,
        &mut row.snapshot_low,
        &mut row.vwap,
    ] {
        *price += gap;
    }
    for price in [
        &mut row.mid_price,
        &mut row.twap,
        &mut row.buy_vwap,
        &mut row.sell_vwap,
    ] {
        if let Some(price) = price {
            *price += gap;
        }
    }
    if let Some(book) = &mut row.book {
        for level in book.bids.iter_mut().chain(&mut book.asks) {
            if let Some(price) = &mut level.price {
                *price += gap;
            }
        }
    }
}
