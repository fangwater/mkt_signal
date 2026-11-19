//! Match server minutes to independently captured ZMQ minutes, then keep the live tail.
use super::{
    config::Config,
    engine::{Bars, Engine, Ledger},
    extras::Extras,
    history::{Dominant, History},
    live::{Event, Input},
};
use anyhow::{ensure, Context, Result};
use cn_futures_pbs::msg::MarketDataMsg;
use cn_market_proto::MinuteBar;
use std::collections::VecDeque;

const MAX_QUOTES: usize = 200_000;
#[derive(Default)]
pub struct Buffer {
    quotes: VecDeque<MarketDataMsg>,
}
impl Buffer {
    pub fn clear(&mut self) {
        self.quotes.clear();
    }
    pub fn push(&mut self, event: &Event, main: &Dominant) -> Result<()> {
        match event {
            Event::Reset => self.clear(),
            Event::Quote(q)
                if q.symbol() == main.contract.as_bytes() && q.trading_day == main.trading_day =>
            {
                ensure!(
                    self.quotes.len() < MAX_QUOTES,
                    "pending ZMQ buffer exhausted"
                );
                self.quotes.push_back(**q);
            }
            Event::Quote(_) => {}
        }
        Ok(())
    }
    pub fn has(&self) -> bool {
        !self.quotes.is_empty()
    }
    pub fn take(&mut self) -> Vec<MarketDataMsg> {
        self.quotes.drain(..).collect()
    }
}
fn number(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan())
        || (a.is_finite() && b.is_finite() && (a - b).abs() <= a.abs().max(b.abs()).max(1.) * 1e-10)
}
fn optional(a: Option<f64>, b: Option<f64>) -> bool {
    number(a.unwrap_or(f64::NAN), b.unwrap_or(f64::NAN))
}
/// Compare precisely the native minute inputs consumed by the hourly engine.
/// A timestamp alone cannot prove that missing intervals or fields were repaired.
pub fn same_minute(a: &MinuteBar, b: &MinuteBar) -> bool {
    if a.open_time_ms != b.open_time_ms
        || a.trading_day != b.trading_day
        || a.snapshot_trades != b.snapshot_trades
    {
        return false;
    }
    let values = |r: &MinuteBar| {
        [
            r.open,
            r.snapshot_high,
            r.snapshot_low,
            r.close,
            r.volume_lots,
            r.turnover_cny,
            r.vwap,
            r.buy_volume_lots,
            r.sell_volume_lots,
            r.buy_turnover_cny,
            r.sell_turnover_cny,
            r.buy_snapshot_trades,
            r.sell_snapshot_trades,
            r.net_buy_turnover_cny,
            r.net_buy_volume_lots,
        ]
    };
    if !values(a)
        .into_iter()
        .zip(values(b))
        .all(|(a, b)| number(a, b))
        || !optional(a.mid_price, b.mid_price)
        || !optional(a.buy_vwap, b.buy_vwap)
        || !optional(a.sell_vwap, b.sell_vwap)
    {
        return false;
    }
    let Some((sa, sb)) = a.size_buckets.as_ref().zip(b.size_buckets.as_ref()) else {
        return false;
    };
    let sizes = |s: &cn_market_proto::SizeBuckets| {
        [
            s.large_turnover_cny,
            s.medium_turnover_cny,
            s.small_turnover_cny,
            s.large_buy_cny,
            s.large_sell_cny,
            s.medium_buy_cny,
            s.medium_sell_cny,
            s.small_buy_cny,
            s.small_sell_cny,
            s.net_buy_large_cny,
            s.net_buy_medium_cny,
            s.net_buy_small_cny,
        ]
    };
    if !sizes(sa)
        .into_iter()
        .zip(sizes(sb))
        .all(|(a, b)| number(a, b))
    {
        return false;
    }
    let Some((ba, bb)) = a.book.as_ref().zip(b.book.as_ref()) else {
        return false;
    };
    ba.bids.len() == 5
        && ba.asks.len() == 5
        && bb.bids.len() == 5
        && bb.asks.len() == 5
        && ba
            .bids
            .iter()
            .chain(&ba.asks)
            .zip(bb.bids.iter().chain(&bb.asks))
            .all(|(a, b)| optional(a.price, b.price) && optional(a.volume_lots, b.volume_lots))
}
pub struct Recovered {
    pub engine: Engine,
    pub hours: Bars,
    pub extras: Extras,
    pub anchor_ms: i64,
    pub history_rows: usize,
}
pub(super) fn warm_hfq(
    archive: &super::hfq::History,
    main: &Dominant,
    context: &super::live::Context,
    engine: &mut Engine,
    hours: &mut Bars,
    extras: &mut Extras,
) -> Result<()> {
    ensure!(
        context.ends_after(archive.cutoff),
        "HFQ archive ends inside a session"
    );
    for row in &archive.rows {
        extras.push_hfq(row.clone(), archive.multiple, main, context)?;
        for hour in hours.push_hfq(row.clone(), archive.multiple)? {
            engine.observe(&hour, &main.contract, archive.multiple, false)?;
        }
    }
    if let Some(hour) = hours.finish(archive.multiple)? {
        engine.observe(&hour, &main.contract, archive.multiple, false)?;
    }
    Ok(())
}
pub fn recover(
    cfg: &Config,
    main: &Dominant,
    history: &History,
    ledger: Ledger,
    input: &Input,
    local: &[MinuteBar],
) -> Result<Recovered> {
    ensure!(
        input.context.snapshot_as_of == history.session_snapshot,
        "gRPC minute/session snapshot differs"
    );
    let multiple = history.multiple()?;
    // This boundary belongs to the ZMQ generation, not to gRPC's progress.
    // Keep every later local minute while the delayed server catches this one.
    let first = local
        .first()
        .context("waiting for the first complete ZMQ minute")?;
    let anchor = first.open_time_ms;
    let remote = history
        .minutes
        .get(&anchor)
        .with_context(|| format!("waiting for gRPC to reach first ZMQ minute {anchor}"))?;
    ensure!(
        !remote.provisional || anchor + 60_000 <= history.through_ms,
        "gRPC has not finalized first ZMQ minute {anchor}"
    );
    ensure!(
        same_minute(remote, first),
        "gRPC/first ZMQ minute inputs differ at {anchor}"
    );
    ensure!(
        history
            .minutes
            .range(..=anchor)
            .all(|(_, r)| !r.provisional || r.open_time_ms + 60_000 <= history.through_ms),
        "gRPC prefix contains unfinished minutes"
    );
    let mut engine = Engine::new(cfg, ledger.clone())?;
    let mut hours = Bars::default();
    let mut extras = Extras::new(cfg)?;
    let extra_ledgers = extras.ledgers();
    let mut count = 0;
    if let Some(archive) = &history.hfq {
        ensure!(
            multiple == archive.multiple,
            "HFQ/native multipliers differ"
        );
        ensure!(
            anchor > archive.cutoff,
            "waiting for ZMQ beyond the HFQ archive"
        );
        warm_hfq(
            archive,
            main,
            &input.context,
            &mut engine,
            &mut hours,
            &mut extras,
        )?;
        count = archive.rows.len();
    }
    for row in history
        .minutes
        .range(..=anchor)
        .filter(|(ts, _)| {
            history
                .hfq
                .as_ref()
                .is_none_or(|archive| **ts > archive.cutoff)
        })
        .map(|(_, r)| r.clone())
        .chain(local.iter().filter(|r| r.open_time_ms > anchor).cloned())
    {
        count += 1;
        extras.push(row.clone(), multiple, main, &input.context, None, false)?;
        for hour in hours.push(row, multiple)? {
            engine.observe(&hour, &main.contract, multiple, false)?;
        }
    }
    if hours
        .last_minute_ms()
        .is_some_and(|last| input.context.ends_after(last))
    {
        if let Some(hour) = hours.finish(input.multiple)? {
            engine.observe(&hour, &main.contract, input.multiple, false)?;
        }
    }
    ensure!(
        !cfg.enabled || engine.ready(),
        "hourly formulas are not ready after gRPC/ZMQ catch-up"
    );
    ensure!(
        engine.ledger == ledger,
        "catch-up changed factual positions"
    );
    ensure!(
        extras.ledgers() == extra_ledgers,
        "catch-up changed minute-frequency ledgers"
    );
    Ok(Recovered {
        engine,
        hours,
        extras,
        anchor_ms: anchor,
        history_rows: count,
    })
}
