//! Compile-only on development/intranet hosts. Execution needs user authorization
//! of an external host. No CTP SDK is linked by the default ZMQ-only build.
use crate::msg::{day, timestamp, MarketDataMsg};
use anyhow::{Context, Result};
use cn_futures::{
    ctp::market_data::{self, Feed, FeedEvent},
    placement::Placement,
};
use std::{path::Path, time::Instant};

pub struct CtpSource {
    feed: Feed,
    // Drop the SDK/feed before restoring its CPU/NUMA domains.
    _placement: Placement,
    generation: u64,
}
fn text(s: &[u8]) -> &[u8] {
    &s[..s.iter().position(|b| *b == 0).unwrap_or(s.len())]
}
fn price(value: f64) -> f64 {
    if value.is_finite() && value.abs() < 1e100 {
        value
    } else {
        f64::NAN
    }
}
impl CtpSource {
    pub fn new(path: &Path) -> Result<Self> {
        let config = market_data::Config::load(path)?;
        let placement = Placement::enter(&config.placement)?;
        let feed = Feed::standalone(&config, &placement)?;
        Ok(Self {
            feed,
            _placement: placement,
            generation: 0,
        })
    }
}
impl super::Source for CtpSource {
    fn poll(&mut self) -> Result<Option<MarketDataMsg>> {
        let Some(event) = self.feed.poll(Instant::now())? else {
            return Ok(None);
        };
        let tick = match event {
            FeedEvent::Connected { generation } | FeedEvent::Ready { generation } => {
                self.generation = generation;
                return Ok(Some(MarketDataMsg::reset(generation)));
            }
            FeedEvent::Disconnected { .. } => {
                return Ok(Some(MarketDataMsg::reset(self.generation)))
            }
            FeedEvent::Tick(tick) => tick,
        };
        let mut msg = MarketDataMsg {
            received_us: tick.received_us,
            source_generation: tick.connection_generation,
            ..MarketDataMsg::default()
        };
        msg.set_symbol(text(&tick.symbol))?;
        msg.set_venue(tick.venue.as_str())?;
        msg.trading_day = day(text(&tick.trading_day))?;
        msg.action_day = day(text(&tick.action_day))?;
        msg.exchange_ts_us = timestamp(
            msg.action_day,
            text(&tick.update_time),
            tick.millisec
                .try_into()
                .context("negative CTP MD millisecond")?,
        )?;
        msg.last_price = price(tick.last_price);
        msg.turnover = price(tick.turnover);
        msg.open_interest = price(tick.open_interest);
        msg.volume = tick
            .volume
            .try_into()
            .context("negative CTP cumulative volume")?;
        for i in 0..5 {
            msg.bid[i].price = price(tick.bid[i]);
            msg.ask[i].price = price(tick.ask[i]);
            msg.bid[i].quantity = tick.bid_volume[i]
                .try_into()
                .context("negative CTP bid volume")?;
            msg.ask[i].quantity = tick.ask_volume[i]
                .try_into()
                .context("negative CTP ask volume")?;
        }
        Ok(Some(msg))
    }
}
