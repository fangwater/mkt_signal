use super::*;
use crate::core::TradingCore;
use anyhow::{ensure, Context, Result};
use std::collections::{HashMap, HashSet};

/// Only consumes copied native records. notifyFinishInit is not a recovery barrier;
/// a snapshot emitted inside notifyCaughtUp is required for each login.
#[derive(Default)]
pub struct Session {
    snapshot: Option<Snapshot>,
    frozen: HashMap<(Venue, String, Side), (i32, i32)>,
    seen_positions: HashSet<(Venue, String, Side, i32)>,
    poisoned: bool,
    logged_in: bool,
    initialized: bool,
    pub ready: bool,
}
impl Session {
    pub fn on_event(
        &mut self,
        e: &RawEvent,
        config: &config::Config,
        key: Option<OrderKey>,
    ) -> Result<Option<Snapshot>> {
        let result = self.process(e, config, key);
        if result.is_err() {
            self.ready = false;
            self.snapshot = None;
            self.poisoned = true;
        }
        result
    }
    fn process(
        &mut self,
        e: &RawEvent,
        config: &config::Config,
        key: Option<OrderKey>,
    ) -> Result<Option<Snapshot>> {
        if self.poisoned && !matches!(e.kind, CONNECTED | DISCONNECTED) {
            return Ok(None);
        }
        ensure!(
            !(self.snapshot.is_some()
                && matches!(e.kind, ORDER | TRADE | ACCOUNT)
                && e.replay == 0),
            "live native update interleaved with caught-up snapshot; recover again"
        );
        match e.kind {
            CONNECTED | DISCONNECTED => {
                self.poisoned = false;
                self.ready = false;
                self.logged_in = false;
                self.snapshot = None;
            }
            LOGIN => {
                ensure!(
                    e.error == 0 && e.flag == 0,
                    "native login failed or monitor login; code={}",
                    e.error
                );
                self.logged_in = true;
            }
            INIT => self.initialized = true,
            BEGIN => {
                ensure!(
                    self.initialized && self.logged_in && self.snapshot.is_none(),
                    "snapshot before native init/login or nested snapshot"
                );
                self.ready = false;
                self.frozen.clear();
                self.seen_positions.clear();
                self.snapshot = Some(Snapshot {
                    trading_day: e.day.to_string(),
                    session: session_key(e.day),
                    max_order_ref: e
                        .reference
                        .try_into()
                        .context("negative native maximum order ref")?,
                    available_cash: f64::NAN,
                    instruments: vec![],
                    positions: vec![],
                    orders: vec![],
                    trades: vec![],
                });
            }
            END => {
                let s = self.snapshot.take().context("snapshot end without begin")?;
                ensure!(
                    s.trading_day == e.day.to_string(),
                    "trading day changed during snapshot"
                );
                ensure!(
                    s.instruments.len() == config.symbols.len(),
                    "native catalog missing configured contract"
                );
                // Native frozen positions must equal the reservations reconstructed from
                // accepted orders. This also detects interleaving/incomplete replay.
                let mut check = TradingCore::new(config.risk.clone(), scope())?;
                check.restore(s.clone())?;
                for p in check.view().positions {
                    let expected = self
                        .frozen
                        .remove(&(p.venue, p.symbol.to_owned(), p.side))
                        .unwrap_or_default();
                    ensure!(
                        (p.position.frozen_today, p.position.frozen_yesterday) == expected,
                        "native frozen inventory disagrees with replayed working orders"
                    );
                }
                ensure!(
                    self.frozen.values().all(|x| *x == (0, 0)),
                    "native frozen inventory absent from reconstructed positions"
                );
                self.ready = true;
                return Ok(Some(s));
            }
            ACCOUNT => {
                ensure!(
                    e.login_count == 1 && (0..=2).contains(&e.right),
                    "dedicated trader account requires exactly one login and known permissions"
                );
                ensure!(e.available.is_finite(), "nonfinite native usable cash");
                if let Some(s) = &mut self.snapshot {
                    ensure!(
                        !s.available_cash.is_finite(),
                        "duplicate native snapshot account"
                    );
                    s.available_cash = e.available;
                }
            }
            INSTRUMENT => {
                if let Some(s) = &mut self.snapshot {
                    let venue = e.venue()?;
                    let symbol = text(&e.symbol)?;
                    if let Some(c) = config
                        .symbols
                        .iter()
                        .find(|c| c.venue == venue && c.symbol == symbol)
                    {
                        ensure!(
                            e.product_class == 1 && e.expiry > 0 && (0..=2).contains(&e.right),
                            "invalid native contract class/expiry/permissions"
                        );
                        ensure!(
                            !s.instruments
                                .iter()
                                .any(|r| r.venue == venue && r.symbol == symbol),
                            "duplicate native catalog contract"
                        );
                        s.instruments.push(Instrument {
                            venue,
                            symbol,
                            price_tick: e.tick,
                            min_lots: e.minimum,
                            max_lots: e.maximum,
                            active: e.expiry >= e.day && e.right != 2,
                            cash_budget_per_open_lot: c.cash_budget_per_open_lot,
                        });
                    }
                }
            }
            POSITION => {
                let s = self
                    .snapshot
                    .as_mut()
                    .context("position row outside caught-up snapshot")?;
                let venue = e.venue()?;
                let symbol = text(&e.symbol)?;
                ensure!(
                    scope().venues.contains(&venue),
                    "account holds positions outside SHFE/INE scope"
                );
                ensure!(
                    e.hedge == 1
                        && e.product_class == 1
                        && e.lots >= 0
                        && e.frozen >= 0
                        && e.frozen <= e.lots,
                    "unsupported/invalid native position: venue={} symbol={} product_class={} hedge={} lots={} frozen={}",
                    venue.as_str(), symbol, e.product_class, e.hedge, e.lots, e.frozen
                );
                let side = match e.position_direction {
                    2 => Side::Buy,
                    3 => Side::Sell,
                    _ => anyhow::bail!("invalid native position direction"),
                };
                ensure!(
                    self.seen_positions
                        .insert((venue, symbol.clone(), side, e.date)),
                    "duplicate native position date row"
                );
                let index = s
                    .positions
                    .iter()
                    .position(|p| p.venue == venue && p.symbol == symbol && p.side == side);
                let p = match index {
                    Some(i) => &mut s.positions[i],
                    None => {
                        s.positions.push(PositionSnapshot {
                            venue,
                            symbol: symbol.clone(),
                            side,
                            today: 0,
                            yesterday: 0,
                        });
                        s.positions.last_mut().unwrap()
                    }
                };
                let frozen = self.frozen.entry((venue, symbol, side)).or_default();
                match e.date {
                    1 => {
                        ensure!(p.today == 0 && frozen.0 == 0, "duplicate today position");
                        p.today = e.lots;
                        frozen.0 = e.frozen;
                    }
                    2 => {
                        ensure!(
                            p.yesterday == 0 && frozen.1 == 0,
                            "duplicate history position"
                        );
                        p.yesterday = e.lots;
                        frozen.1 = e.frozen;
                    }
                    _ => anyhow::bail!("invalid native position date"),
                }
            }
            ORDER => {
                if let Some(s) = &mut self.snapshot {
                    s.orders
                        .push(e.to_order(key.context("missing order key")?)?);
                }
            }
            TRADE => {
                if let Some(s) = &mut self.snapshot {
                    s.trades.push(e.to_fill()?);
                }
            }
            FATAL => anyhow::bail!("native SDK fatal/missing-order event; stop and recover"),
            INVALIDATED if self.ready || self.snapshot.is_some() => {
                self.ready = false;
                anyhow::bail!("native permissions/account changed; reconnect and recover");
            }
            _ => {}
        }
        Ok(None)
    }
}
