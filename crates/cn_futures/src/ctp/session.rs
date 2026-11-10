//! Correlated, throttled bootstrap. Two quiet identical position/order/trade passes
//! are required; this is snapshot convergence, not a vendor-provided atomic barrier.
use super::*;
use crate::config::Config;
use anyhow::{ensure, Context, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct Session {
    config: Config,
    investor: String,
    next_kind: Option<i32>,
    waiting: Option<(i32, i32, Instant)>,
    next_send: Instant,
    login: Option<(String, i32, i32, u64)>,
    instruments: HashMap<String, Instrument>,
    available: Option<f64>,
    orders: Vec<OrderUpdate>,
    trades: Vec<Fill>,
    positions: HashMap<(String, Side, i32), PositionSnapshot>,
    revision: u64,
    pass_revision: u64,
    previous: Option<(String, f64)>,
    passes: usize,
    ready: bool,
    maintenance: bool,
    last_cash_poll: Instant,
    last_rule_poll: Instant,
    cash_update: Option<f64>,
    rule_update: Option<Vec<Instrument>>,
}
impl Session {
    pub fn new(config: Config, investor: String, now: Instant) -> Self {
        Self {
            config,
            investor,
            next_kind: None,
            waiting: None,
            next_send: now,
            login: None,
            instruments: HashMap::new(),
            available: None,
            orders: Vec::new(),
            trades: Vec::new(),
            positions: HashMap::new(),
            revision: 0,
            pass_revision: 0,
            previous: None,
            passes: 0,
            ready: false,
            maintenance: false,
            last_cash_poll: now,
            last_rule_poll: now,
            cash_update: None,
            rule_update: None,
        }
    }
    pub fn ready(&self) -> bool {
        self.ready
    }
    pub fn login_identity(&self) -> Option<(i32, i32)> {
        self.login.as_ref().map(|(_, f, s, _)| (*f, *s))
    }
    pub fn take_cash_update(&mut self) -> Option<f64> {
        self.cash_update.take()
    }
    pub fn take_rule_update(&mut self) -> Option<Vec<Instrument>> {
        self.rule_update.take()
    }
    pub fn request_due(&mut self, now: Instant) -> Result<Option<i32>> {
        if let Some((_, _, sent)) = self.waiting {
            ensure!(
                now.saturating_duration_since(sent)
                    <= Duration::from_millis(self.config.response_timeout_ms),
                "CTP response timeout; session must be recovered"
            );
            return Ok(None);
        }
        if self.ready && self.next_kind.is_none() {
            if now.saturating_duration_since(self.last_rule_poll) >= Duration::from_secs(60) {
                self.maintenance = true;
                self.next_kind = Some(INSTRUMENT);
                self.instruments.clear();
            } else if now.saturating_duration_since(self.last_cash_poll) >= Duration::from_secs(5) {
                self.maintenance = true;
                self.next_kind = Some(ACCOUNT);
                self.available = None;
            }
        }
        Ok(if now >= self.next_send {
            self.next_kind
        } else {
            None
        })
    }
    pub fn sent(&mut self, kind: i32, id: i32, code: i32, now: Instant) -> Result<()> {
        ensure!(
            self.next_kind == Some(kind) && self.waiting.is_none(),
            "unexpected CTP control dispatch"
        );
        self.next_send = now + Duration::from_millis(self.config.query_interval_ms);
        match code {
            0 => {
                if kind == ACCOUNT {
                    self.pass_revision = self.revision;
                }
                self.waiting = Some((kind, id, now));
            }
            -2 | -3 => {} // Documented CTP in-flight/rate-limit rejection; retry with the configured spacing.
            _ => anyhow::bail!("CTP control request rejected locally ({code})"),
        }
        Ok(())
    }
    pub fn restart_queries(&mut self, now: Instant) -> Result<()> {
        ensure!(
            self.login.is_some() && self.waiting.is_none(),
            "cannot start reconciliation before login or while a query is in flight"
        );
        self.ready = false;
        self.maintenance = false;
        self.cash_update = None;
        self.rule_update = None;
        self.previous = None;
        self.passes = 0;
        self.instruments.clear();
        self.clear_pass();
        self.next_kind = Some(INSTRUMENT);
        self.next_send = now;
        Ok(())
    }
    fn clear_pass(&mut self) {
        self.available = None;
        self.orders.clear();
        self.trades.clear();
        self.positions.clear();
    }
    fn owns(&self, e: &RawEvent) -> Result<bool> {
        Ok(text(&e.investor)? == self.investor && text(&e.exchange)? == "CFFEX")
    }

    pub fn on_event(&mut self, e: &RawEvent, now: Instant) -> Result<Option<Snapshot>> {
        if e.kind == CONNECTED {
            self.ready = false;
            self.maintenance = false;
            self.cash_update = None;
            self.rule_update = None;
            self.login = None;
            self.waiting = None;
            self.previous = None;
            self.passes = 0;
            self.instruments.clear();
            self.clear_pass();
            self.next_kind = Some(if self.config.require_authentication {
                AUTHENTICATED
            } else {
                LOGGED_IN
            });
            self.next_send = now;
            return Ok(None);
        }
        if e.kind == DISCONNECTED {
            self.ready = false;
            self.next_kind = None;
            self.waiting = None;
            self.login = None;
            return Ok(None);
        }
        if matches!(e.kind, ORDER | TRADE) {
            if self.owns(e)? {
                self.revision = self
                    .revision
                    .checked_add(1)
                    .context("event revision overflow")?;
            }
            return Ok(None);
        }
        let Some((kind, id, _)) = self.waiting else {
            return Ok(None);
        };
        if e.request != id {
            return Ok(None);
        } // Late response stays correlated to its original request.
        ensure!(
            e.error == 0,
            "CTP request {id} failed, kind={kind} error_code={}",
            e.error
        );
        ensure!(
            e.kind == kind,
            "CTP response kind does not match in-flight request"
        );
        match kind {
            LOGGED_IN => {
                ensure!(e.present != 0, "login success without response fields");
                let day = text(&e.trading_day)?;
                ensure!(day.len() == 8, "invalid login trading day");
                let order_ref = text(&e.max_order_ref)?;
                let max_ref = if order_ref.trim().is_empty() {
                    0
                } else {
                    order_ref
                        .trim()
                        .parse::<u64>()
                        .context("nonnumeric login MaxOrderRef")?
                };
                self.login = Some((day, e.front, e.session, max_ref));
            }
            INSTRUMENT if e.present != 0 => {
                let symbol = text(&e.symbol)?;
                if let Some(config) = self.config.symbols.iter().find(|s| s.symbol == symbol) {
                    ensure!(
                        text(&e.exchange)? == "CFFEX" && e.product_class == i32::from(b'1'),
                        "configured contract is not a CFFEX future"
                    );
                    let instrument = Instrument {
                        option: None,
                        venue: Venue::Cffex,
                        symbol: symbol.clone(),
                        price_tick: e.price_tick,
                        min_lots: e.minimum,
                        max_lots: e.maximum,
                        market_lots: None,
                        active: e.active != 0,
                        cash_budget_per_open_lot: config.cash_budget_per_open_lot,
                    };
                    instrument.validate()?;
                    if let Some(old) = self.instruments.insert(symbol, instrument.clone()) {
                        ensure!(old == instrument, "conflicting duplicate instrument row");
                    }
                }
            }
            ACCOUNT if e.present != 0 => {
                ensure!(
                    text(&e.investor)? == self.investor && text(&e.exchange)? == "CNY",
                    "unexpected CTP account/currency snapshot"
                );
                ensure!(e.available.is_finite(), "nonfinite CTP account available");
                let day = text(&e.trading_day)?;
                ensure!(
                    day.is_empty() || self.login.as_ref().is_some_and(|login| login.0 == day),
                    "account trading day changed; reconnect and reconcile"
                );
                ensure!(self.available.is_none(), "duplicate CNY account row");
                self.available = Some(e.available);
            }
            QUERY_ORDER if e.present != 0 && self.owns(e)? => {
                self.orders.push(e.to_order()?);
            }
            QUERY_TRADE if e.present != 0 && self.owns(e)? => {
                self.trades.push(e.to_fill()?);
            }
            POSITION if e.present != 0 && self.owns(e)? && e.volume != 0 => {
                ensure!(
                    e.hedge == i32::from(b'1'),
                    "non-speculative CFFEX position requires a separate hedge ledger"
                );
                ensure!(
                    e.volume >= 0 && e.today >= 0 && e.today <= e.volume,
                    "invalid CTP position row"
                );
                let side = match e.position_direction {
                    50 => Side::Buy,
                    51 => Side::Sell,
                    _ => anyhow::bail!("unsupported CFFEX position direction"),
                };
                let symbol = text(&e.symbol)?;
                // YdPosition is the opening historical inventory, not necessarily today's remaining yesterday position.
                let row = PositionSnapshot {
                    venue: Venue::Cffex,
                    symbol: symbol.clone(),
                    side,
                    today: e.today,
                    yesterday: e.volume - e.today,
                };
                ensure!(
                    self.positions
                        .insert((symbol, side, e.position_date), row)
                        .is_none(),
                    "duplicate position-date row; investment-unit split is not supported"
                );
            }
            _ => {}
        }
        ensure!(
            self.orders.len() <= 200_000
                && self.trades.len() <= 400_000
                && self.positions.len() <= 20_000,
            "CTP bootstrap snapshot capacity exceeded"
        );
        if e.last == 0 {
            return Ok(None);
        }
        self.waiting = None;
        if self.maintenance {
            match kind {
                ACCOUNT => {
                    self.cash_update = Some(self.available.context("empty CNY account poll")?);
                    self.last_cash_poll = now;
                }
                INSTRUMENT => {
                    self.rule_update = Some(self.instruments.values().cloned().collect());
                    self.last_rule_poll = now;
                }
                _ => anyhow::bail!("unexpected maintenance request"),
            }
            self.maintenance = false;
            self.next_kind = None;
            return Ok(None);
        }
        self.next_kind = Some(match kind {
            AUTHENTICATED => LOGGED_IN,
            LOGGED_IN => SETTLED,
            SETTLED => INSTRUMENT,
            INSTRUMENT => {
                ensure!(
                    self.instruments.len() == self.config.symbols.len(),
                    "complete instrument response omitted an allowlisted contract"
                );
                self.clear_pass();
                ACCOUNT
            }
            ACCOUNT => {
                ensure!(self.available.is_some(), "empty CNY account snapshot");
                QUERY_ORDER
            }
            QUERY_ORDER => QUERY_TRADE,
            QUERY_TRADE => POSITION,
            POSITION => {
                return self.finish_pass(now);
            }
            _ => anyhow::bail!("unexpected bootstrap stage"),
        });
        Ok(None)
    }

    fn finish_pass(&mut self, now: Instant) -> Result<Option<Snapshot>> {
        self.passes += 1;
        ensure!(
            self.passes <= 12,
            "account snapshot did not converge; keep new orders blocked"
        );
        let mut positions = HashMap::<(String, Side), PositionSnapshot>::new();
        for row in self.positions.values() {
            let p = positions
                .entry((row.symbol.clone(), row.side))
                .or_insert_with(|| PositionSnapshot {
                    venue: Venue::Cffex,
                    symbol: row.symbol.clone(),
                    side: row.side,
                    today: 0,
                    yesterday: 0,
                });
            p.today = p
                .today
                .checked_add(row.today)
                .context("position sum overflow")?;
            p.yesterday = p
                .yesterday
                .checked_add(row.yesterday)
                .context("position sum overflow")?;
        }
        let mut positions: Vec<_> = positions.into_values().collect();
        positions.sort_by_key(|p| {
            (
                p.symbol.clone(),
                match p.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
            )
        });
        self.orders
            .sort_by_key(|o| (o.key.session.clone(), o.key.order_ref.clone()));
        self.trades.sort_by_key(|f| {
            (
                f.identity.symbol.clone(),
                f.identity.trade_id.clone(),
                match f.identity.side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                },
            )
        });
        let signature = serde_json::to_string(&(&positions, &self.orders, &self.trades))
            .context("encode snapshot signature")?;
        let available = self.available.context("missing available cash")?;
        if self.revision == self.pass_revision {
            if let Some((previous, previous_cash)) = &self.previous {
                if previous == &signature {
                    let (day, front, session, max_ref) =
                        self.login.clone().context("missing login state")?;
                    self.ready = true;
                    self.next_kind = None;
                    self.last_cash_poll = now;
                    self.last_rule_poll = now;
                    return Ok(Some(Snapshot {
                        trading_day: day,
                        session: session_key(front, session),
                        max_order_ref: max_ref,
                        available_cash: available.min(*previous_cash),
                        instruments: self.instruments.values().cloned().collect(),
                        positions,
                        orders: self.orders.clone(),
                        trades: self.trades.clone(),
                    }));
                }
            }
            self.previous = Some((signature, available));
        } else {
            self.previous = None;
        }
        self.clear_pass();
        self.next_kind = Some(ACCOUNT);
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
