//! Native Binance PM FR cash buying power, independent of portfolio margin headroom.
use crate::pre_trade::signal_throttle::SIGNAL_THROTTLE_ERROR_CODE_BALANCE_INSUFFICIENT;
use crate::pre_trade::{monitor_channel::MonitorChannel, QueryEngHub};
use bytes::Bytes;
use log::{info, warn};
use once_cell::sync::Lazy;
use order_common::{trade_error_code::binance, Side, TradingVenue};
use parking_lot::Mutex;
use persist_common::SignalBbo;
use runtime_common::exchange::Exchange;
use runtime_common::execution_backend::ExecBackend;
use runtime_common::time_util::get_timestamp_us;
use serde_json::Value;
use signal_common::common::align_price_floor;
use signal_common::tick_math::QuantizedValue;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use trade_engine::query_request::{GenericQueryRequest, QueryRequestType};

const POLL_INTERVAL_US: i64 = 30_000_000;
const MINIMUM_BORROW_REFRESH_INTERVAL_US: i64 = 10_000_000;
const MAX_AGE_US: i64 = 180_000_000;
const RETRY_US: i64 = 60_000_000;
const MAX_RETRY_US: i64 = 15 * RETRY_US;
const BUY_BUFFER: f64 = 1.001;
const USDT_MINIMUM_BORROW: f64 = 5.0;

#[derive(Debug, PartialEq)]
pub(crate) enum OpenFunding {
    AutoBorrow,
    CashOnly { max_notional: f64 },
}

struct SubmittedBuy {
    client_order_id: i64,
    sent_us: i64,
    reserved: f64,
}

#[derive(Clone, Copy)]
struct Snapshot {
    cash: f64,
    borrow: f64,
    started_us: i64,
}

#[derive(Default)]
struct FrequencyCooldown {
    retry_us: i64,
    delay_us: i64,
}

#[derive(Default)]
struct State {
    poll_us: i64,
    cash_id: Option<i64>,
    borrow_id: Option<i64>,
    cash: Option<f64>,
    borrow: Option<f64>,
    snapshot: Option<Snapshot>,
    // Do not reuse the same REST buying power for a burst of submissions.
    // Keep consumption until a later complete exchange snapshot covers it.
    submitted: VecDeque<SubmittedBuy>,
    borrow_failed_us: i64,
    borrow_retry_us: i64,
    cash_refresh_after_us: i64,
    frequency_cooldowns: HashMap<String, FrequencyCooldown>,
}

impl State {
    fn reserved(&self) -> f64 {
        self.submitted.iter().map(|buy| buy.reserved).sum()
    }

    fn fresh(&self, now: i64) -> Option<Snapshot> {
        self.snapshot
            .filter(|s| now >= s.started_us && now - s.started_us <= MAX_AGE_US)
    }

    fn usable_borrow(&self, snapshot: Snapshot, now: i64) -> f64 {
        if now < self.borrow_retry_us || snapshot.started_us <= self.borrow_failed_us {
            0.0
        } else {
            snapshot.borrow
        }
    }

    fn frequency_retry_us(&self, symbol: &str) -> i64 {
        self.frequency_cooldowns
            .get(symbol)
            .map_or(0, |cooldown| cooldown.retry_us)
    }

    fn funding(
        &self,
        symbol: &str,
        side: Side,
        required: f64,
        allows_cash_only: bool,
        now: i64,
    ) -> Result<OpenFunding, &'static str> {
        if now < self.frequency_retry_us(symbol) {
            return Err("binance_margin_frequency_cooldown");
        }
        if side != Side::Buy || !symbol.ends_with("USDT") {
            return Ok(OpenFunding::AutoBorrow);
        }
        let snapshot = self.fresh(now).ok_or("binance_usdt_capacity_not_ready")?;
        if snapshot.started_us <= self.cash_refresh_after_us {
            return Err("binance_usdt_cash_refresh_pending");
        }
        if !required.is_finite() || required <= 0.0 {
            return Err("binance_usdt_buy_capacity_insufficient");
        }
        let reserved = self.reserved();
        let free_cash = (snapshot.cash - reserved).max(0.0);
        // Only reserve and spend confirmed free cash. A possible cancellation
        // refund cannot justify submitting a cash-only order.
        if allows_cash_only && free_cash > 0.0 && required - free_cash < USDT_MINIMUM_BORROW {
            return Ok(OpenFunding::CashOnly {
                max_notional: (free_cash / BUY_BUFFER).min(required),
            });
        }
        if required * BUY_BUFFER > free_cash + 1e-8 && now < self.borrow_retry_us {
            return Err("binance_usdt_borrow_cooldown");
        }
        let available = snapshot.cash + self.usable_borrow(snapshot, now) - reserved;
        if available <= 0.0 || available + 1e-8 < required * BUY_BUFFER {
            return Err("binance_usdt_buy_capacity_insufficient");
        }
        Ok(OpenFunding::AutoBorrow)
    }

    fn begin_poll(&mut self, now: i64, after_rejection: bool) -> Option<(i64, i64)> {
        let interval_us = if after_rejection {
            if self.cash_refresh_after_us == 0
                || self
                    .snapshot
                    .is_some_and(|s| s.started_us > self.cash_refresh_after_us)
            {
                return None;
            }
            MINIMUM_BORROW_REFRESH_INTERVAL_US
        } else {
            POLL_INTERVAL_US
        };
        if self.poll_us > 0 && now - self.poll_us < interval_us {
            return None;
        }
        self.frequency_cooldowns
            .retain(|_, cooldown| now - cooldown.retry_us <= MAX_RETRY_US);
        let cash_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        let borrow_id = NEXT_QUERY_ID.fetch_sub(1, Ordering::Relaxed);
        self.poll_us = now;
        self.cash_id = Some(cash_id);
        self.borrow_id = Some(borrow_id);
        self.cash = None;
        self.borrow = None;
        Some((cash_id, borrow_id))
    }

    fn reserve_buy(
        &mut self,
        symbol: &str,
        client_order_id: i64,
        notional: f64,
        cash_only: bool,
        now: i64,
    ) -> Result<(), &'static str> {
        let funding = self.funding(symbol, Side::Buy, notional, cash_only, now)?;
        if cash_only
            && !matches!(funding, OpenFunding::CashOnly { max_notional } if notional <= max_notional + 1e-8)
        {
            return Err("binance_usdt_cash_reservation_insufficient");
        }
        self.submitted.push_back(SubmittedBuy {
            client_order_id,
            sent_us: now,
            reserved: notional * BUY_BUFFER,
        });
        Ok(())
    }

    fn release_buy(&mut self, client_order_id: i64) {
        self.submitted
            .retain(|buy| buy.client_order_id != client_order_id);
    }

    fn reject(&mut self, symbol: &str, side: Side, code: i32, now: i64) {
        if code == binance::OPERATION_TOO_FREQUENT {
            let cooldown = self
                .frequency_cooldowns
                .entry(symbol.to_owned())
                .or_default();
            // Responses already in flight belong to the same cooldown; do not
            // multiply the delay once per rejected order in that burst.
            if now >= cooldown.retry_us {
                cooldown.delay_us = if now - cooldown.retry_us > MAX_RETRY_US {
                    RETRY_US
                } else {
                    (cooldown.delay_us * 2).clamp(RETRY_US, MAX_RETRY_US)
                };
                cooldown.retry_us = now + cooldown.delay_us;
            }
        } else if side == Side::Buy && symbol.ends_with("USDT") {
            if code == binance::BORROW_BELOW_MINIMUM {
                self.cash_refresh_after_us = now;
            } else if matches!(code, binance::ASSET_NOT_BORROWABLE | 51006 | 51061) {
                self.borrow_failed_us = now;
                self.borrow_retry_us = now + RETRY_US;
            }
        }
    }

    fn accept(&mut self, req_type: QueryRequestType, query_id: i64, body: &[u8]) {
        let expected = match req_type {
            QueryRequestType::BinancePmUsdtCash => self.cash_id,
            QueryRequestType::BinancePmUsdtMaxBorrowable => self.borrow_id,
            _ => return,
        };
        if expected != Some(query_id) {
            return;
        }
        let valid = match req_type {
            QueryRequestType::BinancePmUsdtCash => {
                self.cash = parse_cash(body);
                self.cash.is_some()
            }
            QueryRequestType::BinancePmUsdtMaxBorrowable => {
                self.borrow = parse_max_borrowable(body);
                self.borrow.is_some()
            }
            _ => return,
        };
        if !valid {
            // An exchange error is unknown capacity, never a fabricated zero or
            // permission to keep spending a previous successful snapshot.
            self.snapshot = None;
            warn!(
                "BinancePmBuyCapacity: invalid/error query response type={:?} query_id={}",
                req_type, query_id
            );
            return;
        }
        if let (Some(cash), Some(borrow)) = (self.cash, self.borrow) {
            self.snapshot = Some(Snapshot {
                cash,
                borrow,
                started_us: self.poll_us,
            });
            // Keep a short settlement allowance for submissions still in transit
            // when the exchange took the two independent REST snapshots.
            while self
                .submitted
                .front()
                .is_some_and(|buy| buy.sent_us < self.poll_us - 5_000_000)
            {
                self.submitted.pop_front();
            }
            info!("BinancePmBuyCapacity: cash={:.8} max_borrowable={:.8} reserved={:.8} snapshot_us={}", cash, borrow, self.reserved(), self.poll_us);
        }
    }
}

static STATE: Lazy<Mutex<State>> = Lazy::new(|| Mutex::new(State::default()));
static NATIVE_EXECUTION: Lazy<bool> = Lazy::new(|| {
    ExecBackend::for_exchange(Exchange::Binance).is_ok_and(|b| b == ExecBackend::Native)
});
static NEXT_QUERY_ID: AtomicI64 = AtomicI64::new(-9_200_000);

pub fn enabled() -> bool {
    if MonitorChannel::try_venues()
        != Some((TradingVenue::BinanceMargin, TradingVenue::BinanceFutures))
    {
        return false;
    }
    MonitorChannel::instance().arb_mode() == trade_signal::ArbMode::FundingArb
        && MonitorChannel::try_order_manager().is_some_and(|m| !m.borrow().binance_is_standard())
}

pub fn drive_poll(now: i64) {
    if !enabled() {
        return;
    }
    let queries = STATE.lock().begin_poll(now, false);
    publish_poll(now, queries);
}

fn publish_poll(now: i64, queries: Option<(i64, i64)>) {
    let Some((cash_id, borrow_id)) = queries else {
        return;
    };
    for (kind, id) in [
        (QueryRequestType::BinancePmUsdtCash, cash_id),
        (QueryRequestType::BinancePmUsdtMaxBorrowable, borrow_id),
    ] {
        let params = Bytes::from_static(b"asset=USDT");
        let request = GenericQueryRequest::create(kind, now, id, params);
        if let Err(error) = QueryEngHub::publish_query_request("binance", &request.to_bytes()) {
            STATE.lock().snapshot = None;
            warn!(
                "BinancePmBuyCapacity: query publish failed type={:?}: {:#}",
                kind, error
            );
        }
    }
}

pub fn handle_query_response(kind: QueryRequestType, id: i64, body: &[u8]) -> bool {
    if !matches!(
        kind,
        QueryRequestType::BinancePmUsdtCash | QueryRequestType::BinancePmUsdtMaxBorrowable
    ) {
        return false;
    }
    STATE.lock().accept(kind, id, body);
    true
}

fn parse_cash(body: &[u8]) -> Option<f64> {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let json: Value = serde_json::from_slice(&body[..end]).ok()?;
    let row = if let Some(rows) = json.as_array() {
        let mut usdt = rows.iter().filter(|r| r["asset"] == "USDT");
        let row = usdt.next()?;
        if usdt.next().is_some() {
            return None;
        }
        row
    } else {
        &json
    };
    if row["asset"] != "USDT" {
        return None;
    }
    let amount = |field: &str| {
        let value = row.get(field)?;
        let n = value
            .as_f64()
            .or_else(|| value.as_str()?.parse::<f64>().ok())?;
        (n.is_finite() && n >= 0.0).then_some(n)
    };
    amount("crossMarginFree")
}

fn parse_max_borrowable(body: &[u8]) -> Option<f64> {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let json: Value = serde_json::from_slice(&body[..end]).ok()?;
    // Queried with asset=USDT: amount is already a USDT quantity.
    let value = json.get("amount")?;
    let number = value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())?;
    (number.is_finite() && number >= 0.0).then_some(number)
}

pub(crate) fn open_funding(
    venue: TradingVenue,
    side: Side,
    symbol: &str,
    required_usdt: f64,
    allows_cash_only: bool,
) -> Result<OpenFunding, &'static str> {
    if venue != TradingVenue::BinanceMargin || !enabled() {
        return Ok(OpenFunding::AutoBorrow);
    }
    let now = get_timestamp_us();
    let (funding, queries) = {
        let mut state = STATE.lock();
        let funding = state.funding(
            symbol,
            side,
            required_usdt,
            allows_cash_only && *NATIVE_EXECUTION,
            now,
        );
        let queries = state.begin_poll(now, true);
        (funding, queries)
    };
    publish_poll(now, queries);
    funding
}

pub(crate) fn cash_only_quantity(
    symbol: &str,
    mut quantity: QuantizedValue,
    price: f64,
    max_notional: f64,
    signal_bbo: Option<SignalBbo>,
) -> Result<QuantizedValue, &'static str> {
    if quantity.get_val() * price <= max_notional {
        return Ok(quantity);
    }
    let hedge_price = signal_bbo
        .and_then(|bbo| bbo.hedge)
        .filter(|leg| leg.venue == TradingVenue::BinanceFutures.to_u8())
        .map(|leg| leg.bid_price)
        .filter(|price| price.is_finite() && *price > 0.0)
        .ok_or("binance_usdt_cash_only_hedge_price_unavailable")?;
    let monitor = MonitorChannel::instance();
    let rules_unavailable = "binance_usdt_cash_only_order_rules_unavailable";
    let spot = monitor
        .try_venue_min_qty_table(TradingVenue::BinanceMargin)
        .ok_or(rules_unavailable)?;
    let hedge = monitor
        .try_venue_min_qty_table(TradingVenue::BinanceFutures)
        .ok_or(rules_unavailable)?;
    let spot_step = spot.step_size(symbol).ok_or(rules_unavailable)?;
    let hedge_step = hedge.step_size(symbol).ok_or(rules_unavailable)?;
    let common_step = spot_step.max(hedge_step);
    quantity.set_count_floor_from_val(align_price_floor(max_notional / price, common_step));
    let qty = quantity.get_val();
    if qty <= 0.0 || qty * price > max_notional + 1e-8 {
        return Err("binance_usdt_cash_only_quantity_unavailable");
    }
    for (table, step, leg_price) in [(&spot, spot_step, price), (&hedge, hedge_step, hedge_price)] {
        let aligned = align_price_floor(qty, step);
        if aligned <= 0.0
            || aligned + 1e-12 < table.min_qty(symbol).unwrap_or(0.0)
            || aligned * leg_price + 1e-8 < table.min_notional(symbol).unwrap_or(0.0)
        {
            return Err("binance_usdt_cash_only_below_order_minimum");
        }
    }
    Ok(quantity)
}

pub(crate) fn reserve_submission(
    venue: TradingVenue,
    side: Side,
    symbol: &str,
    client_order_id: i64,
    notional: f64,
    cash_only: bool,
) -> Result<(), &'static str> {
    if venue == TradingVenue::BinanceMargin
        && side == Side::Buy
        && symbol.ends_with("USDT")
        && enabled()
    {
        return STATE.lock().reserve_buy(
            symbol,
            client_order_id,
            notional,
            cash_only,
            get_timestamp_us(),
        );
    }
    Ok(())
}

pub(crate) fn release_submission(venue: TradingVenue, client_order_id: i64) {
    if venue == TradingVenue::BinanceMargin && enabled() {
        STATE.lock().release_buy(client_order_id);
    }
}

pub(crate) fn record_rejection(
    venue: TradingVenue,
    side: Side,
    symbol: &str,
    client_order_id: i64,
    code: i32,
    cash_only: bool,
) -> bool {
    if venue != TradingVenue::BinanceMargin || !enabled() {
        return false;
    }
    let cash_only_rejected = cash_only && code == SIGNAL_THROTTLE_ERROR_CODE_BALANCE_INSUFFICIENT;
    if !cash_only_rejected
        && !matches!(
            code,
            binance::ASSET_NOT_BORROWABLE
                | binance::BORROW_BELOW_MINIMUM
                | binance::OPERATION_TOO_FREQUENT
                | 51006
                | 51061
        )
    {
        return false;
    }
    let rejected_us = get_timestamp_us();
    let (borrow_retry_us, frequency_retry_us, poll_us, queries) = {
        let mut s = STATE.lock();
        s.release_buy(client_order_id);
        if cash_only_rejected {
            s.cash_refresh_after_us = rejected_us;
        }
        s.reject(symbol, side, code, rejected_us);
        let poll_us = get_timestamp_us().max(rejected_us + 1);
        (
            s.borrow_retry_us,
            s.frequency_retry_us(symbol),
            poll_us,
            s.begin_poll(poll_us, true),
        )
    };
    warn!("BinancePmBuyCapacity: rejected symbol={symbol} side={side:?} code={code} cash_only={cash_only} borrow_retry_us={borrow_retry_us} frequency_retry_us={frequency_retry_us}");
    publish_poll(poll_us, queries);
    cash_only_rejected
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_000_000_000;
    const BTC_USDT: &str = "BTCUSDT";
    const ETH_USDT: &str = "ETHUSDT";

    fn ready(cash: f64, borrow: f64) -> State {
        State {
            snapshot: Some(Snapshot {
                cash,
                borrow,
                started_us: NOW,
            }),
            ..State::default()
        }
    }

    #[test]
    fn funding_prefers_cash_for_small_shortfalls_without_counting_pending_cash_twice() {
        let mut s = ready(98.0, 1000.0);
        assert_eq!(
            s.funding(BTC_USDT, Side::Buy, 100.0, true, NOW),
            Ok(OpenFunding::CashOnly {
                max_notional: 98.0 / BUY_BUFFER
            })
        );
        assert_eq!(
            s.funding(BTC_USDT, Side::Buy, 100.0, false, NOW),
            Ok(OpenFunding::AutoBorrow)
        );
        assert_eq!(
            ready(101.0, 0.0).funding(BTC_USDT, Side::Buy, 100.0, true, NOW),
            Ok(OpenFunding::CashOnly {
                max_notional: 100.0
            })
        );
        assert_eq!(
            ready(95.0, 1000.0).funding(BTC_USDT, Side::Buy, 100.0, true, NOW),
            Ok(OpenFunding::AutoBorrow)
        );
        assert_eq!(
            ready(95.0175808, 1000.0).funding("CATIUSDT", Side::Buy, 99.96996, true, NOW),
            Ok(OpenFunding::CashOnly {
                max_notional: 95.0175808 / BUY_BUFFER,
            })
        );
        s.submitted.push_back(SubmittedBuy {
            client_order_id: 1,
            sent_us: NOW,
            reserved: 98.0,
        });
        assert_eq!(
            s.funding(ETH_USDT, Side::Buy, 100.0, true, NOW),
            Ok(OpenFunding::AutoBorrow)
        );
    }

    #[test]
    fn minimum_borrow_rejection_recovers_on_a_new_snapshot_without_a_minute_lock() {
        let mut s = ready(0.0, 1000.0);
        s.poll_us = NOW;
        s.reject(BTC_USDT, Side::Buy, binance::BORROW_BELOW_MINIMUM, NOW + 1);
        assert_eq!(
            s.funding(ETH_USDT, Side::Buy, 100.0, true, NOW + 2),
            Err("binance_usdt_cash_refresh_pending")
        );
        assert!(s
            .funding(ETH_USDT, Side::Sell, 100.0, true, NOW + 2)
            .is_ok());
        assert!(s.begin_poll(NOW + 2, true).is_none());
        let refresh = NOW + MINIMUM_BORROW_REFRESH_INTERVAL_US;
        let (cash_id, borrow_id) = s.begin_poll(refresh, true).unwrap();
        assert!(s.begin_poll(refresh + 1, true).is_none());
        s.accept(
            QueryRequestType::BinancePmUsdtCash,
            cash_id,
            br#"{"asset":"USDT","crossMarginFree":"98"}"#,
        );
        assert!(s
            .funding(ETH_USDT, Side::Buy, 100.0, true, refresh)
            .is_err());
        s.accept(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            borrow_id,
            br#"{"amount":"1000"}"#,
        );
        assert_eq!(
            s.funding(ETH_USDT, Side::Buy, 100.0, true, refresh),
            Ok(OpenFunding::CashOnly {
                max_notional: 98.0 / BUY_BUFFER
            })
        );
        assert_eq!(
            s.funding(ETH_USDT, Side::Buy, 110.0, true, refresh),
            Ok(OpenFunding::AutoBorrow)
        );
        assert!(s.begin_poll(refresh + RETRY_US, true).is_none());
    }

    #[test]
    fn cash_only_order_reserves_cash_before_publication_and_releases_it_on_failure() {
        let mut s = ready(98.0, 1000.0);
        assert!(s.reserve_buy(BTC_USDT, 1, 97.5, true, NOW).is_ok());
        assert_eq!(
            s.reserve_buy(ETH_USDT, 2, 97.5, true, NOW),
            Err("binance_usdt_cash_reservation_insufficient")
        );
        s.release_buy(1);
        assert!(s.reserve_buy(ETH_USDT, 2, 97.5, true, NOW).is_ok());
    }

    #[test]
    fn borrow_rejections_cool_down_then_retry_with_a_new_snapshot() {
        for code in [binance::ASSET_NOT_BORROWABLE, 51006, 51061] {
            let mut s = ready(95.0, 1000.0);
            s.reject(BTC_USDT, Side::Buy, code, NOW + 1);
            assert!(s.funding(BTC_USDT, Side::Buy, 90.0, false, NOW + 2).is_ok());
            assert!(s
                .funding(BTC_USDT, Side::Sell, 100.0, false, NOW + 2)
                .is_ok());
            assert_eq!(
                s.funding(ETH_USDT, Side::Buy, 100.0, false, NOW + 2),
                Err("binance_usdt_borrow_cooldown")
            );
            // A fresh query cannot bypass the error-code timer.
            s.snapshot.as_mut().unwrap().started_us = NOW + 2;
            assert_eq!(
                s.funding(BTC_USDT, Side::Buy, 100.0, false, NOW + RETRY_US),
                Err("binance_usdt_borrow_cooldown")
            );
            assert!(s
                .funding(BTC_USDT, Side::Buy, 100.0, false, NOW + RETRY_US + 1)
                .is_ok());
            // A rejected retry starts a new bounded interval.
            s.reject(BTC_USDT, Side::Buy, code, NOW + RETRY_US + 2);
            assert_eq!(
                s.funding(BTC_USDT, Side::Buy, 100.0, false, NOW + RETRY_US + 3),
                Err("binance_usdt_borrow_cooldown")
            );
        }
    }

    #[test]
    fn unrelated_rejections_do_not_start_a_borrow_cooldown() {
        for (symbol, side, code) in [
            (BTC_USDT, Side::Sell, 51015),
            ("ETHBTC", Side::Buy, 51015),
            (BTC_USDT, Side::Buy, 12345),
        ] {
            let mut s = ready(95.0, 1000.0);
            s.reject(symbol, side, code, NOW + 1);
            assert!(s
                .funding(BTC_USDT, Side::Buy, 100.0, false, NOW + 2)
                .is_ok());
        }
    }

    #[test]
    fn cash_uses_free_usdt_without_assuming_locked_funds_will_be_refunded() {
        assert_eq!(
            parse_cash(br#"{"asset":"USDT","crossMarginFree":"0","crossMarginLocked":"99.99738"}"#),
            Some(0.0)
        );
        assert_eq!(
            parse_cash(br#"{"asset":"USDT","crossMarginFree":"10"}"#),
            Some(10.0)
        );
        for body in [
            br#"{"asset":"BTC","crossMarginFree":"10"}"#.as_slice(),
            br#"{"asset":"USDT","crossMarginFree":"NaN"}"#,
            br#"{"asset":"USDT","crossMarginFree":"-1"}"#,
            br#"{"asset":"USDT"}"#,
        ] {
            assert!(parse_cash(body).is_none());
        }
    }

    #[test]
    fn buying_requires_cash_or_borrow_even_with_portfolio_margin_headroom() {
        let s = ready(0.0, 0.0);
        assert_eq!(
            s.funding(BTC_USDT, Side::Buy, 100.0, false, NOW),
            Err("binance_usdt_buy_capacity_insufficient")
        );
        assert!(s.funding(BTC_USDT, Side::Sell, 100.0, false, NOW).is_ok());
        assert!(s.funding("ETHBTC", Side::Buy, 100.0, false, NOW).is_ok());
        assert!(ready(101.0, 0.0)
            .funding(BTC_USDT, Side::Buy, 100.0, false, NOW)
            .is_ok());
        assert!(ready(0.0, 101.0)
            .funding(BTC_USDT, Side::Buy, 100.0, false, NOW)
            .is_ok());
    }

    #[test]
    fn unknown_stale_and_burst_budget_fail_closed() {
        assert!(State::default()
            .funding(BTC_USDT, Side::Buy, 100.0, false, NOW)
            .is_err());
        let mut s = ready(150.0, 0.0);
        assert!(s
            .funding(BTC_USDT, Side::Buy, 100.0, false, NOW + MAX_AGE_US + 1)
            .is_err());
        s.submitted.push_back(SubmittedBuy {
            client_order_id: 1,
            sent_us: NOW,
            reserved: 100.1,
        });
        assert!(s.funding(BTC_USDT, Side::Buy, 100.0, false, NOW).is_err());
    }

    #[test]
    fn borrow_failure_does_not_block_cash_and_requires_new_snapshot_to_recover() {
        let mut s = ready(101.0, 500.0);
        s.reject(BTC_USDT, Side::Buy, 51014, NOW + 1);
        assert!(s
            .funding(BTC_USDT, Side::Buy, 100.0, false, NOW + 2)
            .is_ok());
        assert!(s
            .funding(BTC_USDT, Side::Buy, 200.0, false, NOW + RETRY_US + 2)
            .is_err());
        s.snapshot.as_mut().unwrap().started_us = NOW + RETRY_US + 2;
        assert!(s
            .funding(BTC_USDT, Side::Buy, 200.0, false, NOW + RETRY_US + 2)
            .is_ok());
    }

    #[test]
    fn frequency_cooldown_is_per_symbol_and_backs_off_independently() {
        let mut s = ready(1000.0, 0.0);
        s.reject(BTC_USDT, Side::Buy, binance::OPERATION_TOO_FREQUENT, NOW);
        s.reject(
            BTC_USDT,
            Side::Sell,
            binance::OPERATION_TOO_FREQUENT,
            NOW + 1,
        );
        assert_eq!(s.frequency_retry_us(BTC_USDT), NOW + RETRY_US);
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(
                s.funding(BTC_USDT, side, 10.0, false, NOW + 2),
                Err("binance_margin_frequency_cooldown")
            );
            assert!(s.funding(ETH_USDT, side, 10.0, false, NOW + 2).is_ok());
        }
        let retry = NOW + RETRY_US;
        assert!(s.funding(BTC_USDT, Side::Buy, 10.0, false, retry).is_ok());
        s.reject(BTC_USDT, Side::Buy, binance::OPERATION_TOO_FREQUENT, retry);
        s.reject(ETH_USDT, Side::Buy, binance::OPERATION_TOO_FREQUENT, retry);
        assert_eq!(s.frequency_retry_us(BTC_USDT), retry + 2 * RETRY_US);
        assert_eq!(s.frequency_retry_us(ETH_USDT), retry + RETRY_US);
        assert!(s
            .funding(ETH_USDT, Side::Buy, 10.0, false, retry + RETRY_US)
            .is_ok());
    }

    #[test]
    fn queries_require_matching_ids_complete_pair_and_valid_finite_values() {
        let mut s = State {
            poll_us: NOW,
            cash_id: Some(-1),
            borrow_id: Some(-2),
            ..State::default()
        };
        let cash = br#"{"asset":"USDT","crossMarginFree":"10","crossMarginLocked":"0"}"#;
        s.accept(QueryRequestType::BinancePmUsdtCash, -3, cash);
        assert!(s.cash.is_none());
        s.accept(QueryRequestType::BinancePmUsdtCash, -1, cash);
        assert!(s.snapshot.is_none());
        s.accept(
            QueryRequestType::BinancePmUsdtMaxBorrowable,
            -2,
            br#"{"amount":"200","borrowLimit":"0"}"#,
        );
        assert_eq!(s.snapshot.unwrap().borrow, 200.0);
        for body in [
            br#"{"code":51042}"#.as_slice(),
            br#"{"amount":"NaN"}"#,
            br#"{"amount":"-1"}"#,
            br#"{}"#,
        ] {
            s.accept(QueryRequestType::BinancePmUsdtMaxBorrowable, -2, body);
            assert!(s.snapshot.is_none());
        }
        assert_eq!(
            parse_cash(br#"[{"asset":"USDT","crossMarginFree":"0","crossMarginLocked":"0"}]"#),
            Some(0.0)
        );
    }
}
