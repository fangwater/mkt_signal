//! Trade signal decision module (xarb cross-venue).
//!
//! Compared with FR decision, xarb decision is **spread-only**:
//! - ignores funding/loan signals for Open/Close decisions
//! - still supports Cancel (spread-only) and backward hedge queries

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use super::common::{Quote, ThresholdKey, VenuePair};
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::fr_decision::{DEFAULT_BACKWARD_CHANNEL, DEFAULT_SIGNAL_CHANNEL};

thread_local! {
    static XARB_DECISION: OnceCell<RefCell<XarbDecision>> = OnceCell::new();
}

pub struct XarbDecision {
    signal_pub: SignalPublisher,
    backward_sub: GenericSignalSubscriber,
    channel_name: String,
    _node: Node<ipc::Service>,

    price_offsets: Vec<f64>,

    open_min_qty_table: VenueMinQtyTable,
    hedge_min_qty_table: VenueMinQtyTable,
    venues: VenuePair,

    order_amount: f32,
    open_order_ttl_us: i64,
    hedge_timeout_mm_us: i64,
    hedge_price_offset: f64,
    hedge_aggressive_seq_threshold: u32,

    signal_cooldown_us: i64,
    last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_cancel_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
}

impl XarbDecision {
    fn normalize_symbol_key(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    pub fn is_initialized() -> bool {
        XARB_DECISION.with(|cell| cell.get().is_some())
    }

    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("XarbDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("XarbDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let venues = (open_venue, hedge_venue);
        let result: Result<()> = XARB_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new_sync(venues)?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize XarbDecision singleton"))?;
            info!(
                "XarbDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        });
        result?;

        Self::refresh_min_qty_async(venues).await;
        Self::spawn_backward_listener();
        info!("XarbDecision backward listener started");

        Ok(())
    }

    fn new_sync(venues: VenuePair) -> Result<Self> {
        let node_name = NodeName::new("xarb_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;

        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;

        let price_offsets = vec![0.0002, 0.0004, 0.0006, 0.0008, 0.001];
        let open_min_qty_table = VenueMinQtyTable::new(venues.0);
        let hedge_min_qty_table = VenueMinQtyTable::new(venues.1);

        Ok(Self {
            signal_pub,
            backward_sub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            _node: node,
            price_offsets,
            open_min_qty_table,
            hedge_min_qty_table,
            venues,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            signal_cooldown_us: 5_000_000,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_cancel_ts: Rc::new(RefCell::new(HashMap::new())),
        })
    }

    async fn refresh_min_qty_async(venues: VenuePair) {
        let mut open_table = VenueMinQtyTable::new(venues.0);
        let mut hedge_table = VenueMinQtyTable::new(venues.1);

        let open_res = open_table.refresh().await;
        let hedge_res = hedge_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.open_min_qty_table = open_table;
            }
            if hedge_res.is_ok() {
                decision.hedge_min_qty_table = hedge_table;
            }
        });

        match open_res {
            Ok(_) => info!(
                "XarbDecision: open venue min_qty_table loaded, venue={:?}",
                venues.0
            ),
            Err(err) => warn!(
                "XarbDecision: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.0
            ),
        }
        match hedge_res {
            Ok(_) => info!(
                "XarbDecision: hedge venue min_qty_table loaded, venue={:?}",
                venues.1
            ),
            Err(err) => warn!(
                "XarbDecision: failed to refresh hedge venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.1
            ),
        }
    }

    fn create_subscriber(
        node: &Node<ipc::Service>,
        channel_name: &str,
    ) -> Result<GenericSignalSubscriber> {
        let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(GenericSignalSubscriber::Size4K(subscriber))
    }

    pub fn make_spread_only_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        let spread_factor = SpreadFactor::instance();
        let now = get_timestamp_us();
        let symbol_list = SymbolList::instance();
        let open_symbol_key = Self::normalize_symbol_key(open_symbol);
        let hedge_symbol_key = Self::normalize_symbol_key(hedge_symbol);

        // 1) Cancel (spread-only)
        if spread_factor.satisfy_forward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        ) || spread_factor.satisfy_backward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        ) {
            if !self.check_signal_cooldown(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                now,
                &SignalType::ArbCancel,
            ) {
                self.emit_signals(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    SignalType::ArbCancel,
                    None,
                )?;
                self.update_last_signal_ts(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    now,
                    &SignalType::ArbCancel,
                );
                return Ok(Some(SignalType::ArbCancel));
            }
        }

        let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());

        let forward_open = !in_dump
            && spread_factor.satisfy_forward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            )
            && symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str());
        let backward_open = !in_dump
            && spread_factor.satisfy_backward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            )
            && symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str());

        let forward_close = spread_factor.satisfy_forward_close(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        );
        let backward_close = spread_factor.satisfy_backward_close(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        );

        let final_signal_and_side: Option<(SignalType, Side)> = if forward_close && backward_open {
            Some((SignalType::ArbOpen, Side::Sell))
        } else if backward_close && forward_open {
            Some((SignalType::ArbOpen, Side::Buy))
        } else if forward_close {
            Some((SignalType::ArbClose, Side::Sell))
        } else if backward_close {
            Some((SignalType::ArbClose, Side::Buy))
        } else if forward_open {
            Some((SignalType::ArbOpen, Side::Buy))
        } else if backward_open {
            Some((SignalType::ArbOpen, Side::Sell))
        } else {
            None
        };

        let Some((signal_type, side)) = final_signal_and_side else {
            return Ok(None);
        };

        if self.check_signal_cooldown(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            now,
            &signal_type,
        ) {
            return Ok(None);
        }

        self.emit_signals(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            signal_type.clone(),
            Some(side),
        )?;

        self.update_last_signal_ts(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            now,
            &signal_type,
        );

        Ok(Some(signal_type))
    }

    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match ArbHedgeSignalQueryMsg::from_bytes(data) {
            Ok(q) => q,
            Err(err) => {
                warn!("XarbDecision: 解析 hedge query 失败: {err}");
                return;
            }
        };

        let Some(side) = query.get_side() else {
            warn!("XarbDecision: hedge query side 无效: {}", query.hedge_side);
            return;
        };

        let Some(hedge_venue) = TradingVenue::from_u8(query.hedging_venue) else {
            warn!(
                "XarbDecision: hedge query venue 无效: {}",
                query.hedging_venue
            );
            return;
        };

        let hedge_symbol = query.get_hedging_symbol();
        if hedge_symbol.is_empty() {
            warn!("XarbDecision: hedge query 未携带对冲 symbol");
            return;
        }

        let open_symbol = query.get_opening_symbol();
        if open_symbol.is_empty() {
            warn!("XarbDecision: hedge query 未携带开仓 symbol");
            return;
        }

        let Some(open_venue) = TradingVenue::from_u8(query.opening_venue) else {
            warn!(
                "XarbDecision: hedge query opening venue 无效: {}",
                query.opening_venue
            );
            return;
        };

        let qty = query.hedge_qty;
        if qty <= 0.0 {
            warn!(
                "XarbDecision: hedge query quantity <= 0 strategy_id={} qty={:.8}",
                query.strategy_id, qty
            );
            return;
        }

        let mkt_channel = MktChannel::instance();

        let Some(open_quote) = mkt_channel.get_quote(&open_symbol, open_venue) else {
            warn!(
                "XarbDecision: hedge query 开仓侧无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, open_symbol, open_venue
            );
            return;
        };

        let Some(hedge_quote) = mkt_channel.get_quote(&hedge_symbol, hedge_venue) else {
            warn!(
                "XarbDecision: hedge query 无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, hedge_symbol, hedge_venue
            );
            return;
        };

        let price_tick = self
            .table_for(hedge_venue)
            .price_tick(&hedge_symbol)
            .unwrap_or(0.0);

        let now = get_timestamp_us();
        let seq_threshold = self.hedge_aggressive_seq_threshold;
        let aggressive = query.request_seq >= seq_threshold;
        let offset = if aggressive {
            0.0
        } else {
            self.hedge_price_offset.abs()
        };

        let base_price = match side {
            Side::Buy => hedge_quote.bid,
            Side::Sell => hedge_quote.ask,
        };
        let limit_price = if base_price > 0.0 {
            match side {
                Side::Buy => base_price * (1.0 - offset),
                Side::Sell => base_price * (1.0 + offset),
            }
        } else {
            0.0
        };

        if limit_price <= 0.0 {
            warn!(
                "XarbDecision: hedge query limit_price 无效 strategy_id={} price={:.8}",
                query.strategy_id, limit_price
            );
            return;
        }

        let mut ctx = ArbHedgeCtx::new_maker(
            query.strategy_id,
            query.client_order_id,
            qty,
            side.to_u8(),
            limit_price,
            price_tick,
            false,
            now + self.hedge_timeout_mm_us,
        );
        ctx.opening_leg = TradingLeg::new(open_venue, open_quote.bid, open_quote.ask);
        ctx.set_opening_symbol(&open_symbol);
        ctx.hedging_leg = TradingLeg::new(hedge_venue, hedge_quote.bid, hedge_quote.ask);
        ctx.set_hedging_symbol(&hedge_symbol);
        ctx.market_ts = now;
        ctx.price_offset = offset;

        let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());

        if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "XarbDecision: 发送 hedge 信号失败 strategy_id={} err={:?}",
                query.strategy_id, err
            );
            return;
        }

        info!(
            "XarbDecision: 回复 hedge query strategy_id={} hedge_symbol={} qty={:.6} side={:?} seq={} aggressive={} limit_price={:.8} offset={:.6} (maker)",
            query.strategy_id,
            hedge_symbol,
            qty,
            side,
            query.request_seq,
            aggressive,
            limit_price,
            offset
        );
    }

    fn emit_signals(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        signal_type: SignalType,
        side: Option<Side>,
    ) -> Result<()> {
        let now = get_timestamp_us();
        let mkt_channel = MktChannel::instance();

        let open_quote = mkt_channel.get_quote(open_symbol, open_venue);
        let hedge_quote = mkt_channel.get_quote(hedge_symbol, hedge_venue);

        if open_quote.is_none() || hedge_quote.is_none() {
            warn!(
                "XarbDecision: quote unavailable open={} ({:?}) hedge={} ({:?})",
                open_symbol,
                open_quote.is_some(),
                hedge_symbol,
                hedge_quote.is_some()
            );
            return Ok(());
        }

        let open_quote = open_quote.unwrap();
        let hedge_quote = hedge_quote.unwrap();

        if open_quote.bid >= open_quote.ask {
            warn!(
                "XarbDecision: invalid open quote bid={:.8} >= ask={:.8} for {}",
                open_quote.bid, open_quote.ask, open_symbol
            );
            return Ok(());
        }
        if hedge_quote.bid >= hedge_quote.ask {
            warn!(
                "XarbDecision: invalid hedge quote bid={:.8} >= ask={:.8} for {}",
                hedge_quote.bid, hedge_quote.ask, hedge_symbol
            );
            return Ok(());
        }

        match signal_type {
            SignalType::ArbOpen | SignalType::ArbClose => {
                let side = match side {
                    Some(s) => s,
                    None => {
                        warn!("XarbDecision: {:?} signal missing side info", signal_type);
                        return Ok(());
                    }
                };

                for offset in &self.price_offsets {
                    let ctx = self.build_open_context(
                        open_symbol,
                        hedge_symbol,
                        open_venue,
                        hedge_venue,
                        &open_quote,
                        &hedge_quote,
                        *offset,
                        now,
                        side,
                    );

                    let signal = TradeSignal::create(signal_type.clone(), now, 0.0, ctx.to_bytes());
                    self.signal_pub.publish(&signal.to_bytes())?;
                }

                info!(
                    "XarbDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={}",
                    self.price_offsets.len(),
                    signal_type,
                    self.channel_name,
                    open_symbol,
                    hedge_symbol
                );
            }

            SignalType::ArbCancel => {
                let ctx = self.build_cancel_context(
                    open_symbol,
                    hedge_symbol,
                    open_venue,
                    hedge_venue,
                    &open_quote,
                    &hedge_quote,
                    now,
                );

                let signal = TradeSignal::create(signal_type.clone(), now, 0.0, ctx.to_bytes());
                self.signal_pub.publish(&signal.to_bytes())?;
            }

            _ => {
                warn!("XarbDecision: unsupported signal type: {:?}", signal_type);
            }
        }

        Ok(())
    }

    fn build_open_context(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_quote: &Quote,
        hedge_quote: &Quote,
        price_offset: f64,
        now: i64,
        side: Side,
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let mkt_channel = MktChannel::instance();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        ctx.opening_leg = TradingLeg::new(open_venue, open_quote.bid, open_quote.ask);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(hedge_venue, hedge_quote.bid, hedge_quote.ask);
        ctx.set_hedging_symbol(&hedge_trade_symbol);

        ctx.set_side(side);
        ctx.set_order_type(OrderType::Limit);

        let base_price = match side {
            Side::Buy => open_quote.bid,
            Side::Sell => open_quote.ask,
        };
        ctx.price = if base_price > 0.0 {
            match side {
                Side::Buy => base_price * (1.0 - price_offset),
                Side::Sell => base_price * (1.0 + price_offset),
            }
        } else {
            0.0
        };
        ctx.price_tick = self
            .table_for(open_venue)
            .price_tick(&open_trade_symbol)
            .unwrap_or(0.0);
        let qty = self.convert_order_amount_to_qty(open_venue, &open_trade_symbol, ctx.price);
        ctx.amount = qty as f32;

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.price_offset = price_offset;

        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,
            super::common::FactorMode::MM => self.hedge_timeout_mm_us,
        };

        let rate_fetcher = RateFetcher::instance();
        ctx.funding_ma = mkt_channel
            .get_funding_rate_mean(hedge_symbol, hedge_venue)
            .unwrap_or(0.0);
        ctx.predicted_funding_rate = rate_fetcher
            .get_predicted_funding_rate(hedge_symbol, hedge_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);
        ctx.loan_rate = rate_fetcher
            .get_predict_loan_rate(hedge_symbol, hedge_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);

        ctx
    }

    fn build_cancel_context(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_quote: &Quote,
        hedge_quote: &Quote,
        now: i64,
    ) -> ArbCancelCtx {
        let mut ctx = ArbCancelCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        ctx.opening_leg = TradingLeg::new(open_venue, open_quote.bid, open_quote.ask);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(hedge_venue, hedge_quote.bid, hedge_quote.ask);
        ctx.set_hedging_symbol(&hedge_trade_symbol);

        ctx.trigger_ts = now;
        ctx
    }

    fn table_for(&self, venue: TradingVenue) -> &VenueMinQtyTable {
        if venue == self.venues.0 {
            &self.open_min_qty_table
        } else {
            &self.hedge_min_qty_table
        }
    }

    fn convert_order_amount_to_qty(&self, venue: TradingVenue, symbol: &str, price: f64) -> f64 {
        if !(self.order_amount > 0.0) {
            warn!(
                "XarbDecision: order_amount <= 0 when building signal for {}, skip",
                symbol
            );
            return 0.0;
        }
        if price <= 0.0 {
            warn!(
                "XarbDecision: price for {} <= 0 when converting order amount, fallback to 0",
                symbol
            );
            return 0.0;
        }

        let table = self.table_for(venue);
        let min_qty = table.min_qty(symbol);
        let step = table.step_size(symbol);
        convert_usdt_amount_to_qty(self.order_amount as f64, price, min_qty, step)
    }

    pub fn update_price_offsets(&mut self, offsets: Vec<f64>) {
        if offsets.is_empty() {
            warn!("XarbDecision: 忽略空的 price_offsets 更新请求");
            return;
        }
        self.price_offsets = offsets;
        info!(
            "XarbDecision: price_offsets 已更新，总档位 {}",
            self.price_offsets.len()
        );
    }

    pub fn update_open_order_timeout(&mut self, open_secs: u64) {
        if open_secs == 0 {
            warn!("XarbDecision: open_secs=0 无效，忽略更新");
            return;
        }
        let ttl = open_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.open_order_ttl_us = ttl as i64;
        info!("XarbDecision: open_order_ttl 更新为 {}s", open_secs);
    }

    pub fn update_hedge_timeout(&mut self, hedge_secs: u64) {
        if hedge_secs == 0 {
            warn!("XarbDecision: hedge_secs=0 无效，忽略更新");
            return;
        }
        let ttl = hedge_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.hedge_timeout_mm_us = ttl as i64;
        info!("XarbDecision: hedge_timeout_mm 更新为 {}s", hedge_secs);
    }

    pub fn update_order_amount(&mut self, amount: f32) {
        if amount <= 0.0 {
            warn!("XarbDecision: amount <= 0 无效，忽略更新");
            return;
        }
        self.order_amount = amount;
        info!("XarbDecision: order_amount 更新为 {:.4}", self.order_amount);
    }

    pub fn update_hedge_price_offset(&mut self, offset: f64) {
        if offset <= 0.0 {
            warn!("XarbDecision: hedge offset <= 0 无效，忽略更新");
            return;
        }
        self.hedge_price_offset = offset;
        info!("XarbDecision: hedge_price_offset 更新为 {:.6}", offset);
    }

    pub fn update_hedge_aggressive_seq_threshold(&mut self, threshold: u32) {
        if threshold == 0 {
            warn!("XarbDecision: hedge_aggressive_seq_threshold=0 无效，忽略更新");
            return;
        }
        self.hedge_aggressive_seq_threshold = threshold;
        info!(
            "XarbDecision: hedge_aggressive_seq_threshold 更新为 {}",
            threshold
        );
    }

    pub fn update_signal_cooldown(&mut self, cooldown_secs: u64) {
        if cooldown_secs == 0 {
            warn!("XarbDecision: cooldown_secs=0 无效，忽略更新");
            return;
        }
        let cooldown_us = cooldown_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.signal_cooldown_us = cooldown_us as i64;
        info!(
            "XarbDecision: signal_cooldown 更新为 {}s ({}us)",
            cooldown_secs, self.signal_cooldown_us
        );
    }

    fn check_signal_cooldown(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) -> bool {
        let key = (
            open_venue,
            open_symbol.to_uppercase(),
            hedge_venue,
            hedge_symbol.to_uppercase(),
        );

        let last_ts_map = match signal_type {
            SignalType::ArbOpen => self.last_open_ts.borrow(),
            SignalType::ArbClose => self.last_close_ts.borrow(),
            SignalType::ArbCancel => self.last_cancel_ts.borrow(),
            _ => {
                warn!("XarbDecision: 不支持的信号类型 {:?}", signal_type);
                return false;
            }
        };

        if let Some(&last_ts) = last_ts_map.get(&key) {
            let elapsed = now - last_ts;
            if elapsed < self.signal_cooldown_us {
                return true;
            }
        }
        false
    }

    fn update_last_signal_ts(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) {
        let key = (
            open_venue,
            open_symbol.to_uppercase(),
            hedge_venue,
            hedge_symbol.to_uppercase(),
        );

        match signal_type {
            SignalType::ArbOpen => {
                self.last_open_ts.borrow_mut().insert(key, now);
            }
            SignalType::ArbClose => {
                self.last_close_ts.borrow_mut().insert(key, now);
            }
            SignalType::ArbCancel => {
                self.last_cancel_ts.borrow_mut().insert(key, now);
            }
            _ => {
                warn!("XarbDecision: 不支持的信号类型 {:?}", signal_type);
            }
        }
    }

    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            info!("XarbDecision backward 监听任务启动");

            loop {
                let has_message = XARB_DECISION.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            decision.handle_backward_query(data);
                            true
                        }
                        Ok(None) => false,
                        Err(err) => {
                            warn!("XarbDecision: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
                });

                if !has_message {
                    tokio::task::yield_now().await;
                }
            }
        });
    }
}

fn convert_usdt_amount_to_qty(
    order_amount_u: f64,
    price: f64,
    min_qty: Option<f64>,
    step: Option<f64>,
) -> f64 {
    if order_amount_u <= 0.0 || price <= 0.0 {
        return 0.0;
    }
    let mut qty = order_amount_u / price;
    if let Some(step) = step {
        qty = align_step_ceil(qty, step);
    }

    if let Some(min_qty) = min_qty {
        if min_qty > 0.0 && qty < min_qty {
            qty = if let Some(step) = step {
                align_step_ceil(min_qty, step)
            } else {
                min_qty
            };
        }
    }

    qty
}

fn align_step_ceil(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value;
    }
    let units = (value / step).ceil();
    units * step
}
