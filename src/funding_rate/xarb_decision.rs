//! Trade signal decision module (xarb cross-venue).
//!
//! Compared with FR decision, xarb decision is **spread-only**:
//! - ignores funding/loan signals for Open decisions
//! - only emits Open/Cancel (spread-only) and supports backward hedge queries

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use super::common::{compute_spread_rate, Quote, ThresholdKey, VenuePair};
use super::factor_value_hub::{FactorValueHub, FactorValueLookupResult, PnluCheckResult};
use super::mkt_channel::MktChannel;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use crate::common::bbo::Bbo;
use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{
    align_price_ceil, align_price_floor, SignalBytes, TradingLeg, TradingVenue,
};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::fr_decision::{DEFAULT_BACKWARD_CHANNEL, DEFAULT_SIGNAL_CHANNEL};

const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
const DEFAULT_PNLU_REDIS_DB: i64 = 0;
const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;

fn min_qty_symbol_key(venue: TradingVenue, trade_symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => trade_symbol
            .to_uppercase()
            .replace("-SWAP", "")
            .replace('-', ""),
        TradingVenue::GateMargin | TradingVenue::GateFutures => trade_symbol
            .to_uppercase()
            .replace('_', "")
            .replace('-', ""),
        _ => trade_symbol.to_uppercase(),
    }
}

fn venue_qty_is_contracts(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
    )
}

fn contract_qty_multiplier(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol_key: &str,
) -> Option<f64> {
    match venue {
        TradingVenue::BinanceFutures => Some(1.0),
        TradingVenue::OkexFutures | TradingVenue::GateFutures => table
            .contract_multiplier_opt(symbol_key)
            .filter(|v| v.is_finite() && *v > 0.0),
        _ => Some(1.0),
    }
}

fn base_step_size(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    trade_symbol: &str,
) -> (Option<f64>, Option<f64>) {
    let symbol_key = min_qty_symbol_key(venue, trade_symbol);
    let step = table.step_size(&symbol_key);
    let min_qty = table.min_qty(&symbol_key);

    if venue_qty_is_contracts(venue) {
        let mult = contract_qty_multiplier(table, venue, &symbol_key);
        let step_base = step.zip(mult).map(|(s, m)| s * m);
        let min_qty_base = min_qty.zip(mult).map(|(q, m)| q * m);
        (step_base, min_qty_base)
    } else {
        (step, min_qty)
    }
}

thread_local! {
    static XARB_DECISION: OnceCell<RefCell<XarbDecision>> = OnceCell::new();
}

pub struct XarbDecision {
    signal_pub: SignalPublisher,
    backward_sub: GenericSignalSubscriber,
    factor_value_hub: FactorValueHub,
    channel_name: String,
    node: Node<ipc::Service>,

    price_offsets: Vec<f64>,

    open_min_qty_table: VenueMinQtyTable,
    hedge_min_qty_table: VenueMinQtyTable,
    venues: VenuePair,

    order_amount: f32,
    open_order_ttl_us: i64,
    hedge_timeout_mm_us: i64,
    hedge_price_offset: f64,
    hedge_aggressive_seq_threshold: u32,
    max_hedge_price_pct_change: f64, // percent, 1~99

    signal_cooldown_us: i64,
    last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_cancel_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
}

impl XarbDecision {
    fn normalize_symbol_key(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    fn poll_model_output_updates(&mut self) {
        self.factor_value_hub.poll_model_output_updates();
    }

    fn lookup_target_factor_value(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        self.factor_value_hub
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
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
        let pnlu_settings = RedisSettings {
            host: DEFAULT_PNLU_REDIS_HOST.to_string(),
            port: DEFAULT_PNLU_REDIS_PORT,
            db: DEFAULT_PNLU_REDIS_DB,
            username: None,
            password: None,
            prefix: None,
        };
        let pnlu_key_suffix = DEFAULT_PNLU_KEY_SUFFIX.to_string();
        let factor_value_hub = FactorValueHub::new(
            &node,
            venues.0,
            venues.1,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings.clone(),
            pnlu_key_suffix.clone(),
            PNLU_MAX_AGE_SECS,
        )?;
        let pnlu_key_prefix = format!(
            "pnlu:{}:{}:",
            venues.0.data_pub_slug(),
            venues.1.data_pub_slug()
        );

        let price_offsets = vec![0.0002, 0.0004, 0.0006, 0.0008, 0.001];
        let open_min_qty_table = VenueMinQtyTable::new(venues.0);
        let hedge_min_qty_table = VenueMinQtyTable::new(venues.1);

        info!(
            "XarbDecision: pnlu redis configured host={} port={} db={} key_prefix={} suffix={}",
            pnlu_settings.host,
            pnlu_settings.port,
            pnlu_settings.db,
            pnlu_key_prefix,
            pnlu_key_suffix
        );

        Ok(Self {
            signal_pub,
            backward_sub,
            factor_value_hub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            node,
            price_offsets,
            open_min_qty_table,
            hedge_min_qty_table,
            venues,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            max_hedge_price_pct_change: 5.0,
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

    pub fn update_model_output_services(&mut self, services: Vec<String>) {
        self.factor_value_hub
            .update_model_output_services(&self.node, services);
    }

    pub fn update_pnlu_key_suffix(&mut self, key_suffix: String) {
        self.factor_value_hub.update_pnlu_key_suffix(key_suffix);
    }

    pub fn make_spread_only_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        self.poll_model_output_updates();
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
            let key = Self::threshold_key(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            );
            if !self.is_cooldown_hit(&self.last_cancel_ts, &key, now) {
                self.emit_cancel_signal(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                )?;
                self.update_last_ts(&self.last_cancel_ts, key, now);
                return Ok(Some(SignalType::ArbCancel));
            }
        }

        let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
        if in_dump {
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
            let side = if forward_close {
                Some(Side::Sell)
            } else if backward_close {
                Some(Side::Buy)
            } else {
                None
            };
            let Some(side) = side else {
                return Ok(None);
            };
            let key = Self::threshold_key(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            );
            if !self.is_cooldown_hit(&self.last_close_ts, &key, now) {
                self.emit_close_signals(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    side,
                )?;
                self.update_last_ts(&self.last_close_ts, key, now);
                return Ok(Some(SignalType::ArbClose));
            }
            return Ok(None);
        }

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

        let side = if forward_open {
            Some(Side::Buy)
        } else if backward_open {
            Some(Side::Sell)
        } else {
            None
        };

        let Some(side) = side else {
            return Ok(None);
        };

        let key = Self::threshold_key(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        );
        let cooldown_hit = self.is_cooldown_hit(&self.last_open_ts, &key, now);
        if cooldown_hit {
            return Ok(None);
        }
        let pnlu_check = self.check_pnlu_factor(open_symbol_key.as_str(), now);
        if !pnlu_check.ok {
            return Ok(None);
        }
        let pnlu_factor = pnlu_check.factor.unwrap_or(0.0);
        let rl_return_volatility_factor = self
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value
            .unwrap_or(0.0);

        self.emit_open_signals(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            side,
            pnlu_factor,
            rl_return_volatility_factor,
        )?;

        self.update_last_ts(&self.last_open_ts, key, now);
        self.log_pnlu_check(
            open_symbol_key.as_str(),
            forward_open,
            backward_open,
            false,
            &pnlu_check,
        );

        Ok(Some(SignalType::ArbOpen))
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

        let hedge_base_qty = query.hedge_base_qty;
        if hedge_base_qty <= 0.0 {
            warn!(
                "XarbDecision: hedge query quantity <= 0 strategy_id={} qty={:.8}",
                query.strategy_id, hedge_base_qty
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

        let table = self.table_for(hedge_venue);
        let symbol_key = min_qty_symbol_key(hedge_venue, &hedge_symbol);
        let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);

        let now = get_timestamp_us();
        let open_symbol_key = Self::normalize_symbol_key(&open_symbol);
        let pnlu_factor = self
            .check_pnlu_factor(open_symbol_key.as_str(), now)
            .factor
            .unwrap_or(0.0);
        let target_factor_lookup = self.lookup_target_factor_value(&hedge_symbol, hedge_venue);
        let seq_threshold = self.hedge_aggressive_seq_threshold;
        let aggressive = query.request_seq >= seq_threshold;
        let default_offset = self.hedge_price_offset.abs();
        let open_bbo = Bbo::new(query.hedge_bid0, query.hedge_ask0, query.hedge_leg_ts);
        let hedge_bbo = Bbo::new(hedge_quote.bid, hedge_quote.ask, hedge_quote.ts);
        let open_mid = open_bbo.get_mid_price().unwrap_or(0.0);
        let hedge_mid = hedge_bbo.get_mid_price().unwrap_or(0.0);
        let mut pct_change = 0.0;
        let mut stop_loss_triggered = false;
        let mut stop_loss_valid = false;
        let threshold_pct = self.max_hedge_price_pct_change;
        let threshold_ratio = threshold_pct / 100.0;
        if open_mid > 0.0 && hedge_mid > 0.0 {
            pct_change = (hedge_mid - open_mid).abs() / open_mid;
            stop_loss_valid = true;
            stop_loss_triggered = pct_change > threshold_ratio;
        }
        info!(
            "XarbDecision: hedge stop-loss check strategy_id={} open_mid={:.6} hedge_mid={:.6} pct_change={:.6} threshold_pct={:.2} trigger={} valid={}",
            query.strategy_id,
            open_mid,
            hedge_mid,
            pct_change,
            threshold_pct,
            stop_loss_triggered,
            stop_loss_valid
        );
        if stop_loss_triggered {
            let market_price = match side {
                Side::Buy => hedge_quote.ask,
                Side::Sell => hedge_quote.bid,
            };
            let aligned_hedge_qty = self.convert_aligned_base_qty_to_open_venue_qty(
                hedge_venue,
                &hedge_symbol,
                market_price,
                hedge_base_qty,
            );
            if aligned_hedge_qty <= 0.0 {
                warn!(
                    "XarbDecision: stop-loss taker aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                    query.strategy_id,
                    symbol_key,
                    hedge_base_qty
                );
                return;
            }
            let mut ctx = ArbHedgeCtx::new_taker(
                query.strategy_id,
                query.client_order_id,
                side.to_u8(),
                aligned_hedge_qty,
                qty_tick,
            );
            ctx.opening_leg =
                TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
            ctx.set_opening_symbol(&open_symbol);
            ctx.hedging_leg = TradingLeg::new(
                hedge_venue,
                hedge_quote.bid,
                hedge_quote.ask,
                hedge_quote.ts,
            );
            ctx.set_hedging_symbol(&hedge_symbol);
            ctx.market_ts = now;
            ctx.price_offset = 0.0;
            ctx.spread_rate = spread_rate;
            ctx.maker_only = false;
            let from_key = self.build_hedge_from_key(
                now,
                pnlu_factor,
                target_factor_lookup.target_factor_value,
                pct_change,
                spread_rate,
            );
            ctx.set_from_key(from_key);

            let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());
            if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
                warn!(
                    "XarbDecision: 发送 stop-loss taker hedge 失败 strategy_id={} err={:?}",
                    query.strategy_id, err
                );
                return;
            }
            info!(
                "XarbDecision: 触发 stop-loss taker hedge strategy_id={} hedge_symbol={} qty={:.6} side={:?} pct_change={:.6} threshold_pct={:.2} spread_rate={:.6}",
                query.strategy_id,
                hedge_symbol,
                ctx.hedge_qty_value(),
                side,
                pct_change,
                threshold_pct,
                spread_rate
            );
            return;
        }
        let mut offset = default_offset;
        let mut offset_source = "config";
        let mut offset_note = String::new();

        let ready = target_factor_lookup.ready.unwrap_or(false);

        if ready {
            if let Some(value) = target_factor_lookup.target_factor_value {
                if value.is_finite() && value > 0.0 {
                    offset = value;
                    offset_source = "rl_return_volatility_factor";
                } else {
                    offset_note = "invalid_factor".to_string();
                }
            } else {
                offset_note = "missing_factor".to_string();
            }
        } else if target_factor_lookup.note == "ok" {
            offset_note = "not_ready".to_string();
        } else {
            offset_note = target_factor_lookup.note.clone();
        }

        if aggressive {
            offset = 0.0;
            offset_source = "aggressive";
            if offset_note.is_empty() {
                offset_note = "aggressive_override".to_string();
            } else {
                offset_note = format!("aggressive_override({})", offset_note);
            }
        }

        info!(
            "XarbDecision: hedge query offset source={} key={} symbol={} venue={:?} norm_symbol={} ready={} factor={:?} factor_index={:?} ts_ms={:?} offset={:.6} default_offset={:.6} aggressive={} note={}",
            offset_source,
            target_factor_lookup.key,
            hedge_symbol,
            hedge_venue,
            target_factor_lookup.symbol_key,
            ready,
            target_factor_lookup.target_factor_value,
            target_factor_lookup.factor_index,
            target_factor_lookup.ts_ms,
            offset,
            default_offset,
            aggressive,
            offset_note
        );

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

        let aligned_hedge_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            hedge_venue,
            &hedge_symbol,
            limit_price,
            hedge_base_qty,
        );
        if aligned_hedge_qty <= 0.0 {
            warn!(
                "XarbDecision: hedge query aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                query.strategy_id,
                symbol_key,
                hedge_base_qty
            );
            return;
        }

        let mut ctx = ArbHedgeCtx::new_maker(
            query.strategy_id,
            query.client_order_id,
            side.to_u8(),
            aligned_hedge_qty,
            qty_tick,
            limit_price,
            price_tick,
            false,
            now + self.hedge_timeout_mm_us,
        );
        if ctx.hedge_qty_count() <= 0 || ctx.hedge_price_count() <= 0 {
            warn!(
                "XarbDecision: hedge query qv invalid strategy_id={} qty={:.8} price={:.8}",
                query.strategy_id,
                ctx.hedge_qty_value(),
                ctx.hedge_price_value()
            );
            return;
        }
        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_symbol);
        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_symbol);
        ctx.market_ts = now;
        ctx.price_offset = offset;
        ctx.spread_rate = spread_rate;
        let from_key = self.build_hedge_from_key(
            now,
            pnlu_factor,
            target_factor_lookup.target_factor_value,
            pct_change,
            spread_rate,
        );
        ctx.set_from_key(from_key);

        let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());

        if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "XarbDecision: 发送 hedge 信号失败 strategy_id={} err={:?}",
                query.strategy_id, err
            );
            return;
        }

        info!(
            "XarbDecision: 回复 hedge query strategy_id={} hedge_symbol={} qty={:.6} side={:?} seq={} aggressive={} limit_price={:.8} offset={:.6} spread_rate={:.6} (maker)",
            query.strategy_id,
            hedge_symbol,
            ctx.hedge_qty_value(),
            side,
            query.request_seq,
            aggressive,
            ctx.hedge_price_value(),
            offset,
            spread_rate
        );
    }

    fn load_valid_quotes(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<(Quote, Quote)> {
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
            return None;
        }

        let open_quote = open_quote.unwrap();
        let hedge_quote = hedge_quote.unwrap();

        if open_quote.bid >= open_quote.ask {
            warn!(
                "XarbDecision: invalid open quote bid={:.8} >= ask={:.8} for {}",
                open_quote.bid, open_quote.ask, open_symbol
            );
            return None;
        }
        if hedge_quote.bid >= hedge_quote.ask {
            warn!(
                "XarbDecision: invalid hedge quote bid={:.8} >= ask={:.8} for {}",
                hedge_quote.bid, hedge_quote.ask, hedge_symbol
            );
            return None;
        }

        Some((open_quote, hedge_quote))
    }

    fn emit_open_signals(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
        pnlu_factor: f64,
        rl_return_volatility_factor: f64,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        // Use one batch timestamp for all grid offsets in this emit call.
        let batch_ts = get_timestamp_us();
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let from_key = format!(
            "{batch_ts}:{pnlu_factor:.6}:{rl_return_volatility_factor:.6}:{spread_rate:.6}"
        );

        for offset in &self.price_offsets {
            let ctx = self.build_open_context(
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                &open_quote,
                &hedge_quote,
                *offset,
                batch_ts,
                side,
                from_key.as_str(),
            );

            let signal = TradeSignal::create(SignalType::ArbOpen, batch_ts, 0.0, ctx.to_bytes());
            self.signal_pub.publish(&signal.to_bytes())?;
        }

        info!(
            "XarbDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={}",
            self.price_offsets.len(),
            SignalType::ArbOpen,
            self.channel_name,
            open_symbol,
            hedge_symbol
        );

        Ok(())
    }

    fn emit_close_signals(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        let batch_ts = get_timestamp_us();
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let from_key = format!("{batch_ts}:dump:{spread_rate:.6}");

        for offset in &self.price_offsets {
            let ctx = self.build_open_context(
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                &open_quote,
                &hedge_quote,
                *offset,
                batch_ts,
                side,
                from_key.as_str(),
            );

            let signal = TradeSignal::create(SignalType::ArbClose, batch_ts, 0.0, ctx.to_bytes());
            self.signal_pub.publish(&signal.to_bytes())?;
        }

        info!(
            "XarbDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={}",
            self.price_offsets.len(),
            SignalType::ArbClose,
            self.channel_name,
            open_symbol,
            hedge_symbol
        );

        Ok(())
    }

    fn emit_cancel_signal(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        let batch_ts = get_timestamp_us();

        let ctx = self.build_cancel_context(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
            &open_quote,
            &hedge_quote,
            batch_ts,
        );

        let signal = TradeSignal::create(SignalType::ArbCancel, batch_ts, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
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
        from_key: &str,
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_trade_symbol);

        ctx.set_side(side);
        ctx.set_order_type(OrderType::Limit);

        let base_price = match side {
            Side::Buy => open_quote.bid,
            Side::Sell => open_quote.ask,
        };
        let raw_open_price = if base_price > 0.0 {
            match side {
                Side::Buy => base_price * (1.0 - price_offset),
                Side::Sell => base_price * (1.0 + price_offset),
            }
        } else {
            0.0
        };
        let aligned_open_price =
            self.align_open_price_for_signal(open_venue, &open_trade_symbol, raw_open_price);

        let base_qty = self.convert_order_amount_to_aligned_base_qty(
            open_venue,
            &open_trade_symbol,
            hedge_venue,
            &hedge_trade_symbol,
            open_quote,
            hedge_quote,
            aligned_open_price,
            side,
        );
        let aligned_open_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            open_venue,
            &open_trade_symbol,
            aligned_open_price,
            base_qty,
        );

        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, &open_trade_symbol);

        let raw_price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let price_fallback = ctx.set_price_with_tick_floor(aligned_open_price, raw_price_tick);
        if raw_price_tick <= 0.0 || price_fallback {
            warn!(
                "XarbDecision: missing price_tick for {:?} symbol_key={}, fallback to full-price tick",
                open_venue, symbol_key
            );
        }
        if ctx.price_count() <= 0 {
            warn!(
                "XarbDecision: invalid aligned_open_price for {:?} symbol_key={}, fallback to zero",
                open_venue, symbol_key
            );
        }

        let raw_amount_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let amount_fallback = ctx.set_amount_with_tick_floor(aligned_open_qty, raw_amount_tick);
        if raw_amount_tick <= 0.0 || amount_fallback {
            warn!(
                "XarbDecision: missing step_size for {:?} symbol_key={}, fallback to full-qty tick",
                open_venue, symbol_key
            );
        }
        if ctx.amount_count() <= 0 {
            warn!(
                "XarbDecision: invalid aligned_open_qty for {:?} symbol_key={}, fallback to zero",
                open_venue, symbol_key
            );
        }

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.price_offset = price_offset;
        ctx.spread_rate = compute_spread_rate(open_quote, hedge_quote);

        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,
            super::common::FactorMode::MM => self.hedge_timeout_mm_us,
        };

        ctx.set_from_key(from_key.as_bytes().to_vec());

        ctx
    }

    fn build_hedge_from_key(
        &self,
        now: i64,
        pnlu_factor: f64,
        rl_return_volatility_factor: Option<f64>,
        pct_change: f64,
        spread_rate: f64,
    ) -> Vec<u8> {
        let rl_return_volatility_factor = rl_return_volatility_factor.unwrap_or(0.0);
        format!(
            "{now}:{pnlu_factor:.6}:{rl_return_volatility_factor:.6}:{pct_change:.6}:{spread_rate:.6}"
        )
        .into_bytes()
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

        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
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

    fn align_open_price_for_signal(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        raw_price: f64,
    ) -> f64 {
        if raw_price <= 0.0 {
            return 0.0;
        }
        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, open_symbol);
        let tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        if tick > 0.0 {
            align_price_floor(raw_price, tick)
        } else {
            raw_price
        }
    }

    fn convert_aligned_base_qty_to_open_venue_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        open_price: f64,
        aligned_base_qty: f64,
    ) -> f64 {
        if aligned_base_qty <= 0.0 || open_price <= 0.0 {
            return 0.0;
        }

        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, open_symbol);

        let qty_multiplier = if venue_qty_is_contracts(open_venue) {
            let Some(multiplier) = contract_qty_multiplier(table, open_venue, &symbol_key) else {
                warn!(
                    "XarbDecision: missing/invalid contract_multiplier for {:?} symbol_key={}, cannot convert aligned base qty to venue qty",
                    open_venue, symbol_key
                );
                return 0.0;
            };
            multiplier
        } else {
            1.0
        };

        let mut venue_qty = aligned_base_qty / qty_multiplier;
        let step = table.step_size(&symbol_key).unwrap_or(0.0);
        if step > 0.0 {
            venue_qty = align_price_floor(venue_qty, step);
            if venue_qty <= 0.0 {
                venue_qty = step;
            }
        }

        if let Some(min_qty) = table.min_qty(&symbol_key) {
            if min_qty > 0.0 && venue_qty < min_qty {
                venue_qty = min_qty;
            }
        }

        if open_venue.is_futures() {
            if let Some(min_notional) = table.min_notional(&symbol_key) {
                if min_notional > 0.0 {
                    let required_base_qty = min_notional / open_price;
                    let required_venue_qty = required_base_qty / qty_multiplier;
                    if venue_qty + 1e-12 < required_venue_qty {
                        venue_qty = if step > 0.0 {
                            align_price_ceil(required_venue_qty, step)
                        } else {
                            required_venue_qty
                        };
                    }
                }
            }
        }

        venue_qty
    }

    fn convert_order_amount_to_aligned_base_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        hedge_venue: TradingVenue,
        hedge_symbol: &str,
        open_quote: &Quote,
        hedge_quote: &Quote,
        open_price: f64,
        open_side: Side,
    ) -> f64 {
        if !(self.order_amount > 0.0) {
            warn!(
                "XarbDecision: order_amount <= 0 when building signal for {}, skip",
                open_symbol
            );
            return 0.0;
        }
        if open_price <= 0.0 {
            warn!(
                "XarbDecision: price for {} <= 0 when converting order amount, fallback to 0",
                open_symbol
            );
            return 0.0;
        }

        let open_table = self.table_for(open_venue);
        let hedge_table = self.table_for(hedge_venue);

        let raw_base_qty = self.order_amount as f64 / open_price;
        let (open_base_step, open_min_base_qty) =
            base_step_size(open_table, open_venue, open_symbol);
        let (hedge_base_step, hedge_min_base_qty) =
            base_step_size(hedge_table, hedge_venue, hedge_symbol);

        let align_step = open_base_step
            .unwrap_or(0.0)
            .max(hedge_base_step.unwrap_or(0.0));
        if align_step <= 0.0 {
            return raw_base_qty;
        }

        let mut base_qty = align_price_floor(raw_base_qty, align_step);
        if base_qty <= 0.0 {
            base_qty = align_step;
        }

        let mut required_min_base = align_step;
        if let Some(v) = open_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }
        if let Some(v) = hedge_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }

        let open_symbol_key = min_qty_symbol_key(open_venue, open_symbol);
        let hedge_symbol_key = min_qty_symbol_key(hedge_venue, hedge_symbol);
        if let Some(min_notional) = open_table.min_notional(&open_symbol_key) {
            if min_notional > 0.0 && open_price > 0.0 {
                required_min_base = required_min_base.max(min_notional / open_price);
            }
        }
        let hedge_side = if open_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let hedge_price_for_min_notional = match hedge_side {
            Side::Buy => hedge_quote.ask,
            Side::Sell => hedge_quote.bid,
        };
        if let Some(min_notional) = hedge_table.min_notional(&hedge_symbol_key) {
            if min_notional > 0.0 && hedge_price_for_min_notional > 0.0 {
                required_min_base =
                    required_min_base.max(min_notional / hedge_price_for_min_notional);
            }
        }

        if base_qty + 1e-12 < required_min_base {
            base_qty = align_price_ceil(required_min_base, align_step);
        }

        // 对合约腿（Binance/OKX/Gate）确保 multiplier 可用：Binance 固定为 1，OKX/Gate 需已加载
        for (venue, table, symbol_key) in [
            (open_venue, open_table, &open_symbol_key),
            (hedge_venue, hedge_table, &hedge_symbol_key),
        ] {
            if venue_qty_is_contracts(venue)
                && contract_qty_multiplier(table, venue, symbol_key).is_none()
            {
                warn!(
                    "XarbDecision: missing contract_multiplier for {:?} symbol_key={} (BinanceFutures fixed=1, OKX/Gate need table), qty alignment may be inaccurate",
                    venue, symbol_key
                );
            }
        }

        // 打印一次关键对齐信息（用于排障）
        if log::log_enabled!(log::Level::Debug) {
            let open_step = open_base_step.unwrap_or(0.0);
            let hedge_step = hedge_base_step.unwrap_or(0.0);
            let open_mid = (open_quote.bid + open_quote.ask) * 0.5;
            let hedge_mid = (hedge_quote.bid + hedge_quote.ask) * 0.5;
            log::debug!(
                "XarbDecision qty_align: open={:?} {} step_base={:.10} hedge={:?} {} step_base={:.10} align_step={:.10} raw_base_qty={:.10} base_qty={:.10} required_min_base={:.10} open_price={:.6} hedge_mid={:.6} open_mid={:.6}",
                open_venue,
                open_symbol_key,
                open_step,
                hedge_venue,
                hedge_symbol_key,
                hedge_step,
                align_step,
                raw_base_qty,
                base_qty,
                required_min_base,
                open_price,
                hedge_mid,
                open_mid
            );
        }

        base_qty
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

    pub fn update_max_hedge_price_pct_change(&mut self, pct_change: f64) {
        if !(pct_change.is_finite() && pct_change >= 1.0 && pct_change <= 99.0) {
            warn!(
                "XarbDecision: max_hedge_price_pct_change 无效(需在1~99)，忽略更新 value={}",
                pct_change
            );
            return;
        }
        self.max_hedge_price_pct_change = pct_change;
        info!(
            "XarbDecision: max_hedge_price_pct_change 更新为 {:.2}",
            pct_change
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

    fn threshold_key(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> ThresholdKey {
        (
            open_venue,
            open_symbol.to_uppercase(),
            hedge_venue,
            hedge_symbol.to_uppercase(),
        )
    }

    fn is_cooldown_hit(
        &self,
        last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
        key: &ThresholdKey,
        now: i64,
    ) -> bool {
        if let Some(&last_ts) = last_ts_map.borrow().get(key) {
            let elapsed = now - last_ts;
            if elapsed < self.signal_cooldown_us {
                return true;
            }
        }
        false
    }

    fn update_last_ts(
        &self,
        last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
        key: ThresholdKey,
        now: i64,
    ) {
        last_ts_map.borrow_mut().insert(key, now);
    }

    fn check_pnlu_factor(&mut self, symbol_key: &str, now_us: i64) -> PnluCheckResult {
        self.factor_value_hub.check_pnlu_factor(symbol_key, now_us)
    }

    fn log_pnlu_check(
        &self,
        symbol_key: &str,
        forward_open: bool,
        backward_open: bool,
        cooldown_hit: bool,
        result: &PnluCheckResult,
    ) {
        let direction = match (forward_open, backward_open) {
            (true, true) => "both",
            (true, false) => "forward",
            (false, true) => "backward",
            (false, false) => "none",
        };
        info!(
            "XarbDecision: pnlu_check symbol={} dir={} ok={} reason={} factor={:?} threshold={:?} ts={:?} target_ts={:?} age_s={:?} ready={:?} quantiles={:?} cooldown_hit={}",
            symbol_key,
            direction,
            result.ok,
            result.reason,
            result.factor,
            result.threshold,
            result.ts,
            result.target_ts,
            result.age_secs,
            result.ready,
            result.quantiles,
            cooldown_hit
        );
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
                    decision.poll_model_output_updates();
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
