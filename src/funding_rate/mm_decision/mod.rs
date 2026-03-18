use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::time::Duration;

use super::common::ReturnScoreThresholdsResolved;
use super::fr_decision::DEFAULT_BACKWARD_CHANNEL;
use super::mkt_channel::MktChannel;
use crate::common::iceoryx_publisher::SIGNAL_PAYLOAD;
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::time_util::get_timestamp_us;
use crate::market_maker::hedge_quote_plan::{
    build_mm_hedge_ctx as build_mm_hedge_ctx_core, resolve_mm_hedge_signal_inputs,
    MmHedgeBuildInput,
};
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::MmHedgeSignalQueryMsg;
use crate::signal::trade_signal::{SignalType, TradeSignal};

mod cancel;
pub mod from_key;
mod open;
mod state;

use cancel::MmCancelDecision;
use open::MmOpenDecision;
use state::MmDecisionState;

thread_local! {
    static MM_DECISION: OnceCell<RefCell<MmDecision>> = OnceCell::new();
}

pub struct MmDecision {
    state: MmDecisionState,
    open_decision: MmOpenDecision,
    cancel_decision: MmCancelDecision,
    backward_sub: GenericSignalSubscriber,
    _node: Node<ipc::Service>,
}

impl MmDecision {
    pub fn is_initialized() -> bool {
        MM_DECISION.with(|cell| cell.get().is_some())
    }

    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let result: Result<()> = MM_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }

            let decision = Self::new_sync(open_venue, hedge_venue)?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize MmDecision singleton"))?;

            debug!(
                "MmDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        });
        result?;

        Self::refresh_min_qty_async(open_venue).await;
        Self::spawn_backward_listener();
        info!("MmDecision backward listener started");
        Ok(())
    }

    fn new_sync(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<Self> {
        let node_name = NodeName::new("mm_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;
        let state = MmDecisionState::new(&node, open_venue, hedge_venue)?;
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;

        Ok(Self {
            state,
            open_decision: MmOpenDecision::new(),
            cancel_decision: MmCancelDecision::new(),
            backward_sub,
            _node: node,
        })
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

    async fn refresh_min_qty_async(open_venue: TradingVenue) {
        let mut open_table = crate::signal::venue_min_qty_table::VenueMinQtyTable::new(open_venue);
        let open_res = open_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.state.open_min_qty_table = open_table;
            }
        });

        match open_res {
            Ok(_) => debug!(
                "MmDecision: open venue min_qty_table loaded, venue={:?}",
                open_venue
            ),
            Err(err) => warn!(
                "MmDecision: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
                open_venue
            ),
        }
    }

    pub fn update_order_interval_ms(&mut self, interval_ms: u64) {
        self.state.update_order_interval_ms(interval_ms);
    }

    pub fn update_open_orders_per_round(&mut self, open_orders_per_round: u32) {
        self.state
            .update_open_orders_per_round(open_orders_per_round);
    }

    pub fn update_mm_hedge_params(
        &mut self,
        hedge_orders_per_round: u32,
        hedge_vol_multiplier: f64,
        hedge_offset_ratio: f64,
        hedge_price_offset_limit_lower: f64,
        hedge_price_offset_limit_upper: f64,
        next_query_delay_ms: u64,
    ) {
        self.state.update_mm_hedge_params(
            hedge_orders_per_round,
            hedge_vol_multiplier,
            hedge_offset_ratio,
            hedge_price_offset_limit_lower,
            hedge_price_offset_limit_upper,
            next_query_delay_ms,
        );
    }

    pub fn update_order_amount(&mut self, order_amount: f32) {
        self.state.update_order_amount(order_amount);
    }

    pub fn update_open_order_timeout(&mut self, open_order_timeout_secs: u64) {
        self.state
            .update_open_order_timeout(open_order_timeout_secs);
    }

    pub fn update_prediction_mode(&mut self, enabled: bool) {
        self.state.update_prediction_mode(enabled);
    }

    pub fn update_enable_open_cancel(&mut self, enabled: bool) {
        self.state.update_enable_open_cancel(enabled);
    }

    pub fn update_model_service_roles(
        &mut self,
        return_model_service: String,
        environment_model_service: String,
    ) {
        self.state.update_model_service_roles(
            &self._node,
            return_model_service,
            environment_model_service,
        );
    }

    pub fn update_return_score_thresholds(
        &mut self,
        thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    ) {
        self.state.update_return_score_thresholds(thresholds);
    }

    pub fn process_open_interval(&mut self) {
        self.open_decision.process_interval(&mut self.state);
    }

    pub fn process_return_score_updates(&mut self) {
        self.cancel_decision
            .process_return_score_updates(&mut self.state);
    }

    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match MmHedgeSignalQueryMsg::from_bytes(data) {
            Ok(query) => query,
            Err(err) => {
                warn!("MmDecision: decode MMHedge query failed: {err}");
                return;
            }
        };
        let symbol = query.get_symbol().to_uppercase();
        if symbol.is_empty() {
            warn!("MmDecision: MMHedge query missing symbol");
            return;
        }
        let quote = match MktChannel::instance().get_quote(&symbol, self.state.hedge_venue) {
            Some(quote) => quote,
            None => {
                warn!(
                    "MmDecision: MMHedge quote unavailable symbol={} venue={:?}",
                    symbol, self.state.hedge_venue
                );
                return;
            }
        };
        let Some(model_service) = self.state.return_model_service.clone() else {
            warn!("MmDecision: MMHedge return_model_service unavailable");
            return;
        };
        let (signal, volatility) = match resolve_mm_hedge_signal_inputs(
            &mut self.state.factor_value_hub,
            &model_service,
            &symbol,
            self.state.hedge_venue,
        ) {
            Ok(values) => values,
            Err(err) => {
                warn!(
                    "MmDecision: MMHedge factor lookup failed symbol={} err={}",
                    symbol, err
                );
                return;
            }
        };
        let input = MmHedgeBuildInput {
            venue: self.state.hedge_venue,
            symbol: &symbol,
            quote,
            volatility,
            signal,
            hedge_vol_multiplier: self.state.hedge_vol_multiplier,
            hedge_offset_ratio: self.state.hedge_offset_ratio,
            order_amount_u: self.state.order_amount_u,
            hedge_orders_per_round: self.state.hedge_orders_per_round,
            offset_low: self.state.hedge_price_offset_limit_lower,
            offset_high_limit: self.state.hedge_price_offset_limit_upper,
            next_query_delay_ms: self.state.next_query_delay_ms,
        };
        let ctx = match build_mm_hedge_ctx_core(input, &self.state.open_min_qty_table, &query) {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!(
                    "MmDecision: build MMHedge ctx failed symbol={} err={}",
                    symbol, err
                );
                return;
            }
        };
        let mut ctx = ctx;
        let hedge_symbol = ctx.get_opening_symbol();
        let tick_indices: Vec<i64> = ctx.price_qv_list.iter().map(|qv| qv.get_count()).collect();
        if tick_indices.is_empty() {
            warn!(
                "MmDecision: MMHedge has empty tick_indices symbol={}",
                symbol
            );
            return;
        }
        match self
            .state
            .depth_query_client
            .query_batch_tick_indices(&hedge_symbol, &tick_indices)
        {
            Ok(tlens) => ctx.tlen_values = tlens,
            Err(err) => {
                warn!(
                    "MmDecision: MMHedge tlen batch query failed symbol={} levels={} err={:#}",
                    symbol,
                    tick_indices.len(),
                    err
                );
                ctx.tlen_values = vec![0.0; tick_indices.len()];
            }
        }
        let signal =
            TradeSignal::create(SignalType::MMHedge, get_timestamp_us(), 0.0, ctx.to_bytes());
        if let Err(err) = self.state.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "MmDecision: publish MMHedge failed symbol={} err={:#}",
                symbol, err
            );
            return;
        }
        info!(
            "MmDecision: MMHedge query reply symbol={} levels={}",
            symbol,
            ctx.price_qv_list.len()
        );
    }

    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                MmDecision::with_mut(|decision| loop {
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => decision.handle_backward_query(data),
                        Ok(None) => break,
                        Err(err) => {
                            warn!("MmDecision: backward_sub receive error: {}", err);
                            break;
                        }
                    }
                });
            }
        });
    }
}
