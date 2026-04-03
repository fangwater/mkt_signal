use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::depth_pub::query_client::DepthQueryClient;
use crate::depth_pub::query_msg::price_to_tick_index;
use crate::signal::common::SignalBytes;
use crate::signal::common::TradingLeg;
use crate::signal::common::TradingVenue;
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use anyhow::Result;
use log::warn;

use super::arb_qty_align::min_qty_symbol_key;
use super::common::{
    append_key_value_fields, append_tlen_to_from_key, build_decision_from_key_base,
    format_tlen_value,
};
use super::common::Quote;

pub struct ArbHedgeContextCommonInput<'a> {
    pub open_symbol: &'a str,
    pub hedge_symbol: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_quote: &'a Quote,
    pub hedge_quote: &'a Quote,
    pub now: i64,
    pub price_offset: f64,
    pub spread_rate: f64,
    pub from_key: Vec<u8>,
}

pub fn build_arb_taker_hedge_ctx(
    strategy_id: i32,
    client_order_id: i64,
    side: u8,
    hedge_qty: f64,
    qty_tick: f64,
) -> ArbHedgeCtx {
    let mut ctx = ArbHedgeCtx::new_taker(strategy_id, client_order_id, side, hedge_qty, qty_tick);
    ctx.maker_only = false;
    ctx
}

pub struct ArbTakerHedgeSignalInput<'a> {
    pub signal_pub: &'a SignalPublisher,
    pub now: i64,
    pub strategy_id: i32,
    pub client_order_id: i64,
    pub side: u8,
    pub hedge_qty: f64,
    pub qty_tick: f64,
    pub common: ArbHedgeContextCommonInput<'a>,
}

pub fn build_fill_and_publish_arb_taker_hedge(
    input: ArbTakerHedgeSignalInput<'_>,
) -> Result<ArbHedgeCtx> {
    let mut ctx = build_arb_taker_hedge_ctx(
        input.strategy_id,
        input.client_order_id,
        input.side,
        input.hedge_qty,
        input.qty_tick,
    );
    fill_and_publish_arb_hedge_signal(input.signal_pub, input.now, &mut ctx, input.common)?;
    Ok(ctx)
}

pub fn build_arb_maker_hedge_ctx(
    strategy_id: i32,
    client_order_id: i64,
    side: u8,
    hedge_qty: f64,
    qty_tick: f64,
    hedge_price: f64,
    price_tick: f64,
    expire_ts: i64,
) -> Option<ArbHedgeCtx> {
    let ctx = ArbHedgeCtx::new_maker(
        strategy_id,
        client_order_id,
        side,
        hedge_qty,
        qty_tick,
        hedge_price,
        price_tick,
        false,
        expire_ts,
    );
    if ctx.hedge_qty_count() <= 0 || ctx.hedge_price_count() <= 0 {
        return None;
    }
    Some(ctx)
}

pub fn fill_arb_hedge_context_common(ctx: &mut ArbHedgeCtx, input: ArbHedgeContextCommonInput<'_>) {
    ctx.opening_leg = TradingLeg::new(
        input.open_venue,
        input.open_quote.bid,
        input.open_quote.ask,
        input.open_quote.ts,
    );
    ctx.set_opening_symbol(input.open_symbol);
    ctx.hedging_leg = TradingLeg::new(
        input.hedge_venue,
        input.hedge_quote.bid,
        input.hedge_quote.ask,
        input.hedge_quote.ts,
    );
    ctx.set_hedging_symbol(input.hedge_symbol);
    ctx.market_ts = input.now;
    ctx.price_offset = input.price_offset;
    ctx.spread_rate = input.spread_rate;
    ctx.set_from_key(input.from_key);
}

pub fn publish_arb_hedge_signal(
    signal_pub: &SignalPublisher,
    now: i64,
    ctx: &ArbHedgeCtx,
) -> Result<()> {
    let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());
    signal_pub.publish(&signal.to_bytes())?;
    Ok(())
}

pub fn fill_and_publish_arb_hedge_signal(
    signal_pub: &SignalPublisher,
    now: i64,
    ctx: &mut ArbHedgeCtx,
    input: ArbHedgeContextCommonInput<'_>,
) -> Result<()> {
    fill_arb_hedge_context_common(ctx, input);
    publish_arb_hedge_signal(signal_pub, now, ctx)
}

pub struct ArbMakerHedgeSignalInput<'a> {
    pub signal_pub: &'a SignalPublisher,
    pub now: i64,
    pub strategy_id: i32,
    pub client_order_id: i64,
    pub side: u8,
    pub hedge_qty: f64,
    pub qty_tick: f64,
    pub hedge_price: f64,
    pub price_tick: f64,
    pub expire_ts: i64,
    pub common: ArbHedgeContextCommonInput<'a>,
}

pub fn build_fill_and_publish_arb_maker_hedge(
    input: ArbMakerHedgeSignalInput<'_>,
) -> Result<Option<ArbHedgeCtx>> {
    let Some(ctx) = build_and_fill_arb_maker_hedge(ArbMakerHedgeBuildInput {
        strategy_id: input.strategy_id,
        client_order_id: input.client_order_id,
        side: input.side,
        hedge_qty: input.hedge_qty,
        qty_tick: input.qty_tick,
        hedge_price: input.hedge_price,
        price_tick: input.price_tick,
        expire_ts: input.expire_ts,
        common: input.common,
    }) else {
        return Ok(None);
    };
    publish_arb_hedge_signal(input.signal_pub, input.now, &ctx)?;
    Ok(Some(ctx))
}

pub struct ArbMakerHedgeBuildInput<'a> {
    pub strategy_id: i32,
    pub client_order_id: i64,
    pub side: u8,
    pub hedge_qty: f64,
    pub qty_tick: f64,
    pub hedge_price: f64,
    pub price_tick: f64,
    pub expire_ts: i64,
    pub common: ArbHedgeContextCommonInput<'a>,
}

pub fn build_and_fill_arb_maker_hedge(input: ArbMakerHedgeBuildInput<'_>) -> Option<ArbHedgeCtx> {
    let Some(mut ctx) = build_arb_maker_hedge_ctx(
        input.strategy_id,
        input.client_order_id,
        input.side,
        input.hedge_qty,
        input.qty_tick,
        input.hedge_price,
        input.price_tick,
        input.expire_ts,
    ) else {
        return None;
    };
    fill_arb_hedge_context_common(&mut ctx, input.common);
    Some(ctx)
}

pub fn build_spread_arb_hedge_from_key_base(
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    hedge_volatility_factor: Option<f64>,
    pct_change: f64,
    spread_rate: f64,
    premium_rate: Option<f64>,
    spread_fr: Option<f64>,
) -> String {
    let base = build_decision_from_key_base(
        now,
        return_score,
        return_threshold,
        hedge_volatility_factor,
        Some(environment_score),
        environment_threshold,
    );
    append_key_value_fields(
        base,
        &[
            ("spread", format!("{spread_rate:.6}")),
            (
                "premium_rate",
                premium_rate
                    .filter(|v| v.is_finite())
                    .map(|v| format!("{v:.6}"))
                    .unwrap_or_else(|| "NA".to_string()),
            ),
            (
                "spread_fr",
                spread_fr
                    .filter(|v| v.is_finite())
                    .map(|v| format!("{v:.6}"))
                    .unwrap_or_else(|| "NA".to_string()),
            ),
            ("pct_change", format!("{pct_change:.6}")),
        ],
    )
}

pub fn build_simple_hedge_from_key(now: i64, request_seq: u32, spread_rate: f64) -> Vec<u8> {
    format!("{now}:{request_seq}:{spread_rate:.6}").into_bytes()
}

pub fn append_single_tlen_to_hedge_from_key(
    source: &str,
    base_from_key: String,
    hedge_venue: TradingVenue,
    hedge_symbol: &str,
    hedge_price: f64,
    table: &VenueMinQtyTable,
    depth_query_client: &DepthQueryClient,
) -> Vec<u8> {
    let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);
    let symbol_key = min_qty_symbol_key(hedge_venue, &hedge_trade_symbol);
    let raw_price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);

    if !(raw_price_tick.is_finite() && raw_price_tick > 0.0) {
        warn!(
            "{source}: hedge from_key missing price_tick hedge={} venue={:?} symbol_key={}",
            hedge_trade_symbol, hedge_venue, symbol_key
        );
        return append_tlen_to_from_key(&base_from_key, 0.0).into_bytes();
    }
    if hedge_price <= 0.0 {
        warn!(
            "{source}: hedge from_key invalid price hedge={} venue={:?} price={:.8}",
            hedge_trade_symbol, hedge_venue, hedge_price
        );
        return append_tlen_to_from_key(&base_from_key, 0.0).into_bytes();
    }
    let Some(tick_index) = price_to_tick_index(hedge_price, raw_price_tick) else {
        warn!(
            "{source}: hedge from_key tick conversion failed hedge={} venue={:?} price={:.8} price_tick={:.8}",
            hedge_trade_symbol, hedge_venue, hedge_price, raw_price_tick
        );
        return append_tlen_to_from_key(&base_from_key, 0.0).into_bytes();
    };

    match depth_query_client.query_single_tick_index(&hedge_trade_symbol, tick_index) {
        Ok(tlen) => format!("{base_from_key}:tlen={}", format_tlen_value(tlen)).into_bytes(),
        Err(err) => {
            warn!(
                "{source}: hedge from_key tlen query failed hedge={} venue={:?} tick_index={} err={err:#}",
                hedge_trade_symbol, hedge_venue, tick_index
            );
            append_tlen_to_from_key(&base_from_key, 0.0).into_bytes()
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_spread_arb_hedge_from_key(
    source: &str,
    now: i64,
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    hedge_volatility_factor: Option<f64>,
    pct_change: f64,
    spread_rate: f64,
    premium_rate: Option<f64>,
    spread_fr: Option<f64>,
    hedge_venue: TradingVenue,
    hedge_symbol: &str,
    hedge_price: f64,
    table: &VenueMinQtyTable,
    depth_query_client: &DepthQueryClient,
) -> Vec<u8> {
    let base = build_spread_arb_hedge_from_key_base(
        now,
        return_score,
        return_threshold,
        environment_score,
        environment_threshold,
        hedge_volatility_factor,
        pct_change,
        spread_rate,
        premium_rate,
        spread_fr,
    );
    append_single_tlen_to_hedge_from_key(
        source,
        base,
        hedge_venue,
        hedge_symbol,
        hedge_price,
        table,
        depth_query_client,
    )
}
