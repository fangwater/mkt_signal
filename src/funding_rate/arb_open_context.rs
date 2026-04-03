use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::market_maker::quote_plan_levels::QuotePlanLevel;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{TradingLeg, TradingVenue};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

use super::arb_qty_align::{
    convert_aligned_base_qty_to_open_venue_qty, convert_order_amount_to_aligned_base_qty,
    min_qty_symbol_key,
};
use super::common::{compute_spread_rate, FactorMode, Quote};

pub struct ArbOpenContextInput<'a> {
    pub open_symbol: &'a str,
    pub hedge_symbol: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_quote: &'a Quote,
    pub hedge_quote: &'a Quote,
    pub level: &'a QuotePlanLevel,
    pub now: i64,
    pub from_key: &'a str,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub factor_mode: FactorMode,
    pub open_symbol_key: String,
    pub table: &'a VenueMinQtyTable,
    pub convert_order_amount_to_aligned_base_qty:
        &'a dyn Fn(TradingVenue, &str, TradingVenue, &str, &Quote, &Quote, f64, Side) -> f64,
    pub convert_aligned_base_qty_to_open_venue_qty: &'a dyn Fn(TradingVenue, &str, f64, f64) -> f64,
}

pub struct ArbOpenContextTablesInput<'a> {
    pub open_symbol: &'a str,
    pub hedge_symbol: &'a str,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_quote: &'a Quote,
    pub hedge_quote: &'a Quote,
    pub level: &'a QuotePlanLevel,
    pub now: i64,
    pub from_key: &'a str,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub factor_mode: FactorMode,
    pub order_amount: f64,
    pub open_table: &'a VenueMinQtyTable,
    pub hedge_table: &'a VenueMinQtyTable,
}

pub fn build_arb_open_context_from_level(input: ArbOpenContextInput<'_>) -> ArbOpenCtx {
    let mut ctx = ArbOpenCtx::new();
    let open_trade_symbol = normalize_symbol_for_venue(input.open_symbol, input.open_venue);
    let hedge_trade_symbol = normalize_symbol_for_venue(input.hedge_symbol, input.hedge_venue);

    ctx.opening_leg = TradingLeg::new(
        input.open_venue,
        input.open_quote.bid,
        input.open_quote.ask,
        input.open_quote.ts,
    );
    ctx.set_opening_symbol(&open_trade_symbol);

    ctx.hedging_leg = TradingLeg::new(
        input.hedge_venue,
        input.hedge_quote.bid,
        input.hedge_quote.ask,
        input.hedge_quote.ts,
    );
    ctx.set_hedging_symbol(&hedge_trade_symbol);

    ctx.set_side(input.level.side);
    ctx.set_order_type(OrderType::Limit);
    let aligned_open_price = input.level.aligned_price;

    let base_qty = (input.convert_order_amount_to_aligned_base_qty)(
        input.open_venue,
        &open_trade_symbol,
        input.hedge_venue,
        &hedge_trade_symbol,
        input.open_quote,
        input.hedge_quote,
        aligned_open_price,
        input.level.side,
    );
    let aligned_open_qty = (input.convert_aligned_base_qty_to_open_venue_qty)(
        input.open_venue,
        &open_trade_symbol,
        aligned_open_price,
        base_qty,
    );

    let raw_price_tick = input
        .table
        .price_tick(&input.open_symbol_key)
        .unwrap_or(0.0);
    let _ = ctx.set_price_with_tick_floor(aligned_open_price, raw_price_tick);

    let raw_amount_tick = input.table.step_size(&input.open_symbol_key).unwrap_or(0.0);
    let _ = ctx.set_amount_with_tick_floor(aligned_open_qty, raw_amount_tick);

    ctx.exp_time = input.now + input.open_order_ttl_us;
    ctx.create_ts = input.now;
    ctx.price_offset = input.level.offset;
    ctx.spread_rate = compute_spread_rate(input.open_quote, input.hedge_quote);
    ctx.hedge_timeout_us = match input.factor_mode {
        FactorMode::MT => 0,
        FactorMode::MM => input.hedge_timeout_mm_us,
    };

    ctx.set_from_key(input.from_key.as_bytes().to_vec());

    ctx
}

pub fn build_arb_open_context_from_level_with_tables(
    input: ArbOpenContextTablesInput<'_>,
) -> ArbOpenCtx {
    let open_trade_symbol = normalize_symbol_for_venue(input.open_symbol, input.open_venue);
    let symbol_key = min_qty_symbol_key(input.open_venue, &open_trade_symbol);
    build_arb_open_context_from_level(ArbOpenContextInput {
        open_symbol: input.open_symbol,
        hedge_symbol: input.hedge_symbol,
        open_venue: input.open_venue,
        hedge_venue: input.hedge_venue,
        open_quote: input.open_quote,
        hedge_quote: input.hedge_quote,
        level: input.level,
        now: input.now,
        from_key: input.from_key,
        open_order_ttl_us: input.open_order_ttl_us,
        hedge_timeout_mm_us: input.hedge_timeout_mm_us,
        factor_mode: input.factor_mode,
        open_symbol_key: symbol_key,
        table: input.open_table,
        convert_order_amount_to_aligned_base_qty:
            &|open_venue,
              open_symbol,
              hedge_venue,
              hedge_symbol,
              open_quote,
              hedge_quote,
              aligned_open_price,
              side| {
                convert_order_amount_to_aligned_base_qty(
                    input.order_amount as f32,
                    input.open_table,
                    open_venue,
                    open_symbol,
                    input.hedge_table,
                    hedge_venue,
                    hedge_symbol,
                    open_quote,
                    hedge_quote,
                    aligned_open_price,
                    side,
                )
            },
        convert_aligned_base_qty_to_open_venue_qty:
            &|open_venue, open_symbol, open_price, aligned_base_qty| {
                convert_aligned_base_qty_to_open_venue_qty(
                    input.open_table,
                    open_venue,
                    open_symbol,
                    open_price,
                    aligned_base_qty,
                )
            },
    })
}
