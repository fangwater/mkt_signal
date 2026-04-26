use crate::common::symbol_util::min_qty_symbol_key;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::pre_trade::TradeEngHub;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{align_price_floor, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use log::{error, info, warn};

pub struct ArbOpenSignalParams {
    pub aligned_price: f64,
    pub aligned_qty: f64,
    pub symbol: String,
    pub hedge_symbol: String,
    pub order_type: OrderType,
    pub open_side: Side,
    pub venue: TradingVenue,
    pub hedge_venue: TradingVenue,
}

pub fn refresh_qty_multipliers(
    open_venue: TradingVenue,
    open_symbol: &str,
    hedge_venue: TradingVenue,
    hedge_symbol: &str,
) -> Result<(f64, f64), String> {
    let monitor = MonitorChannel::instance();
    let open_qty_multiplier = monitor.qty_multiplier_for_venue(open_venue, open_symbol)?;
    let hedge_qty_multiplier = monitor.qty_multiplier_for_venue(hedge_venue, hedge_symbol)?;
    Ok((open_qty_multiplier, hedge_qty_multiplier))
}

pub fn base_to_venue_qty(base_qty: f64, multiplier: f64, leg_name: &str) -> Result<f64, String> {
    if multiplier <= 0.0 {
        return Err(format!("{} multiplier invalid: {}", leg_name, multiplier));
    }
    Ok(base_qty / multiplier)
}

pub fn align_taker_qty(venue: TradingVenue, symbol: &str, raw_qty: f64) -> Result<f64, String> {
    if raw_qty <= 0.0 {
        return Err(format!(
            "symbol={} 原始下单量无效 raw_qty={}",
            symbol, raw_qty
        ));
    }

    let symbol_key = min_qty_symbol_key(venue, symbol);

    let Some(table) = MonitorChannel::instance().venue_min_qty_table(venue) else {
        return Err(format!(
            "未初始化 {:?} 的最小下单量表，请检查启动参数",
            venue
        ));
    };

    let mut qty = raw_qty;
    if matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
    ) {
        if venue == TradingVenue::BinanceFutures {
            qty = raw_qty;
        } else {
            let contract_size = table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                format!(
                    "symbol={} 缺少 {:?} 合约乘数，无法将 base qty 转成 contracts",
                    symbol_key, venue
                )
            })?;
            if contract_size <= 0.0 {
                return Err(format!(
                    "symbol={} {:?} contract multiplier invalid: {}",
                    symbol_key, venue, contract_size
                ));
            }
            qty = raw_qty / contract_size;
        }
    }

    if let Some(step) = table.step_size(&symbol_key) {
        if step > 0.0 {
            qty = align_price_floor(qty, step);
        }
    }

    if let Some(min_qty) = table.min_qty(&symbol_key) {
        if min_qty > 0.0 && qty < min_qty {
            qty = min_qty;
        }
    }

    if qty <= 0.0 {
        return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol_key, qty));
    }

    Ok(qty)
}

pub fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

pub fn signed_qty_for_open_side(side: Side, qty: f64) -> f64 {
    match side {
        Side::Buy => qty.abs(),
        Side::Sell => -qty.abs(),
    }
}

pub fn arb_cancel_log_reason(ctx: &ArbCancelCtx) -> &'static str {
    ctx.get_reason().as_log_reason()
}

pub fn parse_arb_open_signal_params(
    strategy_id: i32,
    ctx: &ArbOpenCtx,
) -> Option<ArbOpenSignalParams> {
    let aligned_price = ctx.price_value();
    let aligned_qty = ctx.amount_value();
    let symbol = ctx.get_opening_symbol();
    let hedge_symbol = ctx.get_hedging_symbol();
    let order_type_opt = OrderType::from_u8(ctx.order_type);

    if aligned_price <= 0.0 {
        warn!(
            "HedgeArbStrategy: strategy_id={} open signal price invalid: symbol={} order_type={:?} price={:.8}",
            strategy_id, symbol, order_type_opt, aligned_price
        );
        return None;
    }
    if aligned_qty <= 0.0 {
        error!(
            "HedgeArbStrategy: strategy_id={} 开仓信号数量无效 qty={:.8} price={:.8}，标记策略为不活跃",
            strategy_id, aligned_qty, aligned_price
        );
        return None;
    }

    let order_type = order_type_opt.unwrap_or_else(|| panic!("无效的订单类型: {}", ctx.order_type));
    let open_side =
        Side::from_u8(ctx.side).unwrap_or_else(|| panic!("无效的开仓方向: {}", ctx.side));
    let venue = TradingVenue::from_u8(ctx.opening_leg.venue)
        .unwrap_or_else(|| panic!("无效的交易场所: {}", ctx.opening_leg.venue));
    let hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
        .unwrap_or_else(|| panic!("无效的对冲交易场所: {}", ctx.hedging_leg.venue));

    if !venue.supports_pre_trade_stack() {
        panic!(
            "HedgeArbStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 Binance/OKX/Bybit/Bitget/Gate 的 futures 或 margin",
            strategy_id, venue
        );
    }

    Some(ArbOpenSignalParams {
        aligned_price,
        aligned_qty,
        symbol,
        hedge_symbol,
        order_type,
        open_side,
        venue,
        hedge_venue,
    })
}

pub fn log_force_close_skip(strategy_id: i32, check_name: &str, ctx: &ArbOpenCtx) {
    let opening_symbol = ctx.get_opening_symbol();
    let hedging_symbol = ctx.get_hedging_symbol();
    let opening_venue = TradingVenue::describe_u8(ctx.opening_leg.venue);
    let hedging_venue = TradingVenue::describe_u8(ctx.hedging_leg.venue);
    let side = Side::from_u8(ctx.side).unwrap_or(Side::Buy);
    info!(
        "HedgeArbStrategy: strategy_id={} force_close_mode=true, skip {} 风控 | opening={} {} hedging={} {} side={:?} amount={:.4} price={:.6}",
        strategy_id,
        check_name,
        opening_symbol,
        opening_venue,
        hedging_symbol,
        hedging_venue,
        side,
        ctx.amount_value(),
        ctx.price_value()
    );
}

pub fn panic_invalid_maker_hedge_price(
    strategy_id: i32,
    hedge_symbol: &str,
    fallback_hedge_side: Side,
    ctx: &ArbHedgeCtx,
    check_name: &str,
    qty: f64,
    price: f64,
) -> ! {
    panic!(
        "HedgeArbStrategy: maker hedge limit price invalid check={} strategy_id={} client_order_id={} market_ts={} price_offset={:.6} opening_venue={} hedging_venue={} hedge_symbol={} side={:?} qty={:.8} price={:.8}",
        check_name,
        strategy_id,
        ctx.client_order_id,
        ctx.market_ts,
        ctx.price_offset,
        ctx.opening_leg.venue,
        ctx.hedging_leg.venue,
        hedge_symbol,
        ctx.get_side().unwrap_or(fallback_hedge_side),
        qty,
        price
    );
}

pub fn create_and_send_order(
    strategy_id: i32,
    client_order_id: i64,
    order_type_str: &str,
    symbol: &str,
) -> Result<(), String> {
    let order = MonitorChannel::instance()
        .order_manager()
        .borrow_mut()
        .get(client_order_id);
    if let Some(order) = order.as_ref() {
        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_request_bytes() {
            Ok(req_bin) => {
                if let Err(e) = TradeEngHub::publish_order_request(exchange, &req_bin) {
                    error!(
                        "HedgeArbStrategy: strategy_id={} symbol={} exchange={} 推送{}订单失败: {}",
                        strategy_id, symbol, exchange, order_type_str, e
                    );
                    return Err(format!("推送{}订单失败: {}", order_type_str, e));
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} symbol={} 获取{}订单请求字节失败: {}",
                    strategy_id, symbol, order_type_str, e
                );
                Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
            }
        }
    } else {
        error!(
            "HedgeArbStrategy: strategy_id={} symbol={} 未找到创建的{}订单 client_order_id={}",
            strategy_id, symbol, order_type_str, client_order_id
        );
        Err(format!("未找到创建的{}订单", order_type_str))
    }
}
