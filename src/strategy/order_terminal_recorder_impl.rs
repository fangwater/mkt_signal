use crate::pre_trade::order_manager::Side;
use crate::strategy::arb_hedge_strategy::ArbHedgeStrategy;
use crate::strategy::manager::{OrderTerminalRecorder, Strategy};
use crate::strategy::mm_hedge_strategy::MarketMakerHedgeStrategy;
use log::info;

const TERMINAL_QTY_EPS: f64 = 1e-12;

impl OrderTerminalRecorder for ArbHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        close_ts: i64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        // Arb 开仓 terminal 直接更新净敞口
        self.net_qty_queue
            .apply_fill(terminal_ts, signed_base_qty, price);
        // Arb 开仓 terminal 会形成一笔待对冲需求；close_ts 决定它什么时候进入 due 数量。
        self.pending_hedge_queue
            .put(terminal_ts, close_ts, signed_base_qty, price);
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=open side={:?} order_base_qty={:.8} filled_base_qty={:.8} qv={:.8} price={:.8} terminal_ts={} close_ts={} net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            price,
            terminal_ts,
            close_ts,
            self.net_qty_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if order_base_qty <= TERMINAL_QTY_EPS && filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        let unfilled_base_qty = (order_base_qty - filled_base_qty).max(0.0);
        let pending_release_qv = match side {
            Side::Buy => -unfilled_base_qty,
            Side::Sell => unfilled_base_qty,
        };
        let released_qv = self
            .pending_hedge_queue
            .release(terminal_ts, pending_release_qv, price);
        if filled_base_qty > TERMINAL_QTY_EPS {
            self.net_qty_queue
                .apply_fill(terminal_ts, signed_base_qty, price);
        }
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=hedge side={:?} order_base_qty={:.8} filled_base_qty={:.8} unfilled_base_qty={:.8} released_pending={:.8} qv={:.8} price={:.8} terminal_ts={} net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            side,
            order_base_qty,
            filled_base_qty,
            unfilled_base_qty,
            released_qv,
            signed_base_qty,
            price,
            terminal_ts,
            self.net_qty_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }
}

impl OrderTerminalRecorder for MarketMakerHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        _close_ts: i64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        let buy_qty = if signed_base_qty > 0.0 {
            signed_base_qty
        } else {
            0.0
        };
        let sell_qty = if signed_base_qty < 0.0 {
            -signed_base_qty
        } else {
            0.0
        };
        info!(
            "MMHedgeRecord: strategy_id={} symbol={} leg=open side={:?} order_base_qty={:.8} filled_base_qty={:.8} qv={:.8} price={:.8} terminal_ts={}",
            self.get_id(),
            self.symbol().unwrap_or(""),
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            price,
            terminal_ts
        );
        self.apply_net_qty_fill(terminal_ts, signed_base_qty, price, "open_order_terminal");
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
        // MM open terminal 计入当前 period 的买/卖成交量，后续 hedge query 会带上这段增量。
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if order_base_qty <= TERMINAL_QTY_EPS && filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        info!(
            "MMHedgeRecord: strategy_id={} symbol={} leg=hedge side={:?} order_base_qty={:.8} filled_base_qty={:.8} qv={:.8} price={:.8} terminal_ts={}",
            self.get_id(),
            self.symbol().unwrap_or(""),
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            price,
            terminal_ts
        );
        // MM hedge terminal 只更新净库存；period 买/卖量只由 open/external fill 累加。
        if filled_base_qty > TERMINAL_QTY_EPS {
            self.apply_net_qty_fill(terminal_ts, signed_base_qty, price, "hedge_order_terminal");
        }
        true
    }
}

fn signed_qty_from_side(side: Side, qty: f64) -> f64 {
    match side {
        Side::Buy => qty.abs(),
        Side::Sell => -qty.abs(),
    }
}
