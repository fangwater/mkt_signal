use crate::strategy::arb_hedge_strategy::ArbHedgeStrategy;
use crate::strategy::manager::OrderTerminalRecorder;
use crate::strategy::mm_hedge_strategy::MarketMakerHedgeStrategy;
use log::info;

const TERMINAL_QTY_EPS: f64 = 1e-12;

impl OrderTerminalRecorder for ArbHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
        close_ts: i64,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
        // Arb 开仓 terminal 直接更新净敞口
        self.net_qty_queue
            .apply_fill(fill_ts, signed_base_qty, price);
        // Arb 开仓 terminal 会形成一笔待对冲需求；close_ts 决定它什么时候进入 due 数量。
        self.pending_hedge_queue
            .put(fill_ts, close_ts, signed_base_qty, price);
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=open qv={:.8} price={:.8} fill_ts={} close_ts={} net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            signed_base_qty,
            price,
            fill_ts,
            close_ts,
            self.net_qty_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
        // Arb 对冲 terminal 只更新真实净敞口；pending 队列由挂单 borrow 和未成交 release 维护。
        self.net_qty_queue
            .apply_fill(fill_ts, signed_base_qty, price);
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=hedge qv={:.8} price={:.8} fill_ts={} net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            signed_base_qty,
            price,
            fill_ts,
            self.net_qty_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }
}

impl OrderTerminalRecorder for MarketMakerHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
        _close_ts: i64,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
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
        self.apply_net_qty_fill(fill_ts, signed_base_qty, price, "open_order_terminal");
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
        // MM open terminal 计入当前 period 的买/卖成交量，后续 hedge query 会带上这段增量。
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
        // MM hedge terminal 只更新净库存；period 买/卖量只由 open/external fill 累加。
        self.apply_net_qty_fill(fill_ts, signed_base_qty, price, "hedge_order_terminal");
        true
    }
}
