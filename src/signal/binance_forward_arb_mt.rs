use std::cell::{Cell, RefCell};
use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::common::account_msg::{ExecutionReportMsg, OrderTradeUpdateMsg};
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::exposure_manager::ExposureManager;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::price_table::PriceTable;
use crate::signal::strategy::Strategy;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceNewMarginOrderRequest, BinanceNewUMOrderRequest,
    TradeRequestType,
};
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use std::cmp::Ordering;

/// 内部统一的 UM 订单更新结果，用于将 REST 推送的状态转换为易于判断的枚举。
/// 这样可以避免在核心逻辑中反复解析字符串状态，也便于统一打印日志。
#[derive(Debug, Clone, Copy)]
enum UmOrderUpdateOutcome {
    Created,
    PartiallyFilled(f64),
    Filled,
    Expired,
    Ignored,
}


/// 策略运行参数
#[derive(Debug, Clone)]
pub struct ArbStrategyCfg {
    pub open_range: f64,
    pub order_timeout_ms: i64,
    pub max_position: f64,
}


impl Default for BinSingleForwardArbStrategyCfg {
    fn default() -> Self {
        Self {
            open_range: 0.001,
            order_timeout_ms: 3_000,
            max_position: 1_000.0,
        }
    }
}

const CANCEL_PENDING_TIMEOUT_US: i64 = 5_000_000; // 5s


/// 对冲套利策略，衍生maker-taker和taker-taker，包括跨所对冲
pub struct HedgeArbStrategy {
    pub strategy_id: i32, //策略id
    pub symbol: String, //交易的统一symbol
    pub create_time: i64, //策略构建时间
    pub open_order_id: i64, //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>, //对冲单会产生一个or多个，因为部分成交
    pub open_timeout_us: Option<i64>, //开仓单最长挂单时间，超过撤销
    pub hedge_timeout_us: Option<i64>, //对冲单最长挂单时间，超过撤销
    pub order_seq: u32, //订单号计数器
    pub risk_checker: RiskChecker, //风控检查
    pub env: PreTradeEnv //预处理global变量封装 
}

impl HedgeArbStrategy {
    pub fn new(
        id: i32,
        now: i64,
        symbol: String,
        order_manager: Rc<RefCell<OrderManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_tx: UnboundedSender<Bytes>,
        max_symbol_exposure_ratio: f64,
        max_total_exposure_ratio: f64,
        max_pos_u: f64,
        max_leverage: f64,
        max_pending_limit_orders: Rc<Cell<i32>>,
        min_qty_table: Rc<MinQtyTable>,
        price_table: Rc<std::cell::RefCell<PriceTable>>,
    ) -> Self {
        let strategy = Self {
            strategy_id: id,
            symbol,
            create_time: now,
            mode,
            margin_order_id: 0,
            initial_margin_order_id: 0,
            um_hedge_order_ids: Vec::new(),
            open_timeout_us: None,
            signal_tx: None,
            lifecycle_logged: Cell::new(false),
            open_signal_meta: None,
            open_signal_logged: Cell::new(false),
            cancel_pending: Cell::new(false),
            cancel_pending_since_us: Cell::new(None),
            order_seq: 0,
            order_manager,
            exposure_manager,
            order_tx,
            max_symbol_exposure_ratio,
            max_total_exposure_ratio,
            max_pos_u,
            max_leverage,
            max_pending_limit_orders,
            min_qty_table,
            price_table,
            period_log_flags: RefCell::new(PeriodLogFlags::default()),
        };

        strategy
    }

    pub fn new_open(
        id: i32,
        now: i64,
        symbol: String,
        order_manager: Rc<RefCell<OrderManager>>,
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_tx: UnboundedSender<Bytes>,
        max_symbol_exposure_ratio: f64,
        max_total_exposure_ratio: f64,
        max_pos_u: f64,
        max_leverage: f64,
        max_pending_limit_orders: Rc<Cell<i32>>,
        min_qty_table: Rc<MinQtyTable>,
        price_table: Rc<std::cell::RefCell<PriceTable>>,
    ) -> Self {
        Self::new(
            StrategyMode::Open,
            id,
            now,
            symbol,
            order_manager,
            exposure_manager,
            order_tx,
            max_symbol_exposure_ratio,
            max_total_exposure_ratio,
            max_pos_u,
            max_leverage,
            max_pending_limit_orders,
            min_qty_table,
            price_table,
        )
    }

    

    pub fn set_signal_sender(&mut self, signal_tx: UnboundedSender<Bytes>) {
        self.signal_tx = Some(signal_tx);
    }

    fn begin_cancel_wait(&self) {
        self.cancel_pending.set(true);
        self.cancel_pending_since_us.set(Some(get_timestamp_us()));
        if let Ok(mut flags) = self.period_log_flags.try_borrow_mut() {
            flags.cancel_pending_logged = false;-
        }
    }

    fn clear_cancel_wait(&self) {
        self.cancel_pending.set(false);
        self.cancel_pending_since_us.set(None);
        if let Ok(mut flags) = self.period_log_flags.try_borrow_mut() {
            flags.cancel_pending_logged = false;
        }
    }

    fn register_um_hedge_order(&mut self, order_id: i64) {
        self.um_hedge_order_ids.push(order_id);
    }

    fn aggregate_um_hedge_position(&self) -> Option<(String, Side, f64)> {
        let manager = self.order_manager.borrow();
        let mut symbol: Option<String> = None;
        let mut side: Option<Side> = None;
        let mut total_qty = 0.0_f64;
        for id in &self.um_hedge_order_ids {
            match manager.get(*id) {
                Some(order) => {
                    let executed_qty = if order.cumulative_filled_quantity > 0.0 {
                        order.cumulative_filled_quantity
                    } else {
                        order.quantity
                    };
                    if executed_qty <= 0.0 {
                        debug!(
                            "{}: strategy_id={} UM 对冲订单 id={} quantity 无效 executed={:.8}",
                            Self::strategy_name(),
                            self.strategy_id,
                            id,
                            executed_qty
                        );
                        continue;
                    }
                    if let Some(sym) = &symbol {
                        if !order.symbol.eq_ignore_ascii_case(sym) {
                            warn!(
                                "{}: strategy_id={} UM 对冲订单 symbol 不一致 id={} symbol={} expected={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                id,
                                order.symbol,
                                sym
                            );
                        }
                    } else {
                        symbol = Some(order.symbol.clone());
                    }
                    if let Some(s) = side {
                        if s != order.side {
                            warn!(
                                "{}: strategy_id={} UM 对冲订单 side 不一致 id={} side={:?} expected={:?}",
                                Self::strategy_name(),
                                self.strategy_id,
                                id,
                                order.side,
                                s
                            );
                        }
                    } else {
                        side = Some(order.side);
                    }
                    total_qty += executed_qty;
                }
                None => warn!(
                    "{}: strategy_id={} 未找到 UM 对冲订单 id={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    id
                ),
            }
        }
        match (symbol, side) {
            (Some(sym), Some(sd)) if total_qty > 1e-8 => Some((sym, sd, total_qty)),
            _ => None,
        }
    }

    fn first_unfilled_order(
        manager: &OrderManager,
        ids: &[i64],
    ) -> Option<(i64, Option<OrderExecutionStatus>)> {
        for id in ids {
            match manager.get(*id) {
                Some(order) => {
                    if order.status != OrderExecutionStatus::Filled {
                        return Some((*id, Some(order.status)));
                    }
                }
                None => return Some((*id, None)),
            }
        }
        None
    }

    fn emit_hedge_signal(&mut self, order_id: i64, hedge_qty: f64) {
        if hedge_qty <= 1e-8 {
            debug!(
                "{}: strategy_id={} order_id={} 对冲增量过小 delta={:.8}",
                Self::strategy_name(),
                self.strategy_id,
                order_id,
                hedge_qty
            );
            return;
        }
        let ctx = BinSingleForwardArbHedgeMTCtx {
            strategy_id: self.strategy_id,
            client_order_id: order_id,
            hedge_qty,
        };
        if let Some(tx) = &self.signal_tx {
            let sig = TradeSignal::create(
                SignalType::BinSingleForwardArbHedgeMT,
                get_timestamp_us(),
                0.0,
                ctx.to_bytes(),
            );
            if let Err(err) = tx.send(sig.to_bytes()) {
                warn!(
                    "{}: strategy_id={} 发送 Hedge 信号失败: {}，改为直接下单",
                    Self::strategy_name(),
                    self.strategy_id,
                    err
                );
                if let Err(e) = self.create_hedge_um_order_from_margin_order(&ctx) {
                    warn!(
                        "{}: strategy_id={} 直接创建对冲失败: {}",
                        Self::strategy_name(),
                        self.strategy_id,
                        e
                    );
                }
            } else {
                debug!(
                    "{}: strategy_id={} 已派发 Hedge 信号 client_order_id={} qty={:.6}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_id,
                    hedge_qty
                );
            }
        } else {
            warn!(
                "{}: strategy_id={} signal_tx 未配置，直接创建对冲单",
                Self::strategy_name(),
                self.strategy_id
            );
            if let Err(err) = self.create_hedge_um_order_from_margin_order(&ctx) {
                warn!(
                    "{}: strategy_id={} 创建对冲订单失败: {}",
                    Self::strategy_name(),
                    self.strategy_id,
                    err
                );
            }
        }
    }
    
    fn handle_signal_open(&mut self, signal: TradeSignal) {
        match signal.signal_type {
            SignalType::BinSingleForwardArbOpenMT => {
                match BinSingleForwardArbOpenCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => match self.create_margin_order(&ctx) {
                        Ok(()) => debug!(
                            "{}: strategy_id={} 成功创建开仓订单",
                            Self::strategy_name(),
                            self.strategy_id
                        ),
                        Err(err) => warn!(
                            "{}: strategy_id={} 开仓下单失败: {}",
                            Self::strategy_name(),
                            self.strategy_id,
                            err
                        ),
                    },
                    Err(err) => warn!(
                        "{}: strategy_id={} 解析开仓上下文失败: {}",
                        Self::strategy_name(),
                        self.strategy_id,
                        err
                    ),
                }
            }
            SignalType::BinSingleForwardArbCancelMT => {
                match BinSingleForwardArbCancelCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_ladder_cancel(&ctx) {
                            warn!(
                                "{}: strategy_id={} 阶梯撤单处理失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                    Err(err) => warn!(
                        "{}: strategy_id={} 解析阶梯撤单上下文失败: {}",
                        Self::strategy_name(),
                        self.strategy_id,
                        err
                    ),
                }
            }
            SignalType::BinSingleForwardArbHedgeMT => {
                match BinSingleForwardArbHedgeMTCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if ctx.strategy_id != self.strategy_id {
                            debug!(
                                "{}: strategy_id={} 忽略他人 hedge 信号 for strategy_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                ctx.strategy_id
                            );
                        } else if let Err(err) = self.create_hedge_um_order_from_margin_order(&ctx)
                        {
                            warn!(
                                "{}: strategy_id={} 创建对冲订单失败: {}",
                                Self::strategy_name(),
                                self.strategy_id,
                                err
                            );
                        }
                    }
                    Err(err) => warn!(
                        "{}: strategy_id={} 解析对冲上下文失败: {}",
                        Self::strategy_name(),
                        self.strategy_id,
                        err
                    ),
                }
            }
            other => debug!(
                "{}: strategy_id={} open-mode 忽略信号 {:?}",
                Self::strategy_name(),
                self.strategy_id,
                other
            ),
        }
    }

    fn handle_ladder_cancel(&mut self, ctx: &BinSingleForwardArbCancelCtx) -> Result<(), String> {
        if !ctx.spot_symbol.eq_ignore_ascii_case(&self.symbol) {
            debug!(
                "{}: strategy_id={} 阶梯撤单信号 symbol={} 与策略 symbol={} 不匹配",
                Self::strategy_name(),
                self.strategy_id,
                ctx.spot_symbol,
                self.symbol
            );
            return Ok(());
        }
        info!(
            "{}: strategy_id={} 阶梯撤单触发 bidask_sr={:.6}",
            Self::strategy_name(),
            self.strategy_id,
            ctx.bidask_sr().unwrap_or(f64::NAN)
        );

        self.force_cancel_open_margin_order()
    }

    fn force_cancel_open_margin_order(&mut self) -> Result<(), String> {
        if self.margin_order_id == 0 {
            debug!(
                "{}: strategy_id={} 阶梯撤单触发但无 margin 开仓单",
                Self::strategy_name(),
                self.strategy_id
            );
            return Ok(());
        }

        let order_snapshot = {
            let manager = self.order_manager.borrow();
            manager.get(self.margin_order_id)
        };

        let Some(order) = order_snapshot else {
            warn!(
                "{}: strategy_id={} 阶梯撤单信号收到但未找到订单 id={}",
                Self::strategy_name(),
                self.strategy_id,
                self.margin_order_id
            );
            self.margin_order_id = 0;
            self.open_timeout_us = None;
            self.clear_cancel_wait();
            return Ok(());
        };

        if order.cancel_requested {
            debug!(
                "{}: strategy_id={} 阶梯撤单已提交，等待交易所确认 order_id={}",
                Self::strategy_name(),
                self.strategy_id,
                order.order_id
            );
            return Ok(());
        }

        if order.status.is_terminal() {
            debug!(
                "{}: strategy_id={} 阶梯撤单触发但订单已终止 status={:?}",
                Self::strategy_name(),
                self.strategy_id,
                order.status
            );
            self.margin_order_id = 0;
            self.open_timeout_us = None;
            self.clear_cancel_wait();
            return Ok(());
        }

        {
            let remaining = (order.quantity - order.cumulative_filled_quantity).max(0.0);
            let rows = vec![vec![
                order.order_id.to_string(),
                order.symbol.clone(),
                order.side.as_str().to_string(),
                Self::format_decimal(order.quantity),
                Self::format_decimal(order.cumulative_filled_quantity),
                Self::format_decimal(remaining),
                Self::format_decimal(order.price),
                order.status.as_str().to_string(),
            ]];
            let table = Self::render_three_line_table(
                &[
                    "OrderId", "Symbol", "Side", "Qty", "Filled", "Remain", "Price", "Status",
                ],
                &rows,
            );
            debug!(
                "{}: strategy_id={} 阶梯撤单提交 margin 限价单\n{}",
                Self::strategy_name(),
                self.strategy_id,
                table
            );
        }

        if let Err(err) = self.submit_margin_cancel(&order.symbol, order.order_id) {
            warn!(
                "{}: strategy_id={} 阶梯撤单提交失败: {}",
                Self::strategy_name(),
                self.strategy_id,
                err
            );
            return Err(err);
        }

        {
            let mut manager = self.order_manager.borrow_mut();
            if !manager.update(order.order_id, |o| {
                o.cancel_requested = true;
            }) {
                warn!(
                    "{}: strategy_id={} 阶梯撤单后更新订单状态失败 id={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order.order_id
                );
            }
        }

        self.open_timeout_us = None;
        self.begin_cancel_wait();

        Ok(())
    }

    fn is_active_open(&self) -> bool {
        if self.cancel_pending.get() && self.margin_order_id == 0 {
            let now = get_timestamp_us();
            let since = self.cancel_pending_since_us.get().unwrap_or(now);
            let elapsed = now.saturating_sub(since);
            if elapsed <= CANCEL_PENDING_TIMEOUT_US {
                if let Ok(mut flags) = self.period_log_flags.try_borrow_mut() {
                    if !flags.cancel_pending_logged {
                        debug!(
                            "{}: strategy_id={} waiting for margin cancel confirmation (elapsed={}ms)",
                            Self::strategy_name(),
                            self.strategy_id,
                            elapsed / 1_000
                        );
                        flags.cancel_pending_logged = true;
                    }
                }
                return true;
            } else {
                if let Ok(mut flags) = self.period_log_flags.try_borrow_mut() {
                    flags.cancel_pending_logged = false;
                }
                warn!(
                    "{}: strategy_id={} cancel pending exceeded timeout {}ms, forcing teardown",
                    Self::strategy_name(),
                    self.strategy_id,
                    CANCEL_PENDING_TIMEOUT_US / 1_000
                );
                self.clear_cancel_wait();
            }
        } else if let Ok(mut flags) = self.period_log_flags.try_borrow_mut() {
            flags.cancel_pending_logged = false;
        }

        let manager_ref = self.order_manager.borrow();
        if self.margin_order_id == 0 {
            if !self.um_hedge_order_ids.is_empty() {
                self.log_lifecycle_summary("开仓提前结束");
            }
            self.period_log_flags.borrow_mut().pending_um_hedge_logged = false;
            warn!(
                "{}: strategy_id={} 缺少 margin 开仓单，返回非活跃",
                Self::strategy_name(),
                self.strategy_id
            );
            return false;
        }

        let Some(margin_order) = manager_ref.get(self.margin_order_id) else {
            self.period_log_flags.borrow_mut().pending_um_hedge_logged = false;
            warn!(
                "{}: strategy_id={} 未找到 margin 开仓单 id={}，返回非活跃",
                Self::strategy_name(),
                self.strategy_id,
                self.margin_order_id
            );
            return false;
        };

        if margin_order.status == OrderExecutionStatus::Filled {
            {
                let mut flags = self.period_log_flags.borrow_mut();
                flags.last_margin_open_status = Some(OrderExecutionStatus::Filled);
            }

            let filled_qty = margin_order.cumulative_filled_quantity.max(0.0);
            let hedged_qty = margin_order.hedged_filled_quantity.max(0.0);
            let unhedged_qty = (filled_qty - hedged_qty).max(0.0);

            if unhedged_qty > 1e-8 {
                let mut flags = self.period_log_flags.borrow_mut();
                if !flags.pending_um_hedge_logged {
                    debug!(
                        "{}: strategy_id={} margin 开仓单已成交 id={} 待派发 UM 对冲 delta_qty={:.6}",
                        Self::strategy_name(),
                        self.strategy_id,
                        margin_order.order_id,
                        unhedged_qty
                    );
                    flags.pending_um_hedge_logged = true;
                }
                return true;
            } else {
                let mut flags = self.period_log_flags.borrow_mut();
                flags.pending_um_hedge_logged = false;
            }
        } else if margin_order.status.is_terminal() {
            warn!(
                "{}: strategy_id={} margin 开仓单终结 status={}，策略退出",
                Self::strategy_name(),
                self.strategy_id,
                margin_order.status.as_str()
            );
            self.period_log_flags.borrow_mut().last_margin_open_status = Some(margin_order.status);
            self.period_log_flags.borrow_mut().pending_um_hedge_logged = false;
            return false;
        } else {
            let mut flags = self.period_log_flags.borrow_mut();
            if flags.last_margin_open_status != Some(margin_order.status) {
                debug!(
                    "{}: strategy_id={} margin 开仓单状态={}，等待成交",
                    Self::strategy_name(),
                    self.strategy_id,
                    margin_order.status.as_str()
                );
                flags.last_margin_open_status = Some(margin_order.status);
            }
            flags.pending_um_hedge_logged = false;
            return true;
        }

        if self.um_hedge_order_ids.is_empty() {
            self.period_log_flags.borrow_mut().last_um_hedge_status = None;
            info!(
                "{}: strategy_id={} margin 开仓已完成且无 UM 对冲单，生命周期结束",
                Self::strategy_name(),
                self.strategy_id
            );
            self.log_lifecycle_summary("开仓完成");
            return false;
        }

        if let Some((order_id, status_opt)) =
            Self::first_unfilled_order(&manager_ref, &self.um_hedge_order_ids)
        {
            let mut flags = self.period_log_flags.borrow_mut();
            if flags.last_um_hedge_status != status_opt {
                match status_opt {
                    Some(status) => debug!(
                        "{}: strategy_id={} UM 对冲单 id={} 状态={}，等待成交",
                        Self::strategy_name(),
                        self.strategy_id,
                        order_id,
                        status.as_str()
                    ),
                    None => info!(
                        "{}: strategy_id={} 未找到 UM 对冲单 id={}，待平仓，保持激活状态",
                        Self::strategy_name(),
                        self.strategy_id,
                        order_id
                    ),
                }
                flags.last_um_hedge_status = status_opt;
            }
            return true;
        } else {
            self.period_log_flags.borrow_mut().last_um_hedge_status =
                Some(OrderExecutionStatus::Filled);
        }

        info!(
            "{}: strategy_id={} margin 开仓及 UM 对冲均已完成，生命周期结束",
            Self::strategy_name(),
            self.strategy_id
        );

        self.log_lifecycle_summary("开仓完成");

        false
    }
    // 检查当前 symbol 的限价挂单是否超过阈值

    //2、传入binance_pm_spot_manager的ref，检测当前symbol的敞口
    //symbol是xxusdt，查看当前symbol的敞口是否大于总资产比例的3%
    fn check_for_symbol_exposure(&self, symbol: &str, exposure_manager: &ExposureManager) -> bool {
        let limit = self.max_symbol_exposure_ratio;
        if limit <= 0.0 {
            return true;
        }

        let symbol_upper = symbol.to_uppercase();
        let Some(base_asset) = Self::extract_base_asset(&symbol_upper) else {
            return true;
        };

        let Some(entry) = exposure_manager.exposure_for_asset(&base_asset) else {
            return true;
        };

        let total_equity = exposure_manager.total_equity();
        if total_equity <= f64::EPSILON {
            debug!(
                "{}: 账户总权益近似为 0，无法计算敞口占比",
                Self::strategy_name()
            );
            return false;
        }

        // 使用标记价格将该资产敞口估值为 USDT
        let mark = if base_asset.eq_ignore_ascii_case("USDT") {
            1.0
        } else {
            let sym = format!("{}USDT", base_asset);
            // 一次性获取快照，避免持有 RefCell 借用贯穿日志
            let snap = self.price_table.borrow().snapshot();
            snap.get(&sym).map(|e| e.mark_price).unwrap_or(0.0)
        };

        let exposure_usdt = if mark > 0.0 {
            entry.exposure * mark
        } else {
            0.0
        };

        // 若缺少价格但敞口非零，回退到数量比例并提示
        if mark == 0.0 && entry.exposure != 0.0 {
            let ratio = entry.exposure.abs() / total_equity;
            if ratio > limit {
                debug!(
                    "{}: 资产 {} 敞口占比(数量) {:.4}% 超过阈值 {:.2}% (敞口qty={:.6}, 权益={:.6})",
                    Self::strategy_name(),
                    base_asset,
                    ratio * 100.0,
                    limit * 100.0,
                    entry.exposure,
                    total_equity
                );
                return false;
            }
            return true;
        }

        let ratio = exposure_usdt.abs() / total_equity;
        if ratio > limit {
            debug!(
                "{}: 资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口USDT={:.6}, 权益={:.6})",
                Self::strategy_name(),
                base_asset,
                ratio * 100.0,
                limit * 100.0,
                exposure_usdt,
                total_equity
            );
            false
        } else {
            true
        }
    }

    //3、检查总敞口是否超过配置阈值
    fn check_for_total_exposure(&self, exposure_manager: &ExposureManager) -> bool {
        let limit = self.max_total_exposure_ratio;
        if limit <= 0.0 {
            return true;
        }

        let total_equity = exposure_manager.total_equity();
        if total_equity <= f64::EPSILON {
            debug!(
                "{}: 账户总权益近似为 0，无法计算总敞口占比",
                Self::strategy_name()
            );
            return false;
        }

        // 使用价格表将所有非 USDT 资产净敞口估值为 USDT，并取绝对值求和
        let snap = self.price_table.borrow().snapshot();
        let mut abs_total_usdt = 0.0_f64;
        for e in exposure_manager.exposures() {
            let asset = e.asset.to_uppercase();
            if asset == "USDT" {
                continue;
            }
            let sym = format!("{}USDT", asset);
            let mark = snap.get(&sym).map(|p| p.mark_price).unwrap_or(0.0);
            let exposure_usdt = (e.spot_total_wallet + e.um_net_position) * mark;
            abs_total_usdt += exposure_usdt.abs();
        }

        let ratio = abs_total_usdt / total_equity;
        if ratio > limit {
            debug!(
                "{}: 总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益={:.6})",
                Self::strategy_name(),
                ratio * 100.0,
                limit * 100.0,
                abs_total_usdt,
                total_equity
            );
            false
        } else {
            true
        }
    }

    fn check_for_leverage(&self, exposure_manager: &ExposureManager) -> bool {
        let limit = self.max_leverage;
        if limit <= 0.0 {
            return true;
        }

        let total_equity = exposure_manager.total_equity();
        if total_equity <= f64::EPSILON {
            debug!(
                "{}: 账户总权益近似为 0，无法计算杠杆占比",
                Self::strategy_name()
            );
            return false;
        }

        let total_position = exposure_manager.total_position();
        let leverage = total_position / total_equity;
        if leverage > limit {
            debug!(
                "{}: 当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益={:.6})",
                Self::strategy_name(),
                leverage,
                limit,
                total_position,
                total_equity
            );
            false
        } else {
            true
        }
    }

    fn ensure_max_pos_u(
        &self,
        spot_symbol: &str,
        base_asset: &str,
        current_spot_qty: f64,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        if !(self.max_pos_u > 0.0) {
            return Ok(());
        }

        let base_upper = base_asset.to_uppercase();
        let mark_symbol = format!("{}USDT", base_upper);
        let price_from_table = {
            let table = self.price_table.borrow();
            table.mark_price(&mark_symbol)
        };
        let price = price_from_table.or_else(|| {
            if price_hint > 0.0 {
                Some(price_hint)
            } else {
                None
            }
        });

        let Some(price) = price else {
            warn!(
                "{}: symbol={} 缺少 USDT 标记价格，无法校验 max_pos_u",
                Self::strategy_name(),
                spot_symbol
            );
            return Err(format!(
                "{}: symbol={} 缺少价格信息，无法校验 max_pos_u",
                Self::strategy_name(),
                spot_symbol
            ));
        };

        let projected_qty = current_spot_qty + additional_qty;
        let current_usdt = current_spot_qty.abs() * price;
        let order_usdt = additional_qty.abs() * price;
        let projected_usdt = projected_qty.abs() * price;
        let limit_eps = 1e-6_f64;

        if projected_usdt > self.max_pos_u + limit_eps {
            warn!(
                "{}: symbol={} 当前现货={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 预计现货={:.4}USDT 超过阈值 {:.4}USDT",
                Self::strategy_name(),
                spot_symbol,
                current_spot_qty,
                current_usdt,
                additional_qty,
                order_usdt,
                projected_usdt,
                self.max_pos_u
            );
            return Err(format!(
                "{}: symbol={} 预计现货持仓 {:.4}USDT 超过阈值 {:.4}USDT",
                Self::strategy_name(),
                spot_symbol,
                projected_usdt,
                self.max_pos_u
            ));
        }

        debug!(
            "{}: symbol={} 现货持仓限制通过 current={:.4}USDT add={:.4}USDT projected={:.4}USDT limit={:.4}USDT",
            Self::strategy_name(),
            spot_symbol,
            current_usdt,
            order_usdt,
            projected_usdt,
            self.max_pos_u
        );

        Ok(())
    }

    fn extract_base_asset(symbol_upper: &str) -> Option<String> {
        const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
        for quote in QUOTES {
            if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
                return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
            }
        }
        None
    }

    pub fn strategy_name() -> &'static str {
        "BinSingleForwardArbStrategyMT"
    }

    fn create_hedge_um_order_from_margin_order(
        &mut self,
        hedge_ctx: &BinSingleForwardArbHedgeMTCtx,
    ) -> Result<(), String> {
        let margin_order = {
            let manager = self.order_manager.borrow();
            manager
                .get(hedge_ctx.client_order_id)
                .ok_or_else(|| {
                    format!(
                        "{}: 未找到 margin 订单 id={}",
                        Self::strategy_name(),
                        hedge_ctx.client_order_id
                    )
                })?
                .clone()
        };

        if margin_order.get_strategy_id() != self.strategy_id {
            return Err(format!(
                "{}: strategy_id={} hedge 信号携带的订单属于 strategy_id={}",
                Self::strategy_name(),
                self.strategy_id,
                margin_order.get_strategy_id()
            ));
        }

        if margin_order.quantity <= 0.0 {
            return Err(format!(
                "{}: margin 订单数量非法 amount={}",
                Self::strategy_name(),
                margin_order.quantity
            ));
        }

        let (hedge_side_enum, hedge_side_str, position_side_str) = match margin_order.side {
            Side::Buy => (Side::Sell, "SELL", "SHORT"),
            Side::Sell => (Side::Buy, "BUY", "LONG"),
        };

        let filled_qty = margin_order.cumulative_filled_quantity;
        let hedged_qty = margin_order.hedged_filled_quantity;
        let mut target_qty = if hedge_ctx.hedge_qty > 0.0 {
            hedge_ctx.hedge_qty
        } else {
            filled_qty - hedged_qty
        };

        target_qty = target_qty.min(margin_order.quantity - hedged_qty);
        if target_qty <= 1e-8 {
            debug!(
                "{}: strategy_id={} 无需触发对冲，filled={:.6} hedged={:.6}",
                Self::strategy_name(),
                self.strategy_id,
                filled_qty,
                hedged_qty
            );
            return Ok(());
        }

        let hedge_symbol = margin_order.symbol.clone();
        let qty_step = self
            .min_qty_table
            .futures_um_step_by_symbol(&hedge_symbol)
            .or_else(|| self.min_qty_table.margin_step_by_symbol(&hedge_symbol))
            .or_else(|| self.min_qty_table.spot_step_by_symbol(&hedge_symbol))
            .unwrap_or(0.0);
        let aligned_qty = if qty_step > 0.0 {
            align_price_floor(target_qty, qty_step)
        } else {
            target_qty
        };

        if aligned_qty <= 1e-8 {
            debug!(
                "{}: strategy_id={} 对冲数量在对齐后过小 step={:.8} raw_qty={:.8}",
                Self::strategy_name(),
                self.strategy_id,
                qty_step,
                target_qty
            );
            return Ok(());
        }

        let min_qty = self
            .min_qty_table
            .futures_um_min_qty_by_symbol(&hedge_symbol)
            .or_else(|| self.min_qty_table.margin_min_qty_by_symbol(&hedge_symbol))
            .or_else(|| self.min_qty_table.spot_min_qty_by_symbol(&hedge_symbol))
            .unwrap_or(0.0);
        if min_qty > 0.0 && aligned_qty + 1e-12 < min_qty {
            debug!(
                "{}: strategy_id={} 对冲数量 {:.8} 小于最小下单量 {:.8}，等待更多成交",
                Self::strategy_name(),
                self.strategy_id,
                aligned_qty,
                min_qty
            );
            return Ok(());
        }

        let min_notional = self
            .min_qty_table
            .futures_um_min_notional_by_symbol(&hedge_symbol)
            .unwrap_or(0.0);
        if min_notional > 0.0 {
            let mark_price = self
                .price_table
                .borrow()
                .mark_price(&hedge_symbol)
                .unwrap_or(0.0);
            if mark_price <= 0.0 {
                debug!(
                    "{}: strategy_id={} 缺少 {} 的标记价格，延迟对冲 qty={:.8}",
                    Self::strategy_name(),
                    self.strategy_id,
                    hedge_symbol,
                    aligned_qty
                );
                return Ok(());
            }
            let notional = mark_price * aligned_qty;
            if notional + 1e-8 < min_notional {
                debug!(
                    "{}: strategy_id={} 对冲名义金额 {:.8} 低于阈值 {:.8} (px={:.8} qty={:.8})，等待更多成交",
                    Self::strategy_name(),
                    self.strategy_id,
                    notional,
                    min_notional,
                    mark_price,
                    aligned_qty
                );
                return Ok(());
            }
        }

        let hedge_quantity_str = format_quantity(aligned_qty);
        let order_id = self.next_order_id();
        let create_time = get_timestamp_us();

        let params = Bytes::from(format!(
            "symbol={}&side={}&type=MARKET&quantity={}&positionSide={}&newClientOrderId={}",
            hedge_symbol, hedge_side_str, hedge_quantity_str, position_side_str, order_id
        ));

        let request = BinanceNewUMOrderRequest::create(create_time, order_id, params);
        let payload = request.to_bytes();
        debug!(
            "{}: strategy_id={} UM 对冲下单参数 symbol={} side={} pos_side={} qty={} clientOrderId={} payload_len={}",
            Self::strategy_name(),
            self.strategy_id,
            hedge_symbol,
            hedge_side_str,
            position_side_str,
            hedge_quantity_str,
            order_id,
            payload.len()
        );

        self.order_tx
            .send(payload)
            .map_err(|e| format!("{}: 推送对冲订单失败: {}", Self::strategy_name(), e))?;

        let mut order_manager = self.order_manager.borrow_mut();
        let mut hedge_order = Order::new(
            order_id,
            OrderType::Market,
            hedge_symbol,
            hedge_side_enum,
            aligned_qty,
            0.0,
        );
        hedge_order.set_submit_time(create_time);
        order_manager.insert(hedge_order);

        let new_hedged_total = hedged_qty + aligned_qty;
        let _ = order_manager.update(margin_order.order_id, |order| {
            order.update_hedged_filled_quantity(new_hedged_total);
        });
        drop(order_manager);

        self.register_um_hedge_order(order_id);

        debug!(
            "{}: strategy_id={} 创建 UM 对冲订单成功 order_id={} qty={:.6} side={} hedged_total={:.6}",
            Self::strategy_name(),
            self.strategy_id,
            order_id,
            aligned_qty,
            hedge_side_str,
            new_hedged_total
        );

        Ok(())
    }

    fn um_order_label(&self, client_order_id: i64) -> &'static str {
        if self.um_hedge_order_ids.contains(&client_order_id) {
            "UM 对冲单"
        } else {
            "UM 未识别订单"
        }
    }

    fn apply_um_order_update(
        order: &mut Order,
        event: &OrderTradeUpdateMsg,
    ) -> UmOrderUpdateOutcome {
        match event.execution_type.as_str() {
            "NEW" | "TRADE" => Self::apply_um_fill_state(order, event),
            "EXPIRED" => {
                warn!(
                    "UM Order Execution Type=EXPIRED，保持在 Expired 状态，raw_event={:?}",
                    event
                );
                UmOrderUpdateOutcome::Expired
            }
            "CANCELED" => {
                panic!(
                    "UM Order Execution Type=CANCELED，不符合预期，raw_event={:?}",
                    event
                );
            }
            "CALCULATED" => {
                panic!(
                    "UM Order Execution Type=CALCULATED(强平)，raw_event={:?}",
                    event
                );
            }
            other => {
                warn!(
                    "UM Order Execution Type={} 未识别，raw_event={:?}，忽略",
                    other, event
                );
                UmOrderUpdateOutcome::Ignored
            }
        }
    }

    /// 打印该 symbol 下当前挂着的限价单（非终态）为三线表
    fn log_pending_limit_orders(symbol: &str, order_manager: &OrderManager) {
        let now_us = get_timestamp_us();
        let mut rows: Vec<Vec<String>> = Vec::new();
        for id in order_manager.get_all_ids() {
            if let Some(ord) = order_manager.get(id) {
                if !ord.symbol.eq_ignore_ascii_case(symbol) {
                    continue;
                }
                if !ord.order_type.is_limit() {
                    continue;
                }
                if ord.status.is_terminal() {
                    continue;
                }
                let age_ms = if ord.submit_time > 0 {
                    (now_us.saturating_sub(ord.submit_time) / 1000) as i64
                } else {
                    0
                };
                rows.push(vec![
                    ord.order_id.to_string(),
                    ord.side.as_str().to_string(),
                    Self::format_decimal(ord.quantity),
                    Self::format_decimal(ord.price),
                    ord.status.as_str().to_string(),
                    age_ms.to_string(),
                ]);
            }
        }
        // 稳定排序：按 age_ms 降序，其次按 order_id 升序
        rows.sort_by(|a, b| {
            let age_a: i64 = a[5].parse().unwrap_or(0);
            let age_b: i64 = b[5].parse().unwrap_or(0);
            match age_b.cmp(&age_a) {
                Ordering::Equal => a[0].cmp(&b[0]),
                other => other,
            }
        });
        let table = Self::render_three_line_table(
            &["OrderId", "Side", "Qty", "Price", "Status", "Age(ms)"],
            &rows,
        );
        warn!(
            "{}: symbol={} 当前待成交限价单列表\n{}",
            BinSingleForwardArbStrategyMT::strategy_name(),
            symbol,
            table
        );
    }
}

impl Drop for BinSingleForwardArbStrategyMT {
    fn drop(&mut self) {
        self.log_lifecycle_summary("生命周期结束");
        self.cleanup_strategy_orders();
        debug!(
            "{}: strategy_id={} 生命周期结束，相关订单已回收",
            Self::strategy_name(),
            self.strategy_id
        );
    }
}

impl BinSingleForwardArbStrategyMT {
    fn format_decimal(value: f64) -> String {
        if value == 0.0 {
            return "0".to_string();
        }
        let mut s = format!("{:.6}", value);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        if s.is_empty() {
            "0".to_string()
        } else {
            s
        }
    }

    fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
        let widths = Self::compute_widths(headers, rows);
        let mut out = String::new();
        out.push_str(&Self::build_separator(&widths, '-'));
        out.push('\n');
        out.push_str(&Self::build_row(
            headers
                .iter()
                .map(|h| h.to_string())
                .collect::<Vec<String>>(),
            &widths,
        ));
        out.push('\n');
        out.push_str(&Self::build_separator(&widths, '='));
        if rows.is_empty() {
            out.push('\n');
            out.push_str(&Self::build_separator(&widths, '-'));
            return out;
        }
        for row in rows {
            out.push('\n');
            out.push_str(&Self::build_row(row.clone(), &widths));
        }
        out.push('\n');
        out.push_str(&Self::build_separator(&widths, '-'));
        out
    }

    fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
        let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
        for row in rows {
            for (idx, cell) in row.iter().enumerate() {
                if idx >= widths.len() {
                    continue;
                }
                widths[idx] = widths[idx].max(cell.len());
            }
        }
        widths
    }

    fn build_separator(widths: &[usize], fill: char) -> String {
        let mut line = String::new();
        line.push('+');
        for width in widths {
            line.push_str(&fill.to_string().repeat(width + 2));
            line.push('+');
        }
        line
    }

    fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
        let mut row = String::new();
        row.push('|');
        for (cell, width) in cells.iter().zip(widths.iter()) {
            row.push(' ');
            row.push_str(&format!("{:<width$}", cell, width = *width));
            row.push(' ');
            row.push('|');
        }
        row
    }
}

impl BinSingleForwardArbStrategyMT {
    fn apply_um_fill_state(order: &mut Order, event: &OrderTradeUpdateMsg) -> UmOrderUpdateOutcome {
        order.set_exchange_order_id(event.order_id);
        match event.order_status.as_str() {
            "NEW" => {
                order.update_status(OrderExecutionStatus::Create);
                order.record_exchange_create(event.event_time);
                UmOrderUpdateOutcome::Created
            }
            "PARTIALLY_FILLED" => {
                order.record_exchange_update(event.event_time);
                order.update_cumulative_filled_quantity(event.cumulative_filled_quantity);
                // 市价单填充已知的成交均价/最新成交价，便于生命周期汇总展示
                if order.order_type.is_market() {
                    let px = if event.average_price > 0.0 {
                        event.average_price
                    } else if event.last_executed_price > 0.0 {
                        event.last_executed_price
                    } else {
                        0.0
                    };
                    if px > 0.0 {
                        order.price = px;
                    }
                }
                UmOrderUpdateOutcome::PartiallyFilled(event.cumulative_filled_quantity)
            }
            "FILLED" => {
                order.update_status(OrderExecutionStatus::Filled);
                order.set_filled_time(event.event_time);
                order.update_cumulative_filled_quantity(event.cumulative_filled_quantity);
                order.record_exchange_update(event.event_time);
                // 市价单在完全成交时写入最终成交均价
                if order.order_type.is_market() {
                    let px = if event.average_price > 0.0 {
                        event.average_price
                    } else if event.last_executed_price > 0.0 {
                        event.last_executed_price
                    } else {
                        0.0
                    };
                    if px > 0.0 {
                        order.price = px;
                    }
                }

                if order.hedged_quantily.abs() > 1e-8 {
                    panic!(
                        "UM Order Status=FILLED 但仍有剩余数量，order={:?} remaining={}",
                        order, order.hedged_quantily
                    );
                }

                UmOrderUpdateOutcome::Filled
            }
            "EXPIRED_IN_MATCH" => UmOrderUpdateOutcome::Ignored,
            "CANCELED" => {
                panic!("UM Order Status=CANCELED，不符合预期，order={:?}", order);
            }
            "EXPIRED" => {
                warn!("UM Order Status=EXPIRED，order={:?}", order);
                order.record_exchange_update(event.event_time);
                UmOrderUpdateOutcome::Expired
            }
            other => {
                panic!("UM Order Status={} 未识别，order={:?}", other, order);
            }
        }
    }

    fn submit_margin_cancel(&self, symbol: &str, order_id: i64) -> Result<(), String> {
        let now = get_timestamp_us();
        // 使用 origClientOrderId 以客户端订单ID撤单；当前未保存交易所 orderId
        let params = Bytes::from(format!("symbol={}&origClientOrderId={}", symbol, order_id));
        let request = BinanceCancelMarginOrderRequest::create(now, order_id, params);

        self.order_tx
            .send(request.to_bytes())
            .map_err(|e| format!("{}: 发送 margin 撤单失败: {}", Self::strategy_name(), e))
    }

    fn next_order_id(&mut self) -> i64 {
        let seq = self.order_seq;
        self.order_seq = self.order_seq.wrapping_add(1);
        Self::compose_order_id(self.strategy_id, seq)
    }

    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn log_open_signal_summary(&self, order: &Order) {
        if self.mode != StrategyMode::Open {
            return;
        }
        if self.open_signal_logged.get() {
            return;
        }
        if order.cumulative_filled_quantity <= 1e-8 {
            return;
        }
        let Some(meta) = &self.open_signal_meta else {
            return;
        };

        self.open_signal_logged.set(true);

        let mut rows: Vec<Vec<String>> = Vec::new();
        rows.push(vec![
            "Signal".to_string(),
            meta.spot_symbol.clone(),
            meta.side.as_str().to_string(),
            Self::format_decimal(meta.amount as f64),
            Self::format_decimal(meta.price),
            meta.order_type.as_str().to_string(),
            format!(
                "tick={:.8}, exp={}, ts={}",
                meta.price_tick, meta.exp_time, meta.trigger_ts_us
            ),
        ]);

        let filled = Self::format_decimal(order.cumulative_filled_quantity);
        let qty = Self::format_decimal(order.quantity);
        let exchange_info = order
            .exchange_order_id
            .map(|id| format!(" exchange_id={}", id))
            .unwrap_or_default();
        let create_tp = if order.create_time > 0 {
            order.create_time.to_string()
        } else {
            "-".to_string()
        };
        let updates_str = if order.update_event_times.is_empty() {
            "-".to_string()
        } else {
            format!("{:?}", order.update_event_times)
        };
        let extra = format!(
            "filled={}/{} status={}{} create_tp={} updates={}",
            filled,
            qty,
            order.status.as_str(),
            exchange_info,
            create_tp,
            updates_str
        );
        rows.push(vec![
            format!("Order {}", order.order_id),
            order.symbol.clone(),
            order.side.as_str().to_string(),
            qty.clone(),
            Self::format_decimal(order.price),
            order.order_type.as_str().to_string(),
            extra,
        ]);

        let fmt_decimal = |value: f64| Self::format_decimal(value);
        rows.push(vec![
            "Book".to_string(),
            format!("spot_bid={}", fmt_decimal(meta.spot_bid0)),
            format!("spot_ask={}", fmt_decimal(meta.spot_ask0)),
            format!("swap_bid={}", fmt_decimal(meta.swap_bid0)),
            format!("swap_ask={}", fmt_decimal(meta.swap_ask0)),
            "-".to_string(),
            String::new(),
        ]);

        if meta.funding_ma.is_some()
            || meta.predicted_funding_rate.is_some()
            || meta.loan_rate.is_some()
        {
            let format_opt = |v: Option<f64>| match v {
                Some(val) => Self::format_decimal(val),
                None => "-".to_string(),
            };
            rows.push(vec![
                "Metrics".to_string(),
                "-".to_string(),
                "-".to_string(),
                format!("fund_ma={}", format_opt(meta.funding_ma)),
                format!("pred={}", format_opt(meta.predicted_funding_rate)),
                format!("loan={}", format_opt(meta.loan_rate)),
                String::new(),
            ]);
        }

        let table = Self::render_three_line_table(
            &["Source", "Symbol", "Side", "Qty", "Price", "Type", "Extra"],
            &rows,
        );

        info!(
            "{}: strategy_id={} 开仓信号摘要\n{}",
            Self::strategy_name(),
            self.strategy_id,
            table
        );
    }

    fn log_lifecycle_summary(&self, stage: &str) {
        if self.lifecycle_logged.get() {
            return;
        }
        self.lifecycle_logged.set(true);

        let mgr_ro = self.order_manager.borrow();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut record_entries: Vec<(&str, i64)> = Vec::new();
        if let Some(margin_id) = if self.margin_order_id != 0 {
            Some(self.margin_order_id)
        } else if self.initial_margin_order_id != 0 {
            Some(self.initial_margin_order_id)
        } else {
            None
        } {
            record_entries.push(("MarginOpen", margin_id));
        }
        for id in &self.um_hedge_order_ids {
            record_entries.push(("UMHedge", *id));
        }

        for (kind, id) in record_entries {
            if let Some(o) = mgr_ro.get(id) {
                let exchange_id = o
                    .exchange_order_id
                    .map(|val| val.to_string())
                    .unwrap_or_else(|| "-".to_string());
                let create_ts = if o.create_time > 0 {
                    o.create_time.to_string()
                } else {
                    "-".to_string()
                };
                let updates_str = if o.update_event_times.is_empty() {
                    "-".to_string()
                } else {
                    format!("{:?}", o.update_event_times)
                };
                rows.push(vec![
                    id.to_string(),
                    exchange_id,
                    kind.to_string(),
                    o.side.as_str().to_string(),
                    Self::format_decimal(o.quantity),
                    Self::format_decimal(o.price),
                    o.status.as_str().to_string(),
                    create_ts,
                    updates_str,
                ]);
            } else {
                rows.push(vec![
                    id.to_string(),
                    "-".to_string(),
                    kind.to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "Removed".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ]);
            }
        }
        drop(mgr_ro);

        if rows.is_empty() {
            return;
        }

        let table = Self::render_three_line_table(
            &[
                "OrderId",
                "ExchangeId",
                "Kind",
                "Side",
                "Qty",
                "Price",
                "Status",
                "CreateTs",
                "UpdateTs",
            ],
            &rows,
        );

        let stage_prefix = if stage.is_empty() {
            String::new()
        } else {
            format!("{} ", stage)
        };

        info!(
            "{}: strategy_id={} {}订单生命周期汇总\n{}",
            Self::strategy_name(),
            self.strategy_id,
            stage_prefix,
            table
        );
    }

    fn cleanup_strategy_orders(&mut self) {
        let mut mgr = self.order_manager.borrow_mut();
        if self.margin_order_id != 0 {
            let _ = mgr.remove(self.margin_order_id);
        }
        for id in &self.um_hedge_order_ids {
            let _ = mgr.remove(*id);
        }
    }
}

fn format_decimal(value: f64) -> String {
    let mut s = format!("{:.8}", value);
    if let Some(dot_pos) = s.find('.') {
        while s.len() > dot_pos + 1 && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

fn align_price_floor(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return value;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((value * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((value / tick) - 1e-9).ceil();
    scaled * tick
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

impl Strategy for BinSingleForwardArbStrategyMT {
    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn symbol(&self) -> Option<&str> { 
        Some(&self.symbol) 
    } 

    fn is_strategy_order(&self, order_id: i64) -> bool {
        ((order_id >> 32) as i32) == self.strategy_id
    } 

    fn handle_trade_signal(&mut self, signal_raws: &Bytes) {
        match TradeSignal::from_bytes(signal_raws) {
            Ok(signal) => self.handle_signal_open(signal),
            Err(err) => warn!(
                "failed to parse trade signal for strategy_id={}: {}",
                self.strategy_id, err
            ),
        }
    } 

    fn handle_trade_response(&mut self, outcome: &TradeExecOutcome) {
        if self.is_strategy_order(outcome.client_order_id) {
            //判断是以下哪个成交回报
            match outcome.req_type {
                TradeRequestType::BinanceNewMarginOrder | TradeRequestType::BinanceNewUMOrder => {
                    debug!(
                        "{}: strategy_id={} trade_response req_type={:?} client_order_id={} status={} body_len={}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type,
                        outcome.client_order_id,
                        outcome.status,
                        outcome.body.len()
                    );

                    let success = (200..300).contains(&outcome.status);
                    let now = get_timestamp_us();
                    let mut final_status: Option<OrderExecutionStatus> = None;

                    let mut manager = self.order_manager.borrow_mut();
                    if !manager.update(outcome.client_order_id, |order| {
                        if success {
                            order.update_status(OrderExecutionStatus::Create);
                            order.set_ack_time(now);
                        } else {
                            order.update_status(OrderExecutionStatus::Rejected);
                            order.set_end_time(now);
                        }
                        final_status = Some(order.status);
                    }) {
                        warn!(
                            "{}: strategy_id={} 未找到 client_order_id={} 对应订单",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id
                        );
                    } else if outcome.req_type == TradeRequestType::BinanceNewMarginOrder {
                        if let Some(status) = final_status {
                            if outcome.client_order_id == self.margin_order_id {
                                // 开仓回执
                                if status.is_terminal() && status != OrderExecutionStatus::Filled {
                                    self.margin_order_id = 0;
                                    self.open_timeout_us = None;
                                    self.clear_cancel_wait();
                                    warn!(
                                        "{}: strategy_id={} margin 开仓回执终止 status={} body={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        status.as_str(),
                                        outcome.body
                                    );
                                }
                                // 平仓回执
                                if status.is_terminal() && status != OrderExecutionStatus::Filled {
                                    warn!(
                                        "{}: strategy_id={} margin 平仓回执终止 status={} body={}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        status.as_str(),
                                        outcome.body
                                    );
                                }
                            } else {
                                debug!(
                                    "{}: strategy_id={} 收到未知 margin 订单回执 client_order_id={} status={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    outcome.client_order_id,
                                    outcome.status
                                );
                            }
                        }
                    }

                    if !success {
                        warn!(
                            "{}: strategy_id={} 下单失败 req_type={:?} status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.req_type,
                            outcome.status,
                            outcome.body
                        );
                    }
                }
                TradeRequestType::BinanceCancelMarginOrder => {
                    let success = (200..300).contains(&outcome.status);
                    if success {
                        info!(
                            "{}: strategy_id={} margin 撤单成功 client_order_id={} status={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.client_order_id,
                            outcome.status
                        );
                    } else {
                        {
                            let mut manager = self.order_manager.borrow_mut();
                            if !manager.update(outcome.client_order_id, |order| {
                                order.cancel_requested = false;
                            }) {
                                debug!(
                                    "{}: strategy_id={} margin 撤单失败且未找到订单 id={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    outcome.client_order_id
                                );
                            }
                        }

                        warn!(
                            "{}: strategy_id={} margin 撤单失败 status={} body={}",
                            Self::strategy_name(),
                            self.strategy_id,
                            outcome.status,
                            outcome.body
                        );
                    }
                }
                _ => {
                    debug!(
                        "{}: strategy_id={} 忽略响应 req_type={:?}",
                        Self::strategy_name(),
                        self.strategy_id,
                        outcome.req_type
                    );
                }
            }
        }
    }

    fn handle_binance_margin_order_update(&mut self, report: &ExecutionReportMsg) {
        let order_id = report.client_order_id;
        if !self.is_strategy_order(order_id) {
            return;
        }

        let status_str = report.order_status.as_str();
        let execution_type = report.execution_type.as_str();

        let mut remove_after_update = false;
        {
            let mut manager = self.order_manager.borrow_mut();
            if !manager.update(order_id, |order| {
                order.set_exchange_order_id(report.order_id);
                match status_str {
                    "NEW" => {
                        order.update_status(OrderExecutionStatus::Create);
                        order.record_exchange_create(report.event_time);
                    } 
                    "PARTIALLY_FILLED" => {
                        order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                        order.record_exchange_update(report.event_time);
                    } 
                    "FILLED" => {
                        order.update_status(OrderExecutionStatus::Filled);
                        order.set_filled_time(report.event_time);
                        order.update_cumulative_filled_quantity(report.cumulative_filled_quantity);
                        order.record_exchange_update(report.event_time);
                        // 成功路径不立即移除，等待配对订单也 FILLED 后成对移除
                    }
                    "CANCELED" | "EXPIRED" => {
                        order.update_status(OrderExecutionStatus::Cancelled);
                        order.set_end_time(report.event_time);
                        order.record_exchange_update(report.event_time);
                        remove_after_update = true;
                    } 
                    "REJECTED" | "TRADE_PREVENTION" => {
                        order.update_status(OrderExecutionStatus::Rejected);
                        order.set_end_time(report.event_time);
                        order.record_exchange_update(report.event_time);
                        remove_after_update = true;
                    }
                    _ => {}
                } 
            }) {
                warn!(
                    "{}: strategy_id={} execution_report 未找到订单 id={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_id
                ); 
            } 
        } 

        if remove_after_update {
            self.order_manager.borrow_mut().remove(order_id);
        } 

        if order_id == self.margin_order_id {
            if let Some(order_snapshot) = {
                let manager = self.order_manager.borrow();
                manager.get(order_id)
            } {
                self.log_open_signal_summary(&order_snapshot);
            }
            if status_str == "FILLED" {
                let hedge_delta = {
                    let manager = self.order_manager.borrow();
                    manager
                        .get(order_id)
                        .map(|order| {
                            (order.cumulative_filled_quantity - order.hedged_filled_quantity)
                                .max(0.0)
                        })
                        .unwrap_or(0.0)
                };
                if hedge_delta > 1e-8 {
                    self.order_manager.borrow_mut().update(order_id, |order| {
                        order.hedged_filled_quantity += hedge_delta;
                    });
                    debug!(
                        "{}: strategy_id={} margin order filled, emitting hedge delta={:.6}",
                        Self::strategy_name(),
                        self.strategy_id,
                        hedge_delta
                    );
                    self.emit_hedge_signal(order_id, hedge_delta);
                }
            }

            debug!(
                "{}: strategy_id={} margin execution report status={} execution_type={}",
                Self::strategy_name(),
                self.strategy_id,
                status_str,
                execution_type
            );

            match status_str {
                "FILLED" => {
                    self.open_timeout_us = None;
                    self.clear_cancel_wait();
                    debug!(
                        "{}: strategy_id={} margin 开仓单 FILLED，保留订单供生命周期管理",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                }
                "CANCELED" | "EXPIRED" | "REJECTED" | "TRADE_PREVENTION" => {
                    self.open_timeout_us = None;
                    self.margin_order_id = 0;
                    self.clear_cancel_wait();
                }
                _ => {}
            }
            return;
        }


        debug!(
            "{}: strategy_id={} margin execution report (其他订单) status={} execution_type={}",
            Self::strategy_name(),
            self.strategy_id,
            status_str,
            execution_type
        );
    }
    // handle_binance_futures_order_update
    // 1、处理 UM 对冲单的挂单 / 成交状态
    // 2、处理 UM 平仓单的挂单 / 成交状态
    fn handle_binance_futures_order_update(&mut self, event: &OrderTradeUpdateMsg) {
        let client_order_id = event.client_order_id;
        if !self.is_strategy_order(client_order_id) {
            return;
        }

        let order_label = self.um_order_label(client_order_id);
        debug!(
            "{}: strategy_id={} 收到 {} UM Order Execution Type={} UM Order Status={}",
            Self::strategy_name(),
            self.strategy_id,
            order_label,
            event.execution_type,
            event.order_status
        );

        if event.business_unit != "UM" {
            error!(
                "{}: strategy_id={} {} business_unit 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.business_unit
            );
        }

        if event.order_type != "MARKET" {
            error!(
                "{}: strategy_id={} {} order_type 异常={}",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                event.order_type
            );
        }

        let mut manager = self.order_manager.borrow_mut();
        let mut outcome = UmOrderUpdateOutcome::Ignored;

        if !manager.update(client_order_id, |order| {
            outcome = Self::apply_um_order_update(order, event);
        }) {
            warn!(
                "{}: strategy_id={} 未找到 {} client_order_id={}，保留激活状态",
                Self::strategy_name(),
                self.strategy_id,
                order_label,
                client_order_id
            );
            return;
        }

        drop(manager);

        match outcome {
            UmOrderUpdateOutcome::Created => {
                debug!(
                    "{}: strategy_id={} {} 已确认，等待成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::PartiallyFilled(cumulative) => {
                info!(
                    "{}: strategy_id={} {} 部分成交累计数量={:.6}，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    cumulative,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Filled => {
                info!(
                    "{}: strategy_id={} {} 已全部成交，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Expired => {
                warn!(
                    "{}: strategy_id={} {} 收到 EXPIRED 状态，请核实，UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.order_status
                );
            }
            UmOrderUpdateOutcome::Ignored => {
                debug!(
                    "{}: strategy_id={} {} 状态无需处理，UM Order Execution Type={} UM Order Status={}",
                    Self::strategy_name(),
                    self.strategy_id,
                    order_label,
                    event.execution_type,
                    event.order_status
                );
            }
        }

        // 成功路径不立即移除，保留订单直到策略生命周期结束
    }

    fn hanle_period_clock(&mut self, current_tp: i64) {
        // 周期性检查：开仓限价/市价单是否长时间未成交，需要撤单
        if self.mode == StrategyMode::Open {
            if self.margin_order_id == 0 {
                let mut flags = self.period_log_flags.borrow_mut();
                if !flags.margin_open_absent_logged {
                    warn!(
                        "{}: strategy_id={} 当前无 margin 开仓单，策略将等待回收",
                        Self::strategy_name(),
                        self.strategy_id
                    );
                    flags.margin_open_absent_logged = true;
                }
                flags.last_margin_open_status = None;
                self.open_timeout_us = None;
            } else {
                {
                    let mut flags = self.period_log_flags.borrow_mut();
                    flags.margin_open_absent_logged = false;
                }
                let open_order = {
                    let manager = self.order_manager.borrow();
                    manager.get(self.margin_order_id)
                };

                match open_order {
                    Some(order) => {
                        {
                            let mut flags = self.period_log_flags.borrow_mut();
                            flags.margin_open_missing_logged = false;
                        }
                        if order.status.is_terminal() {
                            self.open_timeout_us = None;
                        } else if order.cancel_requested {
                            debug!(
                                "{}: strategy_id={} margin 开仓单撤单已在进行中 order_id={}",
                                Self::strategy_name(),
                                self.strategy_id,
                                order.order_id
                            );
                        } else if let (Some(timeout_us), submit_time) =
                            (self.open_timeout_us, order.submit_time)
                        {
                            if submit_time > 0
                                && current_tp.saturating_sub(submit_time) >= timeout_us
                            {
                                info!(
                                    "{}: strategy_id={} margin 开仓单超时，尝试撤单 order_id={}",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    order.order_id
                                );

                                if let Err(err) =
                                    self.submit_margin_cancel(&order.symbol, order.order_id)
                                {
                                    warn!(
                                        "{}: strategy_id={} margin 开仓撤单失败: {}",
                                        Self::strategy_name(),
                                        self.strategy_id,
                                        err
                                    );
                                } else {
                                    let mut manager = self.order_manager.borrow_mut();
                                    if !manager.update(order.order_id, |o| {
                                        o.cancel_requested = true;
                                    }) {
                                        warn!(
                                            "{}: strategy_id={} 撤单后更新订单状态失败 id={}",
                                            Self::strategy_name(),
                                            self.strategy_id,
                                            order.order_id
                                        );
                                    }
                                    self.open_timeout_us = None;
                                }
                            }
                        }
                    }
                    None => {
                        {
                            let mut flags = self.period_log_flags.borrow_mut();
                            if !flags.margin_open_missing_logged {
                                warn!(
                                    "{}: strategy_id={} 未找到 margin 开仓单 id={}，清除本地状态",
                                    Self::strategy_name(),
                                    self.strategy_id,
                                    self.margin_order_id
                                );
                                flags.margin_open_missing_logged = true;
                            }
                            flags.last_margin_open_status = None;
                            flags.margin_open_absent_logged = false;
                        }
                        self.margin_order_id = 0;
                        self.open_timeout_us = None;
                        self.clear_cancel_wait();
                    }
                }
            }
        } else {
            let mut flags = self.period_log_flags.borrow_mut();
            flags.margin_open_absent_logged = false;
            flags.margin_open_missing_logged = false;
            flags.last_margin_open_status = None;
            self.open_timeout_us = None;
        }
    }

    fn is_active(&self) -> bool {
        self.is_active_open()
    }
}
