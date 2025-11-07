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
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::strategy::Strategy;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_request::BinanceCancelMarginOrderRequest;
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use crate::strategy::risk_checker::{self, RiskChecker};
use crate::strategy::risk_checker::PreTradeEnv;
use std::cmp::Ordering;


/// 对冲套利策略，衍生maker-taker和taker-taker，包括跨所对冲
pub struct HedgeArbStrategy {
    pub strategy_id: i32, //策略id
    pub symbol: String, //交易的统一symbol
    pub open_order_id: i64, //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>, //对冲单会产生一个or多个，因为部分成交
    pub open_timeout_us: Option<i64>, //开仓单最长挂单时间，超过撤销
    pub hedge_timeout_us: Option<i64>, //对冲单最长挂单时间，超过撤销，度过是maker-taker模式，则没有这个timeout，设置为None
    pub order_seq: u32, //订单号计数器
    pub pre_trade_env: Rc<PreTradeEnv>, //预处理global变量封装
    pub cumulative_hedged_qty: f64, //累计对冲数量
    pub cumulative_open_qty: f64, //累计开仓数量
    pub alive_flag: bool, //策略是否存活
}

impl HedgeArbStrategy {
    pub fn new(
        id: i32,
        symbol: String,
        env: Rc<PreTradeEnv>,
        open_timeout_us: Option<i64>,
        hedge_timeout_us: Option<i64>,
    ) -> Self {
        let strategy = Self {
            strategy_id: id,
            symbol,
            open_order_id: 0,
            hedge_order_ids: Vec::new(),
            open_timeout_us,
            hedge_timeout_us,
            order_seq: 0,
            pre_trade_env: env.clone(),
            cumulative_hedged_qty: 0.0,
            cumulative_open_qty: 0.0,
            alive_flag: true,
        };
        strategy
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号
    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    /// 从订单ID中提取策略ID
    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    /// 从订单ID中提取序列号
    fn extract_order_seq(order_id: i64) -> u32 {
        (order_id & 0xFFFFFFFF) as u32
    }
    fn handle_arb_open_signal(&mut self, ctx: ArbOpenCtx) {
        // 开仓open leg，打开头寸，根据信号创建开仓leg, 进行风控判断，失败就直接把策略标记为不活跃，等待定时器清理

        // 1、检查杠杆, 失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_leverage() {
            error!(
                "HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃",
                self.strategy_id,
                e
            );
            self.alive_flag = false;
            return;
        }

        // 2、检查symbol的敞口，失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_symbol_exposure(&self.symbol) {
            error!(
                "HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id,
                self.symbol,
                e
            );
            self.alive_flag = false;
            return;
        }

        // 3、检查总敞口，失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_total_exposure() {
            error!(
                "HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id,
                e
            );
            self.alive_flag = false;
            return;
        }

        // 4、检查限价挂单数量限制（如果是限价单）
        let order_type = OrderType::from_u8(ctx.order_type);
        if order_type == Some(OrderType::Limit) {
            if let Err(e) = self.pre_trade_env.risk_checker.check_pending_limit_order(&self.symbol) {
                error!(
                    "HedgeArbStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃",
                    self.strategy_id,
                    self.symbol,
                    e
                );
                self.alive_flag = false;
                return;
            }
        }

        // 5、通过全部风控检查，生成订单ID
        self.order_seq += 1;
        let order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
        self.open_order_id = order_id;

        // 6、根据资产类型创建开仓订单
        self.open_order_id = order_id;
        self.cumulative_open_qty = 0.0;
        self.alive_flag = true;
        let ts = get_timestamp_us();
        self.open_timeout_us = Some(ctx.exp_time + ts);

        // 7、根据交易标的物，修正量、价格
        let symbol = ctx.get_opening_symbol();
        let raw_qty = ctx.amount as f64;
        let raw_price = ctx.price;

        let venue = TradingVenue::from_u8(ctx.opening_leg.venue)
            .ok_or_else(|| format!("无效的交易场所: {}", ctx.opening_leg.venue))
            .unwrap();

        // 检查 venue 必须是 BinanceUm 或 BinanceMargin
        match venue {
            TradingVenue::BinanceUm | TradingVenue::BinanceMargin => {
                // 允许的交易场所，继续执行
            }
            _ => {
                panic!(
                    "HedgeArbStrategy: strategy_id={} 不支持的交易场所 {:?}，仅支持 BinanceUm 或 BinanceMargin",
                    self.strategy_id,
                    venue
                );
            }
        }

        let (aligned_qty, aligned_price) = match TradingVenue::align_um_order(
            &symbol,
            raw_qty,
            raw_price,
            &self.pre_trade_env.min_qty_table,
        ) {
            Ok((qty, price)) => (qty, price),
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 订单对齐失败: {}，原始量={:.8} 原始价格={:.8}，标记策略为不活跃",
                    self.strategy_id,
                    e,
                    raw_qty,
                    raw_price
                );
                self.alive_flag = false;
                return;
            }
        };

        // 9、考虑修正量，判断下单后是否会大于max u
        if let Err(e) = self.pre_trade_env.risk_checker.ensure_max_pos_u(
            &symbol,
            aligned_qty,
            aligned_price,
        ) {
            error!(
                "HedgeArbStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_id,
                e
            );
            self.alive_flag = false;
            return;
        }

        // 10、用修正量价，开仓订单记录到order manager
        // todo: 订单持久化直接写死在manager中
        let client_order_id = self.pre_trade_env.risk_checker.order_manager.borrow_mut().create_order(
            venue,
            order_id,
            OrderType::from_u8(ctx.order_type).unwrap(),
            symbol.clone(),
            Side::from_u8(ctx.side).unwrap(),
            aligned_qty,
            aligned_price,
            ts,
        );
        log::info!("created open order with client_order_id: {}", client_order_id);

        // 9、推送开仓订单到交易引擎
        if let Err(e) = self.create_and_send_order(client_order_id, "开仓", &symbol) {
            error!("HedgeArbStrategy: strategy_id={} {}", self.strategy_id, e);
            return;
        }
    }
    // 收到对冲信号，按照需求进行maker对冲，或者直接taker对冲
    fn handle_arb_hedge_signal(&mut self, ctx: ArbHedgeCtx) -> Result<(), String> {
        // 1. 确定对冲数量
        let target_qty = if ctx.hedge_qty > 0.0 {
            ctx.hedge_qty as f64
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} 对冲信号的数量无效: {}",
                self.strategy_id,
                ctx.hedge_qty
            );
            return Err(format!("对冲数量无效: {}", ctx.hedge_qty));
        };

        // 2. 获取对冲交易的 symbol 和 venue
        let hedge_symbol = ctx.get_hedging_symbol();
        let hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
            .ok_or_else(|| format!("无效的对冲交易场所: {}", ctx.hedging_leg.venue))?;

        // 3. 使用预交易环境对齐订单量和价格
        // 使用get_hedge_price 函数获取合适的对冲价格
        let hedge_price = ctx.get_hedge_price();
        let (aligned_qty, aligned_price) = self.pre_trade_env.align_order_by_venue(
            hedge_venue,
            &hedge_symbol,
            target_qty,
            hedge_price,
        )?;

        // 4. 检查最小交易要求
        if let Err(e) = self.pre_trade_env.check_min_trading_requirements(
            hedge_venue,
            &hedge_symbol,
            aligned_qty,
            Some(aligned_price),
        ) {
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲订单不满足最小要求: {}，等待更多成交",
                self.strategy_id,
                e
            );
            return Ok(());
        }

        // 5. 对冲无需风控逻辑，直接构造订单即可
        // 生成对冲订单ID
        self.order_seq += 1;
        let hedge_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);

        // 获取当前时间戳
        let ts = get_timestamp_us();

        // 根据对冲信号确定订单类型，确定是maker对冲，还是taker对冲
        let order_type = if ctx.is_maker() {
            OrderType::Limit  // Maker订单使用限价单
        } else {
            OrderType::Market  // Taker订单使用市价单
        };

        // 获取对冲方向
        let hedge_side = ctx.get_side().ok_or_else(|| {
            format!("无效的对冲方向: {}", ctx.hedge_side)
        })?;

        // 6. 创建对冲订单并记录到order manager
        let hedge_client_order_id = self.pre_trade_env.risk_checker.order_manager.borrow_mut().create_order(
            hedge_venue,
            hedge_order_id,
            order_type,
            hedge_symbol.clone(),
            hedge_side,
            aligned_qty,
            aligned_price,
            ts,
        );

        log::info!(
            "HedgeArbStrategy: created hedge order with client_order_id: {} for strategy_id: {}",
            hedge_client_order_id,
            self.strategy_id
        );

        // 7. 将对冲订单ID添加到策略的对冲订单列表
        self.hedge_order_ids.push(hedge_order_id);
        // 对冲量只有实质成交才会更新，挂单不更新

        // 8. 推送对冲订单到交易引擎
        if let Err(e) = self.create_and_send_order(hedge_client_order_id, "对冲", &hedge_symbol) {
            return Err(e);
        }

        // 9. 设置对冲超时时间(决定了maker的生效方式)
        if ctx.exp_time > 0 {
            self.hedge_timeout_us = Some(ctx.exp_time + ts);
        }

        debug!(
            "HedgeArbStrategy: strategy_id={} 对冲订单创建成功 order_id={} symbol={} side={:?} qty={:.8} price={:.8} type={:?}",
            self.strategy_id,
            hedge_order_id,
            hedge_symbol,
            hedge_side,
            aligned_qty,
            aligned_price,
            order_type
        );

        Ok(())
    }

    /// 创建订单并发送到交易引擎的公共函数
    ///
    /// # Arguments
    /// * `client_order_id` - 客户端订单ID
    /// * `order_type_str` - 订单类型描述（用于日志）
    /// * `symbol` - 交易对符号
    ///
    /// # Returns
    /// * `Ok(())` - 订单成功发送
    /// * `Err(String)` - 发送失败的错误信息
    fn create_and_send_order(
        &mut self,
        client_order_id: i64,
        order_type_str: &str,
        symbol: &str,
    ) -> Result<(), String> {
        let mut order = self.pre_trade_env.risk_checker.order_manager.borrow_mut().get(client_order_id);
        if let Some(order) = order.as_mut() {
            match order.get_order_request_bytes() {
                Ok(req_bin) => {
                    if let Err(e) = self.pre_trade_env.trade_request_tx.send(req_bin) {
                        error!(
                            "HedgeArbStrategy: strategy_id={} symbol={} 推送{}订单失败: {}，标记策略为不活跃",
                            self.strategy_id,
                            symbol,
                            order_type_str,
                            e
                        );
                        self.alive_flag = false;
                        return Err(format!("推送{}订单失败: {}", order_type_str, e));
                    }
                    Ok(())
                }
                Err(e) => {
                    error!(
                        "HedgeArbStrategy: strategy_id={} symbol={} 获取{}订单请求字节失败: {}，标记策略为不活跃",
                        self.strategy_id,
                        symbol,
                        order_type_str,
                        e
                    );
                    self.alive_flag = false;
                    Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
                }
            }
        } else {
            error!(
                "HedgeArbStrategy: strategy_id={} symbol={} 未找到创建的{}订单 client_order_id={}",
                self.strategy_id,
                symbol,
                order_type_str,
                client_order_id
            );
            self.alive_flag = false;
            Err(format!("未找到创建的{}订单", order_type_str))
        }
    }


    // cancel的本质就是构造取消，实际处理的是account monitor的撤销回报
    fn handle_arb_cancel_signal(&mut self, ctx: ArbCancelCtx) -> Result<(), String> {
        // 从 order manager 获取开仓订单
        let order = self.pre_trade_env.risk_checker.order_manager.borrow().get(self.open_order_id);
        if let Some(order) = order {
            // 使用 order 的 get_order_cancel_bytes 方法获取撤单请求
            match order.get_order_cancel_bytes() {
                Ok(cancel_bytes) => {
                    // 发送撤单请求到交易引擎
                    if let Err(e) = self.pre_trade_env.trade_request_tx.send(cancel_bytes) {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 发送撤单请求失败: {}",
                            self.strategy_id, e
                        );
                        return Err(format!("发送撤单请求失败: {}", e));
                    }
                    debug!(
                        "HedgeArbStrategy: strategy_id={} 已发送撤单请求 order_id={}",
                        self.strategy_id, self.open_order_id
                    );
                }
                Err(e) => {
                    error!(
                        "HedgeArbStrategy: strategy_id={} 获取撤单请求字节失败: {}",
                        self.strategy_id, e
                    );
                    return Err(format!("获取撤单请求字节失败: {}", e));
                }
            }
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到要撤销的订单 order_id={}",
                self.strategy_id, self.open_order_id
            );
            return Err(format!("未找到要撤销的订单"));
        }

        Ok(())
    }

    fn handle_signal(&mut self, signal: TradeSignal) {
        match signal.signal_type {
            SignalType::ArbOpen => {
                // 套利开仓信号
                match ArbOpenCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        self.handle_arb_open_signal(ctx);
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析开仓信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            SignalType::ArbCancel => {
                // 套利撤单信号
                match ArbCancelCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_cancel_signal(ctx) {
                            warn!(
                                "HedgeArbStrategy: strategy_id={} 撤单处理失败: {}",
                                self.strategy_id, err
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析撤单信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            SignalType::ArbHedge => {
                // 套利对冲信号
                match ArbHedgeCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_hedge_signal(ctx) {
                            warn!(
                                "HedgeArbStrategy: strategy_id={} 对冲处理失败: {}",
                                self.strategy_id, err
                            );
                        }
                    }
                    Err(err) => {
                        error!(
                            "HedgeArbStrategy: strategy_id={} 解析对冲信号上下文失败: {}",
                            self.strategy_id, err
                        );
                    }
                }
            }
            other => {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 忽略信号类型 {:?}",
                    self.strategy_id, other
                );
            }
        }
    }

    // 处理开仓测，超过最长挂单时间
    fn handle_open_leg_timeout(&mut self) {
        // 检查是否设置了超时时间，并且已经超时
        if let Some(timeout_us) = self.open_timeout_us {
            let now = get_timestamp_us();
            if now >= timeout_us && self.alive_flag && self.open_order_id != 0 {
                info!(
                    "HedgeArbStrategy: strategy_id={} 开仓订单超时，直接撤单 order_id={}",
                    self.strategy_id, self.open_order_id
                );

                // 获取开仓订单并直接发送撤单请求
                let order = self.pre_trade_env.risk_checker.order_manager.borrow().get(self.open_order_id);
                if let Some(order) = order {
                    match order.get_order_cancel_bytes() {
                        Ok(cancel_bytes) => {
                            if let Err(e) = self.pre_trade_env.trade_request_tx.send(cancel_bytes) {
                                error!(
                                    "HedgeArbStrategy: strategy_id={} 发送开仓撤单请求失败: {}",
                                    self.strategy_id, e
                                );
                            } else {
                                debug!(
                                    "HedgeArbStrategy: strategy_id={} 已发送开仓撤单请求 order_id={}",
                                    self.strategy_id, self.open_order_id
                                );
                                // 清除超时时间，避免重复发送撤单
                                self.open_timeout_us = None;
                            }
                        }
                        Err(e) => {
                            error!(
                                "HedgeArbStrategy: strategy_id={} 获取开仓撤单请求字节失败: {}",
                                self.strategy_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    fn handle_hedge_leg_timeout(&mut self) {
        // 检查是否设置了对冲超时时间
        if let Some(timeout_us) = self.hedge_timeout_us {
            let now = get_timestamp_us();
            if now >= timeout_us {
                info!(
                    "HedgeArbStrategy: strategy_id={} 对冲订单超时，直接撤单",
                    self.strategy_id
                );

                // 遍历所有对冲订单，直接撤单
                for &hedge_order_id in &self.hedge_order_ids.clone() {
                    let order = self.pre_trade_env.risk_checker.order_manager.borrow().get(hedge_order_id);
                    if let Some(order) = order {
                        match order.get_order_cancel_bytes() {
                            Ok(cancel_bytes) => {
                                if let Err(e) = self.pre_trade_env.trade_request_tx.send(cancel_bytes) {
                                    error!(
                                        "HedgeArbStrategy: strategy_id={} 发送对冲撤单请求失败 order_id={}: {}",
                                        self.strategy_id, hedge_order_id, e
                                    );
                                } else {
                                    debug!(
                                        "HedgeArbStrategy: strategy_id={} 已发送对冲撤单请求 order_id={}",
                                        self.strategy_id, hedge_order_id
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    "HedgeArbStrategy: strategy_id={} 获取对冲撤单请求字节失败 order_id={}: {}",
                                    self.strategy_id, hedge_order_id, e
                                );
                            }
                        }
                    }
                }

                // 清除对冲超时时间，避免重复发送撤单
                self.hedge_timeout_us = None;
            }
        }
    }

    fn is_active_open(&self) -> bool {
        self.alive_flag
    }
    // 检查当前 symbol 的限价挂单是否超过阈值

    fn extract_base_asset(symbol_upper: &str) -> Option<String> {
        const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
        for quote in QUOTES {
            if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
                return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
            }
        }
        None
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

impl Drop for HedgeArbStrategy {
    fn drop(&mut self) {
        self.log_lifecycle_summary("生命周期结束");
        self.cleanup_strategy_orders();
        debug!("trategy_id={} 生命周期结束，相关订单已回收", self.strategy_id);
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
