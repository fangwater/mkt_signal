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
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::strategy::Strategy;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::trade_engine::trade_request::BinanceCancelMarginOrderRequest;
use crate::trade_engine::trade_response_handle::TradeExecOutcome;
use crate::strategy::risk_checker::{self, RiskChecker};
use crate::strategy::risk_checker::PreTradeEnv;
use std::cmp::Ordering;

pub struct HedgeArbStrategy {
    pub strategy_id: i32, //策略id
    pub symbol: String, //交易的统一symbol (开仓侧symbol)
    pub open_order_id: i64, //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>, //对冲单会产生一个or多个，因为部分成交
    pub open_timeout_us: Option<i64>, //开仓单最长挂单时间，超过撤销
    pub hedge_timeout_us: Option<i64>, //对冲单最长挂单时间，超过撤销，度过是maker-taker模式，则没有这个timeout，设置为None
    pub order_seq: u32, //订单号计数器
    pub pre_trade_env: Rc<PreTradeEnv>, //预处理global变量封装
    pub cumulative_hedged_qty: f64, //累计对冲数量
    pub cumulative_open_qty: f64, //累计开仓数量
    pub alive_flag: bool, //策略是否存活
    pub hedge_symbol: String, //对冲侧symbol
    pub hedge_venue: TradingVenue, //对冲侧交易场所
    pub hedge_side: Side, //对冲侧方向
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
            hedge_symbol: String::new(),
            hedge_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            hedge_side: Side::Buy, // 默认值，将在开仓时更新
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
            error!("HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃", self.strategy_id, e);
            self.alive_flag = false;
            return;
        }

        // 2、检查symbol的敞口，失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_symbol_exposure(&self.symbol) {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.symbol, e);
            self.alive_flag = false;
            return;
        }

        // 3、检查总敞口，失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_total_exposure() {
            error!("HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃", self.strategy_id, e);
            self.alive_flag = false;
            return;
        }

        // 4、检查限价挂单数量限制（如果是限价单）
        let order_type = OrderType::from_u8(ctx.order_type);
        if order_type == Some(OrderType::Limit) {
            if let Err(e) = self.pre_trade_env.risk_checker.check_pending_limit_order(&self.symbol) {
                error!("HedgeArbStrategy: strategy_id={} symbol={} 限价挂单数量风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.symbol, e);
                self.alive_flag = false;
                return;
            }
        }

        // 5、通过全部风控检查，生成订单ID
        self.order_seq += 1;
        let order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
        self.open_order_id = order_id;

        // 6、根据资产类型创建开仓订单，并保存对冲侧信息
        self.open_order_id = order_id;
        self.cumulative_open_qty = 0.0;
        self.alive_flag = true;
        let ts = get_timestamp_us();
        self.open_timeout_us = Some(ctx.exp_time + ts);

        // 保存对冲侧信息
        self.hedge_symbol = ctx.get_hedging_symbol();
        self.hedge_venue = TradingVenue::from_u8(ctx.hedging_leg.venue)
            .ok_or_else(|| format!("无效的对冲交易场所: {}", ctx.hedging_leg.venue))
            .unwrap();
        // 对冲侧方向与开仓侧相反
        let open_side = Side::from_u8(ctx.side).unwrap();
        self.hedge_side = if open_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };

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
                        error!("HedgeArbStrategy: strategy_id={} 发送撤单请求失败: {}", self.strategy_id, e);
                        return Err(format!("发送撤单请求失败: {}", e));
                    }
                }
                Err(e) => {
                    error!("HedgeArbStrategy: strategy_id={} 获取撤单请求字节失败: {}", self.strategy_id, e);
                    return Err(format!("获取撤单请求字节失败: {}", e));
                }
            }
        } else {
            warn!("HedgeArbStrategy: strategy_id={} 未找到要撤销的订单 order_id={}",self.strategy_id, self.open_order_id);
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
                        error!("HedgeArbStrategy: strategy_id={} 解析开仓信号上下文失败: {}", self.strategy_id, err);
                    }
                }
            }
            SignalType::ArbCancel => {
                // 套利撤单信号
                match ArbCancelCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_cancel_signal(ctx) {
                            warn!("HedgeArbStrategy: strategy_id={} 撤单处理失败: {}", self.strategy_id, err); 
                        }
                    }
                    Err(err) => {
                        error!("HedgeArbStrategy: strategy_id={} 解析撤单信号上下文失败: {}", self.strategy_id, err);
                    }
                }
            }
            SignalType::ArbHedge => {
                // 套利对冲信号
                match ArbHedgeCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => {
                        if let Err(err) = self.handle_arb_hedge_signal(ctx) {
                            warn!("HedgeArbStrategy: strategy_id={} 对冲处理失败: {}", self.strategy_id, err);
                        }
                    }
                    Err(err) => {
                        error!("HedgeArbStrategy: strategy_id={} 解析对冲信号上下文失败: {}", self.strategy_id, err);
                    }
                }
            }
            other => {
                debug!("HedgeArbStrategy: strategy_id={} 忽略信号类型 {:?}", self.strategy_id, other);
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

    // 检查当前的对冲单的id列表的最后一个。查看是否是terminal状态
    fn has_pending_hedge_order(&self) -> bool {
        // 获取最后一个对冲订单ID
        if let Some(&last_hedge_id) = self.hedge_order_ids.last() {
            // 从order manager获取订单
            let order = self.pre_trade_env.risk_checker.order_manager.borrow().get(last_hedge_id);
            if let Some(order) = order {
                // 检查是否不是终结状态（即是pending状态）
                return !order.status.is_terminal();
            }
        }
        // 如果没有对冲订单或找不到订单，返回false
        false
    }


    /// 以市价单对冲
    ///
    /// # Arguments
    /// * `venue` - 对冲交易场所
    /// * `side` - 对冲方向
    /// * `eff_qty` - 有效对冲量（累计成交量 - 累计对冲量）
    fn hedge_as_market_order(&mut self, venue: TradingVenue, side: Side, eff_qty: f64) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id,
                eff_qty
            );
            return Ok(());
        }

        // 1. 创建对冲上下文（市价单模式，exp_time = 0）
        let mut hedge_ctx = ArbHedgeCtx::new_taker(
            self.strategy_id,
            self.open_order_id,
            eff_qty,
            side.to_u8(),
        );

        // 2. 设置对冲symbol
        hedge_ctx.set_hedging_symbol(&self.symbol);

        // 3. 设置对冲leg（市价单不需要盘口价格，设置为0）
        hedge_ctx.hedging_leg = crate::signal::common::TradingLeg {
            venue: venue.to_u8(),
            bid0: 0.0,  // 市价单不需要盘口价格
            ask0: 0.0,  // 市价单不需要盘口价格
        };

        // 4. 设置市场数据时间戳
        hedge_ctx.market_ts = get_timestamp_us();

        // 5. 创建交易信号
        let signal = TradeSignal::create(
            SignalType::ArbHedge,
            get_timestamp_us(),
            0.0,
            hedge_ctx.to_bytes(),
        );

        // 6. 发送信号到signal_tx
        if let Some(ref signal_tx) = self.pre_trade_env.signal_tx {
            if let Err(e) = signal_tx.send(signal.to_bytes()) {
                error!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲信号失败: {}",
                    self.strategy_id, e
                );
                return Err(format!("发送对冲信号失败: {}", e));
            }

            debug!(
                "HedgeArbStrategy: strategy_id={} 发送对冲信号成功 venue={:?} side={:?} qty={:.8}",
                self.strategy_id,
                venue,
                side,
                eff_qty
            );
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} signal_tx 未配置，无法发送对冲信号",
                self.strategy_id
            );
            return Err("signal_tx 未配置".to_string());
        }

        Ok(())
    }

    /// 以限价单对冲（需要先向上游模型查询挂单价格）
    ///
    /// # Arguments
    /// * `venue` - 对冲交易场所
    /// * `side` - 对冲方向
    /// * `eff_qty` - 有效对冲量（累计成交量 - 累计对冲量）
    fn hedge_as_limit_order(&mut self, venue: TradingVenue, side: Side, eff_qty: f64) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 限价单对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id,
                eff_qty
            );
            return Ok(());
        }

        // 1. 创建对冲查询消息
        let query_msg = crate::signal::hedge_signal::ArbHedgeSignalQueryMsg::new(
            self.strategy_id,
            self.open_order_id,
            get_timestamp_us(),
            eff_qty,
            side.to_u8(),
            venue.to_u8(),
        );

        // 2. 发送查询到 signal_query_tx
        if let Some(ref signal_query_tx) = self.pre_trade_env.signal_query_tx {
            if let Err(e) = signal_query_tx.send(query_msg.to_bytes()) {
                error!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲查询失败: {}",
                    self.strategy_id, e
                );
                return Err(format!("发送对冲查询失败: {}", e));
            }

            debug!(
                "HedgeArbStrategy: strategy_id={} 发送对冲查询成功 venue={:?} side={:?} qty={:.8}",
                self.strategy_id,
                venue,
                side,
                eff_qty
            );
        } else {
            warn!(
                "HedgeArbStrategy: strategy_id={} signal_query_tx 未配置，无法发送对冲查询",
                self.strategy_id
            );
            return Err("signal_query_tx 未配置".to_string());
        }

        Ok(())
    }

    /// 尝试对冲（含残值处理）
    ///
    /// # Arguments
    /// * `base_qty` - 基础待对冲量（不含残值）
    ///
    /// # Returns
    /// 是否成功发起对冲（true=已发起对冲，false=不满足要求已累加到残值）
    fn try_hedge_with_residual(&mut self, base_qty: f64) -> bool {
        // 1. 计算总待对冲量（基础量 + 残值表中的残余量）
        let mut total_pending_qty = base_qty;
        let residual_qty = self.pre_trade_env.clear_hedge_residual(&self.hedge_symbol, self.hedge_venue);
        total_pending_qty += residual_qty;

        info!(
            "HedgeArbStrategy: strategy_id={} 待对冲量: 基础={:.8} 残余={:.8} 总计={:.8}",
            self.strategy_id, base_qty, residual_qty, total_pending_qty
        );

        // 2. 检查是否满足最小交易要求
        let can_hedge = self.pre_trade_env.check_min_trading_requirements(
            self.hedge_venue,
            &self.hedge_symbol,
            total_pending_qty,
            None,
        ).is_ok();

        if !can_hedge {
            // 不满足最小交易要求，累加到残值表
            if total_pending_qty > 1e-12 {
                self.pre_trade_env.inc_hedge_residual(
                    self.hedge_symbol.clone(),
                    self.hedge_venue,
                    total_pending_qty
                );
                info!(
                    "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 不满足最小交易要求，累加到残余量表",
                    self.strategy_id, total_pending_qty
                );
            }
            return false;
        }

        // 3. 满足要求，发起对冲
        info!(
            "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 满足最小交易要求，准备对冲",
            self.strategy_id, total_pending_qty
        );

        let hedge_result = if self.hedge_timeout_us.is_some() {
            // MM 模式：使用限价单对冲
            info!(
                "HedgeArbStrategy: strategy_id={} MM模式，使用限价单对冲",
                self.strategy_id
            );
            self.hedge_as_limit_order(self.hedge_venue, self.hedge_side, total_pending_qty)
        } else {
            // MT 模式：使用市价单对冲
            info!(
                "HedgeArbStrategy: strategy_id={} MT模式，使用市价单对冲",
                self.strategy_id
            );
            self.hedge_as_market_order(self.hedge_venue, self.hedge_side, total_pending_qty)
        };

        match hedge_result {
            Ok(_) => {
                // 对冲成功
                info!(
                    "HedgeArbStrategy: strategy_id={} 对冲信号已发送，数量={:.8}",
                    self.strategy_id, total_pending_qty
                );
                true
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 对冲失败: {}",
                    self.strategy_id, e
                );
                false
            }
        }
    }

    fn process_open_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        // 1. 确认 open-order 的订单状态，修改为 cancel。更新交易所 end 时间和本地 end 时间等数据
        let order_id = cancel_update.client_order_id();
        let event_time = cancel_update.event_time();

        let mut order_manager = self.pre_trade_env.risk_checker.order_manager.borrow_mut();
        let updated = order_manager.update(order_id, |order| {
            order.status = OrderExecutionStatus::Cancelled;
            order.timestamp.end_t = event_time;
        });

        if !updated {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到开仓订单 order_id={}",
                self.strategy_id, order_id
            );
            return;
        }

        // 获取订单的累计成交量
        let order = order_manager.get(order_id);
        drop(order_manager); // 释放借用

        let cumulative_filled_qty = match order {
            Some(o) => o.cumulative_filled_quantity,
            None => {
                warn!(
                    "HedgeArbStrategy: strategy_id={} 订单已被删除 order_id={}",
                    self.strategy_id, order_id
                );
                return;
            }
        };

        info!(
            "HedgeArbStrategy: strategy_id={} 开仓订单撤单 order_id={} 累计成交量={:.8} 已对冲量={:.8}",
            self.strategy_id, order_id, cumulative_filled_qty, self.cumulative_hedged_qty
        );

        // 2. 计算待对冲量 = 已经成交的量 - 已经对冲的量
        let base_pending_qty = cumulative_filled_qty - self.cumulative_hedged_qty;

        info!(
            "HedgeArbStrategy: strategy_id={} 开仓撤单: 累计成交={:.8} 已对冲={:.8} 基础待对冲={:.8}",
            self.strategy_id, cumulative_filled_qty, self.cumulative_hedged_qty, base_pending_qty
        );

        // 3. 尝试对冲（含残值处理）
        let can_hedge = self.try_hedge_with_residual(base_pending_qty);

        // 4. 如果无法对冲，根据模式决定是否关闭策略
        if !can_hedge {
            if self.hedge_timeout_us.is_some() {
                // MM 模式：直接关闭策略
                info!(
                    "HedgeArbStrategy: strategy_id={} MM模式，待对冲量不足，关闭策略",
                    self.strategy_id
                );
                self.alive_flag = false;
            } else {
                // MT 模式：检查是否有待处理的对冲订单
                let has_pending_hedge = self.has_pending_hedge_order();
                if !has_pending_hedge {
                    info!(
                        "HedgeArbStrategy: strategy_id={} MT模式，无待处理对冲订单，关闭策略",
                        self.strategy_id
                    );
                    self.alive_flag = false;
                } else {
                    info!(
                        "HedgeArbStrategy: strategy_id={} MT模式，有待处理对冲订单，保持策略活跃",
                        self.strategy_id
                    );
                }
            }
        }
    }

    fn process_hedge_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        // 只有 MM 模式才会出现对冲侧的 cancel（因为 MT 模式是市价单，立即成交）
        if self.hedge_timeout_us.is_none() {
            error!("HedgeArbStrategy: strategy_id={} MT模式不应该有对冲侧撤单，订单ID={}", self.strategy_id, cancel_update.client_order_id());
            return;
        }

        // 1. 更新订单状态为 Cancelled
        let order_id = cancel_update.client_order_id();
        let event_time = cancel_update.event_time();

        let mut order_manager = self.pre_trade_env.risk_checker.order_manager.borrow_mut();
        let updated = order_manager.update(order_id, |order| {
            order.status = OrderExecutionStatus::Cancelled;
            order.set_end_time(event_time);
            order.cumulative_filled_quantity = cancel_update.cumulative_filled_quantity();
        });

        if !updated {
            warn!("HedgeArbStrategy: strategy_id={} 未找到对冲订单 order_id={}", self.strategy_id, order_id);
            return;
        }

        // 3. 计算待对冲量 = 订单量 - 累计成交量（这个订单的未成交部分）
        let base_pending_qty = quantity - cumulative_filled_qty;

        info!(
            "HedgeArbStrategy: strategy_id={} 对冲撤单: 订单量={:.8} 累计成交={:.8} 基础待对冲={:.8}",
            self.strategy_id, quantity, cumulative_filled_qty, base_pending_qty
        );

        // 4. 尝试对冲（含残值处理）
        let can_hedge = self.try_hedge_with_residual(base_pending_qty);

        // 5. MM模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
        if !can_hedge && !self.has_pending_hedge_order() {
            info!(
                "HedgeArbStrategy: strategy_id={} MM模式，对冲订单撤单后无法继续对冲，关闭策略",
                self.strategy_id
            );
            self.alive_flag = false;
        }
    }

    fn process_open_leg_trade(&mut self, trade: &dyn TradeUpdate) {
        // 开仓成交后，触发对冲逻辑

        // 1. MM 模式：只有完全成交才对冲，部分成交等撤单时再对冲
        if self.hedge_timeout_us.is_some() {
            // 检查订单是否完全成交
            let is_filled = self.pre_trade_env.risk_checker.order_manager.borrow()
                .get(self.open_order_id)
                .map(|order| order.status == OrderExecutionStatus::Filled)
                .unwrap_or_else(|| {
                    warn!(
                        "HedgeArbStrategy: strategy_id={} 未找到开仓订单 order_id={}",
                        self.strategy_id, self.open_order_id
                    );
                    false
                });

            if !is_filled {
                debug!(
                    "HedgeArbStrategy: strategy_id={} MM模式，开仓订单部分成交，等待完全成交或撤单",
                    self.strategy_id
                );
                return;
            }
        }

        // 2. 计算待对冲量 = 已成交量 - 已对冲量
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

        info!(
            "HedgeArbStrategy: strategy_id={} 开仓成交: 累计成交={:.8} 已对冲={:.8} 基础待对冲={:.8}",
            self.strategy_id, self.cumulative_open_qty, self.cumulative_hedged_qty, base_pending_qty
        );

        // 3. 尝试对冲（含残值处理）
        let _can_hedge = self.try_hedge_with_residual(base_pending_qty);

        // 注意：MT 模式下成交后立即对冲，不关闭策略
        // 如果对冲失败，残值会累加，等待后续成交或撤单时再处理
    }
    fn process_hedge_leg_trade(&mut self, _trade: &dyn TradeUpdate) {
        // 对冲侧成交处理

        info!(
            "HedgeArbStrategy: strategy_id={} 对冲成交: 开仓量={:.8} 对冲量={:.8}",
            self.strategy_id, self.cumulative_open_qty, self.cumulative_hedged_qty
        );

        // MT 模式：检查是否还有未对冲的量，如果有且无法对冲则关闭策略
        if self.hedge_timeout_us.is_none() {
            // 计算剩余未对冲量
            let remaining_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

            // 获取残值
            let residual_qty = self.pre_trade_env.get_hedge_residual(&self.hedge_symbol, self.hedge_venue);
            let total_remaining = remaining_qty + residual_qty;

            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式对冲成交后检查: 剩余={:.8} 残值={:.8} 总计={:.8}",
                self.strategy_id, remaining_qty, residual_qty, total_remaining
            );

            // 检查剩余量是否满足最小交易要求
            if total_remaining > 1e-12 {
                let can_hedge = self.pre_trade_env.check_min_trading_requirements(
                    self.hedge_venue,
                    &self.hedge_symbol,
                    total_remaining,
                    None,
                ).is_ok();

                if !can_hedge {
                    // 剩余量不足以对冲，且没有待处理的对冲订单，关闭策略
                    if !self.has_pending_hedge_order() {
                        info!(
                            "HedgeArbStrategy: strategy_id={} MT模式，剩余量={:.8} 不足以对冲，关闭策略",
                            self.strategy_id, total_remaining
                        );
                        self.alive_flag = false;
                    }
                }
            }
        }

        // MM 模式：对冲侧成交不需要特殊处理，等待完全成交或撤单
    }

    // 处理交易更新，需要区分是开仓侧成交，还是对冲侧成交
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate){
        //1 根据client order id，判断是开仓成交，还是对冲成交, 更新开仓量或对冲量
        let order_id = trade.client_order_id();
        if order_id == self.open_order_id {
            // 开仓成交，更新累计开仓量, 打印成交量
            self.cumulative_open_qty = trade.cumulative_filled_quantity();
            debug!(
                "HedgeArbStrategy: strategy_id={} 开仓订单成交 order_id={} 成交量={:.8} 开仓量/已对冲量={:.8}/{:.8}",
                self.strategy_id,
                order_id,
                trade.quantity(),
                self.cumulative_open_qty, self.cumulative_hedged_qty);
            self.process_open_leg_trade(trade);
        } else if self.hedge_order_ids.contains(&order_id) {
            // 对冲成交，更新累计对冲量
            self.cumulative_hedged_qty = trade.cumulative_filled_quantity();
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲订单成交 order_id={} 成交量={:.8} 开仓量/已对冲量={:.8}/{:.8}",
                self.strategy_id,
                order_id,
                trade.quantity(),
                self.cumulative_open_qty,
                self.cumulative_hedged_qty);
            self.process_hedge_leg_trade(trade);
        } else {
            // 非法成交，忽略
            warn!(
                "HedgeArbStrategy: strategy_id={} 收到未知订单的成交更新 order_id={}",
                self.strategy_id,
                order_id);      
        } 
        
    }





    fn apply_order_update(&mut self, order: &dyn OrderUpdate){
        
        
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