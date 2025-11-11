use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{OrderExecutionStatus, OrderType, Side};
use crate::pre_trade::{PersistChannel, SignalChannel};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::SignalRecordMessage;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::Strategy;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::risk_checker::PreTradeEnv;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, error, info, warn};
use std::rc::Rc;

pub struct HedgeArbStrategy {
    pub strategy_id: i32,               //策略id
    pub symbol: String,                 //交易的统一symbol (开仓侧symbol)
    pub open_order_id: i64,             //开仓单唯一，报多单对应多个Strategy
    pub hedge_order_ids: Vec<i64>,      //对冲单会产生一个or多个，因为部分成交
    pub open_timeout_us: Option<i64>,   //开仓单最长挂单时间，超过撤销
    pub hedge_timeout_us: Option<i64>, //对冲单最长挂单时间，超过撤销，度过是maker-taker模式，则没有这个timeout，设置为None
    pub order_seq: u32,                //订单号计数器
    pub pre_trade_env: Rc<PreTradeEnv>, //预处理global变量封装
    pub cumulative_hedged_qty: f64,    //累计对冲数量
    pub cumulative_open_qty: f64,      //累计开仓数量
    pub alive_flag: bool,              //策略是否存活
    pub hedge_symbol: String,          //对冲侧symbol
    pub hedge_venue: TradingVenue,     //对冲侧交易场所
    pub hedge_side: Side,              //对冲侧方向
} 

impl HedgeArbStrategy {
    pub fn new(id: i32, symbol: String, env: Rc<PreTradeEnv>) -> Self {
        let strategy = Self {
            strategy_id: id,
            symbol,
            open_order_id: 0,
            hedge_order_ids: Vec::new(),
            open_timeout_us: None,
            hedge_timeout_us: None,
            order_seq: 0,
            pre_trade_env: env.clone(),
            cumulative_hedged_qty: 0.0,
            cumulative_open_qty: 0.0,
            alive_flag: true,
            hedge_symbol: String::new(),
            hedge_venue: TradingVenue::BinanceMargin, // 默认值，将在开仓时更新
            hedge_side: Side::Buy,                    // 默认值，将在开仓时更新
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
                self.strategy_id, e 
            ); 
            self.alive_flag = false; 
            return;
        }

        // 2、检查symbol的敞口，失败打印error
        if let Err(e) = self
            .pre_trade_env
            .risk_checker
            .check_symbol_exposure(&self.symbol)
        {
            error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}，标记策略为不活跃", self.strategy_id, self.symbol, e);
            self.alive_flag = false;
            return;
        }

        // 3、检查总敞口，失败打印error
        if let Err(e) = self.pre_trade_env.risk_checker.check_total_exposure() {
            error!(
                "HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        // 4、检查限价挂单数量限制（如果是限价单）
        let order_type = OrderType::from_u8(ctx.order_type);
        if order_type == Some(OrderType::Limit) {
            if let Err(e) = self
                .pre_trade_env
                .risk_checker
                .check_pending_limit_order(&self.symbol)
            {
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
        if let Err(e) =
            self.pre_trade_env
                .risk_checker
                .ensure_max_pos_u(&symbol, aligned_qty, aligned_price)
        {
            error!(
                "HedgeArbStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃",
                self.strategy_id, e
            );
            self.alive_flag = false;
            return;
        }

        // 10、用修正量价，开仓订单记录到order manager
        // todo: 订单持久化直接写死在manager中
        let client_order_id = self
            .pre_trade_env
            .risk_checker
            .order_manager
            .borrow_mut()
            .create_order(
                venue,
                order_id,
                OrderType::from_u8(ctx.order_type).unwrap(),
                symbol.clone(),
                Side::from_u8(ctx.side).unwrap(),
                aligned_qty,
                aligned_price,
                ts,
            );
        log::info!(
            "created open order with client_order_id: {}",
            client_order_id
        );

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
                self.strategy_id, ctx.hedge_qty
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
                self.strategy_id, e
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
            OrderType::Limit // Maker订单使用限价单
        } else {
            OrderType::Market // Taker订单使用市价单
        };

        // 获取对冲方向
        let hedge_side = ctx
            .get_side()
            .ok_or_else(|| format!("无效的对冲方向: {}", ctx.hedge_side))?;

        // 6. 创建对冲订单并记录到order manager
        let hedge_client_order_id = self
            .pre_trade_env
            .risk_checker
            .order_manager
            .borrow_mut()
            .create_order(
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
        let mut order = self
            .pre_trade_env
            .risk_checker
            .order_manager
            .borrow_mut()
            .get(client_order_id);
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
                self.strategy_id, symbol, order_type_str, client_order_id
            );
            self.alive_flag = false;
            Err(format!("未找到创建的{}订单", order_type_str))
        }
    }

    // cancel的本质就是构造取消，实际处理的是account monitor的撤销回报
    fn handle_arb_cancel_signal(&mut self, _ctx: ArbCancelCtx) -> Result<(), String> {
        // 从 order manager 获取开仓订单
        let order = self
            .pre_trade_env
            .risk_checker
            .order_manager
            .borrow()
            .get(self.open_order_id);
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

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
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
            _ => {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 忽略信号类型 {:?}",
                    self.strategy_id, signal.signal_type
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
                let order = self
                    .pre_trade_env
                    .risk_checker
                    .order_manager
                    .borrow()
                    .get(self.open_order_id);
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
                    let order = self
                        .pre_trade_env
                        .risk_checker
                        .order_manager
                        .borrow()
                        .get(hedge_order_id);
                    if let Some(order) = order {
                        match order.get_order_cancel_bytes() {
                            Ok(cancel_bytes) => {
                                if let Err(e) =
                                    self.pre_trade_env.trade_request_tx.send(cancel_bytes)
                                {
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


    // 检查当前的对冲单的id列表的最后一个。查看是否是terminal状态
    fn has_pending_hedge_order(&self) -> bool {
        // 获取最后一个对冲订单ID
        if let Some(&last_hedge_id) = self.hedge_order_ids.last() {
            // 从order manager获取订单
            let order = self
                .pre_trade_env
                .risk_checker
                .order_manager
                .borrow()
                .get(last_hedge_id);
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
    fn hedge_as_market_order(
        &mut self,
        venue: TradingVenue,
        side: Side,
        eff_qty: f64,
    ) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id, eff_qty
            );
            return Ok(());
        }

        // 1. 创建对冲上下文（市价单模式，exp_time = 0）
        let mut hedge_ctx =
            ArbHedgeCtx::new_taker(self.strategy_id, self.open_order_id, eff_qty, side.to_u8());

        // 2. 设置对冲symbol
        hedge_ctx.set_hedging_symbol(&self.symbol);

        // 3. 设置对冲leg（市价单不需要盘口价格，设置为0）
        hedge_ctx.hedging_leg = crate::signal::common::TradingLeg {
            venue: venue.to_u8(),
            bid0: 0.0, // 市价单不需要盘口价格
            ask0: 0.0, // 市价单不需要盘口价格
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
                self.strategy_id, venue, side, eff_qty
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
    fn hedge_as_limit_order(
        &mut self,
        venue: TradingVenue,
        side: Side,
        eff_qty: f64,
    ) -> Result<(), String> {
        if eff_qty <= 1e-8 {
            debug!(
                "HedgeArbStrategy: strategy_id={} 限价单对冲量过小 eff_qty={:.8}，跳过",
                self.strategy_id, eff_qty
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

        // 2. 通过 SignalChannel 直接发送到上游
        let send_result = SignalChannel::with(|ch| {
            ch.publish_backward(&query_msg.to_bytes())
        });

        match send_result {
            Ok(true) => {
                debug!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲查询成功 venue={:?} side={:?} qty={:.8}",
                    self.strategy_id, venue, side, eff_qty
                );
            }
            Ok(false) => {
                warn!(
                    "HedgeArbStrategy: strategy_id={} backward publisher 未配置，无法发送对冲查询",
                    self.strategy_id
                );
                return Err("backward publisher 未配置".to_string());
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 发送对冲查询失败: {}",
                    self.strategy_id, e
                );
                return Err(format!("发送对冲查询失败: {}", e));
            }
        }

        Ok(())
    }

    /// 尝试对冲（含残值处理）
    ///
    /// # Arguments
    /// * `base_qty` - 基础待对冲量（不含残值）
    ///
    /// # Returns
    /// (是否成功发起对冲, 实际对冲量, 从残值表取出的数量)
    /// - 成功发起对冲时：(true, 对冲数量, 从残值表增加的数量)
    /// - 不满足要求时：(false, 0.0, 累加到残值表的数量)
    fn try_hedge_with_residual(&mut self, base_qty: f64) -> (bool, f64, f64) {
        // 1. 计算总待对冲量（基础量 + 残值表中的残余量）
        let mut total_pending_qty = base_qty;
        let residual_qty = self
            .pre_trade_env
            .clear_hedge_residual(&self.hedge_symbol, self.hedge_venue);
        total_pending_qty += residual_qty;

        info!(
            "HedgeArbStrategy: strategy_id={} 待对冲量: 基础={:.8} 残余={:.8} 总计={:.8}",
            self.strategy_id, base_qty, residual_qty, total_pending_qty
        );

        // 2. 检查是否满足最小交易要求
        let can_hedge: bool = self
            .pre_trade_env
            .check_min_trading_requirements(
                self.hedge_venue,
                &self.hedge_symbol,
                total_pending_qty,
                None,
            )
            .is_ok();

        if !can_hedge {
            // 不满足最小交易要求，累加到残值表
            if total_pending_qty > 1e-12 {
                self.pre_trade_env.inc_hedge_residual(
                    self.hedge_symbol.clone(),
                    self.hedge_venue,
                    total_pending_qty,
                );
                info!(
                    "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 不满足最小交易要求，累加到残余量表",
                    self.strategy_id, total_pending_qty
                );
                return (false, 0.0, total_pending_qty);
            }
            return (false, 0.0, 0.0);
        }

        // 3. 满足要求，发起对冲
        info!(
            "HedgeArbStrategy: strategy_id={} 待对冲量={:.8} 满足最小交易要求，准备对冲",
            self.strategy_id, total_pending_qty
        );

        let hedge_result: Result<(), String> = if self.hedge_timeout_us.is_some() {
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
                (true, total_pending_qty, residual_qty)
            }
            Err(e) => {
                error!(
                    "HedgeArbStrategy: strategy_id={} 对冲失败: {}",
                    self.strategy_id, e
                );
                // 对冲失败，将数量累加回残值表
                if total_pending_qty > 1e-12 {
                    self.pre_trade_env.inc_hedge_residual(
                        self.hedge_symbol.clone(),
                        self.hedge_venue,
                        total_pending_qty,
                    );
                    (false, 0.0, total_pending_qty)
                } else {
                    (false, 0.0, 0.0)
                }
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
        drop(order_manager);
        if !updated {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到开仓订单 order_id={}",
                self.strategy_id, order_id
            );
            return;
        }
        // 2. 计算待对冲量 = 已经成交的量 - 已经对冲的量
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
        // 3. 尝试对冲（含残值处理）
        let (can_hedge, hedged_qty, residual_qty) = self.try_hedge_with_residual(base_pending_qty);

        if can_hedge {
            info!(
                "HedgeArbStrategy: strategy_id={} 开仓撤单后对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            info!(
                "HedgeArbStrategy: strategy_id={} 开仓撤单后无法对冲，残值={:.8}",
                self.strategy_id, residual_qty
            );
        }
        // 4. 剩余量已经无法开单
        if !can_hedge {
            if self.hedge_timeout_us.is_some() {
                self.alive_flag = false;
            } else {
                let has_pending_hedge = self.has_pending_hedge_order();
                if !has_pending_hedge {
                    self.alive_flag = false;
                }
            }
        }
    }

    fn process_hedge_leg_cancel(&mut self, cancel_update: &dyn OrderUpdate) {
        // 只有 MM 模式才会出现对冲侧的 cancel（因为 MT 模式是市价单，立即成交）
        if self.hedge_timeout_us.is_none() {
            error!(
                "HedgeArbStrategy: strategy_id={} MT模式不应该有对冲侧撤单，订单ID={}",
                self.strategy_id,
                cancel_update.client_order_id() 
            );
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
        drop(order_manager);

        if !updated {
            warn!(
                "HedgeArbStrategy: strategy_id={} 未找到对冲订单 order_id={}",
                self.strategy_id, order_id
            );
            return;
        }

        // 2. 计算待对冲量 = 已经成交的量 - 已经对冲的量
        let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;

        // 3、 尝试对冲（含残值处理）
        let (can_hedge, hedged_qty, residual_qty) = self.try_hedge_with_residual(base_pending_qty);

        if can_hedge {
            info!(
                "HedgeArbStrategy: strategy_id={} 对冲撤单后重新对冲成功，对冲量={:.8}",
                self.strategy_id, hedged_qty
            );
        } else if residual_qty > 1e-12 {
            info!(
                "HedgeArbStrategy: strategy_id={} 对冲撤单后无法对冲，残值={:.8}",
                self.strategy_id, residual_qty
            );
        }

        // 4. MM模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
        if !can_hedge && !self.has_pending_hedge_order() {
            self.alive_flag = false;
        }
    }

    fn process_open_leg_trade(&mut self, trade: &dyn TradeUpdate) {
        // 开仓成交后，触发对冲逻辑
        // MM 模式：只有完全成交才对冲, 部分成交只扣除量
        if self.hedge_timeout_us.is_some() {
            match trade.order_status() {
                Some(OrderStatus::Filled) => {
                    // 计算待对冲量 = 已经成交的量 - 已经对冲的量
                    let base_pending_qty: f64 =
                        self.cumulative_open_qty - self.cumulative_hedged_qty;
                    // 尝试对冲（含残值处理）
                    let (can_hedge, _hedged_qty, residual_qty) =
                        self.try_hedge_with_residual(base_pending_qty);
                    if can_hedge {
                        //开仓量修改
                        self.cumulative_open_qty += residual_qty;
                        //MM 模式，挂单不一定成交。所以等实际成交再更新对冲量
                    }
                    if !can_hedge && !self.has_pending_hedge_order() {
                        self.alive_flag = false;
                    }
                }
                Some(OrderStatus::PartiallyFilled) => {
                    //部分成交，只扣量，不对冲
                    self.cumulative_hedged_qty += trade.quantity();
                }
                _ => {
                    error!(
                        "unexpected MM model trade event: {} {}",
                        trade.client_order_id(),
                        trade.trade_id()
                    );
                }
            }
        }
        // MT 模式：部分成交和完全成交都对冲
        if self.hedge_timeout_us == None {
            if trade.order_status() == Some(OrderStatus::Filled)
                || trade.order_status() == Some(OrderStatus::PartiallyFilled)
            {
                let base_pending_qty = self.cumulative_open_qty - self.cumulative_hedged_qty;
                // 尝试对冲（含残值处理）
                let (can_hedge, hedged_qty, residual_qty) =
                    self.try_hedge_with_residual(base_pending_qty);
                if can_hedge {
                    self.cumulative_open_qty += residual_qty;
                    self.cumulative_hedged_qty += hedged_qty;
                } else if residual_qty > 1e-12 {
                    info!(
                        "HedgeArbStrategy: strategy_id={} MT模式开仓成交但无法对冲，残值={:.8}",
                        self.strategy_id, residual_qty
                    );
                }
                // MT 模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
                if !can_hedge && !self.has_pending_hedge_order() {
                    self.alive_flag = false;
                }
            }
        }
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
            let residual_qty = self
                .pre_trade_env
                .get_hedge_residual(&self.hedge_symbol, self.hedge_venue);
            let total_remaining = remaining_qty + residual_qty;

            debug!(
                "HedgeArbStrategy: strategy_id={} MT模式对冲成交后检查: 剩余={:.8} 残值={:.8} 总计={:.8}",
                self.strategy_id, remaining_qty, residual_qty, total_remaining
            );

            // 检查剩余量是否满足最小交易要求
            if total_remaining > 1e-12 {
                let can_hedge = self
                    .pre_trade_env
                    .check_min_trading_requirements(
                        self.hedge_venue,
                        &self.hedge_symbol,
                        total_remaining,
                        None,
                    )
                    .is_ok();

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

    // 处理交易更新
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
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
            // 对冲侧成交，增加累计对冲量
            self.cumulative_hedged_qty = trade.quantity();
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
                self.strategy_id, order_id
            );
        }
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) {
        //状态更新是通用部分，非成交只更新状态，不更新量
        let client_order_id = order_update.client_order_id();
        let mut order_manager = self.pre_trade_env.risk_checker.order_manager.borrow_mut();
        let updated = order_manager.update(client_order_id, |order| match order_update.status() {
            OrderStatus::New => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.set_create_time(order_update.event_time());
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Expired => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_end_time(order_update.event_time());
            }
            _ => {
                panic!(
                    "unexpected order status received {} {} {:?}",
                    order_update.client_order_id(),
                    order_update.order_id(),
                    order_update.status()
                );
            }
        });
        drop(order_manager);

        if !updated {
            error!(
                "update failed {} {} {:?}",
                order_update.client_order_id(),
                order_update.order_id(),
                order_update.status()
            );
            return;
        }
        if order_update.status() == OrderStatus::Canceled {
            if order_update.client_order_id() == self.open_order_id {
                self.process_open_leg_cancel(order_update);
            }
            if self.hedge_timeout_us.is_none() {
                if self
                    .hedge_order_ids
                    .contains(&order_update.client_order_id())
                {
                    //只有MT模式，才会有hedge的定时器cancel
                    self.process_hedge_leg_cancel(order_update);
                }
            }
        }
    }

    fn cleanup_strategy_orders(&mut self) {
        let mut mgr = self.pre_trade_env.risk_checker.order_manager.borrow_mut();

        // 检查并清理开仓订单
        if self.open_order_id != 0 {
            if let Some(order) = mgr.get(self.open_order_id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "开仓订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(self.open_order_id);
        }

        // 检查并清理对冲订单
        for id in &self.hedge_order_ids {
            if let Some(order) = mgr.get(*id) {
                if !order.status.is_terminal() {
                    mgr.log_order_details(&order, "对冲订单未达到终结状态被清理", self.strategy_id);
                }
            }
            let _ = mgr.remove(*id);
        }
    }
}

impl Drop for HedgeArbStrategy {
    fn drop(&mut self) {
        self.cleanup_strategy_orders();
    }
}

impl Strategy for HedgeArbStrategy {
    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
    }

    fn handle_signal_with_record(&mut self, signal: &TradeSignal){
        HedgeArbStrategy::handle_signal(self, signal);

        // 持久化信号记录
        let record = SignalRecordMessage::new(
            self.strategy_id,
            signal.signal_type.clone(),
            signal.context.clone().to_vec(),
            signal.generation_time,
        );
        PersistChannel::with(|ch| ch.publish_signal_record(&record));
    }

    fn apply_order_update_with_record(&mut self, update: &dyn OrderUpdate) {
        HedgeArbStrategy::apply_order_update(self, update);

        // 持久化订单更新记录
        PersistChannel::with(|ch| ch.publish_order_update(update));
    }

    fn apply_trade_update_with_record(&mut self, trade: &dyn TradeUpdate){
        HedgeArbStrategy::apply_trade_update(self, trade);

        // 持久化成交记录
        PersistChannel::with(|ch| ch.publish_trade_update(trade));
    } 

    fn handle_period_clock(&mut self, _current_tp: i64) {
        // 周期性检查开仓和对冲订单的超时情况
        self.handle_open_leg_timeout();
        self.handle_hedge_leg_timeout();
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}
