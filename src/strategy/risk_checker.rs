use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use bytes::Bytes;
use log::{debug, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::exposure_manager::ExposureManager;
use crate::pre_trade::order_manager::{OrderManager, Side};
use crate::pre_trade::price_table::PriceTable;
use crate::signal::common::TradingVenue;

fn extract_base_asset(symbol_upper: &str) -> Option<String> {
    const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
    for quote in QUOTES {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
        }
    }
    None
}

#[derive(Clone)]
pub struct RiskChecker {
    pub exposure_manager: Rc<RefCell<ExposureManager>>, // 敞口管理，用于风控决策
    pub order_manager: Rc<RefCell<OrderManager>>, // 订单管理，所有订单维护在其中，完全成交或者撤单会被移除
    pub price_table: std::rc::Rc<std::cell::RefCell<PriceTable>>, //定价计算，维护markprice等
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_pos_u: f64,
    max_leverage: f64,
    max_pending_limit_orders: Rc<Cell<i32>>,
}

impl RiskChecker {
    /// 创建新的风控检查器
    pub fn new(
        exposure_manager: Rc<RefCell<ExposureManager>>,
        order_manager: Rc<RefCell<OrderManager>>,
        price_table: Rc<RefCell<PriceTable>>,
        max_symbol_exposure_ratio: f64,
        max_total_exposure_ratio: f64,
        max_pos_u: f64,
        max_leverage: f64,
        max_pending_limit_orders: Rc<Cell<i32>>,
    ) -> Self {
        Self {
            exposure_manager,
            order_manager,
            price_table,
            max_symbol_exposure_ratio,
            max_total_exposure_ratio,
            max_pos_u,
            max_leverage,
            max_pending_limit_orders,
        }
    }

    // 检查当前 symbol 的限价挂单数量
    pub fn check_pending_limit_order(&self, symbol: &str) -> Result<(), String> {
        if self.max_pending_limit_orders.get() <= 0 {
            return Ok(());
        }

        let symbol_upper = symbol.to_uppercase();
        let count = self
            .order_manager
            .borrow()
            .get_symbol_pending_limit_order_count(&symbol_upper);

        if count >= self.max_pending_limit_orders.get() {
            return Err(format!(
                "symbol={} 当前限价挂单数={}，达到上限 {}",
                symbol,
                count,
                self.max_pending_limit_orders.get()
            ));
        }

        Ok(())
    }
    //symbol是xxusdt，查看当前symbol的敞口是否大于总资产比例的3%
    pub fn check_symbol_exposure(&self, symbol: &str) -> Result<(), String> {
        let limit = self.max_symbol_exposure_ratio;
        if limit <= 0.0 {
            return Ok(());
        }

        let symbol_upper = symbol.to_uppercase();
        let Some(base_asset) = extract_base_asset(&symbol_upper) else {
            return Err(format!(
                "无法识别 symbol={} 的基础资产，无法校验敞口比例",
                symbol
            ));
        };

        // 获取敞口信息和总权益
        let (entry, total_equity) = {
            let exposure_manager = self.exposure_manager.borrow();
            let entry = exposure_manager.exposure_for_asset(&base_asset).cloned();
            let total_equity = exposure_manager.total_equity();
            (entry, total_equity)
        };

        let Some(entry) = entry else {
            return Err(format!(
                "无法获取资产 {} 的敞口信息，无法校验敞口比例",
                base_asset
            ));
        };
        if total_equity <= f64::EPSILON {
            return Err(format!(
                "symbol={} 敞口比例超过限制 {}",
                symbol, self.max_symbol_exposure_ratio
            ));
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

        // 计算净敞口 (现货 + 合约)
        let net_exposure = entry.spot_total_wallet + entry.um_net_position;
        let exposure_usdt = if mark > 0.0 { net_exposure * mark } else { 0.0 };

        // 若缺少价格但敞口非零，回退到数量比例并提示
        if mark == 0.0 && net_exposure != 0.0 {
            let ratio = net_exposure.abs() / total_equity;
            if ratio > limit {
                debug!(
                    "资产 {} 敞口占比(数量) {:.4}% 超过阈值 {:.2}% (敞口qty={:.6}, 权益={:.6})",
                    base_asset,
                    ratio * 100.0,
                    limit * 100.0,
                    net_exposure,
                    total_equity
                );
                return Err(format!(
                    "symbol={} 敞口比例超过限制 {}",
                    symbol, self.max_symbol_exposure_ratio
                ));
            }
            return Ok(());
        }

        let ratio = exposure_usdt.abs() / total_equity;
        if ratio > limit {
            debug!(
                "资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口USDT={:.6}, 权益={:.6})",
                base_asset,
                ratio * 100.0,
                limit * 100.0,
                exposure_usdt,
                total_equity
            );
            return Err(format!(
                "symbol={} 敞口比例超过限制 {}",
                symbol, self.max_symbol_exposure_ratio
            ));
        } else {
            Ok(())
        }
    }

    // 检查总敞口是否超过配置阈值
    pub fn check_total_exposure(&self) -> Result<(), String> {
        let limit = self.max_total_exposure_ratio;
        if limit <= 0.0 {
            return Ok(());
        }

        // 获取必要的数据
        let (total_equity, exposures) = {
            let exposure_manager = self.exposure_manager.borrow();
            let total_equity = exposure_manager.total_equity();
            let exposures = exposure_manager.exposures().to_vec();
            (total_equity, exposures)
        };

        if total_equity <= f64::EPSILON {
            return Err("账户总权益近似为 0，无法计算总敞口占比".to_string());
        }

        // 使用价格表将所有非 USDT 资产净敞口估值为 USDT，并取绝对值求和
        let snap = self.price_table.borrow().snapshot();
        let mut abs_total_usdt = 0.0_f64;

        for e in exposures.iter() {
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
                "总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益={:.6})",
                ratio * 100.0,
                limit * 100.0,
                abs_total_usdt,
                total_equity
            );
            return Err(format!(
                "总敞口比例 {:.2}% 超过限制 {:.2}%",
                ratio * 100.0,
                limit * 100.0
            ));
        }

        Ok(())
    }

    // 检查当前是否可以平仓，返回需要平仓现货、合约数量
    fn check_closable_position(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
    ) -> Result<(f64, f64), String> {
        let symbol_upper = symbol.to_uppercase();
        let Some(base_asset) = extract_base_asset(&symbol_upper) else {
            return Err(format!(
                "无法识别 symbol={} 的基础资产，无法校验平仓能力",
                symbol
            ));
        };

        // 获取敞口信息
        let entry = {
            let exposure_manager = self.exposure_manager.borrow();
            exposure_manager.exposure_for_asset(&base_asset).cloned()
        };

        let Some(entry) = entry else {
            return Err(format!(
                "无法获取资产 {} 的敞口信息，无法校验平仓能力",
                base_asset
            ));
        };

        let mut spot_close_qty = 0.0_f64;
        let mut um_close_qty = 0.0_f64;

        match side {
            Side::Buy => {
                // 买入平空
                if entry.spot_total_wallet < 0.0 {
                    spot_close_qty = qty.min(-entry.spot_total_wallet);
                }
                if entry.um_net_position < 0.0 {
                    um_close_qty = qty.min(-entry.um_net_position);
                }
            }
            Side::Sell => {
                // 卖出平多
                if entry.spot_total_wallet > 0.0 {
                    spot_close_qty = qty.min(entry.spot_total_wallet);
                }
                if entry.um_net_position > 0.0 {
                    um_close_qty = qty.min(entry.um_net_position);
                }
            }
        }
        log::info!(
            "可平仓头寸 symbol={} side={:?} qty={} => spot_close_qty={} um_close_qty={}",
            symbol,
            side,
            qty,
            spot_close_qty,
            um_close_qty
        );
        Ok((spot_close_qty, um_close_qty))
    }

    pub fn ensure_max_pos_u(
        &self,
        symbol: &str,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        if !(self.max_pos_u > 0.0) {
            panic!("max_pos_u not set!!");
        }
        let symbol_upper = symbol.to_uppercase();
        let base_asset = extract_base_asset(&symbol_upper)
            .ok_or_else(|| format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol))?;
        let current_spot_qty = {
            let exposure_manager = self.exposure_manager.borrow();
            exposure_manager
                .exposure_for_asset(&base_asset)
                .map(|entry| entry.spot_total_wallet)
                .unwrap_or(0.0)
        };

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
            warn!("symbol={} 缺少 USDT 标记价格，无法校验 max_pos_u", symbol);
            return Err(format!(
                "symbol={} 缺少价格信息，无法校验 max_pos_u",
                symbol
            ));
        };

        let projected_qty = current_spot_qty + additional_qty;
        let current_usdt = current_spot_qty.abs() * price;
        let order_usdt = additional_qty.abs() * price;
        let projected_usdt = projected_qty.abs() * price;
        let limit_eps = 1e-6_f64;

        if projected_usdt > self.max_pos_u + limit_eps {
            warn!(
                "symbol={} 当前现货={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 预计现货={:.4}USDT 超过阈值 {:.4}USDT",
                symbol,
                current_spot_qty,
                current_usdt,
                additional_qty,
                order_usdt,
                projected_usdt,
                self.max_pos_u
            );
            return Err(format!(
                "symbol={} 预计现货持仓 {:.4}USDT 超过阈值 {:.4}USDT",
                symbol, projected_usdt, self.max_pos_u
            ));
        }
        Ok(())
    }

    // 检查杠杆率是否超过配置阈值
    pub fn check_leverage(&self) -> Result<(), String> {
        let limit = self.max_leverage;
        if limit <= 0.0 {
            return Ok(());
        }

        // 获取必要的数据
        let (total_equity, total_position) = {
            let exposure_manager = self.exposure_manager.borrow();
            let total_equity = exposure_manager.total_equity();
            let total_position = exposure_manager.total_position();
            (total_equity, total_position)
        };

        if total_equity <= f64::EPSILON {
            return Err("账户总权益近似为 0，无法计算杠杆率".to_string());
        }

        let leverage = total_position / total_equity;
        if leverage > limit {
            debug!(
                "当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益={:.6})",
                leverage, limit, total_position, total_equity
            );
            return Err(format!("杠杆率 {:.2} 超过限制 {:.2}", leverage, limit));
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct PreTradeEnv {
    pub min_qty_table: std::rc::Rc<MinQtyTable>, //最小下单量维护，用于修证下单
    pub risk_checker: RiskChecker,
    pub hedge_residual_map: Rc<RefCell<HashMap<(String, TradingVenue), f64>>>, //对冲残余量哈希表，key=(symbol, venue)，value=残余量
} 

impl PreTradeEnv {
    /// 创建新的预交易环境
    pub fn new(
        min_qty_table: Rc<MinQtyTable>,
        risk_checker: RiskChecker,
    ) -> Self {
        Self {
            min_qty_table,
            risk_checker,
            hedge_residual_map: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// 根据交易场所对齐订单量和价格
    /// 返回 (对齐后的数量, 对齐后的价格)
    pub fn align_order_by_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
    ) -> Result<(f64, f64), String> {
        match venue {
            TradingVenue::BinanceUm | TradingVenue::BinanceMargin => {
                TradingVenue::align_um_order(symbol, raw_qty, raw_price, &self.min_qty_table)
            }
            TradingVenue::BinanceSwap => {
                // TODO: 实现 BinanceSwap 的对齐逻辑
                Err(format!("尚未实现 BinanceSwap 的订单对齐"))
            }
            TradingVenue::BinanceSpot => {
                // TODO: 实现 BinanceSpot 的对齐逻辑
                Err(format!("尚未实现 BinanceSpot 的订单对齐"))
            }
            TradingVenue::OkexSwap | TradingVenue::OkexSpot => {
                // TODO: 实现 Okex 的对齐逻辑
                Err(format!("尚未实现 Okex 的订单对齐"))
            }
        }
    }

    /// 检查交易量是否满足最小要求
    /// 包括最小下单量和最小名义金额检查
    pub fn check_min_trading_requirements(
        &self,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price_hint: Option<f64>,
    ) -> Result<(), String> {
        // 1. 检查最小下单量
        let min_qty = match venue {
            TradingVenue::BinanceUm => self.min_qty_table.futures_um_min_qty_by_symbol(symbol),
            TradingVenue::BinanceMargin => self.min_qty_table.margin_min_qty_by_symbol(symbol),
            TradingVenue::BinanceSpot | TradingVenue::OkexSpot => {
                self.min_qty_table.spot_min_qty_by_symbol(symbol)
            }
            TradingVenue::BinanceSwap | TradingVenue::OkexSwap => {
                // TODO: 根据实际情况添加 swap 的最小量获取
                None
            }
        }
        .unwrap_or(0.0);

        if min_qty > 0.0 && qty + 1e-12 < min_qty {
            return Err(format!("交易量 {:.8} 小于最小下单量 {:.8}", qty, min_qty));
        }

        // 2. 检查最小名义金额（仅对 UM 合约）
        if venue == TradingVenue::BinanceUm {
            let min_notional = self
                .min_qty_table
                .futures_um_min_notional_by_symbol(symbol)
                .unwrap_or(0.0);

            if min_notional > 0.0 {
                // 如果没有提供价格提示，尝试从价格表获取
                let price = if let Some(p) = price_hint {
                    p
                } else {
                    self.risk_checker
                        .price_table
                        .borrow()
                        .mark_price(symbol)
                        .unwrap_or(0.0)
                };

                if price <= 0.0 {
                    return Err(format!("缺少 {} 的价格信息，无法验证名义金额", symbol));
                }

                let notional = price * qty;
                if notional + 1e-8 < min_notional {
                    return Err(format!(
                        "名义金额 {:.8} 低于最小要求 {:.8} (价格={:.8} 数量={:.8})",
                        notional, min_notional, price, qty
                    ));
                }
            }
        }

        Ok(())
    }

    /// 增加对冲残余量
    ///
    /// # Arguments
    /// * `symbol` - 交易对符号
    /// * `venue` - 交易场所
    /// * `delta` - 要增加的残余量
    pub fn inc_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) {
        if delta <= 1e-12 {
            return;
        }
        let mut map = self.hedge_residual_map.borrow_mut();
        let key = (symbol.to_uppercase(), venue);
        let current = map.get(&key).copied().unwrap_or(0.0);
        let new_value = current + delta;
        map.insert(key.clone(), new_value);
        debug!(
            "对冲残余量增加: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
            key.0, venue, delta, current, new_value
        );
    }

    /// 减少对冲残余量
    ///
    /// # Arguments
    /// * `symbol` - 交易对符号
    /// * `venue` - 交易场所
    /// * `delta` - 要减少的残余量
    ///
    /// # Returns
    /// 实际减少的量（不会超过当前残余量）
    pub fn dec_hedge_residual(&self, symbol: String, venue: TradingVenue, delta: f64) -> f64 {
        if delta <= 1e-12 {
            return 0.0;
        }
        let mut map = self.hedge_residual_map.borrow_mut();
        let key = (symbol.to_uppercase(), venue);
        let current = map.get(&key).copied().unwrap_or(0.0);
        let actual_dec = delta.min(current);
        let new_value = (current - actual_dec).max(0.0);

        if new_value <= 1e-12 {
            // 残余量归零，从 map 中移除
            map.remove(&key);
            debug!(
                "对冲残余量清零: symbol={} venue={:?} 原值={:.8} 减少={:.8}",
                key.0, venue, current, actual_dec
            );
        } else {
            map.insert(key.clone(), new_value);
            debug!(
                "对冲残余量减少: symbol={} venue={:?} delta={:.8} 当前值={:.8} -> {:.8}",
                key.0, venue, actual_dec, current, new_value
            );
        }

        actual_dec 
    }

    /// 查询对冲残余量
    ///
    /// # Arguments
    /// * `symbol` - 交易对符号
    /// * `venue` - 交易场所
    ///
    /// # Returns 
    /// 当前的残余量，如果不存在则返回 0.0 
    pub fn get_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        let map = self.hedge_residual_map.borrow();
        let key = (symbol.to_uppercase(), venue);
        map.get(&key).copied().unwrap_or(0.0)
    } 

    /// 清除指定 symbol 和 venue 的对冲残余量
    ///
    /// # Arguments
    /// * `symbol` - 交易对符号
    /// * `venue` - 交易场所
    ///
    /// # Returns
    /// 被清除的残余量
    pub fn clear_hedge_residual(&self, symbol: &str, venue: TradingVenue) -> f64 {
        let mut map = self.hedge_residual_map.borrow_mut();
        let key = (symbol.to_uppercase(), venue);
        let removed = map.remove(&key).unwrap_or(0.0);
        if removed > 1e-12 {
            debug!(
                "对冲残余量清除: symbol={} venue={:?} 清除量={:.8}",
                key.0, venue, removed
            );
        }
        removed
    } 
}
