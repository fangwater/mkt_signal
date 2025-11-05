use std::cell::{Cell, RefCell};
use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use libc::ERA;
use log::{debug, error, info, warn};
use tokio::sync::mpsc::UnboundedSender;

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::exposure_manager::ExposureManager;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::price_table::PriceTable;
use crate::signal::strategy::Strategy;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceNewMarginOrderRequest, BinanceNewUMOrderRequest,
    TradeRequestType,
};

fn extract_base_asset(symbol_upper: &str) -> Option<String> {
    const QUOTES: [&str; 6] = ["USDT", "BUSD", "USDC", "FDUSD", "BIDR", "TRY"];
    for quote in QUOTES {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            return Some(symbol_upper[..symbol_upper.len() - quote.len()].to_string());
        }
    }
    None
}


pub struct RiskChecker{
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
    fn check_symbol_exposure(&self, symbol: &str) -> Result<(), String>  {
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
        let exposure_usdt = if mark > 0.0 {
            net_exposure * mark
        } else {
            0.0
        };

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
            ))
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
                leverage,
                limit,
                total_position,
                total_equity
            );
            return Err(format!(
                "杠杆率 {:.2} 超过限制 {:.2}",
                leverage,
                limit
            ));
        }

        Ok(())
    }

    fn {
        let symbol_upper = open_ctx.spot_symbol.to_uppercase();
        let base_asset = Self::extract_base_asset(&symbol_upper).ok_or_else(|| {
            format!(
                "{}: 无法识别 symbol={} 的基础资产，无法校验 max_pos_u",
                Self::strategy_name(),
                open_ctx.spot_symbol
            )
        })?;
        let current_spot_qty = {
            let exposure_manager = self.exposure_manager.borrow();
            exposure_manager
                .exposure_for_asset(&base_asset)
                .map(|entry| entry.spot_total_wallet)
                .unwrap_or(0.0)
        };

        let now = get_timestamp_us();
        let side_str = open_ctx.side.as_str();
        let order_type_str = open_ctx.order_type.as_str();
        // 数量 LOT_SIZE 对齐（优先使用合约 UM 参数，确保对冲数量一致），若缺失再退化 margin/spot
        let raw_qty = f64::from(open_ctx.amount);
        let step = self
            .min_qty_table
            .futures_um_step_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.margin_step_by_symbol(&self.symbol))
            .or_else(|| self.min_qty_table.spot_step_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        let min_qty = self
            .min_qty_table
            .futures_um_min_qty_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.margin_min_qty_by_symbol(&self.symbol))
            .or_else(|| self.min_qty_table.spot_min_qty_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        let mut eff_qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };
        if min_qty > 0.0 && eff_qty < min_qty {
            eff_qty = min_qty;
        }
        if eff_qty <= 0.0 {
            return Err(format!(
                "{}: symbol={} 数量对齐后为 0，raw_qty={} step={} min_qty={}",
                Self::strategy_name(),
                self.symbol,
                raw_qty,
                step,
                min_qty
            ));
        }
        // 价格对齐（限价单）
        let price_tick = open_ctx.price_tick.max(0.0);
        let raw_price = open_ctx.price;
        let mut effective_price = raw_price;

        if open_ctx.order_type.is_limit() {
            if price_tick > 0.0 {
                effective_price = align_price_floor(effective_price, price_tick);
            }
        }

        // 名义金额下限（minNotional），若有价格可用则按名义金额抬升数量
        let min_notional = self
            .min_qty_table
            .margin_min_notional_by_symbol(&self.symbol)
            .or_else(|| self.min_qty_table.spot_min_notional_by_symbol(&self.symbol))
            .unwrap_or(0.0);
        if min_notional > 0.0 && effective_price > 0.0 {
            let required_qty = min_notional / effective_price;
            if eff_qty < required_qty {
                let before = eff_qty;
                eff_qty = if step > 0.0 {
                    align_price_ceil(required_qty, step)
                } else {
                    required_qty
                };
                debug!(
                    "{}: strategy_id={} 名义金额对齐 min_notional={:.8} price={:.8} qty_up: {} -> {}",
                    Self::strategy_name(),
                    self.strategy_id,
                    min_notional,
                    effective_price,
                    before,
                    eff_qty
                );
            }
        }

        debug!(
            "{}: strategy_id={} 数量/名义金额对齐 raw_qty={:.8} step={:.8} min_qty={:.8} min_notional={:.8} price={:.8} aligned_qty={:.8}",
            Self::strategy_name(),
            self.strategy_id,
            raw_qty,
            step,
            min_qty,
            min_notional,
            effective_price,
            eff_qty
        );

        self.ensure_max_pos_u(
            &open_ctx.spot_symbol,
            &base_asset,
            current_spot_qty,
            eff_qty,
            effective_price,
        )?;

        let qty_str = format_quantity(eff_qty);
    }
}

pub struct PreTradeEnv {
    pub min_qty_table: std::rc::Rc<MinQtyTable>, //最小下单量维护，用于修证下单
    pub signal_tx: Option<UnboundedSender<Bytes>>, //预处理信号队列，对冲信号有些从策略发送，需要维护一份sender端作为copy
    pub order_record_tx: UnboundedSender<Bytes>, //推送订单记录到publish
    pub risk_checker: RiskChecker,
}

impl PreTradeEnv {
}
