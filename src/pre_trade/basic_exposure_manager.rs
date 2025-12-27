use std::collections::{BTreeMap, BTreeSet, HashMap};

use log::debug;

use crate::common::{exchange::Exchange, min_qty_table::MinQtyTable};
use crate::pre_trade::{
    basic_balance_manager::BasicBalanceManager, basic_um_manager::BasicUmManager,
    net_position::NetPosition, price_table::PriceEntry, symbol_mapper::SymbolMapper,
};

/// 单个资产维度的敞口信息（简化版）
#[derive(Debug, Clone)]
pub struct BasicExposureEntry {
    pub asset: String,    // 基础资产（如 "BTC"）
    pub balance: f64,     // 现货余额（净值 = 余额 - 借币 - 利息）
    pub borrowed: f64,    // 借币数量
    pub interest: f64,    // 累计利息
    pub um_position: f64, // UM 持仓（标的资产数量，已考虑合约乘数）
    pub exposure: f64,    // 净敞口 = balance + um_position
}

/// 敞口管理器（简化版），负责汇总 balance 与 UM 持仓的资产敞口
#[derive(Debug)]
pub struct BasicExposureManager {
    exchange: Exchange,
    symbol_mapper: Box<dyn SymbolMapper>,
    exposures: Vec<BasicExposureEntry>,
    total_equity: f64,
    total_exposure: f64,
    total_borrowed_usd: f64,
    total_interest_usd: f64,
}

impl BasicExposureManager {
    /// 计算某个交易所（exchange）的敞口快照。
    ///
    /// - `balance_mgrs`: 现货/保证金的基础余额管理器列表（可以为空）
    /// - `um_mgrs`: U 本位合约持仓管理器与对应的 min_qty_table 列表（可以为空）
    pub fn compute_exposures_for_exchange(
        exchange: Exchange,
        balance_mgrs: &[&BasicBalanceManager],
        um_mgrs: &[(&BasicUmManager, &MinQtyTable)],
    ) -> Vec<BasicExposureEntry> {
        let symbol_mapper = crate::pre_trade::symbol_mapper::create_symbol_mapper(exchange);

        // 收集所有资产（balance 和 um）
        let mut assets: BTreeSet<String> = BTreeSet::new();

        for mgr in balance_mgrs {
            for bal in mgr.snapshot() {
                assets.insert(bal.symbol.to_uppercase());
            }
        }

        for (mgr, _) in um_mgrs {
            for pos in mgr.snapshot() {
                if let Some(base_asset) = symbol_mapper.inst_id_to_base_asset(&pos.inst_id) {
                    assets.insert(base_asset);
                }
            }
        }

        let mut exposures: Vec<BasicExposureEntry> = assets
            .into_iter()
            .map(|asset| {
                // 现货：净头寸（base qty）+ 借币/利息
                let mut balance_pos = 0.0_f64;
                let mut borrowed = 0.0_f64;
                let mut interest = 0.0_f64;
                for mgr in balance_mgrs {
                    balance_pos += mgr.net_position(&asset, None);
                    if let Some(b) = mgr.get(&asset) {
                        borrowed += b.borrowed;
                        interest += b.cumulative_interest;
                    }
                }

                // 合约：折算成标的数量（base qty）
                let um_symbol = symbol_mapper.balance_asset_to_um_symbol(&asset);
                let mut um_position = 0.0_f64;
                for (mgr, min_qty) in um_mgrs {
                    um_position += mgr.net_position(&um_symbol, Some(min_qty));
                }

                BasicExposureEntry {
                    asset,
                    balance: balance_pos,
                    borrowed,
                    interest,
                    um_position,
                    exposure: balance_pos + um_position,
                }
            })
            .collect();

        exposures.sort_by(|a, b| a.asset.cmp(&b.asset));
        exposures
    }

    /// 创建新的敞口管理器（支持空的 balance/um 输入）
    pub fn new_from_sources(
        exchange: Exchange,
        balance_mgrs: &[&BasicBalanceManager],
        um_mgrs: &[(&BasicUmManager, &MinQtyTable)],
    ) -> Self {
        let symbol_mapper = crate::pre_trade::symbol_mapper::create_symbol_mapper(exchange);
        let exposures = Self::compute_exposures_for_exchange(exchange, balance_mgrs, um_mgrs);
        let total_exposure = exposures.iter().map(|e| e.exposure.abs()).sum();

        debug!(
            "BasicExposureManager 初始化: 资产数={}, 总敞口(数量)={:.6}",
            exposures.len(),
            total_exposure
        );

        Self {
            exchange,
            symbol_mapper,
            exposures,
            total_equity: 0.0,
            total_exposure,
            total_borrowed_usd: 0.0,
            total_interest_usd: 0.0,
        }
    }

    /// 创建新的敞口管理器（旧接口）
    pub fn new(
        exchange: Exchange,
        balance_mgr: &BasicBalanceManager,
        um_mgr: &BasicUmManager,
        min_qty_table: &MinQtyTable,
    ) -> Self {
        Self::new_from_sources(
            exchange,
            std::slice::from_ref(&balance_mgr),
            std::slice::from_ref(&(um_mgr, min_qty_table)),
        )
    }

    /// 重新计算敞口，返回是否发生变更
    pub fn recompute(
        &mut self,
        balance_mgr: &BasicBalanceManager,
        um_mgr: &BasicUmManager,
        min_qty_table: &MinQtyTable,
    ) -> bool {
        // manager 本身是按 exchange 初始化的，recompute 直接沿用 exchange 口径即可
        let new_exposures = Self::compute_exposures_for_exchange(
            self.exchange,
            std::slice::from_ref(&balance_mgr),
            std::slice::from_ref(&(um_mgr, min_qty_table)),
        );
        let changed = Self::positions_changed(&self.exposures, &new_exposures);

        if changed {
            debug!(
                "BasicExposureManager 检测到变更: 资产数={} -> {}",
                self.exposures.len(),
                new_exposures.len()
            );
        }

        self.exposures = new_exposures;
        self.total_exposure = self.exposures.iter().map(|e| e.exposure.abs()).sum();

        changed
    }

    /// 基于标记价格重新估值，更新总权益等 USDT 计价字段
    pub fn revalue_with_prices(&mut self, price_map: &BTreeMap<String, PriceEntry>) {
        let mut total_spot_value = 0.0;
        let mut total_borrowed_value = 0.0;
        let mut total_interest_value = 0.0;

        for entry in &self.exposures {
            let asset = entry.asset.to_uppercase();
            let symbol = self.symbol_mapper.asset_to_price_symbol(&asset);

            let mark = if asset == "USDT" {
                1.0
            } else {
                price_map.get(&symbol).map(|p| p.mark_price).unwrap_or(0.0)
            };

            if mark == 0.0 {
                if asset != "USDT" && (entry.balance != 0.0 || entry.um_position != 0.0) {
                    debug!("缺少 {} 的标记价格, 估值按 0 处理", asset);
                }
                continue;
            }

            // balance 是净值（已扣除借币和利息），需要加回利息单独统计
            let spot_value = (entry.balance + entry.interest) * mark;
            let borrowed_value = entry.borrowed * mark;
            let interest_value = entry.interest * mark;

            total_spot_value += spot_value;
            total_borrowed_value += borrowed_value;
            total_interest_value += interest_value;
        }

        // 总权益 = spot 估值 - 利息（负债已在 balance 中扣除）
        self.total_equity = total_spot_value - total_interest_value;
        self.total_borrowed_usd = total_borrowed_value;
        self.total_interest_usd = total_interest_value;

        debug!(
            "BasicExposureManager 重估完成: equity={:.2} borrowed={:.2} interest={:.2}",
            self.total_equity, self.total_borrowed_usd, self.total_interest_usd
        );
    }

    /// 根据资产名称查找敞口信息
    pub fn exposure_for_asset(&self, asset: &str) -> Option<&BasicExposureEntry> {
        self.exposures
            .iter()
            .find(|e| e.asset.eq_ignore_ascii_case(asset))
    }

    /// 返回所有敞口快照
    pub fn exposures(&self) -> &[BasicExposureEntry] {
        &self.exposures
    }

    /// 返回总权益（USDT 计价）
    pub fn total_equity(&self) -> f64 {
        self.total_equity
    }

    /// 返回总敞口绝对值（数量）
    pub fn total_exposure(&self) -> f64 {
        self.total_exposure
    }

    /// 返回总借币估值（USDT）
    pub fn total_borrowed_usd(&self) -> f64 {
        self.total_borrowed_usd
    }

    /// 返回总利息估值（USDT）
    pub fn total_interest_usd(&self) -> f64 {
        self.total_interest_usd
    }

    /// 判断持仓是否发生变更
    fn positions_changed(prev: &[BasicExposureEntry], next: &[BasicExposureEntry]) -> bool {
        let mut prev_map: HashMap<String, (f64, f64)> = HashMap::new();
        for entry in prev {
            prev_map.insert(
                entry.asset.to_uppercase(),
                (entry.balance, entry.um_position),
            );
        }

        for entry in next {
            let key = entry.asset.to_uppercase();
            let current = (entry.balance, entry.um_position);
            match prev_map.remove(&key) {
                Some(prev_vals) => {
                    if !Self::amount_approx_eq(current.0, prev_vals.0)
                        || !Self::amount_approx_eq(current.1, prev_vals.1)
                    {
                        return true;
                    }
                }
                None => {
                    if !Self::amount_approx_eq(current.0, 0.0)
                        || !Self::amount_approx_eq(current.1, 0.0)
                    {
                        return true;
                    }
                }
            }
        }

        // 检查是否有资产被移除
        for (_, (balance, um)) in prev_map.into_iter() {
            if !Self::amount_approx_eq(balance, 0.0) || !Self::amount_approx_eq(um, 0.0) {
                return true;
            }
        }

        false
    }

    /// 浮点数近似相等判断
    fn amount_approx_eq(lhs: f64, rhs: f64) -> bool {
        const EPS_ABS: f64 = 1e-9;
        const EPS_REL: f64 = 1e-8;
        let diff = (lhs - rhs).abs();
        if diff <= EPS_ABS {
            return true;
        }
        let scale = lhs.abs().max(rhs.abs()).max(1.0);
        diff <= EPS_REL * scale
    }
}
