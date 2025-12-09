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
    pub asset: String,      // 基础资产（如 "BTC"）
    pub balance: f64,       // 现货余额（净值 = 余额 - 借币 - 利息）
    pub borrowed: f64,      // 借币数量
    pub interest: f64,      // 累计利息
    pub um_position: f64,   // UM 持仓（标的资产数量，已考虑合约乘数）
    pub exposure: f64,      // 净敞口 = balance + um_position
}

/// 敞口管理器（简化版），负责汇总 balance 与 UM 持仓的资产敞口
#[derive(Debug)]
pub struct BasicExposureManager {
    symbol_mapper: Box<dyn SymbolMapper>,
    exposures: Vec<BasicExposureEntry>,
    total_equity: f64,
    total_exposure: f64,
    total_borrowed_usd: f64,
    total_interest_usd: f64,
}

impl BasicExposureManager {
    /// 创建新的敞口管理器
    ///
    /// # 参数
    /// - `exchange`: 交易所
    /// - `balance_mgr`: 现货余额管理器
    /// - `um_mgr`: UM 持仓管理器
    /// - `min_qty_table`: 用于获取合约乘数
    pub fn new(
        exchange: Exchange,
        balance_mgr: &BasicBalanceManager,
        um_mgr: &BasicUmManager,
        min_qty_table: &MinQtyTable,
    ) -> Self {
        let symbol_mapper = crate::pre_trade::symbol_mapper::create_symbol_mapper(exchange);
        let exposures = Self::compute_exposures(
            balance_mgr,
            um_mgr,
            min_qty_table,
            symbol_mapper.as_ref(),
        );
        let total_exposure = exposures.iter().map(|e| e.exposure.abs()).sum();

        debug!(
            "BasicExposureManager 初始化: 资产数={}, 总敞口(数量)={:.6}",
            exposures.len(),
            total_exposure
        );

        Self {
            symbol_mapper,
            exposures,
            total_equity: 0.0,
            total_exposure,
            total_borrowed_usd: 0.0,
            total_interest_usd: 0.0,
        }
    }

    /// 重新计算敞口，返回是否发生变更
    pub fn recompute(
        &mut self,
        balance_mgr: &BasicBalanceManager,
        um_mgr: &BasicUmManager,
        min_qty_table: &MinQtyTable,
    ) -> bool {
        let new_exposures = Self::compute_exposures(
            balance_mgr,
            um_mgr,
            min_qty_table,
            self.symbol_mapper.as_ref(),
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

    /// 计算敞口列表
    fn compute_exposures(
        balance_mgr: &BasicBalanceManager,
        um_mgr: &BasicUmManager,
        min_qty_table: &MinQtyTable,
        symbol_mapper: &dyn SymbolMapper,
    ) -> Vec<BasicExposureEntry> {
        // 收集所有资产（balance 和 um）
        let mut assets: BTreeSet<String> = BTreeSet::new();

        // 从 balance 收集
        for bal in balance_mgr.snapshot() {
            assets.insert(bal.symbol.to_uppercase());
        }

        // 从 UM 持仓收集（提取基础资产）
        for pos in um_mgr.snapshot() {
            if let Some(base_asset) = symbol_mapper.inst_id_to_base_asset(&pos.inst_id) {
                assets.insert(base_asset);
            }
        }

        // 为每个资产创建 ExposureEntry
        let mut exposures: Vec<BasicExposureEntry> = assets
            .into_iter()
            .map(|asset| {
                // 查询 balance（直接用资产名）
                let balance_pos = balance_mgr.net_position(&asset, None);
                let balance_data = balance_mgr.get(&asset);
                let borrowed = balance_data.map(|b| b.borrowed).unwrap_or(0.0);
                let interest = balance_data.map(|b| b.cumulative_interest).unwrap_or(0.0);

                // 查询 UM 持仓（使用 symbol_mapper 转换）
                let um_symbol = symbol_mapper.balance_asset_to_um_symbol(&asset);
                let um_position = um_mgr.net_position(&um_symbol, Some(min_qty_table));

                // 计算净敞口
                let exposure = balance_pos + um_position;

                BasicExposureEntry {
                    asset,
                    balance: balance_pos,
                    borrowed,
                    interest,
                    um_position,
                    exposure,
                }
            })
            .collect();

        // 按资产名称排序
        exposures.sort_by(|a, b| a.asset.cmp(&b.asset));

        exposures
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
