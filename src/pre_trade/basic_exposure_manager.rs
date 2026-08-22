use std::collections::HashMap;

use log::debug;

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::{
    basic_balance_manager::BasicBalanceManager, basic_um_manager::BasicUmManager,
    price_table::PriceEntry, symbol_mapper::SymbolMapper,
};
use runtime_common::exchange::Exchange;

/// 单个资产维度的敞口信息（简化版）
#[derive(Debug, Clone)]
pub struct BasicExposureEntry {
    pub asset: String,    // 基础资产（如 "BTC"）
    pub balance: f64,     // 现货净头寸（wallet - borrowed - interest）
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

        let mut entries: HashMap<String, BasicExposureEntry> =
            HashMap::with_capacity(balance_mgrs.iter().map(|mgr| mgr.len()).sum());
        for mgr in balance_mgrs {
            for bal in mgr.balances_iter() {
                let entry = entries
                    .entry(bal.symbol.clone())
                    .or_insert_with_key(|asset| BasicExposureEntry {
                        asset: asset.clone(),
                        balance: 0.0,
                        borrowed: 0.0,
                        interest: 0.0,
                        um_position: 0.0,
                        exposure: 0.0,
                    });
                entry.balance += bal.net();
                entry.borrowed += bal.borrowed;
                entry.interest += bal.cumulative_interest;
            }
        }

        for (mgr, min_qty) in um_mgrs {
            for (symbol, net_contracts) in mgr.net_contracts_iter() {
                if net_contracts == 0.0 {
                    continue;
                }
                let Some(base_asset) = symbol_mapper.inst_id_to_base_asset(symbol) else {
                    continue;
                };
                let ct_mult = min_qty.contract_multiplier(symbol);
                let entry = entries
                    .entry(base_asset.clone())
                    .or_insert_with_key(|asset| BasicExposureEntry {
                        asset: asset.clone(),
                        balance: 0.0,
                        borrowed: 0.0,
                        interest: 0.0,
                        um_position: 0.0,
                        exposure: 0.0,
                    });
                entry.um_position += net_contracts as f64 * ct_mult;
            }
        }

        let mut exposures: Vec<BasicExposureEntry> = entries
            .into_values()
            .map(|mut entry| {
                entry.exposure = entry.balance + entry.um_position;
                entry
            })
            .collect();
        exposures.sort_unstable_by(|lhs, rhs| lhs.asset.cmp(&rhs.asset));
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
    pub fn revalue_with_price_lookup(&mut self, mut mark_price: impl FnMut(&str) -> Option<f64>) {
        let mut total_spot_value = 0.0;
        let mut total_borrowed_value = 0.0;
        let mut total_interest_value = 0.0;

        for entry in &self.exposures {
            let asset = entry.asset.to_uppercase();
            let symbol = self.symbol_mapper.asset_to_price_symbol(&asset);

            let mark = if asset == "USDT" {
                1.0
            } else {
                mark_price(&symbol).unwrap_or(0.0)
            };

            if mark == 0.0 {
                if asset != "USDT" && (entry.balance != 0.0 || entry.um_position != 0.0) {
                    debug!("缺少 {} 的标记价格, 估值按 0 处理", asset);
                }
                continue;
            }

            total_spot_value += entry.balance * mark;
            total_borrowed_value += entry.borrowed * mark;
            total_interest_value += entry.interest * mark;
        }

        self.total_equity = total_spot_value;
        self.total_borrowed_usd = total_borrowed_value;
        self.total_interest_usd = total_interest_value;

        debug!(
            "BasicExposureManager 重估完成: equity={:.2} borrowed={:.2} interest={:.2}",
            self.total_equity, self.total_borrowed_usd, self.total_interest_usd
        );
    }

    /// 基于标记价格重新估值，更新总权益等 USDT 计价字段
    pub fn revalue_with_prices(&mut self, price_map: &HashMap<String, PriceEntry>) {
        self.revalue_with_price_lookup(|symbol| price_map.get(symbol).map(|p| p.mark_price));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
    use mkt_parsers::msg::basic_account_msg::{
        BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg,
    };
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    #[test]
    fn revalue_uses_net_balance_directly() {
        let mut balance_mgr = BasicBalanceManager::new(Exchange::Binance);
        balance_mgr.apply_balance(&BasicBalanceMsg::create(1, "BTC".to_string(), 100.0));
        balance_mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
            1,
            "BTC".to_string(),
            30.0,
            2.0,
        ));

        let mut exposure_mgr =
            BasicExposureManager::new_from_sources(Exchange::Binance, &[&balance_mgr], &[]);
        let mut price_map = HashMap::new();
        price_map.insert(
            "BTCUSDT".to_string(),
            PriceEntry {
                symbol: "BTCUSDT".to_string(),
                mark_price: 10.0,
                index_price: 10.0,
                update_time: 1,
            },
        );

        exposure_mgr.revalue_with_prices(&price_map);

        assert!((exposure_mgr.total_equity() - 680.0).abs() < 1e-12);
        assert!((exposure_mgr.total_borrowed_usd() - 300.0).abs() < 1e-12);
        assert!((exposure_mgr.total_interest_usd() - 20.0).abs() < 1e-12);
    }

    fn install_fake_exposure_sources(
        assets: usize,
    ) -> (BasicBalanceManager, BasicUmManager, MinQtyTable) {
        let mut balance_mgr = BasicBalanceManager::new(Exchange::Binance);
        let mut um_mgr = BasicUmManager::new(Exchange::Binance);
        let min_qty_table = MinQtyTable::new(Exchange::Binance);

        for idx in 0..assets {
            let asset = format!("T{idx:04}");
            let symbol = format!("{asset}USDT");
            balance_mgr.apply_balance(&BasicBalanceMsg::create(
                idx as i64,
                asset.clone(),
                1.0 + (idx % 17) as f64,
            ));
            if idx % 5 == 0 {
                balance_mgr.apply_borrow_interest(&BasicBorrowInterestMsg::create(
                    idx as i64,
                    asset.clone(),
                    0.1,
                    0.01,
                ));
            }

            let side = if idx % 2 == 0 { 'L' } else { 'S' };
            um_mgr.apply_position(&BasicPositionMsg::create(
                idx as i64,
                symbol,
                side,
                0.5 + (idx % 11) as f32,
            ));
        }

        (balance_mgr, um_mgr, min_qty_table)
    }

    fn exposure_compute_bench_once(assets: usize, iterations: usize) {
        let (balance_mgr, um_mgr, min_qty_table) = install_fake_exposure_sources(assets);
        let balance_mgrs = [&balance_mgr];
        let um_mgrs = [(&um_mgr, &min_qty_table)];

        for _ in 0..100 {
            let exposures = BasicExposureManager::compute_exposures_for_exchange(
                Exchange::Binance,
                &balance_mgrs,
                &um_mgrs,
            );
            black_box(exposures);
        }

        let mut samples = Vec::with_capacity(iterations);
        let mut total = Duration::ZERO;
        let mut exposure_count = 0usize;
        for _ in 0..iterations {
            let start = Instant::now();
            let exposures = BasicExposureManager::compute_exposures_for_exchange(
                Exchange::Binance,
                &balance_mgrs,
                &um_mgrs,
            );
            let elapsed = start.elapsed();
            exposure_count = exposures.len();
            black_box(exposures);
            total += elapsed;
            samples.push(elapsed.as_nanos());
        }
        samples.sort_unstable();

        let percentile = |pct: usize| -> u128 {
            let idx = ((samples.len() - 1) * pct) / 100;
            samples[idx]
        };
        let avg_ns = total.as_nanos() / iterations as u128;
        println!(
            "exposure_compute_bench assets={} exposures={} iterations={} avg={}ns p50={}ns p95={}ns p99={}ns min={}ns max={}ns",
            assets,
            exposure_count,
            iterations,
            avg_ns,
            percentile(50),
            percentile(95),
            percentile(99),
            samples[0],
            samples[samples.len() - 1],
        );
    }

    #[test]
    #[ignore = "micro-benchmark; run with --ignored --nocapture"]
    fn bench_compute_exposures_fake_data() {
        for (assets, iterations) in [(20, 20_000), (100, 10_000), (300, 5_000), (1_000, 1_000)] {
            exposure_compute_bench_once(assets, iterations);
        }
    }
}
