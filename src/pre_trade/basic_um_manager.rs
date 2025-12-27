use std::collections::HashMap;

use crate::common::{
    basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg},
    exchange::Exchange,
    min_qty_table::MinQtyTable,
};
use crate::pre_trade::net_position::NetPosition;

/// 最小化的 U 本位持仓管理器：仅维护 inst_id、side、持仓量、时间戳。
#[derive(Debug, Clone)]
pub struct BasicUmPosition {
    pub exchange: Exchange,
    pub inst_id: String,
    pub side: char, // 'L' / 'S' / 'N'
    pub amount: f32,
    pub timestamp: i64, // uTime
    /// 合约未实现盈亏（USDT 计价），当前未填充时为 0
    pub unrealized_pnl_usdt: f64,
}

#[derive(Debug, Clone)]
pub struct BasicUmManager {
    exchange: Exchange,
    positions: HashMap<String, BasicUmPosition>, // key: inst_id|side
}

impl BasicUmManager {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            positions: HashMap::new(),
        }
    }

    /// 应用持仓消息：按 inst_id+side 覆盖当前仓位。
    pub fn apply_position(&mut self, msg: &BasicPositionMsg) {
        let inst = msg.inst_id().to_string();
        let side = msg.position_side();
        let key = format!("{}|{}", inst, side);

        let entry = self
            .positions
            .entry(key)
            .or_insert_with(|| BasicUmPosition {
                exchange: self.exchange,
                inst_id: inst.clone(),
                side,
                amount: 0.0,
                timestamp: msg.timestamp(),
                unrealized_pnl_usdt: 0.0,
            });

        entry.exchange = self.exchange;
        entry.inst_id = inst;
        entry.side = side;
        entry.amount = msg.position_amount;
        entry.timestamp = msg.timestamp();
    }

    /// 应用未实现盈亏消息：按 inst_id+side 覆盖当前值。
    pub fn apply_unrealized_pnl(&mut self, msg: &BasicUmUnrealizedMsg) {
        let inst = msg.inst_id.clone();
        let side = msg.position_side;
        let key = format!("{}|{}", inst, side);

        let entry = self
            .positions
            .entry(key)
            .or_insert_with(|| BasicUmPosition {
                exchange: self.exchange,
                inst_id: inst.clone(),
                side,
                amount: 0.0,
                timestamp: msg.timestamp,
                unrealized_pnl_usdt: 0.0,
            });

        entry.exchange = self.exchange;
        entry.inst_id = inst;
        entry.side = side;
        entry.timestamp = msg.timestamp;
        entry.unrealized_pnl_usdt = msg.unrealized_pnl;
    }

    /// 获取单个 inst_id + side 的持仓。
    pub fn get(&self, inst_id: &str, side: char) -> Option<&BasicUmPosition> {
        let key = format!("{}|{}", inst_id, side);
        self.positions.get(&key)
    }

    /// 返回当前全部持仓的快照副本。
    pub fn snapshot(&self) -> Vec<BasicUmPosition> {
        self.positions.values().cloned().collect()
    }

    /// 汇总合约未实现盈亏（USDT 计价）。
    pub fn total_unrealized_pnl_usdt(&self) -> f64 {
        self.positions
            .values()
            .map(|p| p.unrealized_pnl_usdt)
            .sum()
    }

    /// 计算指定 inst_id 的净持仓张数（long - short）
    fn net_contracts(&self, inst_id: &str) -> f32 {
        let long_key = format!("{}|L", inst_id);
        let short_key = format!("{}|S", inst_id);
        let net_key = format!("{}|N", inst_id);

        let long_amount = self
            .positions
            .get(&long_key)
            .map(|p| p.amount)
            .unwrap_or(0.0);
        let short_amount = self
            .positions
            .get(&short_key)
            .map(|p| p.amount)
            .unwrap_or(0.0);
        let net_amount = self
            .positions
            .get(&net_key)
            .map(|p| p.amount)
            .unwrap_or(0.0);

        // 对于 hedge 模式：long - short；对于 net 模式：直接取 net_amount
        if long_amount != 0.0 || short_amount != 0.0 {
            long_amount - short_amount
        } else {
            net_amount
        }
    }
}

impl NetPosition for BasicUmManager {
    fn net_position(&self, symbol: &str, min_qty_table: Option<&MinQtyTable>) -> f64 {
        let symbol_normalized = symbol.to_uppercase().replace("-SWAP", "").replace('-', "");

        // 对于 OKX，需要将 symbol 映射回 inst_id 进行查找
        // 这里我们遍历所有持仓，找到匹配的 inst_id
        let mut net_contracts = 0.0f32;

        // 收集所有唯一的 inst_id
        let mut inst_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for pos in self.positions.values() {
            inst_ids.insert(pos.inst_id.clone());
        }

        // 查找匹配 symbol 的 inst_id
        for inst_id in inst_ids {
            let pos_symbol = match self.exchange {
                Exchange::Okex => {
                    // BTC-USDT-SWAP -> BTCUSDT
                    inst_id.replace("-SWAP", "").replace('-', "")
                }
                _ => inst_id.clone(),
            };

            if pos_symbol.to_uppercase() == symbol_normalized {
                net_contracts += self.net_contracts(&inst_id);
            }
        }

        // 获取合约乘数
        let ct_mult = min_qty_table
            .map(|t| t.contract_multiplier(symbol))
            .unwrap_or(1.0);

        // 返回标的资产数量 = 净张数 × 合约乘数
        (net_contracts as f64) * ct_mult
    }
}
