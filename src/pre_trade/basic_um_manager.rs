use std::collections::{HashMap, HashSet};

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::net_position::NetPosition;
use mkt_parsers::msg::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
use runtime_common::exchange::Exchange;

/// 最小化的 U 本位持仓管理器：仅维护 inst_id、side、持仓量、时间戳。
#[derive(Debug, Clone)]
pub struct BasicUmPosition {
    pub exchange: Exchange,
    pub inst_id: String,
    pub side: char, // 'L' / 'S' / 'N'
    pub amount: f32,
    pub timestamp: i64, // position uTime
    pub pnl_timestamp: i64,
    /// 合约未实现盈亏（USDT 计价），当前未填充时为 0
    pub unrealized_pnl_usdt: f64,
}

#[derive(Debug, Clone)]
pub struct BasicUmManager {
    exchange: Exchange,
    positions: HashMap<String, BasicUmPosition>, // key: inst_id|side
    position_timestamps: HashMap<String, i64>,
    pnl_timestamps: HashMap<String, i64>,
    net_contracts_by_symbol: HashMap<String, f32>,
}

impl BasicUmManager {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            positions: HashMap::new(),
            position_timestamps: HashMap::new(),
            pnl_timestamps: HashMap::new(),
            net_contracts_by_symbol: HashMap::new(),
        }
    }

    /// 应用持仓消息：按 inst_id+side 覆盖当前仓位。
    pub fn apply_position(&mut self, msg: &BasicPositionMsg) {
        let inst = msg.inst_id().to_string();
        let side = msg.position_side();
        let key = format!("{}|{}", inst, side);
        let msg_ts = msg.timestamp();

        if self
            .position_timestamps
            .get(&key)
            .map(|last_ts| msg_ts < *last_ts)
            .unwrap_or(false)
        {
            return;
        }
        self.position_timestamps.insert(key.clone(), msg_ts);

        if msg.position_amount == 0.0 {
            let mut should_remove = false;
            if let Some(entry) = self.positions.get_mut(&key) {
                entry.timestamp = msg_ts;
                if entry.unrealized_pnl_usdt == 0.0 {
                    should_remove = true;
                } else {
                    entry.amount = 0.0;
                }
            }
            if should_remove {
                self.positions.remove(&key);
            }
            self.refresh_net_contracts_for_inst(&inst);
            return;
        }

        let entry = self
            .positions
            .entry(key)
            .or_insert_with(|| BasicUmPosition {
                exchange: self.exchange,
                inst_id: inst.clone(),
                side,
                amount: 0.0,
                timestamp: msg_ts,
                pnl_timestamp: 0,
                unrealized_pnl_usdt: 0.0,
            });

        entry.exchange = self.exchange;
        entry.inst_id = inst.clone();
        entry.side = side;
        entry.amount = msg.position_amount;
        entry.timestamp = msg_ts;
        self.refresh_net_contracts_for_inst(&inst);
    }

    /// 应用未实现盈亏消息：按 inst_id+side 覆盖当前值。
    pub fn apply_unrealized_pnl(&mut self, msg: &BasicUmUnrealizedMsg) {
        let inst = msg.inst_id.clone();
        let side = msg.position_side;
        let key = format!("{}|{}", inst, side);
        let msg_ts = msg.timestamp;

        if self
            .pnl_timestamps
            .get(&key)
            .map(|last_ts| msg_ts < *last_ts)
            .unwrap_or(false)
        {
            return;
        }
        self.pnl_timestamps.insert(key.clone(), msg_ts);

        if msg.unrealized_pnl == 0.0 {
            let mut should_remove = false;
            if let Some(entry) = self.positions.get_mut(&key) {
                entry.pnl_timestamp = msg_ts;
                if entry.amount == 0.0 {
                    should_remove = true;
                } else {
                    entry.unrealized_pnl_usdt = 0.0;
                }
            }
            if should_remove {
                self.positions.remove(&key);
            }
            return;
        }

        let entry = self
            .positions
            .entry(key)
            .or_insert_with(|| BasicUmPosition {
                exchange: self.exchange,
                inst_id: inst.clone(),
                side,
                amount: 0.0,
                timestamp: 0,
                pnl_timestamp: msg_ts,
                unrealized_pnl_usdt: 0.0,
            });

        entry.exchange = self.exchange;
        entry.inst_id = inst;
        entry.side = side;
        entry.pnl_timestamp = msg_ts;
        entry.unrealized_pnl_usdt = msg.unrealized_pnl;
    }

    /// 获取单个 inst_id + side 的持仓。
    pub fn get(&self, inst_id: &str, side: char) -> Option<&BasicUmPosition> {
        let key = format!("{}|{}", inst_id, side);
        self.positions.get(&key)
    }

    /// 返回当前全部持仓的快照副本。
    pub fn snapshot(&self) -> Vec<BasicUmPosition> {
        self.positions_iter().cloned().collect()
    }

    /// 返回当前全部持仓的只读迭代器，避免只读汇总路径 clone 整张表。
    pub fn positions_iter(&self) -> impl Iterator<Item = &BasicUmPosition> {
        self.positions.values()
    }

    /// 返回已缓存的 symbol 净张数，用于只读汇总路径避免按资产反查和重复归一化。
    pub fn net_contracts_iter(&self) -> impl Iterator<Item = (&str, f32)> {
        self.net_contracts_by_symbol
            .iter()
            .map(|(symbol, contracts)| (symbol.as_str(), *contracts))
    }

    /// 清空当前维护的全部 UM 持仓状态。
    pub fn clear(&mut self) {
        self.positions.clear();
        self.position_timestamps.clear();
        self.pnl_timestamps.clear();
        self.net_contracts_by_symbol.clear();
    }

    /// 汇总合约未实现盈亏（USDT 计价）。
    pub fn total_unrealized_pnl_usdt(&self) -> f64 {
        self.positions.values().map(|p| p.unrealized_pnl_usdt).sum()
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

    fn normalized_symbol_for_inst(&self, inst_id: &str) -> String {
        match self.exchange {
            Exchange::Okex => inst_id
                .replace("-SWAP", "")
                .replace('-', "")
                .to_ascii_uppercase(),
            _ => inst_id.to_ascii_uppercase(),
        }
    }

    fn normalized_symbol(symbol: &str) -> String {
        symbol
            .to_ascii_uppercase()
            .replace("-SWAP", "")
            .replace('-', "")
    }

    fn normalized_symbol_key(symbol: &str) -> std::borrow::Cow<'_, str> {
        if !symbol.is_empty()
            && !symbol.ends_with("SWAP")
            && symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            std::borrow::Cow::Borrowed(symbol)
        } else {
            std::borrow::Cow::Owned(Self::normalized_symbol(symbol))
        }
    }

    fn refresh_net_contracts_for_inst(&mut self, inst_id: &str) {
        let symbol = self.normalized_symbol_for_inst(inst_id);
        let mut inst_ids = HashSet::new();
        for pos in self.positions.values() {
            if self.normalized_symbol_for_inst(&pos.inst_id) == symbol {
                inst_ids.insert(pos.inst_id.clone());
            }
        }
        let net_contracts = inst_ids
            .iter()
            .map(|inst_id| self.net_contracts(inst_id))
            .sum::<f32>();
        if net_contracts == 0.0 {
            self.net_contracts_by_symbol.remove(&symbol);
        } else {
            self.net_contracts_by_symbol.insert(symbol, net_contracts);
        }
    }
}

impl NetPosition for BasicUmManager {
    fn net_position(&self, symbol: &str, min_qty_table: Option<&MinQtyTable>) -> f64 {
        let symbol_normalized = Self::normalized_symbol_key(symbol);
        let net_contracts = self
            .net_contracts_by_symbol
            .get(symbol_normalized.as_ref())
            .copied()
            .unwrap_or(0.0);

        // 获取合约乘数
        let ct_mult = min_qty_table
            .map(|t| t.contract_multiplier(symbol))
            .unwrap_or(1.0);

        // 返回标的资产数量 = 净张数 × 合约乘数
        (net_contracts as f64) * ct_mult
    }
}

#[cfg(test)]
mod tests {
    use super::BasicUmManager;
    use mkt_parsers::msg::basic_account_msg::{BasicPositionMsg, BasicUmUnrealizedMsg};
    use runtime_common::exchange::Exchange;

    #[test]
    fn removes_entry_after_zero_position_and_zero_pnl() {
        let mut mgr = BasicUmManager::new(Exchange::Bybit);
        mgr.apply_position(&BasicPositionMsg::create(
            1,
            "BTCUSDT".to_string(),
            'L',
            1.5,
        ));
        mgr.apply_unrealized_pnl(&BasicUmUnrealizedMsg::create(
            2,
            "BTCUSDT".to_string(),
            'L',
            12.0,
        ));

        mgr.apply_position(&BasicPositionMsg::create(
            3,
            "BTCUSDT".to_string(),
            'L',
            0.0,
        ));
        assert!(mgr.get("BTCUSDT", 'L').is_some());

        mgr.apply_unrealized_pnl(&BasicUmUnrealizedMsg::create(
            4,
            "BTCUSDT".to_string(),
            'L',
            0.0,
        ));
        assert!(mgr.get("BTCUSDT", 'L').is_none());
    }

    #[test]
    fn zero_position_without_existing_entry_is_ignored() {
        let mut mgr = BasicUmManager::new(Exchange::Bybit);
        mgr.apply_position(&BasicPositionMsg::create(
            1,
            "ETHUSDT".to_string(),
            'S',
            0.0,
        ));
        assert!(mgr.snapshot().is_empty());
    }

    #[test]
    fn stale_position_does_not_recreate_after_zero_cleanup() {
        let mut mgr = BasicUmManager::new(Exchange::Okex);
        mgr.apply_position(&BasicPositionMsg::create(
            20,
            "XTZ-USDT-SWAP".to_string(),
            'N',
            -320.0,
        ));
        mgr.apply_position(&BasicPositionMsg::create(
            30,
            "XTZ-USDT-SWAP".to_string(),
            'N',
            0.0,
        ));
        assert!(mgr.get("XTZ-USDT-SWAP", 'N').is_none());

        mgr.apply_position(&BasicPositionMsg::create(
            10,
            "XTZ-USDT-SWAP".to_string(),
            'N',
            -320.0,
        ));
        assert!(mgr.get("XTZ-USDT-SWAP", 'N').is_none());
    }

    #[test]
    fn stale_unrealized_pnl_does_not_override_newer_zero() {
        let mut mgr = BasicUmManager::new(Exchange::Okex);
        mgr.apply_position(&BasicPositionMsg::create(
            20,
            "BTC-USDT-SWAP".to_string(),
            'N',
            2.0,
        ));
        mgr.apply_unrealized_pnl(&BasicUmUnrealizedMsg::create(
            20,
            "BTC-USDT-SWAP".to_string(),
            'N',
            -12.0,
        ));
        mgr.apply_unrealized_pnl(&BasicUmUnrealizedMsg::create(
            30,
            "BTC-USDT-SWAP".to_string(),
            'N',
            0.0,
        ));
        mgr.apply_unrealized_pnl(&BasicUmUnrealizedMsg::create(
            10,
            "BTC-USDT-SWAP".to_string(),
            'N',
            -12.0,
        ));
        assert_eq!(
            mgr.get("BTC-USDT-SWAP", 'N').unwrap().unrealized_pnl_usdt,
            0.0
        );
    }

    #[test]
    fn clear_removes_all_positions() {
        let mut mgr = BasicUmManager::new(Exchange::Bybit);
        mgr.apply_position(&BasicPositionMsg::create(
            1,
            "BTCUSDT".to_string(),
            'L',
            1.0,
        ));
        mgr.apply_position(&BasicPositionMsg::create(
            2,
            "ETHUSDT".to_string(),
            'S',
            2.0,
        ));

        mgr.clear();

        assert!(mgr.snapshot().is_empty());
    }

    #[test]
    fn net_position_cache_updates_with_position_changes() {
        use crate::pre_trade::net_position::NetPosition;

        let mut mgr = BasicUmManager::new(Exchange::Bybit);
        mgr.apply_position(&BasicPositionMsg::create(
            1,
            "BTCUSDT".to_string(),
            'L',
            2.5,
        ));
        mgr.apply_position(&BasicPositionMsg::create(
            2,
            "BTCUSDT".to_string(),
            'S',
            1.0,
        ));
        assert_eq!(mgr.net_position("BTCUSDT", None), 1.5);

        mgr.apply_position(&BasicPositionMsg::create(
            3,
            "BTCUSDT".to_string(),
            'L',
            0.0,
        ));
        assert_eq!(mgr.net_position("BTCUSDT", None), -1.0);

        mgr.apply_position(&BasicPositionMsg::create(
            4,
            "BTCUSDT".to_string(),
            'S',
            0.0,
        ));
        assert_eq!(mgr.net_position("BTCUSDT", None), 0.0);
    }
}
