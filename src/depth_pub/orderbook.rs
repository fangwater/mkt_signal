//! 订单簿维护模块
//!
//! 使用 BTreeMap 维护每个 symbol 的买卖订单簿

use std::collections::BTreeMap;

/// 单边订单簿 (买或卖)
/// - 买方: 价格降序 (最高价在前)
/// - 卖方: 价格升序 (最低价在前)
#[derive(Debug, Clone, Default)]
pub struct OrderBookSide {
    /// 价格 -> (数量, update_id) 的映射
    /// 使用 i64 存储价格 (价格 * 1e8 转整数，避免浮点精度问题)
    /// 每个档位记录 update_id，只有更新的 update_id 更大时才会覆盖
    levels: BTreeMap<i64, (f64, i64)>,
}

impl OrderBookSide {
    pub fn new() -> Self {
        Self {
            levels: BTreeMap::new(),
        }
    }

    /// 更新价格档位
    /// - amount > 0: 更新或新增（只有当 update_id > 当前档位的 update_id 时才更新）
    /// - amount == 0: 删除该档位（只有当 update_id > 当前档位的 update_id 时才删除）
    pub fn update(&mut self, price: f64, amount: f64, update_id: i64) {
        let price_key = price_to_key(price);

        // 检查是否需要更新：只有新的 update_id 大于当前档位的 update_id 时才更新
        if let Some(&(_, existing_update_id)) = self.levels.get(&price_key) {
            if update_id <= existing_update_id {
                // 旧消息，跳过
                return;
            }
        }

        if amount == 0.0 {
            self.levels.remove(&price_key);
        } else {
            self.levels.insert(price_key, (amount, update_id));
        }
    }

    /// 清空订单簿
    pub fn clear(&mut self) {
        self.levels.clear();
    }

    /// 获取最优 N 档 (买方: 最高价优先; 卖方: 最低价优先)
    /// is_bid: true 表示买方 (降序), false 表示卖方 (升序)
    /// 返回 (price, amount)，不包含 update_id
    pub fn top_n(&self, n: usize, is_bid: bool) -> Vec<(f64, f64)> {
        let mut result = Vec::with_capacity(n);

        if is_bid {
            // 买方: 价格降序 (从高到低)
            for (&price_key, &(amount, _)) in self.levels.iter().rev().take(n) {
                result.push((key_to_price(price_key), amount));
            }
        } else {
            // 卖方: 价格升序 (从低到高)
            for (&price_key, &(amount, _)) in self.levels.iter().take(n) {
                result.push((key_to_price(price_key), amount));
            }
        }

        result
    }

    pub fn top_n_keys(&self, n: usize, is_bid: bool) -> Vec<(i64, f64)> {
        let mut result = Vec::with_capacity(n);

        if is_bid {
            for (&price_key, &(amount, _)) in self.levels.iter().rev().take(n) {
                result.push((price_key, amount));
            }
        } else {
            for (&price_key, &(amount, _)) in self.levels.iter().take(n) {
                result.push((price_key, amount));
            }
        }

        result
    }

    pub fn levels_keys(&self) -> Vec<(i64, f64)> {
        self.levels
            .iter()
            .map(|(&price_key, &(amount, _))| (price_key, amount))
            .collect()
    }

    /// 获取最优档位价格 (price key)
    pub fn best_key(&self, is_bid: bool) -> Option<i64> {
        if is_bid {
            self.levels.keys().next_back().copied()
        } else {
            self.levels.keys().next().copied()
        }
    }

    /// 获取最优档位 (price, amount, update_id)
    pub fn best_level(&self, is_bid: bool) -> Option<(f64, f64, i64)> {
        if is_bid {
            self.levels
                .iter()
                .next_back()
                .map(|(&price_key, &(amount, update_id))| {
                    (key_to_price(price_key), amount, update_id)
                })
        } else {
            self.levels
                .iter()
                .next()
                .map(|(&price_key, &(amount, update_id))| {
                    (key_to_price(price_key), amount, update_id)
                })
        }
    }

    /// 获取档位数量
    pub fn len(&self) -> usize {
        self.levels.len()
    }

    pub fn amount_at_price(&self, price: f64) -> Option<f64> {
        let key = price_to_key(price);
        self.levels.get(&key).map(|(amount, _)| *amount)
    }

    pub fn amount_at_price_key(&self, price_key: i64) -> Option<f64> {
        self.levels.get(&price_key).map(|(amount, _)| *amount)
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// 删除 <= 给定价格的所有档位，返回删除数量
    pub fn remove_leq(&mut self, price: f64) -> usize {
        let price_key = price_to_key(price);
        let old_len = self.levels.len();
        let split_key = match price_key.checked_add(1) {
            Some(v) => v,
            None => {
                self.levels.clear();
                return old_len;
            }
        };
        let keep = self.levels.split_off(&split_key);
        self.levels = keep;
        old_len.saturating_sub(self.levels.len())
    }

    /// 删除 >= 给定价格的所有档位，返回删除数量
    pub fn remove_geq(&mut self, price: f64) -> usize {
        let price_key = price_to_key(price);
        let old_len = self.levels.len();
        let _removed = self.levels.split_off(&price_key);
        old_len.saturating_sub(self.levels.len())
    }
}

/// 完整订单簿
#[derive(Debug, Clone, Default)]
pub struct OrderBook {
    pub bids: OrderBookSide,
    pub asks: OrderBookSide,
    pub last_update_id: i64,
    pub timestamp: i64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: OrderBookSide::new(),
            asks: OrderBookSide::new(),
            last_update_id: 0,
            timestamp: 0,
        }
    }

    /// 应用增量更新
    /// 每个档位会检查 update_id，只有更新的 update_id 大于当前档位的 update_id 时才覆盖
    pub fn apply_update(
        &mut self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        update_id: i64,
        timestamp: i64,
    ) {
        for &(price, amount) in bids {
            self.bids.update(price, amount, update_id);
        }
        for &(price, amount) in asks {
            self.asks.update(price, amount, update_id);
        }
        // 更新全局 last_update_id（取最大值，防止乱序）
        if update_id > self.last_update_id {
            self.last_update_id = update_id;
            self.timestamp = timestamp;
        }
    }

    /// 应用快照（与增量更新相同处理，不清空）
    /// 快照只是一部分档位，不是完整订单簿，视为普通更新即可
    #[inline]
    pub fn apply_snapshot(
        &mut self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        update_id: i64,
        timestamp: i64,
    ) {
        self.apply_update(bids, asks, update_id, timestamp);
    }

    /// 获取最优 N 档
    pub fn get_depth(&self, n: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let bids = self.bids.top_n(n, true);
        let asks = self.asks.top_n(n, false);
        (bids, asks)
    }

    pub fn get_depth_keys(&self, n: usize) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
        let bids = self.bids.top_n_keys(n, true);
        let asks = self.asks.top_n_keys(n, false);
        (bids, asks)
    }

    pub fn bid_levels_keys(&self) -> Vec<(i64, f64)> {
        self.bids.levels_keys()
    }

    pub fn ask_levels_keys(&self) -> Vec<(i64, f64)> {
        self.asks.levels_keys()
    }

    /// 检查订单簿是否有效 (至少有一档买卖)
    pub fn is_valid(&self) -> bool {
        if self.bids.is_empty() || self.asks.is_empty() {
            return false;
        }

        match (self.bids.best_key(true), self.asks.best_key(false)) {
            (Some(best_bid), Some(best_ask)) => best_bid < best_ask,
            _ => false,
        }
    }

    pub fn amount_at_price(&self, price: f64) -> Option<f64> {
        self.bids
            .amount_at_price(price)
            .or_else(|| self.asks.amount_at_price(price))
    }

    pub fn amount_at_price_key(&self, price_key: i64) -> Option<f64> {
        self.bids
            .amount_at_price_key(price_key)
            .or_else(|| self.asks.amount_at_price_key(price_key))
    }

    pub fn best_bid_price(&self) -> Option<f64> {
        self.best_bid_level().map(|(price, _, _)| price)
    }

    pub fn best_ask_price(&self) -> Option<f64> {
        self.best_ask_level().map(|(price, _, _)| price)
    }

    pub fn best_bid_level(&self) -> Option<(f64, f64, i64)> {
        self.bids.best_level(true)
    }

    pub fn best_ask_level(&self) -> Option<(f64, f64, i64)> {
        self.asks.best_level(false)
    }

    /// 裁剪所有 <= best_bid 的 asks，返回删除档位数
    pub fn prune_asks_leq_best_bid(&mut self) -> usize {
        let Some(best_bid) = self.best_bid_price() else {
            return 0;
        };
        self.asks.remove_leq(best_bid)
    }

    /// 裁剪所有 >= best_ask 的 bids，返回删除档位数
    pub fn prune_bids_geq_best_ask(&mut self) -> usize {
        let Some(best_ask) = self.best_ask_price() else {
            return 0;
        };
        self.bids.remove_geq(best_ask)
    }

    /// crossed-book 时根据 ask0/bid0 的 update_id 选择可信侧并裁剪对手盘：
    /// - bid0.update_id > ask0.update_id: 保留 bid0，裁剪 asks <= bid0
    /// - ask0.update_id > bid0.update_id: 保留 ask0，裁剪 bids >= ask0
    /// - update_id 相等: 无法判定可信侧，不裁剪
    pub fn prune_crossed_by_best_update_id(&mut self) -> usize {
        let Some((best_bid_price, _, best_bid_update_id)) = self.best_bid_level() else {
            return 0;
        };
        let Some((best_ask_price, _, best_ask_update_id)) = self.best_ask_level() else {
            return 0;
        };

        if best_bid_price < best_ask_price {
            return 0;
        }

        if best_bid_update_id > best_ask_update_id {
            self.asks.remove_leq(best_bid_price)
        } else if best_ask_update_id > best_bid_update_id {
            self.bids.remove_geq(best_ask_price)
        } else {
            0
        }
    }
}

/// 价格转换为整数 key (乘以 1e8)
#[inline]
pub fn price_to_key(price: f64) -> i64 {
    (price * 100_000_000.0).round() as i64
}

/// 整数 key 转换回价格
#[inline]
pub fn key_to_price(key: i64) -> f64 {
    key as f64 / 100_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_side() {
        let mut bids = OrderBookSide::new();

        // 添加买单 (update_id = 1)
        bids.update(100.0, 1.0, 1);
        bids.update(99.0, 2.0, 1);
        bids.update(101.0, 0.5, 1);

        // 获取 top 2 (买方: 价格降序)
        let top2 = bids.top_n(2, true);
        assert_eq!(top2.len(), 2);
        assert_eq!(top2[0].0, 101.0); // 最高价
        assert_eq!(top2[1].0, 100.0);

        // 删除一档 (update_id = 2)
        bids.update(101.0, 0.0, 2);
        let top2 = bids.top_n(2, true);
        assert_eq!(top2[0].0, 100.0);
    }

    #[test]
    fn test_orderbook_side_update_id_check() {
        let mut bids = OrderBookSide::new();

        // 添加买单 (update_id = 10)
        bids.update(100.0, 1.0, 10);
        assert_eq!(bids.top_n(1, true)[0].1, 1.0);

        // 尝试用旧的 update_id 更新，应该被忽略
        bids.update(100.0, 5.0, 5);
        assert_eq!(bids.top_n(1, true)[0].1, 1.0); // 仍然是 1.0

        // 用新的 update_id 更新，应该成功
        bids.update(100.0, 3.0, 15);
        assert_eq!(bids.top_n(1, true)[0].1, 3.0); // 更新为 3.0
    }

    #[test]
    fn test_orderbook() {
        let mut ob = OrderBook::new();

        // 应用快照
        let bids = vec![(100.0, 1.0), (99.0, 2.0)];
        let asks = vec![(101.0, 1.5), (102.0, 2.5)];
        ob.apply_snapshot(&bids, &asks, 1, 1000);

        assert!(ob.is_valid());

        let (top_bids, top_asks) = ob.get_depth(5);
        assert_eq!(top_bids.len(), 2);
        assert_eq!(top_asks.len(), 2);
        assert_eq!(top_bids[0].0, 100.0); // 最高买价
        assert_eq!(top_asks[0].0, 101.0); // 最低卖价
    }

    #[test]
    fn test_price_key_roundtrip_avoids_tick_truncation() {
        let price = 72645.9;
        let key = price_to_key(price);
        let restored = key_to_price(key);
        assert_eq!(restored, 72645.9);
    }

    #[test]
    fn test_orderbook_is_valid() {
        let mut ob = OrderBook::new();
        assert!(!ob.is_valid());

        ob.apply_update(&[(100.0, 1.0)], &[(101.0, 1.0)], 1, 1000);
        assert!(ob.is_valid());

        ob.apply_update(&[(102.0, 1.0)], &[(101.0, 1.0)], 2, 1001);
        assert!(!ob.is_valid());
    }

    #[test]
    fn test_prune_crossed_asks_by_best_bid() {
        let mut ob = OrderBook::new();
        ob.apply_update(&[(100.0, 1.0)], &[(99.0, 1.0), (101.0, 1.0)], 1, 1000);
        assert!(!ob.is_valid());

        let removed = ob.prune_asks_leq_best_bid();
        assert_eq!(removed, 1);
        assert!(ob.is_valid());
        assert_eq!(ob.best_ask_price(), Some(101.0));
    }

    #[test]
    fn test_prune_crossed_bids_by_best_ask() {
        let mut ob = OrderBook::new();
        ob.apply_update(&[(102.0, 1.0), (99.0, 1.0)], &[(101.0, 1.0)], 1, 1000);
        assert!(!ob.is_valid());

        let removed = ob.prune_bids_geq_best_ask();
        assert_eq!(removed, 1);
        assert!(ob.is_valid());
        assert_eq!(ob.best_bid_price(), Some(99.0));
    }

    #[test]
    fn test_prune_crossed_prefers_newer_bid0_update_id() {
        let mut ob = OrderBook::new();
        ob.apply_update(&[(100.0, 1.0)], &[(101.0, 1.0), (103.0, 1.0)], 10, 1000);
        ob.apply_update(&[(102.0, 1.0)], &[], 20, 1001);
        assert!(!ob.is_valid());

        let removed = ob.prune_crossed_by_best_update_id();
        assert_eq!(removed, 1);
        assert!(ob.is_valid());
        assert_eq!(ob.best_ask_price(), Some(103.0));
    }

    #[test]
    fn test_prune_crossed_prefers_newer_ask0_update_id() {
        let mut ob = OrderBook::new();
        ob.apply_update(&[(102.0, 1.0), (99.0, 1.0)], &[(103.0, 1.0)], 10, 1000);
        ob.apply_update(&[], &[(101.0, 1.0)], 20, 1001);
        assert!(!ob.is_valid());

        let removed = ob.prune_crossed_by_best_update_id();
        assert_eq!(removed, 1);
        assert!(ob.is_valid());
        assert_eq!(ob.best_bid_price(), Some(99.0));
    }

    #[test]
    fn test_prune_crossed_no_action_when_best_update_id_equal() {
        let mut ob = OrderBook::new();
        ob.apply_update(&[(102.0, 1.0)], &[(101.0, 1.0), (103.0, 1.0)], 10, 1000);
        assert!(!ob.is_valid());

        let removed = ob.prune_crossed_by_best_update_id();
        assert_eq!(removed, 0);
        assert!(!ob.is_valid());
    }

    #[test]
    fn test_orderbook_out_of_order() {
        let mut ob = OrderBook::new();

        // 先收到 update_id = 10
        ob.apply_update(&[(100.0, 1.0)], &[(101.0, 1.0)], 10, 1000);
        assert_eq!(ob.last_update_id, 10);

        // 再收到乱序的 update_id = 5（应该被忽略）
        ob.apply_update(&[(100.0, 5.0)], &[(101.0, 5.0)], 5, 900);
        assert_eq!(ob.last_update_id, 10); // 仍然是 10
        let (bids, _) = ob.get_depth(1);
        assert_eq!(bids[0].1, 1.0); // 数量仍然是 1.0，没有被覆盖

        // 收到更新的 update_id = 15
        ob.apply_update(&[(100.0, 3.0)], &[(101.0, 3.0)], 15, 1100);
        assert_eq!(ob.last_update_id, 15);
        let (bids, _) = ob.get_depth(1);
        assert_eq!(bids[0].1, 3.0); // 数量更新为 3.0
    }
}
