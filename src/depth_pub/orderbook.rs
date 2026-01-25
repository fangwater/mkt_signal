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

    /// 获取最优档位价格 (price key)
    pub fn best_key(&self, is_bid: bool) -> Option<i64> {
        if is_bid {
            self.levels.keys().next_back().copied()
        } else {
            self.levels.keys().next().copied()
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

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
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
}

/// 价格转换为整数 key (乘以 1e8)
#[inline]
fn price_to_key(price: f64) -> i64 {
    (price * 100_000_000.0) as i64
}

/// 整数 key 转换回价格
#[inline]
fn key_to_price(key: i64) -> f64 {
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
    fn test_orderbook_is_valid() {
        let mut ob = OrderBook::new();
        assert!(!ob.is_valid());

        ob.apply_update(&[(100.0, 1.0)], &[(101.0, 1.0)], 1, 1000);
        assert!(ob.is_valid());

        ob.apply_update(&[(102.0, 1.0)], &[(101.0, 1.0)], 2, 1001);
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
