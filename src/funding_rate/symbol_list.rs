//! 交易对列表管理模块 - 单例访问模式
//!
//! 管理两个热更新列表：
//! - dump_symbol_list: 平仓列表（算法会平仓）
//! - trade_symbol_list: 建仓列表（算法会根据信号建仓）
//!
//! 数据结构：Exchange -> Vec<String>
//! Symbol 列表是 exchange 维度的（所有 venue 共享）
//! 从 Redis 读取并支持热更新

use anyhow::Result;
use log::info;
use serde_json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::common::exchange::Exchange;
use crate::common::redis_client::RedisClient;
use crate::signal::common::TradingVenue;

// Redis key 前缀
const DUMP_SYMBOL_KEY_PREFIX: &str = "fr_dump_symbols";
const TRADE_SYMBOL_KEY_PREFIX: &str = "fr_trade_symbols";
const FWD_TRADE_SYMBOL_KEY_PREFIX: &str = "fr_fwd_trade_symbols";
const BWD_TRADE_SYMBOL_KEY_PREFIX: &str = "fr_bwd_trade_symbols";

// Thread-local 单例存储
thread_local! {
    static SYMBOL_LIST: RefCell<Option<SymbolListInner>> = RefCell::new(None);
}

/// SymbolList 单例访问器（零大小类型）
pub struct SymbolList;

/// SymbolList 内部实现
struct SymbolListInner {
    /// 平仓列表：Exchange -> HashSet<Symbol>
    dump_symbols: HashMap<Exchange, HashSet<String>>,

    /// 建仓列表：Exchange -> HashSet<Symbol>
    trade_symbols: HashMap<Exchange, HashSet<String>>,

    /// 正套建仓列表：Exchange -> HashSet<Symbol>
    fwd_trade_symbols: HashMap<Exchange, HashSet<String>>,

    /// 反套建仓列表：Exchange -> HashSet<Symbol>
    bwd_trade_symbols: HashMap<Exchange, HashSet<String>>,
}

impl SymbolList {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        SymbolList
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&SymbolListInner) -> R,
    {
        SYMBOL_LIST.with(|sl| {
            let sl_ref = sl.borrow();
            let inner = sl_ref.as_ref().expect("SymbolList not initialized");
            f(inner)
        })
    }

    /// 访问内部状态的可变辅助方法（内部使用）
    fn with_inner_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut SymbolListInner) -> R,
    {
        SYMBOL_LIST.with(|sl| {
            let mut sl_ref = sl.borrow_mut();
            let inner = sl_ref.as_mut().expect("SymbolList not initialized");
            f(inner)
        })
    }

    /// 初始化单例
    pub fn init_singleton() -> Result<()> {
        let inner = SymbolListInner {
            dump_symbols: HashMap::new(),
            trade_symbols: HashMap::new(),
            fwd_trade_symbols: HashMap::new(),
            bwd_trade_symbols: HashMap::new(),
        };

        SYMBOL_LIST.with(|sl| {
            *sl.borrow_mut() = Some(inner);
        });

        info!("SymbolList 初始化完成");
        Ok(())
    }

    /// 从 Redis 热更新列表
    ///
    /// # 参数
    /// - `client`: Redis 客户端
    /// - `exchanges`: 需要更新的 exchange 列表
    pub async fn reload_from_redis(
        &self,
        client: &mut RedisClient,
        exchanges: &[Exchange],
    ) -> Result<()> {
        for &exchange in exchanges {
            let exchange_str = exchange.as_str();

            // 读取平仓列表
            let dump_key = format!("{}:{}", DUMP_SYMBOL_KEY_PREFIX, exchange_str);
            if let Ok(Some(value)) = client.get_string(&dump_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                    Self::with_inner_mut(|inner| {
                        let symbol_set: HashSet<String> =
                            symbols.iter().map(|s| s.to_uppercase()).collect();
                        inner.dump_symbols.insert(exchange, symbol_set.clone());
                        info!(
                            "更新平仓列表 {}: {} 个交易对",
                            exchange_str,
                            symbol_set.len()
                        );
                    });
                }
            }

            // 读取建仓列表
            let trade_key = format!("{}:{}", TRADE_SYMBOL_KEY_PREFIX, exchange_str);
            if let Ok(Some(value)) = client.get_string(&trade_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                    Self::with_inner_mut(|inner| {
                        let symbol_set: HashSet<String> =
                            symbols.iter().map(|s| s.to_uppercase()).collect();
                        inner.trade_symbols.insert(exchange, symbol_set.clone());
                        info!(
                            "更新建仓列表 {}: {} 个交易对",
                            exchange_str,
                            symbol_set.len()
                        );
                    });
                }
            }

            // 读取正套建仓列表
            let fwd_trade_key = format!("{}:{}", FWD_TRADE_SYMBOL_KEY_PREFIX, exchange_str);
            if let Ok(Some(value)) = client.get_string(&fwd_trade_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                    Self::with_inner_mut(|inner| {
                        let symbol_set: HashSet<String> =
                            symbols.iter().map(|s| s.to_uppercase()).collect();
                        inner.fwd_trade_symbols.insert(exchange, symbol_set.clone());
                        info!(
                            "更新正套建仓列表 {}: {} 个交易对",
                            exchange_str,
                            symbol_set.len()
                        );
                    });
                }
            }

            // 读取反套建仓列表
            let bwd_trade_key = format!("{}:{}", BWD_TRADE_SYMBOL_KEY_PREFIX, exchange_str);
            if let Ok(Some(value)) = client.get_string(&bwd_trade_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                    Self::with_inner_mut(|inner| {
                        let symbol_set: HashSet<String> =
                            symbols.iter().map(|s| s.to_uppercase()).collect();
                        inner.bwd_trade_symbols.insert(exchange, symbol_set.clone());
                        info!(
                            "更新反套建仓列表 {}: {} 个交易对",
                            exchange_str,
                            symbol_set.len()
                        );
                    });
                }
            }
        }

        Ok(())
    }

    // ==================== 查询接口 ====================

    /// 判断交易对是否在平仓列表中
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn is_in_dump_list(&self, symbol: &str, venue: TradingVenue) -> bool {
        let symbol_upper = symbol.to_uppercase();
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .dump_symbols
                .get(&exchange)
                .map(|set| set.contains(&symbol_upper))
                .unwrap_or(false)
        })
    }

    /// 判断交易对是否在建仓列表中
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn is_in_trade_list(&self, symbol: &str, venue: TradingVenue) -> bool {
        let symbol_upper = symbol.to_uppercase();
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .trade_symbols
                .get(&exchange)
                .map(|set| set.contains(&symbol_upper))
                .unwrap_or(false)
        })
    }

    /// 判断交易对是否在正套建仓列表中
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn is_in_fwd_trade_list(&self, symbol: &str, venue: TradingVenue) -> bool {
        let symbol_upper = symbol.to_uppercase();
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .fwd_trade_symbols
                .get(&exchange)
                .map(|set| set.contains(&symbol_upper))
                .unwrap_or(false)
        })
    }

    /// 判断交易对是否在反套建仓列表中
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn is_in_bwd_trade_list(&self, symbol: &str, venue: TradingVenue) -> bool {
        let symbol_upper = symbol.to_uppercase();
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .bwd_trade_symbols
                .get(&exchange)
                .map(|set| set.contains(&symbol_upper))
                .unwrap_or(false)
        })
    }

    /// 获取指定交易场所的平仓列表
    ///
    /// # 参数
    /// - `venue`: 交易场所
    pub fn get_dump_symbols(&self, venue: TradingVenue) -> Vec<String> {
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .dump_symbols
                .get(&exchange)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        })
    }

    /// 获取指定交易场所的建仓列表
    ///
    /// # 参数
    /// - `venue`: 交易场所
    pub fn get_trade_symbols(&self, venue: TradingVenue) -> Vec<String> {
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            inner
                .trade_symbols
                .get(&exchange)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default()
        })
    }

    /// 获取 online symbols（平仓列表 ∪ 建仓列表）
    ///
    /// # 参数
    /// - `venue`: 交易场所
    ///
    /// # 返回
    /// 返回 dump_symbols 和 trade_symbols 的并集
    pub fn get_online_symbols(&self, venue: TradingVenue) -> Vec<String> {
        let exchange = Self::venue_to_exchange(venue);
        Self::with_inner(|inner| {
            let mut online_set = HashSet::new();

            // 添加平仓列表
            if let Some(dump_set) = inner.dump_symbols.get(&exchange) {
                online_set.extend(dump_set.iter().cloned());
            }

            // 添加建仓列表
            if let Some(trade_set) = inner.trade_symbols.get(&exchange) {
                online_set.extend(trade_set.iter().cloned());
            }

            // 添加正套建仓列表
            if let Some(fwd_set) = inner.fwd_trade_symbols.get(&exchange) {
                online_set.extend(fwd_set.iter().cloned());
            }

            // 添加反套建仓列表
            if let Some(bwd_set) = inner.bwd_trade_symbols.get(&exchange) {
                online_set.extend(bwd_set.iter().cloned());
            }

            online_set.into_iter().collect()
        })
    }

    /// 获取所有交易场所的 online symbols
    ///
    /// # 返回
    /// HashMap<TradingVenue, Vec<String>>
    pub fn get_all_online_symbols(&self) -> HashMap<TradingVenue, Vec<String>> {
        Self::with_inner(|inner| {
            let mut result = HashMap::new();

            // 遍历所有 exchange
            for (exchange, _) in &inner.dump_symbols {
                // 为每个 exchange 对应的 venues 设置相同的 symbol 列表
                let online_set = Self::get_online_symbols_for_exchange(inner, exchange);

                // 将 exchange 映射到所有对应的 venues
                for venue in Self::exchange_to_venues(exchange) {
                    result.insert(venue, online_set.clone());
                }
            }

            result
        })
    }

    // ==================== 内部辅助方法 ====================

    /// 获取指定 exchange 的 online symbols
    fn get_online_symbols_for_exchange(
        inner: &SymbolListInner,
        exchange: &Exchange,
    ) -> Vec<String> {
        let mut online_set = HashSet::new();

        if let Some(dump_set) = inner.dump_symbols.get(exchange) {
            online_set.extend(dump_set.iter().cloned());
        }

        if let Some(trade_set) = inner.trade_symbols.get(exchange) {
            online_set.extend(trade_set.iter().cloned());
        }

        if let Some(fwd_set) = inner.fwd_trade_symbols.get(exchange) {
            online_set.extend(fwd_set.iter().cloned());
        }

        if let Some(bwd_set) = inner.bwd_trade_symbols.get(exchange) {
            online_set.extend(bwd_set.iter().cloned());
        }

        online_set.into_iter().collect()
    }

    /// 将 TradingVenue 转换为交易所枚举
    fn venue_to_exchange(venue: TradingVenue) -> Exchange {
        match venue {
            TradingVenue::BinanceMargin => Exchange::Binance,
            TradingVenue::BinanceFutures => Exchange::Binance,
            TradingVenue::OkexFutures => Exchange::Okex,
            TradingVenue::OkexMargin => Exchange::Okex,
            TradingVenue::BitgetMargin => Exchange::Bitget,
            TradingVenue::BitgetFutures => Exchange::Bitget,
            TradingVenue::BybitMargin => Exchange::Bybit,
            TradingVenue::BybitFutures => Exchange::Bybit,
            TradingVenue::GateMargin => Exchange::Gate,
            TradingVenue::GateFutures => Exchange::Gate,
        }
    }

    /// 将交易所枚举转换为所有对应的 TradingVenue
    fn exchange_to_venues(exchange: &Exchange) -> Vec<TradingVenue> {
        match exchange {
            Exchange::Binance => vec![TradingVenue::BinanceMargin, TradingVenue::BinanceFutures],
            Exchange::Okex => vec![TradingVenue::OkexFutures, TradingVenue::OkexMargin],
            Exchange::Bitget => vec![TradingVenue::BitgetMargin, TradingVenue::BitgetFutures],
            Exchange::Bybit => vec![TradingVenue::BybitMargin, TradingVenue::BybitFutures],
            Exchange::Gate => vec![TradingVenue::GateMargin, TradingVenue::GateFutures],
        }
    }
}
