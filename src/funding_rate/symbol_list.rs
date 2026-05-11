//! 交易对列表管理模块 - 单例访问模式
//!
//! 管理两个热更新列表：
//! - dump_symbol_list: 平仓列表（算法会平仓）
//! - trade_symbol_list: 建仓列表（算法会根据信号建仓）
//!
//! 数据结构：Exchange -> Vec<String>
//! Symbol 列表按 key_suffix 维度加载（可区分 open/hedge venue 组合）
//! 从 Redis 读取并支持热更新

use anyhow::Result;
use log::info;
use serde_json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::common::exchange::Exchange;
use crate::common::redis_client::RedisClient;
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

const DEFAULT_SYMBOL_NAMESPACE: &str = "fr";

// Thread-local 单例存储
thread_local! {
    static SYMBOL_LIST: RefCell<Option<SymbolListInner>> = const { RefCell::new(None) };
}

/// SymbolList 单例访问器（零大小类型）
pub struct SymbolList;

/// SymbolList 内部实现
struct SymbolListInner {
    /// 当前运行 exchange（加载列表时记录）
    current_exchange: Option<Exchange>,

    /// 平仓列表
    dump_symbols: HashSet<String>,

    /// 正套建仓列表
    fwd_trade_symbols: HashSet<String>,

    /// 反套建仓列表
    bwd_trade_symbols: HashSet<String>,
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
            current_exchange: None,
            dump_symbols: HashSet::new(),
            fwd_trade_symbols: HashSet::new(),
            bwd_trade_symbols: HashSet::new(),
        };

        SYMBOL_LIST.with(|sl| {
            *sl.borrow_mut() = Some(inner);
        });

        info!("SymbolList 初始化完成");
        Ok(())
    }

    /// 从 Redis 热更新列表（单 exchange）
    ///
    /// # 参数
    /// - `client`: Redis 客户端
    /// - `exchange`: 当前运行的 exchange
    pub async fn reload_from_redis(
        &self,
        client: &mut RedisClient,
        exchange: Exchange,
    ) -> Result<()> {
        self.reload_from_redis_with_key_suffix(client, exchange.as_str(), DEFAULT_SYMBOL_NAMESPACE)
            .await
    }

    pub async fn reload_from_redis_with_namespace(
        &self,
        client: &mut RedisClient,
        exchange: Exchange,
        namespace: &str,
    ) -> Result<()> {
        self.reload_from_redis_with_key_suffix(client, exchange.as_str(), namespace)
            .await?;
        Self::with_inner_mut(|inner| {
            inner.current_exchange = Some(exchange);
        });
        Ok(())
    }

    pub async fn reload_from_redis_with_key_suffix(
        &self,
        client: &mut RedisClient,
        key_suffix: &str,
        namespace: &str,
    ) -> Result<()> {
        self.reload_from_redis_with_key_prefix(client, key_suffix, namespace, None)
            .await
    }

    pub async fn reload_from_redis_with_key_prefix(
        &self,
        client: &mut RedisClient,
        key_suffix: &str,
        namespace: &str,
        key_prefix: Option<&str>,
    ) -> Result<()> {
        let key_suffix = key_suffix.trim().to_ascii_lowercase();
        let ns = normalize_symbol_list_namespace(namespace);
        let key_prefix = normalize_symbol_list_key_prefix(key_prefix);

        // 读取平仓列表
        let dump_key =
            symbol_list_redis_key(key_prefix.as_deref(), &ns, "dump_symbols", &key_suffix);
        if let Ok(Some(value)) = client.get_string(&dump_key).await {
            if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                Self::with_inner_mut(|inner| {
                    inner.dump_symbols = symbols.iter().map(|s| s.to_uppercase()).collect();
                    info!(
                        "更新平仓列表 key='{}': {} 个交易对",
                        dump_key,
                        inner.dump_symbols.len()
                    );
                });
            }
        }

        // 读取建仓列表
        // （废弃）建仓列表现阶段未使用，留空
        if ns == "mm" {
            let trade_key =
                symbol_list_redis_key(key_prefix.as_deref(), &ns, "trade_symbols", &key_suffix);
            if let Ok(Some(value)) = client.get_string(&trade_key).await {
                if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                    let normalized: HashSet<String> =
                        symbols.iter().map(|s| s.to_uppercase()).collect();
                    Self::with_inner_mut(|inner| {
                        // MM 当前只维护一套交易列表，映射到正反两个方向以复用触发逻辑
                        inner.fwd_trade_symbols = normalized.clone();
                        inner.bwd_trade_symbols = normalized.clone();
                        info!(
                            "更新 MM 交易列表 key='{}': {} 个交易对",
                            trade_key,
                            inner.fwd_trade_symbols.len()
                        );
                    });
                }
            }
        }

        // 读取正套建仓列表
        let fwd_trade_key =
            symbol_list_redis_key(key_prefix.as_deref(), &ns, "fwd_trade_symbols", &key_suffix);
        if let Ok(Some(value)) = client.get_string(&fwd_trade_key).await {
            if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                Self::with_inner_mut(|inner| {
                    inner.fwd_trade_symbols = symbols.iter().map(|s| s.to_uppercase()).collect();
                    info!(
                        "更新正套建仓列表 key='{}': {} 个交易对",
                        fwd_trade_key,
                        inner.fwd_trade_symbols.len()
                    );
                });
            }
        }

        // 读取反套建仓列表
        let bwd_trade_key =
            symbol_list_redis_key(key_prefix.as_deref(), &ns, "bwd_trade_symbols", &key_suffix);
        if let Ok(Some(value)) = client.get_string(&bwd_trade_key).await {
            if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&value) {
                Self::with_inner_mut(|inner| {
                    inner.bwd_trade_symbols = symbols.iter().map(|s| s.to_uppercase()).collect();
                    info!(
                        "更新反套建仓列表 key='{}': {} 个交易对",
                        bwd_trade_key,
                        inner.bwd_trade_symbols.len()
                    );
                });
            }
        }

        // intra: 同所期现没有正反开方向限制，fwd ∪ bwd 视为单一 online 列表，
        // 让 is_in_fwd_trade_list / is_in_bwd_trade_list 对任一方向都放行
        if ns == "intra" {
            Self::with_inner_mut(|inner| {
                let union: HashSet<String> = inner
                    .fwd_trade_symbols
                    .union(&inner.bwd_trade_symbols)
                    .cloned()
                    .collect();
                let count = union.len();
                inner.fwd_trade_symbols = union.clone();
                inner.bwd_trade_symbols = union;
                info!(
                    "intra online 列表 {}: 合并 fwd∪bwd = {} 个交易对",
                    key_suffix, count
                );
            });
        }

        // 记录当前 exchange（仅当 suffix 可解析为单交易所）
        Self::with_inner_mut(|inner| {
            inner.current_exchange = exchange_from_key_suffix(&key_suffix);
        });

        Ok(())
    }

    // ==================== 查询接口 ====================

    /// 判断交易对是否在平仓列表中
    pub fn is_in_dump_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| Self::contains_normalized(&inner.dump_symbols, symbol))
    }

    /// 判断交易对是否在正套建仓列表中
    pub fn is_in_fwd_trade_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| Self::contains_normalized(&inner.fwd_trade_symbols, symbol))
    }

    /// 判断交易对是否在反套建仓列表中
    pub fn is_in_bwd_trade_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| Self::contains_normalized(&inner.bwd_trade_symbols, symbol))
    }

    /// 获取平仓列表
    pub fn get_dump_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.dump_symbols.iter().cloned().collect())
    }

    /// 获取正套建仓列表
    pub fn get_fwd_trade_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.fwd_trade_symbols.iter().cloned().collect())
    }

    /// 获取反套建仓列表
    pub fn get_bwd_trade_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.bwd_trade_symbols.iter().cloned().collect())
    }

    /// 获取 online symbols（平仓 ∪ 正套/反套建仓列表）
    pub fn get_online_symbols(&self) -> Vec<String> {
        Self::with_inner(Self::collect_online)
    }

    /// 获取所有交易场所的 online symbols（基于当前 exchange）
    pub fn get_all_online_symbols(&self) -> HashMap<TradingVenue, Vec<String>> {
        Self::with_inner(|inner| {
            let mut result = HashMap::new();
            if let Some(exchange) = inner.current_exchange {
                let online_set = Self::collect_online(inner);
                for venue in Self::exchange_to_venues(&exchange) {
                    result.insert(venue, online_set.clone());
                }
            }
            result
        })
    }

    // ==================== 内部辅助方法 ====================

    /// 汇总 online symbols（平仓 ∪ 建仓 ∪ 正套/反套建仓）
    fn collect_online(inner: &SymbolListInner) -> Vec<String> {
        let mut online_set = HashSet::new();
        online_set.extend(inner.dump_symbols.iter().cloned());
        online_set.extend(inner.fwd_trade_symbols.iter().cloned());
        online_set.extend(inner.bwd_trade_symbols.iter().cloned());
        online_set.into_iter().collect()
    }

    /// 判断集合中是否包含归一化后的 symbol（忽略分隔符和 OKEx SWAP 后缀）
    fn contains_normalized(set: &HashSet<String>, symbol: &str) -> bool {
        let target = Self::normalize_for_filtering(symbol);
        set.iter()
            .any(|s| Self::normalize_for_filtering(s) == target)
    }

    /// 归一化符号用于白名单过滤：大写，移除 '-'/'_'，并去掉 "-SWAP"/"SWAP" 后缀
    /// 不区分 open/hedge，统一用 OkexFutures 触发去 SWAP 逻辑
    fn normalize_for_filtering(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    /// 将交易所枚举转换为所有对应的 TradingVenue
    fn exchange_to_venues(exchange: &Exchange) -> Vec<TradingVenue> {
        match exchange {
            Exchange::Binance => vec![TradingVenue::BinanceMargin, TradingVenue::BinanceFutures],
            Exchange::Okex => vec![TradingVenue::OkexFutures, TradingVenue::OkexMargin],
            Exchange::Bitget => vec![TradingVenue::BitgetMargin, TradingVenue::BitgetFutures],
            Exchange::Bybit => vec![TradingVenue::BybitMargin, TradingVenue::BybitFutures],
            Exchange::Gate => vec![TradingVenue::GateMargin, TradingVenue::GateFutures],
            Exchange::Hyperliquid => {
                vec![
                    TradingVenue::HyperliquidMargin,
                    TradingVenue::HyperliquidFutures,
                ]
            }
            Exchange::Aster => {
                vec![TradingVenue::AsterMargin, TradingVenue::AsterFutures]
            }
        }
    }
}

fn normalize_symbol_list_namespace(namespace: &str) -> String {
    let namespace = namespace
        .trim()
        .trim_end_matches(['_', '-', ':'])
        .to_ascii_lowercase();
    if namespace.is_empty() {
        DEFAULT_SYMBOL_NAMESPACE.to_string()
    } else {
        namespace
    }
}

fn normalize_symbol_list_key_prefix(prefix: Option<&str>) -> Option<String> {
    prefix
        .map(|p| p.trim().trim_end_matches(':').to_ascii_lowercase())
        .filter(|p| !p.is_empty())
}

fn symbol_list_redis_key(
    prefix: Option<&str>,
    namespace: &str,
    list_name: &str,
    suffix: &str,
) -> String {
    let base = format!("{namespace}_{list_name}:{suffix}");
    match prefix {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}:{base}"),
        _ => base,
    }
}

fn exchange_from_key_suffix(key_suffix: &str) -> Option<Exchange> {
    let suffix = key_suffix.trim().to_ascii_lowercase();
    if !suffix.contains('_') {
        return Exchange::from_str(&suffix);
    }
    let open_part = suffix.split('_').next()?;
    let open_exchange = open_part.split('-').next().unwrap_or(open_part);
    Exchange::from_str(open_exchange)
}

#[cfg(test)]
mod tests {
    use super::symbol_list_redis_key;

    #[test]
    fn symbol_list_redis_key_without_prefix_matches_legacy_shape() {
        assert_eq!(
            symbol_list_redis_key(
                None,
                "fr",
                "fwd_trade_symbols",
                "binance-margin_binance-futures"
            ),
            "fr_fwd_trade_symbols:binance-margin_binance-futures"
        );
    }

    #[test]
    fn symbol_list_redis_key_with_prefix_is_env_scoped() {
        assert_eq!(
            symbol_list_redis_key(
                Some("binance_fr_trade01"),
                "fr",
                "bwd_trade_symbols",
                "binance-margin_binance-futures"
            ),
            "binance_fr_trade01:fr_bwd_trade_symbols:binance-margin_binance-futures"
        );
    }
}
