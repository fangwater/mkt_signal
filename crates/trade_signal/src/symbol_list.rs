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
use log::{info, warn};
use runtime_common::fast_hash::{
    fast_hash_map, fast_hash_set, fast_hash_set_from_iter, FastHashMap, FastHashSet,
};
use serde_json;
use std::cell::RefCell;

use mkt_parsers::symbol_match::{
    normalize_symbol_for_whitelist, normalize_symbol_for_whitelist_cow,
};
use order_common::TradingVenue;
use runtime_common::exchange::Exchange;
use runtime_common::redis_client::RedisClient;
use std::borrow::Cow;

const DEFAULT_SYMBOL_NAMESPACE: &str = "fr";

// Thread-local 单例存储
thread_local! {
    static SYMBOL_LIST: RefCell<Option<SymbolListInner>> = const { RefCell::new(None) };
}

/// SymbolList 单例访问器（零大小类型）
pub struct SymbolList;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SymbolListMembership {
    pub in_fwd: bool,
    pub in_bwd: bool,
    pub in_dump: bool,
    pub in_vol_gate: bool,
}

impl SymbolListMembership {
    pub fn is_online(self) -> bool {
        self.in_fwd || self.in_bwd || self.in_dump
    }
}

/// SymbolList 内部实现
struct SymbolListInner {
    /// 当前运行 exchange（加载列表时记录）
    current_exchange: Option<Exchange>,

    /// intra 列表在加载完成后统一保存为 whitelist canonical key。
    /// 此时查询可以直接走 HashSet borrowed lookup，无需逐项规范化。
    canonical_filter_keys: bool,

    /// 平仓列表
    dump_symbols: FastHashSet<String>,

    /// FR 仓位集中度风控维护的隐式平仓列表。
    pos_dump_symbols: FastHashSet<String>,

    /// 正套建仓列表
    fwd_trade_symbols: FastHashSet<String>,

    /// 反套建仓列表
    bwd_trade_symbols: FastHashSet<String>,

    /// UniMMR 算法平仓 symbol list（仅 FR 加载）。
    /// 参与 `collect_online` 并集，确保下游阈值/订阅会覆盖这些 symbol。
    unimmr_close_symbols: FastHashSet<String>,

    /// Intra 专用：只有命中该列表的 symbol 才应用 inline volatility open gate。
    vol_gate_symbols: FastHashSet<String>,
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
            canonical_filter_keys: false,
            dump_symbols: fast_hash_set(),
            pos_dump_symbols: fast_hash_set(),
            fwd_trade_symbols: fast_hash_set(),
            bwd_trade_symbols: fast_hash_set(),
            unimmr_close_symbols: fast_hash_set(),
            vol_gate_symbols: fast_hash_set(),
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
        // Redis reads below contain await points and update the sets independently. Keep the
        // compatibility lookup active until an intra reload has canonicalized every set.
        Self::with_inner_mut(|inner| {
            inner.canonical_filter_keys = false;
        });

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
                    let normalized: FastHashSet<String> =
                        fast_hash_set_from_iter(symbols.iter().map(|s| s.to_uppercase()));
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

        // UniMMR 算法平仓 symbol list 仅属于 FR。
        // 与 `pre_trade::unimmr_close_symbol_list` 共用同一 Redis key 与归一化口径，
        // 这里加载只是为了把它并入 online 集合，保证下游阈值/数据订阅覆盖到这些
        // symbol；pre_trade 仍以自己的副本做 close 决策。
        if ns == "fr" {
            let unimmr_key = symbol_list_redis_key(
                key_prefix.as_deref(),
                &ns,
                "unimmr_close_symbols",
                &key_suffix,
            );
            match client.get_string(&unimmr_key).await {
                Ok(Some(value)) => match serde_json::from_str::<Vec<String>>(&value) {
                    Ok(symbols) => {
                        Self::with_inner_mut(|inner| {
                            inner.unimmr_close_symbols =
                                symbols.iter().map(|s| s.to_uppercase()).collect();
                            info!(
                                "更新 UniMMR 平仓列表 key='{}': {} 个交易对",
                                unimmr_key,
                                inner.unimmr_close_symbols.len()
                            );
                        });
                    }
                    Err(err) => {
                        Self::with_inner_mut(|inner| inner.unimmr_close_symbols.clear());
                        warn!(
                            "UniMMR 平仓列表 key='{}' 解析失败 raw={} err={:#}，清空本地缓存",
                            unimmr_key, value, err
                        );
                    }
                },
                Ok(None) => {
                    Self::with_inner_mut(|inner| inner.unimmr_close_symbols.clear());
                }
                Err(err) => {
                    warn!(
                        "UniMMR 平仓列表 key='{}' 读取失败: {:#}（保留旧缓存）",
                        unimmr_key, err
                    );
                }
            }
            if let Err(err) = self
                .reload_fr_pos_dump_from_redis(client, &key_suffix, key_prefix.as_deref())
                .await
            {
                warn!("FR pos dump 列表刷新失败: {err:#}（保留旧缓存）");
            }
        } else {
            Self::with_inner_mut(|inner| inner.pos_dump_symbols.clear());
        }

        if ns == "intra" {
            let vol_gate_key =
                symbol_list_redis_key(key_prefix.as_deref(), &ns, "vol_gate_symbols", &key_suffix);
            match client.get_string(&vol_gate_key).await {
                Ok(Some(value)) => match serde_json::from_str::<Vec<String>>(&value) {
                    Ok(symbols) => {
                        Self::with_inner_mut(|inner| {
                            inner.vol_gate_symbols =
                                symbols.iter().map(|s| s.to_uppercase()).collect();
                            info!(
                                "更新 intra vol gate 列表 key='{}': {} 个交易对",
                                vol_gate_key,
                                inner.vol_gate_symbols.len()
                            );
                        });
                    }
                    Err(err) => {
                        Self::with_inner_mut(|inner| inner.vol_gate_symbols.clear());
                        warn!(
                            "intra vol gate 列表 key='{}' 解析失败 raw={} err={:#}，清空本地缓存",
                            vol_gate_key, value, err
                        );
                    }
                },
                Ok(None) => {
                    Self::with_inner_mut(|inner| inner.vol_gate_symbols.clear());
                }
                Err(err) => {
                    warn!(
                        "intra vol gate 列表 key='{}' 读取失败: {:#}（保留旧缓存）",
                        vol_gate_key, err
                    );
                }
            }
        } else {
            Self::with_inner_mut(|inner| inner.vol_gate_symbols.clear());
        }

        // intra: 同所期现没有正反开方向限制，fwd ∪ bwd 视为单一 online 列表，
        // 让 is_in_fwd_trade_list / is_in_bwd_trade_list 对任一方向都放行
        if ns == "intra" {
            Self::with_inner_mut(|inner| {
                Self::canonicalize_filter_set(&mut inner.dump_symbols);
                Self::canonicalize_filter_set(&mut inner.pos_dump_symbols);
                Self::canonicalize_filter_set(&mut inner.fwd_trade_symbols);
                Self::canonicalize_filter_set(&mut inner.bwd_trade_symbols);
                Self::canonicalize_filter_set(&mut inner.unimmr_close_symbols);
                Self::canonicalize_filter_set(&mut inner.vol_gate_symbols);

                let union: FastHashSet<String> = fast_hash_set_from_iter(
                    inner
                        .fwd_trade_symbols
                        .union(&inner.bwd_trade_symbols)
                        .cloned(),
                );
                let count = union.len();
                inner.fwd_trade_symbols = union.clone();
                inner.bwd_trade_symbols = union;
                inner.canonical_filter_keys = true;
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

    /// Snapshot all hot-path memberships with one canonicalization and one state borrow.
    pub fn membership(&self, symbol: &str) -> SymbolListMembership {
        Self::with_inner(|inner| {
            let symbol = Self::normalize_for_filtering_cow(symbol);
            Self::membership_for_canonical(inner, symbol.as_ref())
        })
    }

    pub(crate) fn membership_canonical(&self, symbol_key: &str) -> SymbolListMembership {
        Self::with_inner(|inner| Self::membership_for_canonical(inner, symbol_key))
    }

    /// 判断交易对是否在平仓列表中。
    ///
    /// 语义：原有 `dump_symbols` ∪（当 `UnimmrCloseGate` 任一 scope 处于
    /// `CloseAllowed` 时，叠加 `unimmr_close_symbols`）。也就是把 UniMMR
    /// 平仓列表看作"满足条件时才生效的 dump"，arb_decision 现有 dump 路径
    /// 自然把这些 symbol 走 close 流程，无需新增分支。
    pub fn is_in_dump_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| {
            if inner.canonical_filter_keys {
                let symbol = Self::normalize_for_filtering_cow(symbol);
                let symbol = symbol.as_ref();
                if inner.dump_symbols.contains(symbol) {
                    return true;
                }
                if inner.pos_dump_symbols.contains(symbol) {
                    return true;
                }
                return super::unimmr_close_gate::UnimmrCloseGate::instance().any_close_allowed()
                    && inner.unimmr_close_symbols.contains(symbol);
            }

            if Self::contains_normalized(&inner.dump_symbols, symbol) {
                return true;
            }
            if Self::contains_normalized(&inner.pos_dump_symbols, symbol) {
                return true;
            }
            if super::unimmr_close_gate::UnimmrCloseGate::instance().any_close_allowed()
                && Self::contains_normalized(&inner.unimmr_close_symbols, symbol)
            {
                return true;
            }
            false
        })
    }

    /// 判断交易对是否在正套建仓列表中
    pub fn is_in_fwd_trade_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| {
            if inner.canonical_filter_keys {
                let symbol = Self::normalize_for_filtering_cow(symbol);
                inner.fwd_trade_symbols.contains(symbol.as_ref())
            } else {
                Self::contains_normalized(&inner.fwd_trade_symbols, symbol)
            }
        })
    }

    /// 判断交易对是否在反套建仓列表中
    pub fn is_in_bwd_trade_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| {
            if inner.canonical_filter_keys {
                let symbol = Self::normalize_for_filtering_cow(symbol);
                inner.bwd_trade_symbols.contains(symbol.as_ref())
            } else {
                Self::contains_normalized(&inner.bwd_trade_symbols, symbol)
            }
        })
    }

    /// 判断交易对是否在 intra vol gate 列表中。
    pub fn is_in_vol_gate_list(&self, symbol: &str) -> bool {
        Self::with_inner(|inner| {
            if inner.canonical_filter_keys {
                let symbol = Self::normalize_for_filtering_cow(symbol);
                inner.vol_gate_symbols.contains(symbol.as_ref())
            } else {
                Self::contains_normalized(&inner.vol_gate_symbols, symbol)
            }
        })
    }

    /// 获取平仓列表
    pub fn get_dump_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.dump_symbols.iter().cloned().collect())
    }

    /// 获取 FR 仓位集中度风控维护的隐式平仓列表。
    pub fn get_pos_dump_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.pos_dump_symbols.iter().cloned().collect())
    }

    /// 获取正套建仓列表
    pub fn get_fwd_trade_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.fwd_trade_symbols.iter().cloned().collect())
    }

    /// 获取反套建仓列表
    pub fn get_bwd_trade_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.bwd_trade_symbols.iter().cloned().collect())
    }

    /// 获取 UniMMR 算法平仓列表（仅 FR 加载；其他模式为空）
    pub fn get_unimmr_close_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.unimmr_close_symbols.iter().cloned().collect())
    }

    /// 获取 intra vol gate 列表。
    pub fn get_vol_gate_symbols(&self) -> Vec<String> {
        Self::with_inner(|inner| inner.vol_gate_symbols.iter().cloned().collect())
    }

    /// 获取 online symbols（平仓 ∪ 正套/反套建仓列表）
    pub fn get_online_symbols(&self) -> Vec<String> {
        Self::with_inner(Self::collect_online)
    }

    /// 获取所有交易场所的 online symbols（基于当前 exchange）
    pub fn get_all_online_symbols(&self) -> FastHashMap<TradingVenue, Vec<String>> {
        Self::with_inner(|inner| {
            let mut result = fast_hash_map();
            if let Some(exchange) = inner.current_exchange {
                let online_set = Self::collect_online(inner);
                for venue in Self::exchange_to_venues(&exchange) {
                    result.insert(venue, online_set.clone());
                }
            }
            result
        })
    }

    pub async fn reload_fr_pos_dump_from_redis(
        &self,
        client: &mut RedisClient,
        key_suffix: &str,
        key_prefix: Option<&str>,
    ) -> Result<()> {
        let suffix = key_suffix.trim().to_ascii_lowercase();
        let prefix = normalize_symbol_list_key_prefix(key_prefix);
        let key = symbol_list_redis_key(
            prefix.as_deref(),
            DEFAULT_SYMBOL_NAMESPACE,
            "pos_dump_symbols",
            &suffix,
        );
        match client.get_string(&key).await? {
            Some(value) => match serde_json::from_str::<Vec<String>>(&value) {
                Ok(symbols) => {
                    Self::with_inner_mut(|inner| {
                        inner.pos_dump_symbols =
                            symbols.iter().map(|symbol| symbol.to_uppercase()).collect();
                        info!(
                            "更新 FR pos dump 列表 key={} count={}",
                            key,
                            inner.pos_dump_symbols.len()
                        );
                    });
                }
                Err(err) => {
                    warn!(
                        "FR pos dump 列表解析失败 key={} raw={} err={:#}，保留旧缓存",
                        key, value, err
                    );
                }
            },
            None => {
                Self::with_inner_mut(|inner| inner.pos_dump_symbols.clear());
            }
        }
        Ok(())
    }

    // ==================== 内部辅助方法 ====================

    /// 汇总 online symbols（平仓 ∪ 正套/反套建仓 ∪ UniMMR 算法平仓）
    fn collect_online(inner: &SymbolListInner) -> Vec<String> {
        let mut online_set = fast_hash_set();
        online_set.extend(inner.dump_symbols.iter().cloned());
        online_set.extend(inner.pos_dump_symbols.iter().cloned());
        online_set.extend(inner.fwd_trade_symbols.iter().cloned());
        online_set.extend(inner.bwd_trade_symbols.iter().cloned());
        online_set.extend(inner.unimmr_close_symbols.iter().cloned());
        online_set.into_iter().collect()
    }

    /// 判断集合中是否包含归一化后的 symbol（忽略分隔符和 OKEx SWAP 后缀）
    fn contains_normalized(set: &FastHashSet<String>, symbol: &str) -> bool {
        let target = Self::normalize_for_filtering(symbol);
        Self::contains_normalized_target(set, &target)
    }

    fn contains_normalized_target(set: &FastHashSet<String>, target: &str) -> bool {
        set.iter()
            .any(|symbol| Self::normalize_for_filtering(symbol) == target)
    }

    fn membership_for_canonical(inner: &SymbolListInner, symbol_key: &str) -> SymbolListMembership {
        let contains = |set: &FastHashSet<String>| {
            if inner.canonical_filter_keys {
                set.contains(symbol_key)
            } else {
                Self::contains_normalized_target(set, symbol_key)
            }
        };
        let in_dump = contains(&inner.dump_symbols)
            || contains(&inner.pos_dump_symbols)
            || (super::unimmr_close_gate::UnimmrCloseGate::instance().any_close_allowed()
                && contains(&inner.unimmr_close_symbols));

        SymbolListMembership {
            in_fwd: contains(&inner.fwd_trade_symbols),
            in_bwd: contains(&inner.bwd_trade_symbols),
            in_dump,
            in_vol_gate: contains(&inner.vol_gate_symbols),
        }
    }

    fn canonicalize_filter_set(set: &mut FastHashSet<String>) {
        let canonical = fast_hash_set_from_iter(
            set.drain()
                .map(|symbol| Self::normalize_for_filtering(symbol.trim()))
                .filter(|symbol| !symbol.is_empty()),
        );
        *set = canonical;
    }

    fn normalize_for_filtering_cow(symbol: &str) -> Cow<'_, str> {
        normalize_symbol_for_whitelist_cow(symbol, TradingVenue::OkexFutures)
    }

    /// 归一化符号用于白名单过滤：大写，移除 '-'/'_'，并去掉 "-SWAP"/"SWAP" 后缀
    /// 不区分 open/hedge，统一用 OkexFutures 触发去 SWAP 逻辑
    fn normalize_for_filtering(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    /// 将交易所枚举转换为所有对应的 TradingVenue
    fn exchange_to_venues(exchange: &Exchange) -> Vec<TradingVenue> {
        match exchange {
            Exchange::Binance => vec![
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
                TradingVenue::BinanceCoinFutures,
            ],
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
    use super::{symbol_list_redis_key, SymbolList};
    use std::borrow::Cow;

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

    #[test]
    fn pos_dump_key_is_env_scoped() {
        assert_eq!(
            symbol_list_redis_key(
                Some("binance_fr_arb02"),
                "fr",
                "pos_dump_symbols",
                "binance-margin_binance-futures"
            ),
            "binance_fr_arb02:fr_pos_dump_symbols:binance-margin_binance-futures"
        );
    }

    #[test]
    fn pos_dump_is_implicitly_part_of_dump_and_online_lists() {
        SymbolList::init_singleton().unwrap();
        SymbolList::with_inner_mut(|inner| {
            inner.pos_dump_symbols.insert("ETHUSDT".to_string());
        });

        let list = SymbolList::instance();
        assert!(list.is_in_dump_list("ETH-USDT-SWAP"));
        assert!(list.get_online_symbols().contains(&"ETHUSDT".to_string()));
    }

    #[test]
    fn intra_canonical_filter_set_deduplicates_aliases_and_uses_hash_lookup() {
        SymbolList::init_singleton().unwrap();
        SymbolList::with_inner_mut(|inner| {
            inner.fwd_trade_symbols = ["BTCUSDT", "btcusdt", "BTC_USDT", "BTC-USDT-SWAP"]
                .into_iter()
                .map(str::to_string)
                .collect();
            SymbolList::canonicalize_filter_set(&mut inner.fwd_trade_symbols);
            inner.canonical_filter_keys = true;
        });

        let list = SymbolList::instance();
        assert_eq!(list.get_fwd_trade_symbols(), vec!["BTCUSDT".to_string()]);
        assert!(list.is_in_fwd_trade_list("BTCUSDT"));
        assert!(list.is_in_fwd_trade_list("btcusdt"));
        assert!(list.is_in_fwd_trade_list("BTC_USDT"));
        assert!(list.is_in_fwd_trade_list("BTC-USDT-SWAP"));
    }

    #[test]
    fn canonical_filter_key_borrows_already_normalized_symbol() {
        let canonical = SymbolList::normalize_for_filtering_cow("BTCUSDT");
        assert!(matches!(canonical, Cow::Borrowed(_)));
        assert_eq!(canonical, "BTCUSDT");

        let venue_form = SymbolList::normalize_for_filtering_cow("btc-usdt-swap");
        assert!(matches!(venue_form, Cow::Owned(_)));
        assert_eq!(venue_form, "BTCUSDT");
    }

    #[test]
    fn membership_snapshots_all_lists_for_one_canonical_symbol() {
        SymbolList::init_singleton().unwrap();
        SymbolList::with_inner_mut(|inner| {
            inner.fwd_trade_symbols.insert("BTCUSDT".to_string());
            inner.dump_symbols.insert("ETHUSDT".to_string());
            inner.vol_gate_symbols.insert("BTCUSDT".to_string());
            inner.canonical_filter_keys = true;
        });

        let list = SymbolList::instance();
        let btc = list.membership("btc-usdt-swap");
        assert!(btc.in_fwd);
        assert!(!btc.in_bwd);
        assert!(!btc.in_dump);
        assert!(btc.in_vol_gate);
        assert!(btc.is_online());

        let eth = list.membership("ETHUSDT");
        assert!(!eth.in_fwd);
        assert!(eth.in_dump);
        assert!(eth.is_online());
    }
}
