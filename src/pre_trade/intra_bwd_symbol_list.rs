//! Intra arb 借贷白名单（来源于 trade_signal 维护的 `intra_bwd_trade_symbols` 列表）
//!
//! `handle_open_signal_common` 在 IntraArb 部署里默认禁用 PM 自动借币，
//! 但当 symbol 命中此白名单且账户为 UNIFIED 时允许放行。
//! 数据源 Redis key 与 trade_signal 共享：`intra_bwd_trade_symbols:<exchange>`。
//! 列表内容会在后台 60s 间隔刷新；查表前会做与 trade_signal 一致的归一化
//! （大写 + 去 '-'/'_' + 去 OKEx `-SWAP` 后缀），避免格式差异导致漏判。

use anyhow::{Context, Result};
use log::{info, warn};
use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

const REFRESH_INTERVAL_SECS: u64 = 60;

thread_local! {
    static SYMBOLS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// IntraBwdSymbolList 单例访问器（零大小类型）
pub struct IntraBwdSymbolList;

impl IntraBwdSymbolList {
    pub fn instance() -> Self {
        IntraBwdSymbolList
    }

    /// 归一化后判断是否在白名单中。未初始化（默认空集）时永远返回 false。
    pub fn contains(&self, symbol: &str) -> bool {
        let target = Self::normalize(symbol);
        SYMBOLS.with(|s| s.borrow().contains(&target))
    }

    /// 当前缓存大小（调试 / 监控用）
    pub fn len(&self) -> usize {
        SYMBOLS.with(|s| s.borrow().len())
    }

    pub fn is_empty(&self) -> bool {
        SYMBOLS.with(|s| s.borrow().is_empty())
    }

    fn normalize(symbol: &str) -> String {
        // 与 funding_rate::SymbolList 共用同一套 whitelist 归一化策略。
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    /// 从 Redis 拉取一次 `intra_bwd_trade_symbols:<key_suffix>` 并整体替换本地缓存。
    /// key 不存在时清空缓存（语义对齐"白名单空 = 默认禁借贷"）。
    pub async fn load_from_redis(redis: &RedisSettings, key_suffix: &str) -> Result<()> {
        let key_suffix = key_suffix.trim().to_ascii_lowercase();
        let key = format!("intra_bwd_trade_symbols:{key_suffix}");
        let mut client = RedisClient::connect(redis.clone())
            .await
            .context("connect redis for intra_bwd_trade_symbols")?;
        match client.get_string(&key).await? {
            Some(raw) => {
                let parsed: Vec<String> = serde_json::from_str(&raw).with_context(|| {
                    format!("Redis string '{}' 不是合法 JSON(Vec<String>): {}", key, raw)
                })?;
                let normalized: HashSet<String> =
                    parsed.iter().map(|s| Self::normalize(s)).collect();
                let count = normalized.len();
                SYMBOLS.with(|s| *s.borrow_mut() = normalized);
                info!("intra_bwd 借贷白名单已更新 key='{}' count={}", key, count);
            }
            None => {
                SYMBOLS.with(|s| s.borrow_mut().clear());
                info!(
                    "intra_bwd 借贷白名单 key='{}' 不存在，清空本地缓存（默认禁借贷）",
                    key
                );
            }
        }
        Ok(())
    }

    /// 启动后台 60s 间隔刷新任务（与 PreTradeParamsLoader 同款）。
    /// 启动处的同步加载已经做过一次，这里跳过 interval 的首个立即触发，避免重复读 Redis。
    pub fn start_background_refresh(redis: RedisSettings, key_suffix: String) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
            interval.tick().await; // 立即返回的第一次 tick：跳过，已有启动同步加载
            loop {
                interval.tick().await;
                if let Err(err) = IntraBwdSymbolList::load_from_redis(&redis, &key_suffix).await {
                    warn!(
                        "intra_bwd 借贷白名单后台刷新失败 key_suffix='{}': {:#}",
                        key_suffix, err
                    );
                }
            }
        });
        info!(
            "intra_bwd 借贷白名单后台刷新任务已启动 (间隔: {}s)",
            REFRESH_INTERVAL_SECS
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() {
        SYMBOLS.with(|s| s.borrow_mut().clear());
    }

    #[test]
    fn empty_default_does_not_contain_anything() {
        reset();
        assert!(!IntraBwdSymbolList::instance().contains("BTCUSDT"));
        assert!(IntraBwdSymbolList::instance().is_empty());
    }

    #[test]
    fn contains_normalizes_separator_and_case() {
        reset();
        SYMBOLS.with(|s| {
            let mut set = s.borrow_mut();
            set.insert(IntraBwdSymbolList::normalize("BTC-USDT"));
        });
        let list = IntraBwdSymbolList::instance();
        assert!(list.contains("BTCUSDT"));
        assert!(list.contains("btc_usdt"));
        assert!(list.contains("BTC-USDT-SWAP"));
        assert!(!list.contains("ETHUSDT"));
    }
}
