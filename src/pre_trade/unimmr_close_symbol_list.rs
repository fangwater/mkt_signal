//! UniMMR 平仓调整 symbol list（仅 fr / intra / cross arb 部署使用）
//!
//! 配合账户级 AccountRisk 触发的算法平仓：当 UniMMR 跌破触发线，进程会按本表
//! 给出的 symbol 集合执行平仓动作。本模块只负责加载 Redis 配置与解析"实际可平
//! 集合"，**不**接交易决策，由后续 PR 在适当位置消费 [`effective_close_symbols`]。
//!
//! # Redis key 拼法（沿用 fwd/bwd/dump 同款规则）
//!
//! - 带 env prefix：`{prefix}:{namespace}_unimmr_close_symbols:{suffix}`
//! - 无 prefix：`{namespace}_unimmr_close_symbols:{suffix}`
//!
//! 其中：
//! - `prefix`：pre_trade `infer_dir_prefix_from_cwd()`（None 时省略前缀冒号）
//! - `namespace`：`fr` / `intra` / `cross`（由 ArbMode 派生；MM 不使用本表）
//! - `suffix`：`{open_venue.data_pub_slug()}_{hedge_venue.data_pub_slug()}`
//!
//! 例：`okex-intra-arb01:intra_unimmr_close_symbols:okex-margin_okex-futures`
//!
//! # Value 格式
//!
//! JSON `Vec<String>`，与 `dump_symbols` / `fwd_trade_symbols` / `bwd_trade_symbols`
//! 一致。symbol 经 [`normalize_symbol_for_whitelist`] 归一化后入集合。
//!
//! # 语义
//!
//! - key 不存在 / value 为空数组 / 解析失败 → 缓存清空
//! - "未配置 → 视为允许平所有 online symbol"，由 [`effective_close_symbols`] 的
//!   fallback 路径实现；本模块不在加载阶段做语义膨胀
//! - 后台 60s 间隔 refresh，启动时同步先加载一次

use anyhow::{Context, Result};
use log::{info, warn};
use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Duration;

use crate::common::redis_client::{RedisClient, RedisSettings};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

const POSITION_EPSILON: f64 = 1e-12;

const REFRESH_INTERVAL_SECS: u64 = 60;
const LIST_NAME: &str = "unimmr_close_symbols";

thread_local! {
    static SYMBOLS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// `UnimmrCloseSymbolList` 单例访问器（零大小类型）
pub struct UnimmrCloseSymbolList;

impl UnimmrCloseSymbolList {
    pub fn instance() -> Self {
        UnimmrCloseSymbolList
    }

    /// 归一化后判断 symbol 是否在表中。
    pub fn contains(&self, symbol: &str) -> bool {
        let target = Self::normalize(symbol);
        SYMBOLS.with(|s| s.borrow().contains(&target))
    }

    pub fn len(&self) -> usize {
        SYMBOLS.with(|s| s.borrow().len())
    }

    pub fn is_empty(&self) -> bool {
        SYMBOLS.with(|s| s.borrow().is_empty())
    }

    /// 当前缓存的归一化 symbol 集合快照（无序）。
    pub fn snapshot(&self) -> Vec<String> {
        SYMBOLS.with(|s| s.borrow().iter().cloned().collect())
    }

    /// 推导"本次实际可平 symbol 集合"：
    /// - 若 Redis 表非空：以本表（已归一化）为白名单
    /// - 若 Redis 表为空：回退到 `online_fallback`（视为允许平所有 online）
    /// - 在上述结果上，再按 `has_net_position` 过滤出当前确有 net 头寸的 symbol
    ///
    /// `online_fallback` 与 `has_net_position` 由调用方提供，便于按部署模式选择
    /// 数据源、便于单元测试。返回值 symbol 已用 [`normalize_symbol_for_whitelist`]
    /// 归一化，调用方做下游比对前请保持一致归一化口径。
    pub fn effective_close_symbols<I, F>(
        &self,
        online_fallback: I,
        has_net_position: F,
    ) -> Vec<String>
    where
        I: IntoIterator<Item = String>,
        F: Fn(&str) -> bool,
    {
        let universe: HashSet<String> = SYMBOLS.with(|s| s.borrow().clone());
        let candidates: HashSet<String> = if universe.is_empty() {
            online_fallback
                .into_iter()
                .map(|s| Self::normalize(&s))
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            universe
        };
        candidates
            .into_iter()
            .filter(|s| has_net_position(s))
            .collect()
    }

    fn normalize(symbol: &str) -> String {
        // 与 fwd/bwd/dump 同一归一化口径，避免格式差异漏判。
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    /// 便捷版：以 [`MonitorChannel`] 当前资产 universe 作 fallback，并用同一份
    /// `basic_state_snapshot` 推导"任一腿有头寸"集合（不要求净额非零）。
    ///
    /// 判据：`|open_qty| > eps` **或** `|hedge_qty| > eps` —— 完美套保位（net=0
    /// 但两腿各自有仓）也计入，因为算法平仓需要把两腿都收回。
    ///
    /// asset → symbol 用 `"{ASSET}USDT"` 拼装后再归一化（与 whitelist 口径一致），
    /// 假设所有 arb pair 都是 USDT 计价（仓内现状如此）。USDT 自身会被跳过
    /// （计价货币，不是被平的标的）。
    ///
    /// 返回归一化后的 symbol 列表（如 `BTCUSDT`），与 Redis 表内项可直接比对。
    pub fn effective_close_symbols_from_monitor(&self, mon: &MonitorChannel) -> Vec<String> {
        let (exposures, _, _, _, _) = mon.basic_state_snapshot();
        let positioned: HashSet<String> = exposures
            .iter()
            .filter(|(asset, &(open_qty, hedge_qty))| {
                !asset.eq_ignore_ascii_case("USDT")
                    && (open_qty.abs() > POSITION_EPSILON
                        || hedge_qty.abs() > POSITION_EPSILON)
            })
            .map(|(asset, _)| {
                let asset_upper = asset.to_ascii_uppercase();
                Self::normalize(&format!("{asset_upper}USDT"))
            })
            .filter(|s| !s.is_empty())
            .collect();

        let universe: HashSet<String> = SYMBOLS.with(|s| s.borrow().clone());
        if universe.is_empty() {
            positioned.into_iter().collect()
        } else {
            universe.intersection(&positioned).cloned().collect()
        }
    }

    /// 从 Redis 拉一次并整体替换本地缓存。key 不存在 / 解析失败 → 清空。
    pub async fn load_from_redis(
        redis: &RedisSettings,
        env_prefix: Option<&str>,
        namespace: &str,
        key_suffix: &str,
    ) -> Result<()> {
        let key = build_redis_key(env_prefix, namespace, key_suffix);
        let mut client = RedisClient::connect(redis.clone())
            .await
            .context("connect redis for unimmr_close_symbols")?;
        match client.get_string(&key).await? {
            Some(raw) => {
                match serde_json::from_str::<Vec<String>>(&raw) {
                    Ok(parsed) => {
                        let normalized: HashSet<String> = parsed
                            .iter()
                            .map(|s| Self::normalize(s))
                            .filter(|s| !s.is_empty())
                            .collect();
                        let count = normalized.len();
                        SYMBOLS.with(|s| *s.borrow_mut() = normalized);
                        info!(
                            "unimmr_close_symbols 已更新 key='{}' count={}",
                            key, count
                        );
                    }
                    Err(err) => {
                        SYMBOLS.with(|s| s.borrow_mut().clear());
                        warn!(
                            "unimmr_close_symbols 解析失败 key='{}' raw={} err={:#}，清空本地缓存",
                            key, raw, err
                        );
                    }
                }
            }
            None => {
                SYMBOLS.with(|s| s.borrow_mut().clear());
                info!(
                    "unimmr_close_symbols key='{}' 不存在，清空本地缓存（视为未配置）",
                    key
                );
            }
        }
        Ok(())
    }

    /// 启动后台 60s 间隔 refresh（与 PreTradeParamsLoader / IntraBwdSymbolList 同款）。
    /// 启动处的同步加载已经做过一次，这里跳过 interval 的首个立即触发，避免重复读 Redis。
    pub fn start_background_refresh(
        redis: RedisSettings,
        env_prefix: Option<String>,
        namespace: String,
        key_suffix: String,
    ) {
        tokio::task::spawn_local(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(REFRESH_INTERVAL_SECS));
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(err) = UnimmrCloseSymbolList::load_from_redis(
                    &redis,
                    env_prefix.as_deref(),
                    &namespace,
                    &key_suffix,
                )
                .await
                {
                    warn!(
                        "unimmr_close_symbols 后台刷新失败 ns='{}' suffix='{}' prefix={:?}: {:#}",
                        namespace, key_suffix, env_prefix, err
                    );
                }
            }
        });
        info!(
            "unimmr_close_symbols 后台刷新任务已启动 (间隔: {}s)",
            REFRESH_INTERVAL_SECS
        );
    }
}

/// 构造 Redis key（暴露给单测；运行时通过 `load_from_redis` 自动构造）。
fn build_redis_key(env_prefix: Option<&str>, namespace: &str, key_suffix: &str) -> String {
    let ns = namespace
        .trim()
        .trim_end_matches([':', '_', '-'])
        .to_ascii_lowercase();
    let suffix = key_suffix.trim().to_ascii_lowercase();
    let base = format!("{ns}_{LIST_NAME}:{suffix}");
    match env_prefix
        .map(str::trim)
        .map(|p| p.trim_end_matches(':'))
        .filter(|p| !p.is_empty())
    {
        Some(prefix) => format!("{prefix}:{base}"),
        None => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset() {
        SYMBOLS.with(|s| s.borrow_mut().clear());
    }

    fn install(symbols: &[&str]) {
        SYMBOLS.with(|s| {
            let mut set = s.borrow_mut();
            set.clear();
            for sym in symbols {
                set.insert(UnimmrCloseSymbolList::normalize(sym));
            }
        });
    }

    #[test]
    fn redis_key_without_prefix_matches_fwd_bwd_dump_shape() {
        assert_eq!(
            build_redis_key(None, "fr", "binance-margin_binance-futures"),
            "fr_unimmr_close_symbols:binance-margin_binance-futures"
        );
    }

    #[test]
    fn redis_key_with_prefix_is_env_scoped() {
        assert_eq!(
            build_redis_key(
                Some("okex-intra-arb01"),
                "intra",
                "okex-margin_okex-futures"
            ),
            "okex-intra-arb01:intra_unimmr_close_symbols:okex-margin_okex-futures"
        );
    }

    #[test]
    fn redis_key_normalizes_namespace_and_suffix_case_and_trailing_seps() {
        assert_eq!(
            build_redis_key(Some("env:"), "INTRA::", "Okex-Margin_Okex-Futures"),
            "env:intra_unimmr_close_symbols:okex-margin_okex-futures"
        );
    }

    #[test]
    fn empty_default_does_not_contain_anything() {
        reset();
        let list = UnimmrCloseSymbolList::instance();
        assert!(!list.contains("BTCUSDT"));
        assert!(list.is_empty());
    }

    #[test]
    fn contains_normalizes_separator_and_okex_swap_suffix() {
        install(&["BTC-USDT"]);
        let list = UnimmrCloseSymbolList::instance();
        assert!(list.contains("BTCUSDT"));
        assert!(list.contains("btc_usdt"));
        assert!(list.contains("BTC-USDT-SWAP"));
        assert!(!list.contains("ETHUSDT"));
    }

    #[test]
    fn effective_uses_redis_list_when_present_filtering_by_net_position() {
        install(&["BTCUSDT", "ETHUSDT"]);
        let list = UnimmrCloseSymbolList::instance();
        let fallback: Vec<String> = vec!["SOLUSDT".to_string(), "BTCUSDT".to_string()];
        let result = list.effective_close_symbols(fallback, |sym| sym == "BTCUSDT");
        assert_eq!(result, vec!["BTCUSDT".to_string()]);
    }

    #[test]
    fn effective_falls_back_to_online_when_redis_list_empty() {
        reset();
        let list = UnimmrCloseSymbolList::instance();
        let fallback: Vec<String> =
            vec!["BTC-USDT".to_string(), "ETHUSDT".to_string(), "SOLUSDT".to_string()];
        let with_pos: HashSet<String> = ["BTCUSDT", "SOLUSDT"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut result = list.effective_close_symbols(fallback, |sym| with_pos.contains(sym));
        result.sort();
        assert_eq!(result, vec!["BTCUSDT".to_string(), "SOLUSDT".to_string()]);
    }

    #[test]
    fn effective_returns_empty_when_both_empty() {
        reset();
        let list = UnimmrCloseSymbolList::instance();
        let result =
            list.effective_close_symbols(Vec::<String>::new(), |_| true);
        assert!(result.is_empty());
    }

    #[test]
    fn effective_returns_empty_when_redis_present_but_no_net_position() {
        install(&["BTCUSDT", "ETHUSDT"]);
        let list = UnimmrCloseSymbolList::instance();
        let result = list.effective_close_symbols(Vec::<String>::new(), |_| false);
        assert!(result.is_empty());
    }
}
