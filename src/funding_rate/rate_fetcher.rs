//! 费率拉取器 - 单例访问模式
//!
//! 负责从交易所 API 拉取资金费率和借贷利率数据，只拉取 online symbols
//! 拉取频率：5秒一次，整点时全量刷新
//! 目前支持：Binance（8h 资金费率 + 实时借贷利率）

use anyhow::Result;
use chrono::{Timelike, Utc};
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use tokio::time::{sleep, Duration};

use crate::signal::common::TradingVenue;
use super::param_loader::fetch_binance_funding_items;
use super::symbol_list::SymbolList;

// 预测资费参数（硬编码）
const PREDICT_INTERVAL: usize = 6;   // 用于预测的均值窗口
const PREDICT_NUM: usize = 1;         // 预测回溯偏移

// 拉取频率：5秒
const FETCH_INTERVAL_SECS: u64 = 5;

/// 资金费率周期类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundingRatePeriod {
    /// 4小时周期（一天6次）
    Hours4,
    /// 8小时周期（一天3次）
    Hours8,
}

impl FundingRatePeriod {
    /// 计算指定天数需要拉取的条数
    pub fn calculate_limit(&self, days: u32) -> usize {
        match self {
            FundingRatePeriod::Hours4 => (days * 6) as usize,  // 每天6次
            FundingRatePeriod::Hours8 => (days * 3) as usize,  // 每天3次
        }
    }

    /// 将 24h 借贷利率转换为当前周期利率
    ///
    /// # 参数
    /// - `daily_rate`: 24小时借贷利率
    ///
    /// # 返回
    /// 对应周期的借贷利率
    pub fn convert_daily_rate(&self, daily_rate: f64) -> f64 {
        match self {
            FundingRatePeriod::Hours4 => daily_rate / 6.0,  // 一天6次
            FundingRatePeriod::Hours8 => daily_rate / 3.0,  // 一天3次
        }
    }

    /// 判断当前时间是否为整点小时
    pub fn is_hour_boundary() -> bool {
        let now = Utc::now();
        now.minute() == 0 && now.second() < FETCH_INTERVAL_SECS as u32
    }
}

/// Binance 借贷利率 API 响应项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceLendingRateItem {
    asset: String,
    next_hourly_interest_rate: String,
}

/// 交易所配置
#[derive(Debug, Clone)]
struct ExchangeConfig {
    venue: TradingVenue,
    period: FundingRatePeriod,
    fetch_days: u32,
}

// Binance 配置（8h 周期，拉取 3 天数据）
const BINANCE_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::BinanceUm,
    period: FundingRatePeriod::Hours8,
    fetch_days: 3,
};

// Binance API 端点
const BINANCE_LENDING_RATE_API: &str = "https://api.binance.com/sapi/v1/margin/next-hourly-interest-rate";

// Thread-local 单例存储
thread_local! {
    static RATE_FETCHER: RefCell<Option<RateFetcherInner>> = RefCell::new(None);
}

/// RateFetcher 单例访问器（零大小类型）
pub struct RateFetcher;

/// RateFetcher 内部实现
struct RateFetcherInner {
    /// 资金费率数据：TradingVenue -> Symbol -> Vec<f64>
    funding_rates: HashMap<TradingVenue, HashMap<String, Vec<f64>>>,

    /// 借贷利率数据：TradingVenue -> BaseAsset -> f64 (修正后的周期利率)
    lending_rates: HashMap<TradingVenue, HashMap<String, f64>>,

    /// Symbol 缓存：已拉取过的交易对
    symbol_cache: HashSet<String>,

    /// HTTP 客户端
    http_client: Client,

    /// 上次整点拉取的时间戳
    last_full_fetch_hour: Option<u32>,
}

impl RateFetcher {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        RateFetcher
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&RateFetcherInner) -> R,
    {
        RATE_FETCHER.with(|frf| {
            let frf_ref = frf.borrow();
            let inner = frf_ref.as_ref().expect("RateFetcher not initialized");
            f(inner)
        })
    }

    /// 访问内部状态的可变辅助方法（内部使用）
    fn with_inner_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut RateFetcherInner) -> R,
    {
        RATE_FETCHER.with(|frf| {
            let mut frf_ref = frf.borrow_mut();
            let inner = frf_ref.as_mut().expect("RateFetcher not initialized");
            f(inner)
        })
    }

    /// 初始化单例并启动定时拉取任务
    pub fn init_singleton() -> Result<()> {
        let inner = RateFetcherInner {
            funding_rates: HashMap::new(),
            lending_rates: HashMap::new(),
            symbol_cache: HashSet::new(),
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            last_full_fetch_hour: None,
        };

        RATE_FETCHER.with(|frf| {
            *frf.borrow_mut() = Some(inner);
        });

        info!(
            "RateFetcher 初始化完成 (fetch_interval={}s, predict_interval={}, predict_num={})",
            FETCH_INTERVAL_SECS, PREDICT_INTERVAL, PREDICT_NUM
        );

        // 启动 Binance 拉取任务
        Self::spawn_binance_fetch_task();

        Ok(())
    }

    /// 启动 Binance 费率拉取任务（5秒间隔）
    fn spawn_binance_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("Binance 费率拉取任务启动（{}秒间隔，整点全量拉取）", FETCH_INTERVAL_SECS);

            // 先立即拉取一次（全量）
            if let Err(e) = Self::fetch_binance_rates(true).await {
                warn!("初始拉取 Binance 费率失败: {:?}", e);
            }

            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;

                // 检查是否为整点
                let is_full_fetch = Self::should_do_full_fetch();

                if let Err(e) = Self::fetch_binance_rates(is_full_fetch).await {
                    warn!("拉取 Binance 费率失败: {:?}", e);
                }
            }
        });
    }

    /// 判断是否需要执行全量拉取
    fn should_do_full_fetch() -> bool {
        let now = Utc::now();
        let current_hour = now.hour();

        Self::with_inner_mut(|inner| {
            if FundingRatePeriod::is_hour_boundary() {
                if inner.last_full_fetch_hour != Some(current_hour) {
                    inner.last_full_fetch_hour = Some(current_hour);
                    return true;
                }
            }
            false
        })
    }

    /// 拉取 Binance 资金费率和借贷利率
    ///
    /// # 参数
    /// - `is_full_fetch`: true 表示全量拉取，false 表示增量拉取
    async fn fetch_binance_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let online_symbols = symbol_list.get_online_symbols(BINANCE_CONFIG.venue);

        if online_symbols.is_empty() {
            return Ok(());
        }

        // 检测新增 symbol
        let (new_symbols, all_changed) = Self::with_inner_mut(|inner| {
            let current_set: HashSet<String> = online_symbols.iter().cloned().collect();

            if is_full_fetch {
                // 全量拉取：重置缓存
                let old_cache = std::mem::replace(&mut inner.symbol_cache, current_set.clone());
                let changed = old_cache != current_set;
                (current_set.iter().cloned().collect::<Vec<_>>(), changed)
            } else {
                // 增量拉取：只拉取新symbol
                let new: Vec<String> = current_set
                    .difference(&inner.symbol_cache)
                    .cloned()
                    .collect();

                // 更新缓存
                inner.symbol_cache.extend(new.iter().cloned());

                let changed = !new.is_empty();
                (new, changed)
            }
        });

        // 打印日志
        let now = Utc::now();
        if is_full_fetch {
            info!(
                "[{}] 整点全量拉取: 当前 {} 个 symbol",
                now.format("%H:%M:%S"),
                online_symbols.len()
            );
        } else if !new_symbols.is_empty() {
            info!(
                "[{}] 增量拉取: 新增 {} 个 symbol, 当前 {} 个 symbol",
                now.format("%H:%M:%S"),
                new_symbols.len(),
                online_symbols.len()
            );
        } else {
            info!(
                "[{}] 定期检查: 当前 {} 个 symbol (无变化)",
                now.format("%H:%M:%S"),
                online_symbols.len()
            );
        }

        // 决定要拉取的 symbol 列表
        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            // 无新增，只拉取借贷利率（实时数据）
            return Self::fetch_binance_lending_rates(&online_symbols, false).await;
        };

        // 并发拉取资金费率和借贷利率
        let (funding_result, lending_result) = tokio::join!(
            Self::fetch_binance_funding_rates(symbols_to_fetch),
            Self::fetch_binance_lending_rates(&online_symbols, true)
        );

        if let Err(e) = funding_result {
            warn!("拉取资金费率失败: {:?}", e);
        }

        if let Err(e) = lending_result {
            warn!("拉取借贷利率失败: {:?}", e);
        }

        // 如果有变化或整点，打印表格
        if all_changed || is_full_fetch {
            Self::print_rate_table(&online_symbols);
        }

        Ok(())
    }

    /// 拉取 Binance 资金费率
    async fn fetch_binance_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let limit = BINANCE_CONFIG.period.calculate_limit(BINANCE_CONFIG.fetch_days);

        let mut success_count = 0;
        let mut fail_count = 0;

        for symbol in symbols {
            match fetch_binance_funding_items(&client, symbol, limit).await {
                Ok(items) => {
                    let rates: Vec<f64> = items
                        .iter()
                        .filter_map(|item| item.funding_rate.parse::<f64>().ok())
                        .collect();

                    if !rates.is_empty() {
                        Self::with_inner_mut(|inner| {
                            let venue_rates = inner
                                .funding_rates
                                .entry(BINANCE_CONFIG.venue)
                                .or_insert_with(HashMap::new);
                            venue_rates.insert(symbol.clone(), rates);
                        });
                        success_count += 1;
                    }
                }
                Err(e) => {
                    warn!("拉取 {} 资金费率失败: {:?}", symbol, e);
                    fail_count += 1;
                }
            }

            sleep(Duration::from_millis(100)).await;
        }

        if success_count + fail_count > 0 {
            info!("资金费率: 成功 {}, 失败 {}", success_count, fail_count);
        }

        Ok(())
    }

    /// 拉取 Binance 借贷利率（批量查询）
    ///
    /// # 参数
    /// - `online_symbols`: 当前在线的交易对列表
    /// - `save_result`: 是否保存结果（false时仅用于触发更新）
    async fn fetch_binance_lending_rates(online_symbols: &[String], save_result: bool) -> Result<()> {
        let client = Self::with_inner(|inner| inner.http_client.clone());

        // 提取基础资产
        let mut base_assets: Vec<String> = online_symbols
            .iter()
            .filter_map(|symbol| {
                if symbol.ends_with("USDT") {
                    let base = symbol.strip_suffix("USDT")?;
                    Some(base.to_string())
                } else {
                    None
                }
            })
            .collect();

        base_assets.sort();
        base_assets.dedup();

        if base_assets.is_empty() {
            return Ok(());
        }

        let chunks: Vec<_> = base_assets.chunks(20).collect();
        let mut total_success = 0;

        for chunk in chunks {
            let assets_param = chunk.join(",");
            let url = format!(
                "{}?assets={}&isIsolated=FALSE",
                BINANCE_LENDING_RATE_API, assets_param
            );

            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<Vec<BinanceLendingRateItem>>().await {
                            Ok(items) => {
                                if save_result {
                                    Self::with_inner_mut(|inner| {
                                        let venue_rates = inner
                                            .lending_rates
                                            .entry(BINANCE_CONFIG.venue)
                                            .or_insert_with(HashMap::new);

                                        for item in items {
                                            // 解析小时利率
                                            if let Ok(hourly_rate) = item.next_hourly_interest_rate.parse::<f64>() {
                                                // 转换为24h利率，然后根据周期修正
                                                let daily_rate = hourly_rate * 24.0;
                                                let period_rate = BINANCE_CONFIG.period.convert_daily_rate(daily_rate);

                                                venue_rates.insert(item.asset.to_uppercase(), period_rate);
                                                total_success += 1;
                                            }
                                        }
                                    });
                                }
                            }
                            Err(e) => {
                                warn!("解析借贷利率响应失败: {:?}", e);
                            }
                        }
                    } else {
                        warn!("借贷利率 API 请求失败: status={}", response.status());
                    }
                }
                Err(e) => {
                    warn!("请求借贷利率失败: {:?}", e);
                }
            }

            sleep(Duration::from_millis(200)).await;
        }

        if save_result && total_success > 0 {
            info!("借贷利率: 成功 {} 个资产", total_success);
        }

        Ok(())
    }

    /// 打印费率表格（三线表）
    fn print_rate_table(symbols: &[String]) {
        let mut table_data: Vec<(String, Option<f64>, Option<f64>)> = symbols
            .iter()
            .map(|symbol| {
                let predicted = Self::instance().get_predicted_funding_rate(symbol, BINANCE_CONFIG.venue);
                let lending = Self::instance().get_current_lending_rate(symbol, BINANCE_CONFIG.venue);
                (symbol.clone(), predicted, lending)
            })
            .collect();

        // 按 symbol 排序
        table_data.sort_by(|a, b| a.0.cmp(&b.0));

        // 打印表格
        info!("┌────────────────────────────────────────────────────────┐");
        info!("│ Symbol         │ Predicted Funding │ Lending Rate (8h) │");
        info!("├────────────────────────────────────────────────────────┤");

        for (symbol, predicted, lending) in table_data {
            let pred_str = predicted
                .map(|v| format!("{:>16.6}%", v * 100.0))
                .unwrap_or_else(|| format!("{:>17}", "N/A"));

            let lend_str = lending
                .map(|v| format!("{:>16.6}%", v * 100.0))
                .unwrap_or_else(|| format!("{:>17}", "N/A"));

            info!("│ {:<14} │ {} │ {} │", symbol, pred_str, lend_str);
        }

        info!("└────────────────────────────────────────────────────────┘");
    }

    // ==================== 资金费率查询接口 ====================

    /// 获取指定交易对的资金费率历史
    pub fn get_funding_rates(&self, symbol: &str, venue: TradingVenue) -> Option<Vec<f64>> {
        let symbol_upper = symbol.to_uppercase();
        Self::with_inner(|inner| {
            inner
                .funding_rates
                .get(&venue)
                .and_then(|venue_rates| venue_rates.get(&symbol_upper))
                .cloned()
        })
    }

    /// 计算预测资金费率（使用硬编码参数）
    pub fn get_predicted_funding_rate(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let rates = self.get_funding_rates(symbol, venue)?;
        let n = rates.len();

        if n == 0 {
            return None;
        }

        if n - 1 < PREDICT_NUM {
            return None;
        }

        let end = n - 1 - PREDICT_NUM;
        if end + 1 < PREDICT_INTERVAL {
            return None;
        }

        let start = end + 1 - PREDICT_INTERVAL;
        let slice = &rates[start..=end];
        let sum: f64 = slice.iter().sum();
        Some(sum / PREDICT_INTERVAL as f64)
    }

    // ==================== 借贷利率查询接口 ====================

    /// 获取当前借贷利率（已修正为对应周期，如 8h）
    ///
    /// # 参数
    /// - `symbol`: 交易对符号（例如 "BTCUSDT"）
    /// - `venue`: 交易场所
    ///
    /// # 返回
    /// - Some(f64): 修正后的周期借贷利率（8h 或 4h）
    /// - None: 未找到数据
    pub fn get_current_lending_rate(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let symbol_upper = symbol.to_uppercase();
        let base_asset = if symbol_upper.ends_with("USDT") {
            symbol_upper.strip_suffix("USDT")?.to_string()
        } else {
            symbol_upper
        };

        Self::with_inner(|inner| {
            inner
                .lending_rates
                .get(&venue)
                .and_then(|venue_rates| venue_rates.get(&base_asset))
                .copied()
        })
    }

    /// 获取指定基础资产的借贷利率（已修正为对应周期）
    pub fn get_lending_rate_by_asset(&self, base_asset: &str, venue: TradingVenue) -> Option<f64> {
        let asset_upper = base_asset.to_uppercase();
        Self::with_inner(|inner| {
            inner
                .lending_rates
                .get(&venue)
                .and_then(|venue_rates| venue_rates.get(&asset_upper))
                .copied()
        })
    }

    // ==================== 手动触发接口 ====================

    /// 手动触发立即拉取（全量）
    pub async fn fetch_now(&self) -> Result<()> {
        Self::fetch_binance_rates(true).await
    }
}
