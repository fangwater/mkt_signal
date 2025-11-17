//! 费率拉取器 - 单例访问模式
//!
//! 负责从交易所 API 拉取资金费率和借贷利率数据，只拉取 online symbols
//! 拉取频率：5秒一次，整点时全量刷新
//! 目前支持：Binance（8h 资金费率 + 实时借贷利率）

use anyhow::Result;
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use tokio::time::{sleep, Duration};

type HmacSha256 = Hmac<Sha256>;

use super::common::{ArbDirection, OperationType};
use super::funding_rate_factor::FundingRateFactor;
use super::mkt_channel::MktChannel;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use crate::signal::common::TradingVenue;

/// Binance 资金费率历史数据项
#[derive(Debug, Deserialize)]
struct BinanceFundingHistItem {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: Option<i64>,
}

// 预测资费参数（硬编码）
const PREDICT_INTERVAL: usize = 6; // 用于预测的均值窗口
const PREDICT_NUM: usize = 1; // 预测回溯偏移

// 拉取频率：5秒
const FETCH_INTERVAL_SECS: u64 = 5;

/// 资金费率周期类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            FundingRatePeriod::Hours4 => (days * 6) as usize, // 每天6次
            FundingRatePeriod::Hours8 => (days * 3) as usize, // 每天3次
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
            FundingRatePeriod::Hours4 => daily_rate / 6.0, // 一天6次
            FundingRatePeriod::Hours8 => daily_rate / 3.0, // 一天3次
        }
    }

    /// 判断当前时间是否为整点小时
    pub fn is_hour_boundary() -> bool {
        let now = Utc::now();
        now.minute() == 0 && now.second() < FETCH_INTERVAL_SECS as u32
    }
}

/// Binance 借贷利率历史 API 响应项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceLendingRateHistoryItem {
    daily_interest_rate: String,
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

// 默认测试 symbols
const DEFAULT_TEST_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "SUIUSDT"];

// Binance API 端点
const BINANCE_LENDING_RATE_HISTORY_API: &str =
    "https://api.binance.com/sapi/v1/margin/interestRateHistory";

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

    /// 资金费率周期：TradingVenue -> Symbol -> FundingRatePeriod (推算得到)
    funding_periods: HashMap<TradingVenue, HashMap<String, FundingRatePeriod>>,

    /// Symbol 缓存：已拉取过的交易对
    symbol_cache: HashSet<String>,

    /// HTTP 客户端
    http_client: Client,

    /// 上次整点拉取的时间戳
    last_full_fetch_hour: Option<u32>,

    /// Binance API Key
    api_key: String,

    /// Binance API Secret
    api_secret: String,
}

impl RateFetcher {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        RateFetcher
    }

    /// 获取指定 symbol 的资金费率周期
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易所
    ///
    /// # 返回
    /// 对应 symbol 的资金费率周期（如果未找到，返回默认 8h）
    pub fn get_period(&self, symbol: &str, venue: TradingVenue) -> FundingRatePeriod {
        let symbol_upper = symbol.to_uppercase();
        Self::with_inner(|inner| {
            inner
                .funding_periods
                .get(&venue)
                .and_then(|map| map.get(&symbol_upper))
                .copied()
                .unwrap_or(FundingRatePeriod::Hours8) // 默认 8h
        })
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
    ///
    /// 从环境变量获取 API Key 和 Secret：
    /// - `BINANCE_API_KEY`
    /// - `BINANCE_API_SECRET`
    pub fn init_singleton() -> Result<()> {
        // 从环境变量获取 API Key 和 Secret
        let api_key = std::env::var("BINANCE_API_KEY").expect("环境变量 BINANCE_API_KEY 未设置");
        let api_secret =
            std::env::var("BINANCE_API_SECRET").expect("环境变量 BINANCE_API_SECRET 未设置");

        let inner = RateFetcherInner {
            funding_rates: HashMap::new(),
            lending_rates: HashMap::new(),
            funding_periods: HashMap::new(),
            symbol_cache: HashSet::new(),
            http_client: Client::builder().timeout(Duration::from_secs(10)).build()?,
            last_full_fetch_hour: None,
            api_key,
            api_secret,
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
            info!(
                "Binance 费率拉取任务启动（{}秒间隔，整点全量拉取）",
                FETCH_INTERVAL_SECS
            );

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
        let mut online_symbols = symbol_list.get_online_symbols(BINANCE_CONFIG.venue);

        // 如果没有 online symbols，使用默认测试 symbols
        if online_symbols.is_empty() {
            online_symbols = DEFAULT_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
            info!("使用默认测试 symbols: {:?}", online_symbols);
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
            // 打印信号状态表
            Self::print_signal_table(&online_symbols);
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
        let limit = BINANCE_CONFIG
            .period
            .calculate_limit(BINANCE_CONFIG.fetch_days);

        let mut success_count = 0;
        let mut fail_count = 0;

        for symbol in symbols {
            match Self::fetch_binance_funding_items(&client, symbol, limit).await {
                Ok(items) => {
                    let rates: Vec<f64> = items
                        .iter()
                        .filter_map(|item| item.funding_rate.parse::<f64>().ok())
                        .collect();

                    if !rates.is_empty() {
                        // 推断资金费率周期（4h or 8h）
                        let period =
                            match Self::infer_binance_funding_frequency(&client, symbol).await {
                                Some(freq) if freq == "4h" => FundingRatePeriod::Hours4,
                                _ => FundingRatePeriod::Hours8, // 默认 8h
                            };

                        Self::with_inner_mut(|inner| {
                            // 存储资金费率数据
                            let venue_rates = inner
                                .funding_rates
                                .entry(BINANCE_CONFIG.venue)
                                .or_insert_with(HashMap::new);
                            venue_rates.insert(symbol.clone(), rates);

                            // 存储周期信息
                            let venue_periods = inner
                                .funding_periods
                                .entry(BINANCE_CONFIG.venue)
                                .or_insert_with(HashMap::new);
                            venue_periods.insert(symbol.clone(), period);
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

    /// 拉取 Binance 借贷利率历史（逐个查询，取最近24条均值）
    async fn fetch_binance_lending_rates(
        online_symbols: &[String],
        save_result: bool,
    ) -> Result<()> {
        // 提取去重的基础资产
        let mut base_assets: Vec<String> = online_symbols
            .iter()
            .filter_map(|s| s.strip_suffix("USDT").map(String::from))
            .collect();
        base_assets.sort_unstable();
        base_assets.dedup();

        if base_assets.is_empty() {
            return Ok(());
        }

        let mut success = 0;
        let mut failed = 0;

        for asset in &base_assets {
            match Self::fetch_lending_rate_for_asset(asset).await {
                Ok(Some(daily_rate)) if save_result => {
                    let period_rate = BINANCE_CONFIG.period.convert_daily_rate(daily_rate);
                    Self::with_inner_mut(|inner| {
                        inner
                            .lending_rates
                            .entry(BINANCE_CONFIG.venue)
                            .or_insert_with(HashMap::new)
                            .insert(asset.to_uppercase(), period_rate);
                    });
                    success += 1;
                }
                Ok(Some(_)) => success += 1,
                Ok(None) => {}
                Err(e) => {
                    warn!("拉取 {} 借贷利率失败: {:?}", asset, e);
                    failed += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }

        if save_result && success > 0 {
            info!("借贷利率: 成功 {}, 失败 {}", success, failed);
        }

        Ok(())
    }

    /// 拉取单个 asset 的借贷利率（取最近24条均值）
    async fn fetch_lending_rate_for_asset(asset: &str) -> Result<Option<f64>> {
        let (client, api_key, api_secret) = Self::with_inner(|inner| {
            (
                inner.http_client.clone(),
                inner.api_key.clone(),
                inner.api_secret.clone(),
            )
        });

        let query = format!(
            "asset={}&isIsolated=FALSE&timestamp={}",
            asset,
            Utc::now().timestamp_millis()
        );
        let signature = Self::sign_query(&api_secret, &query)?;
        let url = format!(
            "{}?{}&signature={}",
            BINANCE_LENDING_RATE_HISTORY_API, query, signature
        );

        let response = client
            .get(&url)
            .header("X-MBX-APIKEY", &api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            warn!(
                "借贷利率 API 失败 [{}]: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
            return Ok(None);
        }

        let items: Vec<BinanceLendingRateHistoryItem> = response.json().await?;
        let rates: Vec<f64> = items
            .iter()
            .take(24)
            .filter_map(|item| item.daily_interest_rate.parse().ok())
            .collect();

        if rates.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rates.iter().sum::<f64>() / rates.len() as f64))
        }
    }

    /// 签名查询字符串
    fn sign_query(api_secret: &str, query: &str) -> Result<String> {
        let mut mac =
            HmacSha256::new_from_slice(api_secret.as_bytes()).expect("invalid API secret");
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    /// 打印费率表格（三线表）
    fn print_rate_table(symbols: &[String]) {
        let period = BINANCE_CONFIG.period;
        let period_str = match period {
            FundingRatePeriod::Hours4 => "4h",
            FundingRatePeriod::Hours8 => "8h",
        };

        let mut table_data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let fr = Self::instance()
                    .get_predicted_funding_rate(s, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v);
                let loan = Self::instance()
                    .get_predict_loan_rate(s, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), fr, loan)
            })
            .collect();
        table_data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        info!("┌───────────────────────────────────────────────────────────┐");
        info!(
            "│ Symbol         │ Predict FR ({})   │ Predict Loan ({}) │",
            period_str, period_str
        );
        info!("├───────────────────────────────────────────────────────────┤");

        for (symbol, fr, loan) in table_data {
            let fr_str = fr.map_or("             N/A".to_string(), |v| {
                format!("{:>15.6}%", v * 100.0)
            });
            let loan_str = loan.map_or("             N/A".to_string(), |v| {
                format!("{:>15.6}%", v * 100.0)
            });
            info!("│ {:<14} │ {} │ {} │", symbol, fr_str, loan_str);
        }

        info!("└───────────────────────────────────────────────────────────┘");
    }

    /// 打印信号状态表（带阈值对比）
    fn print_signal_table(symbols: &[String]) {
        let fr_factor = FundingRateFactor::instance();
        let spread_factor = SpreadFactor::instance();
        let rate_fetcher = Self::instance();
        let mkt_channel = MktChannel::instance();

        // 先打印 FR 阈值配置
        let period = BINANCE_CONFIG.period;
        let fwd_open_config = fr_factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Open);
        let fwd_close_config = fr_factor.get_threshold_config(period, ArbDirection::Forward, OperationType::Close);
        let bwd_open_config = fr_factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Open);
        let bwd_close_config = fr_factor.get_threshold_config(period, ArbDirection::Backward, OperationType::Close);

        info!("FR 阈值配置:");
        if let Some(cfg) = &fwd_open_config {
            info!("  ForwardOpen:   预测FR {:?} {:.4}%", cfg.compare_op, cfg.threshold * 100.0);
        }
        if let Some(cfg) = &fwd_close_config {
            info!("  ForwardClose:  当前FR_MA {:?} {:.4}%", cfg.compare_op, cfg.threshold * 100.0);
        }
        if let Some(cfg) = &bwd_open_config {
            info!("  BackwardOpen:  预测FR+Loan {:?} {:.4}%", cfg.compare_op, cfg.threshold * 100.0);
        }
        if let Some(cfg) = &bwd_close_config {
            info!("  BackwardClose: 当前FR_MA+Loan {:?} {:.4}%", cfg.compare_op, cfg.threshold * 100.0);
        }
        info!("");

        let mut table_data: Vec<_> = symbols
            .iter()
            .map(|symbol| {
                let period = rate_fetcher.get_period(symbol, BINANCE_CONFIG.venue);

                // 获取 FR、Loan、FR_MA 值
                let fr = rate_fetcher.get_predicted_funding_rate(symbol, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v).unwrap_or(0.0);
                let loan = rate_fetcher.get_predict_loan_rate(symbol, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v).unwrap_or(0.0);
                let fr_ma = mkt_channel.get_funding_rate_mean(symbol, BINANCE_CONFIG.venue)
                    .unwrap_or(0.0);
                let fr_loan = fr + loan;

                // 检查 FR 信号（优先级与 decision.rs::get_funding_rate_signal 保持一致）
                let forward_open = fr_factor.satisfy_forward_open(symbol, period);
                let forward_close = fr_factor.satisfy_forward_close(symbol, period, BINANCE_CONFIG.venue);
                let backward_open = fr_factor.satisfy_backward_open(symbol, period);
                let backward_close = fr_factor.satisfy_backward_close(symbol, period, BINANCE_CONFIG.venue);

                let fr_signal = if forward_close && backward_open {
                    "BwdOpen"
                } else if backward_close && forward_open {
                    "FwdOpen"
                } else if forward_close {
                    "FwdClose"
                } else if backward_close {
                    "BwdClose"
                } else if forward_open {
                    "FwdOpen"
                } else if backward_open {
                    "BwdOpen"
                } else {
                    "-"
                };

                // 检查 Spread 信号
                let spread_signal = if spread_factor.satisfy_forward_cancel(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "FwdCancel"
                } else if spread_factor.satisfy_backward_cancel(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "BwdCancel"
                } else if spread_factor.satisfy_forward_close(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "FwdClose"
                } else if spread_factor.satisfy_backward_close(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "BwdClose"
                } else if spread_factor.satisfy_forward_open(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "FwdOpen"
                } else if spread_factor.satisfy_backward_open(
                    TradingVenue::BinanceMargin, symbol, BINANCE_CONFIG.venue, symbol,
                ) {
                    "BwdOpen"
                } else {
                    "-"
                };

                (symbol.clone(), fr, fr_ma, loan, fr_loan, fr_signal, spread_signal)
            })
            .collect();
        table_data.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        info!("┌──────────────────────────────────────────────────────────────────────────────────────────────┐");
        info!("│ Symbol         │ 预测FR% │ FR_MA% │ Loan% │ FR+Loan% │ FR Sig     │ Spread Sig │");
        info!("├──────────────────────────────────────────────────────────────────────────────────────────────┤");

        for (symbol, fr, fr_ma, loan, fr_loan, fr_sig, spread_sig) in table_data {
            info!(
                "│ {:<14} │ {:>7.3} │ {:>6.3} │ {:>5.3} │ {:>8.3} │ {:<10} │ {:<10} │",
                symbol, fr * 100.0, fr_ma * 100.0, loan * 100.0, fr_loan * 100.0, fr_sig, spread_sig
            );
        }

        info!("└──────────────────────────────────────────────────────────────────────────────────────────────┘");
    }

    // ==================== 公开接口（仅2个）====================

    /// 获取预测资金费率（返回 symbol 对应周期和值）
    pub fn get_predicted_funding_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        if venue != BINANCE_CONFIG.venue {
            panic!(
                "RateFetcher::get_predicted_funding_rate currently only supports {:?}",
                BINANCE_CONFIG.venue
            );
        }
        let period = self.get_period(symbol, venue);
        let value = self.get_binance_predicted_funding_rate(symbol)?;
        Some((period, value))
    }

    /// 获取预测借贷利率（返回 symbol 对应周期和值）
    pub fn get_predict_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        if venue != BINANCE_CONFIG.venue {
            panic!(
                "RateFetcher::get_predict_loan_rate currently only supports {:?}",
                BINANCE_CONFIG.venue
            );
        }
        let period = self.get_period(symbol, venue);
        let value = self.get_binance_loan_rate(symbol, period)?;
        Some((period, value))
    }

    fn get_binance_predicted_funding_rate(&self, symbol: &str) -> Option<f64> {
        let rates = Self::get_funding_rates_internal(symbol, BINANCE_CONFIG.venue)?;
        Self::calculate_predicted_rate(&rates)
    }

    fn get_binance_loan_rate(&self, symbol: &str, period: FundingRatePeriod) -> Option<f64> {
        let base_asset = symbol.to_uppercase().strip_suffix("USDT")?.to_string();
        let daily_rate = Self::with_inner(|inner| {
            inner
                .lending_rates
                .get(&BINANCE_CONFIG.venue)?
                .get(&base_asset)
                .copied()
        })?;

        let stored_daily = match BINANCE_CONFIG.period {
            FundingRatePeriod::Hours4 => daily_rate * 6.0,
            FundingRatePeriod::Hours8 => daily_rate * 3.0,
        };

        Some(period.convert_daily_rate(stored_daily))
    }

    // ==================== 内部方法 ====================

    /// 获取资金费率历史（内部方法）
    fn get_funding_rates_internal(symbol: &str, venue: TradingVenue) -> Option<Vec<f64>> {
        let symbol_upper = symbol.to_uppercase();
        Self::with_inner(|inner| inner.funding_rates.get(&venue)?.get(&symbol_upper).cloned())
    }

    /// 计算预测资金费率（内部方法）
    fn calculate_predicted_rate(rates: &[f64]) -> Option<f64> {
        let n = rates.len();
        if n == 0 || n - 1 < PREDICT_NUM {
            return None;
        }

        let end = n - 1 - PREDICT_NUM;
        if end + 1 < PREDICT_INTERVAL {
            return None;
        }

        let start = end + 1 - PREDICT_INTERVAL;
        Some(rates[start..=end].iter().sum::<f64>() / PREDICT_INTERVAL as f64)
    }

    /// 从 Binance API 获取资金费率数据（近期）
    async fn fetch_binance_funding_items(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<BinanceFundingHistItem>> {
        let url = "https://fapi.binance.com/fapi/v1/fundingRate";
        let end_time = Utc::now().timestamp_millis();
        let start_time = end_time - 3 * 24 * 3600 * 1000; // 3d window
        let limit_s = limit.max(1).min(1000).to_string();
        let params = [
            ("symbol", symbol),
            ("startTime", &start_time.to_string()),
            ("endTime", &end_time.to_string()),
            ("limit", &limit_s),
        ];
        let resp = client.get(url).query(&params).send().await?;
        if !resp.status().is_success() {
            return Ok(vec![]);
        }
        let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
        items.sort_by_key(|it| it.funding_time.unwrap_or_default());
        Ok(items)
    }

    /// 从 Binance API 获取指定时间范围的资金费率历史
    #[allow(dead_code)]
    async fn fetch_binance_funding_history_range(
        client: &Client,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        limit: usize,
    ) -> Result<Vec<f64>> {
        let url = "https://fapi.binance.com/fapi/v1/fundingRate";
        let end = end_time.max(0);
        let start = start_time.min(end).max(0);
        let limit_s = limit.max(1).min(1000).to_string();
        let params = [
            ("symbol", symbol),
            ("startTime", &start.to_string()),
            ("endTime", &end.to_string()),
            ("limit", &limit_s),
        ];
        let resp = client.get(url).query(&params).send().await?;
        if !resp.status().is_success() {
            return Ok(vec![]);
        }
        let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
        items.sort_by_key(|it| it.funding_time.unwrap_or_default());
        let mut out = Vec::with_capacity(items.len());
        for it in items {
            if let Ok(v) = it.funding_rate.parse::<f64>() {
                out.push(v);
            }
        }
        if out.len() > limit {
            let drop_n = out.len() - limit;
            out.drain(0..drop_n);
        }
        Ok(out)
    }

    /// 推断 Binance 合约的资金费率频率（4h 或 8h）
    async fn infer_binance_funding_frequency(client: &Client, symbol: &str) -> Option<String> {
        let items = Self::fetch_binance_funding_items(client, symbol, 40)
            .await
            .ok()?;
        let mut times: Vec<i64> = items.iter().filter_map(|it| it.funding_time).collect();
        if times.len() < 3 {
            return Some("8h".to_string());
        }
        times.sort_unstable();
        let mut diffs: Vec<i64> = Vec::with_capacity(times.len().saturating_sub(1));
        for w in times.windows(2) {
            if let [a, b] = w {
                diffs.push(b - a);
            }
        }
        if diffs.is_empty() {
            return Some("8h".to_string());
        }
        diffs.sort_unstable();
        let median = diffs[diffs.len() / 2];
        // 阈值 6 小时分界
        let six_hours_ms = 6 * 3600 * 1000;
        let freq = if median <= six_hours_ms { "4h" } else { "8h" };
        debug!("频率推断: {} median={}ms => {}", symbol, median, freq);
        Some(freq.to_string())
    }
}
