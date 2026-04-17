//! 费率拉取器 - 单例访问模式
//!
//! 负责从交易所 API 拉取资金费率和借贷利率数据，只拉取 online symbols
//! 拉取频率：5秒一次，整点时全量刷新
//! 支持：
//! - Binance: 4h/8h 资金费率 + 借贷利率
//! - OKEx: 1h/2h/4h/6h/8h 资金费率
//! - Bybit/Bitget/Gate: 8h 资金费率（Gate 同时支持签名拉取借贷利率）

use anyhow::Result;
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use tokio::time::{sleep, Duration};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

use super::common::{FundingRatePeriod, RateFetcherTrait};
use super::symbol_list::SymbolList;
use crate::common::exchange::Exchange;
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

// ==================== API 响应结构 ====================

/// Binance 资金费率历史数据项
#[derive(Debug, Deserialize)]
struct BinanceFundingHistItem {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: Option<i64>,
}

/// Binance 借贷利率历史 API 响应项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceLendingRateHistoryItem {
    daily_interest_rate: String,
}

/// OKEx 资金费率历史响应
#[derive(Debug, Deserialize)]
struct OkexFundingRateHistoryResponse {
    code: String,
    data: Vec<OkexFundingRateHistoryItem>,
}

/// OKEx 资金费率历史项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkexFundingRateHistoryItem {
    funding_rate: String,
    /// 资金费率时间（毫秒时间戳）
    #[serde(default)]
    funding_time: Option<String>,
}

/// OKEx 当前资金费率响应
#[derive(Debug, Deserialize)]
struct OkexCurrentFundingRateResponse {
    code: String,
    data: Vec<OkexCurrentFundingRateItem>,
}

/// OKEx 当前资金费率项（用于获取周期）
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkexCurrentFundingRateItem {
    funding_time: String,
    next_funding_time: String,
}

/// Bitget 资金费率历史响应
#[derive(Debug, Deserialize)]
struct BitgetFundingRateResponse {
    code: String,
    data: BitgetFundingRateData,
}

/// Bitget 资金费率历史数据
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetFundingRateData {
    result_list: Vec<BitgetFundingRateItem>,
}

/// Bitget 资金费率历史项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetFundingRateItem {
    funding_rate: String,
    funding_rate_timestamp: String,
}

/// Bybit v5 资金费率历史响应
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitFundingRateResponse {
    ret_code: i32,
    result: BybitFundingRateResult,
}

/// Bybit v5 资金费率结果
#[derive(Debug, Deserialize)]
struct BybitFundingRateResult {
    list: Vec<BybitFundingRateItem>,
}

/// Bybit v5 资金费率历史项
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitFundingRateItem {
    #[allow(dead_code)]
    symbol: String,
    funding_rate: String,
    funding_rate_timestamp: String,
}

/// Gate.io 资金费率历史项
/// 响应格式: [{"t": 1543968000, "r": "0.000157"}, ...]
#[derive(Debug, Deserialize)]
struct GateFundingRateItem {
    /// 时间戳（秒）
    t: i64,
    /// 资金费率
    r: String,
}

/// Gate.io 历史借贷利率响应
#[derive(Debug, Deserialize)]
struct GateLoanRateResponse {
    #[allow(dead_code)]
    currency: String,
    #[serde(default)]
    rates: Vec<GateLoanRateItem>,
}

/// Gate.io 历史借贷利率项（每小时一个点）
#[derive(Debug, Deserialize)]
struct GateLoanRateItem {
    /// 时间戳（毫秒）
    #[allow(dead_code)]
    time: i64,
    /// 利率字符串
    rate: String,
}

/// Okex 借贷利率缓存服务响应
#[derive(Debug, Deserialize)]
struct OkexLoanRateResponse {
    #[serde(default)]
    entries: Vec<OkexLoanRateEntry>,
}

/// Okex 借贷利率条目
#[derive(Debug, Deserialize)]
struct OkexLoanRateEntry {
    #[serde(default)]
    rate: Option<f64>,
}

// ==================== 常量配置 ====================

// 预测资费参数
const PREDICT_INTERVAL: usize = 6;
const PREDICT_NUM: usize = 0;

// 拉取频率：5秒
const FETCH_INTERVAL_SECS: u64 = 5;

// 默认拉取天数
const DEFAULT_FETCH_DAYS: u32 = 3;

/// 判断当前时间是否为整点小时
fn is_hour_boundary() -> bool {
    let now = Utc::now();
    now.minute() == 0 && now.second() < FETCH_INTERVAL_SECS as u32
}

/// 交易所配置
#[derive(Debug, Clone)]
pub struct ExchangeConfig {
    pub venue: TradingVenue,
    pub period: FundingRatePeriod,
    pub fetch_days: u32,
}

// Binance 配置
pub const BINANCE_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::BinanceFutures,
    period: FundingRatePeriod::Hours8,
    fetch_days: DEFAULT_FETCH_DAYS,
};

// OKEx 配置
pub const OKEX_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::OkexFutures,
    period: FundingRatePeriod::Hours8,
    fetch_days: DEFAULT_FETCH_DAYS,
};

// Bitget 配置
pub const BITGET_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::BitgetFutures,
    period: FundingRatePeriod::Hours8,
    fetch_days: DEFAULT_FETCH_DAYS,
};

// Bybit 配置
pub const BYBIT_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::BybitFutures,
    period: FundingRatePeriod::Hours8,
    fetch_days: DEFAULT_FETCH_DAYS,
};

// Gate 配置
pub const GATE_CONFIG: ExchangeConfig = ExchangeConfig {
    venue: TradingVenue::GateFutures,
    period: FundingRatePeriod::Hours8,
    fetch_days: DEFAULT_FETCH_DAYS,
};

// 默认测试 symbols
const BINANCE_TEST_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"];
const OKEX_TEST_SYMBOLS: &[&str] = &["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"];
const BITGET_TEST_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const BYBIT_TEST_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const GATE_TEST_SYMBOLS: &[&str] = &["BTC_USDT", "ETH_USDT", "SOL_USDT"];

// API 端点
const BINANCE_FUNDING_RATE_API: &str = "https://fapi.binance.com/fapi/v1/fundingRate";
const BINANCE_LENDING_RATE_API: &str = "https://api.binance.com/sapi/v1/margin/interestRateHistory";
const OKEX_FUNDING_RATE_HISTORY_API: &str =
    "https://www.okx.com/api/v5/public/funding-rate-history";
const OKEX_FUNDING_RATE_API: &str = "https://www.okx.com/api/v5/public/funding-rate";
const BITGET_FUNDING_RATE_HISTORY_API: &str =
    "https://api.bitget.com/api/v3/market/history-fund-rate";
const BYBIT_FUNDING_RATE_HISTORY_API: &str = "https://api.bybit.com/v5/market/funding/history";
// Gate 仅支持实盘，Base URL 固定写死，不做配置化
const GATE_API_BASE_URL: &str = "https://api.gateio.ws/api/v4";
const GATE_FUNDING_RATE_HISTORY_PATH: &str = "/futures/usdt/funding_rate";
const GATE_LOAN_RATE_HISTORY_PATH: &str = "/unified/history_loan_rate";
const GATE_LOAN_RATE_ESTIMATE_PATH: &str = "/unified/estimate_rate";
const DEFAULT_OKEX_LOAN_RATE_URL: &str = "http://127.0.0.1:28901/rates";

// ==================== Thread-local 单例 ====================

thread_local! {
    static RATE_FETCHER: RefCell<Option<RateFetcherInner>> = RefCell::new(None);
}

/// RateFetcher 单例访问器
pub struct RateFetcher;

/// 借贷利率缓存
#[derive(Debug, Clone)]
struct LendingRateCache {
    predict_daily_rate: f64,
    current_daily_rate: f64,
    #[allow(dead_code)]
    raw_daily_rates: Vec<f64>,
}

/// 交易所状态
#[derive(Debug, Default)]
struct VenueState {
    symbol_cache: HashSet<String>,
    last_full_fetch_hour: Option<u32>,
    initial_ready: bool,
    last_not_ready_reason: Option<String>,
    last_not_ready_symbol: Option<String>,
    last_missing_fr: usize,
    last_missing_loan: usize,
}

/// RateFetcher 内部实现
struct RateFetcherInner {
    /// 资金费率数据：TradingVenue -> Symbol -> Vec<f64>
    funding_rates: HashMap<TradingVenue, HashMap<String, Vec<f64>>>,
    /// 借贷利率数据：TradingVenue -> BaseAsset -> LendingRateCache
    lending_rates: HashMap<TradingVenue, HashMap<String, LendingRateCache>>,
    /// 资金费率周期：TradingVenue -> Symbol -> FundingRatePeriod
    funding_periods: HashMap<TradingVenue, HashMap<String, FundingRatePeriod>>,
    /// 交易所状态：TradingVenue -> VenueState
    venue_states: HashMap<TradingVenue, VenueState>,
    /// HTTP 客户端
    http_client: Client,
    /// Binance API Key (可选)
    binance_api_key: Option<String>,
    /// Binance API Secret (可选)
    binance_api_secret: Option<String>,
    /// Okex 借贷利率缓存服务地址
    okex_loan_rate_url: Option<String>,
    /// Gate API Key（必填，仅 Gate 使用）
    gate_api_key: Option<String>,
    /// Gate API Secret（必填，仅 Gate 使用）
    gate_api_secret: Option<String>,
    /// 已启动的交易所任务（避免重复 spawn）
    started_exchanges: HashSet<Exchange>,
}

impl RateFetcher {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        RateFetcher
    }

    /// 访问内部状态
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

    /// 访问内部状态（可变）
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

    /// 确保单例已初始化
    fn ensure_initialized() -> Result<()> {
        RATE_FETCHER.with(|frf| {
            if frf.borrow().is_none() {
                let inner = RateFetcherInner {
                    funding_rates: HashMap::new(),
                    lending_rates: HashMap::new(),
                    funding_periods: HashMap::new(),
                    venue_states: HashMap::new(),
                    http_client: Client::builder().timeout(Duration::from_secs(10)).build()?,
                    binance_api_key: None,
                    binance_api_secret: None,
                    okex_loan_rate_url: None,
                    gate_api_key: None,
                    gate_api_secret: None,
                    started_exchanges: HashSet::new(),
                };
                *frf.borrow_mut() = Some(inner);
            }
            Ok(())
        })
    }

    fn exchange_from_venue(venue: TradingVenue) -> Exchange {
        match venue {
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Exchange::Binance,
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => Exchange::Okex,
            TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => Exchange::Bitget,
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => Exchange::Bybit,
            TradingVenue::GateMargin | TradingVenue::GateFutures => Exchange::Gate,
            TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures => {
                Exchange::Hyperliquid
            }
        }
    }

    // ==================== 初始化接口 ====================

    /// 根据 Exchange 初始化对应的费率拉取任务
    pub fn init(exchange: Exchange) -> Result<()> {
        Self::ensure_initialized()?;

        let is_first_init = Self::with_inner_mut(|inner| inner.started_exchanges.insert(exchange));
        if !is_first_init {
            debug!("RateFetcher: {exchange} already initialized, skip spawn");
            return Ok(());
        }

        match exchange {
            Exchange::Binance => {
                let api_key = std::env::var("BINANCE_API_KEY").ok();
                let api_secret = std::env::var("BINANCE_API_SECRET").ok();

                Self::with_inner_mut(|inner| {
                    inner.binance_api_key = api_key.clone();
                    inner.binance_api_secret = api_secret.clone();
                    inner.venue_states.entry(BINANCE_CONFIG.venue).or_default();
                });

                let has_lending = api_key.is_some() && api_secret.is_some();
                info!(
                    "RateFetcher: Binance 初始化完成 (lending_rate={})",
                    has_lending
                );
                Self::spawn_binance_fetch_task();
            }
            Exchange::Okex => {
                let loan_url = std::env::var("OKEX_LOAN_RATE_URL")
                    .unwrap_or_else(|_| DEFAULT_OKEX_LOAN_RATE_URL.to_string());
                Self::with_inner_mut(|inner| {
                    inner.venue_states.entry(OKEX_CONFIG.venue).or_default();
                    inner.okex_loan_rate_url = Some(loan_url.clone());
                });
                info!("RateFetcher: OKEx 初始化完成 (loan_rate_url={})", loan_url);
                Self::spawn_okex_fetch_task();
            }
            Exchange::Bitget => {
                Self::with_inner_mut(|inner| {
                    inner.venue_states.entry(BITGET_CONFIG.venue).or_default();
                });
                info!("RateFetcher: Bitget 初始化完成");
                Self::spawn_bitget_fetch_task();
            }
            Exchange::Bybit => {
                Self::with_inner_mut(|inner| {
                    inner.venue_states.entry(BYBIT_CONFIG.venue).or_default();
                });
                info!("RateFetcher: Bybit 初始化完成");
                Self::spawn_bybit_fetch_task();
            }
            Exchange::Gate => {
                let api_key = std::env::var("GATE_API_KEY")
                    .expect("Gate 需要环境变量 GATE_API_KEY（Access Key），未设置将无法启动");
                let api_secret = std::env::var("GATE_API_SECRET")
                    .expect("Gate 需要环境变量 GATE_API_SECRET（Secret Key），未设置将无法启动");
                Self::with_inner_mut(|inner| {
                    inner.venue_states.entry(GATE_CONFIG.venue).or_default();
                    inner.gate_api_key = Some(api_key.clone());
                    inner.gate_api_secret = Some(api_secret.clone());
                });
                info!("RateFetcher: Gate 初始化完成");
                Self::spawn_gate_fetch_task();
            }
            Exchange::Hyperliquid => {
                Self::with_inner_mut(|inner| {
                    inner
                        .venue_states
                        .entry(TradingVenue::HyperliquidFutures)
                        .or_default();
                });
                info!("RateFetcher: Hyperliquid initialized (fetch task not enabled)");
            }
        }

        Ok(())
    }

    pub fn init_for_venues(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let open_exchange = Self::exchange_from_venue(open_venue);
        let hedge_exchange = Self::exchange_from_venue(hedge_venue);

        if open_exchange == hedge_exchange {
            return Self::init(open_exchange);
        }

        Self::init(open_exchange)?;
        Self::init(hedge_exchange)?;
        Ok(())
    }

    // ==================== Binance 拉取任务 ====================

    fn spawn_binance_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("Binance 费率拉取任务启动（{}秒间隔）", FETCH_INTERVAL_SECS);
            if let Err(e) = Self::fetch_binance_rates(true).await {
                warn!("初始拉取 Binance 费率失败: {:?}", e);
            }
            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;
                let is_full = Self::should_do_full_fetch(BINANCE_CONFIG.venue);
                if let Err(e) = Self::fetch_binance_rates(is_full).await {
                    warn!("拉取 Binance 费率失败: {:?}", e);
                }
            }
        });
    }

    async fn fetch_binance_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let mut online_symbols = symbol_list.get_online_symbols();
        if online_symbols.is_empty() {
            online_symbols = BINANCE_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
        }

        let (new_symbols, all_changed) =
            Self::update_symbol_cache(BINANCE_CONFIG.venue, &online_symbols, is_full_fetch);
        Self::log_fetch_status("Binance", is_full_fetch, &new_symbols, online_symbols.len());

        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            return Self::fetch_binance_lending_rates(&online_symbols, false).await;
        };

        let (fr_result, lr_result) = tokio::join!(
            Self::fetch_binance_funding_rates(symbols_to_fetch),
            Self::fetch_binance_lending_rates(&online_symbols, true)
        );
        if let Err(e) = fr_result {
            warn!("Binance 资金费率拉取失败: {:?}", e);
        }
        if let Err(e) = lr_result {
            warn!("Binance 借贷利率拉取失败: {:?}", e);
        }

        Self::check_initial_rates_if_needed(
            BINANCE_CONFIG.venue,
            &online_symbols,
            true,
            all_changed || is_full_fetch,
        );

        if all_changed || is_full_fetch {
            Self::print_binance_rate_table(&online_symbols);
        }
        Ok(())
    }

    async fn fetch_binance_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let limit = BINANCE_CONFIG
            .period
            .calculate_limit(BINANCE_CONFIG.fetch_days);
        let mut success = 0;
        let mut fail = 0;

        for symbol in symbols {
            match Self::fetch_binance_funding_items(&client, symbol, limit).await {
                Ok(items) => {
                    let rates: Vec<f64> = items
                        .iter()
                        .filter_map(|it| it.funding_rate.parse().ok())
                        .collect();
                    if !rates.is_empty() {
                        let period = match Self::infer_binance_period(&client, symbol).await {
                            Some(p) if p == "4h" => FundingRatePeriod::Hours4,
                            _ => FundingRatePeriod::Hours8,
                        };
                        Self::with_inner_mut(|inner| {
                            inner
                                .funding_rates
                                .entry(BINANCE_CONFIG.venue)
                                .or_default()
                                .insert(symbol.clone(), rates);
                            inner
                                .funding_periods
                                .entry(BINANCE_CONFIG.venue)
                                .or_default()
                                .insert(symbol.clone(), period);
                        });
                        success += 1;
                    }
                }
                Err(e) => {
                    warn!("Binance {} 资金费率失败: {:?}", symbol, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if success + fail > 0 {
            info!("Binance 资金费率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    async fn fetch_binance_funding_items(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<BinanceFundingHistItem>> {
        let limit_s = limit.max(1).min(1000).to_string();
        let resp = client
            .get(BINANCE_FUNDING_RATE_API)
            .query(&[("symbol", symbol), ("limit", &limit_s)])
            .send()
            .await?;
        if !resp.status().is_success() {
            return Ok(vec![]);
        }
        let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
        items.sort_by_key(|it| it.funding_time.unwrap_or_default());
        Ok(items)
    }

    async fn infer_binance_period(client: &Client, symbol: &str) -> Option<String> {
        let items = Self::fetch_binance_funding_items(client, symbol, 40)
            .await
            .ok()?;
        let mut times: Vec<i64> = items.iter().filter_map(|it| it.funding_time).collect();
        if times.len() < 3 {
            return Some("8h".to_string());
        }
        times.sort_unstable();
        let mut diffs: Vec<i64> = times.windows(2).map(|w| w[1] - w[0]).collect();
        if diffs.is_empty() {
            return Some("8h".to_string());
        }
        diffs.sort_unstable();
        let median = diffs[diffs.len() / 2];
        let freq = if median <= 6 * 3600 * 1000 {
            "4h"
        } else {
            "8h"
        };
        debug!("Binance {} 周期推断: {}ms => {}", symbol, median, freq);
        Some(freq.to_string())
    }

    async fn fetch_binance_lending_rates(symbols: &[String], save: bool) -> Result<()> {
        let has_keys = Self::with_inner(|inner| {
            inner.binance_api_key.is_some() && inner.binance_api_secret.is_some()
        });
        if !has_keys {
            return Ok(());
        }

        let assets = Self::collect_base_assets(symbols);
        if assets.is_empty() {
            return Ok(());
        }

        let mut success = 0;
        let mut fail = 0;
        for asset in &assets {
            match Self::fetch_binance_lending_rate_for_asset(asset).await {
                Ok(Some(rates)) if save => {
                    let predict = if rates.is_empty() {
                        0.0
                    } else {
                        rates.iter().sum::<f64>() / rates.len() as f64
                    };
                    let current = if rates.is_empty() {
                        0.0
                    } else {
                        let n = 3.min(rates.len());
                        rates.iter().take(n).sum::<f64>() / n as f64
                    };
                    Self::with_inner_mut(|inner| {
                        inner
                            .lending_rates
                            .entry(BINANCE_CONFIG.venue)
                            .or_default()
                            .insert(
                                asset.to_uppercase(),
                                LendingRateCache {
                                    predict_daily_rate: predict,
                                    current_daily_rate: current,
                                    raw_daily_rates: rates,
                                },
                            );
                    });
                    success += 1;
                }
                Ok(Some(_)) => success += 1,
                Ok(None) => {}
                Err(e) => {
                    warn!("Binance {} 借贷利率失败: {:?}", asset, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if save && success > 0 {
            info!("Binance 借贷利率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    async fn fetch_binance_lending_rate_for_asset(asset: &str) -> Result<Option<Vec<f64>>> {
        let (client, api_key, api_secret) = Self::with_inner(|inner| {
            (
                inner.http_client.clone(),
                inner.binance_api_key.clone(),
                inner.binance_api_secret.clone(),
            )
        });
        let (api_key, api_secret) = match (api_key, api_secret) {
            (Some(k), Some(s)) => (k, s),
            _ => return Ok(None),
        };

        let query = format!(
            "asset={}&isIsolated=FALSE&timestamp={}",
            asset,
            Utc::now().timestamp_millis()
        );
        let signature = Self::sign_binance_query(&api_secret, &query)?;
        let url = format!(
            "{}?{}&signature={}",
            BINANCE_LENDING_RATE_API, query, signature
        );

        let resp = client
            .get(&url)
            .header("X-MBX-APIKEY", &api_key)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Ok(None);
        }

        let items: Vec<BinanceLendingRateHistoryItem> = resp.json().await?;
        let rates: Vec<f64> = items
            .iter()
            .take(24)
            .filter_map(|it| it.daily_interest_rate.parse().ok())
            .collect();
        Ok(if rates.is_empty() { None } else { Some(rates) })
    }

    fn sign_binance_query(secret: &str, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("invalid secret");
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn print_binance_rate_table(symbols: &[String]) {
        let mut data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let fr = RateFetcher::instance()
                    .get_predicted_funding_rate(s, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v);
                let loan = RateFetcher::instance()
                    .get_predict_loan_rate(s, BINANCE_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), fr, loan)
            })
            .collect();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        info!("┌─────────────────────────────────────────────────────────┐");
        info!("│ Binance        │ Predict FR      │ Predict Loan        │");
        info!("├─────────────────────────────────────────────────────────┤");
        for (sym, fr, loan) in data {
            let fr_s = fr.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            let loan_s = loan.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            info!("│ {:<14} │ {} │ {} │", sym, fr_s, loan_s);
        }
        info!("└─────────────────────────────────────────────────────────┘");
    }

    // ==================== OKEx 拉取任务 ====================

    fn spawn_okex_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("OKEx 费率拉取任务启动（{}秒间隔）", FETCH_INTERVAL_SECS);
            if let Err(e) = Self::fetch_okex_rates(true).await {
                warn!("初始拉取 OKEx 费率失败: {:?}", e);
            }
            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;
                let is_full = Self::should_do_full_fetch(OKEX_CONFIG.venue);
                if let Err(e) = Self::fetch_okex_rates(is_full).await {
                    warn!("拉取 OKEx 费率失败: {:?}", e);
                }
            }
        });
    }

    /// 将本地 symbol 规范化为用于存储/查询的 key（去除分隔符和 SWAP 后缀）
    fn normalize_okex_symbol(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    /// 将本地 symbol 转换为 OKEx API 需要的 instId（如 BTCUSDT -> BTC-USDT-SWAP）
    fn format_okex_inst_id(symbol: &str) -> String {
        let mut upper = symbol.to_uppercase().replace('_', "-");
        if let Some(stripped) = upper.strip_suffix("-SWAP") {
            upper = stripped.to_string();
        } else if let Some(stripped) = upper.strip_suffix("SWAP") {
            upper = stripped.to_string();
        }

        if upper.contains('-') {
            let cleaned = upper.trim_end_matches('-');
            if cleaned.ends_with("-SWAP") {
                cleaned.to_string()
            } else {
                format!("{cleaned}-SWAP")
            }
        } else if let Some(base) = upper.strip_suffix("USDT") {
            format!("{}-USDT-SWAP", base.trim_end_matches('-'))
        } else if let Some(base) = upper.strip_suffix("USDC") {
            format!("{}-USDC-SWAP", base.trim_end_matches('-'))
        } else if let Some(base) = upper.strip_suffix("USD") {
            format!("{}-USD-SWAP", base.trim_end_matches('-'))
        } else {
            format!("{upper}-SWAP")
        }
    }

    async fn fetch_okex_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let mut online_symbols = symbol_list.get_online_symbols();
        if online_symbols.is_empty() {
            online_symbols = OKEX_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
        }

        let (new_symbols, all_changed) =
            Self::update_symbol_cache(OKEX_CONFIG.venue, &online_symbols, is_full_fetch);
        Self::log_fetch_status("OKEx", is_full_fetch, &new_symbols, online_symbols.len());

        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            return Self::fetch_okex_lending_rates(&online_symbols).await;
        };

        Self::fetch_okex_funding_rates(symbols_to_fetch).await?;
        Self::fetch_okex_lending_rates(&online_symbols).await?;

        Self::check_initial_rates_if_needed(
            OKEX_CONFIG.venue,
            &online_symbols,
            true,
            all_changed || is_full_fetch,
        );

        if all_changed || is_full_fetch {
            Self::print_okex_rate_table(&online_symbols);
        }
        Ok(())
    }

    async fn fetch_okex_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let mut success = 0;
        let mut fail = 0;

        for symbol in symbols {
            let inst_id = Self::format_okex_inst_id(symbol);
            let store_key = Self::normalize_okex_symbol(symbol);

            // 先获取周期
            let period = match Self::fetch_okex_period(&client, &inst_id).await {
                Ok(p) => p,
                Err(e) => {
                    warn!("OKEx {} 周期获取失败: {:?}", inst_id, e);
                    FundingRatePeriod::Hours8
                }
            };
            let limit = period.calculate_limit(OKEX_CONFIG.fetch_days);

            match Self::fetch_okex_funding_history(&client, &inst_id, limit).await {
                Ok(rates) if !rates.is_empty() => {
                    Self::with_inner_mut(|inner| {
                        inner
                            .funding_rates
                            .entry(OKEX_CONFIG.venue)
                            .or_default()
                            .insert(store_key.clone(), rates);
                        inner
                            .funding_periods
                            .entry(OKEX_CONFIG.venue)
                            .or_default()
                            .insert(store_key.clone(), period);
                    });
                    success += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("OKEx {} 资金费率失败: {:?}", inst_id, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if success + fail > 0 {
            info!("OKEx 资金费率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    async fn fetch_okex_period(client: &Client, symbol: &str) -> Result<FundingRatePeriod> {
        let resp = client
            .get(OKEX_FUNDING_RATE_API)
            .query(&[("instId", symbol)])
            .send()
            .await?;
        if resp.status().is_success() {
            let data: OkexCurrentFundingRateResponse = resp.json().await?;
            if data.code == "0" && !data.data.is_empty() {
                let item = &data.data[0];
                let ft: i64 = item.funding_time.parse().unwrap_or(0);
                let nft: i64 = item.next_funding_time.parse().unwrap_or(0);
                let hours = (nft - ft) / 1000 / 3600;
                if let Some(period) = Self::map_hours_to_period(hours) {
                    debug!("OKEx {} 周期: {}h (current)", symbol, hours);
                    return Ok(period);
                }
            }
        }

        // 当前接口失败或返回异常时，使用历史数据推断周期
        if let Some(period) = Self::fetch_okex_period_from_history(client, symbol).await? {
            debug!(
                "OKEx {} 周期: {} (history fallback)",
                symbol,
                period.as_str()
            );
            return Ok(period);
        }

        Ok(FundingRatePeriod::Hours8)
    }

    async fn fetch_okex_period_from_history(
        client: &Client,
        symbol: &str,
    ) -> Result<Option<FundingRatePeriod>> {
        let resp = client
            .get(OKEX_FUNDING_RATE_HISTORY_API)
            .query(&[("instId", symbol), ("limit", "2")])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let data: OkexFundingRateHistoryResponse = resp.json().await?;
        if data.code != "0" || data.data.len() < 2 {
            return Ok(None);
        }

        let ts1 = data.data[0]
            .funding_time
            .as_deref()
            .and_then(|s| s.parse::<i64>().ok());
        let ts2 = data.data[1]
            .funding_time
            .as_deref()
            .and_then(|s| s.parse::<i64>().ok());

        if let (Some(t1), Some(t2)) = (ts1, ts2) {
            let hours = (t1 - t2).abs() / 1000 / 3600;
            if let Some(period) = Self::map_hours_to_period(hours) {
                return Ok(Some(period));
            }
        }

        Ok(None)
    }

    fn map_hours_to_period(hours: i64) -> Option<FundingRatePeriod> {
        match hours {
            1 => Some(FundingRatePeriod::Hours1),
            2 => Some(FundingRatePeriod::Hours2),
            4 => Some(FundingRatePeriod::Hours4),
            6 => Some(FundingRatePeriod::Hours6),
            8 => Some(FundingRatePeriod::Hours8),
            _ => None,
        }
    }

    async fn fetch_okex_funding_history(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<f64>> {
        let limit_s = limit.max(1).min(100).to_string();
        let resp = client
            .get(OKEX_FUNDING_RATE_HISTORY_API)
            .query(&[("instId", symbol), ("limit", &limit_s)])
            .send()
            .await?;
        if !resp.status().is_success() {
            return Ok(vec![]);
        }

        let data: OkexFundingRateHistoryResponse = resp.json().await?;
        if data.code != "0" {
            return Ok(vec![]);
        }

        // OKEx 返回最新在前，反转为时间顺序
        let mut rates: Vec<f64> = data
            .data
            .iter()
            .filter_map(|it| it.funding_rate.parse().ok())
            .collect();
        rates.reverse();
        Ok(rates)
    }

    fn print_okex_rate_table(symbols: &[String]) {
        let mut data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let key = Self::normalize_symbol_for_lookup(s, OKEX_CONFIG.venue);
                let period = Self::with_inner(|inner| {
                    inner
                        .funding_periods
                        .get(&OKEX_CONFIG.venue)
                        .and_then(|m| m.get(&key))
                        .copied()
                        .unwrap_or(FundingRatePeriod::Hours8)
                });
                let fr = RateFetcher::instance()
                    .get_predicted_funding_rate(s, OKEX_CONFIG.venue)
                    .map(|(_, v)| v);
                let loan = RateFetcher::instance()
                    .get_predict_loan_rate(s, OKEX_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), period, fr, loan)
            })
            .collect();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        info!("┌──────────────────────────────────────────────────────────────┐");
        info!("│ OKEx                │ Period │ Predict FR      │ Predict Loan │");
        info!("├──────────────────────────────────────────────────────────────┤");
        for (sym, period, fr, loan) in data {
            let fr_s = fr.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            let loan_s = loan.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            info!(
                "│ {:<19} │ {:>6} │ {} │ {} │",
                sym,
                period.as_str(),
                fr_s,
                loan_s
            );
        }
        info!("└──────────────────────────────────────────────────────────────┘");
    }

    async fn fetch_okex_lending_rates(symbols: &[String]) -> Result<()> {
        let assets = Self::collect_base_assets(symbols);
        if assets.is_empty() {
            return Ok(());
        }

        let mut success = 0;
        let mut fail = 0;

        for asset in assets {
            match Self::fetch_okex_lending_rate_for_asset(&asset).await {
                Ok(Some(rates)) => {
                    let predict = if rates.is_empty() {
                        0.0
                    } else {
                        rates.iter().sum::<f64>() / rates.len() as f64
                    };
                    let current = if rates.is_empty() {
                        0.0
                    } else {
                        let n = 3.min(rates.len());
                        rates.iter().rev().take(n).copied().sum::<f64>() / n as f64
                    };
                    Self::with_inner_mut(|inner| {
                        inner
                            .lending_rates
                            .entry(OKEX_CONFIG.venue)
                            .or_default()
                            .insert(
                                asset.to_uppercase(),
                                LendingRateCache {
                                    predict_daily_rate: predict,
                                    current_daily_rate: current,
                                    raw_daily_rates: rates,
                                },
                            );
                    });
                    success += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("OKEx {} 借贷利率失败: {:?}", asset, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }

        if success + fail > 0 {
            debug!("OKEx 借贷利率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    async fn fetch_okex_lending_rate_for_asset(asset: &str) -> Result<Option<Vec<f64>>> {
        let client = Self::with_inner(|inner| inner.http_client.clone());
        let url = Self::okex_loan_rate_url();

        let resp = client.get(&url).query(&[("symbol", asset)]).send().await?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let data: OkexLoanRateResponse = resp.json().await?;
        let rates: Vec<f64> = data.entries.iter().filter_map(|entry| entry.rate).collect();

        Ok(if rates.is_empty() { None } else { Some(rates) })
    }

    // ==================== Bitget 拉取任务 ====================

    fn spawn_bitget_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("Bitget 费率拉取任务启动（{}秒间隔）", FETCH_INTERVAL_SECS);
            if let Err(e) = Self::fetch_bitget_rates(true).await {
                warn!("初始拉取 Bitget 费率失败: {:?}", e);
            }
            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;
                let is_full = Self::should_do_full_fetch(BITGET_CONFIG.venue);
                if let Err(e) = Self::fetch_bitget_rates(is_full).await {
                    warn!("拉取 Bitget 费率失败: {:?}", e);
                }
            }
        });
    }

    async fn fetch_bitget_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let mut online_symbols = symbol_list.get_online_symbols();
        if online_symbols.is_empty() {
            online_symbols = BITGET_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
        }

        let (new_symbols, all_changed) =
            Self::update_symbol_cache(BITGET_CONFIG.venue, &online_symbols, is_full_fetch);
        Self::log_fetch_status("Bitget", is_full_fetch, &new_symbols, online_symbols.len());

        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            return Ok(());
        };

        Self::fetch_bitget_funding_rates(symbols_to_fetch).await?;

        Self::check_initial_rates_if_needed(
            BITGET_CONFIG.venue,
            &online_symbols,
            false,
            all_changed || is_full_fetch,
        );

        if all_changed || is_full_fetch {
            Self::print_bitget_rate_table(&online_symbols);
        }
        Ok(())
    }

    async fn fetch_bitget_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let mut success = 0;
        let mut fail = 0;

        for symbol in symbols {
            // 先拉取历史数据（用于推断周期和获取费率）
            match Self::fetch_bitget_funding_history(&client, symbol, 30).await {
                Ok((rates, period)) if !rates.is_empty() => {
                    Self::with_inner_mut(|inner| {
                        inner
                            .funding_rates
                            .entry(BITGET_CONFIG.venue)
                            .or_default()
                            .insert(symbol.clone(), rates);
                        inner
                            .funding_periods
                            .entry(BITGET_CONFIG.venue)
                            .or_default()
                            .insert(symbol.clone(), period);
                    });
                    success += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Bitget {} 资金费率失败: {:?}", symbol, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if success + fail > 0 {
            info!("Bitget 资金费率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    /// 从 Bitget API 获取资金费率历史，同时推断周期
    async fn fetch_bitget_funding_history(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<(Vec<f64>, FundingRatePeriod)> {
        let limit_s = limit.max(1).min(100).to_string();
        let resp = client
            .get(BITGET_FUNDING_RATE_HISTORY_API)
            .query(&[
                ("category", "USDT-FUTURES"),
                ("symbol", symbol),
                ("limit", &limit_s),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        let data: BitgetFundingRateResponse = resp.json().await?;
        if data.code != "00000" || data.data.result_list.is_empty() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        let items = &data.data.result_list;

        // 推断周期：计算前两条记录的时间差
        let period = if items.len() >= 2 {
            let t1: i64 = items[0].funding_rate_timestamp.parse().unwrap_or(0);
            let t2: i64 = items[1].funding_rate_timestamp.parse().unwrap_or(0);
            if t1 > 0 && t2 > 0 {
                let hours = (t1 - t2).abs() / 1000 / 3600;
                match hours {
                    1 => FundingRatePeriod::Hours1,
                    2 => FundingRatePeriod::Hours2,
                    4 => FundingRatePeriod::Hours4,
                    6 => FundingRatePeriod::Hours6,
                    _ => FundingRatePeriod::Hours8,
                }
            } else {
                FundingRatePeriod::Hours8
            }
        } else {
            FundingRatePeriod::Hours8
        };

        // Bitget 返回最新在前，反转为时间顺序
        let mut rates: Vec<f64> = items
            .iter()
            .filter_map(|it| it.funding_rate.parse().ok())
            .collect();
        rates.reverse();

        debug!("Bitget {} 周期: {}", symbol, period.as_str());
        Ok((rates, period))
    }

    fn print_bitget_rate_table(symbols: &[String]) {
        let mut data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let period = Self::with_inner(|inner| {
                    inner
                        .funding_periods
                        .get(&BITGET_CONFIG.venue)
                        .and_then(|m| m.get(s))
                        .copied()
                        .unwrap_or(FundingRatePeriod::Hours8)
                });
                let fr = RateFetcher::instance()
                    .get_predicted_funding_rate(s, BITGET_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), period, fr)
            })
            .collect();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        info!("┌────────────────────────────────────────────────┐");
        info!("│ Bitget              │ Period │ Predict FR      │");
        info!("├────────────────────────────────────────────────┤");
        for (sym, period, fr) in data {
            let fr_s = fr.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            info!("│ {:<19} │ {:>6} │ {} │", sym, period.as_str(), fr_s);
        }
        info!("└────────────────────────────────────────────────┘");
    }

    // ==================== Bybit 拉取任务 ====================

    fn spawn_bybit_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("Bybit 费率拉取任务启动（{}秒间隔）", FETCH_INTERVAL_SECS);
            if let Err(e) = Self::fetch_bybit_rates(true).await {
                warn!("初始拉取 Bybit 费率失败: {:?}", e);
            }
            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;
                let is_full = Self::should_do_full_fetch(BYBIT_CONFIG.venue);
                if let Err(e) = Self::fetch_bybit_rates(is_full).await {
                    warn!("拉取 Bybit 费率失败: {:?}", e);
                }
            }
        });
    }

    async fn fetch_bybit_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let mut online_symbols = symbol_list.get_online_symbols();
        if online_symbols.is_empty() {
            online_symbols = BYBIT_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
        }

        let (new_symbols, all_changed) =
            Self::update_symbol_cache(BYBIT_CONFIG.venue, &online_symbols, is_full_fetch);
        Self::log_fetch_status("Bybit", is_full_fetch, &new_symbols, online_symbols.len());

        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            return Ok(());
        };

        Self::fetch_bybit_funding_rates(symbols_to_fetch).await?;

        Self::check_initial_rates_if_needed(
            BYBIT_CONFIG.venue,
            &online_symbols,
            false,
            all_changed || is_full_fetch,
        );

        if all_changed || is_full_fetch {
            Self::print_bybit_rate_table(&online_symbols);
        }
        Ok(())
    }

    async fn fetch_bybit_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let mut success = 0;
        let mut fail = 0;

        for symbol in symbols {
            match Self::fetch_bybit_funding_history(&client, symbol, 30).await {
                Ok((rates, period)) if !rates.is_empty() => {
                    Self::with_inner_mut(|inner| {
                        inner
                            .funding_rates
                            .entry(BYBIT_CONFIG.venue)
                            .or_default()
                            .insert(symbol.clone(), rates);
                        inner
                            .funding_periods
                            .entry(BYBIT_CONFIG.venue)
                            .or_default()
                            .insert(symbol.clone(), period);
                    });
                    success += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Bybit {} 资金费率失败: {:?}", symbol, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if success + fail > 0 {
            info!("Bybit 资金费率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    /// 从 Bybit v5 API 获取资金费率历史，同时推断周期
    async fn fetch_bybit_funding_history(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<(Vec<f64>, FundingRatePeriod)> {
        let limit_s = limit.max(1).min(200).to_string();
        let resp = client
            .get(BYBIT_FUNDING_RATE_HISTORY_API)
            .query(&[
                ("category", "linear"),
                ("symbol", symbol),
                ("limit", &limit_s),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        let data: BybitFundingRateResponse = resp.json().await?;
        if data.ret_code != 0 || data.result.list.is_empty() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        let items = &data.result.list;

        // 推断周期：计算前两条记录的时间差
        let period = if items.len() >= 2 {
            let t1: i64 = items[0].funding_rate_timestamp.parse().unwrap_or(0);
            let t2: i64 = items[1].funding_rate_timestamp.parse().unwrap_or(0);
            if t1 > 0 && t2 > 0 {
                let hours = (t1 - t2).abs() / 1000 / 3600;
                match hours {
                    1 => FundingRatePeriod::Hours1,
                    2 => FundingRatePeriod::Hours2,
                    4 => FundingRatePeriod::Hours4,
                    6 => FundingRatePeriod::Hours6,
                    _ => FundingRatePeriod::Hours8,
                }
            } else {
                FundingRatePeriod::Hours8
            }
        } else {
            FundingRatePeriod::Hours8
        };

        // Bybit 返回最新在前，反转为时间顺序
        let mut rates: Vec<f64> = items
            .iter()
            .filter_map(|it| it.funding_rate.parse().ok())
            .collect();
        rates.reverse();

        debug!("Bybit {} 周期: {}", symbol, period.as_str());
        Ok((rates, period))
    }

    fn print_bybit_rate_table(symbols: &[String]) {
        let mut data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let period = Self::with_inner(|inner| {
                    inner
                        .funding_periods
                        .get(&BYBIT_CONFIG.venue)
                        .and_then(|m| m.get(s))
                        .copied()
                        .unwrap_or(FundingRatePeriod::Hours8)
                });
                let fr = RateFetcher::instance()
                    .get_predicted_funding_rate(s, BYBIT_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), period, fr)
            })
            .collect();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        info!("┌────────────────────────────────────────────────┐");
        info!("│ Bybit               │ Period │ Predict FR      │");
        info!("├────────────────────────────────────────────────┤");
        for (sym, period, fr) in data {
            let fr_s = fr.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            info!("│ {:<19} │ {:>6} │ {} │", sym, period.as_str(), fr_s);
        }
        info!("└────────────────────────────────────────────────┘");
    }

    // ==================== Gate 拉取任务 ====================

    /// Gate 存储/查询 key 规范化（移除分隔符）
    fn normalize_gate_symbol(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::GateFutures)
    }

    /// 将本地 symbol 转换为 Gate futures API 需要的 contract（如 BTCUSDT -> BTC_USDT）
    fn format_gate_contract(symbol: &str) -> String {
        let mut upper = symbol.to_uppercase().replace('-', "_");
        if let Some(stripped) = upper.strip_suffix("_SWAP") {
            upper = stripped.to_string();
        } else if let Some(stripped) = upper.strip_suffix("-SWAP") {
            upper = stripped.to_string();
        } else if let Some(stripped) = upper.strip_suffix("SWAP") {
            upper = stripped.to_string();
        }

        if upper.contains('_') {
            return upper;
        }

        for quote in ["USDT", "USDC", "USD"] {
            if let Some(base) = upper.strip_suffix(quote) {
                let base = base.trim_end_matches('_').trim_end_matches('-');
                if !base.is_empty() {
                    return format!("{}_{}", base, quote);
                }
            }
        }

        upper
    }

    fn sign_gate_request(
        secret: &str,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
        timestamp: i64,
    ) -> Result<String> {
        let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
        let to_sign = format!(
            "{}\n{}\n{}\n{}\n{}",
            method.to_uppercase(),
            path,
            query,
            body_hash,
            timestamp
        );
        let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).expect("invalid secret");
        mac.update(to_sign.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn spawn_gate_fetch_task() {
        tokio::task::spawn_local(async move {
            info!("Gate 费率拉取任务启动（{}秒间隔）", FETCH_INTERVAL_SECS);
            if let Err(e) = Self::fetch_gate_rates(true).await {
                warn!("初始拉取 Gate 费率失败: {:?}", e);
            }
            loop {
                sleep(Duration::from_secs(FETCH_INTERVAL_SECS)).await;
                let is_full = Self::should_do_full_fetch(GATE_CONFIG.venue);
                if let Err(e) = Self::fetch_gate_rates(is_full).await {
                    warn!("拉取 Gate 费率失败: {:?}", e);
                }
            }
        });
    }

    async fn fetch_gate_rates(is_full_fetch: bool) -> Result<()> {
        let symbol_list = SymbolList::instance();
        let mut online_symbols = symbol_list.get_online_symbols();
        if online_symbols.is_empty() {
            online_symbols = GATE_TEST_SYMBOLS.iter().map(|s| s.to_string()).collect();
        }

        let (new_symbols, all_changed) =
            Self::update_symbol_cache(GATE_CONFIG.venue, &online_symbols, is_full_fetch);
        Self::log_fetch_status("Gate", is_full_fetch, &new_symbols, online_symbols.len());

        let symbols_to_fetch = if is_full_fetch {
            &online_symbols
        } else if !new_symbols.is_empty() {
            &new_symbols
        } else {
            // 无增量时不拉资金费率；借贷利率也沿用上次结果
            return Ok(());
        };

        Self::fetch_gate_funding_rates(symbols_to_fetch).await?;
        if is_full_fetch || all_changed || !new_symbols.is_empty() {
            if let Err(e) = Self::fetch_gate_lending_rates(&online_symbols).await {
                warn!("Gate 借贷利率拉取失败: {:?}", e);
            }
        }

        Self::check_initial_rates_if_needed(
            GATE_CONFIG.venue,
            &online_symbols,
            true,
            all_changed || is_full_fetch,
        );

        if all_changed || is_full_fetch {
            Self::print_gate_rate_table(&online_symbols);
        }
        Ok(())
    }

    async fn fetch_gate_funding_rates(symbols: &[String]) -> Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }

        let client = Self::with_inner(|inner| inner.http_client.clone());
        let limit = GATE_CONFIG.period.calculate_limit(GATE_CONFIG.fetch_days);
        let mut success = 0;
        let mut fail = 0;

        for symbol in symbols {
            let contract = Self::format_gate_contract(symbol);
            let store_key = Self::normalize_gate_symbol(symbol);
            match Self::fetch_gate_funding_history(&client, &contract, limit).await {
                Ok((rates, period)) if !rates.is_empty() => {
                    Self::with_inner_mut(|inner| {
                        inner
                            .funding_rates
                            .entry(GATE_CONFIG.venue)
                            .or_default()
                            .insert(store_key.clone(), rates);
                        inner
                            .funding_periods
                            .entry(GATE_CONFIG.venue)
                            .or_default()
                            .insert(store_key.clone(), period);
                    });
                    success += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Gate {} 资金费率失败: {:?}", contract, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        if success + fail > 0 {
            info!("Gate 资金费率: 成功 {}, 失败 {}", success, fail);
        }
        Ok(())
    }

    async fn fetch_gate_lending_rates(symbols: &[String]) -> Result<()> {
        let assets = Self::collect_base_assets(symbols);
        if assets.is_empty() {
            return Ok(());
        }

        // 1) 先拉历史借贷利率，用于预测（predict_daily_rate）
        let mut predict_map: HashMap<String, LendingRateCache> = HashMap::new();
        let mut success = 0;
        let mut fail = 0;
        for asset in &assets {
            match Self::fetch_gate_lending_rate_for_asset(asset).await {
                Ok(Some(rates)) => {
                    let predict = if rates.is_empty() {
                        0.0
                    } else {
                        rates.iter().sum::<f64>() / rates.len() as f64
                    };
                    predict_map.insert(
                        asset.to_uppercase(),
                        LendingRateCache {
                            predict_daily_rate: predict,
                            current_daily_rate: 0.0, // 先占位，后面用 estimate 覆盖
                            raw_daily_rates: rates,
                        },
                    );
                    success += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("Gate {} 历史借贷利率失败: {:?}", asset, e);
                    fail += 1;
                }
            }
            sleep(Duration::from_millis(80)).await;
        }

        // 2) 再拉实时 estimate_rate，用于 current_daily_rate（和 Binance 逻辑类似）
        let current_map = Self::fetch_gate_estimate_rates(&assets)
            .await
            .unwrap_or_default();
        for (asset, current_daily) in current_map {
            if let Some(cache) = predict_map.get_mut(&asset) {
                cache.current_daily_rate = current_daily;
            }
        }

        // 3) 写入缓存
        if !predict_map.is_empty() {
            Self::with_inner_mut(|inner| {
                inner
                    .lending_rates
                    .insert(GATE_CONFIG.venue, predict_map.clone());
            });
        }

        if success + fail > 0 {
            info!(
                "Gate 借贷利率: 历史成功 {}, 失败 {}, 实时更新 {} 个",
                success,
                fail,
                predict_map
                    .iter()
                    .filter(|(_, v)| v.current_daily_rate > 0.0)
                    .count()
            );
        }
        Ok(())
    }

    /// 批量从 Gate estimate_rate 获取实时借贷利率（最多10个币种一批）
    async fn fetch_gate_estimate_rates(assets: &[String]) -> Result<HashMap<String, f64>> {
        if assets.is_empty() {
            return Ok(HashMap::new());
        }

        let (client, api_key, api_secret) = Self::with_inner(|inner| {
            (
                inner.http_client.clone(),
                inner.gate_api_key.clone(),
                inner.gate_api_secret.clone(),
            )
        });
        let (api_key, api_secret) = match (api_key, api_secret) {
            (Some(k), Some(s)) => (k, s),
            _ => return Ok(HashMap::new()),
        };

        let mut result: HashMap<String, f64> = HashMap::new();
        let mut i = 0;
        while i < assets.len() {
            let chunk = &assets[i..assets.len().min(i + 10)];
            let currencies = chunk
                .iter()
                .map(|a| a.to_uppercase())
                .collect::<Vec<_>>()
                .join(",");
            let query = format!("currencies={}", currencies);
            let url = format!(
                "{}{}?{}",
                GATE_API_BASE_URL, GATE_LOAN_RATE_ESTIMATE_PATH, query
            );
            let ts = Utc::now().timestamp();
            let sign_path = format!("/api/v4{}", GATE_LOAN_RATE_ESTIMATE_PATH);
            let sign = Self::sign_gate_request(&api_secret, "GET", &sign_path, &query, "", ts)?;

            let resp = client
                .get(&url)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("KEY", &api_key)
                .header("Timestamp", ts.to_string())
                .header("SIGN", sign)
                .send()
                .await?;

            if resp.status().is_success() {
                let v: Value = resp.json().await.unwrap_or(Value::Null);
                if let Value::Object(map) = v {
                    for (k, val) in map {
                        let rate_str = val.as_str().unwrap_or_default();
                        if let Ok(hourly) = rate_str.parse::<f64>() {
                            if hourly > 0.0 {
                                // estimate_rate 返回小时利率，转为日利率存入缓存
                                result.insert(k.to_uppercase(), hourly * 24.0);
                            }
                        }
                    }
                }
            }

            i += 10;
            sleep(Duration::from_millis(60)).await;
        }

        Ok(result)
    }

    async fn fetch_gate_lending_rate_for_asset(asset: &str) -> Result<Option<Vec<f64>>> {
        let (client, api_key, api_secret) = Self::with_inner(|inner| {
            (
                inner.http_client.clone(),
                inner.gate_api_key.clone(),
                inner.gate_api_secret.clone(),
            )
        });
        let (api_key, api_secret) = match (api_key, api_secret) {
            (Some(k), Some(s)) => (k, s),
            _ => return Ok(None),
        };

        let currency = asset.to_uppercase();
        let query = format!("currency={}&limit=100", currency);
        let url = format!(
            "{}{}?{}",
            GATE_API_BASE_URL, GATE_LOAN_RATE_HISTORY_PATH, query
        );
        let ts = Utc::now().timestamp(); // Gate 使用秒级时间戳
        let sign_path = format!("/api/v4{}", GATE_LOAN_RATE_HISTORY_PATH);
        let sign = Self::sign_gate_request(&api_secret, "GET", &sign_path, &query, "", ts)?;

        let resp = client
            .get(&url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("KEY", api_key)
            .header("Timestamp", ts.to_string())
            .header("SIGN", sign)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let data: GateLoanRateResponse = resp.json().await?;
        // Gate 每小时一个 rate；这里按“小时利率 * 24 => 日利率”存入缓存
        let rates: Vec<f64> = data
            .rates
            .iter()
            .take(24)
            .filter_map(|it| it.rate.parse::<f64>().ok())
            .map(|hourly| hourly * 24.0)
            .collect();

        Ok(if rates.is_empty() { None } else { Some(rates) })
    }

    /// 从 Gate.io API 获取资金费率历史，同时推断周期
    /// Gate 响应格式: [{"t": 1543968000, "r": "0.000157"}, ...]
    async fn fetch_gate_funding_history(
        client: &Client,
        symbol: &str,
        limit: usize,
    ) -> Result<(Vec<f64>, FundingRatePeriod)> {
        let limit_s = limit.max(1).min(1000).to_string();
        let url = format!("{}{}", GATE_API_BASE_URL, GATE_FUNDING_RATE_HISTORY_PATH);
        let resp = client
            .get(&url)
            .query(&[("contract", symbol), ("limit", &limit_s)])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        let items: Vec<GateFundingRateItem> = resp.json().await?;
        if items.is_empty() {
            return Ok((vec![], FundingRatePeriod::Hours8));
        }

        // 推断周期：计算前两条记录的时间差（Gate时间戳为秒）
        let period = if items.len() >= 2 {
            let t1 = items[0].t;
            let t2 = items[1].t;
            if t1 > 0 && t2 > 0 {
                let hours = (t1 - t2).abs() / 3600;
                match hours {
                    1 => FundingRatePeriod::Hours1,
                    2 => FundingRatePeriod::Hours2,
                    4 => FundingRatePeriod::Hours4,
                    6 => FundingRatePeriod::Hours6,
                    _ => FundingRatePeriod::Hours8,
                }
            } else {
                FundingRatePeriod::Hours8
            }
        } else {
            FundingRatePeriod::Hours8
        };

        // Gate 返回最新在前，反转为时间顺序
        let mut rates: Vec<f64> = items.iter().filter_map(|it| it.r.parse().ok()).collect();
        rates.reverse();

        debug!("Gate {} 周期: {}", symbol, period.as_str());
        Ok((rates, period))
    }

    fn print_gate_rate_table(symbols: &[String]) {
        let mut data: Vec<_> = symbols
            .iter()
            .map(|s| {
                let key = Self::normalize_symbol_for_lookup(s, GATE_CONFIG.venue);
                let period = Self::with_inner(|inner| {
                    inner
                        .funding_periods
                        .get(&GATE_CONFIG.venue)
                        .and_then(|m| m.get(&key))
                        .copied()
                        .unwrap_or(FundingRatePeriod::Hours8)
                });
                let fr = RateFetcher::instance()
                    .get_predicted_funding_rate(s, GATE_CONFIG.venue)
                    .map(|(_, v)| v);
                let loan = RateFetcher::instance()
                    .get_predict_loan_rate(s, GATE_CONFIG.venue)
                    .map(|(_, v)| v);
                (s.clone(), period, fr, loan)
            })
            .collect();
        data.sort_by(|a, b| a.0.cmp(&b.0));

        info!("┌──────────────────────────────────────────────────────────────┐");
        info!("│ Gate                │ Period │ Predict FR      │ Predict Loan │");
        info!("├──────────────────────────────────────────────────────────────┤");
        for (sym, period, fr, loan) in data {
            let fr_s = fr.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            let loan_s = loan.map_or("          N/A".into(), |v| format!("{:>12.6}%", v * 100.0));
            info!(
                "│ {:<19} │ {:>6} │ {} │ {} │",
                sym,
                period.as_str(),
                fr_s,
                loan_s
            );
        }
        info!("└──────────────────────────────────────────────────────────────┘");
    }

    // ==================== 公共工具方法 ====================

    fn should_do_full_fetch(venue: TradingVenue) -> bool {
        let now = Utc::now();
        let hour = now.hour();
        Self::with_inner_mut(|inner| {
            let state = inner.venue_states.entry(venue).or_default();
            if is_hour_boundary() && state.last_full_fetch_hour != Some(hour) {
                state.last_full_fetch_hour = Some(hour);
                return true;
            }
            false
        })
    }

    fn update_symbol_cache(
        venue: TradingVenue,
        symbols: &[String],
        is_full: bool,
    ) -> (Vec<String>, bool) {
        Self::with_inner_mut(|inner| {
            let state = inner.venue_states.entry(venue).or_default();
            let current: HashSet<String> = symbols.iter().cloned().collect();
            if is_full {
                let old = std::mem::replace(&mut state.symbol_cache, current.clone());
                let changed = old != current;
                (current.into_iter().collect(), changed)
            } else {
                let new: Vec<String> = current.difference(&state.symbol_cache).cloned().collect();
                state.symbol_cache.extend(new.iter().cloned());
                let changed = !new.is_empty();
                (new, changed)
            }
        })
    }

    fn log_fetch_status(exchange: &str, is_full: bool, new_symbols: &[String], total: usize) {
        let now = Utc::now();
        if is_full {
            info!(
                "[{}] {} 整点全量拉取: {} 个 symbol",
                now.format("%H:%M:%S"),
                exchange,
                total
            );
        } else if !new_symbols.is_empty() {
            info!(
                "[{}] {} 增量拉取: 新增 {} 个",
                now.format("%H:%M:%S"),
                exchange,
                new_symbols.len()
            );
        }
    }

    fn check_initial_rates_if_needed(
        venue: TradingVenue,
        symbols: &[String],
        require_loan: bool,
        force_check: bool,
    ) {
        let ready = Self::with_inner(|inner| {
            inner
                .venue_states
                .get(&venue)
                .map(|state| state.initial_ready)
                .unwrap_or(false)
        });
        if (ready && !force_check) || symbols.is_empty() {
            return;
        }

        let mut missing_fr = Vec::new();
        let mut missing_loan = Vec::new();
        for sym in symbols {
            if Self::instance()
                .get_predicted_funding_rate(sym, venue)
                .is_none()
            {
                missing_fr.push(sym.clone());
            }
            if require_loan && Self::instance().get_predict_loan_rate(sym, venue).is_none() {
                missing_loan.push(sym.clone());
            }
        }

        if missing_fr.is_empty() && missing_loan.is_empty() {
            if !ready {
                Self::with_inner_mut(|inner| {
                    if let Some(state) = inner.venue_states.get_mut(&venue) {
                        state.initial_ready = true;
                        state.last_not_ready_reason = None;
                        state.last_not_ready_symbol = None;
                        state.last_missing_fr = 0;
                        state.last_missing_loan = 0;
                    }
                });
                info!(
                    "RateFetcher: 初次费率数据就绪 venue={:?} symbols={}",
                    venue,
                    symbols.len()
                );
            }
            return;
        }

        Self::with_inner_mut(|inner| {
            if let Some(state) = inner.venue_states.get_mut(&venue) {
                state.initial_ready = false;
                state.last_not_ready_reason = Some("initial_missing".to_string());
                state.last_not_ready_symbol = missing_fr
                    .first()
                    .cloned()
                    .or_else(|| missing_loan.first().cloned());
                state.last_missing_fr = missing_fr.len();
                state.last_missing_loan = missing_loan.len();
            }
        });
        log::error!(
            "RateFetcher: 初次费率数据不完整 venue={:?} missing_fr={:?} missing_loan={:?}",
            venue,
            missing_fr,
            missing_loan
        );
    }

    pub fn is_initial_ready(venue: TradingVenue) -> bool {
        Self::with_inner(|inner| {
            inner
                .venue_states
                .get(&venue)
                .map(|state| state.initial_ready)
                .unwrap_or(false)
        })
    }

    pub fn mark_missing(venue: TradingVenue, symbol: &str, reason: &str) {
        let ready = Self::with_inner(|inner| {
            inner
                .venue_states
                .get(&venue)
                .map(|state| state.initial_ready)
                .unwrap_or(false)
        });
        if !ready {
            return;
        }
        Self::with_inner_mut(|inner| {
            if let Some(state) = inner.venue_states.get_mut(&venue) {
                state.initial_ready = false;
                state.last_not_ready_reason = Some(reason.to_string());
                state.last_not_ready_symbol = Some(symbol.to_string());
                state.last_missing_fr = 0;
                state.last_missing_loan = 0;
            }
        });
        log::error!(
            "RateFetcher: missing data, disable signals venue={:?} symbol={} reason={}",
            venue,
            symbol,
            reason
        );
    }

    pub fn not_ready_detail(venue: TradingVenue) -> Option<String> {
        Self::with_inner(|inner| {
            let state = inner.venue_states.get(&venue)?;
            if state.initial_ready {
                return None;
            }
            let reason = state.last_not_ready_reason.as_deref().unwrap_or("unknown");
            let symbol = state.last_not_ready_symbol.as_deref().unwrap_or("-");
            let mut detail = format!("reason={} symbol={}", reason, symbol);
            if state.last_missing_fr > 0 || state.last_missing_loan > 0 {
                detail.push_str(&format!(
                    " missing_fr={} missing_loan={}",
                    state.last_missing_fr, state.last_missing_loan
                ));
            }
            Some(detail)
        })
    }

    fn okex_loan_rate_url() -> String {
        Self::with_inner(|inner| {
            inner
                .okex_loan_rate_url
                .clone()
                .unwrap_or_else(|| DEFAULT_OKEX_LOAN_RATE_URL.to_string())
        })
    }

    fn base_asset_from_symbol(symbol: &str) -> Option<String> {
        let mut s = symbol.to_uppercase().replace('_', "-");
        if let Some(stripped) = s.strip_suffix("-SWAP") {
            s = stripped.to_string();
        } else if let Some(stripped) = s.strip_suffix("SWAP") {
            s = stripped.to_string();
        }

        for suffix in ["-USDT", "-USDC", "-USD", "USDT", "USDC", "USD"] {
            if let Some(stripped) = s.strip_suffix(suffix) {
                let base = stripped.trim_end_matches('-');
                if !base.is_empty() {
                    return Some(base.to_string());
                }
            }
        }

        if let Some(idx) = s.find('-') {
            let base = &s[..idx];
            if !base.is_empty() {
                return Some(base.to_string());
            }
        }

        if !s.is_empty() {
            Some(s)
        } else {
            None
        }
    }

    fn normalize_symbol_for_lookup(symbol: &str, venue: TradingVenue) -> String {
        if matches!(venue, TradingVenue::OkexFutures | TradingVenue::OkexMargin) {
            return Self::normalize_okex_symbol(symbol);
        }
        if matches!(venue, TradingVenue::GateFutures | TradingVenue::GateMargin) {
            return Self::normalize_gate_symbol(symbol);
        }
        symbol.to_uppercase()
    }

    fn collect_base_assets(symbols: &[String]) -> Vec<String> {
        let mut assets: Vec<String> = symbols
            .iter()
            .filter_map(|s| Self::base_asset_from_symbol(s))
            .collect();
        assets.sort_unstable();
        assets.dedup();
        assets
    }

    // ==================== 公开查询接口 ====================

    /// 获取指定 symbol 的资金费率周期
    pub fn get_period(&self, symbol: &str, venue: TradingVenue) -> FundingRatePeriod {
        let key = Self::normalize_symbol_for_lookup(symbol, venue);
        Self::with_inner(|inner| {
            inner
                .funding_periods
                .get(&venue)
                .and_then(|m| m.get(&key))
                .copied()
                .unwrap_or(FundingRatePeriod::Hours8)
        })
    }

    /// 获取预测资金费率
    pub fn get_predicted_funding_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        let key = Self::normalize_symbol_for_lookup(symbol, venue);
        let period = self.get_period(&key, venue);
        let rates = Self::with_inner(|inner| inner.funding_rates.get(&venue)?.get(&key).cloned())?;
        let value = Self::calculate_predicted_rate(&rates)?;
        Some((period, value))
    }

    /// 获取预测借贷利率
    pub fn get_predict_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        let period = self.get_period(symbol, venue);
        let base = Self::base_asset_from_symbol(symbol)?;
        let daily = Self::with_inner(|inner| {
            inner
                .lending_rates
                .get(&venue)?
                .get(&base)
                .map(|c| c.predict_daily_rate)
        })?;
        Some((period, period.convert_daily_rate(daily)))
    }

    /// 获取当前借贷利率
    pub fn get_current_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        let period = self.get_period(symbol, venue);
        let base = Self::base_asset_from_symbol(symbol)?;
        let daily = Self::with_inner(|inner| {
            inner
                .lending_rates
                .get(&venue)?
                .get(&base)
                .map(|c| c.current_daily_rate)
        })?;
        Some((period, period.convert_daily_rate(daily)))
    }

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

    /// 打印信号状态表
    pub fn print_signal_table(symbols: &[String]) {
        super::arb_decision::ArbDecision::print_signal_table(symbols);
    }
}

// ==================== Trait 实现 ====================

impl RateFetcherTrait for RateFetcher {
    fn get_period(&self, symbol: &str, venue: TradingVenue) -> FundingRatePeriod {
        RateFetcher::get_period(self, symbol, venue)
    }

    fn get_predicted_funding_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        RateFetcher::get_predicted_funding_rate(self, symbol, venue)
    }

    fn get_predict_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        RateFetcher::get_predict_loan_rate(self, symbol, venue)
    }

    fn get_current_loan_rate(
        &self,
        symbol: &str,
        venue: TradingVenue,
    ) -> Option<(FundingRatePeriod, f64)> {
        RateFetcher::get_current_loan_rate(self, symbol, venue)
    }
}
