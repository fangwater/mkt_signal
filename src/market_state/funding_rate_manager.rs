use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::array::from_fn;
use chrono::Utc;
use log::{info, error};
use reqwest;
use serde::Deserialize;
use crate::exchange::Exchange;
use tokio::time::{sleep, Duration};
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;
use base64::{engine::general_purpose, Engine as _};

/// 全局资金费率管理器实例
static FUNDING_RATE_MANAGER: once_cell::sync::Lazy<Arc<FundingRateManager>> = 
    once_cell::sync::Lazy::new(|| Arc::new(FundingRateManager::new()));

#[derive(Debug, Deserialize)]
struct FundingRateHistory {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

#[derive(Debug, Deserialize)]
struct PremiumIndexSymbol {
    symbol: String,
}

// OKEx 数据结构
#[derive(Debug, Deserialize)]
struct OkexFundingRate {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

#[derive(Debug, Deserialize)]
struct OkexResponse<T> {
    data: Vec<T>,
}

// Bybit 数据结构
#[derive(Debug, Deserialize)]
struct BybitFundingRate {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
}

#[derive(Debug, Deserialize)]
struct BybitResponse<T> {
    result: T,
}

#[derive(Debug, Deserialize)]
struct BybitResultList {
    list: Vec<BybitFundingRate>,
}

/// 资金费率数据
#[derive(Debug, Clone, Default)]
pub struct FundingRateData {
    pub predicted_funding_rate: f64,  // 预测资金费率（6期移动平均）
    pub loan_rate_8h: f64,            // 8小时借贷利率
    pub last_update: i64,              // 最后更新时间
}

struct ExchangeStore {
    data: RwLock<HashMap<String, FundingRateData>>,      // 该交易所的所有 symbol 数据
    last_check_hour_bucket: AtomicI64,                   // 最近一次检查的小时桶
    refresh_in_progress: AtomicBool,                     // 是否正在刷新
}

impl ExchangeStore {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            last_check_hour_bucket: AtomicI64::new(-1),
            refresh_in_progress: AtomicBool::new(false),
        }
    }
}

/// 资金费率管理器 - 管理所有交易对的预测资金费率和借贷利率
pub struct FundingRateManager {
    // 每个交易所一个独立的存储与状态，避免大粒度锁竞争
    stores: [ExchangeStore; 6],
    client: reqwest::Client,
}

impl FundingRateManager {
    fn new() -> Self {
        Self {
            stores: from_fn(|_| ExchangeStore::new()),
            client: reqwest::Client::new(),
        }
    }

    /// 获取全局实例
    pub fn instance() -> Arc<FundingRateManager> {
        FUNDING_RATE_MANAGER.clone()
    }

    /// 同步获取指定交易对的资金费率数据，根据时间戳判断是否需要刷新
    pub fn get_rates_sync(&self, symbol: &str, exchange: Exchange, timestamp: i64) -> FundingRateData {
        // 检查是否需要触发刷新（基于时间戳）
        self.check_and_trigger_refresh(exchange, timestamp);

        // 直接返回当前数据（可能是默认值0）
        let store = &self.stores[Self::ex_idx(exchange)];
        let data = store.data.read().unwrap();
        if let Some(v) = data.get(symbol) { return v.clone(); }
        let up = symbol.to_uppercase();
        if let Some(v) = data.get(&up) { return v.clone(); }
        let low = symbol.to_lowercase();
        if let Some(v) = data.get(&low) { return v.clone(); }
        FundingRateData::default()
    }

    /// 检查并触发异步刷新
    fn check_and_trigger_refresh(&self, exchange: Exchange, timestamp: i64) {
        let now_hour = (timestamp / 1000 / 3600) % 24; // 当前小时数(UTC)

        // 刷新时间点（UTC 0/8/16时）
        let should_refresh = now_hour == 0 || now_hour == 8 || now_hour == 16;
        if !should_refresh { return; }

        let hour_bucket = timestamp / 1000 / 3600; // 小时级别桶
        let store = &self.stores[Self::ex_idx(exchange)];

        let prev = store.last_check_hour_bucket.swap(hour_bucket, Ordering::Relaxed);
        if prev == hour_bucket {
            return; // 同一小时已经检查过
        }

        if store
            .refresh_in_progress
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // 启动异步刷新任务
            self.trigger_async_refresh(exchange);
        }
    }

    /// 触发异步刷新任务
    fn trigger_async_refresh(&self, exchange: Exchange) {
        let manager = Self::instance();
        tokio::spawn(async move {
            info!("触发{:?}交易所资金费率数据异步刷新", exchange);
            if let Err(e) = manager.refresh_exchange_rates(exchange).await {
                error!("{:?}交易所异步刷新资金费率失败: {}", exchange, e);
            }
            
            // 刷新完成，释放锁
            let store = &manager.stores[Self::ex_idx(exchange)];
            store.refresh_in_progress.store(false, Ordering::Relaxed);
        });
    }

    #[inline]
    fn ex_idx(ex: Exchange) -> usize {
        match ex {
            Exchange::Binance => 0,
            Exchange::BinanceFutures => 1,
            Exchange::Okex => 2,
            Exchange::OkexSwap => 3,
            Exchange::Bybit => 4,
            Exchange::BybitSpot => 5,
        }
    }

    /// 根据交易所刷新资金费率数据
    pub async fn refresh_exchange_rates(&self, exchange: Exchange) -> Result<()> {
        match exchange {
            Exchange::Binance | Exchange::BinanceFutures => {
                self.refresh_binance_rates().await
            }
            Exchange::Okex | Exchange::OkexSwap => {
                self.refresh_okex_rates().await
            }
            Exchange::Bybit | Exchange::BybitSpot => {
                self.refresh_bybit_rates().await
            }
        }
    }

    async fn get_with_retry(
        &self,
        url: &str,
        params: &[(&str, String)],
        max_retries: u8,
    ) -> Result<reqwest::Response> {
        let mut attempt = 0u8;
        let mut delay_ms = 200u64;
        loop {
            let resp = self.client.get(url).query(&params).send().await;
            match resp {
                Ok(r) if r.status().is_success() => return Ok(r),
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    
                    // 不可重试的错误：认证失败、权限不足、请求参数错误等
                    if status.as_u16() == 401 || status.as_u16() == 403 {
                        error!("认证失败或权限不足 ({}): {}", status, body);
                        return Err(anyhow::anyhow!("认证失败或权限不足: {} - {}", status, body));
                    }
                    if status.as_u16() == 400 {
                        error!("请求参数错误 ({}): {}", status, body);
                        return Err(anyhow::anyhow!("请求参数错误: {} - {}", status, body));
                    }
                    
                    // 可重试的错误：429(限流)、5xx(服务器错误)
                    if status.as_u16() == 429 || status.is_server_error() {
                        attempt += 1;
                        if attempt >= max_retries {
                            error!("HTTP请求失败，状态码: {}, 响应: {}", status, body);
                            return Err(anyhow::anyhow!("HTTP请求失败: {} - {}", status, body));
                        }
                        info!("可重试错误 ({}), 尝试 {}/{}, 等待 {}ms", status, attempt, max_retries, delay_ms);
                        sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = (delay_ms * 2).min(5_000);
                    } else {
                        // 其他错误，不重试
                        error!("未预期的HTTP错误 ({}): {}", status, body);
                        return Err(anyhow::anyhow!("未预期的HTTP错误: {} - {}", status, body));
                    }
                }
                Err(e) => {
                    // 网络错误，可以重试
                    attempt += 1;
                    if attempt >= max_retries {
                        error!("网络请求失败: {}", e);
                        return Err(anyhow::anyhow!("网络请求失败: {}", e));
                    }
                    info!("网络错误, 尝试 {}/{}: {}", attempt, max_retries, e);
                    sleep(Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(5_000);
                }
            }
        }
    }

    /// 刷新币安资金费率数据
    async fn refresh_binance_rates(&self) -> Result<()> {
        info!("开始刷新币安资金费率和借贷利率数据");
        
        // 获取活跃交易对列表
        let symbols = self.get_binance_symbols().await?;
        
        // 获取借贷利率：优先签名接口、然后覆盖、最后默认
        let loan_rates = self.get_binance_loan_rates().await;
        
        let mut new_data: HashMap<String, FundingRateData> = HashMap::new();
        
        // 并发获取每个交易对的历史资金费率
        let mut tasks = vec![];
        for symbol in symbols {
            let client = self.client.clone();
            let symbol_clone = symbol.clone();
            tasks.push(tokio::spawn(async move {
                // 重试 3 次
                let url = "https://fapi.binance.com/fapi/v1/fundingRate";
                let mut attempt = 0u8;
                let mut delay_ms = 200u64;
                let history = loop {
                    let end_time = Utc::now().timestamp_millis();
                    let start_time = end_time - (2 * 24 * 60 * 60 * 1000);
                    let start_s = start_time.to_string();
                    let end_s = end_time.to_string();
                    let params = vec![
                        ("symbol", symbol_clone.as_str()),
                        ("startTime", start_s.as_str()),
                        ("endTime", end_s.as_str()),
                        ("limit", "100"),
                    ];
                    match client.get(url).query(&params).send().await {
                        Ok(r) if r.status().is_success() => match r.json::<Vec<FundingRateHistory>>().await {
                            Ok(v) => break Ok(v),
                            Err(e) => {
                                log::error!("解析资金费率数据失败 ({}): {}", symbol_clone, e);
                                break Err(anyhow::anyhow!(e));
                            }
                        },
                        Ok(r) => {
                            let status = r.status();
                            let body = r.text().await.unwrap_or_default();
                            
                            // 4xx错误不重试
                            if status.is_client_error() {
                                log::error!("获取资金费率失败 ({}) - {}: {}", symbol_clone, status, body);
                                break Ok(vec![]);
                            }
                            
                            // 5xx或429可以重试
                            attempt += 1;
                            if attempt >= 3 {
                                log::warn!("获取资金费率失败 ({}) 达到最大重试次数", symbol_clone);
                                break Ok(vec![]);
                            }
                            log::debug!("获取资金费率重试 ({}) {}/{}", symbol_clone, attempt, 3);
                            sleep(Duration::from_millis(delay_ms)).await;
                            delay_ms = (delay_ms * 2).min(5_000);
                        }
                        Err(e) => {
                            // 网络错误，可重试
                            attempt += 1;
                            if attempt >= 3 {
                                log::warn!("获取资金费率网络错误 ({}): {}", symbol_clone, e);
                                break Ok(vec![]);
                            }
                            log::debug!("获取资金费率网络重试 ({}) {}/{}: {}", symbol_clone, attempt, 3, e);
                            sleep(Duration::from_millis(delay_ms)).await;
                            delay_ms = (delay_ms * 2).min(5_000);
                        }
                    }
                }?;
                Ok::<_, anyhow::Error>((symbol_clone, history))
            }));
        }
        
        // 收集结果并计算预测值
        for task in tasks {
            if let Ok(Ok((symbol, history))) = task.await {
                let predicted_rate = Self::calculate_predicted_rate_binance(&history);
                let loan_rate = loan_rates.get(&symbol).copied().unwrap_or(0.0);
                
                new_data.insert(symbol.clone(), FundingRateData {
                    predicted_funding_rate: predicted_rate,
                    loan_rate_8h: loan_rate,
                    last_update: Utc::now().timestamp_millis(),
                });
            }
        }
        
        // 更新数据（写入 Binance 存储）
        if !new_data.is_empty() {
            let store = &self.stores[Self::ex_idx(Exchange::Binance)];
            let mut data = store.data.write().unwrap();
            for (symbol, value) in new_data { data.insert(symbol, value); }
        }
        
        info!("成功刷新币安资金费率数据");
        Ok(())
    }

    /// 组合获取币安借贷利率（8h），优先级：签名接口 > 覆盖 > 默认
    async fn get_binance_loan_rates(&self) -> HashMap<String, f64> {
        // 1) 签名接口
        let mut result = match self.fetch_binance_loan_rates_signed().await {
            Ok(map) => map,
            Err(e) => {
                error!("获取币安借贷利率(签名)失败: {}", e);
                HashMap::new()
            }
        };

        // 2) 覆盖（覆盖签名结果）: 环境变量 BINANCE_LOAN_RATE_OVERRIDES = '{"BTCUSDT":0.00005,"ETHUSDT":0.00006}'
        if let Ok(overrides_str) = std::env::var("BINANCE_LOAN_RATE_OVERRIDES") {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&overrides_str) {
                if let Some(obj) = val.as_object() {
                    for (k, v) in obj {
                        if let Some(rate) = v.as_f64() {
                            result.insert(k.clone(), rate);
                        }
                    }
                }
            }
        }

        // 3) 默认（为常见交易对填充默认值，不覆盖已有值）
        let default_rate = 0.0001_f64;
        for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"].iter() {
            result.entry((*sym).to_string()).or_insert(default_rate);
        }

        result
    }

    /// 通过币安 SAPI 签名接口获取借贷利率（cross margin dailyInterest / 3 -> 8h），需设置环境变量：
    /// BINANCE_API_KEY, BINANCE_API_SECRET
    async fn fetch_binance_loan_rates_signed(&self) -> Result<HashMap<String, f64>> {
        let api_key = std::env::var("BINANCE_API_KEY").map_err(|_| anyhow::anyhow!("BINANCE_API_KEY 未设置"))?;
        let api_secret = std::env::var("BINANCE_API_SECRET").map_err(|_| anyhow::anyhow!("BINANCE_API_SECRET 未设置"))?;

        let host = "https://api.binance.com";
        let path = "/sapi/v1/margin/crossMarginData";
        let timestamp = Utc::now().timestamp_millis().to_string();
        let query = format!("timestamp={}", timestamp);

        // 计算 HMAC-SHA256 签名
        let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())?;
        mac.update(query.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!("{}{}?{}&signature={}", host, path, query, signature);
        let resp = self.client
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            
            // 认证相关错误，不应重试
            if status.as_u16() == 401 || status.as_u16() == 403 {
                error!("Binance SAPI 认证失败 ({}): {}", status, body);
                return Err(anyhow::anyhow!("API密钥无效或权限不足: {}", body));
            }
            
            // 参数错误
            if status.as_u16() == 400 {
                error!("Binance SAPI 请求参数错误 ({}): {}", status, body);
                return Err(anyhow::anyhow!("请求参数错误: {}", body));
            }
            
            error!("Binance SAPI 返回错误 ({}): {}", status, body);
            return Err(anyhow::anyhow!("Binance SAPI 返回非200: {} - {}", status, body));
        }

        let val: serde_json::Value = resp.json().await?;
        let mut out = HashMap::new();

        // 兼容不同返回结构：优先数组；否则尝试 data/interestRate 等路径
        match val {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    if let (Some(coin), Some(daily)) = (
                        item.get("coin").and_then(|v| v.as_str()),
                        item.get("dailyInterest").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                    ) {
                        let symbol = format!("{}USDT", coin);
                        out.insert(symbol, daily / 3.0);
                    }
                }
            }
            serde_json::Value::Object(_) => {
                // 兜底：在对象中查找包含 coin/dailyInterest 的数组字段
                let mut push_rate = |coin: &str, daily: f64| {
                    let key = format!("{}USDT", coin);
                    out.insert(key, daily / 3.0);
                };
                // 常见路径尝试
                for key in ["data", "interestRate", "rows", "list"].iter() {
                    if let Some(arr) = val.get(*key).and_then(|v| v.as_array()) {
                        for item in arr {
                            if let (Some(coin), Some(daily)) = (
                                item.get("coin").and_then(|v| v.as_str()),
                                item.get("dailyInterest").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok()),
                            ) {
                                push_rate(coin, daily);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(out)
    }

    /// 刷新 OKEx 资金费率数据（SWAP），并计算预测值（最近6期均值）
    async fn refresh_okex_rates(&self) -> Result<()> {
        info!("开始刷新OKEx资金费率数据");

        // 获取全部 SWAP 合约列表
        let instruments_url = "https://www.okx.com/api/v5/public/instruments";
        let instruments_resp = self.get_with_retry(
            instruments_url,
            &[("instType", "SWAP".to_string())],
            3
        ).await?;

        #[derive(Deserialize)]
        struct OkexInst { #[serde(rename = "instId")] inst_id: String }
        let insts: OkexResponse<OkexInst> = instruments_resp.json().await?;

        let mut tasks = vec![];
        for inst in insts.data {
            let client = self.client.clone();
            let inst_id = inst.inst_id.clone();
            tasks.push(tokio::spawn(async move {
                // 最近6期资金费率历史
                let history_url = "https://www.okx.com/api/v5/public/funding-rate-history";
                let resp = client
                    .get(history_url)
                    .query(&[("instId", inst_id.as_str()), ("limit", "20")])
                    .send()
                    .await;
                let list: Result<Vec<OkexFundingRate>> = match resp {
                    Ok(r) if r.status().is_success() => {
                        let r: OkexResponse<OkexFundingRate> = r.json().await.map_err(|e| anyhow::anyhow!(e))?;
                        Ok(r.data)
                    }
                    _ => Ok(vec![]),
                };
                Ok::<_, anyhow::Error>((inst_id, list?))
            }));
        }

        let mut new_data: HashMap<String, FundingRateData> = HashMap::new();
        for t in tasks {
            if let Ok(Ok((inst_id, mut list))) = t.await {
                // 取最近6期
                list.sort_by_key(|_| std::cmp::Ordering::Equal); // 保持收到顺序，OKEx已按时间返回
                let rates: Vec<f64> = list
                    .into_iter()
                    .rev()
                    .filter_map(|x| x.funding_rate.parse::<f64>().ok())
                    .take(6)
                    .collect();
                let predicted = if rates.is_empty() { 0.0 } else { rates.iter().sum::<f64>() / rates.len() as f64 };
                new_data.insert(inst_id, FundingRateData {
                    predicted_funding_rate: predicted,
                    loan_rate_8h: 0.0001,
                    last_update: Utc::now().timestamp_millis(),
                });
            }
        }

        // 获取 OKX 借贷利率（按币种）：签名接口/覆盖/默认
        let loan_rates = self.get_okx_loan_rates().await;

        // 写入共享数据（OKEx Swap 存储）
        if !new_data.is_empty() {
            let store = &self.stores[Self::ex_idx(Exchange::OkexSwap)];
            let mut data = store.data.write().unwrap();
            for (symbol, mut v) in new_data {
                // 从 instId 推导基础币，如 BTC-USDT-SWAP -> BTC
                let base_coin = symbol.split('-').next().unwrap_or("");
                if let Some(r) = loan_rates.get(base_coin) {
                    v.loan_rate_8h = *r;
                }
                data.insert(symbol, v);
            }
            info!("成功刷新OKEx资金费率数据");
        }

        Ok(())
    }

    /// 刷新 Bybit 资金费率数据，并计算预测值（最近6期均值）
    async fn refresh_bybit_rates(&self) -> Result<()> {
        info!("开始刷新Bybit资金费率数据");

        // 获取全部线性合约列表
        let inst_url = "https://api.bybit.com/v5/market/instruments-info";
        let inst_resp = self.get_with_retry(
            inst_url,
            &[("category", "linear".to_string())],
            3
        ).await?;

        #[derive(Deserialize)]
        struct BybitInstList { list: Vec<BybitInst> }
        #[derive(Deserialize)]
        struct BybitInst { symbol: String }
        #[derive(Deserialize)]
        struct BybitInstResp { result: BybitInstList }
        let inst_parsed: BybitInstResp = inst_resp.json().await?;

        // 并发获取每个交易对的资金费率历史
        let mut tasks = vec![];
        for inst in inst_parsed.result.list {
            let client = self.client.clone();
            let symbol = inst.symbol.clone();
            tasks.push(tokio::spawn(async move {
                let url = "https://api.bybit.com/v5/market/funding/history";
                let resp = client
                    .get(url)
                    .query(&[("category", "linear"), ("symbol", symbol.as_str()), ("limit", "50")])
                    .send()
                    .await;
                let list: Result<Vec<BybitFundingRate>> = match resp {
                    Ok(r) if r.status().is_success() => {
                        let r: BybitResponse<BybitResultList> = r.json().await.map_err(|e| anyhow::anyhow!(e))?;
                        Ok(r.result.list)
                    }
                    _ => Ok(vec![]),
                };
                Ok::<_, anyhow::Error>((symbol, list?))
            }));
        }

        let mut new_data: HashMap<String, FundingRateData> = HashMap::new();
        for t in tasks {
            if let Ok(Ok((symbol, mut list))) = t.await {
                // 取最近6期
                let rates: Vec<f64> = list
                    .drain(..)
                    .rev()
                    .filter_map(|x| x.funding_rate.parse::<f64>().ok())
                    .take(6)
                    .collect();
                let predicted = if rates.is_empty() { 0.0 } else { rates.iter().sum::<f64>() / rates.len() as f64 };
                new_data.insert(symbol, FundingRateData {
                    predicted_funding_rate: predicted,
                    loan_rate_8h: 0.0001,
                    last_update: Utc::now().timestamp_millis(),
                });
            }
        }

        // 获取 Bybit 借贷利率（按币种）：签名接口/覆盖/默认
        let loan_rates = self.get_bybit_loan_rates().await;

        if !new_data.is_empty() {
            let store = &self.stores[Self::ex_idx(Exchange::Bybit)];
            let mut data = store.data.write().unwrap();
            for (symbol, mut v) in new_data {
                // 从 symbol 推导基础币，如 BTCUSDT -> BTC，考虑 1000 前缀
                let base_coin = Self::extract_base_coin_from_usdt_sym(&symbol);
                if let Some(r) = loan_rates.get(&base_coin) {
                    v.loan_rate_8h = *r;
                }
                data.insert(symbol, v);
            }
            info!("成功刷新Bybit资金费率数据");
        }

        Ok(())
    }

    /// 获取币安活跃交易对列表
    async fn get_binance_symbols(&self) -> Result<Vec<String>> {
        let url = "https://fapi.binance.com/fapi/v1/premiumIndex";
        let response = self.client.get(url).send().await?;
        
        if response.status().is_success() {
            let indices: Vec<PremiumIndexSymbol> = response.json().await?;
            Ok(indices.into_iter().map(|i| i.symbol).collect())
        } else {
            error!("获取币安交易对列表失败");
            Ok(vec![])
        }
    }

    /// 获取币安借贷利率（简化版）
    async fn fetch_binance_loan_rates(&self) -> Result<HashMap<String, f64>> {
        // 简化处理：对所有币种使用默认借贷利率
        let mut rates = HashMap::new();
        let default_rate = 0.0001;  // 默认借贷利率 0.01% per 8h
        
        // 为主要交易对设置默认利率
        for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].iter() {
            rates.insert(symbol.to_string(), default_rate);
        }
        
        Ok(rates)
    }

    /// 获取币安单个交易对的历史资金费率
    async fn fetch_binance_funding_history(
        client: &reqwest::Client,
        symbol: &str,
    ) -> Result<Vec<FundingRateHistory>> {
        let url = "https://fapi.binance.com/fapi/v1/fundingRate";
        
        let end_time = Utc::now().timestamp_millis();
        let start_time = end_time - (2 * 24 * 60 * 60 * 1000); // 2天前
        
        let params = [
            ("symbol", symbol),
            ("startTime", &start_time.to_string()),
            ("endTime", &end_time.to_string()),
            ("limit", "100"),
        ];
        
        let response = client.get(url).query(&params).send().await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Ok(vec![])
        }
    }

    /// 计算币安预测资金费率（6期移动平均）
    fn calculate_predicted_rate_binance(history: &[FundingRateHistory]) -> f64 {
        if history.len() < 6 {
            return 0.0;
        }
        
        let recent_rates: Vec<f64> = history
            .iter()
            .rev()
            .take(6)
            .filter_map(|h| h.funding_rate.parse::<f64>().ok())
            .collect();
        
        if recent_rates.is_empty() {
            return 0.0;
        }
        
        recent_rates.iter().sum::<f64>() / recent_rates.len() as f64
    }

    /// 手动触发初始化刷新（在应用启动时调用）
    pub async fn initialize(&self) -> Result<()> {
        info!("初始化资金费率数据");
        
        // 并发刷新所有交易所
        for exchange in [Exchange::Binance, Exchange::OkexSwap, Exchange::Bybit] {
            let m = Self::instance();
            tokio::spawn(async move {
                if let Err(e) = m.refresh_exchange_rates(exchange).await {
                    error!("初始化刷新失败: {}", e);
                }
            });
        }
        
        Ok(())
    }

    // 提取 USDT 合约符号的基础币名，如 BTCUSDT/1000SHIBUSDT -> BTC/SHIB
    fn extract_base_coin_from_usdt_sym(sym: &str) -> String {
        let s = sym.trim().to_uppercase();
        if s.ends_with("USDT") {
            let base = &s[..s.len()-4];
            if base.starts_with("1000") {
                base[4..].to_string()
            } else {
                base.to_string()
            }
        } else {
            s
        }
    }

    // 获取 OKX 借贷利率（8h），返回 coin -> rate(8h)
    async fn get_okx_loan_rates(&self) -> HashMap<String, f64> {
        let mut out = match self.fetch_okx_loan_rates_signed().await {
            Ok(m) => m,
            Err(e) => { error!("获取OKX借贷利率(签名)失败: {}", e); HashMap::new() }
        };
        if let Ok(overrides) = std::env::var("OKX_LOAN_RATE_OVERRIDES") {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&overrides) {
                if let Some(obj) = val.as_object() {
                    for (k,v) in obj {
                        if let Some(rate) = v.as_f64() { out.insert(k.to_uppercase(), rate); }
                    }
                }
            }
        }
        // 默认填充少量常用币
        for c in ["BTC","ETH","SOL","XRP","ADA","DOGE"].iter() {
            out.entry((*c).to_string()).or_insert(0.0001);
        }
        out
    }

    async fn fetch_okx_loan_rates_signed(&self) -> Result<HashMap<String, f64>> {
        let key = std::env::var("OKX_API_KEY").map_err(|_| anyhow::anyhow!("OKX_API_KEY 未设置"))?;
        let secret = std::env::var("OKX_API_SECRET").map_err(|_| anyhow::anyhow!("OKX_API_SECRET 未设置"))?;
        let passphrase = std::env::var("OKX_API_PASSPHRASE").map_err(|_| anyhow::anyhow!("OKX_API_PASSPHRASE 未设置"))?;

        let host = "https://www.okx.com";
        let path = "/api/v5/asset/interest-rate"; // 若后续有变更，可通过覆盖变量替换
        let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let prehash = format!("{}GET{}", ts, path);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(prehash.as_bytes());
        let sign = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let url = format!("{}{}", host, path);
        let resp = self.client
            .get(&url)
            .header("OK-ACCESS-KEY", key)
            .header("OK-ACCESS-SIGN", sign)
            .header("OK-ACCESS-TIMESTAMP", ts)
            .header("OK-ACCESS-PASSPHRASE", passphrase)
            .send().await?;
        if !resp.status().is_success() { return Err(anyhow::anyhow!("OKX 返回非200: {}", resp.status())); }
        let val: serde_json::Value = resp.json().await?;
        let mut out = HashMap::new();
        if let Some(arr) = val.get("data").and_then(|v| v.as_array()) {
            for item in arr {
                let coin = item.get("ccy").and_then(|v| v.as_str()).unwrap_or("");
                let rate = item.get("dailyInterest").and_then(|v| v.as_str())
                    .or_else(|| item.get("interestRate").and_then(|v| v.as_str()))
                    .or_else(|| item.get("rate").and_then(|v| v.as_str()))
                    .and_then(|s| s.parse::<f64>().ok());
                if !coin.is_empty() {
                    if let Some(daily) = rate {
                        out.insert(coin.to_uppercase(), daily/3.0);
                    }
                }
            }
        }
        Ok(out)
    }

    // 获取 Bybit 借贷利率（8h），返回 coin -> rate(8h)
    async fn get_bybit_loan_rates(&self) -> HashMap<String, f64> {
        let mut out = match self.fetch_bybit_loan_rates_signed().await {
            Ok(m) => m,
            Err(e) => { error!("获取Bybit借贷利率(签名)失败: {}", e); HashMap::new() }
        };
        if let Ok(overrides) = std::env::var("BYBIT_LOAN_RATE_OVERRIDES") {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&overrides) {
                if let Some(obj) = val.as_object() {
                    for (k,v) in obj {
                        if let Some(rate) = v.as_f64() { out.insert(k.to_uppercase(), rate); }
                    }
                }
            }
        }
        for c in ["BTC","ETH","SOL","XRP","ADA","DOGE"].iter() {
            out.entry((*c).to_string()).or_insert(0.0001);
        }
        out
    }

    async fn fetch_bybit_loan_rates_signed(&self) -> Result<HashMap<String, f64>> {
        let key = std::env::var("BYBIT_API_KEY").map_err(|_| anyhow::anyhow!("BYBIT_API_KEY 未设置"))?;
        let secret = std::env::var("BYBIT_API_SECRET").map_err(|_| anyhow::anyhow!("BYBIT_API_SECRET 未设置"))?;

        // Bybit v5 签名：sign = HMAC_SHA256(secret, timestamp + api_key + recv_window + queryString)
        let host = "https://api.bybit.com";
        let path = "/v5/asset/coin/query-info"; // 可能需要根据账户权限调整
        let recv_window = "5000";
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let query = String::new(); // 无查询参数
        let sign_payload = format!("{}{}{}{}", timestamp, key, recv_window, query);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(sign_payload.as_bytes());
        let sign = hex::encode(mac.finalize().into_bytes());

        let url = format!("{}{}", host, path);
        let resp = self.client
            .get(&url)
            .header("X-BAPI-API-KEY", key)
            .header("X-BAPI-TIMESTAMP", timestamp)
            .header("X-BAPI-RECV-WINDOW", recv_window)
            .header("X-BAPI-SIGN", sign)
            .send().await?;
        if !resp.status().is_success() { return Err(anyhow::anyhow!("Bybit 返回非200: {}", resp.status())); }
        let val: serde_json::Value = resp.json().await?;
        let mut out = HashMap::new();
        // 兼容不同 JSON 结构：在 result 下的 list/rows 等，查找 coin 与 dailyInterest/interestRate
        let search_lists = ["list", "rows", "data"];
        let root = val.get("result").unwrap_or(&val);
        for key in &search_lists {
            if let Some(arr) = root.get(*key).and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(coin) = item.get("coin").and_then(|v| v.as_str()) {
                        let rate = item.get("dailyInterest").and_then(|v| v.as_str())
                            .or_else(|| item.get("interestRate").and_then(|v| v.as_str()))
                            .or_else(|| item.get("rate").and_then(|v| v.as_str()))
                            .and_then(|s| s.parse::<f64>().ok());
                        if let Some(daily) = rate {
                            out.insert(coin.to_uppercase(), daily/3.0);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}
