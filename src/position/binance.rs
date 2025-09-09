use anyhow::Result;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::api::{ApiConfig, Balance, ExchangeApiClient, PmAccountSummary, RawPosition};
use super::types::{PositionSide, PositionType};

/// 币安API客户端
pub struct BinanceApiClient {
    /// HTTP客户端
    client: Client,
    /// API配置
    config: ApiConfig,
    /// 现货API基础URL
    spot_base_url: String,
}

impl BinanceApiClient {
    #[inline]
    fn val_as_f64(v: &Value) -> Option<f64> {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    }

    #[inline]
    fn val_as_u32(v: &Value) -> Option<u32> {
        v.as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .or_else(|| v.as_str().and_then(|s| s.parse::<u32>().ok()))
    }
    /// 创建新的币安API客户端
    pub fn new(config: ApiConfig) -> Self {
        let spot_base_url = if config.testnet {
            "https://testnet.binance.vision".to_string()
        } else {
            "https://api.binance.com".to_string()
        };

        Self { client: Client::new(), config, spot_base_url }
    }

    /// 生成签名
    fn sign(&self, query_string: &str) -> String {
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.config.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(query_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// 获取当前时间戳
    fn timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[inline]
    fn is_stable_asset(asset: &str) -> bool {
        matches!(asset, "USDT" | "BUSD" | "USDC" | "FDUSD" | "TUSD" | "DAI" | "SUSD")
    }

    /// 构建带签名的请求URL
    fn build_signed_url(&self, base_url: &str, endpoint: &str, params: &str) -> String {
        let timestamp = Self::timestamp();
        let mut parts: Vec<String> = Vec::new();
        if !params.is_empty() {
            parts.push(params.to_string());
        }
        parts.push(format!("timestamp={}", timestamp));
        // 为提高时间漂移容错，默认加上 recvWindow=5000（如调用方已提供则不重复添加）
        let has_recv_window = params.contains("recvWindow=");
        if !has_recv_window {
            parts.push("recvWindow=5000".to_string());
        }
        let query_string = parts.join("&");
        let signature = self.sign(&query_string);
        format!(
            "{}/{}?{}&signature={}",
            base_url, endpoint, query_string, signature
        )
    }

    /// 发送带签名的GET请求
    async fn signed_get(&self, base_url: &str, endpoint: &str, params: &str) -> Result<Value> {
        let url = self.build_signed_url(base_url, endpoint, params);

        let response = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            // 尝试解析标准错误格式 { code, msg }
            let (code, msg) = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    Some((
                        v.get("code").and_then(|c| c.as_i64()),
                        v.get("msg").and_then(|m| m.as_str()).map(|s| s.to_string()),
                    ))
                })
                .unwrap_or((None, None));

            let rate_limited = status.as_u16() == 429 || status.as_u16() == 418;
            if rate_limited {
                return Err(anyhow::anyhow!(
                    "Binance rate limited ({} {}): code={:?}, msg={:?}, endpoint={}/{}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or(""),
                    code,
                    msg.as_deref().unwrap_or(&text),
                    base_url,
                    endpoint
                ));
            } else {
                return Err(anyhow::anyhow!(
                    "Binance API error ({} {}): code={:?}, msg={:?}, endpoint={}/{}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or(""),
                    code,
                    msg.as_deref().unwrap_or(&text),
                    base_url,
                    endpoint
                ));
            }
        }

        Ok(response.json().await?)
    }

    /// 组合保证金账户（Portfolio Margin）信息（PM-only）
    async fn fetch_portfolio_account(&self) -> Result<Value> {
        // 纯 PM-only：只依赖 SAPI 统一账户
        self.signed_get(&self.spot_base_url, "sapi/v1/portfolio/account", "")
            .await
    }

    /// PAPI 余额（补充分账户/统一账户下资产视图）
    async fn fetch_papi_balance(&self) -> Result<Value> {
        self.signed_get(&self.spot_base_url, "papi/v1/balance", "").await
    }

    /// 从 PM 账户响应解析持仓（缺失字段置 None）
    fn parse_pm_positions(&self, root: &Value) -> Vec<RawPosition> {
        let mut res = Vec::new();
        let positions = root.get("positions").and_then(|v| v.as_array());
        if positions.is_none() {
            return res;
        }
        for item in positions.unwrap() {
            let symbol = item
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }

            let position_amt = item
                .get("positionAmt")
                .or_else(|| item.get("posAmt"))
                .and_then(Self::val_as_f64)
                .unwrap_or(0.0);
            if position_amt == 0.0 {
                continue;
            }
            let side = if position_amt > 0.0 {
                PositionSide::Long
            } else {
                PositionSide::Short
            };

            let entry_price = item
                .get("entryPrice")
                .or_else(|| item.get("avgEntryPrice"))
                .and_then(Self::val_as_f64)
                .unwrap_or(0.0);
            let mark_price = item.get("markPrice").and_then(Self::val_as_f64);
            let unrealized_pnl = item
                .get("unRealizedProfit")
                .or_else(|| item.get("unrealizedProfit"))
                .and_then(Self::val_as_f64);
            let leverage = item.get("leverage").and_then(Self::val_as_u32);
            // PM-only: 不保证有，统一置 None
            let margin = None;
            let liquidation_price = item
                .get("liquidationPrice")
                .and_then(Self::val_as_f64)
                .filter(|&p| p > 0.0);

            res.push(RawPosition {
                symbol,
                position_type: PositionType::Perpetual,
                side,
                quantity: position_amt.abs(),
                entry_price,
                mark_price,
                unrealized_pnl,
                realized_pnl: None,
                margin,
                leverage,
                liquidation_price,
            });
        }
        res
    }

    /// 从 PM 账户响应解析资产为“现货仓位”（数量>阈值）。
    /// 稳定币也会展示（USDT/USDC等），其 symbol 直接用资产名，其他资产按 `{ASSET}USDT`。
    fn parse_pm_spot_positions(&self, root: &Value) -> Vec<RawPosition> {
        let mut res = Vec::new();
        let candidates = [
            "assets",
            "userAssets",
            "balances",
            "uniAccountAssets",
            "accountAssets",
        ];
        let assets_array = candidates
            .iter()
            .find_map(|k| root.get(*k).and_then(|v| v.as_array()));
        if assets_array.is_none() {
            return res;
        }
        for item in assets_array.unwrap() {
            let asset = item
                .get("asset")
                .or_else(|| item.get("coin"))
                .or_else(|| item.get("currency"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if asset.is_empty() {
                continue;
            }

            let total = Self::val_as_f64(item.get("total").unwrap_or(&Value::Null))
                .or_else(|| Self::val_as_f64(item.get("walletBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("marginBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("balance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("equity").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("availableBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("crossWalletBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("totalWalletBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("netAsset").unwrap_or(&Value::Null)))
                .or_else(|| {
                    let free = Self::val_as_f64(item.get("free").unwrap_or(&Value::Null)).unwrap_or(0.0);
                    let locked = Self::val_as_f64(item.get("locked").unwrap_or(&Value::Null)).unwrap_or(0.0);
                    let sum = free + locked;
                    if sum > 0.0 { Some(sum) } else { None }
                })
                .unwrap_or(0.0);

            if total <= 0.0 || total < 0.0001 {
                continue;
            }

            let symbol = if Self::is_stable_asset(&asset) { asset.clone() } else { format!("{}USDT", asset) };
            res.push(RawPosition {
                symbol,
                position_type: PositionType::Spot,
                side: PositionSide::Long,
                quantity: total,
                entry_price: 0.0,
                // 临时占位：现货标记价统一按 1 处理
                mark_price: Some(1.0),
                unrealized_pnl: None,
                realized_pnl: None,
                margin: None,
                leverage: None,
                liquidation_price: None,
            });
        }
        res
    }

    /// 从 PAPI `/papi/v1/balance` 解析资产为“现货仓位”
    fn parse_papi_spot_positions(&self, root: &Value) -> Vec<RawPosition> {
        let mut res = Vec::new();
        let arr = root.as_array();
        if arr.is_none() {
            return res;
        }
        for item in arr.unwrap() {
            let asset = item
                .get("asset")
                .or_else(|| item.get("coin"))
                .or_else(|| item.get("currency"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if asset.is_empty() {
                continue;
            }

            let total = Self::val_as_f64(item.get("total").unwrap_or(&Value::Null))
                .or_else(|| Self::val_as_f64(item.get("balance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("crossWalletBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("walletBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("availableBalance").unwrap_or(&Value::Null)))
                .or_else(|| Self::val_as_f64(item.get("marginBalance").unwrap_or(&Value::Null)))
                .or_else(|| {
                    let free = Self::val_as_f64(item.get("free").unwrap_or(&Value::Null)).unwrap_or(0.0);
                    let locked = Self::val_as_f64(item.get("locked").unwrap_or(&Value::Null)).unwrap_or(0.0);
                    let sum = free + locked;
                    if sum > 0.0 { Some(sum) } else { None }
                })
                .unwrap_or(0.0);
            if total <= 0.0 || total < 0.0001 {
                continue;
            }

            let symbol = if Self::is_stable_asset(&asset) {
                asset.clone()
            } else {
                format!("{}USDT", asset)
            };

            res.push(RawPosition {
                symbol,
                position_type: PositionType::Spot,
                side: PositionSide::Long,
                quantity: total,
                entry_price: 0.0,
                mark_price: Some(1.0),
                unrealized_pnl: None,
                realized_pnl: None,
                margin: None,
                leverage: None,
                liquidation_price: None,
            });
        }
        res
    }
}

#[async_trait]
impl ExchangeApiClient for BinanceApiClient {
    /// PM-only：现货余额不通过 `api/v3` 获取
    async fn fetch_spot_balances(&self) -> Result<Vec<Balance>> {
        Ok(Vec::new())
    }

    /// PM-only：Futures 余额改由 PM 账户统一体现
    async fn fetch_futures_balances(&self) -> Result<Vec<Balance>> {
        Ok(Vec::new())
    }

    /// PM-only：不从现货余额构造仓位
    async fn fetch_spot_positions(&self) -> Result<Vec<RawPosition>> {
        Ok(Vec::new())
    }

    /// PM-only：不从 fapi 获取仓位
    async fn fetch_perpetual_positions(&self) -> Result<Vec<RawPosition>> {
        Ok(Vec::new())
    }

    /// 覆盖默认实现：仅用 `/sapi/v1/portfolio/account` 解析所有持仓
    async fn fetch_all_positions(&self) -> Result<Vec<RawPosition>> {
        let data = self.fetch_portfolio_account().await?;

        // 打印 PM 原始返回
        match serde_json::to_string_pretty(&data) {
            Ok(s) => log::info!("PM account raw response:\n{}", s),
            Err(_) => log::info!("PM account raw response: {}", data),
        }
        let pos_cnt = data
            .get("positions")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let top_keys = data
            .as_object()
            .map(|o| o.keys().cloned().collect::<Vec<_>>().join(", "))
            .unwrap_or_else(|| "<non-object>".to_string());
        log::info!("PM account top-level keys: {} | positions count: {}", top_keys, pos_cnt);

        // 解析 PM 衍生品及 PM 资产转现货
        let pm_perp = self.parse_pm_positions(&data);
        let pm_spot = self.parse_pm_spot_positions(&data);

        // 解析 PAPI 余额为现货
        let papi = self.fetch_papi_balance().await?;
        match serde_json::to_string_pretty(&papi) {
            Ok(s) => log::info!("PAPI balance raw response:\n{}", s),
            Err(_) => log::info!("PAPI balance raw response: {}", papi),
        }
        let papi_spot = self.parse_papi_spot_positions(&papi);

        // 合并并按 (symbol, position_type) 去重聚合数量
        let mut map: HashMap<(String, PositionType), RawPosition> = HashMap::new();
        for p in pm_perp.into_iter().chain(pm_spot).chain(papi_spot) {
            let key = (p.symbol.clone(), p.position_type);
            if let Some(e) = map.get_mut(&key) {
                // 聚合数量（适用于现货映射），保留已有可选字段
                e.quantity += p.quantity;
                if e.mark_price.is_none() {
                    e.mark_price = p.mark_price;
                }
                if e.unrealized_pnl.is_none() {
                    e.unrealized_pnl = p.unrealized_pnl;
                }
                if e.realized_pnl.is_none() {
                    e.realized_pnl = p.realized_pnl;
                }
                if e.margin.is_none() {
                    e.margin = p.margin;
                }
                if e.leverage.is_none() {
                    e.leverage = p.leverage;
                }
                if e.liquidation_price.is_none() {
                    e.liquidation_price = p.liquidation_price;
                }
            } else {
                map.insert(key, p);
            }
        }
        Ok(map.into_values().collect())
    }

    /// PM-only：返回组合保证金账户汇总
    async fn fetch_pm_summary(&self) -> Result<Option<PmAccountSummary>> {
        let data = self.fetch_portfolio_account().await?;

        let s = PmAccountSummary {
            account_equity: data
                .get("accountEquity")
                .or_else(|| data.get("totalCrossWalletBalance"))
                .and_then(Self::val_as_f64),
            total_maint_margin: data
                .get("totalMaintMargin")
                .or_else(|| data.get("maintMargin"))
                .and_then(Self::val_as_f64),
            total_initial_margin: data
                .get("totalInitialMargin")
                .or_else(|| data.get("initialMargin"))
                .and_then(Self::val_as_f64),
            total_unrealized_pnl: data
                .get("totalCrossUnPnl")
                .or_else(|| data.get("unRealizedProfit"))
                .and_then(Self::val_as_f64),
            total_margin_balance: data
                .get("totalMarginBalance")
                .and_then(Self::val_as_f64),
            max_withdraw_amount: data
                .get("maxWithdrawAmount")
                .and_then(Self::val_as_f64),
        };
        Ok(Some(s))
    }
}
