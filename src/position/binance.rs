use anyhow::Result;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
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
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
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
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let mark_price = item
                .get("markPrice")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let unrealized_pnl = item
                .get("unRealizedProfit")
                .or_else(|| item.get("unrealizedProfit"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok());
            let leverage = item
                .get("leverage")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<u32>().ok());
            // PM-only: 不保证有，统一置 None
            let margin = None;
            let liquidation_price = item
                .get("liquidationPrice")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
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
        Ok(self.parse_pm_positions(&data))
    }

    /// PM-only：返回组合保证金账户汇总
    async fn fetch_pm_summary(&self) -> Result<Option<PmAccountSummary>> {
        let data = self.fetch_portfolio_account().await?;

        let s = PmAccountSummary {
            account_equity: data
                .get("accountEquity")
                .or_else(|| data.get("totalCrossWalletBalance"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
            total_maint_margin: data
                .get("totalMaintMargin")
                .or_else(|| data.get("maintMargin"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
            total_initial_margin: data
                .get("totalInitialMargin")
                .or_else(|| data.get("initialMargin"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
            total_unrealized_pnl: data
                .get("totalCrossUnPnl")
                .or_else(|| data.get("unRealizedProfit"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
            total_margin_balance: data
                .get("totalMarginBalance")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
            max_withdraw_amount: data
                .get("maxWithdrawAmount")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok()),
        };
        Ok(Some(s))
    }
}
