use async_trait::async_trait;
use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

use super::api::{ExchangeApiClient, ApiConfig, Balance, RawPosition};
use super::types::{PositionType, PositionSide};

/// 币安API客户端
pub struct BinanceApiClient {
    /// HTTP客户端
    client: Client,
    /// API配置
    config: ApiConfig,
    /// 现货API基础URL
    spot_base_url: String,
    /// 合约API基础URL
    futures_base_url: String,
}

impl BinanceApiClient {
    /// 创建新的币安API客户端
    pub fn new(config: ApiConfig) -> Self {
        let (spot_base_url, futures_base_url) = if config.testnet {
            (
                "https://testnet.binance.vision".to_string(),
                "https://testnet.binancefuture.com".to_string(),
            )
        } else {
            (
                "https://api.binance.com".to_string(),
                "https://fapi.binance.com".to_string(),
            )
        };

        Self {
            client: Client::new(),
            config,
            spot_base_url,
            futures_base_url,
        }
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
        let query_string = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query_string);
        format!("{}/{}?{}&signature={}", base_url, endpoint, query_string, signature)
    }

    /// 发送带签名的GET请求
    async fn signed_get(&self, base_url: &str, endpoint: &str, params: &str) -> Result<Value> {
        let url = self.build_signed_url(base_url, endpoint, params);
        
        let response = self.client
            .get(&url)
            .header("X-MBX-APIKEY", &self.config.api_key)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(anyhow::anyhow!("API request failed: {}", error_text));
        }
        
        Ok(response.json().await?)
    }
}

#[async_trait]
impl ExchangeApiClient for BinanceApiClient {
    /// 获取现货账户余额
    async fn fetch_spot_balances(&self) -> Result<Vec<Balance>> {
        let data = self.signed_get(&self.spot_base_url, "api/v3/account", "").await?;
        
        let mut balances = Vec::new();
        if let Some(balance_array) = data["balances"].as_array() {
            for item in balance_array {
                let asset = item["asset"].as_str().unwrap_or("").to_string();
                let free = item["free"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let locked = item["locked"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                // 只返回有余额的资产
                if free > 0.0 || locked > 0.0 {
                    balances.push(Balance {
                        asset,
                        free,
                        locked,
                        total: free + locked,
                    });
                }
            }
        }
        
        Ok(balances)
    }

    /// 获取合约账户余额
    async fn fetch_futures_balances(&self) -> Result<Vec<Balance>> {
        let data = self.signed_get(&self.futures_base_url, "fapi/v2/account", "").await?;
        
        let mut balances = Vec::new();
        if let Some(asset_array) = data["assets"].as_array() {
            for item in asset_array {
                let asset = item["asset"].as_str().unwrap_or("").to_string();
                let available = item["availableBalance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let margin = item["marginBalance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                // 只返回有余额的资产
                if margin > 0.0 {
                    balances.push(Balance {
                        asset,
                        free: available,
                        locked: margin - available,
                        total: margin,
                    });
                }
            }
        }
        
        Ok(balances)
    }

    /// 获取现货持仓（将余额转换为仓位形式）
    async fn fetch_spot_positions(&self) -> Result<Vec<RawPosition>> {
        let balances = self.fetch_spot_balances().await?;
        let mut positions = Vec::new();
        
        // 获取当前价格（这里简化处理，实际应该批量获取）
        for balance in balances {
            // 跳过稳定币和小额余额
            if balance.asset == "USDT" || balance.asset == "BUSD" || balance.total < 0.0001 {
                continue;
            }
            
            // 构造交易对符号（假设都是对USDT）
            let symbol = format!("{}USDT", balance.asset);
            
            positions.push(RawPosition {
                symbol,
                position_type: PositionType::Spot,
                side: PositionSide::Long, // 现货默认为多头
                quantity: balance.total,
                entry_price: 0.0, // 现货没有开仓价概念，需要从历史订单计算
                mark_price: None,
                unrealized_pnl: None,
                realized_pnl: None,
                margin: None,
                leverage: None,
                liquidation_price: None,
            });
        }
        
        Ok(positions)
    }

    /// 获取永续合约持仓
    async fn fetch_perpetual_positions(&self) -> Result<Vec<RawPosition>> {
        let data = self.signed_get(&self.futures_base_url, "fapi/v2/positionRisk", "").await?;
        
        let mut positions = Vec::new();
        if let Some(position_array) = data.as_array() {
            for item in position_array {
                let position_amt = item["positionAmt"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                // 跳过没有持仓的
                if position_amt == 0.0 {
                    continue;
                }
                
                let symbol = item["symbol"].as_str().unwrap_or("").to_string();
                let side = if position_amt > 0.0 {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                };
                
                let entry_price = item["entryPrice"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                let mark_price = item["markPrice"].as_str()
                    .and_then(|s| s.parse::<f64>().ok());
                
                let unrealized_pnl = item["unRealizedProfit"].as_str()
                    .and_then(|s| s.parse::<f64>().ok());
                
                let margin = item["isolatedWallet"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| {
                        item["maintMargin"].as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                    });
                
                let leverage = item["leverage"].as_str()
                    .and_then(|s| s.parse::<u32>().ok());
                
                let liquidation_price = item["liquidationPrice"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|&p| p > 0.0); // 过滤掉0值
                
                positions.push(RawPosition {
                    symbol,
                    position_type: PositionType::Perpetual,
                    side,
                    quantity: position_amt.abs(),
                    entry_price,
                    mark_price,
                    unrealized_pnl,
                    realized_pnl: None, // 需要从历史订单计算
                    margin,
                    leverage,
                    liquidation_price,
                });
            }
        }
        
        Ok(positions)
    }
}