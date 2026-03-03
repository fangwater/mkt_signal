use anyhow::{anyhow, Result};
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Binance PM 资金全量归集服务（UM -> Cross Margin）
///
/// 调度策略：
/// 1. pre_trade 启动后立即执行一次；
/// 2. 之后每天 UTC+8 12:00 再执行一次。
pub struct AutoCollectionService {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
}

impl AutoCollectionService {
    pub fn new(
        rest_base: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        recv_window_ms: u64,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_base: rest_base.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window_ms,
        }
    }

    /// 启动“启动即执行 + 每天 UTC+8 12:00 执行”任务。
    pub fn start_startup_and_daily_task(self) {
        tokio::spawn(async move {
            info!("自动资金归集服务已启动（startup + daily UTC+8 12:00）");
            self.try_auto_collect("startup").await;

            loop {
                let wait_duration = Self::time_until_next_shanghai_noon();
                info!(
                    "下次自动资金归集时间: {} 秒后（UTC+8 12:00）",
                    wait_duration.as_secs()
                );
                tokio::time::sleep(wait_duration).await;

                self.try_auto_collect("daily_utc8_noon").await;
            }
        });
    }

    async fn try_auto_collect(&self, reason: &str) {
        match self.auto_collect_all_assets().await {
            Ok((status, body, used_weight)) => {
                info!(
                    "自动资金归集完成: reason={} status={} used_weight={:?} body={}",
                    reason, status, used_weight, body
                );
            }
            Err(err) => {
                warn!("自动资金归集失败: reason={} err={:#}", reason, err);
            }
        }
    }

    /// Binance PM 全量归集 API：
    /// POST /sapi/v1/portfolio/auto-collection
    async fn auto_collect_all_assets(&self) -> Result<(u16, String, Option<String>)> {
        if self.api_key.trim().is_empty() || self.api_secret.trim().is_empty() {
            return Err(anyhow!("BINANCE_API_KEY/SECRET is empty"));
        }

        let mut params = BTreeMap::new();
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if self.recv_window_ms > 0 {
            params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        }
        let query = build_query(&params);
        let signature = self.sign_query(&query)?;
        let url = format!(
            "{}/sapi/v1/portfolio/auto-collection?{}&signature={}",
            self.rest_base, query, signature
        );

        debug!("调用自动资金归集 API: POST {}", url);

        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.text().await.unwrap_or_default();

        let used_weight = headers
            .get("x-mbx-used-weight-1m")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .or_else(|| {
                headers
                    .get("x-sapi-used-ip-weight-1m")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            });

        if !status.is_success() {
            return Err(anyhow!(
                "auto-collection API failed: status={} body={}",
                status,
                body
            ));
        }

        Ok((status.as_u16(), body, used_weight))
    }

    fn sign_query(&self, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|e| anyhow!("HMAC key error: {}", e))?;
        mac.update(query.as_bytes());
        let result = mac.finalize();
        Ok(hex::encode(result.into_bytes()))
    }

    fn time_until_next_shanghai_noon() -> Duration {
        let now_utc = Utc::now();
        let now_shanghai = now_utc + chrono::Duration::hours(8);

        let current_seconds = (now_shanghai.hour() as i64) * 3600
            + (now_shanghai.minute() as i64) * 60
            + (now_shanghai.second() as i64);
        let target_seconds = 12_i64 * 3600;

        let wait_seconds = if current_seconds < target_seconds {
            target_seconds - current_seconds
        } else {
            (24_i64 * 3600) - (current_seconds - target_seconds)
        };

        Duration::from_secs(wait_seconds as u64)
    }
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}
