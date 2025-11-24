use anyhow::{anyhow, Result};
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// 自动还款服务
/// 定时检查负债并自动还款，减少利息支出
pub struct AutoRepayService {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepayResponse {
    amount: String,
    asset: String,
    #[serde(default)]
    _specify_repay_assets: Vec<String>,
    update_time: i64,
    success: bool,
}

impl AutoRepayService {
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

    /// 启动定时还款任务
    /// 每小时的 55 分执行（0:55, 1:55, 2:55...）
    pub fn start_auto_repay_task(self) {
        tokio::spawn(async move {
            info!("自动还款服务已启动，将在每小时 55 分执行");

            loop {
                // 计算到下一个 XX:55 的等待时间
                let wait_duration = Self::time_until_next_55min();
                info!(
                    "下次自动还款时间: {} 秒后",
                    wait_duration.as_secs()
                );
                tokio::time::sleep(wait_duration).await;

                // 执行还款检查
                self.check_and_repay().await;
            }
        });
    }

    /// 计算到下一个 XX:55 的等待时间
    fn time_until_next_55min() -> Duration {
        let now = Utc::now();
        let current_min = now.minute();
        let current_sec = now.second();

        // 计算到下一个 55 分的分钟差
        let minutes_to_wait = if current_min < 55 {
            55 - current_min
        } else {
            // 已经过了 55 分，等到下一个小时的 55 分
            60 - current_min + 55
        };

        // 转换为秒，减去当前秒数以精确对齐
        let total_seconds = minutes_to_wait as u64 * 60 - current_sec as u64;
        Duration::from_secs(total_seconds)
    }

    /// 检查负债并执行还款
    async fn check_and_repay(&self) {
        info!("🕐 开始检查负债...");

        // 从 MonitorChannel 获取负债信息
        let liabilities = match Self::get_liabilities_to_repay() {
            Some(liabs) if !liabs.is_empty() => liabs,
            _ => {
                info!("✅ 无负债或无可用余额，跳过还款");
                return;
            }
        };

        info!("检测到 {} 项负债需要还款:", liabilities.len());
        for (asset, borrowed, interest, available) in &liabilities {
            info!(
                "  {} - 借入:{:.8} 利息:{:.8} 可用:{:.8}",
                asset, borrowed, interest, available
            );
        }

        // 调用还款 API
        match self.repay_all_debts().await {
            Ok(response) => {
                if response.success {
                    info!(
                        "✅ 自动还款成功: asset={} amount={} time={}",
                        response.asset, response.amount, response.update_time
                    );
                } else {
                    warn!("❌ 还款 API 返回失败状态");
                }
            }
            Err(e) => {
                warn!("❌ 自动还款失败: {}", e);
            }
        }
    }

    /// 从 MonitorChannel 获取需要还款的负债信息
    /// 返回: Vec<(asset, borrowed, interest, available_balance)>
    fn get_liabilities_to_repay() -> Option<Vec<(String, f64, f64, f64)>> {
        use crate::pre_trade::monitor_channel::MonitorChannel;

        let spot_mgr = MonitorChannel::instance().spot_manager();
        let mgr = spot_mgr.borrow();
        let snapshot = mgr.snapshot()?;

        let mut liabilities = Vec::new();

        for balance in &snapshot.balances {
            let borrowed = balance.cross_margin_borrowed;
            let interest = balance.cross_margin_interest;
            let available = balance.cross_margin_free;

            // 有负债且有可用余额可以还款
            if borrowed > 0.0 && available > 0.0 {
                liabilities.push((
                    balance.asset.clone(),
                    borrowed,
                    interest,
                    available,
                ));
            }
        }

        if liabilities.is_empty() {
            None
        } else {
            Some(liabilities)
        }
    }

    /// 调用币安还款 API
    /// 自动还清所有可还的负债
    async fn repay_all_debts(&self) -> Result<RepayResponse> {
        let mut params = BTreeMap::new();
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if self.recv_window_ms > 0 {
            params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        }

        // 不指定 asset 和 amount，自动还清所有有余额的负债
        let query = build_query(&params);
        let signature = self.sign_query(&query)?;

        let url = format!(
            "{}/papi/v1/margin/repay-debt?{}&signature={}",
            self.rest_base, query, signature
        );

        debug!("调用还款 API: POST {}", url);

        let resp = self
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;

        debug!("还款 API 响应: status={} body={}", status, body);

        if !status.is_success() {
            return Err(anyhow!("还款 API 失败: {} - {}", status, body));
        }

        let response: RepayResponse = serde_json::from_str(&body)?;
        Ok(response)
    }

    fn sign_query(&self, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|e| anyhow!("HMAC key error: {}", e))?;
        mac.update(query.as_bytes());
        let result = mac.finalize();
        let signature = hex::encode(result.into_bytes());
        Ok(signature)
    }
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}
