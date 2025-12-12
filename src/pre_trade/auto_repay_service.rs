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

/// 负债检查结果
#[allow(dead_code)]
enum LiabilityCheckResult {
    /// 无数据（snapshot 为 None）
    NoData,
    /// 无负债
    NoLiability,
    /// 有负债但无可用余额
    HasLiabilityNoBalance(Vec<(String, f64, f64)>), // (asset, borrowed, interest)
    /// 有负债且有可用余额可以还款
    CanRepay(Vec<(String, f64, f64, f64)>), // (asset, borrowed, interest, available)
}

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
    tran_id: i64,
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
                info!("下次自动还款时间: {} 秒后", wait_duration.as_secs());
                tokio::time::sleep(wait_duration).await;

                // 执行还款检查
                self.check_and_repay().await;
            }
        });
    }

    /// 启动测试用高频还款任务
    /// 每 30 秒检查一次，能还就还
    pub fn start_test_repay_task(self) {
        tokio::spawn(async move {
            info!("⚠️ 测试还款服务已启动，每 30 秒检查一次");

            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
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
            LiabilityCheckResult::NoData => {
                info!("✅ 无账户数据，跳过还款");
                return;
            }
            LiabilityCheckResult::NoLiability => {
                info!("✅ 无负债，跳过还款");
                return;
            }
            LiabilityCheckResult::HasLiabilityNoBalance(debts) => {
                info!("⚠️ 有 {} 项负债但无可用余额还款:", debts.len());
                for (asset, borrowed, interest) in &debts {
                    info!(
                        "  {} - 借入:{:.8} 利息:{:.8} 可用:0",
                        asset, borrowed, interest
                    );
                }
                return;
            }
            LiabilityCheckResult::CanRepay(liabs) => liabs,
        };

        info!("检测到 {} 项负债需要还款:", liabilities.len());
        for (asset, borrowed, interest, available) in &liabilities {
            info!(
                "  {} - 借入:{:.8} 利息:{:.8} 可用:{:.8}",
                asset, borrowed, interest, available
            );
        }

        // 逐个资产调用还款 API
        for (asset, borrowed, interest, available) in &liabilities {
            // 计算还款金额：min(借款 + 利息, 可用余额)
            let total_debt = borrowed + interest;
            let repay_amount = total_debt.min(*available);
            if repay_amount <= 0.0 {
                continue;
            }

            match self.repay_loan(asset, repay_amount).await {
                Ok(response) => {
                    info!(
                        "✅ 自动还款成功: asset={} amount={:.8} tranId={}",
                        asset, repay_amount, response.tran_id
                    );
                }
                Err(e) => {
                    warn!("❌ {} 自动还款失败: {}", asset, e);
                }
            }
        }
    }

    /// 从 MonitorChannel 获取需要还款的负债信息
    fn get_liabilities_to_repay() -> LiabilityCheckResult {
        // pre_trade 已切换为 basic 管理器，不再维护 Binance PM liabilities。
        // 后续如需自动还款，需要在 basic 事件中补充借贷/可用余额语义。
        LiabilityCheckResult::NoData
    }

    /// 调用币安还款 API (POST /papi/v1/repayLoan)
    /// 权重: 100
    async fn repay_loan(&self, asset: &str, amount: f64) -> Result<RepayResponse> {
        let mut params = BTreeMap::new();
        params.insert("asset".to_string(), asset.to_string());
        params.insert("amount".to_string(), format!("{:.8}", amount));
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
            "{}/papi/v1/repayLoan?{}&signature={}",
            self.rest_base, query, signature
        );

        debug!(
            "调用还款 API: POST {} asset={} amount={:.8}",
            url, asset, amount
        );

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
