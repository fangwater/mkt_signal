use anyhow::{anyhow, Result};
use chrono::{Timelike, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, warn};
use reqwest::Client;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Binance PM 资金全量归集服务（UM -> Cross Margin）
///
/// 调度策略：
/// 1. pre_trade 启动后立即尝试一次；
/// 2. 之后每天 UTC+8 12:00 再执行一次。
///
/// 防抖策略：
/// - 通过本地 guard 文件记录最近一次“尝试时间”（成功/失败都会记录）；
/// - 若距上次尝试小于 `min_interval`，则跳过，避免高权重接口被频繁触发。
pub struct AutoCollectionService {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
    min_interval: Duration,
    guard_file: PathBuf,
}

impl AutoCollectionService {
    pub fn new(
        rest_base: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        recv_window_ms: u64,
        min_interval: Duration,
        guard_file: Option<PathBuf>,
    ) -> Self {
        Self {
            client: Client::new(),
            rest_base: rest_base.into(),
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window_ms,
            min_interval,
            guard_file: guard_file.unwrap_or_else(Self::default_guard_file),
        }
    }

    /// 启动“启动即执行 + 每天 UTC+8 12:00 执行”任务。
    pub fn start_startup_and_daily_task(self) {
        tokio::spawn(async move {
            info!(
                "自动资金归集服务已启动（startup + daily UTC+8 12:00, min_interval={}s, guard_file={})",
                self.min_interval.as_secs(),
                self.guard_file.display()
            );

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
        let now_ms = Utc::now().timestamp_millis();
        if let Some(last_ms) = Self::read_last_attempt_ms(&self.guard_file) {
            let elapsed_ms = now_ms.saturating_sub(last_ms);
            let min_interval_ms = self.min_interval.as_millis() as i64;
            if elapsed_ms >= 0 && elapsed_ms < min_interval_ms {
                let remain_s = ((min_interval_ms - elapsed_ms) / 1000).max(0);
                info!(
                    "自动资金归集跳过: reason={} remain_s={} last_attempt_ms={} guard_file={}",
                    reason,
                    remain_s,
                    last_ms,
                    self.guard_file.display()
                );
                return;
            }
        }

        if let Err(err) = Self::write_last_attempt_ms(&self.guard_file, now_ms) {
            warn!(
                "自动资金归集 guard 写入失败（继续执行归集）: file={} err={:#}",
                self.guard_file.display(),
                err
            );
        }

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

    fn default_guard_file() -> PathBuf {
        let dir_tag = std::env::current_dir()
            .ok()
            .and_then(|p| p.file_name().map(|s| s.to_string_lossy().to_string()))
            .unwrap_or_else(|| "default".to_string());

        let mut normalized = String::with_capacity(dir_tag.len());
        for ch in dir_tag.chars() {
            if ch.is_ascii_alphanumeric() {
                normalized.push(ch.to_ascii_lowercase());
            } else {
                normalized.push('_');
            }
        }
        if normalized.is_empty() {
            normalized = "default".to_string();
        }

        PathBuf::from("/tmp").join(format!(
            "mkt_signal_pm_auto_collection_{}.stamp",
            normalized
        ))
    }

    fn read_last_attempt_ms(path: &Path) -> Option<i64> {
        let raw = std::fs::read_to_string(path).ok()?;
        raw.trim().parse::<i64>().ok()
    }

    fn write_last_attempt_ms(path: &Path, ts_ms: i64) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, ts_ms.to_string())?;
        Ok(())
    }
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}
