//! 币安统一账户（PM）listenKey 管理器
//!
//! 职责：
//! - 通过 `POST /papi/v1/listenKey` 创建 listenKey；
//! - 每隔 30 分钟调用 `PUT /papi/v1/listenKey` 进行保活；
//! - 如返回 -1125 等无效提示，自动重建并通过 watch 通道广播新 key；
//! - 退出时调用 `DELETE /papi/v1/listenKey` 及时释放。
//!
//! 使用方式：调用 `start(shutdown_rx)` 获取一个 `watch::Receiver<String>`，
//! 初次会发出创建到的 listenKey，后续重建也会更新。
use anyhow::Result;
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{self, Duration};
use tokio::{select, sync::watch};

/// listenKey 管理服务，封装与 REST 的交互与定时保活逻辑
#[derive(Clone)]
pub struct BinanceListenKeyService {
    client: Client,
    rest_base: String,
    api_key: String,
}

#[derive(Deserialize)]
struct ListenKeyResp {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

impl BinanceListenKeyService {
    /// 创建服务实例
    pub fn new(rest_base: impl Into<String>, api_key: impl Into<String>) -> Self {
        let client = Client::new();
        Self {
            client,
            rest_base: rest_base.into(),
            api_key: api_key.into(),
        }
    } 

    /// 通过 REST 创建 listenKey
    async fn create_listen_key(&self) -> Result<String> {
        let url = format!("{}{}", self.rest_base, "/papi/v1/listenKey");
        debug!("[listenKey] Creating on {}", self.rest_base);
        let resp = self
            .client
            .post(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            error!("Create listenKey failed ({}): {}", status, text);
            return Err(anyhow::anyhow!(
                "Create listenKey failed: {} - {}",
                status,
                text
            ));
        }
        let body: ListenKeyResp = resp.json().await?;
        Ok(body.listen_key)
    }

    /// 对现有 listenKey 进行保活（有效期延长 60 分钟）
    async fn keepalive_listen_key(&self, key: &str) -> Result<()> {
        let url = format!(
            "{}{}?listenKey={}",
            self.rest_base, "/papi/v1/listenKey", key
        );
        debug!("[listenKey] Keepalive sent");
        let resp = self
            .client
            .put(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            // -1125 means invalid listen key (expired/closed)
            warn!("Keepalive failed ({}): {}", status, text);
            return Err(anyhow::anyhow!(
                "Keepalive listenKey failed: {} - {}",
                status,
                text
            ));
        }
        Ok(())
    }

    /// 删除 listenKey，立即失效
    async fn delete_listen_key(&self, key: &str) -> Result<()> {
        let url = format!(
            "{}{}?listenKey={}",
            self.rest_base, "/papi/v1/listenKey", key
        );
        debug!("[listenKey] Deleting before shutdown");
        let resp = self
            .client
            .delete(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!("Delete listenKey failed ({}): {}", status, text);
        }
        Ok(())
    }

    /// Start background service: create key immediately, then keepalive every 30 minutes; recreate on failure.
    pub async fn start(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<watch::Receiver<String>> {
        let (tx, rx) = watch::channel(String::new());

        tokio::spawn(async move {
            // 1) 初次创建 listenKey，失败则重试
            // initial creation with retry
            let mut listen_key = loop {
                match self.create_listen_key().await {
                    Ok(k) => break k,
                    Err(e) => {
                        error!("Create listenKey error: {}. Retry in 3s", e);
                        time::sleep(Duration::from_secs(3)).await;
                    }
                }
            };
            info!("Obtained listenKey, broadcasting to consumers");
            let _ = tx.send(listen_key.clone());

            // 2) 定时保活；失败则立即重建并广播新 key
            let mut ticker = time::interval(Duration::from_secs(30 * 60)); // 30 min
            loop {
                select! {
                    _ = ticker.tick() => {
                        match self.keepalive_listen_key(&listen_key).await {
                            Ok(_) => {
                                debug!("listenKey keepalive succeeded");
                            }
                            Err(e) => {
                                warn!("listenKey keepalive failed: {}. Recreating...", e);
                                // try recreate immediately
                                match self.create_listen_key().await {
                                    Ok(new_key) => {
                                        listen_key = new_key;
                                        let _ = tx.send(listen_key.clone());
                                        info!("listenKey recreated and broadcast to consumers");
                                    }
                                    Err(e2) => {
                                        error!("Failed to recreate listenKey: {}", e2);
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            // 3) 应用退出时，尝试删除 listenKey
                            info!("Shutdown received, deleting listenKey");
                            let _ = self.delete_listen_key(&listen_key).await;
                            break;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}
