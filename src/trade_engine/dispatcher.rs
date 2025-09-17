use crate::trade_engine::config::{ApiKey, LimitsCfg, RestCfg, TradeEngineCfg};
use crate::trade_engine::order_event::OrderRequestEvent;
use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use log::{debug, warn};
use reqwest::{header::HeaderMap, Client};
use sha2::Sha256;
use std::net::IpAddr;
use std::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug)]
struct IpClient {
    ip: IpAddr,
    client: Client,
    /// used weight for 1m window (if header missing we approximate)
    used_weight_1m: u32,
    cooldown_until: Option<Instant>,
    banned_until: Option<Instant>,
}

impl IpClient {
    fn is_available(&self) -> bool {
        let now = Instant::now();
        if let Some(t) = self.cooldown_until {
            if now < t {
                return false;
            }
        }
        if let Some(t) = self.banned_until {
            if now < t {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
struct AccountState {
    key: ApiKey,
    used_orders_1m: u32,
}

pub struct Dispatcher {
    rest: RestCfg,
    limits: LimitsCfg,
    ip_clients: Vec<IpClient>,
    accounts: Vec<AccountState>,
}

impl Dispatcher {
    pub fn new(cfg: &TradeEngineCfg) -> Result<Self> {
        // Build clients per IP
        let mut ip_clients = Vec::new();
        for ip in &cfg.network.local_ips {
            let mut builder = reqwest::Client::builder()
                .local_address(*ip)
                .tcp_keepalive(Some(Duration::from_secs(30)));
            if let Some(ms) = cfg.rest.timeout_ms {
                builder = builder.timeout(Duration::from_millis(ms));
            }
            let client = builder.build()?;
            ip_clients.push(IpClient {
                ip: *ip,
                client,
                used_weight_1m: 0,
                cooldown_until: None,
                banned_until: None,
            });
        }

        let accounts = cfg
            .accounts
            .keys
            .iter()
            .cloned()
            .map(|key| AccountState {
                key,
                used_orders_1m: 0,
            })
            .collect();

        Ok(Self {
            rest: cfg.rest.clone(),
            limits: cfg.limits.clone(),
            ip_clients,
            accounts,
        })
    }

    fn select_ip(&self, req_weight: u32) -> Option<usize> {
        let limit = self.limits.ip_weight_limit();
        let mut best: Option<(usize, f32)> = None; // index, score (remaining ratio)
        for (i, c) in self.ip_clients.iter().enumerate() {
            if !c.is_available() {
                continue;
            }
            let used = c.used_weight_1m as f32;
            let rem_ratio =
                ((limit as f32 - used - req_weight as f32) / limit as f32).clamp(-1.0, 1.0);
            if best.map(|(_, s)| rem_ratio > s).unwrap_or(true) {
                best = Some((i, rem_ratio));
            }
        }
        best.map(|(i, _)| i)
    }

    fn select_account(&self, hint: Option<&str>) -> Option<usize> {
        if let Some(h) = hint {
            return self.accounts.iter().position(|a| a.key.name == h);
        }
        let limit = self.limits.account_limit();
        let mut best: Option<(usize, f32)> = None; // index, score
        for (i, a) in self.accounts.iter().enumerate() {
            let used = a.used_orders_1m as f32;
            let rem_ratio = ((limit as f32 - used) / limit as f32).clamp(-1.0, 1.0);
            if best.map(|(_, s)| rem_ratio > s).unwrap_or(true) {
                best = Some((i, rem_ratio));
            }
        }
        best.map(|(i, _)| i)
    }

    pub async fn dispatch(&mut self, evt: OrderRequestEvent) -> Result<DispatchResponse> {
        let ip_idx = self
            .select_ip(evt.weight())
            .ok_or_else(|| anyhow!("no available IP client (cooldown/banned or all saturated)"))?;
        let acc_idx = self
            .select_account(evt.account.as_deref())
            .ok_or_else(|| anyhow!("no available account key"))?;

        let ip = self.ip_clients[ip_idx].ip;
        // clone required account values to avoid borrow issues across mutation
        let account_name = self.accounts[acc_idx].key.name.clone();
        let api_key_value = self.accounts[acc_idx].key.key.clone();
        let secret_value = self.accounts[acc_idx].key.secret.clone();
        let api_key_trimmed = api_key_value.trim();
        debug!(
            "using account {}, api key length={}, secret length={}",
            account_name,
            api_key_trimmed.len(),
            secret_value.len()
        );
        debug!("dispatch select: ip={}, account={}", ip, account_name);

        // Warn when approaching limits
        let warn_ratio = self.limits.warn_ratio();
        let ip_used = self.ip_clients[ip_idx].used_weight_1m;
        let ip_limit = self.limits.ip_weight_limit();
        if (ip_used as f32) / (ip_limit as f32) > warn_ratio {
            warn!(
                "IP {} used_weight ~{}/{} > {}%",
                ip,
                ip_used,
                ip_limit,
                (warn_ratio * 100.0) as u32
            );
        }
        let acc_used = self.accounts[acc_idx].used_orders_1m;
        let acc_limit = self.limits.account_limit();
        if (acc_used as f32) / (acc_limit as f32) > warn_ratio {
            warn!(
                "Account {} order_count ~{}/{} > {}%",
                account_name,
                acc_used,
                acc_limit,
                (warn_ratio * 100.0) as u32
            );
        }

        let url = format!("{}{}", self.rest.base_url, evt.endpoint);
        let recv_window = self.rest.recv_window_ms.unwrap_or(5000);
        let ts = chrono::Utc::now().timestamp_millis();

        // Merge params + timestamp + recvWindow
        let mut params = evt.params.clone();
        params.insert("timestamp".to_string(), ts.to_string());
        params.insert("recvWindow".to_string(), recv_window.to_string());

        // Build query string (sorted by key as BTreeMap)
        let mut parts: Vec<(String, String)> = params.into_iter().collect();
        parts.sort_by(|a, b| a.0.cmp(&b.0));
        let mut ser = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in &parts {
            ser.append_pair(k, v);
        }
        let query = ser.finish();
        debug!(
            "dispatch request: method={}, url_path={}, qs_keys={:?}",
            evt.method,
            evt.endpoint,
            parts.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>()
        );

        // HMAC-SHA256 signature
        let mut mac = HmacSha256::new_from_slice(secret_value.as_bytes())
            .map_err(|_| anyhow!("invalid secret for account {}", account_name))?;
        mac.update(query.as_bytes());
        let sig = hex::encode(mac.finalize().into_bytes());

        let full_url = format!("{}?{}&signature={}", url, query, sig);
        let client = &self.ip_clients[ip_idx].client;

        let request_builder = match evt.method.as_str() {
            "POST" => client.post(&full_url),
            "DELETE" => client.delete(&full_url),
            "GET" => client.get(&full_url),
            other => return Err(anyhow!("unsupported method: {}", other)),
        };

        let resp = request_builder
            .header("X-MBX-APIKEY", api_key_trimmed)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let (ip_used_1m, acc_used_1m) =
                    self.update_limits_from_headers(ip_idx, acc_idx, r.headers());

                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                // 打印完整响应 JSON（调试模式）；4xx/5xx 同时以 warn 级别输出
                debug!("dispatch response body (raw): {}", text);
                if !status.is_success() {
                    warn!("http {} body: {}", status.as_u16(), text);
                }
                classify_http_and_log(status.as_u16(), &text);
                debug!(
                    "dispatch response: status={}, ip_used_1m={:?}, acc_used_1m={:?}, body_len={}",
                    status.as_u16(),
                    ip_used_1m,
                    acc_used_1m,
                    text.len()
                );

                if status.as_u16() == 429 {
                    warn!("429 Too Many Requests from IP {}. Cooling down.", ip);
                    let ms = self.limits.cooldown_429();
                    self.ip_clients[ip_idx].cooldown_until =
                        Some(Instant::now() + Duration::from_millis(ms));
                }
                if status.as_u16() == 418 {
                    warn!("418 Banned for IP {}. Backing off.", ip);
                    let ms = self.limits.ban_backoff_418();
                    self.ip_clients[ip_idx].banned_until =
                        Some(Instant::now() + Duration::from_millis(ms));
                }
                // Build uniform response regardless of success or error
                Ok(DispatchResponse {
                    status: status.as_u16(),
                    body: text,
                    ip,
                    account: account_name,
                    ip_used_weight_1m: ip_used_1m,
                    order_count_1m: acc_used_1m,
                })
            }
            Err(e) => {
                debug!("dispatch network error: {}", e);
                Ok(DispatchResponse {
                    status: 0,
                    body: format!("request error: {}", e),
                    ip,
                    account: account_name,
                    ip_used_weight_1m: Some(self.ip_clients[ip_idx].used_weight_1m),
                    order_count_1m: Some(self.accounts[acc_idx].used_orders_1m),
                })
            }
        }
    }

    fn update_limits_from_headers(
        &mut self,
        ip_idx: usize,
        acc_idx: usize,
        headers: &HeaderMap,
    ) -> (Option<u32>, Option<u32>) {
        // Headers are case-insensitive and stored lowercase in reqwest
        // Prefer the "-1m" variants when available
        let mut ip_used_generic: Option<u32> = None;
        let mut ip_used_1m: Option<u32> = None;
        let mut acc_used_generic: Option<u32> = None;
        let mut acc_used_1m: Option<u32> = None;

        for (k, v) in headers.iter() {
            let key = k.as_str();
            if key.starts_with("x-mbx-used-weight") {
                if let Ok(s) = v.to_str() {
                    debug!("resp header {}: {}", key, s);
                    if let Ok(n) = s.parse::<u32>() {
                        if key.contains("-1m") {
                            ip_used_1m = Some(n);
                        } else {
                            ip_used_generic = Some(n);
                        }
                    }
                }
            }
            if key.starts_with("x-mbx-order-count") {
                if let Ok(s) = v.to_str() {
                    debug!("resp header {}: {}", key, s);
                    if let Ok(n) = s.parse::<u32>() {
                        if key.contains("-1m") {
                            acc_used_1m = Some(n);
                        } else {
                            acc_used_generic = Some(n);
                        }
                    }
                }
            }
        }

        let ip_final = ip_used_1m.or(ip_used_generic);
        let acc_final = acc_used_1m.or(acc_used_generic);

        if let Some(x) = ip_final {
            self.ip_clients[ip_idx].used_weight_1m = x;
        } else {
            self.ip_clients[ip_idx].used_weight_1m =
                self.ip_clients[ip_idx].used_weight_1m.saturating_add(1);
        }

        if let Some(x) = acc_final {
            self.accounts[acc_idx].used_orders_1m = x;
        } else {
            self.accounts[acc_idx].used_orders_1m =
                self.accounts[acc_idx].used_orders_1m.saturating_add(1);
        }

        debug!(
            "limits from headers => used_weight_1m={:?} (generic={:?}), order_count_1m={:?} (generic={:?})",
            ip_used_1m, ip_used_generic, acc_used_1m, acc_used_generic
        );
        (ip_final, acc_final)
    }
}

#[derive(Debug, Clone)]
pub struct DispatchResponse {
    pub status: u16,
    pub body: String,
    pub ip: IpAddr,
    pub account: String,
    pub ip_used_weight_1m: Option<u32>,
    pub order_count_1m: Option<u32>,
}

fn classify_http_and_log(status: u16, body: &str) {
    if status < 400 {
        return;
    }
    let bl = body.to_ascii_lowercase();
    let summary: String;
    let mut hint: String;

    match status {
        403 => {
            summary = "403 禁止：可能违反 WAF 限制".into();
            hint = "检查请求频率/白名单/风控策略".into();
        }
        418 => {
            summary = "418 已被封禁（此前 429 后继续访问）".into();
            hint = "立即停止请求并按照策略退避，等待解封".into();
        }
        429 => {
            summary = "429 访问频次超限（即将被封 IP）".into();
            hint = "已进入冷却窗口，降低速率或切换出口 IP".into();
        }
        503 => {
            summary = "503 服务不可用/未知结果".into();
            if bl.contains("unknown error, please check your request or try again later.") {
                hint = "请求已提交到业务核心但未获响应，结果未知：可能已执行或失败，需要后续确认"
                    .into();
            } else if bl.contains("service unavailable.") {
                hint = "服务暂不可用：本次请求失败，可稍后重试".into();
            } else if bl
                .contains("internal error; unable to process your request. please try again.")
            {
                hint = "服务端内部错误：本次请求失败，可选择立即重试".into();
            } else {
                hint = "通用 503：建议稍后重试或切换线路".into();
            }
        }
        401 => {
            summary = "401 未授权/权限不足".into();
            hint = "检查 API Key/Secret 是否正确、IP 白名单、接口域与路径是否匹配(PAPI/FAPI)、密钥是否具备该接口权限".into();
        }
        s if (400..500).contains(&s) => {
            summary = "4XX 客户端请求错误（内容/行为/格式）".into();
            hint = "检查参数、签名、权限与持仓模式等".into();
        }
        _ => {
            summary = "5XX 服务端问题".into();
            hint = "服务异常：建议稍后重试；如频繁出现可切换线路".into();
        }
    }

    if bl.contains("request occur unknown error.") {
        if !hint.is_empty() {
            hint.push_str("；");
        }
        hint.push_str("检测到 'Request occur unknown error.'，建议稍后重试");
    }

    warn!(
        "http classify: status={}, {}; 提示：{}",
        status, summary, hint
    );
}
