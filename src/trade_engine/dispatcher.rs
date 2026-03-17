use crate::trade_engine::config::{ApiKey, LimitConstants, RestConstants};
use crate::trade_engine::order_event::OrderRequestEvent;
use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use log::{debug, warn};
use reqwest::{header::HeaderMap, Client};
use serde_json::Value;
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
    window_started_at: Instant,
    cooldown_until: Option<Instant>,
    banned_until: Option<Instant>,
}

impl IpClient {
    fn reset_if_window_elapsed(&mut self, now: Instant) {
        if now.duration_since(self.window_started_at) >= Duration::from_secs(60) {
            self.used_weight_1m = 0;
            self.window_started_at = now;
        }
    }

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
    window_started_at: Instant,
}

impl AccountState {
    fn reset_if_window_elapsed(&mut self, now: Instant) {
        if now.duration_since(self.window_started_at) >= Duration::from_secs(60) {
            self.used_orders_1m = 0;
            self.window_started_at = now;
        }
    }
}

pub struct Dispatcher {
    base_url_papi: String,
    base_url_fapi: String,
    base_url_sapi: String,
    ip_clients: Vec<IpClient>,
    accounts: Vec<AccountState>,
}

impl Dispatcher {
    pub fn new(local_ips: &[IpAddr], account_keys: &[ApiKey]) -> Result<Self> {
        // Build clients per IP
        let mut ip_clients = Vec::new();
        for ip in local_ips {
            let builder = reqwest::Client::builder()
                .local_address(*ip)
                .tcp_keepalive(Some(Duration::from_secs(30)))
                .timeout(Duration::from_millis(RestConstants::TIMEOUT_MS));
            let client = builder.build()?;
            let now = Instant::now();
            ip_clients.push(IpClient {
                ip: *ip,
                client,
                used_weight_1m: 0,
                window_started_at: now,
                cooldown_until: None,
                banned_until: None,
            });
        }

        if account_keys.is_empty() {
            return Err(anyhow!("no API keys configured for dispatcher"));
        }
        let accounts = account_keys
            .iter()
            .cloned()
            .map(|key| AccountState {
                key,
                used_orders_1m: 0,
                window_started_at: Instant::now(),
            })
            .collect();

        let base_url_papi = std::env::var("BINANCE_PAPI_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| RestConstants::BINANCE_BASE_URL.to_string());
        let base_url_fapi = std::env::var("BINANCE_FAPI_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| RestConstants::BINANCE_FAPI_BASE_URL.to_string());
        let base_url_sapi = std::env::var("BINANCE_SAPI_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| RestConstants::BINANCE_SAPI_BASE_URL.to_string());

        Ok(Self {
            base_url_papi,
            base_url_fapi,
            base_url_sapi,
            ip_clients,
            accounts,
        })
    }

    fn refresh_limit_windows(&mut self) {
        let now = Instant::now();
        for client in &mut self.ip_clients {
            client.reset_if_window_elapsed(now);
        }
        for account in &mut self.accounts {
            account.reset_if_window_elapsed(now);
        }
    }

    fn select_ip(&self, req_weight: u32) -> Option<usize> {
        let limit = LimitConstants::IP_WEIGHT_PER_MIN;
        let mut best: Option<(usize, f32)> = None; // index, score (remaining ratio)
        for (i, c) in self.ip_clients.iter().enumerate() {
            if !c.is_available() {
                continue;
            }
            let used = c.used_weight_1m as f32;
            if used + req_weight as f32 > limit as f32 {
                continue;
            }
            let rem_ratio =
                ((limit as f32 - used - req_weight as f32) / limit as f32).clamp(-1.0, 1.0);
            if best.map(|(_, s)| rem_ratio > s).unwrap_or(true) {
                best = Some((i, rem_ratio));
            }
        }
        best.map(|(i, _)| i)
    }

    fn select_account(&self, hint: Option<&str>, enforce_order_limit: bool) -> Option<usize> {
        if let Some(h) = hint {
            let idx = self.accounts.iter().position(|a| a.key.name == h)?;
            if enforce_order_limit
                && self.accounts[idx].used_orders_1m >= LimitConstants::ACCOUNT_PER_MIN
            {
                return None;
            }
            return Some(idx);
        }
        if !enforce_order_limit {
            return (!self.accounts.is_empty()).then_some(0);
        }
        let limit = LimitConstants::ACCOUNT_PER_MIN;
        let mut best: Option<(usize, f32)> = None; // index, score
        for (i, a) in self.accounts.iter().enumerate() {
            let used = a.used_orders_1m as f32;
            if used >= limit as f32 {
                continue;
            }
            let rem_ratio = ((limit as f32 - used) / limit as f32).clamp(-1.0, 1.0);
            if best.map(|(_, s)| rem_ratio > s).unwrap_or(true) {
                best = Some((i, rem_ratio));
            }
        }
        best.map(|(i, _)| i)
    }

    pub async fn dispatch(&mut self, evt: OrderRequestEvent) -> Result<DispatchResponse> {
        self.refresh_limit_windows();
        let ip_idx = self
            .select_ip(evt.weight())
            .ok_or_else(|| anyhow!("no available IP client (cooldown/banned or all saturated)"))?;
        let acc_idx = self
            .select_account(evt.account.as_deref(), evt.counts_toward_order_limit)
            .ok_or_else(|| anyhow!("no available account key (all saturated)"))?;

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
        let warn_ratio = LimitConstants::WARN_RATIO;
        let ip_used = self.ip_clients[ip_idx].used_weight_1m;
        let ip_limit = LimitConstants::IP_WEIGHT_PER_MIN;
        if (ip_used as f32) / (ip_limit as f32) > warn_ratio {
            warn!(
                "IP {} used_weight ~{}/{} > {}%",
                ip,
                ip_used,
                ip_limit,
                (warn_ratio * 100.0) as u32
            );
        }
        if evt.counts_toward_order_limit {
            let acc_used = self.accounts[acc_idx].used_orders_1m;
            let acc_limit = LimitConstants::ACCOUNT_PER_MIN;
            if (acc_used as f32) / (acc_limit as f32) > warn_ratio {
                warn!(
                    "Account {} order_count ~{}/{} > {}%",
                    account_name,
                    acc_used,
                    acc_limit,
                    (warn_ratio * 100.0) as u32
                );
            }
        }

        let base_url = if evt.endpoint.starts_with("/fapi/") {
            &self.base_url_fapi
        } else if evt.endpoint.starts_with("/api/") || evt.endpoint.starts_with("/sapi/") {
            &self.base_url_sapi
        } else {
            &self.base_url_papi
        };
        let url = format!("{}{}", base_url, evt.endpoint);
        let recv_window = RestConstants::RECV_WINDOW_MS;
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
                let (ip_used_1m, acc_used_1m) = self.update_limits_from_headers(
                    ip_idx,
                    acc_idx,
                    r.headers(),
                    evt.counts_toward_order_limit,
                );

                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                if is_duplicate_order_sent(status.as_u16(), &text) {
                    panic!(
                        "binance duplicate order detected: endpoint={} account={} ip={} req_id={:?} body={}",
                        evt.endpoint, account_name, ip, evt.req_id, text
                    );
                }
                let suppress_http_warn = should_suppress_http_error_log(status.as_u16(), &text);
                let top_level_code = parse_top_level_error_code(&text);
                // 打印完整响应 JSON（调试模式）；4xx/5xx 同时以 warn 级别输出
                debug!("dispatch response body (raw): {}", text);
                if !status.is_success() {
                    if suppress_http_warn {
                        debug!(
                            "http {} body: [suppressed code={}]",
                            status.as_u16(),
                            top_level_code
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "unknown".to_string())
                        );
                    } else {
                        warn!("http {} body: {}", status.as_u16(), text);
                    }
                }
                if !suppress_http_warn {
                    classify_http_and_log(status.as_u16(), &text);
                }
                debug!(
                    "dispatch response: status={}, ip_used_1m={:?}, acc_used_1m={:?}, body_len={}",
                    status.as_u16(),
                    ip_used_1m,
                    acc_used_1m,
                    text.len()
                );

                if status.as_u16() == 429 {
                    warn!("429 Too Many Requests from IP {}. Cooling down.", ip);
                    self.ip_clients[ip_idx].cooldown_until = Some(
                        Instant::now() + Duration::from_millis(LimitConstants::COOLDOWN_MS_429),
                    );
                }
                if status.as_u16() == 418 {
                    warn!("418 Banned for IP {}. Backing off.", ip);
                    self.ip_clients[ip_idx].banned_until = Some(
                        Instant::now() + Duration::from_millis(LimitConstants::BAN_BACKOFF_MS_418),
                    );
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
        fallback_increment_order_count: bool,
    ) -> (Option<u32>, Option<u32>) {
        self.refresh_limit_windows();
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
        } else if fallback_increment_order_count {
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

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderName, HeaderValue};
    use std::net::{IpAddr, Ipv4Addr};

    fn test_dispatcher() -> Dispatcher {
        Dispatcher::new(
            &[IpAddr::V4(Ipv4Addr::LOCALHOST)],
            &[ApiKey {
                name: "default".to_string(),
                key: "key".to_string(),
                secret: "secret".to_string(),
            }],
        )
        .expect("dispatcher")
    }

    #[test]
    fn refresh_limit_windows_resets_stale_state() {
        let mut dispatcher = test_dispatcher();
        dispatcher.ip_clients[0].used_weight_1m = 123;
        dispatcher.ip_clients[0].window_started_at = Instant::now() - Duration::from_secs(61);
        dispatcher.accounts[0].used_orders_1m = 456;
        dispatcher.accounts[0].window_started_at = Instant::now() - Duration::from_secs(61);

        dispatcher.refresh_limit_windows();

        assert_eq!(dispatcher.ip_clients[0].used_weight_1m, 0);
        assert_eq!(dispatcher.accounts[0].used_orders_1m, 0);
    }

    #[test]
    fn query_requests_do_not_increment_order_count_without_headers() {
        let mut dispatcher = test_dispatcher();
        dispatcher.accounts[0].used_orders_1m = 99;
        let headers = HeaderMap::new();

        let (_ip_used, acc_used) = dispatcher.update_limits_from_headers(0, 0, &headers, false);

        assert_eq!(acc_used, None);
        assert_eq!(dispatcher.accounts[0].used_orders_1m, 99);
        assert_eq!(dispatcher.ip_clients[0].used_weight_1m, 1);
    }

    #[test]
    fn order_headers_override_local_counter() {
        let mut dispatcher = test_dispatcher();
        dispatcher.accounts[0].used_orders_1m = 99;
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-mbx-order-count-1m"),
            HeaderValue::from_static("12"),
        );

        let (_ip_used, acc_used) = dispatcher.update_limits_from_headers(0, 0, &headers, true);

        assert_eq!(acc_used, Some(12));
        assert_eq!(dispatcher.accounts[0].used_orders_1m, 12);
    }

    #[test]
    fn select_account_rejects_saturated_accounts_for_order_requests() {
        let mut dispatcher = test_dispatcher();
        dispatcher.accounts[0].used_orders_1m = LimitConstants::ACCOUNT_PER_MIN;

        assert_eq!(dispatcher.select_account(None, true), None);
        assert_eq!(dispatcher.select_account(None, false), Some(0));
    }

    #[test]
    fn detects_duplicate_order_sent_response() {
        assert!(is_duplicate_order_sent(
            400,
            r#"{"code":-2010,"msg":"Duplicate order sent."}"#
        ));
        assert!(!is_duplicate_order_sent(
            400,
            r#"{"code":-2010,"msg":"New order rejected"}"#
        ));
        assert!(!is_duplicate_order_sent(
            200,
            r#"{"code":-2010,"msg":"Duplicate order sent."}"#
        ));
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

fn parse_top_level_error_code(body: &str) -> Option<i32> {
    let v: Value = serde_json::from_str(body).ok()?;
    let code = v.get("code")?;
    if let Some(n) = code.as_i64() {
        return i32::try_from(n).ok();
    }
    if let Some(s) = code.as_str() {
        return s.parse::<i32>().ok();
    }
    None
}

fn parse_top_level_error_msg(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    v.get("msg")
        .and_then(|msg| msg.as_str())
        .map(str::to_string)
}

fn is_duplicate_order_sent(status: u16, body: &str) -> bool {
    if status != 400 {
        return false;
    }
    matches!(
        parse_top_level_error_msg(body).as_deref(),
        Some("Duplicate order sent.")
    )
}

fn should_suppress_http_error_log(status: u16, body: &str) -> bool {
    if status != 400 {
        return false;
    }
    matches!(parse_top_level_error_code(body), Some(51169 | 51061))
}
