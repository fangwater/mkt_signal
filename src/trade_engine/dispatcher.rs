use crate::trade_engine::config::{ApiKey, LimitsCfg, RestCfg, TradeEngineCfg};
use crate::trade_engine::order_event::OrderRequestEvent;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use reqwest::{header::HeaderMap, Client};
use sha2::Sha256;
use std::collections::HashMap;
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
        if let Some(t) = self.cooldown_until { if now < t { return false; } }
        if let Some(t) = self.banned_until { if now < t { return false; } }
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
            if let Some(ms) = cfg.rest.timeout_ms { builder = builder.timeout(Duration::from_millis(ms)); }
            let client = builder.build()?;
            ip_clients.push(IpClient{ ip: *ip, client, used_weight_1m: 0, cooldown_until: None, banned_until: None });
        }

        let accounts = cfg.accounts.keys.iter().cloned().map(|key| AccountState{ key, used_orders_1m: 0 }).collect();

        Ok(Self { rest: cfg.rest.clone(), limits: cfg.limits.clone(), ip_clients, accounts })
    }

    fn select_ip(&self, req_weight: u32) -> Option<usize> {
        let limit = self.limits.ip_weight_limit();
        let mut best: Option<(usize, f32)> = None; // index, score (remaining ratio)
        for (i, c) in self.ip_clients.iter().enumerate() {
            if !c.is_available() { continue; }
            let used = c.used_weight_1m as f32;
            let rem_ratio = ((limit as f32 - used - req_weight as f32) / limit as f32).clamp(-1.0, 1.0);
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
        let ip_idx = self.select_ip(evt.weight())
            .ok_or_else(|| anyhow!("no available IP client (cooldown/banned or all saturated)"))?;
        let acc_idx = self.select_account(evt.account.as_deref())
            .ok_or_else(|| anyhow!("no available account key"))?;

        let ip = self.ip_clients[ip_idx].ip;
        // clone required account values to avoid borrow issues across mutation
        let account_name = self.accounts[acc_idx].key.name.clone();
        let api_key_value = self.accounts[acc_idx].key.key.clone();
        let secret_value = self.accounts[acc_idx].key.secret.clone();

        // Warn when approaching limits
        let warn_ratio = self.limits.warn_ratio();
        let ip_used = self.ip_clients[ip_idx].used_weight_1m;
        let ip_limit = self.limits.ip_weight_limit();
        if (ip_used as f32) / (ip_limit as f32) > warn_ratio {
            warn!("IP {} used_weight ~{}/{} > {}%", ip, ip_used, ip_limit, (warn_ratio * 100.0) as u32);
        }
        let acc_used = self.accounts[acc_idx].used_orders_1m;
        let acc_limit = self.limits.account_limit();
        if (acc_used as f32) / (acc_limit as f32) > warn_ratio {
            warn!("Account {} order_count ~{}/{} > {}%", account_name, acc_used, acc_limit, (warn_ratio * 100.0) as u32);
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
        for (k, v) in &parts { ser.append_pair(k, v); }
        let query = ser.finish();

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
            .header("X-MBX-APIKEY", &api_key_value)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let (ip_used_1m, acc_used_1m) = self.update_limits_from_headers(ip_idx, acc_idx, r.headers());

                let status = r.status();
                let text = r.text().await.unwrap_or_default();

                if status.as_u16() == 429 {
                    warn!("429 Too Many Requests from IP {}. Cooling down.", ip);
                    let ms = self.limits.cooldown_429();
                    self.ip_clients[ip_idx].cooldown_until = Some(Instant::now() + Duration::from_millis(ms));
                }
                if status.as_u16() == 418 {
                    warn!("418 Banned for IP {}. Backing off.", ip);
                    let ms = self.limits.ban_backoff_418();
                    self.ip_clients[ip_idx].banned_until = Some(Instant::now() + Duration::from_millis(ms));
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

    fn update_limits_from_headers(&mut self, ip_idx: usize, acc_idx: usize, headers: &HeaderMap) -> (Option<u32>, Option<u32>) {
        // Headers are case-insensitive and stored lowercase in reqwest
        // X-MBX-USED-WEIGHT(-1m) and X-MBX-ORDER-COUNT(-1m)
        let mut ip_used_1m: Option<u32> = None;
        let mut acc_used_1m: Option<u32> = None;
        for (k, v) in headers.iter() {
            let key = k.as_str();
            if key.starts_with("x-mbx-used-weight") {
                if let Ok(s) = v.to_str() { if let Ok(n) = s.parse::<u32>() { ip_used_1m = Some(n); } }
            }
            if key.starts_with("x-mbx-order-count") {
                if let Ok(s) = v.to_str() { if let Ok(n) = s.parse::<u32>() { acc_used_1m = Some(n); } }
            }
        }

        if let Some(x) = ip_used_1m { self.ip_clients[ip_idx].used_weight_1m = x; }
        else { self.ip_clients[ip_idx].used_weight_1m = self.ip_clients[ip_idx].used_weight_1m.saturating_add(1); }

        if let Some(x) = acc_used_1m { self.accounts[acc_idx].used_orders_1m = x; }
        else { self.accounts[acc_idx].used_orders_1m = self.accounts[acc_idx].used_orders_1m.saturating_add(1); }

        (ip_used_1m, acc_used_1m)
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
