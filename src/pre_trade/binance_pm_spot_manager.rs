use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use log::{debug, trace, warn};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 币安统一账户（Portfolio Margin）现货余额初始化管理器。
///
/// - 通过 `/papi/v1/balance` 拉取统一账户余额；
/// - 请求成功后缓存快照；
/// - 若初始化失败，调用方可阻止后续消息处理。
#[derive(Clone)]
pub struct BinancePmSpotAccountManager {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
    asset_filter: Option<String>,
    state: Rc<RefCell<SpotAccountState>>, // 单线程内共享
}

#[derive(Debug, Default)]
struct SpotAccountState {
    snapshot: Option<BinanceSpotBalanceSnapshot>,
}

#[derive(Debug, Clone)]
pub struct BinanceSpotBalanceSnapshot {
    pub balances: Vec<BinanceSpotBalance>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct BinanceSpotBalance {
    pub asset: String,
    pub total_wallet_balance: f64,
    pub cross_margin_asset: f64,
    pub cross_margin_borrowed: f64,
    pub cross_margin_free: f64,
    pub cross_margin_interest: f64,
    pub cross_margin_locked: f64,
    pub um_wallet_balance: f64,
    pub um_unrealized_pnl: f64,
    pub cm_wallet_balance: f64,
    pub cm_unrealized_pnl: f64,
    pub update_time: i64,
    pub negative_balance: bool,
}

#[derive(Debug, Deserialize)]
struct RawSpotBalance {
    asset: String,
    #[serde(rename = "totalWalletBalance", default)]
    total_wallet_balance: String,
    #[serde(rename = "crossMarginAsset", default)]
    cross_margin_asset: String,
    #[serde(rename = "crossMarginBorrowed", default)]
    cross_margin_borrowed: String,
    #[serde(rename = "crossMarginFree", default)]
    cross_margin_free: String,
    #[serde(rename = "crossMarginInterest", default)]
    cross_margin_interest: String,
    #[serde(rename = "crossMarginLocked", default)]
    cross_margin_locked: String,
    #[serde(rename = "umWalletBalance", default)]
    um_wallet_balance: String,
    #[serde(rename = "umUnrealizedPNL", default)]
    um_unrealized_pnl: String,
    #[serde(rename = "cmWalletBalance", default)]
    cm_wallet_balance: String,
    #[serde(rename = "cmUnrealizedPNL", default)]
    cm_unrealized_pnl: String,
    #[serde(rename = "updateTime", default)]
    update_time: i64,
    #[serde(rename = "negativeBalance", default)]
    negative_balance: String,
}

impl TryFrom<RawSpotBalance> for BinanceSpotBalance {
    type Error = anyhow::Error;

    fn try_from(raw: RawSpotBalance) -> Result<Self> {
        Ok(BinanceSpotBalance {
            asset: raw.asset.clone(),
            total_wallet_balance: parse_num(
                &raw.total_wallet_balance,
                "totalWalletBalance",
                &raw.asset,
            )?,
            cross_margin_asset: parse_num(&raw.cross_margin_asset, "crossMarginAsset", &raw.asset)?,
            cross_margin_borrowed: parse_num(
                &raw.cross_margin_borrowed,
                "crossMarginBorrowed",
                &raw.asset,
            )?,
            cross_margin_free: parse_num(&raw.cross_margin_free, "crossMarginFree", &raw.asset)?,
            cross_margin_interest: parse_num(
                &raw.cross_margin_interest,
                "crossMarginInterest",
                &raw.asset,
            )?,
            cross_margin_locked: parse_num(
                &raw.cross_margin_locked,
                "crossMarginLocked",
                &raw.asset,
            )?,
            um_wallet_balance: parse_num(&raw.um_wallet_balance, "umWalletBalance", &raw.asset)?,
            um_unrealized_pnl: parse_num(&raw.um_unrealized_pnl, "umUnrealizedPNL", &raw.asset)?,
            cm_wallet_balance: parse_num(&raw.cm_wallet_balance, "cmWalletBalance", &raw.asset)?,
            cm_unrealized_pnl: parse_num(&raw.cm_unrealized_pnl, "cmUnrealizedPNL", &raw.asset)?,
            update_time: raw.update_time,
            negative_balance: parse_negative_flag(&raw.negative_balance, &raw.asset)?,
        })
    }
}

impl BinancePmSpotAccountManager {
    pub fn new(
        rest_base: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        recv_window_ms: u64,
        asset_filter: Option<String>,
    ) -> Self {
        let client = Client::new();
        let rest_base = rest_base.into();
        let rest_base = rest_base.trim_end_matches('/').to_string();
        Self {
            client,
            rest_base,
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            recv_window_ms,
            asset_filter: asset_filter.map(|x| x.to_uppercase()),
            state: Rc::new(RefCell::new(SpotAccountState::default())),
        }
    }

    pub async fn init(&self) -> Result<BinanceSpotBalanceSnapshot> {
        let snapshot = self.fetch_snapshot().await?;
        let mut state = self.state.borrow_mut();
        state.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub fn is_initialized(&self) -> bool {
        self.state.borrow().snapshot.is_some()
    }

    pub fn snapshot(&self) -> Option<BinanceSpotBalanceSnapshot> {
        self.state.borrow().snapshot.clone()
    }

    pub fn apply_balance_delta(&self, asset: &str, delta: f64, update_time: i64) {
        let upper = asset.to_uppercase();
        let mut state = self.state.borrow_mut();
        let Some(snapshot) = state.snapshot.as_mut() else {
            return;
        };

        snapshot.fetched_at = Utc::now();

        if let Some(balance) = snapshot
            .balances
            .iter_mut()
            .find(|bal| bal.asset.eq_ignore_ascii_case(&upper))
        {
            balance.total_wallet_balance += delta;
            balance.cross_margin_free += delta;
            balance.cross_margin_asset = balance.cross_margin_free + balance.cross_margin_locked;
            balance.update_time = update_time;
            balance.negative_balance = balance.cross_margin_free < 0.0;
            return;
        }

        let new_balance = BinanceSpotBalance {
            asset: upper.clone(),
            total_wallet_balance: delta,
            cross_margin_asset: delta,
            cross_margin_borrowed: 0.0,
            cross_margin_free: delta,
            cross_margin_interest: 0.0,
            cross_margin_locked: 0.0,
            um_wallet_balance: 0.0,
            um_unrealized_pnl: 0.0,
            cm_wallet_balance: 0.0,
            cm_unrealized_pnl: 0.0,
            update_time,
            negative_balance: delta < 0.0,
        };
        snapshot.balances.push(new_balance);
    }

    pub fn apply_balance_snapshot(
        &self,
        asset: &str,
        wallet_balance: f64,
        cross_wallet_balance: f64,
        _balance_change: f64,
        update_time: i64,
    ) {
        let upper = asset.to_uppercase();
        let mut state = self.state.borrow_mut();
        let Some(snapshot) = state.snapshot.as_mut() else {
            return;
        };

        snapshot.fetched_at = Utc::now();

        if let Some(balance) = snapshot
            .balances
            .iter_mut()
            .find(|bal| bal.asset.eq_ignore_ascii_case(&upper))
        {
            balance.total_wallet_balance = wallet_balance;
            balance.cross_margin_asset = cross_wallet_balance;
            let locked = balance.cross_margin_locked;
            if cross_wallet_balance >= locked {
                balance.cross_margin_free = cross_wallet_balance - locked;
            } else {
                balance.cross_margin_locked = 0.0;
                balance.cross_margin_free = cross_wallet_balance;
            }
            balance.update_time = update_time;
            balance.negative_balance =
                balance.total_wallet_balance < 0.0 || balance.cross_margin_free < 0.0;
            return;
        }

        let balance = BinanceSpotBalance {
            asset: upper.clone(),
            total_wallet_balance: wallet_balance,
            cross_margin_asset: cross_wallet_balance,
            cross_margin_borrowed: 0.0,
            cross_margin_free: cross_wallet_balance,
            cross_margin_interest: 0.0,
            cross_margin_locked: 0.0,
            um_wallet_balance: 0.0,
            um_unrealized_pnl: 0.0,
            cm_wallet_balance: 0.0,
            cm_unrealized_pnl: 0.0,
            update_time,
            negative_balance: wallet_balance < 0.0 || cross_wallet_balance < 0.0,
        };
        snapshot.balances.push(balance);
    }

    async fn fetch_snapshot(&self) -> Result<BinanceSpotBalanceSnapshot> {
        let mut params = BTreeMap::new();
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        if self.recv_window_ms > 0 {
            params.insert("recvWindow".to_string(), self.recv_window_ms.to_string());
        }
        if let Some(asset) = &self.asset_filter {
            params.insert("asset".to_string(), asset.clone());
        }

        let query = build_query(&params);
        let signature = self.sign_query(&query)?;
        let url = format!(
            "{}/papi/v1/balance?{}&signature={}",
            self.rest_base, query, signature
        );

        trace!("requesting Binance PM balance snapshot");
        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;

        debug!(
            "Binance PM balance response: status={} body={}",
            status, body
        );
        if !status.is_success() {
            return Err(anyhow!(
                "GET /papi/v1/balance failed: {} - {}",
                status,
                body
            ));
        }

        let raw_balances = match serde_json::from_str::<Vec<RawSpotBalance>>(&body) {
            Ok(list) => list,
            Err(err) => {
                debug!("balance response not array: {err}; trying single object");
                let single: RawSpotBalance = serde_json::from_str(&body)
                    .context("failed to parse balance response as object")?;
                vec![single]
            }
        };

        let balances: Vec<BinanceSpotBalance> = raw_balances
            .into_iter()
            .filter_map(|raw| match raw.try_into() {
                Ok(balance) => Some(balance),
                Err(err) => {
                    warn!("skip invalid balance entry: {err:#}");
                    None
                }
            })
            .collect();

        if balances.is_empty() {
            return Err(anyhow!("balance snapshot empty after parsing"));
        }

        let snapshot = BinanceSpotBalanceSnapshot {
            balances,
            fetched_at: Utc::now(),
        };

        if let Some(asset) = &self.asset_filter {
            if snapshot
                .balances
                .iter()
                .all(|b| b.asset.to_uppercase() != *asset)
            {
                warn!("balance snapshot missing requested asset {}", asset);
            }
        }

        debug!(
            "fetched Binance PM balance snapshot: assets={}",
            snapshot.balances.len()
        );
        Ok(snapshot)
    }

    fn sign_query(&self, query: &str) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| anyhow!("invalid API secret"))?;
        mac.update(query.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

fn build_query(params: &BTreeMap<String, String>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in params.iter() {
        serializer.append_pair(k, v);
    }
    serializer.finish()
}

fn parse_num(value: &str, field: &str, asset: &str) -> Result<f64> {
    if value.trim().is_empty() {
        return Ok(0.0);
    }
    value
        .parse::<f64>()
        .with_context(|| format!("asset={} field={}", asset, field))
}

fn parse_negative_flag(value: &str, asset: &str) -> Result<bool> {
    if value.trim().is_empty() {
        return Ok(false);
    }

    let trimmed = value.trim();
    match trimmed {
        "0" | "false" | "FALSE" => Ok(false),
        "1" | "true" | "TRUE" => Ok(true),
        _ => {
            if let Ok(num) = trimmed.parse::<f64>() {
                Ok(num != 0.0)
            } else {
                Err(anyhow!(
                    "asset={} invalid negativeBalance value: {}",
                    asset,
                    trimmed
                ))
            }
        }
    }
}
