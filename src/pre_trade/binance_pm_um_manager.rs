use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use log::{debug, info, trace, warn};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 管理币安统一账户（UM）初始化快照。
///
/// - 启动时通过 REST 请求拉取 `/papi/v1/um/account`；
/// - 请求成功后缓存快照供后续风控逻辑使用；
/// - 若初始化失败，则调用方可据此阻止后续的消息订阅处理。
#[derive(Clone)]
pub struct BinancePmUmAccountManager {
    client: Client,
    rest_base: String,
    api_key: String,
    api_secret: String,
    recv_window_ms: u64,
    state: Rc<RefCell<UmAccountState>>,
}

#[derive(Debug, Default)]
struct UmAccountState {
    snapshot: Option<BinanceUmAccountSnapshot>,
}

#[derive(Debug, Clone)]
pub struct BinanceUmAccountSnapshot {
    pub positions: Vec<BinanceUmPosition>,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct BinanceUmPosition {
    pub symbol: String,
    pub position_side: PositionSide,
    pub leverage: f64,
    pub entry_price: f64,
    pub position_amt: f64,
    pub unrealized_profit: f64,
    pub initial_margin: f64,
    pub maint_margin: f64,
    pub position_initial_margin: f64,
    pub open_order_initial_margin: f64,
    pub max_notional: f64,
    pub update_time: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PositionSide {
    Both,
    Long,
    Short,
}

impl fmt::Display for PositionSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let side = match self {
            PositionSide::Both => "BOTH",
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
        };
        write!(f, "{side}")
    }
}

impl PositionSide {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "BOTH" => Ok(PositionSide::Both),
            "LONG" => Ok(PositionSide::Long),
            "SHORT" => Ok(PositionSide::Short),
            other => Err(anyhow!("unsupported positionSide: {}", other)),
        }
    }

    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' | 'b' => Some(PositionSide::Both),
            'L' | 'l' => Some(PositionSide::Long),
            'S' | 's' => Some(PositionSide::Short),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawUmAccountResponse {
    #[serde(default)]
    positions: Vec<RawUmPosition>,
}

#[derive(Debug, Deserialize)]
struct RawUmPosition {
    symbol: String,
    #[serde(rename = "initialMargin")]
    initial_margin: String,
    #[serde(rename = "maintMargin")]
    maint_margin: String,
    #[serde(rename = "unrealizedProfit")]
    unrealized_profit: String,
    #[serde(rename = "positionInitialMargin")]
    position_initial_margin: String,
    #[serde(rename = "openOrderInitialMargin")]
    open_order_initial_margin: String,
    leverage: String,
    #[serde(rename = "entryPrice")]
    entry_price: String,
    #[serde(rename = "maxNotional")]
    max_notional: String,
    #[serde(rename = "positionSide")]
    position_side: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "updateTime", default)]
    update_time: i64,
}

impl TryFrom<RawUmPosition> for BinanceUmPosition {
    type Error = anyhow::Error;

    fn try_from(raw: RawUmPosition) -> Result<Self> {
        let position_side = PositionSide::parse(&raw.position_side)
            .with_context(|| format!("symbol={}", raw.symbol))?;
        Ok(BinanceUmPosition {
            symbol: raw.symbol.clone(),
            position_side,
            leverage: parse_num(&raw.leverage, "leverage", &raw.symbol)?,
            entry_price: parse_num(&raw.entry_price, "entryPrice", &raw.symbol)?,
            position_amt: parse_num(&raw.position_amt, "positionAmt", &raw.symbol)?,
            unrealized_profit: parse_num(&raw.unrealized_profit, "unrealizedProfit", &raw.symbol)?,
            initial_margin: parse_num(&raw.initial_margin, "initialMargin", &raw.symbol)?,
            maint_margin: parse_num(&raw.maint_margin, "maintMargin", &raw.symbol)?,
            position_initial_margin: parse_num(
                &raw.position_initial_margin,
                "positionInitialMargin",
                &raw.symbol,
            )?,
            open_order_initial_margin: parse_num(
                &raw.open_order_initial_margin,
                "openOrderInitialMargin",
                &raw.symbol,
            )?,
            max_notional: parse_num(&raw.max_notional, "maxNotional", &raw.symbol)?,
            update_time: raw.update_time,
        })
    }
}

impl BinancePmUmAccountManager {
    pub fn new(
        rest_base: impl Into<String>,
        api_key: impl Into<String>,
        api_secret: impl Into<String>,
        recv_window_ms: u64,
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
            state: Rc::new(RefCell::new(UmAccountState::default())),
        }
    }

    /// 首次初始化；请求失败则返回错误，调用方可阻止后续流程。
    pub async fn init(&self) -> Result<BinanceUmAccountSnapshot> {
        let snapshot = self.fetch_snapshot().await?;
        let mut state = self.state.borrow_mut();
        state.snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    /// 当前是否已经完成初始化。
    pub fn is_initialized(&self) -> bool {
        self.state.borrow().snapshot.is_some()
    }

    /// 返回最近的账户快照副本。
    pub fn snapshot(&self) -> Option<BinanceUmAccountSnapshot> {
        self.state.borrow().snapshot.clone()
    }

    pub fn apply_position_update(
        &self,
        symbol: &str,
        position_side: char,
        position_amount: f64,
        entry_price: f64,
        unrealized_pnl: f64,
        breakeven_price: f64,
        update_time: i64,
    ) {
        let Some(side) = PositionSide::from_char(position_side) else {
            warn!(
                "ignored account position update for symbol={} unknown side={}",
                symbol, position_side
            );
            return;
        };

        let upper = symbol.to_uppercase();
        let mut state = self.state.borrow_mut();
        let Some(snapshot) = state.snapshot.as_mut() else {
            return;
        };

        snapshot.fetched_at = Utc::now();

        if let Some(position) = snapshot
            .positions
            .iter_mut()
            .find(|pos| pos.symbol.eq_ignore_ascii_case(&upper) && pos.position_side == side)
        {
            position.position_amt = position_amount;
            position.entry_price = entry_price;
            position.unrealized_profit = unrealized_pnl;
            position.update_time = update_time;
            info!(
                "UM持仓更新 symbol={} side={} 持仓量={} 入场价={} 未实现盈亏={} 更新时间={}",
                position.symbol.as_str(),
                position.position_side,
                position.position_amt,
                position.entry_price,
                position.unrealized_profit,
                update_time
            );
            return;
        }

        let new_position = BinanceUmPosition {
            symbol: upper,
            position_side: side,
            leverage: 0.0,
            entry_price,
            position_amt: position_amount,
            unrealized_profit: unrealized_pnl,
            initial_margin: 0.0,
            maint_margin: 0.0,
            position_initial_margin: 0.0,
            open_order_initial_margin: 0.0,
            max_notional: 0.0,
            update_time,
        };
        if breakeven_price != 0.0 {
            debug!(
                "account position update new symbol={} side={:?} breakeven={} amount={}",
                symbol, side, breakeven_price, position_amount
            );
        }
        info!(
            "新增UM持仓 symbol={} side={} 持仓量={} 入场价={} 未实现盈亏={} breakeven={} 更新时间={}",
            symbol,
            side,
            position_amount,
            entry_price,
            unrealized_pnl,
            breakeven_price,
            update_time
        );
        snapshot.positions.push(new_position);
    }

    async fn fetch_snapshot(&self) -> Result<BinanceUmAccountSnapshot> {
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
            "{}/papi/v1/um/account?{}&signature={}",
            self.rest_base, query, signature
        );

        trace!("requesting Binance UM account snapshot");
        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!(
                "GET /papi/v1/um/account failed: {} - {}",
                status,
                body
            ));
        }
        let raw: RawUmAccountResponse = serde_json::from_str(&body)
            .context("failed to deserialize unified margin account response")?;

        let positions: Vec<BinanceUmPosition> = raw
            .positions
            .into_iter()
            .filter_map(|raw_pos| match raw_pos.try_into() {
                Ok(pos) => Some(pos),
                Err(err) => {
                    warn!("skip invalid position entry: {err:#}");
                    None
                }
            })
            .filter(|pos: &BinanceUmPosition| {
                pos.position_amt != 0.0
                    || pos.open_order_initial_margin != 0.0
                    || pos.position_initial_margin != 0.0
            })
            .collect();

        let snapshot = BinanceUmAccountSnapshot {
            positions,
            fetched_at: Utc::now(),
        };

        debug!(
            "fetched Binance UM snapshot: positions={} (active={})",
            snapshot.positions.len(),
            snapshot
                .positions
                .iter()
                .filter(|p| p.position_amt != 0.0)
                .count()
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

fn parse_num(value: &str, field: &str, symbol: &str) -> Result<f64> {
    if value.trim().is_empty() {
        return Ok(0.0);
    }
    value
        .parse::<f64>()
        .with_context(|| format!("symbol={} field={}", symbol, field))
}
