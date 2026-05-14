//! Gate-only REST 补丁：每分钟拉 `/futures/usdt/contracts` 把 `funding_rate` 灌进 MktChannel。
//!
//! 背景：Gate 的 `futures.tickers` 是事件驱动；冷门 symbol 长时间无推送，TS 的
//! `current_fr_ma` 一直拿不到，整个 DecisionRouter 卡在 degraded mode。
//! 这里用同一个 `funding_rate` 字段（REST contracts 端点 = WS futures.tickers）兜底，
//! WS 继续走 rolling buffer 的 push，新 WS 样本会通过 VecDeque 自然把旧的 REST 样本顶出。
//!
//! 仅对 Gate 启用。其它 venue 完全不调用。
use std::time::Duration;

use anyhow::{Context, Result};
use log::{info, warn};
use serde::Deserialize;

use crate::funding_rate::mkt_channel::MktChannel;
use crate::signal::common::TradingVenue;

const GATE_FUTURES_CONTRACTS_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
const REQ_TIMEOUT_SECS: u64 = 10;

/// 默认补丁周期：每 60 秒拉一次 + seed 一次。
pub const SEED_INTERVAL_SECS: u64 = 60;

#[derive(Deserialize)]
struct GateContract {
    name: String,
    /// 字符串形式的 funding_rate（"0.0001" 之类），跟 WS futures.tickers 同字段。
    funding_rate: Option<String>,
    /// "trading" / "delisted" / ...
    status: Option<String>,
    in_delisting: Option<bool>,
}

/// 拉一次 contracts 列表，返回 (symbol, funding_rate) 列表（仅 trading 且 funding_rate 合法）。
async fn fetch_gate_current_funding_rates() -> Result<Vec<(String, f64)>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQ_TIMEOUT_SECS))
        .build()
        .context("build reqwest client")?;
    let resp = client
        .get(GATE_FUTURES_CONTRACTS_URL)
        .send()
        .await
        .context("GET gate /futures/usdt/contracts")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("gate contracts http {} body={}", status.as_u16(), body);
    }
    let contracts: Vec<GateContract> = resp.json().await.context("parse contracts JSON")?;

    let mut out = Vec::with_capacity(contracts.len());
    for c in contracts {
        if c.status.as_deref() != Some("trading") {
            continue;
        }
        if c.in_delisting.unwrap_or(false) {
            continue;
        }
        let Some(s) = c.funding_rate else {
            continue;
        };
        let Ok(fr) = s.trim().parse::<f64>() else {
            continue;
        };
        if !fr.is_finite() {
            continue;
        }
        out.push((c.name, fr));
    }
    Ok(out)
}

/// 启动 Gate 专用 REST 补丁循环：首 tick 立即执行（暖启动），之后每 60s 一次。
/// 必须在 `spawn_local` 上下文里调用（依赖 MktChannel 的 thread-local）。
/// `hedge_venue` 不是 `GateFutures` 时直接返回，不挂任务。
pub fn spawn_gate_current_fr_seeder(hedge_venue: TradingVenue) {
    if !matches!(hedge_venue, TradingVenue::GateFutures) {
        return;
    }
    tokio::task::spawn_local(async move {
        info!(
            "Gate current FR REST seeder: interval={}s venue={:?}",
            SEED_INTERVAL_SECS, hedge_venue
        );
        let mut tick = tokio::time::interval(Duration::from_secs(SEED_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match fetch_gate_current_funding_rates().await {
                Ok(list) => {
                    let mc = MktChannel::instance();
                    let mut seeded = 0usize;
                    let mut skipped = 0usize;
                    for (sym, fr) in &list {
                        if mc.seed_funding_rate(sym, hedge_venue, *fr) {
                            seeded += 1;
                        } else {
                            skipped += 1;
                        }
                    }
                    info!(
                        "Gate FR seed: fetched={} seeded={} skipped={}",
                        list.len(),
                        seeded,
                        skipped
                    );
                }
                Err(e) => warn!("Gate FR seed fetch failed: {:#}", e),
            }
        }
    });
}
