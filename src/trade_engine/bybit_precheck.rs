//! Bybit 启动前置校验：确认账号已升级到 UTA 且已开启 Spot Margin Trading。
//!
//! 下单链路 (`bybit.rs`) 的现货单 (`BybitNewMarginOrder`) 强制带 `isLeverage=1`，
//! 由 Bybit UTA 自动按需借币撮合。任一前置条件未满足时，交易所会直接拒单：
//!
//! 1. `unifiedMarginStatus` 必须 ≥ 3 (UTA 1.0 / 1.0 Pro / 2.0 / 2.0 Pro)
//! 2. `spotMarginMode` 必须 == "1"
//!
//! 校验失败时返回 `Err`，让 `trade_engine` 进程启动失败而非"在线但全部下单失败"。

use anyhow::{anyhow, bail, Context, Result};
use log::info;
use reqwest::Client;
use serde_json::Value;

use crate::portfolio_margin::bybit_auth::BybitCredentials;
use crate::trade_engine::bybit_query::bybit_rest_get;

const ACCOUNT_INFO_PATH: &str = "/v5/account/info";
const SPOT_MARGIN_STATE_PATH: &str = "/v5/spot-margin-trade/state";

const MIN_UNIFIED_MARGIN_STATUS: i64 = 3;

pub async fn ensure_uta_and_spot_margin(client: &Client, creds: &BybitCredentials) -> Result<()> {
    let unified_status = fetch_unified_margin_status(client, creds).await?;
    if unified_status < MIN_UNIFIED_MARGIN_STATUS {
        bail!(
            "bybit precheck failed: unifiedMarginStatus={} (classic account, need >= {}); \
             upgrade via scripts/setup_bybit_spot_margin.py or POST /v5/account/upgrade-to-uta",
            unified_status,
            MIN_UNIFIED_MARGIN_STATUS,
        );
    }

    let spot_margin_mode = fetch_spot_margin_mode(client, creds).await?;
    if spot_margin_mode != "1" {
        bail!(
            "bybit precheck failed: spotMarginMode=\"{}\" (spot margin disabled); \
             enable via scripts/setup_bybit_spot_margin.py or \
             POST /v5/spot-margin-trade/switch-mode {{\"spotMarginMode\":\"1\"}} \
             (note: Bybit web/app quiz must be completed first)",
            spot_margin_mode,
        );
    }

    info!(
        "bybit precheck pass: unifiedMarginStatus={} spotMarginMode={}",
        unified_status, spot_margin_mode
    );
    Ok(())
}

async fn fetch_unified_margin_status(client: &Client, creds: &BybitCredentials) -> Result<i64> {
    let (status, body) = bybit_rest_get(client, creds, ACCOUNT_INFO_PATH, "")
        .await
        .with_context(|| format!("bybit precheck: GET {ACCOUNT_INFO_PATH} failed"))?;
    if status != 200 {
        bail!(
            "bybit precheck: GET {ACCOUNT_INFO_PATH} http_status={} body={}",
            status,
            truncate_body(&body)
        );
    }
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "bybit precheck: GET {ACCOUNT_INFO_PATH} response not JSON: {}",
            truncate_body(&body)
        )
    })?;
    check_ret_code(&v, ACCOUNT_INFO_PATH)?;
    v.get("result")
        .and_then(|r| r.get("unifiedMarginStatus"))
        .and_then(|s| s.as_i64())
        .ok_or_else(|| {
            anyhow!(
                "bybit precheck: GET {ACCOUNT_INFO_PATH} missing result.unifiedMarginStatus: {}",
                truncate_body(&body)
            )
        })
}

async fn fetch_spot_margin_mode(client: &Client, creds: &BybitCredentials) -> Result<String> {
    let (status, body) = bybit_rest_get(client, creds, SPOT_MARGIN_STATE_PATH, "")
        .await
        .with_context(|| format!("bybit precheck: GET {SPOT_MARGIN_STATE_PATH} failed"))?;
    if status != 200 {
        bail!(
            "bybit precheck: GET {SPOT_MARGIN_STATE_PATH} http_status={} body={}",
            status,
            truncate_body(&body)
        );
    }
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "bybit precheck: GET {SPOT_MARGIN_STATE_PATH} response not JSON: {}",
            truncate_body(&body)
        )
    })?;
    check_ret_code(&v, SPOT_MARGIN_STATE_PATH)?;
    v.get("result")
        .and_then(|r| r.get("spotMarginMode"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            anyhow!(
                "bybit precheck: GET {SPOT_MARGIN_STATE_PATH} missing result.spotMarginMode: {}",
                truncate_body(&body)
            )
        })
}

fn check_ret_code(v: &Value, path: &str) -> Result<()> {
    let ret_code = v.get("retCode").and_then(|c| c.as_i64()).unwrap_or(-1);
    if ret_code != 0 {
        let ret_msg = v
            .get("retMsg")
            .and_then(|m| m.as_str())
            .unwrap_or("<missing>");
        bail!("bybit precheck: GET {path} retCode={ret_code} retMsg={ret_msg}");
    }
    Ok(())
}

fn truncate_body(body: &str) -> String {
    const MAX: usize = 256;
    if body.len() <= MAX {
        body.to_string()
    } else {
        format!("{}...(truncated {} bytes)", &body[..MAX], body.len() - MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ret_code_ok_passes() {
        let v: Value = serde_json::from_str(r#"{"retCode":0,"retMsg":"OK"}"#).unwrap();
        assert!(check_ret_code(&v, "/x").is_ok());
    }

    #[test]
    fn ret_code_nonzero_bails() {
        let v: Value = serde_json::from_str(r#"{"retCode":10001,"retMsg":"err"}"#).unwrap();
        let err = check_ret_code(&v, "/x").unwrap_err().to_string();
        assert!(err.contains("retCode=10001"));
        assert!(err.contains("retMsg=err"));
    }

    #[test]
    fn truncate_body_short_passthrough() {
        assert_eq!(truncate_body("abc"), "abc");
    }

    #[test]
    fn truncate_body_long_truncates() {
        let long = "x".repeat(300);
        let out = truncate_body(&long);
        assert!(out.contains("...(truncated"));
        assert!(out.len() < long.len());
    }
}
