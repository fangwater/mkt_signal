//! Bitget 启动前置校验：账户必须升级到统一交易账户（UTA / v3），否则 `category=margin`
//! 的现货下单（自动借币）会被拒。
//!
//! `order_manager.rs` 在 `TradingVenue::BitgetMargin` 路径强制 `category=margin` 走 cross-margin
//! 现货并依赖 UTA 自动借币（Bitget UTA v3 没有显式 `loanType`，借币由账户级 cross-margin 配置触发）。
//! 仍处于 V2 经典账户的 key 调用 `/api/v3/account/settings` 会返回错误。
//!
//! 校验失败时返回 `Err`，让 `trade_engine` 启动失败。

use anyhow::{anyhow, bail, Context, Result};
use log::info;
use reqwest::Client;
use serde_json::Value;

use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use crate::trade_engine::bitget_query::bitget_rest_get;

const ACCOUNT_SETTINGS_PATH: &str = "/api/v3/account/settings";

pub async fn ensure_unified_account(client: &Client, creds: &BitgetCredentials) -> Result<()> {
    let account_mode = fetch_account_mode(client, creds).await?;
    if account_mode.is_empty() {
        bail!(
            "bitget precheck failed: GET {ACCOUNT_SETTINGS_PATH} returned empty accountMode; \
             API key may not be on UTA — upgrade in Bitget web/app and ensure key has UTA permission",
        );
    }
    info!("bitget precheck pass: accountMode={}", account_mode);
    Ok(())
}

async fn fetch_account_mode(client: &Client, creds: &BitgetCredentials) -> Result<String> {
    let (status, body) = bitget_rest_get(client, creds, ACCOUNT_SETTINGS_PATH, "")
        .await
        .with_context(|| format!("bitget precheck: GET {ACCOUNT_SETTINGS_PATH} failed"))?;
    if status != 200 {
        bail!(
            "bitget precheck: GET {ACCOUNT_SETTINGS_PATH} http_status={} body={}",
            status,
            truncate_body(&body)
        );
    }
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "bitget precheck: GET {ACCOUNT_SETTINGS_PATH} response not JSON: {}",
            truncate_body(&body)
        )
    })?;
    check_code(&v, ACCOUNT_SETTINGS_PATH)?;
    let mode = v
        .get("data")
        .and_then(|d| d.get("accountMode"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| {
            anyhow!(
                "bitget precheck: GET {ACCOUNT_SETTINGS_PATH} missing data.accountMode: {}",
                truncate_body(&body)
            )
        })?
        .to_string();
    Ok(mode)
}

fn check_code(v: &Value, path: &str) -> Result<()> {
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("-1");
    if code != "00000" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("<missing>");
        bail!("bitget precheck: GET {path} code={code} msg={msg}");
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
    fn code_zero_passes() {
        let v: Value =
            serde_json::from_str(r#"{"code":"00000","msg":"","data":{"accountMode":"hybrid"}}"#)
                .unwrap();
        assert!(check_code(&v, "/x").is_ok());
    }

    #[test]
    fn code_nonzero_bails() {
        let v: Value = serde_json::from_str(r#"{"code":"40001","msg":"err"}"#).unwrap();
        let err = check_code(&v, "/x").unwrap_err().to_string();
        assert!(err.contains("code=40001"));
        assert!(err.contains("msg=err"));
    }
}
