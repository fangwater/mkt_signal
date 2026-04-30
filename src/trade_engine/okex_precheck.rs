//! OKX 启动前置校验：账户必须处于跨保证金或组合保证金模式，否则下单时 `tdMode=cross` 会被拒。
//!
//! 我们在 `okex.rs::ToOkexWsJson` 中对 OkexMargin/OkexUM 都强制 `tdMode=cross`，依赖 OKX
//! Unified 账户做自动借币与共享保证金。OKX 文档明确：自动借币仅在「多币种保证金模式 (acctLv=3)」
//! 或「组合保证金模式 (acctLv=4)」下生效。Spot/Futures 模式 (acctLv=1/2) 下 `tdMode=cross`
//! 在现货/合约会被直接拒单。
//!
//! 校验失败时返回 `Err`，让 `trade_engine` 启动失败，避免「在线但全部下单失败」。

use anyhow::{anyhow, bail, Context, Result};
use log::info;
use reqwest::Client;
use serde_json::Value;

use crate::portfolio_margin::okex_auth::OkexCredentials;
use crate::trade_engine::okex_query::okex_rest_get;

const ACCOUNT_CONFIG_PATH: &str = "/api/v5/account/config";

/// 多币种保证金模式 (3) 或组合保证金模式 (4)
const MIN_ACCT_LV: i64 = 3;

pub async fn ensure_unified_margin_mode(client: &Client, creds: &OkexCredentials) -> Result<()> {
    let acct_lv = fetch_acct_lv(client, creds).await?;
    if acct_lv < MIN_ACCT_LV {
        bail!(
            "okex precheck failed: acctLv={} (need >= {} for tdMode=cross auto-borrow); \
             switch to Multi-currency margin (3) or Portfolio margin (4) on OKX web/app",
            acct_lv,
            MIN_ACCT_LV,
        );
    }
    info!("okex precheck pass: acctLv={}", acct_lv);
    Ok(())
}

async fn fetch_acct_lv(client: &Client, creds: &OkexCredentials) -> Result<i64> {
    let (status, body) = okex_rest_get(client, creds, ACCOUNT_CONFIG_PATH)
        .await
        .with_context(|| format!("okex precheck: GET {ACCOUNT_CONFIG_PATH} failed"))?;
    if status != 200 {
        bail!(
            "okex precheck: GET {ACCOUNT_CONFIG_PATH} http_status={} body={}",
            status,
            truncate_body(&body)
        );
    }
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "okex precheck: GET {ACCOUNT_CONFIG_PATH} response not JSON: {}",
            truncate_body(&body)
        )
    })?;
    check_code(&v, ACCOUNT_CONFIG_PATH)?;
    let acct_lv_str = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|arr| arr.first())
        .and_then(|item| item.get("acctLv"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| {
            anyhow!(
                "okex precheck: GET {ACCOUNT_CONFIG_PATH} missing data[0].acctLv: {}",
                truncate_body(&body)
            )
        })?;
    acct_lv_str.parse::<i64>().with_context(|| {
        format!(
            "okex precheck: GET {ACCOUNT_CONFIG_PATH} acctLv not int: {}",
            acct_lv_str
        )
    })
}

fn check_code(v: &Value, path: &str) -> Result<()> {
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("-1");
    if code != "0" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("<missing>");
        bail!("okex precheck: GET {path} code={code} msg={msg}");
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
            serde_json::from_str(r#"{"code":"0","msg":"","data":[{"acctLv":"3"}]}"#).unwrap();
        assert!(check_code(&v, "/x").is_ok());
    }

    #[test]
    fn code_nonzero_bails() {
        let v: Value = serde_json::from_str(r#"{"code":"50001","msg":"err"}"#).unwrap();
        let err = check_code(&v, "/x").unwrap_err().to_string();
        assert!(err.contains("code=50001"));
        assert!(err.contains("msg=err"));
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
