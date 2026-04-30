//! Gate 启动前置校验：账户必须升级到统一账户（非 classic），否则 `account=unified`
//! + `auto_borrow=true` 的现货下单会被拒。
//!
//! `order_manager.rs` 在 `TradingVenue::GateMargin` 路径强制 `account=unified` 并开启
//! `auto_borrow=true` / `auto_repay=true`，依赖 Gate 统一账户的跨币种保证金。Gate 文档明确：
//! - `mode=classic`         -> 仅普通现货，不支持 auto_borrow
//! - `mode=single_currency` -> 单币种保证金 (含 spot 杠杆)
//! - `mode=multi_currency`  -> 多币种保证金（共享）
//! - `mode=portfolio`       -> 组合保证金
//!
//! 校验失败时返回 `Err`，让 `trade_engine` 启动失败。

use anyhow::{anyhow, bail, Context, Result};
use log::info;
use reqwest::Client;
use serde_json::Value;

use crate::portfolio_margin::gate_auth::GateCredentials;
use crate::trade_engine::gate_query::gate_rest_get;

const UNIFIED_ACCOUNTS_PATH: &str = "/api/v4/unified/accounts";

pub async fn ensure_unified_account(client: &Client, creds: &GateCredentials) -> Result<()> {
    let mode = fetch_account_mode(client, creds).await?;
    if mode.eq_ignore_ascii_case("classic") {
        bail!(
            "gate precheck failed: unified account mode=\"{}\" (auto_borrow needs unified); \
             switch to single_currency / multi_currency / portfolio in Gate web/app",
            mode,
        );
    }
    info!("gate precheck pass: unified account mode={}", mode);
    Ok(())
}

async fn fetch_account_mode(client: &Client, creds: &GateCredentials) -> Result<String> {
    let (status, body) = gate_rest_get(client, creds, UNIFIED_ACCOUNTS_PATH, "")
        .await
        .with_context(|| format!("gate precheck: GET {UNIFIED_ACCOUNTS_PATH} failed"))?;
    if status != 200 {
        bail!(
            "gate precheck: GET {UNIFIED_ACCOUNTS_PATH} http_status={} body={}",
            status,
            truncate_body(&body)
        );
    }
    let v: Value = serde_json::from_str(&body).with_context(|| {
        format!(
            "gate precheck: GET {UNIFIED_ACCOUNTS_PATH} response not JSON: {}",
            truncate_body(&body)
        )
    })?;
    let mode = v
        .get("mode")
        .and_then(|s| s.as_str())
        .ok_or_else(|| {
            anyhow!(
                "gate precheck: GET {UNIFIED_ACCOUNTS_PATH} missing top-level mode: {}",
                truncate_body(&body)
            )
        })?
        .to_string();
    Ok(mode)
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
