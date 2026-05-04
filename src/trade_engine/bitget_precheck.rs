//! Bitget 启动前置校验：账户必须升级到统一交易账户（UTA / v3），且在需要
//! `category=margin` 现货下单时必须是 Advanced 账户，否则自动借币下单会被拒。
//!
//! `order_manager.rs` 在 `TradingVenue::BitgetMargin` 路径强制 `category=margin` 走 cross-margin
//! 现货并依赖 UTA 自动借币（Bitget UTA v3 没有显式 `loanType`，借币由账户级 cross-margin 配置触发）。
//! `TradingVenue::BitgetFutures` 路径按 one-way 持仓模式发 futures 单：开/平仓只由 `side`
//! 和 `reduceOnly` 区分，不携带 hedge-mode 的 `posSide`。
//! 仍处于 V2 经典账户的 key 调用 `/api/v3/account/settings` 会返回错误。
//!
//! 校验失败时返回 `Err`，调用方会让 `trade_engine` 启动失败。

use anyhow::{anyhow, bail, Context, Result};
use log::{info, warn};
use reqwest::Client;
use serde_json::Value;

use crate::portfolio_margin::bitget_auth::BitgetCredentials;
use crate::trade_engine::bitget_query::bitget_rest_get;

const ACCOUNT_SETTINGS_PATH: &str = "/api/v3/account/settings";
const REQUIRED_MARGIN_ACCOUNT_LEVEL: &str = "advanced";
const REQUIRED_HOLD_MODE: &str = "one_way_mode";

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitgetAccountSettings {
    account_mode: String,
    account_level: Option<String>,
    hold_mode: Option<String>,
}

pub async fn ensure_unified_account(client: &Client, creds: &BitgetCredentials) -> Result<()> {
    let settings = fetch_account_settings(client, creds).await?;
    let margin_required = bitget_margin_required_from_env();
    validate_account_settings(&settings, margin_required)?;

    info!(
        "bitget precheck pass: accountMode={} accountLevel={} holdMode={} margin_required={}",
        settings.account_mode,
        settings.account_level.as_deref().unwrap_or("<missing>"),
        settings.hold_mode.as_deref().unwrap_or("<missing>"),
        margin_required
    );
    Ok(())
}

fn validate_account_settings(
    settings: &BitgetAccountSettings,
    margin_required: bool,
) -> Result<()> {
    if settings.account_mode.is_empty() {
        bail!(
            "bitget precheck failed: GET {ACCOUNT_SETTINGS_PATH} returned empty accountMode; \
             API key may not be on UTA — upgrade in Bitget web/app and ensure key has UTA permission",
        );
    }

    let hold_mode = settings.hold_mode.as_deref().unwrap_or("<missing>");
    if !hold_mode.eq_ignore_ascii_case(REQUIRED_HOLD_MODE) {
        bail!(
            "bitget precheck failed: accountMode={} accountLevel={} holdMode={} (need {}); \
             Bitget futures order_manager is configured for one-way mode, so switch hold mode \
             with scripts/bitget_set_hold_mode.py --mode one_way_mode before starting trade_engine",
            settings.account_mode,
            settings.account_level.as_deref().unwrap_or("<missing>"),
            hold_mode,
            REQUIRED_HOLD_MODE,
        );
    }

    if margin_required {
        let level = settings.account_level.as_deref().unwrap_or("<missing>");
        if !level.eq_ignore_ascii_case(REQUIRED_MARGIN_ACCOUNT_LEVEL) {
            bail!(
                "bitget precheck failed: accountMode={} accountLevel={} holdMode={} (need {} for \
                 category=margin auto-borrow); upgrade Bitget UTA from Basic to Advanced \
                 before running bitget-margin strategies",
                settings.account_mode,
                level,
                hold_mode,
                REQUIRED_MARGIN_ACCOUNT_LEVEL,
            );
        }
    } else if matches!(
        settings.account_level.as_deref(),
        Some(level) if !level.eq_ignore_ascii_case(REQUIRED_MARGIN_ACCOUNT_LEVEL)
    ) {
        warn!(
            "bitget precheck: accountMode={} accountLevel={} (margin auto-borrow unavailable; \
             no bitget-margin venue requested in env)",
            settings.account_mode,
            settings.account_level.as_deref().unwrap_or("<missing>")
        );
    }

    Ok(())
}

async fn fetch_account_settings(
    client: &Client,
    creds: &BitgetCredentials,
) -> Result<BitgetAccountSettings> {
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
    parse_account_settings(&v, &body)
}

fn parse_account_settings(v: &Value, body: &str) -> Result<BitgetAccountSettings> {
    let account_mode = v
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
    let account_level = v
        .get("data")
        .and_then(|d| d.get("accountLevel"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());
    let hold_mode = v
        .get("data")
        .and_then(|d| d.get("holdMode"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string());
    Ok(BitgetAccountSettings {
        account_mode,
        account_level,
        hold_mode,
    })
}

fn bitget_margin_required_from_env() -> bool {
    if let Ok(v) = std::env::var("BITGET_REQUIRE_UTA_MARGIN") {
        if parse_bool_env(&v).unwrap_or(false) {
            return true;
        }
        if parse_bool_env(&v) == Some(false) {
            return false;
        }
    }

    std::env::var("OPEN_VENUE")
        .ok()
        .as_deref()
        .is_some_and(venue_name_is_bitget_margin)
        || std::env::var("HEDGE_VENUE")
            .ok()
            .as_deref()
            .is_some_and(venue_name_is_bitget_margin)
}

fn parse_bool_env(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn venue_name_is_bitget_margin(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "bitget-margin" | "bitget_margin" | "bitgetmargin"
    )
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

    #[test]
    fn parses_account_mode_and_level() {
        let v: Value = serde_json::from_str(
            r#"{"code":"00000","msg":"success","data":{"accountMode":"unified","accountLevel":"basic","holdMode":"one_way_mode"}}"#,
        )
        .unwrap();
        let parsed = parse_account_settings(&v, "{}").unwrap();
        assert_eq!(
            parsed,
            BitgetAccountSettings {
                account_mode: "unified".to_string(),
                account_level: Some("basic".to_string()),
                hold_mode: Some("one_way_mode".to_string()),
            }
        );
    }

    #[test]
    fn rejects_non_one_way_hold_mode() {
        let settings = BitgetAccountSettings {
            account_mode: "unified".to_string(),
            account_level: Some("advanced".to_string()),
            hold_mode: Some("hedge_mode".to_string()),
        };

        let err = validate_account_settings(&settings, true)
            .unwrap_err()
            .to_string();

        assert!(err.contains("holdMode=hedge_mode"));
        assert!(err.contains("one_way_mode"));
    }

    #[test]
    fn accepts_one_way_advanced_margin_account() {
        let settings = BitgetAccountSettings {
            account_mode: "unified".to_string(),
            account_level: Some("advanced".to_string()),
            hold_mode: Some("one_way_mode".to_string()),
        };

        validate_account_settings(&settings, true).unwrap();
    }

    #[test]
    fn detects_bitget_margin_venue_names() {
        assert!(venue_name_is_bitget_margin("bitget-margin"));
        assert!(venue_name_is_bitget_margin("BITGET_MARGIN"));
        assert!(!venue_name_is_bitget_margin("bitget-futures"));
    }

    #[test]
    fn parses_bool_env_values() {
        assert_eq!(parse_bool_env("true"), Some(true));
        assert_eq!(parse_bool_env("0"), Some(false));
        assert_eq!(parse_bool_env("maybe"), None);
    }
}
