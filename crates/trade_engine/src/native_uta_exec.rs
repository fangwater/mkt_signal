//! Native Bybit/Bitget Exec startup gates, scoped to USDT perpetuals.
//! Private REST always uses the account's first explicit trading IP, without
//! proxy or default-route fallback. No account-mode changes are performed.
use crate::{
    bitget_query::{bitget_rest_get, bitget_rest_post},
    bybit_query::{bybit_rest_get, bybit_rest_get_position_list_pages, bybit_rest_post},
};
use account_common::{bitget_auth::BitgetCredentials, bybit_auth::BybitCredentials};
use anyhow::{bail, ensure, Context, Result};
use order_common::TradingVenue;
use reqwest::Client;
use serde_json::{json, Value};
use std::{net::IpAddr, time::Duration};

pub fn account_client() -> Result<Client> {
    let (raw, _) = runtime_common::mkt_cfg::load_primary_local_ip_from_trade_engine_sync()?;
    let ip: IpAddr = raw.parse().context("invalid UTA account trading IP")?;
    ensure!(
        !ip.is_unspecified() && !ip.is_loopback(),
        "UTA Exec requires an explicit trading local_ips[0]"
    );
    Ok(Client::builder()
        .local_address(ip)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?)
}

fn decode(venue: TradingVenue, response: (u16, String), unchanged_leverage: bool) -> Result<Value> {
    let (status, body) = response;
    ensure!(
        (200..300).contains(&status),
        "UTA private request failed: HTTP {status}"
    );
    let v: Value = serde_json::from_str(&body).context("UTA response is not JSON")?;
    let success = match venue {
        TradingVenue::BybitFutures => {
            let code = v["retCode"].as_i64();
            code == Some(0) || (unchanged_leverage && code == Some(110043))
        }
        TradingVenue::BitgetFutures => v["code"].as_str() == Some("00000"),
        _ => false,
    };
    // Do not copy private response bodies into startup logs.
    ensure!(
        success,
        "UTA private request rejected for {}",
        venue.data_pub_slug()
    );
    let field = if venue == TradingVenue::BybitFutures {
        "result"
    } else {
        "data"
    };
    v.get(field)
        .cloned()
        .context("UTA response is missing result/data")
}

fn bybit_mode(info: &Value) -> Result<bool> {
    ensure!(
        matches!(info["unifiedMarginStatus"].as_i64(), Some(3..=6)),
        "Bybit Exec requires UTA 1.0/2.0"
    );
    match info["marginMode"].as_str() {
        Some("PORTFOLIO_MARGIN") => Ok(true),
        Some("REGULAR_MARGIN") => Ok(false),
        _ => bail!("Bybit Exec requires cross or portfolio margin"),
    }
}

fn bybit_positions(data: &Value, symbol: Option<&str>) -> Result<()> {
    let rows = data["list"]
        .as_array()
        .context("Bybit position list missing")?;
    if symbol.is_some() {
        ensure!(
            rows.len() == 1,
            "Bybit symbol position must contain exactly one one-way row"
        );
    }
    for row in rows {
        let name = row["symbol"]
            .as_str()
            .context("Bybit position symbol missing")?;
        runtime_common::exec_market::validate_symbol(TradingVenue::BybitFutures, name)?;
        ensure!(
            symbol.is_none_or(|expected| expected == name),
            "Bybit position symbol mismatch"
        );
        ensure!(
            row["tradeMode"].as_i64() != Some(1),
            "Bybit Exec rejects isolated positions: {name}"
        );
        ensure!(
            row["positionIdx"].as_i64() == Some(0),
            "Bybit Exec requires one-way positions: {name}"
        );
    }
    Ok(())
}

fn bitget_settings(data: &Value) -> Result<()> {
    ensure!(
        matches!(data["accountMode"].as_str(), Some("unified" | "hybrid")),
        "Bitget Exec requires a ready unified account"
    );
    ensure!(
        matches!(
            data["accountLevel"].as_str(),
            Some("basic" | "advanced" | "delta")
        ),
        "Bitget Exec requires a cross-margin UTA level"
    );
    ensure!(
        data["holdMode"].as_str() == Some("one_way_mode"),
        "Bitget Exec requires one_way_mode"
    );
    let rows = data["symbolConfigList"]
        .as_array()
        .context("Bitget symbolConfigList missing")?;
    for row in rows.iter().filter(|row| row["category"] == "USDT-FUTURES") {
        ensure!(
            row["marginMode"] == "crossed",
            "Bitget USDT Exec requires crossed symbol margin"
        );
    }
    Ok(())
}

fn assert_native(venue: TradingVenue) -> Result<()> {
    ensure!(
        matches!(
            venue,
            TradingVenue::BybitFutures | TradingVenue::BitgetFutures
        ),
        "unsupported UTA Exec venue"
    );
    let exchange = runtime_common::exchange::Exchange::from_str(venue.trade_engine_exchange())
        .context("UTA exchange")?;
    ensure!(
        runtime_common::execution_backend::ExecBackend::for_exchange(exchange)?
            == runtime_common::execution_backend::ExecBackend::Native,
        "Bybit/Bitget Exec requires native execution"
    );
    Ok(())
}

pub async fn validate_startup(venue: TradingVenue) -> Result<()> {
    assert_native(venue)?;
    let client = account_client()?;
    if venue == TradingVenue::BybitFutures {
        let credentials = BybitCredentials::from_env()?;
        bybit_mode(&decode(
            venue,
            bybit_rest_get(&client, &credentials, "/v5/account/info", "").await?,
            false,
        )?)?;
        let pages = bybit_rest_get_position_list_pages(
            &client,
            &credentials,
            "/v5/position/list",
            "category=linear&settleCoin=USDT",
        )
        .await?;
        for body in pages {
            bybit_positions(&decode(venue, (200, body), false)?, None)?;
        }
    } else {
        let credentials = BitgetCredentials::from_env()?;
        bitget_settings(&decode(
            venue,
            bitget_rest_get(&client, &credentials, "/api/v3/account/settings", "").await?,
            false,
        )?)?;
    }
    Ok(())
}

fn orders_empty(venue: TradingVenue, data: &Value) -> Result<bool> {
    let (field, cursor) = if venue == TradingVenue::BybitFutures {
        ("list", "nextPageCursor")
    } else {
        ("list", "cursor")
    };
    let rows = data[field]
        .as_array()
        .context("UTA open-order list missing")?;
    // We only accept an empty first page with no continuation. Otherwise
    // re-cancel the whole category, including Bybit's 500-order batch limit.
    if rows.is_empty() {
        ensure!(
            data[cursor].as_str().unwrap_or("").is_empty(),
            "UTA empty open-order page has a continuation"
        );
        return Ok(true);
    }
    for row in rows {
        let symbol = row["symbol"]
            .as_str()
            .context("UTA open-order symbol missing")?;
        runtime_common::exec_market::validate_symbol(venue, symbol)?;
    }
    Ok(false)
}

pub async fn cancel_orders_on_startup(venue: TradingVenue) -> Result<()> {
    assert_native(venue)?;
    let client = account_client()?;
    let timeout = std::env::var("EXEC_STARTUP_CANCEL_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(120)
        .clamp(1, 600);
    tokio::time::timeout(Duration::from_secs(timeout), async {
        if venue == TradingVenue::BybitFutures {
            let credentials = BybitCredentials::from_env()?;
            loop {
                decode(
                    venue,
                    bybit_rest_post(
                        &client,
                        &credentials,
                        "/v5/order/cancel-all",
                        &json!({"category":"linear","settleCoin":"USDT"}).to_string(),
                    )
                    .await?,
                    false,
                )?;
                tokio::time::sleep(Duration::from_millis(300)).await;
                let orders = decode(
                    venue,
                    bybit_rest_get(
                        &client,
                        &credentials,
                        "/v5/order/realtime",
                        "category=linear&settleCoin=USDT&openOnly=0&limit=50",
                    )
                    .await?,
                    false,
                )?;
                if orders_empty(venue, &orders)? {
                    break;
                }
            }
        } else {
            let credentials = BitgetCredentials::from_env()?;
            loop {
                let strategies = decode(
                    venue,
                    bitget_rest_get(
                        &client,
                        &credentials,
                        "/api/v3/trade/unfilled-strategy-orders",
                        "category=USDT-FUTURES",
                    )
                    .await?,
                    false,
                )?;
                let rows = strategies
                    .as_array()
                    .context("Bitget strategy-order list missing")?;
                for row in rows {
                    ensure!(
                        row["category"] == "USDT-FUTURES",
                        "Bitget strategy order outside Exec market"
                    );
                    runtime_common::exec_market::validate_symbol(
                        venue,
                        row["symbol"]
                            .as_str()
                            .context("Bitget strategy symbol missing")?,
                    )?;
                    let id = row["orderId"]
                        .as_str()
                        .filter(|v| !v.is_empty())
                        .context("Bitget strategy orderId missing")?;
                    decode(
                        venue,
                        bitget_rest_post(
                            &client,
                            &credentials,
                            "/api/v3/trade/cancel-strategy-order",
                            &json!({"orderId":id}).to_string(),
                        )
                        .await?,
                        false,
                    )?;
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }

                decode(
                    venue,
                    bitget_rest_post(
                        &client,
                        &credentials,
                        "/api/v3/trade/cancel-symbol-order",
                        &json!({"category":"USDT-FUTURES"}).to_string(),
                    )
                    .await?,
                    false,
                )?;
                tokio::time::sleep(Duration::from_millis(300)).await;
                let orders = decode(
                    venue,
                    bitget_rest_get(
                        &client,
                        &credentials,
                        "/api/v3/trade/unfilled-orders",
                        "category=USDT-FUTURES&limit=100",
                    )
                    .await?,
                    false,
                )?;
                if orders_empty(venue, &orders)? && rows.is_empty() {
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("UTA startup cancellation did not confirm an empty market before timeout")??;
    Ok(())
}

pub async fn set_and_verify_leverage(
    venue: TradingVenue,
    symbol: &str,
    leverage: u8,
) -> Result<()> {
    assert_native(venue)?;
    runtime_common::exec_market::validate_symbol(venue, symbol)?;
    let client = account_client()?;
    if venue == TradingVenue::BybitFutures {
        let credentials = BybitCredentials::from_env()?;
        let portfolio = bybit_mode(&decode(
            venue,
            bybit_rest_get(&client, &credentials, "/v5/account/info", "").await?,
            false,
        )?)?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("category", "linear")
            .append_pair("symbol", symbol)
            .finish();
        let positions = decode(
            venue,
            bybit_rest_get(&client, &credentials, "/v5/position/list", &query).await?,
            false,
        )?;
        bybit_positions(&positions, Some(symbol))?;
        if portfolio {
            return Ok(());
        } // PM does not use a fixed leverage multiple.
        let body = json!({"category":"linear", "symbol":symbol, "buyLeverage":leverage.to_string(), "sellLeverage":leverage.to_string()}).to_string();
        decode(
            venue,
            bybit_rest_post(&client, &credentials, "/v5/position/set-leverage", &body).await?,
            true,
        )?;
        let positions = decode(
            venue,
            bybit_rest_get(&client, &credentials, "/v5/position/list", &query).await?,
            false,
        )?;
        bybit_positions(&positions, Some(symbol))?;
        ensure!(
            positions["list"][0]["leverage"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                == Some(f64::from(leverage)),
            "Bybit leverage readback mismatch"
        );
    } else {
        let credentials = BitgetCredentials::from_env()?;
        bitget_settings(&decode(
            venue,
            bitget_rest_get(&client, &credentials, "/api/v3/account/settings", "").await?,
            false,
        )?)?;
        let body = json!({"category":"USDT-FUTURES", "symbol":symbol, "marginMode":"crossed", "leverage":leverage.to_string()}).to_string();
        decode(
            venue,
            bitget_rest_post(&client, &credentials, "/api/v3/account/set-leverage", &body).await?,
            false,
        )?;
        let settings = decode(
            venue,
            bitget_rest_get(&client, &credentials, "/api/v3/account/settings", "").await?,
            false,
        )?;
        bitget_settings(&settings)?;
        let rows: Vec<_> = settings["symbolConfigList"]
            .as_array()
            .context("Bitget symbolConfigList missing")?
            .iter()
            .filter(|r| r["category"] == "USDT-FUTURES" && r["symbol"] == symbol)
            .collect();
        ensure!(
            rows.len() == 1
                && rows[0]["leverage"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    == Some(f64::from(leverage)),
            "Bitget leverage readback mismatch"
        );
    }
    Ok(())
}

/// Preserve account-wide collateral and risk while publishing only the
/// deployment's order, fill and position market to its IPC namespace.
pub fn account_event_allowed(venue: TradingVenue, raw: &[u8]) -> bool {
    use mkt_parsers::msg::{
        basic_account_msg::*, bitget_account_msg::BitgetBasicOrderMsg,
        bybit_account_msg::BybitBasicOrderMsg,
    };
    let Some((kind, scope, body)) = split_basic_account_event(raw) else {
        return false;
    };
    let expected = match venue {
        TradingVenue::BybitFutures => BasicAccountScope::BybitUnified,
        TradingVenue::BitgetFutures => BasicAccountScope::BitgetUnified,
        _ => return false,
    };
    if scope != expected {
        return false;
    }
    let valid = |symbol: &str| runtime_common::exec_market::validate_symbol(venue, symbol).is_ok();
    match kind {
        BasicAccountEventType::ExternalFuturesFill => {
            mkt_parsers::msg::external_futures_fill::ExternalFuturesFill::from_bytes(&body)
                .is_ok_and(|m| m.venue == venue as u8 && valid(&m.symbol))
        }
        BasicAccountEventType::BalanceUpdate
        | BasicAccountEventType::BorrowInterest
        | BasicAccountEventType::AccountRisk => true,
        BasicAccountEventType::PositionUpdate => {
            BasicPositionMsg::from_bytes(&body).is_ok_and(|m| valid(&m.inst_id))
        }
        BasicAccountEventType::UnrealizedPnlUpdate => {
            BasicUmUnrealizedMsg::from_bytes(&body).is_ok_and(|m| valid(&m.inst_id))
        }
        BasicAccountEventType::TradeUpdateLite => BasicTradeLiteMsg::from_bytes(&body)
            .is_ok_and(|m| m.venue == venue as u8 && valid(&m.symbol)),
        BasicAccountEventType::OrderUpdate => match venue {
            TradingVenue::BybitFutures => BybitBasicOrderMsg::from_bytes(&body)
                .is_ok_and(|m| m.venue == BybitBasicOrderMsg::VENUE_FUTURES && valid(&m.symbol)),
            TradingVenue::BitgetFutures => BitgetBasicOrderMsg::from_bytes(&body)
                .is_ok_and(|m| m.venue == BitgetBasicOrderMsg::VENUE_FUTURES && valid(&m.symbol)),
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn uta_ipc_retains_only_own_futures_fills_and_account_risk() {
        use mkt_parsers::msg::basic_account_msg::*;
        for (venue, scope) in [
            (TradingVenue::BybitFutures, BasicAccountScope::BybitUnified),
            (
                TradingVenue::BitgetFutures,
                BasicAccountScope::BitgetUnified,
            ),
        ] {
            let risk = BasicAccountEventMsg::create(
                BasicAccountEventType::AccountRisk,
                scope,
                bytes::Bytes::new(),
            );
            assert!(account_event_allowed(venue, &risk.to_bytes()));
            for (market, symbol, want) in [
                (venue as u8, "BTCUSDT", true),
                (venue as u8, "BTCUSDC", false),
                (0, "BTCUSDT", false),
            ] {
                let fill = BasicTradeLiteMsg::create(
                    market,
                    1,
                    1,
                    symbol.into(),
                    123,
                    "456",
                    1,
                    false,
                    50000.0,
                    0.1,
                );
                let event = BasicAccountEventMsg::create(
                    BasicAccountEventType::TradeUpdateLite,
                    scope,
                    fill.to_bytes(),
                );
                assert_eq!(account_event_allowed(venue, &event.to_bytes()), want);
            }
        }
    }

    #[test]
    fn pro_status_does_not_imply_portfolio_margin() {
        for status in 3..=6 {
            assert!(!bybit_mode(
                &json!({"unifiedMarginStatus":status,"marginMode":"REGULAR_MARGIN"})
            )
            .unwrap());
            assert!(bybit_mode(
                &json!({"unifiedMarginStatus":status,"marginMode":"PORTFOLIO_MARGIN"})
            )
            .unwrap());
        }
        assert!(
            bybit_mode(&json!({"unifiedMarginStatus":1,"marginMode":"REGULAR_MARGIN"})).is_err()
        );
        assert!(
            bybit_mode(&json!({"unifiedMarginStatus":6,"marginMode":"ISOLATED_MARGIN"})).is_err()
        );
    }
    #[test]
    fn rejects_hedged_and_foreign_bybit_positions() {
        assert!(bybit_positions(
            &json!({"list":[{"symbol":"BTCUSDT","positionIdx":0}]}),
            Some("BTCUSDT")
        )
        .is_ok());
        for row in [
            json!({"symbol":"BTCUSDT","positionIdx":1}),
            json!({"symbol":"BTCUSDC","positionIdx":0}),
        ] {
            assert!(bybit_positions(&json!({"list":[row]}), None).is_err());
        }
        assert!(bybit_positions(&json!({"list":[]}), Some("BTCUSDT")).is_err());
    }
    #[test]
    fn rejects_bitget_switching_hedge_and_isolated_modes() {
        let valid = json!({"accountMode":"hybrid","accountLevel":"advanced","holdMode":"one_way_mode","symbolConfigList":[{"category":"USDT-FUTURES","symbol":"BTCUSDT","marginMode":"crossed"}]});
        bitget_settings(&valid).unwrap();
        for (field, value) in [
            ("accountMode", "switching"),
            ("accountLevel", "isolated"),
            ("holdMode", "hedge_mode"),
        ] {
            let mut v = valid.clone();
            v[field] = json!(value);
            assert!(bitget_settings(&v).is_err());
        }
        let mut v = valid;
        v["symbolConfigList"][0]["marginMode"] = json!("isolated");
        assert!(bitget_settings(&v).is_err());
    }
    #[test]
    fn empty_requires_a_complete_successful_order_response() {
        for venue in [TradingVenue::BybitFutures, TradingVenue::BitgetFutures] {
            assert!(orders_empty(venue, &json!({"list":[]})).unwrap());
            assert!(!orders_empty(venue, &json!({"list":[{"symbol":"BTCUSDT"}]})).unwrap());
            assert!(orders_empty(venue, &json!({})).is_err());
            assert!(orders_empty(
                venue,
                &json!({"list":[],"cursor":"next","nextPageCursor":"next"})
            )
            .is_err());
            assert!(decode(
                venue,
                (
                    200,
                    json!({"code":"40001","retCode":10001,"result":{"list":[]},"data":{"list":[]}})
                        .to_string()
                ),
                false
            )
            .is_err());
        }
    }
}
