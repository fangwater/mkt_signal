//! RapidX perpetual leverage initialization for BatchExec.
//!
//! RapidX accepts leverage updates asynchronously. A successful set response is
//! therefore insufficient to activate a BatchExec symbol: read back the exact
//! perpetual pair before reporting success to the caller.

use crate::ltp_rest::LtpRestClient;
use anyhow::{anyhow, bail, Context, Result};
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const SET_LEVERAGE_PATH: &str = "/api/v1/trading/position/leverage";
const GET_LEVERAGE_PATH: &str = "/api/v1/trading/perp/leverage";
const VERIFY_ATTEMPTS: usize = 5;
const VERIFY_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Sets a RapidX perpetual leverage and waits until the read endpoint reports
/// the requested setting for that exact logical-exchange symbol.
pub async fn set_and_verify_batch_exec_leverage(
    venue: TradingVenue,
    internal_symbol: &str,
    leverage: u8,
) -> Result<()> {
    let sym = rapidx_perp_symbol(venue, internal_symbol)?;
    anyhow::ensure!(leverage > 0, "RapidX perpetual leverage must be positive");
    let client = LtpRestClient::from_env().context("build RapidX leverage REST client")?;
    set_and_verify_with_client(&client, &sym, leverage).await
}

async fn set_and_verify_with_client(client: &LtpRestClient, sym: &str, leverage: u8) -> Result<()> {
    let request = json!({"sym": sym, "leverage": leverage.to_string()});
    let (status, body) = client
        .signed_json(reqwest::Method::POST, SET_LEVERAGE_PATH, &request)
        .await
        .context("submit RapidX leverage update")?;
    ensure_rapidx_success(status, &body, "set leverage")?;

    let params = BTreeMap::from([("sym".to_string(), sym.to_owned())]);
    for attempt in 1..=VERIFY_ATTEMPTS {
        let (status, body) = client
            .signed_get(GET_LEVERAGE_PATH, &params)
            .await
            .with_context(|| format!("read RapidX leverage attempt={attempt} sym={sym}"))?;
        if leverage_response_matches(status, &body, sym, leverage)? {
            return Ok(());
        }
        if attempt < VERIFY_ATTEMPTS {
            tokio::time::sleep(VERIFY_RETRY_DELAY).await;
        }
    }

    bail!(
        "RapidX leverage update was accepted but not confirmed: sym={} leverage={} attempts={}",
        sym,
        leverage,
        VERIFY_ATTEMPTS
    )
}

fn rapidx_perp_symbol(venue: TradingVenue, internal_symbol: &str) -> Result<String> {
    let exchange = match venue {
        TradingVenue::BinanceFutures => "BINANCE",
        TradingVenue::OkexFutures => "OKX",
        other => bail!("RapidX leverage does not support BatchExec venue {other:?}"),
    };
    let symbol = normalize_symbol_for_internal(internal_symbol);
    let base = symbol
        .strip_suffix("USDT")
        .filter(|base| !base.is_empty())
        .ok_or_else(|| anyhow!("RapidX perpetual leverage requires a USDT internal symbol"))?;
    anyhow::ensure!(
        base.bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()),
        "RapidX perpetual leverage symbol must be ASCII uppercase alphanumeric"
    );
    Ok(format!("{exchange}_PERP_{base}_USDT"))
}

fn ensure_rapidx_success(status: u16, body: &str, operation: &str) -> Result<()> {
    let response: Value = serde_json::from_str(body)
        .with_context(|| format!("decode RapidX {operation} response"))?;
    if !(200..300).contains(&status) || response.get("code").and_then(Value::as_i64) != Some(200000)
    {
        bail!(
            "RapidX {operation} failed: status={} code={:?} message={:?}",
            status,
            response.get("code"),
            response.get("message")
        );
    }
    Ok(())
}

fn leverage_response_matches(status: u16, body: &str, sym: &str, leverage: u8) -> Result<bool> {
    anyhow::ensure!(status == 200, "RapidX get leverage HTTP status {status}");
    ensure_rapidx_success(status, body, "get leverage")?;
    let response: Value =
        serde_json::from_str(body).context("decode RapidX get leverage response")?;
    let rows = response
        .get("data")
        .and_then(Value::as_array)
        .context("RapidX get leverage response missing data array")?;
    let row = rows
        .first()
        .context("RapidX get leverage response has no requested symbol")?;
    anyhow::ensure!(
        rows.len() == 1,
        "RapidX get leverage response has duplicate or unexpected rows"
    );
    let returned_sym = row
        .get("sym")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .context("RapidX get leverage response row missing sym")?;
    let returned_leverage = row
        .get("leverage")
        .and_then(Value::as_str)
        .context("RapidX get leverage response row missing leverage")?
        .parse::<u8>()
        .context("RapidX get leverage response has invalid leverage")?;
    anyhow::ensure!(
        returned_leverage > 0,
        "RapidX get leverage response has zero leverage"
    );
    Ok(returned_sym == sym && returned_leverage == leverage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_documented_rapidx_perp_symbols() {
        assert_eq!(
            rapidx_perp_symbol(TradingVenue::BinanceFutures, "btc-usdt").unwrap(),
            "BINANCE_PERP_BTC_USDT"
        );
        assert_eq!(
            rapidx_perp_symbol(TradingVenue::OkexFutures, "ETHUSDT").unwrap(),
            "OKX_PERP_ETH_USDT"
        );
        assert!(rapidx_perp_symbol(TradingVenue::GateFutures, "BTCUSDT").is_err());
        assert!(rapidx_perp_symbol(TradingVenue::BinanceFutures, "BTCUSD").is_err());
    }

    #[test]
    fn verifies_exact_symbol_and_leverage_from_rapidx_response() {
        let body = r#"{"code":200000,"message":"Success","data":[{"sym":"OKX_PERP_BTC_USDT","leverage":"5"}]}"#;
        assert!(leverage_response_matches(200, body, "OKX_PERP_BTC_USDT", 5).unwrap());
        assert!(!leverage_response_matches(200, body, "OKX_PERP_ETH_USDT", 5).unwrap());
        assert!(!leverage_response_matches(200, body, "OKX_PERP_BTC_USDT", 4).unwrap());
    }

    #[test]
    fn rejects_non_successful_rapidx_responses() {
        assert!(leverage_response_matches(
            202,
            r#"{"code":200000,"data":[]}"#,
            "BINANCE_PERP_BTC_USDT",
            5
        )
        .is_err());
        assert!(leverage_response_matches(
            200,
            r#"{"code":400001,"message":"bad","data":[]}"#,
            "BINANCE_PERP_BTC_USDT",
            5
        )
        .is_err());
        assert!(leverage_response_matches(
            500,
            r#"{"code":200000,"data":[]}"#,
            "BINANCE_PERP_BTC_USDT",
            5
        )
        .is_err());
    }

    #[test]
    fn rejects_empty_duplicate_and_malformed_readback_rows() {
        let empty = r#"{"code":200000,"data":[]}"#;
        let duplicate = r#"{"code":200000,"data":[{"sym":"OKX_PERP_BTC_USDT","leverage":"5"},{"sym":"OKX_PERP_BTC_USDT","leverage":"5"}]}"#;
        let malformed = r#"{"code":200000,"data":[{"sym":"OKX_PERP_BTC_USDT","leverage":5}]}"#;
        assert!(leverage_response_matches(200, empty, "OKX_PERP_BTC_USDT", 5).is_err());
        assert!(leverage_response_matches(200, duplicate, "OKX_PERP_BTC_USDT", 5).is_err());
        assert!(leverage_response_matches(200, malformed, "OKX_PERP_BTC_USDT", 5).is_err());
    }

    #[tokio::test]
    async fn posts_documented_body_then_reads_exact_rapidx_leverage() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for (index, response) in [
                r#"{"code":200000,"message":"Success","data":{}}"#,
                r#"{"code":200000,"message":"Success","data":[{"sym":"OKX_PERP_BTC_USDT","leverage":"5"}]}"#,
            ]
            .into_iter()
            .enumerate()
            {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0; 4096];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(header_end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let header = std::str::from_utf8(&bytes[..header_end]).unwrap();
                        let content_length = header
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= header_end + 4 + content_length {
                            break;
                        }
                    }
                }
                let request = std::str::from_utf8(&bytes).unwrap();
                assert!(request.to_ascii_lowercase().contains("\r\nsignature: "));
                if index == 0 {
                    assert!(request.starts_with("POST /api/v1/trading/position/leverage HTTP/1.1"));
                    let body = request.split("\r\n\r\n").nth(1).unwrap();
                    assert_eq!(
                        serde_json::from_str::<Value>(body).unwrap(),
                        json!({"sym":"OKX_PERP_BTC_USDT","leverage":"5"})
                    );
                } else {
                    assert!(request.starts_with("GET /api/v1/trading/perp/leverage?sym=OKX_PERP_BTC_USDT HTTP/1.1"));
                }
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                            response.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        set_and_verify_with_client(
            &LtpRestClient::fixture(format!("http://{address}")),
            "OKX_PERP_BTC_USDT",
            5,
        )
        .await
        .unwrap();
        server.await.unwrap();
    }
}
