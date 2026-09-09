//! Startup cancellation is portfolio-scoped; RapidX cancelAll is user-wide.
use crate::ltp_rest::LtpRestClient;
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const PAGE_SIZE: usize = 1000;
const MAX_PAGES: usize = 100;

fn response(status: u16, body: &str) -> Result<Value> {
    ensure!(
        (200..300).contains(&status),
        "RapidX startup HTTP status {status}"
    );
    let value: Value = serde_json::from_str(body).context("decode RapidX startup response")?;
    ensure!(
        value["code"].as_i64() == Some(200000),
        "RapidX startup response unsuccessful"
    );
    Ok(value)
}

fn order_id(row: &Value, portfolio: &str, exchange: &str, business_type: &str) -> Result<String> {
    ensure!(
        row["portfolioId"].as_str() == Some(portfolio),
        "RapidX startup order portfolio mismatch"
    );
    ensure!(
        row["exchangeType"].as_str() == Some(exchange)
            && row["businessType"].as_str() == Some(business_type),
        "RapidX startup order market mismatch"
    );
    let symbol = row["sym"].as_str().context("missing RapidX order symbol")?;
    let parts: Vec<_> = symbol.split('_').collect();
    ensure!(
        parts.len() == 4
            && parts[0] == exchange
            && parts[1] == business_type
            && !parts[2].is_empty()
            && !parts[3].is_empty(),
        "invalid RapidX startup order symbol"
    );
    ensure!(
        matches!(
            row["orderState"].as_str(),
            Some("NEW" | "OPEN" | "PARTIALLY_FILLED")
        ),
        "unexpected RapidX open order state"
    );
    Ok(row["orderId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .context("missing RapidX order ID")?
        .into())
}

impl LtpRestClient {
    pub async fn open_order_ids(&self, exchange: &str, business_type: &str) -> Result<Vec<String>> {
        ensure!(
            matches!(exchange, "OKX" | "BINANCE"),
            "unsupported RapidX Exec exchange"
        );
        ensure!(
            matches!(business_type, "SPOT" | "PERP"),
            "unsupported RapidX business type"
        );
        let mut orders = Vec::new();
        let mut ids = HashSet::new();
        let mut expected = None;
        for page in 1..=MAX_PAGES {
            if page > 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let params = BTreeMap::from([
                ("exchange".into(), exchange.into()),
                ("businessType".into(), business_type.into()),
                ("page".into(), page.to_string()),
                ("pageSize".into(), PAGE_SIZE.to_string()),
            ]);
            let (status, body) = self.signed_get("/api/v1/trading/orders", &params).await?;
            let value = response(status, &body)?;
            let data = &value["data"];
            let number = |key: &str| -> Result<usize> {
                usize::try_from(
                    data[key]
                        .as_u64()
                        .with_context(|| format!("missing order pagination {key}"))?,
                )
                .context("order pagination overflow")
            };
            let pages = number("pageNum")?;
            let total = number("totalSize")?;
            ensure!(
                number("page")? == page
                    && number("pageSize")? == PAGE_SIZE
                    && pages <= MAX_PAGES
                    && total <= MAX_PAGES * PAGE_SIZE
                    && if total == 0 {
                        page == 1 && pages <= 1
                    } else {
                        pages == total.div_ceil(PAGE_SIZE) && page <= pages
                    },
                "invalid RapidX order pagination"
            );
            ensure!(
                expected.is_none_or(|old| old == (pages, total)),
                "RapidX open orders changed during pagination; retry startup"
            );
            expected = Some((pages, total));
            let rows = data["list"]
                .as_array()
                .context("missing RapidX open order list")?;
            ensure!(
                rows.len() == total.saturating_sub((page - 1) * PAGE_SIZE).min(PAGE_SIZE),
                "incomplete RapidX open order page"
            );
            for row in rows {
                let id = order_id(row, self.portfolio_id(), exchange, business_type)?;
                ensure!(ids.insert(id.clone()), "duplicate RapidX open order ID");
                orders.push(id);
            }
            if page >= pages {
                return Ok(orders);
            }
        }
        anyhow::bail!("RapidX open order page limit exceeded")
    }

    pub async fn open_perp_order_ids(&self, exchange: &str) -> Result<Vec<String>> {
        self.open_order_ids(exchange, "PERP").await
    }

    /// Never invoke the user-wide cancelAll endpoint or infer cancellation from an ACK.
    pub async fn cancel_open_orders(
        &self,
        exchange: &str,
        business_type: &str,
        timeout: Duration,
    ) -> Result<()> {
        tokio::time::timeout(timeout, async {
            let mut sent = HashSet::new();
            loop {
                let orders = self.open_order_ids(exchange, business_type).await?;
                if orders.is_empty() {
                    return Ok(());
                }
                for id in orders {
                    if !sent.insert(id.clone()) {
                        continue;
                    }
                    let (status, body) = self
                        .signed_json(
                            reqwest::Method::DELETE,
                            "/api/v1/trading/order",
                            &serde_json::json!({"orderId":id}),
                        )
                        .await?;
                    let value = response(status, &body)?;
                    ensure!(
                        value["data"]["orderId"].as_str() == Some(&id),
                        "RapidX cancel response order mismatch"
                    );
                    ensure!(
                        matches!(
                            value["data"]["action"].as_str(),
                            Some("CANCEL_PENDING" | "CANCEL_COMPLETE")
                        ),
                        "RapidX cancel not accepted"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .with_context(|| {
            format!(
                "RapidX cancel timed out; exchange={} business_type={} orders not confirmed empty",
                exchange, business_type
            )
        })?
    }

    pub async fn cancel_exec_orders_on_startup(
        &self,
        exchange: &str,
        timeout: Duration,
    ) -> Result<()> {
        self.cancel_open_orders(exchange, "PERP", timeout)
            .await
            .context("RapidX startup cancel failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn startup_uses_scoped_queries_single_order_json_and_verifies_empty() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for index in 0..4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (header, body) = loop {
                    let mut buf = [0; 4096];
                    let count = socket.read(&mut buf).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let header = std::str::from_utf8(&bytes[..end]).unwrap();
                        let size: usize = header
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break (header.to_owned(), bytes[end + 4..end + 4 + size].to_vec());
                        }
                    }
                };
                assert!(header.to_ascii_lowercase().contains("\r\nsignature: "));
                let data = if index == 1 {
                    assert_eq!(
                        header.lines().next().unwrap(),
                        "DELETE /api/v1/trading/order HTTP/1.1"
                    );
                    assert_eq!(
                        serde_json::from_slice::<Value>(&body).unwrap(),
                        json!({"orderId":"external"})
                    );
                    json!({"orderId":"external","action":"CANCEL_PENDING","orderState":"OPEN"})
                } else {
                    assert_eq!(header.lines().next().unwrap(), "GET /api/v1/trading/orders?businessType=PERP&exchange=OKX&page=1&pageSize=1000 HTTP/1.1");
                    let rows = if index == 3 {
                        vec![]
                    } else {
                        vec![
                            json!({"portfolioId":"123","exchangeType":"OKX","businessType":"PERP","sym":"OKX_PERP_BTC_USDT","orderId":"external","orderState":"OPEN"}),
                        ]
                    };
                    json!({"page":1,"pageSize":1000,"pageNum":1,"totalSize":rows.len(),"list":rows})
                };
                let body = json!({"code":200000,"data":data}).to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        LtpRestClient::fixture(format!("http://{address}"))
            .cancel_exec_orders_on_startup("OKX", Duration::from_secs(5))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_snapshot_never_causes_cancel() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for row in [
            json!({"portfolioId":"456","exchangeType":"OKX","businessType":"PERP","sym":"OKX_PERP_BTC_USDT","orderId":"other","orderState":"OPEN"}),
            Value::Null,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut buf = [0; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                assert!(bytes.starts_with(b"GET "));
                let rows = if row.is_null() { vec![] } else { vec![row] };
                let body = json!({"code":200000,"data":{"page":1,"pageSize":1000,"pageNum":1,"totalSize":1,"list":rows}}).to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_millis(200), listener.accept())
                        .await
                        .is_err()
                );
            });
            assert!(LtpRestClient::fixture(format!("http://{address}"))
                .cancel_exec_orders_on_startup("OKX", Duration::from_secs(2))
                .await
                .is_err());
            server.await.unwrap();
        }
    }

    #[test]
    fn rejects_cross_portfolio_spot_and_bad_identity_before_cancellation() {
        let row = json!({"portfolioId":"123","exchangeType":"OKX","businessType":"PERP",
            "sym":"OKX_PERP_BTC_USDT","orderState":"OPEN","orderId":"external"});
        assert_eq!(order_id(&row, "123", "OKX", "PERP").unwrap(), "external");
        for (key, value) in [
            ("portfolioId", "456"),
            ("exchangeType", "BINANCE"),
            ("businessType", "SPOT"),
            ("sym", "OKX_SPOT_BTC_USDT"),
            ("orderState", "FILLED"),
            ("orderId", ""),
        ] {
            let mut bad = row.clone();
            bad[key] = value.into();
            assert!(order_id(&bad, "123", "OKX", "PERP").is_err());
        }
        let spot = json!({"portfolioId":"123","exchangeType":"OKX","businessType":"SPOT",
            "sym":"OKX_SPOT_BTC_USDT","orderState":"PARTIALLY_FILLED","orderId":"spot"});
        assert_eq!(order_id(&spot, "123", "OKX", "SPOT").unwrap(), "spot");
        assert!(response(200, r#"{"code":2000}"#).is_err());
        assert!(response(500, r#"{"code":200000}"#).is_err());
    }
}
