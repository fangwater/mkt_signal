use crate::ltp_rest::LtpRestClient;
use anyhow::{anyhow, Result};
use persist_common::rapidx_statement::StatementRecord;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const PATH: &str = "/api/v1/trading/statement";
const PAGE_SIZE: usize = 1000;
const MAX_PAGES: usize = 100;
// Leave headroom when the same portfolio has both Binance and OKX monitors.
pub const STATEMENT_REQUEST_INTERVAL: Duration = Duration::from_millis(1500);

impl LtpRestClient {
    pub async fn fetch_statement_history(
        &self,
        exchange: &str,
        begin_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<StatementRecord>> {
        let now = chrono::Utc::now().timestamp_millis();
        if !matches!(exchange, "BINANCE" | "OKX")
            || begin_ms <= 0
            || end_ms < begin_ms
            || end_ms > now
            || begin_ms < now - 90 * 86_400_000
        {
            return Err(anyhow!("invalid statement history scope or window"));
        }
        let mut rows = Vec::new();
        let mut ids = HashSet::new();
        let mut pages = None;
        let mut total = None;
        for page in 1..=MAX_PAGES {
            if page > 1 {
                tokio::time::sleep(STATEMENT_REQUEST_INTERVAL).await;
            }
            let params = BTreeMap::from([
                ("exchange".into(), exchange.into()),
                ("startTime".into(), begin_ms.to_string()),
                ("endTime".into(), end_ms.to_string()),
                ("page".into(), page.to_string()),
                ("pageSize".into(), PAGE_SIZE.to_string()),
            ]);
            let (status, body) = self.signed_get(PATH, &params).await?;
            let parsed = validate_page(
                status,
                &body,
                page,
                self.portfolio_id(),
                exchange,
                begin_ms,
                end_ms,
                pages,
                total,
                &mut ids,
            )?;
            pages = Some(parsed.0);
            total = Some(parsed.1);
            rows.extend(parsed.2);
            if page >= parsed.0 {
                if rows.len() != parsed.1 {
                    return Err(anyhow!("statement totalSize mismatch"));
                }
                return Ok(rows);
            }
        }
        Err(anyhow!("statement history exceeds 100 pages"))
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_page(
    status: u16,
    body: &str,
    wanted: usize,
    portfolio: &str,
    exchange: &str,
    begin: i64,
    end: i64,
    old_pages: Option<usize>,
    old_total: Option<usize>,
    ids: &mut HashSet<String>,
) -> Result<(usize, usize, Vec<StatementRecord>)> {
    if status != 200 {
        return Err(anyhow!("statement HTTP {status}"));
    }
    let root: Value = serde_json::from_str(body)?;
    if !matches!(root.get("code").and_then(Value::as_i64), Some(200000 | 200)) {
        return Err(anyhow!("statement response code invalid"));
    }
    let data = root
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("statement data missing"))?;
    let int = |k| {
        data.get(k)
            .and_then(Value::as_u64)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| anyhow!("statement {k} missing"))
    };
    let page = int("page")?;
    let size = int("pageSize")?;
    let pages = int("pageNum")?;
    let total = int("totalSize")?;
    if page != wanted
        || size != PAGE_SIZE
        || pages > MAX_PAGES
        || total > MAX_PAGES * PAGE_SIZE
        || (total > 0 && (pages != total.div_ceil(PAGE_SIZE) || page > pages))
        || (total == 0 && (pages > 1 || page != 1))
        || old_pages.is_some_and(|x| x != pages)
        || old_total.is_some_and(|x| x != total)
    {
        return Err(anyhow!("statement pagination invalid"));
    }
    let list = data
        .get("list")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("statement list missing"))?;
    let expected_len = total.saturating_sub((page - 1) * PAGE_SIZE).min(PAGE_SIZE);
    anyhow::ensure!(
        list.len() == expected_len,
        "incomplete or oversized statement page"
    );
    let begin_us = begin
        .checked_mul(1000)
        .ok_or_else(|| anyhow!("statement begin overflows"))?;
    let end_us = end
        .checked_mul(1000)
        .ok_or_else(|| anyhow!("statement end overflows"))?;
    let mut out = Vec::new();
    for row in list {
        let id = row
            .get("statementId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("statement id missing"))?;
        if !ids.insert(id.into()) {
            return Err(anyhow!("duplicate statement id"));
        }
        let parsed = StatementRecord::parse(row, portfolio, exchange)?;
        if parsed.timestamp_us < begin_us || parsed.timestamp_us > end_us {
            return Err(anyhow!("statement timestamp out of range"));
        }
        out.push(parsed);
    }
    Ok((pages, total, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_statement_history_accepts_zero_or_one_total_pages() {
        for pages in [0, 1] {
            let body = serde_json::json!({"code":200000,"data":{"page":1,"pageSize":1000,
                "pageNum":pages,"totalSize":0,"list":[]}})
            .to_string();
            let parsed = validate_page(
                200,
                &body,
                1,
                "123",
                "BINANCE",
                0,
                10,
                None,
                None,
                &mut HashSet::new(),
            )
            .unwrap();
            assert!(parsed.2.is_empty());
        }
    }

    #[test]
    fn rejects_short_nonfinal_pages_oversized_and_changing_totals() {
        for (pages, total) in [(2, 1001), (1, 0), (101, 100001)] {
            assert!(validate_page(
                200,
                &body(1, pages, total, "a", Value::from(123), 10),
                1,
                "123",
                "BINANCE",
                0,
                10,
                None,
                None,
                &mut HashSet::new()
            )
            .is_err());
        }
        assert!(validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(123), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            Some(1),
            Some(2),
            &mut HashSet::new()
        )
        .is_err());
        assert!(validate_page(
            200,
            r#"{"code":401001}"#,
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut HashSet::new()
        )
        .is_err());
    }
    fn body(
        page: usize,
        pages: usize,
        total: usize,
        id: &str,
        portfolio: Value,
        ts: i64,
    ) -> String {
        serde_json::json!({"code":200000,"data":{"page":page,"pageSize":1000,"pageNum":pages,
            "totalSize":total,"list":[{"portfolioId":portfolio,"statementId":id,"requestId":"r",
            "coin":"USDT","sym":"","statementType":"FUNDING_FEE","exchangeType":"BINANCE",
            "businessType":"PERP","beforeAvailable":"0","afterAvailable":"1","beforeOverdraw":"0",
            "afterOverdraw":"0","beforeBorrow":"0","afterBorrow":"0","deltaAmount":"1","createAt":ts}]}}).to_string()
    }
    #[test]
    fn accepts_numeric_portfolio_and_valid_page() {
        let mut ids = HashSet::new();
        assert!(validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(123), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut ids
        )
        .is_ok());
    }
    #[test]
    fn rejects_http_code_pagination_duplicate_scope_and_time() {
        let mut ids = HashSet::new();
        assert!(
            validate_page(500, "{}", 1, "123", "BINANCE", 0, 10, None, None, &mut ids).is_err()
        );
        assert!(validate_page(
            200,
            &body(2, 1, 1, "a", Value::from(123), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut ids
        )
        .is_err());
        let mut ids = HashSet::new();
        validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(123), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut ids,
        )
        .unwrap();
        assert!(validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(123), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            Some(1),
            Some(1),
            &mut ids
        )
        .is_err());
        let mut ids = HashSet::new();
        assert!(validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(456), 10),
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut ids
        )
        .is_err());
        let mut ids = HashSet::new();
        assert!(validate_page(
            200,
            &body(1, 1, 1, "a", Value::from(123), 11),
            1,
            "123",
            "BINANCE",
            0,
            10,
            None,
            None,
            &mut ids
        )
        .is_err());
    }
}
