use crate::ltp_rest::LtpRestClient;
use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const EXECUTIONS_PAGEABLE_PATH: &str = "/api/v1/trading/executions/pageable";
const ARCHIVED_EXECUTIONS_PATH: &str = "/api/v1/trading/archive/executions/pageable";
const PAGE_SIZE: usize = 1000;
const MAX_PAGES: usize = 100;
const PAGE_INTERVAL: Duration = Duration::from_millis(2100);

impl LtpRestClient {
    /// Fetches a bounded, complete RapidX execution window. The official
    /// Uses Unix ms consistently with execution `createAt`.
    pub async fn fetch_transaction_history(
        &self,
        exchange: &str,
        begin_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>> {
        let ranges = history_ranges(begin_ms, end_ms, chrono::Utc::now().timestamp_millis())?;
        let mut rows = Vec::new();
        let mut ids = std::collections::HashMap::<String, Value>::new();
        for (index, (path, begin, end)) in ranges.into_iter().enumerate() {
            if index > 0 {
                tokio::time::sleep(PAGE_INTERVAL).await;
            }
            for row in self
                .fetch_transaction_window(path, exchange, begin, end)
                .await?
            {
                let id = row["transactionId"]
                    .as_str()
                    .context("missing execution identity")?
                    .to_string();
                if let Some(previous) = ids.get(&id) {
                    anyhow::ensure!(
                        previous == &row,
                        "conflicting execution at recent/archive boundary"
                    );
                } else {
                    ids.insert(id, row.clone());
                    rows.push(row);
                }
            }
        }
        Ok(rows)
    }

    async fn fetch_transaction_window(
        &self,
        path: &str,
        exchange: &str,
        begin_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Value>> {
        if !matches!(exchange, "BINANCE" | "OKX") {
            return Err(anyhow!("unsupported RapidX execution exchange {exchange}"));
        }
        if begin_ms < 0 || end_ms < begin_ms {
            return Err(anyhow!("invalid execution history time window"));
        }
        let mut all = Vec::new();
        let mut ids = HashSet::new();
        let mut expected_total = None;
        let mut expected_pages = None;
        for page in 1..=MAX_PAGES {
            if page > 1 {
                tokio::time::sleep(PAGE_INTERVAL).await;
            }
            let params = BTreeMap::from([
                ("exchange".to_string(), exchange.to_string()),
                ("begin".to_string(), begin_ms.to_string()),
                ("end".to_string(), end_ms.to_string()),
                ("page".to_string(), page.to_string()),
                ("pageSize".to_string(), PAGE_SIZE.to_string()),
            ]);
            let (http_status, body) = self.signed_get(path, &params).await?;
            let parsed = validate_execution_page(
                http_status,
                &body,
                page,
                exchange,
                self.portfolio_id(),
                begin_ms,
                end_ms,
                expected_pages,
                expected_total,
                &mut ids,
            )?;
            expected_pages = Some(parsed.pages);
            expected_total = Some(parsed.total);
            all.extend(parsed.rows);
            if page >= parsed.pages {
                if all.len() != parsed.total {
                    return Err(anyhow!("execution pagination totalSize mismatch"));
                }
                return Ok(all);
            }
        }
        Err(anyhow!(
            "execution history exceeds hard page limit {MAX_PAGES}"
        ))
    }
}

fn history_ranges(begin: i64, end: i64, now: i64) -> Result<Vec<(&'static str, i64, i64)>> {
    anyhow::ensure!(
        begin > 0 && begin <= end && end <= now,
        "invalid execution history window"
    );
    anyhow::ensure!(
        begin >= now - 90 * 86_400_000,
        "execution recovery exceeds the documented 90-day archive"
    );
    let cutoff = now - 7 * 86_400_000;
    let mut ranges = Vec::new();
    if begin < cutoff {
        ranges.push((ARCHIVED_EXECUTIONS_PATH, begin, end.min(cutoff)));
    }
    if end >= cutoff {
        ranges.push((EXECUTIONS_PAGEABLE_PATH, begin.max(cutoff), end));
    }
    Ok(ranges)
}

struct Page {
    pages: usize,
    total: usize,
    rows: Vec<Value>,
}

#[allow(clippy::too_many_arguments)]
fn validate_execution_page(
    http_status: u16,
    body: &str,
    requested_page: usize,
    exchange: &str,
    portfolio_id: &str,
    begin_ms: i64,
    end_ms: i64,
    expected_pages: Option<usize>,
    expected_total: Option<usize>,
    ids: &mut HashSet<String>,
) -> Result<Page> {
    if http_status != 200 {
        return Err(anyhow!("execution history HTTP status {http_status}"));
    }
    let root: Value =
        serde_json::from_str(body).with_context(|| "decode execution history response")?;
    let code = root
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("execution response missing code"))?;
    if !matches!(code, 200 | 200000) {
        return Err(anyhow!("execution response code {code}"));
    }
    let data = root
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("execution response missing data object"))?;
    let page = integer(data, "page")?;
    let page_size = integer(data, "pageSize")?;
    let pages = integer(data, "pageNum")?;
    let total = integer(data, "totalSize")?;
    if page != requested_page
        || page_size != PAGE_SIZE
        || pages > MAX_PAGES
        || (total > 0 && (pages == 0 || page > pages))
        || (total == 0 && (pages > 1 || page != 1))
    {
        return Err(anyhow!("invalid execution pagination envelope"));
    }
    if expected_pages.is_some_and(|value| value != pages)
        || expected_total.is_some_and(|value| value != total)
    {
        return Err(anyhow!("execution pagination changed during fetch"));
    }
    let list = data
        .get("list")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("execution response missing list"))?;
    if list.is_empty() && total != 0 {
        return Err(anyhow!("unexpected empty execution page"));
    }
    if list.len() > PAGE_SIZE || list.len() > total || (page < pages && list.len() != PAGE_SIZE) {
        return Err(anyhow!("execution page is incomplete or oversized"));
    }
    let mut rows = Vec::with_capacity(list.len());
    for row in list {
        let object = row
            .as_object()
            .ok_or_else(|| anyhow!("execution list row must be object"))?;
        let id = required_string(object, "transactionId")?;
        if !ids.insert(id.to_string()) {
            return Err(anyhow!("duplicate transactionId {id}"));
        }
        if required_string(object, "portfolioId")? != portfolio_id
            || !required_string(object, "exchangeType")?.eq_ignore_ascii_case(exchange)
        {
            return Err(anyhow!("execution scope mismatch"));
        }
        let timestamp = required_string(object, "createAt")?
            .parse::<i64>()
            .map_err(|_| anyhow!("execution createAt is not milliseconds"))?;
        if timestamp < begin_ms || timestamp > end_ms {
            return Err(anyhow!("execution timestamp outside requested window"));
        }
        rows.push(row.clone());
    }
    Ok(Page { pages, total, rows })
}

fn integer(object: &serde_json::Map<String, Value>, key: &str) -> Result<usize> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| anyhow!("execution response missing integer {key}"))
}
fn required_string<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("execution row missing string {key}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn splits_archive_boundary_without_skipping_it_or_overstating_retention() {
        let now = 100 * 86_400_000;
        let cutoff = now - 7 * 86_400_000;
        assert_eq!(
            history_ranges(cutoff - 1000, cutoff + 1000, now).unwrap(),
            vec![
                (ARCHIVED_EXECUTIONS_PATH, cutoff - 1000, cutoff),
                (EXECUTIONS_PAGEABLE_PATH, cutoff, cutoff + 1000)
            ]
        );
        assert_eq!(history_ranges(cutoff, now, now).unwrap().len(), 1);
        assert!(history_ranges(now - 91 * 86_400_000, now, now).is_err());
        assert!(history_ranges(now, now + 1, now).is_err());
    }
    fn page(page: usize, pages: usize, total: usize, id: &str) -> String {
        format!(
            r#"{{"code":200000,"data":{{"page":{page},"pageSize":1000,"pageNum":{pages},"totalSize":{total},"list":[{{"transactionId":"{id}","portfolioId":"p","exchangeType":"BINANCE","createAt":"10","fee":"x"}}]}}}}"#
        )
    }
    #[test]
    fn validates_stable_paginated_rows() {
        let mut ids = HashSet::new();
        let parsed = validate_execution_page(
            200,
            &page(1, 1, 1, "id1"),
            1,
            "BINANCE",
            "p",
            0,
            10,
            None,
            None,
            &mut ids,
        )
        .unwrap();
        assert_eq!(parsed.rows[0]["transactionId"], "id1");
    }
    #[test]
    fn rejects_duplicate_or_changed_pagination() {
        let mut ids = HashSet::new();
        let mut first: Value = serde_json::from_str(&page(1, 2, 1001, "id1")).unwrap();
        let row = first["data"]["list"][0].clone();
        first["data"]["list"] = Value::Array(
            (0..1000)
                .map(|index| {
                    let mut row = row.clone();
                    row["transactionId"] = Value::String(format!("id{index}"));
                    row
                })
                .collect(),
        );
        validate_execution_page(
            200,
            &first.to_string(),
            1,
            "BINANCE",
            "p",
            0,
            10,
            None,
            None,
            &mut ids,
        )
        .unwrap();
        assert!(validate_execution_page(
            200,
            &page(2, 3, 2, "id1"),
            2,
            "BINANCE",
            "p",
            0,
            10,
            Some(2),
            Some(1001),
            &mut ids
        )
        .is_err());
        assert!(validate_execution_page(
            200,
            &page(2, 2, 1001, "id1"),
            2,
            "BINANCE",
            "p",
            0,
            10,
            Some(2),
            Some(1001),
            &mut ids
        )
        .is_err());
    }
    #[test]
    fn empty_complete_page_is_not_an_error_but_truncation_is() {
        for pages in [0, 1] {
            let body = serde_json::json!({"code":200000,"data":{"page":1,"pageSize":1000,"pageNum":pages,"totalSize":0,"list":[]}});
            assert!(validate_execution_page(
                200,
                &body.to_string(),
                1,
                "BINANCE",
                "p",
                0,
                10,
                None,
                None,
                &mut HashSet::new()
            )
            .unwrap()
            .rows
            .is_empty());
        }
        assert!(validate_execution_page(
            200,
            &page(1, 2, 1001, "id"),
            1,
            "BINANCE",
            "p",
            0,
            10,
            None,
            None,
            &mut HashSet::new()
        )
        .is_err());
        assert!(validate_execution_page(
            200,
            &page(1, 101, 100001, "id"),
            1,
            "BINANCE",
            "p",
            0,
            10,
            None,
            None,
            &mut HashSet::new()
        )
        .is_err());
    }
    #[test]
    fn rejects_wrong_scope_timestamp_http_and_missing_code() {
        for (status, body, exchange, portfolio, end) in [
            (404, page(1, 1, 1, "id"), "BINANCE", "p", 10),
            (200, page(1, 1, 1, "id"), "OKX", "p", 10),
            (200, page(1, 1, 1, "id"), "BINANCE", "other", 10),
            (200, page(1, 1, 1, "id"), "BINANCE", "p", 9),
            (200, "{}".into(), "BINANCE", "p", 10),
        ] {
            assert!(validate_execution_page(
                status,
                &body,
                1,
                exchange,
                portfolio,
                0,
                end,
                None,
                None,
                &mut HashSet::new()
            )
            .is_err());
        }
    }
}
