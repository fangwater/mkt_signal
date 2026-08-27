//! Postgres 持久化：原始公告 + 拉取 / LLM 状态，便于重启恢复与失败查询。

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::common::announcement_watch::RawAnnouncement;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceStatus {
    pub source: String,
    pub kind: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_ms: Option<i64>,
    pub last_attempt_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRunStatus {
    pub exchange: String,
    pub announcement_id: String,
    #[serde(default)]
    pub title: String,
    pub ok: bool,
    pub last_attempt_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    pub ok: bool,
    pub as_of_ms: i64,
    pub degraded: bool,
    pub postgres: bool,
    pub sources: Vec<SourceStatus>,
    pub llm_failures: Vec<LlmRunStatus>,
}

#[derive(Clone, Default)]
pub struct StatusBook {
    sources: BTreeMap<String, SourceStatus>,
    llm: BTreeMap<(String, String), LlmRunStatus>,
}

impl StatusBook {
    pub fn snapshot(&self, postgres: bool) -> StatusSnapshot {
        let as_of_ms = chrono::Utc::now().timestamp_millis();
        let mut sources: Vec<SourceStatus> = self.sources.values().cloned().collect();
        sources.sort_by(|left, right| left.source.cmp(&right.source));
        let mut llm_failures: Vec<LlmRunStatus> =
            self.llm.values().filter(|item| !item.ok).cloned().collect();
        llm_failures.sort_by(|left, right| right.last_attempt_ms.cmp(&left.last_attempt_ms));
        llm_failures.truncate(50);
        let degraded = sources.iter().any(|item| !item.ok) || !llm_failures.is_empty();
        StatusSnapshot {
            ok: true,
            as_of_ms,
            degraded,
            postgres,
            sources,
            llm_failures,
        }
    }

    pub fn replace_sources(&mut self, rows: Vec<SourceStatus>) {
        self.sources = rows
            .into_iter()
            .map(|row| (row.source.clone(), row))
            .collect();
    }

    pub fn replace_llm(&mut self, rows: Vec<LlmRunStatus>) {
        self.llm = rows
            .into_iter()
            .map(|row| ((row.exchange.clone(), row.announcement_id.clone()), row))
            .collect();
    }

    pub fn mark_ok(&mut self, source: &str, kind: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        self.sources.insert(
            source.to_string(),
            SourceStatus {
                source: source.to_string(),
                kind: kind.to_string(),
                ok: true,
                last_success_ms: Some(now),
                last_attempt_ms: now,
                last_error: None,
                last_error_ms: None,
            },
        );
    }

    pub fn mark_err(&mut self, source: &str, kind: &str, err: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let previous = self.sources.get(source);
        self.sources.insert(
            source.to_string(),
            SourceStatus {
                last_success_ms: previous.and_then(|item| item.last_success_ms),
                source: source.to_string(),
                kind: kind.to_string(),
                ok: false,
                last_attempt_ms: now,
                last_error: Some(truncate_error(err)),
                last_error_ms: Some(now),
            },
        );
    }

    pub fn mark_llm(
        &mut self,
        exchange: &str,
        announcement_id: &str,
        title: &str,
        ok: bool,
        err: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp_millis();
        let key = (exchange.to_string(), announcement_id.to_string());
        let previous = self.llm.get(&key);
        self.llm.insert(
            key,
            LlmRunStatus {
                last_success_ms: if ok {
                    Some(now)
                } else {
                    previous.and_then(|item| item.last_success_ms)
                },
                exchange: exchange.to_string(),
                announcement_id: announcement_id.to_string(),
                title: title.to_string(),
                ok,
                last_attempt_ms: now,
                last_error: if ok { None } else { err.map(truncate_error) },
            },
        );
        if ok {
            self.mark_ok("llm", "llm");
        } else if let Some(err) = err {
            self.mark_err("llm", "llm", err);
        }
    }

    pub fn source(&self, name: &str) -> Option<&SourceStatus> {
        self.sources.get(name)
    }

    pub fn llm(&self, exchange: &str, announcement_id: &str) -> Option<&LlmRunStatus> {
        self.llm
            .get(&(exchange.to_string(), announcement_id.to_string()))
    }
}

pub struct DelistStore {
    url: String,
    client: Mutex<Option<Client>>,
}

impl DelistStore {
    pub fn new(url: String) -> Arc<Self> {
        Arc::new(Self {
            url,
            client: Mutex::new(None),
        })
    }

    pub async fn connect(url: &str) -> Result<Arc<Self>> {
        let store = Self::new(url.to_string());
        store.ensure_client().await?;
        store.migrate().await?;
        Ok(store)
    }

    async fn ensure_client(&self) -> Result<()> {
        let mut slot = self.client.lock().await;
        if slot.is_some() {
            return Ok(());
        }
        let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
            .await
            .with_context(|| format!("connect postgres failed: {}", sanitize_url(&self.url)))?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                log::warn!("postgres connection closed: {err:#}");
            }
        });
        *slot = Some(client);
        Ok(())
    }

    async fn run<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<(Client, T)>>,
    {
        self.ensure_client().await?;
        let mut slot = self.client.lock().await;
        let client = slot.take().context("postgres client missing")?;
        match f(client).await {
            Ok((client, value)) => {
                *slot = Some(client);
                Ok(value)
            }
            Err(err) => {
                *slot = None;
                Err(err)
            }
        }
    }

    pub async fn migrate(&self) -> Result<()> {
        self.run(|client| async move {
            client
                .batch_execute(
                    r#"
                    CREATE TABLE IF NOT EXISTS announcements (
                        exchange TEXT NOT NULL,
                        id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        url TEXT NOT NULL,
                        published_ms BIGINT NOT NULL,
                        source TEXT NOT NULL,
                        extra JSONB,
                        raw JSONB NOT NULL,
                        first_fetched_ms BIGINT NOT NULL,
                        last_fetched_ms BIGINT NOT NULL,
                        PRIMARY KEY (exchange, id)
                    );
                    CREATE TABLE IF NOT EXISTS source_status (
                        source TEXT PRIMARY KEY,
                        kind TEXT NOT NULL,
                        ok BOOLEAN NOT NULL,
                        last_success_ms BIGINT,
                        last_attempt_ms BIGINT NOT NULL,
                        last_error TEXT,
                        last_error_ms BIGINT
                    );
                    CREATE TABLE IF NOT EXISTS llm_status (
                        exchange TEXT NOT NULL,
                        announcement_id TEXT NOT NULL,
                        title TEXT NOT NULL DEFAULT '',
                        ok BOOLEAN NOT NULL,
                        last_attempt_ms BIGINT NOT NULL,
                        last_success_ms BIGINT,
                        last_error TEXT,
                        PRIMARY KEY (exchange, announcement_id)
                    );
                    "#,
                )
                .await
                .context("migrate delist_risk schema failed")?;
            Ok((client, ()))
        })
        .await
    }

    pub async fn upsert_announcement(&self, item: &RawAnnouncement) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let raw = serde_json::to_value(item).context("serialize raw announcement failed")?;
        let extra = item.extra.clone();
        let exchange = item.exchange.clone();
        let id = item.id.clone();
        let title = item.title.clone();
        let url = item.url.clone();
        let source = item.source.clone();
        let published_ms = item.published_ms;
        self.run(move |client| async move {
            client
                .execute(
                    r#"
                    INSERT INTO announcements (
                        exchange, id, title, url, published_ms, source, extra, raw,
                        first_fetched_ms, last_fetched_ms
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
                    ON CONFLICT (exchange, id) DO UPDATE SET
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        published_ms = EXCLUDED.published_ms,
                        source = EXCLUDED.source,
                        extra = EXCLUDED.extra,
                        raw = EXCLUDED.raw,
                        last_fetched_ms = EXCLUDED.last_fetched_ms
                    "#,
                    &[
                        &exchange,
                        &id,
                        &title,
                        &url,
                        &published_ms,
                        &source,
                        &extra,
                        &raw,
                        &now,
                    ],
                )
                .await
                .context("upsert announcement failed")?;
            Ok((client, ()))
        })
        .await
    }

    pub async fn load_announcements(&self) -> Result<Vec<RawAnnouncement>> {
        self.run(|client| async move {
            let rows = client
                .query(
                    "SELECT raw FROM announcements ORDER BY last_fetched_ms DESC",
                    &[],
                )
                .await
                .context("load announcements failed")?;
            let mut out = Vec::new();
            for row in rows {
                let raw: Value = row.get(0);
                match serde_json::from_value::<RawAnnouncement>(raw) {
                    Ok(item) => out.push(item),
                    Err(err) => log::warn!("skip stored announcement: {err:#}"),
                }
            }
            Ok((client, out))
        })
        .await
    }

    pub async fn upsert_source(&self, status: &SourceStatus) -> Result<()> {
        let source = status.source.clone();
        let kind = status.kind.clone();
        let ok = status.ok;
        let last_success_ms = status.last_success_ms;
        let last_attempt_ms = status.last_attempt_ms;
        let last_error = status.last_error.clone();
        let last_error_ms = status.last_error_ms;
        self.run(move |client| async move {
            client
                .execute(
                    r#"
                    INSERT INTO source_status (
                        source, kind, ok, last_success_ms, last_attempt_ms, last_error, last_error_ms
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (source) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        ok = EXCLUDED.ok,
                        last_success_ms = EXCLUDED.last_success_ms,
                        last_attempt_ms = EXCLUDED.last_attempt_ms,
                        last_error = EXCLUDED.last_error,
                        last_error_ms = EXCLUDED.last_error_ms
                    "#,
                    &[
                        &source,
                        &kind,
                        &ok,
                        &last_success_ms,
                        &last_attempt_ms,
                        &last_error,
                        &last_error_ms,
                    ],
                )
                .await
                .context("upsert source_status failed")?;
            Ok((client, ()))
        })
        .await
    }

    pub async fn load_sources(&self) -> Result<Vec<SourceStatus>> {
        self.run(|client| async move {
            let rows = client
                .query(
                    r#"
                    SELECT source, kind, ok, last_success_ms, last_attempt_ms, last_error, last_error_ms
                    FROM source_status
                    ORDER BY source
                    "#,
                    &[],
                )
                .await
                .context("load source_status failed")?;
            let out = rows
                .into_iter()
                .map(|row| SourceStatus {
                    source: row.get(0),
                    kind: row.get(1),
                    ok: row.get(2),
                    last_success_ms: row.get(3),
                    last_attempt_ms: row.get(4),
                    last_error: row.get(5),
                    last_error_ms: row.get(6),
                })
                .collect();
            Ok((client, out))
        })
        .await
    }

    pub async fn upsert_llm(&self, status: &LlmRunStatus) -> Result<()> {
        let exchange = status.exchange.clone();
        let announcement_id = status.announcement_id.clone();
        let title = status.title.clone();
        let ok = status.ok;
        let last_attempt_ms = status.last_attempt_ms;
        let last_success_ms = status.last_success_ms;
        let last_error = status.last_error.clone();
        self.run(move |client| async move {
            client
                .execute(
                    r#"
                    INSERT INTO llm_status (
                        exchange, announcement_id, title, ok, last_attempt_ms, last_success_ms, last_error
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (exchange, announcement_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        ok = EXCLUDED.ok,
                        last_attempt_ms = EXCLUDED.last_attempt_ms,
                        last_success_ms = EXCLUDED.last_success_ms,
                        last_error = EXCLUDED.last_error
                    "#,
                    &[
                        &exchange,
                        &announcement_id,
                        &title,
                        &ok,
                        &last_attempt_ms,
                        &last_success_ms,
                        &last_error,
                    ],
                )
                .await
                .context("upsert llm_status failed")?;
            Ok((client, ()))
        })
        .await
    }

    pub async fn load_llm(&self) -> Result<Vec<LlmRunStatus>> {
        self.run(|client| async move {
            let rows = client
                .query(
                    r#"
                    SELECT exchange, announcement_id, title, ok, last_attempt_ms, last_success_ms, last_error
                    FROM llm_status
                    ORDER BY last_attempt_ms DESC
                    "#,
                    &[],
                )
                .await
                .context("load llm_status failed")?;
            let out = rows
                .into_iter()
                .map(|row| LlmRunStatus {
                    exchange: row.get(0),
                    announcement_id: row.get(1),
                    title: row.get(2),
                    ok: row.get(3),
                    last_attempt_ms: row.get(4),
                    last_success_ms: row.get(5),
                    last_error: row.get(6),
                })
                .collect();
            Ok((client, out))
        })
        .await
    }
}

fn truncate_error(err: &str) -> String {
    const LIMIT: usize = 2_000;
    let trimmed = err.trim();
    if trimmed.chars().count() <= LIMIT {
        trimmed.to_string()
    } else {
        trimmed.chars().take(LIMIT).collect::<String>() + "…"
    }
}

fn sanitize_url(url: &str) -> String {
    match url.find('@') {
        Some(at) => format!("postgres://***@{}", &url[at + 1..]),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_err_keeps_last_success() {
        let mut book = StatusBook::default();
        book.mark_ok("binance_cms", "fetch");
        let success = book.source("binance_cms").unwrap().last_success_ms;
        book.mark_err("binance_cms", "fetch", "timeout");
        let row = book.source("binance_cms").unwrap();
        assert!(!row.ok);
        assert_eq!(row.last_success_ms, success);
        assert_eq!(row.last_error.as_deref(), Some("timeout"));
        let snap = book.snapshot(false);
        assert!(snap.degraded);
        assert_eq!(snap.sources.len(), 1);
    }

    #[test]
    fn llm_success_clears_error() {
        let mut book = StatusBook::default();
        book.mark_llm("binance", "a1", "title", false, Some("401 unauthorized"));
        book.mark_llm("binance", "a1", "title", true, None);
        let snap = book.snapshot(true);
        assert!(snap.llm_failures.is_empty());
        assert!(snap
            .sources
            .iter()
            .any(|item| item.source == "llm" && item.ok));
        assert!(snap.postgres);
    }

    #[test]
    fn sanitize_hides_password() {
        assert_eq!(
            sanitize_url("postgres://u:secret@127.0.0.1:5432/db"),
            "postgres://***@127.0.0.1:5432/db"
        );
    }
}
