//! 交易所下架公告拉取的公共状态 / 输出。
//! 只保存原文元数据，不做标题分类或 ticker 解析。

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SeenStore {
    pub seen: BTreeMap<String, SeenItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeenItem {
    pub id: String,
    pub title: String,
    pub url: String,
    pub published_ms: i64,
    pub first_seen_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawAnnouncement {
    pub exchange: String,
    pub id: String,
    pub title: String,
    pub url: String,
    pub published_ms: i64,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

impl RawAnnouncement {
    pub fn key(&self) -> String {
        format!("{}:{}", self.exchange, self.id)
    }
}

pub fn http_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(DEFAULT_USER_AGENT)
        .build()
        .context("build announcement HTTP client failed")
}

pub fn load_store(path: &Path) -> Result<SeenStore> {
    if !path.exists() {
        return Ok(SeenStore::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read state file {} failed", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parse state file {} failed", path.display()))
}

pub fn save_store(path: &Path, store: &SeenStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state dir {} failed", parent.display()))?;
        }
    }
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let raw = serde_json::to_string_pretty(store).context("serialize seen store failed")?;
    std::fs::write(&tmp, raw).with_context(|| format!("write {} failed", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {} failed", tmp.display(), path.display()))?;
    Ok(())
}

pub fn remember(store: &mut SeenStore, item: &RawAnnouncement, now_ms: i64) -> bool {
    let key = item.key();
    if store.seen.contains_key(&key) {
        return false;
    }
    store.seen.insert(
        key,
        SeenItem {
            id: item.id.clone(),
            title: item.title.clone(),
            url: item.url.clone(),
            published_ms: item.published_ms,
            first_seen_ms: now_ms,
        },
    );
    true
}

pub fn emit_announcement(store: &mut SeenStore, item: RawAnnouncement) -> bool {
    let now_ms = chrono::Utc::now().timestamp_millis();
    if remember(store, &item, now_ms) {
        log::info!(
            "announcement exchange={} id={} title={}",
            item.exchange,
            item.id,
            item.title
        );
        println!("{}", serde_json::to_string(&item).unwrap_or_default());
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remember_is_idempotent() {
        let mut store = SeenStore::default();
        let item = RawAnnouncement {
            exchange: "bitget".into(),
            id: "1".into(),
            title: "delist".into(),
            url: "https://example".into(),
            published_ms: 1,
            source: "test".into(),
            extra: None,
        };
        assert!(remember(&mut store, &item, 10));
        assert!(!remember(&mut store, &item, 11));
        assert_eq!(store.seen.len(), 1);
    }
}
