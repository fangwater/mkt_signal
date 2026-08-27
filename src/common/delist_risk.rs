//! 下架风险簿：官方快照 + LLM 抽取，按 venue / exchange 全量扁平查询。

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::common::announcement_llm::{LlmAction, LlmExtractInput};
use crate::common::announcement_watch::RawAnnouncement;
use crate::common::binance_announcement::{OfficialSnapshot, ParsedAnnouncement};
use crate::common::delist_schedule::DelistEvent;

const PAST_GRACE: ChronoDuration = ChronoDuration::days(7);
const MAX_ANNOUNCEMENTS: usize = 200;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RiskEvent {
    pub action: String,
    pub venue: String,
    pub exchange: String,
    pub utc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_ms: Option<i64>,
    #[serde(default)]
    pub assets: Vec<String>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub note: String,
    pub source: String,
    #[serde(default)]
    pub announcement_id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub published_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RiskBook {
    #[serde(default)]
    pub events: Vec<RiskEvent>,
    #[serde(default)]
    pub announcements: Vec<AnnouncementMeta>,
    #[serde(default)]
    pub updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnouncementMeta {
    pub exchange: String,
    pub id: String,
    pub title: String,
    pub url: String,
    pub published_ms: i64,
    #[serde(default)]
    pub relevant: bool,
}

#[derive(Debug, Clone, Default)]
pub struct RiskQuery {
    pub venue: Option<String>,
    pub exchange: Option<String>,
    pub days: Option<i64>,
    pub include_past: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskQueryResponse {
    pub ok: bool,
    pub as_of_ms: i64,
    pub abnormal: bool,
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,
    pub items: Vec<RiskEventView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskEventView {
    pub exchange: String,
    pub venue: String,
    pub action: String,
    pub utc: String,
    pub status: String,
    pub assets: Vec<String>,
    pub symbols: Vec<String>,
    pub note: String,
    pub source: String,
    pub title: String,
    pub url: String,
    pub announcement_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VenueSummary {
    pub venue: String,
    pub exchange: String,
    pub abnormal: bool,
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_utc: Option<String>,
}

impl RiskEvent {
    pub fn id(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.source,
            self.announcement_id,
            self.venue,
            self.action,
            self.utc,
            self.index_keys().join(",")
        )
    }

    pub fn index_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for item in self.symbols.iter().chain(self.assets.iter()) {
            let key = normalize_symbol(item);
            if !key.is_empty() && !keys.iter().any(|seen| seen == &key) {
                keys.push(key);
            }
        }
        keys
    }

    pub fn status_at(&self, now_ms: i64) -> &'static str {
        match self.utc_ms {
            None => "unknown",
            Some(ms) if ms > now_ms => "upcoming",
            Some(ms) if ms > now_ms - PAST_GRACE.num_milliseconds() => "due",
            Some(_) => "past",
        }
    }

    pub fn is_active(&self, now_ms: i64, include_past: bool) -> bool {
        include_past || self.status_at(now_ms) != "past"
    }
}

impl RiskBook {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read risk book {} failed", path.display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("parse risk book {} failed", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create risk dir {} failed", parent.display()))?;
            }
        }
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));
        let raw = serde_json::to_string_pretty(self).context("serialize risk book failed")?;
        std::fs::write(&tmp, raw).with_context(|| format!("write {} failed", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename {} -> {} failed", tmp.display(), path.display()))?;
        Ok(())
    }

    pub fn touch(&mut self) {
        self.updated_ms = Utc::now().timestamp_millis();
    }

    pub fn remember_announcement(&mut self, item: &AnnouncementMeta) {
        if self
            .announcements
            .iter()
            .any(|seen| seen.exchange == item.exchange && seen.id == item.id)
        {
            return;
        }
        self.announcements.insert(0, item.clone());
        if self.announcements.len() > MAX_ANNOUNCEMENTS {
            self.announcements.truncate(MAX_ANNOUNCEMENTS);
        }
        self.touch();
    }

    pub fn upsert_events(&mut self, events: Vec<RiskEvent>) -> usize {
        let mut added = 0;
        for event in events {
            if event.venue.is_empty() || event.index_keys().is_empty() {
                continue;
            }
            let id = event.id();
            if self.events.iter().any(|seen| seen.id() == id) {
                continue;
            }
            self.events.push(event);
            added += 1;
        }
        if added > 0 {
            self.touch();
        }
        added
    }

    pub fn replace_source(&mut self, source: &str, events: Vec<RiskEvent>) -> usize {
        self.events.retain(|event| event.source != source);
        let n = events.len();
        self.upsert_events(events);
        self.touch();
        n
    }

    pub fn ingest_llm(&mut self, input: &LlmExtractInput, relevant: bool, actions: &[LlmAction]) {
        self.remember_announcement(&AnnouncementMeta {
            exchange: input.exchange.clone(),
            id: input.id.clone(),
            title: input.title.clone(),
            url: input.url.clone(),
            published_ms: input.published_ms,
            relevant,
        });
        if !relevant {
            return;
        }
        let events = actions
            .iter()
            .map(|action| RiskEvent {
                utc_ms: parse_utc_ms(&action.utc),
                action: action.action.clone(),
                venue: action.venue.clone(),
                exchange: action.exchange.clone(),
                utc: action.utc.clone(),
                assets: action.assets.clone(),
                symbols: action.symbols.clone(),
                note: action.note.clone(),
                source: "llm_extract".to_string(),
                announcement_id: input.id.clone(),
                title: input.title.clone(),
                url: input.url.clone(),
                published_ms: input.published_ms,
            })
            .collect();
        self.upsert_events(events);
    }

    pub fn ingest_llm_value(&mut self, input: &LlmExtractInput, value: &Value) {
        let relevant = value
            .get("relevant")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let actions: Vec<LlmAction> = value
            .get("actions")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        self.ingest_llm(input, relevant, &actions);
    }

    pub fn query(&self, q: &RiskQuery) -> RiskQueryResponse {
        let now_ms = Utc::now().timestamp_millis();
        let horizon_ms = q
            .days
            .filter(|days| *days > 0)
            .map(|days| now_ms + days.saturating_mul(86_400_000));
        let mut items: Vec<RiskEventView> = self
            .events
            .iter()
            .filter(|event| event_matches(event, q, now_ms, horizon_ms))
            .map(|event| RiskEventView {
                exchange: event.exchange.clone(),
                venue: event.venue.clone(),
                action: event.action.clone(),
                utc: event.utc.clone(),
                status: event.status_at(now_ms).to_string(),
                assets: event.assets.clone(),
                symbols: event.symbols.clone(),
                note: event.note.clone(),
                source: event.source.clone(),
                title: event.title.clone(),
                url: event.url.clone(),
                announcement_id: event.announcement_id.clone(),
            })
            .collect();
        items.sort_by(|left, right| {
            left.utc
                .cmp(&right.utc)
                .then_with(|| left.exchange.cmp(&right.exchange))
                .then_with(|| left.venue.cmp(&right.venue))
                .then_with(|| left.action.cmp(&right.action))
                .then_with(|| left.announcement_id.cmp(&right.announcement_id))
                .then_with(|| left.assets.cmp(&right.assets))
                .then_with(|| left.symbols.cmp(&right.symbols))
        });
        let abnormal = items.iter().any(|item| item.status != "past");
        RiskQueryResponse {
            count: items.len(),
            as_of_ms: now_ms,
            ok: true,
            abnormal,
            venue: q.venue.clone(),
            exchange: q.exchange.clone(),
            items,
        }
    }

    pub fn venue_summaries(&self, q: &RiskQuery) -> Vec<VenueSummary> {
        let scanned = self.query(q);
        let mut by_venue: BTreeMap<String, VenueSummary> = BTreeMap::new();
        for item in scanned.items {
            let entry = by_venue.entry(item.venue.clone()).or_insert(VenueSummary {
                exchange: if item.exchange.is_empty() {
                    venue_exchange(&item.venue).to_string()
                } else {
                    item.exchange.clone()
                },
                venue: item.venue.clone(),
                abnormal: false,
                count: 0,
                next_utc: None,
            });
            entry.count += 1;
            if item.status != "past" {
                entry.abnormal = true;
            }
            if item.status == "upcoming" && !item.utc.is_empty() {
                if entry
                    .next_utc
                    .as_ref()
                    .map(|existing| item.utc.as_str() < existing.as_str())
                    .unwrap_or(true)
                {
                    entry.next_utc = Some(item.utc);
                }
            }
        }
        by_venue.into_values().collect()
    }
}

pub fn events_from_official_snapshot(
    venue: &str,
    exchange: &str,
    action: &str,
    snapshot: &OfficialSnapshot,
) -> Vec<RiskEvent> {
    snapshot
        .items
        .iter()
        .filter(|item| !item.key.trim().is_empty())
        .map(|item| {
            let (utc, utc_ms) = match item.when_ms {
                Some(ms) => (utc_from_ms(ms), Some(ms)),
                None => (String::new(), None),
            };
            let key = item.key.clone();
            let (assets, symbols) = if looks_like_pair(&key) {
                (Vec::new(), vec![normalize_symbol(&key)])
            } else {
                (vec![normalize_symbol(&key)], Vec::new())
            };
            RiskEvent {
                action: action.to_string(),
                venue: venue.to_string(),
                exchange: exchange.to_string(),
                utc,
                utc_ms,
                assets,
                symbols,
                note: item.detail.clone(),
                source: snapshot.source.clone(),
                announcement_id: snapshot.source.clone(),
                title: snapshot.source.clone(),
                url: String::new(),
                published_ms: 0,
            }
        })
        .collect()
}

pub fn events_from_delist_schedule(events: &[DelistEvent]) -> Vec<RiskEvent> {
    events
        .iter()
        .map(|event| {
            let utc_ms = event.delist_time.map(|ts| ts.timestamp_millis());
            let utc = event
                .delist_time
                .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
                .unwrap_or_default();
            let key = event.symbol.clone();
            let (assets, symbols) = if looks_like_pair(&key) {
                (Vec::new(), vec![normalize_symbol(&key)])
            } else {
                (vec![normalize_symbol(&key)], Vec::new())
            };
            RiskEvent {
                action: "delist".to_string(),
                venue: event.venue.data_pub_slug().to_string(),
                exchange: event.venue.trade_engine_exchange().to_string(),
                utc,
                utc_ms,
                assets,
                symbols,
                note: event
                    .detail
                    .clone()
                    .unwrap_or_else(|| event.risk_type.to_string()),
                source: event.source.to_string(),
                announcement_id: event.source.to_string(),
                title: event.risk_type.to_string(),
                url: String::new(),
                published_ms: 0,
            }
        })
        .collect()
}

pub fn events_from_gate_snapshot(snapshot: &Value) -> Vec<RiskEvent> {
    let mut out = Vec::new();
    if let Some(pairs) = snapshot.get("spot_pairs").and_then(Value::as_array) {
        for pair in pairs {
            let id = pair.get("id").and_then(Value::as_str).unwrap_or("");
            if id.is_empty() {
                continue;
            }
            let ts = pair.get("delisting_time").and_then(Value::as_i64);
            let utc_ms = ts.map(|sec| sec.saturating_mul(1000));
            out.push(RiskEvent {
                action: "delist".to_string(),
                venue: "gate-margin".to_string(),
                exchange: "gate".to_string(),
                utc: utc_ms.map(utc_from_ms).unwrap_or_default(),
                utc_ms,
                assets: Vec::new(),
                symbols: vec![normalize_symbol(id)],
                note: pair
                    .get("trade_status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                source: "gate_market".to_string(),
                announcement_id: "gate_market".to_string(),
                title: "gate spot delisting_time".to_string(),
                url: String::new(),
                published_ms: 0,
            });
        }
    }
    for (field, venue) in [
        ("usdt_futures_in_delisting", "gate-futures"),
        ("btc_futures_in_delisting", "gate-coin-futures"),
    ] {
        if let Some(contracts) = snapshot.get(field).and_then(Value::as_array) {
            for contract in contracts {
                let name = contract.get("name").and_then(Value::as_str).unwrap_or("");
                if name.is_empty() {
                    continue;
                }
                out.push(RiskEvent {
                    action: "delist".to_string(),
                    venue: venue.to_string(),
                    exchange: "gate".to_string(),
                    utc: String::new(),
                    utc_ms: None,
                    assets: Vec::new(),
                    symbols: vec![normalize_symbol(name)],
                    note: contract
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("in_delisting")
                        .to_string(),
                    source: "gate_market".to_string(),
                    announcement_id: "gate_market".to_string(),
                    title: "gate futures in_delisting".to_string(),
                    url: String::new(),
                    published_ms: 0,
                });
            }
        }
    }
    out
}

pub fn events_from_bitget_offtime(snapshot: &Value) -> Vec<RiskEvent> {
    let mut out = Vec::new();
    for (field, venue) in [
        ("spot", "bitget-margin"),
        ("usdt_futures", "bitget-futures"),
        ("coin_futures", "bitget-coin-futures"),
    ] {
        if let Some(rows) = snapshot.get(field).and_then(Value::as_array) {
            for row in rows {
                let symbol = row.get("symbol").and_then(Value::as_str).unwrap_or("");
                if symbol.is_empty() {
                    continue;
                }
                let utc_ms = row.get("offTime").and_then(Value::as_i64);
                out.push(RiskEvent {
                    action: "delist".to_string(),
                    venue: venue.to_string(),
                    exchange: "bitget".to_string(),
                    utc: utc_ms.map(utc_from_ms).unwrap_or_default(),
                    utc_ms,
                    assets: Vec::new(),
                    symbols: vec![normalize_symbol(symbol)],
                    note: row
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                    source: "bitget_instrument_offtime".to_string(),
                    announcement_id: "bitget_instrument_offtime".to_string(),
                    title: "bitget instrument offTime".to_string(),
                    url: String::new(),
                    published_ms: 0,
                });
            }
        }
    }
    out
}

pub fn announcement_from_raw(item: &RawAnnouncement) -> AnnouncementMeta {
    AnnouncementMeta {
        exchange: item.exchange.clone(),
        id: item.id.clone(),
        title: item.title.clone(),
        url: item.url.clone(),
        published_ms: item.published_ms,
        relevant: true,
    }
}

pub fn announcement_from_parsed(item: &ParsedAnnouncement) -> AnnouncementMeta {
    AnnouncementMeta {
        exchange: "binance".to_string(),
        id: item.code.clone(),
        title: item.title.clone(),
        url: item.url.clone(),
        published_ms: item.release_date_ms,
        relevant: true,
    }
}

pub fn utc_from_ms(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}

pub fn parse_utc_ms(raw: &str) -> Option<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .ok()
        .map(|ts| ts.timestamp_millis())
}

pub fn normalize_symbol(raw: &str) -> String {
    raw.trim().to_ascii_uppercase().replace(['/', '-', '_'], "")
}

fn looks_like_pair(token: &str) -> bool {
    let compact = normalize_symbol(token);
    quote_suffix(&compact).is_some()
}

fn quote_suffix(token: &str) -> Option<&'static str> {
    ["USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH"]
        .into_iter()
        .find(|quote| token.len() > quote.len() && token.ends_with(quote))
}

fn venue_exchange(venue: &str) -> &str {
    venue.split('-').next().unwrap_or(venue)
}

fn event_matches(event: &RiskEvent, q: &RiskQuery, now_ms: i64, horizon_ms: Option<i64>) -> bool {
    if let Some(venue) = q.venue.as_deref() {
        if !event.venue.eq_ignore_ascii_case(venue) {
            return false;
        }
    }
    if let Some(exchange) = q.exchange.as_deref() {
        if !event.exchange.eq_ignore_ascii_case(exchange) {
            return false;
        }
    }
    if !event.is_active(now_ms, q.include_past) {
        return false;
    }
    if let (Some(limit), Some(utc_ms)) = (horizon_ms, event.utc_ms) {
        if utc_ms > limit {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(symbol: &str, venue: &str, utc: &str, action: &str) -> RiskEvent {
        RiskEvent {
            action: action.to_string(),
            venue: venue.to_string(),
            exchange: "binance".to_string(),
            utc: utc.to_string(),
            utc_ms: parse_utc_ms(utc),
            assets: vec![symbol.to_string()],
            symbols: Vec::new(),
            note: "test".to_string(),
            source: "llm_extract".to_string(),
            announcement_id: "a1".to_string(),
            title: "delist".to_string(),
            url: "https://example".to_string(),
            published_ms: 1,
        }
    }

    #[test]
    fn query_returns_flat_events_across_venues() {
        let mut book = RiskBook::default();
        book.upsert_events(vec![
            sample_event("ICX", "binance-margin", "2026-09-03T03:00:00Z", "delist"),
            sample_event("ICX", "binance-futures", "2026-08-26T09:00:00Z", "delist"),
            sample_event(
                "ICX",
                "binance-margin",
                "2026-08-21T06:00:00Z",
                "disable_margin",
            ),
        ]);
        let margin = book.query(&RiskQuery {
            venue: Some("binance-margin".into()),
            include_past: true,
            ..RiskQuery::default()
        });
        assert_eq!(margin.count, 2);
        assert!(margin
            .items
            .iter()
            .all(|item| item.exchange == "binance" && item.venue == "binance-margin"));
        assert_eq!(margin.items[0].action, "disable_margin");
        assert_eq!(margin.items[0].assets, vec!["ICX"]);
        assert_eq!(margin.items[1].action, "delist");

        let futures = book.query(&RiskQuery {
            venue: Some("binance-futures".into()),
            include_past: true,
            ..RiskQuery::default()
        });
        assert_eq!(futures.count, 1);
        assert_eq!(futures.items[0].venue, "binance-futures");
        assert_eq!(futures.items[0].action, "delist");
        assert_eq!(futures.items[0].utc, "2026-08-26T09:00:00Z");

        let all = book.query(&RiskQuery {
            include_past: true,
            ..RiskQuery::default()
        });
        assert_eq!(all.count, 3);
        assert_eq!(all.items[0].venue, "binance-margin");
        assert_eq!(all.items[1].venue, "binance-futures");
        assert_eq!(all.items[2].venue, "binance-margin");
    }

    #[test]
    fn gate_snapshot_maps_spot_pairs() {
        let snapshot = serde_json::json!({
            "spot_pairs": [{
                "id": "TRC_USDT",
                "trade_status": "tradable",
                "delisting_time": 1788318000
            }],
            "usdt_futures_in_delisting": []
        });
        let events = events_from_gate_snapshot(&snapshot);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].venue, "gate-margin");
        assert_eq!(events[0].symbols, vec!["TRCUSDT"]);
        assert_eq!(events[0].utc, "2026-09-02T03:00:00Z");
    }

    #[test]
    fn replace_source_swaps_official_snapshot() {
        let mut book = RiskBook::default();
        book.replace_source(
            "gate_market",
            events_from_gate_snapshot(&serde_json::json!({
                "spot_pairs": [{"id": "AAA_USDT", "delisting_time": 1788318000}]
            })),
        );
        book.replace_source(
            "gate_market",
            events_from_gate_snapshot(&serde_json::json!({
                "spot_pairs": [{"id": "BBB_USDT", "delisting_time": 1788318000}]
            })),
        );
        let resp = book.query(&RiskQuery {
            venue: Some("gate-margin".into()),
            include_past: true,
            ..RiskQuery::default()
        });
        assert_eq!(resp.count, 1);
        assert_eq!(resp.items[0].exchange, "gate");
        assert_eq!(resp.items[0].venue, "gate-margin");
        assert_eq!(resp.items[0].symbols, vec!["BBBUSDT"]);
    }
}
