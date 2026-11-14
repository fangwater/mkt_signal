//! Durable page events plus a read-only fallback for changes outside config pages.
use anyhow::{ensure, Context, Result};
use mkt_signal::common::{
    delist_accounts::{AccountSpec, RedisSite},
    parameter_audit::{self as audit, Batch, Change, Event, Metadata, Writer},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct SiteState {
    pub cursor: u64,
    initialized: bool,
    baseline: BTreeMap<String, Watched>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Watched {
    metadata: Metadata,
    storage: String,
    value: Value,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct History {
    pub site: String,
    pub event: Event,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub sites: BTreeMap<String, SiteState>,
    pub recent: VecDeque<History>,
}
fn identity(storage: &str, key: &str) -> String {
    format!("{storage}:{key}")
}
fn metadata(family: &str, environment: &str) -> Metadata {
    Metadata {
        family: family.into(),
        environment: environment.into(),
        source: "outside_config_page".into(),
        actor: None,
        source_ip: None,
        session_id: None,
        request_path: None,
    }
}
fn missing(storage: &str) -> Value {
    if storage == "redis" {
        serde_json::json!({"type":"none","value":null})
    } else {
        Value::Null
    }
}
fn unknown(mut event: Event, objects: BTreeMap<String, Change>) -> Event {
    event.id = chrono::Utc::now().timestamp_micros() as u64;
    event.time_us = event.id as i64;
    event.metadata.source = "outside_config_page".into();
    event.metadata.actor = None;
    event.metadata.source_ip = None;
    event.metadata.session_id = None;
    event.metadata.request_path = None;
    event.objects = objects;
    event
}

impl State {
    /// Caller saves this state and the resulting outbox in one atomic transaction.
    pub fn ingest(
        &mut self,
        site: &str,
        batch: Batch,
        snapshot: Option<BTreeMap<String, Watched>>,
    ) -> Result<Vec<History>> {
        let state = self.sites.entry(site.into()).or_default();
        ensure!(
            batch.watermark >= state.cursor,
            "parameter journal sequence moved backwards"
        );
        let mut events = Vec::new();
        for (index, record) in batch.records.iter().enumerate() {
            let mut event = record.event.clone();
            if event.id <= state.cursor {
                continue;
            }
            if record.pending {
                let Some(current) = &snapshot else {
                    break;
                };
                for (key, change) in &mut event.objects {
                    change.after = batch.records[index + 1..]
                        .iter()
                        .filter(|next| next.event.storage == event.storage)
                        .find_map(|next| next.event.objects.get(key).map(|v| v.before.clone()))
                        .or_else(|| {
                            current
                                .get(&identity(&event.storage, key))
                                .map(|v| v.value.clone())
                        })
                        .unwrap_or_else(|| missing(&event.storage));
                }
                event.metadata.source = "recovered_pending_write".into();
            }
            // Preserve external writes observed between two journalled requests.
            let mut gaps = BTreeMap::new();
            for (key, change) in &event.objects {
                if let Some(previous) = state.baseline.get(&identity(&event.storage, key)) {
                    if !audit::equivalent(&previous.value, &change.before) {
                        gaps.insert(
                            key.clone(),
                            Change {
                                before: previous.value.clone(),
                                after: change.before.clone(),
                            },
                        );
                    }
                }
            }
            if state.initialized && !gaps.is_empty() {
                events.push(unknown(event.clone(), gaps));
            }
            for (key, change) in &event.objects {
                state.baseline.insert(
                    identity(&event.storage, key),
                    Watched {
                        metadata: event.metadata.clone(),
                        storage: event.storage.clone(),
                        value: change.after.clone(),
                    },
                );
            }
            state.cursor = event.id;
            event
                .objects
                .retain(|_, v| !audit::equivalent(&v.before, &v.after));
            if !event.objects.is_empty() {
                events.push(event);
            }
        }
        if batch.complete && state.cursor <= batch.watermark {
            // Sequence allocation can precede a crash before a pending file is made.
            if snapshot.is_some() {
                state.cursor = batch.watermark;
            }
        }
        if let Some(mut current) =
            snapshot.filter(|_| batch.complete && state.cursor == batch.watermark)
        {
            let mut groups: BTreeMap<(String, String, String), BTreeMap<String, Change>> =
                BTreeMap::new();
            let mut metadatas = BTreeMap::new();
            // Deleted CTA files remain visible as deletions. Removed Redis model
            // dependencies simply stop being watched.
            for (id, previous) in &state.baseline {
                if previous.storage == "cta" && !current.contains_key(id) {
                    let mut absent = previous.clone();
                    absent.value = Value::Null;
                    current.insert(id.clone(), absent);
                }
            }
            for (id, item) in &current {
                if let Some(previous) = state.baseline.get(id) {
                    if state.initialized && !audit::equivalent(&previous.value, &item.value) {
                        let group = (
                            item.metadata.family.clone(),
                            item.metadata.environment.clone(),
                            item.storage.clone(),
                        );
                        metadatas.insert(
                            group.clone(),
                            metadata(&item.metadata.family, &item.metadata.environment),
                        );
                        groups.entry(group).or_default().insert(
                            id[item.storage.len() + 1..].into(),
                            Change {
                                before: previous.value.clone(),
                                after: item.value.clone(),
                            },
                        );
                    }
                } else if state.initialized && item.storage == "cta" && !item.value.is_null() {
                    let group = (
                        item.metadata.family.clone(),
                        item.metadata.environment.clone(),
                        item.storage.clone(),
                    );
                    metadatas.insert(
                        group.clone(),
                        metadata(&item.metadata.family, &item.metadata.environment),
                    );
                    groups.entry(group).or_default().insert(
                        id[item.storage.len() + 1..].into(),
                        Change {
                            before: Value::Null,
                            after: item.value.clone(),
                        },
                    );
                }
            }
            for (group, objects) in groups {
                let now = chrono::Utc::now().timestamp_micros();
                events.push(Event {
                    id: now as u64,
                    time_us: now,
                    storage: group.2.clone(),
                    writer: Writer {
                        pid: 0,
                        start_ticks: String::new(),
                    },
                    metadata: metadatas.remove(&group).unwrap(),
                    objects,
                });
            }
            state.baseline = current;
            state.initialized = true;
        }
        let history: Vec<_> = events
            .into_iter()
            .map(|event| History {
                site: site.into(),
                event,
            })
            .collect();
        self.recent.extend(history.clone());
        while self.recent.len() > 200 {
            self.recent.pop_front();
        }
        Ok(history)
    }
}

fn leaf_diffs(path: &str, before: &Value, after: &Value, out: &mut Vec<String>) {
    if audit::equivalent(before, after) {
        return;
    }
    if let (Some(a), Some(b)) = (before.as_object(), after.as_object()) {
        for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
            leaf_diffs(
                &format!("{path}.{key}"),
                a.get(key).unwrap_or(&Value::Null),
                b.get(key).unwrap_or(&Value::Null),
                out,
            );
        }
    } else {
        out.push(format!("{path}: {} → {}", before, after));
    }
}
pub fn message(history: &History) -> String {
    let e = &history.event;
    let mut diffs = Vec::new();
    for (key, change) in &e.objects {
        leaf_diffs(key, &change.before, &change.after, &mut diffs);
    }
    let source = match e.metadata.source.as_str() {
        "config_page" => "参数页面",
        "failed_config_request" => "页面请求失败，但检测到实际写入",
        "recovered_pending_write" => "中断写入恢复比对（仅观察，没有恢复参数）",
        _ => "页面外变更，写入者未知",
    };
    let time = chrono::DateTime::from_timestamp_micros(e.time_us)
        .map(|v| v.to_rfc3339())
        .unwrap_or_default();
    let prefix = format!(
        "[参数变更] {} / {}\n站点：{}；时间：{}\n来源：{}\n登录账号：{}；IP：{}；会话：{}\n",
        e.metadata.family,
        e.metadata.environment,
        history.site,
        time,
        source,
        e.metadata.actor.as_deref().unwrap_or("未知"),
        e.metadata.source_ip.as_deref().unwrap_or("未知"),
        e.metadata.session_id.as_deref().unwrap_or("未知")
    );
    let link = format!(
        "\n完整记录： http://13.115.227.29:4191/delist/parameter-changes#{}-{}",
        history.site, e.id
    );
    format!(
        "{}{}{}",
        prefix,
        super::notifications::clip(&diffs.join("\n"), 3100usize.saturating_sub(prefix.len())),
        link
    )
}
fn validate(event: &Event, accounts: &[AccountSpec]) -> Result<()> {
    ensure!(event.objects.len() <= 256, "parameter event too large");
    match event.storage.as_str() {
        "redis" => {
            let account = accounts
                .iter()
                .find(|a| a.slug == event.metadata.environment && a.kind == event.metadata.family)
                .context("unrecognized parameter environment")?;
            ensure!(
                event
                    .objects
                    .keys()
                    .all(|key| super::backup::allowed_parameter_key(account, key)),
                "invalid parameter key scope"
            );
        }
        "cta" => {
            let worker = super::cta_backup::worker(&event.metadata.environment)
                .context("invalid CTA worker")?;
            ensure!(
                event.metadata.family
                    == if worker == super::cta_factors::Worker::Perp {
                        "cta_prep"
                    } else {
                        "cta_spread"
                    },
                "invalid CTA family"
            );
            let prefix = format!("{}/", worker.name());
            ensure!(
                event.objects.keys().all(|key| key
                    .strip_prefix(&prefix)
                    .and_then(|name| name.strip_suffix(".json"))
                    .is_some_and(|name| !name.is_empty()
                        && name.len() <= 128
                        && name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')))),
                "invalid CTA object scope"
            );
        }
        _ => anyhow::bail!("invalid parameter storage"),
    }
    Ok(())
}
async fn journal(site: &str, host: &str, cursor: u64) -> Result<Batch> {
    if site == "jp" {
        let root = audit::default_root()?;
        return tokio::task::spawn_blocking(move || audit::read_batch(&root, cursor)).await?;
    }
    ensure!(
        !host.is_empty()
            && !host.starts_with('-')
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'@')),
        "invalid audit SSH host"
    );
    let mut child = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            host,
            "python3",
            "-",
            "--read",
            &cursor.to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut input = child.stdin.take().context("audit SSH stdin unavailable")?;
    let writer = tokio::spawn(async move {
        input
            .write_all(include_bytes!("../../../scripts/parameter_audit.py"))
            .await
    });
    let mut output = Vec::new();
    child
        .stdout
        .take()
        .context("audit SSH stdout unavailable")?
        .take(33 * 1024 * 1024)
        .read_to_end(&mut output)
        .await?;
    writer.await??;
    ensure!(
        child.wait().await?.success() && output.len() <= 32 * 1024 * 1024,
        "remote parameter journal unavailable"
    );
    let value: Value = serde_json::from_slice(&output)?;
    if value["busy"] == true {
        return Err(audit::Busy.into());
    }
    Ok(serde_json::from_value(value)?)
}
async fn snapshot(
    state: &super::AppState,
    site: &str,
    accounts: &[AccountSpec],
) -> Result<BTreeMap<String, Watched>> {
    let mut current = BTreeMap::new();
    let url = if site == "jp" {
        state.jp_redis.as_str()
    } else {
        state.sg_redis.as_deref().context("SG Redis unavailable")?
    };
    for account in accounts {
        for (key, value) in super::backup::parameter_values(url, account).await? {
            // Shared Redis keys produce one observation for all affected envs.
            let id = identity("redis", &key);
            let item = current.entry(id).or_insert_with(|| Watched {
                metadata: metadata(&account.kind, &account.slug),
                storage: "redis".into(),
                value: audit::redact(&value),
            });
            if item.metadata.environment != account.slug
                && !item
                    .metadata
                    .environment
                    .split(',')
                    .any(|s| s == account.slug)
            {
                item.metadata.environment.push(',');
                item.metadata.environment.push_str(&account.slug);
            }
        }
    }
    if site == "jp" {
        for (worker, env, family) in [
            (
                super::cta_factors::Worker::Perp,
                &state.cta_mapping.factor_env,
                "cta_prep",
            ),
            (
                super::cta_factors::Worker::Spread,
                &state.cta_mapping.spread_factor_env,
                "cta_spread",
            ),
        ] {
            let env = env.clone();
            let values = tokio::task::spawn_blocking(move || {
                super::cta_factors::parameter_values(&env, worker)
            })
            .await??;
            for (key, value) in values {
                current.insert(
                    identity("cta", &key),
                    Watched {
                        metadata: metadata(family, worker.name()),
                        storage: "cta".into(),
                        value: audit::redact(&value),
                    },
                );
            }
        }
    }
    Ok(current)
}
async fn collect(state: &super::AppState, site: &str) -> Result<bool> {
    let notifier = state
        .fr_notifications
        .as_ref()
        .context("parameter notifications disabled")?;
    if !super::nav_accounts_ready(state).await {
        return Ok(false);
    }
    let all = state.accounts.read().await.clone();
    let accounts: Vec<_> = all
        .into_iter()
        .filter(|a| {
            matches!(a.kind.as_str(), "funding_rate" | "market_making")
                && if site == "jp" {
                    a.site == RedisSite::Jp
                } else {
                    a.site == RedisSite::Sg
                }
        })
        .collect();
    if site == "sg" && accounts.is_empty() {
        return Ok(false);
    }
    let cursor = notifier.parameter_cursor(site).await;
    let mut batch = journal(site, &state.backups.sg_ssh_host, cursor).await?;
    for record in &mut batch.records {
        validate(&record.event, &accounts)?;
        for change in record.event.objects.values_mut() {
            change.before = audit::redact(&change.before);
            change.after = audit::redact(&change.after);
        }
    }
    let current = match snapshot(state, site, &accounts).await {
        Ok(current) => current,
        Err(error) => {
            notifier.parameters(site, batch, None).await?;
            return Err(error).context("read-only parameter comparison unavailable");
        }
    };
    let after = journal(site, &state.backups.sg_ssh_host, batch.watermark).await;
    let stable = matches!(&after,Ok(b) if b.watermark==batch.watermark && b.complete);
    if let Err(error) = after {
        if !error.is::<audit::Busy>() {
            return Err(error);
        }
    }
    notifier
        .parameters(site, batch, stable.then_some(current))
        .await?;
    Ok(true)
}
pub async fn run(state: super::AppState, site: &'static str) {
    let mut interval = tokio::time::interval(Duration::from_secs(if site == "jp" { 2 } else { 5 }));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut busy_since: Option<std::time::Instant> = None;
    loop {
        interval.tick().await;
        let result = tokio::time::timeout(Duration::from_secs(20), collect(&state, site))
            .await
            .map_err(anyhow::Error::from)
            .and_then(|v| v);
        if result.as_ref().is_err_and(|e| e.is::<audit::Busy>()) {
            let since = busy_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() < Duration::from_secs(30) {
                continue;
            }
        } else {
            busy_since = None;
        }
        if matches!(&result, Ok(false)) {
            continue;
        }
        if result.is_err() {
            log::warn!("{site} parameter change collection failed");
        }
        if let Some(notifier) = &state.fr_notifications {
            let _ = notifier
                .observe(vec![super::notifications::Observation {
                    key: format!("parameter-monitor/{site}"),
                    title: format!("{site} 参数变更监控"),
                    problem: Some(result.is_err()),
                    detail: if result.is_ok() {
                        "日志和只读参数比对已恢复"
                    } else {
                        "无法完成参数变更采集，将持续重试；详见服务日志"
                    }
                    .into(),
                }])
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(id: u64, a: i64, b: i64) -> Event {
        Event {
            id,
            time_us: 1_700_000_000_000_000,
            storage: "cta".into(),
            writer: Writer {
                pid: 1,
                start_ticks: "1".into(),
            },
            metadata: Metadata {
                source: "config_page".into(),
                ..metadata("cta_prep", "cta-prep-binance-futures")
            },
            objects: BTreeMap::from([(
                "cta-prep-binance-futures/one.json".into(),
                Change {
                    before: serde_json::json!({"amount":a}),
                    after: serde_json::json!({"amount":b}),
                },
            )]),
        }
    }
    fn batch(events: Vec<Event>) -> Batch {
        Batch {
            watermark: events.last().map(|e| e.id).unwrap_or(0),
            complete: true,
            records: events
                .into_iter()
                .map(|event| audit::Record {
                    event,
                    pending: false,
                })
                .collect(),
        }
    }
    fn values(n: i64) -> BTreeMap<String, Watched> {
        BTreeMap::from([(
            "cta:cta-prep-binance-futures/one.json".into(),
            Watched {
                metadata: metadata("cta_prep", "cta-prep-binance-futures"),
                storage: "cta".into(),
                value: serde_json::json!({"amount":n}),
            },
        )])
    }
    #[test]
    fn baseline_silent_roundtrip_events_once_and_restart_dedup() {
        let mut state = State::default();
        assert!(state
            .ingest("jp", batch(vec![]), Some(values(1)))
            .unwrap()
            .is_empty());
        let events = vec![event(1, 1, 2), event(2, 2, 1)];
        assert_eq!(
            state
                .ingest("jp", batch(events.clone()), Some(values(1)))
                .unwrap()
                .len(),
            2
        );
        let mut state: State =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert!(state
            .ingest("jp", batch(events), Some(values(1)))
            .unwrap()
            .is_empty());
        assert_eq!(state.sites["jp"].cursor, 2);
    }
    #[test]
    fn external_changes_unknown_noops_silent_and_journal_suppresses_fallback() {
        let mut state = State::default();
        state.ingest("jp", batch(vec![]), Some(values(1))).unwrap();
        let changed = state.ingest("jp", batch(vec![]), Some(values(4))).unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].event.metadata.source, "outside_config_page");
        assert!(changed[0].event.metadata.actor.is_none());
        let mut noop = event(1, 4, 4);
        noop.objects.clear();
        assert!(state
            .ingest("jp", batch(vec![noop]), Some(values(4)))
            .unwrap()
            .is_empty());
        assert_eq!(
            state
                .ingest("jp", batch(vec![event(2, 4, 5)]), Some(values(5)))
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn orphan_pending_observes_actual_value_and_never_restores_parameters() {
        let mut state = State::default();
        state.ingest("jp", batch(vec![]), Some(values(1))).unwrap();
        let pending = Batch {
            watermark: 1,
            complete: true,
            records: vec![audit::Record {
                event: event(1, 1, 1),
                pending: true,
            }],
        };
        assert!(state
            .ingest("jp", pending.clone(), None)
            .unwrap()
            .is_empty());
        assert_eq!(state.sites["jp"].cursor, 0);
        let result = state.ingest("jp", pending, Some(values(2))).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].event.metadata.source, "recovered_pending_write");
        assert_eq!(
            result[0].event.objects.values().next().unwrap().after,
            serde_json::json!({"amount":2})
        );
    }
    #[test]
    fn concurrent_backlog_defers_fallback_and_sequence_reset_is_reported() {
        let mut state = State::default();
        state.ingest("jp", batch(vec![]), Some(values(1))).unwrap();
        let mut busy = batch(vec![event(1, 1, 2)]);
        busy.complete = false;
        busy.watermark = 3;
        assert_eq!(state.ingest("jp", busy, Some(values(5))).unwrap().len(), 1);
        assert_eq!(state.sites["jp"].cursor, 1);
        assert!(state.ingest("jp", batch(vec![]), None).is_err());
    }
    #[test]
    fn scope_validation_rejects_paths_and_messages_retain_history_link() {
        let mut e = event(1, 1, 2);
        assert!(validate(&e, &[]).is_ok());
        let value = e.objects.values().next().unwrap().clone();
        e.objects
            .insert("cta-prep-binance-futures/../../env.sh".into(), value);
        assert!(validate(&e, &[]).is_err());
        let msg = message(&History {
            site: "jp".into(),
            event: event(1, 1, 2),
        });
        assert!(msg.contains("1 → 2"));
        assert!(msg.contains("parameter-changes#jp-1"));
        assert!(msg.len() < 3600);
    }
}
