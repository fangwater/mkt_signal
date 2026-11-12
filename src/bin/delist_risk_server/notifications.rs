//! Independent FR DingTalk outbox. Successful alerts are never repeated; only
//! confirmed recovery rearms an issue. State and pending deliveries survive restart.
//! Ready risk changes across accounts share one bounded message and acknowledgement.

use anyhow::{ensure, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

pub struct DingTalk {
    client: reqwest::Client,
    url: reqwest::Url,
    secret: Option<String>,
}

impl DingTalk {
    #[cfg(test)]
    pub(super) fn test_sender(url: reqwest::Url) -> Self {
        Self {
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            url,
            secret: None,
        }
    }
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("FR_DINGTALK_WEBHOOK_URL")
            .context("FR_DINGTALK_WEBHOOK_URL is required")?;
        let url =
            reqwest::Url::parse(&raw).map_err(|_| anyhow::anyhow!("invalid FR DingTalk URL"))?;
        ensure!(
            url.scheme() == "https"
                && url.host_str() == Some("oapi.dingtalk.com")
                && url.path() == "/robot/send"
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && url
                    .query_pairs()
                    .any(|(k, v)| k == "access_token" && !v.is_empty()),
            "invalid FR DingTalk destination"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            url,
            secret: std::env::var("FR_DINGTALK_SECRET")
                .ok()
                .filter(|s| !s.trim().is_empty()),
        })
    }

    fn signed_url(&self, now: i64) -> Result<reqwest::Url> {
        let mut url = self.url.clone();
        if let Some(secret) = &self.secret {
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
            mac.update(format!("{now}\n{secret}").as_bytes());
            let sign =
                base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            url.query_pairs_mut()
                .append_pair("timestamp", &now.to_string())
                .append_pair("sign", &sign);
        }
        Ok(url)
    }

    pub async fn send(&self, body: &str) -> Result<()> {
        let content = format!("[FR][jp-meta-elvpn]\n{}", clip(body, 3700));
        let response = self.client.post(self.signed_url(now_ms())?)
            .json(&serde_json::json!({"msgtype":"text","text":{"content":content},"at":{"isAtAll":false}}))
            .send().await.map_err(|_| anyhow::anyhow!("FR DingTalk transport failed"))?;
        ensure!(
            response.status().is_success(),
            "FR DingTalk HTTP {}",
            response.status().as_u16()
        );
        let result: serde_json::Value = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("invalid FR DingTalk response"))?;
        // Neither reqwest errors nor response bodies may expose the webhook/token.
        ensure!(
            result["errcode"].as_i64() == Some(0),
            "FR DingTalk rejected message (code={})",
            result["errcode"].as_i64().unwrap_or(-1)
        );
        Ok(())
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn fr_source(source: &str, active_fr: &BTreeSet<String>) -> bool {
    if source.starts_with("okex_")
        || source.starts_with("schedule:okex-")
        || matches!(source, "llm" | "fr_unimmr_close_list")
        || source.starts_with("cta_")
        || source.contains("_cta_")
        || source.starts_with("redis_mm_backup_")
        || source.starts_with("cos_mm_")
    {
        return false;
    }
    if source.contains("_fr_arb") {
        return active_fr.iter().any(|slug| source.ends_with(slug));
    }
    true
}

pub fn cta_backup_observation(
    source: &mkt_signal::common::delist_store::SourceStatus,
) -> Option<Observation> {
    for (prefix, stage) in [
        ("cta_backup_quality_", "归档完整性"),
        ("cta_backup_", "本地归档"),
        ("cos_cta_backup_", "COS 上传及回读校验"),
        ("cos_cta_retention_", "COS 保留清理"),
    ] {
        let Some(name) = source.source.strip_prefix(prefix) else {
            continue;
        };
        let worker = super::cta_backup::worker(name)?;
        let label = if worker == super::cta_factors::Worker::Perp {
            "CTA Prep"
        } else {
            "CTA Spread"
        };
        return Some(Observation {
            key: format!("source/{}", source.source),
            title: format!("{label} 备份：{stage}"),
            problem: Some(!source.ok),
            // Backup errors can contain connection strings or private paths.
            // Keep the robot message independent of raw provider errors.
            detail: if source.ok {
                format!("{name} 的{stage}已恢复；本通知仅表示该环节检查通过。")
            } else {
                format!("{name} 的{stage}失败，系统将自动重试。详情见 Delist 状态页与服务日志。")
            },
        });
    }
    None
}

pub fn mm_backup_observation(
    source: &mkt_signal::common::delist_store::SourceStatus,
) -> Option<Observation> {
    for (prefix, stage) in [
        ("redis_mm_backup_", "本地参数归档"),
        ("cos_mm_backup_", "COS 上传及回读校验"),
        ("cos_mm_retention_", "COS 保留清理"),
    ] {
        let Some(name) = source.source.strip_prefix(prefix) else {
            continue;
        };
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return None;
        }
        return Some(Observation {
            key: format!("source/{}", source.source),
            title: format!("MM 参数备份 {name}：{stage}"),
            problem: Some(!source.ok),
            detail: if source.ok {
                format!("{stage}已恢复；本通知仅表示该环节检查通过。")
            } else {
                format!("{stage}失败，系统将自动重试。详情见 Delist 状态页与服务日志。")
            },
        });
    }
    None
}

pub fn clip(s: &str, bytes: usize) -> String {
    if s.len() <= bytes {
        return s.to_owned();
    }
    let mut end = bytes.saturating_sub(24);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n…详情见 Delist", &s[..end])
}

#[derive(Clone, Debug)]
pub struct Observation {
    pub key: String,
    pub title: String,
    /// Some(true)=problem; Some(false)=confirmed healthy; None=unknown.
    pub problem: Option<bool>,
    pub detail: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Issue {
    title: String,
    detail: String,
    observed: Option<bool>,
    notified: bool,
    pending: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Delivery {
    id: u64,
    kind: String,
    body: String,
    created_ms: i64,
    priority: u8,
    issue: Option<String>,
    recovery: bool,
    attempts: u32,
    next_attempt_ms: i64,
}

struct DeliveryBatch {
    deliveries: Vec<Delivery>,
    body: String,
}

fn batch_message(deliveries: &[Delivery]) -> String {
    if deliveries.len() == 1 && deliveries[0].issue.is_none() {
        return deliveries[0].body.clone();
    }
    let recoveries = deliveries.iter().filter(|d| d.recovery).count();
    let mut message = format!(
        "[风险汇总] 新增 {} 项 · 恢复 {} 项",
        deliveries.len() - recoveries,
        recoveries
    );
    for (recovery, label) in [(false, "新增报警"), (true, "恢复")] {
        let rows: Vec<_> = deliveries
            .iter()
            .filter(|d| d.recovery == recovery)
            .collect();
        if rows.is_empty() {
            continue;
        }
        message.push_str(&format!("\n\n{label}："));
        for delivery in rows {
            let mut lines = delivery.body.lines();
            let title = lines
                .next()
                .unwrap_or_default()
                .trim_start_matches("[告警] ")
                .trim_start_matches("[恢复] ");
            message.push_str(&format!("\n• {}", clip(title, 240)));
            if !recovery {
                message.push_str(&format!(
                    "\n  {}",
                    clip(lines.next().unwrap_or_default(), 180)
                ));
            }
        }
    }
    let observed = deliveries.iter().map(|d| d.created_ms).max().unwrap_or(0);
    message.push_str(&format!(
        "\n\n检查时间：{}\n详情：http://13.115.227.29:4191/delist/",
        utc(observed)
    ));
    message
}

#[derive(Clone, Serialize, Deserialize)]
struct JournalEntry {
    ts_ms: i64,
    kind: String,
    body: String,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Book {
    next_id: u64,
    issues: BTreeMap<String, Issue>,
    outbox: BTreeMap<u64, Delivery>,
    journal: VecDeque<JournalEntry>,
    delivered: u64,
    last_sent_ms: Option<i64>,
    last_error: Option<String>,
    last_heartbeat_ms: Option<i64>,
    #[serde(default)]
    listing_catalog: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    parameters: super::parameter_changes::State,
    #[serde(skip)]
    sending: BTreeSet<u64>,
    #[serde(skip)]
    needs_save: bool,
}

impl Book {
    fn listing_changes(
        &mut self,
        rows: &[mkt_signal::common::exchange_info::ListingSnapshotRow],
        now: i64,
    ) -> Result<()> {
        let mut current: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for row in rows {
            let symbols = current.entry(row.venue.clone()).or_default();
            if row.listed {
                symbols.insert(row.symbol.clone());
            }
        }
        for (venue, symbols) in current {
            if let Some(previous) = self.listing_catalog.get(&venue) {
                let added: Vec<_> = symbols.difference(previous).cloned().collect();
                let removed: Vec<_> = previous.difference(&symbols).cloned().collect();
                if !added.is_empty() || !removed.is_empty() {
                    let body = format!("[上下架目录变化] {venue}\n新增/恢复可交易 {} 个：{}\n移除/停止可交易 {} 个：{}\n依据两次完整官方产品目录比较；没有自动新增策略名单。", added.len(), added.iter().take(50).cloned().collect::<Vec<_>>().join(", "), removed.len(), removed.iter().take(50).cloned().collect::<Vec<_>>().join(", "));
                    self.queue("listing", body, 1, None, false, now)?;
                }
            }
            // The first complete catalog is a baseline, not thousands of listings.
            if self.listing_catalog.get(&venue) != Some(&symbols) {
                self.needs_save = true;
            }
            self.listing_catalog.insert(venue, symbols);
        }
        // A missing venue is unknown; it must not manufacture mass delistings.
        Ok(())
    }
    fn queue(
        &mut self,
        kind: &str,
        body: String,
        priority: u8,
        issue: Option<String>,
        recovery: bool,
        now: i64,
    ) -> Result<u64> {
        ensure!(self.outbox.len() < 10_000, "FR notification outbox is full");
        self.next_id = self
            .next_id
            .checked_add(1)
            .context("FR notification ID overflow")?;
        let id = self.next_id;
        let body = clip(&body, 3600);
        self.journal.push_back(JournalEntry {
            ts_ms: now,
            kind: kind.into(),
            body: body.clone(),
        });
        while self.journal.len() > 2000 {
            self.journal.pop_front();
        }
        self.outbox.insert(
            id,
            Delivery {
                id,
                kind: kind.into(),
                body,
                created_ms: now,
                priority,
                issue,
                recovery,
                attempts: 0,
                next_attempt_ms: now,
            },
        );
        self.needs_save = true;
        Ok(id)
    }

    fn reconcile(&mut self, key: &str, now: i64) -> Result<()> {
        let Some(issue) = self.issues.get(key).cloned() else {
            return Ok(());
        };
        if let Some(id) = issue.pending {
            // Cancel an unsent recovery if the issue recurs. An in-flight
            // recovery is acknowledged first, then the recurrence gets an alert.
            if issue.observed == Some(true)
                && !self.sending.contains(&id)
                && self.outbox.get(&id).is_some_and(|d| d.recovery)
            {
                self.outbox.remove(&id);
                self.issues.get_mut(key).unwrap().pending = None;
            } else {
                return Ok(());
            }
        }
        let recovery = match issue.observed {
            Some(true) if !issue.notified => false,
            Some(false) if issue.notified => true,
            _ => return Ok(()),
        };
        let kind = if recovery { "recover" } else { "alert" };
        let body = format!(
            "[{}] {}\n{}\n检查时间：{}\nhttp://13.115.227.29:4191/delist/",
            if recovery { "恢复" } else { "告警" },
            issue.title,
            issue.detail,
            utc(now)
        );
        let id = self.queue(kind, body, 0, Some(key.into()), recovery, now)?;
        self.issues.get_mut(key).unwrap().pending = Some(id);
        Ok(())
    }

    fn observe(&mut self, observations: Vec<Observation>, now: i64) -> Result<()> {
        for observation in observations {
            let key = observation.key;
            if observation.problem != Some(true) && !self.issues.contains_key(&key) {
                continue;
            }
            let issue = self.issues.entry(key.clone()).or_default();
            self.needs_save |= issue.title != observation.title
                || issue.observed != observation.problem
                || observation.problem.is_some() && issue.detail != clip(&observation.detail, 1600);
            issue.title = observation.title;
            if observation.problem.is_some() {
                issue.detail = clip(&observation.detail, 1600);
            }
            issue.observed = observation.problem;
            self.reconcile(&key, now)?;
        }
        Ok(())
    }

    fn claim(&mut self, now: i64) -> Option<DeliveryBatch> {
        if !self.sending.is_empty() {
            return None;
        }
        let mut ready: Vec<_> = self
            .outbox
            .values()
            .filter(|d| d.next_attempt_ms <= now)
            .collect();
        ready.sort_by_key(|d| (d.priority, d.id));
        let first = *ready.first()?;
        let mut deliveries = vec![first.clone()];
        let mut body = batch_message(&deliveries);
        if first.issue.is_some() {
            for next in ready
                .into_iter()
                .skip(1)
                .filter(|d| d.issue.is_some() && d.priority == first.priority)
            {
                deliveries.push(next.clone());
                let candidate = batch_message(&deliveries);
                if candidate.len() > 3600 {
                    deliveries.pop();
                    break;
                }
                body = candidate;
            }
        }
        self.sending.extend(deliveries.iter().map(|d| d.id));
        Some(DeliveryBatch { deliveries, body })
    }

    fn finish(&mut self, batch: &DeliveryBatch, success: bool, now: i64) -> Result<()> {
        self.sending.clear();
        self.needs_save = true;
        if success {
            self.delivered += 1;
            self.last_sent_ms = Some(now);
            self.last_error = None;
            for delivery in &batch.deliveries {
                self.outbox.remove(&delivery.id);
                if let Some(key) = &delivery.issue {
                    if let Some(issue) = self.issues.get_mut(key) {
                        issue.notified = !delivery.recovery;
                        issue.pending = None;
                    }
                    self.reconcile(key, now)?;
                }
            }
        } else {
            for delivery in &batch.deliveries {
                if let Some(pending) = self.outbox.get_mut(&delivery.id) {
                    pending.attempts = pending.attempts.saturating_add(1);
                    let backoff = 5_000_i64
                        .saturating_mul(1_i64 << pending.attempts.min(6))
                        .min(300_000);
                    pending.next_attempt_ms = now.saturating_add(backoff);
                }
            }
            self.last_error = Some("钉钉发送失败，待重试；尚未确认送达".into());
        }
        Ok(())
    }
}

pub struct Notifications {
    path: PathBuf,
    book: Mutex<Book>,
    sender: DingTalk,
    wake: Notify,
}

impl Notifications {
    pub async fn open(path: PathBuf, sender: DingTalk) -> Result<Arc<Self>> {
        let book = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .context("read FR notification state; refusing to discard delivery history")?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Book::default(),
            Err(err) => return Err(err).context("read FR notification state"),
        };
        Ok(Arc::new(Self {
            path,
            book: Mutex::new(book),
            sender,
            wake: Notify::new(),
        }))
    }

    async fn save(&self, book: &mut Book) -> Result<()> {
        if !book.needs_save {
            return Ok(());
        }
        use tokio::io::AsyncWriteExt;
        let bytes = serde_json::to_vec(book)?;
        let temporary = self.path.with_extension("json.tmp");
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temporary)
            .await
            .context("open private FR notification state")?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(temporary, &self.path)
            .await
            .context("save FR notification state")?;
        std::fs::File::open(
            self.path
                .parent()
                .context("notification parent unavailable")?,
        )?
        .sync_all()?;
        book.needs_save = false;
        Ok(())
    }

    pub async fn observe(&self, observations: Vec<Observation>) -> Result<()> {
        let mut book = self.book.lock().await;
        book.observe(observations, now_ms())?;
        self.save(&mut book).await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn event(&self, kind: &str, message: String) -> Result<()> {
        let mut book = self.book.lock().await;
        book.queue(
            kind,
            message,
            if kind == "operation" { 1 } else { 2 },
            None,
            false,
            now_ms(),
        )?;
        self.save(&mut book).await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn parameter_cursor(&self, site: &str) -> u64 {
        self.book
            .lock()
            .await
            .parameters
            .sites
            .get(site)
            .map(|s| s.cursor)
            .unwrap_or(0)
    }

    pub async fn parameters(
        &self,
        site: &str,
        batch: mkt_signal::common::parameter_audit::Batch,
        snapshot: Option<BTreeMap<String, super::parameter_changes::Watched>>,
    ) -> Result<()> {
        use std::os::unix::fs::DirBuilderExt;
        ensure!(matches!(site, "jp" | "sg"), "invalid audit site");
        let mut current = self.book.lock().await;
        let mut next = current.clone();
        let history = next.parameters.ingest(site, batch, snapshot)?;
        if history.is_empty()
            && serde_json::to_vec(&next.parameters)? == serde_json::to_vec(&current.parameters)?
        {
            return Ok(());
        }
        let root = self
            .path
            .parent()
            .context("notification parent unavailable")?
            .join("parameter_changes")
            .join(site);
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)?;
        for entry in &history {
            let path = root.join(format!("{:020}.json", entry.event.id));
            mkt_signal::common::parameter_audit::atomic(&path, entry)?;
            next.queue(
                "parameter_change",
                super::parameter_changes::message(entry),
                1,
                None,
                false,
                now_ms(),
            )?;
        }
        next.needs_save = true;
        self.save(&mut next).await?;
        *current = next;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn parameter_history(&self) -> serde_json::Value {
        let book = self.book.lock().await;
        serde_json::json!({"sites":book.parameters.sites.keys().map(|site| (site,book.parameters.sites[site].cursor)).collect::<BTreeMap<_,_>>(),"changes":book.parameters.recent})
    }

    pub async fn parameter_record(
        &self,
        site: &str,
        id: u64,
    ) -> Result<super::parameter_changes::History> {
        ensure!(matches!(site, "jp" | "sg"), "invalid audit site");
        let path = self
            .path
            .parent()
            .context("notification parent unavailable")?
            .join("parameter_changes")
            .join(site)
            .join(format!("{id:020}.json"));
        ensure!(
            tokio::fs::metadata(&path).await?.len()
                <= mkt_signal::common::parameter_audit::MAX_RECORD,
            "audit record too large"
        );
        Ok(serde_json::from_slice(&tokio::fs::read(path).await?)?)
    }

    pub async fn listings(
        &self,
        rows: &[mkt_signal::common::exchange_info::ListingSnapshotRow],
    ) -> Result<()> {
        let mut book = self.book.lock().await;
        book.listing_changes(rows, now_ms())?;
        self.save(&mut book).await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn exposure_observations(
        &self,
        report: &super::exposure::Report,
    ) -> Vec<Observation> {
        let mut observations = exposure_observations(report);
        let book = self.book.lock().await;
        for account in report
            .accounts
            .iter()
            .filter(|a| a.complete && a.data_status == "fresh")
        {
            for (key, issue) in book.issues.iter() {
                let Some(asset) = key
                    .strip_prefix(&format!("exposure/{}/", account.slug))
                    .or_else(|| key.strip_prefix(&format!("exposure_limit/{}/", account.slug)))
                else {
                    continue;
                };
                if asset != "total" && !account.rows.iter().any(|row| row.asset == asset) {
                    observations.push(Observation {
                        key: key.clone(),
                        title: issue.title.clone(),
                        problem: Some(false),
                        detail: "完整的新鲜账户快照已不再持有该资产".into(),
                    });
                }
            }
        }
        observations
    }

    pub async fn heartbeat(&self, summary: &str) -> Result<()> {
        let now = now_ms();
        let mut book = self.book.lock().await;
        if book
            .last_heartbeat_ms
            .is_some_and(|t| now.saturating_sub(t) < 23 * 3_600_000)
        {
            return Ok(());
        }
        let since = book.last_heartbeat_ms.unwrap_or(now - 86_400_000);
        let recent: Vec<_> = book.journal.iter().filter(|e| e.ts_ms > since).collect();
        let count = |kind: &str| recent.iter().filter(|e| e.kind == kind).count();
        let operations = recent
            .iter()
            .filter(|e| e.kind == "operation")
            .rev()
            .take(4)
            .map(|e| clip(&e.body, 300))
            .collect::<Vec<_>>()
            .join("\n");
        let listings = recent
            .iter()
            .filter(|e| e.kind == "listing")
            .rev()
            .take(4)
            .map(|e| clip(&e.body, 300))
            .collect::<Vec<_>>()
            .join("\n");
        let message = format!("[心跳] 公告扫描完成\n时间：{}\n{}\n日志更新：Luna成功 {} 次，失败 {} 次；首次告警 {} 条，恢复 {} 条\n上下架目录变化 {} 批{}\n已记录实际操作 {} 次{}\n没有新公告时复用结果，不额外调用 Luna。\nhttp://13.115.227.29:4191/delist/", utc(now), summary, count("luna_ok"), count("luna_failed"), count("alert"), count("recover"), count("listing"), if listings.is_empty() { String::new() } else { format!("\n{listings}") }, count("operation"), if operations.is_empty() { String::new() } else { format!("\n{operations}") });
        book.queue("heartbeat", message, 3, None, false, now)?;
        book.last_heartbeat_ms = Some(now);
        self.save(&mut book).await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn status(&self) -> serde_json::Value {
        let book = self.book.lock().await;
        let luna = book
            .journal
            .iter()
            .rev()
            .find(|e| matches!(e.kind.as_str(), "luna_ok" | "luna_failed"));
        serde_json::json!({"enabled":true,"queued":book.outbox.len(),"active_issues":book.issues.values().filter(|i| i.notified).count(),"delivered":book.delivered,"last_sent_ms":book.last_sent_ms,"last_error":book.last_error,"last_heartbeat_ms":book.last_heartbeat_ms,"last_luna_ms":luna.map(|e|e.ts_ms),"last_luna_ok":luna.map(|e|e.kind == "luna_ok")})
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            let delivery = {
                let mut book = self.book.lock().await;
                // Flush all observations/acknowledgements before another send.
                // A disk failure must not turn an accepted message into a retry.
                if let Err(err) = self.save(&mut book).await {
                    log::warn!("FR notification persistence unavailable: {err}");
                    drop(book);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                book.claim(now_ms())
            };
            if let Some(delivery) = delivery {
                let result = self.sender.send(&delivery.body).await;
                if let Err(err) = &result {
                    log::warn!("FR DingTalk delivery failed: {err}");
                }
                let mut book = self.book.lock().await;
                if let Err(err) = book.finish(&delivery, result.is_ok(), now_ms()) {
                    log::warn!("FR notification acknowledge failed: {err}");
                }
                if let Err(err) = self.save(&mut book).await {
                    // Stop delivery until the acknowledged state can be saved;
                    // never silently discard pending work after a disk failure.
                    log::warn!("FR notification state save failed: {err}");
                }
                drop(book);
                tokio::time::sleep(Duration::from_secs(4)).await;
            } else {
                tokio::select! { _ = self.wake.notified() => {}, _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
            }
        }
    }
}

pub fn exposure_observations(report: &super::exposure::Report) -> Vec<Observation> {
    let mut observations = vec![Observation {
        key: "catalog".into(),
        title: "FR 账户目录".into(),
        problem: Some(!report.catalog_current),
        detail: if report.catalog_current {
            format!("当前覆盖 {} 个 FR", report.accounts.len())
        } else {
            "目录不可用，不能确认完整覆盖".into()
        },
    }];
    for account in &report.accounts {
        let fresh = matches!(account.data_status, "fresh" | "partial") && report.catalog_current;
        observations.push(Observation {
            key: format!("data/{}", account.slug),
            title: format!("{} 持仓数据", account.slug),
            problem: if account.complete {
                Some(false)
            } else if account.data_incomplete_confirmed {
                Some(true)
            } else {
                None
            },
            detail: if account.complete {
                "敞口与账户监控快照已完整恢复".into()
            } else {
                format!("{}；已定价敞口合计仅为下界", account.errors.join("；"))
            },
        });
        for row in &account.rows {
            if row.ignored_spot_tail {
                continue;
            }
            let key = format!("asset:{}", row.asset);
            let breach = account.breaches.get(&key);
            let hard = breach.is_some_and(|b| b.kind == super::exposure::BreachKind::HardLimit);
            let problem = if !fresh {
                None
            } else if hard {
                // A hard-limit incident has its own identity so an earlier maker
                // warning cannot deduplicate away the escalation. Do not fake recovery.
                None
            } else if breach.is_some_and(|b| b.confirmed) {
                Some(true)
            } else if breach.is_none() && row.assessable() {
                Some(false)
            } else {
                None
            };
            observations.push(Observation {
                key: format!("exposure_limit/{}/{}", account.slug, row.asset),
                title: format!(
                    "{} {} 超出方向 maker 区间并达到限仓线",
                    account.slug, row.asset
                ),
                problem: if !fresh || !row.assessable() {
                    None
                } else {
                    Some(hard)
                },
                detail: format!(
                    "实际净敞口 {} USDT；交易限仓线 {} USDT；方向 maker 正常区间 {} USDT；{}",
                    row.uncovered_usdt
                        .map(|v| format!("{v:+.2}"))
                        .unwrap_or_else(|| "未知".into()),
                    row.limits
                        .as_ref()
                        .map(|l| format!("{:.2}", l.symbol_limit_usdt))
                        .unwrap_or_else(|| "未知".into()),
                    row.normal_buffer_usdt
                        .map(|v| format!("{v:.2}"))
                        .unwrap_or_else(|| "未设有限区间".into()),
                    if hard {
                        "超出正常区间并达到限仓线，首次观察即报警"
                    } else {
                        "未触发单币立即报警条件"
                    },
                ),
            });
            observations.push(Observation {
                key: format!("exposure/{}/{}", account.slug, row.asset),
                title: format!("{} {} 实际净敞口", account.slug, row.asset),
                problem,
                detail: format!(
                    "{}；实际净敞口 {} USDT；交易限仓线 {}，正常 maker 区间 {} USDT；实际净量 {:.8}，策略对冲计划净量 {}（仅参考，不抵扣风险）",
                    breach.map(|b| b.kind.label()).unwrap_or("未触发异常条件"),
                    row.uncovered_usdt
                        .map(|v| format!("{v:+.2}"))
                        .unwrap_or_else(|| "未知".into()),
                    row.limits.as_ref().map(|l| if l.symbol_limit_usdt > 0.0 { format!("{:.2} USDT", l.symbol_limit_usdt) } else { "未启用".into() }).unwrap_or_else(|| "未知".into()),
                    row.normal_buffer_usdt.map(|v| format!("{v:.2}")).unwrap_or_else(|| "未知".into()),
                    row.net_qty,
                    row.hedge_plan_qty
                        .map(|v| format!("{v:.8}"))
                        .unwrap_or_else(|| "未知".into()),
                ),
            });
        }
        observations.push(Observation {
            key: format!("exposure/{}/total", account.slug),
            title: format!("{} 实际净敞口合计", account.slug),
            problem: account.risk_abs_usdt.filter(|_| fresh).and_then(|value| {
                if account.breaches.get("total").is_some_and(|b| b.confirmed) {
                    Some(true)
                } else if account.complete
                    && account.limit_usdt.is_some_and(|l| l == 0.0 || value < l)
                {
                    Some(false)
                } else {
                    None
                }
            }),
            detail: format!(
                "实际净敞口逐币绝对值合计 {:.2} USDT；策略账户硬限额 {}{}",
                account.risk_abs_usdt.unwrap_or(0.0),
                account
                    .limit_usdt
                    .map(|l| if l > 0.0 {
                        format!("{l:.2} USDT")
                    } else {
                        "未启用".into()
                    })
                    .unwrap_or_else(|| "未知".into()),
                if account.complete {
                    ""
                } else {
                    "；缺价/缺失持仓未计入，只是下界"
                }
            ),
        });
    }
    observations
}

pub fn unimmr_observation(slug: &str, risk: Option<&serde_json::Value>) -> Observation {
    let assessed = (|| -> Option<(Option<bool>, String)> {
        let risk = risk?;
        let recover = risk["unimmr_force_close_recover_line"]
            .as_f64()
            .filter(|v| v.is_finite())?;
        let force = risk["unimmr_force_close_line"]
            .as_f64()
            .filter(|v| v.is_finite())?;
        let accounts = risk["account_risks"].as_array().filter(|a| !a.is_empty())?;
        let mut bad = false;
        let mut healthy = true;
        let mut values = Vec::new();
        for account in accounts {
            let state = account["state"].as_str()?;
            if !matches!(
                state,
                "free_trade" | "warning" | "reduce_only" | "liquidation"
            ) {
                return None;
            }
            let ratio = account["margin_ratio"].as_f64().filter(|v| v.is_finite())?;
            bad |= state != "free_trade" || force > 0.0 && ratio <= force;
            healthy &= state == "free_trade" && (force <= 0.0 || ratio >= recover.max(force));
            values.push(format!("UniMMR={ratio:.4} ({state})"));
        }
        Some((
            if bad {
                Some(true)
            } else if healthy {
                Some(false)
            } else {
                None
            },
            format!(
                "{}；强制平仓线 {force:.4}，强平恢复线 {recover:.4}；算法限仓不作为风险报警",
                values.join("；")
            ),
        ))
    })();
    Observation {
        key: format!("unimmr/{slug}"),
        title: format!("{slug} UniMMR"),
        problem: assessed.as_ref().and_then(|x| x.0),
        detail: assessed
            .map(|x| x.1)
            .unwrap_or_else(|| "账户风险数据未知，不确认恢复".into()),
    }
}

fn utc(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_default()
}

pub fn luna_message(
    input: &mkt_signal::common::announcement_llm::LlmExtractInput,
    value: Option<&serde_json::Value>,
    saved: bool,
) -> String {
    let result = if let Some(value) = value {
        let actions = value["actions"].as_array().cloned().unwrap_or_default();
        if actions.is_empty() {
            "结果：没有识别出下架/停止开仓事项；上架变化以官方完整产品目录为准".into()
        } else {
            format!(
                "结果：\n{}",
                actions
                    .iter()
                    .take(12)
                    .map(|a| format!(
                        "{} | {} | {} | 资产 {} | 币对 {}",
                        a["venue"].as_str().unwrap_or("未知"),
                        a["action"].as_str().unwrap_or("未知"),
                        a["utc"].as_str().unwrap_or("时间未知"),
                        a["assets"]
                            .as_array()
                            .map(|x| x
                                .iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(","))
                            .unwrap_or_default(),
                        a["symbols"]
                            .as_array()
                            .map(|x| x
                                .iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(","))
                            .unwrap_or_default()
                    ))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
    } else {
        "结果：Luna处理失败；具体错误见 Delist 状态页和服务日志".into()
    };
    format!(
        "[公告处理] Luna {}\n交易所：{}\n公告：{}\nID：{}\n{}\n实际操作：{}\n{}",
        if value.is_some() { "完成" } else { "失败" },
        input.exchange,
        input.title,
        input.id,
        result,
        if saved {
            "已更新并保存风险簿；后续实际名单/清仓操作另行记录"
        } else {
            "本次没有确认风险簿更新成功"
        },
        input.url
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(bad: Option<bool>) -> Vec<Observation> {
        vec![Observation {
            key: "risk/a".into(),
            title: "风险".into(),
            problem: bad,
            detail: "数值".into(),
        }]
    }
    fn deliver(book: &mut Book, now: i64) -> Delivery {
        let mut d = book.claim(now).unwrap();
        book.finish(&d, true, now).unwrap();
        assert_eq!(d.deliveries.len(), 1);
        d.deliveries.remove(0)
    }

    fn account_observations(bad: Option<bool>) -> Vec<Observation> {
        (1..=11)
            .map(|i| Observation {
                key: format!("exposure/test_fr_arb{i:02}/ON"),
                title: format!("test_fr_arb{i:02} ON 实际净敞口"),
                problem: bad,
                detail: "实际净敞口 1600 USDT；正常区间 1500 USDT".into(),
            })
            .collect()
    }

    #[tokio::test]
    async fn eleven_accounts_send_one_alert_and_one_recovery_message() {
        use axum::{routing::post, Json, Router};
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/notify",
            post(move |Json(payload): Json<serde_json::Value>| {
                let tx = tx.clone();
                async move {
                    tx.send(payload["text"]["content"].as_str().unwrap().to_owned())
                        .unwrap();
                    Json(serde_json::json!({"errcode":0}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("fr-batch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let notifier = Notifications::open(
            dir.join("state.json"),
            DingTalk::test_sender(
                reqwest::Url::parse(&format!("http://{address}/notify")).unwrap(),
            ),
        )
        .await
        .unwrap();
        notifier
            .observe(account_observations(Some(true)))
            .await
            .unwrap();
        let worker = tokio::spawn(notifier.clone().run());
        for (count, expected) in [(1, "新增 11 项 · 恢复 0 项"), (2, "新增 0 项 · 恢复 11 项")]
        {
            let message = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(message.contains(expected), "{message}");
            assert!(message.len() <= 3610);
            for i in 1..=11 {
                assert!(message.contains(&format!("test_fr_arb{i:02}")));
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if notifier.status().await["delivered"] == count {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(notifier.status().await["queued"], 0);
            if count == 1 {
                notifier
                    .observe(account_observations(Some(false)))
                    .await
                    .unwrap();
            }
        }
        assert!(rx.try_recv().is_err());
        worker.abort();
        server.abort();
        let restored: Book =
            serde_json::from_slice(&std::fs::read(dir.join("state.json")).unwrap()).unwrap();
        assert_eq!(restored.delivered, 2);
        assert!(restored.outbox.is_empty());
        assert!(restored
            .issues
            .values()
            .all(|i| !i.notified && i.pending.is_none()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_batch_survives_restart_and_mixed_changes_reconcile_individually() {
        let mut book = Book::default();
        book.observe(account_observations(Some(true)), 100).unwrap();
        let batch = book.claim(100).unwrap();
        assert_eq!(batch.deliveries.len(), 11);
        assert!(book.claim(100).is_none());
        book.finish(&batch, false, 100).unwrap();
        assert!(book.issues.values().all(|i| !i.notified));
        let mut book: Book = serde_json::from_slice(&serde_json::to_vec(&book).unwrap()).unwrap();
        assert!(book.claim(101).is_none());
        let batch = book.claim(20_000).unwrap();
        let mut changes = account_observations(None);
        changes[0].problem = Some(false);
        book.observe(changes, 20_001).unwrap();
        book.finish(&batch, true, 20_002).unwrap();
        assert_eq!(book.outbox.len(), 1); // Only the known recovery can clear.
        let mut new = obs(Some(true));
        new[0].title = "new_fr_arb01 风险".into();
        book.observe(new, 20_003).unwrap();
        let mixed = book.claim(20_003).unwrap();
        assert_eq!(mixed.deliveries.len(), 2);
        assert!(mixed.body.contains("新增 1 项 · 恢复 1 项"));
        // An in-flight recovery is not cancelled when its issue recurs.
        book.observe(account_observations(Some(true)), 20_004)
            .unwrap();
        assert_eq!(book.outbox.len(), 2);
        book.finish(&mixed, true, 20_005).unwrap();
        let recurrence = book.claim(20_006).unwrap();
        assert_eq!(recurrence.deliveries.len(), 1);
        assert!(!recurrence.deliveries[0].recovery);
        assert_eq!(
            recurrence.deliveries[0].issue.as_deref(),
            Some("exposure/test_fr_arb01/ON")
        );
    }

    #[test]
    fn oversized_batches_split_by_bytes_without_losing_or_acknowledging_unsent_items() {
        let mut book = Book::default();
        let changes = (0..100)
            .map(|i| Observation {
                key: format!("exposure/env{i}/TEST"),
                title: format!("env{i} TEST {}", "风险".repeat(100)),
                problem: Some(true),
                detail: "实际净敞口超过正常区间；".repeat(100),
            })
            .collect();
        book.observe(changes, 100).unwrap();
        let mut sent = BTreeSet::new();
        while let Some(batch) = book.claim(100) {
            assert!(batch.body.len() <= 3600);
            assert!(batch.deliveries.len() > 1);
            for d in &batch.deliveries {
                assert!(sent.insert(d.id));
            }
            book.finish(&batch, true, 101).unwrap();
            assert!(book
                .outbox
                .values()
                .all(|d| !book.issues[d.issue.as_ref().unwrap()].notified));
        }
        assert_eq!(sent.len(), 100);
        assert!(book.delivered > 1 && book.delivered < 100);
        assert!(book
            .issues
            .values()
            .all(|i| i.notified && i.pending.is_none()));
    }

    #[tokio::test]
    async fn parameter_cursor_outbox_and_full_history_commit_together_across_restart() {
        use mkt_signal::common::parameter_audit::{Batch, Record};
        let root = std::env::temp_dir().join(format!("parameter-outbox-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("state.json");
        let sender =
            || DingTalk::test_sender(reqwest::Url::parse("http://127.0.0.1:1/mock").unwrap());
        let event = |id, before, after| {
            serde_json::from_value(serde_json::json!({"id":id,"time_us":1700000000000000i64,"storage":"cta","writer":{"pid":1,"start_ticks":"1"},"family":"cta_prep","environment":"cta-prep-binance-futures","source":"config_page","actor":"tester","source_ip":null,"session_id":null,"request_path":null,"objects":{"cta-prep-binance-futures/one.json":{"before":{"amount":before},"after":{"amount":after}}}})).unwrap()
        };
        let batch = Batch {
            watermark: 2,
            complete: true,
            records: vec![
                Record {
                    event: event(1, 1, 2),
                    pending: false,
                },
                Record {
                    event: event(2, 2, 1),
                    pending: false,
                },
            ],
        };
        let notifier = Notifications::open(path.clone(), sender()).await.unwrap();
        notifier
            .parameters("jp", batch.clone(), None)
            .await
            .unwrap();
        assert_eq!(notifier.parameter_cursor("jp").await, 2);
        assert_eq!(notifier.book.lock().await.outbox.len(), 2);
        assert_eq!(
            notifier.parameter_record("jp", 1).await.unwrap().event.id,
            1
        );
        drop(notifier);
        let notifier = Notifications::open(path, sender()).await.unwrap();
        notifier.parameters("jp", batch, None).await.unwrap();
        let mut book = notifier.book.lock().await;
        assert_eq!(book.outbox.len(), 2);
        let pending = book.claim(now_ms()).unwrap();
        book.finish(&pending, false, now_ms()).unwrap();
        assert_eq!(book.outbox.len(), 2);
        drop(book);
        drop(notifier);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn first_alert_and_recovery_only_survive_restart() {
        let mut book = Book::default();
        book.observe(obs(Some(true)), 100).unwrap();
        assert!(!deliver(&mut book, 100).recovery);
        for now in [200, 100_000, 100_000_000] {
            book.observe(obs(Some(true)), now).unwrap();
            assert!(book.claim(now).is_none());
        }
        let mut book: Book = serde_json::from_slice(&serde_json::to_vec(&book).unwrap()).unwrap();
        book.observe(obs(Some(true)), 100_000_001).unwrap();
        assert!(book.outbox.is_empty());
        book.observe(obs(None), 100_000_002).unwrap();
        assert!(book.outbox.is_empty());
        book.observe(obs(Some(false)), 100_000_003).unwrap();
        assert!(deliver(&mut book, 100_000_003).recovery);
        book.observe(obs(Some(false)), 100_000_004).unwrap();
        assert!(book.outbox.is_empty());
        book.observe(obs(Some(true)), 100_000_005).unwrap();
        assert!(!deliver(&mut book, 100_000_005).recovery);
    }

    #[test]
    fn failed_send_stays_pending_with_backoff_and_cannot_recover_unknown() {
        let mut book = Book::default();
        book.observe(obs(Some(true)), 100).unwrap();
        let d = book.claim(100).unwrap();
        book.finish(&d, false, 100).unwrap();
        assert!(!book.issues["risk/a"].notified);
        assert!(book.claim(101).is_none());
        book.observe(obs(None), 200).unwrap();
        assert_eq!(book.outbox.len(), 1);
        let d = deliver(&mut book, 20_000);
        assert!(!d.recovery);
        assert!(book.issues["risk/a"].notified);
        assert!(book.outbox.is_empty());
    }

    #[test]
    fn recovery_during_alert_delivery_and_recurrence_during_recovery() {
        let mut book = Book::default();
        book.observe(obs(Some(true)), 100).unwrap();
        let alarm = book.claim(100).unwrap();
        book.observe(obs(Some(false)), 101).unwrap();
        book.finish(&alarm, true, 102).unwrap();
        let recovery = book.claim(103).unwrap();
        assert!(recovery.deliveries[0].recovery);
        book.observe(obs(Some(true)), 104).unwrap();
        book.finish(&recovery, true, 105).unwrap();
        assert!(!deliver(&mut book, 106).recovery);
    }

    #[test]
    fn recurrence_cancels_recovery_before_it_is_sent_and_critical_preempts_luna() {
        let mut book = Book::default();
        book.queue("luna_ok", "公告".into(), 2, None, false, 1)
            .unwrap();
        book.observe(obs(Some(true)), 2).unwrap();
        assert_eq!(deliver(&mut book, 3).kind, "alert");
        book.observe(obs(Some(false)), 4).unwrap();
        book.observe(obs(Some(true)), 5).unwrap();
        assert_eq!(deliver(&mut book, 6).kind, "luna_ok");
        assert!(book.outbox.is_empty());
    }

    #[test]
    fn every_actual_luna_processing_is_an_event_and_messages_remain_bounded() {
        let mut book = Book::default();
        for _ in 0..2 {
            book.queue("luna_ok", "same input".into(), 2, None, false, 1)
                .unwrap();
        }
        assert_eq!(book.outbox.len(), 2);
        let clipped = clip(&"中".repeat(2000), 3700);
        assert!(clipped.len() <= 3700);
        let input = mkt_signal::common::announcement_llm::LlmExtractInput {
            exchange: "gate".into(),
            id: "1".into(),
            title: "公告".into(),
            url: String::new(),
            published_ms: 1,
            body: String::new(),
        };
        assert!(
            luna_message(&input, Some(&serde_json::json!({"actions":[]})), true)
                .contains("没有识别")
        );
        assert!(luna_message(&input, None, false).contains("失败"));
    }

    #[test]
    fn dingtalk_hmac_matches_independent_vector_and_unimmr_uses_force_recovery_line() {
        let mut sender = DingTalk::test_sender(
            reqwest::Url::parse("https://oapi.dingtalk.com/robot/send?access_token=test").unwrap(),
        );
        sender.secret = Some("SECtest".into());
        let signed = sender.signed_url(123456789).unwrap();
        let query: BTreeMap<_, _> = signed.query_pairs().into_owned().collect();
        assert_eq!(
            query["sign"],
            "YaoDhyN8cSsTib7CZYidXNi4q6x+gMdZvxGPnn6+jZQ="
        );
        for (ratio, expected) in [
            (2.9, Some(true)),
            (3.0, Some(true)),
            (3.05, None),
            (3.1, Some(false)),
            (3.4, Some(false)),
        ] {
            let risk = serde_json::json!({"unimmr_trigger_line":3.5,"unimmr_recover_line":3.6,"unimmr_force_close_line":3.0,"unimmr_force_close_recover_line":3.1,"account_risks":[{"state":"free_trade","margin_ratio":ratio}]});
            assert_eq!(unimmr_observation("test", Some(&risk)).problem, expected);
        }
        assert_eq!(unimmr_observation("test", None).problem, None);
    }

    #[test]
    fn normal_unimmr_position_control_is_quiet_but_exchange_risk_still_alerts() {
        let mut risk = serde_json::json!({"unimmr_trigger_line":50.0,"unimmr_recover_line":65.0,
            "unimmr_force_close_line":2.8,"unimmr_force_close_recover_line":2.9,
            "account_risks":[{"state":"free_trade","margin_ratio":42.9}]});
        let mut book = Book::default();
        book.observe(vec![unimmr_observation("test", Some(&risk))], 1)
            .unwrap();
        assert!(book.outbox.is_empty());
        for state in ["warning", "reduce_only", "liquidation"] {
            risk["account_risks"][0]["state"] = serde_json::json!(state);
            assert_eq!(unimmr_observation("test", Some(&risk)).problem, Some(true));
        }
        risk["account_risks"][0]["state"] = serde_json::json!("unrecognized");
        assert_eq!(unimmr_observation("test", Some(&risk)).problem, None);
    }

    #[tokio::test]
    async fn dingtalk_requires_successful_api_response_without_exposing_url() {
        use axum::{routing::post, Json, Router};
        let app=Router::new().route("/ok",post(|Json(payload):Json<serde_json::Value>| async move {
            assert_eq!(payload["msgtype"],"text");assert_eq!(payload["at"]["isAtAll"],false);
            assert!(payload["text"]["content"].as_str().unwrap().starts_with("[FR]"));
            Json(serde_json::json!({"errcode":0}))
        })).route("/bad",post(|| async {Json(serde_json::json!({"errcode":310000,"errmsg":"private-token-do-not-expose"}))}));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        DingTalk::test_sender(reqwest::Url::parse(&format!("http://{addr}/ok")).unwrap())
            .send("配置测试")
            .await
            .unwrap();
        let error = DingTalk::test_sender(
            reqwest::Url::parse(&format!(
                "http://{addr}/bad?access_token=private-token-do-not-expose"
            ))
            .unwrap(),
        )
        .send("配置测试")
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("310000"));
        assert!(!error.contains("private-token"));
        assert!(!error.contains("http"));
        server.abort();
    }

    #[tokio::test]
    async fn private_outbox_and_daily_heartbeat_survive_reload() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("fr-notifications-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.json");
        let sender =
            || DingTalk::test_sender(reqwest::Url::parse("http://127.0.0.1:1/unused").unwrap());
        let first = Notifications::open(path.clone(), sender()).await.unwrap();
        first.observe(obs(Some(true))).await.unwrap();
        {
            let mut b = first.book.lock().await;
            deliver(&mut b, now_ms());
            first.save(&mut b).await.unwrap();
        }
        first.heartbeat("当前公告扫描已完成").await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(first);
        let next = Notifications::open(path.clone(), sender()).await.unwrap();
        next.observe(obs(Some(true))).await.unwrap();
        next.heartbeat("重启后本轮扫描").await.unwrap();
        let status = next.status().await;
        assert_eq!(status["queued"], 1);
        assert_eq!(status["delivered"], 1);
        assert_eq!(
            next.book.lock().await.outbox.values().next().unwrap().kind,
            "heartbeat"
        );
        drop(next);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn listing_baseline_is_quiet_and_complete_catalog_changes_are_recorded() {
        use mkt_signal::common::exchange_info::ListingSnapshotRow;
        let row = |symbol: &str, listed: bool| ListingSnapshotRow {
            exchange: "gate".into(),
            venue: "gate-futures".into(),
            market_type: "futures".into(),
            symbol: symbol.into(),
            normalized_symbol: symbol.into(),
            status: if listed { "trading" } else { "offline" }.into(),
            listed,
            pending: false,
        };
        let mut book = Book::default();
        book.listing_changes(&[row("OLDUSDT", true)], 1).unwrap();
        assert!(book.outbox.is_empty());
        book.listing_changes(&[], 2).unwrap();
        assert!(book.outbox.is_empty());
        book.listing_changes(&[row("NEWUSDT", true), row("OLDUSDT", false)], 3)
            .unwrap();
        let d = book.outbox.values().next().unwrap();
        assert_eq!(d.kind, "listing");
        assert!(d.body.contains("NEWUSDT"));
        assert!(d.body.contains("OLDUSDT"));
        book.listing_changes(&[row("NEWUSDT", true), row("OLDUSDT", false)], 4)
            .unwrap();
        assert_eq!(book.outbox.len(), 1);
    }

    #[test]
    fn fr_channel_excludes_cta_and_retired_fr_source_history() {
        let active = BTreeSet::from(["binance_fr_arb02".into()]);
        for source in [
            "okex_announcements",
            "okex_article_detail",
            "okex_spot_exchange_info",
            "schedule:okex-futures",
            "cta_backup_catalog",
            "cta_backup_quality_binance-cta-perp-rx02",
            "cos_cta_backup_active",
            "redis_fr_backup_binance_fr_arb01",
            "llm",
            "fr_unimmr_close_list",
        ] {
            assert!(!fr_source(source, &active), "{source}");
        }
        for source in [
            "redis_fr_backup_binance_fr_arb02",
            "redis_fr_backup_catalog",
            "exchange_info",
            "bybit_announcements",
        ] {
            assert!(fr_source(source, &active), "{source}");
        }
    }

    #[test]
    fn current_cta_backup_failures_retry_deduplicate_and_recover_after_restart() {
        use mkt_signal::common::delist_store::SourceStatus;
        for worker in ["cta-prep-binance-futures", "cta-factor-spread"] {
            for prefix in [
                "cta_backup_",
                "cta_backup_quality_",
                "cos_cta_backup_",
                "cos_cta_retention_",
            ] {
                let mut source = SourceStatus {
                    source: format!("{prefix}{worker}"),
                    kind: "backup".into(),
                    ok: true,
                    last_attempt_ms: 1,
                    last_success_ms: Some(1),
                    last_error: Some("postgres://private-user:private-secret@localhost/db".into()),
                    last_error_ms: None,
                };
                let mut book = Book::default();
                book.observe(vec![cta_backup_observation(&source).unwrap()], 1)
                    .unwrap();
                assert!(book.outbox.is_empty(), "healthy startup must stay silent");
                source.ok = false;
                for now in [2, 3] {
                    book.observe(vec![cta_backup_observation(&source).unwrap()], now)
                        .unwrap();
                }
                assert_eq!(book.outbox.len(), 1);
                let failed = book.claim(3).unwrap();
                assert!(failed.body.contains(if worker.contains("spread") {
                    "CTA Spread"
                } else {
                    "CTA Prep"
                }));
                assert!(!failed.body.contains("private-secret"));
                book.finish(&failed, false, 3).unwrap();
                let mut book: Book =
                    serde_json::from_slice(&serde_json::to_vec(&book).unwrap()).unwrap();
                assert!(!deliver(&mut book, 20_000).recovery);
                book.observe(vec![cta_backup_observation(&source).unwrap()], 20_001)
                    .unwrap();
                assert!(book.outbox.is_empty(), "continuing failure must not repeat");
                source.ok = true;
                book.observe(vec![cta_backup_observation(&source).unwrap()], 20_002)
                    .unwrap();
                assert!(deliver(&mut book, 20_003).recovery);
                book.observe(vec![cta_backup_observation(&source).unwrap()], 20_004)
                    .unwrap();
                assert!(book.outbox.is_empty());
            }
        }
    }

    #[test]
    fn cta_backup_alerts_exclude_retired_archives_and_unrelated_cta_state() {
        use mkt_signal::common::delist_store::SourceStatus;
        for name in [
            "cta_backup_catalog",
            "cta_backup_binance-cta-perp-rx01",
            "cos_cta_backup_cta-prep-binance-futures-extra",
            "cta_backup_quality_cta-factor-spread/../other",
            "cta_factor_catalog",
            "cos_fr_backup_binance_fr_arb02",
        ] {
            let source = SourceStatus {
                source: name.into(),
                kind: "backup".into(),
                ok: false,
                last_attempt_ms: 1,
                last_success_ms: None,
                last_error: None,
                last_error_ms: Some(1),
            };
            assert!(cta_backup_observation(&source).is_none(), "{name}");
        }
    }

    #[test]
    fn mm_backup_failure_and_recovery_use_the_durable_outbox_without_fr_catalog() {
        for prefix in ["redis_mm_backup_", "cos_mm_backup_", "cos_mm_retention_"] {
            let mut source = mkt_signal::common::delist_store::SourceStatus {
                source: format!("{prefix}okex_mm_83777"),
                kind: "backup".into(),
                ok: false,
                last_attempt_ms: 1,
                last_success_ms: None,
                last_error: Some("private credential".into()),
                last_error_ms: Some(1),
            };
            assert!(!fr_source(&source.source, &BTreeSet::new()));
            let mut book = Book::default();
            book.observe(vec![mm_backup_observation(&source).unwrap()], 1)
                .unwrap();
            let failed = book.claim(1).unwrap();
            assert!(failed.body.contains("MM 参数备份 okex_mm_83777"));
            assert!(!failed.body.contains("private credential"));
            book.finish(&failed, false, 1).unwrap();
            let mut book: Book =
                serde_json::from_slice(&serde_json::to_vec(&book).unwrap()).unwrap();
            assert!(!deliver(&mut book, 20_000).recovery);
            book.observe(vec![mm_backup_observation(&source).unwrap()], 20_001)
                .unwrap();
            assert!(book.outbox.is_empty());
            source.ok = true;
            book.observe(vec![mm_backup_observation(&source).unwrap()], 20_002)
                .unwrap();
            assert!(deliver(&mut book, 20_003).recovery);
        }
    }

    #[tokio::test]
    async fn fully_closed_asset_recovers_only_from_complete_fresh_coverage() {
        let dir = std::env::temp_dir().join(format!("fr-closed-asset-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let notifier = Notifications::open(
            dir.join("state.json"),
            DingTalk::test_sender(reqwest::Url::parse("http://127.0.0.1:1/unused").unwrap()),
        )
        .await
        .unwrap();
        notifier
            .observe(vec![Observation {
                key: "exposure/test/BTC".into(),
                title: "BTC净敞口".into(),
                detail: "160U".into(),
                problem: Some(true),
            }])
            .await
            .unwrap();
        {
            let mut b = notifier.book.lock().await;
            deliver(&mut b, now_ms());
        }
        let account = super::super::exposure::Account {
            slug: "test".into(),
            alias: "test".into(),
            exchange: "gate".into(),
            host: "jp".into(),
            checked_ms: now_ms(),
            snapshot_ms: Some(now_ms()),
            account_ms: Some(now_ms()),
            data_status: "partial",
            risk_status: "unknown",
            complete: false,
            data_incomplete_since_ms: None,
            data_incomplete_confirmed: false,
            abs_usdt: Some(0.0),
            risk_abs_usdt: Some(0.0),
            limit_usdt: Some(500.0),
            ignored_spot_tail_n: 0,
            net_usdt: Some(0.0),
            largest_asset: None,
            largest_usdt: None,
            rows: vec![],
            errors: vec!["unpriced".into()],
            breaches: BTreeMap::new(),
        };
        let mut report = super::super::exposure::Report {
            as_of_ms: now_ms(),
            checked_ms: now_ms(),
            catalog_current: true,
            settings: super::super::exposure::Settings::default(),
            accounts: vec![account],
            errors: vec![],
        };
        let observations = notifier.exposure_observations(&report).await;
        assert!(!observations
            .iter()
            .any(|o| o.key == "exposure/test/BTC" && o.problem == Some(false)));
        report.accounts[0].complete = true;
        report.accounts[0].data_status = "fresh";
        let observations = notifier.exposure_observations(&report).await;
        assert!(observations
            .iter()
            .any(|o| o.key == "exposure/test/BTC" && o.problem == Some(false)));
        drop(notifier);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
