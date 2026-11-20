use super::super::config::PublicationConfig;
use super::*;
use reqwest::header::{HeaderMap, HeaderValue};
use std::{collections::BTreeSet, path::PathBuf, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Request {
    pub strategy_group: String,
    pub batch_id: String,
    pub expected_revision: i64,
    pub decision_us: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trading_day: Option<String>,
    pub signal_times: BTreeMap<String, i64>,
    pub strategies: BTreeMap<String, Targets>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Pending {
    signature: String,
    trigger_us: i64,
    request: Request,
}
#[derive(Clone, Serialize, Deserialize)]
struct Outbox {
    manager_url: String,
    source_id: String,
    strategy_group: String,
    acknowledged_signature: Option<String>,
    discarded_signature: Option<String>,
    pending: Option<Pending>,
}
#[derive(Deserialize)]
struct Watermark {
    source_id: String,
    strategy_group: String,
    revision: i64,
    members: BTreeSet<String>,
    contracts: BTreeSet<String>,
}
#[derive(Deserialize)]
struct Receipt {
    source_id: String,
    strategy_group: String,
    batch_id: String,
}

pub(super) struct Client {
    http: reqwest::Client,
    settings: PublicationConfig,
    group: String,
    path: PathBuf,
    outbox: Outbox,
}

impl Client {
    pub fn new(settings: &PublicationConfig, group: String, path: PathBuf) -> Result<Self> {
        let token = std::env::var(&settings.token_env).with_context(|| {
            format!(
                "native publication token environment {} is required",
                settings.token_env
            )
        })?;
        Self::with_token(settings, group, path, &token)
    }
    fn with_token(
        settings: &PublicationConfig,
        group: String,
        path: PathBuf,
        token: &str,
    ) -> Result<Self> {
        ensure!(
            !token.is_empty(),
            "native publication token must not be empty"
        );
        let mut token = HeaderValue::from_str(token)
            .map_err(|_| anyhow::anyhow!("invalid native publication token header"))?;
        token.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert("X-CTA-Publish-Token", token);
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .default_headers(headers)
            .build()
            .context("create native Manager client")?;
        let outbox = if path.exists() {
            let stored: Outbox = serde_json::from_slice(&std::fs::read(&path)?)?;
            ensure!(stored.manager_url==settings.manager_url && stored.source_id==settings.source_id && stored.strategy_group==group,
                "native publication outbox belongs to a different Manager/account/group; migrate explicitly");
            stored
        } else {
            Outbox {
                manager_url: settings.manager_url.clone(),
                source_id: settings.source_id.clone(),
                strategy_group: group.clone(),
                acknowledged_signature: None,
                discarded_signature: None,
                pending: None,
            }
        };
        Ok(Self {
            http,
            settings: settings.clone(),
            group,
            path,
            outbox,
        })
    }
    fn retired_hourly_member(&self, name: &str) -> bool {
        self.settings.retire_hourly_factor_members
            && name
                .strip_prefix(&self.group)
                .and_then(|name| name.strip_prefix('-'))
                .and_then(|name| name.strip_suffix("-1h"))
                .is_some_and(|id| {
                    !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
    }
    pub(super) fn check_pending_members<'a>(
        &self,
        members: impl Iterator<Item = &'a String>,
    ) -> Result<()> {
        if let Some(pending) = &self.outbox.pending {
            let current: BTreeSet<_> = members.collect();
            ensure!(
                current.iter().all(|name| pending.request.strategies.contains_key(*name))
                    && pending.request.strategies.iter().all(|(name, targets)| current.contains(name)
                        || (self.retired_hourly_member(name) && targets.values().all(|&lots| lots == 0))),
                "pending publication uses a different strategy roster; reconcile the original batch before changing the roster"
            );
        }
        Ok(())
    }

    fn endpoint(&self, suffix: &str) -> String {
        format!(
            "{}/native/sources/{}/{}",
            self.settings.manager_url.trim_end_matches('/'),
            self.settings.source_id,
            suffix
        )
    }
    async fn persist(&self) -> Result<()> {
        let path = self.path.clone();
        let outbox = self.outbox.clone();
        tokio::task::spawn_blocking(move || super::super::atomic(&path, &outbox))
            .await
            .context("native outbox writer stopped")??;
        Ok(())
    }
    async fn prepare(&mut self, desired: &Desired) -> Result<()> {
        let response = self
            .http
            .get(self.endpoint("netting/publication"))
            .query(&[("strategy_group", &self.group)])
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("native Manager publication metadata unavailable"))?;
        ensure!(
            response.status().is_success(),
            "native Manager metadata HTTP {}",
            response.status().as_u16()
        );
        let watermark: Watermark = response
            .json()
            .await
            .context("decode native Manager group metadata")?;
        ensure!(
            watermark.source_id == self.settings.source_id
                && watermark.strategy_group == self.group,
            "native Manager metadata identity mismatch"
        );
        let current: BTreeSet<_> = desired.positions.keys().cloned().collect();
        let retired = watermark.members.difference(&current);
        let aggregate = format!("{}-1h", self.group);
        ensure!(
            current.is_subset(&watermark.members)
                && (retired.clone().next().is_none()
                    || (current.contains(&aggregate)
                        && retired.clone().all(|name| self.retired_hourly_member(name)))),
            "native Manager registered group roster differs from the complete published group"
        );
        let mut strategies = BTreeMap::new();
        let mut signal_times = BTreeMap::new();
        for (name, position) in &desired.positions {
            ensure!(position.targets.iter().all(|(market,qty)|*qty==0 || watermark.contracts.contains(market)),"native target contract is not registered in Manager; retain old targets and configure rollover first");
            let rows = watermark
                .contracts
                .iter()
                .map(|market| {
                    (
                        market.clone(),
                        position.targets.get(market).copied().unwrap_or(0),
                    )
                })
                .collect();
            strategies.insert(name.clone(), rows);
            signal_times.insert(
                name.clone(),
                position
                    .decision_ms
                    .checked_mul(1000)
                    .context("native signal time overflow")?,
            );
        }
        // Keep historical member identities in Manager's complete roster while clearing their targets.
        for name in retired {
            strategies.insert(
                name.clone(),
                watermark
                    .contracts
                    .iter()
                    .map(|market| (market.clone(), 0))
                    .collect(),
            );
            signal_times.insert(name.clone(), desired.trigger_us);
        }
        let now = chrono::Utc::now().timestamp_micros();
        ensure!(
            desired.live_ready
                && desired.trigger_us > 0
                && desired.trigger_us <= now
                && now - desired.trigger_us <= 120_000_000,
            "waiting for a fresh factual native decision"
        );
        let trading_day = if self.settings.include_trading_day {
            Some(
                desired
                    .trading_day
                    .filter(|day| (10_000_000..=99_999_999).contains(day))
                    .context("native decision is missing its trading day")?
                    .to_string(),
            )
        } else {
            None
        };
        let request = Request {
            strategy_group: self.group.clone(),
            batch_id: format!("{}-{}-{}", self.group, now, &desired.signature[..16]),
            expected_revision: watermark.revision,
            decision_us: now,
            trading_day,
            signal_times,
            strategies,
        };
        self.outbox.pending = Some(Pending {
            trigger_us: desired.trigger_us,
            signature: desired.signature.clone(),
            request,
        });
        // Store the exact request before dispatch: a timeout must retry identical
        // content, including its CAS revision.
        if let Err(error) = self.persist().await {
            self.outbox.pending = None;
            return Err(error);
        }
        Ok(())
    }
    async fn deliver(&mut self) -> Result<Option<String>> {
        let pending = self
            .outbox
            .pending
            .clone()
            .context("missing native pending batch")?;
        let response = self
            .http
            .post(self.endpoint("target-batches"))
            .json(&pending.request)
            .send()
            .await
            .map_err(|_| {
                anyhow::anyhow!("native Manager result unknown; exact batch retained for retry")
            })?;
        let status = response.status();
        if status.is_success() {
            let receipt: Receipt = response
                .json()
                .await
                .context("decode native Manager batch receipt")?;
            ensure!(
                receipt.source_id == self.settings.source_id
                    && receipt.strategy_group == self.group
                    && receipt.batch_id == pending.request.batch_id,
                "native Manager receipt identity mismatch; retain request"
            );
            self.outbox.acknowledged_signature = Some(pending.signature);
            self.outbox.pending = None;
            self.persist().await?;
            return Ok(Some(receipt.batch_id));
        }
        if status == reqwest::StatusCode::CONFLICT {
            // Replayed committed batches return 200 before the CAS check. A 409
            // therefore rejected this exact request and permits a fresh CAS read.
            self.outbox.pending = None;
            self.persist().await?;
        } else if status == reqwest::StatusCode::BAD_REQUEST {
            if chrono::Utc::now().timestamp_micros() - pending.trigger_us > 120_000_000 {
                self.outbox.discarded_signature = Some(pending.signature);
            }
            self.outbox.pending = None;
            self.persist().await?;
        }
        anyhow::bail!("native Manager batch HTTP {}", status.as_u16())
    }
    pub async fn run(
        mut self,
        mut desired: watch::Receiver<Option<Desired>>,
        state: Arc<Mutex<Status>>,
    ) {
        loop {
            let latest = desired.borrow_and_update().clone();
            let mut result = Ok(None);
            if self.outbox.pending.is_none() {
                if let Some(latest) = latest.as_ref().filter(|d| {
                    d.live_ready
                        && self.outbox.acknowledged_signature.as_ref() != Some(&d.signature)
                        && self.outbox.discarded_signature.as_ref() != Some(&d.signature)
                }) {
                    result = self.prepare(latest).await.map(|_| None);
                }
            }
            if result.is_ok() && self.outbox.pending.is_some() {
                result = self.deliver().await;
            }
            {
                let mut status = state.lock().unwrap_or_else(|p| p.into_inner());
                status.pending_batch = self
                    .outbox
                    .pending
                    .as_ref()
                    .map(|p| p.request.batch_id.clone());
                match result {
                    Ok(Some(id)) => {
                        status.acknowledged_batch = Some(id);
                        status.error = None;
                    }
                    Ok(None) => {}
                    Err(error) => status.error = Some(format!("{error:#}")),
                }
            }
            if latest.is_none() && self.outbox.pending.is_none() {
                if desired.changed().await.is_err() {
                    return;
                }
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if desired.has_changed().is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
