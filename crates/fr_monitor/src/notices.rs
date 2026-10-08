use anyhow::{ensure, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    checks::{Check, Issue},
    config::Monitor,
    now_ms,
};

#[derive(Clone, Serialize)]
pub struct ActiveIssue {
    pub scope: String,
    #[serde(flatten)]
    pub issue: Issue,
    pub first_seen_ms: i64,
    pub observed: bool,
    #[serde(skip)]
    last_sent_ms: Option<i64>,
    #[serde(skip)]
    sent_severity: Option<String>,
    #[serde(skip)]
    resolved: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct Notice {
    pub scope: String,
    pub key: String,
    pub message: String,
    pub severity: String,
    pub recovery: bool,
}

impl Notice {
    pub fn market(&self) -> bool {
        self.scope.ends_with("/market")
    }
}

#[derive(Default)]
pub struct Tracker {
    states: BTreeMap<(String, String), ActiveIssue>,
}

impl Tracker {
    pub fn update(&mut self, checks: &[Check], now: i64) {
        for check in checks {
            let present: BTreeSet<_> = check.issues.iter().map(|i| i.key.as_str()).collect();
            for state in self.states.values_mut().filter(|s| s.scope == check.scope) {
                state.observed = present.contains(state.issue.key.as_str());
                if check.complete && !state.observed {
                    state.resolved = true;
                }
                if !check.complete && !state.observed {
                    // The outage also invalidates a queued recovery not yet delivered.
                    state.resolved = false;
                }
            }
            for issue in &check.issues {
                let state = self
                    .states
                    .entry((check.scope.clone(), issue.key.clone()))
                    .or_insert(ActiveIssue {
                        scope: check.scope.clone(),
                        issue: issue.clone(),
                        first_seen_ms: now,
                        observed: true,
                        last_sent_ms: None,
                        sent_severity: None,
                        resolved: false,
                    });
                state.issue = issue.clone();
                state.observed = true;
                state.resolved = false;
            }
        }
        self.states
            .retain(|_, s| !(s.resolved && s.last_sent_ms.is_none()));
    }

    pub fn active(&self) -> Vec<ActiveIssue> {
        self.states
            .values()
            .filter(|s| !s.resolved)
            .cloned()
            .collect()
    }

    pub fn pending(&self, m: &Monitor, now: i64) -> Vec<Notice> {
        self.states
            .values()
            .filter(|s| {
                if s.resolved {
                    return s.last_sent_ms.is_some();
                }
                if !s.observed {
                    return false;
                }
                let ready = s.issue.severity == "critical"
                    || now.saturating_sub(s.first_seen_ms) >= m.alert_delay_secs as i64 * 1000;
                ready
                    && (s.last_sent_ms.is_none()
                        || s.sent_severity.as_deref() != Some(s.issue.severity.as_str())
                        || now.saturating_sub(s.last_sent_ms.unwrap_or(0))
                            >= m.repeat_secs as i64 * 1000)
            })
            .map(|s| Notice {
                scope: s.scope.clone(),
                key: s.issue.key.clone(),
                message: s.issue.message.clone(),
                severity: s.issue.severity.clone(),
                recovery: s.resolved,
            })
            .collect()
    }

    pub fn acknowledge(&mut self, notices: &[Notice], now: i64) {
        for n in notices {
            let key = (n.scope.clone(), n.key.clone());
            if n.recovery {
                self.states.remove(&key);
            } else if let Some(s) = self.states.get_mut(&key) {
                s.last_sent_ms = Some(now);
                s.sent_severity = Some(n.severity.clone());
            }
        }
    }
}

pub struct DingTalk {
    client: reqwest::Client,
    urls: [reqwest::Url; 2],
    secrets: [Option<String>; 2],
    host: String,
}

impl DingTalk {
    /// Only called with --execute; dry-run never reads webhook credentials.
    pub fn from_env(m: &Monitor) -> Result<Self> {
        let read = |name: &str| -> Result<reqwest::Url> {
            let raw = std::env::var(name)
                .with_context(|| format!("missing webhook environment variable {name}"))?;
            let url = reqwest::Url::parse(&raw)
                .map_err(|_| anyhow::anyhow!("invalid webhook URL in {name}"))?;
            ensure!(
                url.scheme() == "https"
                    && url.host_str() == Some("oapi.dingtalk.com")
                    && url.username().is_empty()
                    && url.password().is_none(),
                "invalid DingTalk webhook destination"
            );
            Ok(url)
        };
        let secret = |name: &Option<String>| -> Result<Option<String>> {
            name.as_ref()
                .map(|n| {
                    std::env::var(n).with_context(|| format!("missing signing secret env {n}"))
                })
                .transpose()
        };
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(m.request_timeout_secs))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            urls: [
                read(&m.market_webhook_url_env)?,
                read(&m.order_webhook_url_env)?,
            ],
            secrets: [secret(&m.market_secret_env)?, secret(&m.order_secret_env)?],
            host: m.host_tag.clone(),
        })
    }

    pub async fn send(&self, notices: &[Notice], market: bool) -> Result<()> {
        let index = usize::from(!market);
        let mut url = self.urls[index].clone();
        if let Some(secret) = &self.secrets[index] {
            let timestamp = now_ms().to_string();
            let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
            mac.update(format!("{timestamp}\n{secret}").as_bytes());
            let signature =
                base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            url.query_pairs_mut()
                .append_pair("timestamp", &timestamp)
                .append_pair("sign", &signature);
        }
        let mut content = format!("[{}] FR 运行监控\n", self.host);
        for n in notices {
            let kind = if n.recovery { "恢复" } else { "告警" };
            let mut message = n.message.clone();
            if message.len() > 600 {
                let mut end = 600;
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                message.truncate(end);
                message.push_str("…详情见看板");
            }
            content.push_str(&format!("[{kind}][{}] {}\n", n.scope, message));
        }
        ensure!(content.len() <= 3800, "DingTalk notice batch too large");
        // Deliberately omit reqwest errors and response text: they may contain the secret URL.
        let response = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "msgtype": "text", "text": {"content": content},
                "at": {"isAtAll": false}
            }))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("DingTalk transport failed"))?;
        ensure!(
            response.status().is_success(),
            "DingTalk HTTP failure: {}",
            response.status().as_u16()
        );
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("invalid DingTalk response"))?;
        ensure!(
            body["errcode"].as_i64() == Some(0),
            "DingTalk rejected notice"
        );
        Ok(())
    }
}
