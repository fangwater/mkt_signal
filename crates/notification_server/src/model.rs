use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use uuid::Uuid;

const MAX_SOURCE_BYTES: usize = 128;
const MAX_TITLE_BYTES: usize = 256;
const MAX_FIELDS: usize = 32;
const MAX_FIELD_KEY_BYTES: usize = 128;
const MAX_FIELD_VALUE_BYTES: usize = 2_048;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    #[default]
    Info,
    Warning,
    Critical,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => f.write_str("INFO"),
            Self::Warning => f.write_str("WARNING"),
            Self::Critical => f.write_str("CRITICAL"),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NotifyRequest {
    pub source: String,
    pub title: String,
    pub message: String,
    #[serde(default)]
    pub severity: Severity,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
    #[serde(default)]
    pub dedup_key: Option<String>,
}

impl NotifyRequest {
    pub fn validate_and_normalize(mut self, max_message_chars: usize) -> Result<Self> {
        self.source = self.source.trim().to_string();
        self.title = self.title.trim().to_string();
        self.message = self.message.trim().to_string();
        self.dedup_key = self
            .dedup_key
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        validate_required("source", &self.source, MAX_SOURCE_BYTES)?;
        validate_required("title", &self.title, MAX_TITLE_BYTES)?;
        validate_required_chars("message", &self.message, max_message_chars)?;
        if self.fields.len() > MAX_FIELDS {
            bail!("fields must contain at most {MAX_FIELDS} entries");
        }
        for (key, value) in &self.fields {
            validate_required("field key", key.trim(), MAX_FIELD_KEY_BYTES)?;
            if value.len() > MAX_FIELD_VALUE_BYTES {
                bail!("field value for {key} exceeds {MAX_FIELD_VALUE_BYTES} bytes");
            }
        }
        Ok(self)
    }
}

fn validate_required(name: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    if value.len() > max_bytes {
        bail!("{name} exceeds {max_bytes} bytes");
    }
    Ok(())
}

fn validate_required_chars(name: &str, value: &str, max_chars: usize) -> Result<()> {
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    let chars = value.chars().count();
    if chars > max_chars {
        bail!("{name} exceeds {max_chars} characters");
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct QueuedNotification {
    pub id: Uuid,
    pub accepted_at: DateTime<Utc>,
    pub request: NotifyRequest,
}

impl QueuedNotification {
    pub fn new(request: NotifyRequest) -> Self {
        Self {
            id: Uuid::new_v4(),
            accepted_at: Utc::now(),
            request,
        }
    }

    pub fn render_text(&self) -> String {
        let mut lines = vec![self.request.title.clone(), self.request.message.clone()];
        if !self.request.fields.is_empty() {
            lines.push(String::new());
            for (key, value) in &self.request.fields {
                lines.push(format!("{}: {}", key.trim(), value.trim()));
            }
        }
        lines.join("\n")
    }

    pub fn validate_rendered_chars(&self, max_chars: usize) -> Result<()> {
        let rendered_chars = self.render_text().chars().count();
        if rendered_chars > max_chars {
            bail!("rendered notification exceeds {max_chars} characters");
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct AcceptedResponse {
    pub accepted: bool,
    pub event_id: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub queue_capacity: usize,
    pub queue_available: usize,
    pub enqueued: u64,
    pub delivered: u64,
    pub failed: u64,
    pub rejected: u64,
    pub dry_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> NotifyRequest {
        NotifyRequest {
            source: "pre_trade".to_string(),
            title: "Position concentration".to_string(),
            message: "BTCUSDT ratio reached 10%".to_string(),
            severity: Severity::Warning,
            fields: BTreeMap::from([
                ("ratio".to_string(), "10.00%".to_string()),
                ("symbol".to_string(), "BTCUSDT".to_string()),
            ]),
            dedup_key: Some("btc-concentration".to_string()),
        }
    }

    #[test]
    fn renders_concise_display_text() {
        let notification = QueuedNotification {
            id: Uuid::nil(),
            accepted_at: DateTime::parse_from_rfc3339("2026-07-26T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            request: request(),
        };
        let text = notification.render_text();
        assert_eq!(
            text,
            "Position concentration\nBTCUSDT ratio reached 10%\n\nratio: 10.00%\nsymbol: BTCUSDT"
        );
        assert!(!text.contains("source:"));
        assert!(!text.contains("event_id:"));
    }

    #[test]
    fn rejects_empty_and_oversized_requests() {
        let mut empty = request();
        empty.title = "  ".to_string();
        assert!(empty.validate_and_normalize(1024).is_err());

        let mut oversized = request();
        oversized.message = "x".repeat(1025);
        assert!(oversized.validate_and_normalize(1024).is_err());
    }

    #[test]
    fn rejects_oversized_rendered_notification() {
        let mut oversized = request();
        oversized
            .fields
            .insert("detail".to_string(), "x".repeat(1100));
        let notification = QueuedNotification::new(oversized.validate_and_normalize(1024).unwrap());
        assert!(notification.validate_rendered_chars(1024).is_err());
    }
}
