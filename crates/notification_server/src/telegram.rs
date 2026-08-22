use crate::model::QueuedNotification;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use log::{info, warn};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;
use tokio::time::sleep;

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";

#[async_trait]
pub trait NotificationSink: Send + Sync {
    async fn deliver(&self, notification: &QueuedNotification) -> Result<()>;
}

pub struct TelegramSink {
    client: Client,
    endpoint: Option<Url>,
    chat_id: Option<String>,
    message_thread_id: Option<i64>,
    disable_notification: bool,
    retry_attempts: u32,
    retry_base_delay: Duration,
    dry_run: bool,
}

impl TelegramSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bot_token: Option<String>,
        chat_id: Option<String>,
        message_thread_id: Option<i64>,
        disable_notification: bool,
        request_timeout: Duration,
        retry_attempts: u32,
        retry_base_delay: Duration,
        dry_run: bool,
    ) -> Result<Self> {
        let endpoint = bot_token
            .map(|token| {
                let token = token.trim();
                validate_bot_token(token)?;
                Url::parse(&format!("{TELEGRAM_API_BASE}/bot{token}/sendMessage"))
                    .context("build Telegram sendMessage URL")
            })
            .transpose()?;
        let chat_id = chat_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if chat_id
            .as_deref()
            .is_some_and(|value| value.chars().any(char::is_whitespace))
        {
            bail!("TELEGRAM_CHAT_ID must not contain whitespace");
        }
        if !dry_run && endpoint.is_none() {
            bail!("TELEGRAM_BOT_TOKEN is required unless NOTIFICATION_DRY_RUN=1");
        }
        if !dry_run && chat_id.is_none() {
            bail!("TELEGRAM_CHAT_ID is required unless NOTIFICATION_DRY_RUN=1");
        }

        let client = Client::builder()
            .timeout(request_timeout)
            .build()
            .context("build Telegram HTTP client")?;
        Ok(Self {
            client,
            endpoint,
            chat_id,
            message_thread_id,
            disable_notification,
            retry_attempts,
            retry_base_delay,
            dry_run,
        })
    }

    async fn send_once(
        &self,
        notification: &QueuedNotification,
    ) -> std::result::Result<(), SendFailure> {
        if self.dry_run {
            info!(
                "notification dry-run delivered event_id={} source={} severity={} title={}",
                notification.id,
                notification.request.source,
                notification.request.severity,
                notification.request.title
            );
            return Ok(());
        }

        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| SendFailure::new("Telegram bot token is not configured"))?;
        let chat_id = self
            .chat_id
            .as_deref()
            .ok_or_else(|| SendFailure::new("Telegram chat ID is not configured"))?;
        let payload = SendMessagePayload {
            chat_id,
            text: notification.render_text(),
            message_thread_id: self.message_thread_id,
            disable_notification: self.disable_notification,
        };
        let response = self
            .client
            .post(endpoint.clone())
            .json(&payload)
            .send()
            .await
            .map_err(|err| {
                SendFailure::new(format!(
                    "send Telegram request failed: {}",
                    err.without_url()
                ))
            })?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|err| {
            SendFailure::new(format!(
                "read Telegram response failed: {}",
                err.without_url()
            ))
        })?;
        parse_telegram_response(status, &body)
    }
}

#[async_trait]
impl NotificationSink for TelegramSink {
    async fn deliver(&self, notification: &QueuedNotification) -> Result<()> {
        let mut last_error = None;
        for attempt in 1..=self.retry_attempts {
            match self.send_once(notification).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let fallback_delay = self
                        .retry_base_delay
                        .saturating_mul(1_u32 << (attempt - 1).min(6));
                    let delay = err
                        .retry_after
                        .map(|value| value.saturating_add(Duration::from_millis(100)))
                        .unwrap_or(fallback_delay);
                    last_error = Some(err);
                    if attempt < self.retry_attempts {
                        warn!(
                            "Telegram delivery failed; retrying event_id={} attempt={}/{} delay_ms={} error={}",
                            notification.id,
                            attempt,
                            self.retry_attempts,
                            delay.as_millis(),
                            last_error.as_ref().expect("last error set")
                        );
                        sleep(delay).await;
                    }
                }
            }
        }
        let error = last_error
            .map(|value| value.to_string())
            .unwrap_or_else(|| "Telegram delivery did not run".to_string());
        Err(anyhow::anyhow!(error))
    }
}

#[derive(Debug, Serialize)]
struct SendMessagePayload<'a> {
    chat_id: &'a str,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<i64>,
    disable_notification: bool,
}

#[derive(Debug, Deserialize)]
struct TelegramApiResponse {
    ok: bool,
    #[serde(default)]
    error_code: Option<i64>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    parameters: Option<TelegramResponseParameters>,
}

#[derive(Debug, Deserialize)]
struct TelegramResponseParameters {
    #[serde(default)]
    retry_after: Option<u64>,
}

#[derive(Debug)]
struct SendFailure {
    message: String,
    retry_after: Option<Duration>,
}

impl SendFailure {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retry_after: None,
        }
    }
}

impl fmt::Display for SendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SendFailure {}

fn parse_telegram_response(http_status: u16, body: &str) -> std::result::Result<(), SendFailure> {
    let response: TelegramApiResponse = serde_json::from_str(body).map_err(|err| {
        SendFailure::new(format!(
            "parse Telegram response failed: {err}; status={http_status} body={}",
            truncate(body, 512)
        ))
    })?;
    if (200..300).contains(&http_status) && response.ok {
        return Ok(());
    }

    let code = response
        .error_code
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let description = response
        .description
        .unwrap_or_else(|| "unknown error".to_string());
    Err(SendFailure {
        message: format!(
            "Telegram API rejected request http_status={http_status} error_code={code} description={description}"
        ),
        retry_after: response
            .parameters
            .and_then(|value| value.retry_after)
            .map(Duration::from_secs),
    })
}

fn validate_bot_token(token: &str) -> Result<()> {
    let Some((bot_id, secret)) = token.split_once(':') else {
        bail!("invalid TELEGRAM_BOT_TOKEN format");
    };
    if bot_id.is_empty()
        || !bot_id.bytes().all(|value| value.is_ascii_digit())
        || secret.is_empty()
        || !secret
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-'))
    {
        bail!("invalid TELEGRAM_BOT_TOKEN format");
    }
    Ok(())
}

fn truncate(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_bot_token_shape_without_exposing_it() {
        assert!(validate_bot_token("123456:abc_DEF-123").is_ok());
        assert_eq!(
            validate_bot_token("not-a-token").unwrap_err().to_string(),
            "invalid TELEGRAM_BOT_TOKEN format"
        );
        assert!(validate_bot_token("123:bad/value").is_err());
    }

    #[test]
    fn accepts_success_response() {
        assert!(parse_telegram_response(200, r#"{"ok":true,"result":{"message_id":1}}"#).is_ok());
    }

    #[test]
    fn captures_retry_after_from_error_response() {
        let error = parse_telegram_response(
            429,
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":17}}"#,
        )
        .unwrap_err();
        assert_eq!(error.retry_after, Some(Duration::from_secs(17)));
        assert!(error.to_string().contains("error_code=429"));
    }

    #[test]
    fn serializes_plain_text_payload() {
        let payload = SendMessagePayload {
            chat_id: "-100123",
            text: "risk alert".to_string(),
            message_thread_id: Some(42),
            disable_notification: false,
        };
        assert_eq!(
            serde_json::to_value(payload).unwrap(),
            serde_json::json!({
                "chat_id": "-100123",
                "text": "risk alert",
                "message_thread_id": 42,
                "disable_notification": false
            })
        );
    }
}
