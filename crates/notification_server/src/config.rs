use anyhow::{bail, Context, Result};
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 18100;
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 500;
const DEFAULT_MAX_MESSAGE_CHARS: usize = 4_096;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub bot_token: Option<String>,
    pub chat_id: Option<String>,
    pub message_thread_id: Option<i64>,
    pub disable_notification: bool,
    pub api_token: Option<String>,
    pub queue_capacity: usize,
    pub request_timeout: Duration,
    pub retry_attempts: u32,
    pub retry_base_delay: Duration,
    pub max_message_chars: usize,
    pub dry_run: bool,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let host = env_string("NOTIFICATION_HOST").unwrap_or_else(|| DEFAULT_HOST.to_string());
        let host = IpAddr::from_str(&host)
            .with_context(|| format!("invalid NOTIFICATION_HOST: {host}"))?;
        let port = env_parse("NOTIFICATION_PORT", DEFAULT_PORT)?;
        let dry_run = env_bool("NOTIFICATION_DRY_RUN", false)?;
        let bot_token = env_string("TELEGRAM_BOT_TOKEN");
        let chat_id = env_string("TELEGRAM_CHAT_ID");
        if bot_token.is_none() && !dry_run {
            bail!("TELEGRAM_BOT_TOKEN is required unless NOTIFICATION_DRY_RUN=1");
        }
        if chat_id.is_none() && !dry_run {
            bail!("TELEGRAM_CHAT_ID is required unless NOTIFICATION_DRY_RUN=1");
        }

        let message_thread_id = env_optional_parse("TELEGRAM_MESSAGE_THREAD_ID")?;
        if message_thread_id.is_some_and(|value: i64| value <= 0) {
            bail!("TELEGRAM_MESSAGE_THREAD_ID must be greater than zero");
        }
        let queue_capacity = env_parse("NOTIFICATION_QUEUE_CAPACITY", DEFAULT_QUEUE_CAPACITY)?;
        if queue_capacity == 0 {
            bail!("NOTIFICATION_QUEUE_CAPACITY must be greater than zero");
        }
        let retry_attempts: u32 = env_parse("NOTIFICATION_RETRY_ATTEMPTS", DEFAULT_RETRY_ATTEMPTS)?;
        if !(1..=10).contains(&retry_attempts) {
            bail!("NOTIFICATION_RETRY_ATTEMPTS must be in [1, 10]");
        }
        let max_message_chars =
            env_parse("NOTIFICATION_MAX_MESSAGE_CHARS", DEFAULT_MAX_MESSAGE_CHARS)?;
        if !(256..=4_096).contains(&max_message_chars) {
            bail!("NOTIFICATION_MAX_MESSAGE_CHARS must be in [256, 4096]");
        }

        Ok(Self {
            bind_addr: SocketAddr::new(host, port),
            bot_token,
            chat_id,
            message_thread_id,
            disable_notification: env_bool("TELEGRAM_DISABLE_NOTIFICATION", false)?,
            api_token: env_string("NOTIFICATION_API_TOKEN"),
            queue_capacity,
            request_timeout: Duration::from_millis(env_parse(
                "NOTIFICATION_REQUEST_TIMEOUT_MS",
                DEFAULT_REQUEST_TIMEOUT_MS,
            )?),
            retry_attempts,
            retry_base_delay: Duration::from_millis(env_parse(
                "NOTIFICATION_RETRY_BASE_DELAY_MS",
                DEFAULT_RETRY_BASE_DELAY_MS,
            )?),
            max_message_chars,
            dry_run,
        })
    }
}

fn env_string(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let Some(raw) = env_string(name) else {
        return Ok(default);
    };
    raw.parse::<T>()
        .map_err(|err| anyhow::anyhow!("invalid {name}={raw}: {err}"))
}

fn env_optional_parse<T>(name: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    env_string(name)
        .map(|raw| {
            raw.parse::<T>()
                .map_err(|err| anyhow::anyhow!("invalid {name}={raw}: {err}"))
        })
        .transpose()
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    let Some(raw) = env_string(name) else {
        return Ok(default);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("invalid {name}={raw}: expected true/false or 1/0"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boolean_values() {
        let name = "NOTIFICATION_SERVER_TEST_BOOL";
        std::env::set_var(name, "YES");
        assert!(env_bool(name, false).unwrap());
        std::env::set_var(name, "0");
        assert!(!env_bool(name, true).unwrap());
        std::env::set_var(name, "invalid");
        assert!(env_bool(name, false).is_err());
        std::env::remove_var(name);
    }
}
