use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;
use url::Url;

const DEFAULT_TIMEOUT_MS: u64 = 250;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NotificationSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NotificationRequest {
    pub source: String,
    pub title: String,
    pub message: String,
    pub severity: NotificationSeverity,
    pub fields: BTreeMap<String, String>,
    pub dedup_key: Option<String>,
}

#[derive(Clone)]
pub struct LocalNotificationClient {
    endpoint: NotificationEndpoint,
    api_token: Option<String>,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct NotificationEndpoint {
    addresses: Vec<SocketAddr>,
    host_header: String,
    path_and_query: String,
}

impl fmt::Debug for LocalNotificationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalNotificationClient")
            .field("endpoint", &self.endpoint)
            .field("auth", &self.api_token.is_some())
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl LocalNotificationClient {
    pub fn from_env() -> Result<Self> {
        let url = std::env::var("PRE_TRADE_NOTIFICATION_URL")
            .context("PRE_TRADE_NOTIFICATION_URL is required")?;
        let url = url.trim();
        if url.is_empty() {
            bail!("PRE_TRADE_NOTIFICATION_URL must not be empty");
        }
        let api_token = std::env::var("NOTIFICATION_API_TOKEN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let timeout_ms = std::env::var("PRE_TRADE_NOTIFICATION_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        Self::new(url, api_token, Duration::from_millis(timeout_ms))
    }

    pub(crate) fn new(url: &str, api_token: Option<String>, timeout: Duration) -> Result<Self> {
        let url = Url::parse(url).with_context(|| format!("invalid notification URL: {url}"))?;
        if url.scheme() != "http" {
            bail!("notification URL must use http");
        }
        if url.username() != "" || url.password().is_some() {
            bail!("notification URL must not contain credentials");
        }
        let host = url
            .host_str()
            .context("notification URL is missing a host")?;
        let port = url
            .port_or_known_default()
            .context("notification URL is missing a port")?;
        let addresses = (host, port)
            .to_socket_addrs()
            .with_context(|| format!("resolve notification endpoint {host}:{port}"))?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            bail!("notification endpoint resolved to no addresses");
        }
        if addresses.iter().any(|address| !address.ip().is_loopback()) {
            bail!("notification endpoint must resolve only to loopback addresses");
        }
        if api_token
            .as_deref()
            .is_some_and(|token| token.contains(['\r', '\n']))
        {
            bail!("notification API token contains invalid characters");
        }

        let host_header = match url.host() {
            Some(url::Host::Ipv6(address)) => format!("[{address}]:{port}"),
            _ => format!("{host}:{port}"),
        };
        let mut path_and_query = url.path().to_string();
        if path_and_query.is_empty() {
            path_and_query.push('/');
        }
        if let Some(query) = url.query() {
            path_and_query.push('?');
            path_and_query.push_str(query);
        }

        Ok(Self {
            endpoint: NotificationEndpoint {
                addresses,
                host_header,
                path_and_query,
            },
            api_token,
            timeout,
        })
    }

    pub fn send(&self, notification: &NotificationRequest) -> Result<()> {
        let body = serde_json::to_vec(notification).context("serialize notification request")?;
        let mut stream = self.connect()?;
        stream
            .set_read_timeout(Some(self.timeout))
            .context("set notification read timeout")?;
        stream
            .set_write_timeout(Some(self.timeout))
            .context("set notification write timeout")?;

        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.endpoint.path_and_query,
            self.endpoint.host_header,
            body.len()
        )
        .context("write notification request headers")?;
        if let Some(token) = self.api_token.as_deref() {
            write!(stream, "Authorization: Bearer {token}\r\n")
                .context("write notification authorization header")?;
        }
        stream
            .write_all(b"\r\n")
            .context("finish notification request headers")?;
        stream
            .write_all(&body)
            .context("write notification request body")?;
        stream.flush().context("flush notification request")?;

        let mut status_line = String::new();
        BufReader::new(stream)
            .read_line(&mut status_line)
            .context("read notification response status")?;
        let status = status_line
            .split_ascii_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .with_context(|| format!("invalid notification response: {}", status_line.trim()))?;
        if status != 202 {
            bail!("notification server returned HTTP {status}");
        }
        Ok(())
    }

    fn connect(&self) -> Result<TcpStream> {
        let mut last_error = None;
        for address in &self.endpoint.addresses {
            match TcpStream::connect_timeout(address, self.timeout) {
                Ok(stream) => return Ok(stream),
                Err(err) => last_error = Some(err),
            }
        }
        let error = last_error.context("notification endpoint has no resolved address")?;
        Err(error).context("connect to local notification server")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn sends_structured_request_and_requires_accepted_status() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            let expected_body = b"\"title\":\"FR warning\"";
            while !request
                .windows(expected_body.len())
                .any(|window| window == expected_body)
            {
                let read = stream.read(&mut buffer).unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            let text = String::from_utf8_lossy(&request);
            assert!(text.starts_with("POST /v1/notify HTTP/1.1\r\n"));
            assert!(text.contains("Authorization: Bearer secret\r\n"));
            assert!(text.contains("\"title\":\"FR warning\""));
            stream
                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
        });

        let client = LocalNotificationClient::new(
            &format!("http://{address}/v1/notify"),
            Some("secret".to_string()),
            Duration::from_secs(5),
        )
        .unwrap();
        client
            .send(&NotificationRequest {
                source: "test".to_string(),
                title: "FR warning".to_string(),
                message: "test message".to_string(),
                severity: NotificationSeverity::Warning,
                fields: BTreeMap::new(),
                dedup_key: None,
            })
            .unwrap();
        server.join().unwrap();
    }

    #[test]
    fn rejects_non_loopback_endpoint() {
        let result = LocalNotificationClient::new(
            "http://192.0.2.1:18100/v1/notify",
            None,
            Duration::from_millis(10),
        );
        assert!(result.is_err());
    }
}
