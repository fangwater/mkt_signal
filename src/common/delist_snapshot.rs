//! Explicit loopback Viz targets for internal Delist reads, independent of web auth.

use anyhow::{ensure, Context, Result};
use reqwest::{redirect::Policy, Client, Url};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, net::IpAddr, path::Path, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    snapshots: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct SnapshotClient {
    client: Client,
    targets: BTreeMap<String, Url>,
}

impl SnapshotClient {
    pub fn load(path: &Path) -> Result<Self> {
        let config: Config = toml::from_str(
            &std::fs::read_to_string(path)
                .with_context(|| format!("read snapshot targets {}", path.display()))?,
        )
        .context("parse snapshot targets")?;
        Self::from_urls(config.snapshots)
    }

    pub fn from_urls(targets: BTreeMap<String, String>) -> Result<Self> {
        let mut parsed = BTreeMap::new();
        for (slug, value) in targets {
            ensure!(
                !slug.is_empty()
                    && slug.len() <= 128
                    && slug
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-')),
                "invalid snapshot environment name"
            );
            let url =
                Url::parse(&value).with_context(|| format!("invalid Viz target for {slug}"))?;
            let loopback = url
                .host_str()
                .and_then(|host| host.parse::<IpAddr>().ok())
                .is_some_and(|ip| ip.is_loopback());
            ensure!(
                url.scheme() == "http"
                    && loopback
                    && url.port().is_some_and(|port| port != 0)
                    && url.path() == "/snapshot"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "Viz target for {slug} must be http://<loopback>:<port>/snapshot without credentials or query"
            );
            parsed.insert(slug, url);
        }
        Ok(Self {
            client: Client::builder()
                .no_proxy()
                .redirect(Policy::none())
                .build()?,
            targets: parsed,
        })
    }

    pub fn environments(&self) -> impl Iterator<Item = &str> {
        self.targets.keys().map(String::as_str)
    }

    pub async fn fetch(&self, slug: &str, timeout: Duration) -> Result<Value> {
        let url = self
            .targets
            .get(slug)
            .with_context(|| format!("no private Viz snapshot target configured for {slug}"))?;
        let response = self
            .client
            .get(url.clone())
            .timeout(timeout)
            .send()
            .await
            .with_context(|| format!("fetch Viz snapshot failed: account={slug} url={url}"))?;
        ensure!(
            response.status().is_success(),
            "Viz snapshot HTTP {}: account={slug} url={url}",
            response.status()
        );
        let mut snapshot: Value = response
            .json()
            .await
            .with_context(|| format!("decode Viz snapshot failed: account={slug}"))?;
        let entries = snapshot
            .get_mut("entries")
            .and_then(Value::as_array_mut)
            .context("Viz snapshot entries missing")?;
        entries.retain(|entry| entry.get("namespace").and_then(Value::as_str) == Some(slug));
        ensure!(
            entries.iter().any(|entry| matches!(
                entry.get("channel").and_then(Value::as_str),
                Some("pre_trade_exposure" | "pre_trade_risk")
            )),
            "no pre_trade snapshot for expected namespace {slug}"
        );
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{response::Redirect, routing::get, Json, Router};
    use serde_json::json;

    #[test]
    fn rejects_gateway_public_and_credential_bearing_targets() {
        for target in [
            "http://127.0.0.1:4191/fr/gate_fr_arb01/snapshot",
            "http://13.115.227.29:20121/snapshot",
            "http://user:password@127.0.0.1:20121/snapshot",
            "http://127.0.0.1:20121/snapshot?account=other",
            "http://127.0.0.1:20121/snapshot#other",
        ] {
            assert!(SnapshotClient::from_urls(BTreeMap::from([(
                "gate_fr_arb01".into(),
                target.into()
            )]))
            .is_err());
        }
    }

    #[tokio::test]
    async fn direct_viz_requires_exact_namespace_and_does_not_follow_redirects() {
        let app = Router::new().route("/snapshot", get(|| async {
            Json(json!({"entries":[
                {"namespace":"gate_fr_arb01","channel":"pre_trade_exposure","entry":{"ts_ms":1}},
                {"namespace":"other","channel":"pre_trade_risk","entry":{"ts_ms":2}}
            ]}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}/snapshot", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = SnapshotClient::from_urls(BTreeMap::from([
            ("gate_fr_arb01".into(), target.clone()),
            ("wrong".into(), target),
        ]))
        .unwrap();
        let value = client
            .fetch("gate_fr_arb01", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(value["entries"].as_array().unwrap().len(), 1);
        assert!(client
            .fetch("wrong", Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("namespace"));
        assert!(client
            .fetch("missing", Duration::from_secs(1))
            .await
            .is_err());
        let app = Router::new().route(
            "/snapshot",
            get(|| async { Redirect::temporary("http://127.0.0.1:1/snapshot") }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = format!("http://{}/snapshot", listener.local_addr().unwrap());
        let redirect_server =
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client =
            SnapshotClient::from_urls(BTreeMap::from([("gate_fr_arb01".into(), target)])).unwrap();
        assert!(client
            .fetch("gate_fr_arb01", Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string()
            .contains("307"));
        server.abort();
        redirect_server.abort();
    }
}
