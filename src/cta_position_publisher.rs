//! Independent delivery of complete position targets to host-local Managers.
use anyhow::{ensure, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue};
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

fn normalize_url(raw: &str) -> Result<String> {
    let url = reqwest::Url::parse(raw.trim()).context("invalid CTA Manager URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "CTA Manager URL must use HTTP or HTTPS"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "CTA Manager URL must not contain credentials, a query or a fragment"
    );
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn manager_urls(plural: Option<&str>, single: Option<&str>) -> Result<Vec<String>> {
    let Some(raw) = plural.or(single) else {
        return Ok(Vec::new());
    };
    let mut urls = Vec::new();
    let mut seen = BTreeSet::new();
    for item in raw.split(',') {
        ensure!(
            !item.trim().is_empty(),
            "CTA Manager URL list contains an empty entry"
        );
        let url = normalize_url(item)?;
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }
    Ok(urls)
}

// Keep token values out of Debug, errors, status files and delivery journals.
struct Endpoint {
    url: String,
    token: Option<HeaderValue>,
}

fn endpoints(
    urls: Vec<String>,
    common_token: Option<&str>,
    per_url_tokens: Option<&str>,
) -> Result<Vec<Endpoint>> {
    let overrides: BTreeMap<String, String> = per_url_tokens
        .map(serde_json::from_str)
        .transpose()
        .context("CTA_PUBLISH_TOKENS must be a JSON object of Manager URLs to tokens")?
        .unwrap_or_default();
    let mut tokens = BTreeMap::new();
    for (url, token) in overrides {
        let url = normalize_url(&url)?;
        ensure!(
            urls.contains(&url),
            "CTA_PUBLISH_TOKENS contains an unconfigured Manager URL"
        );
        ensure!(
            tokens.insert(url, token).is_none(),
            "duplicate CTA Manager token override"
        );
    }
    urls.into_iter()
        .map(|url| {
            let token = tokens.get(&url).map(String::as_str).or(common_token);
            let token = token
                .map(|token| {
                    ensure!(!token.is_empty(), "CTA publish token must not be empty");
                    let mut value = HeaderValue::from_str(token)
                        .map_err(|_| anyhow::anyhow!("invalid CTA publish token header"))?;
                    value.set_sensitive(true);
                    Ok(value)
                })
                .transpose()?;
            Ok(Endpoint { url, token })
        })
        .collect()
}

#[derive(Clone)]
pub(crate) struct Publication {
    pub payload: Value,
    /// Observation-time journal data, never sent to Manager.
    pub context: Value,
}

pub(crate) struct Delivery {
    pub manager_url: String,
    pub publication: Publication,
    pub published_ts_us: i64,
}

#[derive(Clone, Default)]
struct Desired {
    generation: u64,
    publications: Option<Arc<BTreeMap<String, Publication>>>,
}

#[derive(Default)]
struct EndpointState {
    acknowledged: BTreeMap<String, (u64, Value)>,
    in_flight: Option<(u64, String, Value)>,
    errors: BTreeMap<String, (u64, String)>,
    last_success_ts_us: Option<i64>,
}

#[derive(Serialize)]
pub(crate) struct EndpointStatus {
    pub manager_url: String,
    pub pending_strategies: Vec<String>,
    pub last_success_ts_us: Option<i64>,
    pub error: Option<String>,
}

pub(crate) struct PositionPublisher {
    desired: watch::Sender<Desired>,
    states: Vec<(String, Arc<Mutex<EndpointState>>)>,
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    workers: Vec<JoinHandle<()>>,
}

impl PositionPublisher {
    pub fn from_env() -> Result<Self> {
        let plural = std::env::var("CTA_MANAGER_API_URLS").ok();
        let single = std::env::var("CTA_MANAGER_API_URL").ok();
        let urls = manager_urls(plural.as_deref(), single.as_deref())?;
        let common = std::env::var("CTA_PUBLISH_TOKEN").ok();
        let tokens = std::env::var("CTA_PUBLISH_TOKENS").ok();
        Self::new(
            endpoints(urls, common.as_deref(), tokens.as_deref())?,
            REQUEST_TIMEOUT,
            RETRY_INTERVAL,
        )
    }

    fn new(endpoints: Vec<Endpoint>, timeout: Duration, retry: Duration) -> Result<Self> {
        let clients = endpoints
            .into_iter()
            .map(|endpoint| {
                let mut headers = HeaderMap::new();
                if let Some(token) = endpoint.token {
                    headers.insert("X-CTA-Publish-Token", token);
                }
                let client = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .default_headers(headers)
                    .timeout(timeout)
                    .build()
                    .context("create CTA Manager publication client")?;
                Ok((endpoint.url, client))
            })
            .collect::<Result<Vec<_>>>()?;
        let (desired, _) = watch::channel(Desired::default());
        let (deliveries, receiver) = mpsc::unbounded_channel();
        let mut states = Vec::new();
        let mut workers = Vec::new();
        for (url, client) in clients {
            let state = Arc::new(Mutex::new(EndpointState::default()));
            states.push((url.clone(), state.clone()));
            workers.push(tokio::spawn(deliver_targets(
                url,
                client,
                desired.subscribe(),
                state,
                deliveries.clone(),
                retry,
            )));
        }
        Ok(Self {
            desired,
            states,
            deliveries: receiver,
            workers,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn submit(&self, publications: BTreeMap<String, Publication>) {
        self.desired
            .send_modify(|desired| desired.publications = Some(Arc::new(publications)));
    }

    pub fn pause(&self) {
        self.desired.send_if_modified(|desired| {
            if desired.publications.is_none() {
                return false;
            }
            desired.publications = None;
            desired.generation += 1;
            true
        });
    }

    pub fn reset(&self) {
        self.desired.send_modify(|desired| desired.generation += 1);
    }

    pub fn drain_deliveries(&mut self) -> Vec<Delivery> {
        let mut result = Vec::new();
        while let Ok(delivery) = self.deliveries.try_recv() {
            result.push(delivery);
        }
        result
    }

    pub fn statuses(&self) -> Vec<EndpointStatus> {
        let desired = self.desired.borrow();
        self.states
            .iter()
            .zip(&self.workers)
            .map(|((url, state), worker)| {
                let state = state.lock().unwrap_or_else(|error| error.into_inner());
                let pending = desired
                    .publications
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .filter(|(name, publication)| {
                                !acknowledged(
                                    &state,
                                    desired.generation,
                                    name,
                                    &publication.payload,
                                )
                            })
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let errors = pending
                    .iter()
                    .filter_map(|name| {
                        state
                            .errors
                            .get(name)
                            .filter(|(generation, _)| *generation == desired.generation)
                            .map(|(_, error)| format!("{name}: {error}"))
                    })
                    .collect::<Vec<_>>();
                let error = if worker.is_finished() {
                    Some("publication worker stopped".to_string())
                } else if errors.is_empty() {
                    None
                } else {
                    Some(errors.join("; "))
                };
                EndpointStatus {
                    manager_url: url.clone(),
                    pending_strategies: pending,
                    last_success_ts_us: state.last_success_ts_us,
                    error,
                }
            })
            .collect()
    }

    pub fn error(&self) -> Option<String> {
        let errors = self
            .statuses()
            .into_iter()
            .filter_map(|status| {
                status
                    .error
                    .map(|error| format!("{}: {error}", status.manager_url))
            })
            .collect::<Vec<_>>();
        (!errors.is_empty()).then(|| errors.join("; "))
    }
}

impl Drop for PositionPublisher {
    fn drop(&mut self) {
        for worker in &self.workers {
            worker.abort();
        }
    }
}

fn acknowledged(state: &EndpointState, generation: u64, name: &str, payload: &Value) -> bool {
    if state
        .in_flight
        .as_ref()
        .is_some_and(|(active_generation, active_name, active_payload)| {
            active_name == name && (*active_generation != generation || active_payload != payload)
        })
    {
        return false;
    }
    state
        .acknowledged
        .get(name)
        .is_some_and(|(previous_generation, previous)| {
            *previous_generation == generation && previous == payload
        })
}

async fn deliver_targets(
    manager_url: String,
    client: reqwest::Client,
    mut desired: watch::Receiver<Desired>,
    state: Arc<Mutex<EndpointState>>,
    deliveries: mpsc::UnboundedSender<Delivery>,
    retry: Duration,
) {
    let url = format!("{manager_url}/catalog/position-strategies");
    loop {
        let current = desired.borrow_and_update().clone();
        let names = current
            .publications
            .as_ref()
            .map(|items| items.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for name in names {
            // Re-read before every request; an offline Manager needs the newest
            // complete targets, not a backlog of superseded position changes.
            let current = desired.borrow_and_update().clone();
            let Some(publication) = current
                .publications
                .as_ref()
                .and_then(|items| items.get(&name))
                .cloned()
            else {
                continue;
            };
            if acknowledged(
                &state.lock().unwrap_or_else(|error| error.into_inner()),
                current.generation,
                &name,
                &publication.payload,
            ) {
                continue;
            }
            // Do not overlap client requests, including A -> B -> A while B
            // is awaiting acknowledgement. An HTTP timeout remains ambiguous.
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .in_flight = Some((
                current.generation,
                name.clone(),
                publication.payload.clone(),
            ));
            let result = client.post(&url).json(&publication.payload).send().await;
            let error = match result {
                Ok(response) if response.status().is_success() => None,
                Ok(response) => Some(format!("HTTP {}", response.status().as_u16())),
                Err(error) if error.is_timeout() => Some("request timed out".to_string()),
                Err(_) => Some("request unavailable".to_string()),
            };
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            state.in_flight = None;
            if let Some(error) = error {
                state.errors.insert(name, (current.generation, error));
            } else {
                let published_ts_us = get_timestamp_us();
                state.acknowledged.insert(
                    name.clone(),
                    (current.generation, publication.payload.clone()),
                );
                state.errors.remove(&name);
                state.last_success_ts_us = Some(published_ts_us);
                let _ = deliveries.send(Delivery {
                    manager_url: manager_url.clone(),
                    publication,
                    published_ts_us,
                });
            }
        }
        tokio::select! {
            changed = desired.changed() => { if changed.is_err() { return; } }
            _ = tokio::time::sleep(retry) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Semaphore;

    struct MockState {
        requests: Mutex<Vec<(Value, Option<String>)>>,
        fail: AtomicBool,
        stall: AtomicBool,
        release: Semaphore,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                fail: AtomicBool::new(false),
                stall: AtomicBool::new(false),
                release: Semaphore::new(0),
            }
        }
    }

    async fn receive(
        State(state): State<Arc<MockState>>,
        headers: HeaderMap,
        Json(payload): Json<Value>,
    ) -> StatusCode {
        let token = headers
            .get("X-CTA-Publish-Token")
            .map(|value| value.to_str().unwrap().to_string());
        state.requests.lock().unwrap().push((payload, token));
        if state.stall.load(Ordering::SeqCst) {
            state.release.acquire().await.unwrap().forget();
        }
        if state.fail.load(Ordering::SeqCst) {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        }
    }

    struct MockManager {
        url: String,
        state: Arc<MockState>,
        task: JoinHandle<()>,
    }

    impl MockManager {
        async fn new() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/api", listener.local_addr().unwrap());
            let state = Arc::new(MockState::default());
            let app = Router::new()
                .route("/api/catalog/position-strategies", post(receive))
                .with_state(state.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self { url, state, task }
        }

        fn requests(&self) -> Vec<Value> {
            self.state
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|(payload, _)| payload.clone())
                .collect()
        }
    }

    impl Drop for MockManager {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("publication condition was not met");
    }

    fn publication(qty: Value) -> BTreeMap<String, Publication> {
        BTreeMap::from([(
            "strategy".to_string(),
            Publication {
                payload: json!({"strategy_name":"cta_strategy","targets":{"BTCUSDT":{"qty":qty,"signal":0}}}),
                context: json!({"model_ts_ms":123,"private_journal_context":true}),
            },
        )])
    }

    fn publisher(managers: &[&MockManager]) -> PositionPublisher {
        PositionPublisher::new(
            endpoints(
                managers.iter().map(|manager| manager.url.clone()).collect(),
                None,
                None,
            )
            .unwrap(),
            Duration::from_secs(2),
            Duration::from_millis(40),
        )
        .unwrap()
    }

    #[test]
    fn url_list_overrides_single_and_normalizes_duplicates() {
        assert_eq!(manager_urls(Some(" http://localhost:4191/api/,https://example.com/api,http://localhost:4191/api "),Some("http://ignored/api")).unwrap(),
            ["http://localhost:4191/api","https://example.com/api"]);
        assert_eq!(
            manager_urls(None, Some("http://localhost/api/")).unwrap(),
            ["http://localhost/api"]
        );
        assert!(manager_urls(None, None).unwrap().is_empty());
        for bad in [
            "",
            "http://localhost/api,",
            "ftp://localhost/api",
            "http://user:secret@localhost/api",
            "https://example.com/api?token=secret",
            "https://example.com/api#fragment",
        ] {
            assert!(manager_urls(Some(bad), None).is_err());
        }
        assert!(manager_urls(Some(""), Some("http://localhost/api")).is_err());
    }

    #[test]
    fn invalid_token_configuration_fails_without_disclosing_tokens() {
        for overrides in [
            r#"{"https://unknown/api":"secret-value"}"#,
            r#"{"https://example.com/api":"secret\nvalue"}"#,
            r#"{"https://example.com/api":""}"#,
            r#"{"https://example.com/api":"a","https://example.com/api/":"b"}"#,
        ] {
            let error = endpoints(
                vec!["https://example.com/api".into()],
                None,
                Some(overrides),
            )
            .err()
            .unwrap();
            assert!(!format!("{error:#}").contains("secret"));
        }
    }

    #[tokio::test]
    async fn independent_token_headers_and_exact_target_contract() {
        let first = MockManager::new().await;
        let second = MockManager::new().await;
        let overrides = json!({format!("{}/",second.url):"second-token"}).to_string();
        let endpoints = endpoints(
            vec![first.url.clone(), second.url.clone()],
            Some("first-token"),
            Some(&overrides),
        )
        .unwrap();
        assert!(endpoints
            .iter()
            .all(|endpoint| endpoint.token.as_ref().unwrap().is_sensitive()));
        let mut publisher =
            PositionPublisher::new(endpoints, Duration::from_secs(2), Duration::from_millis(40))
                .unwrap();
        let target = publication(json!("clear"));
        let payload = target["strategy"].payload.clone();
        publisher.submit(target);
        until(|| {
            publisher
                .statuses()
                .iter()
                .all(|status| status.pending_strategies.is_empty())
        })
        .await;
        assert_eq!(first.requests(), vec![payload.clone()]);
        assert_eq!(second.requests(), vec![payload]);
        assert_eq!(
            first.state.requests.lock().unwrap()[0].1.as_deref(),
            Some("first-token")
        );
        assert_eq!(
            second.state.requests.lock().unwrap()[0].1.as_deref(),
            Some("second-token")
        );
        let status = serde_json::to_string(&publisher.statuses()).unwrap();
        assert!(!status.contains("first-token") && !status.contains("second-token"));
        let deliveries = publisher.drain_deliveries();
        assert_eq!(deliveries.len(), 2);
        assert!(deliveries
            .iter()
            .all(|delivery| delivery.publication.context["model_ts_ms"] == 123));
        assert!(publisher.error().is_none());
    }

    #[tokio::test]
    async fn failed_destination_retries_latest_without_repeating_successes() {
        let good = MockManager::new().await;
        let bad = MockManager::new().await;
        bad.state.fail.store(true, Ordering::SeqCst);
        let mut publisher = publisher(&[&bad, &good]);
        publisher.submit(publication(json!(1)));
        until(|| publisher.error().is_some() && good.requests().len() == 1).await;
        for _ in 0..3 {
            publisher.submit(publication(json!(1)));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(good.requests().len(), 1);
        publisher.submit(publication(json!(2)));
        publisher.submit(publication(json!(3)));
        until(|| good.requests().len() == 2).await;
        assert_eq!(good.requests()[1]["targets"]["BTCUSDT"]["qty"], 3);
        bad.state.fail.store(false, Ordering::SeqCst);
        until(|| {
            publisher
                .statuses()
                .iter()
                .all(|status| status.pending_strategies.is_empty())
        })
        .await;
        assert_eq!(
            bad.requests().last().unwrap()["targets"]["BTCUSDT"]["qty"],
            3
        );
        assert_eq!(good.requests().len(), 2);
        assert!(publisher.error().is_none());
        let delivered = publisher.drain_deliveries();
        assert_eq!(
            delivered
                .iter()
                .filter(|delivery| delivery.manager_url == bad.url)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stalled_destination_does_not_delay_new_targets_elsewhere() {
        let slow = MockManager::new().await;
        let good = MockManager::new().await;
        slow.state.stall.store(true, Ordering::SeqCst);
        let publisher = publisher(&[&slow, &good]);
        publisher.submit(publication(json!(1)));
        until(|| !slow.requests().is_empty() && !good.requests().is_empty()).await;
        publisher.submit(publication(json!(2)));
        tokio::time::timeout(
            Duration::from_millis(500),
            until(|| good.requests().len() == 2),
        )
        .await
        .unwrap();
        assert_eq!(slow.requests().len(), 1);
        slow.state.stall.store(false, Ordering::SeqCst);
        slow.state.release.add_permits(1);
        until(|| {
            publisher
                .statuses()
                .iter()
                .all(|status| status.pending_strategies.is_empty())
        })
        .await;
        assert_eq!(
            slow.requests().last().unwrap()["targets"]["BTCUSDT"]["qty"],
            2
        );
    }

    #[tokio::test]
    async fn timed_out_destination_recovers_with_latest_target() {
        let manager = MockManager::new().await;
        manager.state.stall.store(true, Ordering::SeqCst);
        let publisher = PositionPublisher::new(
            endpoints(vec![manager.url.clone()], None, None).unwrap(),
            Duration::from_millis(100),
            Duration::from_millis(40),
        )
        .unwrap();
        publisher.submit(publication(json!(1)));
        until(|| {
            publisher
                .error()
                .is_some_and(|error| error.contains("timed out"))
        })
        .await;
        manager.state.stall.store(false, Ordering::SeqCst);
        publisher.submit(publication(json!("clear")));
        until(|| publisher.statuses()[0].pending_strategies.is_empty()).await;
        assert_eq!(
            manager.requests().last().unwrap()["targets"]["BTCUSDT"]["qty"],
            "clear"
        );
        assert!(publisher.error().is_none());
        manager.state.release.add_permits(1);
    }

    #[tokio::test]
    async fn reverting_to_acknowledged_target_waits_for_in_flight_update() {
        let manager = MockManager::new().await;
        let publisher = publisher(&[&manager]);
        publisher.submit(publication(json!(1)));
        until(|| publisher.statuses()[0].pending_strategies.is_empty()).await;
        manager.state.stall.store(true, Ordering::SeqCst);
        publisher.submit(publication(json!(2)));
        until(|| manager.requests().len() == 2).await;
        publisher.submit(publication(json!(1)));
        assert_eq!(publisher.statuses()[0].pending_strategies, vec!["strategy"]);
        manager.state.stall.store(false, Ordering::SeqCst);
        manager.state.release.add_permits(1);
        until(|| {
            manager.requests().len() == 3 && publisher.statuses()[0].pending_strategies.is_empty()
        })
        .await;
        assert_eq!(
            manager
                .requests()
                .iter()
                .map(|request| request["targets"]["BTCUSDT"]["qty"].clone())
                .collect::<Vec<_>>(),
            vec![json!(1), json!(2), json!(1)]
        );
    }

    #[tokio::test]
    async fn pause_discards_pending_retries_and_resume_resends() {
        let manager = MockManager::new().await;
        manager.state.fail.store(true, Ordering::SeqCst);
        let publisher = publisher(&[&manager]);
        publisher.submit(publication(json!(1)));
        until(|| publisher.error().is_some()).await;
        publisher.pause();
        let count = manager.requests().len();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(manager.requests().len(), count);
        assert!(publisher.error().is_none());
        manager.state.fail.store(false, Ordering::SeqCst);
        publisher.submit(publication(json!(1)));
        until(|| publisher.statuses()[0].pending_strategies.is_empty()).await;
        let count = manager.requests().len();
        publisher.reset();
        until(|| {
            manager.requests().len() == count + 1
                && publisher.statuses()[0].pending_strategies.is_empty()
        })
        .await;
        drop(publisher);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(manager.requests().len(), count + 1);
    }
}
