use super::*;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};

struct Mock {
    requests: Vec<Value>,
    members: BTreeSet<String>,
    fail_first: bool,
}
async fn metadata(State(state): State<Arc<Mutex<Mock>>>) -> Json<Value> {
    Json(
        json!({"source_id":"native-account","strategy_group":"cn-futures-01","revision":7,
        "members":state.lock().unwrap().members,"contracts":["SHFE:sn2611","SHFE:sn2612"],"external_demand_open":false}),
    )
}
async fn receive(
    State(state): State<Arc<Mutex<Mock>>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(headers["X-CTA-Publish-Token"], "test-secret");
    let mut state = state.lock().unwrap();
    state.requests.push(body.clone());
    if state.fail_first && state.requests.len() == 1 {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":"ack lost after accepting"})),
        );
    }
    (
        StatusCode::OK,
        Json(
            json!({"source_id":"native-account","strategy_group":"cn-futures-01","batch_id":body["batch_id"]}),
        ),
    )
}
fn desired() -> Desired {
    let now = chrono::Utc::now().timestamp_micros();
    let positions = (0..10)
        .map(|i| {
            (
                format!("cn-futures-01-factor_{i:03}-1h"),
                StrategyPosition {
                    decision_ms: now / 1000,
                    targets: BTreeMap::from([(
                        "SHFE:sn2612".into(),
                        if i % 2 == 0 { 1 } else { -1 },
                    )]),
                },
            )
        })
        .collect();
    Desired {
        signature: signature(&positions).unwrap(),
        positions,
        trigger_us: now,
        trading_day: Some(20261118),
        live_ready: true,
        quotes: BTreeMap::from([(
            "SHFE:sn2612".into(),
            Quote {
                bid: 100.0,
                ask: 102.0,
                observed_us: now,
                source: "fixture_bbo".into(),
            },
        )]),
    }
}
async fn server(desired: &Desired, fail_first: bool) -> (String, Arc<Mutex<Mock>>, JoinHandle<()>) {
    let state = Arc::new(Mutex::new(Mock {
        requests: vec![],
        members: desired.positions.keys().cloned().collect(),
        fail_first,
    }));
    let app = Router::new()
        .route(
            "/api/native/sources/native-account/netting/publication",
            get(metadata),
        )
        .route(
            "/api/native/sources/native-account/target-batches",
            post(receive),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/api", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (address, state, task)
}
fn settings(url: String) -> PublicationConfig {
    PublicationConfig {
        enabled: true,
        manager_url: url,
        source_id: "native-account".into(),
        token_env: "unused_test_env".into(),
        include_trading_day: true,
        per_factor_intervals: BTreeSet::from(["1h".into()]),
    }
}

#[tokio::test]
async fn ten_factors_are_one_request_with_a_complete_contract_vector() {
    let desired = desired();
    let (url, state, task) = server(&desired, false).await;
    let temp = tempfile::tempdir().unwrap();
    let mut client = Client::with_token(
        &settings(url),
        "cn-futures-01".into(),
        temp.path().join("outbox.json"),
        "test-secret",
    )
    .unwrap();
    client.prepare(&desired).await.unwrap();
    assert!(client.deliver().await.unwrap().is_some());
    let state = state.lock().unwrap();
    assert_eq!(state.requests.len(), 1);
    let request = &state.requests[0];
    assert_eq!(request["strategies"].as_object().unwrap().len(), 10);
    assert!(request["strategies"].get("cn-futures-01-1h").is_none());
    assert_eq!(request["signal_times"].as_object().unwrap().len(), 10);
    assert_eq!(request["expected_revision"], 7);
    assert_eq!(request["trading_day"], "20261118");
    for value in request["strategies"].as_object().unwrap().values() {
        assert_eq!(value["SHFE:sn2611"], 0);
    }
    assert_eq!(
        request["strategies"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v["SHFE:sn2612"].as_i64().unwrap())
            .sum::<i64>(),
        0
    );
    task.abort();
}

#[tokio::test]
async fn lost_ack_retries_the_identical_batch_after_process_restart() {
    let desired = desired();
    let (url, state, task) = server(&desired, true).await;
    let settings = settings(url);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("outbox.json");
    let mut original = Client::with_token(
        &settings,
        "cn-futures-01".into(),
        path.clone(),
        "test-secret",
    )
    .unwrap();
    original.prepare(&desired).await.unwrap();
    assert!(original.deliver().await.is_err());
    drop(original);
    assert!(!std::fs::read_to_string(&path)
        .unwrap()
        .contains("test-secret"));
    let mut restarted =
        Client::with_token(&settings, "cn-futures-01".into(), path, "test-secret").unwrap();
    assert!(restarted.deliver().await.unwrap().is_some());
    let state = state.lock().unwrap();
    assert_eq!(state.requests.len(), 2);
    assert_eq!(state.requests[0], state.requests[1]);
    task.abort();
}

#[tokio::test]
async fn incomplete_group_and_outbox_identity_changes_are_rejected_without_publication() {
    let desired = desired();
    let (url, state, task) = server(&desired, false).await;
    let settings = settings(url);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("outbox.json");
    let mut client = Client::with_token(
        &settings,
        "cn-futures-01".into(),
        path.clone(),
        "test-secret",
    )
    .unwrap();
    let mut incomplete = desired.clone();
    incomplete.positions.pop_first();
    assert!(client.prepare(&incomplete).await.is_err());
    assert!(state.lock().unwrap().requests.is_empty());
    client.prepare(&desired).await.unwrap();
    assert!(Client::with_token(&settings, "cn-futures-02".into(), path, "test-secret").is_err());
    task.abort();
}

#[tokio::test]
async fn undurable_or_stale_decisions_are_never_sent() {
    let desired = desired();
    let (url, state, task) = server(&desired, false).await;
    let settings = settings(url);
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("blocked");
    let mut client = Client::with_token(
        &settings,
        "cn-futures-01".into(),
        parent.join("outbox.json"),
        "test-secret",
    )
    .unwrap();
    std::fs::write(parent, b"not a directory").unwrap();
    assert!(client.prepare(&desired).await.is_err());
    assert!(client.outbox.pending.is_none());
    assert!(state.lock().unwrap().requests.is_empty());
    let mut expired = desired.clone();
    expired.trigger_us -= 180_000_000;
    assert!(client.prepare(&expired).await.is_err());
    assert!(client.outbox.pending.is_none());
    task.abort();
}
