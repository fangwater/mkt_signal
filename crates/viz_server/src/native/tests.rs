use super::*;
use cn_futures_state::{Adapter, Position, PositionRow, Side, Snapshot, Text, Venue};
use futures_util::StreamExt;
use serde_json::{json, Value};

fn source(id: &str) -> Counter {
    Counter {
        id: id.into(),
        label: "易达观察".into(),
        account: "paper".into(),
        adapter: Adapter::Yida,
        position_namespace: format!("test_native_{}_{}", id, now_us()),
        expected_account_hash: "a".repeat(64),
    }
}
fn snapshot(source: &Counter, epoch: u64, sequence: u64) -> Snapshot {
    let mut s = Snapshot::empty(source.adapter, &source.expected_account_hash).unwrap();
    s.publisher_epoch = epoch;
    s.sequence = sequence;
    s.observed_us = now_us();
    s.trading_day = Text::new("20261112").unwrap();
    s.position_ready = true;
    s.connected = true;
    s.ready = true;
    s.transport_ready = true;
    s.available_local_budget = 1234567.89;
    s.positions.push(PositionRow {
        venue: Venue::Shfe,
        symbol: Text::new("cu2612").unwrap(),
        side: Side::Buy,
        position: Position {
            today: 2,
            yesterday: 3,
            frozen_today: 1,
            frozen_yesterday: 0,
        },
    });
    s
}
fn frame(hub: &Hub) -> Value {
    serde_json::from_str(&hub.latest()[0]).unwrap()
}

#[test]
fn relay_preserves_producer_fields_replaces_full_state_and_pins_identity() {
    let source = source("paper");
    let hub = Hub::new(std::slice::from_ref(&source), 1000);
    assert!(frame(&hub)["entry"].is_null());
    let s = snapshot(&source, u64::MAX, 1);
    let mut expected = serde_json::to_value(&s).unwrap();
    expected.as_object_mut().unwrap().remove("account_hash");
    expected["publisher_epoch"] = json!(u64::MAX.to_string());
    expected["sequence"] = json!("1");
    expected["replaced_snapshots"] = json!("0");
    expected["l1_dropped"] = json!("0");
    hub.apply(&source, s).unwrap();
    assert_eq!(frame(&hub)["entry"], expected);
    assert!(!hub.latest()[0].contains(&source.expected_account_hash));
    let mut bad = snapshot(&source, u64::MAX, 2);
    bad.account_hash = Text::new(&"b".repeat(64)).unwrap();
    assert!(hub.apply(&source, bad).is_err());
    assert!(hub.apply(&source, snapshot(&source, u64::MAX, 0)).is_err());
    let mut flat = snapshot(&source, u64::MAX, 2);
    flat.positions.clear();
    hub.apply(&source, flat).unwrap();
    assert!(frame(&hub)["entry"]["positions"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(frame(&hub)["entry"]["position_ready"], true);
    assert!(hub.apply(&source, snapshot(&source, u64::MAX, 1)).is_err());
    let mut bootstrap = snapshot(&source, 123, 1);
    bootstrap.position_ready = false;
    bootstrap.positions.clear();
    hub.apply(&source, bootstrap).unwrap();
    assert_eq!(frame(&hub)["entry"]["position_ready"], false);
    hub.unavailable(&source, "IPC unavailable");
    assert_eq!(frame(&hub)["entry"]["publisher_epoch"], "123");
    assert_eq!(frame(&hub)["error"], "IPC unavailable");
    assert!(hub.health()["sources"][0].get("positions").is_none());
}

#[test]
fn native_config_has_one_ipc_contract_and_safe_ws_path() {
    let source = source("paper");
    let mut cfg = Config {
        http: crate::config::HttpCfg {
            bind: "127.0.0.1".into(),
            port: 8811,
            ws_path: "/observe/live".into(),
        },
        stale_after_ms: 1000,
        counters: vec![source],
    };
    cfg.validate().unwrap();
    for path in [
        "/",
        "/healthz",
        "/snapshot",
        "/ws/:id",
        "/ws?x",
        "/ws\"</script>",
        "/行情",
    ] {
        cfg.http.ws_path = path.into();
        assert!(cfg.validate().is_err(), "{path}");
    }
    cfg.http.ws_path = "/ws".into();
    cfg.counters.push(cfg.counters[0].clone());
    assert!(cfg.validate().is_err());
    let text = include_str!("../../../../config/cn_futures_viz.example.toml")
        .replace("<64-character account hash>", &"a".repeat(64));
    let cfg: Config = toml::from_str(&text).unwrap();
    cfg.validate().unwrap();
    assert!(toml::from_str::<Config>(&format!("poll_interval_ms = 1000\n{text}")).is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn direct_ws_replays_each_source_and_forwards_without_flush_timer_or_rest_bootstrap() {
    let sources = vec![source("a"), source("b")];
    let hub = Hub::new(&sources, 1000);
    for source in &sources {
        hub.apply(source, snapshot(source, 123, 1)).unwrap();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = crate::config::HttpCfg {
        bind: "127.0.0.1".into(),
        port: addr.port(),
        ws_path: "/observe/live".into(),
    };
    let app = server::router(&cfg, hub.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/observe/live"))
        .await
        .unwrap();
    let mut ids = std::collections::HashSet::new();
    for _ in 0..2 {
        let raw = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let f: Value = serde_json::from_str(raw.to_text().unwrap()).unwrap();
        ids.insert(f["source_id"].as_str().unwrap().to_owned());
    }
    assert_eq!(ids.len(), 2);
    hub.apply(&sources[0], snapshot(&sources[0], 123, 2))
        .unwrap();
    let raw = tokio::time::timeout(Duration::from_millis(250), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let f: Value = serde_json::from_str(raw.to_text().unwrap()).unwrap();
    assert_eq!(f["entry"]["sequence"], "2");
    let client = reqwest::Client::new();
    let html = client
        .get(format!("http://{addr}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(html.contains("const WS_PATH = \"/observe/live\""));
    assert!(!html.contains("fetch("));
    assert!(!html.contains("data-activity"));
    assert!(html.contains("人民币元") && html.contains("沪铜"));
    for path in ["/snapshot", "/orders", "/cancel", "/config/"] {
        assert_eq!(
            client
                .get(format!("http://{addr}{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    assert_eq!(
        client
            .post(format!("http://{addr}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    task.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn ipc_listener_relays_common_state_without_sdk_or_database() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let source = source("ipc");
            let publisher =
                cn_futures_state::transport::StatePublisher::new(&source.position_namespace)
                    .unwrap();
            let hub = Hub::new(std::slice::from_ref(&source), 1000);
            let mut rx = hub.tx.subscribe();
            let handle = spawn(source.clone(), hub.clone());
            tokio::time::timeout(Duration::from_secs(3), async {
                let mut seq = 0;
                loop {
                    seq += 1;
                    publisher.publish(&snapshot(&source, 123, seq)).unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if !frame(&hub)["entry"].is_null() {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            let raw = rx.recv().await.unwrap();
            let frame: Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(frame["entry"]["positions"][0]["position"]["today"], 2);
            assert_eq!(frame["entry"]["can_trade"], false);
            assert_eq!(hub.health()["sources"][0]["fresh"], true);
            handle.abort();
        })
        .await;
}
