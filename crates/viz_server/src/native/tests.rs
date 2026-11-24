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
        manager_source_id: None,
        account_snapshot_path: None,
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
fn saved_account_never_makes_a_restarted_counter_live() {
    let mut source = source("saved");
    source.account_snapshot_path = Some("/unused/native-account.bin".into());
    let hub = Hub::new(std::slice::from_ref(&source), 15000);
    let settled = snapshot(&source, 100, 1);
    hub.restore(&source, settled.clone()).unwrap();
    assert_eq!(
        frame(&hub)["saved_entry"]["positions"][0]["symbol"],
        "cu2612"
    );
    assert_eq!(frame(&hub)["saved_entry"]["can_trade"], false);
    assert!(!hub.latest()[0].contains(&source.expected_account_hash));
    assert!(hub.health()["sources"][0]["position_ready"].is_null());

    let mut waiting = snapshot(&source, 200, 1);
    waiting.position_ready = false;
    waiting.positions.clear();
    waiting.ready = false;
    hub.apply(&source, waiting).unwrap();
    assert_eq!(frame(&hub)["entry"]["position_ready"], false);
    assert_eq!(
        frame(&hub)["saved_entry"]["observed_us"],
        settled.observed_us
    );

    let mut flat = snapshot(&source, 200, 2);
    flat.positions.clear();
    hub.apply(&source, flat).unwrap();
    assert!(frame(&hub).get("saved_entry").is_none());
    assert_eq!(hub.health()["sources"][0]["position_rows"], 0);
}

#[test]
fn account_query_freshness_is_independent_of_snapshot_heartbeat_and_trade_permission() {
    let mut source = source("ctp");
    source.adapter = Adapter::Ctp;
    let hub = Hub::new(std::slice::from_ref(&source), 15000);
    let mut s = snapshot(&source, 100, 1);
    s.ready = false;
    s.reason = Text::new("settlement not confirmed; read-only queries remain available").unwrap();
    s.account = Some(cn_futures_state::AccountFunds {
        observed_us: s.observed_us - 1,
        trading_day: Text::new("20261123").unwrap(),
        currency: Text::new("CNY").unwrap(),
        pre_balance: 499999.72,
        deposit: 0.0,
        withdraw: 0.0,
        balance: 499999.72,
        available: 499999.72,
        withdraw_quota: 499899.72,
        curr_margin: 0.0,
        frozen_margin: 0.0,
        frozen_cash: 0.0,
        frozen_commission: 0.0,
        position_profit: 0.0,
        close_profit: 0.0,
        commission: 0.0,
    });
    hub.apply(&source, s.clone()).unwrap();
    assert_eq!(frame(&hub)["entry"]["account"]["balance"], 499999.72);
    assert_eq!(hub.health()["sources"][0]["account_fresh"], true);
    s.sequence += 1;
    s.observed_us = now_us();
    s.account.as_mut().unwrap().observed_us = s.observed_us - 60_000_000;
    hub.apply(&source, s.clone()).unwrap();
    assert_eq!(hub.health()["sources"][0]["fresh"], true);
    assert_eq!(hub.health()["sources"][0]["account_fresh"], false);
    s.sequence += 1;
    s.account = None;
    hub.apply(&source, s).unwrap();
    assert_eq!(hub.health()["sources"][0]["account_available"], false);
    assert!(frame(&hub)["entry"]["account"].is_null());
}

#[test]
fn account_funds_are_served_only_by_explicit_manager_identity() {
    let mut source = source("ctp");
    source.adapter = Adapter::Ctp;
    source.manager_source_id = Some("sh_dev_ctp01".into());
    let hub = Hub::new(std::slice::from_ref(&source), 15000);
    assert!(hub.account_funds("ctp").is_none());
    assert!(hub.account_funds("sh_dev_ctp01").unwrap()["account"].is_null());
    let mut s = snapshot(&source, 100, 1);
    s.account = Some(cn_futures_state::AccountFunds {
        observed_us: s.observed_us - 1,
        trading_day: Text::new("20261123").unwrap(),
        currency: Text::new("CNY").unwrap(),
        pre_balance: 500000.0,
        deposit: 100.0,
        withdraw: 50.0,
        balance: 501234.5,
        available: 400000.0,
        withdraw_quota: 399000.0,
        curr_margin: 90000.0,
        frozen_margin: 0.0,
        frozen_cash: 0.0,
        frozen_commission: 0.0,
        position_profit: 1000.0,
        close_profit: 200.0,
        commission: 15.5,
    });
    hub.apply(&source, s.clone()).unwrap();
    let funds = hub.account_funds("sh_dev_ctp01").unwrap();
    assert_eq!(funds["counter_id"], "ctp");
    assert_eq!(funds["adapter"], "ctp");
    assert_eq!(funds["observed_us"], s.observed_us);
    assert_eq!(funds["account"]["pre_balance"], 500000.0);
    assert_eq!(funds["account"]["trading_day"], "20261123");
    assert!(funds.get("account_hash").is_none() && funds["error"].is_null());
    hub.unavailable(&source, "等待持仓 IPC");
    assert_eq!(
        hub.account_funds("sh_dev_ctp01").unwrap()["error"],
        "等待持仓 IPC"
    );
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
fn relay_rejects_future_heartbeat_without_replacing_cached_observation() {
    let source = source("paper");
    let hub = Hub::new(std::slice::from_ref(&source), 10_000);
    let current = snapshot(&source, 100, 1);
    hub.apply(&source, current.clone()).unwrap();

    let mut future = snapshot(&source, 100, 2);
    future.observed_us = now_us() + 60_000_000;
    assert!(hub.apply(&source, future).is_err());
    assert_eq!(frame(&hub)["entry"]["observed_us"], current.observed_us);
    assert_eq!(frame(&hub)["entry"]["sequence"], "1");
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
        manager_port: 18211,
        cta_signal_port: None,
        l1: None,
        counters: vec![source],
    };
    cfg.validate().unwrap();
    for path in [
        "/",
        "/healthz",
        "/snapshot",
        "/account-funds/x",
        "/ws/:id",
        "/ws?x",
        "/ws\"</script>",
        "/行情",
    ] {
        cfg.http.ws_path = path.into();
        assert!(cfg.validate().is_err(), "{path}");
    }
    cfg.http.ws_path = "/ws".into();
    let mut other = cfg.counters[0].clone();
    other.id = "other".into();
    other.position_namespace = "other".into();
    cfg.counters.push(other);
    assert!(cfg.validate().is_err(), "one Viz per account environment");
    cfg.counters.clear();
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
    let positions = manager::Positions::new(&Config {
        http: cfg.clone(),
        stale_after_ms: 1000,
        manager_port: 18211,
        cta_signal_port: None,
        l1: None,
        counters: sources.clone(),
    })
    .unwrap();
    let app = server::router(&cfg, hub.clone(), positions);
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
    let response = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let html = response.text().await.unwrap();
    assert!(html.contains("const WS_PATH = \"/observe/live\""));
    assert!(html.contains("const STRATEGY_SOURCES = new Set([])"));
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
            let mut publisher =
                cn_futures_state::transport::StatePublisher::new(&source.position_namespace)
                    .unwrap();
            let hub = Hub::new(std::slice::from_ref(&source), 1000);
            let mut rx = hub.tx.subscribe();
            let handle = spawn(source.clone(), hub.clone(), None);
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

#[tokio::test]
async fn strategy_positions_forward_only_the_session_to_the_pinned_manager_source() {
    use axum::{
        http::{header, HeaderMap, StatusCode},
        routing::get,
        Json, Router,
    };
    let ledger = Router::new().route(
        "/manager/api/native/sources/manager-ctp/netting",
        get(|headers: HeaderMap| async move {
            if headers.get(header::COOKIE).and_then(|v| v.to_str().ok())
                != Some("cn_cta_session=allowed")
            {
                return Err(StatusCode::FORBIDDEN);
            }
            Ok(Json(json!({
                "portfolio": {
                    "source_id": "manager-ctp",
                    "strategy_groups": {"active":{"members":["active-1h","active-retired-1h","beta"]}},
                    "targets": {"active-1h":{"SHFE:rb2701":3},"active-retired-1h":{"SHFE:rb2701":0},"beta":{"SHFE:rb2701":-2}}
                },
                "net_targets":{"SHFE:rb2701":1},
                "exec_ledger":{"SHFE:rb2701":{"active-1h":2,"beta":-2}}
            })))
        }),
    )
    .route("/state", get(|headers: HeaderMap| async move {
        assert!(headers.get(header::COOKIE).is_none());
        Json(json!({
            "strategy_group":"held",
            "manager_publication":{"source_id":"another-account"},
            "factor_signals":{"market":"SHFE:rb2701","decision_ms":999,"rules":[{"factor":"other-account","target_lots":0}]},
            "products":{"RB":{"strategy_group":"active","manager_publication":{"source_id":"manager-ctp"},
                "factor_signals":{"market":"SHFE:rb2701","decision_ms":123,"rules":[
                    {"factor":"alpha","target_lots":3},{"factor":"beta","id":"flat","target_lots":0}
                ]}}}
        }))
    }));
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let manager_port = upstream.local_addr().unwrap().port();
    let upstream_task = tokio::spawn(async move { axum::serve(upstream, ledger).await.unwrap() });
    let mut ctp = source("ctp");
    ctp.manager_source_id = Some("manager-ctp".into());
    let local = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = local.local_addr().unwrap();
    let cfg = Config {
        http: crate::config::HttpCfg {
            bind: "127.0.0.1".into(),
            port: addr.port(),
            ws_path: "/ws".into(),
        },
        stale_after_ms: 1000,
        manager_port,
        cta_signal_port: Some(manager_port),
        l1: None,
        counters: vec![ctp],
    };
    let app = server::router(
        &cfg.http,
        Hub::new(&cfg.counters, 1000),
        manager::Positions::new(&cfg).unwrap(),
    );
    let local_task = tokio::spawn(async move { axum::serve(local, app).await.unwrap() });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let ctp_url = format!("http://{addr}/strategy-positions/ctp");
    for (cookie, expected) in [
        ("theme=dark", StatusCode::UNAUTHORIZED),
        ("cn_cta_session=denied", StatusCode::FORBIDDEN),
    ] {
        let response = client
            .get(&ctp_url)
            .header(header::COOKIE, cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    let result = client
        .get(&ctp_url)
        .header(header::COOKIE, "theme=dark; cn_cta_session=allowed")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(result.headers()[header::CACHE_CONTROL], "no-store");
    let value: Value = serde_json::from_slice(&result.bytes().await.unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"SHFE:rb2701": {
            "target_lots":1,"attributed_lots":0, "strategies":[
                {"name":"active-1h","signal_target_lots":3,"target_lots":3,"attributed_lots":2},
                {"name":"beta","signal_target_lots":-2,"target_lots":-2,"attributed_lots":-2}
            ], "factor_signals":{"market":"SHFE:rb2701","decision_ms":123,"rules":[
                {"factor":"alpha","id":null,"target_lots":3},{"factor":"beta","id":"flat","target_lots":0}
            ]}
        }})
    );
    assert_eq!(
        client
            .get(format!("http://{addr}/strategy-positions/paper"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    local_task.abort();
    upstream_task.abort();
}
