use super::config::{Adapter, Counter, Environment, MarketData};
use super::state::{Lifecycle, OrderPhase};
use super::*;
use persist_common::cn_futures::*;
use persist_manager::cn_futures::{Evidence, NativeStore};
use std::path::Path;

fn event(seq: u64, value: Value) -> Evidence {
    Evidence {
        sequence: seq,
        received_us: 1_000_000 + seq,
        value,
        projection_error: None,
    }
}
fn ready() -> Value {
    json!({"type":"reconciled","state":{"ready":true,"connected":true,"reason":"","trading_day":"20261113",
        "available_local_budget":1000.0,"positions":[{"venue":"CFFEX","symbol":"IF2612","side":"buy",
        "position":{"today":1,"yesterday":2,"frozen_today":0,"frozen_yesterday":1}}]}})
}
fn counter(path: &Path, secondary: &Path) -> Counter {
    Counter {
        id: "ctp-sim".into(),
        label: "仿真柜台".into(),
        account: "test-alias".into(),
        adapter: Adapter::Ctp,
        rocksdb_path: path.into(),
        secondary_path: secondary.into(),
        expected_account_hash: Some("a".repeat(64)),
    }
}
fn record() -> NativeOrderRecord {
    let key = OrderKey {
        session: "ctp:12:34".into(),
        order_ref: "18446744073709551614".into(),
    };
    let request = Intent {
        venue: Venue::Cffex,
        symbol: "IF2612".into(),
        side: Side::Buy,
        offset: Offset::Open,
        lots: 3,
        price_type: PriceType::Limit,
        price_ticks: 20_000,
        time_in_force: TimeInForce::Day,
        execution_route: None,
    };
    NativeOrderRecord {
        identity: PersistedOrderKey {
            trading_day: "20261113".into(),
            venue: Venue::Cffex,
            native: key.clone(),
        },
        order: Order {
            request,
            key,
            system_id: "18446744073709551613".into(),
            limit_price: 4000.0,
            status: OrderStatus::Working,
            filled: 0,
            reported_filled: 0,
            cancelled: 0,
            reserved_today: 0,
            reserved_yesterday: 0,
            local_cash_held: 300.0,
            cash_reserved_per_lot: 100.0,
            cancel_pending: false,
            owned: false,
        },
        submitted_intent: None,
        locally_submitted: true,
        send_state: SendState::Observed,
        instrument: None,
        first_received_us: 1_000_000,
        updated_us: 1_000_002,
        evidence_seq: 0,
    }
}
#[test]
fn lifecycle_later_disconnect_restart_transport_and_staleness_override_recovery() {
    let mut s = Lifecycle::default();
    s.apply(&event(1, json!({"type":"runtime_start","execute":true})))
        .unwrap();
    s.apply(&event(2, ready())).unwrap();
    assert_eq!(s.view(1_000_010, 1000).status, "observing");
    assert_eq!(s.view(1_000_010, 1000).reported_can_trade, None);
    s.apply(&event(3, json!({"type":"ctp_event","event":{"kind":2}})))
        .unwrap();
    assert_eq!(s.view(1_000_010, 1000).status, "disconnected");
    s.apply(&event(4, json!({"type":"runtime_start","execute":false})))
        .unwrap();
    let v = s.view(1_000_010, 1000);
    assert!(!v.snapshot_current_runtime);
    assert_eq!(v.status, "recovering");
    s.apply(&event(5, ready())).unwrap();
    s.apply(&event(6, json!({"type":"transport_status","ready":false})))
        .unwrap();
    assert_eq!(s.view(1_000_010, 1000).status, "transport_unavailable");
    s.apply(&event(7, json!({"type":"transport_status","ready":true})))
        .unwrap();
    assert_eq!(s.view(1_000_100, 1000).status, "transport_unavailable");
    s.apply(&event(8, ready())).unwrap();
    assert_eq!(s.view(4_000_000, 1000).status, "stale");
    s.apply(&event(9, json!({"type":"runtime_stopped"})))
        .unwrap();
    assert_eq!(s.view(4_000_000, 1000).status, "stopped");
    assert_eq!(s.view(4_000_000, 1000).transport_ready, None);
}
#[test]
fn yida_replay_is_not_disconnect_and_blocking_evidence_clears_only_on_reconciliation() {
    let mut s = Lifecycle::default();
    s.apply(&event(1, ready())).unwrap();
    s.apply(&event(
        2,
        json!({"type":"yida_event","event":{"kind":2,"replay":1}}),
    ))
    .unwrap();
    assert_eq!(s.view(1_000_100, 1000).status, "observing");
    s.apply(&event(
        3,
        json!({"type":"yida_event","event":{"kind":14,"replay":0}}),
    ))
    .unwrap();
    assert_eq!(s.view(1_000_100, 1000).status, "blocked");
    let mut bad = event(4, json!({"type":"order_projection"}));
    bad.projection_error = Some("conflicting fill".into());
    s.apply(&bad).unwrap();
    let mut state = ready();
    state["type"] = json!("state");
    s.apply(&event(5, state)).unwrap();
    assert_eq!(s.view(1_000_100, 1000).status, "blocked");
    s.apply(&event(6, ready())).unwrap();
    assert_eq!(s.view(1_000_100, 1000).status, "observing");
    assert_eq!(s.projection_errors, 1);
}
#[test]
fn cancel_is_pending_until_report_and_terminal_waits_for_factual_trades() {
    let mut r = record();
    r.order.cancel_pending = true;
    assert_eq!(
        state::observe(r.clone()).unwrap().phase,
        OrderPhase::CancelPending
    );
    r.order.status = OrderStatus::Cancelled;
    r.order.reported_filled = 1;
    r.order.cancelled = 2;
    let o = state::observe(r.clone()).unwrap();
    assert_eq!(o.phase, OrderPhase::Settling);
    assert!(!o.facts_complete);
    r.order.filled = 1;
    assert_eq!(
        state::observe(r.clone()).unwrap().phase,
        OrderPhase::Cancelled
    );
    r.send_state = SendState::Uncertain;
    assert_eq!(
        state::observe(r.clone()).unwrap().phase,
        OrderPhase::Uncertain
    );
    r.order.filled = 4;
    assert!(state::observe(r).is_err());
}

fn tree(path: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut out: Vec<(std::path::PathBuf, Vec<u8>)> = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let p = entry.unwrap().path();
        if p.is_file() {
            out.push((p.file_name().unwrap().into(), std::fs::read(&p).unwrap()));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
#[test]
fn live_primary_secondary_reads_atomic_facts_without_changing_primary_or_id_precision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary");
    let source = counter(&path, &dir.path().join("secondary"));
    let mut primary = NativeStore::open(
        &path,
        &StoreIdentity {
            adapter: "ctp".into(),
            account_hash: "a".repeat(64),
        },
    )
    .unwrap();
    primary
        .append(
            1_000_001,
            json!({"type":"runtime_start","execute":false}),
            NativeMutation::default(),
        )
        .unwrap();
    primary
        .append(1_000_002, ready(), NativeMutation::default())
        .unwrap();
    let r = record();
    primary
        .append(
            1_000_003,
            json!({"type":"order_projection"}),
            NativeMutation {
                orders: vec![r.clone()],
                observed: vec![r.identity.clone()],
                ..Default::default()
            },
        )
        .unwrap();
    let before = tree(&path);
    let mut reader = reader::Reader::open(&source).unwrap();
    let first = reader.read(&source, 10, 1_000_100, 1000).unwrap();
    assert_eq!(first["total_orders"], 1);
    assert_eq!(
        first["lifecycle"]["state"]["positions"][0]["venue"],
        "CFFEX"
    );
    assert_eq!(
        first["orders"][0]["record"]["identity"]["native"]["order_ref"],
        "18446744073709551614"
    );
    assert_eq!(before, tree(&path));
    let mut updated = r.clone();
    updated.order.filled = 1;
    updated.order.reported_filled = 1;
    updated.updated_us = 1_000_004;
    let fill = NativeTradeRecord {
        fill: Fill {
            identity: TradeIdentity {
                venue: Venue::Cffex,
                symbol: "IF2612".into(),
                trading_day: "20261113".into(),
                trade_id: "18446744073709551612".into(),
                side: Side::Buy,
            },
            order_ref: r.identity.native.order_ref.clone(),
            order_key: Some(r.identity.native.clone()),
            system_id: r.order.system_id.clone(),
            offset: Offset::Open,
            lots: 1,
            price: 4000.0,
        },
        commission: None,
        commission_currency: None,
        native_time: Some(123456789),
        received_us: 1_000_004,
        evidence_seq: 0,
    };
    primary
        .append(
            1_000_004,
            json!({"type":"trade"}),
            NativeMutation {
                orders: vec![updated],
                trades: vec![fill],
                observed: vec![r.identity],
                ..Default::default()
            },
        )
        .unwrap();
    let before = tree(&path);
    let second = reader.read(&source, 10, 1_000_100, 1000).unwrap();
    assert_eq!(second["total_fills"], 1);
    assert_eq!(second["positions_may_lag"], true);
    assert_eq!(second["orders"][0]["record"]["order"]["filled"], 1);
    assert!(second["fills"][0]["commission"].is_null());
    assert_eq!(before, tree(&path));
    let mut wrong = source.clone();
    wrong.adapter = Adapter::Yida;
    drop(reader);
    assert!(reader::Reader::open(&wrong).is_err());
}

fn market_config(root: &str) -> MarketData {
    MarketData {
        id: "intranet".into(),
        label: "公司内网通联".into(),
        environment: Environment::CompanyIntranet,
        service_root: root.into(),
        venues: vec!["CFFEX".into()],
        symbols: vec![],
        max_symbols: 10,
        stale_after_ms: 1000,
    }
}
fn quote(seq: u64, symbol: &str) -> cn_futures_pbs::msg::MarketDataMsg {
    let mut m = cn_futures_pbs::msg::MarketDataMsg {
        publisher_epoch: 100,
        source_generation: 1,
        sequence: seq,
        received_us: 1_000_000,
        last_price: 4000.0,
        ..Default::default()
    };
    m.set_venue("CFFEX").unwrap();
    m.set_symbol(symbol.as_bytes()).unwrap();
    m.bid[0].quantity = 0;
    m
}
#[test]
fn quote_boundaries_clear_previous_books_and_preserve_missing_zero_and_large_sequences() {
    let mut m = market::Market::new(market_config("test/native_viz"));
    m.attached("CFFEX");
    m.apply("CFFEX", quote(1, "IF2612")).unwrap();
    let snapshot = m.snapshot(1_000_100);
    assert!(snapshot["quotes"][0]["bid"][0]["price"].is_null());
    assert_eq!(snapshot["quotes"][0]["bid"][0]["lots"], "0");
    m.apply("CFFEX", quote(3, "IH2612")).unwrap();
    let snapshot = m.snapshot(1_000_100);
    assert_eq!(snapshot["quotes"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["channels"][0]["sequence_gap_count"], 1);
    let mut reset = quote(4, "IF2612");
    reset.flags = cn_futures_pbs::msg::SOURCE_RESET;
    reset.source_generation = 2;
    m.apply("CFFEX", reset).unwrap();
    assert!(m.snapshot(1_000_100)["quotes"]
        .as_array()
        .unwrap()
        .is_empty());
    let mut q = quote(5, "IF2612");
    q.source_generation = 2;
    q.source_sequence = u64::MAX - 1;
    m.apply("CFFEX", q).unwrap();
    assert_eq!(
        m.snapshot(1_000_100)["quotes"][0]["source_sequence"],
        "18446744073709551614"
    );
    assert_eq!(m.snapshot(4_000_000)["quotes"][0]["data_stale"], true);
}
#[test]
fn ipc_publisher_to_viz_subscriber_uses_shared_contract_without_sdk() {
    let root = format!("test/native_viz_{}", now_us());
    let mut publishers = cn_futures_pbs::ipc::Publishers::new(&root).unwrap();
    let sub = market::Subscription::open(&root, "CFFEX").unwrap();
    let mut m = market::Market::new(market_config(&root));
    m.attached("CFFEX");
    publishers.publish(quote(1, "IF2612")).unwrap();
    for _ in 0..50 {
        sub.drain(&mut m, "CFFEX").unwrap();
        if !m.snapshot(now_us())["quotes"]
            .as_array()
            .unwrap()
            .is_empty()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(m.snapshot(now_us())["quotes"][0]["symbol"], "IF2612");
    publishers
        .publish(cn_futures_pbs::msg::MarketDataMsg::reset(2))
        .unwrap();
    sub.drain(&mut m, "CFFEX").unwrap();
    assert!(m.snapshot(now_us())["quotes"]
        .as_array()
        .unwrap()
        .is_empty());
}
#[test]
fn config_rejects_symlink_to_primary_and_shared_secondary_or_market_namespace() {
    let defaults: Config = toml::from_str("[http]\nport = 18811").unwrap();
    assert_eq!(defaults.http.bind, "127.0.0.1");
    let defaults: Config = toml::from_str("").unwrap();
    assert_eq!(defaults.http.port, 8811);
    assert_eq!(defaults.http.bind, "127.0.0.1");
    let dir = tempfile::tempdir().unwrap();
    let primary = dir.path().join("primary");
    std::fs::create_dir(&primary).unwrap();
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink(&primary, &alias).unwrap();
    let mut cfg = Config {
        http: Default::default(),
        poll_interval_ms: 1000,
        stale_after_ms: 15000,
        row_limit: 200,
        counters: vec![counter(&primary, &alias.join("viz"))],
        market_data: vec![],
    };
    assert!(cfg.validate().is_err());
    cfg.counters[0].secondary_path = dir.path().join("viz");
    assert!(cfg.validate().is_ok());
    cfg.market_data = vec![market_config("same/root"), market_config("same/root")];
    cfg.market_data[1].id = "another".into();
    assert!(cfg.validate().is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn http_and_ws_replay_native_sources_and_expose_no_trading_proxy() {
    use futures_util::StreamExt;
    tokio::task::LocalSet::new()
        .run_until(async {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            drop(listener);
            let hub = WsHub::new(8);
            hub.broadcast(json!({"type":"cn_execution","sources":[{"id":"ctp"}]}).to_string());
            hub.broadcast(json!({"type":"cn_market","sources":[{"id":"intranet"}]}).to_string());
            let cfg = crate::config::HttpCfg {
                bind: "127.0.0.1".into(),
                port,
                ws_path: "/observe".into(),
            };
            let task =
                tokio::task::spawn_local(serve_dashboard(cfg, hub.clone(), Some(DASHBOARD), None));
            let base = format!("http://127.0.0.1:{port}");
            let client = reqwest::Client::new();
            for _ in 0..100 {
                if client.get(format!("{base}/healthz")).send().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let html = client
                .get(&base)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            assert!(html.contains("国内期货 · 运行观察"));
            let snapshot: Value = serde_json::from_str(
                &client
                    .get(format!("{base}/snapshot"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(snapshot["entries"].as_array().unwrap().len(), 2);
            let (mut ws, _) =
                tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/observe"))
                    .await
                    .unwrap();
            let mut types = std::collections::HashSet::new();
            for _ in 0..2 {
                let raw = tokio::time::timeout(Duration::from_secs(3), ws.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let v: Value = serde_json::from_str(raw.to_text().unwrap()).unwrap();
                types.insert(v["type"].as_str().unwrap().to_owned());
            }
            assert!(types.contains("cn_execution") && types.contains("cn_market"));
            hub.broadcast(
                json!({"type":"cn_execution","sources":[{"id":"ctp"}],"changed":true}).to_string(),
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let raw = ws.next().await.unwrap().unwrap();
                    let v: Value = serde_json::from_str(raw.to_text().unwrap()).unwrap();
                    if v["changed"] == true {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(
                client
                    .post(format!("{base}/snapshot"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                405
            );
            for path in ["/config/", "/orders", "/cancel"] {
                assert_eq!(
                    client
                        .post(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    404
                );
            }
            task.abort();
        })
        .await;
}
