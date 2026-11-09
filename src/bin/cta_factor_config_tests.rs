use super::*;

async fn test_symbol_filter(inactive: &[&str]) -> ConfigurationSymbolFilter {
    let fixture = Combination::load(std::path::Path::new(
        "config/cta_prep_binance_futures/binance-futures_15min.json",
    ))
    .unwrap();
    let mut symbols = std::collections::BTreeSet::from([
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
        "BTCUSDT".to_string(),
        "ICXUSDT".to_string(),
    ]);
    for rule in &fixture.factor_rules {
        symbols.insert(fixture.rule_id(rule).unwrap().symbol);
    }
    let body = json!({"symbols":symbols.into_iter().map(|symbol|{
        let status = if inactive.contains(&symbol.as_str()) {"SETTLING"} else {"TRADING"};
        json!({"symbol":symbol,"status":status})
    }).collect::<Vec<_>>()});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/fapi/v1/exchangeInfo",
                get(move || {
                    let body = body.clone();
                    async { Json(body) }
                }),
            ),
        )
        .await
        .unwrap()
    });
    ConfigurationSymbolFilter::new(&base).unwrap()
}

#[tokio::test]
async fn preview_and_save_filter_settling_contracts_before_persisting() {
    let root = std::env::temp_dir().join(format!("cta-filter-http-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let mut config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    config.combinations_dir = root.clone();
    let app = Arc::new(App {
        config,
        worker: Worker::Perp,
        spread_config: None,
        hourly_config: None,
        audit_dir: None,
        symbol_filter: test_symbol_filter(&["ICXUSDT"]).await,
    });
    let combination: Combination = serde_json::from_value(json!({
        "name":"hourly","strategy_group":"prep01","venue":"binance-futures","interval":"1h",
        "factor_rules":[
            {"标的":"ETH","因子":"baseline_148","符号":1,"nq":"关闭"},
            {"标的":"ICX","因子":"factor_113","符号":-1,"nq":"0.50"}
        ]
    }))
    .unwrap();
    let revision = catalog_revision(&Plan::load(&root).unwrap()).unwrap();
    let request = || Save {
        worker: None,
        combination: combination.clone(),
        expected_revision: revision.clone(),
    };
    let preview = edit(app.clone(), HeaderMap::new(), request(), false)
        .await
        .unwrap()
        .0;
    assert_eq!(preview["excluded_symbols"][0]["status"], "SETTLING");
    assert_eq!(
        preview["plan"]["combinations"]["hourly"]["factor_rules"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(!root.join("hourly.json").exists());
    let saved = edit(app, HeaderMap::new(), request(), true)
        .await
        .unwrap()
        .0;
    assert_eq!(saved["excluded_symbols"][0]["symbol"], "ICXUSDT");
    let persisted = Combination::load(&root.join("hourly.json")).unwrap();
    assert_eq!(persisted.factor_rules.len(), 1);
    assert_eq!(
        persisted
            .rule_id(&persisted.factor_rules[0])
            .unwrap()
            .symbol,
        "ETHUSDT"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn symbol_groups_api_preserves_membership_ownership_and_catalog_revisions() {
    use mkt_signal::cta_factor_calc::runtime::atomic_json;
    let root = std::env::temp_dir().join(format!("cta-groups-http-{}", uuid::Uuid::new_v4()));
    let mut config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    config.combinations_dir = root.join("prep");
    config.history_dir = root.join("history");
    let mut spread_config = config.clone();
    spread_config.combinations_dir = root.join("spread");
    let groups = json!({"top-5":["ETHUSDT"],"top-20":["ETHUSDT","SOLUSDT"]});
    for dir in [&config.combinations_dir, &spread_config.combinations_dir] {
        atomic_json(&dir.join("symbol_groups.json"), &groups).unwrap();
    }
    let first = json!({"name":"first","strategy_group":"prep02","symbol_group":"top-5","venue":"binance-futures","interval":"1min",
        "factor_rules":[{"标的":"ETH","nq":"关闭","符号":1,"因子":"factor_036"}],
        "signal":{"enabled":false,"max_pos_u":100,"entry":{},"execution":{}}});
    atomic_json(&config.combinations_dir.join("first.json"), &first).unwrap();
    let prep_groups_path = config.combinations_dir.join("symbol_groups.json");
    let original = std::fs::read(&prep_groups_path).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let root_for_server = root.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(App {
                config,
                worker: Worker::Perp,
                audit_dir: Some(root_for_server.join("audit")),
                spread_config: Some(spread_config),
                hourly_config: None,
                symbol_filter: test_symbol_filter(&[]).await,
            }),
        )
        .await
        .unwrap()
    });
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let current: Value = http
        .get(format!("{url}/api/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        current["position_strategies"]["first"],
        "cta-top-5-prep02-bn-1min-pos"
    );
    let expanded = json!({"top-5":["ETHUSDT","BTCUSDT"],"top-20":["ETHUSDT","SOLUSDT"]});
    let body = json!({"symbol_groups":expanded,"expected_revision":current["revision"]});
    let preview = http
        .post(format!("{url}/api/symbol-groups/preview"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(preview.status(), StatusCode::OK);
    assert_eq!(std::fs::read(&prep_groups_path).unwrap(), original);
    assert!(!root.join("audit/.sequence").exists());
    let denied = http
        .post(format!("{url}/api/symbol-groups"))
        .header("origin", "https://other.example")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let saved = http
        .post(format!("{url}/api/symbol-groups"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::OK);
    let saved: Value = saved.json().await.unwrap();
    assert_ne!(saved["revision"], current["revision"]);
    assert_eq!(
        http.post(format!("{url}/api/symbol-groups"))
            .json(&body)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let outside = json!({"symbol_groups":{"top-5":["BTCUSDT"],"top-20":["ETHUSDT","SOLUSDT"]},"expected_revision":saved["revision"]});
    assert_eq!(
        http.post(format!("{url}/api/symbol-groups"))
            .json(&outside)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut renamed = first.clone();
    renamed["symbol_group"] = json!("top-20");
    assert_eq!(
        http.post(format!("{url}/api/combinations"))
            .json(&json!({"combination":renamed,"expected_revision":saved["revision"]}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut second = first;
    second["name"] = json!("second");
    second["symbol_group"] = json!("top-20");
    let response = http
        .post(format!("{url}/api/combinations"))
        .json(&json!({"combination":second,"expected_revision":saved["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["unique_factor_count"], 1);
    assert_eq!(
        response["position_strategies"]["second"],
        "cta-top-20-prep02-bn-1min-pos"
    );
    let spread: Value = http
        .get(format!("{url}/api/config?worker=cta-factor-spread"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(spread["plan"]["symbol_groups"], groups);
    assert_eq!(spread["plan"]["combinations"], json!({}));
    let prep_after = std::fs::read(&prep_groups_path).unwrap();
    let spread_saved = http.post(format!("{url}/api/symbol-groups")).json(&json!({"worker":"cta-factor-spread","symbol_groups":expanded,"expected_revision":spread["revision"]})).send().await.unwrap();
    assert_eq!(spread_saved.status(), StatusCode::OK);
    assert_eq!(std::fs::read(&prep_groups_path).unwrap(), prep_after);
    let audit = parameter_audit::read_batch(&root.join("audit"), 0).unwrap();
    assert!(audit.complete && audit.records.iter().any(|r| !r.event.objects.is_empty()));
    assert!(audit.records.iter().all(|r| !r.pending));
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn shared_catalog_round_trips_rows_and_signal_with_conflict_protection() {
    let root = std::env::temp_dir().join(format!("cta-config-http-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let mut config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    config.combinations_dir = root.clone();
    config.history_dir = root.join("history");
    let mut combination = Combination::load(std::path::Path::new(
        "config/cta_prep_binance_futures/binance-futures_15min.json",
    ))
    .unwrap();
    combination.name = "one".into();
    mkt_signal::cta_factor_calc::runtime::atomic_json(&root.join("one.json"), &combination)
        .unwrap();
    combination.name = "two".into();
    combination.strategy_group = "prep02".into();
    mkt_signal::cta_factor_calc::runtime::atomic_json(&root.join("two.json"), &combination)
        .unwrap();
    let untouched = std::fs::read(root.join("two.json")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let root_for_server = root.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(App {
                config,
                worker: Worker::Perp,
                spread_config: None,
                hourly_config: None,
                audit_dir: Some(root_for_server.join("audit")),
                symbol_filter: test_symbol_filter(&[]).await,
            }),
        )
        .await
        .unwrap()
    });
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let initial: Value = http
        .get(format!("{url}/api/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(initial["unique_factor_count"], 180);
    assert_eq!(
        initial["position_strategies"]["one"],
        "cta-prep01-bn-15min-pos"
    );
    assert_eq!(
        initial["position_strategies"]["two"],
        "cta-prep02-bn-15min-pos"
    );
    assert_eq!(
        initial["plan"]["combinations"]["one"]["factor_rules"]
            .as_array()
            .unwrap()
            .len(),
        146
    );
    assert!(initial["plan"]["combinations"]["one"]
        .get("factors")
        .is_none());
    combination.name = "one".into();
    combination.strategy_group = "prep01".into();
    combination.signal.as_mut().unwrap().max_pos_u = 321.0;
    let body = json!({"combination":combination,"expected_revision":initial["revision"]});
    let preview: Value = http
        .post(format!("{url}/api/preview"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(preview["unique_factor_count"], 180);
    assert_ne!(
        Combination::load(&root.join("one.json"))
            .unwrap()
            .signal
            .unwrap()
            .max_pos_u,
        321.0
    );
    let denied = http
        .post(format!("{url}/api/combinations"))
        .header("origin", "https://other.example")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let saved = http
        .post(format!("{url}/api/combinations"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::OK);
    let saved: Value = saved.json().await.unwrap();
    assert_eq!(
        Combination::load(&root.join("one.json"))
            .unwrap()
            .signal
            .unwrap()
            .max_pos_u,
        321.0
    );
    assert_eq!(std::fs::read(root.join("two.json")).unwrap(), untouched);
    let stale = http
        .post(format!("{url}/api/combinations"))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let mut renamed = combination.clone();
    renamed.strategy_group = "prep03".into();
    let denied = http
        .post(format!("{url}/api/combinations"))
        .json(&json!({"combination":renamed,"expected_revision":saved["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        Combination::load(&root.join("one.json"))
            .unwrap()
            .strategy_group,
        "prep01"
    );
    combination.signal = None;
    let retired = http
        .post(format!("{url}/api/combinations"))
        .json(&json!({"combination":combination,"expected_revision":saved["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(retired.status(), StatusCode::OK);
    let retired: Value = retired.json().await.unwrap();
    assert_eq!(retired["unique_factor_count"], 180);
    assert!(Combination::load(&root.join("one.json"))
        .unwrap()
        .signal
        .is_none());
    let deleted = http
        .delete(format!("{url}/api/combinations/one"))
        .json(&json!({"expected_revision":retired["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::OK);
    let deleted: Value = deleted.json().await.unwrap();
    assert_eq!(deleted["unique_factor_count"], 180);
    assert!(!root.join("one.json").exists());
    assert_eq!(std::fs::read(root.join("two.json")).unwrap(), untouched);
    let audit = parameter_audit::read_batch(&root.join("audit"), 0).unwrap();
    assert!(audit.complete && audit.records.iter().any(|r| !r.event.objects.is_empty()));
    assert!(audit.records.iter().all(|r| !r.pending));
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn independent_hourly_runtime_shares_catalog_and_routes_edits_without_touching_minutes() {
    use mkt_signal::cta_factor_calc::runtime::atomic_json;
    let root = std::env::temp_dir().join(format!("cta-hourly-routing-{}", uuid::Uuid::new_v4()));
    let mut config =
        Config::load(std::path::Path::new("config/cta_prep_binance_futures.toml")).unwrap();
    config.combinations_dir = root.join("minutes");
    config.history_dir = root.join("minute-history");
    let mut hourly = config.clone();
    hourly.combinations_dir = root.join("hourly");
    hourly.history_dir = root.join("hourly-history");
    let groups = json!({"top-5":["ETHUSDT","SOLUSDT"]});
    atomic_json(&config.combinations_dir.join("symbol_groups.json"), &groups).unwrap();
    std::fs::create_dir_all(&hourly.combinations_dir).unwrap();
    std::os::unix::fs::symlink(
        config.combinations_dir.join("symbol_groups.json"),
        hourly.combinations_dir.join("symbol_groups.json"),
    )
    .unwrap();
    let doc = |name: &str, interval: &str, symbol: &str| {
        json!({"name":name,"strategy_group":"prep02","symbol_group":"top-5","venue":"binance-futures","interval":interval,
        "factor_rules":[{"标的":symbol,"因子":"factor_036","符号":1,"nq":"关闭"}],
        "signal":{"enabled":false,"max_pos_u":100,"entry":{},"execution":{}}})
    };
    atomic_json(
        &config.combinations_dir.join("minutes.json"),
        &doc("minutes", "5min", "ETH"),
    )
    .unwrap();
    atomic_json(
        &hourly.combinations_dir.join("hours.json"),
        &doc("hours", "1h", "SOL"),
    )
    .unwrap();
    let before = std::fs::read(config.combinations_dir.join("minutes.json")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = App {
        config: config.clone(),
        worker: Worker::Perp,
        spread_config: None,
        hourly_config: Some(hourly.clone()),
        audit_dir: None,
        symbol_filter: test_symbol_filter(&[]).await,
    };
    let server = tokio::spawn(async move { axum::serve(listener, router(app)).await.unwrap() });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let initial: Value = client
        .get(format!("{url}/api/config"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        initial["plan"]["combinations"].as_object().unwrap().len(),
        2
    );
    let mut hours = initial["plan"]["combinations"]["hours"].clone();
    hours["signal"]["max_pos_u"] = json!(50);
    let response = client
        .post(format!("{url}/api/combinations"))
        .json(&json!({"combination":hours,"expected_revision":initial["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let saved: Value = response.json().await.unwrap();
    assert_eq!(
        Combination::load(&hourly.combinations_dir.join("hours.json"))
            .unwrap()
            .signal
            .unwrap()
            .max_pos_u,
        50.0
    );
    assert_eq!(
        std::fs::read(config.combinations_dir.join("minutes.json")).unwrap(),
        before
    );
    assert!(!config.combinations_dir.join("hours.json").exists());
    let stale=client.post(format!("{url}/api/combinations")).json(&json!({"combination":saved["plan"]["combinations"]["minutes"],"expected_revision":initial["revision"]})).send().await.unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let invalid = client
        .post(format!("{url}/api/symbol-groups/preview"))
        .json(&json!({"symbol_groups":{"top-5":["ETHUSDT"]},"expected_revision":saved["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let expanded = json!({"top-5":["ETHUSDT","SOLUSDT","BTCUSDT"]});
    let response = client
        .post(format!("{url}/api/symbol-groups"))
        .json(&json!({"symbol_groups":expanded,"expected_revision":saved["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let changed: Value = response.json().await.unwrap();
    assert!(hourly
        .combinations_dir
        .join("symbol_groups.json")
        .is_symlink());
    assert_eq!(
        Plan::load(&config.combinations_dir).unwrap().symbol_groups,
        Plan::load(&hourly.combinations_dir).unwrap().symbol_groups
    );
    for (c, generation) in [(&config, "minute-running"), (&hourly, "hour-running")] {
        atomic_json(
            &c.history_dir.join("state.json"),
            &json!({"worker":"cta-prep-binance-futures","generation":generation,"warming":false}),
        )
        .unwrap();
    }
    let status: Value = client
        .get(format!("{url}/api/status?interval=1h"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["generation"], "hour-running");
    let status: Value = client
        .get(format!("{url}/api/status?interval=5min"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["generation"], "minute-running");
    let removed = client
        .delete(format!("{url}/api/combinations/hours"))
        .json(&json!({"expected_revision":changed["revision"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(removed.status(), StatusCode::OK);
    assert!(!hourly.combinations_dir.join("hours.json").exists());
    assert_eq!(
        std::fs::read(config.combinations_dir.join("minutes.json")).unwrap(),
        before
    );
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}
