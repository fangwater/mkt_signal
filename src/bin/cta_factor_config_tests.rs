use super::*;

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
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(App {
                config,
                worker: Worker::Perp,
                spread_config: Some(spread_config),
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
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router(App {
                config,
                worker: Worker::Perp,
                spread_config: None,
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
    server.abort();
    let _ = server.await;
    std::fs::remove_dir_all(root).unwrap();
}
