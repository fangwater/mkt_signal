use super::super::config::{FactorId, Parameters};
use super::*;
use serde_json::json;

fn combination() -> Combination {
    serde_json::from_value(json!({
        "name":"hourly", "strategy_group":"prep01", "venue":"binance-futures", "interval":"1h",
        "factor_rules":[
            {"标的":"ETH", "因子":"baseline_148", "符号":1, "nq":"关闭"},
            {"标的":"ICX", "因子":"factor_113", "符号":-1, "nq":"0.50"},
            {"标的":"ICXUSDT", "因子":"factor_177", "符号":1, "nq":"关闭"}
        ]
    }))
    .unwrap()
}

#[test]
fn inactive_contracts_remove_rules_nq_parameters_and_overrides_together() {
    let mut c = combination();
    for name in c.factors().unwrap().keys() {
        c.parameters.insert(
            name.clone(),
            Parameters::for_id(&FactorId::parse(name).unwrap()),
        );
    }
    c.signal = Some(
        serde_json::from_value(json!({
            "enabled":false,"max_pos_u":100,"entry":{},"execution":{},
            "symbol_overrides":{"ICXUSDT":{}}
        }))
        .unwrap(),
    );
    let snapshot = BTreeMap::from([
        ("ETHUSDT".into(), "TRADING".into()),
        ("ICXUSDT".into(), "SETTLING".into()),
    ]);
    let (filtered, excluded) = filter_with_snapshot(c, &snapshot).unwrap();
    assert_eq!(filtered.factor_rules.len(), 1);
    assert_eq!(
        excluded,
        vec![ExcludedSymbol {
            symbol: "ICXUSDT".into(),
            market: Market::BinanceFutures,
            status: "SETTLING".into(),
            removed_rules: 2
        }]
    );
    assert!(filtered.parameters.keys().all(|name| !name.contains("icx")));
    assert!(filtered
        .factors()
        .unwrap()
        .keys()
        .all(|name| !name.contains("icx")));
    assert!(filtered.signal.unwrap().symbol_overrides.is_empty());
}

#[test]
fn absent_symbols_are_filtered_but_other_market_rules_stay() {
    let mut c = combination();
    c.factor_rules[2].venue = Some(Market::OkexFutures);
    let snapshot = BTreeMap::from([("ETHUSDT".into(), "TRADING".into())]);
    let (filtered, excluded) = filter_with_snapshot(c, &snapshot).unwrap();
    assert_eq!(filtered.factor_rules.len(), 2);
    assert_eq!(excluded[0].status, "NOT_LISTED");
    assert_eq!(excluded[0].removed_rules, 1);
}

#[test]
fn spread_checks_binance_reference_leg() {
    let mut c = combination();
    c.venue = Market::OkexFutures;
    c.reference_venue = Some(Market::BinanceFutures);
    for rule in &mut c.factor_rules {
        rule.factor = "spread".into();
    }
    let snapshot = BTreeMap::from([
        ("ETHUSDT".into(), "TRADING".into()),
        ("ICXUSDT".into(), "SETTLING".into()),
    ]);
    let (filtered, excluded) = filter_with_snapshot(c, &snapshot).unwrap();
    assert_eq!(filtered.factor_rules.len(), 1);
    assert_eq!(excluded[0].removed_rules, 2);
}

#[tokio::test]
async fn caches_complete_statuses_but_does_not_treat_failed_catalog_as_empty() {
    use axum::{routing::get, Json, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let app = Router::new().route("/fapi/v1/exchangeInfo",get(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        async {Json(json!({"symbols":[
            {"symbol":"ETHUSDT","status":"TRADING","deliveryDate":chrono::Utc::now().timestamp_millis()+86_400_000},
            {"symbol":"ICXUSDT","status":"SETTLING"}
        ]}))}
    })).route("/broken/fapi/v1/exchangeInfo",get(|| async {Json(json!({"symbols":[]}))}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let filter = ConfigurationSymbolFilter::new(&base).unwrap();
    for _ in 0..2 {
        let (filtered, excluded) = filter.filter(combination()).await.unwrap();
        assert_eq!(filtered.factor_rules.len(), 1);
        assert_eq!(excluded[0].status, "SETTLING");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let broken = ConfigurationSymbolFilter::new(&format!("{base}/broken")).unwrap();
    assert!(broken.filter(combination()).await.is_err());
    assert!(broken.snapshot.lock().await.is_none());
    server.abort();
}
