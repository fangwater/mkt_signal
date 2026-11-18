use super::super::{
    config::{Config, PublicationConfig},
    engine::{Engine, Ledger},
};
use super::*;
use std::collections::BTreeSet;

fn configuration() -> Config {
    serde_json::from_str(include_str!("../../../config/cta_cn_futures_01.json")).unwrap()
}
fn settings() -> PublicationConfig {
    PublicationConfig {
        enabled: false,
        manager_url: "http://127.0.0.1:18202/api".into(),
        source_id: "native-account".into(),
        token_env: "CN_CTA_01_PUBLISH_TOKEN".into(),
        include_trading_day: false,
        per_factor_intervals: BTreeSet::from(["1h".into()]),
    }
}
#[test]
fn per_factor_names_carry_instance_factor_and_frequency_without_aggregate_double_counting() {
    let mut cfg = configuration();
    cfg.publication = Some(settings());
    let ledger = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 1000,
        lots_per_rule: 2,
        flags: cfg
            .rules
            .iter()
            .enumerate()
            .map(|(i, r)| (r.factor.clone(), if i % 2 == 0 { 1 } else { -1 }))
            .collect(),
        retired_contracts: vec!["sn2611".into()],
    };
    let engine = Engine::new(&cfg, ledger).unwrap();
    let targets = engine.publication_positions().unwrap();
    assert_eq!(targets.len(), 10);
    assert!(targets.contains_key("cn-futures-01-td_pr_010-1h"));
    assert!(!targets.contains_key("cn-futures-01-1h"));
    assert_eq!(
        targets["cn-futures-01-td_pr_010-1h"].targets["SHFE:sn2611"],
        0
    );
    assert_eq!(
        targets["cn-futures-01-td_pr_010-1h"].targets["SHFE:sn2612"],
        2
    );
    assert_eq!(
        targets
            .values()
            .map(|p| p.targets["SHFE:sn2612"])
            .sum::<i64>(),
        0
    );
}
#[test]
fn ordinary_instances_keep_one_aggregate_strategy_per_frequency() {
    let mut cfg = configuration();
    cfg.name = "cta-cn_futures-02".into();
    cfg.publication = None;
    let ledger = Ledger {
        contract: "sn2612".into(),
        last_decision_ms: 1000,
        flags: BTreeMap::from([("td_pr_010".into(), 1), ("factor_094".into(), -1)]),
        ..Ledger::default()
    };
    let engine = Engine::new(&cfg, ledger).unwrap();
    let positions = engine.publication_positions().unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions["cn-futures-02-1h"].targets["SHFE:sn2612"], 0);
    cfg.interval = "5min".into();
    cfg.name = "cta-cn_futures-02-5min".into();
    assert_eq!(cfg.strategy_group().unwrap(), "cn-futures-02");
}

#[test]
fn products_with_a_shared_factor_keep_distinct_attribution() {
    let configs =
        super::super::configs(std::path::Path::new("config/cta_cn_futures_01.json")).unwrap();
    let mut group = BTreeMap::new();
    for (_, cfg) in configs {
        let suffix = cfg.symbol.to_ascii_lowercase();
        let (group_name, token) = if suffix == "sn" {
            (
                "cn-futures-01".to_owned(),
                "CN_CTA_01_PUBLISH_TOKEN".to_owned(),
            )
        } else {
            (
                format!("cn-futures-01-{suffix}"),
                format!("CN_CTA_01_PUBLISH_TOKEN_{}", cfg.symbol),
            )
        };
        assert_eq!(cfg.strategy_group().unwrap(), group_name);
        assert_eq!(cfg.publication_settings().unwrap().token_env, token);
        let engine = Engine::new(&cfg, Ledger::default()).unwrap();
        for (name, position) in engine.publication_positions().unwrap() {
            assert!(group.insert(name, position).is_none());
        }
    }
    assert_eq!(group.len(), 38);
    assert!(group.contains_key("cn-futures-01-baseline_035-1h"));
    assert!(group.contains_key("cn-futures-01-rb-baseline_035-1h"));
    assert!(group.contains_key("cn-futures-01-sa-factor_081-1h"));
    assert!(group.contains_key("cn-futures-01-sm-factor_081-1h"));
}

#[tokio::test]
async fn products_publish_only_their_own_positions_on_independent_clocks() {
    let build = |positions| {
        let (tx, rx) = watch::channel(None);
        (
            Publisher {
                positions,
                latest: None,
                quotes: BTreeMap::new(),
                tx,
                state: Arc::new(Mutex::new(Status::default())),
                task: tokio::spawn(std::future::pending()),
            },
            rx,
        )
    };
    let target = |strategy: &str, contract: &str, decision_ms, lots| {
        BTreeMap::from([(
            strategy.to_owned(),
            StrategyPosition {
                decision_ms,
                targets: BTreeMap::from([(contract.to_owned(), lots)]),
            },
        )])
    };
    let sn = target("tin", "SHFE:sn2612", 100, 1);
    let rb = target("rebar", "SHFE:rb2701", 0, 0);
    let (mut tin, tin_rx) = build(sn.clone());
    let (mut rebar, rebar_rx) = build(rb.clone());

    rebar.observe(rb, None, 20261118, true).unwrap();
    assert!(rebar_rx.borrow().is_none());
    let rb_new = target("rebar", "SHFE:rb2701", 200, -1);
    rebar
        .observe(rb_new.clone(), Some((200, 20261118)), 20261118, true)
        .unwrap();
    assert!(tin_rx.borrow().is_none());
    {
        let batch = rebar_rx.borrow();
        let batch = batch.as_ref().unwrap();
        assert!(batch.live_ready);
        assert_eq!(batch.trigger_us, 200_000);
        assert!(batch.positions == rb_new);
    }

    // A later-arriving decision with an earlier bar time is factual for tin.
    let sn_new = target("tin", "SHFE:sn2612", 150, 2);
    tin.observe(sn_new.clone(), Some((150, 20261118)), 20261118, true)
        .unwrap();
    rebar.observe(rb_new, None, 20261118, false).unwrap();
    let batch = tin_rx.borrow();
    let batch = batch.as_ref().unwrap();
    assert!(batch.live_ready);
    assert_eq!(batch.trigger_us, 150_000);
    assert!(batch.positions == sn_new);
    assert!(!rebar_rx.borrow().as_ref().unwrap().live_ready);
}

#[test]
fn unsafe_manager_urls_and_cross_environment_identities_are_rejected() {
    let mut cfg = configuration();
    cfg.name = "cta-cn_futures-00".into();
    assert!(cfg.validate().is_err());
    cfg = configuration();
    cfg.publication.as_mut().unwrap().manager_url = "http://token@localhost/api".into();
    assert!(cfg.validate().is_err());
    cfg = configuration();
    cfg.name = "cta-cn_futures-01-5min".into();
    assert!(cfg.validate().is_err());
}
