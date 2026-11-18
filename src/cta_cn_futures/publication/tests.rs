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
