use super::*;
use crate::cta_factor_calc::config::{Combination, FactorRule};
use std::path::PathBuf;

const CUTOVER: i64 = 1_800_000_060_000_000;

struct Fixture {
    root: PathBuf,
    plan: Plan,
    request: Request,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!("cta-symbol-migration-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let plan = Plan::load(Path::new("config/cta_prep_binance_futures")).unwrap();
    let mut originals = BTreeMap::<Interval, Combination>::new();
    let mut seen = BTreeSet::new();
    for combination in plan
        .combinations
        .values()
        .filter(|c| c.strategy_group == "prep02")
    {
        let c = originals.entry(combination.interval).or_insert_with(|| {
            let mut c = combination.clone();
            c.symbol_group = None;
            c.name = format!("original_{}", c.interval.name());
            c.factor_rules.clear();
            c
        });
        for rule in &combination.factor_rules {
            let key = (
                combination.interval,
                combination.rule_id(rule).unwrap().name(),
                rule.nq.clone(),
            );
            if seen.insert(key) {
                c.factor_rules.push(rule.clone());
            }
        }
    }
    let mut old_quantities = BTreeMap::new();
    for (interval, combination) in originals {
        let mut table = Table::new(combination, CUTOVER - 1_000_000);
        table.ever_enabled = true;
        let held: Vec<(&str, &str, &str, f64)> = match interval {
            Interval::Minute => vec![
                ("DOGEUSDT", "baseline_141", "0.50", 3.0),
                ("DOGEUSDT", "baseline_141", "0.2/0.8", -4.0),
                ("KNCUSDT", "baseline_141", "0.2/0.8", -1.0),
            ],
            Interval::FiveMinutes => vec![("SOLUSDT", "factor_049", "关闭", 5.0)],
            Interval::FifteenMinutes => vec![
                ("SOLUSDT", "baseline_104", "关闭", 2.0),
                ("BTCUSDT", "baseline_022", "0.2/0.8", -1.0),
            ],
        };
        let config = table.combination.signal_config().unwrap();
        for (symbol, factor, nq, qty) in held {
            let rule = config
                .factor_rules
                .iter()
                .find(|r| r.venue_symbol() == symbol && r.factor == factor && r.nq == nq)
                .unwrap();
            let key = config.rule_strategy(rule);
            let Ledger::Prep(rows) = &mut table.ledger else {
                unreachable!()
            };
            rows.entry(key.clone()).or_default().insert(
                symbol.into(),
                PrepPosition {
                    flag: if qty > 0.0 { 1 } else { -1 },
                    qty,
                    entry_notional: 200.0,
                },
            );
            table.clock.insert(
                format!("{key}:{symbol}"),
                CUTOVER / 1000 - interval.millis(),
            );
        }
        table.validate().unwrap();
        old_quantities.insert(table.strategy_name.clone(), quantities(&table).unwrap());
        super::super::super::runtime::atomic_json(
            &root.join(format!("{}.json", table.strategy_name)),
            &table,
        )
        .unwrap();
    }
    let sources = [("source-a", 1.0), ("source-b", 2.0)].into_iter().map(|(id, shares)| {
        let mut owned = old_quantities.clone();
        for row in owned.values_mut() { for qty in row.values_mut() { *qty *= shares; } }
        let mut accounts = Quantities::from([("OTHERUSDT".into(), 17.0)]);
        let mut projection = vec![json!(["unrelated", "OTHERUSDT", 1, 17.0])];
        for (name, row) in &owned {
            for (symbol, qty) in row {
                *accounts.entry(symbol.clone()).or_default() += qty;
                projection.push(json!([name, symbol, 1, qty]));
            }
        }
        let marks = accounts.keys().map(|s| (s.clone(), Mark { close_ts_us: CUTOVER.div_euclid(60_000_000) * 60_000_000, price: 100.0 })).collect();
        let mut rows = Vec::new();
        for (name, positions) in &owned {
            for (symbol, qty) in positions {
                rows.push(json!({"algorithm":"ctaker","strategy_name":name,"symbol":symbol,"current_qty":qty,"account_position_qty":accounts[symbol],"active_batches":0,"live_order_qty":0.0}));
            }
        }
        rows.push(json!({"algorithm":"chase_exec","strategy_name":"unrelated","symbol":"OTHERUSDT","current_qty":17.0,"account_position_qty":17.0,"active_batches":0,"live_order_qty":0.0}));
        let account = accounts.into_iter().map(|(s, q)| json!([s, 1, q])).collect::<Vec<_>>();
        let ledgers = BTreeMap::from([
            ("batch_exec".into(), json!({"version":1,"updated_at_us":CUTOVER-1,"positions":owned,"inverse_notionals":{}})),
            ("chase_exec".into(), json!({"version":1,"updated_at_us":CUTOVER-1,"positions":{"unrelated":{"OTHERUSDT":17.0}},"inverse_notionals":{}})),
        ]);
        Source {
            source_id: id.into(), shares: old_quantities.keys().map(|n| (n.clone(), shares)).collect(),
            ledgers_after: ledgers.clone(), ledgers,
            exec_snapshot: json!({"position_ready":true,"ts_ms":CUTOVER/1000,"rows":rows}),
            allocation_audit: json!({"source_id":id,"snapshot_ts_us":CUTOVER,"account":account,"strategies":projection}),
            strategy_anchor: true, marks,
        }
    }).collect();
    Fixture {
        root,
        plan,
        request: Request {
            strategy_group: "prep02".into(),
            cutover_ts_us: CUTOVER,
            affected_source_ids: BTreeSet::from(["source-a".into(), "source-b".into()]),
            sources,
        },
    }
}

fn output_table(bundle: &Bundle, name: &str) -> Table {
    serde_json::from_value(bundle.files[&format!("signal_positions/{name}.json")].clone()).unwrap()
}

#[test]
fn migration_splits_overlaps_and_renames_nq_sleeves_without_changing_account_inventory() {
    let f = fixture();
    let before = fs::read(f.root.join("cta-prep02-bn-1min-pos.json")).unwrap();
    let bundle = prepare(&f.plan, &f.root, &f.request).unwrap();
    assert_eq!(bundle.summary["new_strategy_count"], 9);
    let empty = output_table(&bundle, "cta-top-20-prep02-bn-5min-pos");
    assert!(empty.targets().unwrap().is_empty());
    for pool in ["top-5", "top-20"] {
        let table = output_table(&bundle, &format!("cta-{pool}-prep02-bn-15min-pos"));
        let Ledger::Prep(rows) = table.ledger else {
            unreachable!()
        };
        assert_eq!(rows["baseline_104"]["SOLUSDT"].qty, 1.0);
        assert_eq!(rows["baseline_104"]["SOLUSDT"].entry_notional, 100.0);
        assert_eq!(rows["baseline_022"]["BTCUSDT"].qty, -0.5);
    }
    for (pool, qty) in [("top-5", 3.0), ("top-20", -4.0)] {
        let table = output_table(&bundle, &format!("cta-{pool}-prep02-bn-1min-pos"));
        let Ledger::Prep(rows) = &table.ledger else {
            unreachable!()
        };
        assert_eq!(rows["baseline_141"]["DOGEUSDT"].qty, qty);
        assert!(table.clock.contains_key("baseline_141:DOGEUSDT"));
    }
    let retired = output_table(&bundle, "cta-prep02-bn-1min-pos");
    assert!(retired.retired);
    assert!(quantities(&retired).unwrap().values().all(|q| *q == 0.0));
    assert_eq!(
        fs::read(f.root.join("cta-prep02-bn-1min-pos.json")).unwrap(),
        before
    );
    for source in ["source-a", "source-b"] {
        let candidate = &bundle.files[&format!("sources/{source}/manager-allocation.json")];
        let mut changes = Quantities::new();
        for row in candidate["positions"].as_array().unwrap() {
            *changes
                .entry(row["symbol"].as_str().unwrap().into())
                .or_default() +=
                row["after_qty"].as_f64().unwrap() - row["before_qty"].as_f64().unwrap();
        }
        assert!(changes.values().all(|q| q.abs() < 1e-12));
        let chase = &bundle.files[&format!("sources/{source}/chase_exec-allocations.json")];
        assert_eq!(chase["positions"]["unrelated"]["OTHERUSDT"], 17.0);
        let batch = &bundle.files[&format!("sources/{source}/batch_exec-allocations.json")];
        assert_eq!(
            batch["positions"]["cta-top-5-prep02-bn-15min-pos"]["SOLUSDT"],
            if source == "source-a" { 1.0 } else { 2.0 }
        );
    }
    let output = f.root.join("diagnostic");
    write_bundle(&output, &bundle).unwrap();
    assert!(output.join("migration.json").exists());
    assert!(write_bundle(&output, &bundle).is_err());
}

#[test]
fn migration_rejects_drift_missing_captures_reused_destinations_and_stale_marks() {
    for fault in 0..9 {
        let mut f = fixture();
        match fault {
            0 => {
                f.request.sources[0].ledgers.get_mut("batch_exec").unwrap()["positions"]
                    ["cta-prep02-bn-15min-pos"]["SOLUSDT"] = json!(3.0)
            }
            1 => {
                f.request.sources.pop();
            }
            2 => {
                f.request.sources[0].ledgers.get_mut("batch_exec").unwrap()["positions"]
                    ["cta-top-5-prep02-bn-1min-pos"] = json!({})
            }
            3 => {
                f.request.sources[0]
                    .marks
                    .get_mut("SOLUSDT")
                    .unwrap()
                    .close_ts_us -= 60_000_000
            }
            4 => f.request.sources[0].allocation_audit["snapshot_ts_us"] = json!(CUTOVER - 1),
            5 => {
                f.request.sources[0].ledgers.remove("chase_exec");
            }
            6 => f.request.sources[0].exec_snapshot["rows"][0]["active_batches"] = json!(1),
            7 => f.request.sources[0].exec_snapshot["rows"][0]["current_qty"] = json!(1234.0),
            8 => {
                f.request.sources[0]
                    .ledgers_after
                    .get_mut("batch_exec")
                    .unwrap()["updated_at_us"] = json!(CUTOVER)
            }
            _ => unreachable!(),
        }
        if matches!(fault, 0 | 2) {
            f.request.sources[0].ledgers_after = f.request.sources[0].ledgers.clone();
        }
        assert!(
            prepare(&f.plan, &f.root, &f.request).is_err(),
            "fault {fault}"
        );
    }
}

#[test]
fn migration_rejects_a_held_rule_without_a_destination_instead_of_guessing() {
    let mut f = fixture();
    let mut combinations = f.plan.combinations.values().cloned().collect::<Vec<_>>();
    for combination in &mut combinations {
        if combination.strategy_group == "prep02"
            && combination.interval == Interval::FifteenMinutes
        {
            combination
                .factor_rules
                .retain(|r: &FactorRule| !(r.symbol == "SOLUSDT" && r.factor == "baseline_104"));
        }
    }
    f.plan = Plan::merge_with_symbol_groups(combinations, f.plan.symbol_groups.clone()).unwrap();
    assert!(prepare(&f.plan, &f.root, &f.request)
        .unwrap_err()
        .to_string()
        .contains("no destination"));
}
