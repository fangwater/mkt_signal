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
    // Freeze the historical migration case independently of the current live templates.
    let snapshot: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/cta_prep02_migration_plan.json"
    ))
    .unwrap();
    let combinations: Vec<Combination> =
        serde_json::from_value(snapshot["combinations"].clone()).unwrap();
    let symbol_groups = serde_json::from_value(snapshot["symbol_groups"].clone()).unwrap();
    let plan = Plan::merge_with_symbol_groups(combinations, symbol_groups).unwrap();
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
            Interval::Hour => unreachable!("migration fixture contains only minute strategies"),
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
            strategy_anchor: true, marks, residual_quote_limit: 0.0, reconcile_current_ownership: false,
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

#[test]
fn migration_preserves_observed_settled_residuals_and_an_absent_unused_family() {
    let mut f = fixture();
    let source = &mut f.request.sources[0];
    source.residual_quote_limit = 5.0;
    source.ledgers.get_mut("batch_exec").unwrap()["positions"]["cta-prep02-bn-15min-pos"]
        ["SOLUSDT"] = json!(1.99);
    source.ledgers.get_mut("batch_exec").unwrap()["positions"]["unrelated"] =
        json!({"OTHERUSDT":17.0});
    source.ledgers.insert("chase_exec".into(), Value::Null);
    source.ledgers_after = source.ledgers.clone();
    for row in source.exec_snapshot["rows"].as_array_mut().unwrap() {
        if row["symbol"] == "SOLUSDT" {
            row["account_position_qty"] = json!(6.99);
        }
        if row["strategy_name"] == "cta-prep02-bn-15min-pos" && row["symbol"] == "SOLUSDT" {
            row["current_qty"] = json!(1.99);
            row["target_qty"] = json!(2.0);
            row["execution_complete"] = json!(true);
            row["pending_qty"] = json!(0.0);
            row["completion_reason"] = json!("target_tolerance");
        }
        if row["strategy_name"] == "unrelated" {
            row["algorithm"] = json!("grid");
        }
    }
    for row in source.allocation_audit["strategies"]
        .as_array_mut()
        .unwrap()
    {
        if row[0] == "cta-prep02-bn-15min-pos" && row[1] == "SOLUSDT" {
            row[3] = json!(1.99);
        }
    }
    for row in source.allocation_audit["account"].as_array_mut().unwrap() {
        if row[0] == "SOLUSDT" {
            row[2] = json!(6.99);
        }
    }
    let bundle = prepare(&f.plan, &f.root, &f.request).unwrap();
    let ledger = &bundle.files["sources/source-a/batch_exec-allocations.json"]["positions"];
    assert_eq!(ledger["cta-top-5-prep02-bn-15min-pos"]["SOLUSDT"], 0.995);
    assert_eq!(ledger["cta-top-20-prep02-bn-15min-pos"]["SOLUSDT"], 0.995);
    assert_eq!(
        bundle.files["sources/source-a/chase_exec-allocations.json"],
        Value::Null
    );
    assert_eq!(
        bundle.summary["execution_sources"][0]["execution_residuals"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    f.request.sources[0].residual_quote_limit = 0.0;
    assert!(prepare(&f.plan, &f.root, &f.request).is_err());
}

#[test]
fn current_ownership_reconciliation_is_zero_sum_and_requires_full_account_proof() {
    let mut f = fixture();
    let source = &mut f.request.sources[0];
    source.strategy_anchor = false;
    for row in source.allocation_audit["strategies"]
        .as_array_mut()
        .unwrap()
    {
        if row[0] == "cta-prep02-bn-15min-pos" && row[1] == "SOLUSDT" {
            row[3] = json!(2.1);
        }
    }
    source.allocation_audit["strategies"]
        .as_array_mut()
        .unwrap()
        .push(json!(["system_position_close", "SOLUSDT", 1, -0.1]));
    assert!(prepare(&f.plan, &f.root, &f.request).is_err());
    f.request.sources[0].reconcile_current_ownership = true;
    let bundle = prepare(&f.plan, &f.root, &f.request).unwrap();
    let positions = bundle.files["sources/source-a/manager-allocation.json"]["positions"]
        .as_array()
        .unwrap();
    let residual = positions
        .iter()
        .find(|p| p["strategy"] == "system_position_close" && p["symbol"] == "SOLUSDT")
        .unwrap();
    assert_eq!(residual["before_qty"], -0.1);
    assert_eq!(residual["after_qty"], 0.0);
    let sum: f64 = positions
        .iter()
        .filter(|p| p["symbol"] == "SOLUSDT")
        .map(|p| p["after_qty"].as_f64().unwrap() - p["before_qty"].as_f64().unwrap())
        .sum();
    assert!(sum.abs() < 1e-12);
    f.request.sources[0].allocation_audit["account"][0][2] = json!(999.0);
    assert!(prepare(&f.plan, &f.root, &f.request).is_err());
}
