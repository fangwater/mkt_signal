//! Offline preparation of a conserved symbol-group ownership transfer.
//! The bundle contains current signal/Exec states and Manager's existing
//! allocation-correction contract. It never submits orders or changes a store.
use super::*;
use crate::cta_factor_calc::config::{Interval, Market};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub strategy_group: String,
    pub cutover_ts_us: i64,
    /// Complete affected source inventory captured from Manager bindings.
    pub affected_source_ids: BTreeSet<String>,
    pub sources: Vec<Source>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub source_id: String,
    /// Effective Manager binding shares multiplied by the account multiplier.
    pub shares: BTreeMap<String, f64>,
    /// Complete captured batch_exec / chase_exec position_allocations values.
    pub ledgers: BTreeMap<String, Value>,
    /// A second capture after the settled Viz observation; values must match.
    pub ledgers_after: BTreeMap<String, Value>,
    /// The complete exec_pre_trade_state entry, including readiness and rows.
    pub exec_snapshot: Value,
    /// Complete same-source Manager allocation_audit at the cutover timestamp.
    pub allocation_audit: Value,
    pub strategy_anchor: bool,
    pub marks: BTreeMap<String, Mark>,
    /// Explicit cap for already-settled exchange-minimum/tolerance residuals.
    /// Zero retains exact matching; never accepts a pending execution gap.
    #[serde(default)]
    pub residual_quote_limit: f64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mark {
    pub close_ts_us: i64,
    pub price: f64,
}

#[derive(Debug, Serialize)]
pub struct Bundle {
    pub summary: Value,
    pub files: BTreeMap<String, Value>,
}

type RuleKey = (String, String, i8, String);
type Quantities = BTreeMap<String, f64>;

fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 160
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        && !matches!(name, "." | "..")
}

fn fingerprint(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

fn rule_key(rule: &crate::cta_perp::config::FactorNqRule) -> RuleKey {
    (
        rule.venue_symbol(),
        rule.factor.clone(),
        rule.sign,
        rule.nq_key(),
    )
}

fn quantities(table: &Table) -> Result<Quantities> {
    let Ledger::Prep(rows) = &table.ledger else {
        anyhow::bail!("migration supports Prep ledgers only")
    };
    let mut result = Quantities::new();
    for positions in rows.values() {
        for (symbol, p) in positions {
            add_target_quantity(&mut result, symbol, p.qty)?;
        }
    }
    Ok(result)
}

fn same_qty(a: f64, b: f64) -> bool {
    a.is_finite() && b.is_finite() && (a - b).abs() <= 1e-10 * a.abs().max(b.abs()).max(1.0)
}

/// Read captured old tables, split each factual sub-position by its full rule
/// identity, and prepare all three ownership layers without live side effects.
pub fn prepare(plan: &Plan, positions_dir: &Path, request: &Request) -> Result<Bundle> {
    ensure!(
        request.cutover_ts_us > 0 && request.cutover_ts_us % 1000 == 0,
        "positive cutover timestamp on the captured millisecond boundary required"
    );
    let targets = plan
        .combinations
        .values()
        .filter(|c| c.strategy_group == request.strategy_group && c.signal.is_some())
        .collect::<Vec<_>>();
    ensure!(!targets.is_empty(), "no destination strategies");
    let mut tables = BTreeMap::new();
    let mut by_interval = BTreeMap::<Interval, Vec<String>>::new();
    let mut destinations = BTreeMap::<(Interval, RuleKey), Vec<(String, String)>>::new();
    for c in targets {
        ensure!(
            c.venue == Market::BinanceFutures
                && c.reference_venue.is_none()
                && c.symbol_group.is_some(),
            "destinations must be symbol-group Prep strategies"
        );
        let config = c.signal_config()?;
        ensure!(
            config.position_algorithm == PositionAlgorithm::V0 && !config.enabled,
            "destination strategies must be disabled and use independent factor positions"
        );
        let name = c.manager_strategy_name();
        by_interval
            .entry(c.interval)
            .or_default()
            .push(name.clone());
        for rule in &config.factor_rules {
            destinations
                .entry((c.interval, rule_key(rule)))
                .or_default()
                .push((name.clone(), config.rule_strategy(rule)));
        }
        tables.insert(name, Table::new(c.clone(), request.cutover_ts_us));
    }
    let mut files = BTreeMap::new();
    let mut old_tables = BTreeMap::new();
    let mut split_audit = Vec::new();
    let mut old_to_new = BTreeMap::<String, Vec<String>>::new();
    let mut input_hashes = BTreeMap::new();
    for (interval, names) in &by_interval {
        let old_name = format!("cta-{}-bn-{}-pos", request.strategy_group, interval.name());
        let path = positions_dir.join(format!("{old_name}.json"));
        let bytes = fs::read(&path)
            .with_context(|| format!("read original position table {}", path.display()))?;
        input_hashes.insert(old_name.clone(), format!("{:x}", Sha256::digest(&bytes)));
        let old: Table =
            serde_json::from_slice(&bytes).context("decode original position table")?;
        old.validate()?;
        ensure!(
            old.strategy_name == old_name
                && old.combination.symbol_group.is_none()
                && old.combination.strategy_group == request.strategy_group
                && old.combination.interval == *interval
                && old.combination.reference_venue.is_none()
                && !old.retiring
                && !old.retired
                && old.updated_ts_us <= request.cutover_ts_us,
            "old table identity, retirement state or timestamp does not match migration"
        );
        let config = old.combination.signal_config()?;
        ensure!(
            config.position_algorithm == PositionAlgorithm::V0,
            "cannot infer factor ownership from a symbol-mean ledger"
        );
        let old_rules = config
            .factor_rules
            .iter()
            .map(|r| ((config.rule_strategy(r), r.venue_symbol()), rule_key(r)))
            .collect::<BTreeMap<_, _>>();
        let Ledger::Prep(rows) = &old.ledger else {
            anyhow::bail!("original is not a Prep ledger")
        };
        for (rule_name, positions) in rows {
            for (symbol, p) in positions {
                if p.qty == 0.0 {
                    continue;
                }
                let key = old_rules
                    .get(&(rule_name.clone(), symbol.clone()))
                    .context("held sub-position has no factual rule identity")?;
                let matching = destinations
                    .get(&(*interval, key.clone()))
                    .with_context(|| {
                        format!("held rule {old_name}/{symbol}/{rule_name} has no destination")
                    })?;
                let mut remaining_qty = p.qty;
                let mut remaining_notional = p.entry_notional;
                let count = matching.len() as f64;
                let mut allocations = Vec::new();
                for (index, (name, next_rule)) in matching.iter().enumerate() {
                    let last = index + 1 == matching.len();
                    let qty = if last { remaining_qty } else { p.qty / count };
                    let entry_notional = if last {
                        remaining_notional
                    } else {
                        p.entry_notional / count
                    };
                    remaining_qty -= qty;
                    remaining_notional -= entry_notional;
                    let allocated = PrepPosition {
                        flag: p.flag,
                        qty,
                        entry_notional,
                    };
                    let table = tables.get_mut(name).unwrap();
                    let Ledger::Prep(next_rows) = &mut table.ledger else {
                        unreachable!()
                    };
                    ensure!(
                        next_rows
                            .entry(next_rule.clone())
                            .or_default()
                            .insert(symbol.clone(), allocated.clone())
                            .is_none(),
                        "two old sub-positions map to one destination rule"
                    );
                    let old_clock = format!("{rule_name}:{symbol}");
                    if let Some(ts) = old.clock.get(&old_clock) {
                        table.clock.insert(format!("{next_rule}:{symbol}"), *ts);
                    }
                    table.ever_enabled |= old.ever_enabled;
                    table.observations.entry(next_rule.clone()).or_default().insert(symbol.clone(), json!({"position":allocated,"reason":"symbol_group_migration","source_strategy":old_name,"cutover_ts_us":request.cutover_ts_us}));
                    allocations.push(json!({"strategy":name,"rule":next_rule,"qty":qty,"entry_notional":entry_notional}));
                }
                split_audit.push(json!({"source_strategy":old_name,"rule":rule_name,"symbol":symbol,"qty":p.qty,"entry_notional":p.entry_notional,"allocations":allocations}));
            }
        }
        old_to_new.insert(old_name.clone(), names.clone());
        old_tables.insert(old_name, old);
    }
    let mut manager_targets = Vec::new();
    for (name, table) in &tables {
        table.validate()?;
        files.insert(
            format!("signal_positions/{name}.json"),
            serde_json::to_value(table)?,
        );
        manager_targets.push(json!({"strategy_name":name,"targets":table.targets()?}));
    }
    for (name, old) in &old_tables {
        files.insert(
            format!("original_signal_positions/{name}.json"),
            serde_json::to_value(old)?,
        );
        let mut retired = old.clone();
        retired.retire(request.cutover_ts_us)?;
        retired.retired = true;
        retired.updated_ts_us = request.cutover_ts_us;
        retired.validate()?;
        files.insert(
            format!("signal_positions/{name}.json"),
            serde_json::to_value(&retired)?,
        );
        manager_targets.push(json!({"strategy_name":name,"targets":retired.targets()?}));
    }
    let mut seen_sources = BTreeSet::new();
    let mut source_audits = Vec::new();
    for source in &request.sources {
        ensure!(
            safe_name(&source.source_id) && seen_sources.insert(source.source_id.clone()),
            "invalid or duplicate execution source"
        );
        source_audits.push(prepare_source(
            source,
            request.cutover_ts_us,
            &old_tables,
            &tables,
            &old_to_new,
            &mut files,
        )?);
    }
    ensure!(
        seen_sources == request.affected_source_ids,
        "missing or extra execution source capture"
    );
    ensure!(
        !request.sources.is_empty() || split_audit.is_empty(),
        "held positions require complete execution-source captures"
    );
    files.insert("manager-targets.json".into(), json!(manager_targets));
    let output_hashes = files
        .iter()
        .map(|(path, value)| Ok((path.clone(), fingerprint(value)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(Bundle {
        summary: json!({"strategy_group":request.strategy_group,"cutover_ts_us":request.cutover_ts_us,"new_strategy_count":tables.len(),"old_to_new":old_to_new,"input_position_sha256":input_hashes,"catalog_sha256":super::super::runtime::catalog_revision(plan)?,"request_sha256":fingerprint(&serde_json::to_value(request)?)?,"files_sha256":output_hashes,"sub_positions":split_audit,"execution_sources":source_audits,"live_state_changed":false,"historical_pnl_rewritten":false}),
        files,
    })
}

fn ledger_positions(ledger: &Value) -> Result<BTreeMap<String, Quantities>> {
    if ledger.is_null() {
        return Ok(BTreeMap::new());
    }
    let positions =
        serde_json::from_value::<BTreeMap<String, Quantities>>(ledger["positions"].clone())
            .context("Exec ledger requires complete positions")?;
    ensure!(
        positions
            .values()
            .flat_map(|row| row.values())
            .all(|q| q.is_finite()),
        "non-finite Exec quantity"
    );
    Ok(positions)
}

fn prepare_source(
    source: &Source,
    ts_us: i64,
    old: &BTreeMap<String, Table>,
    new: &BTreeMap<String, Table>,
    mapping: &BTreeMap<String, Vec<String>>,
    files: &mut BTreeMap<String, Value>,
) -> Result<Value> {
    ensure!(
        source.allocation_audit["source_id"] == source.source_id
            && source.allocation_audit["snapshot_ts_us"] == ts_us,
        "Manager audit source or timestamp mismatch"
    );
    let audit: Vec<(String, String, i16, f64)> =
        serde_json::from_value(source.allocation_audit["strategies"].clone())
            .context("complete Manager strategy projection required")?;
    let mut before = BTreeMap::new();
    for (owner, symbol, venue, qty) in audit {
        ensure!(
            qty.is_finite() && before.insert((owner, symbol, venue), qty).is_none(),
            "invalid or duplicate Manager allocation row"
        );
    }
    let account_rows: Vec<(String, i16, f64)> =
        serde_json::from_value(source.allocation_audit["account"].clone())
            .context("complete Manager account projection required")?;
    let mut account = BTreeMap::new();
    for (symbol, venue, qty) in account_rows {
        ensure!(
            qty.is_finite() && account.insert((symbol, venue), qty).is_none(),
            "invalid or duplicate account projection"
        );
    }
    ensure!(
        before
            .keys()
            .all(|(_, symbol, venue)| account.contains_key(&(symbol.clone(), *venue))),
        "Manager audit lacks account coverage"
    );
    ensure!(
        before
            .iter()
            .all(|((name, _, _), qty)| !new.contains_key(name) || *qty == 0.0),
        "destination already owns historical Manager inventory"
    );
    let mut ledgers = source.ledgers.clone();
    ensure!(
        source.ledgers == source.ledgers_after,
        "Exec ownership changed across the capture"
    );
    ensure!(
        ledgers.len() == 2
            && ledgers.contains_key("batch_exec")
            && ledgers.contains_key("chase_exec"),
        "capture both complete Exec ownership ledgers"
    );
    let mut positions = ledgers
        .iter()
        .map(|(family, ledger)| Ok((family.clone(), ledger_positions(ledger)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    ensure!(
        source.exec_snapshot["position_ready"] == true
            && source.exec_snapshot["ts_ms"].as_i64() == Some(ts_us / 1000),
        "settled Exec snapshot must match cutover time"
    );
    let mut observed = BTreeSet::new();
    for row in source.exec_snapshot["rows"]
        .as_array()
        .context("complete Exec snapshot rows required")?
    {
        ensure!(
            row["active_batches"].as_u64() == Some(0)
                && row["live_order_qty"].as_f64() == Some(0.0),
            "capture Exec without active orders or batches"
        );
        let family = match row["algorithm"].as_str() {
            Some("batch" | "pov" | "ctaker" | "grid") => "batch_exec",
            Some("chase_exec") => "chase_exec",
            _ => anyhow::bail!("unknown Exec algorithm in capture"),
        };
        let name = row["strategy_name"]
            .as_str()
            .context("Exec strategy name missing")?;
        let symbol = row["symbol"].as_str().context("Exec symbol missing")?;
        let qty = row["current_qty"]
            .as_f64()
            .context("Exec current quantity missing")?;
        ensure!(
            observed.insert((family, name, symbol)),
            "duplicate Exec ownership observation"
        );
        ensure!(
            positions[family]
                .get(name)
                .and_then(|p| p.get(symbol))
                .is_some_and(|q| same_qty(*q, qty)),
            "Exec snapshot differs from durable ownership"
        );
        let expected_account = *account
            .get(&(symbol.into(), 1))
            .context("Manager audit lacks observed account symbol")?;
        let observed_account = row["account_position_qty"]
            .as_f64()
            .context("observed account quantity missing")?;
        ensure!(
            same_qty(expected_account, observed_account)
                || same_qty(f64::from(expected_account as f32), observed_account),
            "Manager account projection differs from observed Exec account"
        );
    }
    for rows in positions.values() {
        for name in new.keys() {
            ensure!(
                !rows.contains_key(name),
                "destination already exists in Exec: {name}"
            );
        }
    }
    let mut changes = BTreeMap::<(String, String, i16), (f64, f64)>::new();
    let mut new_shares = BTreeMap::new();
    let mut residuals = Vec::new();
    ensure!(
        source.residual_quote_limit.is_finite()
            && (0.0..=10.0).contains(&source.residual_quote_limit),
        "residual cap must be 0..=10 quote units"
    );
    for (old_name, share) in &source.shares {
        ensure!(
            share.is_finite() && *share >= 0.0 && old.contains_key(old_name),
            "invalid or unknown original binding shares"
        );
        let total = quantities(&old[old_name])?;
        let families = positions
            .iter()
            .filter(|(_, rows)| rows.contains_key(old_name))
            .map(|(family, _)| family.clone())
            .collect::<Vec<_>>();
        ensure!(
            families.len() == 1,
            "original strategy must belong to exactly one captured execution family: {old_name}"
        );
        let family = &families[0];
        for ledger in ledgers.values() {
            ensure!(
                ledger["inverse_notionals"]
                    .get(old_name)
                    .is_none_or(|v| v.as_object().is_some_and(|m| m.is_empty())),
                "inverse allocation is outside Prep linear migration scope"
            );
        }
        let owned = positions.get_mut(family).unwrap().remove(old_name).unwrap();
        let mut allocations = mapping[old_name]
            .iter()
            .map(|name| {
                Ok((
                    name.clone(),
                    quantities(&new[name])?
                        .into_iter()
                        .map(|(s, q)| (s, q * share))
                        .collect::<Quantities>(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let symbols = total
            .keys()
            .chain(owned.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for symbol in symbols {
            let qty = owned.get(&symbol).copied().unwrap_or_default();
            let target = total.get(&symbol).copied().unwrap_or_default() * share;
            if !same_qty(qty, target) {
                let mark = source
                    .marks
                    .get(&symbol)
                    .context("residual requires a completed minute mark")?;
                let row = source.exec_snapshot["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| row["strategy_name"] == *old_name && row["symbol"] == symbol)
                    .context("residual lacks a factual Exec completion observation")?;
                ensure!(
                    source.residual_quote_limit > 0.0
                        && ((qty - target) * mark.price).abs() <= source.residual_quote_limit
                        && row["execution_complete"] == true
                        && row["pending_qty"].as_f64() == Some(0.0)
                        && matches!(
                            row["completion_reason"].as_str(),
                            Some("target_reached" | "target_tolerance" | "exchange_minimum")
                        )
                        && row["target_qty"]
                            .as_f64()
                            .is_some_and(|q| same_qty(q, target)),
                    "Exec has not settled to the old signal target: {old_name}/{symbol}"
                );
                let eligible = mapping[old_name]
                    .iter()
                    .filter(|name| {
                        new[*name].combination.factor_rules.iter().any(|r| {
                            new[*name]
                                .combination
                                .rule_id(r)
                                .is_ok_and(|id| id.symbol == symbol)
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                ensure!(
                    !eligible.is_empty(),
                    "execution residual has no factual destination universe: {symbol}"
                );
                let gross: f64 = eligible
                    .iter()
                    .map(|name| {
                        allocations[name]
                            .get(&symbol)
                            .copied()
                            .unwrap_or_default()
                            .abs()
                    })
                    .sum();
                let mut remaining = qty - target;
                let mut audit = Vec::new();
                for (index, name) in eligible.iter().enumerate() {
                    let weight = if gross > 0.0 {
                        allocations[name]
                            .get(&symbol)
                            .copied()
                            .unwrap_or_default()
                            .abs()
                            / gross
                    } else {
                        1.0 / eligible.len() as f64
                    };
                    let adjustment = if index + 1 == eligible.len() {
                        remaining
                    } else {
                        (qty - target) * weight
                    };
                    remaining -= adjustment;
                    *allocations
                        .get_mut(name)
                        .unwrap()
                        .entry(symbol.clone())
                        .or_default() += adjustment;
                    audit.push(json!({"strategy":name,"adjustment":adjustment}));
                }
                residuals.push(json!({"old_strategy":old_name,"symbol":symbol,"target_qty":target,"actual_qty":qty,"adjustments":audit}));
            }
            ensure!(
                same_qty(
                    qty,
                    before
                        .get(&(old_name.clone(), symbol.clone(), 1))
                        .copied()
                        .unwrap_or_default()
                ),
                "Manager historical ownership differs from Exec: {old_name}/{symbol}"
            );
            if qty != 0.0 {
                changes.insert((old_name.clone(), symbol, 1), (qty, 0.0));
            }
        }
        // Preserve explicit zeros for retired strategy names in the current ledger.
        positions.get_mut(family).unwrap().insert(
            old_name.clone(),
            owned.keys().map(|s| (s.clone(), 0.0)).collect(),
        );
        for name in &mapping[old_name] {
            let allocated = allocations.remove(name).unwrap();
            for (symbol, qty) in &allocated {
                ensure!(
                    qty.is_finite()
                        && before
                            .get(&(name.clone(), symbol.clone(), 1))
                            .copied()
                            .unwrap_or_default()
                            == 0.0,
                    "destination already owns a Manager position or quantity overflowed"
                );
                if *qty != 0.0 {
                    changes.insert((name.clone(), symbol.clone(), 1), (0.0, *qty));
                }
            }
            positions
                .get_mut(family)
                .unwrap()
                .insert(name.clone(), allocated);
            new_shares.insert(name.clone(), share);
        }
    }
    for (old_name, table) in old {
        if !source.shares.contains_key(old_name) {
            ensure!(
                positions.values().all(|rows| rows
                    .get(old_name)
                    .is_none_or(|p| p.values().all(|q| *q == 0.0)))
                    && before
                        .iter()
                        .all(|((name, _, _), qty)| name != old_name || *qty == 0.0),
                "source owns an original strategy without a captured binding: {}",
                table.strategy_name
            );
        }
    }
    // Every symbol's family totals and account total must survive redistribution.
    for (family, rows) in &positions {
        let original = ledger_positions(&source.ledgers[family])?;
        let sum = |rows: &BTreeMap<String, Quantities>| -> Result<Quantities> {
            let mut result = Quantities::new();
            for row in rows.values() {
                for (s, q) in row {
                    add_target_quantity(&mut result, s, *q)?;
                }
            }
            Ok(result)
        };
        let a = sum(&original)?;
        let b = sum(rows)?;
        ensure!(
            a.keys().chain(b.keys()).all(|s| same_qty(
                a.get(s).copied().unwrap_or_default(),
                b.get(s).copied().unwrap_or_default()
            )),
            "execution family total changed"
        );
        let ledger = ledgers.get_mut(family).unwrap();
        if !ledger.is_null() {
            ledger["positions"] = serde_json::to_value(rows)?;
            ledger["updated_at_us"] = json!(ts_us);
        } else {
            ensure!(
                rows.is_empty(),
                "cannot allocate to an absent execution family"
            );
        }
        files.insert(
            format!(
                "sources/{}/original-{family}-allocations.json",
                source.source_id
            ),
            source.ledgers[family].clone(),
        );
        files.insert(
            format!("sources/{}/{family}-allocations.json", source.source_id),
            ledger.clone(),
        );
    }
    let mut markets = BTreeMap::<String, f64>::new();
    let mut corrections = Vec::new();
    for ((strategy, symbol, venue), (before_qty, after_qty)) in changes {
        let mark = source
            .marks
            .get(&symbol)
            .with_context(|| format!("completed minute mark required for {symbol}"))?;
        ensure!(
            mark.close_ts_us == ts_us.div_euclid(60_000_000) * 60_000_000
                && mark.price.is_finite()
                && mark.price > 0.0,
            "invalid or mismatched completed minute mark for {symbol}"
        );
        *markets.entry(symbol.clone()).or_default() += after_qty - before_qty;
        corrections.push(json!({"strategy":strategy,"symbol":symbol,"venue_code":venue,"before_qty":before_qty,"after_qty":after_qty,"reference_price":mark.price}));
    }
    for (symbol, delta) in &markets {
        ensure!(
            (delta * source.marks[symbol].price).abs() < 1e-6,
            "PnL ownership transfer changes account quantity: {symbol}"
        );
        if source.strategy_anchor {
            let qty = before
                .get(&("__unallocated__".into(), symbol.clone(), 1))
                .copied()
                .unwrap_or_default();
            corrections.push(json!({"strategy":"__unallocated__","symbol":symbol,"venue_code":1,"before_qty":qty,"after_qty":qty,"reference_price":source.marks[symbol].price}));
        }
    }
    if !corrections.is_empty() {
        files.insert(
            format!("sources/{}/manager-allocation.json", source.source_id),
            json!({"source_id":source.source_id,"snapshot_ts_us":ts_us,"positions":corrections}),
        );
    }
    Ok(
        json!({"source_id":source.source_id,"old_shares":source.shares,"new_shares":new_shares,"execution_residuals":residuals,"manager_audit_sha256":fingerprint(&source.allocation_audit)?,"account_quantity_unchanged":true,"pnl_correction_required":!corrections.is_empty()}),
    )
}

/// Save into a new diagnostic directory; existing destinations are never replaced.
pub fn write_bundle(directory: &Path, bundle: &Bundle) -> Result<()> {
    ensure!(!directory.exists(), "migration output already exists");
    let parent = directory.parent().context("output parent missing")?;
    ensure!(parent.is_dir(), "create the output parent first");
    let temporary = parent.join(format!(".cta-migration-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&temporary)?;
    let result = (|| -> Result<()> {
        for (path, value) in &bundle.files {
            super::super::runtime::atomic_json(&temporary.join(path), value)?;
        }
        super::super::runtime::atomic_json(&temporary.join("migration.json"), &bundle.summary)?;
        fs::rename(&temporary, directory)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
