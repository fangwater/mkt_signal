//! Integer, point-in-time roll engine; Python correctness oracle is
//! scripts/cme_causal_roll.py. No calendar or availability is inferred here.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
pub struct Input {
    #[serde(default)]
    config: Config,
    decisions: Vec<Decision>,
    contracts: Vec<Contract>,
    volumes: Vec<Volume>,
    settlements: Vec<Settlement>,
}
#[derive(Deserialize)]
#[serde(default)]
struct Config {
    confirmation_days: u64,
    decision_buffer_seconds: u64,
    max_reference_age: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            confirmation_days: 2,
            decision_buffer_seconds: 300,
            max_reference_age: 3,
        }
    }
}
#[derive(Deserialize)]
struct ReferenceDay {
    day: String,
    settlement_expected: bool,
}
#[derive(Deserialize)]
struct Decision {
    product: String,
    trading_day: String,
    previous_day: String,
    decision_ns: u64,
    effective_ns: u64,
    previous_close_ns: u64,
    calendar_known_at_ns: Option<u64>,
    calendar_source: String,
    reference_days: Vec<ReferenceDay>,
    deadline_buffer_session: Option<BufferSession>,
}
#[derive(Deserialize, Serialize)]
struct BufferSession {
    day: String,
    segments: Vec<[u64; 2]>,
    known_at_ns: u64,
    source: String,
}
#[derive(Deserialize)]
struct Contract {
    product: String,
    ric: String,
    delivery_month: String,
    known_at_ns: Option<u64>,
    valid_from_ns: u64,
    valid_to_ns: Option<u64>,
    listed_ns: u64,
    roll_by_ns: Option<u64>,
    risk_deadline_ns: Option<u64>,
    risk_date: Option<String>,
    verified: bool,
    source: String,
}
#[derive(Deserialize)]
struct Quantity {
    ric: String,
    volume: Option<u64>,
    volume_lower: Option<u64>,
    volume_upper: Option<u64>,
}
impl Quantity {
    fn bounds(&self) -> Option<(u64, u64)> {
        self.volume
            .map(|n| (n, n))
            .or_else(|| self.volume_lower.zip(self.volume_upper))
    }
}
#[derive(Deserialize)]
struct Volume {
    product: String,
    trading_day: String,
    available_at_ns: Option<u64>,
    complete: bool,
    source: String,
    rows: Vec<Quantity>,
}
#[derive(Deserialize)]
struct Settlement {
    product: String,
    ric: String,
    reference_day: String,
    price_e9: Option<i64>,
    published_ns: u64,
    part: u64,
    seq: u64,
    basis: String,
}
#[derive(Default)]
struct State {
    current: Option<String>,
    floor: Option<String>,
    challenger: Option<String>,
    count: u64,
    last_day: Option<String>,
    offset: i64,
    segment: u64,
    gap: bool,
}

fn product(p: &str) -> Result<()> {
    ensure!(
        ["URO", "JY", "HG", "SI", "TU", "FV", "TY", "TN", "US", "AUL"].contains(&p),
        "unsupported product {p}"
    );
    Ok(())
}
fn day(d: &str) -> Result<()> {
    ensure!(
        d.len() == 10 && d.as_bytes()[..4].iter().all(u8::is_ascii_digit) && !d.starts_with("0000"),
        "date outside Python baseline range"
    );
    ensure!(
        chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")?.to_string() == d,
        "non-canonical date"
    );
    Ok(())
}
fn number(n: u64) -> Result<()> {
    ensure!(n <= i64::MAX as u64, "integer out of range");
    Ok(())
}
fn distinct<K: Ord>(set: &mut BTreeSet<K>, key: K) -> Result<()> {
    ensure!(set.insert(key), "duplicate input key");
    Ok(())
}

fn validate(data: &Input) -> Result<()> {
    ensure!(
        data.config.confirmation_days > 0
            && data.config.max_reference_age > 0
            && data.config.decision_buffer_seconds > 0,
        "invalid config"
    );
    number(data.config.confirmation_days)?;
    number(data.config.max_reference_age as u64)?;
    number(data.config.decision_buffer_seconds)?;
    let buffer_ns = data
        .config
        .decision_buffer_seconds
        .checked_mul(1_000_000_000)
        .context("buffer overflow")?;
    let mut seen = BTreeSet::new();
    for c in &data.contracts {
        product(&c.product)?;
        day(&format!("{}-01", c.delivery_month))?;
        if let Some(risk_date) = &c.risk_date {
            day(risk_date)?;
        }
        distinct(&mut seen, (&c.product, &c.ric, c.known_at_ns))?;
        for n in [
            Some(c.listed_ns),
            Some(c.valid_from_ns),
            c.known_at_ns,
            c.valid_to_ns,
            c.roll_by_ns,
            c.risk_deadline_ns,
        ]
        .into_iter()
        .flatten()
        {
            number(n)?;
        }
        if let (Some(r), Some(b)) = (c.roll_by_ns, c.risk_deadline_ns) {
            ensure!(r < b, "roll_by must precede risk deadline");
        }
    }
    let mut seen = BTreeSet::new();
    for v in &data.volumes {
        product(&v.product)?;
        day(&v.trading_day)?;
        distinct(&mut seen, (&v.product, &v.trading_day, v.available_at_ns))?;
        if let Some(n) = v.available_at_ns {
            number(n)?;
        }
        let mut rics = BTreeSet::new();
        for q in &v.rows {
            distinct(&mut rics, &q.ric)?;
            if let Some(n) = q.volume {
                number(n)?;
            }
            ensure!(
                q.volume_lower.is_some() == q.volume_upper.is_some(),
                "volume bounds must be supplied together"
            );
            if let (Some(lower), Some(upper)) = (q.volume_lower, q.volume_upper) {
                number(lower)?;
                number(upper)?;
                ensure!(
                    lower <= upper && q.volume.is_none_or(|n| lower == n && upper == n),
                    "inconsistent volume bounds"
                );
            }
        }
    }
    let mut seen = BTreeSet::new();
    for s in &data.settlements {
        product(&s.product)?;
        day(&s.reference_day)?;
        for n in [s.published_ns, s.part, s.seq] {
            number(n)?;
        }
        distinct(
            &mut seen,
            (
                &s.product,
                &s.ric,
                &s.reference_day,
                &s.basis,
                s.published_ns,
                s.part,
                s.seq,
            ),
        )?;
    }
    let mut seen = BTreeSet::new();
    let mut last = None;
    for d in &data.decisions {
        product(&d.product)?;
        day(&d.trading_day)?;
        day(&d.previous_day)?;
        distinct(&mut seen, (&d.product, &d.trading_day))?;
        let key = (d.decision_ns, &d.product);
        ensure!(last.is_none_or(|x| x <= key), "decisions out of order");
        last = Some(key);
        for n in [d.decision_ns, d.effective_ns, d.previous_close_ns] {
            number(n)?;
        }
        if let Some(n) = d.calendar_known_at_ns {
            number(n)?;
        }
        ensure!(
            d.effective_ns.checked_sub(d.decision_ns) == Some(buffer_ns)
                && d.previous_close_ns < d.decision_ns
                && d.previous_day < d.trading_day,
            "bad decision timing"
        );
        if let Some(session) = &d.deadline_buffer_session {
            day(&session.day)?;
            number(session.known_at_ns)?;
            ensure!(
                session.day > d.trading_day
                    && !session.source.trim().is_empty()
                    && !session.segments.is_empty(),
                "invalid deadline buffer calendar"
            );
            let mut previous = None;
            for &[lo, hi] in &session.segments {
                number(lo)?;
                number(hi)?;
                ensure!(
                    lo % 60_000_000_000 == 0
                        && hi % 60_000_000_000 == 0
                        && lo < hi
                        && previous.is_none_or(|p| p <= lo),
                    "invalid deadline buffer segments"
                );
                previous = Some(hi);
            }
            ensure!(
                session.segments[0][0] > d.effective_ns,
                "deadline buffer must open after effective time"
            );
        }
        ensure!(
            d.reference_days
                .first()
                .is_some_and(|r| r.day == d.previous_day),
            "bad reference start"
        );
        let mut previous = None;
        for r in &d.reference_days {
            day(&r.day)?;
            ensure!(previous.is_none_or(|p| p > &r.day), "bad reference order");
            previous = Some(&r.day);
        }
    }
    Ok(())
}

fn blocked(
    s: &mut State,
    out: &mut Value,
    d: &Decision,
    detail: &str,
    exit: bool,
    old: &Option<String>,
) {
    out["detail"] = json!(detail);
    s.challenger = None;
    s.count = 0;
    s.gap = true;
    s.last_day = Some(d.trading_day.clone());
    if exit {
        out["exit_ric"] = json!(old);
        s.current = None;
    }
}

fn bridge(data: &Input, d: &Decision, old: &str, new: &str) -> Option<(Value, i128)> {
    for (i, reference) in d
        .reference_days
        .iter()
        .take(data.config.max_reference_age)
        .enumerate()
    {
        if !reference.settlement_expected {
            continue;
        }
        let mut selected = Vec::new();
        for ric in [old, new] {
            let last = data
                .settlements
                .iter()
                .filter(|r| {
                    r.product == d.product
                        && r.ric == ric
                        && r.reference_day == reference.day
                        && r.basis == "daily_settlement"
                        && r.published_ns < d.decision_ns
                })
                .max_by_key(|r| (r.published_ns, r.part, r.seq))?;
            last.price_e9?;
            selected.push(last);
        }
        let a = selected[0];
        let b = selected[1];
        return Some((
            json!({"reference_day":reference.day,"reference_age":i+1,"old_price_e9":a.price_e9,
            "new_price_e9":b.price_e9,"old_published_ns":a.published_ns,"new_published_ns":b.published_ns}),
            a.price_e9? as i128 - b.price_e9? as i128,
        ));
    }
    None
}

fn has_full_buffer(d: &Decision, contract: &Contract) -> bool {
    d.deadline_buffer_session.as_ref().is_some_and(|session| {
        session.known_at_ns < d.decision_ns
            && contract
                .risk_date
                .as_ref()
                .is_some_and(|risk| &session.day < risk)
            && contract
                .risk_deadline_ns
                .is_some_and(|risk| session.segments.last().unwrap()[1] < risk)
    })
}

fn before_roll(d: &Decision, contract: &Contract) -> bool {
    contract.risk_deadline_ns.is_some()
        && contract.roll_by_ns.map_or_else(
            || has_full_buffer(d, contract),
            |roll_by| d.effective_ns < roll_by,
        )
}

fn step(data: &Input, d: &Decision, s: &mut State) -> Result<Value> {
    ensure!(
        s.last_day.as_ref().is_none_or(|p| p < &d.trading_day),
        "decisions must advance per product"
    );
    if s.last_day.as_ref() != Some(&d.previous_day) {
        s.challenger = None;
        s.count = 0;
        if s.last_day.is_some() {
            s.gap = true;
        }
    }
    let old = s.current.clone();
    let mut out = json!({"product":d.product,"trading_day":d.trading_day,"decision_ns":d.decision_ns,"effective_ns":d.effective_ns,
        "previous_day":d.previous_day,"previous_ric":old,"selected_ric":null,"reason":"blocked","detail":"",
        "exit_ric":null,"entry_ric":null,"challenger":null,"confirmation_count":0,"segment_id":null,"offset_e9":null,
        "adjustment_status":"gap","reference_day":null,"reference_age":null,"old_price_e9":null,"new_price_e9":null,
        "old_published_ns":null,"new_published_ns":null,"calendar_source":d.calendar_source,"calendar_known_at_ns":d.calendar_known_at_ns,
        "volume_available_at_ns":null,"volume_source":null,"volume_complete":false,"current_metadata_known_at_ns":null,
        "current_roll_by_ns":null,"current_risk_deadline_ns":null,"selected_metadata_known_at_ns":null,"selected_roll_by_ns":null,
        "selected_risk_deadline_ns":null,"current_volume":null,"selected_volume":null,"candidate_comparisons":[]});
    if d.calendar_known_at_ns.is_none_or(|n| n >= d.decision_ns) || d.calendar_source.is_empty() {
        blocked(s, &mut out, d, "calendar_not_known", false, &old);
        return Ok(out);
    }
    let mut versions: BTreeMap<&str, &Contract> = BTreeMap::new();
    for c in &data.contracts {
        if c.product != d.product
            || c.known_at_ns.is_none_or(|n| n >= d.decision_ns)
            || c.valid_from_ns > d.effective_ns
        {
            continue;
        }
        if versions
            .get(c.ric.as_str())
            .is_none_or(|v| v.known_at_ns < c.known_at_ns)
        {
            versions.insert(&c.ric, c);
        }
    }
    let metadata = versions
        .into_iter()
        .filter(|(_, c)| {
            c.verified
                && !c.source.is_empty()
                && c.valid_from_ns <= d.effective_ns
                && c.valid_to_ns.is_none_or(|n| d.effective_ns < n)
        })
        .collect::<BTreeMap<_, _>>();
    let current = old.as_deref().and_then(|r| metadata.get(r)).copied();
    if old.is_some()
        && current.is_none_or(|m| {
            m.risk_deadline_ns.is_none() || (m.roll_by_ns.is_none() && !has_full_buffer(d, m))
        })
    {
        blocked(
            s,
            &mut out,
            d,
            "current_deadline_unknown_or_invalid",
            true,
            &old,
        );
        return Ok(out);
    }
    let forced = current.is_some_and(|m| m.roll_by_ns.is_some_and(|r| d.effective_ns >= r));
    if let Some(c) = current {
        out["current_metadata_known_at_ns"] = json!(c.known_at_ns);
        out["current_roll_by_ns"] = json!(c.roll_by_ns);
        out["current_risk_deadline_ns"] = json!(c.risk_deadline_ns);
    }
    let candidates = metadata
        .iter()
        .filter(|(ric, m)| {
            m.listed_ns < d.decision_ns
                && before_roll(d, m)
                && s.floor.as_ref().is_none_or(|f| &m.delivery_month >= f)
                && current.is_none_or(|c| {
                    Some(**ric) == old.as_deref() || m.delivery_month > c.delivery_month
                })
        })
        .map(|(r, c)| (*r, *c))
        .collect::<BTreeMap<_, _>>();
    if current.is_some_and(|m| m.roll_by_ns.is_none())
        || candidates.values().any(|m| m.roll_by_ns.is_none())
    {
        out["deadline_buffer_session"] = json!(d.deadline_buffer_session);
    }
    let batch = data
        .volumes
        .iter()
        .filter(|b| {
            b.product == d.product
                && b.trading_day == d.previous_day
                && b.available_at_ns.is_some_and(|n| n < d.decision_ns)
        })
        .max_by_key(|b| b.available_at_ns);
    let quantities = batch
        .map(|b| {
            b.rows
                .iter()
                .map(|q| (q.ric.as_str(), q.volume))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let bounds = batch
        .map(|b| {
            b.rows
                .iter()
                .map(|q| (q.ric.as_str(), q.bounds()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let bounded = bounds
        .values()
        .any(|pair| pair.is_some_and(|(lo, hi)| lo != hi));
    if let Some(b) = batch {
        out["volume_available_at_ns"] = json!(b.available_at_ns);
        out["volume_source"] = json!(b.source);
        out["volume_complete"] = json!(b.complete);
        out["current_volume"] = json!(old
            .as_deref()
            .and_then(|r| quantities.get(r))
            .copied()
            .flatten());
    }
    if bounded {
        let current = old
            .as_deref()
            .and_then(|r| bounds.get(r))
            .copied()
            .flatten();
        out["volume_comparison_basis"] = json!("bounded_source_quantities");
        out["current_volume_lower"] = json!(current.map(|(lo, _)| lo));
        out["current_volume_upper"] = json!(current.map(|(_, hi)| hi));
        out["selected_volume_lower"] = Value::Null;
        out["selected_volume_upper"] = Value::Null;
    }
    out["candidate_comparisons"] = json!(candidates.iter().map(|(ric, m)| {
        let mut row = json!({"ric":ric,"delivery_month":m.delivery_month,
            "volume":quantities.get(*ric).copied().flatten(),"known_at_ns":m.known_at_ns,"roll_by_ns":m.roll_by_ns,"source":m.source});
        if bounded {
            let pair = bounds.get(*ric).copied().flatten();
            row["volume_lower"] = json!(pair.map(|(lo, _)| lo));
            row["volume_upper"] = json!(pair.map(|(_, hi)| hi));
        }
        if m.roll_by_ns.is_none() {
            row["deadline_basis"] = json!("known_future_session");
            row["risk_date"] = json!(m.risk_date);
        }
        row
    }).collect::<Vec<_>>());
    let mut required = candidates.keys().copied().collect::<BTreeSet<_>>();
    if !forced {
        if let Some(r) = old.as_deref() {
            required.insert(r);
        }
    }
    let volume_ok = batch.is_some_and(|b| b.complete && b.source == "tas_printable")
        && required
            .iter()
            .all(|r| bounds.get(*r).is_some_and(Option::is_some));
    let mut selected = old.clone();
    let mut reason = "hold";
    if !volume_ok {
        if !forced {
            blocked(
                s,
                &mut out,
                d,
                "volume_missing_incomplete_or_unavailable",
                false,
                &old,
            );
            return Ok(out);
        }
        selected = None;
        reason = "exit_only";
        out["detail"] = json!("deadline_without_complete_volume");
    } else {
        let winner = candidates
            .keys()
            .copied()
            .filter(|ric| bounds[*ric].unwrap().1 > 0 && (!forced || Some(*ric) != old.as_deref()))
            .min_by(|a, b| {
                bounds[b]
                    .unwrap()
                    .0
                    .cmp(&bounds[a].unwrap().0)
                    .then(
                        candidates[a]
                            .delivery_month
                            .cmp(&candidates[b].delivery_month),
                    )
                    .then(a.cmp(b))
            });
        let mut ambiguous = winner.is_some_and(|w| {
            let lower = bounds[w].unwrap().0;
            lower == 0
                || candidates.keys().copied().any(|r| {
                    if r == w || (forced && Some(r) == old.as_deref()) {
                        return false;
                    }
                    let upper = bounds[r].unwrap().1;
                    upper > lower
                        || (upper == lower
                            && (&candidates[r].delivery_month, r)
                                < (&candidates[w].delivery_month, w))
                })
        });
        if let (Some(w), Some(current)) = (winner, old.as_deref()) {
            if !forced && w != current && !ambiguous {
                let (lo, hi) = bounds[w].unwrap();
                let (old_lo, old_hi) = bounds[current].unwrap();
                ambiguous = lo <= old_hi && hi > old_lo;
            }
        }
        if ambiguous {
            if !forced {
                blocked(s, &mut out, d, "ambiguous_volume_ranking", false, &old);
                return Ok(out);
            }
            selected = None;
            reason = "exit_only";
            out["detail"] = json!("deadline_ambiguous_volume_ranking");
        } else if forced {
            selected = winner.map(str::to_owned);
            reason = if winner.is_some() {
                "deadline"
            } else {
                "exit_only"
            };
            if winner.is_none() {
                out["detail"] = json!("deadline_without_eligible_successor");
            }
        } else if old.is_none() {
            let Some(w) = winner else {
                blocked(s, &mut out, d, "no_eligible_contract", false, &old);
                return Ok(out);
            };
            selected = Some(w.to_string());
            reason = "seed";
        } else if winner.is_some_and(|w| {
            Some(w) != old.as_deref()
                && bounds[w].unwrap().0 > bounds[old.as_deref().unwrap()].unwrap().1
        }) {
            let w = winner.unwrap();
            s.count = if s.challenger.as_deref() == Some(w) {
                s.count + 1
            } else {
                1
            };
            s.challenger = Some(w.to_string());
            if s.count >= data.config.confirmation_days {
                selected = Some(w.to_string());
                reason = "volume";
            }
        } else {
            s.challenger = None;
            s.count = 0;
        }
    }
    out["selected_ric"] = json!(selected);
    out["reason"] = json!(reason);
    out["challenger"] = json!(s.challenger);
    out["confirmation_count"] = json!(s.count);
    if let Some(r) = selected.as_deref() {
        let c = metadata[r];
        out["selected_metadata_known_at_ns"] = json!(c.known_at_ns);
        out["selected_roll_by_ns"] = json!(c.roll_by_ns);
        out["selected_risk_deadline_ns"] = json!(c.risk_deadline_ns);
        out["selected_volume"] = json!(quantities.get(r).copied().flatten());
        if bounded {
            let pair = bounds.get(r).copied().flatten();
            out["selected_volume_lower"] = json!(pair.map(|(lo, _)| lo));
            out["selected_volume_upper"] = json!(pair.map(|(_, hi)| hi));
        }
    }
    if selected != old {
        out["exit_ric"] = json!(old);
        out["entry_ric"] = json!(selected);
    }
    let Some(ref selected_ric) = selected else {
        s.current = None;
        s.challenger = None;
        s.count = 0;
        s.gap = true;
        s.last_day = Some(d.trading_day.clone());
        return Ok(out);
    };
    if s.gap || old.is_none() {
        s.segment += 1;
        s.offset = 0;
        out["adjustment_status"] = json!("anchor");
    } else if selected != old {
        if let Some((b, delta)) = bridge(data, d, old.as_deref().unwrap(), selected_ric) {
            s.offset = (s.offset as i128 + delta)
                .try_into()
                .context("offset overflow")?;
            for (k, v) in b.as_object().context("bridge object")? {
                out[k] = v.clone();
            }
            out["adjustment_status"] = json!("settlement_bridge");
        } else {
            s.segment += 1;
            s.offset = 0;
            out["adjustment_status"] = json!("missing_bridge_new_segment");
        }
    } else {
        out["adjustment_status"] = json!("carry");
    }
    s.floor = Some(metadata[selected_ric.as_str()].delivery_month.clone());
    s.current = selected.clone();
    s.gap = false;
    s.last_day = Some(d.trading_day.clone());
    out["segment_id"] = json!(s.segment);
    out["offset_e9"] = json!(s.offset);
    if selected != old {
        s.challenger = None;
        s.count = 0;
    }
    Ok(out)
}

pub fn replay(data: &Input) -> Result<Vec<Value>> {
    validate(data)?;
    let mut states = BTreeMap::<&str, State>::new();
    let mut output = Vec::new();
    for d in &data.decisions {
        let s = states.entry(&d.product).or_insert_with(|| State {
            gap: true,
            ..State::default()
        });
        output.push(step(data, d, s)?);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_quantity_python_fixture() {
        let input: Input =
            serde_json::from_str(include_str!("../tests/fixtures/causal_roll_bounded.json"))
                .unwrap();
        let expected: Vec<Value> = serde_json::from_str(include_str!(
            "../tests/fixtures/causal_roll_bounded.expected.json"
        ))
        .unwrap();
        assert_eq!(replay(&input).unwrap(), expected);
    }
    #[test]
    fn python_fixture() {
        let input: Input =
            serde_json::from_str(include_str!("../tests/fixtures/causal_roll.json")).unwrap();
        let expected: Vec<Value> =
            serde_json::from_str(include_str!("../tests/fixtures/causal_roll.expected.json"))
                .unwrap();
        assert_eq!(replay(&input).unwrap(), expected);
    }
    #[test]
    fn future_calendar_python_fixture() {
        let input: Input = serde_json::from_str(include_str!(
            "../tests/fixtures/causal_roll_future_calendar.json"
        ))
        .unwrap();
        let expected: Vec<Value> = serde_json::from_str(include_str!(
            "../tests/fixtures/causal_roll_future_calendar.expected.json"
        ))
        .unwrap();
        assert_eq!(replay(&input).unwrap(), expected);
    }
}
