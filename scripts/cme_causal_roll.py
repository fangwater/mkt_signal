"""Point-in-time CME roll/forward adjustment correctness baseline.

Input is the reviewed decision/metadata/volume/settlement contract documented in
preprocess/data_format/lseg/causal_roll_contract.md. This does not infer missing
availability timestamps, exchange calendars, fills, or delivery deadlines.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path

PRODUCTS = frozenset("URO JY HG SI TU FV TY TN US AUL".split())
I64_MIN, I64_MAX = -(2**63), 2**63 - 1


def integer(value, name, minimum=0, maximum=I64_MAX):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name}: expected integer in [{minimum}, {maximum}]")
    return value


def day(value):
    if date.fromisoformat(value).isoformat() != value:
        raise ValueError(f"non-canonical date: {value}")
    return value


def unique(rows, key, label):
    seen = set()
    for row in rows:
        k = key(row)
        if k in seen:
            raise ValueError(f"duplicate {label}: {k}")
        seen.add(k)


def quantity_range(row):
    """Exact quantities are degenerate intervals; missing data has no bound."""
    exact = row["volume"]
    lower, upper = row.get("volume_lower"), row.get("volume_upper")
    if exact is not None:
        integer(exact, "volume")
    if (lower is None) != (upper is None):
        raise ValueError("volume bounds must be supplied together")
    if lower is not None:
        integer(lower, "volume_lower"); integer(upper, "volume_upper")
        if lower > upper or (exact is not None and (lower != exact or upper != exact)):
            raise ValueError("inconsistent volume bounds")
        return lower, upper
    return (exact, exact) if exact is not None else None


@dataclass
class State:
    current: str | None = None
    floor: str | None = None
    challenger: str | None = None
    count: int = 0
    last_day: str | None = None
    offset: int = 0
    segment: int = 0
    gap: bool = True


class Replay:
    def __init__(self, data):
        cfg = data.get("config", {})
        self.confirm = integer(cfg.get("confirmation_days", 2), "confirmation_days", 1)
        self.buffer_ns = integer(cfg.get("decision_buffer_seconds", 300), "decision_buffer_seconds", 1) * 10**9
        self.lookback = integer(cfg.get("max_reference_age", 3), "max_reference_age", 1)
        self.contracts, self.volumes, self.prices = defaultdict(list), defaultdict(list), defaultdict(list)
        self.states = defaultdict(State)
        unique(data["contracts"], lambda r: (r["product"], r["ric"], r["known_at_ns"]), "contract version")
        unique(data["volumes"], lambda r: (r["product"], r["trading_day"], r["available_at_ns"]), "volume version")
        unique(data["settlements"], lambda r: (r["product"], r["ric"], r["reference_day"], r["basis"], r["published_ns"], r["part"], r["seq"]), "settlement event")
        for r in data["contracts"]:
            self.product(r)
            day(r["delivery_month"] + "-01")
            if r.get("risk_date") is not None:
                day(r["risk_date"])
            for key in ("known_at_ns", "valid_to_ns", "roll_by_ns", "risk_deadline_ns"):
                if r[key] is not None:
                    integer(r[key], key)
            for key in ("valid_from_ns", "listed_ns"):
                integer(r[key], key)
            if type(r["verified"]) is not bool:
                raise ValueError("verified must be boolean")
            if r["roll_by_ns"] is not None and r["risk_deadline_ns"] is not None and r["roll_by_ns"] >= r["risk_deadline_ns"]:
                raise ValueError("roll_by must precede risk deadline")
            self.contracts[(r["product"], r["ric"])].append(r)
        for r in data["volumes"]:
            self.product(r)
            day(r["trading_day"])
            if r["available_at_ns"] is not None:
                integer(r["available_at_ns"], "available_at_ns")
            if type(r["complete"]) is not bool:
                raise ValueError("complete must be boolean")
            unique(r["rows"], lambda x: x["ric"], "volume RIC")
            for row in r["rows"]:
                quantity_range(row)
            self.volumes[(r["product"], r["trading_day"])].append(r)
        for r in data["settlements"]:
            self.product(r)
            day(r["reference_day"])
            for key in ("published_ns", "part", "seq"):
                integer(r[key], key)
            if r["price_e9"] is not None:
                integer(r["price_e9"], "price_e9", I64_MIN)
            self.prices[(r["product"], r["ric"], r["reference_day"], r["basis"])].append(r)

    @staticmethod
    def product(row):
        if row["product"] not in PRODUCTS:
            raise ValueError(f"unsupported product: {row['product']}")

    def contract_snapshot(self, product, cutoff, effective):
        result = {}
        for (p, ric), versions in self.contracts.items():
            if p != product:
                continue
            visible = [v for v in versions if v["known_at_ns"] is not None and v["known_at_ns"] < cutoff
                       and v["valid_from_ns"] <= effective]
            if not visible:
                continue
            # Do not resurrect an older valid version after an invalidation.
            latest = max(visible, key=lambda r: r["known_at_ns"])
            if latest["verified"] and latest["source"] and latest["valid_from_ns"] <= effective and (latest["valid_to_ns"] is None or effective < latest["valid_to_ns"]):
                result[ric] = latest
        return result

    def bridge(self, packet, old, new):
        product, cutoff = packet["product"], packet["decision_ns"]
        for age, reference in enumerate(packet["reference_days"][:self.lookback], 1):
            # A holiday may have an indicative value; it is not a daily settlement.
            if not reference["settlement_expected"]:
                continue
            chosen = []
            for ric in (old, new):
                rows = self.prices.get((product, ric, reference["day"], "daily_settlement"), ())
                rows = [r for r in rows if r["published_ns"] < cutoff]
                last = max(rows, key=lambda r: (r["published_ns"], r["part"], r["seq"]), default=None)
                if last is None or last["price_e9"] is None:
                    return None  # Missing expected settlement is not a holiday.
                chosen.append(last)
            return dict(reference_day=reference["day"], reference_age=age,
                        old_price_e9=chosen[0]["price_e9"], new_price_e9=chosen[1]["price_e9"],
                        old_published_ns=chosen[0]["published_ns"], new_published_ns=chosen[1]["published_ns"])
        return None

    @staticmethod
    def deadline_buffer_session(packet):
        session = packet.get("deadline_buffer_session")
        if session is None:
            return None
        if day(session["day"]) <= packet["trading_day"]:
            raise ValueError("deadline buffer session must follow the effective trading day")
        known = integer(session["known_at_ns"], "buffer calendar known_at_ns")
        if not isinstance(session["source"], str) or not session["source"].strip():
            raise ValueError("deadline buffer session needs a calendar source")
        segments = session["segments"]
        if not segments:
            raise ValueError("deadline buffer session must be a complete open trading day")
        previous = None
        for lo, hi in segments:
            integer(lo, "buffer open_ns"); integer(hi, "buffer close_ns")
            if lo % 60_000_000_000 or hi % 60_000_000_000 or hi <= lo or (previous is not None and lo < previous):
                raise ValueError("invalid deadline buffer session segments")
            previous = hi
        if segments[0][0] <= packet["effective_ns"]:
            raise ValueError("deadline buffer session must open after effective time")
        if known >= packet["decision_ns"]:
            return None
        return {k: session[k] for k in ("day", "segments", "known_at_ns", "source")}

    def step(self, p):
        self.product(p)
        t, d = day(p["trading_day"]), day(p["previous_day"])
        cutoff, effective = integer(p["decision_ns"], "decision_ns"), integer(p["effective_ns"], "effective_ns")
        if effective - cutoff != self.buffer_ns or integer(p["previous_close_ns"], "previous_close_ns") >= cutoff or d >= t:
            raise ValueError("decision must follow the previous close and precede effective open by configured buffer")
        references = p["reference_days"]
        if not references or references[0]["day"] != d:
            raise ValueError("reference_days must start at previous_day")
        days = [day(r["day"]) for r in references]
        if days != sorted(set(days), reverse=True) or any(type(r["settlement_expected"]) is not bool for r in references):
            raise ValueError("reference_days must be distinct descending completed trading days")
        buffer_session = self.deadline_buffer_session(p)
        s = self.states[p["product"]]
        if s.last_day is not None and t <= s.last_day:
            raise ValueError("decisions must advance per product")
        if s.last_day != d:
            s.challenger, s.count = None, 0
            if s.last_day is not None:
                s.gap = True
        old = s.current
        out = dict(product=p["product"], trading_day=t, decision_ns=cutoff, effective_ns=effective,
                   previous_day=d, previous_ric=old, selected_ric=None, reason="blocked", detail="",
                   exit_ric=None, entry_ric=None, challenger=None, confirmation_count=0,
                   segment_id=None, offset_e9=None, adjustment_status="gap", reference_day=None,
                   reference_age=None, old_price_e9=None, new_price_e9=None,
                   old_published_ns=None, new_published_ns=None,
                   calendar_source=p["calendar_source"], calendar_known_at_ns=p["calendar_known_at_ns"],
                   volume_available_at_ns=None, volume_source=None, volume_complete=False,
                   current_metadata_known_at_ns=None, current_roll_by_ns=None, current_risk_deadline_ns=None,
                   selected_metadata_known_at_ns=None, selected_roll_by_ns=None, selected_risk_deadline_ns=None,
                   current_volume=None, selected_volume=None, candidate_comparisons=[])

        def blocked(detail, exit_old=False):
            out["detail"] = detail
            s.challenger, s.count, s.gap, s.last_day = None, 0, True, t
            if exit_old:
                out["exit_ric"] = old
                s.current = None
            return out

        known = p["calendar_known_at_ns"]
        if known is None or integer(known, "calendar_known_at_ns") >= cutoff or not p["calendar_source"]:
            return blocked("calendar_not_known")
        metadata = self.contract_snapshot(p["product"], cutoff, effective)

        def has_full_buffer(m):
            return (m["risk_deadline_ns"] is not None and buffer_session is not None
                    and m.get("risk_date") is not None and buffer_session["day"] < m["risk_date"]
                    and buffer_session["segments"][-1][1] < m["risk_deadline_ns"])

        def before_roll(m):
            if m["risk_deadline_ns"] is None:
                return False
            return effective < m["roll_by_ns"] if m["roll_by_ns"] is not None else has_full_buffer(m)

        if old and (old not in metadata or metadata[old]["risk_deadline_ns"] is None
                    or (metadata[old]["roll_by_ns"] is None and not has_full_buffer(metadata[old]))):
            return blocked("current_deadline_unknown_or_invalid", exit_old=True)
        forced = bool(old and metadata[old]["roll_by_ns"] is not None and effective >= metadata[old]["roll_by_ns"])
        if old:
            out.update(current_metadata_known_at_ns=metadata[old]["known_at_ns"],
                       current_roll_by_ns=metadata[old]["roll_by_ns"],
                       current_risk_deadline_ns=metadata[old]["risk_deadline_ns"])
        floor = s.floor
        candidates = {ric: m for ric, m in metadata.items()
                      if m["listed_ns"] < cutoff and before_roll(m) and (floor is None or m["delivery_month"] >= floor)
                      and (not old or ric == old or m["delivery_month"] > metadata[old]["delivery_month"])}
        if ((old and metadata[old]["roll_by_ns"] is None)
                or any(m["roll_by_ns"] is None for m in candidates.values())):
            out["deadline_buffer_session"] = buffer_session
        batches = [b for b in self.volumes.get((p["product"], d), ())
                   if b["available_at_ns"] is not None and b["available_at_ns"] < cutoff]
        batch = max(batches, key=lambda b: b["available_at_ns"], default=None)
        quantities = {r["ric"]: r["volume"] for r in batch["rows"]} if batch else {}
        bounds = {r["ric"]: quantity_range(r) for r in batch["rows"]} if batch else {}
        bounded = any(pair is not None and pair[0] != pair[1] for pair in bounds.values())
        if batch:
            out.update(volume_available_at_ns=batch["available_at_ns"], volume_source=batch["source"],
                       volume_complete=batch["complete"], current_volume=quantities.get(old))
        if bounded:
            out.update(volume_comparison_basis="bounded_source_quantities",
                       current_volume_lower=bounds[old][0] if bounds.get(old) else None,
                       current_volume_upper=bounds[old][1] if bounds.get(old) else None,
                       selected_volume_lower=None, selected_volume_upper=None)
        out["candidate_comparisons"] = [dict(ric=ric, delivery_month=m["delivery_month"],
                                             volume=quantities.get(ric), known_at_ns=m["known_at_ns"],
                                             roll_by_ns=m["roll_by_ns"], source=m["source"])
                                        for ric, m in sorted(candidates.items())]
        for c in out["candidate_comparisons"]:
            if bounded:
                pair = bounds.get(c["ric"])
                c.update(volume_lower=pair[0] if pair else None, volume_upper=pair[1] if pair else None)
            if c["roll_by_ns"] is None:
                c["deadline_basis"] = "known_future_session"
                c["risk_date"] = candidates[c["ric"]]["risk_date"]
        required = set(candidates) | ({old} if old and not forced else set())
        volume_ok = bool(batch and batch["complete"] and batch["source"] == "tas_printable"
                         and all(bounds.get(ric) is not None for ric in required))
        selected, reason = old, "hold"
        if not volume_ok:
            if not forced:
                return blocked("volume_missing_incomplete_or_unavailable")
            selected, reason = None, "exit_only"
            out["detail"] = "deadline_without_complete_volume"
        else:
            eligible = [ric for ric in candidates if bounds[ric][1] > 0 and (not forced or ric != old)]
            priority = lambda ric: (candidates[ric]["delivery_month"], ric)
            winner = min(eligible, key=lambda ric: (-bounds[ric][0], *priority(ric)), default=None)
            ambiguous = bool(winner and (bounds[winner][0] == 0 or any(
                (-bounds[winner][0], *priority(winner)) > (-bounds[ric][1], *priority(ric))
                for ric in eligible if ric != winner)))
            # A normal roll additionally requires strict outperformance of the
            # current leg, even if metadata excluded it from the candidate set.
            if winner and old and not forced and winner != old and not ambiguous:
                ambiguous = bounds[winner][0] <= bounds[old][1] and bounds[winner][1] > bounds[old][0]
            if ambiguous:
                if not forced:
                    return blocked("ambiguous_volume_ranking")
                selected, reason = None, "exit_only"
                out["detail"] = "deadline_ambiguous_volume_ranking"
            elif forced:
                selected, reason = winner, "deadline" if winner else "exit_only"
                out["detail"] = "" if winner else "deadline_without_eligible_successor"
            elif old is None:
                if winner is None:
                    return blocked("no_eligible_contract")
                selected, reason = winner, "seed"
            elif winner and winner != old and bounds[winner][0] > bounds[old][1]:
                s.count = s.count + 1 if s.challenger == winner else 1
                s.challenger = winner
                if s.count >= self.confirm:
                    selected, reason = winner, "volume"
            else:
                s.challenger, s.count = None, 0
        out.update(selected_ric=selected, reason=reason, challenger=s.challenger, confirmation_count=s.count)
        if selected is not None:
            out.update(selected_metadata_known_at_ns=metadata[selected]["known_at_ns"],
                       selected_roll_by_ns=metadata[selected]["roll_by_ns"],
                       selected_risk_deadline_ns=metadata[selected]["risk_deadline_ns"],
                       selected_volume=quantities.get(selected))
            if bounded:
                pair = bounds.get(selected)
                out.update(selected_volume_lower=pair[0] if pair else None,
                           selected_volume_upper=pair[1] if pair else None)
        if selected != old:
            out.update(exit_ric=old, entry_ric=selected)
        if selected is None:
            s.current, s.challenger, s.count, s.gap, s.last_day = None, None, 0, True, t
            return out
        if s.gap or old is None:
            s.segment += 1
            s.offset = 0
            out["adjustment_status"] = "anchor"
        elif selected != old:
            bridge = self.bridge(p, old, selected)
            if bridge is None:
                s.segment += 1
                s.offset = 0
                out["adjustment_status"] = "missing_bridge_new_segment"
            else:
                s.offset = integer(s.offset + bridge["old_price_e9"] - bridge["new_price_e9"], "offset_e9", I64_MIN)
                out.update(bridge)
                out["adjustment_status"] = "settlement_bridge"
        else:
            out["adjustment_status"] = "carry"
        s.current, s.floor, s.gap, s.last_day = selected, metadata[selected]["delivery_month"], False, t
        out.update(segment_id=s.segment, offset_e9=s.offset)
        if selected != old:
            s.challenger, s.count = None, 0
        return out


def replay(data):
    engine = Replay(data)
    decisions = data["decisions"]
    unique(decisions, lambda p: (p["product"], p["trading_day"]), "decision")
    if decisions != sorted(decisions, key=lambda p: (p["decision_ns"], p["product"])):
        raise ValueError("decisions must be ordered by decision_ns, product")
    return [engine.step(p) for p in decisions]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    raw = args.input.read_bytes()
    result = replay(json.loads(raw))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as out:
        json.dump(dict(input_sha256=hashlib.sha256(raw).hexdigest(), decisions=result), out, indent=2, allow_nan=False)
        out.write("\n")
    print(json.dumps(dict(decisions=len(result), blocked=sum(r["reason"] == "blocked" for r in result))))


if __name__ == "__main__":
    main()
