#!/usr/bin/env python3
"""Export causal roll decisions to daily metadata and Chinese per-roll reports.

Production correctness baseline. Input availability is validated by the engine;
the caller still supplies the reviewed historical calendar/metadata evidence.
Blocked decisions never become a publishable daily table.
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from cme_causal_roll import replay

EXCHANGES = {"URO": "CME", "JY": "CME", "HG": "COMEX", "SI": "COMEX",
             **dict.fromkeys(("TU", "FV", "TY", "TN", "US", "AUL"), "CBOT")}
FIELDS = ("product", "trading_day", "previous_day", "contract_id", "ric",
          "decision_ns", "effective_ns", "segment_id", "offset_e9", "reason",
          "adjustment_status")
ROLL_FIELDS = ("product", "effective_trading_day", "decision_ns", "effective_ns",
               "old_ric", "new_ric", "reason", "segment_id", "offset_e9",
               "adjustment_status", "reference_day", "old_price_e9", "new_price_e9",
               "old_published_ns", "new_published_ns")


def daily_rows(decisions):
    """Convert already replayed decisions, rejecting ambiguous or blocked rows."""
    rows = []
    seen = set()
    for d in decisions:
        key = d["product"], d["trading_day"]
        if d["product"] not in EXCHANGES or key in seen:
            raise ValueError(f"unsupported or duplicate decision {key}")
        seen.add(key)
        if d["reason"] == "blocked":
            raise ValueError(f"blocked decision {key}: {d['detail']}")
        ric = d["selected_ric"]
        contract = ""
        if ric:
            matches = [c for c in d["candidate_comparisons"] if c["ric"] == ric]
            if len(matches) != 1:
                raise ValueError(f"selected identity absent/ambiguous {key} {ric}")
            contract = f"{EXCHANGES[d['product']]}:{d['product']}:{matches[0]['delivery_month']}"
        elif d["reason"] != "exit_only":
            raise ValueError(f"missing selection without exit_only: {key}")
        rows.append({**{k: d[k] for k in FIELDS if k not in ("ric", "contract_id")},
                     "ric": ric or "", "contract_id": contract})
    validate_daily_rows(rows)
    return rows


def validate_daily_rows(rows):
    previous = {}
    for r in rows:
        p, day = r["product"], r["trading_day"]
        for key in ("decision_ns", "effective_ns", "segment_id", "offset_e9"):
            if key in ("segment_id", "offset_e9") and r["reason"] == "exit_only" and r[key] is None:
                continue
            if type(r[key]) is not int or not -(2**63) <= r[key] < 2**63:
                raise ValueError(f"invalid integer {key}")
        if p not in EXCHANGES or r["reason"] not in ("seed", "hold", "volume", "deadline", "exit_only"):
            raise ValueError("unsupported product/reason")
        for key in ("trading_day", "previous_day"):
            if datetime.strptime(r[key], "%Y-%m-%d").strftime("%Y-%m-%d") != r[key]:
                raise ValueError("non-canonical date")
        if r["previous_day"] >= day or not 0 <= r["decision_ns"] < r["effective_ns"]:
            raise ValueError("invalid decision timing")
        if r["reason"] == "exit_only":
            if r["ric"] or r["contract_id"] or r["segment_id"] is not None or r["offset_e9"] is not None:
                raise ValueError("exit_only has a selected contract")
        elif not r["ric"] or not r["contract_id"] or r["segment_id"] < 1:
            raise ValueError("missing selected identity/segment")
        prior = previous.get(p)
        if prior:
            if day <= prior["trading_day"] or r["previous_day"] != prior["trading_day"]:
                raise ValueError("daily table has a missing/out-of-order decision")
            if r["decision_ns"] <= prior["decision_ns"] or r["effective_ns"] <= prior["effective_ns"]:
                raise ValueError("decision timestamps are not increasing")
            if r["segment_id"] is not None and prior["segment_id"] is not None and r["segment_id"] < prior["segment_id"]:
                raise ValueError("segment goes backwards")
            if r["reason"] == "hold" and (r["ric"], r["segment_id"], r["offset_e9"]) != (
                    prior["ric"], prior["segment_id"], prior["offset_e9"]):
                raise ValueError("hold changes identity/adjustment")
        previous[p] = r


def load_daily(path):
    with Path(path).open(newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for name in ("decision_ns", "effective_ns", "segment_id", "offset_e9"):
            r[name] = int(r[name]) if r[name] else None
    validate_daily_rows(rows)
    return rows


def time_text(ns):
    if ns is None:
        return "缺失"
    seconds, nanos = divmod(ns, 10**9)
    dt = datetime.fromtimestamp(seconds, timezone.utc).astimezone(ZoneInfo("America/Chicago"))
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{nanos:09d} {dt.tzname()}（{ns} ns）"


def volume_text(exact, lower=None, upper=None):
    if exact is not None:
        return str(exact)
    if lower is not None and upper is not None:
        return f"[{lower}, {upper}]（精确值未知）"
    return "缺失"


def chinese_report(product, decisions, contracts):
    reasons = {"seed": "初始化：取前一完整交易日合格候选中成交量最大者",
               "volume": "成交量：相同远月连续两个完整相邻交易日严格领先",
               "deadline": "期限优先：旧腿到达提前换出时点，不再要求新腿量领先",
               "exit_only": "期限退出：没有可用新腿，仅记录退出意图，不虚构成交"}
    lines = [f"# {product} 因果换月逐次依据", "",
             "本文件由实际决策结果生成。时间采用 America/Chicago，并保留 UTC 纳秒。",
             "成交量、日历和期限证据均须在决策截止前可得；研究选择不等于真实成交。", ""]
    history = []
    count = 0
    for d in decisions:
        if d["reason"] != "hold":
            count += 1
            rule = reasons[d['reason']]
            if d['reason'] == 'volume':
                rule = f"成交量：相同远月连续 {d['confirmation_count']} 个完整相邻交易日严格领先"
            old_sources = [c['source'] for c in contracts if c['product'] == product
                           and c['ric'] == d['previous_ric']
                           and c['known_at_ns'] == d['current_metadata_known_at_ns']]
            old_quantity = volume_text(d['current_volume'], d.get('current_volume_lower'), d.get('current_volume_upper'))
            new_quantity = volume_text(d['selected_volume'], d.get('selected_volume_lower'), d.get('selected_volume_upper'))
            lines += [f"## {count}. {d['trading_day']}：{d['previous_ric'] or '空仓'} → {d['selected_ric'] or '空仓'}", "",
                      f"- 规则依据：{rule}。",
                      f"- 决策截止：{time_text(d['decision_ns'])}；生效：{time_text(d['effective_ns'])}。",
                      f"- 量参考日：{d['previous_day']}；旧腿 {old_quantity} 手，新腿 {new_quantity} 手。",
                      f"- 量批次可得时间：{time_text(d['volume_available_at_ns'])}；来源 {d['volume_source']}；完整={d['volume_complete']}。",
                      f"- 旧腿换出期限：{time_text(d['current_roll_by_ns'])}；交割/到期风险：{time_text(d['current_risk_deadline_ns'])}。",
                      f"- 旧腿期限来源：{'；'.join(old_sources) or '无旧腿'}；可得时间 {time_text(d['current_metadata_known_at_ns'])}。",
                      f"- 新腿元数据可得时间：{time_text(d['selected_metadata_known_at_ns'])}；新腿换出期限：{time_text(d['selected_roll_by_ns'])}。",
                      f"- 日历依据：{d['calendar_source']}；可得时间 {time_text(d['calendar_known_at_ns'])}。"]
            if d.get('volume_comparison_basis') == 'bounded_source_quantities':
                lines += ['- 数量区间：未解决时钟记录按 0 至原始数量保留，上下界不替代精确量；完整表示全部指定候选源扫描完成。',
                          '- 比较依据：在期限及月份筛选后，新腿下界须胜过其他候选上界；相等保留交割月、RIC 顺序。普通成交量换月另须严格高于旧腿上界并连续确认；未使用截止后的记录。']
            if d.get('deadline_buffer_session'):
                session = d['deadline_buffer_session']
                intervals = '；'.join(f"{time_text(lo)} 至 {time_text(hi)}" for lo, hi in session['segments'])
                lines += [f"- 远月期限判断：官方交易日 {session['day']} 的全部计划区间为 {intervals}。",
                          f"- 上述日历来源：{session['source']}；可得时间 {time_text(session['known_at_ns'])}。",
                          '- 精确退出时刻暂缺的候选，仅在该完整交易日位于本次生效之后、已知风险日期和时刻之前时参与比较；风险日自身不能作为缓冲日，未推测或提前填入退出时刻。']
                for candidate in d['candidate_comparisons']:
                    if candidate.get('deadline_basis') == 'known_future_session':
                        lines.append(f"- {candidate['ric']} 的民用风险日期为 {candidate['risk_date']}；上述完整交易日标签严格早于该日期。")
            if d["reason"] == "volume":
                needed = d["confirmation_count"] - 1
                for prior in history[-needed:] if needed else []:
                    older = {c["ric"]: volume_text(c['volume'],c.get('volume_lower'),c.get('volume_upper'))
                             for c in prior["candidate_comparisons"]}
                    lines += [f"- 较早确认日 {prior['previous_day']}：旧腿 {older.get(d['previous_ric'], '缺失')} 手，新腿 {older.get(d['selected_ric'], '缺失')} 手。"]
            lines += [f"- 复权：{d['adjustment_status']}；参考日 {d['reference_day']}，两腿价格（e9）{d['old_price_e9']} / {d['new_price_e9']}。",
                      f"- 两腿结算发布时间：{time_text(d['old_published_ns'])} / {time_text(d['new_published_ns'])}。",
                      f"- 生效后段号 {d['segment_id']}，偏移 {d['offset_e9']} e9；此前价格不回改。", "",
                      "候选比较（按确定量或下界降序、交割月及 RIC 升序展示；区间按上下界判定）：", "",
                      "| RIC | 交割月 | 成交量 | 期限/身份来源 |", "| --- | --- | ---: | --- |"]
            for c in sorted(d["candidate_comparisons"], key=lambda c: (
                    c["volume"] is None and c.get('volume_lower') is None,
                    -(c["volume"] if c["volume"] is not None else c.get('volume_lower') or 0),
                    c["delivery_month"], c["ric"])):
                source = c["source"].replace("|", "\\|").replace("\n", " ")
                quantity = volume_text(c['volume'],c.get('volume_lower'),c.get('volume_upper'))
                lines.append(f"| {c['ric']} | {c['delivery_month']} | {quantity} | {source} |")
            lines.append("")
        history.append(d)
    return "\n".join(lines) + "\n"


def write_csv(path, fields, rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def export_tables(data, destination, input_hash):
    decisions = replay(data)
    rows = daily_rows(decisions)  # Validate all products before creating any output.
    if not rows:
        raise ValueError("empty decision table")
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=destination.name + ".partial.", dir=destination.parent))
    try:
        grouped = defaultdict(list)
        for r in rows:
            grouped[r["product"]].append(r)
        counts = {}
        for p, daily in sorted(grouped.items()):
            directory = staging / EXCHANGES[p] / p
            directory.mkdir(parents=True)
            selected = [d for d in decisions if d["product"] == p]
            write_csv(directory / "dominant.csv", FIELDS, daily)
            changes = []
            for d in selected:
                if d["reason"] == "hold":
                    continue
                changes.append({**{k: d[k] for k in ROLL_FIELDS if k not in (
                    "effective_trading_day", "old_ric", "new_ric")},
                    "effective_trading_day": d["trading_day"],
                    "old_ric": d["previous_ric"], "new_ric": d["selected_ric"]})
            write_csv(directory / "adjustment.csv", ROLL_FIELDS, changes)
            (directory / "换月依据.md").write_text(chinese_report(p, selected, data['contracts']))
            counts[p] = {"days": len(daily), "selected_days": sum(bool(r["ric"]) for r in daily),
                         "changes_including_initialization": len(changes)}
        (staging / "decisions.json").write_text(json.dumps(decisions, ensure_ascii=False, indent=2) + "\n")
        outputs = {str(f.relative_to(staging)): hashlib.sha256(f.read_bytes()).hexdigest()
                   for f in sorted(staging.rglob("*")) if f.is_file()}
        manifest = {"input_sha256": input_hash, "adjustment_direction": "causal_forward",
                    "blocked_days": 0, "products": counts, "output_sha256": outputs}
        manifest['code_sha256'] = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                                  for name in ('cme_causal_roll_tables.py', 'cme_causal_roll.py')}
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        os.rename(staging, destination)
    except BaseException:
        shutil.rmtree(staging)
        raise
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    print(json.dumps(export_tables(json.loads(raw), args.output_root, hashlib.sha256(raw).hexdigest())))


if __name__ == "__main__":
    main()
