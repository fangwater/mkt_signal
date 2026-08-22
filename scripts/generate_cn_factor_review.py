#!/usr/bin/env python3
"""Generate the CN factor semantic review and its test-only dependency manifest."""

from __future__ import annotations

import argparse
import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = ROOT / "final_factor_pool_update20260123.py"
DOC_PATH = ROOT / "docs/cn_features_factor_review.md"
MANIFEST_PATH = ROOT / "src/factor_pub/cn_features/review_manifest.rs"
RUST_FACTOR_ENUM_PATH = ROOT / "src/factor_pub/cn_features/factor_enum.rs"
RUST_BASELINES_PATH = ROOT / "src/factor_pub/cn_features/baselines.rs"
RUST_OPV_PATH = ROOT / "src/factor_pub/cn_features/opv_factors.rs"
RUST_PLAIN_PATH = ROOT / "src/factor_pub/cn_features/plain_factors.rs"
RUST_APP_PATH = ROOT / "src/factor_pub/cn_features/app.rs"

TARGET_NAME = re.compile(
    r"^(?:factor_\d{3}|factor_trades_\d{3}|baseline_\d{3}|"
    r"(?:TD_TI|TD_MT|TP_VPI|TD_VI|TD_PT|TD_CI|TD_SI|TD_PR)_\d{3})$"
)
BOOK_FIELD = re.compile(r"^(bid|ask)(\d+)(p|v)$")

TRADE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "avg_amount",
    "count",
    "trade_time",
    "buy_count",
    "sell_count",
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "vwap",
    "buy_vwap",
    "sell_vwap",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
)
TRADE_FIELD_ORDER = {name: index for index, name in enumerate(TRADE_FIELDS)}

DERIVED_AGGREGATE_FIELDS = {
    "active_buy_ratio_5m",
    "large_pct_30m",
    "large_pct_120m",
    "small_pct_30m",
    "small_pct_120m",
    "net_buy_small_pct_15m",
    "active_buy_ratio_240m",
}
KNOWN_DATA_FIELDS = set(TRADE_FIELDS) | DERIVED_AGGREGATE_FIELDS

DIRECTION_FIELDS = {
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "buy_vwap",
    "sell_vwap",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
}
COUNT_FIELDS = {"avg_amount", "count", "buy_count", "sell_count"}
ORDER_SIZE_FIELDS = {
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
}
OHLC_APPROX_FIELDS = {"open", "high", "low"}
VWAP_FIELDS = {"vwap", "buy_vwap", "sell_vwap"}

RECONSTRUCTED_AGGREGATES = {
    "baseline_048": (
        "active_buy_ratio_5m",
        "5-minute buy_amount share reconstructed from 60 five-second rows before the source 30-row mean",
    ),
    "baseline_075": (
        "large_pct_30m",
        "30-minute large_order share reconstructed from 360 five-second rows before the source 300-row ratio",
    ),
    "baseline_078": (
        "large_pct_120m",
        "120-minute large_order share reconstructed from 1,440 five-second rows before the source 300-row ratio",
    ),
    "baseline_094": (
        "small_pct_30m",
        "30-minute small_order share reconstructed from 360 five-second rows before the source 300-row ratio",
    ),
    "baseline_095": (
        "small_pct_120m",
        "120-minute small_order share reconstructed from 1,440 five-second rows before the source 300-row ratio",
    ),
    "baseline_102": (
        "net_buy_small_pct_15m",
        "15-minute net_buy_small / small_order reconstructed from 180 five-second rows before the source 120-row mean",
    ),
    "baseline_155": (
        "active_buy_ratio_240m",
        "240-minute buy_amount share reconstructed from 2,880 five-second rows before the source 150-row mean",
    ),
}

FORMULA_REPAIRS = {
    "factor_054": (
        "dimension repair: the mid term is (bid_price_1 + ask_price_1) / 2 instead of "
        "mixing bid price with bid amount"
    ),
    "factor_166": (
        "dimension repair: native five-level amount imbalance replaces subtraction of an "
        "amount from a price"
    ),
    "baseline_157": "source-comment repair: close-price efficiency replaces volume efficiency",
    "baseline_159": "stationarity repair: sine is applied to one-row close log return",
    "baseline_160": "stationarity repair: cosine is applied to one-row close log return",
}

RESTORED_IMPLEMENTATIONS = {
    "factor_049": "restored source five-level bid-price mean before the 30-row sample std",
    "factor_050": "restored source five-level ask-price mean before the 30-row sample std",
    "factor_051": "restored source five-level ask-price mean",
    "factor_052": (
        "restored source five-level ask-price mean; its 300-difference window "
        "rejects missing observations instead of masking NaN comparisons as zero"
    ),
    "factor_093": "restored source five-level ask-price mean",
    "factor_094": "restored source five-level ask-price mean",
    "factor_118": "restored source second-level mid and native five-level bid VWAP",
    "factor_119": "restored source second-level mid and native five-level ask VWAP",
    "TD_TI_033": "restored rolling(300, min_periods=100) correlation semantics",
}

EXPECTED_DEPTH_COUNTS = Counter({0: 455, 1: 23, 5: 32, 10: 54, 15: 17, 20: 51})
EXPECTED_RUST_ROUTE_COUNTS = {
    "baselines": 200,
    "opv": 205,
    "plain": 59,
    "direct": 168,
}


@dataclass(frozen=True)
class DirectDependencies:
    book_fields: frozenset[str]
    data_fields: frozenset[str]
    calls: frozenset[str]


@dataclass(frozen=True)
class Dependencies:
    book_fields: frozenset[str]
    data_fields: frozenset[str]

    @property
    def depth(self) -> int:
        return max((int(BOOK_FIELD.fullmatch(name).group(2)) + 1 for name in self.book_fields), default=0)


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _int_constant(node: ast.AST) -> int | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    return None


class DependencyVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.book_fields: set[str] = set()
        self.data_fields: set[str] = set()
        self.calls: set[str] = set()
        self._index_scopes: list[dict[str, tuple[int, ...]]] = [{}]

    @property
    def _indices(self) -> dict[str, tuple[int, ...]]:
        return self._index_scopes[-1]

    def _record_field(self, name: str) -> None:
        if BOOK_FIELD.fullmatch(name):
            self.book_fields.add(name)
        elif name in KNOWN_DATA_FIELDS:
            self.data_fields.add(name)

    def _iter_values(self, node: ast.AST) -> tuple[int, ...] | None:
        if isinstance(node, ast.Name):
            return self._indices.get(node.id)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            values = [_int_constant(item) for item in node.elts]
            if all(value is not None for value in values):
                return tuple(value for value in values if value is not None)
            return None
        if not isinstance(node, ast.Call):
            return None

        name = _call_name(node.func)
        if name in {"range", "arange"}:
            args = [_int_constant(arg) for arg in node.args]
            if not args or any(arg is None for arg in args):
                return None
            return tuple(range(*(arg for arg in args if arg is not None)))
        if name == "choice" and node.args:
            stop = _int_constant(node.args[0])
            if stop is not None:
                return tuple(range(stop))
        if name in {"enumerate", "iter"} and node.args:
            return self._iter_values(node.args[0])
        return None

    def _bind_target(self, target: ast.AST, values: tuple[int, ...] | None) -> None:
        if values is None:
            return
        if isinstance(target, ast.Name):
            self._indices[target.id] = values
        elif isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                if isinstance(element, ast.Name):
                    self._indices[element.id] = values

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str):
            self._record_field(node.value)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self._record_field(node.attr)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name):
            self.calls.add(node.func.id)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        values = self._iter_values(node.value)
        for target in node.targets:
            self._bind_target(target, values)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
            self._bind_target(node.target, self._iter_values(node.value))

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        chunks: list[str | ast.FormattedValue] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                chunks.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                chunks.append(value)
            else:
                return

        formatted = [chunk for chunk in chunks if isinstance(chunk, ast.FormattedValue)]
        if len(formatted) != 1 or not isinstance(formatted[0].value, ast.Name):
            return
        index_values = self._indices.get(formatted[0].value.id)
        if index_values is None:
            return
        for index in index_values:
            value = "".join(str(index) if isinstance(chunk, ast.FormattedValue) else chunk for chunk in chunks)
            self._record_field(value)

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        scope = dict(self._indices)
        self._index_scopes.append(scope)
        self._bind_target(node.target, self._iter_values(node.iter))
        for statement in node.body:
            self.visit(statement)
        self._index_scopes.pop()
        for statement in node.orelse:
            self.visit(statement)

    def _visit_comprehension(
        self,
        generators: list[ast.comprehension],
        results: list[ast.AST],
    ) -> None:
        self._index_scopes.append(dict(self._indices))
        for generator in generators:
            self.visit(generator.iter)
            self._bind_target(generator.target, self._iter_values(generator.iter))
            for condition in generator.ifs:
                self.visit(condition)
        for result in results:
            self.visit(result)
        self._index_scopes.pop()

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self._visit_comprehension(node.generators, [node.elt])

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self._visit_comprehension(node.generators, [node.key, node.value])


def analyze(source: str) -> tuple[list[str], dict[str, Dependencies]]:
    tree = ast.parse(source)
    definitions = {
        node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    target_names = [name for name in definitions if TARGET_NAME.fullmatch(name)]
    direct: dict[str, DirectDependencies] = {}
    for name, node in definitions.items():
        visitor = DependencyVisitor()
        visitor.visit(node)
        direct[name] = DirectDependencies(
            frozenset(visitor.book_fields),
            frozenset(visitor.data_fields),
            frozenset(visitor.calls),
        )

    cache: dict[str, Dependencies] = {}

    def resolve(name: str, stack: frozenset[str] = frozenset()) -> Dependencies:
        if name in cache:
            return cache[name]
        item = direct[name]
        book_fields = set(item.book_fields)
        data_fields = set(item.data_fields)
        next_stack = stack | {name}
        for called in item.calls:
            if called not in direct or called in next_stack:
                continue
            child = resolve(called, next_stack)
            book_fields.update(child.book_fields)
            data_fields.update(child.data_fields)
        result = Dependencies(frozenset(book_fields), frozenset(data_fields))
        cache[name] = result
        return result

    dependencies = {name: resolve(name) for name in target_names}
    depths = Counter(item.depth for item in dependencies.values())
    if len(target_names) != 632:
        raise RuntimeError(f"expected 632 factor functions, found {len(target_names)}")
    if depths != EXPECTED_DEPTH_COUNTS:
        raise RuntimeError(f"unexpected legacy depth distribution: {dict(sorted(depths.items()))}")
    return target_names, dependencies


def _assert_unique(label: str, values: list[str]) -> None:
    duplicates = sorted(name for name, count in Counter(values).items() if count != 1)
    if duplicates:
        raise RuntimeError(f"duplicate {label} entries: {duplicates}")


def audit_rust_routes(source_names: list[str]) -> None:
    """Require every source factor to have exactly one Rust dispatcher owner."""
    factor_enum = RUST_FACTOR_ENUM_PATH.read_text()
    variant_pairs = re.findall(
        r'Self::([A-Za-z0-9]+)\s*=>\s*"([^"]+)"', factor_enum
    )
    variants = [variant for variant, _ in variant_pairs]
    rust_names = [name for _, name in variant_pairs]
    _assert_unique("CnFactorId variant", variants)
    _assert_unique("CnFactorId source name", rust_names)
    variant_to_name = dict(variant_pairs)

    baselines_source = RUST_BASELINES_PATH.read_text()
    supported_match = re.search(
        r"pub const SUPPORTED_BASELINES:.*?=\s*&\[(.*?)\];",
        baselines_source,
        re.DOTALL,
    )
    if supported_match is None:
        raise RuntimeError("could not locate SUPPORTED_BASELINES")
    supported_baselines = re.findall(
        r'"(baseline_\d{3})"', supported_match.group(1)
    )
    baseline_routes = re.findall(
        r'^\s*"(baseline_\d{3})"\s*=>\s*compute_baseline_',
        baselines_source,
        re.MULTILINE,
    )
    if supported_baselines != baseline_routes:
        raise RuntimeError(
            "SUPPORTED_BASELINES and compute_baseline dispatch differ: "
            f"supported_only={sorted(set(supported_baselines) - set(baseline_routes))}, "
            f"dispatch_only={sorted(set(baseline_routes) - set(supported_baselines))}"
        )

    route_variants = {
        "opv": re.findall(
            r"^\s*([A-Za-z][A-Za-z0-9]+)\s*=>\s*compute_[a-z0-9_]+,\s*$",
            RUST_OPV_PATH.read_text(),
            re.MULTILINE,
        ),
        "plain": re.findall(
            r"^\s*([A-Za-z][A-Za-z0-9]+)\s*=>\s*compute_[a-z0-9_]+\([a-z]+\),\s*$",
            RUST_PLAIN_PATH.read_text(),
            re.MULTILINE,
        ),
        "direct": re.findall(
            r"^\s*Some\(CnFactorId::([A-Za-z0-9]+)\)\s*=>\s*\{",
            RUST_APP_PATH.read_text(),
            re.MULTILINE,
        ),
    }
    routes: dict[str, list[str]] = {"baselines": baseline_routes}
    for label, route in route_variants.items():
        _assert_unique(f"{label} dispatcher", route)
        unknown = sorted(set(route) - set(variant_to_name))
        if unknown:
            raise RuntimeError(f"unknown CnFactorId variants in {label} dispatcher: {unknown}")
        routes[label] = [variant_to_name[variant] for variant in route]

    for label, expected_count in EXPECTED_RUST_ROUTE_COUNTS.items():
        _assert_unique(f"{label} route", routes[label])
        if len(routes[label]) != expected_count:
            raise RuntimeError(
                f"expected {expected_count} {label} routes, found {len(routes[label])}"
            )

    ownership = Counter(name for route in routes.values() for name in route)
    duplicate_owners = sorted(name for name, count in ownership.items() if count != 1)
    source_set = set(source_names)
    rust_set = set(rust_names)
    if rust_set != source_set:
        raise RuntimeError(
            "CnFactorId/source factor mismatch: "
            f"source_only={sorted(source_set - rust_set)}, rust_only={sorted(rust_set - source_set)}"
        )
    missing = sorted(source_set - set(ownership))
    extra = sorted(set(ownership) - source_set)
    if duplicate_owners or missing or extra:
        raise RuntimeError(
            "Rust factor route ownership mismatch: "
            f"duplicate={duplicate_owners}, missing={missing}, extra={extra}"
        )


def _book_dependency_summary(fields: frozenset[str]) -> str:
    if not fields:
        return ""
    groups: dict[tuple[str, str], list[int]] = {}
    for field in fields:
        match = BOOK_FIELD.fullmatch(field)
        assert match is not None
        side, raw_level, kind = match.groups()
        groups.setdefault((side, kind), []).append(int(raw_level) + 1)

    labels = {("bid", "p"): "bid px", ("bid", "v"): "bid qty", ("ask", "p"): "ask px", ("ask", "v"): "ask qty"}
    parts = []
    for key in (("bid", "p"), ("bid", "v"), ("ask", "p"), ("ask", "v")):
        levels = sorted(set(groups.get(key, [])))
        if not levels:
            continue
        if levels == list(range(levels[0], levels[-1] + 1)):
            level_text = f"L{levels[0]}" if len(levels) == 1 else f"L{levels[0]}-{levels[-1]}"
        else:
            level_text = "L" + ",".join(str(level) for level in levels)
        parts.append(f"{labels[key]} {level_text}")
    return "; ".join(parts)


def _dependency_summary(item: Dependencies) -> str:
    parts = []
    book = _book_dependency_summary(item.book_fields)
    if book:
        parts.append(book)
    fields = sorted(item.data_fields, key=lambda name: (TRADE_FIELD_ORDER.get(name, 999), name))
    if fields:
        parts.append(", ".join(fields))
    return "; ".join(parts) or "none"


def _status_and_reason(name: str, item: Dependencies) -> tuple[str, str]:
    statuses: list[str] = []
    reasons: list[str] = []
    fields = set(item.data_fields)

    if name in RECONSTRUCTED_AGGREGATES:
        field, reconstruction = RECONSTRUCTED_AGGREGATES[name]
        statuses.append("upstream reconstruction")
        reasons.append(f"`{field}` is not stored in the 32-field input; CN uses {reconstruction}")

    if item.depth > 5:
        statuses.append("five-level redefinition")
        if name == "factor_160":
            reasons.append(
                "source randomly chose 10 of 20 levels; CN deterministically uses all five native levels"
            )
        else:
            reasons.append(
                f"source used {item.depth} levels; CN formula uses the native five, changing depth coverage"
            )

    if name in FORMULA_REPAIRS:
        statuses.append("formula repair")
        reasons.append(FORMULA_REPAIRS[name])

    if fields & ORDER_SIZE_FIELDS:
        statuses.append("weakened trade semantics")
        reasons.append(
            "large/medium/small fields classify inferred snapshot-interval trades by monthly P50/P90, not real orders"
        )
    if fields & COUNT_FIELDS:
        statuses.append("weakened trade semantics")
        reasons.append(
            "count fields measure inferred volume-increase intervals, not exchange trade counts"
        )
    if fields & DIRECTION_FIELDS:
        statuses.append("weakened trade semantics")
        reasons.append(
            "buy/sell direction is inferred per snapshot interval and unknown direction is split 50/50"
        )
    if fields & OHLC_APPROX_FIELDS:
        statuses.append("weakened bar semantics")
        reasons.append(
            "open/high/low come from snapshot candidate prices; intrainterval trades can be missed"
        )
    if fields & VWAP_FIELDS:
        statuses.append("conditional preservation")
        reasons.append(
            "VWAP is valid only with verified turnover source and volume_multiple metadata; no-trade rows are compatibility-filled"
        )
    if "trade_time" in fields:
        statuses.append("time-scale sensitive")
        reasons.append(
            "CN trade_time is the replay event timestamp in milliseconds; absolute-time ratios are scale and epoch sensitive"
        )

    if name in RESTORED_IMPLEMENTATIONS:
        reasons.append(RESTORED_IMPLEMENTATIONS[name])

    statuses = list(dict.fromkeys(statuses))
    reasons = list(dict.fromkeys(reasons))
    if not statuses:
        statuses.append("structure preserved")
    if not reasons:
        reasons.append("source field selection and operation structure are preserved; required missing values propagate")
    return " + ".join(statuses), "; ".join(reasons)


def render_document(names: list[str], dependencies: dict[str, Dependencies]) -> str:
    statuses = Counter(_status_and_reason(name, dependencies[name])[0] for name in names)
    depth_counts = Counter(dependencies[name].depth for name in names)
    book_count = sum(dependencies[name].depth > 0 for name in names)
    trade_only_count = len(names) - book_count
    deep_count = sum(dependencies[name].depth > 5 for name in names)

    lines = [
        "# CN futures factor-by-factor semantic review",
        "",
        "This file is generated by `scripts/generate_cn_factor_review.py` from the 632 functions in `final_factor_pool_update20260123.py`. Do not edit the generated tables by hand. The review is metadata and test input only; replay never filters, overwrites, or falls back by factor number.",
        "",
        "## Reading the review",
        "",
        f"- {book_count} source factors read order-book fields; {trade_only_count} are trade/bar-only.",
        f"- {deep_count} source factors used more than five levels and therefore have a material five-level redefinition.",
        "- `structure preserved` means field selection and operation structure are preserved. It is not a claim of predictive value.",
        "- `weakened trade semantics` identifies inferred direction, inferred counts, or inferred order-size buckets.",
        "- `upstream reconstruction` restores the source preprocessing formula from five-second CN trade fields; its economic meaning still inherits the documented CN trade approximations.",
        "- `five-level redefinition` and `formula repair` require explicit research approval.",
        "- A missing required input remains NULL. Missing book data does not invalidate source trade/bar-only factors.",
        "",
        "Legacy source-depth distribution:",
        "",
        "| Source depth | Factor count |",
        "| ---: | ---: |",
    ]
    for depth in (0, 1, 5, 10, 15, 20):
        label = "none" if depth == 0 else str(depth)
        lines.append(f"| {label} | {depth_counts[depth]} |")
    lines.extend(["", "Primary/composite status counts:", "", "| Status | Count |", "| --- | ---: |"])
    for status, count in sorted(statuses.items()):
        lines.append(f"| {status} | {count} |")

    family_order = (
        ("factor_", "Order-book and mixed factors"),
        ("factor_trades_", "Trade factors"),
        ("baseline_", "Baseline functions"),
        ("TD_TI_", "TD_TI"),
        ("TD_MT_", "TD_MT"),
        ("TP_VPI_", "TP_VPI"),
        ("TD_VI_", "TD_VI"),
        ("TD_PT_", "TD_PT"),
        ("TD_CI_", "TD_CI"),
        ("TD_SI_", "TD_SI"),
        ("TD_PR_", "TD_PR"),
    )
    assigned: set[str] = set()
    for prefix, title in family_order:
        if prefix == "factor_":
            family_names = [name for name in names if name.startswith(prefix) and not name.startswith("factor_trades_")]
        else:
            family_names = [name for name in names if name.startswith(prefix)]
        if not family_names:
            continue
        assigned.update(family_names)
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| Legacy factor | CN output | Source inputs | Source depth | Status | Why |",
                "| --- | --- | --- | ---: | --- | --- |",
            ]
        )
        for name in family_names:
            item = dependencies[name]
            status, reason = _status_and_reason(name, item)
            cn_name = f"cn_features_{name.lower()}"
            depth = "-" if item.depth == 0 else str(item.depth)
            lines.append(
                f"| `{name}` | `{cn_name}` | {_dependency_summary(item)} | {depth} | {status} | {reason} |"
            )
    if assigned != set(names):
        raise RuntimeError(f"unassigned factor families: {sorted(set(names) - assigned)}")
    lines.append("")
    return "\n".join(lines)


def render_manifest(names: list[str], dependencies: dict[str, Dependencies]) -> str:
    trade_only = [name for name in names if dependencies[name].depth == 0]
    book = [name for name in names if dependencies[name].depth > 0]
    lines = [
        "// @generated by scripts/generate_cn_factor_review.py; do not edit.",
        "// Test-only metadata. Runtime factor dispatch must not depend on this list.",
        "",
        "pub(super) const LEGACY_TRADE_ONLY_FACTORS: &[&str] = &[",
    ]
    lines.extend(f'    "{name}",' for name in trade_only)
    lines.extend(["];"])
    lines.extend(["", "pub(super) const LEGACY_BOOK_FACTORS: &[&str] = &["])
    lines.extend(f'    "{name}",' for name in book)
    lines.extend(["];"])
    lines.append("")
    return "\n".join(lines)


def update(path: Path, content: str, check: bool) -> bool:
    current = path.read_text() if path.exists() else None
    if current == content:
        return False
    if check:
        print(f"out of date: {path.relative_to(ROOT)}", file=sys.stderr)
        return True
    path.write_text(content)
    print(f"wrote {path.relative_to(ROOT)}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if generated files differ")
    args = parser.parse_args()

    names, dependencies = analyze(SOURCE_PATH.read_text())
    audit_rust_routes(names)
    changed = False
    changed |= update(DOC_PATH, render_document(names, dependencies), args.check)
    changed |= update(MANIFEST_PATH, render_manifest(names, dependencies), args.check)
    return int(args.check and changed)


if __name__ == "__main__":
    raise SystemExit(main())
