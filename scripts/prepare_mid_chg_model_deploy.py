#!/usr/bin/env python3
"""
Prepare rolling XGBoost mid-change model artifacts for live model_pub deployment.

Input layout expected from the training notebook:
  <source>/<exchange>/<SYMBOL>_<return_name>_{model.pkl,info.pkl,ic.csv,factors.txt}

The live stack expects one model_manager root per horizon, for example:
  /home/ubuntu/model_data/models_202601_202606/binance-futures-mid-chg-30s/
  /home/ubuntu/model_data/models_202601_202606/binance-futures-mid-chg-1m/
  /home/ubuntu/model_data/models_202601_202606/binance-futures-mid-chg-5m/

This script:
  1. splits mixed training artifacts by horizon,
  2. optionally renames mid_re_* groups to mid_chg_* groups,
  3. normalizes info.pkl metadata in the copied target,
  4. validates factor names against the local fusion_factor_pub mappings,
  5. optionally registers/refeshes model_manager models,
  6. optionally syncs tlen factor_plan to the union of registered model factors.
"""

from __future__ import annotations

import argparse
import json
import pickle
import re
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ARTIFACT_SUFFIXES = (
    "_model.pkl",
    "_model.json",
    "_model.onnx",
    "_factors.txt",
    "_info.pkl",
    "_ic.csv",
)

DEFAULT_RETURN_MAP = {
    "mid_re_30s": "mid_chg_30s",
    "mid_re_1m": "mid_chg_1m",
    "mid_re_5m": "mid_chg_5m",
    "mid_chg_30s": "mid_chg_30s",
    "mid_chg_1m": "mid_chg_1m",
    "mid_chg_5m": "mid_chg_5m",
}


@dataclass(frozen=True)
class Artifact:
    source_path: Path
    symbol: str
    source_return: str
    target_return: str
    suffix: str

    @property
    def target_group_key(self) -> str:
        return f"{self.symbol}_{self.target_return}"

    @property
    def target_name(self) -> str:
        return f"{self.target_group_key}{self.suffix}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split/register/sync mid-change XGBoost model artifacts for live deployment"
    )
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Training output root, or the exchange subdir containing *_model.pkl files",
    )
    parser.add_argument(
        "--exchange",
        default="binance-futures",
        help="Venue/exchange slug, e.g. binance-futures or okex-swap",
    )
    parser.add_argument(
        "--target-root",
        default="/home/ubuntu/model_data/models_202601_202606",
        type=Path,
        help="Target root that will contain one subdir per live model_name",
    )
    parser.add_argument(
        "--return-map",
        action="append",
        default=[],
        metavar="SRC=DST",
        help="Return-name rename mapping. Can be repeated. Defaults map mid_re_* to mid_chg_*.",
    )
    parser.add_argument(
        "--model-manager-url",
        default="http://127.0.0.1:6300",
        help="model_manager base URL",
    )
    parser.add_argument(
        "--tlen-url",
        default="http://127.0.0.1:6322",
        help="tlen_config_server base URL",
    )
    parser.add_argument(
        "--mkt-signal-root",
        default="/home/ubuntu/mkt_signal",
        type=Path,
        help="mkt_signal repo root, used for factor-name validation",
    )
    parser.add_argument(
        "--register",
        action="store_true",
        help="POST split target dirs to model_manager after copying",
    )
    parser.add_argument(
        "--sync-tlen-factor-plan",
        action="store_true",
        help="Replace tlen factor_plan with the union of factors from registered models",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing target files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print planned changes; do not copy/register/sync",
    )
    parser.add_argument(
        "--skip-factor-name-check",
        action="store_true",
        help="Do not validate factors against local fusion_factor_pub mappings",
    )
    return parser.parse_args()


def build_return_map(raw_items: list[str]) -> dict[str, str]:
    mapping = dict(DEFAULT_RETURN_MAP)
    for raw in raw_items:
        if "=" not in raw:
            raise SystemExit(f"--return-map must be SRC=DST, got: {raw}")
        src, dst = raw.split("=", 1)
        src = src.strip()
        dst = dst.strip()
        if not src or not dst:
            raise SystemExit(f"--return-map must be SRC=DST, got: {raw}")
        mapping[src] = dst
    return mapping


def resolve_source_dir(source: Path, exchange: str) -> Path:
    source = source.expanduser().resolve()
    exchange_child = source / exchange
    if exchange_child.is_dir():
        return exchange_child
    return source


def strip_artifact_suffix(path: Path) -> tuple[str, str] | None:
    name = path.name
    for suffix in ARTIFACT_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)], suffix
    return None


def collect_artifacts(source_dir: Path, return_map: dict[str, str]) -> list[Artifact]:
    artifacts: list[Artifact] = []
    skipped: list[str] = []
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        parsed = strip_artifact_suffix(path)
        if parsed is None:
            continue
        group_key, suffix = parsed
        if "_" not in group_key:
            skipped.append(path.name)
            continue
        symbol, source_return = group_key.split("_", 1)
        symbol = symbol.strip().upper()
        source_return = source_return.strip()
        target_return = return_map.get(source_return)
        if not target_return:
            skipped.append(path.name)
            continue
        artifacts.append(
            Artifact(
                source_path=path,
                symbol=symbol,
                source_return=source_return,
                target_return=target_return,
                suffix=suffix,
            )
        )
    if skipped:
        print(f"[WARN] skipped {len(skipped)} files not matching return map or group format")
        for name in skipped[:20]:
            print(f"  skipped: {name}")
        if len(skipped) > 20:
            print(f"  ... {len(skipped) - 20} more")
    return artifacts


def horizon_from_return(return_name: str) -> str:
    prefix = "mid_chg_"
    if not return_name.startswith(prefix):
        raise ValueError(f"target return_name must start with {prefix}: {return_name}")
    return return_name[len(prefix) :]


def model_name_for(exchange: str, target_return: str) -> str:
    return f"{exchange}-mid-chg-{horizon_from_return(target_return)}"


def read_factor_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def normalize_info_pickle(source: Path, target: Path, artifact: Artifact, factors: list[str]) -> None:
    try:
        with source.open("rb") as fh:
            payload = pickle.load(fh)
    except Exception:
        shutil.copy2(source, target)
        return

    if not isinstance(payload, dict):
        shutil.copy2(source, target)
        return

    payload = dict(payload)
    payload["symbol"] = artifact.symbol
    payload["return_name"] = artifact.target_return
    payload.setdefault("selected_factors", list(factors))
    if "train_window" not in payload and "train_start" in payload and "train_end" in payload:
        payload["train_window"] = (payload.get("train_start"), payload.get("train_end"))
    payload.setdefault("final_factors_count", len(factors))

    with target.open("wb") as fh:
        pickle.dump(payload, fh)


def copy_artifacts(
    artifacts: list[Artifact],
    target_root: Path,
    exchange: str,
    force: bool,
    dry_run: bool,
) -> dict[str, Path]:
    target_root = target_root.expanduser().resolve()
    by_group: dict[str, list[Artifact]] = defaultdict(list)
    for artifact in artifacts:
        by_group[artifact.target_group_key].append(artifact)

    model_dirs: dict[str, Path] = {}
    copied = 0

    for artifact in artifacts:
        model_name = model_name_for(exchange, artifact.target_return)
        model_dir = target_root / model_name
        model_dirs[model_name] = model_dir
        target_path = model_dir / artifact.target_name
        if target_path.exists() and not force:
            raise SystemExit(
                f"target exists, rerun with --force or choose a new --target-root: {target_path}"
            )
        if dry_run:
            print(f"[DRY] copy {artifact.source_path} -> {target_path}")
            continue
        model_dir.mkdir(parents=True, exist_ok=True)
        if artifact.suffix == "_info.pkl":
            factor_source = artifact.source_path.with_name(
                f"{artifact.source_path.name[: -len('_info.pkl')]}_factors.txt"
            )
            factors = read_factor_file(factor_source)
            normalize_info_pickle(artifact.source_path, target_path, artifact, factors)
        else:
            shutil.copy2(artifact.source_path, target_path)
        copied += 1

    print(f"[OK] planned/copied artifacts={len(artifacts) if dry_run else copied}")
    return dict(sorted(model_dirs.items()))


def validate_complete_groups(model_dirs: dict[str, Path], dry_run: bool) -> None:
    if dry_run:
        return
    missing: list[str] = []
    for model_name, model_dir in sorted(model_dirs.items()):
        groups: dict[str, set[str]] = defaultdict(set)
        for path in model_dir.iterdir():
            parsed = strip_artifact_suffix(path)
            if parsed is None:
                continue
            group_key, suffix = parsed
            groups[group_key].add(suffix)
        for group_key, suffixes in sorted(groups.items()):
            for required in ("_model.pkl", "_factors.txt"):
                if required not in suffixes:
                    missing.append(f"{model_name}/{group_key}{required}")
    if missing:
        raise SystemExit("missing required artifacts:\n" + "\n".join(f"  {x}" for x in missing))


def http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {url}; {detail}") from exc
    if not body:
        return {}
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise RuntimeError(f"unexpected JSON response from {url}: {type(obj).__name__}")
    return obj


def register_models(model_manager_url: str, model_dirs: dict[str, Path], dry_run: bool) -> None:
    base = model_manager_url.rstrip("/")
    for model_name, root_path in sorted(model_dirs.items()):
        payload = {"model_name": model_name, "root_path": str(root_path)}
        if dry_run:
            print(f"[DRY] POST {base}/api/models {payload}")
            continue
        resp = http_json("POST", f"{base}/api/models", payload, timeout=120.0)
        warnings = resp.get("warnings") or []
        print(
            f"[OK] registered {model_name}: symbols={resp.get('symbol_count')} "
            f"groups={resp.get('group_count')} warnings={len(warnings)}"
        )
        for warning in warnings[:20]:
            print(f"  warning: {warning}")
        if warnings:
            raise SystemExit(f"model_manager returned warnings for {model_name}")


def fetch_model_symbol_details(model_manager_url: str, model_name: str) -> list[dict[str, Any]]:
    base = model_manager_url.rstrip("/")
    model_q = urllib.parse.quote(model_name, safe="")
    symbols_payload = http_json("GET", f"{base}/api/models/{model_q}/symbols", timeout=30.0)
    details: list[dict[str, Any]] = []
    for item in symbols_payload.get("items", []):
        symbol = str(item.get("symbol") or "").strip()
        group_key = str(item.get("group_key") or "").strip()
        if not symbol:
            continue
        query = ""
        if group_key:
            query = "?" + urllib.parse.urlencode({"group_key": group_key})
        detail = http_json(
            "GET",
            f"{base}/api/models/{model_q}/symbols/{urllib.parse.quote(symbol, safe='')}{query}",
            timeout=30.0,
        )
        details.append(detail)
    return details


def build_factor_union_from_model_manager(
    model_manager_url: str,
    model_names: list[str],
) -> dict[str, list[str]]:
    union: dict[str, OrderedDict[str, None]] = defaultdict(OrderedDict)
    for model_name in model_names:
        details = fetch_model_symbol_details(model_manager_url, model_name)
        if not details:
            raise RuntimeError(f"model has no symbol details: {model_name}")
        for detail in details:
            symbol = str(detail.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            if not detail.get("grpc_ready"):
                raise RuntimeError(f"model not grpc_ready: {model_name}/{symbol}")
            warnings = detail.get("warnings") or []
            if warnings:
                raise RuntimeError(f"model warnings: {model_name}/{symbol}: {warnings}")
            factors = detail.get("factors") or []
            if not isinstance(factors, list) or not factors:
                raise RuntimeError(f"empty factors: {model_name}/{symbol}")
            for raw in factors:
                factor = str(raw).strip()
                if factor:
                    union[symbol][factor] = None
    return {symbol: list(factors.keys()) for symbol, factors in sorted(union.items())}


def load_supported_factor_names(mkt_signal_root: Path) -> set[str]:
    root = mkt_signal_root.expanduser().resolve()
    factor_enum = root / "src/factor_pub/fusion_factor_pub/factor_enum.rs"
    plan_rs = root / "src/factor_pub/fusion_factor_pub/plan.rs"
    supported: set[str] = set()

    if factor_enum.exists():
        text = factor_enum.read_text(encoding="utf-8")
        supported.update(re.findall(r'"([^"]+)"\s*=>\s*Some\(Self::', text))

    if plan_rs.exists():
        text = plan_rs.read_text(encoding="utf-8")
        supported.update(re.findall(r'"([^"]+)"\s*=>\s*Some\(Self::', text))
        supported.update(re.findall(r'Self::[A-Za-z0-9]+\s*=>\s*"([^"]+)"', text))

    return supported


def validate_supported_factors(factor_plan: dict[str, list[str]], mkt_signal_root: Path) -> None:
    supported = load_supported_factor_names(mkt_signal_root)
    if not supported:
        raise SystemExit(f"could not load supported factor names from {mkt_signal_root}")
    missing: dict[str, list[str]] = {}
    for symbol, factors in factor_plan.items():
        bad = [factor for factor in factors if factor not in supported]
        if bad:
            missing[symbol] = bad
    if missing:
        lines = ["factor names not implemented by fusion_factor_pub:"]
        for symbol, bad in sorted(missing.items()):
            lines.append(f"  {symbol}: {', '.join(bad[:30])}")
            if len(bad) > 30:
                lines.append(f"    ... {len(bad) - 30} more")
        raise SystemExit("\n".join(lines))
    print(f"[OK] factor-name check passed: symbols={len(factor_plan)}")


def sync_tlen_factor_plan(
    tlen_url: str,
    exchange: str,
    factor_plan: dict[str, list[str]],
    dry_run: bool,
) -> None:
    thresholds = {symbol: {"factors": factors} for symbol, factors in factor_plan.items()}
    payload = {"venue": exchange, "config_type": "factor_plan", "thresholds": thresholds}
    url = f"{tlen_url.rstrip('/')}/api/thresholds/replace"
    if dry_run:
        print(f"[DRY] POST {url}")
        print(json.dumps(payload, ensure_ascii=False, indent=2)[:4000])
        return
    resp = http_json("POST", url, payload, timeout=30.0)
    print(f"[OK] synced tlen factor_plan: count={resp.get('count')} venue={exchange}")


def print_summary(model_dirs: dict[str, Path], artifacts: list[Artifact]) -> None:
    by_model: dict[str, set[str]] = defaultdict(set)
    for artifact in artifacts:
        by_model[model_name_for(args.exchange, artifact.target_return)].add(artifact.symbol)
    print("[SUMMARY] target model dirs:")
    for model_name, path in sorted(model_dirs.items()):
        symbols = sorted(by_model.get(model_name, set()))
        print(f"  {model_name}: path={path} symbols={len(symbols)} {symbols}")


def main() -> int:
    global args
    args = parse_args()
    return_map = build_return_map(args.return_map)
    source_dir = resolve_source_dir(args.source, args.exchange)
    if not source_dir.is_dir():
        raise SystemExit(f"source dir not found: {source_dir}")

    artifacts = collect_artifacts(source_dir, return_map)
    if not artifacts:
        raise SystemExit(f"no deployable artifacts found under {source_dir}")

    model_dirs = copy_artifacts(
        artifacts=artifacts,
        target_root=args.target_root,
        exchange=args.exchange,
        force=args.force,
        dry_run=args.dry_run,
    )
    validate_complete_groups(model_dirs, args.dry_run)
    print_summary(model_dirs, artifacts)

    if args.register:
        register_models(args.model_manager_url, model_dirs, args.dry_run)

    if args.sync_tlen_factor_plan:
        if not args.register and not args.dry_run:
            print("[WARN] --sync-tlen-factor-plan uses models already visible in model_manager")
        model_names = sorted(model_dirs)
        factor_plan = build_factor_union_from_model_manager(args.model_manager_url, model_names)
        if not args.skip_factor_name_check:
            validate_supported_factors(factor_plan, args.mkt_signal_root)
        sync_tlen_factor_plan(args.tlen_url, args.exchange, factor_plan, args.dry_run)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        raise SystemExit(130)
