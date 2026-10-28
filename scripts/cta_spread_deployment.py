#!/usr/bin/env python3
"""Validate research deployment JSON and prepare the current Spread configuration."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy

from cta_spread_factor_config import normalize_table, source_factor


def prepare_deployment(raw: dict, current_config: dict) -> dict:
    from cta_spread_config_server import normalize_config

    if not isinstance(raw, dict) or not isinstance(raw.get("factors"), list):
        raise ValueError("deployment.factors must be an array")
    rules = raw["rule"]
    if rules["nq"]["lookback"] != 120 or rules["nq"]["quantile_window"] != 120:
        raise ValueError("NQ lookback and quantile_window must both be 120")
    if rules["spread"]["lag"] != "previous minute":
        raise ValueError("spread.lag must be previous minute")
    counts = Counter(row["factor"] for row in raw["factors"])
    table, identities = {}, set()
    total = 0
    for row in raw["factors"]:
        factor, sign, symbols = row["factor"], row["sign"], row["symbols"]
        if isinstance(sign, bool) or sign not in (-1, 1):
            raise ValueError(f"{factor}: sign must be +1 or -1")
        if (factor, sign) in identities:
            raise ValueError(f"duplicate factor/sign: {factor}/{sign}")
        identities.add((factor, sign))
        if len(symbols) != row["n_symbols"] or len(symbols) != len(set(symbols)):
            raise ValueError(f"{factor}: n_symbols mismatch or duplicate symbols")
        strategy = factor + ("_positive" if sign > 0 else "_negative") if counts[factor] > 1 else factor
        for symbol in symbols:
            table.setdefault(symbol, {})[strategy] = sign
        total += len(symbols)
    if raw["n_factors"] != len(identities) or raw["n_sleeves"] != total:
        raise ValueError("deployment factor/sleeve totals do not match its rows")
    table = normalize_table(table)
    config = deepcopy(current_config)
    config["enabled"] = False
    if "symbol_overrides" in raw:
        config["symbol_overrides"] = deepcopy(raw["symbol_overrides"])
    config["entry"] = {
        "factor_long_quantile": rules["entry"]["long"],
        "factor_short_quantile": rules["entry"]["short"],
        "nq_long_quantile": rules["nq"]["long_quantile"],
        "nq_short_quantile": rules["nq"]["short_quantile"],
        "spread_long_quantile": rules["spread"]["long_quantile"],
        "spread_short_quantile": rules["spread"]["short_quantile"],
    }
    config["execution"] = {
        "factor_exit_quantile_long": rules["exit"]["long"],
        "factor_exit_quantile_short": rules["exit"]["short"],
    }
    return {
        "config": normalize_config(config), "symbol_factors": table,
        "universe": {"symbols": sorted(table), "factors": sorted({source_factor(f) for row in table.values() for f in row})},
        "summary": {"symbols": len(table), "strategies": len(identities), "combinations": total},
    }
