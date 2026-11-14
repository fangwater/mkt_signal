"""Read-only evidence collector; account details and credentials remain on host."""
import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

home = Path.home()
root = home / "binance-cta-perp-rx01"
run = root / "run/net_abs_readiness_20261030"
latest, counts, first_ready = {}, {}, {}
for path in sorted((run / "history").glob("*.jsonl")):
    for line in path.open():
        row = json.loads(line)
        symbol = row["symbol"]
        counts[symbol] = counts.get(symbol, 0) + 1
        latest[symbol] = row
        if all(value["ready"] for value in row["inputs"].values()):
            first_ready.setdefault(symbol, counts[symbol])
assert (run / "probe_exit_code").read_text().strip() == "0"
assert set(latest) == {"BTCUSDT", "ETHUSDT"}
assert "no IPC publishers created" in (run / "probe.log").read_text()
for row in latest.values():
    assert len(row["inputs"]) == 23 and all(v["ready"] for v in row["inputs"].values())
    assert row["nq"]["ready"]
account = json.loads((home / "binance_exec_trade06/run/restore_audit_20261030/account_plan.json").read_text())
cash = [r for r in account["rows"] if r["market"] == "margin"]
summary = {
    "verified_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "scope": "BTC/ETH validation only; operator universe configured separately",
    "probe_exit_code": 0,
    "no_ipc_publishers": True,
    "bars_per_symbol": counts,
    "first_all_23_ready_bar": first_ready,
    "latest": latest,
    "publisher_sha256": hashlib.sha256((root / "cta_perp_factor_model_15m_pub").read_bytes()).hexdigest(),
    "account_observed_at_ms": account["created_at_ms"],
    "account": {
        "open_orders": account["open_orders"], "algo_orders": account["algo_orders"],
        "nonzero_perpetuals": sum(r["market"] == "perpetual" for r in account["rows"]),
        "nonzero_asset_debts": sum(Decimal(r.get("debt", "0")) != 0 for r in account["rows"]),
        "cash_residual_assets": len(cash),
        "cash_rounded_order_qty_nonzero": sum(Decimal(r.get("estimated_quantity", "0")) != 0 for r in cash),
        "cash_residual_notional_usdt": str(sum((Decimal(r["net"]) * Decimal(r["mark"]) for r in cash), Decimal(0))),
    },
}
out = run / "validation.json"
out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
print(json.dumps({"output": str(out), "bars": counts, "first_ready": first_ready,
                  "ready": {s: len(r["inputs"]) for s, r in latest.items()}, "account": summary["account"]}))
