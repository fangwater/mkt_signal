"""Archive and reset retired RX01 allocations/targets, only while Exec is stopped.

Run from outside the env with its env.sh sourced. Default is a GET-only preview.
Uses exchange GET requests and scoped Redis transactions; submits no orders.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import tomllib

import redis

home = Path.home()
env = home / "binance_exec_trade06"
run = env / "run/restore_audit_20261030"
sys.path.insert(0, str(run / "inventory_tools"))
import neutralize_rapidx_binance as n
from binance_local_ip import resolve_local_address
from rapidx_retirement_plan import build_plan

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
n.assert_stopped(env)
manager = tomllib.loads((home / "crypto_cta_manager/config/cta-manager.toml").read_text())
assert all(s["id"] != env.name for s in manager["sources"]), "Manager source already restored"
for p in Path("/proc").glob("[0-9]*"):
    try:
        if (p / "cwd").resolve() == env:
            raise RuntimeError("Exec environment still owns a process")
    except (OSError, ValueError):
        pass
ip, _ = resolve_local_address(env_dir=str(env))
assert ip
client = n.Client(ip, execute=False)
orders = client.pages("/api/v1/trading/orders", {"exchange": "BINANCE"})
algos = client.pages("/api/v1/algo/openOrders", {"exchange": "BINANCE"})
assets = client.pages("/api/v1/trading/portfolio/assets", {"exchangeType": "BINANCE"})
for row in orders + algos + assets:
    client.identity(row)
cash, positions, marks = client.snapshot()  # Also checks explicit loans and NET mode.
rules = client.request("GET", "/api/v1/trading/sym/info")
plan = build_plan(assets, positions, rules)
assert not orders and not algos and not any(positions.values()), "exchange is not flat"
assert all(n.dec(a["debt"]) == 0 for a in assets), "asset debt, including USDT, remains"
residual = [r for r in plan if r["market"] == "margin"]
assert all(r["reason"] == "below_minimum_order" and n.dec(r["estimated_quantity"]) == 0 for r in residual), "tradable cash inventory remains"
summary = {
    "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "environment": env.name, "execute": args.execute,
    "open_orders": 0, "algo_orders": 0, "nonzero_perpetuals": 0,
    "asset_debts_including_usdt": 0, "explicit_loans": 0,
    "cash_dust_assets": len(residual), "cash_dust_notional_usdt": str(sum((n.dec(r["net"]) * n.dec(r["mark"]) for r in residual), n.ZERO)),
    "tradable_cash_quantity": 0,
}
r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"), socket_timeout=5)
all_keys = sorted(r.scan_iter(env.name + ":*"))
prefixes = tuple((f"{env.name}:{market}:{family}:".encode()) for market in ("binance-futures", "binance-margin") for family in ("batch_exec", "batch_exec_state", "chase_exec", "chase_exec_state", "exec_switch"))
selected = [k for k in all_keys if k.startswith(prefixes)]
assert selected, "no retired state to reset"
summary.update(reset_keys=len(selected), preserved_keys=len(all_keys) - len(selected), keys_sha256=hashlib.sha256(b"\n".join(selected)).hexdigest())
if args.execute:
    backup = run / "redis_before.json"
    assert not backup.exists(), "state was already archived; inspect before rerunning"
    with r.pipeline() as pipe:
        pipe.watch(*all_keys)
        assert sorted(r.scan_iter(env.name + ":*")) == all_keys, "concurrent namespace change"
        records = [{"key": k.decode(), "dump": base64.b64encode(pipe.dump(k)).decode(), "pttl": pipe.pttl(k)} for k in all_keys]
        with backup.open("x") as f:
            backup.chmod(0o600)
            json.dump({"summary": summary, "records": records}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        pipe.multi()
        pipe.delete(*selected)
        deleted, = pipe.execute()
        assert deleted == len(selected)
    assert not any(k.startswith(prefixes) for k in r.scan_iter(env.name + ":*"))
    summary["verified"] = True
    (run / "reset_receipt.json").write_text(json.dumps(summary, indent=2) + "\n")
    (run / "reset_receipt.json").chmod(0o600)
else:
    (run / "reset_preview.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary))
