"""Restore RX01's Manager source and new Perp catalog in explicit guarded steps.

Default is read-only. Never restores retired Spread bindings or stored targets.
Secrets, account identities and full backups remain private on the target host.
"""
import argparse
import ast
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tomllib

home = Path.home()
manager = home / "crypto_cta_manager"
root = home / "binance-cta-perp-rx01"
run = home / "binance_exec_trade06/run/restore_audit_20261030"
helper = home / "binance_exec_trade06/run/rx01_activation_ops.py"
tree = ast.parse(helper.read_text())
nodes = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)) or (isinstance(n, ast.FunctionDef) and n.name in {"db_env", "sql", "session"})]
namespace = {"MANAGER": manager}
for n in tree.body:
    if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ORIGIN" for t in n.targets):
        namespace["ORIGIN"] = ast.literal_eval(n.value)
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(helper), "exec"), namespace)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("action", choices=["source", "catalog", "connect", "verify"])
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()


def save(name, value):
    p = run / name
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    p.chmod(0o600)


def ready():
    p = root / "run/operator_list_20261030/formal_readiness.json"
    j = json.loads(p.read_text())
    assert j["ready_rules"] == 146 and j["nq_ready_symbols"] == 38 and j["caught_up"] is True
    assert datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000 - j["model_ts_ms"] <= 1800000


def plan():
    config = json.loads((root / "config/cta_perp.json").read_text())
    sys.path.insert(0, str(root / "scripts"))
    from cta_perp_config_server import rule_strategy
    groups = {}
    for r in config["factor_rules"]:
        name = "binance_cta_perp_rx01_" + rule_strategy(config, r)
        groups.setdefault(name, {})[r["标的"]] = {"qty": "clear"}
    assert len(groups) == 91 and sum(map(len, groups.values())) == 146
    return groups


if args.action == "source":
    path = manager / "config/cta-manager.toml"
    before = path.read_text()
    config = tomllib.loads(before)
    assert len(config["sources"]) == 7 and all(s["id"] != "binance_exec_trade06" for s in config["sources"])
    original = (home / "binance_exec_trade06/run/activation_20261013/manager.toml.after").read_text()
    matches = list(re.finditer(r"(?m)^\[\[sources\]\]\s*$", original))
    block = None
    for m in matches:
        tail = original[m.end():]
        end = re.search(r"(?m)^\[", tail)
        candidate = original[m.start():m.end() + end.start()] if end else original[m.start():]
        source = tomllib.loads(candidate)["sources"][0]
        if source["id"] == "binance_exec_trade06":
            block = candidate.strip() + "\n"
    assert block
    expected = tomllib.loads(block)["sources"][0]
    assert expected["rocksdb_path"] == str(home / "binance_exec_trade06/data/persist_manager")
    assert expected["ipc_namespace"] == "binance_exec_trade06"
    header = "[dashboard.source_viewers]\n"
    assert before.count(header) == 1
    candidate = before.replace(header, header + 'binance_exec_trade06 = ["lgq"]\n') + "\n# RX01 15m Perp; retired Spread bindings remain detached.\n" + block
    after = tomllib.loads(candidate)
    assert after["sources"][:-1] == config["sources"] and len(after["sources"]) == 8
    assert after["dashboard"]["source_viewers"]["binance_exec_trade06"] == ["lgq"]
    assert str(home / "binance_exec_trade06/trade_engine.toml") in after["kline"]["trade_engine_configs"]
    (run / "manager.toml.candidate").write_text(candidate)
    (run / "manager.toml.candidate").chmod(0o600)
    if args.execute:
        ready()
        assert json.loads((run / "reset_receipt.json").read_text())["verified"] is True
        assert not (run / "manager.toml.before").exists()
        shutil.copy2(path, run / "manager.toml.before")
        assert path.read_text() == before
        temp = path.with_name(path.name + ".rx01-next")
        temp.write_text(candidate)
        temp.chmod(path.stat().st_mode & 0o777)
        os.replace(temp, path)
        subprocess.run(["systemctl", "--user", "restart", "crypto-cta-manager-web.service"], check=True)
    print(json.dumps({"action": args.action, "execute": args.execute, "sources_before": 7, "sources_after": 8, "rx01_viewers": ["lgq"], "other_sources_unchanged": True}))
else:
    groups = plan()
    with namespace["session"]("lgq") as api:
        reference = api("/manager/api/catalog/accounts/binance_exec_trade07")
        account = api("/manager/api/catalog/accounts/binance_exec_trade06")
        virtuals = api("/manager/api/catalog/virtual-accounts")
        orders = api("/manager/api/catalog/order-strategies")
        positions = api("/manager/api/catalog/position-strategies")
        old = {"reference": reference, "account": account, "virtuals": virtuals, "orders": orders, "positions": positions}
        if args.action == "catalog":
            save("catalog_before.json", old)
            reference_order = next(x for x in orders if x["strategy_name"] == "lgq_grid_prep")
            name = "binance_cta_perp_rx01_grid"
            assert all(x["strategy_name"] != name for x in orders)
            assert all(not x["strategy_name"].startswith("binance_cta_perp_rx01_") for x in positions)
            assert all(x["name"] != "binance_cta_perp_rx01" for x in virtuals)
            assert not account["bindings"] and account["configuration"]["mode"] == "independent"
            if args.execute:
                ready()
                token = secrets.token_urlsafe(48)
                p = run / "publish_token"
                with p.open("x") as f:
                    p.chmod(0o600)
                    f.write(token)
                api("/manager/api/catalog/order-strategies", {"strategy_name": name, "order_parameters": reference_order["order_parameters"]})
                for strategy, targets in sorted(groups.items()):
                    api("/manager/api/catalog/position-strategies", {"strategy_name": strategy, "targets": targets})
                    api(f"/manager/api/catalog/position-strategies/{strategy}/publish-token", {"publish_token": token}, "PUT")
                bindings = [{"binding_name": s, "position_strategy_name": s, "order_strategy_name": name, "shares": 1.0} for s in sorted(groups)]
                virtual = api("/manager/api/catalog/virtual-accounts", {"name": "binance_cta_perp_rx01", "bindings": bindings})
                save("new_virtual.json", virtual)
            print(json.dumps({"action": args.action, "execute": args.execute, "position_strategies": len(groups), "target_rows": sum(map(len, groups.values())), "order_parameters": reference_order["order_parameters"], "targets": "all clear", "scope": "38 USDT perpetuals"}))
        elif args.action == "connect":
            virtual = json.loads((run / "new_virtual.json").read_text())
            configuration = {"mode": "follow", "follows": [{"virtual_id": virtual["virtual_id"], "multiplier": 1.0}]}
            assert {b["position_strategy_name"] for b in virtual["bindings"]} == set(groups)
            if args.execute:
                ready()
                api("/manager/api/catalog/accounts/binance_exec_trade06/configuration", configuration, "PUT")
                env = root / "env.sh"
                before = env.read_text()
                assert "export CTA_PUBLISH_TOKEN=" not in before and "export CTA_MANAGER_API_URL" not in before
                shutil.copy2(env, run / "signal_env.before")
                token = (run / "publish_token").read_text().strip()
                assert re.fullmatch(r"[A-Za-z0-9_-]{64}", token)
                candidate = before + "\n# New Perp catalog only.\nexport CTA_MANAGER_API_URL='http://127.0.0.1:18201/api'\nexport CTA_FOLLOWER_SOURCE_ID='binance_exec_trade06'\nexport CTA_PUBLISH_TOKEN='" + token + "'\n"
                temp = env.with_name("env.sh.rx01-next")
                temp.write_text(candidate)
                temp.chmod(0o600)
                os.replace(temp, env)
            print(json.dumps({"action": args.action, "execute": args.execute, "configuration": configuration, "bindings": len(groups)}))
        else:
            matching = [p for p in positions if p["strategy_name"] in groups]
            assert len(matching) == 91
            assert {b["position_strategy_name"] for b in account["bindings"]} == set(groups)
            assert all(b["shares"] == 1 for b in account["bindings"])
            assert all(set(p["targets"]) == set(groups[p["strategy_name"]]) for p in matching)
            save("catalog_verified.json", {"account": account, "positions": matching})
            print(json.dumps({"position_strategies": 91, "target_rows": sum(len(p["targets"]) for p in matching), "configuration": account["configuration"], "pending_publishes": account["pending_publishes"]}))
