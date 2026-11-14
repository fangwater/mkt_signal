#!/usr/bin/env python3
"""Enable Delta only on active audited FR accounts; --execute is explicit.

The existing repository helper is supplied in DELTA_HELPER_SOURCE by the local
runner. All private traffic binds to the environment's configured order IP.
"""
import argparse
import http.client
import json
import os
import re
import socket
import sys
import time
import tomllib
import types
import urllib.parse
from pathlib import Path

sys.dont_write_bytecode = True
module = types.ModuleType("existing_delta_helper")
module.__file__ = "scripts/set_delta_neutral_mode.py"
sys.modules[module.__name__] = module
exec(DELTA_HELPER_SOURCE, module.__dict__)


def emit(event, **data):
    print(json.dumps({"event": event, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **data}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--env-name", help="Narrow to one account; explicit gate_fr_arb04 opts it into the later authorization")
    args = parser.parse_args()
    targets = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if (proc / "comm").read_text().strip() != "trade_engine":
                continue
            root = Path(os.readlink(proc / "cwd"))
            if (re.fullmatch(r"(gate|binance|bitget|bybit)_fr_arb\d+", root.name)
                    and (root.name != "gate_fr_arb04" or args.env_name == "gate_fr_arb04")
                    and (not args.env_name or root.name == args.env_name)):
                targets.append((root, proc))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    # Apply known OFF switches first, then verify all remaining active accounts.
    priority = {"gate_fr_arb03": 0, "bitget_fr_arb03": 1, "bybit_fr_arb01": 2}
    targets.sort(key=lambda pair: (priority.get(pair[0].name, 3), pair[0].name))
    results = []
    last_binance_query = 0
    for root, proc in targets:
        ex = root.name.split("_", 1)[0]
        env = dict(item.split("=", 1) for item in (proc / "environ").read_bytes().decode().split("\0") if "=" in item)
        source_ip = tomllib.loads((root / "trade_engine.toml").read_text())["local_ips"][0]
        post_count = 0

        def transport(method, url, headers, data, timeout):
            nonlocal post_count
            parsed = urllib.parse.urlsplit(url)
            method = method.upper()
            allowed_post = {"binance": module.BINANCE_PATH, "gate": "/api/v4" + module.GATE_PATH,
                            "bitget": module.BITGET_SET_PATH, "bybit": module.BYBIT_SET_PATH}[ex]
            if method == "POST":
                assert args.execute and parsed.path == allowed_post and post_count == 0
                body = urllib.parse.parse_qs(data.decode()) if ex == "binance" else json.loads(data)
                if ex == "binance":
                    assert body.get("deltaEnabled") == ["true"]
                elif ex == "gate":
                    assert body == {"enabled": True}
                elif ex == "bitget":
                    assert body == {"mode": "advanced", "deltaSwitch": "yes"}
                else:
                    assert body == {"deltaEnable": "1"}
                post_count += 1
            else:
                assert method == "GET"
            connection = http.client.HTTPSConnection(parsed.hostname, timeout=timeout, source_address=(source_ip, 0))
            try:
                target = parsed.path + ("?" + parsed.query if parsed.query else "")
                connection.request(method, target, body=data, headers=dict(headers))
                response = connection.getresponse()
                body_text = response.read().decode()
                weights = {k: v for k, v in response.getheaders() if "used" in k.lower() and "weight" in k.lower()}
                emit("request_result", env=root.name, method=method, path=parsed.path, status=response.status, weight=weights)
                return module.HttpResponse(response.status, body_text)
            finally:
                connection.close()

        key, secret = env[ex.upper() + "_API_KEY"], env[ex.upper() + "_API_SECRET"]
        base = module.DEFAULT_BASE_URLS[ex]
        if ex == "binance":
            client = module.BinanceClient(key, secret, base, 12, transport)
        elif ex == "gate":
            client = module.GateClient(key, secret, base, 12, transport)
        elif ex == "bybit":
            client = module.BybitClient(key, secret, base, 12, transport)
        else:
            phrase = env.get("BITGET_API_PASSPHRASE") or env["BITGET_PASSPHRASE"]
            client = module.BitgetClient(key, secret, phrase, base, 12, transport=transport)

        def query_state():
            nonlocal last_binance_query
            if ex == "binance":
                # Wait before constructing signed timestamps, never afterward.
                wait = max(0, 31 - (time.monotonic() - last_binance_query))
                if wait:
                    emit("read_weight_spacing", env=root.name, wait_seconds=round(wait, 2))
                    time.sleep(wait)
                last_binance_query = time.monotonic()
            return client.query()

        result = {"env": root.name, "host": socket.getfqdn(), "changed": False}
        try:
            before = query_state()
            result["before"] = before.enabled
            emit("plan", **result, execute=args.execute, mutation=client.mutation_summary(True))
            if args.execute and before.enabled is not True:
                # One POST only: errors/timeouts are reconciled by a GET below.
                try:
                    client.set_enabled(True)
                    result["changed"] = True
                except Exception as error:
                    result["submission_error"] = str(error)
                after = query_state()
            else:
                after = before
            result["after"] = after.enabled
            result["detail"] = after.detail
            if args.execute and after.enabled is not True:
                result["error"] = "Delta mode did not verify ON"
        except Exception as error:
            result["error"] = str(error)
        results.append(result)
        emit("result", **result)
    emit("summary", execute=args.execute, results=results, all_verified_on=bool(results) and all(r.get("after") is True for r in results))
    if args.execute and any(r.get("after") is not True for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
