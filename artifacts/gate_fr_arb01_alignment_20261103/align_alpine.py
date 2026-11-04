#!/usr/bin/env python3
"""One-off ALPINE alignment; dry-run unless --execute is present.

Run on jp-meta-elvpn over stdin. Credentials stay in the live process memory.
Only this contract may be traded. Every submission follows fresh position reads.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D, ROUND_CEILING, ROUND_DOWN
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path.home() / "gate_fr_arb01"
CONTRACT = "ALPINE_USDT"
HELPER_SHA = "583911a7e62c3244d73e2808f0ef0ae47ad8a8796408faf72c22168155b7a0e6"


def emit(kind, **values):
    print(json.dumps({"event": kind, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **values}, default=str), flush=True)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    require(ROOT.is_dir(), "Runtime directory missing")
    helper_path = ROOT / "scripts/flatten_fr_futures_exposure.py"
    require(hashlib.sha256(helper_path.read_bytes()).hexdigest() == HELPER_SHA, "Helper changed; inspect before use")
    spec = importlib.util.spec_from_file_location("alignment_gate_helper", helper_path)
    helper = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = helper
    spec.loader.exec_module(helper)
    raw_http = helper.http_request

    def decimal_http(url, **kwargs):
        kwargs.setdefault("headers", {})["X-Gate-Size-Decimal"] = "1"
        return raw_http(url, **kwargs)

    helper.http_request = decimal_http
    engine_pids = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if os.readlink(proc / "cwd") == str(ROOT) and (proc / "comm").read_text().strip() == "trade_engine":
                engine_pids.append(proc)
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    require(len(engine_pids) == 1, "Expected exactly one target trade engine")
    env = dict(item.split("=", 1) for item in (engine_pids[0] / "environ").read_bytes().decode().split("\0") if "=" in item)
    key, secret = env.get("GATE_API_KEY", ""), env.get("GATE_API_SECRET", "")
    require(bool(key and secret), "Missing Gate credentials")

    def request(method, path, params=None, body=None):
        if method != "GET":
            require(args.execute and method == "POST" and path == "/futures/usdt/orders", "Mutation outside authorized scope")
            require(body["contract"] == CONTRACT and D(body["size"]) < 0 and body["tif"] == "ioc", "Invalid alignment order")
        status, text, _ = helper.gate_private_request(method, path, key, secret, params=params, body=body)
        try:
            result = json.loads(text)
        except ValueError:
            result = {"error": text[:300]}
        return status, result

    def get(path, params=None):
        status, result = request("GET", path, params=params)
        require(status == 200, f"GET {path} failed: {status} {result}")
        return result

    def public(path, params=None):
        import urllib.parse
        suffix = "?" + urllib.parse.urlencode(params) if params else ""
        return helper.load_json_response("https://api.gateio.ws/api/v4" + path + suffix, headers={"X-Gate-Size-Decimal": "1"})

    def state():
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [
                pool.submit(get, "/unified/accounts", {"currency": "ALPINE"}),
                pool.submit(get, "/futures/usdt/positions/" + CONTRACT),
                pool.submit(get, "/futures/usdt/orders", {"contract": CONTRACT, "status": "open", "limit": 100}),
                pool.submit(get, "/spot/open_orders", {"currency_pair": CONTRACT}),
            ]
            account, position, futures_orders, spot_orders = [job.result() for job in jobs]
        require(not futures_orders and not spot_orders, "Target has live orders; recompute after they finish")
        require(position["mode"] == "single" and position["leverage"] == "0", "Unexpected position mode")
        require(position["pos_margin_mode"] == "cross" and not position.get("liq_lock"), "Position unavailable")
        balance = account["balances"]["ALPINE"]
        require(D(balance["borrowed"]) == 0 and D(balance["freeze"]) == 0, "Unexpected ALPINE borrowing/freeze")
        spot = D(balance["equity"])
        size = D(position["size"])
        net = spot + size * multiplier
        return {"spot_qty": spot, "contracts": size, "futures_base_qty": size * multiplier, "net_qty": net,
                "net_usdt": net * D(position["mark_price"]), "mark_price": D(position["mark_price"]),
                "risk_limit": D(position["risk_limit"]), "hedge_status": position.get("hedge_status"),
                "adl_ranking": position.get("adl_ranking")}

    contract = public("/futures/usdt/contracts/" + CONTRACT)
    require(contract["status"] == "trading" and not contract["in_delisting"], "Contract inactive")
    multiplier = D(contract["quanto_multiplier"])
    require(multiplier == 1 and not contract["enable_decimal"], "Unexpected contract units")
    tick = D(contract["order_price_round"])
    require(get("/unified/delta_neutral")["enabled"] is True, "Delta neutral mode disabled")
    initial = state()
    require(D(8400) < initial["net_qty"] < D(8600), "Exposure changed substantially; inspect again")
    target = (initial["net_qty"] / multiplier).to_integral_value(rounding=ROUND_DOWN)
    require((abs(initial["contracts"]) + target) * multiplier * initial["mark_price"] < initial["risk_limit"] - D(2000), "Target exceeds buffered venue limit")
    book = public("/futures/usdt/order_book", {"contract": CONTRACT, "limit": 100})
    reference_bid = D(book["bids"][0]["p"])
    price_floor = (reference_bid * D("0.99") / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    emit("plan", host=socket.getfqdn(), env=ROOT.name, contract=CONTRACT, execute=args.execute,
         before=initial, sell_contracts=target, reference_bid=reference_bid, minimum_sell_price=price_floor,
         max_child_notional_usdt=100, projected_residual_base=initial["net_qty"] - target * multiplier)
    if not args.execute:
        return

    total_filled = D(0)
    order_ids = []
    for attempt in range(1, 51):
        current = state()
        remaining = min(target - total_filled, (max(D(0), current["net_qty"]) / multiplier).to_integral_value(rounding=ROUND_DOWN))
        if remaining < 1:
            break
        require(current["net_qty"] > 0, "Exposure changed direction")
        book = public("/futures/usdt/order_book", {"contract": CONTRACT, "limit": 100})
        best_bid = D(book["bids"][0]["p"])
        if best_bid < price_floor:
            emit("waiting_for_price", best_bid=best_bid, minimum_sell_price=price_floor, remaining=remaining)
            time.sleep(0.5)
            continue
        price = max(price_floor, (best_bid * D("0.995") / tick).to_integral_value(rounding=ROUND_CEILING) * tick)
        available = sum((D(str(level["s"])) for level in book["bids"] if D(level["p"]) >= price), D(0))
        qty = min(remaining, available, (D(100) / (best_bid * multiplier)).to_integral_value(rounding=ROUND_DOWN))
        qty = qty.to_integral_value(rounding=ROUND_DOWN)
        if qty < 1:
            time.sleep(0.5)
            continue
        text = f"t-neut-{int(time.time() * 1000)}-{attempt}"
        body = {"contract": CONTRACT, "size": str(-qty), "price": str(price), "tif": "ioc", "text": text, "account": "unified"}
        emit("submit", before=current, body=body)
        status, order = request("POST", "/futures/usdt/orders", body=body)
        emit("order_response", http_status=status, order=order)
        if not 200 <= status < 300:
            # Reconcile ambiguity by client text; never repeat a POST blindly.
            query_status, queried = request("GET", "/futures/usdt/orders/" + text)
            emit("submission_reconciliation", http_status=query_status, order=queried)
            require(query_status == 200 and queried.get("text") == text, "Submission unresolved/rejected; stopped without retry")
            order = queried
        for _ in range(10):
            if order.get("status") == "finished":
                break
            time.sleep(0.2)
            order = get("/futures/usdt/orders/" + str(order["id"]))
        require(order.get("status") == "finished", "Order not terminal; stop before another submission")
        require(order["contract"] == CONTRACT and D(str(order["size"])) == -qty, "Order identity mismatch")
        filled = abs(D(str(order["size"]))) - abs(D(str(order["left"])))
        require(D(0) <= filled <= qty, "Invalid fill quantity")
        total_filled += filled
        order_ids.append(str(order["id"]))
        emit("terminal", order_id=order["id"], filled_contracts=filled, total_filled=total_filled, finish_as=order.get("finish_as"), fill_price=order.get("fill_price"))
        time.sleep(0.2)
    final = state()
    emit("result", before=initial, after=final, total_filled_contracts=total_filled, order_ids=order_ids,
         aligned=abs(final["net_qty"]) < multiplier)
    require(abs(final["net_qty"]) < multiplier, "Alignment incomplete within price protection; inspect residual")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit("stopped", reason=str(error))
        sys.exit(1)
