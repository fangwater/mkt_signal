#!/usr/bin/env python3
"""Align only excess ESPORTS shorts; defaults to read-only preview."""
import argparse
import hashlib
import importlib.util
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D, ROUND_DOWN, ROUND_HALF_UP
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path.home() / "gate_fr_arb01"
CONTRACT = "ESPORTS_USDT"


def emit(event, **values):
    print(json.dumps({"event": event, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **values}, default=str), flush=True)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    helper_path = ROOT / "scripts/flatten_fr_futures_exposure.py"
    require(ROOT.is_dir(), "Runtime missing")
    require(hashlib.sha256(helper_path.read_bytes()).hexdigest() == "583911a7e62c3244d73e2808f0ef0ae47ad8a8796408faf72c22168155b7a0e6", "Helper changed")
    spec = importlib.util.spec_from_file_location("gate_alignment_helper", helper_path)
    helper = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = helper
    spec.loader.exec_module(helper)
    raw_http = helper.http_request

    def decimal_http(url, **kw):
        kw.setdefault("headers", {})["X-Gate-Size-Decimal"] = "1"
        return raw_http(url, **kw)

    helper.http_request = decimal_http
    pids = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if os.readlink(proc / "cwd") == str(ROOT) and (proc / "comm").read_text().strip() == "trade_engine":
                pids.append(proc)
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    require(len(pids) == 1, "Ambiguous target process")
    env = dict(x.split("=", 1) for x in (pids[0] / "environ").read_bytes().decode().split("\0") if "=" in x)
    key, secret = env["GATE_API_KEY"], env["GATE_API_SECRET"]

    def request(method, path, params=None, body=None):
        if method != "GET":
            require(args.execute and method == "POST" and path == "/futures/usdt/orders", "Unauthorized mutation")
            require(body["contract"] == CONTRACT and body["reduce_only"] is True and D(body["size"]) > 0 and body["tif"] == "ioc", "Invalid reduction order")
        status, text, _ = helper.gate_private_request(method, path, key, secret, params=params, body=body)
        try:
            result = json.loads(text)
        except ValueError:
            result = {"error": text[:200]}
        return status, result

    def get(path, params=None):
        status, result = request("GET", path, params=params)
        require(status == 200, f"GET {path}: {status} {result}")
        return result

    def public(path):
        return helper.load_json_response("https://api.gateio.ws/api/v4" + path, headers={"X-Gate-Size-Decimal": "1"})

    contract = public("/futures/usdt/contracts/" + CONTRACT)
    require(contract["status"] == "trading" and not contract["in_delisting"], "Inactive contract")
    multiplier = D(contract["quanto_multiplier"])
    require(multiplier == 100 and contract["enable_decimal"], "Unexpected contract units")
    step = D(str(contract.get("order_size_step", "0.1")))
    if step <= 0:
        step = D("0.1")
    require(step == D("0.1"), "Unexpected order increment")
    tick = D(contract["order_price_round"])

    def state():
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(get, "/unified/accounts", {"currency": "ESPORTS"}),
                    pool.submit(get, "/futures/usdt/positions/" + CONTRACT),
                    pool.submit(get, "/futures/usdt/orders", {"contract": CONTRACT, "status": "open", "limit": 100}),
                    pool.submit(get, "/spot/open_orders", {"currency_pair": CONTRACT})]
            account, position, futures_orders, spot_orders = [j.result() for j in jobs]
        require(not futures_orders and not spot_orders, "Target has live orders")
        require(position["mode"] == "single" and position["pos_margin_mode"] == "cross", "Unexpected account mode")
        size = D(position["size"])
        require(size < 0 and not position.get("liq_lock"), "No available short position")
        balance = account["balances"]["ESPORTS"]
        require(D(balance["borrowed"]) == 0 and D(balance["freeze"]) == 0, "Borrowed/frozen asset")
        spot = D(balance["equity"])
        net = spot + size * multiplier
        return {"spot_qty": spot, "contracts": size, "net_qty": net, "net_usdt": net * D(position["mark_price"]),
                "hedge_status": position.get("hedge_status"), "unhedged_contracts": position.get("unhedged_size"), "adl_ranking": position.get("adl_ranking")}

    before = state()
    require(D(-13000) < before["net_qty"] < D(-11000), "Exposure changed; inspect before trading")
    qty = ((-before["net_qty"] / multiplier) / step).to_integral_value(rounding=ROUND_HALF_UP) * step
    require(qty <= abs(before["contracts"]) and qty <= 130, "Reduction exceeds authorized mismatch")
    book = public("/futures/usdt/order_book?contract=" + CONTRACT + "&limit=100")
    best_ask = D(book["asks"][0]["p"])
    price = (best_ask * D("1.005") / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    depth = sum((D(str(x["s"])) for x in book["asks"] if D(x["p"]) <= price), D(0))
    require(depth >= qty and qty * multiplier * best_ask < 200, "Insufficient protected liquidity or oversized adjustment")
    body = {"contract": CONTRACT, "size": str(qty), "price": str(price), "tif": "ioc", "reduce_only": True,
            "account": "unified", "text": f"t-neut-{int(time.time() * 1000)}-es"}
    emit("plan", host=socket.getfqdn(), env=ROOT.name, execute=args.execute, before=before, body=body,
         contract_multiplier=multiplier, step=step, best_ask=best_ask, expected_net_qty=before["net_qty"] + qty * multiplier)
    if not args.execute:
        return
    status, order = request("POST", "/futures/usdt/orders", body=body)
    emit("order_response", http_status=status, order=order)
    if not 200 <= status < 300:
        status, order = request("GET", "/futures/usdt/orders/" + body["text"])
        emit("submission_reconciliation", http_status=status, order=order)
        require(status == 200 and order.get("text") == body["text"], "Submission rejected/unresolved; no POST retry")
    for _ in range(15):
        if order.get("status") == "finished":
            break
        time.sleep(0.2)
        order = get("/futures/usdt/orders/" + str(order["id"]))
    require(order.get("status") == "finished", "Order not terminal")
    require(order["contract"] == CONTRACT and D(str(order["size"])) == qty, "Order identity mismatch")
    filled = qty - abs(D(str(order["left"])))
    require(D(0) <= filled <= qty, "Invalid fill quantity")
    after = state()
    aligned = abs(after["net_qty"]) <= multiplier * step / 2
    emit("result", before=before, after=after, filled_contracts=filled, filled_base=filled * multiplier,
         order_id=order["id"], finish_as=order.get("finish_as"), aligned=aligned)
    require(aligned, "Residual remains; inspect live position before further orders")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        emit("stopped", reason=str(error))
        sys.exit(1)
