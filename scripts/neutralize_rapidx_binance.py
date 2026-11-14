#!/usr/bin/env python3
"""Operator-run Binance RapidX cash/perp neutralization; default is GET-only.

Uses the same signing, portfolio identity and sym/info rules as ltp_rest.rs
and Manager's market_rules/rapidx.rs. Does not start any trading process.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import hmac
import http.client
import json
import os
import re
import signal
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from binance_local_ip import resolve_local_address

D = Decimal
ZERO = D(0)
ORDER = "/api/v1/trading/order"
TERMINAL = {"FILLED", "CANCELLED", "REJECT", "EXPIRED"}
ACTIVE = {"NEW", "OPEN", "PARTIALLY_FILLED", "PENDING_NEW", "PENDING_CANCEL"}


class Halt(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise Halt(message)


def dec(value):
    try:
        result = D(str(value))
    except InvalidOperation as exc:
        raise Halt("invalid decimal") from exc
    require(result.is_finite(), "non-finite decimal")
    return result


def fmt(value):
    return format(value, "f")


def quantize(value, step, rounding=ROUND_DOWN):
    require(step > 0, "nonpositive step")
    return (value / step).to_integral_value(rounding=rounding) * step


def signed_headers(key, secret, params, now=None):
    now = time.time_ns() if now is None else now
    nonce = str(now // 1_000_000_000)
    message = "&".join(f"{k}={params[k]}" for k in sorted(params)) + "&" + nonce
    return {"Content-Type": "application/json", "X-MBX-APIKEY": key,
            "nonce": nonce, "ts": str(now // 1000),
            "signature": hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()}


class Client:
    def __init__(self, local_ip, execute=False):
        self.execute = execute
        self.local_ip = local_ip
        self.key = os.environ["LTP_API_KEY"]
        self.secret = os.environ["LTP_API_SECRET"]
        require(self.key.strip() and self.secret.strip(), "missing RapidX credentials")
        self.portfolio = os.environ["LTP_PORTFOLIO_ID"]
        require(re.fullmatch(r"[0-9]{1,64}", self.portfolio), "invalid portfolio identity")
        self.base = os.environ.get("LTP_REST_URL", "https://api.liquiditytech.com").rstrip("/")
        require(self.base == "https://api.liquiditytech.com", "only official RapidX HTTPS endpoint supported")
        self.next_algo = 0.0
        self.next_rules = 0.0

    def request(self, method, path, params=None, public=False):
        params = params or {}
        require(method == "GET" or self.execute, "dry-run forbids all mutating requests")
        require(method == "GET" or (path == ORDER and method in {"POST", "DELETE"}),
                "only single-order placement/cancellation is permitted")
        if path in {"/api/v1/algo/openOrders", "/api/v1/trading/sym/info"}:
            attr = "next_algo" if "algo" in path else "next_rules"
            time.sleep(max(0, getattr(self, attr) - time.monotonic()))
            setattr(self, attr, time.monotonic() + 3.5)
        host = "fapi.binance.com" if public else urlsplit(self.base).hostname
        headers = {} if public else signed_headers(self.key, self.secret, params)
        body = None
        if method == "GET":
            path += ("?" + urlencode(sorted(params.items()))) if params else ""
        else:
            body = json.dumps(params).encode()
        connection = http.client.HTTPSConnection(
            host, timeout=5, source_address=(self.local_ip, 0) if self.local_ip else None)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            require(response.status == 200, f"{method} {path.split('?')[0]} HTTP {response.status}")
            data = json.loads(response.read())
        finally:
            connection.close()
        if public:
            return data
        require(data.get("code") == 200000,
                f"{method} {path.split('?')[0]} RapidX code={data.get('code')}; request not retried")
        return data["data"]

    def pages(self, path, params):
        result = []
        expected = None
        for page in range(1, 101):
            data = self.request("GET", path, dict(params, page=str(page), pageSize="1000"))
            total, pages = data["totalSize"], data["pageNum"]
            require(type(total) is int and type(pages) is int and 0 <= total <= 100000,
                    "invalid pagination totals")
            require(data["page"] == page and data["pageSize"] == 1000
                    and pages in ({0, 1} if total == 0 else {(total + 999) // 1000}),
                    "invalid pagination")
            require(expected is None or expected == (total, pages), "snapshot changed during pagination")
            expected = (total, pages)
            rows = data["list"]
            require(isinstance(rows, list) and len(rows) == min(1000, max(0, total - (page - 1) * 1000)),
                    "incomplete page")
            result.extend(rows)
            if page >= pages:
                require(len(result) == total, "incomplete snapshot")
                return result
        raise Halt("pagination limit exceeded")

    def identity(self, row):
        require(str(row["portfolioId"]) == self.portfolio, "portfolio mismatch")
        exchange = row.get("exchangeType", row.get("sym", "").split("_")[0])
        require(exchange == "BINANCE", "exchange mismatch")

    def empty_orders(self):
        orders = self.pages("/api/v1/trading/orders", {"exchange": "BINANCE"})
        algos = self.pages("/api/v1/algo/openOrders", {"exchange": "BINANCE"})
        for row in orders + algos:
            self.identity(row)
        require(not orders and not algos,
                f"open orders={len(orders)}, algo orders={len(algos)}; cancel externally first")

    def snapshot(self):
        start = time.monotonic()
        accounts = self.request("GET", "/api/v1/trading/account")
        accounts = [r for r in accounts if r.get("exchangeType") == "BINANCE"]
        require(len(accounts) == 1, "expected exactly one Binance account")
        self.identity(accounts[0])
        require(accounts[0]["positionMode"] == "NET", "only NET position mode supported")
        require(accounts[0]["accountStatus"] == "NORMAL", "account is not NORMAL")
        loans = self.request("GET", "/api/v1/trading/rapidxLoan/loan/info")
        require(str(loans["portfolioId"]) == self.portfolio, "loan portfolio mismatch")
        for account in loans["accounts"]:
            if account["exchange"] == "BINANCE":
                require(all(dec(c["loan"]) == 0 for c in account["coins"]),
                        "explicit RapidX loans require separate accounting; stopped")
        assets = self.pages("/api/v1/trading/portfolio/assets", {"exchangeType": "BINANCE"})
        positions = self.request("GET", "/api/v1/trading/position", {"exchange": "BINANCE"})
        cash, perp, marks = {}, {}, {}
        for row in assets:
            self.identity(row)
            coin = row["coin"]
            require(re.fullmatch(r"[A-Z0-9]+", coin) and coin not in cash, "invalid/duplicate asset")
            debt = dec(row["debt"])
            require(debt >= 0, "negative debt")
            cash[coin] = dec(row["balance"]) - debt
            marks[coin] = dec(row["indexPrice"])
        for row in positions:
            self.identity(row)
            match = re.fullmatch(r"BINANCE_PERP_([A-Z0-9]+)_USDT", row["sym"])
            require(match is not None, "non-USDT perpetual found; unsupported account scope")
            coin = match[1]
            require(coin not in perp and row["positionSide"] in {"NONE", "NET"},
                    "duplicate position or hedge mode")
            require(not row.get("tpslOrder"), "position has active TP/SL orders")
            perp[coin] = dec(row["positionQty"])
            if coin not in marks:
                marks[coin] = dec(row["markPrice"])
        require(time.monotonic() - start <= 15, "account snapshot too slow")
        return cash, perp, marks

    def rules(self):
        rows = self.request("GET", "/api/v1/trading/sym/info")
        result = {}
        for sym, row in rows.items():
            match = re.fullmatch(r"BINANCE_PERP_([A-Z0-9]+)_USDT", sym)
            if not match:
                continue
            coin = match[1]
            require(row["sym"] == sym and row["originalSymbol"] == coin + "USDT", "rule identity mismatch")
            require(dec(row["contractSize"]) == 1, "Binance contractSize must be 1")
            result[coin] = rule = {k: dec(row[k]) for k in ("tickSize", "lotSize", "minSize", "minNotional", "maxLimitSize")}
            require(all(rule[k] > 0 for k in ("tickSize", "lotSize", "minSize", "maxLimitSize"))
                    and rule["minNotional"] >= 0, "invalid order rule")
            rule["live"] = row["state"] == "live"
        require(result, "empty Binance perpetual rules")
        return result

    def book(self, coin):
        row = self.request("GET", "/fapi/v1/depth", {"symbol": coin + "USDT", "limit": "5"}, public=True)
        require(-1000 <= time.time() * 1000 - int(row["T"]) <= 5000, "stale/future order book")
        bid, ask = dec(row["bids"][0][0]), dec(row["asks"][0][0])
        require(0 < bid < ask, "invalid/crossed BBO")
        return bid, ask


def plan(cash, perp, mark, rule):
    net = cash + perp
    require(mark > 0, "missing positive valuation price")
    quantity = quantize(abs(net), rule["lotSize"])
    require(quantity <= rule["maxLimitSize"], "adjustment exceeds maximum limit order quantity")
    reason = "ready"
    if net == 0:
        reason = "neutral"
    elif not rule["live"]:
        reason = "inactive"
    elif quantity < rule["minSize"] or quantity == 0:
        reason = "below_min_qty"
    elif quantity * mark < rule["minNotional"]:
        reason = "below_min_notional"
    side = "SELL" if net > 0 else "BUY"
    signed = -quantity if net > 0 else quantity
    # Crossing through zero is necessary when the existing perp has the same
    # direction as cash. Reduce-only is valid only if this whole order reduces it.
    reduce_only = perp * signed < 0 and quantity <= abs(perp)
    return {"cash": fmt(cash), "perp": fmt(perp), "net": fmt(net),
            "net_usdt": fmt(net * mark), "target_perp": fmt(-cash),
            "side": side, "quantity": fmt(quantity), "reduce_only": reduce_only,
            "reason": reason, "residual_if_filled": fmt(net + signed)}


def limit_price(side, bid, ask, tick, ioc, slip_bps):
    if ioc:
        raw = ask * (1 + slip_bps / 10000) if side == "BUY" else bid * (1 - slip_bps / 10000)
        # Never round through the permitted slippage bound.
        return quantize(raw, tick, ROUND_FLOOR if side == "BUY" else ROUND_CEILING)
    return quantize(bid if side == "BUY" else ask, tick,
                    ROUND_FLOOR if side == "BUY" else ROUND_CEILING)


def assert_stopped(env):
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            exe = os.readlink(proc / "exe").removesuffix(" (deleted)")
            cwd = Path(os.readlink(proc / "cwd"))
            name = Path(exe).name
            owned = str(exe).startswith(str(env) + "/") or cwd == env
            require(not (owned and name in {"trade_engine", "pre_trade", "trade_signal", "exec-pre-trade",
                                          "cta_spread_signal", "cta_special_signal", "cta_perp_signal"}),
                    f"trading process still running: pid={proc.name} binary={name}")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            continue  # other users' processes are outside this env's deployment


class Journal:
    def __init__(self, path, portfolio):
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else {"portfolio": portfolio, "pending": None, "orders": []}
        require(self.data["portfolio"] == portfolio, "journal portfolio mismatch")

    def save(self):
        temp = self.path.with_suffix(".tmp")
        with open(temp, "w", encoding="utf8") as f:
            os.chmod(temp, 0o600)
            json.dump(self.data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, self.path)
        fd = os.open(self.path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def read_order(client, request):
    row = client.request("GET", ORDER, {"clientOrderId": request["clientOrderId"]})
    client.identity(row)
    require(row["clientOrderId"] == request["clientOrderId"] and row["sym"] == request["sym"]
            and row["side"] == request["side"] and dec(row["orderQty"]) == dec(request["orderQty"]),
            "order identity/quantity mismatch")
    require(row["orderState"] in TERMINAL | ACTIVE, "unknown order state")
    require(0 <= dec(row["executedQty"]) <= dec(request["orderQty"]), "invalid executed quantity")
    return row


def settle(client, journal, wait_seconds):
    """Resolve one durable intent. No re-submission, including on HTTP timeouts."""
    pending = journal.data["pending"]
    req = pending["request"]
    deadline = time.monotonic() + wait_seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = read_order(client, req)
            if last["orderState"] in TERMINAL:
                break
        except (Halt, OSError, ValueError, KeyError):
            # A placement ACK can precede visibility. Never replace an unknown order.
            pass
        time.sleep(1)
    if last is None or last["orderState"] not in TERMINAL:
        try:
            client.request("DELETE", ORDER, {"clientOrderId": req["clientOrderId"]})
        except (Halt, OSError, ValueError, KeyError):
            pass  # Cancellation ACK is not terminal evidence; query below.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                last = read_order(client, req)
                if last["orderState"] in TERMINAL:
                    break
            except (Halt, OSError, ValueError, KeyError):
                pass
            time.sleep(1)
    require(last is not None and last["orderState"] in TERMINAL,
            f"UNRESOLVED order {req['clientOrderId']}; journal retained; use --execute --recover-only")
    filled = dec(last["executedQty"])
    signed = filled if req["side"] == "BUY" else -filled
    expected = dec(pending["perp_before"]) + signed
    coin = req["sym"].split("_")[2]
    deadline = time.monotonic() + 20
    while True:
        cash, perp, _ = client.snapshot()
        if perp.get(coin, ZERO) == expected:
            break
        require(time.monotonic() < deadline, "position does not reconcile with actual fills; journal retained")
        time.sleep(1)
    record = {"request": req, "state": last["orderState"], "filled": fmt(filled),
              "price": last.get("executedAvgPrice"), "fee": last.get("fee"),
              "fee_coin": last.get("feeCoin"), "rebate": last.get("rebate"),
              "order_id": last["orderId"], "net_after": fmt(cash.get(coin, ZERO) + expected)}
    journal.data["orders"].append(record)
    journal.data["pending"] = None
    journal.save()
    print("[settled] " + json.dumps(record), flush=True)
    return filled * dec(req["limitPrice"])


def submit(client, journal, req, before, wait):
    require(journal.data["pending"] is None, "unresolved previous order")
    require(client.execute, "dry-run cannot submit")
    journal.data["pending"] = {"request": req, "perp_before": fmt(before), "created_ms": int(time.time() * 1000)}
    journal.save()  # Write-ahead intent must survive a lost response or process crash.
    try:
        client.request("POST", ORDER, req)
    except (Halt, OSError, ValueError, KeyError):
        print("[notice] placement outcome uncertain; resolving original clientOrderId without retry", flush=True)
    return settle(client, journal, wait)


def report(client, rules):
    cash, perp, marks = client.snapshot()
    rows = {}
    for coin in sorted(set(cash) | set(perp)):
        if coin == "USDT" or cash.get(coin, ZERO) == perp.get(coin, ZERO) == 0:
            continue
        require(coin in rules, f"no USDT perpetual rule for {coin}; manual review required")
        rows[coin] = plan(cash.get(coin, ZERO), perp.get(coin, ZERO), marks[coin], rules[coin])
    print(json.dumps({"observed_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      "execute": client.execute, "positions": rows}, indent=2), flush=True)
    return rows


def run(args, client, journal):
    assert_stopped(args.env_dir)
    if journal.data["pending"]:
        req = journal.data["pending"]["request"]
        require(args.execute and args.recover_only,
                f"unresolved order {req['clientOrderId']}; use --execute --recover-only; no new orders allowed")
        settle(client, journal, 1)
    if args.recover_only:
        client.empty_orders()
        report(client, client.rules())
        return 0
    client.empty_orders()
    rules = client.rules()
    initial = report(client, rules)
    total = sum((abs(dec(p["net_usdt"])) for p in initial.values() if p["reason"] == "ready"), ZERO)
    require(total <= args.max_total_usdt, "planned turnover exceeds --max-total-usdt")
    require(all(abs(dec(p["net_usdt"])) <= args.max_order_usdt for p in initial.values() if p["reason"] == "ready"),
            "symbol adjustment exceeds --max-order-usdt; review before increasing cap")
    require(not any(p["reason"] == "inactive" for p in initial.values()), "inactive symbol requires manual review")
    for coin, p in initial.items():
        if p["reason"] != "ready":
            continue
        bid, ask = client.book(coin)
        price = limit_price(p["side"], bid, ask, rules[coin]["tickSize"], False, args.max_slip_bps)
        print(f"[quote] {coin} {p['side']} qty={p['quantity']} maker_price={price} "
              f"notional={price * dec(p['quantity'])} reduce_only={p['reduce_only']}", flush=True)
    print(f"[plan] turnover_estimate_usdt={total} maker_seconds={args.maker_seconds} "
          f"maker_attempts={args.maker_attempts} ioc_fallback={args.ioc_fallback}", flush=True)
    if not args.execute:
        print("[dry-run] no POST/DELETE requests; rerun with --execute to trade")
        return 0
    spent = ZERO
    for coin, original in sorted(initial.items(), key=lambda item: -abs(dec(item[1]["net_usdt"]))):
        if original["reason"] != "ready":
            continue
        bid, ask = client.book(coin)
        anchor = (bid + ask) / 2
        rounds = [False] * args.maker_attempts + ([True] if args.ioc_fallback else [])
        for ioc in rounds:
            assert_stopped(args.env_dir)
            client.empty_orders()
            rule = client.rules()[coin]
            cash, perp, _ = client.snapshot()
            bid, ask = client.book(coin)
            mid = (bid + ask) / 2
            require(abs(mid / anchor - 1) * 10000 <= args.max_drift_bps,
                    f"{coin} market moved beyond --max-drift-bps; stopped")
            p = plan(cash.get(coin, ZERO), perp.get(coin, ZERO), mid, rule)
            if p["reason"] != "ready":
                break
            require(p["side"] == original["side"] and dec(p["quantity"]) <= dec(original["quantity"]),
                    "exposure changed externally; refusing to enlarge or reverse adjustment")
            price = limit_price(p["side"], bid, ask, rule["tickSize"], ioc, args.max_slip_bps)
            require(price > 0, "nonpositive limit price")
            notional = price * dec(p["quantity"])
            if notional < rule["minNotional"]:
                break
            require(notional <= args.max_order_usdt and spent + notional <= args.max_total_usdt,
                    "execution notional cap exceeded")
            req = {"sym": f"BINANCE_PERP_{coin}_USDT", "side": p["side"], "orderType": "LIMIT",
                   "timeInForce": "IOC" if ioc else "GTX", "orderQty": p["quantity"],
                   "limitPrice": fmt(price), "reduceOnly": "TRUE" if p["reduce_only"] else "FALSE",
                   "positionSide": "NONE", "clientOrderId": "neutral" + uuid.uuid4().hex[:24]}
            print("[submit] " + json.dumps(req), flush=True)
            spent += submit(client, journal, req, perp.get(coin, ZERO), 3 if ioc else args.maker_seconds)
    client.empty_orders()
    final = report(client, client.rules())
    unfinished = any(p["reason"] in {"ready", "inactive"} for p in final.values())
    print("[result] " + ("tradable residual remains; review and rerun" if unfinished
                          else "only zero/below-minimum residuals remain; see exact net quantities above"))
    return 2 if unfinished else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--recover-only", action="store_true")
    parser.add_argument("--maker-seconds", type=int, default=20)
    parser.add_argument("--maker-attempts", type=int, default=2)
    parser.add_argument("--ioc-fallback", action="store_true", help="after makers, one bounded IOC attempt")
    parser.add_argument("--max-slip-bps", type=dec, default=D(2))
    parser.add_argument("--max-drift-bps", type=dec, default=D(10))
    parser.add_argument("--max-order-usdt", type=dec, default=D(1000))
    parser.add_argument("--max-total-usdt", type=dec, default=D(2000))
    args = parser.parse_args()
    args.env_dir = args.env_dir.expanduser().resolve()
    require(Path.cwd().resolve() == args.env_dir, "cd into --env-dir and source its env.sh first")
    require((args.env_dir / "env.sh").is_file(), "missing env.sh")
    require(1 <= args.maker_seconds <= 60 and 1 <= args.maker_attempts <= 5, "invalid maker limits")
    require(0 <= args.max_slip_bps <= 10 and 0 < args.max_drift_bps <= 100, "invalid price limits")
    require(0 < args.max_order_usdt <= args.max_total_usdt, "invalid notional limits")
    local_ip, _ = resolve_local_address(env_dir=str(args.env_dir))
    client = Client(local_ip, args.execute)
    state_dir = args.env_dir / "run"
    state_dir.mkdir(exist_ok=True)
    with open(state_dir / "rapidx_neutralize.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = Journal(state_dir / "rapidx_neutralize.json", client.portfolio)
        # SIGTERM/Ctrl-C leave the write-ahead order for explicit recovery on restart.
        def interrupted(*_):
            print("[STOP] interrupted; check pending journal and use --execute --recover-only before new trades",
                  file=sys.stderr, flush=True)
            sys.exit(130)
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        return run(args, client, journal)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Halt, OSError, KeyError, ValueError, http.client.HTTPException) as exc:
        print(f"[STOP] {exc}", file=sys.stderr)
        sys.exit(1)
