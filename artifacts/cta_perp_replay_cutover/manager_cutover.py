"""RX02 catalog migration. Dry-run is read-only; --execute changes only virtual02.

Run on JP after publishing a verified Manager with explicit-clear support.
prepare: signals must be stopped; add an empty mean strategy and its binding.
retire: recovery must be complete; verify final targets, republish every retired
strategy's explicit clears, then set its shares to zero without deleting history.
"""
import argparse
import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path.home()
STAGE = Path(__file__).resolve().parent
SOURCE = "binance_exec_trade05"
VIRTUAL = "virtual02"
MEAN = "lgq_cta_symbol_mean"
PREFIX = "lgq_cta_"
SIGNAL = ROOT / "binance-cta-perp-rx02"
API = "http://127.0.0.1:4191/manager/api"
BINDING_KEYS = ("binding_name", "position_strategy_name", "order_strategy_name", "shares")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def same_quantity(left, right):
    # JSON round trips through Manager can differ by one binary64 ULP.
    # Keep zero/clear semantics exact and reject any larger discrepancy.
    if type(left) not in (int, float) or type(right) not in (int, float):
        return False
    if not math.isfinite(left) or not math.isfinite(right):
        return False
    if left == right:
        return True
    return left * right > 0 and abs(left - right) <= max(math.ulp(left), math.ulp(right))


def manager_pid():
    pid = subprocess.check_output(
        ["systemctl", "--user", "show", "crypto-cta-manager-web.service", "-p", "MainPID", "--value"],
        text=True,
    ).strip()
    require(pid.isdigit() and pid != "0", "Manager is not running")
    return pid


def sql(query):
    config = tomllib.loads((ROOT / "crypto_cta_manager/config/cta-manager.toml").read_text())
    raw = Path("/proc", manager_pid(), "environ").read_bytes()
    values = dict(row.split(b"=", 1) for row in raw.split(b"\0") if b"=" in row)
    url = urllib.parse.urlsplit(values[config["database"].get("url_env", "CRYPTO_CTA_LOCAL_DATABASE_URL").encode()].decode())
    env = dict(os.environ, PGHOST=url.hostname, PGPORT=str(url.port or 5432),
               PGUSER=urllib.parse.unquote(url.username),
               PGPASSWORD=urllib.parse.unquote(url.password or ""), PGDATABASE=url.path[1:])
    result = subprocess.run(["psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1"],
                            input=query, text=True, capture_output=True, env=env)
    require(result.returncode == 0, "Manager database request failed; details suppressed")
    return result.stdout.strip()


def snapshot():
    # A single read-only statement; no credentials or token hashes are selected.
    return json.loads(sql("""
        SELECT json_build_object(
          'bindings', (SELECT coalesce(json_agg(b ORDER BY source_id,binding_name),'[]')
             FROM cta_account_strategy_bindings b WHERE source_id IN
             ('binance_exec_trade05','binance_exec_trade06','binance_exec_trade07')),
          'virtuals', (SELECT coalesce(json_agg(b ORDER BY virtual_id,binding_name),'[]')
             FROM cta_virtual_account_bindings b WHERE virtual_id IN ('virtual02','virtual03','virtual04')),
          'followers', (SELECT coalesce(json_agg(f ORDER BY source_id),'[]')
             FROM cta_account_follows f WHERE virtual_id IN ('virtual02','virtual03','virtual04')),
          'virtual', (SELECT row_to_json(v) FROM cta_virtual_accounts v WHERE virtual_id='virtual02'),
          'targets', (SELECT coalesce(json_agg(t ORDER BY strategy_name),'[]') FROM
             (SELECT strategy_name,targets FROM cta_position_strategies WHERE strategy_name LIKE 'lgq_cta_%') t),
          'pending', (SELECT coalesce(json_agg(p ORDER BY source_id,binding_name),'[]')
             FROM cta_follow_publish_queue p WHERE source_id='binance_exec_trade05'));
    """))


def bindings(rows):
    result = [{key: row[key] for key in BINDING_KEYS} for row in rows]
    require(len({row["binding_name"] for row in result}) == len(result), "duplicate binding")
    return sorted(result, key=lambda row: row["binding_name"])


def scope_check(state, expected):
    followers = [row for row in state["followers"] if row["virtual_id"] == VIRTUAL]
    require(len(followers) == 1 and followers[0]["source_id"] == SOURCE
            and followers[0]["multiplier"] == 1, "virtual02 follower scope changed")
    require(bindings([row for row in state["virtuals"] if row["virtual_id"] == VIRTUAL])
            == bindings(expected), "virtual02 bindings differ from reviewed plan")
    require(bindings([row for row in state["bindings"] if row["source_id"] == SOURCE])
            == bindings(expected), "RX02 effective bindings differ from reviewed plan")
    require(not state["pending"], "RX02 configuration publication is still pending")


def protected(state):
    return {
        "bindings": [row for row in state["bindings"] if row["source_id"] != SOURCE],
        "virtuals": [row for row in state["virtuals"] if row["virtual_id"] != VIRTUAL],
        "followers": state["followers"],
    }


def verify_retired_targets(state, old_bindings, shadow):
    catalog = {row["strategy_name"]: row["targets"] for row in state["targets"]}
    for row in old_bindings:
        name = row["position_strategy_name"]
        require(name.startswith(PREFIX), "unexpected retired strategy")
        targets = catalog.get(name)
        require(isinstance(targets, dict) and bool(targets), "missing retired target table: " + name)
        require(set(shadow.get(name[len(PREFIX):], {})) <= set(targets),
                "retired target table omits historical symbols: " + name)
        require(all(isinstance(value, dict) and value.get("qty") == "clear"
                    and value.get("signal", 0) == 0 for value in targets.values()),
                "retired strategy has not published explicit clears: " + name)
    return catalog


def assert_signals_stopped():
    envs = {ROOT / "binance-cta-perp-rx02", ROOT / "binance-cta-perp-rx03"}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            live = proc.joinpath("cwd").resolve() in envs and proc.joinpath("exe").resolve().name == "cta_perp_signal"
        except OSError:
            continue
        require(not live, "RX02 and RX03 signals must be stopped before prepare")


def env_values(env):
    raw = subprocess.check_output(["bash", "-c", 'source "$1/env.sh"; env -0', "bash", str(env)])
    return dict(row.decode().split("=", 1) for row in raw.split(b"\0") if b"=" in row)


def publication_token():
    token = env_values(SIGNAL).get("CTA_PUBLISH_TOKEN", "")
    if not token:
        # RX02's existing strategies allow legacy pushes without a token.
        # Give the newly created strategy its own token and persist it only in
        # the stopped signal's remote env, after installation has backed up state.
        backup = SIGNAL / "run/cutover_before_20261015/env.sh"
        require(backup.parent.is_dir(), "install the reviewed Perp release before preparing a new publish token")
        path = SIGNAL / "env.sh"
        original = path.read_bytes()
        with backup.open("xb") as output:
            os.chmod(backup, 0o600)
            output.write(original)
        token = secrets.token_urlsafe(32)
        replacement = path.with_name("env.sh.clear-cutover-next")
        with replacement.open("xb") as output:
            os.chmod(replacement, path.stat().st_mode & 0o777)
            output.write(original + ("\n# RX02 mean strategy scoped publication token\nexport CTA_PUBLISH_TOKEN='" + token + "'\n").encode())
        require(path.read_bytes() == original, "RX02 environment changed while preparing its token")
        os.replace(replacement, path)
    return token


def verify_runtime_targets(name, expected, published=None):
    import redis
    values = env_values(ROOT / SOURCE)
    client = redis.Redis.from_url(values.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                                 decode_responses=True, socket_timeout=10)
    try:
        # Read-only evidence. All runtime writes go through the Manager API.
        raw = client.get(SOURCE + ":binance-futures:batch_exec:" + name)
    finally:
        client.close()
    require(raw is not None, "strategy is absent from Exec Redis: " + name)
    runtime = json.loads(raw)
    require(runtime["targets"] == expected
            and (published is None or runtime["updated_at_us"] == published["updated_at_us"]),
            "Exec Redis differs from the final published target vector: " + name)


@contextlib.contextmanager
def session(username):
    require(username in ("lgq", "prc"), "unexpected session user")
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    sid = sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '10 minutes' FROM cta_users WHERE username='{username}' AND disabled=false RETURNING session_id;")
    require(sid.isdigit(), "cannot create short-lived authorized session")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, payload=None, method=None):
        request = urllib.request.Request(API + path, method=method,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={"Cookie": "cta_session=" + token, "Content-Type": "application/json"})
        try:
            with opener.open(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Manager {path}: HTTP {error.code}; body suppressed") from None

    try:
        yield api
    finally:
        sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');")


def prepare(state, plan):
    assert_signals_stopped()
    token = publication_token()
    require(not any(row["strategy_name"] == MEAN for row in state["targets"]),
            "mean strategy already exists; inspect partial cutover before retrying")
    with session("lgq") as api:
        api("/catalog/position-strategies", {"strategy_name": MEAN, "targets": {}})
    with session("prc") as api:
        api("/catalog/position-strategies/" + MEAN + "/publish-token", {"publish_token": token}, "PUT")
        current = snapshot()
        scope_check(current, [row for row in plan["bindings_during_recovery"] if row["binding_name"] != MEAN])
        require(protected(current) == protected(state), "protected bindings changed during preparation")
        api("/catalog/virtual-accounts/" + VIRTUAL,
            {"name": state["virtual"]["name"], "bindings": plan["bindings_during_recovery"]}, "PUT")


def recovered_targets(state, plan):
    require(not (SIGNAL / "run/cta_perp_recovery_request.json").exists(), "recovery still requested")
    completed = json.loads((SIGNAL / "run/cta_perp_recovery_completed.json").read_text())
    progress = json.loads((SIGNAL / "data/cta_perp/factor_history/recovery.json").read_text())
    require(completed["generation"] == progress["generation"] and progress["caught_up"],
            "recovery generation differs or publisher is not caught up")
    require(0 <= time.time_ns() // 1_000_000 - progress["updated_ms"] < 5_000,
            "factor publisher heartbeat is stale")
    status = json.loads((SIGNAL / "run/cta_perp_status.json").read_text())
    require(status.get("recovering") is False and status.get("position_algorithm") == "v1"
            and status.get("enabled") is True, "RX02 recovery has not completed with v1 enabled")
    require(0 <= time.time_ns() // 1000 - status["updated_ts_us"] < 10_000_000, "RX02 status is stale")
    require(not any(status.get(key) for key in ("recovery_error", "configuration_error", "publication_error")),
            "RX02 signal reports an error")
    table = json.loads((STAGE / "rx02/cta_perp_symbol_factors.json").read_text())
    require(json.loads((SIGNAL / "config/cta_perp.json").read_text())
            == json.loads((STAGE / "rx02/cta_perp.json").read_text()), "RX02 settings differ from plan")
    require(status["symbol_factors"] == table, "RX02 symbol/factor table differs from plan")
    old = [row for row in plan["bindings_during_recovery"] if row["binding_name"] != MEAN]
    shadow = json.loads((SIGNAL / "run/cta_perp_position_flags.json").read_text())
    catalog = verify_retired_targets(state, old, shadow)
    targets = catalog.get(MEAN, {})
    require(set(targets) == set(table), "mean strategy does not cover every requested active symbol")
    for symbol, target in targets.items():
        qty = target["qty"]
        require(qty == "clear" or (type(qty) in (int, float) and math.isfinite(qty) and qty != 0),
                "mean target must be finite and use explicit clear for zero: " + symbol)
        observed = status["strategies"].get("symbol_mean", {}).get(symbol, {})
        require(observed.get("score_ready") is True
                and 0 <= time.time_ns() // 1_000_000 - observed.get("model_ts_ms", 0) < 130_000,
                "mean factor is not ready/current: " + symbol)
        require(same_quantity(0 if qty == "clear" else qty, observed.get("position_qty")),
                "catalog and local final target differ; retry after publication: " + symbol)
    verify_runtime_targets(MEAN, targets)
    return old


def retire(state, plan):
    old = recovered_targets(state, plan)
    with session("prc") as api:
        # Zero-share legacy bindings are excluded from auto-publish. Explicitly
        # republish them too so old residuals receive the clear command.
        for row in old:
            result = api("/catalog/accounts/" + SOURCE + "/bindings/" + row["binding_name"] + "/publish", {}, "POST")
            expected = next(item["targets"] for item in state["targets"]
                            if item["strategy_name"] == row["position_strategy_name"])
            require(result["source_id"] == SOURCE and result["strategy_name"] == row["binding_name"]
                    and result["nonzero_target_count"] == 0
                    and result["target_count"] == len(expected), "unexpected Manager publish result")
            verify_runtime_targets(row["binding_name"], expected, result)
        current = snapshot()
        scope_check(current, plan["bindings_during_recovery"])
        recovered_targets(current, plan)
        require(protected(current) == protected(state), "protected bindings changed before retirement")
        api("/catalog/virtual-accounts/" + VIRTUAL,
            {"name": state["virtual"]["name"], "bindings": plan["bindings_after_final_targets_verified"]}, "PUT")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "retire"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--manager-sha256", help="required for execution; verified clear-capable release SHA-256")
    args = parser.parse_args()
    require(os.uname().nodename.startswith("ip-172-31-35-228"), "not the reviewed JP host")
    plan = json.loads((STAGE / "manager_binding_plan.json").read_text())
    require(plan["source_id"] == SOURCE and plan["virtual_id"] == VIRTUAL
            and plan["new_strategy"] == {"strategy_name": MEAN, "targets": {}}, "unexpected migration plan")
    state = snapshot()
    expected = plan["bindings_during_recovery"]
    if args.action == "prepare":
        expected = [row for row in expected if row["binding_name"] != MEAN]
    scope_check(state, expected)
    if not args.execute:
        print(json.dumps({"dry_run": True, "action": args.action, "source_id": SOURCE,
                          "virtual_id": VIRTUAL, "old_strategy_count": 9,
                          "requires": "verified clear-capable Manager; stopped signals for prepare; completed recovery for retire"}))
        return
    binary = Path("/proc", manager_pid(), "exe").read_bytes()
    require(args.manager_sha256 and hashlib.sha256(binary).hexdigest() == args.manager_sha256
            and b"qty must be a number or clear" in binary, "verified clear-capable Manager release is required")
    backup = STAGE / ("manager_before_" + args.action + ".json")
    with backup.open("x") as out:
        json.dump(state, out, indent=2)
    (prepare if args.action == "prepare" else retire)(state, plan)
    after = snapshot()
    require(protected(after) == protected(state), "protected bindings changed; stop and inspect")
    desired = plan["bindings_during_recovery" if args.action == "prepare" else "bindings_after_final_targets_verified"]
    require(bindings([row for row in after["virtuals"] if row["virtual_id"] == VIRTUAL]) == bindings(desired)
            and bindings([row for row in after["bindings"] if row["source_id"] == SOURCE]) == bindings(desired),
            "saved bindings differ from reviewed migration")
    print(json.dumps({"executed": args.action, "source_id": SOURCE,
                      "pending_publishes": len(after["pending"]), "protected_bindings_unchanged": True}))


if __name__ == "__main__":
    main()
