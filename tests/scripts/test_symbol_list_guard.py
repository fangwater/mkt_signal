"""防误清库回归测试：symbol list 写入方不得把缺省字段/空内置列表清成 []。

事故背景：{env}:fr_unimmr_close_symbols:* 等运行时管理列表曾被
- config server 的 POST /api/symbol-lists（空 body / 缺字段 → 全部写成 []）
- sync_*_symbol_lists.py（内置列表无条件覆盖 Redis 值）
两条路径误清。

约定：
- config server POST：payload 缺省（未传/null）的字段跳过对应 key；
  全部缺省 → 400；显式传 [] 仍是合法清空。
- sync 脚本：内置列表为空 → 跳过写入，保留线上值（key 缺失等价空列表）。
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
CROSS_SCRIPTS = ROOT / "cross_scripts"
INTRA_SCRIPTS = ROOT / "intra_scripts"


def load_script(path: Path, name: str, extra_syspaths=()):
    for p in extra_syspaths:
        sys.path.insert(0, str(p))
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load {path}")
        module = importlib.util.module_from_spec(spec)
        # dataclass/typing 需要模块注册到 sys.modules
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        for p in extra_syspaths:
            sys.path.remove(str(p))


class FakeRedis:
    def __init__(self, initial=None) -> None:
        self.values: dict[str, str] = dict(initial or {})

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def get(self, key: str):
        return self.values.get(key)

    def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                removed += 1
        return removed


def make_post_handler(handler_cls, *, path: str, payload: dict, context):
    """构造一个无需 socket 的 handler：捕获 _send_json/_send_error 响应。"""
    handler = object.__new__(handler_cls)
    body = json.dumps(payload).encode("utf-8")
    handler.path = path
    handler.headers = {"Content-Length": str(len(body))}
    handler.rfile = io.BytesIO(body)
    handler.server = types.SimpleNamespace(context=context)
    handler.responses: list[tuple[int, dict]] = []
    handler._send_json = lambda status, data: handler.responses.append((status, data))
    handler._send_error = lambda status, msg: handler.responses.append(
        (status, {"error": msg})
    )
    return handler


# ---------- sync 脚本 ----------


class SyncFrSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(SCRIPTS / "sync_fr_symbol_lists.py", "sync_fr_symbol_lists")

    def test_empty_builtin_lists_do_not_clobber_existing_keys(self) -> None:
        mod = self.mod
        env = "binance_fr_test01"
        suffix = "binance-margin_binance-futures"
        dump_key = f"{env}:fr_dump_symbols:{suffix}"
        unimmr_key = f"{env}:fr_unimmr_close_symbols:{suffix}"
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                unimmr_key: json.dumps(["ETHUSDT"]),
            }
        )
        mod.sync_symbol_lists(rds, env, suffix, ["SOLUSDT"], ["XRPUSDT"])
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[unimmr_key]), ["ETHUSDT"])
        self.assertEqual(
            json.loads(rds.values[f"{env}:fr_fwd_trade_symbols:{suffix}"]),
            ["SOLUSDT"],
        )
        self.assertEqual(
            json.loads(rds.values[f"{env}:fr_bwd_trade_symbols:{suffix}"]),
            ["XRPUSDT"],
        )

    def test_empty_builtin_lists_do_not_create_keys(self) -> None:
        mod = self.mod
        env = "okex_fr_test"
        suffix = "okex-margin_okex-futures"
        rds = FakeRedis()
        mod.sync_symbol_lists(rds, env, suffix, ["SOLUSDT"], ["XRPUSDT"])
        self.assertNotIn(f"{env}:fr_dump_symbols:{suffix}", rds.values)
        self.assertNotIn(f"{env}:fr_unimmr_close_symbols:{suffix}", rds.values)

    def test_nonempty_builtin_list_still_writes(self) -> None:
        mod = self.mod
        rds = FakeRedis()
        written = mod.write_or_keep_symbol_list(
            rds, "k", ["BTCUSDT"], "测试"
        )
        self.assertEqual(written, 1)
        self.assertEqual(json.loads(rds.values["k"]), ["BTCUSDT"])


class SyncCrossSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(
            CROSS_SCRIPTS / "sync_cross_symbol_lists.py", "sync_cross_symbol_lists"
        )

    def test_empty_builtin_lists_do_not_clobber_existing_keys(self) -> None:
        mod = self.mod
        env = "okex-binance-cross-test"
        suffix = "okex-binance"
        dump_key = f"cross_dump_symbols:{suffix}"
        unimmr_key = mod.unimmr_close_key(env, "okex-futures", "binance-futures")
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                unimmr_key: json.dumps(["ETHUSDT"]),
            }
        )
        mod.sync_symbol_lists(rds, suffix, env, "okex-futures", "binance-futures")
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[unimmr_key]), ["ETHUSDT"])
        self.assertIn(f"cross_fwd_trade_symbols:{suffix}", rds.values)
        self.assertIn(f"cross_bwd_trade_symbols:{suffix}", rds.values)


class SyncIntraSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(
            INTRA_SCRIPTS / "sync_intra_symbol_lists.py", "sync_intra_symbol_lists"
        )

    def test_empty_builtin_lists_do_not_clobber_existing_keys(self) -> None:
        mod = self.mod
        env = "binance-intra-test01"
        dump_key = f"{env}:intra_dump_symbols:binance"
        vol_key = f"{env}:intra_vol_gate_symbols:binance"
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                vol_key: json.dumps(["ETHUSDT"]),
            }
        )
        mod.sync_symbol_lists(
            rds, "binance", env, "binance-margin", "binance-futures", "intra"
        )
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[vol_key]), ["ETHUSDT"])
        self.assertIn(f"{env}:intra_fwd_trade_symbols:binance", rds.values)
        self.assertIn(f"{env}:intra_bwd_trade_symbols:binance", rds.values)

    def test_cta_branch_preserves_dump_and_cleans_stale(self) -> None:
        mod = self.mod
        env = "binance-cta-test01"
        dump_key = f"{env}:cta_dump_symbols:binance"
        stale_key = f"{env}:cta_fwd_trade_symbols:binance"
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                stale_key: json.dumps(["STALEUSDT"]),
            }
        )
        mod.sync_symbol_lists(
            rds, "binance", env, "binance-margin", "binance-futures", "cta"
        )
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertNotIn(stale_key, rds.values)
        self.assertIn(f"{env}:cta_trade_symbols:binance", rds.values)
        # pre_trade 借贷白名单镜像仍写入
        self.assertIn(f"{env}:intra_bwd_trade_symbols:binance", rds.values)


# ---------- config server POST /api/symbol-lists ----------


class FrConfigServerSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(SCRIPTS / "fr_config_server.py", "fr_config_server")

    def _context(self, rds):
        return types.SimpleNamespace(redis_client=rds, default_exchange="binance")

    def _post(self, payload, rds):
        handler = make_post_handler(
            self.mod.RequestHandler,
            path="/api/symbol-lists",
            payload=payload,
            context=self._context(rds),
        )
        handler.do_POST()
        return handler.responses[-1]

    def test_empty_payload_returns_400_and_writes_nothing(self) -> None:
        rds = FakeRedis()
        status, body = self._post({}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})

    def test_missing_fields_skip_their_keys(self) -> None:
        mod = self.mod
        suffix = "binance-margin_binance-futures"
        dump_key = mod.build_symbol_list_key("dump_symbols", suffix)
        unimmr_key = mod.build_symbol_list_key("unimmr_close_symbols", suffix)
        fwd_key = mod.build_symbol_list_key("fwd_trade_symbols", suffix)
        bwd_key = mod.build_symbol_list_key("bwd_trade_symbols", suffix)
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                unimmr_key: json.dumps(["ETHUSDT"]),
                fwd_key: json.dumps(["SOLUSDT"]),
                bwd_key: json.dumps(["XRPUSDT"]),
            }
        )
        status, body = self._post(
            {"unimmr_close_symbols": ["DOGEUSDT"]}, rds
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rds.values[unimmr_key]), ["DOGEUSDT"])
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[fwd_key]), ["SOLUSDT"])
        self.assertEqual(json.loads(rds.values[bwd_key]), ["XRPUSDT"])
        self.assertEqual(body["unimmr_close_count"], 1)
        self.assertNotIn("dump_count", body)
        self.assertEqual(
            sorted(body["skipped_fields"]),
            ["bwd_trade_symbols", "dump_symbols", "fwd_trade_symbols"],
        )

    def test_explicit_empty_list_still_clears(self) -> None:
        mod = self.mod
        suffix = "binance-margin_binance-futures"
        unimmr_key = mod.build_symbol_list_key("unimmr_close_symbols", suffix)
        rds = FakeRedis({unimmr_key: json.dumps(["ETHUSDT"])})
        status, _ = self._post({"unimmr_close_symbols": []}, rds)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rds.values[unimmr_key]), [])

    def test_invalid_field_type_returns_400(self) -> None:
        rds = FakeRedis()
        status, _ = self._post({"dump_symbols": 123}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})


class CrossConfigServerSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(
            SCRIPTS / "cross_config_server.py", "cross_config_server",
            extra_syspaths=[CROSS_SCRIPTS],
        )

    def _context(self, rds):
        return types.SimpleNamespace(
            redis_client=rds,
            default_exchange="okex",
            default_open_venue="okex-futures",
            default_hedge_venue="binance-futures",
        )

    def _post(self, payload, rds):
        handler = make_post_handler(
            self.mod.RequestHandler,
            path="/api/symbol-lists",
            payload=payload,
            context=self._context(rds),
        )
        handler.do_POST()
        return handler.responses[-1]

    def test_empty_payload_returns_400_and_writes_nothing(self) -> None:
        rds = FakeRedis()
        status, _ = self._post({}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})

    def test_missing_fields_skip_their_keys(self) -> None:
        dump_key = "cross_dump_symbols:okex-binance"
        unimmr_key = self.mod.build_unimmr_close_symbol_list_key(
            "okex-futures", "binance-futures"
        )
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                unimmr_key: json.dumps(["ETHUSDT"]),
            }
        )
        status, body = self._post({"fwd_trade_symbols": ["SOLUSDT"]}, rds)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[unimmr_key]), ["ETHUSDT"])
        self.assertEqual(
            json.loads(rds.values["cross_fwd_trade_symbols:okex-binance"]),
            ["SOLUSDT"],
        )
        self.assertEqual(body["fwd_count"], 1)


class IntraConfigServerSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(
            SCRIPTS / "intra_config_server.py", "intra_config_server",
            extra_syspaths=[INTRA_SCRIPTS],
        )

    def _context(self, rds):
        return types.SimpleNamespace(
            redis_client=rds,
            default_exchange="binance",
            default_open_venue="binance-margin",
            default_hedge_venue="binance-futures",
        )

    def _post(self, payload, rds):
        handler = make_post_handler(
            self.mod.RequestHandler,
            path="/api/symbol-lists",
            payload=payload,
            context=self._context(rds),
        )
        handler.do_POST()
        return handler.responses[-1]

    def test_empty_payload_returns_400_and_writes_nothing(self) -> None:
        rds = FakeRedis()
        status, _ = self._post({}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})

    def test_missing_fields_skip_their_keys(self) -> None:
        mod = self.mod
        env = mod.current_env_name()
        ns = mod.current_namespace()
        dump_key = mod.intra_symbol_list_key(env, "dump_symbols", "binance", ns)
        vol_key = mod.intra_symbol_list_key(env, "vol_gate_symbols", "binance", ns)
        fwd_key = mod.intra_symbol_list_key(env, "fwd_trade_symbols", "binance", ns)
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                vol_key: json.dumps(["ETHUSDT"]),
                fwd_key: json.dumps(["SOLUSDT"]),
            }
        )
        status, body = self._post({"vol_gate_symbols": ["XRPUSDT"]}, rds)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rds.values[vol_key]), ["XRPUSDT"])
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertEqual(json.loads(rds.values[fwd_key]), ["SOLUSDT"])
        self.assertEqual(body["vol_gate_count"], 1)


class CtaConfigServerSymbolListsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(
            SCRIPTS / "cta_config_server.py", "cta_config_server",
            extra_syspaths=[SCRIPTS, INTRA_SCRIPTS],
        )

    def _context(self, rds):
        return types.SimpleNamespace(
            redis_client=rds,
            default_open_venue="binance-margin",
            default_hedge_venue="binance-futures",
        )

    def _post(self, payload, rds):
        handler = make_post_handler(
            self.mod.RequestHandler,
            path="/api/symbol-lists",
            payload=payload,
            context=self._context(rds),
        )
        handler.do_POST()
        return handler.responses[-1]

    def test_empty_payload_returns_400_and_writes_nothing(self) -> None:
        rds = FakeRedis()
        status, _ = self._post({}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})

    def test_missing_dump_preserves_existing_and_cleans_stale(self) -> None:
        mod = self.mod
        env = mod.current_env_name()
        suffix = "binance"
        dump_key = mod.symbol_list_key(env, "dump_symbols", suffix)
        trade_key = mod.symbol_list_key(env, "trade_symbols", suffix)
        stale_key = mod.symbol_list_key(env, "fwd_trade_symbols", suffix)
        rds = FakeRedis(
            {
                dump_key: json.dumps(["BTCUSDT"]),
                stale_key: json.dumps(["STALEUSDT"]),
            }
        )
        status, body = self._post({"trade_symbols": ["ETHUSDT"]}, rds)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(rds.values[trade_key]), ["ETHUSDT"])
        self.assertEqual(json.loads(rds.values[dump_key]), ["BTCUSDT"])
        self.assertNotIn(stale_key, rds.values)
        self.assertEqual(body["trade_count"], 1)
        self.assertEqual(body["skipped_fields"], ["dump_symbols"])

    def test_rejected_fields_still_rejected(self) -> None:
        rds = FakeRedis()
        status, _ = self._post({"fwd_trade_symbols": ["BTCUSDT"]}, rds)
        self.assertEqual(status, 400)
        self.assertEqual(rds.values, {})


class MmConfigServerSymbolListTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_script(SCRIPTS / "mm_config_server.py", "mm_config_server")

    def test_missing_symbols_field_returns_400_without_writing(self) -> None:
        mod = self.mod
        config = mod.AppConfig(
            host="127.0.0.1", port=0, default_exchange="binance", env_name="test_mm"
        )
        handler_cls = mod.build_handler(config)
        handler = make_post_handler(
            handler_cls,
            path="/api/symbol-list",
            payload={},
            context=None,
        )
        handler.do_POST()
        status, body = handler.responses[-1]
        self.assertEqual(status, 400)
        self.assertFalse(body.get("ok", True))


if __name__ == "__main__":
    unittest.main()
