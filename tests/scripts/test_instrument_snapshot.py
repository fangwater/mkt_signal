from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import instrument_snapshot as snapshot  # noqa: E402
import instrument_snapshot_sync as sync  # noqa: E402


CAPTURED_AT = "2026-08-14T01:02:03.123456Z"


def source(source_id: str) -> snapshot.SourceSpec:
    return next(item for item in snapshot.SOURCES if item.source_id == source_id)


def raw_response(
    source_id: str,
    payload: object,
    *,
    page: int = 1,
    market_types: tuple[str, ...] | None = None,
) -> snapshot.RawResponse:
    spec = source(source_id)
    body = snapshot.compact_json(payload).encode("utf-8")
    return snapshot.RawResponse(
        source=spec,
        page=page,
        market_types=market_types or spec.market_types,
        request_url=spec.url,
        request_headers={"Accept": "application/json"},
        response_headers={"Content-Type": "application/json"},
        http_status=200,
        fetched_at=CAPTURED_AT,
        body=body,
        payload=payload,
    )


class HeaderFilteringTests(unittest.TestCase):
    def test_sensitive_response_headers_are_not_archived(self) -> None:
        filtered = sync.safe_response_headers(
            {"Content-Type": "application/json", "Set-Cookie": "secret", "authorization": "secret"}
        )
        self.assertEqual(filtered, {"Content-Type": "application/json"})



class DecimalEncodingTests(unittest.TestCase):
    def test_non_power_of_ten_tick_preserves_raw_and_integer_parts(self) -> None:
        fields = snapshot.decimal_fields("0.2500", positive=True, integer_parts=True)
        self.assertEqual(fields["raw"], "0.2500")
        self.assertEqual(fields["value"], "0.25")
        self.assertEqual(fields["integer"], "25")
        self.assertEqual(fields["scale"], 2)

    def test_integer_lot_has_zero_scale(self) -> None:
        fields = snapshot.decimal_fields("5", positive=True, integer_parts=True)
        self.assertEqual(fields["raw"], "5")
        self.assertEqual(fields["value"], "5")
        self.assertEqual(fields["integer"], "5")
        self.assertEqual(fields["scale"], 0)


class ExchangeNormalizationTests(unittest.TestCase):
    def test_binance_limit_and_market_filters(self) -> None:
        response = raw_response(
            "binance_futures",
            {
                "symbols": [
                    {
                        "symbol": "ODDUSDT",
                        "baseAsset": "ODD",
                        "quoteAsset": "USDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                        "filters": [
                            {
                                "filterType": "PRICE_FILTER",
                                "tickSize": "0.2500",
                            },
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "5",
                                "maxQty": "1000",
                                "stepSize": "5",
                            },
                            {
                                "filterType": "MARKET_LOT_SIZE",
                                "minQty": "10",
                                "maxQty": "500",
                                "stepSize": "5",
                            },
                            {
                                "filterType": "MIN_NOTIONAL",
                                "notional": "20.00",
                            },
                        ],
                    }
                ]
            },
            market_types=("futures",),
        )
        row = snapshot.normalize_binance(
            response.source, [response], ("futures",)
        )[0]
        self.assertEqual(row["price_tick_raw"], "0.2500")
        self.assertEqual(row["price_tick_integer"], "25")
        self.assertEqual(row["qty_step_integer"], "5")
        self.assertEqual(row["market_max_qty"], "500")
        self.assertEqual(row["min_notional_raw"], "20.00")

    def test_okx_contract_multiplier_is_exact_product(self) -> None:
        response = raw_response(
            "okx_swap",
            {
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": "ODD-USDT-SWAP",
                        "instType": "SWAP",
                        "ctType": "linear",
                        "settleCcy": "USDT",
                        "state": "live",
                        "tickSz": "0.25",
                        "lotSz": "5",
                        "minSz": "5",
                        "maxLmtSz": "1000",
                        "maxMktSz": "500",
                        "ctVal": "0.1250",
                        "ctMult": "2",
                        "ctValCcy": "ODD",
                    }
                ],
            },
            market_types=("futures",),
        )
        row = snapshot.normalize_okx(response.source, [response], ("futures",))[0]
        self.assertEqual(row["contract_multiplier"], "0.25")
        self.assertEqual(
            row["contract_multiplier_components"]["left_raw"], "0.1250"
        )
        self.assertEqual(row["qty_step_integer"], "5")

    def test_bybit_preserves_separate_market_max_qty(self) -> None:
        response = raw_response(
            "bybit_linear",
            {
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "nextPageCursor": "",
                    "list": [
                        {
                            "symbol": "ODDUSDT",
                            "status": "Trading",
                            "baseCoin": "ODD",
                            "quoteCoin": "USDT",
                            "contractType": "LinearPerpetual",
                            "priceFilter": {"tickSize": "0.25"},
                            "lotSizeFilter": {
                                "qtyStep": "5",
                                "minOrderQty": "5",
                                "maxOrderQty": "1000",
                                "maxMktOrderQty": "500",
                                "minNotionalValue": "10",
                            },
                        }
                    ],
                },
            },
            market_types=("futures",),
        )
        row = snapshot.normalize_bybit(
            response.source, [response], ("futures",)
        )[0]
        self.assertEqual(row["max_qty"], "1000")
        self.assertEqual(row["market_max_qty"], "500")
        self.assertEqual(row["min_notional"], "10")

    def test_gate_futures_contract_fields(self) -> None:
        response = raw_response(
            "gate_futures",
            [
                {
                    "name": "ODD_USDT",
                    "status": "trading",
                    "order_price_round": "0.25",
                    "order_size_min": "5",
                    "order_size_step": "5",
                    "order_size_max": "1000",
                    "market_order_size_max": "500",
                    "quanto_multiplier": "0.1250",
                }
            ],
            market_types=("futures",),
        )
        row = snapshot.normalize_gate_futures(
            response.source, [response], ("futures",)
        )[0]
        self.assertEqual(row["contract_multiplier_raw"], "0.1250")
        self.assertEqual(row["market_max_qty"], "500")
        self.assertEqual(row["qty_step_source"], "exchange")

    def test_bitget_prefers_explicit_multipliers_over_precision(self) -> None:
        response = raw_response(
            "bitget_futures",
            {
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "symbol": "ODDUSDT",
                        "baseCoin": "ODD",
                        "quoteCoin": "USDT",
                        "status": "normal",
                        "priceMultiplier": "0.25",
                        "pricePrecision": "2",
                        "quantityMultiplier": "5",
                        "quantityPrecision": "0",
                        "minOrderQty": "5",
                        "maxOrderQty": "1000",
                        "maxMarketOrderQty": "500",
                        "minOrderAmount": "10",
                    }
                ],
            },
            market_types=("futures",),
        )
        row = snapshot.normalize_bitget(
            response.source, [response], ("futures",)
        )[0]
        self.assertEqual(row["price_tick"], "0.25")
        self.assertEqual(row["qty_step"], "5")
        self.assertEqual(row["price_tick_source"], "exchange")
        self.assertEqual(row["qty_step_source"], "exchange")


class SnapshotArchiveTests(unittest.TestCase):
    def test_zero_row_scope_is_retained(self) -> None:
        scopes = sync.scope_summaries(
            [], CAPTURED_AT, exchanges=("okx",), market_types=("margin",)
        )
        self.assertEqual(
            scopes,
            [
                {
                    "exchange": "okx",
                    "market_type": "margin",
                    "effective_from": CAPTURED_AT,
                    "instrument_count": 0,
                }
            ],
        )

    def test_archive_round_trip_checks_manifest_and_raw_hashes(self) -> None:
        response = raw_response(
            "binance_futures",
            {
                "symbols": [
                    {
                        "symbol": "ODDUSDT",
                        "baseAsset": "ODD",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.25"},
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "5",
                                "maxQty": "1000",
                                "stepSize": "5",
                            },
                        ],
                    }
                ]
            },
            market_types=("futures",),
        )
        snapshot_id = str(uuid.uuid4())
        rules = snapshot.finalize_rules(
            snapshot.normalize_binance(
                response.source, [response], ("futures",)
            ),
            snapshot_id=snapshot_id,
            captured_at=CAPTURED_AT,
            effective_from=CAPTURED_AT,
        )
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            source_root = temporary_path / f"fixture_{snapshot_id}"
            source_root.mkdir()
            sync.build_snapshot_tree(
                source_root,
                snapshot_id=snapshot_id,
                captured_at=CAPTURED_AT,
                completed_at=CAPTURED_AT,
                effective_from=CAPTURED_AT,
                exchanges=("binance",),
                market_types=("futures",),
                responses=[response],
                rules=rules,
                script_sha256="a" * 64,
            )
            archive = temporary_path / "snapshot.tar.gz"
            sync.create_archive(source_root, archive)
            extracted, manifest = sync.extract_snapshot_archive(
                archive, temporary_path / "extracted"
            )
            self.assertEqual(manifest["snapshot_id"], snapshot_id)
            self.assertEqual(manifest["instrument_count"], 1)
            self.assertTrue((extracted / "raw/binance_futures/page-001.json").is_file())


class LoaderAndCliTests(unittest.TestCase):
    def test_database_env_requires_private_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "database.env"
            path.write_text(
                "CRYPTO_CTA_DATABASE_URL=postgresql://user:p%40ss@127.0.0.1:5432/db\n",
                encoding="utf-8",
            )
            path.chmod(0o600)
            values = sync.read_env_file(path)
            pg_env = sync.postgres_process_env(
                values["CRYPTO_CTA_DATABASE_URL"]
            )
            self.assertEqual(pg_env["PGUSER"], "user")
            self.assertEqual(pg_env["PGPASSWORD"], "p@ss")
            self.assertEqual(pg_env["PGDATABASE"], "db")
            path.chmod(0o644)
            with self.assertRaises(snapshot.SnapshotError):
                sync.read_env_file(path)

    def test_cli_filter_replaces_defaults_and_remains_dry_run(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS / "instrument_snapshot_sync.py"),
                "sync",
                "--exchanges",
                "binance",
                "--market-types",
                "futures",
            ],
            cwd=ROOT,
            env=os.environ.copy(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)["dry_run"]
        self.assertEqual(plan["exchanges"], ["binance"])
        self.assertEqual(plan["market_types"], ["futures"])
        self.assertEqual(
            [item["source_id"] for item in plan["sources"]],
            ["binance_futures"],
        )


if __name__ == "__main__":
    unittest.main()
