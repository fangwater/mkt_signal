"""Replacement validation must not depend on old caches or stop a live worker."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "cta_cn_process", Path(__file__).resolve().parents[1] / "cta_cn_futures_process.py")
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.data = self.base / "data"
        self.data.mkdir()
        self.ledger = b'{"last_decision_ms":123,"flags":{"baseline_078":1}}'
        (self.data / "positions.json").write_bytes(self.ledger)
        (self.data / "positions_1min.json").write_bytes(self.ledger)
        (self.data / "history").mkdir()
        (self.data / "warmup.json").write_text("obsolete and invalid")
        self.binary = self.base / "candidate"
        self.binary.write_bytes(b"candidate binary")
        self.cfg = dict(name="cta-cn_futures-01", exchange="SHFE", symbol="SN", data_directory="data")
        self.receipt = dict(positions_preserved=True, ready_rules=12, total_rules=12)
        self.base_patch = patch.object(WORKER, "BASE", self.base)
        self.base_patch.start()
        self.addCleanup(self.base_patch.stop)

    def test_network_preflight_copies_only_the_factual_ledger(self):
        def run(args, **kwargs):
            stage = Path(args[2]).parent
            self.assertEqual(args[0], self.binary)
            self.assertEqual(args[-1], "--preflight")
            self.assertEqual({p.name for p in stage.iterdir()}, {"positions.json", "positions_1min.json", "config.json"})
            self.assertEqual((stage / "positions.json").read_bytes(), self.ledger)
            return subprocess.CompletedProcess(args, 0, json.dumps(self.receipt))
        with patch.object(WORKER, "run", side_effect=run):
            WORKER.preflight(self.cfg, self.binary)
        self.assertEqual((self.data / "positions.json").read_bytes(), self.ledger)
        self.assertTrue((self.data / "preflight.json").is_file())

    def test_unready_candidate_has_no_success_receipt(self):
        self.receipt["ready_rules"] = 11
        with patch.object(WORKER, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(self.receipt))):
            with self.assertRaises(RuntimeError):
                WORKER.preflight(self.cfg, self.binary)
        self.assertFalse((self.data / "preflight.json").exists())
        self.assertEqual((self.data / "positions.json").read_bytes(), self.ledger)

    def test_failed_restart_preflight_never_stops_the_old_process(self):
        cfg_path = self.base / "config.json"
        cfg_path.write_text(json.dumps(self.cfg))
        rows = [dict(name="cta-cn_futures-01", pid=123, pm2_env=dict(pm_cwd=str(self.base), status="online"))]
        with patch.object(WORKER, "CONFIG", cfg_path), patch.object(WORKER, "processes", return_value=rows), \
                patch.object(WORKER, "preflight", side_effect=RuntimeError("gRPC method unavailable")), \
                patch.object(WORKER, "run") as run, patch("sys.argv", ["process.py", "restart", "--execute"]):
            with self.assertRaisesRegex(RuntimeError, "gRPC method unavailable"):
                WORKER.main()
            run.assert_not_called()
        self.assertEqual((self.data / "positions.json").read_bytes(), self.ledger)

    def test_instance_names_are_independent_and_cannot_cross_environment_directories(self):
        cfg = dict(self.cfg, name="cta-cn_futures-02")
        self.assertEqual(WORKER.environment_name(cfg), "cta-cn_futures-02")
        with patch.object(WORKER, "BASE", self.base / "cta-cn_futures-01"):
            with self.assertRaisesRegex(RuntimeError, "another native CTA environment"):
                WORKER.environment_name(cfg)

    def test_backup_keeps_instance_name_for_stop_and_restart(self):
        cfg_path = self.base / "config.json"
        cfg_path.write_text(json.dumps(dict(self.cfg, name="cta-cn_futures-02")))
        (self.base / "cta-cn-futures").write_bytes(b"candidate binary")
        outbox = b'{"pending":{"batch_id":"unconfirmed"}}'
        (self.data / "manager_publication.json").write_bytes(outbox)
        log_path = self.base / "worker.log"
        log_path.write_text("preserved runtime log")
        rows = [dict(name="cta-cn_futures-02", pid=123, pm2_env=dict(
            pm_cwd=str(self.base), status="online", pm_out_log_path=str(log_path),
            pm_err_log_path=str(log_path)))]
        for action in ("stop", "restart"):
            with self.subTest(action=action), patch.object(WORKER, "CONFIG", cfg_path), \
                    patch.object(WORKER, "processes", return_value=rows), \
                    patch.object(WORKER, "preflight"), patch.object(WORKER, "run") as run, \
                    patch("sys.argv", ["process.py", action, "--execute"]):
                WORKER.main()
                command = run.call_args_list[0].args[0]
                self.assertEqual(command[-1], "cta-cn_futures-02")
                if action == "restart":
                    ecology = json.loads((self.base / "config/ecosystem.json").read_text())
                    self.assertEqual(ecology["apps"][0]["name"], "cta-cn_futures-02")
        backups = list((self.data / "stops").iterdir())
        self.assertTrue(backups)
        for backup in backups:
            self.assertEqual((backup / "manager_publication.json").read_bytes(), outbox)
            self.assertEqual((backup / "positions.json").read_bytes(), self.ledger)


if __name__ == "__main__":
    unittest.main()
