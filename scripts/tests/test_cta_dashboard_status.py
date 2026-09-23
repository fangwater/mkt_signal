import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen


ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import cta_config_server


class CtaDashboardStatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.status_path = pathlib.Path(self.temp.name) / "cta_signal_status.json"
        patcher = patch.object(cta_config_server, "CTA_SIGNAL_STATUS_PATH", self.status_path)
        patcher.start()
        self.addCleanup(patcher.stop)
        server = ThreadingHTTPServer(("127.0.0.1", 0), cta_config_server.RequestHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.url = f"http://127.0.0.1:{server.server_port}/api/signal-status"

    def get(self):
        with urlopen(self.url, timeout=2) as response:
            return json.load(response)

    def test_missing_and_fresh_status(self):
        self.assertEqual(self.get()["state"], "offline")
        status = {
            "updated_ts_us": time.time_ns() // 1000,
            "rules": {"sample": {"BTCUSDT": {"decision": "long"}}},
        }
        self.status_path.write_text(json.dumps(status), encoding="utf-8")
        response = self.get()
        self.assertEqual(response["state"], "online")
        self.assertEqual(response["rules"]["sample"]["BTCUSDT"]["decision"], "long")

    def test_stale_and_invalid_status(self):
        status = {"updated_ts_us": (time.time_ns() // 1000) - 91_000_000, "rules": {}}
        self.status_path.write_text(json.dumps(status), encoding="utf-8")
        self.assertEqual(self.get()["state"], "stale")
        self.status_path.write_text("not json", encoding="utf-8")
        with self.assertRaises(HTTPError) as error:
            self.get()
        self.assertEqual(error.exception.code, 503)
        error.exception.close()


if __name__ == "__main__":
    unittest.main()
