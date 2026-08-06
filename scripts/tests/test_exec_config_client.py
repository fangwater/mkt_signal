import importlib.util
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "exec_config_client.py"
SPEC = importlib.util.spec_from_file_location("exec_config_client", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ExecConfigClientTests(unittest.TestCase):
    def test_api_url_preserves_reverse_proxy_prefix(self):
        self.assertEqual(
            MODULE.api_url(
                "http://172.16.30.42:10041/config/", "strategy?name=alpha"
            ),
            "http://172.16.30.42:10041/config/api/strategy?name=alpha",
        )

    def test_get_path_supports_list_and_encoded_strategy(self):
        self.assertEqual(MODULE.get_api_path(None), "strategies")
        self.assertEqual(
            MODULE.get_api_path("alpha beta"), "strategy?name=alpha%20beta"
        )

    def test_loads_inline_json(self):
        self.assertEqual(
            MODULE.load_json_source('{"strategy_name":"alpha","config":{}}'),
            {"strategy_name": "alpha", "config": {}},
        )

    def test_loads_at_file_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "config.json"
            path.write_text('{"strategy_name":"alpha","config":{}}', encoding="utf-8")
            self.assertEqual(
                MODULE.load_json_source(f"@{path}"),
                {"strategy_name": "alpha", "config": {}},
            )


if __name__ == "__main__":
    unittest.main()
