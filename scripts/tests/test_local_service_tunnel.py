"""Configuration and channel isolation checks; no SSH connections are started."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("local_service_tunnel", ROOT / "scripts/local_service_tunnel.py")
tunnel = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tunnel)


class ServiceTunnel(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        (self.base / "config").mkdir()
        self.ssh = self.base / "private-ssh"
        self.ssh.write_text("Host peer\n")
        self.ssh.chmod(0o600)
        self.patch = patch.object(tunnel, "BASE", self.base)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def write(self, forwards):
        text = f'ssh_config = "{self.ssh}"\npeer_alias = "peer"\n'
        for name, host, remote, local in forwards:
            text += (f'\n[[forwards]]\nname = "{name}"\nlocal_host = "{host}"\n'
                     f'remote_port = {remote}\nlocal_port = {local}\n')
        (self.base / "config/tunnel.toml").write_text(text)

    def test_lan_zmq_and_local_grpc_get_independent_loopback_only_ssh_channels(self):
        self.write([("grpc", "127.0.0.1", 18312, 18312), ("zmq", "10.0.0.2", 13005, 13005)])
        cfg = tunnel.config()
        for name, expected in [("grpc", "127.0.0.1:18312:127.0.0.1:18312"),
                               ("zmq", "127.0.0.1:13005:10.0.0.2:13005")]:
            args = tunnel.ssh_args(cfg, tunnel.select(cfg, name)[0])
            self.assertEqual(args.count("-R"), 1)
            self.assertEqual(args[args.index("-R") + 1], expected)
            self.assertIn("ControlMaster=no", args)
            self.assertIn("ExitOnForwardFailure=yes", args)
        with self.assertRaises(RuntimeError):
            tunnel.select(cfg, "not-configured")

    def test_invalid_routes_duplicate_names_ports_and_public_targets_fail_before_ssh(self):
        for rows in [
            [("grpc", "127.0.0.1", 18312, 18312), ("grpc", "127.0.0.1", 18313, 18313)],
            [("grpc", "127.0.0.1", 18312, 18312), ("zmq", "10.0.0.2", 18312, 13005)],
            [("../grpc", "127.0.0.1", 18312, 18312)],
            [("grpc", "8.8.8.8", 18312, 18312)],
            [("grpc", "0.0.0.0", 18312, 18312)],
            [("grpc", "224.0.0.1", 18312, 18312)],
            [("grpc", "127.0.0.1", 22, 18312)],
        ]:
            with self.subTest(rows=rows):
                self.write(rows)
                with self.assertRaises(RuntimeError):
                    tunnel.config()


if __name__ == "__main__":
    unittest.main()
