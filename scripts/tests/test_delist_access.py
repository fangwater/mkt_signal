"""Guard coverage must survive new routes and repeated renders."""
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from protect_delist_routes import ACCESS, FR_ENVS, GATE, protect


class DelistRoutes(unittest.TestCase):
    def site(self):
        blocks = ['server {', 'location /manager/ { proxy_pass http://127.0.0.1:18201/; }']
        for env in FR_ENVS:
            blocks.append(f'location = /fr/{env} {{ return 301 /fr/{env}/; }}')
            for path in ['config', 'config/', '', 'ws', 'healthz', 'snapshot', 'fr_ws']:
                blocks.append(f'location /fr/{env}/{path} {{ proxy_pass http://127.0.0.1:20000/; }}')
        return '\n'.join(blocks + ['}'])

    def test_every_surface_and_future_config_subroute_is_guarded(self):
        source = self.site().replace('\n}', '\nlocation /fr/gate_fr_arb01/config/api/new { proxy_pass http://127.0.0.1:20021/; }\n}')
        result = protect(source, FR_ENVS)
        self.assertEqual(result.count(GATE), 64)
        self.assertEqual(result.count(ACCESS), 1)
        self.assertEqual(protect(result, FR_ENVS), result)
        self.assertIn('location /manager/ { proxy_pass http://127.0.0.1:18201/; }', result)

    def test_missing_environment_or_snapshot_refuses_partial_patch(self):
        source = self.site().replace('location /fr/gate_fr_arb01/snapshot', 'location /retired/snapshot')
        with self.assertRaisesRegex(ValueError, 'gate_fr_arb01: missing routes'):
            protect(source, FR_ENVS)

    def test_other_environment_keeps_its_existing_policy(self):
        old = 'location /fr/binance_fr_arb01/config { proxy_pass http://127.0.0.1:20031/; }'
        source = self.site().replace('\n}', '\n' + old + '\n}')
        self.assertIn(old, protect(source, FR_ENVS))


if __name__ == '__main__':
    unittest.main()
