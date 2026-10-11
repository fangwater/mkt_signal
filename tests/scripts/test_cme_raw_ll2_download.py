import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.parse import quote

spec = importlib.util.spec_from_file_location(
    'raw_download', Path(__file__).resolve().parents[2] / 'scripts/download_cme_raw_ll2.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class DownloadTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.payload = b'original gzip bytes\x00\x01\xff' * 100
        self.ranges = []
        self.request_paths = []
        self.ignore_range = False
        self.etag = '"fixed-source"'
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_HEAD(self):
                if self.headers.get('If-Match') != owner.etag:
                    self.send_error(412)
                    return
                self.send_response(200)
                self.send_header('ETag', owner.etag)
                self.send_header('Content-Length', str(len(owner.payload)))
                self.end_headers()

            def do_GET(self):
                owner.request_paths.append(self.path)
                if self.headers.get('If-Match') != owner.etag:
                    self.send_error(412)
                    return
                start = int(self.headers.get('Range', 'bytes=0-').split('=')[1].split('-')[0])
                owner.ranges.append(start)
                if owner.ignore_range:
                    start = 0
                self.send_response(206 if start else 200)
                self.send_header('ETag', owner.etag)
                self.send_header('Content-Length', str(len(owner.payload) - start))
                if start:
                    self.send_header('Content-Range', f'bytes {start}-{len(owner.payload)-1}/{len(owner.payload)}')
                self.end_headers()
                self.wfile.write(owner.payload[start:])

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.source = dict(name='merged-Data-part-000000.csv.gz',
                           url=f'http://127.0.0.1:{self.server.server_port}/file',
                           remote_bytes=len(self.payload), etag=self.etag)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def seed_partial(self, offset):
        name = self.source['name']
        (self.directory / (name + '.download.partial')).write_bytes(self.payload[:offset])
        module.atomic_json(self.directory / (name + '.download.json'),
                           dict(source=module.source_record(self.source), status='downloading'))

    def test_publish_original_bytes_and_reuse_without_redownload(self):
        d = module.Downloader(0)
        self.assertEqual(d.download(self.directory, self.source)['status'], 'downloaded')
        final = self.directory / self.source['name']
        self.assertEqual(final.read_bytes(), self.payload)
        self.assertEqual(final.stat().st_mode & 0o222, 0)
        self.assertEqual(d.download(self.directory, self.source)['status'], 'reused')
        self.assertEqual(self.ranges, [0])

    def test_resume_exact_same_source_and_preserve_prefix(self):
        self.seed_partial(117)
        module.Downloader(0).download(self.directory, self.source)
        self.assertEqual(self.ranges, [117])
        self.assertEqual((self.directory / self.source['name']).read_bytes(), self.payload)

    def test_complete_partial_is_published_after_metadata_head_without_body_reread(self):
        self.seed_partial(len(self.payload))
        module.Downloader(0).download(self.directory, self.source)
        self.assertEqual((self.directory / self.source['name']).read_bytes(), self.payload)
        self.assertEqual(self.ranges, [])

    def test_changed_source_and_ignored_range_never_publish(self):
        self.seed_partial(117)
        with self.assertRaises(ValueError):
            module.Downloader(0).download(self.directory, dict(self.source, etag='"changed"'))
        self.ignore_range = True
        with self.assertRaises(RuntimeError):
            module.Downloader(0).download(self.directory, self.source)
        self.assertFalse((self.directory / self.source['name']).exists())

    def test_inventory_notes_filename_with_spaces_uses_canonical_encoded_url(self):
        period = '2025-01-01_2026-01-01'
        directory = 'shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_' + period
        name = 'Shanghai Evolution Futures Raw Market Depth.csv.gz.notes.txt'
        remote = f'http://127.0.0.1:{self.server.server_port}/rawLL2'
        source = dict(self.source, name=name, url=f'{remote}/{directory}/{quote(name, safe="")}')
        inventory = self.directory / 'inventory.json'
        inventory.write_text(json.dumps(dict(remote_root=remote, packages=[
            dict(period=period, directory=directory, files=[source])
        ])))
        config = dict(data_root=str(self.directory / 'download'),
                      rocksdb_dir=str(self.directory / 'db'),
                      shard_root=str(self.directory / 'stage'),
                      inventory=str(inventory), remote_root=remote,
                      periods=[period], min_free_bytes=0)
        result = module.download_sources(config, 1, self.directory / 'audit.json')
        self.assertEqual(len(result['files']), 1)
        self.assertEqual((Path(config['data_root']) / directory / name).read_bytes(), self.payload)
        self.assertEqual(self.request_paths, [f'/rawLL2/{directory}/{quote(name, safe="")}'])
        # Encoding support must not weaken the exact configured-origin/path check.
        source['url'] = source['url'].replace('/rawLL2/', '/different-root/')
        inventory.write_text(json.dumps(dict(remote_root=remote, packages=[
            dict(period=period, directory=directory, files=[source])
        ])))
        with self.assertRaisesRegex(ValueError, 'unexpected Raw source filename or URL'):
            module.download_sources(config, 1, self.directory / 'audit.json')

    def test_untracked_existing_source_is_not_overwritten(self):
        final = self.directory / self.source['name']
        final.write_bytes(self.payload)
        with self.assertRaises(ValueError):
            module.Downloader(0).download(self.directory, self.source)
        self.assertEqual(final.read_bytes(), self.payload)
        self.assertEqual(self.ranges, [])


if __name__ == '__main__':
    unittest.main()
