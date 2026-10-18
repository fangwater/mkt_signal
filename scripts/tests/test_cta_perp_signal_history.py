import gzip
import io
import json
import pathlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from cta_perp_signal_history import (  # noqa: E402
    BinanceMinuteClient, DAY_MS, MINUTE_MS, SignalHistoryStore, collect_symbol,
    configured_targets, normalize_kline,
)
from cta_perp_dashboard import make_handler  # noqa: E402


def kline(ts, close=101):
    return [ts, "100", "103", "99", str(close), "20", ts + MINUTE_MS - 1,
            "2010", 8, "9", "910", "0"]


def record(bar, state="published", event="published", signal="signal", coefficient=1/3):
    return {"event_id":event, "signal_id":signal, "symbol":"ETHUSDT", "model_ts_ms":bar,
            "event_ts_us":(bar+1000)*1000, "state":state, "reason":"allowed",
            "snapshot":{"signal":{"score":coefficient,"execution_coefficient":abs(coefficient),
                         "quantile":0.66,"factor_votes":[{"factor":"factor_036","sign":"+",
                         "weight":2,"normalized_weight":2/3,"score":1.2,"corrected_score":1.2,
                         "quantile":0.95,"corrected_quantile":0.95,"vote":1}]},
                        "symbol_factors":{"factors":["factor_036"],"weights":[2],"signs":["+"]}},
            "publication":{"levels":[{"price":100,"qty":0.333,"notional":33.3,
                                         "level_index":1,"offset":0,"published_ts_us":(bar+1000)*1000}]
                                         if state in {"published","partial"} else [], "error":None}}


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.path = self.root / "history.sqlite3"
        self.store = SignalHistoryStore(self.path)
        self.addCleanup(lambda: self.store.close())
        self.now = 1_800_000_000_000 + 20_000
        self.bucket = self.now // MINUTE_MS * MINUTE_MS
        self.journal = self.root / "journal"
        self.journal.mkdir()
        self.file = self.journal / "20270115.jsonl"

    def append(self, item):
        with self.file.open("a") as stream:
            stream.write(json.dumps(item) + "\n")

    def test_bootstrap_pages_full_day_then_refreshes_two_recent_buckets(self):
        class Client:
            calls = []
            close = 101
            def fetch(client, symbol, start, end):
                client.calls.append((symbol,start,end))
                return [kline(ts,client.close) for ts in range(start,min(end//MINUTE_MS*MINUTE_MS,
                                start+498*MINUTE_MS)+1,MINUTE_MS)]
        client = Client()
        collect_symbol(self.store,client,"ETHUSDT",self.now)
        series = self.store.series("ETHUSDT",current_ms=self.now)
        self.assertEqual(len(client.calls),3)
        self.assertEqual(series["closed_bars"],1440)
        self.assertEqual(len(series["klines"]),1441)
        self.assertEqual(series["missing_minutes"],[])
        self.assertFalse(series["klines"][-1]["closed"])
        client.close = 102
        collect_symbol(self.store,client,"ETHUSDT",self.now+1000)
        self.assertEqual(len(client.calls),4)
        self.assertEqual(client.calls[-1][1],self.bucket-MINUTE_MS)
        self.assertEqual(self.store.series("ETHUSDT",current_ms=self.now+1000)["klines"][-1]["close"],102)

    def test_gap_stays_visible_and_is_retried_without_inventing_candle(self):
        missing = self.bucket - 8*MINUTE_MS
        rows = [normalize_kline(kline(ts),self.now) for ts in range(self.bucket-DAY_MS,self.bucket+1,MINUTE_MS) if ts != missing]
        self.store.save_klines("ETHUSDT",rows,self.now)
        series = self.store.series("ETHUSDT",current_ms=self.now)
        self.assertEqual(series["missing_minutes"],[missing])
        self.assertEqual(self.store.missing_ranges("ETHUSDT",self.bucket-DAY_MS,self.bucket),
                         [(missing,missing),(self.bucket-MINUTE_MS,self.bucket)])
        class Client:
            def fetch(client,symbol,start,end):
                return [kline(ts) for ts in range(start,end//MINUTE_MS*MINUTE_MS+1,MINUTE_MS)]
        collect_symbol(self.store,Client(),"ETHUSDT",self.now)
        self.assertEqual(self.store.series("ETHUSDT",current_ms=self.now)["missing_minutes"],[])

    def test_exchange_errors_retain_saved_data_and_record_feed_failure(self):
        self.store.save_klines("ETHUSDT",[normalize_kline(kline(self.bucket),self.now)],self.now)
        class Client:
            def fetch(client,*args):
                raise RuntimeError("exchange unavailable")
        with self.assertRaisesRegex(RuntimeError,"unavailable"):
            collect_symbol(self.store,Client(),"ETHUSDT",self.now)
        series = self.store.series("ETHUSDT",current_ms=self.now)
        self.assertEqual(len(series["klines"]),1)
        self.assertEqual(series["feed"]["error"],"exchange unavailable")

    def test_journal_replay_restart_and_snapshot_are_idempotent(self):
        queued = record(self.bucket,"scheduled","queue")
        self.append(queued)
        final = record(self.bucket)
        final["event_ts_us"] += 10
        self.append(final)
        self.assertEqual(self.store.import_journal(self.journal,self.now),2)
        self.assertEqual(self.store.import_journal(self.journal,self.now),0)
        self.store.close()
        self.store = SignalHistoryStore(self.path)
        self.assertEqual(self.store.import_journal(self.journal,self.now),0)
        series = self.store.series("ETHUSDT",current_ms=self.now)
        self.assertEqual(len(series["signals"]),1)
        self.assertEqual(series["signals"][0]["snapshot"],final["snapshot"])
        self.assertEqual(series["signals"][0]["publication"]["levels"][0]["qty"],0.333)
        self.assertEqual(self.store.overview(["ETHUSDT"],self.now)["symbols"][0]["published"],1)

    def test_unfinished_tail_waits_then_imports_and_corruption_is_visible(self):
        line = json.dumps(record(self.bucket)).encode()
        self.file.write_bytes(line[:30])
        self.assertEqual(self.store.import_journal(self.journal,self.now),0)
        with self.file.open("ab") as stream:
            stream.write(line[30:]+b"\n")
        self.assertEqual(self.store.import_journal(self.journal,self.now),1)
        with self.file.open("ab") as stream:
            stream.write(b"not-json\n")
        with self.assertRaisesRegex(ValueError,"20270115.jsonl"):
            self.store.import_journal(self.journal,self.now)

    def test_oversized_journal_line_is_reported_instead_of_waiting_forever(self):
        self.file.write_bytes(b"x"*(128*1024))
        with self.assertRaisesRegex(ValueError,"exceeds 128 KiB"):
            self.store.import_journal(self.journal,self.now)

    def test_replaced_and_truncated_journal_is_replayed_without_duplicates(self):
        self.append(record(self.bucket))
        self.store.import_journal(self.journal,self.now)
        self.file.unlink()
        self.append(record(self.bucket))
        self.append(record(self.bucket+MINUTE_MS,"observed","next","next"))
        self.assertEqual(self.store.import_journal(self.journal,self.now+MINUTE_MS),1)
        self.file.write_text(json.dumps(record(self.bucket))+"\n")
        self.assertEqual(self.store.import_journal(self.journal,self.now),0)

    def test_only_factual_publications_are_accepted_and_partial_is_marked(self):
        candidate = record(self.bucket,"observed")
        candidate["publication"]["levels"] = record(self.bucket)["publication"]["levels"]
        with self.assertRaisesRegex(ValueError,"factual"):
            self.store.validate_record(candidate)
        candidate = record(self.bucket)
        candidate["publication"]["levels"] = []
        with self.assertRaisesRegex(ValueError,"factual"):
            self.store.validate_record(candidate)
        self.append(record(self.bucket,"partial"))
        self.store.import_journal(self.journal,self.now)
        self.assertEqual(self.store.overview(["ETHUSDT"],self.now)["symbols"][0]["published"],1)

    def test_target_publications_import_and_resume_after_restart(self):
        for i, qty in enumerate([1.25, -1.25, 0.0, "clear"]):
            item = record(self.bucket, event=f"target-{i}", signal=f"target-{i}")
            item["publication"] = {"levels": [], "error": None, "target": {
                "strategy_name": "lgq_cta_factor_036", "qty": qty,
                "published_ts_us": item["event_ts_us"],
            }}
            self.append(item)
        self.assertEqual(self.store.import_journal(self.journal, self.now), 4)
        self.assertEqual(self.store.overview(["ETHUSDT"], self.now)["symbols"][0]["published"], 4)
        saved = self.store.series("ETHUSDT", current_ms=self.now)["signals"]
        self.assertCountEqual([r["publication"]["target"]["qty"] for r in saved], [1.25, -1.25, 0.0, "clear"])
        self.store.close()
        self.store = SignalHistoryStore(self.path)
        self.assertEqual(self.store.import_journal(self.journal, self.now), 0)
        self.append(record(self.bucket, "observed", "after-target", "after-target"))
        self.assertEqual(self.store.import_journal(self.journal, self.now), 1)

    def test_invalid_or_unpublished_targets_are_rejected(self):
        target = {"strategy_name": "lgq_cta_factor_036", "qty": 0.0,
                  "published_ts_us": (self.bucket + 1000) * 1000}
        invalid = [None, {}, [], "target"]
        for key, values in {
            "qty": [None, True, float("nan"), float("inf"), "1.0"],
            "published_ts_us": [None, True, 0, -1, "123"],
            "strategy_name": [None, "", " ", 123],
        }.items():
            invalid.extend({**target, key: value} for value in values)
        for value in invalid:
            with self.subTest(target=value):
                item = record(self.bucket)
                item["publication"] = {"levels": [], "target": value}
                with self.assertRaises(ValueError):
                    self.store.validate_record(item)
        item = record(self.bucket, "observed")
        item["publication"]["target"] = target
        with self.assertRaisesRegex(ValueError, "factual"):
            self.store.validate_record(item)

    def test_retention_preserves_recent_signals_and_candles(self):
        for i,age in enumerate([3*DAY_MS,DAY_MS]):
            ts=self.bucket-age
            self.store.save_klines("ETHUSDT",[normalize_kline(kline(ts),self.now)],self.now)
            self.append(record(ts,event=str(i),signal=str(i)))
        self.store.import_journal(self.journal,self.now)
        self.store.prune(self.now)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM klines").fetchone()[0],1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM signals").fetchone()[0],1)

    def test_revision_changes_when_an_older_observation_finishes(self):
        self.append(record(self.bucket-MINUTE_MS,"scheduled","old","old"))
        self.append(record(self.bucket,"observed","latest","latest"))
        self.store.import_journal(self.journal,self.now)
        revision=self.store.series("ETHUSDT",current_ms=self.now)["revision"]
        changed=record(self.bucket-MINUTE_MS,"cancelled","cancel","old")
        changed["event_ts_us"]+=100
        self.append(changed)
        self.store.import_journal(self.journal,self.now)
        self.assertNotEqual(revision,self.store.series("ETHUSDT",current_ms=self.now)["revision"])

    def test_http_page_assets_gzip_etag_and_unknown_scope(self):
        current=time.time_ns()//1_000_000
        last=current//MINUTE_MS*MINUTE_MS
        self.store.save_klines("ETHUSDT",[normalize_kline(kline(ts),current)
            for ts in range(last-DAY_MS,last+1,MINUTE_MS)],current)
        server=ThreadingHTTPServer(("127.0.0.1",0),make_handler(self.root/"config.json",self.root/"status.json",
            self.root/"execution.json",ROOT/"web/cta_perp/index.html",self.store,targets=lambda:["ETHUSDT"]))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base=f"http://127.0.0.1:{server.server_port}"
        for path,content in [("/signals/",b"CTA"),("/signals/panel.js",b"CtaPanel"),("/assets/echarts.min.js",b"Apache")]:
            with urllib.request.urlopen(base+path) as response:
                self.assertIn(content,response.read())
        with urllib.request.urlopen(base+"/signals") as response:
            self.assertTrue(response.url.endswith("/signals/"))
        request=urllib.request.Request(base+"/api/signal-history?symbol=ETHUSDT",headers={"Accept-Encoding":"gzip"})
        with patch("cta_perp_signal_history.now_ms",return_value=current):
            with urllib.request.urlopen(request) as response:
                self.assertEqual(response.headers["Content-Encoding"],"gzip")
                self.assertEqual(json.loads(gzip.decompress(response.read()))["closed_bars"],1440)
                etag=response.headers["ETag"]
            request.add_header("If-None-Match",etag)
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request)
            self.assertEqual(caught.exception.code,304)
            caught.exception.close()
        for query in ["symbol=BTCUSDT","symbol=ETHUSDT&hours=25","symbol=ETHUSDT&unexpected=1"]:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(base+"/api/signal-history?"+query)
            self.assertEqual(caught.exception.code,400)
            caught.exception.close()

    def test_scope_hot_reload_empty_table_and_template_fallback(self):
        status=self.root/"status.json"
        config=ROOT/"config/cta_perp_symbol_factors.json"
        self.assertEqual(len(configured_targets(status,config)),9)
        status.write_text("[]")
        self.assertEqual(len(configured_targets(status,config)),9)
        status.write_text(json.dumps({"symbol_factors":{"ETHUSDT":{}}}))
        self.assertEqual(configured_targets(status,config),["ETHUSDT"])
        status.write_text(json.dumps({"symbol_factors":{}}))
        self.assertEqual(configured_targets(status,config),[])

    def test_factor_scope_filters_history_and_replay_and_rejects_unknown_factor(self):
        from cta_perp_position_replay import PositionReplay, simulate
        config_path=self.root/'config.json';status_path=self.root/'status.json';settings_path=self.root/'replay.json'
        config=json.loads((ROOT/'config/cta_perp.json').read_text())
        config_path.write_text(json.dumps(config))
        table={'ETHUSDT':{'factor_036':1,'factor_051':-1}}
        status_path.write_text(json.dumps({'symbol_factors':table}))
        settings=dict(start_ms=self.bucket,initial_equity_usdt=10000,taker_fee_rate=0)
        settings_path.write_text(json.dumps(settings))
        for index,factor in enumerate(table['ETHUSDT']):
            item=record(self.bucket,event=f'event:{factor}',signal=f'signal:{factor}')
            item['snapshot']['strategy_name']=factor
            item['event_ts_us']+=index
            self.append(item)
        self.store.import_journal(self.journal,self.now)
        observations=[dict(symbol='ETHUSDT',model_ts_ms=self.bucket,
            inputs={f:dict(ready=True,score=1,quantile=.99 if w>0 else .01) for f,w in table['ETHUSDT'].items()},
            nq=dict(ready=True,long_quantile=.9,short_quantile=.1,execution_ts_ms=self.bucket+1,execution_bid=100,execution_ask=100))]
        replay=PositionReplay(self.store,config_path,status_path,settings_path)
        replay.result=simulate(observations,table,config,settings);replay.key='fixture'
        for simulation in (None,replay):
            with self.subTest(replay=simulation is not None):
                server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(config_path,status_path,self.root/'execution.json',
                    ROOT/'web/cta_perp/index.html',self.store,targets=lambda:['ETHUSDT'],replay=simulation))
                threading.Thread(target=server.serve_forever,daemon=True).start()
                self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
                base=f'http://127.0.0.1:{server.server_port}'
                with patch('cta_perp_signal_history.now_ms',return_value=self.now):
                    with urllib.request.urlopen(base+'/api/signal-panel') as response:
                        overview=json.load(response)
                    self.assertEqual(overview['factor_strategies']['ETHUSDT'],['factor_036','factor_051'])
                    self.assertFalse(overview['trading_enabled'])
                    with urllib.request.urlopen(base+'/api/signal-history?symbol=ETHUSDT&strategy=factor_036') as response:
                        series=json.load(response)
                    self.assertEqual(len(series['signals']),1)
                    self.assertEqual(series['signals'][0]['snapshot']['strategy_name'],'factor_036')
                    if simulation:
                        self.assertEqual(series['simulation']['summary']['strategy_name'],'factor_036')
                        self.assertTrue(all(r['strategy_name']=='factor_036' for r in series['simulation']['records']))
                        self.assertEqual(len(series['simulation']['open_positions']),1)
                    with self.assertRaises(urllib.error.HTTPError) as caught:
                        urllib.request.urlopen(base+'/api/signal-history?symbol=ETHUSDT&strategy=factor_999')
                    self.assertEqual(caught.exception.code,400);caught.exception.close()

    def test_symbol_mean_is_one_selectable_strategy_in_api_and_replay(self):
        from cta_perp_position_replay import PositionReplay, simulate
        config=json.loads((ROOT/'config/cta_perp.json').read_text())
        config['position_algorithm']='v1'
        config_path=self.root/'mean_config.json';config_path.write_text(json.dumps(config))
        table={'ETHUSDT':{'factor_036':1,'factor_051':-1}}
        status_path=self.root/'mean_status.json';status_path.write_text(json.dumps({'symbol_factors':table}))
        settings_path=self.root/'mean_replay.json';settings_path.write_text('{}')
        observations=[dict(symbol='ETHUSDT',model_ts_ms=self.bucket,
            inputs={f:dict(ready=True,score=1,quantile=.99 if w>0 else .01) for f,w in table['ETHUSDT'].items()},
            nq=dict(ready=True,long_quantile=.9,short_quantile=.1,execution_ts_ms=self.bucket+1,execution_bid=100,execution_ask=100))]
        replay=PositionReplay(self.store,config_path,status_path,settings_path)
        replay.result=simulate(observations,table,config,dict(start_ms=self.bucket,initial_equity_usdt=10000));replay.key='mean'
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(config_path,status_path,self.root/'execution.json',
            ROOT/'web/cta_perp/index.html',self.store,targets=lambda:['ETHUSDT'],replay=replay))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        base=f'http://127.0.0.1:{server.server_port}'
        with patch('cta_perp_signal_history.now_ms',return_value=self.now):
            with urllib.request.urlopen(base+'/api/signal-panel') as response:
                self.assertEqual(json.load(response)['factor_strategies']['ETHUSDT'],['symbol_mean'])
            with urllib.request.urlopen(base+'/api/signal-history?symbol=ETHUSDT&strategy=symbol_mean') as response:
                result=json.load(response)['simulation']
            self.assertEqual(len(result['records']),1)
            self.assertEqual(len(result['open_positions']),1)
            self.assertEqual(len(result['records'][0]['snapshot']['signal']['factor_votes']),2)

    def test_environment_local_service_collects_and_imports_without_repository_paths(self):
        class PublicVenue(BaseHTTPRequestHandler):
            def do_GET(handler):
                parsed=urllib.parse.urlparse(handler.path)
                self.assertEqual(parsed.path,"/fapi/v1/klines")
                self.assertIsNone(handler.headers.get("X-MBX-APIKEY"))
                query=urllib.parse.parse_qs(parsed.query)
                start,end,limit=(int(query[key][0]) for key in ("startTime","endTime","limit"))
                rows=[kline(ts) for ts in range(start,min(end//MINUTE_MS*MINUTE_MS,start+(limit-1)*MINUTE_MS)+1,MINUTE_MS)]
                body=json.dumps(rows).encode()
                handler.send_response(200)
                handler.send_header("Content-Length",str(len(body)))
                handler.end_headers()
                handler.wfile.write(body)
            def log_message(handler,*args):
                pass
        venue=ThreadingHTTPServer(("127.0.0.1",0),PublicVenue)
        threading.Thread(target=venue.serve_forever,daemon=True).start()
        self.addCleanup(venue.server_close)
        self.addCleanup(venue.shutdown)
        env=self.root/"binance-cta-perp-test"
        for name in ["scripts/cta_perp_dashboard.py","scripts/cta_perp_signal_history.py","scripts/cta_perp_position_replay.py",
                     "scripts/cta_perp_config_server.py",
                     "scripts/cta_perp_factor_config.py","scripts/factor_validation_allowlist.txt",
                     "config/cta_perp.json","config/cta_perp_symbol_factors.json",
                     "web/cta_perp/index.html","web/cta_perp_signals/index.html",
                     "web/cta_perp_signals/panel.js","web/vendor/echarts.min.js"]:
            destination=env/name
            destination.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(ROOT/name,destination)
        (env/"run").mkdir()
        (env/"run/cta_perp_status.json").write_text(json.dumps({"symbol_factors":{"ETHUSDT":{}}}))
        journal=env/"data/cta_perp/signal_events"
        journal.mkdir(parents=True)
        now=time.time_ns()//1_000_000
        (journal/"20261010.jsonl").write_text(json.dumps(record(now//MINUTE_MS*MINUTE_MS-2*MINUTE_MS))+"\n")
        # Reserve an ephemeral local port; no env process or supervisor is touched.
        probe=ThreadingHTTPServer(("127.0.0.1",0),PublicVenue)
        port=probe.server_port
        probe.server_close()
        process=subprocess.Popen([sys.executable,"scripts/cta_perp_dashboard.py","--port",str(port)],
            cwd=env,env={**os.environ,"BINANCE_FUTURES_PUBLIC_API_BASE":f"http://127.0.0.1:{venue.server_port}"},
            stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
        def stop():
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            process.communicate(timeout=5)
        self.addCleanup(stop)
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/signal-history?symbol=ETHUSDT",timeout=1) as response:
                    series=json.load(response)
                if series["closed_bars"]==1440 and len(series["signals"])==1:
                    break
            except urllib.error.URLError:
                pass
            time.sleep(.05)
        else:
            self.fail("environment-local collector and journal import did not become ready")
        self.assertEqual(series["signals"][0]["publication"]["levels"][0]["qty"],.333)
        self.assertTrue((env/"data/cta_perp/signal_history.sqlite3").is_file())
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/signals/") as response:
            self.assertIn(b"panel.js",response.read())
        stop()
        self.assertEqual(process.returncode,0)

    def test_normalizer_rejects_bad_ohlc_nonfinite_and_wrong_frequency(self):
        for index,value in [(2,"98"),(1,"nan"),(6,self.bucket+1),(5,"-1")]:
            row=kline(self.bucket)
            row[index]=value
            with self.assertRaises(ValueError):
                normalize_kline(row,self.now)

    def test_public_client_auth_free_pagination_and_shared_rate_limit_backoff(self):
        calls=[]
        def opener(request,timeout):
            calls.append(request)
            return io.BytesIO(json.dumps([kline(self.bucket)]).encode())
        client=BinanceMinuteClient(opener=opener)
        self.assertEqual(len(client.fetch("ETHUSDT",self.bucket,self.now)),1)
        query=urllib.parse.parse_qs(urllib.parse.urlparse(calls[0].full_url).query)
        self.assertEqual(query["limit"],["499"])
        self.assertEqual(query["interval"],["1m"])
        self.assertEqual(set(calls[0].headers),{"User-agent"})
        def limited(request,timeout):
            raise urllib.error.HTTPError(request.full_url,429,"limited",{"Retry-After":"120"},None)
        client.opener=limited
        with self.assertRaisesRegex(RuntimeError,"HTTP 429"):
            client.fetch("ETHUSDT",self.bucket,self.now)
        client.opener=opener
        with self.assertRaisesRegex(RuntimeError,"backoff"):
            client.fetch("BNBUSDT",self.bucket,self.now)
        self.assertEqual(len(calls),1)


if __name__ == "__main__":
    unittest.main()
