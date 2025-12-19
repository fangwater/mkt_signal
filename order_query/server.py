#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    import pandas as pd
except ModuleNotFoundError as e:
    raise SystemExit(
        "Missing dependency: pandas.\n"
        "If you are on this host, you probably want to run with jupyter_env:\n"
        "  /home/ubuntu/jupyter_env/bin/python order_query/server.py ...\n"
        "Or set PYTHON_BIN=/home/ubuntu/jupyter_env/bin/python when using the start scripts."
    ) from e


BASE_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"


def now_ms() -> int:
    return int(time.time() * 1000)


def json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")


def normalize_symbol(value) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).upper()
    if s == "NAN":
        return ""
    s = s.replace("-", "").replace("_", "")
    for suffix in ("SWAP", "PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def safe_join(base: Path, *parts: str) -> Path:
    p = (base.joinpath(*parts)).resolve()
    if str(p).startswith(str(base.resolve()) + os.sep) or p == base.resolve():
        return p
    raise ValueError(f"unsafe path: {p}")


def infer_api_base(listen_port: int, explicit: str | None) -> str:
    if explicit:
        return explicit
    if listen_port >= 10000:
        return f"http://127.0.0.1:{listen_port - 10000}"
    return "http://127.0.0.1:8089"


def read_parquet_values(path: Path, col: str) -> pd.Series:
    if not path.exists():
        return pd.Series(dtype="object")
    df = pd.read_parquet(path, columns=[col])
    if col not in df.columns:
        return pd.Series(dtype="object")
    return df[col]


def compute_symbol_stats(export_dir: Path) -> dict[str, Any]:
    files = {
        "order_updates": ("order_updates.parquet", ["symbol"]),
        "trade_updates": ("trade_updates.parquet", ["symbol"]),
        "signals_arb_open": ("signals_arb_open.parquet", ["opening_symbol", "hedging_symbol"]),
        "signals_arb_hedge": ("signals_arb_hedge.parquet", ["opening_symbol", "hedging_symbol"]),
        "signals_arb_cancel": ("signals_arb_cancel.parquet", ["opening_symbol", "hedging_symbol"]),
        "signals_arb_close": ("signals_arb_close.parquet", ["opening_symbol", "hedging_symbol"]),
    }

    by_key: dict[str, dict[str, Any]] = {}

    def bump(key: str, bucket: str, original: str | None):
        if not key:
            return
        d = by_key.setdefault(
            key,
            {
                "symbol_key": key,
                "counts": {},
                "originals": [],
            },
        )
        d["counts"][bucket] = int(d["counts"].get(bucket, 0)) + 1
        if original and len(d["originals"]) < 4 and original not in d["originals"]:
            d["originals"].append(original)

    for bucket, (fname, cols) in files.items():
        fpath = export_dir / fname
        if not fpath.exists():
            continue
        for col in cols:
            try:
                vals = read_parquet_values(fpath, col)
            except Exception:
                continue
            for v in vals.dropna().astype(str).tolist():
                bump(normalize_symbol(v), f"{bucket}.{col}", v)

    symbols = list(by_key.values())
    for s in symbols:
        s["total"] = int(sum(s["counts"].values()))

    symbols.sort(key=lambda x: (-x["total"], x["symbol_key"]))
    return {
        "export_dir": str(export_dir),
        "export_ts_ms": now_ms(),
        "symbols": symbols,
    }

def ensure_meta(state: "AppState") -> dict[str, Any] | None:
    meta = state.load_meta()
    if meta is None and (state.export_dir / "order_updates.parquet").exists():
        meta = compute_symbol_stats(state.export_dir)
        state.write_meta(meta)
    return meta


@dataclass
class ExportJob:
    job_id: str
    status: str = "idle"  # idle|running|ok|error
    started_ms: int | None = None
    finished_ms: int | None = None
    returncode: int | None = None
    log_path: str | None = None
    api_base: str | None = None
    export_dir: str | None = None
    error: str | None = None


@dataclass
class AppState:
    data_dir: Path
    export_dir: Path
    derived_dir: Path
    tmp_dir: Path
    meta_path: Path
    export_script: Path
    export_symbol_script: Path
    export_job: ExportJob = field(default_factory=lambda: ExportJob(job_id="none"))
    lock: threading.Lock = field(default_factory=threading.Lock)

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.derived_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def load_meta(self) -> dict[str, Any] | None:
        if not self.meta_path.exists():
            return None
        try:
            return json.loads(self.meta_path.read_text("utf-8"))
        except Exception:
            return None

    def write_meta(self, meta: dict[str, Any]) -> None:
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(self.meta_path)


def run_export_all(state: AppState, api_base: str) -> None:
    with state.lock:
        if state.export_job.status == "running":
            return
        job_id = uuid.uuid4().hex
        log_path = state.data_dir / f"export_all_{job_id}.log"
        state.export_job = ExportJob(
            job_id=job_id,
            status="running",
            started_ms=now_ms(),
            log_path=str(log_path),
            api_base=api_base,
            export_dir=str(state.export_dir),
        )

    cmd = ["bash", str(state.export_script), str(state.export_dir), api_base]
    try:
        with open(log_path, "wb") as f:
            p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=str(BASE_DIR))
            rc = p.wait()
        meta = compute_symbol_stats(state.export_dir)
        state.write_meta(meta)
        with state.lock:
            state.export_job.returncode = rc
            state.export_job.finished_ms = now_ms()
            state.export_job.status = "ok" if rc == 0 else "error"
            if rc != 0:
                state.export_job.error = f"export_all failed (rc={rc})"
    except Exception as e:
        with state.lock:
            state.export_job.finished_ms = now_ms()
            state.export_job.status = "error"
            state.export_job.error = str(e)


def build_derived_parquet(state: AppState, symbol: str, overwrite: bool = False) -> dict[str, Any]:
    if not (state.export_dir / "order_updates.parquet").exists():
        raise RuntimeError("export_all not run yet")

    meta = ensure_meta(state) or {}
    export_ts = meta.get("export_ts_ms") or "noexport"
    symbol_key = normalize_symbol(symbol)
    if not symbol_key:
        raise ValueError("invalid symbol")

    out_name = f"{symbol_key}_order.parquet"
    out_path = state.derived_dir / out_name
    out_meta_path = state.derived_dir / f"{symbol_key}_order.meta.json"

    if out_path.exists() and not overwrite:
        try:
            if out_meta_path.exists():
                prev = json.loads(out_meta_path.read_text("utf-8"))
                if prev.get("export_ts_ms") == export_ts:
                    return {"symbol_key": symbol_key, "export_ts_ms": export_ts, "path": str(out_path), "built": False}
        except Exception:
            pass

    tmp_path = state.tmp_dir / f"{symbol_key}_order_{uuid.uuid4().hex}.parquet"
    cmd = [
        sys.executable,
        str(state.export_symbol_script),
        "--dir",
        str(state.export_dir),
        "--symbol",
        symbol,
        "--output",
        str(tmp_path),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=str(BASE_DIR))
    if p.returncode != 0:
        raise RuntimeError(
            f"export_symbol_data_v2 failed rc={p.returncode} stdout={p.stdout[-800:]} stderr={p.stderr[-800:]}"
        )

    tmp_path.replace(out_path)
    out_meta_path.write_text(
        json.dumps({"symbol_key": symbol_key, "export_ts_ms": export_ts, "built_ts_ms": now_ms()}, ensure_ascii=False, indent=2),
        "utf-8",
    )
    return {"symbol_key": symbol_key, "export_ts_ms": export_ts, "path": str(out_path), "built": True}


class Handler(BaseHTTPRequestHandler):
    server_version = "order_query/0.1"

    @property
    def state(self) -> AppState:
        return self.server.state  # type: ignore[attr-defined]

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, obj: Any) -> None:
        self._send(status, json_bytes(obj), "application/json; charset=utf-8")

    def _send_file(self, path: Path, content_type: str, download_name: str | None = None) -> None:
        st = path.stat()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(st.st_size))
        if download_name:
            self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 256)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def _read_json(self) -> dict[str, Any]:
        n = int(self.headers.get("Content-Length", "0") or "0")
        if n <= 0:
            return {}
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        u = urlparse(self.path)
        path = u.path
        qs = parse_qs(u.query)

        if path == "/api/health":
            self._send_json(200, {"ok": True, "ts_ms": now_ms()})
            return

        if path == "/api/export_all/status":
            with self.state.lock:
                job = self.state.export_job
                self._send_json(200, job.__dict__)
            return

        if path == "/api/config":
            self._send_json(
                200,
                {
                    "api_base": self.server.api_base,  # type: ignore[attr-defined]
                    "export_dir": str(self.state.export_dir),
                    "derived_dir": str(self.state.derived_dir),
                    "ts_ms": now_ms(),
                },
            )
            return

        if path == "/api/ready":
            with self.state.lock:
                job = self.state.export_job
                job_d = job.__dict__.copy()

            meta = ensure_meta(self.state)
            has_export = (self.state.export_dir / "order_updates.parquet").exists()

            symbols = (meta or {}).get("symbols") or []
            export_ts_ms = (meta or {}).get("export_ts_ms")
            data_ready = bool(has_export and len(symbols) > 0)

            if job_d.get("status") == "running":
                state = "exporting"
            elif data_ready:
                state = "ready"
            elif job_d.get("status") == "error":
                state = "error"
            else:
                state = "empty"

            self._send_json(
                200,
                {
                    "state": state,
                    "data_ready": data_ready,
                    "export_ts_ms": export_ts_ms,
                    "symbol_count": len(symbols),
                    "job": job_d,
                    "api_base": self.server.api_base,  # type: ignore[attr-defined]
                    "export_dir": str(self.state.export_dir),
                },
            )
            return

        if path == "/api/symbols":
            meta = ensure_meta(self.state)
            if meta is None:
                self._send_json(200, {"symbols": [], "export_dir": str(self.state.export_dir), "export_ts_ms": None})
                return
            self._send_json(200, meta)
            return

        if path == "/api/download_symbol":
            symbol = (qs.get("symbol", [""])[0] or "").strip()
            if not symbol:
                self._send_json(400, {"error": "missing symbol"})
                return
            if not (self.state.export_dir / "order_updates.parquet").exists():
                self._send_json(409, {"error": "export_all not run yet"})
                return
            try:
                built = build_derived_parquet(self.state, symbol, overwrite=False)
            except ValueError as e:
                self._send_json(400, {"error": str(e)})
                return
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return

            out_path = Path(built["path"])
            self._send_file(out_path, "application/octet-stream", download_name=out_path.name)
            return

        if path == "/" or path == "/index.html":
            self._send_file(STATIC_DIR / "index.html", "text/html; charset=utf-8")
            return

        if path == "/view":
            self._send_file(STATIC_DIR / "view.html", "text/html; charset=utf-8")
            return

        if path == "/view/":
            self.send_response(HTTPStatus.MOVED_PERMANENTLY)
            self.send_header("Location", "/view")
            self.end_headers()
            return

        if path.startswith("/assets/"):
            rel = path[len("/assets/") :]
            try:
                fpath = safe_join(STATIC_DIR, rel)
            except ValueError:
                self._send_json(404, {"error": "not found"})
                return
            if not fpath.exists():
                self._send_json(404, {"error": "not found"})
                return
            if fpath.suffix == ".js":
                ct = "application/javascript; charset=utf-8"
            elif fpath.suffix == ".css":
                ct = "text/css; charset=utf-8"
            else:
                ct = "application/octet-stream"
            self._send_file(fpath, ct)
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        u = urlparse(self.path)
        path = u.path

        if path == "/api/export_all":
            payload = self._read_json()
            api_base = (payload.get("api_base") or self.server.api_base).strip()  # type: ignore[attr-defined]
            with self.state.lock:
                running = self.state.export_job.status == "running"
            if running:
                self._send_json(409, {"error": "export already running"})
                return

            t = threading.Thread(target=run_export_all, args=(self.state, api_base), daemon=True)
            t.start()
            with self.state.lock:
                self._send_json(200, self.state.export_job.__dict__)
            return

        if path == "/api/export_symbol":
            payload = self._read_json()
            symbol = (payload.get("symbol") or "").strip()
            overwrite = bool(payload.get("overwrite") or False)
            if not symbol:
                self._send_json(400, {"error": "missing symbol"})
                return
            if not (self.state.export_dir / "order_updates.parquet").exists():
                self._send_json(409, {"error": "export_all not run yet"})
                return
            try:
                built = build_derived_parquet(self.state, symbol, overwrite=overwrite)
            except ValueError as e:
                self._send_json(400, {"error": str(e)})
                return
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            self._send_json(200, built)
            return

        self._send_json(404, {"error": "not found"})


def main() -> None:
    p = argparse.ArgumentParser(description="order_query: export & download parquet via web UI")
    p.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", "18080")))
    p.add_argument("--api-base", default=os.environ.get("API_BASE"))
    p.add_argument("--data-dir", default=os.environ.get("DATA_DIR", str(BASE_DIR / "data" / "order_query")))
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    export_dir = data_dir / "export_data"
    derived_dir = data_dir / "derived"
    tmp_dir = data_dir / "tmp"

    state = AppState(
        data_dir=data_dir,
        export_dir=export_dir,
        derived_dir=derived_dir,
        tmp_dir=tmp_dir,
        meta_path=data_dir / "meta.json",
        export_script=BASE_DIR / "scripts" / "export_all.sh",
        export_symbol_script=BASE_DIR / "scripts" / "export_xarb_symbol_data.py",
    )
    state.ensure_dirs()

    if not state.export_script.exists():
        raise SystemExit(f"missing export script: {state.export_script}")
    if not state.export_symbol_script.exists():
        raise SystemExit(f"missing symbol export script: {state.export_symbol_script}")

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    httpd.state = state  # type: ignore[attr-defined]
    httpd.api_base = infer_api_base(args.port, args.api_base)  # type: ignore[attr-defined]

    print(f"[order_query] listening on http://{args.host}:{args.port} api_base={httpd.api_base} data_dir={data_dir}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
