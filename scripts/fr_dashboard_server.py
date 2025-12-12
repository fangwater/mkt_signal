#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FR Dashboard Python 后端（FastAPI）。

提供：
  - 导航页 / : 5 个交易所入口
  - FR Signal 看板 /fr_signal/{exchange}

看板页面复用 docs/fr_signal_dashboard.html，并附带 exchange query param，
前端会显示对应交易所名称。WebSocket 由 fr_visualization 提供，nginx 可自行代理。

运行：
  python3 scripts/fr_dashboard_server.py --port 8900 --bind 0.0.0.0
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
FR_SIGNAL_DASHBOARD = DOCS_DIR / "fr_signal_dashboard.html"

EXCHANGES: List[str] = ["binance", "okex", "bybit", "bitget", "gate"]
EXCHANGE_LABELS: Dict[str, str] = {
    "binance": "Binance",
    "okex": "OKX",
    "bybit": "Bybit",
    "bitget": "Bitget",
    "gate": "Gate",
}


def load_dashboard_html() -> str:
    if not FR_SIGNAL_DASHBOARD.exists():
        raise FileNotFoundError(f"dashboard not found: {FR_SIGNAL_DASHBOARD}")
    return FR_SIGNAL_DASHBOARD.read_text(encoding="utf-8")


def build_nav_html(base_path: str = "") -> str:
    # base_path 用于 nginx subpath 部署时的前缀（默认为空）
    links = "\n".join(
        f'<a class="card" href="{base_path}/fr_signal/{ex}">'
        f'<div class="title">{EXCHANGE_LABELS.get(ex, ex)}</div>'
        f'<div class="subtitle">FR Signal Dashboard</div>'
        f"</a>"
        for ex in EXCHANGES
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <title>FR Dashboards</title>
    <style>
      :root {{
        color-scheme: dark;
        font-family: "Segoe UI", "PingFang SC", "Helvetica Neue", sans-serif;
        --bg: #0d1117;
        --bg-soft: #161b22;
        --border: #30363d;
        --text: #e6edf3;
        --muted: #8b949e;
        --accent-blue: #58a6ff;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--bg);
        color: var(--text);
        min-height: 100vh;
        display: flex;
        flex-direction: column;
      }}
      header {{
        padding: 18px 24px;
        border-bottom: 1px solid var(--border);
        background: var(--bg-soft);
      }}
      header h1 {{
        margin: 0;
        font-size: 20px;
        letter-spacing: 0.3px;
      }}
      main {{
        padding: 24px;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 16px;
      }}
      .card {{
        display: block;
        padding: 16px 18px;
        border: 1px solid var(--border);
        border-radius: 10px;
        background: rgba(22, 27, 34, 0.6);
        text-decoration: none;
        color: var(--text);
        transition: transform .08s ease, border-color .12s ease, background .12s ease;
      }}
      .card:hover {{
        transform: translateY(-2px);
        border-color: var(--accent-blue);
        background: rgba(88, 166, 255, 0.08);
      }}
      .title {{
        font-size: 18px;
        font-weight: 700;
      }}
      .subtitle {{
        margin-top: 6px;
        font-size: 12px;
        color: var(--muted);
      }}
      footer {{
        margin-top: auto;
        padding: 12px 24px;
        color: var(--muted);
        font-size: 12px;
        border-top: 1px solid var(--border);
        background: var(--bg-soft);
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>FR Signal Dashboards</h1>
    </header>
    <main>
      {links}
    </main>
    <footer>
      后端仅提供页面。WebSocket 数据由 fr_visualization 推送，nginx 代理自行配置。
    </footer>
  </body>
</html>"""


def create_app(base_path: str = "") -> FastAPI:
    app = FastAPI()
    dashboard_html = load_dashboard_html()

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/", response_class=HTMLResponse)
    async def nav():
        return build_nav_html(base_path=base_path)

    @app.get("/fr_signal/{exchange}", response_class=HTMLResponse)
    async def fr_signal(exchange: str):
        ex = exchange.lower()
        if ex not in EXCHANGES:
            raise HTTPException(status_code=404, detail="unsupported exchange")
        # 给前端带上 exchange 参数用于标题显示
        # 直接复用 dashboard 文件内容
        return HTMLResponse(dashboard_html)

    # 兼容老路径：/fr_signal?exchange=binance
    @app.get("/fr_signal")
    async def fr_signal_redirect(exchange: str):
        ex = exchange.lower()
        if ex not in EXCHANGES:
            raise HTTPException(status_code=404, detail="unsupported exchange")
        return RedirectResponse(url=f"/fr_signal/{ex}")

    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FR dashboards python backend")
    p.add_argument("--bind", default="0.0.0.0", help="bind address")
    p.add_argument("--port", type=int, default=8900, help="listen port")
    p.add_argument(
        "--base-path",
        default="",
        help="optional url prefix when deployed under subpath (e.g. /fr)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app(base_path=args.base_path.rstrip("/"))
    uvicorn.run(app, host=args.bind, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

