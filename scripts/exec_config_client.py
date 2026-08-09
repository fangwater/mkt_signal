#!/usr/bin/env python3
"""Read, publish, or explicitly remove Exec strategies through the config API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_BASE_URL = "http://172.16.30.42:10041/config/"


class ApiError(RuntimeError):
    def __init__(self, status: int, payload: Any) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.payload = payload


def normalize_base_url(raw: str) -> str:
    value = str(raw or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("--url must be an absolute http(s) URL")
    if parsed.query or parsed.fragment:
        raise ValueError("--url must not contain a query or fragment")
    return value.rstrip("/") + "/"


def api_url(base_url: str, path: str) -> str:
    return urljoin(normalize_base_url(base_url), f"api/{path.lstrip('/')}")


def decode_json(raw: bytes) -> Any:
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 5.0,
) -> Dict[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(
        api_url(base_url, path),
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            decoded = decode_json(response.read())
    except HTTPError as exc:
        raise ApiError(exc.code, decode_json(exc.read())) from exc
    except URLError as exc:
        raise RuntimeError(f"request failed: {exc.reason}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("server response must be a JSON object")
    return decoded


def load_json_source(source: str) -> Dict[str, Any]:
    if source == "-":
        decoded = json.load(sys.stdin)
    elif source.startswith("@"):
        path = source[1:]
        if not path:
            raise ValueError("JSON file path is empty")
        with Path(path).open("r", encoding="utf-8") as handle:
            decoded = json.load(handle)
    else:
        decoded = json.loads(source)
    if not isinstance(decoded, dict):
        raise ValueError("POST JSON must be an object")
    return decoded


def get_api_path(strategy_name: Optional[str]) -> str:
    if strategy_name is None:
        return "strategies"
    name = strategy_name.strip()
    if not name:
        raise ValueError("strategy_name must not be empty")
    return f"strategy?name={quote(name, safe='')}"


def print_json(payload: Any, *, stream: Any = sys.stdout) -> None:
    json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
    stream.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read, publish, or remove strategies through the Exec config API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s get
  %(prog)s get cta_alpha
  %(prog)s get cta_alpha > cta_alpha.json
  %(prog)s post @cta_alpha.json
  %(prog)s post '{"strategy_name":"cta_alpha","config":{...}}'
  %(prog)s remove cta_alpha
  cat cta_alpha.json | %(prog)s post -
""",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("EXEC_CONFIG_URL", DEFAULT_BASE_URL),
        help="config page base URL (default: %(default)s)",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    commands = parser.add_subparsers(dest="command", required=True)

    get_parser = commands.add_parser("get", help="GET strategy JSON")
    get_parser.add_argument(
        "strategy_name",
        nargs="?",
        help="omit to GET the strategy-name list",
    )

    post_parser = commands.add_parser("post", help="POST strategy JSON")
    post_parser.add_argument(
        "json",
        help="inline JSON, @path/to/file.json, or - for stdin",
    )

    remove_parser = commands.add_parser("remove", help="Request strategy removal")
    remove_parser.add_argument("strategy_name")
    return parser


def run(args: argparse.Namespace) -> int:
    if args.command == "get":
        response = request_json(
            args.url,
            get_api_path(args.strategy_name),
            timeout=args.timeout,
        )
    elif args.command == "post":
        response = request_json(
            args.url,
            "strategy",
            method="POST",
            payload=load_json_source(args.json),
            timeout=args.timeout,
        )
    else:
        response = request_json(
            args.url,
            get_api_path(args.strategy_name),
            method="DELETE",
            timeout=args.timeout,
        )
    print_json(response)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be > 0")
    try:
        return run(args)
    except ApiError as exc:
        print_json(
            {"ok": False, "http_status": exc.status, "response": exc.payload},
            stream=sys.stderr,
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print_json({"ok": False, "error": str(exc)}, stream=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
