#!/usr/bin/env python3
"""Probe a WSS endpoint by connecting to a fixed remote IP.

The TCP connection goes to the IP, while TLS SNI and the WebSocket Host header
keep the original hostname. This is the Python equivalent of an in-process
`curl --resolve` for WSS.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import socket
import ssl
import time
import urllib.parse


def parse_target(raw: str) -> tuple[str, int, str]:
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme.lower() != "wss":
        raise SystemExit("target must be a wss:// URL")
    if not parsed.hostname:
        raise SystemExit(f"target missing host: {raw!r}")
    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    return parsed.hostname, port, path


def recv_http_headers(sock: ssl.SSLSocket, timeout: float) -> bytes:
    sock.settimeout(timeout)
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > 65536:
            raise RuntimeError("response headers too large")
    return bytes(data)


def probe_once(
    *,
    target_url: str,
    remote_ip: str,
    local_ip: str | None,
    timeout: float,
) -> dict[str, object]:
    host, port, path = parse_target(target_url)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    expected_accept = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")

    t0 = time.perf_counter()
    raw = socket.socket(socket.AF_INET6 if ":" in remote_ip else socket.AF_INET, socket.SOCK_STREAM)
    raw.settimeout(timeout)
    try:
        if local_ip:
            raw.bind((local_ip, 0))
        raw.connect((remote_ip, port))
        t_tcp = time.perf_counter()

        ctx = ssl.create_default_context()
        tls = ctx.wrap_socket(raw, server_hostname=host)
        t_tls = time.perf_counter()

        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "User-Agent: mkt_signal-wss-direct-probe/1\r\n"
            "\r\n"
        ).encode("ascii")
        tls.sendall(req)
        headers = recv_http_headers(tls, timeout)
        t_ws = time.perf_counter()
        text = headers.decode("iso-8859-1", errors="replace")
        status_line = text.splitlines()[0] if text else ""
        accept_ok = expected_accept in text
        try:
            tls.close()
        except OSError:
            pass

        return {
            "ok": status_line.startswith("HTTP/1.1 101") and accept_ok,
            "status_line": status_line,
            "accept_ok": accept_ok,
            "tcp_ms": (t_tcp - t0) * 1000.0,
            "tls_ms": (t_tls - t_tcp) * 1000.0,
            "ws_ms": (t_ws - t_tls) * 1000.0,
            "total_ms": (t_ws - t0) * 1000.0,
            "error": "",
        }
    except Exception as exc:
        try:
            raw.close()
        except OSError:
            pass
        return {
            "ok": False,
            "status_line": "",
            "accept_ok": False,
            "tcp_ms": None,
            "tls_ms": None,
            "ws_ms": None,
            "total_ms": (time.perf_counter() - t0) * 1000.0,
            "error": str(exc),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Connect to a fixed remote IP for a WSS URL while preserving SNI/Host.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("target_url", help="Original WSS URL, e.g. wss://ws-fapi-mm.binance.com/ws-fapi/v1")
    parser.add_argument("--remote-ip", action="append", required=True, help="Remote IP to connect; repeatable")
    parser.add_argument("--local-ip", default="", help="Source IP to bind, e.g. binance_um_whitelist_ip")
    parser.add_argument("--timeout", type=float, default=3.0, help="Timeout seconds")
    parser.add_argument("--repeat", type=int, default=1, help="Probe count per remote IP")
    return parser.parse_args()


def fmt_ms(value: object) -> str:
    return "-" if value is None else f"{float(value):.1f}ms"


def main() -> None:
    args = parse_args()
    local_ip = args.local_ip.strip() or None
    exit_code = 0
    for remote_ip in args.remote_ip:
        for idx in range(max(1, args.repeat)):
            result = probe_once(
                target_url=args.target_url,
                remote_ip=remote_ip,
                local_ip=local_ip,
                timeout=args.timeout,
            )
            status = "OK" if result["ok"] else "FAIL"
            print(
                f"{remote_ip} #{idx + 1} {status} "
                f"tcp={fmt_ms(result['tcp_ms'])} tls={fmt_ms(result['tls_ms'])} "
                f"ws={fmt_ms(result['ws_ms'])} total={fmt_ms(result['total_ms'])} "
                f"status={result['status_line']!r} accept_ok={result['accept_ok']} "
                f"error={result['error']!r}"
            )
            if not result["ok"]:
                exit_code = 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
