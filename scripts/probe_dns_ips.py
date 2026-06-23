#!/usr/bin/env python3
"""Resolve a hostname and probe direct IP connectivity.

DNS A/AAAA records only provide addresses, not service ports. This script
infers the default port from the URL scheme, unless --ports is provided.
"""

from __future__ import annotations

import argparse
import csv
import ipaddress
import os
import random
import socket
import ssl
import struct
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Iterable, Optional


DNS_TYPE_A = 1
DNS_TYPE_AAAA = 28
DNS_CLASS_IN = 1


@dataclass(frozen=True)
class DnsAnswer:
    name: str
    record_type: int
    ttl: int
    value: str


@dataclass(frozen=True)
class ProbeResult:
    ip: str
    port: int
    tcp_ok: bool
    tls_ok: Optional[bool]
    latency_ms: Optional[float]
    error: str


def parse_target(raw: str, default_scheme: str) -> tuple[str, str, Optional[int]]:
    value = raw.strip()
    if "://" not in value:
        value = f"{default_scheme}://{value}"
    parsed = urllib.parse.urlparse(value)
    if not parsed.hostname:
        raise SystemExit(f"invalid target: {raw!r}")
    return parsed.scheme.lower(), parsed.hostname, parsed.port


def default_port_for_scheme(scheme: str) -> int:
    if scheme == "https":
        return 443
    if scheme == "http":
        return 80
    if scheme in {"wss", "ws"}:
        return 443 if scheme == "wss" else 80
    return 443


def encode_dns_name(name: str) -> bytes:
    labels = name.rstrip(".").split(".")
    encoded = bytearray()
    for label in labels:
        raw = label.encode("idna")
        if not raw or len(raw) > 63:
            raise ValueError(f"invalid DNS label in {name!r}: {label!r}")
        encoded.append(len(raw))
        encoded.extend(raw)
    encoded.append(0)
    return bytes(encoded)


def read_dns_name(packet: bytes, offset: int) -> tuple[str, int]:
    labels: list[str] = []
    jumped = False
    original_next = offset
    seen_offsets: set[int] = set()

    while True:
        if offset >= len(packet):
            raise ValueError("DNS name exceeds packet length")
        length = packet[offset]
        if length & 0xC0 == 0xC0:
            if offset + 1 >= len(packet):
                raise ValueError("truncated DNS pointer")
            pointer = ((length & 0x3F) << 8) | packet[offset + 1]
            if pointer in seen_offsets:
                raise ValueError("DNS pointer loop")
            seen_offsets.add(pointer)
            if not jumped:
                original_next = offset + 2
            offset = pointer
            jumped = True
            continue
        if length == 0:
            offset += 1
            if not jumped:
                original_next = offset
            break
        offset += 1
        if offset + length > len(packet):
            raise ValueError("truncated DNS label")
        labels.append(packet[offset : offset + length].decode("idna"))
        offset += length
        if not jumped:
            original_next = offset

    return ".".join(labels), original_next


def resolve_dns(
    host: str,
    *,
    nameserver: str,
    dns_port: int,
    qtypes: Iterable[int],
    timeout: float,
    attempts: int,
) -> list[DnsAnswer]:
    answers: list[DnsAnswer] = []
    seen: set[tuple[int, str]] = set()

    for qtype in qtypes:
        query = build_dns_query(host, qtype)
        last_error: Optional[BaseException] = None
        for _ in range(attempts):
            try:
                packet = send_dns_query(query, nameserver, dns_port, timeout)
                for answer in parse_dns_response(packet, expected_id=query[:2]):
                    if answer.record_type != qtype:
                        continue
                    key = (answer.record_type, answer.value)
                    if key in seen:
                        continue
                    seen.add(key)
                    answers.append(answer)
                last_error = None
                break
            except (OSError, ValueError) as exc:
                last_error = exc
        if last_error is not None:
            print(
                f"ERROR: DNS query type {qtype} via {nameserver}:{dns_port} failed: {last_error}",
                file=sys.stderr,
            )

    return answers


def build_dns_query(host: str, qtype: int) -> bytes:
    query_id = random.randint(0, 0xFFFF)
    header = struct.pack("!HHHHHH", query_id, 0x0100, 1, 0, 0, 0)
    question = encode_dns_name(host) + struct.pack("!HH", qtype, DNS_CLASS_IN)
    return header + question


def send_dns_query(query: bytes, nameserver: str, dns_port: int, timeout: float) -> bytes:
    family = socket.AF_INET6 if ":" in nameserver else socket.AF_INET
    with socket.socket(family, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        sock.sendto(query, (nameserver, dns_port))
        packet, _ = sock.recvfrom(4096)
        return packet


def parse_dns_response(packet: bytes, *, expected_id: bytes) -> list[DnsAnswer]:
    if len(packet) < 12:
        raise ValueError("truncated DNS response")
    if packet[:2] != expected_id:
        raise ValueError("DNS response id mismatch")

    _, flags, qdcount, ancount, _, _ = struct.unpack("!HHHHHH", packet[:12])
    rcode = flags & 0x000F
    if rcode != 0:
        raise ValueError(f"DNS response rcode={rcode}")

    offset = 12
    for _ in range(qdcount):
        _, offset = read_dns_name(packet, offset)
        offset += 4
        if offset > len(packet):
            raise ValueError("truncated DNS question")

    answers: list[DnsAnswer] = []
    for _ in range(ancount):
        name, offset = read_dns_name(packet, offset)
        if offset + 10 > len(packet):
            raise ValueError("truncated DNS answer")
        rtype, rclass, ttl, rdlength = struct.unpack("!HHIH", packet[offset : offset + 10])
        offset += 10
        rdata = packet[offset : offset + rdlength]
        offset += rdlength
        if rclass != DNS_CLASS_IN:
            continue
        if rtype == DNS_TYPE_A and len(rdata) == 4:
            value = socket.inet_ntop(socket.AF_INET, rdata)
            answers.append(DnsAnswer(name=name, record_type=rtype, ttl=ttl, value=value))
        elif rtype == DNS_TYPE_AAAA and len(rdata) == 16:
            value = socket.inet_ntop(socket.AF_INET6, rdata)
            answers.append(DnsAnswer(name=name, record_type=rtype, ttl=ttl, value=value))

    return answers


def probe_ip(
    ip: str,
    *,
    host: str,
    port: int,
    timeout: float,
    tls: bool,
) -> ProbeResult:
    start = time.monotonic()
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as raw_sock:
            raw_sock.settimeout(timeout)
            raw_sock.connect((ip, port))
            tcp_latency_ms = (time.monotonic() - start) * 1000.0
            if not tls:
                return ProbeResult(ip, port, True, None, tcp_latency_ms, "")

            context = ssl.create_default_context()
            with context.wrap_socket(raw_sock, server_hostname=host) as tls_sock:
                tls_sock.settimeout(timeout)
                tls_sock.version()
            total_latency_ms = (time.monotonic() - start) * 1000.0
            return ProbeResult(ip, port, True, True, total_latency_ms, "")
    except Exception as exc:
        latency_ms = (time.monotonic() - start) * 1000.0
        return ProbeResult(ip, port, False, False if tls else None, latency_ms, str(exc))


def parse_ports(raw_ports: list[str], fallback_port: int) -> list[int]:
    if not raw_ports:
        return [fallback_port]
    ports: list[int] = []
    seen: set[int] = set()
    for raw in raw_ports:
        for part in raw.split(","):
            value = part.strip()
            if not value:
                continue
            port = int(value)
            if not 1 <= port <= 65535:
                raise SystemExit(f"invalid port: {port}")
            if port in seen:
                continue
            seen.add(port)
            ports.append(port)
    return ports


def record_type_name(record_type: int) -> str:
    if record_type == DNS_TYPE_A:
        return "A"
    if record_type == DNS_TYPE_AAAA:
        return "AAAA"
    return str(record_type)


def sort_ip_key(ip: str) -> tuple[int, int]:
    parsed = ipaddress.ip_address(ip)
    return (parsed.version, int(parsed))


def load_counts(path: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not path or not os.path.exists(path):
        return counts
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            try:
                ipaddress.ip_address(parts[0])
                counts[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return counts


def write_counts(path: str, counts: dict[str, int]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for ip in sorted(counts, key=sort_ip_key):
            fh.write(f"{ip} {counts[ip]}\n")


def append_latency_rows(
    path: str,
    *,
    timestamp: str,
    target: str,
    host: str,
    nameserver: str,
    answer_type: str,
    result: ProbeResult,
) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(
                [
                    "timestamp",
                    "target",
                    "host",
                    "nameserver",
                    "record_type",
                    "ip",
                    "port",
                    "tcp_ok",
                    "tls_ok",
                    "latency_ms",
                    "error",
                ]
            )
        writer.writerow(
            [
                timestamp,
                target,
                host,
                nameserver,
                answer_type,
                result.ip,
                result.port,
                int(result.tcp_ok),
                "" if result.tls_ok is None else int(result.tls_ok),
                "" if result.latency_ms is None else f"{result.latency_ms:.3f}",
                result.error,
            ]
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve a hostname via DNS and probe direct IP:port connectivity.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="https://fapi-mm.binance.com",
        help="Hostname or URL to resolve.",
    )
    parser.add_argument(
        "--nameserver",
        default="ns-735.awsdns-27.net",
        help="DNS server to query.",
    )
    parser.add_argument("--dns-port", type=int, default=53, help="DNS server UDP port.")
    parser.add_argument(
        "--record-type",
        choices=["A", "AAAA", "both"],
        default="A",
        help="DNS record type to query.",
    )
    parser.add_argument(
        "--ports",
        action="append",
        default=[],
        help="Port(s) to probe, comma-separated or repeatable. Defaults from URL scheme.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=2.0,
        help="DNS and TCP/TLS timeout in seconds.",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=2,
        help="DNS attempts per record type.",
    )
    parser.add_argument(
        "--count-file",
        default="ip.txt",
        help="Maintain '<ip> <count>' totals like the shell script. Empty disables writing.",
    )
    parser.add_argument(
        "--latency-file",
        default="",
        help="Append per-probe latency rows as CSV. Empty disables writing.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between loop iterations.",
    )
    parser.add_argument(
        "--no-tls",
        action="store_true",
        help="Only test TCP connect, without TLS SNI/certificate handshake.",
    )
    parser.add_argument(
        "--default-scheme",
        default="https",
        help="Scheme to assume when target is a bare hostname.",
    )
    return parser.parse_args()


def run_once(args: argparse.Namespace) -> int:
    scheme, host, url_port = parse_target(args.target, args.default_scheme)
    ports = parse_ports(args.ports, url_port or default_port_for_scheme(scheme))
    qtypes = {
        "A": [DNS_TYPE_A],
        "AAAA": [DNS_TYPE_AAAA],
        "both": [DNS_TYPE_A, DNS_TYPE_AAAA],
    }[args.record_type]

    answers = resolve_dns(
        host,
        nameserver=args.nameserver,
        dns_port=args.dns_port,
        qtypes=qtypes,
        timeout=args.timeout,
        attempts=max(1, args.attempts),
    )
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    if not answers:
        print(f"{now} {host} no DNS answers from {args.nameserver}:{args.dns_port}")
        return 1

    counts = load_counts(args.count_file) if args.count_file else {}
    for answer in answers:
        counts[answer.value] = counts.get(answer.value, 0) + 1
    if args.count_file:
        write_counts(args.count_file, counts)

    print(f"{now} {host} via {args.nameserver}:{args.dns_port}")
    print(f"DNS answers: {len(answers)}")
    for answer in answers:
        count_text = f" seen={counts.get(answer.value, 0)}" if args.count_file else ""
        print(
            f"  {record_type_name(answer.record_type):4} {answer.value:39} ttl={answer.ttl}{count_text}"
        )

    print("Probe results:")
    probe_failures = 0
    for answer in answers:
        for port in ports:
            result = probe_ip(
                answer.value,
                host=host,
                port=port,
                timeout=args.timeout,
                tls=(not args.no_tls and port == 443),
            )
            answer_type = record_type_name(answer.record_type)
            if args.latency_file:
                append_latency_rows(
                    args.latency_file,
                    timestamp=now,
                    target=args.target,
                    host=host,
                    nameserver=f"{args.nameserver}:{args.dns_port}",
                    answer_type=answer_type,
                    result=result,
                )
            status = "OK" if result.tcp_ok and result.tls_ok is not False else "FAIL"
            tls_text = ""
            if result.tls_ok is True:
                tls_text = " tls=ok"
            elif result.tls_ok is False:
                tls_text = " tls=fail"
            latency_text = ""
            if result.latency_ms is not None:
                latency_text = f" {result.latency_ms:.1f}ms"
            error_text = f" error={result.error}" if result.error else ""
            print(f"  {answer.value}:{port} {status}{tls_text}{latency_text}{error_text}")
            if status != "OK":
                probe_failures += 1

    print("Hosts/curl candidates:")
    for answer in answers:
        if answer.record_type != DNS_TYPE_A:
            continue
        print(f"  /etc/hosts: {answer.value} {host}")
        for port in ports:
            print(f"  curl --resolve {host}:{port}:{answer.value} {scheme}://{host}/")

    return 2 if probe_failures else 0


def main() -> None:
    args = parse_args()
    exit_code = 0
    while True:
        exit_code = run_once(args)
        if not args.loop:
            raise SystemExit(exit_code)
        print()
        time.sleep(max(0.1, args.interval))


if __name__ == "__main__":
    main()
