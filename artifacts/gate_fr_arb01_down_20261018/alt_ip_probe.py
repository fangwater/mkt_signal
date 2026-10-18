"""Read-only TCP/banner and HTTP probes; run locally or over SSH stdin."""

import concurrent.futures
import datetime
import json
import socket
import subprocess
import sys
import time


PUBLIC_IPS = [
    "13.115.227.29", "52.193.90.33", "54.238.72.43", "52.69.78.134",
    "54.238.97.67", "54.64.165.84", "54.64.228.233", "52.69.209.108",
    "54.199.82.56", "52.192.54.88", "18.181.48.65",
]
TARGETS = [(ip, 22) for ip in PUBLIC_IPS] + [("10.61.20.5", 10022)]


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def tcp(ip, port):
    result = {"kind": "tcp", "ip": ip, "port": port, "started_utc": utc()}
    start = time.monotonic()
    phase = "connect"
    try:
        with socket.create_connection((ip, port), timeout=8) as conn:
            result.update(status="connected", local_ip=conn.getsockname()[0],
                          connect_seconds=round(time.monotonic() - start, 3))
            if port in (22, 10022):
                phase = "banner"
                conn.settimeout(8)
                banner = conn.recv(256)
                result["status"] = "ssh_banner" if banner.startswith(b"SSH-") else (
                    "eof_before_banner" if not banner else "non_ssh_banner")
                if banner.startswith(b"SSH-"):
                    result["banner"] = banner.decode("ascii", errors="replace").strip()
    except OSError as exc:
        result.update(status=phase + "_error", error_type=type(exc).__name__, error=str(exc))
    result["elapsed_seconds"] = round(time.monotonic() - start, 3)
    return result


def http(ip):
    url = "http://" + ip + ":4191/fr/gate_fr_arb01"
    result = {"kind": "http", "ip": ip, "port": 4191, "url": url, "started_utc": utc()}
    start = time.monotonic()
    args = ["curl", "-q", "--noproxy", "*", "--connect-timeout", "8",
            "--max-time", "12", "--silent", "--show-error", "--output", "/dev/null",
            "--write-out", "%{http_code}\t%{remote_ip}\t%{local_ip}\t%{time_connect}\t%{time_total}\t%{size_download}", url]
    try:
        p = subprocess.run(args, text=True, capture_output=True, timeout=15)
        fields = p.stdout.strip().split("\t")
        result.update(exit_code=p.returncode, stderr=p.stderr.strip())
        if len(fields) == 6:
            result.update(dict(zip(["http_code", "remote_ip", "local_ip", "time_connect",
                                    "time_total", "size_download"], fields)))
    except (OSError, subprocess.TimeoutExpired) as exc:
        result["error"] = str(exc)
    result["elapsed_seconds"] = round(time.monotonic() - start, 3)
    return result


def main():
    start = utc()
    routes = {}
    for ip in ["52.69.78.134", "10.61.20.5"]:
        p = subprocess.run(["ip", "route", "get", ip], text=True, capture_output=True)
        routes[ip] = {"exit_code": p.returncode, "stdout": p.stdout.strip(), "stderr": p.stderr.strip()}
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        jobs = [pool.submit(tcp, ip, port) for ip, port in TARGETS]
        jobs += [pool.submit(tcp, ip, 4191) for ip, _ in TARGETS]
        jobs += [pool.submit(http, ip) for ip, _ in TARGETS]
        results = [job.result() for job in jobs]
    print(json.dumps({"source": sys.argv[1], "hostname": socket.gethostname(),
                      "started_utc": start, "finished_utc": utc(), "routes": routes,
                      "tcp_connect_timeout_seconds": 8, "ssh_banner_timeout_seconds": 8,
                      "http_connect_timeout_seconds": 8, "http_total_timeout_seconds": 12,
                      "results": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
