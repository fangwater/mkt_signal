#!/usr/bin/env python3
"""Read-only LL2 source inventory; Report ranges are availability, not gap proofs."""

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote, unquote, urljoin
from urllib.request import Request, urlopen

MONTH_CODES = "FGHJKMNQUVXZ"
SIX = ("ES", "NQ", "RTY", "YM", "GC", "CL")


def parse_product(ric):
    # Same live-stem/month-suffix routing as cme_tas_replay::product::parse_product.
    if ric.startswith("."):
        return ric[1:]
    stem = ric.split("^", 1)[0]
    return re.sub(rf"[{MONTH_CODES}]\d+$", "", stem)


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.names = []

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href", "")
        if tag == "a" and href and href != "../" and "/" not in href.rstrip("/"):
            self.names.append(unquote(href))


def listing(url):
    with urlopen(url, timeout=30) as response:
        parser = Links()
        parser.feed(response.read().decode())
    return sorted(parser.names)


def summarize_report(payload):
    rows = list(csv.DictReader(io.StringIO(gzip.decompress(payload).decode("utf-8-sig"))))
    products = {}
    for row in rows:
        count = int(row["Count"])
        if not count:
            continue
        product = parse_product(row["#RIC"])
        item = products.setdefault(product, {"source_rows": 0, "active_rics": []})
        item["source_rows"] += count
        item["active_rics"].append({"ric": row["#RIC"], "count": count,
                                    "start": row["Start"], "end": row["End"]})
    for item in products.values():
        item["first_event_utc"] = min(r["start"] for r in item["active_rics"])
        item["last_event_utc"] = max(r["end"] for r in item["active_rics"])
    return {"report_rows": len(rows), "active_rics": sum(int(r["Count"]) > 0 for r in rows),
            "source_rows": sum(int(r["Count"]) for r in rows), "products": products}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--raw", action="store_true", help="Inventory Raw LL2 files without assuming complete packages")
    parser.add_argument("--compare-normalized", type=Path, help="Compare Raw reports with a saved normalized inventory")
    parser.add_argument("--remote-root", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.raw and args.data_root is None:
        parser.error("--data-root is required for normalized LL2 comparison")
    remote_root = args.remote_root.rstrip("/") + "/"
    directories = [n for n in listing(remote_root) if n.endswith("/")]

    def inspect_directory(name):
        directory = name.rstrip("/")
        url = urljoin(remote_root, name)
        files = [n for n in listing(url) if not n.endswith("/")]

        def inspect_file(filename):
            source_url = urljoin(url, quote(filename))
            with urlopen(Request(source_url, method="HEAD"), timeout=30) as response:
                headers = {k.lower(): v for k, v in response.headers.items()}
                size = int(response.headers["Content-Length"])
            path = args.data_root / directory / filename if args.data_root else None
            exists = path is not None and path.is_file()
            return {"name": filename, "url": source_url, "remote_bytes": size,
                    "last_modified": headers.get("last-modified"), "etag": headers.get("etag"),
                    "local_bytes": path.stat().st_size if exists else None,
                    "size_matches": exists and path.stat().st_size == size}

        entries = [inspect_file(f) for f in files]
        if args.raw:
            indices = sorted(int(m.group(1)) for f in files
                             if (m := re.fullmatch(r"merged-Data-part-(\d+)\.csv\.gz", f)))
            summary = {}
            if "merged-Report.csv.gz" in files:
                with urlopen(urljoin(url, "merged-Report.csv.gz"), timeout=30) as response:
                    report = response.read()
                summary = {"report_sha256": hashlib.sha256(report).hexdigest(), **summarize_report(report)}
            return {"directory": directory, "period": directory[-21:], "files": entries,
                    "finished_gzip_parts": len(indices), "finished_part_indices": indices,
                    "missing_part_indices_through_last": sorted(set(range(max(indices) + 1)) - set(indices)) if indices else [],
                    "partial_files": [f for f in files if f.endswith(".part")],
                    "report_present": "merged-Report.csv.gz" in files,
                    "notes_present": any(f.endswith(".notes.txt") for f in files), **summary}
        with urlopen(urljoin(url, "merged-Report.csv.gz"), timeout=30) as response:
            report = response.read()
        local_report = args.data_root / directory / "merged-Report.csv.gz"
        summary = summarize_report(report)
        return {"directory": directory, "period": directory[-21:], "files": entries,
                "report_sha256": hashlib.sha256(report).hexdigest(),
                "local_report_identical": local_report.is_file() and local_report.read_bytes() == report,
                **summary}

    with ThreadPoolExecutor(max_workers=8) as pool:
        packages = list(pool.map(inspect_directory, directories))
    if args.raw:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        result = {"checked_at_utc": datetime.now(timezone.utc).isoformat(),
                  "remote_root": remote_root, "scope": "Raw LL2 directory/HEAD and available Report inventory; full gzip and reconstruction not verified. Raw Count is not comparable to normalized snapshot Count.",
                  "package_count": len(packages), "file_count": sum(len(p["files"]) for p in packages),
                  "finished_gzip_parts": sum(p["finished_gzip_parts"] for p in packages),
                  "partial_file_count": sum(len(p["partial_files"]) for p in packages),
                  "packages": packages}
        if args.compare_normalized:
            normalized = json.loads(args.compare_normalized.read_text())
            result["normalized_inventory_path"] = str(args.compare_normalized)
            result["normalized_inventory_checked_at_utc"] = normalized["checked_at_utc"]
            comparison = []
            for pack in packages:
                norm_packages = [p for p in normalized["packages"] if p["period"] == pack["period"]]
                norm_rics = {r["ric"] for p in norm_packages for item in p["products"].values() for r in item["active_rics"]}
                raw_rics = {r["ric"] for item in pack.get("products", {}).values() for r in item["active_rics"]}
                raw_only = sorted(raw_rics - norm_rics) if pack["report_present"] else None
                norm_only = sorted(norm_rics - raw_rics) if pack["report_present"] else None
                six = {}
                for product in SIX:
                    norm_records = [r for p in norm_packages for r in p["products"].get(product, {}).get("active_rics", [])]
                    raw_records = pack.get("products", {}).get(product, {}).get("active_rics", [])
                    def window(records):
                        return {"active_rics": len({r["ric"] for r in records}),
                                "first_event_utc": min((r["start"] for r in records), default=None),
                                "last_event_utc": max((r["end"] for r in records), default=None)}
                    six[product] = {"raw": window(raw_records) if pack["report_present"] else None, "normalized": window(norm_records)}
                comparison.append({"period": pack["period"], "raw_report_present": pack["report_present"],
                                   "raw_active_rics": len(raw_rics) if pack["report_present"] else None,
                                   "normalized_active_rics": len(norm_rics), "raw_only_rics": raw_only,
                                   "normalized_only_rics": norm_only,
                                   "raw_only_products": sorted({parse_product(r) for r in raw_only}) if raw_only is not None else None,
                                   "six_products": six})
            result["normalized_comparison"] = comparison
            with args.output.with_suffix(".comparison.csv").open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["period", "raw_report_present", "raw_active_rics", "normalized_active_rics", "raw_only_rics", "normalized_only_rics"])
                for item in comparison:
                    writer.writerow([item["period"], item["raw_report_present"], item["raw_active_rics"], item["normalized_active_rics"],
                                     len(item["raw_only_rics"]) if item["raw_only_rics"] is not None else None,
                                     len(item["normalized_only_rics"]) if item["normalized_only_rics"] is not None else None])
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        for p in packages:
            print(p["period"], "gzip_parts", p["finished_gzip_parts"], "missing", p["missing_part_indices_through_last"],
                  "partial", p["partial_files"], "report", p["report_present"], "notes", p["notes_present"])
        print({k: v for k, v in result.items() if k not in ("packages", "normalized_comparison")})
        return
    all_products = sorted({p for pack in packages for p in pack["products"]})
    original = [p for p in packages if "_futures_" in p["directory"]]
    supplement = [p for p in packages if "_futures_" not in p["directory"]]
    original_products = {p for pack in original for p in pack["products"]}
    supplement_products = {p for pack in supplement for p in pack["products"]}
    unexpected_files = [f for pack in packages for f in pack["files"] if not f["size_matches"]]
    result = {"checked_at_utc": datetime.now(timezone.utc).isoformat(),
              "remote_root": remote_root, "local_root": str(args.data_root),
              "scope": "All logical source products, not a verified CME exporter allowlist. Count>0 does not prove continuous minutes or ten nonempty levels.",
              "package_count": len(packages), "file_count": sum(len(p["files"]) for p in packages),
              "original_package_count": len(original), "supplement_package_count": len(supplement),
              "active_product_count": len(all_products), "active_products": all_products,
              "original_active_product_count": len(original_products),
              "supplement_active_products": sorted(supplement_products),
              "products_new_to_entire_history": sorted(supplement_products - original_products),
              "local_missing_or_size_changed_files": unexpected_files,
              "packages": packages}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    csv_path = args.output.with_suffix(".csv")
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["directory", "period", "product", "active_rics", "source_rows", "first_event_utc", "last_event_utc"])
        for pack in packages:
            for product, item in sorted(pack["products"].items()):
                writer.writerow([pack["directory"], pack["period"], product, len(item["active_rics"]),
                                 item["source_rows"], item["first_event_utc"], item["last_event_utc"]])
    print(json.dumps({k: v for k, v in result.items() if k not in ("packages", "active_products")}, ensure_ascii=False, indent=2))
    for pack in packages:
        print(pack["period"], "supplement" if "_futures_" not in pack["directory"] else "original",
              "products", len(pack["products"]), "six", {p: len(pack["products"].get(p, {}).get("active_rics", [])) for p in SIX})


if __name__ == "__main__":
    main()
