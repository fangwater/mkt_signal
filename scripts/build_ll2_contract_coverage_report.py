#!/usr/bin/env python3
"""Expand LL2 Report inventories into complete product and RIC comparison tables."""

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

if __package__:
    from .audit_cme_ll2_inventory import listing
else:
    from audit_cme_ll2_inventory import listing


def active_records(packages, year):
    records = {}
    for package in packages:
        if int(package["period"][:4]) != year:
            continue
        for product, values in package.get("products", {}).items():
            for record in values["active_rics"]:
                if record["ric"] in records:
                    raise ValueError(f"Overlapping active RIC {record['ric']} in {year}: resolve source provenance before comparing")
                records[record["ric"]] = {**record, "product": product, "source_directory": package["directory"]}
    return records


def compare(normalized, raw, years):
    contracts, products, summaries = [], [], []
    for year in years:
        norm = active_records(normalized["packages"], year)
        raw_packages = [p for p in raw["packages"] if int(p["period"][:4]) == year]
        if len(raw_packages) != 1:
            raise ValueError(f"Expected exactly one Raw package for {year}")
        raw_known = raw_packages[0]["report_present"]
        original = active_records(raw_packages, year) if raw_known else {}
        universe = sorted(set(norm) | set(original))
        year_rows = []
        for ric in universe:
            n, r = norm.get(ric), original.get(ric)
            if n and r and n["product"] != r["product"]:
                raise ValueError(f"Product routing mismatch for {ric}")
            state = "raw_unknown" if not raw_known else "both" if n and r else "raw_only" if r else "normalized_only"
            row = {"year": year, "product": (n or r)["product"], "ric": ric, "coverage": state,
                   "raw_count": r["count"] if r else 0 if raw_known else None,
                   "normalized_count": n["count"] if n else 0,
                   "raw_start_utc": r["start"] if r else None,
                   "raw_end_utc": r["end"] if r else None,
                   "normalized_start_utc": n["start"] if n else None,
                   "normalized_end_utc": n["end"] if n else None,
                   "raw_source_directory": r["source_directory"] if r else None,
                   "normalized_source_directory": n["source_directory"] if n else None}
            contracts.append(row)
            year_rows.append(row)
        for product in sorted({row["product"] for row in year_rows}):
            rows = [row for row in year_rows if row["product"] == product]
            nr = [row for row in rows if row["normalized_count"] > 0]
            rr = [row for row in rows if row["raw_count"] and row["raw_count"] > 0]
            extra = [row for row in rows if row["coverage"] == "raw_only"]
            products.append({"year": year, "product": product,
                             "raw_active_rics": len(rr) if raw_known else None,
                             "normalized_active_rics": len(nr),
                             "both_active_rics": sum(row["coverage"] == "both" for row in rows) if raw_known else None,
                             "raw_only_rics": len(extra) if raw_known else None,
                             "normalized_only_rics": sum(row["coverage"] == "normalized_only" for row in rows) if raw_known else None,
                             "raw_only_count_le_10000": sum(row["raw_count"] <= 10000 for row in extra) if raw_known else None,
                             "raw_only_count_gt_10000": sum(row["raw_count"] > 10000 for row in extra) if raw_known else None,
                             "raw_only_ric_list": " ".join(row["ric"] for row in extra) if raw_known else None,
                             "normalized_ric_list": " ".join(row["ric"] for row in nr)})
        extra = [row for row in year_rows if row["coverage"] == "raw_only"]
        bins = Counter("<=1000" if row["raw_count"] <= 1000 else "1001-10000" if row["raw_count"] <= 10000 else ">10000" for row in extra)
        summaries.append({"year": year, "period": raw_packages[0]["period"],
                          "raw_report_present": raw_known,
                          "raw_active_rics": len(original) if raw_known else None,
                          "normalized_active_rics": len(norm),
                          "raw_active_products": len({r["product"] for r in original.values()}) if raw_known else None,
                          "normalized_active_products": len({r["product"] for r in norm.values()}),
                          "raw_only_rics": len(extra) if raw_known else None,
                          "normalized_only_rics": len(set(norm) - set(original)) if raw_known else None,
                          "raw_extra_pct_of_normalized": len(extra) / len(norm) * 100 if raw_known and norm else None,
                          "raw_only_report_count_bins": dict(bins) if raw_known else None})
    # Make every product visible in every selected year, including known zeros
    # and the unknown Raw year, instead of hiding missing product/year rows.
    all_products = sorted({row["product"] for row in contracts})
    present = {(row["year"], row["product"]) for row in products}
    for summary in summaries:
        year, known = summary["year"], summary["raw_report_present"]
        for product in all_products:
            if (year, product) in present:
                continue
            products.append({"year": year, "product": product,
                             "raw_active_rics": 0 if known else None,
                             "normalized_active_rics": 0,
                             "both_active_rics": 0 if known else None,
                             "raw_only_rics": 0 if known else None,
                             "normalized_only_rics": 0 if known else None,
                             "raw_only_count_le_10000": 0 if known else None,
                             "raw_only_count_gt_10000": 0 if known else None,
                             "raw_only_ric_list": "" if known else None,
                             "normalized_ric_list": ""})
    products.sort(key=lambda row: (row["year"], row["product"]))
    return contracts, products, summaries


def write_csv(path, rows, fieldnames=None):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def markdown_report(summary, products):
    lines = ["# 2024--2026 LL2 全品种 / 合约覆盖", "",
             "本表覆盖全部活跃 source RIC，不限六品种。2024、2025 为年度源包；2026 仅至 2026-06-01 UTC（不含）。2025 Raw 尚无 Report，所有 Raw 计数为未知，不能按零处理。", "",
             "[可搜索查看页](index.html) · [全部合约 CSV](contracts.csv) · [Raw 独有合约 CSV](raw_only_contracts.csv) · [完整品种 CSV](products.csv) · [来源及复核审计](summary.json)", "",
             "## 全源汇总", "", "| 年份 | Raw 活跃 RIC | Normalized 活跃 RIC | Raw 多出 | 多出比例 | Raw product | Normalized product |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for item in summary["years"]:
        values = [item["year"], item["raw_active_rics"], item["normalized_active_rics"], item["raw_only_rics"],
                  f"{item['raw_extra_pct_of_normalized']:.1f}%" if item["raw_extra_pct_of_normalized"] is not None else None,
                  item["raw_active_products"], item["normalized_active_products"]]
        lines.append("| " + " | ".join("未知" if v is None else str(v) for v in values) + " |")
    lines += ["", "Raw Count>0 仅表示有原始记录，可能含状态或停牌；Normalized Count>0 表示规范化盘口行。Raw 多出的 RIC 必须验证有效 BEST_* FID 与重建初始状态后才可用于深度。Count 的阈值仅用于查看分布，不作为可交付盘口判定。", ""]
    for item in summary["years"]:
        if item["raw_only_report_count_bins"] is not None:
            bins = item["raw_only_report_count_bins"]
            lines.append(f"- {item['year']}：仅 Raw 的 {item['raw_only_rics']} 个 RIC 中，Count ≤1,000 为 {bins.get('<=1000',0)} 个，1,001--10,000 为 {bins.get('1001-10000',0)} 个，>10,000 为 {bins.get('>10000',0)} 个。")
    lines += ["", "## 全部逻辑 product", "",
              "同一 RIC 不跨年份推断物理合约身份；product 使用生产 parser 的源代码路由，包含其他交易所、连续报价及别名，不等于已经确认的真实 CME 品种数。0 表示该年度没有活跃 Report RIC。", "",
              "| Product | 2024 Raw | 2024 Norm | 仅 Raw | 2025 Norm（Raw 未知） | 2026 Raw | 2026 Norm | 仅 Raw |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    indexed = {(row["year"], row["product"]): row for row in products}
    for product in sorted({row["product"] for row in products}):
        p24, p25, p26 = [indexed[(year, product)] for year in [2024, 2025, 2026]]
        values = [product, p24["raw_active_rics"], p24["normalized_active_rics"], p24["raw_only_rics"], p25["normalized_active_rics"],
                  p26["raw_active_rics"], p26["normalized_active_rics"], p26["raw_only_rics"]]
        lines.append("| " + " | ".join("未知" if v is None else str(v) for v in values) + " |")
    return "\n".join(lines) + "\n"


HTML = r'''<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>2024–2026 LL2 全合约覆盖</title>
<style>
body{font:15px system-ui,sans-serif;color:#17202a;margin:24px;background:#f5f7fa}h1{font-size:24px}p{max-width:1100px;line-height:1.7}
.controls{display:flex;gap:12px;flex-wrap:wrap;margin:18px 0}input,select,button{font:inherit;padding:8px;border:1px solid #aab7c4;border-radius:5px;background:white}
input{min-width:270px}.wrap{overflow:auto;max-height:65vh;background:white;border:1px solid #d5dde5}table{border-collapse:collapse;white-space:nowrap;width:100%}
th,td{padding:8px 12px;border-bottom:1px solid #e1e5e9;text-align:left}th{position:sticky;top:0;background:#17324d;color:white;cursor:pointer}tbody tr:hover{background:#eef5ff}
.extra{color:#9b3c06;font-weight:600}.unknown{color:#666}.summary{max-height:none;margin:16px 0}.pager{display:flex;gap:14px;align-items:center;margin-top:15px}
</style>
<h1>2024–2026 LL2 全源合约覆盖</h1>
<p>2024、2025 为年度源包；2026 源包仅至 <b>2026-06-01 00:00 UTC（不含）</b>。按 Report 中 Count&gt;0 的原始 RIC 比较，涵盖全部 source product，包含其他交易所和别名。<b>原始记录覆盖不等于可交付十档</b>，Count 不跨 Raw / Normalized 比例比较；记录量筛选只帮助定位待查工具。</p>
<div class="wrap summary"><table id="summary"></table></div>
<p><a href="contracts.csv">全部合约 CSV</a> · <a href="raw_only_contracts.csv">Raw 独有合约 CSV</a> · <a href="products.csv">全部品种 CSV</a> · <a href="summary.json">统计及来源审计</a></p>
<div class="controls">
<select id="view"><option value="contracts">逐合约</option><option value="products">逐品种</option></select>
<select id="year"><option value="">全部年份</option><option>2024</option><option>2025</option><option>2026</option></select>
<input id="search" placeholder="搜索 RIC 或 product，例如 HG、ALI、BP、ES">
<select id="state"><option value="">全部覆盖</option><option value="raw_only">仅 Raw 有记录</option><option value="both">两者都有记录</option><option value="raw_unknown">Raw 未知（2025）</option></select>
<select id="count"><option value="">Raw 独有记录量：全部</option><option value="1000">Raw 独有 Count &gt; 1,000</option><option value="10000">Raw 独有 Count &gt; 10,000</option></select>
<button id="export">导出当前筛选 CSV</button>
</div>
<p id="note"></p><div class="wrap"><table><thead id="head"></thead><tbody id="body"></tbody></table></div>
<div class="pager"><button id="prev">上一页</button><span id="page"></span><button id="next">下一页</button></div>
<script id="data" type="application/json">__PAYLOAD__</script>
<script>
const data=JSON.parse(document.getElementById('data').textContent),$=id=>document.getElementById(id);
const states={both:'两者都有',raw_only:'仅 Raw',normalized_only:'仅 Normalized',raw_unknown:'Raw 未知'};
const cc=[['year','年份'],['product','Product'],['ric','RIC'],['coverage','覆盖'],['raw_count','Raw Count'],['normalized_count','Normalized Count'],['raw_start_utc','Raw 首事件 UTC'],['raw_end_utc','Raw 末事件 UTC'],['normalized_start_utc','Normalized 首事件 UTC'],['normalized_end_utc','Normalized 末事件 UTC']];
const pc=[['year','年份'],['product','Product'],['raw_active_rics','Raw RIC'],['normalized_active_rics','Normalized RIC'],['raw_only_rics','仅 Raw'],['raw_only_count_le_10000','仅 Raw Count ≤1万'],['raw_only_count_gt_10000','仅 Raw Count >1万'],['raw_only_ric_list','仅 Raw 合约'],['normalized_ric_list','Normalized 合约']];
let page=0,rows=[],columns=cc,sortKey='ric',descending=false;
function display(value,key){if(value===null||value===undefined)return '未知 / 无事件';if(key==='coverage')return states[value]||value;return typeof value==='number'?value.toLocaleString():String(value)}
function cell(tag,value){const el=document.createElement(tag);el.textContent=value;return el}
function update(){const products=$('view').value==='products';columns=products?pc:cc;let query=$('search').value.trim().toUpperCase();
rows=(products?data.products:data.contracts).filter(r=>!$('year').value||r.year===Number($('year').value)).filter(r=>!query||[r.product,r.ric,products?r.raw_only_ric_list:'',products?r.normalized_ric_list:''].some(v=>(v||'').toUpperCase().includes(query)));
const state=$('state').value,limit=Number($('count').value);
if(state)rows=rows.filter(r=>products?(state==='raw_only'?r.raw_only_rics>0:state==='both'?r.both_active_rics>0:state==='raw_unknown'?r.raw_active_rics===null:false):r.coverage===state);
if(limit)rows=rows.filter(r=>products?data.contracts.some(c=>c.year===r.year&&c.product===r.product&&c.coverage==='raw_only'&&c.raw_count>limit):r.coverage==='raw_only'&&r.raw_count>limit);
rows.sort((a,b)=>{let x=a[sortKey],y=b[sortKey];let c=typeof x==='number'&&typeof y==='number'?x-y:String(x??'').localeCompare(String(y??''),undefined,{numeric:true});return descending?-c:c});page=0;render()}
function render(){const pages=Math.max(1,Math.ceil(rows.length/100));page=Math.min(page,pages-1);$('head').replaceChildren();let hr=document.createElement('tr');columns.forEach(([key,title])=>{let h=cell('th',title+(sortKey===key?(descending?' ▼':' ▲'):''));h.onclick=()=>{descending=sortKey===key?!descending:false;sortKey=key;update()};hr.appendChild(h)});$('head').appendChild(hr);
$('body').replaceChildren();rows.slice(page*100,(page+1)*100).forEach(r=>{let tr=document.createElement('tr');if(r.coverage==='raw_only')tr.className='extra';if(r.coverage==='raw_unknown')tr.className='unknown';columns.forEach(([key])=>tr.appendChild(cell('td',display(r[key],key))));$('body').appendChild(tr)});
$('note').textContent=`当前筛选 ${rows.length.toLocaleString()} 行。2025 Raw 缺 Report，因此“未知”不能按零处理。点击列标题排序。`;$('page').textContent=`第 ${page+1} / ${pages} 页，每页 100 行`;$('prev').disabled=page===0;$('next').disabled=page+1===pages}
['view','year','state','count'].forEach(id=>$(id).onchange=()=>{sortKey=$('view').value==='products'?'product':'ric';update()});$('search').oninput=update;
$('prev').onclick=()=>{page--;render()};$('next').onclick=()=>{page++;render()};
$('export').onclick=()=>{let keys=Object.keys(rows[0]||{});let escape=v=>{let t=v===null||v===undefined?'':String(v);return '"'+t.replaceAll('"','""')+'"'};let text='\ufeff'+[keys,...rows.map(r=>keys.map(k=>r[k]))].map(r=>r.map(escape).join(',')).join('\r\n');let url=URL.createObjectURL(new Blob([text],{type:'text/csv;charset=utf-8'}));let link=document.createElement('a');link.href=url;link.download='ll2_filtered.csv';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000)};
const sh=document.createElement('tr');['年份 / 源窗口','Raw RIC','Normalized RIC','Raw 多出','多出 / Normalized','Raw product','Normalized product'].forEach(v=>sh.appendChild(cell('th',v)));$('summary').appendChild(sh);
data.summary.years.forEach(r=>{const tr=document.createElement('tr');[r.year===2026?'2026 至5月31日':r.year,r.raw_active_rics,r.normalized_active_rics,r.raw_only_rics,r.raw_extra_pct_of_normalized===null?'未知':r.raw_extra_pct_of_normalized.toFixed(1)+'%',r.raw_active_products,r.normalized_active_products].forEach(v=>tr.appendChild(cell('td',display(v,''))));$('summary').appendChild(tr)});update();
</script></html>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--normalized", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025, 2026])
    parser.add_argument("--verify-remote", action="store_true")
    args = parser.parse_args()
    normalized, raw = (json.loads(p.read_text()) for p in (args.normalized, args.raw))
    contracts, products, summaries = compare(normalized, raw, args.years)
    checks = []
    if args.verify_remote:
        for inventory, kind in [(normalized, "normalized"), (raw, "raw")]:
            for package in inventory["packages"]:
                if int(package["period"][:4]) not in args.years:
                    continue
                directory_url = inventory["remote_root"] + package["directory"] + "/"
                files = listing(directory_url)
                present = "merged-Report.csv.gz" in files
                if present != bool(package.get("report_sha256")):
                    raise ValueError(f"Report availability changed: refresh inventory for {directory_url}")
                report_hash = None
                if present:
                    with urlopen(directory_url + "merged-Report.csv.gz", timeout=30) as response:
                        report_hash = hashlib.sha256(response.read()).hexdigest()
                    if report_hash != package["report_sha256"]:
                        raise ValueError(f"Remote Report changed: refresh inventory for {directory_url}")
                checks.append({"kind": kind, "directory_url": directory_url, "report_present": present,
                               "report_sha256": report_hash, "file_names": files})
    summary = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
               "scope": "Full Report Count>0 RIC union for selected source periods; not verified native depth coverage. 2026 source ends 2026-06-01 exclusive. Raw 2025 unknown.",
               "inputs": [{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in [args.normalized, args.raw]],
               "remote_checks": checks, "years": summaries,
               "contract_year_rows": len(contracts), "raw_only_contract_year_rows": sum(r["coverage"] == "raw_only" for r in contracts),
               "logical_product_union_count": len({r["product"] for r in products})}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "contracts.csv", contracts)
    write_csv(args.output_dir / "products.csv", products)
    write_csv(args.output_dir / "raw_only_contracts.csv", [r for r in contracts if r["coverage"] == "raw_only"], list(contracts[0]))
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    if args.years == [2024, 2025, 2026]:
        (args.output_dir / "coverage.md").write_text(markdown_report(summary, products))
    payload = json.dumps({"contracts": contracts, "products": products, "summary": summary}, ensure_ascii=False).replace("<", "\\u003c")
    (args.output_dir / "index.html").write_text(HTML.replace("__PAYLOAD__", payload))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
