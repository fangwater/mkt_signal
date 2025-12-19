function qs() {
  return new URLSearchParams(window.location.search || "");
}

function setPill(text, cls) {
  const el = document.getElementById("pill");
  el.textContent = text;
  el.classList.remove("ok", "error", "running", "ready", "empty");
  if (cls) el.classList.add(cls);
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderTable(columns, rows) {
  const tbl = document.getElementById("tbl");
  if (!columns || columns.length === 0) {
    tbl.innerHTML = "<tr><td class='muted'>（empty）</td></tr>";
    return;
  }
  const thead =
    "<thead><tr>" +
    columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("") +
    "</tr></thead>";
  const bodyRows = rows
    .map((r) => {
      const tds = columns.map((c) => `<td>${escapeHtml(r[c] ?? "")}</td>`).join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");
  tbl.innerHTML = thead + `<tbody>${bodyRows}</tbody>`;
}

function toRowObjects(arrowTable) {
  const cols = arrowTable.schema.fields.map((f) => f.name);
  const vectors = cols.map((_, i) => arrowTable.getColumnAt(i));
  const rows = [];
  for (let r = 0; r < arrowTable.numRows; r++) {
    const obj = {};
    for (let c = 0; c < cols.length; c++) {
      let v = vectors[c].get(r);
      if (typeof v === "bigint") v = v.toString();
      obj[cols[c]] = v == null ? "" : v;
    }
    rows.push(obj);
  }
  return { columns: cols, rows };
}

async function loadDuckdb() {
  // Requires outbound network access from the browser to jsdelivr.
  const duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser.mjs");
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

async function queryLatest100(db, parquetUrl) {
  const resp = await fetch(parquetUrl);
  if (!resp.ok) throw new Error(`fetch parquet failed: HTTP ${resp.status}`);
  const buf = new Uint8Array(await resp.arrayBuffer());
  const fname = "data.parquet";
  await db.registerFileBuffer(fname, buf);
  const conn = await db.connect();
  try {
    const q = "SELECT * FROM parquet_scan('" + fname + "') ORDER BY local_ts DESC LIMIT 100";
    return await conn.query(q);
  } finally {
    await conn.close();
  }
}

async function main() {
  const symbol = (qs().get("symbol") || "").trim();
  if (!symbol) {
    setPill("missing symbol", "error");
    document.getElementById("txtInfo").textContent = "missing ?symbol=";
    return;
  }

  document.getElementById("subtitle").textContent = `DuckDB-WASM 读取 ${symbol}_order.parquet，展示最新 100 行`;
  const parquetUrl = `api/download_symbol?symbol=${encodeURIComponent(symbol)}`;
  document.getElementById("lnkDownload").href = parquetUrl;
  document.getElementById("btnReload").addEventListener("click", () => window.location.reload());

  setPill("loading wasm", "running");
  document.getElementById("txtInfo").textContent = `symbol=${symbol}`;

  try {
    const db = await loadDuckdb();
    setPill("querying", "running");
    const table = await queryLatest100(db, parquetUrl);
    const { columns, rows } = toRowObjects(table);
    document.getElementById("txtHint").textContent = `rows=${rows.length} order_by=local_ts desc`;
    renderTable(columns, rows);
    setPill("READY", "ready");
  } catch (e) {
    setPill("ERROR", "error");
    document.getElementById("txtHint").textContent = String(e && e.message ? e.message : e);
  }
}

main();

