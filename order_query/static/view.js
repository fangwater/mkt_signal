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
  const cols = (arrowTable.schema && arrowTable.schema.fields ? arrowTable.schema.fields : []).map((f) => f.name);

  function getColAt(i) {
    if (typeof arrowTable.getColumnAt === "function") return arrowTable.getColumnAt(i);
    if (typeof arrowTable.getChildAt === "function") return arrowTable.getChildAt(i);
    if (typeof arrowTable.getChild === "function") return arrowTable.getChild(cols[i]);
    throw new Error("Arrow result: cannot access column vectors");
  }

  const vectors = cols.map((_, i) => getColAt(i));
  const rows = [];
  const numRows = typeof arrowTable.numRows === "number" ? arrowTable.numRows : arrowTable.length;
  for (let r = 0; r < numRows; r++) {
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
  const bundle = bundles.mvp || (await duckdb.selectBundle(bundles));

  async function toBlobUrl(url) {
    const res = await fetch(url, { mode: "cors" });
    if (!res.ok) throw new Error(`fetch worker failed: HTTP ${res.status}`);
    const code = await res.text();
    const blob = new Blob([code], { type: "text/javascript" });
    return URL.createObjectURL(blob);
  }

  // Avoid cross-origin classic worker restrictions by creating same-origin blob workers.
  const mainWorkerUrl = await toBlobUrl(bundle.mainWorker);
  const pthreadWorkerUrl = bundle.pthreadWorker ? await toBlobUrl(bundle.pthreadWorker) : null;

  const worker = new Worker(mainWorkerUrl);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, pthreadWorkerUrl);
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
    const head = await conn.query("SELECT * FROM parquet_scan('" + fname + "') LIMIT 0");
    const cols = head.schema.fields.map((f) => f.name);
    const candidates = ["local_ts", "update_ts", "create_ts", "ts_us", "key"];
    const sortCol = candidates.find((c) => cols.includes(c));
    const orderBy = sortCol ? ` ORDER BY ${sortCol} DESC ` : " ";
    const q = "SELECT * FROM parquet_scan('" + fname + "')" + orderBy + "LIMIT 100";
    const data = await conn.query(q);
    return { table: data, sortCol: sortCol || "" };
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
    const out = await queryLatest100(db, parquetUrl);
    const { columns, rows } = toRowObjects(out.table);
    document.getElementById("txtHint").textContent = `rows=${rows.length} order_by=${out.sortCol || "N/A"} desc`;
    renderTable(columns, rows);
    setPill("READY", "ready");
  } catch (e) {
    setPill("ERROR", "error");
    document.getElementById("txtHint").textContent = String(e && e.message ? e.message : e);
  }
}

main();
