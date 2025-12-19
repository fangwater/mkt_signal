async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const text = await res.text();
  let data = null;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }
  if (!res.ok) {
    const msg = (data && (data.error || data.message)) || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

function fmtTs(ms) {
  if (!ms) return "N/A";
  return new Date(ms).toLocaleString();
}

function setStatusPill(status, error) {
  const el = document.getElementById("pillStatus");
  el.textContent = status || "unknown";
  el.classList.remove("ok", "error", "running");
  if (status === "ok") el.classList.add("ok");
  if (status === "error") el.classList.add("error");
  if (status === "running") el.classList.add("running");
  if (error) el.title = error;
}

function setReadyPill(state, symbolCount, exportTsMs) {
  const el = document.getElementById("pillReady");
  el.classList.remove("ready", "error", "running", "empty");
  if (state === "ready") {
    el.textContent = "READY";
    el.classList.add("ready");
  } else if (state === "exporting") {
    el.textContent = "EXPORTING";
    el.classList.add("running");
  } else if (state === "error") {
    el.textContent = "ERROR";
    el.classList.add("error");
  } else {
    el.textContent = "EMPTY";
    el.classList.add("empty");
  }
  el.title = `symbols=${symbolCount || 0} last_export=${exportTsMs ? fmtTs(exportTsMs) : "N/A"}`;
}

function downloadSelected() {
  const symbol = document.getElementById("symbolSelect").value;
  if (!symbol) return;
  window.location.href = `api/download_symbol?symbol=${encodeURIComponent(symbol)}`;
}

function renderSymbols(meta, keyword) {
  const sel = document.getElementById("symbolSelect");
  sel.innerHTML = "";
  const symbols = (meta && meta.symbols) || [];
  const kw = (keyword || "").trim().toUpperCase();
  const filtered = kw
    ? symbols.filter(
        (s) =>
          (s.symbol_key || "").includes(kw) ||
          (s.originals || []).some((o) => String(o).toUpperCase().includes(kw))
      )
    : symbols;

  document.getElementById("txtSymbolHint").textContent = `symbols=${filtered.length}${kw ? ` (filtered)` : ""}`;

  if (filtered.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "（空）请先 Export All";
    sel.appendChild(opt);
    sel.dispatchEvent(new Event("change"));
    return;
  }

  for (const s of filtered) {
    const opt = document.createElement("option");
    opt.value = s.symbol_key;
    opt.textContent = s.symbol_key;
    sel.appendChild(opt);
  }
  sel.dispatchEvent(new Event("change"));
}

async function refreshReady() {
  const r = await fetchJson("api/ready");
  setReadyPill(r.state, r.symbol_count, r.export_ts_ms);
  setStatusPill(r.job && r.job.status, r.job && r.job.error);
  document.getElementById("txtExportDir").textContent = r.export_dir ? `export_dir=${r.export_dir}` : "";
  document.getElementById("txtApiBase").textContent = r.api_base ? `api_base=${r.api_base}` : "";

  const btnExportAll = document.getElementById("btnExportAll");
  btnExportAll.disabled = r.state === "exporting";
  btnExportAll.textContent = r.state === "exporting" ? "Exporting..." : "Export All";

  const canUse = !!r.data_ready;
  document.getElementById("symbolSelect").disabled = !canUse;
  document.getElementById("btnExportSymbol").disabled = !canUse;

  if (r.state === "ready") {
    document.getElementById("txtExportInfo").textContent = `可用：symbols=${r.symbol_count} last_export=${fmtTs(r.export_ts_ms)}`;
  } else if (r.state === "exporting") {
    document.getElementById("txtExportInfo").textContent = "导出中...";
  } else if (r.state === "error") {
    document.getElementById("txtExportInfo").textContent = "导出失败（可重试 Export All）";
  } else {
    document.getElementById("txtExportInfo").textContent = "未导出：点击 Export All";
  }
  return r;
}

async function refreshSymbols() {
  const meta = await fetchJson("api/symbols");
  renderSymbols(meta, document.getElementById("symbolSearch").value);
  return meta;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function pollUntilNotExporting() {
  for (;;) {
    const r = await refreshReady();
    if (r.state !== "exporting") return r;
    await sleep(1200);
  }
}

async function main() {
  document.getElementById("btnExportAll").addEventListener("click", async () => {
    try {
      await fetchJson("api/export_all", { method: "POST" });
      const r = await pollUntilNotExporting();
      if (r.state === "ready") await refreshSymbols();
    } catch (e) {
      alert(`Export failed: ${e.message}`);
    }
  });

  document.getElementById("btnExportSymbol").addEventListener("click", async () => {
    const symbol = document.getElementById("symbolSelect").value;
    if (!symbol) return;
    const viewerWin = window.open("about:blank", "_blank");
    const btn = document.getElementById("btnExportSymbol");
    const old = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Exporting...";
    try {
      await fetchJson("api/export_symbol", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ symbol }),
      });
      if (viewerWin) viewerWin.location.href = `view?symbol=${encodeURIComponent(symbol)}`;
      downloadSelected();
    } catch (e) {
      if (viewerWin) viewerWin.close();
      alert(`Export symbol failed: ${e.message}`);
    } finally {
      btn.textContent = old;
      btn.disabled = false;
    }
  });

  document.getElementById("symbolSearch").addEventListener("input", async (e) => {
    const kw = e.target.value;
    const meta = await fetchJson("api/symbols");
    renderSymbols(meta, kw);
  });

  document.getElementById("symbolSelect").addEventListener("change", () => {
    const symbol = document.getElementById("symbolSelect").value;
    document.getElementById("btnExportSymbol").textContent = `Export ${symbol || "{symbol}"}_order.parquet`;
  });

  await refreshReady();
  await refreshSymbols();
  const symbol = document.getElementById("symbolSelect").value;
  document.getElementById("btnExportSymbol").textContent = `Export ${symbol || "{symbol}"}_order.parquet`;
}

main();
