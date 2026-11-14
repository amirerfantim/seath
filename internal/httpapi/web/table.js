"use strict";

const $ = (id) => document.getElementById(id);
const id = new URLSearchParams(location.search).get("id");

let offset = 0;

async function api(path, method, body) {
  const opts = { method: method || "GET", headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

let tableName = "";

// renderGrid builds an HTML table from columns + row objects.
function renderGrid(cols, rows) {
  if (!cols || cols.length === 0 || rows.length === 0) {
    return `<table class="preview"><tbody><tr><td class="empty">No rows.</td></tr></tbody></table>`;
  }
  const head = "<thead><tr>" + cols.map((c) => `<th>${esc(c)}</th>`).join("") + "</tr></thead>";
  let body = "<tbody>";
  for (const row of rows) {
    body += "<tr>" + cols.map((c) => `<td title="${esc(row[c])}">${esc(trunc(row[c], 140))}</td>`).join("") + "</tr>";
  }
  body += "</tbody>";
  return `<table class="preview">${head}${body}</table>`;
}

function esc(v) {
  let s = v === null || v === undefined ? "" : String(v);
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function trunc(v, n) {
  const s = v === null || v === undefined ? "" : String(v);
  return s.length > n ? s.slice(0, n) + "…" : s;
}

// ---- export helpers ----
let lastRows = { cols: [], rows: [] };
let lastQuery = { cols: [], rows: [] };

function toCSV(cols, rows) {
  const esc = (v) => {
    const s = v === null || v === undefined ? "" : String(v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [cols.map(esc).join(",")];
  for (const r of rows) lines.push(cols.map((c) => esc(r[c])).join(","));
  return lines.join("\n");
}

function download(name, text, mime) {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function fmtTime(t) {
  if (!t) return "—";
  const d = new Date(t);
  if (isNaN(d) || d.getFullYear() < 1980) return "—"; // unset / zero timestamp
  return d.toLocaleString();
}

function statCard(label, value) {
  return `<div class="stat"><div class="stat-val">${value}</div><div class="stat-label">${label}</div></div>`;
}

async function loadInsights() {
  let d;
  try {
    d = await api(`/api/recordings/${id}/insights`);
  } catch (e) {
    $("table-title").textContent = "Error";
    $("insights").innerHTML = `<div class="last-error">${esc(e.message)}</div>`;
    return;
  }
  document.title = `${d.table} · Seath`;
  $("table-title").innerHTML = `${esc(d.table)} <span class="badge ${esc(d.status)}">${esc(d.status)}</span>`;
  tableName = d.table;
  $("table-export").href = `/api/recordings/${encodeURIComponent(id)}/export.csv`;
  if (!$("sql").value.trim()) {
    $("sql").value = `SELECT * FROM ${d.table} ORDER BY id DESC LIMIT 50;`;
  }

  let span = "—";
  if (d.first_at && d.last_at) {
    const secs = (new Date(d.last_at) - new Date(d.first_at)) / 1000;
    span = humanDuration(secs);
  }
  const rows = (d.rows_estimated ? "~" : "") + (d.rows ?? 0).toLocaleString();
  $("insights").innerHTML =
    statCard("rows", rows) +
    statCard("size", d.size || "—") +
    statCard("columns", (d.columns || []).length) +
    statCard("created by", esc(d.created_by) || "—") +
    statCard("created at", fmtTime(d.created_at)) +
    statCard("topic", d.topic) +
    statCard("sample rate", d.sample_rate + "%") +
    statCard("nats env", d.nats_env) +
    statCard("time span", span) +
    statCard("first row", fmtTime(d.first_at)) +
    statCard("last row", fmtTime(d.last_at));

  $("col-count").textContent = `· ${(d.columns || []).length} columns`;
  $("schema").innerHTML = (d.columns || [])
    .map((c) => `<div class="schema-col"><span class="cname">${esc(c.name)}</span><span class="ctype">${esc(c.type)}</span></div>`)
    .join("");
}

function humanDuration(secs) {
  if (secs < 60) return Math.round(secs) + "s";
  if (secs < 3600) return Math.round(secs / 60) + "m";
  if (secs < 86400) return (secs / 3600).toFixed(1) + "h";
  return (secs / 86400).toFixed(1) + "d";
}

// Falls back to method + path for entries written before actions were recorded.
async function loadActivity() {
  const body = $("activity-body");
  let d;
  try {
    d = await api(`/api/recordings/${id}/activity?limit=100`);
  } catch (e) {
    body.innerHTML = `<tr><td colspan="5" class="last-error">${esc(e.message)}</td></tr>`;
    return;
  }
  const entries = d.entries || [];
  $("activity-count").textContent = entries.length ? `· last ${entries.length}` : "";
  if (entries.length === 0) {
    body.innerHTML = `<tr><td colspan="5" class="empty">No activity recorded for this recording yet.</td></tr>`;
    return;
  }
  body.innerHTML = entries
    .map((e) => {
      const failed = e.status >= 400;
      const action = e.action || `${e.method} ${e.path}`;
      const detail = e.detail || (failed ? "failed" : "");
      return (
        `<tr><td class="nowrap">${fmtTime(e.ts)}</td>` +
        `<td>${esc(e.username)}</td>` +
        `<td><span class="act-tag${failed ? " failed" : ""}">${esc(action)}</span></td>` +
        `<td class="act-detail">${esc(detail)}</td>` +
        `<td class="num ${failed ? "drop-warn" : ""}">${e.status}</td></tr>`
      );
    })
    .join("");
}

async function loadRows() {
  const limit = parseInt($("page-size").value, 10);
  const order = $("order").value;
  let d;
  try {
    d = await api(`/api/recordings/${id}/rows?limit=${limit}&offset=${offset}&order=${order}`);
  } catch (e) {
    $("rows-body").innerHTML = `<tr><td class="last-error">${esc(e.message)}</td></tr>`;
    return;
  }
  const cols = d.columns || [];
  const rows = d.rows || [];
  lastRows = { cols, rows };
  $("rows-csv").disabled = rows.length === 0;
  $("rows-area").innerHTML = renderGrid(cols, rows);

  const start = rows.length ? offset + 1 : 0;
  $("page-info").textContent = `rows ${start}–${offset + rows.length}`;
  $("prev").disabled = offset === 0;
  $("next").disabled = rows.length < limit;
}

async function runQuery() {
  const sql = $("sql").value.trim();
  if (!sql) return;
  const btn = $("run-query");
  btn.disabled = true;
  $("query-msg").textContent = "Running…";
  $("query-msg").className = "msg";
  try {
    const t0 = performance.now();
    const d = await api("/api/query", "POST", { sql });
    const ms = Math.round(performance.now() - t0);
    lastQuery = { cols: d.columns || [], rows: d.rows || [] };
    $("query-csv").disabled = lastQuery.rows.length === 0;
    $("query-json").disabled = lastQuery.rows.length === 0;
    $("query-result").innerHTML = renderGrid(d.columns, d.rows);
    const note = d.truncated ? " · result truncated" : "";
    $("query-msg").textContent = `${d.row_count.toLocaleString()} row${d.row_count === 1 ? "" : "s"}${note} · ${ms} ms`;
    $("query-msg").className = "msg ok";
  } catch (e) {
    $("query-result").innerHTML = "";
    $("query-msg").textContent = e.message;
    $("query-msg").className = "msg err";
  } finally {
    btn.disabled = false;
  }
}

$("run-query").addEventListener("click", runQuery);
$("sql").addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault();
    runQuery();
  }
});

$("order").addEventListener("change", () => { offset = 0; loadRows(); });
$("page-size").addEventListener("change", () => { offset = 0; loadRows(); });
$("prev").addEventListener("click", () => { offset = Math.max(0, offset - parseInt($("page-size").value, 10)); loadRows(); });
$("next").addEventListener("click", () => { offset += parseInt($("page-size").value, 10); loadRows(); });
$("refresh").addEventListener("click", () => { loadInsights(); loadRows(); loadActivity(); });
$("rows-csv").addEventListener("click", () =>
  download(`${tableName || "rows"}-${stamp()}.csv`, toCSV(lastRows.cols, lastRows.rows), "text/csv"));
$("query-csv").addEventListener("click", () =>
  download(`query-${stamp()}.csv`, toCSV(lastQuery.cols, lastQuery.rows), "text/csv"));
$("query-json").addEventListener("click", () =>
  download(`query-${stamp()}.json`, JSON.stringify(lastQuery.rows, null, 2), "application/json"));

async function applyRole() {
  try {
    const me = await api(`/api/me`);
    const badge = $("role-badge");
    if (badge) {
      badge.textContent = me.role;
      badge.classList.toggle("admin", me.role === "admin");
    }
    if (me.role !== "admin") document.body.classList.add("role-viewer");
  } catch { /* ignore */ }
}

async function loadQueryConfig() {
  try {
    const cfg = await api("/api/config");
    $("query-limits").textContent = `· read-only · ${cfg.query_timeout_seconds}s timeout · max ${cfg.query_max_rows.toLocaleString()} rows`;
  } catch { /* keep the server default shown in the page */ }
}

$("logout-btn").addEventListener("click", async () => {
  try { await fetch("/api/logout", { method: "POST" }); } catch { /* ignore */ }
  location.href = "/login.html";
});

applyRole();
loadQueryConfig();
if (!id) {
  $("table-title").textContent = "Missing recording id";
} else {
  loadInsights();
  loadRows();
  loadActivity();
}
