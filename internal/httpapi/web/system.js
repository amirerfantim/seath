"use strict";

const $ = (id) => document.getElementById(id);

async function api(method, path) {
  const res = await fetch(path, { method });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

function fmtBytes(n) {
  if (n === null || n === undefined) return "—";
  if (n < 1024) return n + " B";
  const u = ["KB", "MB", "GB", "TB", "PB"];
  let v = n / 1024, i = 0;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(v >= 100 ? 0 : 1) + " " + u[i];
}

function fmtUptime(secs) {
  secs = Math.floor(secs);
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  if (d) return `${d}d ${h}h ${m}m`;
  if (h) return `${h}h ${m}m ${s}s`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}

// setGauge fills a bar and colors it by usage threshold. When the limit is
// unknown (0/unlimited) it shows the raw usage without a percentage fill.
function setGauge(prefix, percent, hasLimit, sub) {
  const bar = $(prefix + "-bar");
  const pctEl = $(prefix + "-pct");
  $(prefix + "-sub").innerHTML = sub;
  if (!hasLimit) {
    pctEl.textContent = "—";
    bar.style.width = "0%";
    bar.className = "gauge-fill";
    return;
  }
  const p = Math.max(0, Math.min(100, percent));
  pctEl.textContent = p.toFixed(p >= 99.5 || p < 10 ? 1 : 0) + "%";
  bar.style.width = p + "%";
  bar.className = "gauge-fill" + (p >= 90 ? " crit" : p >= 75 ? " warn" : "");
}

function stat(label, value) {
  return `<div class="stat"><div class="stat-val">${value}</div><div class="stat-label">${label}</div></div>`;
}

function esc(v) {
  const s = v === null || v === undefined ? "" : String(v);
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function fmtRate(n) {
  if (n === null || n === undefined) return "—";
  return (n >= 100 ? Math.round(n).toLocaleString() : n.toFixed(1)) + "/s";
}

// prevTotals tracks the last recorder totals so throughput can be derived from
// successive polls; prevIO does the same for PG disk I/O bytes.
let prevTotals = null;
let prevIO = null;

async function refresh() {
  let d;
  try {
    d = await api("GET", "/api/system");
  } catch (e) {
    $("sys-msg").textContent = "· " + e.message;
    return;
  }
  $("sys-msg").textContent = "";
  const r = d.resources || {};

  // CPU
  const cpuLimit = r.cpu_limit_cores > 0;
  setGauge(
    "cpu",
    r.cpu_percent,
    cpuLimit,
    `${(r.cpu_cores || 0).toFixed(3)} / ${cpuLimit ? r.cpu_limit_cores + " cores" : `<span class="unlim">${r.num_cpu} cores (no limit)</span>`}`
  );

  // Memory
  const memLimit = r.memory_limit_bytes > 0;
  setGauge(
    "mem",
    r.memory_percent,
    memLimit,
    `${fmtBytes(r.memory_bytes)} / ${memLimit ? fmtBytes(r.memory_limit_bytes) : `<span class="unlim">no limit</span>`}`
  );

  // CPU throttling appended to the CPU gauge sub when it's biting.
  if (r.cpu_throttled_percent > 0.05) {
    $("cpu-sub").innerHTML +=
      ` · <span class="unlim">throttled ${r.cpu_throttled_percent.toFixed(1)}%</span>`;
  }

  // PostgreSQL host. Cache hit + connections come from standard pg_stat (always
  // present); CPU/mem/disk/IO need the system_stats extension.
  const pg = d.pg_host || {};
  const now0 = Date.now();
  if (pg.available) {
    $("pghost-card").style.display = "";
    $("pghost-meta").textContent = `· ${pg.cores} cores · load ${(pg.load_1 || 0).toFixed(2)} / ${(pg.load_5 || 0).toFixed(2)} / ${(pg.load_15 || 0).toFixed(2)}`;
    setGauge("pgcpu", pg.cpu_used_percent, true,
      `${(pg.cpu_used_percent || 0).toFixed(1)}% of ${pg.cores} cores · iowait ${(pg.iowait_percent || 0).toFixed(1)}%`);
    // Bar reflects real pressure (excludes reclaimable page cache); the cache is
    // shown separately so a cache-filled host doesn't look memory-starved.
    const memReal = pg.mem_used_real_bytes ?? pg.mem_used_bytes;
    const memPct = pg.mem_total_bytes > 0 ? (memReal / pg.mem_total_bytes) * 100 : 0;
    setGauge("pgmem", memPct, pg.mem_total_bytes > 0,
      `${fmtBytes(memReal)} used · ${fmtBytes(pg.cache_bytes)} cache (reclaimable) · ${fmtBytes(pg.mem_total_bytes)} total` +
      (pg.swap_total_bytes > 0 ? ` · swap ${fmtBytes(pg.swap_used_bytes)}/${fmtBytes(pg.swap_total_bytes)}` : ""));
    const diskPct = pg.disk_total_bytes > 0 ? (pg.disk_used_bytes / pg.disk_total_bytes) * 100 : 0;
    setGauge("pgdisk", diskPct, pg.disk_total_bytes > 0,
      `${fmtBytes(pg.disk_used_bytes)} used · ${fmtBytes(pg.disk_free_bytes)} free of ${fmtBytes(pg.disk_total_bytes)}`);

    // Disk I/O rate derived from cumulative bytes across polls.
    let rdRate = null, wrRate = null;
    if (prevIO) {
      const dt = (now0 - prevIO.t) / 1000;
      if (dt > 0) {
        rdRate = Math.max(0, (pg.disk_read_bytes - prevIO.rd) / dt);
        wrRate = Math.max(0, (pg.disk_write_bytes - prevIO.wr) / dt);
      }
    }
    prevIO = { rd: pg.disk_read_bytes || 0, wr: pg.disk_write_bytes || 0, t: now0 };

    const iowaitCls = (pg.iowait_percent || 0) >= 20 ? "drop-warn" : "";
    $("pghost-stats").innerHTML =
      stat("cache hit", `${(pg.cache_hit_percent || 0).toFixed(1)}%`) +
      stat("connections", `${pg.conn_active} active · ${pg.conn_total} / ${pg.conn_max}`) +
      stat("disk read /s", rdRate === null ? "—" : fmtBytes(rdRate) + "/s") +
      stat("disk write /s", wrRate === null ? "—" : fmtBytes(wrRate) + "/s") +
      stat("cpu iowait", `<span class="${iowaitCls}">${(pg.iowait_percent || 0).toFixed(1)}%</span>`);
  } else {
    $("pghost-card").style.display = "none";
  }

  // Recording health — aggregate counters + throughput derived from poll deltas.
  const tot = d.recorder_totals || {};
  const now = Date.now();
  let recvRate = null, writeRate = null, dropRate = null;
  if (prevTotals) {
    const dt = (now - prevTotals.t) / 1000;
    if (dt > 0) {
      recvRate = Math.max(0, (tot.received - prevTotals.received) / dt);
      writeRate = Math.max(0, (tot.written - prevTotals.written) / dt);
      dropRate = Math.max(0, (tot.dropped - prevTotals.dropped) / dt);
    }
  }
  prevTotals = { received: tot.received || 0, written: tot.written || 0, dropped: tot.dropped || 0, t: now };

  const fill = tot.max_buffer_fill_percent || 0;
  const fillCls = fill >= 90 ? "drop-warn" : "";
  const dropped = tot.dropped || 0;
  $("rechealth").innerHTML =
    stat("received /s", fmtRate(recvRate)) +
    stat("written /s", fmtRate(writeRate)) +
    stat("dropped (total)", `<span class="${dropped ? "drop-warn" : ""}">${dropped.toLocaleString()}</span>`) +
    stat("dropped /s", fmtRate(dropRate)) +
    stat("errors (total)", (tot.errors || 0).toLocaleString()) +
    stat("max buffer fill", `<span class="${fillCls}">${fill.toFixed(1)}%</span>`);

  // NATS connection states across running recorders.
  const nats = tot.nats || {};
  const natsStr = Object.keys(nats).length
    ? Object.entries(nats).map(([k, v]) => `${v} ${k.toLowerCase()}`).join(" · ")
    : "no running recorders";
  $("nats-state").textContent = "· NATS: " + natsStr;

  // Workload & database. Seath's own footprint = sum of recording-table sizes,
  // distinct from the whole (shared) database size.
  const rec = d.recordings || {};
  const pool = d.db_pool || {};
  const db = d.database || {};
  let seathBytes = null;
  let recordings = [];
  try {
    ({ recordings = [] } = await api("GET", "/api/recordings"));
    seathBytes = recordings.reduce((sum, r) => sum + (r.table_size_bytes || 0), 0);
  } catch { /* ignore */ }
  renderTopTables(recordings);
  $("workload").innerHTML =
    stat("recordings", rec.total ?? "—") +
    stat("running", rec.running ?? "—") +
    stat("active taps", rec.taps ?? "—") +
    stat("seath tables", seathBytes === null ? "—" : fmtBytes(seathBytes)) +
    stat("database total", db.database_size || "—") +
    stat("db conns (in use / open)", `${pool.in_use ?? "—"} / ${pool.open ?? "—"}`);

  // Runtime
  $("runtime").innerHTML =
    stat("uptime", fmtUptime(r.uptime_seconds || 0)) +
    stat("goroutines", (r.goroutines ?? 0).toLocaleString()) +
    stat("heap in use", fmtBytes(r.heap_alloc_bytes)) +
    stat("heap reserved", fmtBytes(r.heap_sys_bytes)) +
    stat("gc cycles", (r.gc_cycles ?? 0).toLocaleString()) +
    stat("go version", (r.go_version || "—").replace(/^go/, ""));
}

// renderTopTables shows the recordings with the largest tables.
function renderTopTables(recordings) {
  const top = (recordings || [])
    .filter((r) => (r.table_size_bytes || 0) > 0)
    .sort((a, b) => (b.table_size_bytes || 0) - (a.table_size_bytes || 0))
    .slice(0, 8);
  const el = $("toptables");
  $("toptables-count").textContent = top.length ? `· top ${top.length}` : "";
  if (top.length === 0) {
    el.innerHTML = `<tbody><tr><td class="empty">No tables with data yet.</td></tr></tbody>`;
    return;
  }
  el.innerHTML =
    `<thead><tr><th>table</th><th>topic</th><th class="num">rows</th><th class="num">size</th></tr></thead><tbody>` +
    top
      .map((r) => {
        const rows = (r.rows_estimated ? "~" : "") + (r.table_rows || 0).toLocaleString();
        return `<tr><td>${esc(r.table)}</td><td>${esc(r.topic)}</td>` +
          `<td class="num">${rows}</td><td class="num strong">${esc(r.table_size || "—")}</td></tr>`;
      })
      .join("") +
    `</tbody>`;
}

async function applyRole() {
  try {
    const me = await api("GET", "/api/me");
    const badge = $("role-badge");
    if (badge) {
      badge.textContent = me.role;
      badge.classList.toggle("admin", me.role === "admin");
    }
    if (me.role !== "admin") document.body.classList.add("role-viewer");
  } catch { /* ignore */ }
}

$("logout-btn").addEventListener("click", async () => {
  try { await fetch("/api/logout", { method: "POST" }); } catch { /* ignore */ }
  location.href = "/login.html";
});

applyRole();
refresh();
setInterval(refresh, 2000);
