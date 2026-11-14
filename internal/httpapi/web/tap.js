"use strict";

const $ = (id) => document.getElementById(id);

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
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

function setMsg(text, cls) {
  $("tap-msg").textContent = text;
  $("tap-msg").className = "msg " + (cls || "");
}

function fmtBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
  return (n / 1048576).toFixed(1) + " MB";
}

function fmtAgo(t) {
  if (!t) return "—";
  const secs = (Date.now() - new Date(t)) / 1000;
  if (secs < 1) return "just now";
  if (secs < 60) return Math.round(secs) + "s ago";
  if (secs < 3600) return Math.round(secs / 60) + "m ago";
  return Math.round(secs / 3600) + "h ago";
}

async function loadPresets() {
  const { presets } = await api("GET", "/api/presets");
  const box = $("presets");
  box.innerHTML = "";
  presets.forEach((p, i) => {
    const div = document.createElement("div");
    div.className = "preset-opt";
    div.innerHTML =
      `<input type="radio" name="preset" id="preset-${esc(p.name)}" value="${esc(p.name)}" ${i === 0 ? "checked" : ""}/>` +
      `<label for="preset-${esc(p.name)}">${esc(p.name)}</label>` +
      `<span class="url">${esc(p.url)}</span>`;
    div.addEventListener("click", () => {
      div.querySelector("input").checked = true;
    });
    box.appendChild(div);
  });
}

// rate tracking from successive polls
const rateState = {};
let selected = null;

function computeRate(t, now) {
  const prev = rateState[t.id];
  rateState[t.id] = { count: t.count, t: now };
  if (!prev) return "—";
  const dt = (now - prev.t) / 1000;
  if (dt <= 0) return "—";
  const rate = (t.count - prev.count) / dt;
  if (rate < 0) return "—";
  return rate >= 100 ? Math.round(rate).toLocaleString() + "/s" : rate.toFixed(1) + "/s";
}

async function refresh() {
  try {
    renderTaps(await api("GET", "/api/taps"));
  } catch {
    /* ignore */
  }
}

function renderTaps(data) {
  const taps = data.taps || [];
  const body = $("taps-body");
  if (taps.length === 0) {
    body.innerHTML = `<tr><td colspan="8" class="empty">No taps. Start one above.</td></tr>`;
    $("samples-card").style.display = "none";
    selected = null;
    return;
  }
  const now = Date.now();
  body.innerHTML = "";
  let selectedTap = null;
  for (const t of taps) {
    if (t.id === selected) selectedTap = t;
    const rate = computeRate(t, now);
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td>${esc(t.subject)}</td>` +
      `<td>${esc(t.env)}</td>` +
      `<td><span class="badge ${esc(t.status)}">${esc(t.status)}</span></td>` +
      `<td class="num strong">${t.count.toLocaleString()}</td>` +
      `<td class="num">${rate}</td>` +
      `<td class="num">${fmtBytes(t.bytes)}</td>` +
      `<td>${t.count > 0 ? fmtAgo(t.last_at) : "nothing yet"}</td>` +
      `<td><button class="small" data-act="msgs" data-id="${esc(t.id)}">Messages</button> ` +
      `<button class="small danger" data-act="stop" data-id="${esc(t.id)}">Stop</button></td>`;
    body.appendChild(tr);
  }
  if (selected && selectedTap) renderSamples(selectedTap);
  else if (selected && !selectedTap) {
    $("samples-card").style.display = "none";
    selected = null;
  }
}

let currentSamples = [];

function renderSamples(t) {
  $("samples-card").style.display = "";
  $("samples-subject").textContent = `· ${t.subject} (${t.env})`;
  const samples = (t.samples || []).slice().reverse();
  currentSamples = samples;
  if (samples.length === 0) {
    $("samples-body").innerHTML = `<tr><td class="empty">No messages observed yet.</td></tr>`;
    return;
  }
  $("samples-body").innerHTML =
    `<thead><tr><th>time</th><th>subject</th><th class="num">size</th><th>preview</th></tr></thead>` +
    samples
      .map(
        (s, i) =>
          `<tr data-idx="${i}"><td>${new Date(s.at).toLocaleTimeString()}</td>` +
          `<td>${esc(s.subject)}</td>` +
          `<td class="num">${fmtBytes(s.size)}</td>` +
          `<td title="click to expand">${esc(s.preview)}</td></tr>`
      )
      .join("");
}

function showMessage(s) {
  $("msg-title").textContent = new Date(s.at).toLocaleString();
  $("msg-meta").textContent =
    `${s.subject} · ${fmtBytes(s.size)}` + (s.capped ? " · payload capped at 512 B for preview" : "");
  $("msg-decoded").textContent = s.detail || s.preview || "(empty)";
  $("msg-hex").textContent = s.hex || "";
  $("msg-modal").classList.remove("hidden");
}

function onSamplesClick(e) {
  const tr = e.target.closest("tr[data-idx]");
  if (!tr) return;
  const s = currentSamples[parseInt(tr.dataset.idx, 10)];
  if (s) showMessage(s);
}

async function startTap() {
  const subject = $("subject").value.trim();
  const preset = document.querySelector("input[name=preset]:checked");
  if (!subject) {
    setMsg("subject is required", "err");
    return;
  }
  $("start-btn").disabled = true;
  try {
    const t = await api("POST", "/api/taps", { subject, env: preset ? preset.value : "" });
    setMsg(`Tapping "${subject}"…`, "ok");
    selected = t.id;
    await refresh();
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    $("start-btn").disabled = false;
  }
}

async function onTapsClick(e) {
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const { act, id } = btn.dataset;
  if (act === "msgs") {
    selected = id;
    await refresh();
    return;
  }
  if (act === "stop") {
    btn.disabled = true;
    try {
      await api("DELETE", `/api/taps/${id}`);
      if (selected === id) selected = null;
    } catch (err) {
      alert(err.message);
    }
    await refresh();
  }
}

$("start-btn").addEventListener("click", startTap);
$("subject").addEventListener("keydown", (e) => { if (e.key === "Enter") startTap(); });
$("taps-body").addEventListener("click", onTapsClick);
$("samples-close").addEventListener("click", () => { selected = null; $("samples-card").style.display = "none"; });
$("samples-body").addEventListener("click", onSamplesClick);
$("msg-close").addEventListener("click", () => $("msg-modal").classList.add("hidden"));
$("msg-modal").addEventListener("click", (e) => { if (e.target.id === "msg-modal") $("msg-modal").classList.add("hidden"); });
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") $("msg-modal").classList.add("hidden");
});

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

function startLive() {
  if (typeof EventSource === "undefined") {
    refresh();
    setInterval(refresh, 1500);
    return;
  }
  refresh();
  let pollTimer = null;
  const es = new EventSource("/api/taps/stream");
  es.onmessage = (e) => {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    try { renderTaps(JSON.parse(e.data)); } catch { /* ignore */ }
  };
  es.onerror = () => {
    if (!pollTimer) pollTimer = setInterval(refresh, 3000);
  };
}

$("logout-btn").addEventListener("click", async () => {
  try { await fetch("/api/logout", { method: "POST" }); } catch { /* ignore */ }
  location.href = "/login.html";
});

applyRole();
loadPresets();
startLive();
