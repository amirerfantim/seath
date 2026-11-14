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

// Escapes for attribute context too: these land inside data-* and title=.
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
  $("map-msg").textContent = text;
  $("map-msg").className = "msg " + (cls || "");
}

let knownTypes = new Set();
let allTypes = [];

async function loadModules() {
  let d;
  try {
    d = await api("GET", "/api/proto/modules");
  } catch (e) {
    return;
  }
  const modules = d.modules || [];
  const history = d.history || [];
  const staged = history.filter((m) => !m.active).length;
  $("type-count").textContent = String(d.type_count);
  $("module-count").textContent = String(modules.length);
  $("staged-count").textContent = String(staged);
  $("catalogue-source").textContent = d.compiled_fallback
    ? "Using the temporary compiled catalogue until the first database module is activated."
    : "Loaded from this instance’s database. Change versions without rebuilding or redeploying Seath.";
  $("catalogue-mode").textContent = d.compiled_fallback ? "Migration mode" : "Dynamic";
  $("catalogue-mode").className = `mode-pill ${d.compiled_fallback ? "migration" : "dynamic"}`;
  const rows = modules
    .map((m) => `<article class="module-tile"><div class="module-mark">P</div><div class="module-copy">` +
      `<span title="${esc(m.path)}">${esc(m.path)}</span><strong>${esc(m.version)}</strong></div>` +
      `<span class="live-dot" title="Active"></span></article>`)
    .join("");
  $("modules").innerHTML = rows || `<div class="module-empty">No active database modules yet.</div>`;

  $("proto-history").innerHTML = history.length
    ? history.map((m) => {
        const state = m.active ? `<span class="badge running">active</span>` : `<span class="badge stopped">staged</span>`;
        const action = m.active ? "" : `<button class="small accent" data-proto-activate="${m.id}">Activate</button>`;
        const imported = m.created_at ? new Date(m.created_at).toLocaleString() : "—";
        return `<tr><td>${esc(m.path)}</td><td class="strong">${esc(m.version)}</td>` +
          `<td>${state}</td><td class="mono" title="${esc(m.sha256)}">${esc(m.sha256.slice(0, 12))}…</td>` +
          `<td class="nowrap">${esc(imported)}</td><td class="col-act">${action}</td></tr>`;
      }).join("")
    : `<tr><td colspan="6" class="empty">No descriptor bundles imported yet.</td></tr>`;
}

async function importProtoModule() {
  const path = $("proto-module-path").value.trim();
  const version = $("proto-module-version").value.trim();
  const file = $("proto-descriptor-set").files[0];
  if (!path || !version || !file) {
    setMsgEl("proto-import-msg", "module, version and descriptor file are required", "err");
    return;
  }
  const form = new FormData();
  form.append("module_path", path);
  form.append("version", version);
  form.append("descriptor_set", file);
  $("proto-import").disabled = true;
  try {
    const res = await fetch("/api/proto/modules", { method: "POST", body: form });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || res.statusText);
    setMsgEl("proto-import-msg", `Staged ${data.path}@${data.version}. Review it below, then activate.`, "ok");
    $("proto-descriptor-set").value = "";
    $("proto-file-name").textContent = "Choose descriptor bundle";
    await loadModules();
  } catch (err) {
    setMsgEl("proto-import-msg", err.message, "err");
  } finally {
    $("proto-import").disabled = false;
  }
}

async function activateProtoModule(id) {
  if (!confirm("Activate this module version? Existing recordings will keep their pinned proto schemas; new recordings will use the activated catalogue.")) return;
  try {
    const result = await api("PUT", `/api/proto/modules/${id}/activate`);
    const invalid = result.invalid_mappings || [];
    const suffix = invalid.length ? ` ${invalid.length} topic mapping(s) now reference missing types.` : "";
    setMsgEl("proto-import-msg", `Activated ${result.module.path}@${result.module.version}.${suffix}`, invalid.length ? "" : "ok");
    await loadModules();
    await loadTypes();
  } catch (err) {
    setMsgEl("proto-import-msg", err.message, "err");
  }
}

async function onProtoHistoryClick(e) {
  const button = e.target.closest("button[data-proto-activate]");
  if (button) await activateProtoModule(button.dataset.protoActivate);
}

function onProtoFileChange() {
  const file = $("proto-descriptor-set").files[0];
  $("proto-file-name").textContent = file ? file.name : "Choose descriptor bundle";
}

async function loadTypes() {
  let d;
  try {
    d = await api("GET", "/api/proto/types");
  } catch (e) {
    return;
  }
  allTypes = d.types || [];
  knownTypes = new Set(allTypes.map((t) => t.name));
}

const MAX_SUGGESTIONS = 60;

// searchTypes ranks by where the query hits: the short name first, then the
// package path. Someone typing "event" wants ExampleEvent, not every type
// whose package happens to contain it.
function searchTypes(q) {
  q = q.trim().toLowerCase();
  if (!q) return allTypes.slice(0, MAX_SUGGESTIONS);
  const hits = [];
  for (const t of allTypes) {
    const full = t.name.toLowerCase();
    const short = full.slice(full.lastIndexOf(".") + 1);
    let rank;
    if (short === q) rank = 0;
    else if (short.startsWith(q)) rank = 1;
    else if (short.includes(q)) rank = 2;
    else if (full.startsWith(q)) rank = 3;
    else if (full.includes(q)) rank = 4;
    else continue;
    hits.push({ t, rank });
  }
  hits.sort((a, b) => a.rank - b.rank || a.t.name.length - b.t.name.length || (a.t.name < b.t.name ? -1 : 1));
  return hits.slice(0, MAX_SUGGESTIONS).map((h) => h.t);
}

function markMatch(name, q) {
  q = q.trim();
  if (!q) return esc(name);
  const i = name.toLowerCase().indexOf(q.toLowerCase());
  if (i < 0) return esc(name);
  return esc(name.slice(0, i)) + "<b>" + esc(name.slice(i, i + q.length)) + "</b>" + esc(name.slice(i + q.length));
}

// attachTypePicker turns an input into a searchable proto-type combobox. The
// native <datalist> could not do this: setting an option label makes the
// browser show the label instead of the type name, so the list read "3 fields,
// 5 fields, ..." with the names invisible.
//
// One menu lives on <body>, positioned fixed. Rendering it next to the input
// instead would put it inside a .card (whose backdrop-filter makes a stacking
// context the next card paints over) or inside .table-wrap (which clips on
// overflow) — in both places it came out cut off.
let comboMenu = null;

function ensureComboMenu() {
  if (!comboMenu) {
    comboMenu = document.createElement("div");
    comboMenu.className = "combo-menu hidden";
    document.body.appendChild(comboMenu);
  }
  return comboMenu;
}

function attachTypePicker(input, onPick) {
  const menu = ensureComboMenu();
  let items = [];
  let active = -1;
  let open = false;

  function place() {
    const r = input.getBoundingClientRect();
    menu.style.left = `${r.left}px`;
    menu.style.top = `${r.bottom + 6}px`;
    menu.style.width = `${r.width}px`;
  }

  function close() {
    if (!open) return;
    open = false;
    active = -1;
    menu.classList.add("hidden");
    window.removeEventListener("scroll", place, true);
    window.removeEventListener("resize", place);
  }

  function render() {
    const q = input.value;
    items = searchTypes(q);
    menu.innerHTML = items.length
      ? items
          .map(
            (t, i) =>
              `<div class="combo-item${i === active ? " active" : ""}" data-i="${i}">` +
              `<span class="combo-name">${markMatch(t.name, q)}</span>` +
              `<span class="combo-meta">${t.fields} field${t.fields === 1 ? "" : "s"}</span></div>`
          )
          .join("")
      : `<div class="combo-empty">No proto type matches \u201c${esc(q.trim())}\u201d</div>`;
    menu.classList.remove("hidden");
    place();
    if (!open) {
      open = true;
      window.addEventListener("scroll", place, true);
      window.addEventListener("resize", place);
    }
    menu.onmousedown = (e) => {
      const el = e.target.closest(".combo-item");
      if (el) { e.preventDefault(); pick(Number(el.dataset.i)); }
    };
  }

  function pick(i) {
    if (i < 0 || i >= items.length) return;
    input.value = items[i].name;
    close();
    if (onPick) onPick(items[i].name);
  }

  input.addEventListener("input", () => { active = -1; render(); });
  input.addEventListener("focus", render);
  input.addEventListener("blur", () => setTimeout(close, 150));
  input.addEventListener("keydown", (e) => {
    if (!open) return;
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      active = Math.min(items.length - 1, Math.max(0, active + (e.key === "ArrowDown" ? 1 : -1)));
      render();
      menu.querySelector(".combo-item.active")?.scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter" && active >= 0) {
      e.preventDefault();
      pick(active);
    } else if (e.key === "Escape") {
      close();
    }
  });
}

const PAGE_SIZE = 12;

let mappings = [];
let mapPage = 0;
let editing = null; // topic currently open in the inline editor

async function loadMappings() {
  let d;
  try {
    d = await api("GET", "/api/topic-mappings");
  } catch (e) {
    return;
  }
  mappings = d.mappings || [];
  renderMappings();
}

// Case-insensitive substring over both columns.
function visibleMappings() {
  const q = $("map-search").value.trim().toLowerCase();
  if (!q) return mappings;
  return mappings.filter(
    (m) => m.topic.toLowerCase().includes(q) || m.proto_name.toLowerCase().includes(q)
  );
}

function renderMappings() {
  const rows = visibleMappings();
  const pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  if (mapPage >= pages) mapPage = pages - 1;
  const start = mapPage * PAGE_SIZE;
  const page = rows.slice(start, start + PAGE_SIZE);

  $("map-count").textContent = `\u00b7 ${mappings.length} topics`;
  $("map-range").textContent = rows.length
    ? `showing ${start + 1}\u2013${start + page.length} of ${rows.length}`
    : "no matches";
  $("map-page").textContent = `${mapPage + 1} / ${pages}`;
  $("map-prev").disabled = mapPage === 0;
  $("map-next").disabled = mapPage >= pages - 1;
  $("map-clear").classList.toggle("hidden", !$("map-search").value);

  const body = $("mappings-body");
  if (page.length === 0) {
    body.innerHTML = `<tr><td colspan="3" class="empty">No topic matches this search.</td></tr>`;
    return;
  }
  body.innerHTML = page.map(rowHTML).join("");
  const input = $("edit-type");
  if (input) attachTypePicker(input);
}

// Every mapping is a database row, so every row gets the same two actions.
function rowHTML(m) {
  if (editing !== m.topic) {
    return (
      `<tr><td class="strong">${esc(m.topic)}</td><td>${esc(m.proto_name)}</td>` +
      `<td class="col-act"><div class="rowactions">` +
      `<button class="small accent" data-edit="${esc(m.topic)}">Edit</button>` +
      `<button class="small danger" data-topic="${esc(m.topic)}">Delete</button>` +
      `</div></td></tr>`
    );
  }
  // The row becomes the form, keeping the topic next to its replacement.
  return (
    `<tr class="editing"><td class="strong">${esc(m.topic)}</td>` +
    `<td><input id="edit-type" class="edit-input" value="${esc(m.proto_name)}" ` +
    `placeholder="proto type" autocomplete="off" /></td>` +
    `<td class="col-act"><div class="rowactions">` +
    `<button class="small primary" data-save="${esc(m.topic)}">Save</button>` +
    `<button class="small" data-cancel="1">Cancel</button>` +
    `</div></td></tr>` +
    `<tr class="editing"><td colspan="3" class="edit-note" id="edit-msg">` +
    `Currently <b>${esc(m.proto_name)}</b>.</td></tr>`
  );
}

async function previewType() {
  const name = $("map-type").value.trim();
  if (!knownTypes.has(name)) {
    $("type-preview").classList.add("hidden");
    return;
  }
  let d;
  try {
    d = await api("GET", `/api/proto/types/${encodeURIComponent(name)}/schema`);
  } catch (e) {
    $("type-preview").classList.add("hidden");
    return;
  }
  const cols = d.columns || [];
  $("preview-count").textContent = `· ${cols.length} columns`;
  $("preview-schema").innerHTML = cols
    .map(
      (c) =>
        `<div class="schema-col"><span class="cname">${esc(c.column)}</span><span class="ctype">${esc(c.type)}</span></div>`
    )
    .join("");
  $("type-preview").classList.remove("hidden");
}

async function addMapping() {
  const topic = $("map-topic").value.trim();
  const proto_name = $("map-type").value.trim();
  if (!topic || !proto_name) {
    setMsg("topic and proto type are required", "err");
    return;
  }
  $("add-btn").disabled = true;
  try {
    await api("POST", "/api/topic-mappings", { topic, proto_name });
    setMsg(`Added — "${topic}" is now selectable in the recording builder.`, "ok");
    $("map-topic").value = "";
    $("map-type").value = "";
    $("type-preview").classList.add("hidden");
    // Show what was just added without making the reader hunt for its page.
    $("map-search").value = topic;
    mapPage = 0;
    await loadMappings();
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    $("add-btn").disabled = false;
  }
}

function setEditMsg(text, cls) {
  const el = $("edit-msg");
  if (el) el.innerHTML = `<span class="msg ${cls || ""}">${esc(text)}</span>`;
}

async function saveEdit(topic) {
  const input = $("edit-type");
  const proto_name = input ? input.value.trim() : "";
  if (!proto_name) {
    setEditMsg("proto type is required", "err");
    return;
  }
  // Only pre-empt the server once the type list arrived; it validates anyway.
  if (knownTypes.size && !knownTypes.has(proto_name)) {
    setEditMsg(`unknown proto type "${proto_name}"`, "err");
    return;
  }
  try {
    await api("PUT", `/api/topic-mappings/${encodeURIComponent(topic)}`, { proto_name });
  } catch (err) {
    setEditMsg(err.message, "err");
    return;
  }
  editing = null;
  await loadMappings();
  setMsgEl("map-list-msg", `"${topic}" now decodes as ${proto_name}.`, "ok");
}

async function removeMapping(topic) {
  if (!confirm(`Remove "${topic}"? It will no longer be recordable until it is added again.`)) return;
  try {
    await api("DELETE", `/api/topic-mappings/${encodeURIComponent(topic)}`);
    setMsgEl("map-list-msg", `Removed "${topic}".`, "ok");
  } catch (err) {
    setMsgEl("map-list-msg", err.message, "err");
  }
  await loadMappings();
}

async function onMappingsClick(e) {
  const btn = e.target.closest("button");
  if (!btn) return;
  if (btn.dataset.edit !== undefined) {
    editing = btn.dataset.edit;
    renderMappings();
    const input = $("edit-type");
    if (input) { input.focus(); input.select(); }
    return;
  }
  if (btn.dataset.cancel !== undefined) {
    editing = null;
    renderMappings();
    return;
  }
  if (btn.dataset.save !== undefined) {
    btn.disabled = true;
    await saveEdit(btn.dataset.save);
    // A failed save leaves the editor open, so the button has to work again.
    if (editing) btn.disabled = false;
    return;
  }
  if (btn.dataset.topic !== undefined) {
    await removeMapping(btn.dataset.topic);
  }
}

attachTypePicker($("map-type"), previewType);
$("map-type").addEventListener("input", previewType);
$("add-btn").addEventListener("click", addMapping);
$("mappings-body").addEventListener("click", onMappingsClick);
$("mappings-body").addEventListener("keydown", (e) => {
  if (e.target.id !== "edit-type") return;
  if (e.key === "Enter") { e.preventDefault(); saveEdit(editing); }
  if (e.key === "Escape") { editing = null; renderMappings(); }
});

// Reset to page one: a narrowed set could leave the reader past the end.
$("map-search").addEventListener("input", () => { mapPage = 0; editing = null; renderMappings(); });
$("map-clear").addEventListener("click", () => {
  $("map-search").value = "";
  mapPage = 0;
  editing = null;
  renderMappings();
  $("map-search").focus();
});
$("map-prev").addEventListener("click", () => { if (mapPage > 0) { mapPage--; editing = null; renderMappings(); } });
$("map-next").addEventListener("click", () => { mapPage++; editing = null; renderMappings(); });

async function loadEnvs() {
  let d;
  try {
    d = await api("GET", "/api/presets");
  } catch {
    return;
  }
  const envs = d.presets || [];
  $("env-count").textContent = `· ${envs.length}`;
  $("env-body").innerHTML = envs
    .map((e) => {
      const del = `<button class="small danger" data-env="${esc(e.name)}">Delete</button>`;
      return (
        `<tr><td>${esc(e.name)}</td><td>${esc(e.url)}</td>` +
        `<td>${e.has_auth ? "🔒 yes" : "—"}</td>` +
        `<td>${del}</td></tr>`
      );
    })
    .join("");
}

async function addEnv() {
  const body = {
    name: $("env-name").value.trim(),
    url: $("env-url").value.trim(),
    username: $("env-user").value,
    password: $("env-pass").value,
    token: $("env-token").value,
  };
  if (!body.name || !body.url) {
    setMsgEl("env-msg", "name and url are required", "err");
    return;
  }
  $("env-add").disabled = true;
  try {
    await api("POST", "/api/presets", body);
    setMsgEl("env-msg", `Added "${body.name}".`, "ok");
    ["env-name", "env-url", "env-user", "env-pass", "env-token"].forEach((id) => ($(id).value = ""));
    await loadEnvs();
  } catch (e) {
    setMsgEl("env-msg", e.message, "err");
  } finally {
    $("env-add").disabled = false;
  }
}

async function onEnvClick(e) {
  const btn = e.target.closest("button[data-env]");
  if (!btn) return;
  const name = btn.dataset.env;
  if (!confirm(`Delete NATS environment "${name}"?`)) return;
  btn.disabled = true;
  try {
    await api("DELETE", `/api/presets/${encodeURIComponent(name)}`);
  } catch (err) {
    alert(err.message);
  }
  await loadEnvs();
}

function setMsgEl(id, text, cls) {
  $(id).textContent = text;
  $(id).className = "msg " + (cls || "");
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

async function loadAudit() {
  let d;
  try {
    d = await api("GET", "/api/audit?limit=100");
  } catch {
    return; // non-admin → 403, leave the card hidden
  }
  const entries = d.entries || [];
  $("audit-card").style.display = "";
  $("audit-count").textContent = entries.length ? `· last ${entries.length}` : "";
  const body = $("audit-body");
  if (entries.length === 0) {
    body.innerHTML = `<tr><td colspan="5" class="empty">No activity yet.</td></tr>`;
    return;
  }
  body.innerHTML = entries
    .map((e) => {
      const cls = e.status >= 400 ? "drop-warn" : "";
      return (
        `<tr><td class="nowrap">${new Date(e.ts).toLocaleString()}</td>` +
        `<td>${esc(e.username)}</td>` +
        `<td>${esc(e.method)}</td>` +
        `<td>${esc(e.path)}</td>` +
        `<td class="num ${cls}">${e.status}</td></tr>`
      );
    })
    .join("");
}

$("env-add").addEventListener("click", addEnv);
$("env-body").addEventListener("click", onEnvClick);
$("proto-import").addEventListener("click", importProtoModule);
$("proto-history").addEventListener("click", onProtoHistoryClick);
$("proto-descriptor-set").addEventListener("change", onProtoFileChange);

// Enter submits the add-mapping / add-environment forms.
["map-topic", "map-type"].forEach((id) =>
  $(id).addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); addMapping(); } }));
["env-name", "env-url", "env-user", "env-pass", "env-token"].forEach((id) =>
  $(id).addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); addEnv(); } }));
$("logout-btn").addEventListener("click", async () => {
  try { await fetch("/api/logout", { method: "POST" }); } catch { /* ignore */ }
  location.href = "/login.html";
});

applyRole();
loadModules();
loadTypes();
loadMappings();
loadEnvs();
loadAudit();
setInterval(loadAudit, 5000);
