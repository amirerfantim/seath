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

// ---- New-recording builder ----

let currentColumns = [];
const samplingState = {
	"sample-": { fields: [], filters: [] },
	"rate-": { fields: [], filters: [] },
};

async function loadTopics() {
  const { topics } = await api("GET", "/api/topics");
  const dl = $("topics");
  dl.innerHTML = "";
  for (const t of topics) {
    const opt = document.createElement("option");
    opt.value = t;
    dl.appendChild(opt);
  }
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

async function onTopicChange() {
  const topic = $("topic").value.trim();
  if (!topic) return;
  let data;
  try {
    data = await api("GET", `/api/topics/${encodeURIComponent(topic)}/schema`);
  } catch (e) {
    $("schema-area").classList.add("hidden");
    setMsg("builder-msg", e.message, "err");
    return;
  }
  setMsg("builder-msg", "", "");
	currentColumns = data.columns;
	renderColumns(data.columns);
	setSamplingFields("sample-", data.sampling_fields || [], []);
	$("table").value = data.suggested_table;
	$("schema-area").classList.remove("hidden");
}

const FILTER_META = {
	exact: { label: "Exact value", capability: "supports_exact" },
	modulo_range: { label: "Modulo range", capability: "supports_modulo" },
	float_compare: { label: "Float comparison", capability: "supports_compare" },
};

function setSamplingFields(prefix, fields, filters) {
	const state = samplingState[prefix];
	state.fields = fields || [];
	state.filters = (filters || []).map((filter) => ({
		...filter,
		values: [...new Set((filter.values || []).map(String))],
	}));
	renderFilters(prefix);
}

function eligibleFields(prefix, type) {
	const capability = FILTER_META[type]?.capability;
	return samplingState[prefix].fields.filter((field) => capability && field[capability]);
}

function selectedFilterField(prefix, filter) {
	return samplingState[prefix].fields.find((field) => field.field === filter.field) || null;
}

function addFilter(prefix, type) {
	const state = samplingState[prefix];
	if (state.filters.length >= 20) return;
	const fields = eligibleFields(prefix, type);
	if (!fields.length) {
		alert(`This topic has no fields for ${FILTER_META[type].label.toLowerCase()}.`);
		return;
	}
	const filter = { type, field: fields[0].field };
	if (type === "exact") filter.values = [];
	if (type === "modulo_range") Object.assign(filter, { lower: 0, upper: 99 });
	if (type === "float_compare") Object.assign(filter, { operator: ">", value: 0.8 });
	state.filters.push(filter);
	renderFilters(prefix);
}

function addExactValues(prefix, index, raw, allowEmpty = false) {
	const filter = samplingState[prefix].filters[index];
	if (!filter || filter.type !== "exact") return;
	const incoming = allowEmpty ? [""] : String(raw || "").split(",").map((value) => value.trim()).filter(Boolean);
	for (const value of incoming) {
		if (!filter.values.includes(value) && totalExactValues(prefix) < 100) filter.values.push(value);
	}
	renderFilters(prefix);
}

function totalExactValues(prefix) {
	return samplingState[prefix].filters.reduce((total, filter) => total + (filter.values?.length || 0), 0);
}

function exactValueEditor(prefix, filter, field, index) {
	const values = filter.values || [];
	if (field.kind === "boolean" || field.kind === "enum") {
		const options = field.kind === "boolean"
			? [{ value: "true", label: "True" }, { value: "false", label: "False" }]
			: field.options || [];
		return `<div class="value-options compact-values">${options.map((option) =>
			`<button type="button" class="value-option ${values.includes(option.value) ? "active" : ""}" data-filter-action="toggle-value" data-index="${index}" data-value="${esc(option.value)}"><span class="option-check">✓</span>${esc(option.label)}</button>`
		).join("")}</div>`;
	}
	const chips = values.map((value, valueIndex) =>
		`<span class="value-chip"><span>${value === "" ? "Empty string" : esc(value)}</span><button type="button" data-filter-action="remove-value" data-index="${index}" data-value-index="${valueIndex}" aria-label="Remove value">×</button></span>`
	).join("");
	return `<div class="value-entry">${chips}<input class="value-input" data-value-input data-index="${index}" placeholder="${field.kind === "integer" ? "Type an integer, then Enter" : "Type an exact value, then Enter"}" autocomplete="off" ${field.kind === "integer" ? "inputmode=\"numeric\"" : ""}/></div>` +
		(field.kind === "string" ? `<button type="button" class="empty-string-btn" data-filter-action="empty-string" data-index="${index}">+ Empty string</button>` : "");
}

function renderFilters(prefix) {
	const state = samplingState[prefix];
	const box = $(`${prefix}filters`);
	if (!box) return;
	if (!state.filters.length) {
		box.innerHTML = `<div class="filter-empty"><span>No field filters</span><small>Only the traffic percentage will be applied.</small></div>`;
	} else {
		box.innerHTML = state.filters.map((filter, index) => {
			const fields = eligibleFields(prefix, filter.type);
			if (!fields.some((field) => field.field === filter.field)) filter.field = fields[0]?.field || "";
			const field = selectedFilterField(prefix, filter);
			const fieldOptions = fields.map((item) => `<option value="${esc(item.field)}" ${item.field === filter.field ? "selected" : ""}>${esc(item.field)} · ${esc(item.kind)}</option>`).join("");
			let editor = "";
			if (filter.type === "exact" && field) editor = exactValueEditor(prefix, filter, field, index);
			if (filter.type === "modulo_range") editor = `<div class="range-editor"><span class="formula">field % 100</span><label>from <input type="number" min="0" max="99" value="${Number(filter.lower ?? 0)}" data-filter-prop="lower" data-index="${index}" /></label><span>to</span><label><input type="number" min="0" max="99" value="${Number(filter.upper ?? 99)}" data-filter-prop="upper" data-index="${index}" /></label><small>inclusive</small></div>`;
			if (filter.type === "float_compare") editor = `<div class="compare-editor"><select data-filter-prop="operator" data-index="${index}">${[">", ">=", "<", "<=", "=", "!="].map((op) => `<option value="${esc(op)}" ${op === filter.operator ? "selected" : ""}>${esc(op)}</option>`).join("")}</select><input type="number" step="any" value="${Number(filter.value ?? 0.8)}" data-filter-prop="value" data-index="${index}" /></div>`;
			return `<article class="filter-card"><div class="filter-card-head"><span class="filter-type">${esc(FILTER_META[filter.type]?.label || filter.type)}</span><button type="button" class="icon-button" data-filter-action="remove-filter" data-index="${index}" aria-label="Remove filter">×</button></div><div class="filter-grid"><div class="field"><label>Field</label><select data-filter-field data-index="${index}">${fieldOptions}</select><p class="field-kind">${field ? esc(field.proto_kind.toUpperCase()) : "NO COMPATIBLE FIELD"}</p></div><div class="filter-editor">${editor}</div></div></article>`;
		}).join("");
	}
	$(`${prefix}filter-adds`)?.querySelectorAll("button").forEach((button) => {
		const type = button.dataset.addFilter;
		button.disabled = state.filters.length >= 20 || eligibleFields(prefix, type).length === 0;
	});
}

function commitPendingInputs(prefix) {
	$(`${prefix}filters`)?.querySelectorAll("[data-value-input]").forEach((input) => {
		if (input.value.trim()) addExactValues(prefix, Number(input.dataset.index), input.value);
	});
}

function samplingBody(prefix) {
	commitPendingInputs(prefix);
	const rate = prefix === "sample-" ? parseInt($("sample").value, 10) : clampRate($("rate-number").value);
	return {
		sample_mode: samplingState[prefix].filters.length ? "composite" : "rate",
		sample_rate: rate,
		sample_field: "",
		sample_values: [],
		sample_filters: samplingState[prefix].filters.map((filter) => ({
			...filter,
			values: filter.type === "exact" ? [...(filter.values || [])] : undefined,
			lower: filter.type === "modulo_range" ? (filter.lower == null ? null : Number(filter.lower)) : undefined,
			upper: filter.type === "modulo_range" ? (filter.upper == null ? null : Number(filter.upper)) : undefined,
			value: filter.type === "float_compare" ? (filter.value == null ? null : Number(filter.value)) : undefined,
		})),
	};
}

function recordingSampling(rec) {
	if (rec.sample_filters?.length) return { rate: rec.sample_rate, filters: rec.sample_filters };
	if (rec.sample_mode === "field_ratio") return { rate: 100, filters: [{ type: "modulo_range", field: rec.sample_field, lower: 0, upper: rec.sample_rate - 1 }] };
	if (rec.sample_mode === "field_value") return { rate: 100, filters: [{ type: "exact", field: rec.sample_field, values: rec.sample_values || [] }] };
	return { rate: rec.sample_rate, filters: [] };
}

function samplingSummary(rec) {
  const sampling = recordingSampling(rec);
  const count = sampling.filters.length;
  if (count === 0) return `${sampling.rate}%`;
  const filters = `${count} ${count === 1 ? "filter" : "filters"}`;
  return sampling.rate === 100 ? filters : `${sampling.rate}% · ${filters}`;
}

function renderColumns(cols) {
  const body = $("columns-body");
  body.innerHTML = "";
  cols.forEach((c, idx) => {
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td><input type="checkbox" data-idx="${idx}" class="col-include" ${c.include ? "checked" : ""}/></td>` +
      `<td><input type="checkbox" data-idx="${idx}" class="col-index" ${c.index ? "checked" : ""}/></td>` +
      `<td>${esc(c.field)}</td>` +
      `<td><input type="text" data-idx="${idx}" class="col-name" value="${esc(c.column)}"/></td>` +
      `<td class="type">${esc(c.type)}</td>`;
    body.appendChild(tr);
  });
}

function collectColumns() {
  const includes = document.querySelectorAll(".col-include");
  const indexes = document.querySelectorAll(".col-index");
  const names = document.querySelectorAll(".col-name");
  return currentColumns.map((c, idx) => ({
    field: c.field,
    column: names[idx].value.trim() || c.field,
    type: c.type,
    include: includes[idx].checked,
    index: indexes[idx].checked,
  }));
}

// Recording table names must be lower case (the server rejects anything
// else), so fold what the user types instead of failing on submit.
$("table").addEventListener("input", (e) => {
  const el = e.target;
  const lower = el.value.toLowerCase();
  if (lower !== el.value) {
    const pos = el.selectionStart;
    el.value = lower;
    el.setSelectionRange(pos, pos);
  }
});

// Filled from GET /api/config so the ceiling is the server's, not a copy.
let maxRetentionHours = null;

async function loadLimits() {
  try {
    const cfg = await api("GET", "/api/config");
    maxRetentionHours = cfg.max_retention_hours;
  } catch {
    return; // leave the field unbounded; the server still validates
  }
  const input = $("retention");
  input.max = maxRetentionHours;
  if (!input.value) input.value = maxRetentionHours;
  $("retention-hint").textContent = `(hours, 1–${maxRetentionHours}, required)`;
}

function clampRetention(v) {
  const n = parseInt(v, 10);
  if (isNaN(n) || n < 1) return 0; // rejected by the server with a clear message
  return maxRetentionHours ? Math.min(n, maxRetentionHours) : n;
}

async function createRecording() {
  const topic = $("topic").value.trim();
  const preset = document.querySelector("input[name=preset]:checked");
  const spec = {
    topic,
    table: $("table").value.trim().toLowerCase(),
    ...samplingBody("sample-"),
    nats_env: preset ? preset.value : "",
    columns: collectColumns(),
    retention_hours: clampRetention($("retention").value),
    index_time: $("index-time").checked,
  };
  try {
    await api("POST", "/api/recordings", spec);
    setMsg("builder-msg", "Created.", "ok");
    await refreshRecordings();
  } catch (e) {
    setMsg("builder-msg", e.message, "err");
  }
}

// ---- Recordings list ----

async function refreshRecordings() {
  try {
    renderRecordings(await api("GET", "/api/recordings"));
  } catch {
    /* ignore */
  }
}

function renderRecordings(data) {
  const body = $("recordings-body");
  const recs = data.recordings || [];
  // Kept so the sampling modal can open on a recording's full spec (its
  // current rule and its column list) without re-fetching.
  lastRecordings = recs;
  if (recs.length === 0) {
    body.innerHTML = `<tr><td colspan="12" class="empty">No recordings yet.</td></tr>`;
    return;
  }
  body.innerHTML = "";
  const now = Date.now();
  for (const r of recs) {
    const started = r.status === "running" && r.started_at ? new Date(r.started_at).toLocaleTimeString() : "—";
    const rate = computeRate(r, now);
    const fullSampling = r.sample_desc || `${r.sample_rate}%`;
    const compactSampling = r.sample_summary || samplingSummary(r);

    const tr = document.createElement("tr");
    tr.className = "rec-row";
    tr.innerHTML =
      `<td><span class="badge ${esc(r.status)}">${esc(r.status)}</span></td>` +
      `<td>${esc(r.topic)}</td>` +
      `<td>${esc(r.table)}</td>` +
      `<td class="num">${esc(r.table_size) || "—"}</td>` +
      `<td class="num">${rate}</td>` +
      `<td>${esc(r.nats_env)}</td>` +
      `<td class="sample-col"><button class="rate-pill" data-act="rate" data-id="${esc(r.id)}" data-table="${esc(r.table)}" title="${esc(fullSampling)}" aria-label="Edit sampling: ${esc(fullSampling)}"><span>${esc(compactSampling)}</span>${IC.edit}</button></td>` +
      `<td class="num">${r.received.toLocaleString()}</td>` +
      `<td class="num">${r.written.toLocaleString()}</td>` +
      `<td class="num">${r.errors}</td>` +
      `<td class="num ${r.dropped ? "drop-warn" : ""}">${(r.dropped || 0).toLocaleString()}</td>` +
      `<td class="nowrap">${started}</td>`;
    body.appendChild(tr);

    const toggle =
      r.status === "running"
        ? `<button class="small warn" data-act="stop" data-id="${esc(r.id)}">${IC.stop} Stop</button>`
        : `<button class="small success" data-act="start" data-id="${esc(r.id)}">${IC.play} Start</button>`;
    const del =
      r.status === "running"
        ? ""
        : `<button class="small danger" data-act="delete" data-id="${esc(r.id)}" data-table="${esc(r.table)}">${IC.trash} Delete</button>`;
    const preview = `<button class="small" data-act="preview" data-id="${esc(r.id)}" data-table="${esc(r.table)}">${IC.eye} Preview</button>`;
    const browse = `<a class="btn small accent" href="/table.html?id=${encodeURIComponent(r.id)}" target="_blank" rel="noopener">${IC.open} Open table</a>`;
    const errHtml = r.status === "error" && r.last_error ? `<span class="last-error">${esc(r.last_error)}</span>` : "";
    const owner = `<span class="rec-owner">created by <b>${esc(r.created_by) || "—"}</b>${r.created_at ? " · " + relTime(r.created_at) : ""}</span>`;

    const ar = document.createElement("tr");
    ar.className = "rec-actions";
    ar.innerHTML =
      `<td colspan="12"><div class="rec-actionsbar">${owner}${errHtml}<div class="rowactions">${browse} ${preview} ${toggle} ${del}</div></div></td>`;
    body.appendChild(ar);
  }
}

// Minimal Feather-style inline icons for the action buttons.
const IC = {
  open: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>',
  eye: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>',
  play: '<svg viewBox="0 0 24 24" width="11" height="11" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>',
  stop: '<svg viewBox="0 0 24 24" width="11" height="11" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>',
  trash: '<svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg>',
  edit: '<svg viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4z"/></svg>',
};

function esc(v) {
  const s = v === null || v === undefined ? "" : String(v);
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

// relTime renders a timestamp as a coarse "3 days ago"; anything older than a
// month falls back to the date.
function relTime(ts) {
  const d = new Date(ts);
  if (isNaN(d)) return "";
  const secs = (Date.now() - d.getTime()) / 1000;
  if (secs < 90) return "just now";
  if (secs < 3600) return Math.round(secs / 60) + "m ago";
  if (secs < 86400) return Math.round(secs / 3600) + "h ago";
  const days = Math.round(secs / 86400);
  if (days <= 30) return days === 1 ? "yesterday" : days + " days ago";
  return d.toLocaleDateString();
}

// rateState tracks the previous written count + timestamp per recording so we
// can derive a live rows/sec figure from successive polls.
const rateState = {};

function computeRate(r, now) {
  const prev = rateState[r.id];
  rateState[r.id] = { written: r.written, t: now };
  if (r.status !== "running" || !prev || prev.written === undefined) return "—";
  const dt = (now - prev.t) / 1000;
  if (dt <= 0) return "—";
  const rate = (r.written - prev.written) / dt;
  if (rate < 0) return "—";
  return rate >= 100 ? Math.round(rate).toLocaleString() + "/s" : rate.toFixed(1) + "/s";
}

async function onRecordingsClick(e) {
  const btn = e.target.closest("button[data-act]");
  if (!btn) return;
  const { act, id, table } = btn.dataset;
  if (act === "rate") {
    await openRateModal(lastRecordings.find((r) => r.id === id));
    return;
  }
  if (act === "preview") {
    await showPreview(id, table);
    return;
  }
  if (act === "delete" && !confirm(`Delete this recording AND drop its Postgres table "${table}"?\n\nThis permanently removes all recorded data.`)) {
    return;
  }
  btn.disabled = true;
  try {
    if (act === "start") await api("POST", `/api/recordings/${id}/start`);
    else if (act === "stop") await api("POST", `/api/recordings/${id}/stop`);
    else if (act === "delete") await api("DELETE", `/api/recordings/${id}`);
  } catch (err) {
    alert(err.message);
  }
  await refreshRecordings();
}

// ---- Sample-rate editor (modal) ----

let rateEditId = null;
let lastRecordings = [];

function clampRate(v) {
  v = parseInt(v, 10);
  if (isNaN(v)) return 1;
  return Math.min(100, Math.max(1, v));
}

function setRateValue(v) {
  v = clampRate(v);
  $("rate-value").textContent = v;
  $("rate-slider").value = v;
  $("rate-number").value = v;
  document.querySelectorAll("#rate-presets .chip").forEach((c) => {
    c.classList.toggle("active", parseInt(c.dataset.rate, 10) === v);
  });
}

async function openRateModal(rec) {
  if (!rec) return;
  rateEditId = rec.id;
  $("rate-title").textContent = `Sampling · ${rec.table}`;

	setMsg("rate-msg", "Loading field definitions…", "");
	$("rate-modal").classList.remove("hidden");
	let fields = rec.sampling_fields;
	if (!fields) {
		try {
			const schema = await api("GET", `/api/topics/${encodeURIComponent(rec.topic)}/schema`);
			fields = schema.sampling_fields || [];
		} catch (err) {
			setMsg("rate-msg", err.message, "err");
			return;
		}
	}

	const sampling = recordingSampling(rec);
	setSamplingFields("rate-", fields, sampling.filters);
	setRateValue(sampling.rate);

  setMsg("rate-msg", "", "");
}

function closeRateModal() {
  rateEditId = null;
  $("rate-modal").classList.add("hidden");
}

async function applyRate() {
  if (!rateEditId) return;
  const body = samplingBody("rate-");
  $("rate-apply").disabled = true;
  try {
    await api("POST", `/api/recordings/${rateEditId}/sampling`, body);
    closeRateModal();
    await refreshRecordings();
  } catch (err) {
    setMsg("rate-msg", err.message, "err");
  } finally {
    $("rate-apply").disabled = false;
  }
}

async function showPreview(id, table) {
  const modal = $("preview-modal");
  $("preview-title").textContent = `Latest rows · ${table}`;
  $("preview-body").innerHTML = "Loading…";
  modal.classList.remove("hidden");
  let data;
  try {
    data = await api("GET", `/api/recordings/${id}/preview`);
  } catch (e) {
    $("preview-body").innerHTML = `<div class="last-error">${esc(e.message)}</div>`;
    return;
  }
  const rows = data.rows || [];
  if (rows.length === 0) {
    $("preview-body").innerHTML = `<div class="empty">No rows yet.</div>`;
    return;
  }
  const cols = Object.keys(rows[0]);
  const esc = (v) => {
    let s = v === null || v === undefined ? "" : String(v);
    if (s.length > 120) s = s.slice(0, 120) + "…";
    return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
  };
  let html = "<table class='preview'><thead><tr>" + cols.map((c) => `<th>${c}</th>`).join("") + "</tr></thead><tbody>";
  for (const row of rows) {
    html += "<tr>" + cols.map((c) => `<td title="${esc(row[c])}">${esc(row[c])}</td>`).join("") + "</tr>";
  }
  html += "</tbody></table>";
  $("preview-body").innerHTML = html;
}

function setMsg(id, text, cls) {
  const el = $(id);
  el.textContent = text;
  el.className = "msg " + (cls || "");
}

function wireSamplingBuilder(prefix) {
	$(`${prefix}filter-adds`).addEventListener("click", (event) => {
		const button = event.target.closest("[data-add-filter]");
		if (button) addFilter(prefix, button.dataset.addFilter);
	});
	const list = $(`${prefix}filters`);
	list.addEventListener("click", (event) => {
		const button = event.target.closest("[data-filter-action]");
		if (!button) return;
		const index = Number(button.dataset.index);
		const state = samplingState[prefix];
		const filter = state.filters[index];
		if (!filter) return;
		switch (button.dataset.filterAction) {
		case "remove-filter":
			state.filters.splice(index, 1);
			break;
		case "toggle-value": {
			const value = button.dataset.value;
			const found = filter.values.indexOf(value);
			if (found >= 0) filter.values.splice(found, 1);
			else if (totalExactValues(prefix) < 100) filter.values.push(value);
			break;
		}
		case "remove-value":
			filter.values.splice(Number(button.dataset.valueIndex), 1);
			break;
		case "empty-string":
			addExactValues(prefix, index, "", true);
			return;
		}
		renderFilters(prefix);
	});
	list.addEventListener("change", (event) => {
		const index = Number(event.target.dataset.index);
		const filter = samplingState[prefix].filters[index];
		if (!filter) return;
		if (event.target.matches("[data-filter-field]")) {
			filter.field = event.target.value;
			if (filter.type === "exact") filter.values = [];
			renderFilters(prefix);
			return;
		}
		if (event.target.dataset.filterProp === "operator") filter.operator = event.target.value;
	});
	list.addEventListener("input", (event) => {
		const prop = event.target.dataset.filterProp;
		if (!prop || prop === "operator") return;
		const filter = samplingState[prefix].filters[Number(event.target.dataset.index)];
		if (filter) filter[prop] = event.target.value === "" ? null : Number(event.target.value);
	});
	list.addEventListener("keydown", (event) => {
		if (!event.target.matches("[data-value-input]")) return;
		const index = Number(event.target.dataset.index);
		const filter = samplingState[prefix].filters[index];
		if (event.key === "Enter" || event.key === ",") {
			event.preventDefault();
			event.stopPropagation();
			addExactValues(prefix, index, event.target.value);
		} else if (event.key === "Backspace" && !event.target.value && filter?.values.length) {
			filter.values.pop();
			renderFilters(prefix);
		}
	});
	list.addEventListener("focusout", (event) => {
		if (event.target.matches("[data-value-input]") && event.target.value.trim()) {
			addExactValues(prefix, Number(event.target.dataset.index), event.target.value);
		}
	});
}

// ---- Wire up ----

$("topic").addEventListener("change", onTopicChange);
$("sample").addEventListener("input", () => ($("sample-val").textContent = $("sample").value));
$("create-btn").addEventListener("click", createRecording);
$("recordings-body").addEventListener("click", onRecordingsClick);
wireSamplingBuilder("sample-");

// sampling editor wiring
wireSamplingBuilder("rate-");
$("rate-slider").addEventListener("input", (e) => setRateValue(e.target.value));
$("rate-number").addEventListener("input", (e) => setRateValue(e.target.value));
$("rate-presets").addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (chip) setRateValue(chip.dataset.rate);
});
$("rate-apply").addEventListener("click", applyRate);
$("rate-cancel").addEventListener("click", closeRateModal);
$("rate-modal").addEventListener("click", (e) => { if (e.target.id === "rate-modal") closeRateModal(); });
$("preview-close").addEventListener("click", () => $("preview-modal").classList.add("hidden"));
$("preview-modal").addEventListener("click", (e) => {
  if (e.target.id === "preview-modal") $("preview-modal").classList.add("hidden");
});

// Enter submits the new-recording form from its key fields.
["table", "retention"].forEach((id) =>
  $(id).addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); createRecording(); } }));

// Global keyboard shortcuts: Esc closes any open modal; Enter applies the rate editor.
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    $("preview-modal").classList.add("hidden");
    closeRateModal();
  } else if (e.key === "Enter" && !e.target.closest(".value-entry") && !$("rate-modal").classList.contains("hidden")) {
    e.preventDefault();
    applyRate();
  }
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

// Live updates via SSE, with a polling fallback if EventSource is unavailable
// or errors out.
function startLive() {
  if (typeof EventSource === "undefined") {
    refreshRecordings();
    setInterval(refreshRecordings, 2000);
    return;
  }
  refreshRecordings(); // instant first paint
  let pollTimer = null;
  const es = new EventSource("/api/recordings/stream");
  es.onmessage = (e) => {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    try { renderRecordings(JSON.parse(e.data)); } catch { /* ignore */ }
  };
  es.onerror = () => {
    // EventSource auto-reconnects; while down, poll as a safety net.
    if (!pollTimer) pollTimer = setInterval(refreshRecordings, 3000);
  };
}

$("logout-btn").addEventListener("click", async () => {
  try { await fetch("/api/logout", { method: "POST" }); } catch { /* ignore */ }
  location.href = "/login.html";
});

applyRole();
loadLimits();
loadTopics();
loadPresets();
startLive();
