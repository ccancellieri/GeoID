// Drop-and-register — drag GeoJSON files, POST to the target collection,
// render the server's insertion report. Authorization is enforced server-
// side; the page just refuses to enable Transmit until a collection has
// been chosen.

import {
  fetchMe, fetchMyCatalogs, fetchCatalogs, postFeatures, getJSON,
} from "../common/api.js";

const $ = (s) => document.querySelector(s);

const state = {
  isSysadmin: false,
  myCatalogs: [],
  allCatalogs: [],
  files: [],                 // [{name, size, features}]
  totalFeatures: 0,
  selectedCatalog: null,
  selectedCollection: null,
};

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setStatus(msg, cls = "") {
  const el = $("#status");
  el.textContent = msg || "";
  el.className = "status " + cls;
}

function updateButtons() {
  const ready = !!state.selectedCatalog && !!state.selectedCollection && state.totalFeatures > 0;
  $("#submit").disabled = !ready;
  $("#clear").disabled = !state.files.length;
}

function renderFileList() {
  const ul = $("#file-list");
  clearNode(ul);
  if (!state.files.length) return;
  for (const f of state.files) {
    const li = document.createElement("li");
    const count =
      f.features != null
        ? `${f.features} feature${f.features === 1 ? "" : "s"}`
        : (f.error ? `error: ${f.error}` : "parsing…");
    li.textContent = `— ${f.name} (${(f.size / 1024).toFixed(1)} KB) · ${count}`;
    if (f.error) li.className = "file-err";
    ul.appendChild(li);
  }
}

function countFeatures(obj) {
  if (!obj) return 0;
  if (Array.isArray(obj)) return obj.reduce((n, x) => n + countFeatures(x), 0);
  if (obj.type === "FeatureCollection" && Array.isArray(obj.features)) return obj.features.length;
  if (obj.type === "Feature") return 1;
  return 0;
}

function flattenToFeatures(obj) {
  const out = [];
  const push = (x) => {
    if (!x) return;
    if (Array.isArray(x)) { x.forEach(push); return; }
    if (x.type === "FeatureCollection" && Array.isArray(x.features)) {
      x.features.forEach(push); return;
    }
    if (x.type === "Feature") { out.push(x); return; }
  };
  push(obj);
  return out;
}

async function readFile(file) {
  try {
    const text = await file.text();
    const parsed = JSON.parse(text);
    const features = flattenToFeatures(parsed);
    return { name: file.name, size: file.size, features: features.length, raw: parsed, parsed: features };
  } catch (e) {
    return { name: file.name, size: file.size, error: e.message || "invalid JSON", features: 0 };
  }
}

async function handleFiles(filelist) {
  const files = Array.from(filelist || []);
  if (!files.length) return;
  setStatus(`Reading ${files.length} file(s)…`);
  const parsed = await Promise.all(files.map(readFile));
  state.files.push(...parsed);
  state.totalFeatures = state.files.reduce((n, f) => n + (f.features || 0), 0);
  renderFileList();
  const errs = state.files.filter((f) => f.error).length;
  setStatus(
    `${state.totalFeatures} feature(s) ready${errs ? `, ${errs} file(s) failed to parse` : ""}.`,
    errs ? "warn" : "ok",
  );
  updateButtons();
}

function clearFiles() {
  state.files = [];
  state.totalFeatures = 0;
  renderFileList();
  setStatus("");
  updateButtons();
}

function renderReport(status, body, transmitted) {
  const rpt = $("#report");
  clearNode(rpt);

  const meta = $("#report-meta");
  if (status === 201) {
    meta.textContent = `201 · OK · ${transmitted} feature(s) transmitted`;
  } else if (status === 207) {
    meta.textContent = `207 · PARTIAL`;
  } else {
    meta.textContent = `${status}`;
  }

  if (!body) {
    rpt.textContent = "(empty response)";
    return;
  }

  // Possible shapes:
  //   201 BulkCreationResponse: { created: [...], count: N }
  //   207 IngestionReport: { accepted: N, rejected: [{index, error, feature_id}] }
  if (body.accepted != null || body.rejected) {
    const head = document.createElement("div");
    const okLine = document.createElement("div");
    okLine.className = "ok";
    okLine.textContent = `✓ accepted: ${body.accepted ?? 0}`;
    const errLine = document.createElement("div");
    errLine.className = "err";
    errLine.textContent = `✗ rejected: ${(body.rejected || []).length}`;
    head.append(okLine, errLine);
    rpt.appendChild(head);

    if ((body.rejected || []).length) {
      const hr = document.createElement("hr");
      hr.className = "rule";
      rpt.appendChild(hr);
      for (const r of body.rejected) {
        const li = document.createElement("div");
        li.className = "err";
        li.textContent = `#${r.index ?? "?"} ${r.feature_id ? `[${r.feature_id}] ` : ""}${r.error || r.message || r.detail || "error"}`;
        rpt.appendChild(li);
      }
    }
    return;
  }

  if (body.count != null || Array.isArray(body.created)) {
    const line = document.createElement("div");
    line.className = "ok";
    line.textContent = `✓ created: ${body.count ?? (body.created || []).length}`;
    rpt.appendChild(line);
    if (Array.isArray(body.created) && body.created.length) {
      const hr = document.createElement("hr");
      hr.className = "rule";
      rpt.appendChild(hr);
      for (const c of body.created.slice(0, 50)) {
        const id = typeof c === "string" ? c : (c.id || c.feature_id || JSON.stringify(c));
        const li = document.createElement("div");
        li.textContent = `— ${id}`;
        rpt.appendChild(li);
      }
      if (body.created.length > 50) {
        const more = document.createElement("div");
        more.className = "muted";
        more.textContent = `… and ${body.created.length - 50} more`;
        rpt.appendChild(more);
      }
    }
    return;
  }

  // Unknown shape — dump as JSON.
  const pre = document.createElement("pre");
  pre.textContent = JSON.stringify(body, null, 2);
  rpt.appendChild(pre);
}

async function onSubmit() {
  if (!state.selectedCatalog || !state.selectedCollection) return;
  const allFeatures = state.files.flatMap((f) => f.parsed || []);
  if (!allFeatures.length) return;

  const payload = allFeatures.length === 1
    ? allFeatures[0]
    : { type: "FeatureCollection", features: allFeatures };

  setStatus(`Transmitting ${allFeatures.length} feature(s)…`);
  $("#submit").disabled = true;

  try {
    const { status, body } = await postFeatures(state.selectedCatalog, state.selectedCollection, payload);
    setStatus(`Server accepted (${status}).`, "ok");
    renderReport(status, body, allFeatures.length);
  } catch (e) {
    setStatus(`Transmission failed: ${e.message}`, "err");
    renderReport(e.status ?? 500, e.body, allFeatures.length);
  } finally {
    updateButtons();
  }
}

// --- Target selectors -------------------------------------------------

async function populateCatalogs() {
  const sel = $("#ing-catalog");
  clearNode(sel);
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Choose catalog…";
  sel.appendChild(opt0);

  try {
    state.allCatalogs = await fetchCatalogs();
  } catch (_) {
    state.allCatalogs = [];
  }
  const items = Array.isArray(state.allCatalogs)
    ? state.allCatalogs
    : (state.allCatalogs.catalogs || state.allCatalogs.items || []);

  const allowed = state.isSysadmin
    ? items.map((c) => c.id).filter(Boolean)
    : state.myCatalogs.map((c) => c.catalog_id);
  const allowedSet = new Set(allowed);
  for (const c of items) {
    if (!c.id || !allowedSet.has(c.id)) continue;
    const o = document.createElement("option");
    o.value = c.id;
    o.textContent = c.id;
    sel.appendChild(o);
  }
}

async function populateCollections(catalogId) {
  const sel = $("#ing-collection");
  clearNode(sel);
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Choose collection…";
  sel.appendChild(opt0);
  if (!catalogId) {
    sel.disabled = true;
    return;
  }
  sel.disabled = false;

  try {
    const payload = await getJSON(`/stac/catalogs/${encodeURIComponent(catalogId)}/collections`);
    const items = Array.isArray(payload)
      ? payload
      : (payload.collections || payload.items || []);
    for (const c of items) {
      if (!c.id) continue;
      const o = document.createElement("option");
      o.value = c.id;
      o.textContent = c.id;
      sel.appendChild(o);
    }
  } catch (e) {
    setStatus(`Collections load failed: ${e.message}`, "err");
  }
}

// --- Boot -------------------------------------------------------------

async function boot() {
  const me = await fetchMe();
  const roles = me.roles || [];
  state.isSysadmin = roles.includes("sysadmin");

  try {
    state.myCatalogs = await fetchMyCatalogs();
  } catch (_) {
    state.myCatalogs = [];
  }

  if (!me.principal) {
    const main = document.querySelector("main");
    clearNode(main);
    const p = document.createElement("p");
    p.className = "subtitle";
    p.textContent = "Sign in to ingest features.";
    main.appendChild(p);
    return;
  }

  await populateCatalogs();
  await populateCollections("");

  $("#ing-catalog").addEventListener("change", async (e) => {
    state.selectedCatalog = e.target.value || null;
    state.selectedCollection = null;
    await populateCollections(state.selectedCatalog);
    updateButtons();
  });
  $("#ing-collection").addEventListener("change", (e) => {
    state.selectedCollection = e.target.value || null;
    updateButtons();
  });

  // Dropzone + file picker
  const zone = $("#dropzone");
  const picker = $("#filepicker");
  zone.addEventListener("click", () => picker.click());
  picker.addEventListener("change", (e) => handleFiles(e.target.files));

  const prevent = (e) => { e.preventDefault(); e.stopPropagation(); };
  ["dragenter", "dragover"].forEach((ev) =>
    zone.addEventListener(ev, (e) => { prevent(e); zone.classList.add("drag-over"); }),
  );
  ["dragleave", "drop"].forEach((ev) =>
    zone.addEventListener(ev, (e) => { prevent(e); zone.classList.remove("drag-over"); }),
  );
  zone.addEventListener("drop", (e) => {
    const files = e.dataTransfer && e.dataTransfer.files;
    if (files && files.length) handleFiles(files);
  });
  // Prevent window-level drop from replacing the page
  window.addEventListener("dragover", prevent);
  window.addEventListener("drop", prevent);

  $("#clear").addEventListener("click", clearFiles);
  $("#submit").addEventListener("click", onSubmit);
}

boot();
