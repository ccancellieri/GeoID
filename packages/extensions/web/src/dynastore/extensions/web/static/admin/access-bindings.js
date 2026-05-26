// Access & Bindings page (#1342). Vanilla JS, mirrors governance.js style.
// All actual authorization is enforced server-side on /admin/* — this page
// is a thin client over the existing REST surface, including the
// collection-binding CRUD added in #1360 and list_grants_for_resource for
// the reverse "who can access" view.

import { getJSON, postJSON, deleteJSON } from "../common/api.js";

// ---------------------------------------------------------------- state

const state = {
  catalogId: null,
  collectionId: null,
  principalId: null,
  principalLabel: null,
};

const $ = (sel) => document.querySelector(sel);

// HTML-escape a value before inlining into a template string. Catalog ids,
// principal identifiers, role names, etc. all originate from operator-edited
// API responses; escaping defends against an operator (or upstream system)
// putting a stray angle-bracket or quote into one of those fields.
function esc(value) {
  if (value == null) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function setStatus(el, msg, level = "info") {
  el.textContent = msg || "";
  el.dataset.level = level;
}

function fmtDateTime(value) {
  if (!value) return "—";
  try {
    return new Date(value).toISOString().replace("T", " ").slice(0, 19) + "Z";
  } catch (_e) {
    return String(value);
  }
}

function fmtQuota(q) {
  if (q == null) return "—";
  if (typeof q === "object") return JSON.stringify(q);
  return String(q);
}

function clear(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
}

// ---------------------------------------------------------------- catalog + collection pickers

function fillSelect(sel, rows, placeholder, idKey = "id") {
  const opts = [`<option value="">${esc(placeholder)}</option>`];
  for (const r of rows) {
    const v = esc(r[idKey]);
    opts.push(`<option value="${v}">${v}</option>`);
  }
  sel.innerHTML = opts.join("");
}

async function loadCatalogs() {
  const sel = $("#scope-catalog");
  try {
    const rows = await getJSON("/admin/catalogs");
    fillSelect(sel, rows || [], "— pick a catalog —");
  } catch (e) {
    setStatus($("#scope-status"), `Failed to load catalogs: ${e.message}`, "error");
  }
}

async function loadCollections(catalogId) {
  const sel = $("#scope-collection");
  if (!catalogId) {
    sel.disabled = true;
    sel.innerHTML = '<option value="">— pick a collection —</option>';
    return;
  }
  try {
    const data = await getJSON(`/catalogs/${encodeURIComponent(catalogId)}/collections`);
    const cols = data.collections || data || [];
    fillSelect(sel, cols, "— pick a collection —");
    sel.disabled = false;
  } catch (e) {
    setStatus($("#scope-status"), `Failed to load collections: ${e.message}`, "error");
  }
}

// ---------------------------------------------------------------- principal picker

async function searchPrincipals() {
  const q = $("#principal-search").value.trim();
  const results = $("#principal-results");
  clear(results);
  if (!q) {
    const d = document.createElement("div");
    d.className = "hint";
    d.textContent = "Enter at least one character.";
    results.appendChild(d);
    return;
  }
  try {
    const rows = await getJSON(
      `/admin/principals?q=${encodeURIComponent(q)}&limit=15`,
    );
    if (!rows.length) {
      const d = document.createElement("div");
      d.className = "hint";
      d.textContent = "No principals match.";
      results.appendChild(d);
      return;
    }
    for (const p of rows) {
      const label = p.identifier || p.display_name || "(anonymous)";
      const row = document.createElement("div");
      row.className = "result-row";
      row.dataset.id = p.id;
      row.dataset.label = label;
      // Build children with textContent — never inject API strings as HTML.
      const idDiv = document.createElement("div");
      idDiv.className = "result-id";
      idDiv.textContent = label;
      const metaDiv = document.createElement("div");
      metaDiv.className = "result-meta";
      metaDiv.textContent = `${p.id} · ${p.is_active ? "active" : "inactive"}`;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "result-pick";
      btn.textContent = "Pick";
      row.append(idDiv, metaDiv, btn);
      results.appendChild(row);
    }
  } catch (e) {
    setStatus($("#principal-selected"), `Search failed: ${e.message}`, "error");
  }
}

function pickPrincipal(id, label) {
  state.principalId = id;
  state.principalLabel = label;
  setStatus(
    $("#principal-selected"),
    `Principal: ${label} (${id})`,
    "info",
  );
  refreshBindings();
}

// ---------------------------------------------------------------- bindings tables

async function refreshBindings() {
  const el = $("#bindings-table");
  clear(el);
  if (!state.catalogId || !state.collectionId) {
    setHint(el, "Pick a catalog and collection first.");
    return;
  }
  if (!state.principalId) {
    setHint(el, "Pick a principal first.");
    return;
  }
  try {
    const rows = await getJSON(
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/grants?principal_id=${encodeURIComponent(state.principalId)}`,
    );
    renderBindingsTable(el, rows, true);
  } catch (e) {
    setHint(el, `Failed: ${e.message}`, "error");
  }
}

async function refreshReverse() {
  const el = $("#reverse-table");
  clear(el);
  if (!state.catalogId || !state.collectionId) {
    setHint(el, "Pick a catalog and collection first.");
    return;
  }
  try {
    const rows = await getJSON(
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/grants`,
    );
    renderBindingsTable(el, rows, false);
  } catch (e) {
    setHint(el, `Failed: ${e.message}`, "error");
  }
}

function setHint(parent, text, level = "info") {
  const d = document.createElement("div");
  d.className = "hint";
  d.dataset.level = level;
  d.textContent = text;
  parent.appendChild(d);
}

function renderBindingsTable(parent, rows, allowDelete) {
  if (!rows || !rows.length) {
    setHint(parent, "No bindings on this scope.");
    return;
  }
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  for (const h of [
    "Subject",
    "Object",
    "Effect",
    "Valid from",
    "Valid until",
    "Quota",
    allowDelete ? "Action" : null,
  ]) {
    if (h == null) continue;
    const th = document.createElement("th");
    th.textContent = h;
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  for (const r of rows) {
    const effect = r.effect || "allow";
    const tr = document.createElement("tr");
    if (effect === "deny") tr.className = "row-deny";

    const subj = document.createElement("td");
    const subjCode = document.createElement("code");
    subjCode.textContent = r.subject_ref || "—";
    subj.appendChild(subjCode);
    tr.appendChild(subj);

    const obj = document.createElement("td");
    obj.append(`${r.object_kind || "role"}:`);
    const objCode = document.createElement("code");
    objCode.textContent = r.object_ref || "—";
    obj.appendChild(objCode);
    tr.appendChild(obj);

    const eff = document.createElement("td");
    const effChip = document.createElement("span");
    effChip.className = `chip effect-${effect === "deny" ? "DENY" : "ALLOW"}`;
    effChip.textContent = effect;
    eff.appendChild(effChip);
    tr.appendChild(eff);

    const vf = document.createElement("td");
    vf.textContent = fmtDateTime(r.valid_from);
    tr.appendChild(vf);
    const vu = document.createElement("td");
    vu.textContent = fmtDateTime(r.valid_until);
    tr.appendChild(vu);

    const quota = document.createElement("td");
    quota.textContent = fmtQuota(r.quota);
    tr.appendChild(quota);

    if (allowDelete) {
      const act = document.createElement("td");
      act.className = "table-actions";
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "btn btn-danger btn-xs binding-revoke";
      btn.textContent = "Revoke";
      btn.dataset.principalId = r.subject_ref || "";
      btn.dataset.objectKind = r.object_kind || "role";
      btn.dataset.objectRef = r.object_ref || "";
      btn.dataset.effect = effect;
      act.appendChild(btn);
      tr.appendChild(act);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  parent.appendChild(table);
}

// ---------------------------------------------------------------- create / revoke

async function createBinding(ev) {
  ev.preventDefault();
  const statusEl = $("#binding-create-status");
  if (!state.catalogId || !state.collectionId) {
    setStatus(statusEl, "Pick a catalog + collection first.", "error");
    return;
  }
  if (!state.principalId) {
    setStatus(statusEl, "Pick a principal first.", "error");
    return;
  }
  const body = {
    principal_id: state.principalId,
    object_kind: $("#binding-object-kind").value,
    object_ref: $("#binding-object-ref").value.trim(),
    effect: $("#binding-effect").value,
  };
  const vf = $("#binding-valid-from").value;
  const vu = $("#binding-valid-until").value;
  if (vf) body.valid_from = new Date(vf).toISOString();
  if (vu) body.valid_until = new Date(vu).toISOString();
  const quotaRaw = $("#binding-quota").value.trim();
  if (quotaRaw) {
    try {
      body.quota = JSON.parse(quotaRaw);
    } catch (_e) {
      setStatus(statusEl, "Quota must be valid JSON.", "error");
      return;
    }
  }
  try {
    await postJSON(
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/grants`,
      body,
    );
    setStatus(statusEl, "Binding granted.", "info");
    $("#binding-object-ref").value = "";
    $("#binding-quota").value = "";
    refreshBindings();
  } catch (e) {
    setStatus(statusEl, `Failed: ${e.message}`, "error");
  }
}

async function revokeBinding(row) {
  if (!state.catalogId || !state.collectionId) return;
  const params = new URLSearchParams({
    principal_id: row.dataset.principalId,
    object_kind: row.dataset.objectKind,
    object_ref: row.dataset.objectRef,
    effect: row.dataset.effect,
  });
  if (!confirm(`Revoke ${row.dataset.objectKind}:${row.dataset.objectRef}?`)) return;
  try {
    await deleteJSON(
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/grants?${params.toString()}`,
    );
    refreshBindings();
  } catch (e) {
    alert(`Revoke failed: ${e.message}`);
  }
}

// ---------------------------------------------------------------- tabs + wiring

function switchTab(name) {
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll(".tab-panel").forEach((p) => {
    p.classList.toggle("active", p.id === `tab-${name}`);
  });
  if (name === "by-resource") refreshReverse();
}

function bind() {
  $("#scope-catalog").addEventListener("change", async (e) => {
    state.catalogId = e.target.value || null;
    state.collectionId = null;
    await loadCollections(state.catalogId);
    refreshBindings();
    refreshReverse();
  });
  $("#scope-collection").addEventListener("change", (e) => {
    state.collectionId = e.target.value || null;
    refreshBindings();
    refreshReverse();
  });

  $("#principal-search-btn").addEventListener("click", searchPrincipals);
  $("#principal-search").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      searchPrincipals();
    }
  });
  $("#principal-results").addEventListener("click", (e) => {
    const btn = e.target.closest(".result-pick");
    if (!btn) return;
    const row = btn.closest(".result-row");
    pickPrincipal(row.dataset.id, row.dataset.label);
  });

  $("#binding-create").addEventListener("submit", createBinding);

  $("#bindings-table").addEventListener("click", (e) => {
    const btn = e.target.closest(".binding-revoke");
    if (btn) revokeBinding(btn);
  });

  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.addEventListener("click", () => switchTab(b.dataset.tab));
  });
}

// ---------------------------------------------------------------- boot

(async function init() {
  bind();
  await loadCatalogs();
})();
