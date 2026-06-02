// Access & Bindings page (#1342). Vanilla JS, mirrors governance.js style.
// All actual authorization is enforced server-side on /admin/* — this page
// is a thin client over the existing REST surface, including the
// collection-binding CRUD added in #1360 and list_grants_for_resource for
// the reverse "who can access" view.

import { getJSON, postJSON, putJSON, deleteJSON } from "../common/api.js";

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
  // Use DOM methods — never innerHTML with server-derived strings.
  while (sel.firstChild) sel.removeChild(sel.firstChild);
  const blank = document.createElement("option");
  blank.value = "";
  blank.textContent = placeholder;
  sel.appendChild(blank);
  for (const r of rows) {
    const opt = document.createElement("option");
    const v = String(r[idKey] || "");
    opt.value = v;
    opt.textContent = v;
    sel.appendChild(opt);
  }
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
    // Reset via DOM — not innerHTML — so no server content is involved.
    while (sel.firstChild) sel.removeChild(sel.firstChild);
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "— pick a collection —";
    sel.appendChild(opt);
    return;
  }
  try {
    const data = await getJSON(`/stac/catalogs/${encodeURIComponent(catalogId)}/collections`);
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

// Cached usage entries keyed by grant_id for the current principal view.
// Populated by refreshBindings after the bindings load completes.
let _usageByGrantId = {};

async function refreshBindings() {
  const el = $("#bindings-table");
  clear(el);
  _usageByGrantId = {};

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
    // Fetch live counters in parallel with rendering; failure is non-fatal —
    // we render the table without counter data and let the "Usage" modal
    // surface the error when the operator requests it explicitly.
    fetchUsageIntoCache(rows).catch(() => {});
    renderBindingsTable(el, rows, true);
  } catch (e) {
    setHint(el, `Failed: ${e.message}`, "error");
  }
}

// Fetches /admin/iam/usage/grants for the current principal + catalog scope
// and populates _usageByGrantId so renderBindingsTable can merge counters.
// Surfaces a stale-data banner if valkey_available===false.
async function fetchUsageIntoCache(rows) {
  if (!state.principalId || !rows || !rows.length) return;
  const url = `/admin/iam/usage/grants?principal_id=${encodeURIComponent(state.principalId)}`
    + (state.catalogId ? `&catalog_id=${encodeURIComponent(state.catalogId)}` : "");
  const data = await getJSON(url);
  const banner = $("#usage-stale-banner-inline");
  if (data && data.valkey_available === false) {
    if (banner) {
      banner.textContent = "Live Valkey counters unavailable — figures may be stale (PG fallback).";
      banner.style.display = "";
    }
  } else {
    if (banner) banner.style.display = "none";
  }
  const entries = (data && Array.isArray(data.entries)) ? data.entries : [];
  for (const e of entries) {
    _usageByGrantId[String(e.grant_id)] = e;
  }
  // Re-render the counter cells without a full refresh — find all counter
  // cells that were left as "…" and fill them in.
  document.querySelectorAll("td.counter-cell[data-grant-id]").forEach((td) => {
    const entry = _usageByGrantId[td.dataset.grantId];
    fillCounterCell(td, entry);
  });
}

function fillCounterCell(td, entry) {
  clear(td);
  if (!entry || !entry.counters) {
    td.textContent = "—";
    return;
  }
  const counters = entry.counters;
  const lines = [];
  if (counters.rate_limit) {
    const rl = counters.rate_limit;
    const line = document.createElement("div");
    line.textContent = `rate: ${rl.count}/${rl.limit} (${rl.remaining} left)`;
    if (rl.window_start) {
      const sub = document.createElement("div");
      sub.className = "muted";
      sub.style.fontSize = "0.75em";
      sub.textContent = `resets from ${fmtDateTime(rl.window_start)}`;
      line.appendChild(sub);
    }
    td.appendChild(line);
  }
  if (counters.max_count) {
    const mc = counters.max_count;
    const line = document.createElement("div");
    line.textContent = `total: ${mc.count}/${mc.limit} (${mc.remaining} left)`;
    td.appendChild(line);
  }
  if (!counters.rate_limit && !counters.max_count) {
    td.textContent = "—";
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

// Renders `attribute_predicates` from a grant row as compact text.
// Returns "" when there are none so the table cell renders cleanly.
function fmtPredicates(preds) {
  if (!Array.isArray(preds) || !preds.length) return "";
  return preds.map((p) => {
    const vals = Array.isArray(p.values) ? p.values.join(", ") : String(p.values || "");
    return `${p.key} ${p.op} [${vals}]`;
  }).join(" · ");
}

function renderBindingsTable(parent, rows, allowDelete) {
  if (!rows || !rows.length) {
    setHint(parent, "No bindings on this scope.");
    return;
  }
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  const headers = [
    "Subject",
    "Object",
    "Effect",
    "Valid from",
    "Valid until",
    "Quota",
    "Counters",
    "Predicates",
    allowDelete ? "Actions" : null,
  ];
  for (const h of headers) {
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

    // Live counter cell — filled in once fetchUsageIntoCache resolves.
    // If already cached (e.g. table re-render after usage load), fill immediately.
    const counterTd = document.createElement("td");
    counterTd.className = "counter-cell";
    const grantId = r.id ? String(r.id) : null;
    if (grantId) {
      counterTd.dataset.grantId = grantId;
      const cached = _usageByGrantId[grantId];
      if (cached !== undefined) {
        fillCounterCell(counterTd, cached);
      } else {
        counterTd.textContent = "…";
      }
    } else {
      counterTd.textContent = "—";
    }
    tr.appendChild(counterTd);

    // Attribute predicates column — read-only display of any stored predicates.
    const predTd = document.createElement("td");
    const predText = fmtPredicates(r.attribute_predicates);
    if (predText) {
      const pre = document.createElement("pre");
      pre.className = "cell-json";
      pre.textContent = predText;
      predTd.appendChild(pre);
    } else {
      predTd.className = "muted";
      predTd.textContent = "—";
    }
    tr.appendChild(predTd);

    if (allowDelete) {
      const act = document.createElement("td");
      act.className = "table-actions";

      // "Why?" affordance — opens effective-permissions explainer (#1390).
      const whyBtn = document.createElement("button");
      whyBtn.type = "button";
      whyBtn.className = "btn btn-ghost btn-xs";
      whyBtn.textContent = "Why?";
      whyBtn.setAttribute("aria-label", "Explain effective permissions for this binding");
      whyBtn.addEventListener("click", () => openExplainer(r));
      act.appendChild(whyBtn);

      // "Usage" affordance — opens the live counter panel.
      const usageBtn = document.createElement("button");
      usageBtn.type = "button";
      usageBtn.className = "btn btn-ghost btn-xs";
      usageBtn.textContent = "Usage";
      usageBtn.setAttribute("aria-label", "Show live usage counters for this binding");
      usageBtn.addEventListener("click", () => openUsagePanel(r));
      act.appendChild(usageBtn);

      const revokeBtn = document.createElement("button");
      revokeBtn.type = "button";
      revokeBtn.className = "btn btn-danger btn-xs binding-revoke";
      revokeBtn.textContent = "Revoke";
      revokeBtn.dataset.principalId = r.subject_ref || "";
      revokeBtn.dataset.objectKind = r.object_kind || "role";
      revokeBtn.dataset.objectRef = r.object_ref || "";
      revokeBtn.dataset.effect = effect;
      act.appendChild(revokeBtn);
      tr.appendChild(act);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  parent.appendChild(table);
}

// ---------------------------------------------------------------- create / revoke

// Validates and parses the attribute_predicates editor textarea.
// Returns null when empty, or throws on invalid JSON / invalid shape.
function parseAttributePredicates() {
  const raw = $("#binding-attr-predicates").value.trim();
  if (!raw) return null;
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (_e) {
    throw new Error("Attribute predicates must be valid JSON.");
  }
  if (!Array.isArray(parsed)) {
    throw new Error("Attribute predicates must be a JSON array.");
  }
  const KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
  const VALID_OPS = new Set(["in", "eq", "lte", "gte", "between", "lte:timestamp", "gte:timestamp"]);
  for (const p of parsed) {
    if (typeof p !== "object" || p === null) {
      throw new Error("Each predicate must be an object.");
    }
    if (!p.key || !KEY_RE.test(p.key)) {
      throw new Error(`Predicate key "${p.key}" must match [A-Za-z_][A-Za-z0-9_]*.`);
    }
    if (!VALID_OPS.has(p.op)) {
      throw new Error(`Unknown op "${p.op}". Valid: ${[...VALID_OPS].join(", ")}.`);
    }
    if (!Array.isArray(p.values) || !p.values.length) {
      throw new Error(`Predicate "${p.key}" must have a non-empty values array.`);
    }
  }
  return parsed;
}

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
  let preds;
  try {
    preds = parseAttributePredicates();
  } catch (err) {
    setStatus(statusEl, err.message, "error");
    return;
  }
  if (preds) body.attribute_predicates = preds;

  try {
    await postJSON(
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/grants`,
      body,
    );
    setStatus(statusEl, "Binding granted.", "info");
    $("#binding-object-ref").value = "";
    $("#binding-quota").value = "";
    $("#binding-attr-predicates").value = "";
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

// ---------------------------------------------------------------- effective-permissions explainer (#1390)
//
// "Why?" per binding row opens a dialog pre-seeded with the current principal
// and scope, lets the operator pick an action, then calls
// GET /admin/iam/effective and renders the full trace.
// Logic ported from governance.js; all server-derived values set via
// textContent — no innerHTML on response data.

const explainer = {
  principalId: null,
  principalLabel: null,
  catalogId: null,
  collectionId: null,
  resourceKind: null,
  resourceRef: null,
};

function openExplainer(row) {
  if (!state.principalId) return;
  explainer.principalId = state.principalId;
  explainer.principalLabel = state.principalLabel || state.principalId;
  explainer.catalogId = state.catalogId;
  explainer.collectionId = (row && row.resource_kind === "collection")
    ? (row.resource_ref || null)
    : state.collectionId;
  explainer.resourceKind = (row && row.resource_kind) || null;
  explainer.resourceRef = (row && row.resource_ref) || null;

  const subjectLine =
    `Subject: ${explainer.principalLabel}  (${explainer.principalId})`
    + (explainer.catalogId ? `  ·  catalog ${explainer.catalogId}` : "")
    + (explainer.collectionId ? `  ·  collection ${explainer.collectionId}` : "");
  $("#explainer-subject").textContent = subjectLine;
  $("#explainer-action").value = "GET";
  $("#explainer-action-custom").value = "";
  $("#explainer-action-custom").disabled = true;
  $("#explainer-result").style.display = "none";
  clear($("#explainer-grants tbody"));
  setStatus($("#explainer-status"), "");

  const dlg = $("#explainer-modal");
  if (typeof dlg.showModal === "function") dlg.showModal();
  else dlg.setAttribute("open", "");
}

function closeExplainer() {
  const dlg = $("#explainer-modal");
  if (typeof dlg.close === "function") dlg.close();
  else dlg.removeAttribute("open");
}

function selectedExplainerAction() {
  const sel = $("#explainer-action").value;
  if (sel === "__custom__") return $("#explainer-action-custom").value.trim();
  return sel;
}

async function runExplainer(e) {
  if (e) e.preventDefault();
  const action = selectedExplainerAction();
  if (!action) {
    setStatus($("#explainer-status"), "Pick or type an action to evaluate.", "error");
    return;
  }
  setStatus($("#explainer-status"), "Evaluating…");
  try {
    const params = new URLSearchParams({ principal_id: explainer.principalId, action });
    if (explainer.catalogId) params.set("catalog_id", explainer.catalogId);
    if (explainer.collectionId) params.set("collection_id", explainer.collectionId);
    if (explainer.resourceKind) params.set("resource_kind", explainer.resourceKind);
    if (explainer.resourceRef) params.set("resource_ref", explainer.resourceRef);
    const data = await getJSON(`/admin/iam/effective?${params.toString()}`);
    setStatus($("#explainer-status"), "");
    renderExplainerResult(data);
  } catch (err) {
    setStatus($("#explainer-status"), `Failed: ${err.message}`, "error");
  }
}

// Returns the index of the deciding grant, mirroring governance.js logic.
function findWinningGrantIndex(data) {
  if (!data || !Array.isArray(data.grants_considered)) return -1;
  const winnerEffect = (data.decision === "deny" && data.deny_precedence_applied)
    ? "deny"
    : (data.decision === "allow" ? "allow" : null);
  if (!winnerEffect) return -1;
  for (let i = 0; i < data.grants_considered.length; i++) {
    const g = data.grants_considered[i];
    if (g.matched && (g.effect || "allow") === winnerEffect) return i;
  }
  return -1;
}

// Renders the explainer result. Ported from governance.js renderExplainerResult.
// All server-derived strings are set via textContent.
function renderExplainerResult(data) {
  const result = $("#explainer-result");
  result.style.display = "";

  const verdict = data.decision === "allow" ? "ALLOW" : "DENY";
  const chip = $("#explainer-verdict-chip");
  chip.textContent = verdict;
  chip.className = `chip effect-${verdict}`;

  const denyNote = $("#explainer-deny-note");
  denyNote.style.display = data.deny_precedence_applied ? "" : "none";

  $("#explainer-reason").textContent = data.decision_reason || "";

  if (data.compiled_rule_version) {
    const rv = $("#explainer-rule-version");
    if (rv) rv.textContent = `rule version: ${data.compiled_rule_version}`;
  }

  const tbody = $("#explainer-grants tbody");
  clear(tbody);
  const grants = Array.isArray(data.grants_considered) ? data.grants_considered : [];
  if (!grants.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 8;
    td.textContent = "No grants were walked for this request.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const winnerIdx = findWinningGrantIndex(data);
  grants.forEach((g, idx) => {
    const tr = document.createElement("tr");
    const effect = (g.effect === "deny") ? "deny" : "allow";
    if (idx === winnerIdx) {
      tr.classList.add("row-winner");
      if (effect === "deny") tr.classList.add("row-deny");
    }

    const kindCell = document.createElement("td");
    kindCell.textContent = g.object_kind || "policy";

    const refCell = document.createElement("td");
    const refCode = document.createElement("code");
    refCode.textContent = g.object_ref || "—";
    refCell.appendChild(refCode);

    const effCell = document.createElement("td");
    const effChip = document.createElement("span");
    effChip.className = `chip effect-${effect === "deny" ? "DENY" : "ALLOW"}`;
    effChip.textContent = effect;
    effCell.appendChild(effChip);

    const scopeCell = document.createElement("td");
    if (g.resource_kind && g.resource_ref) {
      scopeCell.textContent = `${g.resource_kind} ${g.resource_ref}`;
    } else {
      scopeCell.className = "muted";
      scopeCell.textContent = "whole-catalog";
    }

    const matchedCell = document.createElement("td");
    matchedCell.textContent = g.matched ? "✓" : "✗";
    matchedCell.className = g.matched ? "" : "muted";

    const whyCell = document.createElement("td");
    whyCell.className = "muted";
    whyCell.textContent = g.why_not || "";

    const validityCell = document.createElement("td");
    validityCell.className = "muted";
    if (!g.valid_from && !g.valid_until) {
      validityCell.textContent = "—";
    } else {
      const inWin = g.in_validity_window !== false;
      validityCell.textContent =
        `${fmtDateTime(g.valid_from)} → ${fmtDateTime(g.valid_until)}`
        + (inWin ? "" : " (out of window)");
    }

    const condCell = document.createElement("td");
    const conds = Array.isArray(g.conditions_evaluated) ? g.conditions_evaluated : [];
    if (!conds.length) {
      condCell.className = "muted";
      condCell.textContent = "—";
    } else {
      const pre = document.createElement("pre");
      pre.className = "cell-json";
      pre.textContent = JSON.stringify(conds, null, 2);
      condCell.appendChild(pre);
    }

    tr.append(kindCell, refCell, effCell, scopeCell, matchedCell,
              whyCell, validityCell, condCell);
    tbody.appendChild(tr);
  });
}

// ---------------------------------------------------------------- per-binding live counter panel
//
// "Usage" per binding row opens the usage modal and calls
// GET /admin/iam/usage/grants to render live counter state for the grant.
// All server-derived values set via textContent — no innerHTML.

const usagePanel = {
  grantId: null,
  principalId: null,
  catalogId: null,
  subjectLine: "",
};

function openUsagePanel(row) {
  if (!state.principalId) return;
  usagePanel.grantId = row.id ? String(row.id) : null;
  usagePanel.principalId = state.principalId;
  usagePanel.catalogId = state.catalogId;

  usagePanel.subjectLine =
    `Subject: ${state.principalLabel || state.principalId}  (${state.principalId})`
    + (usagePanel.catalogId ? `  ·  catalog ${usagePanel.catalogId}` : "")
    + `  ·  ${row.object_kind || "role"} ${row.object_ref || ""}`;
  $("#usage-modal-subject").textContent = usagePanel.subjectLine;
  $("#usage-stale-banner-modal").style.display = "none";
  $("#usage-stale-banner-modal").textContent = "";
  setStatus($("#usage-modal-status"), "");
  clear($("#usage-modal-body"));

  const dlg = $("#usage-modal");
  if (typeof dlg.showModal === "function") dlg.showModal();
  else dlg.setAttribute("open", "");

  refreshUsagePanel();
}

function closeUsagePanel() {
  const dlg = $("#usage-modal");
  if (typeof dlg.close === "function") dlg.close();
  else dlg.removeAttribute("open");
}

async function refreshUsagePanel() {
  setStatus($("#usage-modal-status"), "Loading…");
  try {
    const url = `/admin/iam/usage/grants?principal_id=${encodeURIComponent(usagePanel.principalId)}`
      + (usagePanel.catalogId ? `&catalog_id=${encodeURIComponent(usagePanel.catalogId)}` : "");
    const data = await getJSON(url);
    setStatus($("#usage-modal-status"), "");
    renderUsagePanel(data);
  } catch (err) {
    setStatus($("#usage-modal-status"), `Failed: ${err.message}`, "error");
  }
}

// Renders the usage modal body. Logic ported from governance.js renderUsagePanel.
function renderUsagePanel(data) {
  const banner = $("#usage-stale-banner-modal");
  if (data && data.valkey_available === false) {
    banner.textContent = "Live Valkey counters unavailable — figures may be stale (PG fallback).";
    banner.style.display = "";
  } else {
    banner.textContent = "";
    banner.style.display = "none";
  }

  const body = $("#usage-modal-body");
  clear(body);

  const entries = (data && Array.isArray(data.entries)) ? data.entries : [];
  const target = usagePanel.grantId
    ? entries.find((e) => String(e.grant_id) === usagePanel.grantId)
    : null;

  if (!target) {
    const note = document.createElement("p");
    note.className = "muted";
    note.textContent = "No counter state attached to this binding at the current scope.";
    body.appendChild(note);
    return;
  }

  const validity = document.createElement("p");
  validity.className = "muted";
  if (!target.valid_from && !target.valid_until) {
    validity.textContent = "Validity: always";
  } else {
    validity.textContent =
      `Validity: ${fmtDateTime(target.valid_from)} → ${fmtDateTime(target.valid_until)}`;
  }
  body.appendChild(validity);

  const specHead = document.createElement("h4");
  specHead.className = "ci-subhead";
  specHead.textContent = "Quota spec";
  body.appendChild(specHead);
  const specPre = document.createElement("pre");
  specPre.className = "cell-json";
  specPre.textContent = target.quota_spec
    ? JSON.stringify(target.quota_spec, null, 2)
    : "(no per-binding quota)";
  body.appendChild(specPre);

  const head = document.createElement("h4");
  head.className = "ci-subhead";
  head.textContent = "Live counters";
  body.appendChild(head);

  const table = document.createElement("table");
  table.className = "kv-table";
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["Kind", "Count", "Limit", "Remaining", "Window", "Window start"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  const counters = target.counters || {};
  const rows = [];
  if (counters.rate_limit) {
    const rl = counters.rate_limit;
    rows.push({
      kind: "rate_limit",
      count: String(rl.count),
      limit: String(rl.limit),
      remaining: String(rl.remaining),
      window: `${rl.window_seconds}s`,
      windowStart: rl.window_start ? fmtDateTime(rl.window_start) : "—",
    });
  }
  if (counters.max_count) {
    const mc = counters.max_count;
    rows.push({
      kind: "max_count",
      count: String(mc.count),
      limit: String(mc.limit),
      remaining: String(mc.remaining),
      window: "lifetime",
      windowStart: "—",
    });
  }

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 6;
    td.textContent = "No counter conditions configured on this binding.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const r of rows) {
      const tr = document.createElement("tr");
      for (const v of [r.kind, r.count, r.limit, r.remaining, r.window, r.windowStart]) {
        const td = document.createElement("td");
        td.textContent = v;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }
  table.appendChild(tbody);
  body.appendChild(table);

  if (data.fetched_at) {
    const ts = document.createElement("p");
    ts.className = "muted";
    ts.textContent = `Fetched at ${data.fetched_at}`;
    body.appendChild(ts);
  }
}

// ---------------------------------------------------------------- ABAC stamping config editor (#1443)
//
// GET + PUT /configs/catalogs/{cat}/collections/{col}/plugins/attribute_stamping_policy
// Body shape: { "attribute_paths": { "dept": "$.properties.department", ... } }
// Disabled until both catalog and collection are selected.

async function loadStampingConfig() {
  const statusEl = $("#stamping-status");
  const ta = $("#stamping-attr-paths");
  if (!state.catalogId || !state.collectionId) {
    setStatus(statusEl, "Pick a catalog and collection to manage stamping config.", "info");
    ta.value = "";
    ta.disabled = true;
    return;
  }
  ta.disabled = false;
  setStatus(statusEl, "Loading…");
  try {
    const data = await getJSON(
      `/configs/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/plugins/attribute_stamping_policy`,
    );
    const paths = (data && data.attribute_paths) ? data.attribute_paths : {};
    ta.value = JSON.stringify(paths, null, 2);
    setStatus(statusEl, `Loaded. ${Object.keys(paths).length} path(s) configured.`);
  } catch (e) {
    // 404 = not configured yet; surface as empty
    if (e.status === 404) {
      ta.value = "{}";
      setStatus(statusEl, "Not yet configured for this collection (will create on save).");
    } else {
      setStatus(statusEl, `Load failed: ${e.message}`, "error");
    }
  }
}

async function saveStampingConfig(ev) {
  ev.preventDefault();
  const statusEl = $("#stamping-status");
  const ta = $("#stamping-attr-paths");
  if (!state.catalogId || !state.collectionId) {
    setStatus(statusEl, "Pick a catalog and collection first.", "error");
    return;
  }
  let paths;
  try {
    paths = JSON.parse(ta.value.trim() || "{}");
  } catch (_e) {
    setStatus(statusEl, "Attribute paths must be valid JSON object.", "error");
    return;
  }
  if (typeof paths !== "object" || Array.isArray(paths)) {
    setStatus(statusEl, "Attribute paths must be a JSON object (key → JSONPath).", "error");
    return;
  }
  try {
    await putJSON(
      `/configs/catalogs/${encodeURIComponent(state.catalogId)}/collections/${encodeURIComponent(state.collectionId)}/plugins/attribute_stamping_policy`,
      { attribute_paths: paths },
    );
    setStatus(statusEl, `Saved. ${Object.keys(paths).length} path(s) active.`);
  } catch (e) {
    setStatus(statusEl, `Save failed: ${e.message}`, "error");
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
    loadStampingConfig();
  });
  $("#scope-collection").addEventListener("change", (e) => {
    state.collectionId = e.target.value || null;
    refreshBindings();
    refreshReverse();
    loadStampingConfig();
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

  // Explainer modal wiring
  const explainerForm = $("#explainer-form");
  if (explainerForm) {
    explainerForm.addEventListener("submit", runExplainer);
    $("#explainer-close-btn").addEventListener("click", closeExplainer);
    const actionSel = $("#explainer-action");
    if (actionSel) {
      actionSel.addEventListener("change", () => {
        const custom = actionSel.value === "__custom__";
        const input = $("#explainer-action-custom");
        input.disabled = !custom;
        if (custom) input.focus();
      });
    }
  }

  // Usage modal wiring
  const usageCloseBtn = $("#usage-close-btn");
  if (usageCloseBtn) usageCloseBtn.addEventListener("click", closeUsagePanel);
  const usageRefreshBtn = $("#usage-refresh-btn");
  if (usageRefreshBtn) usageRefreshBtn.addEventListener("click", refreshUsagePanel);

  // ABAC stamping config wiring
  const stampingForm = $("#stamping-form");
  if (stampingForm) {
    stampingForm.addEventListener("submit", saveStampingConfig);
  }
  const stampingReloadBtn = $("#stamping-reload-btn");
  if (stampingReloadBtn) {
    stampingReloadBtn.addEventListener("click", (e) => {
      e.preventDefault();
      loadStampingConfig();
    });
  }
}

// ---------------------------------------------------------------- boot

(async function init() {
  bind();
  await loadCatalogs();
  loadStampingConfig();
})();
