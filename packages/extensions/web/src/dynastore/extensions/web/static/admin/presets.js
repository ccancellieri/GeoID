// Presets admin page (#1412). Vanilla JS, mirrors access-bindings.js style.
// Exposes the preset registry and lifecycle via the /admin/presets endpoints
// introduced in PR-1. All preset names are discovered dynamically — no preset
// name is hardcoded here. Authorization is enforced entirely server-side.

import { getJSON, postJSON, deleteJSON } from "../common/api.js";
import { mountSchemaForm } from "../common/schema-form.js";
import { mountContextBar } from "../common/context-bar.js";

// ---------------------------------------------------------------- state

const state = {
  // Current scope (platform / catalog / collection).
  catalogId: null,
  collectionId: null,

  // Filter values.
  q: "",
  filterName: "",
  tier: "",
  keywords: "",

  // Pagination.
  cursor: null,
  prevCursors: [], // stack of previous cursor positions for "back"
  limit: 20,

  // Selected preset.
  selectedPreset: null,   // full detail object returned by GET /admin/presets/{name}
  paramsForm: null,       // { getPatch, reset } from mountSchemaForm
  paramsSchemaSupported: false,

  // IAM keywords: if any of these appear in the preset's keywords array the
  // rollback dialog shows the self-lockout warning.
  iamKeywords: ["iam"],
};

const $ = (sel) => document.querySelector(sel);

function clear(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
}

function setStatus(el, msg, level) {
  el.textContent = msg || "";
  if (level) el.dataset.level = level;
  else delete el.dataset.level;
}

function fmtDateTime(value) {
  if (!value) return "—";
  try {
    return new Date(value).toISOString().replace("T", " ").slice(0, 19) + " UTC";
  } catch (_e) {
    return String(value);
  }
}

// ---------------------------------------------------------------- scope helpers

/** Build the URL-family prefix for the active scope. */
function scopePrefix() {
  if (state.catalogId && state.collectionId) {
    return (
      `/admin/catalogs/${encodeURIComponent(state.catalogId)}`
      + `/collections/${encodeURIComponent(state.collectionId)}`
    );
  }
  if (state.catalogId) {
    return `/admin/catalogs/${encodeURIComponent(state.catalogId)}`;
  }
  return "/admin";
}

/**
 * Decide the tier the current scope corresponds to, so we can highlight
 * which presets are callable here. The backend enforces this; the UI uses
 * it for informational badges only.
 */
function activeTierLabel() {
  if (state.catalogId && state.collectionId) return "collection";
  if (state.catalogId) return "catalog";
  return "platform";
}

/** True when a preset's tier is compatible with the active scope. */
function tierCompatible(presetTier) {
  const active = activeTierLabel();
  if (!presetTier) return true;   // unknown — optimistically allow
  // Platform presets apply at platform scope. Catalog presets at catalog or
  // collection scope (if catalog_scopable). Collection presets at collection.
  // items/assets presets are flexible (catalog_scopable flag arbitrates).
  if (active === "platform" && presetTier === "platform") return true;
  if (active === "catalog" && (presetTier === "catalog" || presetTier === "items" || presetTier === "assets")) return true;
  if (active === "collection" && (presetTier === "collection" || presetTier === "items" || presetTier === "assets")) return true;
  return false;
}

// ---------------------------------------------------------------- preset list

async function loadPresets(resetPagination) {
  if (resetPagination) {
    state.cursor = null;
    state.prevCursors = [];
  }

  const params = new URLSearchParams();
  if (state.q) params.set("q", state.q);
  if (state.filterName) params.set("name", state.filterName);
  if (state.tier) params.set("tier", state.tier);
  if (state.keywords) params.set("keywords", state.keywords);
  if (state.cursor) params.set("cursor", state.cursor);
  params.set("limit", String(state.limit));

  const qs = params.toString();
  const path = `/admin/presets${qs ? "?" + qs : ""}`;
  setStatus($("#filter-status"), "Loading…");

  try {
    const data = await getJSON(path);
    const items = data.presets || data.items || [];
    const nextCursor = data.next_cursor || null;

    renderPresetList(items);
    updatePagination(nextCursor);
    setStatus($("#filter-status"), `${items.length} preset(s) shown.`);
    $("#list-meta").textContent = `GET ${path}`;
  } catch (e) {
    setStatus($("#filter-status"), `Failed: ${e.message}`, "error");
  }
}

function renderPresetList(items) {
  const container = $("#presets-list");
  clear(container);
  if (!items || !items.length) {
    const empty = document.createElement("div");
    empty.className = "hint";
    empty.textContent = "No presets match the current filters.";
    container.appendChild(empty);
    return;
  }

  const activeTier = activeTierLabel();

  for (const preset of items) {
    const row = document.createElement("div");
    row.className = "result-row";
    row.setAttribute("role", "listitem");
    row.dataset.name = preset.name;

    // Left column: name + tier badge + keywords
    const nameCol = document.createElement("div");
    nameCol.className = "result-id";

    const nameSpan = document.createElement("span");
    nameSpan.textContent = preset.name;
    nameCol.appendChild(nameSpan);

    if (preset.tier) {
      const tierChip = document.createElement("span");
      tierChip.className = "chip";
      tierChip.textContent = preset.tier;
      if (tierCompatible(preset.tier)) {
        tierChip.classList.add("effect-ALLOW");
      }
      nameCol.appendChild(tierChip);
    }

    if (preset.catalog_scopable && activeTier === "catalog") {
      const scopeChip = document.createElement("span");
      scopeChip.className = "chip";
      scopeChip.textContent = "catalog-scopable";
      nameCol.appendChild(scopeChip);
    }

    // Keyword chips
    if (preset.keywords && preset.keywords.length) {
      const kwWrap = document.createElement("div");
      kwWrap.className = "result-meta";
      for (const kw of preset.keywords) {
        const kwChip = document.createElement("span");
        kwChip.className = "chip";
        kwChip.textContent = kw;
        kwWrap.appendChild(kwChip);
      }
      nameCol.appendChild(kwWrap);
    }
    row.appendChild(nameCol);

    // Right column: description + pick button
    const metaCol = document.createElement("div");
    metaCol.className = "result-meta";
    metaCol.textContent = preset.description || "";
    row.appendChild(metaCol);

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "result-pick";
    btn.textContent = "View";
    btn.setAttribute("aria-label", `View preset ${preset.name}`);
    row.appendChild(btn);

    container.appendChild(row);
  }
}

function updatePagination(nextCursor) {
  const paginationEl = $("#presets-pagination");
  const nextBtn = $("#presets-next-btn");
  const prevBtn = $("#presets-prev-btn");
  const cursorLabel = $("#presets-cursor-label");

  if (!state.cursor && !nextCursor) {
    paginationEl.style.display = "none";
    return;
  }
  paginationEl.style.display = "";
  nextBtn.disabled = !nextCursor;
  if (nextCursor) nextBtn.dataset.next = nextCursor;
  else delete nextBtn.dataset.next;

  prevBtn.disabled = state.prevCursors.length === 0;
  cursorLabel.textContent = state.cursor ? `after: ${state.cursor}` : "page 1";
}

// ---------------------------------------------------------------- preset detail

async function selectPreset(name) {
  state.selectedPreset = null;
  state.paramsForm = null;
  state.paramsSchemaSupported = false;

  setStatus($("#detail-action-status"), "Loading…");
  try {
    const detail = await getJSON(`/admin/presets/${encodeURIComponent(name)}`);
    state.selectedPreset = detail;
    renderDetail(detail);
    // Fetch docs in parallel with history — errors are swallowed inside.
    await Promise.all([
      refreshAppliedHistory(),
      fetchAndRenderDocs(name),
    ]);
  } catch (e) {
    setStatus($("#detail-action-status"), `Failed to load preset: ${e.message}`, "error");
  }
}

function renderDetail(detail) {
  const plate = $("#detail-plate");
  plate.style.display = "";

  $("#detail-title").textContent = detail.name || "—";
  $("#detail-route").textContent = `GET /admin/presets/${detail.name}`;
  $("#detail-description").textContent = detail.description || "";

  const tierEl = $("#detail-tier");
  tierEl.textContent = detail.tier || "—";
  tierEl.className = "chip";
  if (tierCompatible(detail.tier)) tierEl.classList.add("effect-ALLOW");

  const kwEl = $("#detail-keywords");
  clear(kwEl);
  for (const kw of (detail.keywords || [])) {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = kw;
    kwEl.appendChild(chip);
  }

  // Params form — use schema-form.js if schema is present and parseable.
  const paramsSection = $("#detail-params-section");
  const paramsFallback = $("#detail-params-fallback");
  paramsSection.style.display = "none";
  paramsFallback.style.display = "none";

  if (detail.params_schema && typeof detail.params_schema === "object") {
    const schema = detail.params_schema;
    const hasProperties = schema.properties && Object.keys(schema.properties).length > 0;
    if (hasProperties) {
      try {
        const formContainer = $("#detail-params-form");
        clear(formContainer);
        state.paramsForm = mountSchemaForm(formContainer, {
          schema,
          resolved: {},
          explicit: {},
          allowInherit: false,
          onDirty: () => {},
        });
        state.paramsSchemaSupported = true;
        paramsSection.style.display = "";
      } catch (_e) {
        // schema-form could not render this schema — fall through to raw JSON.
        state.paramsSchemaSupported = false;
        state.paramsForm = null;
        renderParamsFallback(detail.params_schema);
        paramsFallback.style.display = "";
      }
    }
  }

  // Action buttons
  setStatus($("#detail-action-status"), "");
  const compatible = tierCompatible(detail.tier);
  $("#btn-apply").disabled = !compatible;
  $("#btn-dry-run").disabled = !compatible;
  // Rollback is enabled only after we check applied history (done after init).
  $("#btn-rollback").disabled = true;

  // Hide previous dry-run result and docs panel (refreshed async below).
  $("#dry-run-result").style.display = "none";
  $("#preset-docs-panel").style.display = "none";
}

function renderParamsFallback(schema) {
  const pre = $("#detail-params-json");
  pre.textContent = JSON.stringify(schema, null, 2);
}

// ---------------------------------------------------------------- docs panel

/**
 * Fetch GET /admin/presets/{name}/describe?format=json and render the
 * How-to / Docs panel. Silently hides the panel on failure so it never
 * breaks the primary detail view.
 */
async function fetchAndRenderDocs(name) {
  const panel = $("#preset-docs-panel");
  const body = $("#preset-docs-body");
  clear(body);
  panel.style.display = "none";

  try {
    const docs = await getJSON(
      `/admin/presets/${encodeURIComponent(name)}/describe?format=json`,
    );
    renderDocsPanel(docs);
  } catch (_e) {
    // Docs endpoint is best-effort — do not surface errors here.
  }
}

/**
 * Render the docs payload into #preset-docs-panel / #preset-docs-body.
 * Uses .ci-docs definition-list style that already exists in admin.css.
 */
function renderDocsPanel(docs) {
  const panel = $("#preset-docs-panel");
  const body = $("#preset-docs-body");
  clear(body);

  // Description block.
  if (docs.description) {
    const descP = document.createElement("p");
    descP.className = "hint";
    descP.textContent = docs.description;
    body.appendChild(descP);
  }

  // Tier + keywords summary row.
  const metaRow = document.createElement("div");
  metaRow.className = "form-row";

  if (docs.tier) {
    const tierDiv = document.createElement("div");
    const tierLabel = document.createElement("span");
    tierLabel.className = "kv-label";
    tierLabel.textContent = "Tier";
    const tierChip = document.createElement("span");
    tierChip.className = "chip";
    tierChip.textContent = docs.tier;
    tierDiv.appendChild(tierLabel);
    tierDiv.appendChild(tierChip);
    metaRow.appendChild(tierDiv);
  }

  if (docs.keywords && docs.keywords.length) {
    const kwDiv = document.createElement("div");
    const kwLabel = document.createElement("span");
    kwLabel.className = "kv-label";
    kwLabel.textContent = "Keywords";
    kwDiv.appendChild(kwLabel);
    for (const kw of docs.keywords) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = kw;
      kwDiv.appendChild(chip);
    }
    metaRow.appendChild(kwDiv);
  }

  if (metaRow.firstChild) body.appendChild(metaRow);

  // Parameters section — definition list.
  const paramDocs = buildParamDocs(docs);
  if (paramDocs) {
    const h5 = document.createElement("h5");
    h5.className = "ci-subhead";
    h5.textContent = "Parameters";
    body.appendChild(h5);
    body.appendChild(paramDocs);
  }

  // Examples section.
  if (docs.examples && docs.examples.length) {
    const h5 = document.createElement("h5");
    h5.className = "ci-subhead";
    h5.textContent = "Examples";
    body.appendChild(h5);

    for (const ex of docs.examples) {
      body.appendChild(buildExampleBlock(docs, ex));
    }
  }

  panel.style.display = "";
}

/**
 * Build a <dl class="ci-docs"> from _meta.docs (preferred) or
 * params_schema property descriptions (fallback).
 * Returns null when no parameter documentation is available.
 */
function buildParamDocs(docs) {
  const metaDocs = docs._meta && docs._meta.docs;
  const schemaProps = docs.params_schema && docs.params_schema.properties;

  if (!metaDocs && !schemaProps) return null;

  const dl = document.createElement("dl");
  dl.className = "ci-docs";

  // Collect field names: _meta.docs keys first, then schema keys for any
  // fields not covered by _meta.docs.
  const fields = new Set([
    ...Object.keys(metaDocs || {}),
    ...Object.keys(schemaProps || {}),
  ]);

  if (!fields.size) return null;

  for (const field of fields) {
    const description = (metaDocs && metaDocs[field])
      || (schemaProps && schemaProps[field] && schemaProps[field].description)
      || "";

    const dt = document.createElement("dt");
    dt.textContent = field;
    dl.appendChild(dt);

    const dd = document.createElement("dd");
    dd.textContent = description || "—";

    // Mutability badge from _meta.mutability if present.
    const mutability = docs._meta && docs._meta.mutability && docs._meta.mutability[field];
    if (mutability) {
      const badge = document.createElement("span");
      badge.className = "chip";
      badge.textContent = mutability;
      badge.style.marginLeft = "6px";
      dd.appendChild(badge);
    }

    dl.appendChild(dd);
  }

  return dl;
}

/**
 * Build one example block: summary + params JSON + collapsible
 * resulting_config JSON + "Load example" button.
 */
function buildExampleBlock(docs, ex) {
  const wrap = document.createElement("div");
  wrap.className = "plate-note";
  wrap.style.borderLeft = "3px solid var(--rule-hair)";
  wrap.style.paddingLeft = "10px";
  wrap.style.marginBottom = "12px";

  // Summary line.
  if (ex.name || ex.summary) {
    const sumP = document.createElement("p");
    sumP.className = "hint";
    const strong = document.createElement("strong");
    strong.textContent = ex.name || "";
    sumP.appendChild(strong);
    if (ex.name && ex.summary) {
      sumP.appendChild(document.createTextNode(" — "));
    }
    if (ex.summary) {
      sumP.appendChild(document.createTextNode(ex.summary));
    }
    wrap.appendChild(sumP);
  }

  // Params JSON block.
  if (ex.params && Object.keys(ex.params).length > 0) {
    const paramsLabel = document.createElement("div");
    paramsLabel.className = "plate-meta";
    paramsLabel.textContent = "params (preset payload)";
    wrap.appendChild(paramsLabel);

    const paramsPre = document.createElement("pre");
    paramsPre.className = "hint";
    paramsPre.textContent = JSON.stringify(ex.params, null, 2);
    wrap.appendChild(paramsPre);
  }

  // Resulting config — collapsible via <details>.
  if (ex.resulting_config != null) {
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.className = "hint";
    summary.textContent = "Resulting configuration";
    details.appendChild(summary);

    const cfgPre = document.createElement("pre");
    cfgPre.className = "hint";
    cfgPre.textContent = JSON.stringify(ex.resulting_config, null, 2);
    details.appendChild(cfgPre);
    wrap.appendChild(details);
  } else if (ex.error) {
    const errP = document.createElement("p");
    errP.className = "hint";
    errP.dataset.level = "error";
    errP.textContent = ex.error;
    wrap.appendChild(errP);
  }

  // "Load example" button — populates the schema form.
  const loadBtn = document.createElement("button");
  loadBtn.type = "button";
  loadBtn.className = "btn btn-secondary";
  loadBtn.textContent = "Load example";
  loadBtn.setAttribute("aria-label", `Load example${ex.name ? ": " + ex.name : ""}`);
  loadBtn.addEventListener("click", () => loadExample(docs, ex.params || {}));
  wrap.appendChild(loadBtn);

  return wrap;
}

/**
 * Populate the schema form with example params.
 *
 * Mechanism: re-mount the schema form with the example params as both
 * `resolved` (shown as initial values) and `explicit` (so all fields are
 * pre-filled), then replace state.paramsForm with the new handle. If the
 * form was never mounted (no params_schema or unsupported schema), this is
 * a no-op — the "Load example" button remains available but inactive.
 */
function loadExample(docs, params) {
  const detail = state.selectedPreset;
  if (!detail) return;

  const schema = detail.params_schema;
  if (!schema || typeof schema !== "object") return;
  const hasProperties = schema.properties && Object.keys(schema.properties).length > 0;
  if (!hasProperties) return;

  const paramsSection = $("#detail-params-section");
  const paramsFallback = $("#detail-params-fallback");

  try {
    const formContainer = $("#detail-params-form");
    clear(formContainer);
    state.paramsForm = mountSchemaForm(formContainer, {
      schema,
      resolved: params,
      explicit: params,
      allowInherit: false,
      onDirty: () => {},
    });
    state.paramsSchemaSupported = true;
    paramsSection.style.display = "";
    paramsFallback.style.display = "none";
  } catch (_e) {
    // Schema-form could not render — leave the existing form untouched.
  }
}

// ---------------------------------------------------------------- dry-run

async function dryRun() {
  if (!state.selectedPreset) return;
  const name = state.selectedPreset.name;
  const prefix = scopePrefix();
  const path = `${prefix}/presets/${encodeURIComponent(name)}/dry-run`;
  const body = state.paramsForm ? state.paramsForm.getPatch() : {};

  setStatus($("#detail-action-status"), "Running dry-run…");
  try {
    const plan = await postJSON(path, body);
    renderDryRunResult(plan);
    setStatus($("#detail-action-status"), "Dry-run complete — no changes written.");
  } catch (e) {
    setStatus($("#detail-action-status"), `Dry-run failed: ${e.message}`, "error");
  }
}

function renderDryRunResult(plan) {
  const resultEl = $("#dry-run-result");
  resultEl.style.display = "";

  $("#dry-run-scope-label").textContent = `scope_key: ${plan.scope_key || "platform"}`;

  const warningsEl = $("#dry-run-warnings");
  const warningText = $("#dry-run-warning-text");
  if (plan.warnings && plan.warnings.length) {
    warningText.textContent = plan.warnings.join(" | ");
    warningsEl.style.display = "";
  } else {
    warningsEl.style.display = "none";
  }

  const tbody = $("#dry-run-tbody");
  clear(tbody);
  for (const entry of (plan.entries || [])) {
    const tr = document.createElement("tr");

    const kindTd = document.createElement("td");
    const kindChip = document.createElement("span");
    kindChip.className = "chip";
    kindChip.textContent = entry.kind || "—";
    kindTd.appendChild(kindChip);
    tr.appendChild(kindTd);

    const targetTd = document.createElement("td");
    const targetCode = document.createElement("code");
    targetCode.textContent = entry.target || "—";
    targetTd.appendChild(targetCode);
    tr.appendChild(targetTd);

    const detailTd = document.createElement("td");
    detailTd.textContent = entry.detail || "—";
    tr.appendChild(detailTd);

    tbody.appendChild(tr);
  }

  if (!tbody.firstChild) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.className = "hint";
    td.textContent = "No plan entries returned.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
}

// ---------------------------------------------------------------- apply

async function applyPreset(force) {
  if (!state.selectedPreset) return;
  const name = state.selectedPreset.name;
  const prefix = scopePrefix();
  // `force` is a query parameter on the backend (Query(False)); put it on the
  // URL, not in the request body.
  const qs = force ? "?force=true" : "";
  const path = `${prefix}/presets/${encodeURIComponent(name)}${qs}`;
  const body = state.paramsForm ? state.paramsForm.getPatch() : {};

  setStatus($("#detail-action-status"), "Applying…");
  try {
    const result = await postJSON(path, body);
    const applied = result.applied || [];
    setStatus(
      $("#detail-action-status"),
      `Applied: ${applied.join(", ") || "(no slots)"}`,
    );
    await refreshAppliedHistory();
  } catch (e) {
    if (e.status === 409) {
      // Params mismatch — show the 409 detail inline and offer force retry.
      let detail = e.body;
      if (typeof detail === "string") {
        try { detail = JSON.parse(detail); } catch (_) { /* ignore */ }
      }
      const msg = (detail && detail.detail) ? String(detail.detail) : e.message;
      setStatus(
        $("#detail-action-status"),
        `409 conflict: ${msg} — edit parameters and retry.`,
        "error",
      );
    } else {
      setStatus($("#detail-action-status"), `Apply failed: ${e.message}`, "error");
    }
  }
}

// ---------------------------------------------------------------- rollback

function isIamPreset() {
  const kws = (state.selectedPreset && state.selectedPreset.keywords) || [];
  return state.iamKeywords.some((iam) => kws.includes(iam));
}

function rollbackPreset() {
  if (!state.selectedPreset) return;
  if (isIamPreset()) {
    // Show the self-lockout confirmation dialog.
    const dlg = $("#lockout-dialog");
    $("#lockout-force-checkbox").checked = false;
    $("#lockout-confirm-btn").disabled = true;
    dlg.showModal();
    return;
  }
  doRollback(false);
}

async function doRollback(forceSelfRevoke) {
  if (!state.selectedPreset) return;
  const name = state.selectedPreset.name;
  const prefix = scopePrefix();
  let path = `${prefix}/presets/${encodeURIComponent(name)}`;
  if (forceSelfRevoke) path += "?force_self_revoke=true";

  setStatus($("#detail-action-status"), "Rolling back…");
  try {
    const result = await deleteJSON(path);
    const deleted = result.deleted || [];
    setStatus(
      $("#detail-action-status"),
      deleted.length
        ? `Rolled back: ${deleted.join(", ")}`
        : "Nothing to roll back (no matching persisted rows).",
    );
    $("#btn-rollback").disabled = true;
    await refreshAppliedHistory();
  } catch (e) {
    if (e.status === 409) {
      // Diverged rows — parse and display the diverged slots.
      let detail = e.body;
      if (typeof detail === "string") {
        try { detail = JSON.parse(detail); } catch (_) { /* ignore */ }
      }
      const diverged = (detail && detail.detail && detail.detail.diverged) || [];
      const slots = diverged.map((d) => d.slot || d.class || "?").join(", ");
      setStatus(
        $("#detail-action-status"),
        `409 conflict: row(s) diverged [${slots}]. Edit the configuration or force-PUT before rollback.`,
        "error",
      );
    } else {
      setStatus(
        $("#detail-action-status"),
        `Rollback failed: ${e.message}`,
        "error",
      );
    }
  }
}

// ---------------------------------------------------------------- applied history

/**
 * There is no bulk GET /admin/presets/applied endpoint in this revision.
 * We assemble the history client-side by issuing a dry-run for the selected
 * preset at the current scope and checking whether the bundle already matches.
 * A simpler heuristic is to attempt a dry-run and infer from the plan whether
 * the preset is already applied — but that is too noisy at page load for large
 * catalogs. Instead we only check the selected preset.
 *
 * NOTE: a bulk endpoint is tracked as a follow-up to #1412.
 */
async function refreshAppliedHistory() {
  const plate = $("#history-plate");
  const container = $("#history-list");
  clear(container);
  plate.style.display = "none";

  if (!state.selectedPreset) return;

  const name = state.selectedPreset.name;
  const compatible = tierCompatible(state.selectedPreset.tier);
  if (!compatible) return;

  plate.style.display = "";
  const hint = document.createElement("div");
  hint.className = "hint";
  hint.textContent = "Checking…";
  container.appendChild(hint);

  try {
    // Attempt a dry-run: if it returns 0 entries or if the entries are all
    // "no-op" kind, the preset is considered applied. If entries are present
    // we treat the preset as not (or partially) applied.
    const prefix = scopePrefix();
    const path = `${prefix}/presets/${encodeURIComponent(name)}/dry-run`;
    const plan = await postJSON(path, {});
    const entries = plan.entries || [];
    const noOpOnly = entries.every((e) => (e.kind || "").toLowerCase() === "noop");
    const isApplied = entries.length === 0 || noOpOnly;

    clear(container);
    if (isApplied) {
      const row = buildAppliedRow(name, "applied", plan.scope_key);
      container.appendChild(row);
      // Enable rollback.
      $("#btn-rollback").disabled = false;
    } else {
      const row = buildAppliedRow(name, "not-applied", plan.scope_key);
      container.appendChild(row);
      $("#btn-rollback").disabled = true;
    }
  } catch (e) {
    clear(container);
    const errDiv = document.createElement("div");
    errDiv.className = "hint";
    errDiv.dataset.level = "error";
    errDiv.textContent = `Could not determine applied status: ${e.message}`;
    container.appendChild(errDiv);
    $("#btn-rollback").disabled = true;
  }
}

function buildAppliedRow(name, status, scopeKey) {
  const row = document.createElement("div");
  row.className = "result-row";
  row.setAttribute("role", "listitem");

  const nameEl = document.createElement("div");
  nameEl.className = "result-id";
  const nameSpan = document.createElement("span");
  nameSpan.textContent = name;
  nameEl.appendChild(nameSpan);

  const chip = document.createElement("span");
  chip.className = "chip";
  if (status === "applied") {
    chip.textContent = "applied";
    chip.classList.add("effect-ALLOW");
  } else {
    chip.textContent = "not applied";
  }
  nameEl.appendChild(chip);
  row.appendChild(nameEl);

  const scopeEl = document.createElement("div");
  scopeEl.className = "result-meta";
  scopeEl.textContent = scopeKey || activeTierLabel();
  row.appendChild(scopeEl);

  return row;
}

// ---------------------------------------------------------------- wiring

function bindFilters() {
  const refetch = () => loadPresets(true);

  $("#filter-btn").addEventListener("click", refetch);
  $("#filter-q").addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); refetch(); }
  });
  $("#filter-name").addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); refetch(); }
  });
  $("#filter-tier").addEventListener("change", () => {
    state.tier = $("#filter-tier").value;
    refetch();
  });
  $("#filter-keywords").addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); refetch(); }
  });
}

function mountScopePicker() {
  const scopeEl = document.getElementById("scope-picker");
  if (!scopeEl) return;
  mountContextBar(scopeEl, {
    mode: "select",
    onChange: async ({ catalogId, collectionId }) => {
      state.catalogId = catalogId;
      state.collectionId = collectionId;
      await loadPresets(true);
      if (state.selectedPreset) await refreshAppliedHistory();
    },
  });
}

function bindPresetList() {
  $("#presets-list").addEventListener("click", async (e) => {
    const btn = e.target.closest(".result-pick");
    if (!btn) return;
    const row = btn.closest(".result-row");
    if (!row) return;
    const name = row.dataset.name;
    if (name) await selectPreset(name);
  });

  // Keyboard: pressing Enter on a result-row selects it.
  $("#presets-list").addEventListener("keydown", async (e) => {
    if (e.key !== "Enter") return;
    const row = e.target.closest(".result-row");
    if (!row) return;
    const name = row.dataset.name;
    if (name) await selectPreset(name);
  });
}

function bindPagination() {
  $("#presets-next-btn").addEventListener("click", () => {
    const nextBtn = $("#presets-next-btn");
    const next = nextBtn.dataset.next;
    if (!next) return;
    if (state.cursor) state.prevCursors.push(state.cursor);
    state.cursor = next;
    loadPresets(false);
  });

  $("#presets-prev-btn").addEventListener("click", () => {
    state.cursor = state.prevCursors.pop() || null;
    loadPresets(false);
  });
}

function bindDetailActions() {
  $("#btn-dry-run").addEventListener("click", dryRun);
  $("#btn-apply").addEventListener("click", () => applyPreset(false));
  $("#btn-rollback").addEventListener("click", rollbackPreset);
}

function bindLockoutDialog() {
  const dlg = $("#lockout-dialog");
  const checkbox = $("#lockout-force-checkbox");
  const confirmBtn = $("#lockout-confirm-btn");
  const cancelBtn = $("#lockout-cancel-btn");

  checkbox.addEventListener("change", () => {
    confirmBtn.disabled = !checkbox.checked;
  });

  confirmBtn.addEventListener("click", () => {
    dlg.close();
    doRollback(true);
  });

  cancelBtn.addEventListener("click", () => {
    dlg.close();
    setStatus($("#detail-action-status"), "Rollback cancelled.");
  });
}

function syncFiltersFromState() {
  $("#filter-q").value = state.q;
  $("#filter-name").value = state.filterName;
  $("#filter-tier").value = state.tier;
  $("#filter-keywords").value = state.keywords;
}

function bindFilterInputSync() {
  $("#filter-q").addEventListener("input", (e) => { state.q = e.target.value; });
  $("#filter-name").addEventListener("input", (e) => { state.filterName = e.target.value; });
  $("#filter-keywords").addEventListener("input", (e) => { state.keywords = e.target.value; });
}

// ---------------------------------------------------------------- boot

(async function init() {
  bindFilters();
  mountScopePicker();
  bindPresetList();
  bindPagination();
  bindDetailActions();
  bindLockoutDialog();
  bindFilterInputSync();
  syncFiltersFromState();

  await loadPresets(true);
})();
