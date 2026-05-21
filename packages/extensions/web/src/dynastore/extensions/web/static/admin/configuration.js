// Configuration Hub — page glue.
// Wires the reusable common/ modules together. No plugin-specific code.
//
// Teaching-grade layer (#948): the right pane is no longer a single resolved
// blob. It shows a per-field tier stack with provenance badges plus
// Effective / Wire / Diff / Docs tabs, all driven by REAL composed-config
// endpoints (resolved / meta=field / view=delta) — no backend change.

import {
  fetchSchemas,
  fetchConfigSet,
  patchConfigSet,
  fetchMe,
} from "../common/api.js";
import { mountContextBar } from "../common/context-bar.js";
import { mountSchemaList } from "../common/schema-list.js";
import { mountSchemaForm } from "../common/schema-form.js";
import { mountConfigTabs } from "../common/config-tabs.js";
import {
  fetchTierStack,
  flattenComposed,
  defaultsFromSchema,
  lineageForClass,
  diffPatch,
  curlForPatch,
  curlForGet,
  tiersForScope,
} from "../common/config-insight.js";

const $ = (sel) => document.querySelector(sel);

const state = {
  schemas: {},            // class_key -> { json_schema, description, scope }
  scope: { kind: "platform" },
  selectedClassKey: null,
  resolvedSet: {},        // class_key -> resolved (inherited) config dict
  explicitSet: {},        // class_key -> explicit (active-scope delta) dict
  tierStack: { default: {}, platform: {}, catalog: {}, collection: {} },
  tiers: ["default", "platform"],
  form: null,
  tabs: null,
};

function setStatus(msg, cls = "") {
  const s = $("#status");
  s.textContent = msg || "";
  s.className = "status " + cls;
}

async function refreshConfigsForScope() {
  // Effective (waterfall) set with provenance, the active-scope delta, and
  // the per-tier stack — three reads composed client-side. meta=field is
  // what surfaces `_meta.source` provenance and `_meta.docs` per leaf.
  const [resolved, explicit, tierData] = await Promise.all([
    fetchConfigSet(state.scope, { resolved: true, meta: "field" }),
    fetchConfigSet(state.scope, { resolved: false, meta: "field", view: "delta" }),
    fetchTierStack(state.scope, state.schemas),
  ]);
  state.resolvedSet = flattenComposed(resolved, state.schemas);
  state.explicitSet = flattenComposed(explicit, state.schemas);
  state.tierStack = tierData.stack;
  state.tiers = tierData.tiers;
}

// The merged value of everything BELOW the active scope (what the active
// scope inherits). Used by the Diff tab to show resolved before/after.
function inheritedForClass(classKey) {
  const scopeTier = state.scope.kind;
  const order = tiersForScope(state.scope);
  const merged = { ...defaultsFromSchema(state.schemas[classKey]?.json_schema) };
  for (const tier of order) {
    if (tier === "default" || tier === scopeTier) continue;
    const delta = (state.tierStack[tier] || {})[classKey];
    if (delta) {
      for (const [k, v] of Object.entries(delta)) {
        if (k === "_meta" || k === "_links") continue;
        merged[k] = v;
      }
    }
  }
  return merged;
}

function updateTabs() {
  if (!state.tabs) return;
  const classKey = state.selectedClassKey;
  if (!classKey) {
    state.tabs.update({ classKey: null, tiers: state.tiers });
    return;
  }
  const schema = state.schemas[classKey]?.json_schema || {};
  const defaults = defaultsFromSchema(schema);
  const lineage = lineageForClass(
    classKey, defaults, state.tierStack, state.tiers,
  );
  const resolved = state.resolvedSet[classKey] || {};
  const explicit = state.explicitSet[classKey] || {};

  const patch = state.form ? state.form.getPatch() : {};
  const body = Object.keys(patch).length ? { [classKey]: patch } : {};
  const inherited = inheritedForClass(classKey);
  const diff = diffPatch({ patch, explicitBefore: explicit, inherited });

  const knobs = state.tabs.getKnobs();
  state.tabs.update({
    classKey,
    lineage,
    tiers: state.tiers,
    resolved,
    explicit,
    inherited,
    patch,
    diff,
    curlPatch: Object.keys(body).length
      ? curlForPatch(state.scope, body)
      : "# Edit a field to populate the PATCH body.",
    curlGet: curlForGet(state.scope, knobs),
    docs: docsForClass(classKey),
    registryUrl: registryUrlFor(classKey),
  });
}

function docsForClass(classKey) {
  // Provenance meta=field carries _meta.docs on each leaf. Fall back to the
  // schema's per-field descriptions when absent.
  const leaf = state.resolvedSet[classKey] || {};
  const metaDocs = leaf._meta && leaf._meta.docs;
  if (metaDocs && Object.keys(metaDocs).length) return metaDocs;
  const props = state.schemas[classKey]?.json_schema?.properties || {};
  const out = {};
  for (const [field, spec] of Object.entries(props)) {
    if (spec && spec.description) out[field] = spec.description;
  }
  return out;
}

function registryUrlFor(classKey) {
  try {
    return new URL(
      `../../configs/registry/${encodeURIComponent(classKey)}`,
      window.location.href,
    ).href;
  } catch {
    return `/configs/registry/${encodeURIComponent(classKey)}`;
  }
}

function renderForm() {
  const host = $("#schema-form");
  const hdr = $("#form-header");
  $("#apply").disabled = true;
  $("#revert").disabled = true;
  setStatus("");

  if (!state.selectedClassKey) {
    host.replaceChildren();
    hdr.textContent = "Editor";
    const p = document.createElement("div");
    p.className = "empty-hint";
    p.textContent = "Select a plugin from the list to edit its configuration.";
    host.appendChild(p);
    updateTabs();
    return;
  }

  const entry = state.schemas[state.selectedClassKey];
  if (!entry) return;

  hdr.textContent = `${state.selectedClassKey} — ${state.scope.kind}`
    + (state.scope.catalogId ? ` / ${state.scope.catalogId}` : "")
    + (state.scope.collectionId ? ` / ${state.scope.collectionId}` : "");

  const resolved = state.resolvedSet[state.selectedClassKey] || {};
  const explicit = state.explicitSet[state.selectedClassKey] || {};

  state.form = mountSchemaForm(host, {
    schema: entry.json_schema,
    resolved,
    explicit,
    allowInherit: state.scope.kind !== "platform",
    onDirty: (dirty) => {
      $("#apply").disabled = !dirty;
      $("#revert").disabled = !dirty;
      // Live-refresh Wire + Diff as the operator edits.
      updateTabs();
    },
  });

  decorateFormProvenance();
  updateTabs();
}

// Inject a "from: <tier>" badge next to each top-level field label, derived
// from the per-field lineage. Progressive enhancement — the form still works
// without it.
function decorateFormProvenance() {
  const classKey = state.selectedClassKey;
  if (!classKey || !state.form) return;
  const schema = state.schemas[classKey]?.json_schema || {};
  const lineage = lineageForClass(
    classKey, defaultsFromSchema(schema), state.tierStack, state.tiers,
  );
  const host = $("#schema-form");
  const rows = host.querySelectorAll(".field-row");
  rows.forEach((row) => {
    const label = row.querySelector("label");
    if (!label) return;
    // Field name is the label text minus a trailing " *" required marker and
    // any appended inherit-pill text.
    const raw = (label.childNodes[0] && label.childNodes[0].textContent) || "";
    const name = raw.replace(/\s*\*$/, "").trim();
    const f = lineage.fields[name];
    if (!f) return;
    if (label.querySelector(".ci-badge")) return;
    const badge = document.createElement("span");
    badge.className = `ci-badge ci-from-${f.source}`;
    badge.textContent = `from: ${f.source}`;
    badge.title = `Effective value supplied by the ${f.source} tier`;
    label.appendChild(badge);
  });
}

async function onSave() {
  if (!state.form || !state.selectedClassKey) return;
  const patch = state.form.getPatch();
  if (!Object.keys(patch).length) {
    setStatus("No changes.", "");
    return;
  }
  const body = { [state.selectedClassKey]: patch };
  setStatus("Saving…", "");
  try {
    await patchConfigSet(state.scope, body);
    await refreshConfigsForScope();
    renderForm();
    setStatus("Saved.", "ok");
  } catch (e) {
    setStatus(`Save failed: ${e.message || e}`, "err");
  }
}

function onRevert() {
  if (state.form) state.form.reset();
  renderForm();
}

async function boot() {
  // Gate the page: require sysadmin.
  const me = await fetchMe();
  const isSysadmin = (me.roles || []).includes("sysadmin");
  if (!isSysadmin) {
    const main = document.querySelector("main");
    main.replaceChildren();
    const p = document.createElement("p");
    p.className = "empty-hint";
    p.textContent = "You need the 'sysadmin' role to access the Configuration Hub.";
    main.appendChild(p);
    return;
  }

  setStatus("Loading schemas…", "");
  try {
    state.schemas = await fetchSchemas();
  } catch (e) {
    setStatus(`Could not load schemas: ${e.message}`, "err");
    return;
  }

  state.tabs = mountConfigTabs($("#config-tabs"), {
    onKnobs: () => updateTabs(),  // re-render the GET curl when knobs change
  });

  mountSchemaList($("#schema-list"), {
    schemas: state.schemas,
    onSelect: async (classKey) => {
      state.selectedClassKey = classKey;
      renderForm();
    },
  });

  mountContextBar($("#context-bar"), {
    onChange: async (scope) => {
      state.scope = scope;
      try {
        setStatus("Loading configs…", "");
        await refreshConfigsForScope();
        setStatus("", "");
      } catch (e) {
        setStatus(`Load failed: ${e.message}`, "err");
      }
      renderForm();
    },
  });

  $("#apply").addEventListener("click", onSave);
  $("#revert").addEventListener("click", onRevert);
}

boot();
