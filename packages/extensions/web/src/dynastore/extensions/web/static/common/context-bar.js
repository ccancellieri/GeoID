// Scope picker for admin pages. Platform / Catalog / Collection.
// Renders into a container; emits onChange(scope) whenever the selection
// changes. Keeps state internal, but persists to sessionStorage so
// navigation between admin pages preserves scope.

import { fetchCatalogs, getJSON } from "./api.js";

const STORAGE_KEY = "dynastore.admin.scope";

function loadPersistedScope() {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return { kind: "platform" };
    const parsed = JSON.parse(raw);
    if (!parsed || !parsed.kind) return { kind: "platform" };
    return parsed;
  } catch {
    return { kind: "platform" };
  }
}

function persistScope(scope) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(scope));
  } catch {
    /* ignore quota errors */
  }
}

function buildSelect({ id, placeholder, disabled }) {
  const s = document.createElement("select");
  s.id = id;
  s.disabled = !!disabled;
  s.className = "scope-select";
  const o0 = document.createElement("option");
  o0.value = "";
  o0.textContent = placeholder;
  s.appendChild(o0);
  return s;
}

function populateSelect(select, items, keep) {
  while (select.firstChild) select.removeChild(select.firstChild);
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "-- select --";
  select.appendChild(placeholder);
  for (const it of items || []) {
    const o = document.createElement("option");
    o.value = it.value;
    o.textContent = it.label;
    if (it.value === keep) o.selected = true;
    select.appendChild(o);
  }
}

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

export function mountContextBar(container, { onChange } = {}) {
  container.classList.add("context-bar");
  clearNode(container);

  const fieldset = document.createElement("fieldset");
  fieldset.className = "scope-kind";

  const mkRadio = (value, label) => {
    const wrap = document.createElement("label");
    wrap.className = "scope-radio";
    const input = document.createElement("input");
    input.type = "radio";
    input.name = "scope-kind";
    input.value = value;
    wrap.appendChild(input);
    wrap.appendChild(document.createTextNode(label));
    return { wrap, input };
  };

  const rPlatform = mkRadio("platform", "Platform");
  const rCatalog = mkRadio("catalog", "Catalog");
  const rCollection = mkRadio("collection", "Collection");
  fieldset.appendChild(rPlatform.wrap);
  fieldset.appendChild(rCatalog.wrap);
  fieldset.appendChild(rCollection.wrap);

  const catalogSelect = buildSelect({ id: "ctx-catalog", placeholder: "-- catalog --", disabled: true });
  const collectionSelect = buildSelect({ id: "ctx-collection", placeholder: "-- collection --", disabled: true });

  fieldset.appendChild(catalogSelect);
  fieldset.appendChild(collectionSelect);
  container.appendChild(fieldset);

  let scope = loadPersistedScope();
  let catalogs = [];
  const collectionsByCatalog = {};

  function emit() {
    persistScope(scope);
    if (typeof onChange === "function") onChange({ ...scope });
  }

  function syncUI() {
    rPlatform.input.checked = scope.kind === "platform";
    rCatalog.input.checked = scope.kind === "catalog";
    rCollection.input.checked = scope.kind === "collection";
    catalogSelect.disabled = scope.kind === "platform";
    collectionSelect.disabled = scope.kind !== "collection";
    const catalogValue = scope.kind === "platform" ? "" : (scope.catalogId || "");
    populateSelect(
      catalogSelect,
      catalogs.map((c) => ({ value: c.id, label: c.id })),
      catalogValue,
    );
    const colItems = scope.kind === "collection"
      ? (collectionsByCatalog[scope.catalogId] || [])
      : [];
    populateSelect(
      collectionSelect,
      colItems.map((c) => ({ value: c.id, label: c.id })),
      scope.kind === "collection" ? (scope.collectionId || "") : "",
    );
  }

  async function loadCollectionsFor(catalogId) {
    if (!catalogId) return [];
    if (collectionsByCatalog[catalogId]) return collectionsByCatalog[catalogId];
    try {
      const res = await getJSON(`/stac/catalogs/${encodeURIComponent(catalogId)}/collections`);
      const items = Array.isArray(res) ? res : (res.items || res.collections || []);
      const norm = items.map((c) => ({ id: c.id || c.collection_id || c }));
      collectionsByCatalog[catalogId] = norm;
      return norm;
    } catch {
      return [];
    }
  }

  rPlatform.input.addEventListener("change", () => {
    if (!rPlatform.input.checked) return;
    scope = { kind: "platform" };
    syncUI();
    emit();
  });

  rCatalog.input.addEventListener("change", () => {
    if (!rCatalog.input.checked) return;
    const keep = scope.kind !== "platform" ? scope.catalogId : (catalogs[0]?.id || "");
    scope = { kind: "catalog", catalogId: keep };
    syncUI();
    if (keep) emit();
  });

  rCollection.input.addEventListener("change", async () => {
    if (!rCollection.input.checked) return;
    const catId = scope.kind !== "platform" ? scope.catalogId : (catalogs[0]?.id || "");
    scope = { kind: "collection", catalogId: catId, collectionId: "" };
    await loadCollectionsFor(catId);
    syncUI();
  });

  catalogSelect.addEventListener("change", async (e) => {
    const v = e.target.value;
    if (scope.kind === "catalog") {
      scope = { kind: "catalog", catalogId: v };
    } else if (scope.kind === "collection") {
      scope = { kind: "collection", catalogId: v, collectionId: "" };
      await loadCollectionsFor(v);
    }
    syncUI();
    if (v) emit();
  });

  collectionSelect.addEventListener("change", (e) => {
    if (scope.kind !== "collection") return;
    scope = { ...scope, collectionId: e.target.value };
    if (scope.collectionId) emit();
  });

  (async () => {
    try {
      const res = await fetchCatalogs();
      const items = Array.isArray(res) ? res : (res.items || []);
      catalogs = items.map((c) => ({ id: c.id || c.catalog_id || c }));
    } catch {
      catalogs = [];
    }
    if (scope.kind !== "platform" && !scope.catalogId && catalogs.length) {
      scope.catalogId = catalogs[0].id;
    }
    if (scope.kind === "collection") await loadCollectionsFor(scope.catalogId);
    syncUI();
    emit();
  })();

  return {
    getScope: () => ({ ...scope }),
    setScope: (next) => {
      scope = next || { kind: "platform" };
      syncUI();
      emit();
    },
  };
}
