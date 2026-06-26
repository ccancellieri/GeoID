// Scope picker for admin pages. Platform / Catalog / Collection.
//
// Two modes:
//   "bar" (default) — renders a sticky context bar with Platform/Catalog/Collection
//     radio buttons, sessionStorage persistence, and emits scope objects of the form
//     {kind, catalogId?, collectionId?}. Used by Configuration and Governance pages.
//
//   "select" — embeddable, renders only the dropdown selects (no radios, no sticky bar
//     chrome) into a caller-provided container. Used by Dashboard, STAC browser, map
//     viewer, and the bespoke admin <select> pairs. Emits change callbacks with
//     {catalogId, collectionId} (both may be null).
//
// mountContextBar(container, options) — the single public export.

import { fetchCatalogOptions, getJSON } from "./api.js";
import { apiUrl } from "./url.js";

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

function buildSelect({ id, placeholder, disabled, cls }) {
  const s = document.createElement("select");
  if (id) s.id = id;
  s.disabled = !!disabled;
  s.className = cls || "scope-select";
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

// Fetch and cache collections for a catalog.
// Returns a normalized [{id, title}] list; returns [] on error.
async function fetchCollectionsFor(catalogId, collectionsByCatalog, includeVirtual, basePath = "/stac") {
  if (!catalogId) return [];
  if (collectionsByCatalog[catalogId]) return collectionsByCatalog[catalogId];
  try {
    let url = apiUrl(`${basePath}/catalogs/${encodeURIComponent(catalogId)}/collections`);
    if (includeVirtual) url += "?include_virtual=true";
    const res = await getJSON(url);
    const items = Array.isArray(res) ? res : (res.collections || res.items || []);
    const norm = items.map((c) => ({ id: c.id || c.collection_id || c, title: c.title || c.id || c }));
    collectionsByCatalog[catalogId] = norm;
    return norm;
  } catch {
    return [];
  }
}

// ---- BAR MODE (default) ------------------------------------------------
// Preserves 100% of the original mountContextBar behaviour: Platform /
// Catalog / Collection radio buttons, sessionStorage persistence, and scope
// objects of the form {kind, catalogId?, collectionId?}.

function mountBarMode(container, { onChange, basePath = "/stac" } = {}) {
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

  // Shown when the catalog list resolves empty (e.g. a platform admin with a
  // wildcard "*" grant but without sysadmin, who cannot enumerate cross-tenant
  // catalogs), so the empty Catalog/Collection selects are explained rather
  // than reading as a broken picker (#1917).
  const emptyNote = document.createElement("p");
  emptyNote.className = "context-empty-note";
  emptyNote.hidden = true;
  emptyNote.style.cssText = "margin:0.35rem 0 0;font-size:0.8rem;color:#9ca3af;";
  emptyNote.textContent = "No catalogs available for your account.";
  container.appendChild(emptyNote);

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
    await fetchCollectionsFor(catId, collectionsByCatalog, false, basePath);
    syncUI();
  });

  catalogSelect.addEventListener("change", async (e) => {
    const v = e.target.value;
    if (scope.kind === "catalog") {
      scope = { kind: "catalog", catalogId: v };
    } else if (scope.kind === "collection") {
      scope = { kind: "collection", catalogId: v, collectionId: "" };
      await fetchCollectionsFor(v, collectionsByCatalog, false, basePath);
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
      catalogs = await fetchCatalogOptions();
    } catch {
      catalogs = [];
    }
    emptyNote.hidden = catalogs.length > 0;
    if (scope.kind !== "platform" && !scope.catalogId && catalogs.length) {
      scope.catalogId = catalogs[0].id;
    }
    if (scope.kind === "collection") await fetchCollectionsFor(scope.catalogId, collectionsByCatalog, false, basePath);
    syncUI();
    // Avoid emitting an incomplete restored scope on boot (e.g. a persisted
    // "collection" scope with no collection chosen yet), which makes
    // subscribers fire a wasted/incorrect load before the user interacts.
    if (scope.kind !== "collection" || scope.collectionId) emit();
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

// ---- SELECT MODE -------------------------------------------------------
// Embeddable, no sticky-bar chrome. Renders catalog + optional collection
// selects into the container inside a .context-selector-wrapper div.
// Calls onChange({catalogId, collectionId}) whenever either select changes.
// Does NOT write to sessionStorage (no cross-page persistence by design).

function mountSelectMode(container, {
  onChange,
  catalogOnly = false,
  initialCatalog = null,
  initialCollection = null,
  autoSelectFirst = false,
  preferredCollection = null,
  enableVirtualCollections = false,
  enableSearch = false,
  basePath = "/stac",
} = {}) {
  clearNode(container);

  const wrapper = document.createElement("div");
  wrapper.className = "context-selector-wrapper";

  let searchInput = null;
  let allCatalogItems = [];

  if (enableSearch) {
    searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.className = "filter-input";
    searchInput.placeholder = "Filter catalogs…";
    wrapper.appendChild(searchInput);
  }

  const catalogSelect = buildSelect({ placeholder: "-- catalog --", cls: "filter-select" });
  wrapper.appendChild(catalogSelect);

  let collectionSelect = null;
  if (!catalogOnly) {
    collectionSelect = buildSelect({ placeholder: "-- collection --", disabled: true, cls: "filter-select" });
    wrapper.appendChild(collectionSelect);
  }

  // Shown when the catalog list resolves empty, so the deny-by-default
  // behaviour is legible instead of reading as "the picker is broken".
  // A platform admin with a wildcard "*" grant but without sysadmin cannot
  // enumerate cross-tenant catalogs, so the dropdown would otherwise render
  // with no options and no explanation (#1917).
  const emptyNote = document.createElement("p");
  emptyNote.className = "context-empty-note";
  emptyNote.hidden = true;
  emptyNote.style.cssText = "margin:0.35rem 0 0;font-size:0.8rem;color:#9ca3af;";
  emptyNote.textContent = "No catalogs available for your account.";
  wrapper.appendChild(emptyNote);

  container.appendChild(wrapper);

  let currentCatalog = initialCatalog;
  let currentCollection = initialCollection;
  const collectionsByCatalog = {};

  function emitChange() {
    if (typeof onChange === "function") {
      onChange({ catalogId: currentCatalog || null, collectionId: currentCollection || null });
    }
  }

  function applySearchFilter(term) {
    const lower = (term || "").toLowerCase();
    const visible = lower
      ? allCatalogItems.filter((c) => (c.title || c.id).toLowerCase().includes(lower))
      : allCatalogItems;
    while (catalogSelect.firstChild) catalogSelect.removeChild(catalogSelect.firstChild);
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "-- catalog --";
    catalogSelect.appendChild(placeholder);
    for (const c of visible) {
      const o = document.createElement("option");
      o.value = c.id;
      o.textContent = c.title || c.id;
      if (c.id === currentCatalog) o.selected = true;
      catalogSelect.appendChild(o);
    }
    if (currentCatalog && catalogSelect.value !== currentCatalog) {
      currentCatalog = null;
    }
  }

  async function populateCatalogSelect(catalogs) {
    allCatalogItems = catalogs;
    emptyNote.hidden = (catalogs || []).length > 0;
    applySearchFilter(searchInput ? searchInput.value : "");
  }

  async function populateCollectionSelect(collections) {
    if (!collectionSelect) return;
    while (collectionSelect.firstChild) collectionSelect.removeChild(collectionSelect.firstChild);
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "-- collection --";
    collectionSelect.appendChild(placeholder);
    for (const c of collections) {
      const o = document.createElement("option");
      o.value = c.id;
      o.textContent = c.title || c.id;
      if (c.id === currentCollection) o.selected = true;
      collectionSelect.appendChild(o);
    }
    collectionSelect.disabled = false;
  }

  async function onCatalogChanged(catId) {
    currentCatalog = catId || null;
    currentCollection = null;
    if (collectionSelect) {
      clearNode(collectionSelect);
      const ph = document.createElement("option");
      ph.value = "";
      ph.textContent = "-- collection --";
      collectionSelect.appendChild(ph);
      collectionSelect.disabled = true;
    }
    if (!catId) { emitChange(); return; }

    emitChange();

    if (collectionSelect) {
      const cols = await fetchCollectionsFor(catId, collectionsByCatalog, enableVirtualCollections, basePath);
      await populateCollectionSelect(cols);

      if (autoSelectFirst && cols.length) {
        const preferred = preferredCollection
          ? cols.find((c) => c.id === preferredCollection)
          : null;
        const target = preferred || cols[0];
        currentCollection = target.id;
        collectionSelect.value = currentCollection;
        emitChange();
      }
    }
  }

  catalogSelect.addEventListener("change", (e) => {
    onCatalogChanged(e.target.value || null);
  });

  if (searchInput) {
    searchInput.addEventListener("input", (e) => {
      applySearchFilter(e.target.value);
    });
  }

  if (collectionSelect) {
    collectionSelect.addEventListener("change", (e) => {
      currentCollection = e.target.value || null;
      emitChange();
    });
  }

  // Boot: load catalogs and apply initialCatalog / autoSelectFirst
  (async () => {
    let catalogs = [];
    try {
      catalogs = await fetchCatalogOptions();
    } catch {
      catalogs = [];
    }

    await populateCatalogSelect(catalogs);

    if (!currentCatalog && autoSelectFirst && catalogs.length) {
      currentCatalog = catalogs[0].id;
      catalogSelect.value = currentCatalog;
    }

    if (currentCatalog) {
      catalogSelect.value = currentCatalog;
      if (collectionSelect) {
        const cols = await fetchCollectionsFor(currentCatalog, collectionsByCatalog, enableVirtualCollections, basePath);
        await populateCollectionSelect(cols);

        if (!currentCollection && autoSelectFirst && cols.length) {
          const preferred = preferredCollection
            ? cols.find((c) => c.id === preferredCollection)
            : null;
          const target = preferred || cols[0];
          currentCollection = target.id;
          collectionSelect.value = currentCollection;
        } else if (currentCollection) {
          collectionSelect.value = currentCollection;
        }
      }
      emitChange();
    }
  })();

  return {
    getCatalogId: () => currentCatalog,
    getCollectionId: () => currentCollection,
    setCatalogId: (catId) => {
      currentCatalog = catId || null;
      catalogSelect.value = currentCatalog || "";
      if (collectionSelect) {
        currentCollection = null;
        clearNode(collectionSelect);
        const ph = document.createElement("option");
        ph.value = "";
        ph.textContent = "-- collection --";
        collectionSelect.appendChild(ph);
        collectionSelect.disabled = !catId;
      }
      emitChange();
    },
    setCollectionId: (colId) => {
      currentCollection = colId || null;
      if (collectionSelect) collectionSelect.value = currentCollection || "";
      emitChange();
    },
    // Re-fetch the current catalog's collections (busting the cache) and, if
    // selectId is given, select it. Used after creating a collection so the
    // new one appears in the dropdown without re-mounting the picker.
    refreshCollections: async (selectId) => {
      if (!collectionSelect || !currentCatalog) return [];
      delete collectionsByCatalog[currentCatalog];
      const cols = await fetchCollectionsFor(currentCatalog, collectionsByCatalog, enableVirtualCollections, basePath);
      await populateCollectionSelect(cols);
      if (selectId) {
        currentCollection = selectId;
        collectionSelect.value = selectId;
        collectionSelect.disabled = false;
        emitChange();
      }
      return cols;
    },
  };
}

// ---- Public API --------------------------------------------------------

/**
 * Mount the context picker into `container`.
 *
 * @param {HTMLElement} container   - DOM node to render into.
 * @param {Object}      options
 * @param {string}      [options.mode="bar"]           - "bar" or "select".
 * @param {Function}    [options.onChange]             - Callback on scope change.
 *   Bar mode:    called with {kind, catalogId?, collectionId?}
 *   Select mode: called with {catalogId, collectionId} (both nullable)
 * @param {boolean}     [options.catalogOnly=false]    - Select mode: omit collection select.
 * @param {string}      [options.initialCatalog]       - Select mode: pre-select catalog.
 * @param {string}      [options.initialCollection]    - Select mode: pre-select collection.
 * @param {boolean}     [options.autoSelectFirst=false]- Select mode: auto-pick first option.
 * @param {string}      [options.preferredCollection]  - Select mode: preferred collection id.
 * @param {boolean}     [options.enableVirtualCollections=false] - Select mode: include virtual collections.
 * @param {boolean}     [options.enableSearch=false]            - Select mode: show a text filter above the
 *   catalog select that narrows options by case-insensitive substring. Current selection is cleared when
 *   filtered out. Does not affect bar mode.
 * @param {string}      [options.basePath="/stac"]     - API base path for fetching collections.
 *   Use "/features" for Features browser, "/volumes" for Volumes, etc. Defaults to "/stac" for backward compatibility.
 *
 * @returns {Object} Control handle.
 *   Bar mode:    { getScope(), setScope(scope) }
 *   Select mode: { getCatalogId(), getCollectionId(), setCatalogId(id), setCollectionId(id) }
 */
export function mountContextBar(container, options = {}) {
  const { mode = "bar", ...rest } = options;
  if (mode === "select") return mountSelectMode(container, rest);
  return mountBarMode(container, rest);
}
