# Shared OGC Collection-Browser Implementation Plan

> **For agentic workers:** Implement task-by-task; checkbox (`- [ ]`) steps track progress. Work on the feature
> branch for this change. The worktree has no own `.venv`; use `/Users/ccancellieri/work/code/geoid/.venv/bin/python`
> with `PYTHONPATH` over `packages/core/src` + `packages/extensions/*/src`, and strip editable meta-path finders at
> process start (`sys.meta_path[:] = [f for f in sys.meta_path if 'editable' not in repr(f).lower()]`) so the
> worktree source resolves.

**Goal:** Replace per-extension duplication of the OGC catalog→collection browse flow with one shared
`common/ogc-browser.js` shell + pluggable body adapters, then add browser pages for Records, MovingFeatures,
Coverages, and EDR, and retrofit STAC/Features.

**Architecture:** A shared shell owns the identical catalog→collection navigation and delegates the per-collection
body to an adapter (`items` / `coverage` / `edr`). Vanilla ES-module JS, no build step. Each protocol page is the
PR #1966 pattern (`@expose_web_page` + `@expose_static`).

**Tech Stack:** Python 3.12 / FastAPI; vanilla ES-module JS; Leaflet 1.9.4 (unpkg); pytest.

**Design doc:** `docs/design/2026-06-09-shared-ogc-browser-design.md`.

**Hard constraints:** no Configs/Admin edits; only *add* to `web/static/common/`; no AI-context/skill/agent/branch
references in tracked files or commits; no `Co-Authored-By`/AI attribution.

---

## Implementation status (as built)

Phases 1–5 and 7 are implemented; Phase 6 (STAC/Features retrofit) remains gated on the
per-extension web sections landing first. Two corrections to the plan were made during
implementation and are the load-bearing facts for anyone extending this:

1. **Static path resolution.** Pages are served at `/web/pages/{page_id}`. The web
   extension's own static tree is the `static` prefix (`/web/static/...`); each other
   extension's static is its own prefix (`/web/{prefix}/...`). Therefore:
   - A page's HTML references its own JS as `../{prefix}/{file}.js` (e.g.
     `../records/records_browser.js`) — **not** `../static/{prefix}/...`, which resolves
     to the web extension's static tree and 404s.
   - Extension JS imports the shared `common/` modules as `../static/common/...`.
   - A module inside `common/` imports its siblings as `./{file}.js`.
   - An adapter that ships in the same extension prefix is imported as `./{adapter}.js`.
   The earlier draft used `../static/{prefix}/...` for page→JS; that form is incorrect and
   was corrected here.

2. **Catalog/collection navigation routes.** The shell lists catalogs via
   `${basePath}/catalogs` and collections via `${basePath}/catalogs/{id}/collections`.
   Records and EDR already exposed the collection listing but not the catalog listing;
   Coverages exposed neither. Each service that lacked them now registers a thin
   `list_catalogs` (and, for Coverages, `list_collections`) delegating to the catalogs
   service — consistent with how STAC, Features, and MovingFeatures already do it.

---

## File Structure

| File | Responsibility |
|---|---|
| `web/static/common/ogc-browser.js` | NEW shared shell: catalog→collection nav, breadcrumb, i18n, content slot, adapter dispatch |
| `web/static/common/ogc-items-adapter.js` | NEW shared `items` adapter (list+map+detail), config-driven |
| `records/static/records_browser.{html,js}` + `records_service.py` | NEW Records page (items adapter) |
| `moving_features/static/movingfeatures_browser.{html,js}` + `mf_service.py` | NEW MovingFeatures page (items adapter + trajectory detail) |
| `coverages/static/coverages_browser.{html,js}` + `coverage-adapter.js` + `coverages_service.py` | NEW Coverages page + coverage adapter |
| `edr/static/edr_browser.{html,js}` + `edr-adapter.js` + `edr_service.py` | NEW EDR page + query-builder adapter |
| `stac/static/stac_browser.js`, `features/.../features_browser.js` | RETROFIT onto shared shell (Phase 6, #1966-gated) |
| `tests/dynastore/extensions/web/test_ogc_browser_sections.py` | NEW registration + handler tests |

---

## Phase 1 — Shared shell + items adapter (new code, not gated)

### Task 1.1: `ogc-browser.js` shell

**Files:** Create `packages/extensions/web/src/dynastore/extensions/web/static/common/ogc-browser.js`

- [ ] **Step 1: Write the shell.** Public API + navigation; delegates body to the adapter.

```javascript
// common/ogc-browser.js — shared OGC catalog->collection browser shell.
// The per-collection body is rendered by a pluggable adapter; this shell only
// owns navigation, breadcrumb, language threading, and load/empty/error states.
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";
import { initMap } from "../static/common/leaflet-map.js";

register({
  en: { "ogc.catalogs": "Catalogs", "ogc.collections": "Collections", "ogc.back": "Back",
        "ogc.loading": "Loading…", "ogc.none": "Nothing to show", "ogc.error": "Failed to load" },
  fr: { "ogc.catalogs": "Catalogues", "ogc.collections": "Collections", "ogc.back": "Retour",
        "ogc.loading": "Chargement…", "ogc.none": "Rien à afficher", "ogc.error": "Échec du chargement" },
  es: { "ogc.catalogs": "Catálogos", "ogc.collections": "Colecciones", "ogc.back": "Atrás",
        "ogc.loading": "Cargando…", "ogc.none": "Nada que mostrar", "ogc.error": "Error al cargar" },
});

// mountOgcBrowser({ root, basePath, adapter, writeActions }) -> void
export function mountOgcBrowser({ root, basePath, adapter, writeActions }) {
  const navEl = root.querySelector("[data-ogc-nav]");
  const bodyEl = root.querySelector("[data-ogc-body]");
  const mapEl = root.querySelector("[data-ogc-map]");
  const map = adapter.needsMap && mapEl ? initMap(mapEl.id) : null;

  const state = { catalogId: null };

  function setLoading(el) { el.textContent = t("ogc.loading"); }

  async function showCatalogs() {
    state.catalogId = null;
    setLoading(navEl);
    try {
      const cats = await getJSON(`${basePath}/catalogs?language=${lang()}`);
      renderList(navEl, cats, (c) => c.id, (c) => c.title || c.id, (c) => selectCatalog(c.id));
    } catch (e) { navEl.textContent = t("ogc.error"); }
  }

  async function selectCatalog(catalogId) {
    state.catalogId = catalogId;
    setLoading(navEl);
    try {
      const res = await getJSON(`${basePath}/catalogs/${catalogId}/collections?language=${lang()}`);
      const colls = res.collections || res; // OGC envelope or bare list
      renderList(navEl, colls, (c) => c.id, (c) => c.title || c.id, (c) => selectCollection(c.id), true);
    } catch (e) { navEl.textContent = t("ogc.error"); }
  }

  async function selectCollection(collectionId) {
    bodyEl.replaceChildren();
    setLoading(bodyEl);
    try {
      await adapter.renderCollectionBody({
        catalogId: state.catalogId, collectionId, contentEl: bodyEl, map, lang: lang(),
      });
    } catch (e) { bodyEl.textContent = t("ogc.error"); }
  }

  // renderList: builds an accessible list; back row when `withBack`.
  function renderList(el, rows, idOf, labelOf, onClick, withBack) {
    el.replaceChildren();
    if (withBack) {
      const back = document.createElement("button");
      back.textContent = "← " + t("ogc.back");
      back.addEventListener("click", showCatalogs);
      el.appendChild(back);
    }
    if (!rows || rows.length === 0) { const p = document.createElement("p"); p.textContent = t("ogc.none"); el.appendChild(p); return; }
    const ul = document.createElement("ul");
    for (const r of rows) {
      const li = document.createElement("li");
      const a = document.createElement("button");
      a.textContent = labelOf(r);            // textContent => no XSS from API data
      a.addEventListener("click", () => onClick(r));
      li.appendChild(a); ul.appendChild(li);
    }
    el.appendChild(ul);
  }

  if (writeActions && typeof writeActions.mount === "function") writeActions.mount(root, state);
  showCatalogs();
}
```

- [ ] **Step 2:** `node --check` is a false-negative for ES modules (parsed as CJS) — skip; runtime verified by the handler tests. Commit:

```bash
git add packages/extensions/web/src/dynastore/extensions/web/static/common/ogc-browser.js
git commit -m "feat(web): shared OGC catalog/collection browser shell"
```

### Task 1.2: `ogc-items-adapter.js`

**Files:** Create `packages/extensions/web/src/dynastore/extensions/web/static/common/ogc-items-adapter.js`

- [ ] **Step 1: Write the adapter factory.**

```javascript
// common/ogc-items-adapter.js — default body adapter: list collection items in a table + map + detail.
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";
import { showGeoJSON } from "../static/common/leaflet-map.js";

register({
  en: { "items.title": "Items", "items.id": "ID", "items.empty": "No items", "items.view": "View" },
  fr: { "items.title": "Éléments", "items.id": "ID", "items.empty": "Aucun élément", "items.view": "Voir" },
  es: { "items.title": "Elementos", "items.id": "ID", "items.empty": "Sin elementos", "items.view": "Ver" },
});

// makeItemsAdapter({ itemsPath="items", idField="id", columns=[], detail }) -> adapter
export function makeItemsAdapter(cfg = {}) {
  const itemsPath = cfg.itemsPath || "items";
  const idField = cfg.idField || "id";
  const layerRef = { current: null };
  return {
    id: "items",
    needsMap: true,
    async renderCollectionBody({ catalogId, collectionId, contentEl, map }) {
      const base = `/__set_by_shell__`; // shell passes basePath via closure? -> see note
      const url = `${cfg.basePath}/catalogs/${catalogId}/collections/${collectionId}/${itemsPath}?limit=50&language=${lang()}`;
      const fc = await getJSON(url);
      const feats = fc.features || fc.items || fc;
      contentEl.replaceChildren();
      const h = document.createElement("h3"); h.textContent = t("items.title"); contentEl.appendChild(h);
      if (!feats || feats.length === 0) { const p = document.createElement("p"); p.textContent = t("items.empty"); contentEl.appendChild(p); return; }
      const table = document.createElement("table");
      for (const f of feats) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.textContent = f[idField] ?? f.id ?? "";           // textContent => XSS-safe
        tr.appendChild(td);
        const view = document.createElement("button"); view.textContent = t("items.view");
        view.addEventListener("click", () => {
          if (map) showGeoJSON(map, layerRef, f.geometry ? f : (f.bbox ? null : null));
          if (cfg.detail) cfg.detail({ catalogId, collectionId, itemId: f[idField] ?? f.id, contentEl, map });
        });
        const tdv = document.createElement("td"); tdv.appendChild(view); tr.appendChild(tdv);
        table.appendChild(tr);
      }
      contentEl.appendChild(table);
    },
  };
}
```

> **Implementation note for the agent:** the adapter needs `basePath`. Resolve this cleanly by having the shell pass
> `basePath` into the adapter at mount — either set `adapter.basePath = basePath` inside `mountOgcBrowser` before
> first use, or change the adapter to a factory the page calls as `makeItemsAdapter({ basePath, ... })`. Pick the
> factory form (page already knows `basePath`); drop the `__set_by_shell__` placeholder. Keep a single source of
> `basePath`.

- [ ] **Step 2: Commit.**

```bash
git add packages/extensions/web/src/dynastore/extensions/web/static/common/ogc-items-adapter.js
git commit -m "feat(web): shared items body-adapter for the OGC browser"
```

---

## Phase 2 — Records page (items adapter)

### Task 2.1: Records service web contribution

**Files:** Modify `packages/extensions/records/src/dynastore/extensions/records/records_service.py`

- [ ] **Step 1:** Add the five members exactly as STAC does (verify the class name first with
  `grep -n "^class .*Service" records_service.py`): `get_web_pages`, `get_static_assets`,
  `@expose_static("records") provide_static_files`, `@expose_web_page(page_id="records_browser",
  title={en:"Records",fr:"Enregistrements",es:"Registros"}, icon="fa-file-lines", description={…}) async
  provide_records_browser` → `_serve_page_template("records_browser.html")`, and `_serve_page_template`. Imports
  mirror STAC (`from dynastore.extensions.web.decorators import expose_web_page, expose_static`;
  `collect_web_pages`/`collect_static_assets` lazily). Verify with `ast.parse`.

- [ ] **Step 2: Commit** `feat(records): register Records web section page + static provider`.

### Task 2.2: Records page

**Files:** Create `records/static/records_browser.html` + `records_browser.js`

- [ ] **Step 1:** `records_browser.html` — shell with `[data-ogc-nav]`, `[data-ogc-body]`, `[data-ogc-map id="records-map"]`, Leaflet tags, `admin.css`, `{{VERSION}}`, `<script type="module" src="../static/records/records_browser.js">`.
- [ ] **Step 2:** `records_browser.js`:

```javascript
import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeItemsAdapter } from "../static/common/ogc-items-adapter.js";

const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/records", adapter: makeItemsAdapter({ basePath: "/records" }) });
```

- [ ] **Step 3: Commit** `feat(records): OGC-browser page for the Records section`.

---

## Phase 3 — MovingFeatures page (items adapter + trajectory detail)

### Task 3.1: Service web contribution

**Files:** Modify `packages/extensions/moving_features/src/dynastore/extensions/moving_features/mf_service.py`

- [ ] **Step 1:** Same five members; prefix `@expose_static("movingfeatures")`, `page_id="movingfeatures_browser"`,
  `title={en:"Moving Features",fr:"Entités mobiles",es:"Entidades móviles"}`, `icon="fa-route"`. Verify class name +
  `ast.parse`.
- [ ] **Step 2: Commit** `feat(moving_features): register MovingFeatures web section`.

### Task 3.2: Page with trajectory detail

**Files:** Create `moving_features/static/movingfeatures_browser.{html,js}`

- [ ] **Step 1:** HTML shell as in Task 2.2 Step 1 (map id `mf-map`), script `movingfeatures_browser.js`.
- [ ] **Step 2:** `movingfeatures_browser.js`:

```javascript
import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeItemsAdapter } from "../static/common/ogc-items-adapter.js";
import { getJSON } from "../static/common/api.js";
import { showGeoJSON } from "../static/common/leaflet-map.js";

const trajRef = { current: null };
async function detail({ catalogId, collectionId, itemId, map }) {
  // tgsequence carries the temporal geometry; render its path on the map.
  const tg = await getJSON(`/movingfeatures/catalogs/${catalogId}/collections/${collectionId}/items/${itemId}/tgsequence`);
  const coords = (tg.geometrySequence || tg.temporalGeometry || []).map((g) => g.coordinates).filter(Boolean);
  if (coords.length) showGeoJSON(map, trajRef, { type: "Feature", geometry: { type: "LineString", coordinates: coords }, properties: {} });
}
const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/movingfeatures", adapter: makeItemsAdapter({ basePath: "/movingfeatures", detail }) });
```

> Verify the `tgsequence` JSON shape against `mf_service.py` and adjust the coordinate extraction; the OGC MF
> temporal-geometry field name varies (`temporalGeometry` / `geometrySequence`).

- [ ] **Step 3: Commit** `feat(moving_features): OGC-browser page with trajectory detail`.

---

## Phase 4 — Coverages page + coverage adapter

### Task 4.1: Coverage adapter

**Files:** Create `coverages/static/coverage-adapter.js`

- [ ] **Step 1:** Build the adapter (read `coverages_service.py` route table first for exact subpaths/JSON):

```javascript
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";

register({
  en: { "cov.axes": "Axes", "cov.range": "Range", "cov.meta": "Metadata" },
  fr: { "cov.axes": "Axes", "cov.range": "Plage", "cov.meta": "Métadonnées" },
  es: { "cov.axes": "Ejes", "cov.range": "Rango", "cov.meta": "Metadatos" },
});

export function makeCoverageAdapter({ basePath }) {
  return {
    id: "coverage",
    needsMap: true,
    async renderCollectionBody({ catalogId, collectionId, contentEl }) {
      const root = `${basePath}/catalogs/${catalogId}/collections/${collectionId}/coverage`;
      const [domain, range] = await Promise.all([
        getJSON(`${root}/domainset?language=${lang()}`).catch(() => null),
        getJSON(`${root}/rangetype?language=${lang()}`).catch(() => null),
      ]);
      contentEl.replaceChildren();
      appendSection(contentEl, t("cov.axes"), summariseDomain(domain));
      appendSection(contentEl, t("cov.range"), summariseRange(range));
    },
  };
}
function appendSection(el, title, lines) {
  const h = document.createElement("h3"); h.textContent = title; el.appendChild(h);
  const ul = document.createElement("ul");
  for (const line of lines) { const li = document.createElement("li"); li.textContent = line; ul.appendChild(li); }
  el.appendChild(ul);
}
function summariseDomain(d) {
  if (!d) return ["—"];
  const axes = d.generalGrid?.axisLabels || d.axisLabels || [];
  return axes.length ? axes.map(String) : ["—"];
}
function summariseRange(r) {
  if (!r) return ["—"];
  const fields = r.field || r.fields || [];
  return fields.length ? fields.map((f) => f.name || f.definition || String(f)) : ["—"];
}
```

> Confirm `domainset`/`rangetype` JSON shapes against the live coverages responses and fix the summarisers.

- [ ] **Step 2: Commit** `feat(coverages): coverage body-adapter for the OGC browser`.

### Task 4.2: Service contribution + page

**Files:** Modify `coverages_service.py`; create `coverages/static/coverages_browser.{html,js}`

- [ ] **Step 1:** Service: five members, `@expose_static("coverages")`, `page_id="coverages_browser"`,
  `title={en:"Coverages",fr:"Couvertures",es:"Coberturas"}`, `icon="fa-layer-group"`. Verify + `ast.parse`.
- [ ] **Step 2:** Page HTML (map id `cov-map`) + `coverages_browser.js`:

```javascript
import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeCoverageAdapter } from "../static/coverages/coverage-adapter.js";
const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/coverages", adapter: makeCoverageAdapter({ basePath: "/coverages" }) });
```

- [ ] **Step 3: Commit** `feat(coverages): OGC-browser page with coverage description`.

---

## Phase 5 — EDR page + query-builder adapter

### Task 5.1: EDR adapter

**Files:** Create `edr/static/edr-adapter.js`

- [ ] **Step 1:** Build a minimal query builder driven by the collection's advertised `data_queries`/`parameter_names`
  (read `edr_service.py` + a sample collection response first):

```javascript
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";
import { showGeoJSON } from "../static/common/leaflet-map.js";

register({
  en: { "edr.position": "Position (lon,lat)", "edr.datetime": "Datetime", "edr.run": "Query", "edr.params": "Parameters" },
  fr: { "edr.position": "Position (lon,lat)", "edr.datetime": "Date/heure", "edr.run": "Interroger", "edr.params": "Paramètres" },
  es: { "edr.position": "Posición (lon,lat)", "edr.datetime": "Fecha/hora", "edr.run": "Consultar", "edr.params": "Parámetros" },
});

export function makeEdrAdapter({ basePath }) {
  const layerRef = { current: null };
  return {
    id: "edr",
    needsMap: true,
    async renderCollectionBody({ catalogId, collectionId, contentEl, map }) {
      contentEl.replaceChildren();
      const posLabel = document.createElement("label"); posLabel.textContent = t("edr.position");
      const pos = document.createElement("input"); pos.placeholder = "12.5,41.9";
      const dtLabel = document.createElement("label"); dtLabel.textContent = t("edr.datetime");
      const dt = document.createElement("input"); dt.placeholder = "2024-01-01T00:00:00Z";
      const run = document.createElement("button"); run.textContent = t("edr.run");
      const out = document.createElement("pre");
      run.addEventListener("click", async () => {
        const [lon, lat] = pos.value.split(",").map((s) => s.trim());
        const q = new URLSearchParams({ coords: `POINT(${lon} ${lat})`, f: "GeoJSON", language: lang() });
        if (dt.value) q.set("datetime", dt.value);
        const url = `${basePath}/catalogs/${catalogId}/collections/${collectionId}/position?${q}`;
        try {
          const res = await getJSON(url);
          if (map && res && (res.type === "Feature" || res.type === "FeatureCollection")) showGeoJSON(map, layerRef, res);
          out.textContent = JSON.stringify(res, null, 2).slice(0, 4000);
        } catch (e) { out.textContent = String(e); }
      });
      for (const el of [posLabel, pos, dtLabel, dt, run, out]) contentEl.appendChild(el);
    },
  };
}
```

> Confirm the EDR `position` query parameter names (`coords`/`coord`, `f`, `datetime`, `parameter-name`) against
> `edr_service.py`; adjust the `URLSearchParams` keys to match.

- [ ] **Step 2: Commit** `feat(edr): query-builder body-adapter for the OGC browser`.

### Task 5.2: Service contribution + page

**Files:** Modify `edr_service.py`; create `edr/static/edr_browser.{html,js}`

- [ ] **Step 1:** Service: five members, `@expose_static("edr")`, `page_id="edr_browser"`,
  `title={en:"EDR",fr:"EDR",es:"EDR"}`, `icon="fa-magnifying-glass-location"`. Verify + `ast.parse`.
- [ ] **Step 2:** Page HTML (map id `edr-map`) + `edr_browser.js`:

```javascript
import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeEdrAdapter } from "../static/edr/edr-adapter.js";
const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({ root, basePath: "/edr", adapter: makeEdrAdapter({ basePath: "/edr" }) });
```

- [ ] **Step 3: Commit** `feat(edr): OGC-browser page with position query builder`.

---

## Phase 6 — Retrofit STAC + Features (GATED on PR #1966 merging)

> Do NOT start until PR #1966 is merged to main and this branch is rebased onto it. Until then, STAC/Features keep
> their bespoke `*_browser.js`.

### Task 6.1: STAC onto shared shell

**Files:** Modify `stac/static/stac_browser.js`

- [ ] **Step 1:** Replace the bespoke browse logic with `mountOgcBrowser({ root, basePath: "/stac",
  adapter: makeItemsAdapter({ basePath: "/stac" }), writeActions: stacWriteActions })`, where `stacWriteActions.mount`
  wires the existing create-catalog/create-collection forms (kept verbatim from PR #1966). Delete the now-dead
  navigation/list code.
- [ ] **Step 2:** Run the PR #1966 STAC registration + handler tests — they must still pass.
- [ ] **Step 3: Commit** `refactor(stac): browse via the shared OGC browser shell`.

### Task 6.2: Features onto shared shell

**Files:** Modify `features/static/features_browser.js`

- [ ] **Step 1:** Same retrofit with `basePath: "/features"`, items adapter, and the existing create-feature form as
  `writeActions`. Delete dead code.
- [ ] **Step 2:** PR #1966 Features tests must still pass.
- [ ] **Step 3: Commit** `refactor(features): browse via the shared OGC browser shell`.

---

## Phase 7 — Tests, lint, sweep

### Task 7.1: Registration + handler tests

**Files:** Create `tests/dynastore/extensions/web/test_ogc_browser_sections.py`

- [ ] **Step 1:** Parametrize over the new sections — mirror PR #1966's
  `test_web_sections_registration.py` (instantiate via `cls.__new__`, assert page id + static prefix register, assert
  handler returns 200 HTML with `{{VERSION}}` substituted):

```python
SECTIONS = [
    ("dynastore.extensions.records.records_service", "RecordsService", "records_browser", "records", "provide_records_browser"),
    ("dynastore.extensions.moving_features.mf_service", "<MFClass>", "movingfeatures_browser", "movingfeatures", "provide_movingfeatures_browser"),
    ("dynastore.extensions.coverages.coverages_service", "CoveragesService", "coverages_browser", "coverages", "provide_coverages_browser"),
    ("dynastore.extensions.edr.edr_service", "<EdrClass>", "edr_browser", "edr", "provide_edr_browser"),
]
```

> Fill the real class names from `grep -n "^class .*Service" <each>_service.py`. Reuse the helper bodies verbatim from
> `test_web_sections_registration.py`.

- [ ] **Step 2: Run** (finders stripped + worktree PYTHONPATH):

```bash
PYTHONPATH="$PP" /Users/ccancellieri/work/code/geoid/.venv/bin/python -c "import sys; sys.meta_path[:]=[f for f in sys.meta_path if 'editable' not in repr(f).lower()]; import pytest; sys.exit(pytest.main(['tests/dynastore/extensions/web/test_ogc_browser_sections.py','-p','no:randomly','-n0','-q']))"
```
Expected: all passed.

- [ ] **Step 3: Commit** `test(web): registration + handler tests for OGC-browser sections`.

### Task 7.2: Lint + sweep

- [ ] **Step 1:** `uv tool run ruff check` the four modified `*_service.py` (zsh array for the file list). Added
  members must introduce zero new findings.
- [ ] **Step 2:** Three-repo sweep for the new page ids / static prefixes — expect clean (additive).
- [ ] **Step 3:** Open the PR (base main), noting Phase 6 is gated on #1966.

---

## Self-Review (author checklist — completed)

- **Spec coverage:** shared shell (P1) ✓; items adapter + Records/MovingFeatures (P1–P3) ✓; coverage adapter +
  Coverages (P4) ✓; edr adapter + EDR (P5) ✓; STAC/Features retrofit (P6, gated) ✓; tests (P7) ✓.
- **Placeholder scan:** the one placeholder (`__set_by_shell__` basePath) is called out with an explicit resolution
  (factory form) in Task 1.2's note — not left dangling. Class-name and JSON-shape verifications are flagged inline
  per task.
- **Type/name consistency:** `mountOgcBrowser({root,basePath,adapter,writeActions})`, `makeItemsAdapter({basePath,
  detail})`, `makeCoverageAdapter`, `makeEdrAdapter`, adapter `renderCollectionBody({catalogId,collectionId,
  contentEl,map,lang})` — referenced identically across phases.
- **Dependency gate:** Phase 6 explicitly blocked on PR #1966; Phases 1–5 + 7 are independent of it.
