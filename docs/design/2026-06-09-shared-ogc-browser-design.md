# Shared OGC collection-browser for web sections

**Date:** 2026-06-09
**Status:** Design approved — ready for implementation plan
**Depends on:** the per-extension web sections (STAC/Features/Assets, PR #1966) merging first — STAC/Features are retrofitted onto this shared component afterward.
**Scope:** a reusable web collection-browser shared by OGC protocol extensions, and new browser pages for Records, MovingFeatures, Coverages, EDR. **Out of scope:** Configs/Admin UI.

## Problem

The per-extension web sections (PR #1966) gave STAC and Features each their own `*_browser.js` that independently implements the same flow: navigate catalogs → collections → items, on a map, in the chosen language. The remaining OGC protocol extensions (Records, MovingFeatures, Coverages, EDR — all built on `OGCServiceMixin`) have no web UI and would each duplicate that same navigation again.

The navigation is identical across every OGC protocol; only the *collection body* differs. Duplicating it per extension is the wrong trajectory. We want one shared component that owns the identical part and cleanly delegates the divergent part.

## Key finding (verified in tree)

Every OGC protocol mounts at a base prefix and exposes the **same** catalog/collection routes; only the per-collection content diverges:

| Protocol | Base | Per-collection routes | Body kind |
|---|---|---|---|
| STAC | `/stac` | `…/collections/{c}/items`, `/search` | items + map |
| Features | `/features` | `…/collections/{c}/items` | items + map |
| Records | `/records` | `…/collections/{c}/items`, `/items/{id}` | items + map |
| MovingFeatures | `/movingfeatures` | `…/collections/{c}/items`, `/items/{id}/tgsequence` | items + trajectory detail |
| Coverages | `/coverages` | `…/collections/{c}/coverage` + `/domainset` `/rangetype` `/metadata` | coverage description + map |
| EDR | `/edr` | `…/collections/{c}/position` `/area` `/cube` `/locations` | query builder |

All share `…/catalogs/{catalog_id}/collections` and `…/collections/{collection_id}`. Shared host for static assets is the always-on `web/static/common/` layer; pages served at `/web/{prefix}/{file}` import shared modules via `../static/common/...` (see PR #1966).

## Design decisions

- **Approach: shell + pluggable adapters.** A shared `common/ogc-browser.js` owns catalog→collection navigation and delegates the collection body to a per-protocol *adapter*. Chosen over a declarative config-only browser (can't express Coverages/EDR without escape hatches) and over server-side composition (breaks the no-build vanilla-JS convention).
- **One default `items` adapter** covers Records and MovingFeatures by configuration and retrofits STAC + Features.
- **Two bespoke adapters** for the divergent bodies: `coverage` and `edr`.
- **Vanilla JS, no build step**; reuse `api.js`, `context-bar.js`, `i18n.js`, `leaflet-map.js` from PR #1966.
- **Languages:** en/fr/es, via the existing `i18n.js`.

## Components

### Shared shell — `web/static/common/ogc-browser.js` (new)

Exports `mountOgcBrowser({ root, basePath, adapter, writeActions, i18n })`:

- Owns: catalog list → collection list navigation, breadcrumb, `language=` threading, loading/empty/error states, and a content slot element handed to the adapter.
- Uses existing `api.js` (`getJSON`/`postJSON`), `i18n.js` (`register`/`t`/`lang`), and—when the adapter needs a map—`leaflet-map.js`.
- Calls `adapter.renderCollectionBody({ catalogId, collectionId, contentEl, map, lang })` when a collection is selected, and `adapter.renderDetail(...)` if the adapter declares one.
- `writeActions` (optional): protocol-specific create forms (e.g. STAC create-collection) surfaced by the shell; gating is cosmetic, the API enforces.

**Adapter contract:**
```
{
  id: string,
  needsMap?: boolean,
  renderCollectionBody({ catalogId, collectionId, contentEl, map, lang }): Promise<void>,
  renderDetail?({ catalogId, collectionId, itemId, contentEl, map, lang }): Promise<void>
}
```

### Adapters

- **`items` adapter — `web/static/common/ogc-items-adapter.js` (new, shared).** Lists `…/collections/{c}/items?language=…&limit=…`, renders a table + map (`showGeoJSON`) + detail panel. Config object: `{ itemsPath = "items", columns, idField, detail }`. Covers:
  - **Records** — `items` adapter as-is (records are GeoJSON-ish features/metadata).
  - **MovingFeatures** — `items` adapter with a `detail` override that also fetches `/items/{id}/tgsequence` and draws the trajectory.
  - **STAC / Features (retrofit)** — their bespoke `*_browser.js` from PR #1966 collapse to: shell + `items` adapter + their existing create-forms passed as `writeActions`.
- **`coverage` adapter — `coverages/static/coverage-adapter.js` (new, in the coverages ext).** Fetches `/coverage/domainset` + `/rangetype` + `/metadata`, renders an axes/range summary panel and the collection footprint on the map.
- **`edr` adapter — `edr/static/edr-adapter.js` (new, in the edr ext).** A query builder: choose position/area/cube/locations, draw/enter geometry, datetime, parameters; issue the query; render the response on the map. Read-only (no writes).

### Per-extension pages (thin)

Each new section is the PR #1966 shape: a `*_browser.html` shell (loads Leaflet when the adapter needs a map, `admin.css`, `{{VERSION}}`) + a few lines wiring `mountOgcBrowser({ basePath, adapter })`, plus `@expose_web_page` + `@expose_static(prefix)` + `get_web_pages`/`get_static_assets` on the service (mirroring STAC).

- `records` → `records_browser`, `items` adapter.
- `moving_features` → `movingfeatures_browser`, `items` adapter + trajectory detail.
- `coverages` → `coverages_browser`, `coverage` adapter.
- `edr` → `edr_browser`, `edr` adapter.

## Build order

1. **Extract** `ogc-browser.js` + `ogc-items-adapter.js`; **retrofit STAC + Features** onto them (delete their duplicated browse logic, keep create-forms as `writeActions`). *Gated on PR #1966 merging.*
2. **Records** + **MovingFeatures** pages (config-only on the `items` adapter) — independent, parallelizable.
3. **Coverages** page + `coverage` adapter.
4. **EDR** page + `edr` adapter.

Phases 2–4 are independent of each other once Phase 1 lands → suitable for parallel agents.

## Testing

- Per new section: registration + handler tests (page id + static prefix register; handler returns 200 HTML with `{{VERSION}}` substituted) — same harness as PR #1966.
- Retrofit guard: STAC/Features registration tests from PR #1966 must still pass after they switch to the shared shell.
- JS stays thin (no JS harness); correctness rides on the already-tested JSON APIs.
- Run with the worktree PYTHONPATH override; strip editable meta-path finders so the worktree source resolves (see PR #1966 notes).

## Open items to pin during planning

- Exact item/record field names for the `items` adapter columns per protocol (Records record id field; MovingFeatures `tgsequence` shape).
- Coverages `domainset`/`rangetype` JSON shape for the summary panel.
- EDR per-collection `data_queries`/`parameter_names` advertised in the collection metadata, to build the query form from the collection rather than hardcoding.
