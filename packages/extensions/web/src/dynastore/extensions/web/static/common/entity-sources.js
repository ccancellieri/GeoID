// common/entity-sources.js — source adapters for mountEntitySelector().
// Each source describes how to fetch pages of items; entity-selector.js drives
// the pagination and search-debounce logic.
import { getJSON, fetchCatalogOptions } from "./api.js";
import { apiUrl } from "./url.js";

// qs helper: builds ?k=v&… omitting null/undefined/empty values.
function qs(params) {
  const out = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v != null && v !== "") out.set(k, String(v));
  }
  const s = out.toString();
  return s ? `?${s}` : "";
}

// ---------------------------------------------------------------------------
// catalogSource() — wraps fetchCatalogOptions(); client-side filter only.
// ---------------------------------------------------------------------------

/**
 * Returns a source adapter suitable for mountEntitySelector() that lists all
 * catalogs the current user can see. Because /web/catalogs returns the full
 * list in one go, pagination and server-side search are not needed.
 *
 * @returns {object} source adapter
 */
export function catalogSource() {
  return {
    supportsSearch: false,
    paginated:      false,
    labelOf: (c) => c.title || c.id,
    idOf:    (c) => c.id,
    async fetch() {
      const items = await fetchCatalogOptions();
      return { items, hasMore: false };
    },
  };
}

// ---------------------------------------------------------------------------
// collectionSource({ basePath, filter }) — server-side search + pagination.
// ---------------------------------------------------------------------------

/**
 * Factory that produces a source adapter for a specific catalog's collections.
 *
 * @param {object} cfg
 * @param {string}   cfg.basePath - API mount prefix, e.g. "../../volumes"
 * @param {function} [cfg.filter] - optional predicate applied after fetching,
 *                                  e.g. c => c.collectionType === "3dcontainer"
 * @returns {{ forCatalog(catalogId): source }}
 */
export function collectionSource({ basePath, filter } = {}) {
  return {
    /**
     * Bind the source to a specific catalog.
     *
     * @param {string} catalogId
     * @returns {object} source adapter
     */
    forCatalog(catalogId) {
      return {
        supportsSearch: true,
        paginated:      true,
        labelOf: (c) => c.title || c.id,
        idOf:    (c) => c.id,
        async fetch({ q, limit = 25, offset = 0, lang } = {}) {
          const url = apiUrl(`${basePath}/catalogs/${encodeURIComponent(catalogId)}/collections`)
            + qs({ q, limit, offset, language: lang });
          const res  = await getJSON(url);
          // Compute hasMore BEFORE applying the optional client filter so the
          // paging signal is based on what the server returned, not on what
          // passes the filter (prevents premature load-more suppression).
          const raw     = res.collections || res;
          const hasMore = Array.isArray(raw) && raw.length === limit;
          const items   = filter ? raw.filter(filter) : raw;
          return { items, hasMore };
        },
      };
    },
  };
}
