// common/ogc-browser.js — shared OGC catalog->collection browser shell.
// The per-collection body is rendered by a pluggable adapter; this shell only
// owns navigation, breadcrumb, language threading, and load/empty/error states.
import { getJSON } from "./api.js";
import { register, t, lang } from "./i18n.js";
import { initMap } from "./leaflet-map.js";

register({
  en: { "ogc.catalogs": "Catalogs", "ogc.collections": "Collections", "ogc.back": "Back",
        "ogc.loading": "Loading…", "ogc.none": "Nothing to show", "ogc.error": "Failed to load" },
  fr: { "ogc.catalogs": "Catalogues", "ogc.collections": "Collections", "ogc.back": "Retour",
        "ogc.loading": "Chargement…", "ogc.none": "Rien à afficher", "ogc.error": "Échec du chargement" },
  es: { "ogc.catalogs": "Catálogos", "ogc.collections": "Colecciones", "ogc.back": "Atrás",
        "ogc.loading": "Cargando…", "ogc.none": "Nada que mostrar", "ogc.error": "Error al cargar" },
});

// mountOgcBrowser({ root, basePath, adapter, writeActions }) -> void
//   root        : container element holding [data-ogc-nav], [data-ogc-body], optional [data-ogc-map]
//   basePath    : protocol mount prefix, e.g. "/records"
//   adapter     : body adapter ({ id, needsMap?, renderCollectionBody(), renderDetail? })
//   writeActions: optional { mount(root, state) } for protocol-specific create forms
export function mountOgcBrowser({ root, basePath, adapter, writeActions }) {
  const navEl = root.querySelector("[data-ogc-nav]");
  const bodyEl = root.querySelector("[data-ogc-body]");
  const mapEl = root.querySelector("[data-ogc-map]");
  const map = adapter.needsMap && mapEl ? initMap(mapEl.id) : null;

  const state = { catalogId: null };

  function setLoading(el) { el.textContent = t("ogc.loading"); }

  async function showCatalogs() {
    state.catalogId = null;
    bodyEl.replaceChildren();
    setLoading(navEl);
    try {
      const res = await getJSON(`${basePath}/catalogs?language=${lang()}`);
      const cats = res.catalogs || res.collections || res; // OGC envelope or bare list
      renderList(navEl, cats, (c) => c.title || c.id, (c) => selectCatalog(c.id));
    } catch (e) { navEl.textContent = t("ogc.error"); }
  }

  async function selectCatalog(catalogId) {
    state.catalogId = catalogId;
    bodyEl.replaceChildren();
    setLoading(navEl);
    try {
      const res = await getJSON(`${basePath}/catalogs/${catalogId}/collections?language=${lang()}`);
      const colls = res.collections || res; // OGC envelope or bare list
      renderList(navEl, colls, (c) => c.title || c.id, (c) => selectCollection(c.id), true);
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

  // renderList: builds an accessible list of buttons; prepends a back row when `withBack`.
  function renderList(el, rows, labelOf, onClick, withBack) {
    el.replaceChildren();
    if (withBack) {
      const back = document.createElement("button");
      back.textContent = "← " + t("ogc.back");
      back.addEventListener("click", showCatalogs);
      el.appendChild(back);
    }
    if (!rows || rows.length === 0) {
      const p = document.createElement("p"); p.textContent = t("ogc.none"); el.appendChild(p); return;
    }
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
