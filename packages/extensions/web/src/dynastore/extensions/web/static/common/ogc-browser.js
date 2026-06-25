// common/ogc-browser.js — shared OGC catalog->collection browser shell.
// The per-collection body is rendered by a pluggable adapter; this shell only
// owns navigation, breadcrumb, language threading, and load/empty/error states.
import { getJSON } from "./api.js";
import { register, t, lang } from "./i18n.js";
import { initMap } from "./leaflet-map.js";
import { mountEntitySelector } from "./entity-selector.js";
import { catalogSource, collectionSource } from "./entity-sources.js";

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
  let catalogCtrl = null;
  let collectionCtrl = null;

  async function showCatalogs() {
    state.catalogId = null;
    bodyEl.replaceChildren();
    navEl.replaceChildren();

    const catContainer = document.createElement("div");
    catContainer.className = "ogc-catalog-selector";
    navEl.appendChild(catContainer);

    catalogCtrl = mountEntitySelector({
      root: catContainer,
      source: catalogSource(),
      onChange: (cat) => { if (cat) selectCatalog(cat.id); },
    });
  }

  async function selectCatalog(catalogId) {
    state.catalogId = catalogId;
    bodyEl.replaceChildren();
    navEl.replaceChildren();

    const backBtn = document.createElement("button");
    backBtn.className = "ogc-back-btn";
    backBtn.textContent = "← " + t("ogc.back");
    backBtn.addEventListener("click", showCatalogs);
    navEl.appendChild(backBtn);

    const collContainer = document.createElement("div");
    collContainer.className = "ogc-collection-selector";
    navEl.appendChild(collContainer);

    collectionCtrl = mountEntitySelector({
      root: collContainer,
      source: collectionSource({ basePath }).forCatalog(catalogId),
      onChange: (coll) => { if (coll) selectCollection(coll.id); },
    });
  }

  async function selectCollection(collectionId) {
    bodyEl.replaceChildren();
    bodyEl.textContent = t("ogc.loading");
    try {
      await adapter.renderCollectionBody({
        catalogId: state.catalogId, collectionId, contentEl: bodyEl, map, lang: lang(),
      });
    } catch (e) { bodyEl.textContent = t("ogc.error"); }
  }

  if (writeActions && typeof writeActions.mount === "function") writeActions.mount(root, state);
  showCatalogs();
}
