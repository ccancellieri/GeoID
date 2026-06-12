// volumes-globe.js — OGC API 3D GeoVolumes globe browser (ES module).
// Depends on: maplibre-gl.js and deck.gl dist.min.js loaded as CDN plain scripts
// before this module (they expose window.maplibregl and window.deck).

import { mountEntitySelector } from "../static/common/entity-selector.js";
import { catalogSource } from "../static/common/entity-sources.js";
import { createMapLibreGlobe } from "../static/common/maplibre-map.js";
import { register, t } from "../static/common/i18n.js";

register({
  en: {
    "vol.title": "Agro-Informatics Platform: 3D GeoVolumes Browser",
    "vol.back": "Back to Home",
    "vol.catalog": "Catalog",
    "vol.selectCatalog": "Select a catalog to browse 3D containers.",
    "vol.loading": "Loading containers…",
    "vol.loadFailed": "Failed to load 3D containers.",
    "vol.noContainers": "No 3D containers found.",
  },
  fr: {
    "vol.title": "Plateforme agro-informatique : navigateur 3D GeoVolumes",
    "vol.back": "Retour à l'accueil",
    "vol.catalog": "Catalogue",
    "vol.selectCatalog": "Sélectionnez un catalogue pour parcourir les conteneurs 3D.",
    "vol.loading": "Chargement des conteneurs…",
    "vol.loadFailed": "Échec du chargement des conteneurs 3D.",
    "vol.noContainers": "Aucun conteneur 3D trouvé.",
  },
  es: {
    "vol.title": "Plataforma agroinformática: navegador 3D GeoVolumes",
    "vol.back": "Volver al inicio",
    "vol.catalog": "Catálogo",
    "vol.selectCatalog": "Seleccione un catálogo para explorar contenedores 3D.",
    "vol.loading": "Cargando contenedores…",
    "vol.loadFailed": "Error al cargar los contenedores 3D.",
    "vol.noContainers": "No se encontraron contenedores 3D.",
  },
});

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Base path for the OGC API (same origin, rooted at /volumes).
// The HTML page is served at /web/volumes/ so ../../volumes reaches
// the API mount at /volumes.
const API_BASE      = "../../volumes";
const FEATURES_BASE = "../../features";

// 3D Tiles relationship URI per OGC API GeoVolumes spec.
const REL_3DTILES = "http://www.opengis.net/def/rel/ogc/1.0/3dtiles";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let _map            = null;
let _overlay        = null;
let _catalogId      = null;
let _containers     = [];
let _activeContainerId = null;

// ---------------------------------------------------------------------------
// DOM refs (populated after DOMContentLoaded)
// ---------------------------------------------------------------------------

let containerTree, infoPanel, infoPropsDl, infoClose;

// ---------------------------------------------------------------------------
// Initialise globe after DOM is ready
// ---------------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  containerTree = document.getElementById("container-tree");
  infoPanel     = document.getElementById("info-panel");
  infoPropsDl   = document.getElementById("info-props");
  infoClose     = document.getElementById("info-close");

  applyTranslations();

  const globe = createMapLibreGlobe("geovolumes-map");
  _map     = globe.map;
  _overlay = globe.overlay;

  infoClose.addEventListener("click", () => { infoPanel.style.display = "none"; });

  mountEntitySelector({
    root:      "#catalog-selector",
    source:    catalogSource(),
    onChange:  (cat) => {
      _catalogId = cat ? cat.id : null;
      if (_catalogId) loadContainers(_catalogId);
      else resetTree(t("vol.selectCatalog"));
    },
  });
});

function applyTranslations() {
  const labels = {
    "page-title": "vol.title",
    "lbl-back": "vol.back",
    "lbl-catalog": "vol.catalog",
    "lbl-select-catalog": "vol.selectCatalog",
  };
  for (const [id, key] of Object.entries(labels)) {
    const el = document.getElementById(id);
    if (el) el.textContent = t(key);
  }
}

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

function fetchJSON(url) {
  return fetch(url).then((r) => {
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
    return r.json();
  });
}

// ---------------------------------------------------------------------------
// Container tree
// ---------------------------------------------------------------------------

function loadContainers(catalogId) {
  resetTree(t("vol.loading"));
  fetchJSON(`${API_BASE}/catalogs/${encodeURIComponent(catalogId)}/collections`)
    .then((data) => {
      const containers = (data.collections || []).filter(
        (c) => c.collectionType === "3dcontainer",
      );
      _containers = containers;
      renderContainerTree(containers);
      renderVolumeLayers(containers);
    })
    .catch(() => { resetTree(t("vol.loadFailed")); });
}

function resetTree(msg) {
  const spinner = document.createElement("div");
  spinner.className = "loading-spinner";
  const p = document.createElement("p");
  p.textContent = msg;
  spinner.appendChild(p);
  containerTree.replaceChildren(spinner);
  _containers = [];
  _activeContainerId = null;
  clearOverlayLayers();
}

function renderContainerTree(containers) {
  containerTree.replaceChildren();
  if (!containers || containers.length === 0) {
    const spinner = document.createElement("div");
    spinner.className = "loading-spinner";
    const p = document.createElement("p");
    p.textContent = t("vol.noContainers");
    spinner.appendChild(p);
    containerTree.appendChild(spinner);
    return;
  }
  for (const c of containers) {
    containerTree.appendChild(buildNodeEl(c));
  }
}

function buildNodeEl(container) {
  const wrapper = document.createElement("div");

  const node = document.createElement("div");
  node.className = "container-node";
  node.dataset.id = container.id;

  const titleSpan = document.createElement("span");
  titleSpan.textContent = container.title || container.id;

  const typeDiv = document.createElement("div");
  typeDiv.className = "node-type";
  typeDiv.textContent = container.collectionType || "";

  node.appendChild(titleSpan);
  node.appendChild(typeDiv);
  node.addEventListener("click", () => { selectContainer(container); });
  wrapper.appendChild(node);

  const children = container.children || [];
  if (children.length > 0) {
    const childDiv = document.createElement("div");
    childDiv.className = "container-children";
    for (const ch of children) {
      childDiv.appendChild(buildNodeEl(ch));
    }
    wrapper.appendChild(childDiv);
  }

  return wrapper;
}

// ---------------------------------------------------------------------------
// Volume (extruded polygon) layers — one per container
// ---------------------------------------------------------------------------

function renderVolumeLayers(containers) {
  /* global deck */
  const polygonData = [];
  for (const c of containers) {
    const ext  = c.contentExtent || {};
    const bbox = ext.bbox;
    if (!bbox || bbox.length < 6) continue;
    const [w, s, zmin, e, n, zmax] = bbox;
    polygonData.push({
      id: c.id,
      contour: [[w, s], [e, s], [e, n], [w, n], [w, s]],
      elevation: zmin,
      extrudedHeight: zmax,
      container: c,
    });
  }

  const volumeLayer = new deck.PolygonLayer({
    id:        "geovolumes-extents",
    data:      polygonData,
    extruded:  true,
    wireframe: true,
    getPolygon:      (d) => d.contour,
    getElevation:    (d) => d.extrudedHeight - d.elevation,
    getFillColor:    [99, 102, 241, 64],
    getLineColor:    [99, 102, 241, 180],
    lineWidthMinPixels: 1,
    pickable:  true,
    onClick:   (info) => { if (info.object) selectContainer(info.object.container); },
  });

  setOverlayLayers([volumeLayer]);
}

// ---------------------------------------------------------------------------
// Container selection: fly to + add 3D Tiles + footprints layer
// ---------------------------------------------------------------------------

function selectContainer(container) {
  document.querySelectorAll(".container-node").forEach((el) => {
    el.classList.toggle("active", el.dataset.id === container.id);
  });
  _activeContainerId = container.id;

  const ext  = container.contentExtent || {};
  const bbox = ext.bbox;
  if (bbox && bbox.length >= 5) {
    const lng = (bbox[0] + bbox[3]) / 2;
    const lat = (bbox[1] + bbox[4]) / 2;
    _map.flyTo({ center: [lng, lat], zoom: 14, pitch: 50, duration: 1500 });
  }

  // Show or hide the attribution line for this container.
  let attrEl = document.getElementById("container-attribution");
  if (!attrEl) {
    attrEl = document.createElement("div");
    attrEl.id = "container-attribution";
    attrEl.style.cssText = (
      "font-size:0.7rem;color:#64748b;margin-top:0.4rem;"
      + "word-break:break-word;line-height:1.4;"
    );
    const tree = document.getElementById("container-tree");
    if (tree) tree.parentElement.appendChild(attrEl);
  }
  attrEl.textContent = container.attribution || "";
  attrEl.style.display = container.attribution ? "block" : "none";

  setOverlayLayers(buildContainerLayers(container));
}

function buildContainerLayers(container) {
  /* global deck */
  const polygonData = [];
  for (const c of _containers) {
    const ext  = c.contentExtent || {};
    const bbox = ext.bbox;
    if (!bbox || bbox.length < 6) continue;
    const [w, s, zmin, e, n, zmax] = bbox;
    polygonData.push({
      id: c.id,
      contour: [[w, s], [e, s], [e, n], [w, n], [w, s]],
      elevation: zmin,
      extrudedHeight: zmax,
      container: c,
    });
  }

  const layers = [
    new deck.PolygonLayer({
      id:        "geovolumes-extents",
      data:      polygonData,
      extruded:  true,
      wireframe: true,
      getPolygon:   (d) => d.contour,
      getElevation: (d) => d.extrudedHeight - d.elevation,
      // The selected container is drawn as a wireframe-only box (zero fill) so
      // the per-building volumes rendered inside it (footprints layer below)
      // remain visible; other containers keep a faint translucent fill so they
      // read as surrounding context.
      getFillColor: (d) => d.id === container.id ? [99, 102, 241, 0] : [99, 102, 241, 40],
      getLineColor:    [99, 102, 241, 180],
      lineWidthMinPixels: 1,
      pickable:  true,
      onClick:   (info) => { if (info.object) selectContainer(info.object.container); },
    }),
  ];

  const tilesHref = find3dTilesHref(container);
  if (tilesHref) {
    layers.push(new deck.Tile3DLayer({
      id:       `3dtiles-${container.id}`,
      data:     tilesHref,
      pickable: true,
      onTilesetLoad: () => {},
    }));
  }

  if (_catalogId) {
    const itemsUrl =
      `${FEATURES_BASE}/catalogs/${encodeURIComponent(_catalogId)}`
      + `/collections/${encodeURIComponent(container.id)}/items?limit=200`;
    layers.push(new deck.GeoJsonLayer({
      id:       `footprints-${container.id}`,
      data:     itemsUrl,
      pickable: true,
      stroked:  true,
      filled:   true,
      // Extrude each building footprint to its real height so CityJSON-ingested
      // collections render as 3D LoD1 volumes (not flat outlines). Height comes
      // from the per-building `height` attribute when present, otherwise the
      // z-extent (zmax - zmin) stamped at ingest. Both arrive via OGC Features.
      extruded:  true,
      wireframe: true,
      getElevation: featureElevation,
      getFillColor:    [99, 200, 241, 180],
      getLineColor:    [40, 120, 160, 220],
      lineWidthMinPixels: 1,
      onClick: (info) => { if (info.object) showBuildingPopup(info.object); },
    }));
  }

  return layers;
}

// Derive an extrusion height (metres) for a building feature from the
// properties stamped at CityJSON ingest. Prefers the explicit `height`
// attribute; falls back to the vertical extent (zmax - zmin); 0 when neither
// is usable (e.g. external samples carry no footprints and never reach here).
function featureElevation(feature) {
  const p = (feature && feature.properties) || {};
  const h = Number(p.height);
  if (Number.isFinite(h) && h > 0) return h;
  const zmin = Number(p.zmin);
  const zmax = Number(p.zmax);
  if (Number.isFinite(zmin) && Number.isFinite(zmax) && zmax > zmin) {
    return zmax - zmin;
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Building attribute popup
// ---------------------------------------------------------------------------

function showBuildingPopup(feature) {
  const props = feature.properties || {};
  infoPropsDl.replaceChildren();

  const keys = Object.keys(props);
  if (keys.length === 0) {
    const dt = document.createElement("dt");
    dt.textContent = "(no properties)";
    infoPropsDl.appendChild(dt);
  } else {
    const preferred = ["citygml_type", "height", "lod"];
    const rest      = keys.filter((k) => !preferred.includes(k));
    const ordered   = preferred.filter((k) => k in props).concat(rest);

    for (const key of ordered) {
      const dt = document.createElement("dt");
      dt.textContent = key;
      const dd = document.createElement("dd");
      dd.textContent = String(props[key]);
      infoPropsDl.appendChild(dt);
      infoPropsDl.appendChild(dd);
    }
  }

  infoPanel.style.display = "block";
}

// ---------------------------------------------------------------------------
// Overlay layer management
// ---------------------------------------------------------------------------

function setOverlayLayers(layers) {
  _overlay.setProps({ layers });
}

function clearOverlayLayers() {
  _overlay.setProps({ layers: [] });
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function find3dTilesHref(container) {
  const links = (container.content || []).concat(container.links || []);
  for (const link of links) {
    if (link.rel === REL_3DTILES) return resolveApiHref(link.href);
  }
  return null;
}

// The API emits root-relative hrefs ("/volumes/..."), which miss any gateway
// path prefix the deployment sits behind. Re-base them onto API_BASE so they
// resolve through the same prefix as every other request this page makes.
function resolveApiHref(href) {
  if (typeof href === "string" && href.startsWith("/volumes/")) {
    return `${API_BASE}${href.slice("/volumes".length)}`;
  }
  return href;
}
