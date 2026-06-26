// features_browser.js — OGC API Features browser: map-based browse + create-feature.
// Imports shared helpers only; no build step required.

import { getJSON, postJSON } from "../static/common/api.js";
import { mountContextBar } from "../static/common/context-bar.js";
import { register, t, lang } from "../static/common/i18n.js";
import { initMap, showGeoJSON, bboxFromMap } from "../static/common/leaflet-map.js";

// ---------------------------------------------------------------------------
// i18n dictionaries
// ---------------------------------------------------------------------------
register({
  en: {
    "page.title":         "Features Browser",
    "lbl.back":           "Back to Home",
    "lbl.select":         "Select a catalog and collection.",
    "lbl.loading":        "Loading features…",
    "lbl.none":           "No features in current view.",
    "lbl.create.summary": "Create feature",
    "lbl.create.btn":     "Create",
    "lbl.refresh":        "Refresh",
    "create.ok":          "Feature created.",
    "create.refresh.ok":  "Feature created — map refreshed.",
    "err.forbidden":      "Permission denied (401/403). Check your login.",
    "err.props":          "Properties must be valid JSON (e.g. {\"name\":\"value\"}).",
    "err.coords":         "Enter valid longitude and latitude.",
    "err.load":           "Failed to load features.",
  },
  fr: {
    "page.title":         "Explorateur d'entités",
    "lbl.back":           "Retour à l'accueil",
    "lbl.select":         "Sélectionnez un catalogue et une collection.",
    "lbl.loading":        "Chargement des entités…",
    "lbl.none":           "Aucune entité dans la vue actuelle.",
    "lbl.create.summary": "Créer une entité",
    "lbl.create.btn":     "Créer",
    "lbl.refresh":        "Actualiser",
    "create.ok":          "Entité créée.",
    "create.refresh.ok":  "Entité créée — carte actualisée.",
    "err.forbidden":      "Accès refusé (401/403). Vérifiez votre connexion.",
    "err.props":          "Les propriétés doivent être du JSON valide (ex. {\"nom\":\"valeur\"}).",
    "err.coords":         "Entrez une longitude et une latitude valides.",
    "err.load":           "Échec du chargement des entités.",
  },
  es: {
    "page.title":         "Explorador de entidades",
    "lbl.back":           "Volver al inicio",
    "lbl.select":         "Seleccione un catálogo y una colección.",
    "lbl.loading":        "Cargando entidades…",
    "lbl.none":           "No hay entidades en la vista actual.",
    "lbl.create.summary": "Crear entidad",
    "lbl.create.btn":     "Crear",
    "lbl.refresh":        "Actualizar",
    "create.ok":          "Entidad creada.",
    "create.refresh.ok":  "Entidad creada — mapa actualizado.",
    "err.forbidden":      "Acceso denegado (401/403). Compruebe su sesión.",
    "err.props":          "Las propiedades deben ser JSON válido (p.ej. {\"nombre\":\"valor\"}).",
    "err.coords":         "Introduzca longitud y latitud válidas.",
    "err.load":           "Error al cargar las entidades.",
  },
});

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let _map = null;
let _layerRef = { current: null };
let _currentCat = null;
let _currentColl = null;

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------
function el(id) { return document.getElementById(id); }

function setListText(msg) {
  const container = el("items-list");
  container.innerHTML = "";
  const div = document.createElement("div");
  div.className = "loading-spinner";
  div.textContent = msg;
  container.appendChild(div);
}

function setListError(msg) {
  const container = el("items-list");
  container.innerHTML = "";
  const div = document.createElement("div");
  div.className = "notice-box notice-err";
  div.textContent = msg;
  container.appendChild(div);
}

function showNotice(containerId, msg, ok) {
  const box = el(containerId);
  box.className = "notice-box " + (ok ? "notice-ok" : "notice-err");
  box.textContent = msg;
  box.style.display = "block";
}

function hideNotice(containerId) {
  const box = el(containerId);
  if (box) box.style.display = "none";
}

// ---------------------------------------------------------------------------
// Feature list rendering
// ---------------------------------------------------------------------------
function renderFeatureList(features) {
  const container = el("items-list");
  container.innerHTML = "";
  if (!features || features.length === 0) {
    const msg = document.createElement("div");
    msg.className = "loading-spinner";
    msg.textContent = t("lbl.none");
    container.appendChild(msg);
    return;
  }
  for (const f of features) {
    const id = String(f.id ?? "(no id)");
    const geomType = f.geometry ? f.geometry.type : "—";

    const card = document.createElement("div");
    card.className = "feature-card";
    card.dataset.id = id;

    const nameDiv = document.createElement("div");
    nameDiv.style.fontWeight = "500";
    nameDiv.textContent = id;

    const typeDiv = document.createElement("div");
    typeDiv.className = "fid";
    typeDiv.textContent = geomType;

    card.appendChild(nameDiv);
    card.appendChild(typeDiv);
    container.appendChild(card);
  }
}

// ---------------------------------------------------------------------------
// Browse: load features for the current catalog + collection
// ---------------------------------------------------------------------------
async function loadFeatures() {
  if (!_currentCat || !_currentColl) return;
  setListText(t("lbl.loading"));
  const bbox = _map ? bboxFromMap(_map) : null;
  const bboxParam = bbox ? `&bbox=${bbox}` : "";
  const url = `/features/catalogs/${_currentCat}/collections/${_currentColl}/items?limit=50${bboxParam}&language=${lang()}`;
  try {
    const fc = await getJSON(url);
    const features = (fc && fc.features) ? fc.features : [];
    showGeoJSON(_map, _layerRef, fc);
    renderFeatureList(features);
  } catch (err) {
    setListError(t("err.load"));
  }
}

// ---------------------------------------------------------------------------
// Create feature
// ---------------------------------------------------------------------------
async function createFeature() {
  hideNotice("create-notice");
  if (!_currentCat || !_currentColl) return;

  const lon = parseFloat(el("input-lon").value);
  const lat = parseFloat(el("input-lat").value);
  if (isNaN(lon) || isNaN(lat) || lon < -180 || lon > 180 || lat < -90 || lat > 90) {
    showNotice("create-notice", t("err.coords"), false);
    return;
  }

  let props = {};
  const rawProps = (el("input-props").value || "").trim();
  if (rawProps) {
    try {
      props = JSON.parse(rawProps);
    } catch (_) {
      showNotice("create-notice", t("err.props"), false);
      return;
    }
  }

  const body = {
    type: "Feature",
    geometry: { type: "Point", coordinates: [lon, lat] },
    properties: props,
  };

  try {
    await postJSON(
      `/features/catalogs/${_currentCat}/collections/${_currentColl}/items`,
      body,
    );
    showNotice("create-notice", t("create.refresh.ok"), true);
    el("input-lon").value = "";
    el("input-lat").value = "";
    el("input-props").value = "";
    await loadFeatures();
  } catch (err) {
    const status = err && err.status;
    if (status === 401 || status === 403) {
      showNotice("create-notice", t("err.forbidden"), false);
    } else {
      showNotice("create-notice", err.message || t("err.load"), false);
    }
  }
}

// ---------------------------------------------------------------------------
// Localise static labels
// ---------------------------------------------------------------------------
function applyLabels() {
  const pageTitle = el("page-title");
  if (pageTitle) pageTitle.textContent = `Agro-Informatics Platform: ${t("page.title")}`;
  const lblBack = el("lbl-back");
  if (lblBack) lblBack.textContent = t("lbl.back");
  const lblSel = el("lbl-select-collection");
  if (lblSel) lblSel.textContent = t("lbl.select");
  const lblSummary = el("lbl-create-summary");
  if (lblSummary) {
    lblSummary.textContent = "";
    const icon = document.createElement("i");
    icon.className = "fa-solid fa-plus";
    icon.style.marginRight = "0.35rem";
    lblSummary.appendChild(icon);
    lblSummary.appendChild(document.createTextNode(t("lbl.create.summary")));
  }
  const lblCreateBtn = el("lbl-create-btn");
  if (lblCreateBtn) lblCreateBtn.textContent = t("lbl.create.btn");
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
function init() {
  applyLabels();

  // Map
  _map = initMap("features-map");

  // Reload features when map is panned/zoomed (debounced to 600 ms)
  let _reloadTimer = null;
  _map.on("moveend zoomend", () => {
    clearTimeout(_reloadTimer);
    _reloadTimer = setTimeout(() => { if (_currentCat && _currentColl) loadFeatures(); }, 600);
  });

  // Context bar: catalog + collection picker
  const csContainer = el("cs-container");
  if (csContainer) {
    mountContextBar(csContainer, {
      mode: "select",
      enableVirtualCollections: false,
      basePath: "/features",
      onChange: ({ catalogId, collectionId }) => {
        _currentCat = catalogId || null;
        _currentColl = collectionId || null;
        if (_currentCat && _currentColl) {
          loadFeatures();
        } else {
          showGeoJSON(_map, _layerRef, null);
          setListText(t("lbl.select"));
        }
      },
    });
  }

  // Create button
  const btnCreate = el("btn-create-feature");
  if (btnCreate) btnCreate.addEventListener("click", createFeature);
}

init();
