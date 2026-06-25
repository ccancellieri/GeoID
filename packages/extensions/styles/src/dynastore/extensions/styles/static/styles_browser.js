// styles/styles_browser.js — entry point for the Styles Browser page.
// Lists every style visible to the caller via the /styles/all discovery
// endpoint and renders metadata + stylesheet/legend links for the selection.
import { getJSON } from "../static/common/api.js";
import { register, t } from "../static/common/i18n.js";
import { apiUrl } from "../static/common/url.js";
import { mountEntitySelector } from "../static/common/entity-selector.js";

register({
  en: { "sty.loading": "Loading…", "sty.none": "Nothing to show", "sty.error": "Failed to load" },
  fr: { "sty.loading": "Chargement…", "sty.none": "Rien à afficher", "sty.error": "Échec du chargement" },
  es: { "sty.loading": "Cargando…", "sty.none": "Nada que mostrar", "sty.error": "Error al cargar" },
});

const navEl = document.getElementById("nav-list");
const bodyEl = document.getElementById("detail-body");

function styleBase(s) {
  return `/styles/catalogs/${encodeURIComponent(s.catalog_id)}` +
    `/collections/${encodeURIComponent(s.collection_id)}` +
    `/styles/${encodeURIComponent(s.id)}`;
}

function row(table, key, valueNode) {
  const tr = document.createElement("tr");
  const td1 = document.createElement("td");
  td1.textContent = key;
  const td2 = document.createElement("td");
  if (valueNode instanceof Node) td2.appendChild(valueNode);
  else td2.textContent = String(valueNode ?? "");
  tr.appendChild(td1);
  tr.appendChild(td2);
  table.appendChild(tr);
}

function link(href, label) {
  const a = document.createElement("a");
  a.href = apiUrl(href);
  a.target = "_blank";
  a.rel = "noopener";
  a.textContent = label;
  return a;
}

async function showDetail(s) {
  bodyEl.replaceChildren();
  const h = document.createElement("h3");
  h.textContent = s.title || s.id;
  bodyEl.appendChild(h);

  const table = document.createElement("table");
  row(table, "id", s.id);
  row(table, "catalog", s.catalog_id);
  row(table, "collection", s.collection_id);
  if (s.description) row(table, "description", s.description);
  const base = styleBase(s);
  row(table, "stylesheet", link(`${base}/stylesheet`, "stylesheet"));
  row(table, "metadata", link(`${base}/metadata`, "metadata"));
  row(table, "legend", link(`${base}/legend`, "legend"));
  bodyEl.appendChild(table);

  try {
    const meta = await getJSON(`${base}/metadata`);
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify(meta, null, 2);
    bodyEl.appendChild(pre);
  } catch (e) { /* metadata is optional; links already rendered */ }
}

function styleSource() {
  return {
    supportsSearch: false,
    paginated: false,
    labelOf: (s) => `${s.title || s.id} · ${s.catalog_id}/${s.collection_id}`,
    idOf: (s) => `${s.catalog_id}/${s.collection_id}/${s.id}`,
    async fetch() {
      const res = await getJSON("/styles/all");
      const items = res.styles || res || [];
      return { items, hasMore: false };
    },
  };
}

mountEntitySelector({
  root: navEl,
  source: styleSource(),
  onChange: (s) => { if (s) showDetail(s); },
});
