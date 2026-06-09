// coverage-adapter.js — OGC API - Coverages body adapter for the shared ogc-browser shell.
// Fetches domainset + rangetype for a selected collection and renders axis labels and
// range fields into the detail pane.

import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";

register({
  en: { "cov.axes": "Axes", "cov.range": "Range fields", "cov.meta": "Metadata", "cov.none": "—" },
  fr: { "cov.axes": "Axes", "cov.range": "Champs de plage", "cov.meta": "Métadonnées", "cov.none": "—" },
  es: { "cov.axes": "Ejes", "cov.range": "Campos de rango", "cov.meta": "Metadatos", "cov.none": "—" },
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
  const h = document.createElement("h3");
  h.textContent = title;
  el.appendChild(h);
  const ul = document.createElement("ul");
  for (const line of lines) {
    const li = document.createElement("li");
    li.textContent = line;
    ul.appendChild(li);
  }
  el.appendChild(ul);
}

// domainset shape: { "type": "DomainSet", "generalGrid": { "axisLabels": ["Lon","Lat",...], "axis": [...] } }
function summariseDomain(d) {
  if (!d) return [t("cov.none")];
  const axes = (d.generalGrid && d.generalGrid.axisLabels) || d.axisLabels || [];
  return axes.length ? axes.map(String) : [t("cov.none")];
}

// rangetype shape: { "type": "DataRecord", "field": [ { "name": "band", "definition": "...", ... } ] }
function summariseRange(r) {
  if (!r) return [t("cov.none")];
  const fields = r.field || r.fields || [];
  return fields.length ? fields.map((f) => f.name || f.definition || String(f)) : [t("cov.none")];
}
