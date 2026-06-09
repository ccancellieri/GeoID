// common/ogc-items-adapter.js — default body adapter: list collection items in a table + map + detail.
// Built as a factory so the page supplies the single source of `basePath`.
import { getJSON } from "./api.js";
import { register, t, lang } from "./i18n.js";
import { showGeoJSON } from "./leaflet-map.js";

register({
  en: { "items.title": "Items", "items.id": "ID", "items.empty": "No items", "items.view": "View" },
  fr: { "items.title": "Éléments", "items.id": "ID", "items.empty": "Aucun élément", "items.view": "Voir" },
  es: { "items.title": "Elementos", "items.id": "ID", "items.empty": "Sin elementos", "items.view": "Ver" },
});

// makeItemsAdapter({ basePath, itemsPath="items", idField="id", detail }) -> adapter
//   detail({ catalogId, collectionId, itemId, contentEl, map }) : optional per-item hook
export function makeItemsAdapter(cfg = {}) {
  const itemsPath = cfg.itemsPath || "items";
  const idField = cfg.idField || "id";
  const layerRef = { current: null };
  return {
    id: "items",
    needsMap: true,
    async renderCollectionBody({ catalogId, collectionId, contentEl, map }) {
      const url = `${cfg.basePath}/catalogs/${catalogId}/collections/${collectionId}/${itemsPath}`
        + `?limit=50&language=${lang()}`;
      const fc = await getJSON(url);
      const feats = fc.features || fc.items || fc;
      contentEl.replaceChildren();
      const h = document.createElement("h3"); h.textContent = t("items.title"); contentEl.appendChild(h);
      if (!feats || feats.length === 0) {
        const p = document.createElement("p"); p.textContent = t("items.empty"); contentEl.appendChild(p); return;
      }
      const table = document.createElement("table");
      for (const f of feats) {
        const itemId = f[idField] ?? f.id ?? "";
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.textContent = itemId;                             // textContent => XSS-safe
        tr.appendChild(td);
        const view = document.createElement("button"); view.textContent = t("items.view");
        view.addEventListener("click", () => {
          if (map && f.geometry) showGeoJSON(map, layerRef, f);
          if (cfg.detail) cfg.detail({ catalogId, collectionId, itemId, contentEl, map });
        });
        const tdv = document.createElement("td"); tdv.appendChild(view); tr.appendChild(tdv);
        table.appendChild(tr);
      }
      contentEl.appendChild(table);
    },
  };
}
