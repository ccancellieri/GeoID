// edr/edr-adapter.js — EDR position-query body adapter for ogc-browser.js.
// Renders a small query form into the collection body pane and plots GeoJSON
// results on the shared map.
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";
import { showGeoJSON } from "../static/common/leaflet-map.js";

register({
  en: { "edr.position": "Position (lon,lat)", "edr.datetime": "Datetime", "edr.param": "Parameter name(s)", "edr.run": "Query" },
  fr: { "edr.position": "Position (lon,lat)", "edr.datetime": "Date/heure", "edr.param": "Nom(s) de paramètre", "edr.run": "Interroger" },
  es: { "edr.position": "Posición (lon,lat)", "edr.datetime": "Fecha/hora", "edr.param": "Nombre(s) de parámetro", "edr.run": "Consultar" },
});

export function makeEdrAdapter({ basePath }) {
  const layerRef = { current: null };
  return {
    id: "edr",
    needsMap: true,
    async renderCollectionBody({ catalogId, collectionId, contentEl, map }) {
      contentEl.replaceChildren();

      // --- form elements ---
      const posLabel = document.createElement("label");
      posLabel.textContent = t("edr.position");
      posLabel.className = "edr-label";

      const pos = document.createElement("input");
      pos.type = "text";
      pos.placeholder = "12.5,41.9";
      pos.className = "edr-input";

      const dtLabel = document.createElement("label");
      dtLabel.textContent = t("edr.datetime");
      dtLabel.className = "edr-label";

      const dt = document.createElement("input");
      dt.type = "text";
      dt.placeholder = "2024-01-01T00:00:00Z";
      dt.className = "edr-input";

      const paramLabel = document.createElement("label");
      paramLabel.textContent = t("edr.param");
      paramLabel.className = "edr-label";

      const paramInput = document.createElement("input");
      paramInput.type = "text";
      paramInput.placeholder = "temperature,wind_speed";
      paramInput.className = "edr-input";

      const run = document.createElement("button");
      run.textContent = t("edr.run");
      run.className = "edr-btn";

      const out = document.createElement("pre");
      out.className = "edr-output";

      run.addEventListener("click", async () => {
        out.textContent = "";
        const raw = pos.value.trim();
        if (!raw) { out.textContent = "Position is required (e.g. 12.5,41.9)"; return; }
        const parts = raw.split(",");
        if (parts.length < 2) { out.textContent = "Expected lon,lat (e.g. 12.5,41.9)"; return; }
        const [lon, lat] = parts.map((s) => s.trim());

        // The /position route accepts:
        //   coords=POINT(lon lat)  (WKT, positional — the parameter name used by the server)
        //   f=GeoJSON              (format; server accepts "geojson" case-insensitively)
        //   datetime=<ISO>         (optional ISO-8601 instant or interval)
        //   parameter-name=...     (optional comma-separated band names; server alias "parameter-name")
        const q = new URLSearchParams({
          coords: `POINT(${lon} ${lat})`,
          f: "GeoJSON",
          language: lang(),
        });
        if (dt.value.trim()) q.set("datetime", dt.value.trim());
        if (paramInput.value.trim()) q.set("parameter-name", paramInput.value.trim());

        const url = `${basePath}/catalogs/${catalogId}/collections/${collectionId}/position?${q}`;
        try {
          const res = await getJSON(url);
          if (map && res && (res.type === "Feature" || res.type === "FeatureCollection")) {
            showGeoJSON(map, layerRef, res);
          }
          out.textContent = JSON.stringify(res, null, 2).slice(0, 4000);
        } catch (e) {
          out.textContent = String(e);
        }
      });

      for (const el of [posLabel, pos, dtLabel, dt, paramLabel, paramInput, run, out]) {
        contentEl.appendChild(el);
      }
    },
  };
}
