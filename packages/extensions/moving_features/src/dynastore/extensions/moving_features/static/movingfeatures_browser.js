// movingfeatures_browser.js — OGC API Moving Features browser.
// Displays moving features per collection and renders trajectory paths on the map.

import { mountOgcBrowser } from "../static/common/ogc-browser.js";
import { makeItemsAdapter } from "../static/common/ogc-items-adapter.js";
import { getJSON } from "../static/common/api.js";
import { showGeoJSON } from "../static/common/leaflet-map.js";

// One shared layer reference so each trajectory view replaces the previous one.
const trajRef = { current: null };

// detail({ catalogId, collectionId, itemId, map }) is called when "View" is
// clicked on a moving feature row.  It fetches the tgsequence for that feature
// and draws the trajectory as a LineString on the map.
//
// tgsequence returns a JSON array of TemporalGeometry objects.  Each object
// carries a `coordinates` field (List[List[float]], each entry [lon, lat] or
// [lon, lat, elev]) and a `datetimes` field.  The trajectory path is built by
// collecting the coordinate arrays from every sequence record in order.
async function detail({ catalogId, collectionId, itemId, map }) {
  if (!map) return;
  try {
    const sequences = await getJSON(
      `/movingfeatures/catalogs/${catalogId}/collections/${collectionId}/items/${itemId}/tgsequence`
    );
    if (!Array.isArray(sequences) || sequences.length === 0) return;

    // Flatten all per-sequence coordinate arrays into one ordered path.
    // Each TemporalGeometry.coordinates is already [[lon,lat], [lon,lat], …].
    const allCoords = sequences.flatMap((seq) =>
      Array.isArray(seq.coordinates) ? seq.coordinates : []
    );

    if (allCoords.length === 0) return;

    const geojson =
      allCoords.length === 1
        ? { type: "Feature", geometry: { type: "Point", coordinates: allCoords[0] }, properties: {} }
        : { type: "Feature", geometry: { type: "LineString", coordinates: allCoords }, properties: {} };

    showGeoJSON(map, trajRef, geojson);
  } catch (_) {
    // Trajectory fetch failed; leave map as-is.
  }
}

const root = document.querySelector("[data-ogc-root]");
mountOgcBrowser({
  root,
  basePath: "/movingfeatures",
  adapter: makeItemsAdapter({
    basePath: "/movingfeatures",
    idField: "id",
    detail,
  }),
});
