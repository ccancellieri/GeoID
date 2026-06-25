# The EDR Extension

## Implementation Status

**OGC Conformance**: Part 1 v1.1

**Status**: ⚠️ Partial Implementation

**Known Gaps**:
- [Vertical level (z) support](https://github.com/un-fao/GeoID/issues/2399) - Parameter accepted but ignored
- [Output CRS reprojection](https://github.com/un-fao/GeoID/issues/2399) - Output in raster native CRS only
- [Locations endpoints](https://github.com/un-fao/GeoID/issues/XXXX) - Stub implementations (empty list, 404)

**Last Updated**: 2025-06-26

The `edr` extension implements **OGC API – Environmental Data Retrieval (EDR)
Part 1 v1.1** for DynaStore. It exposes raster data (GeoTIFF, COG, or any
GDAL-readable format stored as STAC item assets) through point, area, and
bounding-box query patterns, returning results as CoverageJSON or GeoJSON.

---

## URL structure

```
/edr/                                                               landing page
/edr/conformance                                                    OGC conformance
/edr/catalogs/{catalog_id}/collections                              list EDR collections
/edr/catalogs/{catalog_id}/collections/{collection_id}              collection metadata
/edr/catalogs/{catalog_id}/collections/{collection_id}/position     point extraction
/edr/catalogs/{catalog_id}/collections/{collection_id}/area         polygon extraction
/edr/catalogs/{catalog_id}/collections/{collection_id}/cube         bbox extraction
/edr/catalogs/{catalog_id}/collections/{collection_id}/locations    named locations (stub)
/edr/catalogs/{catalog_id}/collections/{collection_id}/locations/{id}  always 404
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core
http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/collections
http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/json
http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/edr-geojson
http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/queries
```

---

## Request parameters

| Parameter | Query types | Description |
|---|---|---|
| `coords` | position, area | WKT geometry: `POINT(lon lat)` or `POLYGON(...)` |
| `bbox` | cube | `minlon,minlat,maxlon,maxlat` (extra ordinates ignored) |
| `datetime` | all | ISO 8601 instant or interval (`2024-01-01/2024-12-31`, `../end`, `start/..`) |
| `parameter-name` | all | Comma-separated band names to include |
| `f` | all | `CoverageJSON` (default) or `GeoJSON` (position only) |
| `z` | all | Vertical level — **accepted but ignored** |
| `crs` | all | Output CRS URI — **accepted but ignored** (output is in raster native CRS) |

`EDRConfig` (per-catalog/per-collection):

```python
max_items_per_query: int = 100   # declared; hardcoded limit=1 is used in practice
request_deadline_soft_s: int = 60  # declared; not yet enforced
request_deadline_hard_s: int = 120 # declared; not yet enforced
```

---

## Request pipeline

### Position query

```
GET /edr/.../position?coords=POINT(lon lat)&datetime=2024-01-01T00:00:00Z
        │
        ├─ 1. parse_wkt_point(coords) → (lon, lat)
        ├─ 2. parse_datetime_param(datetime) → (start, end)
        ├─ 3. CatalogsProtocol.search_items(limit=1, datetime filters)
        │     → first matching STAC item
        ├─ 4. Resolve raster asset href ("data" or "coverage" key preferred)
        ├─ 5. extract_point_values(href, lon, lat)
        │     → gdal.service.open_raster_vsi(href)
        │     → coverages.window.resolve_window(SubsetRequest)
        │     → ds.read(band, window) per band
        └─ 6. write_position_coveragejson() or write_position_geojson()
              → StreamingResponse
```

### Area and cube queries

Same flow as position but:
- **area**: `parse_wkt_polygon_bbox()` reduces polygon to its bbox (non-rectangular
  AOIs are not clipped). `extract_area_values()` reads a 2-D window.
  Output: CoverageJSON `Grid` domain.
- **cube**: `parse_cube_bbox()` parses `minlon,minlat,maxlon,maxlat`.
  Delegates to same area extraction path.

Key files:

| File | Responsibility |
|---|---|
| `extensions/edr/edr_service.py` | FastAPI router, orchestration (priority 165) |
| `extensions/edr/config.py` | `EDRConfig` runtime knobs |
| `extensions/edr/edr_models.py` | `EDRLandingPage`, `EDRCollection`, `EDRExtent`, `EDRLocations` |
| `modules/edr/collection_metadata.py` | STAC collection → EDR metadata transform |
| `modules/edr/parameter_metadata.py` | Extract band names/units from `raster:bands` STAC metadata |
| `modules/edr/temporal.py` | `parse_datetime_param()` — instant + interval handling |
| `modules/edr/query_handlers/position.py` | WKT point parse + 1-pixel GDAL read |
| `modules/edr/query_handlers/area.py` | WKT polygon bbox + windowed GDAL read |
| `modules/edr/query_handlers/cube.py` | Bbox string parse |
| `modules/edr/output/coveragejson.py` | CoverageJSON `Point` and `Grid` generators |
| `modules/edr/output/geojson.py` | GeoJSON point feature generator |

---

## Output formats

| Format | Media type | Supported by |
|---|---|---|
| `CoverageJSON` | `application/prs.coverage+json` | position, area, cube |
| `GeoJSON` | `application/geo+json` | position only |

Requesting GeoJSON on area or cube returns HTTP 415.

The EDR collection metadata always advertises `output_formats: ["CoverageJSON",
"GeoJSON"]` and `crs: ["CRS84"]` regardless of the actual raster CRS.

---

## Dependencies

| Package | Source | Purpose |
|---|---|---|
| `rasterio` | `modules/gdal` | Windowed raster read |
| `GDAL` | `modules/gdal/service.py` | VSI remote raster open (`open_raster_vsi`) |

The EDR extension reuses `modules/coverages/` helpers (`subset`, `window`,
`reader`) for window resolution and chunked band iteration.

---

## Known limitations

1. **Locations endpoints are stubs** — `list_locations` always returns an empty
   `FeatureCollection`; `get_location` always 404s. Named-location support
   requires explicit metadata not yet defined.
2. **`z` (vertical level) not implemented** — vertical subsetting is declared
   but ignored; the `coverages.subset` layer supports it but is not wired up.
3. **`crs` (output CRS) not implemented** — output is always in the raster
   native CRS; no reprojection is applied.
4. **Deadline config fields not enforced** — `request_deadline_soft_s` and
   `request_deadline_hard_s` in `EDRConfig` have no effect.
5. **Area query uses polygon bbox only** — `parse_wkt_polygon_bbox` reduces any
   polygon to its bounding box; non-rectangular AOIs are not clipped.
6. **Single item lookup** — parameter metadata is derived from `limit=1`; other
   items in the collection may have different band configurations.
7. **Conformance URI scheme** — EDR uses `http://` while DGGS uses `https://`
   for `opengis.net` URIs; worth standardising across extensions.
