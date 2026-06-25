# OGC API Compliance Verification

This document describes how to verify OGC API compliance in deployed DynaStore
instances and provides reference endpoints for testing.

## OpenAPI Specification Endpoints

DynaStore exposes OpenAPI specifications for each API scope. These specs declare
OGC API conformance classes and can be used to verify standards compliance.

### Development Environment

The development deployment (`data.review.fao.org/geospatial/dev`) provides two
API scopes:

**Catalog API** — `https://data.review.fao.org/geospatial/dev/api/catalog/openapi.json`

OGC API extensions exposed:
- **OGC API - Features** (`/features`) — WFS 3.0 core, transactions, CQL2
- **OGC API - Records** (`/records`) — Core, OAS 3.0/3.1, GeoJSON
- **OGC API - Processes** (`/processes`) — Part 1 Core, sync + async dispatch
- **OGC API - STAC** (`/stac`) — SpatioTemporal Asset Catalog
- **OGC API - 3D GeoVolumes** (`/volumes`) — Draft core, 3DTiles, tileset
- **OGC API - Joins** (`/join`) — Draft core, cross-dataset joins
- **OGC Dimensions** (`/dimensions`) — Temporal/vertical dimension discovery

**Maps API** — `https://data.review.fao.org/geospatial/dev/api/maps/openapi.json`

OGC API extensions exposed:
- **OGC API - Maps (WMS)** (`/maps`) — Core, dataset-map, styled-map, PNG/JPEG/TIFF
- **OGC API - Tiles** (`/tiles`) — MVT/PBF, TMS 2.0, two-level cache
- **OGC API - Coverages** (`/coverages`) — Core, GeoTIFF/NetCDF/Zarr/CoverageJSON
- **OGC API - Styles** (`/styles`) — Core, MapboxGL, SLD-1.0/1.1
- **OGC API - Features** (`/features`) — Shared with catalog scope
- **OGC API - Processes** (`/processes`) — Shared with catalog scope
- **OGC API - Joins** (`/join`) — Shared with catalog scope
- **OGC WFS 2.0** (`/wfs`) — Legacy WFS 2.0 SOAP/XML endpoint

## Conformance Endpoints

Each OGC API extension exposes a `/conformance` endpoint that returns the
standard `Conformance` object declaring compliance classes.

### Example: Verify Maps Conformance

```bash
curl https://data.review.fao.org/geospatial/dev/api/maps/maps/conformance
```

Expected response includes conformance URIs for OGC API - Maps:

```json
{
  "conformsTo": [
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/dataset-map",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/collection-map",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/styled-map",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tiff",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/scaling",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/background",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/spatial-subsetting"
  ]
}
```

### Available Conformance Endpoints

**Catalog API scope:**
- `/features/conformance` — OGC API - Features
- `/records/conformance` — OGC API - Records
- `/processes/conformance` — OGC API - Processes
- `/stac/conformance` — STAC API
- `/dimensions/conformance` — OGC Dimensions

**Maps API scope:**
- `/maps/conformance` — OGC API - Maps
- `/tiles/conformance` — OGC API - Tiles
- `/coverages/conformance` — OGC API - Coverages
- `/styles/conformance` — OGC API - Styles
- `/features/conformance` — OGC API - Features

## Verification Workflow

1. **Retrieve OpenAPI spec** for the API scope of interest
2. **Check OGC tags** — all OGC API endpoints are tagged with `OGC API - {Name}`
3. **Call `/conformance`** endpoint for each extension to retrieve conformance URIs
4. **Validate against OGC spec** — verify URIs match expected conformance classes

## Implementation Details

All OGC API extensions follow the same architectural pattern:

```python
class MapsService(ExtensionProtocol, OGCServiceMixin):
    conformance_uris = OGC_API_MAPS_URIS  # List of conformance class URIs
    prefix = "/maps"
    router = APIRouter(tags=["OGC API - Maps (WMS)"], prefix="/maps")
    
    @router.get("/conformance")
    async def get_conformance() -> Conformance:
        return Conformance(conformsTo=self.conformance_uris)
```

The `OGCServiceMixin` provides the base conformance handler, and each
extension declares its specific conformance URIs as a class attribute.
Conformance URIs are defined in the OGC API specifications and referenced
by DynaStore extensions.

## Reference

- **OGC API Standards:** https://ogcapi.ogc.org/
- **OGC API - Maps Part 1:** https://docs.ogc.org/is/15-084r6/15-084r6.html
- **OGC API - Features Part 1:** https://docs.ogc.org/is/17-069r4/17-069r4.html
- **OGC API - Tiles:** https://docs.ogc.org/is/20-057/20-057.html
- **DynaStore OGC Extensions Map:** [../components/ogc-extensions.md](../components/ogc-extensions.md)
