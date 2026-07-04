# The WFS Extension

While modern clients are shifting towards OGC API and STAC, a vast ecosystem of established desktop GIS clients (e.g., QGIS, ArcGIS, etc.) rely on the OGC Web Feature Service (WFS) standard. The `wfs` extension serves as a critical legacy bridge to this ecosystem, ensuring that data is accessible natively.

## The Dual-Endpoint Strategy
The WFS 2.0 standard requires Key-Value Pair (KVP) queries spanning `Capabilities`, `FeatureTypes`, and raw `GetFeature` queries.

We implement dual routing to bridge this RESTfully:
- `GET /wfs`: Root global query (Discovers everything)
- `GET /wfs/catalogs/{catalog_id}`: Scoped isolating a tenant into a specific virtual endpoint (`/wfs/{catalog_id}` is kept as a deprecated alias for existing WFS clients).

## Dynamic XML via `wfs_generator`
We utilize `xml.etree.ElementTree` to synthesize XSD architectures directly dynamically at request time strictly bypassing physical static schema maps.

### `DescribeFeatureType` Strategy
Because we lean heavily on `JSONB` parameter mappings, classical strongly typed schemas don't exist natively.
1. The engine checks standard fixed properties resolving SQL types (`valid_from` -> `dateTime`).
2. It fetches single row snapshots evaluating JSON structures generating derived types for flexible attributes. 
3. Outputs dynamically generated `xsd:schema` logic to satisfy rigid GIS platforms.

## PostGIS `ST_AsGML()` Render Pipeline
Executing `GetFeature` XML creation natively is notoriously CPU expensive. 
We sidestep this utilizing raw C-level PostGIS architecture.

Instead of extracting `WKB` geometry out of the database and marshaling it, the SQL explicitly commands `ST_AsGML(geom)`. 
1. Database handles memory limits rendering text directly.
2. The core python returns strings mapping them linearly directly into the larger wrapper text payload minimizing all marshalling boundaries resulting in a significantly faster endpoint.
