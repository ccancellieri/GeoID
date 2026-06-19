# Sidecar Configurations

Sidecars are typed off-hub tables attached to a Postgres-backed items
driver.  They store domain-specific data (geometry, attributes, item
metadata, STAC overlay) keyed by `geoid`, foreign-keyed back to the
hub.  Each sidecar carries its own DDL, hash columns, and write
policy — operators compose them onto a driver via
`ItemsPostgresqlDriverConfig.sidecars`.

This page catalogs the four shipped sidecars with their **full default
field surface**.  Use it as a copy-paste starting point: every field
shown is the value you'll see in `GET /configs/...` if no override has
been applied.  Override the fields you care about; leave the rest as
the defaults.

## Architecture

```
ItemsPostgresqlDriverConfig
    └── sidecars: List[SidecarConfig]   ← discriminated union
            ├── GeometriesSidecarConfig          (sidecar_type="geometries")
            ├── FeatureAttributeSidecarConfig    (sidecar_type="attributes")
            ├── ItemMetadataSidecarConfig        (sidecar_type="item_metadata")
            └── StacItemsSidecarConfig           (sidecar_type="stac_metadata")
```

Every sidecar inherits two universal fields from `SidecarConfig`:

- `sidecar_type: Literal["..."]` — discriminator (also the wire key the
  registry-resolved union uses to pick the concrete subclass during
  PATCH validation).
- `enabled: bool = True` — flip to `False` to keep the row in the
  `sidecars` list (preserves operator intent + DDL state) but skip
  reads + writes against the sidecar table.
- `indexing: Optional[SidecarIndexingPolicy] = None` — per-sidecar
  override on what to index in OpenSearch when this driver routes
  through ES.  `None` means "follow the driver-level indexing
  defaults."

Sidecars register themselves with `SidecarConfigRegistry` at import
time; the registry-resolved discriminated union on
`ItemsPostgresqlDriverConfig.sidecars` means a new sidecar lands
without touching the driver-config Union — just declare + register.

## Geometries (`sidecar_type="geometries"`)

Owns `{schema}.{table}_geometries`.  One row per item, FK to hub on
`geoid`.  Stores `geom` (and optional `bbox_geom`), STORED GENERATED
`geohash CHAR(N)` from `ST_GeoHash(geom, N)`, and a STORED GENERATED
`geometry_hash CHAR(64)` (SHA256 of `ST_AsBinary(geom)`) consumed by
the `IdentityMatcher.GEOMETRY_HASH` identity matcher (an
`IdentityRule(match_on=["geometry_hash"], on_match="refuse_return")`
on `ItemsWritePolicy.identity`).

Attached by default for VECTOR + RASTER collections; not attached for
RECORDS (no spatial component).

```jsonc
{
  "sidecar_type": "geometries",
  "enabled": true,
  "indexing": null,

  // Geometry storage
  "target_srid": 4326,                                  // target SRID for stored geometry
  "target_dimension": "force_2d",                       // force_2d | force_3dz | force_3dm | force_4d | preserve
  "geom_column": "geom",                                // main geometry column
  "bbox_column": "bbox_geom",                           // separate bbox column; set null to disable

  // Processing policies
  "invalid_geom_policy": "attempt_fix",                 // attempt_fix | reject | accept
  "allowed_geometry_types": [],                         // empty = all types allowed
  "srid_mismatch_policy": "transform",                  // transform | reject | reproject
  "simplification_algorithm": null,                     // null | douglas_peucker | visvalingam_whyatt
  "simplification_tolerance": null,                     // float; required when simplification_algorithm set
  "remove_redundant_vertices": false,

  // Spatial indexes
  "h3_resolutions": [],                                 // 0..15 — H3 cells indexed via STORED columns
  "s2_resolutions": [],                                 // 0..30 — S2 cells indexed via STORED columns
  "geohash_precision": 8,                               // 1..12; STORED `geohash CHAR(N)` from ST_GeoHash

  // Partitioning
  "partition_strategy": null,                           // null | h3 | s2 | geohash | range
  "partition_resolution": 0,                            // resolution / precision when partitioning is on

  // Statistics block (per-row stored columns + indexes)
  "statistics": {
    "enabled": true,
    "storage_mode": "columnar",                         // columnar (typed columns) | jsonb
    "area":   {"enabled": true, "index": true},
    "volume": {"enabled": true, "index": true},
    "length": {"enabled": true, "index": true},
    "centroid_type": "geometric",                       // geometric | weighted | population
    "index_centroid": true,
    "morphological_indices": {},                        // {indicator: {enabled, index}}
    "vertex_count": {"enabled": false, "index": false},
    "hole_count":   {"enabled": false, "index": false},
    "create_gin_index": false                           // GIN on the stats JSONB (legacy)
  },

  // 3D place statistics (population/density attached to centroid) are no
  // longer a sidecar block — declare them on items_write_policy.compute
  // via ComputedField / ComputedKind.

  // Hub-level mirroring
  "store_bbox": true,                                   // mirror bbox_geom on hub for fast filters
  "store_centroid": false
}
```

> User-supplied feature schema lives on `ItemsWritePolicy.schema`
> (collection-tier), not on the sidecar.

## Feature Attributes (`sidecar_type="attributes"`)

Owns `{schema}.{table}_attributes`.  Stores user-supplied feature
properties off-hub: typed columns when `ItemsWritePolicy.schema` is
declared, or a JSONB blob when `storage_mode="automatic"`.  Drives
the `external_id` ↔ `geoid` mapping and the `attributes_hash`
write-policy gate (SHA256 of canonicalised attributes JSON).

```jsonc
{
  "sidecar_type": "attributes",
  "enabled": true,
  "indexing": null,

  // Storage strategy
  "storage_mode": "automatic",                          // automatic (jsonb) | typed (columns from schema) | hybrid
  "storage_only_fields": [],                            // fields persisted but not indexed

  // External ID handling
  "external_id_field": "external_id",                   // storage column name; null disables
  "index_external_id": true,
  "expose_geoid": false,                                // surface internal geoid in API responses

  // Asset linkage
  "asset_id_field": "asset_id",                         // storage column name; null disables
  "index_asset_id": true,

  // Attribute schema (drives indexed JSONB paths)
  "attribute_schema": null,
  "jsonb_column_name": "attributes",
  "use_hot_updates": true,                              // PG HOT updates on attribute-only edits
  "jsonb_indexed_paths": {},                            // {path: index_kind} — path-targeted GIN/B-tree on JSONB

  // Partitioning
  "partition_strategy": null,                           // null | range | hash | list
  "partition_attribute": null
}
```

## Item Metadata (`sidecar_type="item_metadata"`)

Owns `{schema}.{table}_item_metadata`.  Stores hub-internal lifecycle
+ provenance metadata (created_at / updated_at / source_uri / etc.)
that operators rarely need to tune.  The default surface is
intentionally minimal:

```jsonc
{
  "sidecar_type": "item_metadata",
  "enabled": true,
  "indexing": null
}
```

The metadata schema itself is hub-controlled — the sidecar exists so
operators can flip `enabled=false` (skip the metadata table entirely
for ephemeral collections) or tune the ES indexing surface via
`indexing`.

## STAC Items Metadata (`sidecar_type="stac_metadata"`)

Owns `{schema}.{table}_stac_metadata`.  JSONB columns persisting
externally-supplied STAC content (`external_extensions`,
`external_assets`, `extra_fields`).  Lives in `extensions/stac/`
rather than `pg_sidecars/` because STAC is an extension, not a
core hub concern — the registry-resolved discriminated union means
the sidecar still composes onto `ItemsPostgresqlDriverConfig.sidecars`
without core-side imports.

```jsonc
{
  "sidecar_type": "stac_metadata",
  "enabled": true,
  "indexing": null
}
```

The STAC sidecar's payload schema is controlled by the
`StacPluginConfig` extension (the `auto_render_extensions` allowlist
gates which sidecar fields surface verbatim on read vs. require
explicit driver-side rendering).  See
`docs/components/stac.md` for the rendering split.

## Composing sidecars onto a driver

`ItemsPostgresqlDriverConfig.sidecars` is a discriminated-union list.
Operators PATCH the full list (RFC 7396 merge-patch semantics: list
fields replace wholesale, not merge):

```bash
curl -X PATCH /configs/catalogs/{cat}/collections/{coll}/plugins/items_postgresql_driver \
    -H 'Authorization: Bearer <token>' \
    -H 'Content-Type: application/merge-patch+json' \
    -d '{
        "sidecars": [
            {"sidecar_type": "geometries",  "geohash_precision": 6, "store_bbox": true},
            {"sidecar_type": "attributes"},
            {"sidecar_type": "item_metadata"},
            {"sidecar_type": "stac_metadata"}
        ]
    }'
```

`external_id_field`, `require_external_id`, and `enable_validity` are
collection-tier policy knobs on `ItemsWritePolicy`, not sidecar fields.
PATCH `/configs/.../plugins/items_write_policy` to set them.

GET round-trips the slim form back as the full default surface
(every default field visible) so operators can copy-paste-modify a
single field instead of remembering which knobs exist.

### Disabling a sidecar without dropping it

Flip `enabled=false`:

```jsonc
{"sidecar_type": "geometries", "enabled": false, "...": "..."}
```

The sidecar row stays in the list — DDL state preserved, intent
preserved — but reads + writes skip it.  Useful for ephemeral
maintenance windows where geometry indexing is paused without
losing the `geohash_precision`, `h3_resolutions`, etc. operator
overrides.

### Removing a sidecar entirely

PATCH the list without the entry.  The next driver write reconciles
DDL — the sidecar's DDL is dropped and the table goes away.  This is
destructive: feature data persisted by that sidecar is lost.

## Composition with engines

Sidecars attach to `ItemsPostgresqlDriverConfig` (and only to that —
ES drivers have their own sidecar surface modeled differently).  They
ride the driver's `engine_ref` to whichever `PostgresqlEngineConfig`
the driver references; sidecars do not declare their own engine
binding.

When multi-instance engine refs are supported (planned), a single PG
engine can back multiple driver instances with different sidecar
compositions.  Currently every PG-backed collection uses the one driver
instance and its declared sidecars.

## See also

- `docs/components/storage_drivers.md` — driver classes, routing,
  hint-based dispatch
- `docs/components/items_schema.md` — the driver-agnostic field
  declaration these sidecars physically realize (the write-side bridge
  promotes its fields to COLUMNAR columns)
- `docs/components/platform_engines.md` — engine layer + lifecycle
  policy
- `src/dynastore/modules/storage/drivers/pg_sidecars/` — sidecar
  source (geometries, attributes, item_metadata, base + registry)
- `src/dynastore/extensions/stac/stac_metadata_config.py` — STAC
  sidecar (lives outside `pg_sidecars/` by design — extension, not
  core)
- `docs/components/stac.md` — STAC overlay rendering + the
  `auto_render_extensions` allowlist
- `docs/components/configs_api.md` — configs API surface (GET/PATCH
  endpoints, query params, HATEOAS link catalog, scope strictness
  rules); shows how sidecar overrides land via PATCH at the right
  scope
