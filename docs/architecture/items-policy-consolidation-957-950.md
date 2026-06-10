# Items policy consolidation — single SSOT for write/read shape, identity, and derived values

**Date:** 2026-05-19
**Status:** SHIPPED — phases 1–4 landed via PR #961 (`fa686764`); axis-2 follow-ups landed via PR #982 (#976 `feature_type_schema`), PR #987 (#977 `external_id_as_feature_id`), PR #991 (#974 `enable_validity`). Remaining sidecar residue (`compute_geometry_statistics`, h3/s2/`geohash_precision` relocation) tracked under #978.
**Scope:** `ItemsWritePolicy`, new `ItemsReadPolicy`, removal of dead `WritePolicyDefaults` + `ItemsSchema.constraints` family, removal of sidecar-side spatial/statistics duplication. Touches `packages/core/src/dynastore/modules/storage/driver_config.py`, `packages/core/src/dynastore/modules/storage/schema_types.py`, `packages/core/src/dynastore/modules/storage/drivers/pg_sidecars/*_config.py`, `packages/core/src/dynastore/tools/geospatial.py`, the four item drivers (PG, ES, DuckDB, Iceberg), and the cross-sidecar `get_feature_type_schema()` aggregation pipeline.
**Tracking issues:** #950 (ItemsReadPolicy SSOT — closed by #961 phase 3 + #987), #957 (sidecar residue after #940/#943 — closed by #961/#982/#987/#991; remaining items tracked under #974/#977/#978), #978 (Phase 5d follow-up).

> **Reader note (post-merge):** the body below is preserved as design history.
> It describes the intended end-state at the time of the consolidation, not
> the live code. For the live shape see `ItemsWritePolicy` / `ItemsReadPolicy`
> in `packages/core/src/dynastore/modules/storage/driver_config.py` and the
> follow-up PRs listed above.

> **STAC out of scope.** This policy governs data-oriented protocols (OGC API
> Features, MVT tiles, EDR, Coverages). STAC items bypass `feature_type.expose`
> entirely — their input shape passes straight through, because STAC items are
> metadata documents, not properties-projected features.

## Problem

The collection-tier write/read shape is currently split across **five** config surfaces, three of which are dead, all of which carry partial copies of the same concepts:

1. `ItemsWritePolicy` (`driver_config.py:298`) — active SSOT after #940/#943. Carries `external_id_field`, `require_external_id`, `enable_validity`, `geometries` write behaviour, `identity_matchers: List[IdentityMatcher]` (OR-only flat list), `geohash_precision: int` (one of many cell systems), `matcher_actions: Dict[IdentityMatcher, WriteConflictPolicy]`.
2. `WritePolicyDefaults` (`driver_config.py:557`) — declared as the "M8 posture-only" successor, **zero call-sites in the repo** beyond docstrings and module exports.
3. `ItemsSchema.constraints` with `IdentityKeyConstraint` / `ValidityConstraint` / `GeometryHashConstraint` (`schema_types.py:98`) — declared as the field-binding successor; never consumed. `IdentityKeyConstraint.geohash_precision` is the third copy of the same value.
4. `FeatureAttributeSidecarConfig` / `GeometriesSidecarConfig` (PG sidecars) — carry `feature_type_schema` (twice), `h3_resolutions: List[int]`, `s2_resolutions: List[int]`, `geohash_precision`, `enable_validity` (driver-divergent vs the policy field), `external_id_as_feature_id`, plus `statistics` and `place_statistics` blocks. The schema fragments are aggregated by `SidecarProtocol.get_feature_type_schema()` → `QueryOptimizer.get_feature_type_schema()` → `item_service.py:1557` because the wire shape was split between sidecars.
5. **No read-side policy exists at all.** Identity-on-read, output transformer chain, and the wire-shape contract for responses have no declarative home. `PrivateEntityTransformer` (`drivers/elasticsearch_private/transformer.py:42`) is registered standalone via the catalog routing template (#733/PR #906) — there is no policy declaring "this collection produces this read shape".

The result:

- The same per-collection answer ("what's the identity, what's the wire shape, what gets computed") is reachable from three layers, only one of which is live. New contributors waste time figuring out which is canonical.
- Spatial-cell support (H3/S2/geohash) is PG-only by accident: the only driver that reads from `GeometriesSidecarConfig` is PG. ES, DuckDB and Iceberg cannot compute these even though the geometry input is identical.
- "Duplicate feature where geometry AND attributes both match" is not expressible. `identity_matchers` is a flat OR-list (first match wins); there is no AND-composition.
- `enable_validity` is a latent driver-divergence bug: PG reads `attr_sidecar.enable_validity` (`postgresql.py:982`); DuckDB and Iceberg read `policy.enable_validity` (`duckdb.py:447`, `iceberg.py:498`).

## Goals

- One config tree per concern. No silent duplication, no "posture vs binding" split.
- All identity strategies (path-extracted, hash, spatial-cell) expressible under one model, composable via AND within a rule and OR across rules.
- All derived values (statistics, spatial keys, content hashes) declared once. Drivers consult that list and each materialises it however suits the store.
- Wire shape declared once. `items_schema` is the single source of truth; the wire JSON-Schema is **derived** from it (never authored) and the read shape derives from the same source. The read policy declares only additional exposed computed fields (`feature_type.expose`). Transformer wiring is not a read-policy concern — it lives on the routing config (#950).
- Clean break per the no-backcompat invariant (#950 comment 4482694621): no "translate old shape" code paths, destructive schema migration, Pydantic field defaults supply the empty-state ergonomics.

## Non-goals

- Asset-side write policy (`AssetWritePolicy` / `AssetWritePolicyDefaults` in `modules/catalog/write_policy_assets.py`). Untouched here.
- PG sidecar internals (sidecars stay PG-driver-internal serialization, not a public extension surface; see the long-standing distinction between sidecars and `EntityTransformProtocol`).
- DDL-shape decisions on PG (column names, JSONB vs relational, partitioning, indexed_paths, GiST/GIN choice, GIN column name). These stay on the sidecar — `ItemsWritePolicy` only declares **what** to compute; the sidecar decides **how** PG stores and indexes it.
- 3-layer waterfall for these policies. `ItemsWritePolicy` and `ItemsReadPolicy` are collection-scoped only.

## Design

### Models

```python
class ComputedKind(StrEnum):
    # Path-extracted (uses ItemsWritePolicy.external_id_field)
    EXTERNAL_ID = "external_id"
    # Content fingerprints
    GEOMETRY_HASH = "geometry_hash"
    ATTRIBUTES_HASH = "attributes_hash"
    # Spatial cell keys (require resolution)
    GEOHASH = "geohash"       # 1..12
    H3 = "h3"                 # 0..15
    S2 = "s2"                 # 0..30
    # Statistics
    AREA = "area"
    PERIMETER = "perimeter"
    LENGTH = "length"
    CENTROID = "centroid"
    BBOX = "bbox"
    VERTEX_COUNT = "vertex_count"
    HOLE_COUNT = "hole_count"
    VOLUME = "volume"                # 3D-closed types only
    # Morphological indices (2D shape descriptors)
    CIRCULARITY = "circularity"      # (4·π·area) / perimeter²
    CONVEXITY = "convexity"          # area / convex_hull.area
    ASPECT_RATIO = "aspect_ratio"    # bbox width / bbox height
    # SPHERICITY, FLATNESS — 3D mesh metrics, deferred (see geometry_stats.py TODO)

class ComputedField(BaseModel):                # ENGINE value type (internal)
    kind: ComputedKind
    resolution: Optional[int] = None  # required iff kind ∈ {geohash, h3, s2}
    name: Optional[str] = None        # column/field name; default = kind[_resolution]

# AUTHORING layer: homogeneous buckets, no per-kind null noise. DeriveSpec is
# what an operator authors and what the registry JSON-Schema advertises; the
# engine flattens it to List[ComputedField] via DeriveSpec.to_computed_fields().
class SpatialCell(BaseModel):
    grid: Literal["geohash", "h3", "s2"]; resolution: int; name: Optional[str] = None
class GeometryStat(BaseModel):
    stat: ComputedKind                              # geometry/place stat kinds only
    store: Optional[StatisticStorageMode] = None    # jsonb | columnar | None (compute-only)
    indexed: bool = False
    type: Optional[Literal["POINT", "POINTZ"]] = None  # centroid only
    name: Optional[str] = None
class AttributeStat(BaseModel):
    source: str                                     # dotted property path
    store: Optional[StatisticStorageMode] = None; indexed: bool = False; name: Optional[str] = None
class DeriveSpec(BaseModel):
    external_id: Optional[str] = None               # dotted source path (or None)
    content_hashes: List[Literal["geometry", "attributes"]] = []
    spatial_cells: List[SpatialCell] = []
    geometry_stats: List[GeometryStat] = []
    attribute_stats: List[AttributeStat] = []

class IdentityRule(BaseModel):
    match_on: List[str]                            # names referencing derive outputs (AND within rule)
    on_match: Optional[WriteConflictPolicy] = None # overrides policy.on_conflict

class ItemsWritePolicy(PluginConfig):
    _visibility: ClassVar[Optional[str]] = "collection"

    on_conflict: WriteConflictPolicy = UPDATE
    on_asset_conflict: Optional[AssetConflictPolicy] = None

    enable_validity: bool = False
    validity_field: str = "valid_from"

    schema: Optional[Dict[str, Any]] = None  # self-contained JSON Schema (see below); write-time validation

    # Derivations are authored as homogeneous buckets (DeriveSpec); the flat
    # engine list is the read-only `compute` property (DeriveSpec.to_computed_fields()).
    # The external_id source path lives on the bucket:
    #   derive=DeriveSpec(external_id="properties.adm2_pcode")
    # "required external_id" is expressed via the JSON Schema's `required`
    # list — no separate require_external_id boolean needed.
    derive: DeriveSpec = Field(default_factory=DeriveSpec)
    identity: List[IdentityRule] = [IdentityRule(match_on=["external_id"])]
    geometries: GeometriesWriteBehavior = Field(default_factory=GeometriesWriteBehavior)

class FeatureType(BaseModel):
    # Trinary projection control for data-oriented protocols (Features, MVT, EDR,
    # Coverages). STAC items bypass this — their input shape passes through.
    #   None (default)  — surface ALL ItemsSchema.fields declared property fields.
    #                     Wire shape mirrors the write schema.
    #   []  (empty)     — surface NO properties (geometry-only MVT; empty
    #                     `properties` on /items).
    #   ["x","y"]       — schema-declared fields PLUS the listed
    #                     ComputedField.resolved_name values from
    #                     ItemsWritePolicy.compute. Listing is ADDITIVE to the
    #                     schema baseline, never subtractive.
    expose: Optional[List[str]] = None
    failure_mode: Literal["strict", "best_effort"] = "best_effort"
    external_id_as_feature_id: bool = True

class ItemsReadPolicy(PluginConfig):
    _visibility: ClassVar[Optional[str]] = "collection"

    feature_type: FeatureType = Field(default_factory=FeatureType)
    # No transformer chain here — transformer wiring lives on the routing
    # config (OperationDriverEntry.output_transformers), per #950.
```

> **#1065 refinement (implemented).** `schema_ref` and `ItemsReadPolicy.output_transformers`
> were dropped. The wire JSON-Schema is derived from `items_schema` unconditionally (no
> selector), and the transformer chain belongs to the routing config (#950). `items.compute`
> additionally accepts a **preset name** (e.g. `"geometry_stats"`) that expands to a deduped
> `List[ComputedField]`.

### Schema is derived from `items_schema` (#1065)

`items_schema` (the collection's `FieldDefinition` map) is the single source of truth for the wire shape. The wire JSON-Schema (`ItemsWritePolicy.schema`) is **derived** from it (`derive_wire_schema` → `ItemService.get_collection_schema`) and is read-only: authoring a non-null `schema` is rejected at config-save. Each `FieldDefinition` carries:

- `data_type` (mapped to a JSON-Schema `type`/`format`),
- validators (`max_length` / `minimum` / `maximum` / `enum` / `pattern` / `format`),
- `required` (drives the schema's `required` list),
- optional `materialize` (None = driver decides from capabilities; True = native column; False = JSONB).

The Admin UI form, the OpenAPI body schema for `POST /items`, and write-time value validation all consult this one derived object. Computed fields (`policy.compute` + `feature_type.expose`) surface as additional read-only properties under their `resolved_name` — they need not appear in `items_schema` to appear in responses.

A collection with no `items_schema` fields is permissive (a blob: no derived schema, no validation). `strict_unknown_fields` is the operator's lever for strict ingest (`additionalProperties: false`).

### Semantics

**Write path** — for an incoming feature:

1. Apply `geometries` behaviour (SRID transform, invalid fix/reject, allowed-type check, simplification, vertex normalisation). Failure paths raise the matching exception in `tools.geospatial`.
2. Validate `properties` against `schema` if set. JSON-Schema failure → 422 with field-level violations. `additionalProperties: false` is honoured.
3. Compute the union set `U = {policy.compute} ∪ {key for rule in policy.identity for key in rule.match_on}`. Drop `EXTERNAL_ID` (path-extracted, not derived). Pass `U` to `tools.geospatial.compute_derived_fields(feature, U)`, which returns a dict keyed by `ComputedField.resolved_name()` (i.e. `name` override, falling back to `kind` or `kind_resolution`). This dict travels alongside the feature through the write pipeline; each driver decides materialisation.
4. Resolve identity. Walk `policy.identity` in order. For each rule, compute the conjunction key from the row's computed values and probe the store. First rule whose conjunction matches an existing row wins. If the rule has `on_match`, that overrides `policy.on_conflict`. The skip-if-unchanged-geometry-hash flag, when set, degrades `NEW_VERSION` to no-op and `UPDATE` to `REFUSE_RETURN` when the matched row's `geometry_hash` equals the incoming.

**Read path** — for an outgoing feature:

1. Driver returns the row plus its full set of materialised computed values.
2. The response `properties` derive from `items_schema` (the declared fields). There is no `schema_ref` selector — the read shape always tracks the same source of truth as the write shape.
3. Project the response `properties` according to `feature_type.expose`:
   - `expose=None` (default) — surface all `ItemsSchema.fields` declared property fields; no computed values merged.
   - `expose=[]` — suppress both schema fields and computed values; response `properties` is empty (geometry-only MVT, empty `properties` on /items).
   - `expose=["x","y"]` (non-empty list) — schema-declared fields still flow through as the baseline, **plus** the listed `ComputedField.resolved_name`s from `policy.compute` are merged in as additional read-only fields. The computed-value merge only runs in this case. The merge runs once at the read choke point (`SidecarProtocol.apply_exposed_computed_values`); each sidecar declares the names it can produce via `producible_computed_names` / `resolve_computed_value`. An exposed name with no produced value is skipped under `best_effort` and raises under `strict`.

   This projection applies to OGC API Features, MVT, EDR, and Coverages. STAC items bypass it entirely.
4. Transformer chains, when configured, run via the routing config's `output_transformers` (#950) — not the read policy. Read-side (output) transformers are honored only on Elasticsearch read paths by design (#1643); `output_transformers` declared on a SEARCH entry that resolves to a non-ES driver (PostgreSQL, DuckDB, Iceberg, BigQuery) will not fire and the routing-config validator emits a WARN.

### Driver responsibilities

| Driver | `compute` materialisation |
|---|---|
| PG | Each `ComputedField` resolves to a real column on the attributes/geometries sidecar; PG sidecar config decides DDL details (index type, partition role). |
| ES | Each `ComputedField` resolves to a numeric/object field in the index mapping. Spatial cells are stored as `keyword` (for exact match) or `long` (for range). |
| DuckDB | Stored as columns; the driver may choose any computed key as ZONE-MAP / sort key. |
| Iceberg | Stored as columns; spatial-cell columns are first-class partition-key candidates. |

The minimal driver contract gains one method: `materialize_computed(self, computed: Dict[str, Any], row) -> None`. Existing drivers already write the geohash/geometry_hash columns this way today — the change is making it iterate over `policy.compute` instead of hard-coding the list.

### What disappears

| Today | After |
|---|---|
| `IdentityMatcher` enum | replaced by `ComputedKind` |
| `ItemsWritePolicy.identity_matchers: List[IdentityMatcher]` | replaced by `identity: List[IdentityRule]` |
| `ItemsWritePolicy.matcher_actions: Dict[IdentityMatcher, WriteConflictPolicy]` | per-rule `on_match` |
| `ItemsWritePolicy.external_id_field: Optional[str]` | inline on `ComputedField(kind=EXTERNAL_ID, name="properties.X")` |
| `ItemsWritePolicy.require_external_id: bool` | use JSON Schema `required` on the external-id property |
| `ItemsWritePolicy.geohash_precision: int` | inline on `ComputedField(kind=GEOHASH, resolution=N)` |
| `WritePolicyDefaults` class (whole) | deleted, dead code |
| `IdentityKeyConstraint` / `ValidityConstraint` / `GeometryHashConstraint` | deleted, dead code |
| `IdentityKeyConstraint.geohash_precision: int` | deleted (third copy) |
| `GeometriesSidecarConfig.geohash_precision` | deleted, derived from `policy.compute` |
| `GeometriesSidecarConfig.h3_resolutions: List[int]` | deleted, derived from `policy.compute` |
| `GeometriesSidecarConfig.s2_resolutions: List[int]` | deleted, derived from `policy.compute` |
| `GeometriesSidecarConfig.statistics: GeometriesStatisticsConfig` | deleted, derived from `policy.compute` |
| `GeometriesSidecarConfig.place_statistics: PlaceStatisticsConfig` | deleted, derived from `policy.compute` |
| `GeometriesSidecarConfig.store_bbox` / `store_centroid` | deleted, declared via `compute` |
| `FeatureAttributeSidecarConfig.feature_type_schema` | deleted, `policy.schema` is SSOT |
| `GeometriesSidecarConfig.feature_type_schema` | deleted, `policy.schema` is SSOT |
| `FeatureAttributeSidecarConfig.external_id_as_feature_id` | moved to `ItemsReadPolicy.feature_type` |
| `FeatureAttributeSidecarConfig.enable_validity` | deleted (driver-divergent bug; policy is SSOT) |
| `SidecarProtocol.get_feature_type_schema()` aggregation | deleted, thin policy read replaces the chain |

### What sidecars keep

Strictly the DDL/PG-internal surface:

- `FeatureAttributeSidecarConfig`: `storage_mode`, `storage_only_fields`, `attribute_schema`, `jsonb_column_name`, `jsonb_indexed_paths`, `use_hot_updates`, `external_id_field`, `index_external_id`, `asset_id_field`, `index_asset_id`, `expose_geoid`, `partition_strategy`, `partition_attribute`.
- `GeometriesSidecarConfig`: `target_srid`, `target_dimension`, `geom_column`, `bbox_column`, `partition_strategy`, `partition_resolution`, plus a new minimal `index_hints: Dict[str, IndexHint]` keyed by the `ComputedField.resolved_name()` so an operator can say "BTREE the area_m2 column".

### Minimal example — empty policy

Most collections need nothing. With no policy set, defaults apply: identity falls back to `external_id` keyed at `properties.external_id`, no computed fields, no schema validation, conflict policy = `UPDATE`. Equivalent to the implicit default. JSON: `{}`.

### Minimal example — typical collection

External-id identity at a custom path, schema-validated properties, no derived columns:

```json
{
  "items_write_policy": {
    "schema": {
      "type": "object",
      "required": ["code", "name"],
      "additionalProperties": false,
      "properties": {
        "code": { "type": "string", "description": "Stable external id." },
        "name": { "type": "string" }
      }
    },
    "compute": [
      { "kind": "external_id", "name": "properties.code" }
    ]
  }
}
```

That's all you need for "external_id at `properties.code`, validated shape, last-write-wins". Five lines of policy, no waterfall, no aliases.

### Worked example — full feature set

```json
{
  "items_write_policy": {
    "on_conflict": "new_version",
    "enable_validity": true,
    "validity_field": "properties.valid_from",
    "schema": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "required": ["adm2_pcode", "adm2_name", "adm0_iso3", "valid_from"],
      "additionalProperties": false,
      "properties": {
        "adm2_pcode": {
          "type": "string",
          "pattern": "^[A-Z]{3}[0-9A-Z]+$",
          "description": "Stable per-country admin-level-2 code; serves as external_id."
        },
        "adm2_name":  { "type": "string", "minLength": 1, "description": "Display name." },
        "adm1_pcode": { "type": "string", "description": "Parent admin-1 pcode." },
        "adm0_iso3":  { "type": "string", "minLength": 3, "maxLength": 3, "description": "ISO 3166-1 alpha-3 country code." },
        "valid_from": { "type": "string", "format": "date", "description": "Lower bound of validity window." },
        "valid_to":   { "type": ["string", "null"], "format": "date", "default": null, "description": "Upper bound; null = open-ended." },
        "source":     { "type": "string", "default": "GAUL", "description": "Originating dataset (free-form)." }
      }
    },
    "compute": [
      { "kind": "external_id",     "name": "properties.adm2_pcode" },
      { "kind": "area",            "name": "area_m2" },
      { "kind": "perimeter",       "name": "perimeter_m" },
      { "kind": "vertex_count" },
      { "kind": "centroid" },
      { "kind": "bbox" },
      { "kind": "geohash",         "resolution": 6 },
      { "kind": "h3",              "resolution": 7 },
      { "kind": "s2",              "resolution": 10 },
      { "kind": "geometry_hash" },
      { "kind": "attributes_hash" }
    ],
    "identity": [
      { "match_on": [{ "kind": "external_id" }], "on_match": "new_version" },
      { "match_on": [{ "kind": "geometry_hash" }, { "kind": "attributes_hash" }], "on_match": "refuse_return" },
      { "match_on": [{ "kind": "h3", "resolution": 7 }, { "kind": "attributes_hash" }], "on_match": "refuse_return" }
    ],
    "geometries": {
      "invalid_geom_policy": "attempt_fix",
      "srid_mismatch_policy": "transform",
      "allowed_geometry_types": ["Polygon", "MultiPolygon"],
      "simplification_algorithm": "topology_preserving",
      "simplification_tolerance": 0.0001,
      "remove_redundant_vertices": true
    }
  },
  "items_read_policy": {
    "feature_type": {
      "expose": ["area_m2", "perimeter_m", "vertex_count", "centroid", "bbox", "geohash", "h3", "s2"],
      "failure_mode": "best_effort",
      "external_id_as_feature_id": true
    }
  }
}
```

The `expose` value above is the additive case: schema-declared fields (`adm2_pcode`, `adm2_name`, …) still flow through, **plus** the listed computed resolved-names are merged in.

The other two `expose` shapes:

- **Default (schema-only)** — omit `expose` or set it to `null`. Response `properties` mirrors `ItemsSchema.fields` exactly; no computed values are merged.

  ```json
  "items_read_policy": { "feature_type": { "external_id_as_feature_id": true } }
  ```

- **Suppress-all (geometry-only)** — set `expose` to `[]`. No schema fields, no computed values; useful for geometry-only MVT tiles and minimal `/items` payloads.

  ```json
  "items_read_policy": { "feature_type": { "expose": [] } }
  ```

## Test plan

1. **Pydantic-level**:
   - `ComputedField(kind=GEOHASH, resolution=None)` → ValidationError.
   - `ComputedField(kind=AREA, resolution=9)` → ValidationError (no resolution accepted).
   - Duplicate `ComputedField.resolved_name()` across `compute` + `identity[*].match_on` → admission-time warning, deduplicated.

2. **Identity resolution** (TestClient against PG + ES drivers, parameterised):
   - EX1 — external_id default: same pcode → second POST is a new version (when enable_validity).
   - EX2 — geometry_hash only: identical geometry, different attributes → second POST refuses+returns the first.
   - EX3 — geometry+attributes AND: identical geometry, identical attributes → second POST refuses+returns; identical geometry + different attributes → new row.
   - EX4 — multi-rule OR: external_id match short-circuits; absent external_id, geometry_hash rule fires.
   - EX5 — H3+attrs composite: features in the same H3@7 cell with identical attributes → refuse_return; different cell or different attrs → new row.

3. **Driver parity** (matrix: PG, ES, DuckDB, Iceberg):
   - For each driver, with `compute=[h3@7, s2@10, area]`, write 5 features and assert: (a) per-driver column/field exists, (b) value matches `tools.geospatial.compute_derived_fields` reference, (c) read-back round-trip equal.

4. **Read path**:
   - `expose=None` (default) → response `properties` contains all `ItemsSchema.fields` declared fields; no computed values merged.
   - `expose=[]` → response `properties` is empty (no schema fields, no computed values); geometry still flows through.
   - `expose=["area"]` → response feature has schema fields **plus** `area` on `properties`.
   - `expose=["nonexistent"]` under `best_effort` → field omitted (schema baseline still flows); under `strict` → raises.
   - Response `properties` (under `expose=None` or non-empty list) validate against the schema derived from `items_schema`.
   - `external_id_as_feature_id=false` → `feature.id` stays the `geoid`, not the external id.

5. **Migration**:
   - Fresh database (per draft-schema invariant). No idempotent ALTERs, no shim code paths. Tests assume a clean schema; the dev-compose `db-reset` entrypoint runs unchanged.

## Critical analysis — surface area, stale code, gaps

### What survives at the top level of `ItemsWritePolicy`

Four concerns, each irreducible to the others:

1. `schema` (self-contained JSON Schema) — *what the data looks like*.
2. `compute: List[ComputedField]` — *what to derive from each feature*.
3. `identity: List[IdentityRule]` — *how to decide "is this the same row"*.
4. `geometries: GeometriesWriteBehavior` — *geometry-only pre-compute transforms* (SRID, fix, simplify, allow-list). Has to be a sub-config because it runs before `compute`. The geometry-hash version gate is no longer a sub-config flag: declare `derive.content_hashes=["geometry"]` and the default identity chain auto-appends an `IdentityRule(match_on=["geometry_hash"], on_match="refuse_return")` rule.

Plus two small posture flags: `on_conflict`, `enable_validity`/`validity_field`. That's it.

### Simplifications already applied vs the first draft

- `external_id_field` collapsed into `ComputedField(kind=EXTERNAL_ID, name="properties.X")`. One concept, one place.
- `require_external_id` deleted — JSON Schema `required` carries this.
- `geohash_precision` / `h3_resolutions` / `s2_resolutions` collapsed into `ComputedField(kind=GEOHASH|H3|S2, resolution=N)` — same surface for every cell system.
- `matcher_actions: Dict[IdentityMatcher, WriteConflictPolicy]` collapsed into per-rule `on_match`.
- `IdentityMatcher` enum collapses into `ComputedKind` (one enum covers identity, derived columns, and statistics).

### Stale / legacy code that this design deletes

Audited 2026-05-19 against `337f9952..e8a2db60`. Every item below has zero load-bearing call-sites today and is scheduled for phase 4:

| Symbol | Location | Status |
|---|---|---|
| `WritePolicyDefaults` | `driver_config.py:557` | 25 refs, all docstring/module exports, zero call-sites |
| `IdentityKeyConstraint` / `ValidityConstraint` / `GeometryHashConstraint` | `schema_types.py:98` | declared on `ItemsSchema.constraints`, never consumed |
| `IdentityKeyConstraint.geohash_precision: int` | `schema_types.py:98` | third copy of geohash_precision |
| `IdentityMatcher` enum | `driver_config.py:175` | overlaps `ComputedKind` |
| `ItemsWritePolicy.identity_matchers / matcher_actions / geohash_precision / external_id_field / require_external_id` | `driver_config.py:298` | replaced by `compute` + `identity` + `schema` |
| `FeatureAttributeSidecarConfig.enable_validity` | `attributes_config.py:231` | driver-divergent bug (PG reads sidecar, DuckDB/Iceberg read policy) |
| `FeatureAttributeSidecarConfig.feature_type_schema` | `attributes_config.py:198` | duplicates `policy.schema` |
| `GeometriesSidecarConfig.feature_type_schema` | `geometries_config.py:296` | duplicates `policy.schema` |
| `FeatureAttributeSidecarConfig.external_id_as_feature_id` | `attributes_config.py:216` | moves to `ItemsReadPolicy.feature_type` |
| `GeometriesSidecarConfig.{h3_resolutions, s2_resolutions, geohash_precision}` | `geometries_config.py` | replaced by `policy.compute` |
| `GeometriesSidecarConfig.statistics: GeometriesStatisticsConfig` | `geometries_config.py` | replaced by `policy.compute` |
| `tools/geometry_stats.py:compute_geometry_statistics` | sidecar-only consumer | superseded by `compute_derived_fields()` |
| `SidecarProtocol.get_feature_type_schema()` → `QueryOptimizer.get_feature_type_schema()` → `item_service.py:1557` | aggregation chain | one read of `policy.schema` replaces all three |

### Known gaps still to close

- **JSON-Schema validation has no consumer today**. The ingest path does not call `jsonschema.validate()` against `policy.schema`. Phase 2 must wire this in — it is the load-bearing assumption behind "schema is self-contained".
- **No `ItemsReadPolicy` exists** (`rg ItemsReadPolicy` → 0). Phase 3.
- **`output_transformers: List[str]` resolution mechanism** must reuse the existing `EntityTransformProtocol` class-key registry used by `PrivateEntityTransformer`. Confirmed registry exists; integration is phase 3.
- **`expose` enforcement at strict-mode** needs a clear error path on missing fields — currently designed as 502, may want 422 if it is operator-caused vs driver-caused. Open question.

## Implementation phasing

1. **Phase 1 — models + tools** (this PR or first split): introduce `ComputedField` / `ComputedKind` / `IdentityRule` / `FeatureType`; add `compute_derived_fields()` in `tools/geospatial.py` covering all `ComputedKind` values. Pure additive, no driver changes yet.
2. **Phase 2 — `ItemsWritePolicy` rewrite**: replace `identity_matchers` + `matcher_actions` + `geohash_precision` with `identity` + `compute` + `schema`. Update PG, ES, DuckDB, Iceberg drivers to consume the new list (PG: stop reading sidecar h3/s2/geohash; others: gain spatial-key materialisation).
3. **Phase 3 — `ItemsReadPolicy` introduction**: new policy class, registered in the waterfall (collection-tier only). `_resolve_physical_*` and read drivers honour `feature_type.expose` and run the transformer chain.
4. **Phase 4 — dead-code removal**: delete `WritePolicyDefaults`, `IdentityKeyConstraint`, `ValidityConstraint`, `GeometryHashConstraint`, sidecar `feature_type_schema` (×2), sidecar `h3_resolutions` / `s2_resolutions` / `geohash_precision`, sidecar statistics blocks, sidecar `enable_validity`, sidecar `external_id_as_feature_id`, the `get_feature_type_schema()` aggregation pipeline. Update module exports.
5. **Phase 5 — docstring + example fixtures**: rewrite `ItemsWritePolicy` docstring (drops the "legacy vs M8" hedge); update example seed configs under `docker/configs/`; update walkthrough notebooks that touch identity matchers.

Each phase ships as its own PR; phase 4 is the irreversible cleanup and lands last.

## Risks

- **Operator-visible cost**: declaring `compute=[h3@7, s2@10, geohash@6, geometry_hash, attributes_hash, area, perimeter, centroid, bbox, vertex_count]` materialises ten columns per row. Callers who over-declare pay write IO and storage. Docstring on `compute` calls this out; admin UI should sum estimated row width and warn.
- **Driver capability mismatch**: a writer that cannot compute a `ComputedKind` (e.g. minimal Iceberg writer without the `h3-py` extra) must refuse the policy at admission rather than silently skip. Detection lives in `tools.geospatial.compute_derived_fields`: missing library raises a typed `UnsupportedComputedKind`, surfaced as a 422 with the offending kind.
- **`enable_validity` fix is observable**: PG collections that were silently relying on the sidecar value will see behaviour change when the policy value differs. The fresh-DB invariant means there is no production state to migrate, but local-dev configs will need to be re-saved through the admin UI.
- **Phase ordering bug-risk**: phase 2 ships before phase 4. Between them, sidecar fields exist but are unread. The phase-2 PR must include a regression test that grep-asserts no remaining read of sidecar `h3_resolutions` / `s2_resolutions` / `geohash_precision` (mirrors the audit pattern that filed #957 in the first place).

## Open questions

1. Should the spatial-cell `resolution` accept a list (`[7, 8, 9]`) as sugar for three separate `ComputedField` entries? Tempting but adds non-orthogonal sugar. Recommend: no, write the three entries.
2. `expose` is a flat list of names (or `None` for the default-schema case, or `[]` to suppress all properties). Should the non-empty-list form also accept a JSON-Pointer-style path so a transformer's nested output is selectable? Out of scope for this PR — revisit when the first real transformer needs it.
3. Should `output_transformers` be a list of class keys (current `EntityTransformProtocol` registration model) or a richer list with per-entry config? List of keys is fine for v1; per-entry config can land as `output_transformers: List[TransformerSpec]` later without breaking the simpler shape.
