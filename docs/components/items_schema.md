# Items Schema (`items_schema`)

The single source of truth for a collection's **field structure and
declarative constraints**. It is a `PluginConfig` (`class_key`
`items_schema`) that lives in the config waterfall at the collection
tier and drives DDL generation (PostgreSQL columns, Iceberg schema,
DuckDB `CREATE TABLE`), read-path projection, and optional
service-layer validation.

`items_schema` answers *what fields a collection has and what is true of
them*. It is distinct from two neighbours it is often confused with:

- **`data_type` vocabulary** — the canonical token a field *means*
  (`string`, `double`, `timestamp`, `geometry(Point,4326)`, …). The
  full vocabulary, the gdalinfo-derive path, and the deprecated aliases
  are documented in [Field Types](field-types.md). This page references
  those tokens; it does not redefine them.
- **`attribute_schema` / sidecars** — *how* the declared fields are
  physically stored on a PostgreSQL-backed driver (native columns vs. a
  JSONB blob). `items_schema` is the author-facing, driver-agnostic
  declaration; the sidecar layer is the PG-specific realization. See
  [Sidecar Configurations](sidecar_configs.md). The write-side bridge
  `bridge_schema_to_attribute_sidecar` turns the former into the latter.

## Address and freeze semantics

```
platform.catalog.collection.items.schema.items_schema
```

`items_schema` carries `_freeze_at = "collection"`. Every authorable
field below is `Mutable`, which means it can be PATCHed freely **until
the collection's physical items table has at least one row** — after the
first write the schema shape is frozen and further edits to the gated
fields are rejected. Practical rule: settle the schema *before*
ingesting data. Adding a field after materialization requires a fresh
(re)provision, not an in-place change — see
[Schema Evolution & Drift](schema_evolution.md).

## Top-level fields

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `version` | `int` (≥1) | `1` | Explicit, in-payload schema-shape version that travels with the frozen schema. Informational today (drives no auto-migration); a bump is itself a shape change and is gated by the same collection freeze as `fields` — bump it before first materialization, never after. |
| `level` | `EntityLevel` enum | `item` | Which entity level the schema applies to: `catalog` / `collection` / `item` / `asset`. `item` covers both features and records. |
| `fields` | `Dict[str, FieldDefinition]` | `{}` | The field map — see [The `fields` map](#the-fields-map) below. Keyed by field name. |
| `exclude_fields` | `List[str]` \| null | null | Field names to drop from exposure/derivation (acts as a subtractive decorator over an inherited or introspected schema). |
| `metadata_fields` | `Dict[str, Any]` \| null | null | Free-form schema-level metadata bag, surfaced to extensions; not validated against `fields`. |
| `allow_app_level_enforcement` | `bool` | `false` | When `true`, fall back to **service-layer** enforcement of `required`/`unique` constraints if the primary driver does not advertise `REQUIRED_ENFORCEMENT` / `UNIQUE_ENFORCEMENT`. When `false` (default), config admission is *rejected* for constraints the driver can't enforce natively. |
| `strict_unknown_fields` | `bool` | `false` | When `true`, refuse writes whose features carry user-data properties not listed in `fields` (HTTP 422 with the offending field list). System fields (`id`, `geoid`, `geometry`, `bbox`, `properties`, …) always pass. Always service-layer enforcement — JSON-storing drivers accept any field otherwise, so this is the only way to constrain the open schema shape on write. |
| `default_access` | `FieldAccess` enum | `auto` | Schema-wide default query-access intent for fields that leave their own `access` at `auto` — see [Field access intent](#field-access-intent). Replaces the former PG-specific `materialize_fields_as_columns` flag. |
| `constraints` | `List[FieldConstraint]` | `[]` | Declarative constraints — see [Constraints](#constraints). |

## The `fields` map

Each value in `fields` is a `FieldDefinition`
(`models/protocols/field_definition.py`) — driver-agnostic, with every
driver mapping its native type system onto it. The author-facing fields:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `name` | `str` | — | Field name (matches the map key). |
| `alias` | `str` \| null | null | Read-time rename emitted on the wire instead of `name`. |
| `title` | str \| localized dict \| null | null | Human label; accepts a `{lang: text}` localized form. |
| `description` | str \| localized dict \| null | null | Human description; localizable. |
| `data_type` | `str` | — | Canonical, GDAL-rooted type token. See [Field Types](field-types.md) for the full vocabulary. Validated by `canonical_data_type()`; legacy SQL spellings are folded during the migration window, unknown tokens are rejected. |
| `subtype` | `str` \| null | null | Optional OGR-style refinement (`boolean`/`json`/`uuid` promote the base; `int16`/`float32` keep the base and record the subtype). |
| `expose` | `bool` | `true` | Whether the field appears in public OGC/STAC responses. |
| `required` | `bool` | `false` | Reject a feature whose value is null/missing (see enforcement model under [Constraints](#constraints)). |
| `unique` | `bool` | `false` | Value must be unique within the collection. |
| `aggregations` | `List[str]` \| null | null | Allowed aggregation funcs: `null` or `["*"]` = all, `[]` = none, or a specific list (`["count","sum"]`). |
| `transformations` | `List[str]` \| null | null | Allowed transformation funcs (same `null`/`["*"]`/explicit semantics). |
| `max_length` | `int` \| null | null | JSON-Schema-style length bound (strings). |
| `minimum` / `maximum` | `float` \| null | null | Numeric range bounds. |
| `enum` | `List[Any]` \| null | null | Allowed value set. |
| `pattern` | `str` \| null | null | Regex constraint (strings). |
| `format` | `str` \| null | null | JSON-Schema *output* format hint (e.g. `date` vs `date-time`). |
| `parse_format` | `str` \| null | null | Ingestion-time `strptime` *input* pattern for a temporal field (e.g. `%d/%m/%Y`). Disambiguates day-first vs month-first numeric dates before re-emitting canonical ISO-8601. Unset → auto-detection. Ignored for non-temporal fields. |
| `access` | `FieldAccess` enum | `auto` | Per-field query-access intent — see [Field access intent](#field-access-intent). Overrides the schema-wide `default_access`. Replaces the former PG-specific `materialize` boolean. |
| `default` | `Any` \| null | null | Value to apply when the field is missing on write. The driver decides how (native column `DEFAULT`, inject-on-write, or ignore). The value's Python type must match `data_type` (validated at config-parse). Geometry fields reject a default. |
| `capabilities` | `List[FieldCapability]` | `[]` | Driver-advertised capabilities for the field; consulted by `access=auto`. |

> `sql_expression` exists on the model but is **driver-computed and not
> author-facing** — it carries a read-projection detail (e.g.
> `a.asset_id`), is excluded from serialization, and is marked read-only
> so it never enters the author config or leaks into queryables
> responses. Do not set it.

### Field access intent

`FieldAccess` expresses *how aggressively to optimize a field for query
access* in a driver-agnostic way — the intent is portable, the mechanism
stays with the driver:

| Value | Meaning | PostgreSQL realization |
|-------|---------|------------------------|
| `auto` (default) | Let the driver decide from the field's declared `capabilities`. The portable equivalent of the historical "lift only constrained/queryable fields" behaviour. | Driver chooses per capability. |
| `fast` | Optimize for filtering/sorting. | Lift the field into a native attributes-sidecar **column** with an index. (ES maps a typed/keyword field; Iceberg adds a sort field; GeoParquet emits column stats + bloom filter.) |
| `compact` | Minimize storage. | Keep the value in the JSONB `properties` blob. |

A field's effective access is its own `access` if set, otherwise the
schema-wide `default_access`. On a COLUMNAR attributes sidecar the
write-side bridge force-promotes every non-geometry field to a column
regardless — see [Sidecar Configurations](sidecar_configs.md).

## Constraints

`constraints` is an open list of `FieldConstraint` subclass instances
(`modules/storage/schema_types.py`). The built-ins:

| `constraint_type` | Class | Meaning |
|-------------------|-------|---------|
| `required` | `RequiredConstraint(field=...)` | The field must be present (non-null) on every write. |
| `unique` | `UniqueConstraint(field=...)` | Values must be unique across the collection. |

Enforcement is **native (DDL-level) when the primary driver advertises
the matching capability** (`Capability.REQUIRED_ENFORCEMENT` /
`UNIQUE_ENFORCEMENT`). Otherwise behaviour depends on
`allow_app_level_enforcement`: `true` falls through to service-layer
enforcement; `false` (default) **rejects the config at save time** so an
unenforceable constraint never silently passes. The per-field `required`
/ `unique` booleans on `FieldDefinition` are the inline shorthand for the
same intent.

## Validate-time guards

Three `register_validate_handler` hooks run when an `items_schema` (or
the related `items_write_policy`) is saved, so misconfigurations fail
loud at config-save instead of at ingest or read:

1. **Reserved root names** — rejects any `fields` key *or* `alias` that
   collides with a reserved document-root name (`geoid`, `catalog_id`,
   `collection_id`, `external_id`, `asset_id`, `geometry`, `bbox`,
   `simplification_factor`, `simplification_mode`, `properties`,
   `extras`). Such a name would shadow the system field or fail
   downstream in opaque ways. Rename with a domain prefix.
2. **`external_id` ↔ schema cross-validation** — when an
   `items_write_policy` names an `external_id` source path, its leaf
   field must be declared in the resolved `items_schema.fields` at the
   same scope (or be a system identity field `geoid` / `id`). Enforced
   at every scope where a schema resolves, so a policy can't name an
   undeclared field at one tier and be accepted at another.
3. **Storage-realizability** — defense-in-depth backstop for the
   silent-drop class: when the PG attributes sidecar
   resolves COLUMNAR-only (no JSONB blob on disk), every non-geometry
   field MUST become an `AttributeSchemaEntry` after the bridge runs, or
   ingest would swallow it at the SQL boundary. The bridge auto-promotes
   every such field, so this only fires on a genuine bridge regression.
   It is enforced from the **first** save (simulated against a default
   sidecar when no driver config exists yet) and is config-only — pure
   `bridge_schema_to_attribute_sidecar`, no `ALTER TABLE`, honouring the
   no-in-place-DDL invariant.

## Example — set a collection schema

```bash
# PATCH the collection-tier items_schema (RFC 7396 merge-patch).
curl -X PATCH \
  https://api/configs/catalogs/demo/collections/sample/plugins/items_schema \
  -H 'Authorization: Bearer <sysadmin-token>' \
  -H 'Content-Type: application/merge-patch+json' \
  -d '{
    "strict_unknown_fields": true,
    "default_access": "auto",
    "fields": {
      "name":  {"name": "name",  "data_type": "string",  "required": true, "access": "fast"},
      "code":  {"name": "code",  "data_type": "string",  "unique": true},
      "area":  {"name": "area",  "data_type": "double"},
      "obs_date": {"name": "obs_date", "data_type": "date", "parse_format": "%d/%m/%Y"},
      "geom":  {"name": "geom",  "data_type": "geometry(Polygon,4326)"}
    },
    "constraints": [
      {"constraint_type": "required", "field": "name"},
      {"constraint_type": "unique",   "field": "code"}
    ]
  }'

# Revert to the inherited (catalog/platform) schema:
curl -X PATCH \
  https://api/configs/catalogs/demo/collections/sample/plugins/items_schema \
  -H 'Authorization: Bearer <sysadmin-token>' \
  -d 'null'
```

Schema resolution follows the standard config waterfall (collection >
catalog > platform > code defaults), so a collection inherits the
catalog (and platform) schema unless it stores its own row.

## See also

- [Field Types](field-types.md) — the `data_type` / `subtype`
  vocabulary, the gdalinfo-derive path, deprecated aliases.
- [Sidecar Configurations](sidecar_configs.md) — how declared fields are
  physically realized on PG drivers (COLUMNAR vs JSONB), and the
  `bridge_schema_to_attribute_sidecar` write-side bridge.
- [Schema Evolution & Drift](schema_evolution.md) — what happens when a
  schema changes after a collection has materialized (read-only drift
  reconciliation + reprovision; never app-issued `ALTER TABLE`).
- [Configs API](configs_api.md) — the GET/PATCH surface, waterfall
  scopes, and how `items_schema` overrides land at the right tier.
- `src/dynastore/modules/storage/driver_config.py` — the `ItemsSchema`
  `PluginConfig` + its validate handlers.
- `src/dynastore/models/protocols/field_definition.py` —
  `FieldDefinition`, `FieldAccess`, `EntityLevel`.
- `src/dynastore/modules/storage/schema_types.py` — `FieldConstraint`,
  `RequiredConstraint`, `UniqueConstraint`.
