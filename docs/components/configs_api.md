# Configs API

Operator surface for runtime configuration of every dynastore service —
modules, extensions, tasks, engines, drivers, routing, sidecars,
catalog/collection templates.  Every registered `PluginConfig` subclass
is reachable, addressable by class key, and PATCHable at the right
scope.  HATEOAS links surface alternate views and edit endpoints
inline, so operators discover the API without consulting OpenAPI.

The endpoints described here are gated by the `configs_access` IAM
policy; outside `IamMiddleware` they require sysadmin role by default
(per Cycle F.0 IAM rules).

## Endpoints

| Method | Path | Scope |
|---|---|---|
| GET    | `/configs/`                                                 | Platform |
| GET    | `/configs/catalogs/{catalog_id}`                            | Catalog |
| GET    | `/configs/catalogs/{catalog_id}/collections/{collection_id}` | Collection |
| PATCH  | `/configs/plugins/{class_key}`                              | Platform |
| PATCH  | `/configs/catalogs/{catalog_id}/plugins/{class_key}`        | Catalog |
| PATCH  | `/configs/catalogs/{catalog_id}/collections/{collection_id}/plugins/{class_key}` | Collection |

PATCH bodies follow **RFC 7396 JSON Merge Patch**: dict values merge
into the stored config; `null` deletes the row at that scope (revert to
inheritance).  List fields replace wholesale — there is no
deep-list-merge.

## Query parameters

All three GET endpoints accept the same query params:

| Param      | Type        | Default | Effect |
|------------|-------------|---------|--------|
| `resolved` | bool        | `true`  | `true`: return waterfall-resolved values for every visible config; `false`: return only configs explicitly stored at this scope (delta-only — safe for read-modify-write flows). |
| `meta`     | enum        | `field` | `none`: omit `meta` tree entirely; `field` (default): per-class `{field_docs}` payload at the same path as `configs`; `schema`: full Pydantic JSON Schema per class (heavier, form-builder ready). |
| `include`  | enum        | `scope` | `scope` (default): body lists configs owned by the active scope; upstream-tier configs go to the hierarchical `inherited` tree.  `upstream`: every visible class rendered with its waterfall-resolved value (verbose; no `inherited` tree). |
| `strict`   | bool        | `true`  | Cycle F.7d.2 — at platform scope, `true` keeps body to platform-intrinsic configs (`modules`, `engines`, `tasks`, `extensions`).  Catalog-/collection-tier templates route to `inherited` (or are dropped under `include=upstream`).  `false` restores the previous always-true platform-scope inclusion.  No effect at catalog or collection scope. |

The `self` link on every response carries `hrefSchema` (a JSON Schema
2020-12 document) describing every supported query parameter — useful
for clients that want to discover the surface programmatically.

## Response shape

```jsonc
{
  "_links":  [ /* HATEOAS link catalog — see below */ ],
  "scope":   "platform",                 // platform response only
  "configs": { /* nested config tree */ },
  "inherited": { /* mirror tree for upstream-tier configs */ }, // null at platform under strict=false
  "meta":      { /* nested per-class field_docs / json_schema */ }, // null when meta=none
  "catalog_id":   "demo",                // catalog response only
  "collection_id": "sample"              // collection response only
}
```

### `configs` tree shape

The tree mirrors the `_address: ClassVar[Tuple[str, ...]]` declared on
each `PluginConfig` subclass.  Variable-length tuples land at any
depth; the composer walks the address recursively (Cycle D.1).
Post Cycle F.7d.1, the platform tier splits into four sibling groups:

```
configs.platform.{
  modules.{gcp, web, tiles, stats, security, ...}    # core infra singletons
  engines.{postgresql_engine_config, ...}            # connection pool resources (F.1)
  tasks.{ingestion, task_routing_config, ...}        # async / scheduled work
  extensions.{stac, features, edr, coverages, ...}   # OGC + standards extensions (~21)
  catalog.{                                          # CATALOG-TIER TEMPLATES
    drivers.{...}                                    # default catalog driver configs
    routing.{...}                                    # default catalog routing config
    privacy.{collection_defaults: {is_private}}      # default collection privacy
    collection.{                                     # COLLECTION-TIER TEMPLATES
      drivers.{...}
      routing.{...}
      privacy.{is_private}
      info.{type, default_language}
      items.{
        drivers.{items_postgresql_driver: {sidecars}, items_elasticsearch_driver, ...}
        policy.{items_write_policy: {...}}
        routing.{items_routing_config: {operations}}
        schema.{items_schema: {...}}
      }
    }
  }
}
```

Catalog-tier (`platform.catalog.*`) and collection-tier
(`platform.catalog.collection.*`) sub-trees are TEMPLATES stored at
platform scope — they seed every catalog and collection respectively.
Per-catalog and per-collection overrides land at `/configs/catalogs/{cat}/...`
and `/configs/catalogs/{cat}/collections/{coll}/...` PATCH endpoints.

### Module visibility — only what's loaded

The composer walks `list_registered_configs()`, which returns every
`PluginConfig` subclass that was IMPORTED during the service's
boot-time `instantiate_modules()` pass.  A service whose SCOPE doesn't
include a given module never registers its configs — the body of
`/configs/?resolved=true` reflects exactly what THIS service can do.

A different service (catalog / worker / maps) booted with a different
SCOPE surfaces a different config set.  Operators reading the response
see the truth for the service they queried; cross-service composition
is by-convention (the modules each service has installed) rather than
enforced at the configs-API level.

### `inherited` tree

A hierarchical breadcrumb tree mirroring the `configs` shape exactly.
Each leaf carries `{"source": "platform" | "catalog" | "default"}`,
telling operators where the resolved value originated at the SAME path
the value would land at if it were rendered in `configs`.

Populated under three conditions:

1. **Catalog scope, `include=scope`** (default) — platform-tier configs
   that have no catalog override route to `inherited`.
2. **Collection scope, `include=scope`** (default) — both catalog-tier
   and platform-tier configs that have no collection override route
   to `inherited`.
3. **Platform scope, `strict=true`** (default; Cycle F.7d.2) —
   catalog-tier templates (`_visibility="catalog"`) route to
   `inherited`; platform-intrinsic configs stay in body.

`null` under `include=upstream` (everything inlined) and at platform
scope under `strict=false` (where the body is already inclusive).

### `meta` tree

Same shape as `configs`.  Each leaf carries either `{"field_docs":
{field_name: description, ...}}` (default `meta=field`) or
`{"json_schema": <full Pydantic schema>}` (under `meta=schema`).  Use
the schema mode for form builders that need title/description/type/
default/examples per field.  Suppress entirely with `meta=none`.

## HATEOAS link catalog

Every response carries a top-level `_links` array.  Routing-entry DTOs
also carry their own `_links` (Cycle F.7d.3 replaces the old
`config_ref: null` scalar).

### Top-level `_links` rels

| `rel`       | Method | Templated | Purpose |
|-------------|--------|-----------|---------|
| `self`      | GET    | no        | Current view URL.  Carries `hrefSchema` describing every supported query parameter. |
| `alternate` | GET    | no        | `?meta=schema` — full JSON Schema per class (form-builder mode). |
| `alternate` | GET    | no        | `?meta=none` — no field documentation (lean mode). |
| `alternate` | GET    | no        | `?resolved=false` — delta-only; configs explicitly stored at this scope (safe for read-modify-write). |
| `alternate` | GET    | no        | `?include=upstream` — full waterfall; every visible class rendered with its resolved value (verbose; pre-slim default). |
| `alternate` | GET    | no        | `?strict=false` (Cycle F.7d.2) — inclusive view; platform scope keeps catalog-/collection-tier templates inline rather than routing them to `inherited`. |
| `edit`      | PATCH  | yes       | `<base>/plugins/{class_key}` — modify a single config class at this scope. |

### Routing-entry `_links` rels

Each `OperationDriverEntry` (inside `*RoutingConfig.operations[OP]`)
carries an `_links` array (Cycle F.7d.3):

| `rel`           | Method | Purpose |
|-----------------|--------|---------|
| `driver-config` | PATCH  | URL of the PATCH endpoint for the driver's registered config (only emitted when the `driver_ref` binds to a registered config class).  Composition sub-drivers (no registered config) emit no link — `_links: []`. |

The `driver-config` link's `href` is **scope-aware**: at collection
scope it points at
`<base_url>/plugins/<driver_ref>` rooted at the collection (per-
collection override target).  At catalog scope it points at the
catalog-tier override.  At platform scope it points at the platform-
tier default.  The same `class_key` lives at every tier with the
waterfall resolving to the most-specific value at read time.

Future link rels (placeholder; not yet emitted):
- `engine-config` (Cycle F.4c) — when ref-keyed driver storage adds
  multi-instance refs, each driver entry will also carry an
  `engine-config` link pointing at its referenced engine.

## Scope strictness rules

The four parameters interact as follows:

```
strict=true  + include=scope    →   body=owned-only,    inherited=upstream-summary  (default)
strict=true  + include=upstream →   body=inclusive,     inherited=null              (verbose)
strict=false + include=scope    →   body=catalog-tier-included-at-platform, inherited=summary
strict=false + include=upstream →   body=fully-inclusive,                   inherited=null
```

`strict` only affects platform scope (catalog/collection scopes already
honour the per-tier `_visibility` filter).

`resolved=false` is orthogonal: it switches what _data_ gets fetched
(only this scope's deltas vs. waterfall-resolved values) without
changing the _shape_ of the response or what's filtered to `inherited`.

## Live examples

Captured from a default localhost dev stack with one catalog + collection
seeded (`demo` / `demo/sample`):

### Platform scope, default

```bash
GET /configs/?resolved=true
```

```jsonc
{
  "scope": "platform",
  "configs": {
    "platform": {
      "modules": { "gcp": {…}, "security": {…}, "stats": {…}, "tiles": {…}, "web": {…} },
      "engines": {
        "postgresql_engine_config":    { "enabled": true, "lifecycle": {…}, "pool_size": 10, … },
        "elasticsearch_engine_config": { "enabled": true, "lifecycle": {…}, "request_timeout_sec": 30, … },
        "duckdb_engine_config":        { "enabled": true, "lifecycle": {…}, "pool_size": 4, … },
        "iceberg_engine_config":       { "enabled": true, "lifecycle": {…}, "catalog_properties": {} }
      },
      "tasks":      { "task_routing_config": {…}, "tasks_plugin_config": {…} },
      "extensions": { /* ~21 entries: stac, features, edr, coverages, dggs, … */ }
    }
  },
  "inherited": {
    "platform": {
      "catalog":  { /* drivers, routing, privacy, collection — visibility=catalog templates */ },
      "modules":  { "gcp": { /* gcp_catalog_bucket_config, gcp_collection_bucket_config — non-default visibility */ } }
    }
  },
  "_links": [ /* self + 5 alternates + edit */ ]
}
```

### Catalog scope

```bash
GET /configs/catalogs/demo?resolved=true
```

```jsonc
{
  "catalog_id": "demo",
  "configs": {
    "platform": {
      "catalog":  { "drivers": {…}, "routing": {…}, "privacy": {…}, "collection": { "drivers": {…} } },
      "modules":  { "gcp": { /* visibility=catalog */ } }
    }
  },
  "inherited": {
    "platform": {
      "engines":    { /* 4 engines, source=platform */ },
      "extensions": { /* 21 extensions, source=platform */ },
      "modules":    { /* visibility=null modules, source=platform */ },
      "tasks":      { /* source=platform */ }
    }
  },
  "_links": [ /* self + alternates */ ]
}
```

### Collection scope

```bash
GET /configs/catalogs/demo/collections/sample?resolved=true
```

```jsonc
{
  "catalog_id":    "demo",
  "collection_id": "sample",
  "configs": {
    "platform": {
      "catalog": {
        "assets":     { /* asset-tier collection overrides */ },
        "collection": {
          "drivers":  {…},
          "envelope": {…},
          "info":     { "type": "vector", "default_language": "en" },
          "privacy":  { "is_private": false },
          "routing":  {…},
          "items": {
            "drivers": {
              "items_postgresql_driver": { "engine_ref": "postgresql_engine", "sidecars": [] },
              "items_elasticsearch_driver": { "engine_ref": "elasticsearch_engine", "index_prefix": "items" }
            },
            "policy":  { "items_write_policy": {…} },
            "routing": {
              "items_routing_config": {
                "operations": {
                  "WRITE":  [{
                    "driver_ref": "items_postgresql_driver",
                    "on_failure": "fatal",
                    "write_mode": "sync",
                    "_links": [{
                      "rel": "driver-config",
                      "href": "http://localhost:8080/configs/catalogs/demo/collections/sample/plugins/items_postgresql_driver",
                      "method": "PATCH",
                      "title": "PATCH this driver's registered config"
                    }]
                  }],
                  "SEARCH": [/* … */]
                }
              }
            },
            "schema":  { "items_schema": {…} }
          }
        }
      },
      "modules": { "gcp": { /* visibility=collection */ } }
    }
  },
  "inherited": {
    "platform": {
      "catalog":    { /* catalog-tier templates, source=catalog or platform */ },
      "engines":    { /* source=platform */ },
      "extensions": { /* source=platform */ },
      "modules":    { /* source=platform */ },
      "tasks":      { /* source=platform */ }
    }
  },
  "_links": [ /* self + alternates */ ]
}
```

### Modifying a config

```bash
# Set a per-collection sidecar override
curl -X PATCH https://api/configs/catalogs/demo/collections/sample/plugins/items_postgresql_driver \
  -H 'Authorization: Bearer <sysadmin-token>' \
  -H 'Content-Type: application/merge-patch+json' \
  -d '{
    "sidecars": [
      {"sidecar_type": "geometries",  "geohash_precision": 8, "store_bbox": true},
      {"sidecar_type": "attributes",  "external_id_field": "properties.id"}
    ]
  }'

# Revert: delete the per-collection row, fall back to inherited
curl -X PATCH https://api/configs/catalogs/demo/collections/sample/plugins/items_postgresql_driver \
  -H 'Authorization: Bearer <sysadmin-token>' \
  -d 'null'
```

## See also

- `docs/components/storage_drivers.md` — driver classes, routing, hint
  dispatch
- `docs/components/platform_engines.md` — engines layer, lifecycle,
  the `EngineInstanceCache` contract
- `docs/components/sidecar_configs.md` — sidecar configurations on PG-
  backed items drivers (full default field surface)
- `src/dynastore/extensions/configs/config_api_service.py` — composer
  source (variable-length address walker, scope filter, HATEOAS link
  builder)
- `src/dynastore/extensions/configs/config_api_dto.py` — response DTOs
  (`PlatformConfigResponse`, `CatalogConfigResponse`,
  `CollectionConfigResponse`, `DriverRef`, `Link`)
