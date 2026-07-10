# Configs API

Operator surface for runtime configuration of every dynastore service —
modules, extensions, tasks, engines, drivers, routing, sidecars,
catalog/collection templates.  Every registered `PluginConfig` subclass
is reachable, addressable by class key, and PATCHable at the right
scope.  HATEOAS links surface alternate views and edit endpoints
inline, so operators discover the API without consulting OpenAPI.

The endpoints described here are gated by the `configs_access` IAM
policy; outside `IamMiddleware` they require sysadmin role by default.

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
| `meta`     | enum        | `field` | Per-class documentation injected INLINE on each in-scope plugin leaf.  Every in-scope leaf always carries `_meta = {tier, source}` (provenance is structural — `tier` is the active scope, `source` is the tier that supplied the resolved value).  `meta` controls the OPTIONAL extras merged into `_meta`: `none` — only `{tier, source}`; `field` (default) — adds `docs = {field_name: description}`; `schema` — adds `json_schema = <full Pydantic schema>` (heavier, form-builder ready). |
| `include`  | enum        | `scope` | `scope` (default): body lists only configs owned by the active scope; upstream-tier configs are filtered out (their provenance is recoverable from any leaf's `_meta.source`).  `upstream`: every visible class rendered with its waterfall-resolved value (verbose; `_meta.source` on each leaf identifies the tier of origin). |
| `strict`   | bool        | `true`  | At platform scope, `true` keeps body to platform-intrinsic configs (`modules`, `engines`, `tasks`, `extensions`); catalog-/collection-tier templates are filtered out.  `false` restores full platform-scope inclusion (catalog-tier templates appear inline in the body).  No effect at catalog or collection scope. |
| `links`    | enum        | `minimal` | Per-plugin HATEOAS edit affordances injected INLINE on each in-scope leaf as a `_links` sibling.  `none`: no `_links` on any leaf.  `minimal` (default): `rel`/`href`/`method`.  `full`: adds a contextual `title` per link naming the class key and tier, plus `rel="schema"` and (for bound drivers) `rel="engine"`. |

Query parameters are advertised through OpenAPI (`/openapi.json`); the
response carries no runtime `_links` array at the root — query-param
self-description lives in OpenAPI as the single source of truth.

## Response shape

```jsonc
{
  "scope":   "platform",                 // platform response only
  "configs": {
    /* tier-first tree.  Each in-scope plugin leaf carries the resolved
       config fields PLUS siblings:
         "_meta":  {"tier": "<scope>", "source": "<tier>", ...optional docs|json_schema}
         "_links": [self, edit(PUT), edit(DELETE), describedby, ?schema, ?engine]
                                                                (when ?links != "none") */
  },
  "catalog_id":   "demo",                // catalog response only
  "collection_id": "sample"              // collection response only
}
```

**Provenance is structural per-leaf** — every rendered config carries
`_meta.tier` (active scope) and `_meta.source` (originating tier).
The former parallel `inherited` top-level tree is retired; operators
discover upstream provenance from any leaf's `_meta.source` without
cross-walking a second tree.  Field documentation and per-plugin edit
affordances live INLINE on each leaf (as `_meta` / `_links` siblings
of the plugin's own fields).

### `configs` tree shape

The tree mirrors the `_address: ClassVar[Tuple[str, ...]]` declared on
each `PluginConfig` subclass.  Variable-length tuples land at any
depth; the composer walks the address recursively.
The platform tier splits into four sibling groups:

```
configs.platform.{
  modules.{gcp, web, tiles, stats, security, ...}    # core infra singletons
  engines.{postgresql_engine_config, ...}            # connection pool resources (F.1)
  tasks.{ingestion, task_routing_config, ...}        # async / scheduled work
  extensions.{stac, features, edr, coverages, ...}   # OGC + standards extensions (~21)
  catalog.{                                          # CATALOG-TIER TEMPLATES
    drivers.{...}                                    # default catalog driver configs
    routing.{...}                                    # default catalog routing config
    collection.{                                     # COLLECTION-TIER TEMPLATES
      drivers.{...}
      routing.{...}                                  # privacy = routing-pin of private driver
      info.{type, default_language}
      items.{
        drivers.{items_postgresql_driver: {sidecars}, items_elasticsearch_driver, ...}
        policy.{items_write_policy: {...}}
        routing.{items_routing_config: {operations}}
        schema.{items_schema: {version, level, fields, default_access, strict_unknown_fields, constraints, ...}}
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

### Per-leaf provenance (`_meta.tier` / `_meta.source`)

Every in-scope plugin leaf always carries `_meta = {tier, source}`
regardless of the `meta` query param value:

- `tier` — the active scope of the request (`platform`, `catalog`,
  `collection`).
- `source` — the tier whose stored row supplied the resolved value
  (`platform` / `catalog` / `collection` / `default` if no row exists
  at any tier and the class's Pydantic defaults are returned).

This replaces the retired parallel `inherited` tree:
upstream provenance is now recoverable from any leaf's `_meta.source`
without crawling a second tree.

Under the default `?include=scope`, upstream-tier configs that have no
override at the active scope are simply filtered out of the body —
they are not rendered as placeholders.  Set `?include=upstream` to
inline every visible class with its waterfall-resolved value; each
leaf's `_meta.source` identifies the tier of origin.

### Per-leaf `_meta` extras (controlled by `?meta`)

`meta` controls the OPTIONAL extras merged into the always-present
`{tier, source}` block:

- `meta=none` — leaf carries only `{tier, source}`.
- `meta=field` (default) — adds `docs = {field_name: description}`,
  extracted from the class JSON Schema.  Lightweight, suitable for
  dashboards.
- `meta=schema` — adds `json_schema = <full Pydantic schema 2020-12>`.
  Heavier; suitable for form builders that need title / description /
  type / default / examples per field.

## HATEOAS link catalog

The response carries **no top-level `_links` array** — query-parameter
self-description lives in OpenAPI (`/openapi.json`) as the single
source of truth.  Each in-scope plugin leaf carries its own `_links`
array (when `?links != "none"`).  Routing-entry DTOs also carry their
own `_links` (driver-config affordance).

### Per-leaf `_links` rels (when `?links=minimal|full`)

| `rel`          | Method | Purpose |
|----------------|--------|---------|
| `self`         | GET    | Read this plugin at the active scope: `<base>/plugins/{class_key}` (or `/{ref_key}` for multi-instance refs). |
| `edit`         | PUT    | Replace this plugin at the active scope (full body). |
| `edit`         | DELETE | Clear the scope-tier override; falls back to the upstream waterfall. |
| `describedby`  | GET    | JSON Schema entry: `<configs_root>/registry/{class_key}` (scope-agnostic — registry is global; multi-instance refs use the canonical `class_key`, not `ref_key`). |
| `schema`       | GET    | (`links=full` only) `<configs_root>/registry/{class_key}?meta=schema` — full Pydantic schema. |
| `engine`       | GET    | (`links=full` only, driver leaves with resolved `engine_ref`) `/api/platform/configs/engines/{engine_ref}` — the bound engine's config. |

`links=full` adds a contextual `title` per link naming the class_key
and tier phrase (e.g. `"Replace items_routing at catalog 'demo'"`).
`links=minimal` omits titles.

### Routing-entry `_links` rels

Each `OperationDriverEntry` (inside `*RoutingConfig.operations[OP]`)
carries an `_links` array:

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
- `engine-config` — when ref-keyed driver storage adds multi-instance
  refs, each driver entry will also carry an `engine-config` link
  pointing at its referenced engine.

## Scope strictness rules

The four parameters interact as follows (each leaf in the body carries
`_meta.source` identifying its tier of origin):

```
strict=true  + include=scope    →   body=owned-only at active scope                    (default)
strict=true  + include=upstream →   body=inclusive (every visible class, _meta.source) (verbose)
strict=false + include=scope    →   body=catalog-tier-included-at-platform
strict=false + include=upstream →   body=fully-inclusive
```

`strict` only affects platform scope (catalog/collection scopes already
honour the per-tier `_visibility` filter).

`resolved=false` is orthogonal: it switches what _data_ gets fetched
(only this scope's deltas vs. waterfall-resolved values) without
changing the _shape_ of the response or what's filtered from the body.

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
      "modules": { "gcp": {…, "_meta": {"tier": "platform", "source": "platform"}}, … },
      "engines": {
        "postgresql_engine_config":    { "enabled": true, "pool_size": 10, …, "_meta": {"tier": "platform", "source": "platform"} },
        "elasticsearch_engine_config": { "enabled": true, "request_timeout_sec": 30, …, "_meta": {…} },
        "duckdb_engine_config":        { "enabled": true, "pool_size": 4, …, "_meta": {…} },
        "iceberg_engine_config":       { "enabled": true, "catalog_properties": {}, "_meta": {…} }
      },
      "tasks":      { "task_routing_config": {…, "_meta": {…}}, "tasks_plugin_config": {…, "_meta": {…}} },
      "extensions": { /* ~21 entries: stac, features, edr, coverages, dggs, … — each with _meta */ }
    }
  }
  /* Under strict=true (default): catalog-tier templates are filtered from the body.
     Set ?strict=false or ?include=upstream to see them inline. */
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
      "modules":  { "gcp": { /* visibility=catalog, _meta.tier="catalog", _meta.source="catalog"|"platform" */ } }
    }
  }
  /* Under default ?include=scope, platform-tier engines/extensions/tasks/visibility=null modules
     are filtered out — their values are still reachable at /configs/?resolved=true (platform scope),
     and each leaf's _meta.source identifies its tier of origin.
     Set ?include=upstream to inline them here with _meta.source="platform". */
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
                    "_links": [{
                      "rel": "driver-config",
                      "href": "http://localhost:8080/configs/catalogs/demo/collections/sample/plugins/items_postgresql_driver",
                      "method": "PUT",
                      "title": "PUT this driver's config at collection scope"
                    }]
                  }],
                  "READ":   [/* … */],
                  "INDEX":  [/* … */]
                }
              }
            },
            "schema":  { "items_schema": { "version": 1, "fields": {…}, "default_access": "auto", "strict_unknown_fields": false, "constraints": [], "_meta": {"tier": "collection", "source": "collection"} } }
          }
        }
      },
      "modules": { "gcp": { /* visibility=collection, _meta.tier="collection", _meta.source=<resolved tier> */ } }
    }
  }
  /* Under default ?include=scope, upstream tiers (catalog-tier templates, platform engines,
     extensions, tasks, visibility=null modules) are filtered out — their values resolve at
     their own scope endpoints, and each rendered leaf's _meta.source identifies origin.
     Set ?include=upstream to inline every visible class here with _meta.source flagged. */
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

# Revert: delete the per-collection row, fall back to the upstream waterfall
curl -X PATCH https://api/configs/catalogs/demo/collections/sample/plugins/items_postgresql_driver \
  -H 'Authorization: Bearer <sysadmin-token>' \
  -d 'null'
```

## Multi-instance refs

Operators can register multiple instances of the same driver/engine class
side-by-side at any scope.  Each instance is keyed by an operator-chosen
`ref_key` (snake_case `^[a-z][a-z0-9_]{0,62}$`); the canonical
single-instance row is `ref_key == class_key`.

### GET tree shape

Multi-instance ref leaves surface under the parent class's `_address`
alongside the canonical class-keyed leaf.  Example platform-scope
response with `tiles_secondary` registered next to the canonical
`tiles_config`:

```jsonc
{
  "configs": {
    "platform": {
      "modules": {
        "tiles": {
          "tiles_config":    { "min_zoom": 0,  "max_zoom": 12, "..." : "..." },
          "tiles_secondary": { "min_zoom": 5,  "max_zoom": 15, "cache_on_demand": false }
        }
      }
    }
  }
}
```

Multi-instance ref leaves carry their own inline `_meta` (with the
always-present `{tier, source}` plus the same `docs` or `json_schema`
extras as the canonical class — schema is per-class, not per-instance)
so dashboards render the same form for the variant.  Inline `_links` use the `ref_key` for `self`/
`edit` (per-instance CRUD URL) and the canonical `class_key` for
`describedby` (schema lookup).

`list_refs_at_scope()` on `ConfigsProtocol` enumerates `{ref_key: class_key}`
without paying the JSON-deserialise cost — useful for quick "what
multi-instance refs exist at this scope" checks.

### PATCH body discriminator

PATCH dispatches by body-key shape:

| Body key | Treatment |
|---|---|
| Matches a registered `class_key` | Class-keyed path: existing single-instance `set_config` / `delete_config` |
| Anything else | Multi-instance ref: requires `class_key` (or `driver_class` alias) inside the body to resolve the dispatch class; routes to `set_config_by_ref` / `delete_config_by_ref` |

```http
PATCH /configs/catalogs/{cat}/collections/{coll}/configs HTTP/1.1
Content-Type: application/merge-patch+json

{
  "tiles_secondary": {
    "class_key": "tiles_config",
    "min_zoom": 5,
    "max_zoom": 15
  },
  "tiles_legacy": null
}
```

The `class_key` (or `driver_class`) discriminator is stripped from the
merged payload before Pydantic validation — it does NOT need to be
declared on the model.  Existing rows at the same `ref_key` get
merge-patched (RFC 7396 — dict values merge, `null` deletes).  Fresh
refs without a discriminator raise a 400 with an actionable message
pointing operators at the discriminator field.

### Idempotency + class-mismatch guards

* `delete_config_by_ref` returns `True` when a row was removed, `False`
  for an idempotent retry — operators can distinguish 204 vs 404
  semantically.
* `set_config_by_ref` rejects an attempt to overwrite an existing row
  with a different class (the row's stored `class_key` differs from the
  body discriminator) — operators must `delete` the ref first or pick a
  different name.
* Engine-class compatibility is enforced at the driver-config validator:
  a driver's `engine_ref` resolving via the engine registry to an
  incompatible `engine_class` is rejected with a clear ValueError; refs
  unknown to the registry are accepted (deferred to PATCH-handler /
  runtime existence check via `EngineInstanceCache.get`).

## See also

- `docs/components/storage_drivers.md` — driver classes, routing, hint
  dispatch
- `docs/components/platform_engines.md` — engines layer, lifecycle,
  the `EngineInstanceCache` contract
- `docs/components/items_schema.md` — the `items_schema` config field
  surface (top-level fields, the `FieldDefinition` object, access intent,
  constraints, validate-time guards)
- `docs/components/field-types.md` — the `data_type` / `subtype`
  vocabulary referenced by `items_schema.fields`
- `docs/components/sidecar_configs.md` — sidecar configurations on PG-
  backed items drivers (full default field surface)
- `notebook_showcase/notebooks/` — worked scenarios exercising this
  surface end-to-end (sidecars + dual-search routing, schema
  enforcement, private ES, asset config round-trip)
- `src/dynastore/extensions/configs/config_api_service.py` — composer
  source (variable-length address walker, scope filter, HATEOAS link
  builder)
- `src/dynastore/extensions/configs/config_api_dto.py` — response DTOs
  (`PlatformConfigResponse`, `CatalogConfigResponse`,
  `CollectionConfigResponse`, `DriverRef`, `Link`)
