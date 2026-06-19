# Platform Engines

Platform-tier connection-and-pool resources that drivers reference via
`engine_ref`.  Engines decouple **how to connect** (pool sizing,
credentials, lifecycle) from **what to do with the connection** (driver
sidecars, indexing rules, write policy).  Tenants compose drivers; only
sysadmins provision engines.

## Why engines are separate from drivers

Previously, every driver class hard-coded its own connection lifecycle.
Two `ItemsPostgresqlDriver`s on the same physical Postgres meant two
asyncpg pools.  Hot/cold tiering, read/write specialisation, and cross-
class compatibility (e.g. private + public ES drivers sharing a cluster)
either duplicated state or required bespoke wiring.

The engines layer splits the contract:

| Concern | Owner | Tenant editable? |
|---|---|---|
| Connection details (DSN, API keys, pool size, TTL) | `EngineConfig` at `platform.protocols.storage.*` | **No** — sysadmin only |
| Driver class + sidecars + index settings | `DriverPluginConfig` at `platform.catalog.[…].drivers.*` | Yes |
| Routing (which driver_ref serves which operation) | `*RoutingConfig` | Yes (per scope) |

The strict separation means tenant-side configuration cannot influence
platform resource policy.  An operator-disabled engine takes every
referencing driver out of service together; a tenant adjusting sidecars
never touches connection pooling.

## Engine kinds

Four concrete `EngineConfig` subclasses are available.  Each declares
an `engine_class` discriminator that drivers match against their
`required_engine_class: ClassVar[str]`.

| Class | `engine_class` | Driver-side fields |
|---|---|---|
| `PostgresqlEngineConfig` | `postgresql_engine` | `connection_url: Optional[Secret]`, `pool_size: int = 10`, `pool_timeout_sec: int = 30` |
| `ElasticsearchEngineConfig` | `elasticsearch_engine` | `cluster_url: Optional[Secret]`, `api_key: Optional[Secret]`, `request_timeout_sec: int = 30` |
| `DuckdbEngineConfig` | `duckdb_engine` | `pool_size: int = 4`, `max_memory_gb: int = 4`, `threads: int = 4` |
| `IcebergEngineConfig` | `iceberg_engine` | `catalog_uri: Optional[Secret]`, `warehouse_uri: Optional[Secret]`, `catalog_properties: Dict[str, Secret]` |

All connection-detail fields are `Secret`-typed: encrypted at rest via
the standard `dynastore.tools.secrets` infra (Fernet AES-128-CBC + HMAC),
masked in API responses, revealed only at the connection-construction
boundary.

## Engine ↔ driver compatibility

A driver's `required_engine_class` must match the referenced engine's
`engine_class`.  Cross-class refs are rejected at config-validation
time with a 422 error.

| Driver class | Required engine |
|---|---|
| `ItemsPostgresqlDriverConfig` | `postgresql_engine` |
| `CatalogPostgresqlDriverConfig` | `postgresql_engine` |
| `CollectionPostgresqlDriverConfig` | `postgresql_engine` |
| `AssetPostgresqlDriverConfig` | `postgresql_engine` |
| `CatalogStacPostgresqlDriverConfig` | `postgresql_engine` |
| `CollectionStacPostgresqlDriverConfig` | `postgresql_engine` |
| `CatalogCorePostgresqlDriverConfig` | `postgresql_engine` |
| `CollectionCorePostgresqlDriverConfig` | `postgresql_engine` |
| `ItemsElasticsearchDriverConfig` | `elasticsearch_engine` |
| `ItemsElasticsearchPrivateDriverConfig` | `elasticsearch_engine` |
| `CatalogElasticsearchDriverConfig` | `elasticsearch_engine` |
| `CollectionElasticsearchDriverConfig` | `elasticsearch_engine` |
| `AssetElasticsearchDriverConfig` | `elasticsearch_engine` |
| `ItemsDuckdbDriverConfig` | `duckdb_engine` |
| `ItemsIcebergDriverConfig` | `iceberg_engine` |

## Lifecycle policy (`EngineLifecycleConfig`)

Every engine carries a lifecycle block that controls how its runtime
instance (asyncpg pool, ES client, DuckDB connection, Iceberg catalog)
is created, retained, and evicted.

```python
class EngineLifecycleConfig(BaseModel):
    policy: Literal["global", "ttl_lru"] = "global"
    ttl_seconds: Optional[int] = None       # required when policy="ttl_lru"
    max_parallel: Optional[int] = None      # soft cap; warning on exceed
    immutable: bool = True                  # IAM-locked once provisioned
```

| `policy` | When right | Eviction |
|---|---|---|
| `global` | Cheap connection clients (PG pool, ES client) — keeping warm has no downside | Never |
| `ttl_lru` | Heavy local state (DuckDB process, Iceberg catalog cache) | Idle eviction after `ttl_seconds`; `engine_release()` runs |

`max_parallel` is a soft cap: exceeding emits a structured-log warning
rather than queueing or rejecting.  Operators can promote the warning
to an error via log-aggregation rules.

`immutable=True` (default) means the engine's connection-detail fields
cannot be reconfigured via the standard PATCH path once provisioned.
For maintenance windows, flip `enabled=False` on the engine itself —
drivers referencing it then return `503 Service Unavailable` at first
dispatch.

## Engine instance cache

`EngineInstanceCache` lazily instantiates engines on demand.  It's
wired into `DBConfigModule.lifespan` and exposed on
`app_state.engine_cache`.

```
First cache.get(engine_ref):
    1. Resolver looks up engine_ref → EngineConfig
    2. cache calls await engine.engine_init() → runtime instance
    3. instance is cached; subsequent get() calls return it directly

Background sweep (every 60s):
    For each ttl_lru entry whose last_accessed > ttl_seconds ago:
        await engine.engine_release(instance)
        drop from cache
```

The cache snapshots platform engine configs at boot — see
`engine_resolver.build_engine_snapshot`.  Operator changes via PATCH
land in the configs store but do NOT reach the cache until process
restart (a live-reload API is planned).  Treat `enabled=false` PATCH
as advisory until live-reload ships.

## `EngineInstanceProtocol` contract

Engine config classes structurally implement two coroutines:

```python
async def engine_init(self) -> Any:
    """Construct and return a runtime instance.  Called on first
    cache.get() for the engine's ref (under policy='global') or after
    every TTL eviction (under policy='ttl_lru').
    """

async def engine_release(self, instance: Any) -> None:
    """Tear down the runtime instance.  Called on TTL eviction and on
    cache shutdown.  Idempotent.
    """
```

Each shipped engine config implements these against its native runtime:

| Engine | `engine_init` builds | `engine_release` |
|---|---|---|
| Postgresql | dedicated `asyncpg.Pool` from `connection_url` Secret or `DBConfig.database_url` fallback | `await pool.close()` |
| Elasticsearch | env-driven shared `AsyncOpenSearch` (no override) OR isolated client from `cluster_url` + `api_key` | no-op for shared; `close()` for isolated |
| Duckdb | in-memory connection with `threads` / `memory_limit` PRAGMAs | sync `close()` |
| Iceberg | `pyiceberg.load_catalog` with revealed Secrets in properties | no-op (catalog is GC-driven) |

## Operator workflows

### Provision a new engine

Engines live at `configs.platform.protocols.storage.{class_key}`.
PATCH the class-keyed slot with the engine's connection details:

```bash
curl -X PATCH /configs/plugins/postgresql_engine_config \
    -H 'Authorization: Bearer <sysadmin-token>' \
    -H 'Content-Type: application/merge-patch+json' \
    -d '{
        "connection_url": {"__secret__": "<fernet-token>"},
        "pool_size": 20,
        "pool_timeout_sec": 60,
        "lifecycle": {"policy": "global"}
    }'
```

The default deployment has one ref per engine kind, keyed by the
snake_case class name (`postgresql_engine_config`,
`elasticsearch_engine_config`, etc.).  Future releases will add
operator-chosen ref names (e.g. `pg_main`, `pg_secondary`) for
multi-instance deployments.

### Maintenance window without destructive delete

Flip `enabled=False`:

```bash
curl -X PATCH /configs/plugins/postgresql_engine_config \
    -H 'Authorization: Bearer <sysadmin-token>' \
    -H 'Content-Type: application/merge-patch+json' \
    -d '{"enabled": false}'
```

After process restart (the cache snapshots at boot), every driver
referencing this engine returns `503 Service Unavailable` with a
structured log entry until `enabled` is flipped back.

### Tenant-scope engine PATCH is forbidden

Engines are platform-only.  An attempt to PATCH
`/configs/catalogs/{cat}/plugins/postgresql_engine_config` returns
`403 Forbidden` with the structured problem
`engine_write_forbidden_at_tenant_scope`.

## Forward roadmap

Planned extensions to the engines layer include:

- **Multi-instance refs** — operator-chosen `engine_ref` names
  (e.g. `pg_main`, `pg_cold`) keyed in `platform_configs` alongside
  the class_key.
- **Live-reload** — runtime PATCH on `enabled` / connection fields
  refreshes the cache resolver without process restart.
- **Driver dispatch through the cache** — per-driver-instance
  resolution of `engine_ref → EngineInstanceCache.get()` at
  request time.

## See also

- `docs/components/storage_drivers.md` — driver classes, routing, hints
- `src/dynastore/modules/db_config/engine_config.py` — engine class
  definitions
- `src/dynastore/modules/db_config/engine_instance_cache.py` —
  `EngineInstanceProtocol` + cache mechanics
- `src/dynastore/modules/db_config/engine_resolver.py` — boot-time
  snapshot resolver
- `docs/components/sidecar_configs.md` — sidecar configurations
  composed onto PG-backed items drivers (geometries, attributes,
  item_metadata, stac_metadata)
- `docs/components/configs_api.md` — runtime configs API surface;
  GET/PATCH endpoints, query params, HATEOAS link catalog, scope
  strictness rules
