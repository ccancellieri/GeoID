# Tenant Storage Workspace — design discussion and roadmap

**Status:** Proposed (forward-looking design discussion; not yet implemented)
**Scope:** how DynaStore locates per-tenant storage today, and how that should
generalise into a driver-agnostic *tenant storage workspace* as the platform
moves toward multi-driver, multi-tenant storage (e.g. disseminated Iceberg
tenant catalogs) where plugins write transparently through any storage driver.

This document captures the architecture, the invariants that constrain it, and a
phased migration. It deliberately stays at the level of the public platform
contract — it does not describe any particular deployment.

---

## 1. Where we are today

A DynaStore Catalog is a tenant. Each Catalog is backed by an isolated
PostgreSQL **schema**, generated at provisioning time and recorded in the
control-plane registry:

```sql
-- catalog.catalogs (global registry, one row per Catalog)
id                  VARCHAR PRIMARY KEY
physical_schema     VARCHAR NOT NULL UNIQUE   -- the per-tenant PG schema
provisioning_status VARCHAR NOT NULL
deleted_at          TIMESTAMPTZ
...
```

Everything tenant-scoped lives inside `"{physical_schema}"`:

- core tenant tables — `collections`, `roles`, `role_hierarchy`, `grants`,
  `catalog_configs`, `collection_configs`;
- per-collection item tables — machine-named `physical_table` values created by
  the PostgreSQL items driver;
- plugin-owned tenant tables — created by lifecycle initialisers (for example a
  notebooks store, or proxy URL mappings).

`physical_schema` is therefore the **bootstrap key** of the entire per-tenant
world: you must know it before you can read any tenant-scoped table, including
the per-tenant config tables.

### Resolution

`CatalogsProtocol.resolve_physical_schema(catalog_id) -> str` is the single seam
every consumer uses. The PostgreSQL items driver, the assets/STAC/records
search and aggregation paths, IAM authorization, the admin provisioning view,
logs, processes, proxy, stats, and tenant-table initialisers all resolve the
schema through it. It is on a hot path: a single request can resolve it several
times.

### Two related-but-different identifiers

There are two physical-storage identifiers, and they are owned at different
tiers — keeping them straight matters for the rest of this document:

| Identifier        | Scope         | Owner / source of truth                         | On the public model? |
| ----------------- | ------------- | ----------------------------------------------- | -------------------- |
| `physical_schema` | per-Catalog   | `catalog.catalogs` registry column              | No                   |
| `physical_table`  | per-Collection| PostgreSQL items driver config (machine-assigned, validated — see #1135/#1137) | No |

`physical_table` is already a clean, driver-owned concern: it is stored in the
items driver's own config, resolved by the driver, and never carried on the
platform `Collection` model. `physical_schema` is the one that, historically,
leaked upward.

---

## 2. The cleanup that motivated this document

`physical_schema` used to be a field on the shared `BaseMetadata` model (parent
of `Catalog` and `Collection`), marked `exclude=True` so it would not appear in
API output. Two problems followed from that:

1. **It put a storage-driver detail on the public metadata model.** A PG schema
   name is not catalog metadata; it is provisioning state. External output was
   already protected independently by `get_internal_columns()` at the STAC and
   localisation boundaries, so the field's only *effect* on the model was to make
   the model the de-facto carrier of a storage detail.

2. **It did not survive the distributed cache.** Catalog models are cached and
   shared across processes via a tiered L1 (in-process) + L2 (Valkey) cache. The
   L2 encoder serialises Pydantic models with `model_dump_json()`, which honours
   `exclude=True` — so the field was silently dropped on every L2 round-trip. A
   process that read a Catalog *cold from L2* (a sibling service that did not
   create the catalog) saw `physical_schema = None` and failed schema
   resolution, even though the registry column was correctly populated. The
   persistent store was always right; the cache quietly lost a field.

The fix established two rules that this roadmap builds on:

- **The cache is a transient accelerator, never a source of truth.** Any cold
  miss (L1 *and* L2) must fall back to the persistent store and resolve
  correctly. Values that must survive the cache are cached as plain, lossless
  representations (a string), not as models with hidden serialisation rules.
- **Storage-location details do not ride on platform metadata models.** They are
  resolved from the authoritative store through a narrow protocol seam.

Concretely: `physical_schema` was removed from `BaseMetadata`;
`resolve_physical_schema` now reads a plain string straight from
`catalog.catalogs` behind a lossless string cache; and the registry column is
the source of truth.

---

## 3. Invariants the design must preserve

Any future generalisation must keep all of these:

1. **Bootstrap-first.** The per-tenant storage location must be resolvable from a
   *global* location, before any tenant-scoped table is touched.
2. **Driver-agnostic at the seam.** Consumers ask "where does this tenant's data
   live?" without knowing the backend. The PostgreSQL schema is one *answer*, not
   the question.
3. **Authoritative persistence; transient cache.** The location lives in durable
   storage; caches only accelerate and may be dropped at any time.
4. **Tenant isolation is non-negotiable.** The location is system-assigned at
   provisioning; it is never caller-settable (a caller-settable schema/namespace
   would let one tenant address another's data — the reason #1135 kept it out of
   per-collection config).
5. **Cheap on the hot path.** Resolution happens on nearly every read/write;
   it must stay a single indexed lookup or a lossless cache hit.

### Why the location cannot live in a config

It is tempting to move `physical_schema` into "the driver config used to persist
the catalog". That breaks invariant 1. DynaStore's config store has three tiers:

```
configs.platform_configs      — global, platform-tier config classes
"{tenant}".catalog_configs     — per-tenant catalog-tier config
"{tenant}".collection_configs  — per-tenant collection-tier config
```

The catalog- and collection-tier config tables live **inside the tenant
schema**. Reading any of them already requires `physical_schema` first
(`resolve_physical_schema` is called *before* the config lookup, to locate the
table). Storing the schema in a per-tenant config is therefore circular: you'd
need the schema to read the config that tells you the schema.

The genuinely-global `configs.platform_configs` table is bootstrap-safe, but it
is the wrong shape: it stores schema-validated config *classes* keyed by
`ref_key`/`class_key`, with no per-Catalog dimension. `physical_schema` is a
per-Catalog provisioning *fact*, not a config class. Encoding it there would just
reinvent a per-Catalog registry inside the config store, with more indirection
and cost. The per-Catalog registry already exists — `catalog.catalogs`.

The useful distinction this surfaces, and that the target model leans on:

- **Which driver** persists a tenant (default: PostgreSQL) is a *policy* — it
  belongs in global/platform config (or routing config).
- **The concrete location** minted for a given tenant at provisioning
  (`physical_schema = …`, or an Iceberg namespace, or an object-store prefix) is
  a per-Catalog *fact* — it belongs in a global, per-Catalog registry, resolved
  before the tenant world opens.

---

## 4. Future hypothesis: multi-driver tenant storage

The roadmap hypothesis is a move beyond "every tenant is a PG schema" toward a
**tenant storage workspace** that can be realised by different backends, with
plugins writing through storage drivers transparently:

- a tenant could be a **PostgreSQL schema** (today's behaviour);
- or a **disseminated Iceberg tenant catalog** (e.g. a per-tenant namespace in a
  shared lakehouse), with plugins writing tables/partitions through an Iceberg
  driver;
- or an **object-store prefix** for file-based drivers (DuckDB/Parquet);
- potentially a mix, per data class, behind one tenant identity.

Today only PostgreSQL has a per-tenant *isolation* primitive. The other drivers
model storage location **per collection config** (DuckDB `path`, Iceberg
`namespace`/`table_name`), not per tenant, and there is **no cross-driver
abstraction** for "where does this tenant live". That absence is exactly why the
generalisation should be introduced when a second tenant-isolating backend is
actually built — not speculatively against a single implementation.

### Target model: `TenantWorkspace`

Introduce a small, driver-typed descriptor that names a tenant's storage home,
plus a resolver protocol:

```text
TenantWorkspace            # value object, per Catalog
  driver: str              # "postgresql" | "iceberg" | "duckdb" | ...
  location: <descriptor>   # driver-typed:
                           #   postgresql -> { schema }
                           #   iceberg    -> { catalog, namespace }
                           #   duckdb     -> { prefix }

WorkspaceResolver (protocol)
  resolve(catalog_id) -> TenantWorkspace        # global, bootstrap-safe
```

- The **driver default** comes from platform/routing config (policy).
- The **concrete location** is minted at provisioning and persisted in a global,
  per-Catalog registry — the natural evolution of `catalog.catalogs`:

  ```sql
  -- tenant_workspaces (global; supersedes the bare physical_schema column)
  catalog_id          VARCHAR PRIMARY KEY
  driver              VARCHAR NOT NULL          -- backend kind
  location            JSONB   NOT NULL          -- driver-typed descriptor
  provisioning_status VARCHAR NOT NULL
  ```

- Each storage driver maps a `TenantWorkspace` to its native location: the PG
  driver reads `location.schema`; an Iceberg driver reads
  `location.namespace`; etc.
- `resolve_physical_schema` becomes a thin PG-specific accessor over the
  resolver (`resolve(catalog_id).location.schema`), or is retired in favour of
  driver code consuming the workspace directly.

Plugins that create per-tenant tables stop hard-coding a PG schema and instead
ask the workspace for a *table handle* in the tenant's space, which the active
driver fulfils in its own dialect.

---

## 5. Migration phases

The seam (`resolve_physical_schema -> str`) is stable today, which makes the
migration incremental and non-breaking.

- **Phase 0 — done.** `physical_schema` off the platform model; resolution is a
  lossless string from the `catalog.catalogs` registry; cache is a pure
  accelerator. This is the precondition for everything below.

- **Phase 1 — value object, no storage change.** Introduce `TenantWorkspace` and
  a `WorkspaceResolver` whose only implementation wraps the existing
  `physical_schema` column (`driver="postgresql"`, `location={schema}`).
  `resolve_physical_schema` delegates to it. No schema migration; pure
  refactor that gives consumers a driver-agnostic shape to target.

- **Phase 2 — registry generalisation.** Add the `tenant_workspaces` table
  (global, per-Catalog, driver-typed `location`); backfill from
  `catalog.catalogs.physical_schema`; provisioning writes the descriptor
  atomically; the driver *default* is resolved from platform config. Keep
  `physical_schema` as a generated/derived view for compatibility during the
  cutover, then retire it. Requires a coordinated DB migration.

- **Phase 3 — second backend.** Implement a non-PG tenant-isolating workspace
  (e.g. Iceberg tenant catalog). Express plugin per-tenant table creation in
  terms of the workspace abstraction so the same plugin code writes through PG or
  Iceberg. Validate the abstraction against two real backends before declaring
  it stable.

Phases 2 and 3 are gated on real demand for a second backend and on a
coordinated storage migration; they are intentionally not committed to a
milestone here.

---

## 6. Cross-cutting concerns

These are the things that make this a large, careful task rather than a rename —
each must be addressed before Phase 3:

- **Plugin blast radius.** Every plugin that creates or queries per-tenant tables
  consumes the schema today. They must move to the workspace seam. The driver-
  agnostic `WorkspaceResolver` introduced in Phase 1 is what lets them migrate
  one at a time without a flag day.
- **Transactional semantics differ across backends.** PostgreSQL gives
  transactional DDL within the tenant schema; an Iceberg/object-store workspace
  has different atomicity and commit semantics for "create a tenant table".
  Lifecycle initialisers that today assume PG transactional creation need a
  backend-aware contract.
- **Identity & isolation.** The workspace is system-assigned and must remain so;
  the resolver must never accept a caller-supplied location. SQL-identifier
  validation that guards the PG schema/table names generalises to descriptor
  validation per driver.
- **Performance.** Resolution stays on the hot path. The workspace descriptor is
  small and, like the schema string today, must round-trip the distributed cache
  losslessly (plain JSON/string, no models with hidden serialisation rules) and
  fall back to a single indexed registry lookup on a cold miss. Routing
  resolution through the per-tenant config layer would add a table read and a
  model validation to the hottest path and reintroduce the bootstrap circularity
  — it is explicitly out of scope.
- **Observability.** Resolution failures must name the failing tenant and the
  backend, distinct from routing/driver-selection failures (the same diagnostic
  separation applied to driver-resolution errors in #1136).

---

## 7. Decision today / non-goals

- **Today:** keep `catalog.catalogs.physical_schema` as the per-Catalog source of
  truth and the bootstrap key; resolve it as a lossless string; keep the platform
  models free of storage details. This is correct, fast, and forward-compatible.
- **Not now:** moving the location into a (per-tenant) config — circular; or into
  `platform_configs` — wrong shape. Neither fixes a bug and both add cost.
- **Not now:** a cross-driver workspace abstraction designed against a single
  backend. Introduce the value object (Phase 1) when it earns its keep, and the
  registry/second backend (Phases 2–3) when a real second tenant-isolating
  driver is built.

The registry column is not the detail to eliminate — it is the correct home for
the single global key from which the entire per-tenant (and future multi-driver)
world bootstraps. What was wrong was carrying that key on the public model and
resolving it through a lossy cache.

---

## 8. References

- `catalog.catalogs` registry and `resolve_physical_schema` — `modules/catalog/`
- config-store tiers (`platform_configs`, per-tenant `catalog_configs` /
  `collection_configs`) — `modules/db_config/typed_store/`
- machine-assigned, validated `physical_table` — PRs #1135, #1137
- materialisation freeze of shape/identity fields — PR #1079
- driver-resolution vs routing-failure diagnostics — PR #1136
- routing/transformer registry — PR #990
