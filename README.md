# Agro-Informatics Platform — Catalog Services

OGC-native, multi-tenant geospatial catalog platform. Manage trillions of features across isolated tenants and expose them through every major OGC API standard.

## Why it exists

Traditional catalog systems trade interoperability for tenancy or scale for flexibility. AIP collapses the trade-off: each catalog you create maps to a physically isolated PostgreSQL schema (zero data leakage, rename without data movement), and every catalog is exposed through STAC, OGC API – Features, Coverages, Tiles, Maps, Processes, Records, EDR, and DGGS without writing extension code per format.

## Try it

```bash
docker compose -f packages/core/src/dynastore/docker/dev.compose.yml up -d
curl http://localhost/stac/catalogs
open http://localhost/web/
```

The running service declares OGC conformance live at `/stac/conformance`, `/features/conformance`, `/processes/conformance`, `/records/conformance`, `/coverages/conformance`, `/dggs/conformance`, `/consys/conformance`, `/movingfeatures/conformance`, and the maps service at `/{tiles,maps,styles}/conformance`.

## Architecture in one paragraph

Three pillars. **Modules** are backend-agnostic libraries — one module owns one table, no HTTP. **Extensions** are stateless HTTP adapters that translate requests into module calls — adding a new OGC API standard is adding an extension, not refactoring the core. **Tasks** are isolated background workers exposed through OGC API – Processes — ingestion, indexing, and analysis run in their own containers.

The catalog → schema → partition mapping is lazy: a `code` like `"agriculture"` resolves to an immutable schema like `"s_a1b2c3"`, partitions are created just-in-time by database triggers on first insert, and renaming a logical code never moves a byte of data.

## Documentation

Documentation is layered: this README is the entry point, [`docs/`](docs/) holds
high-level concept and reference pages, and each module/extension keeps a
detailed `README.md` next to its code. The full navigable map — every topic
linked to its concept page, its code-adjacent README, and its source directory —
lives in **[`docs/index.md`](docs/index.md)**.

### Start here
- [Getting Started](docs/getting-started.md) — run it locally and make a first request
- [Documentation Index](docs/index.md) — the complete map of all docs
- [Platform Manual](docs/platform_manual.md) — operator-facing tour

### Foundations
- [Architecture Overview](docs/architecture/overview.md) — the three-pillar design
- [The Database Layer](docs/architecture/database.md) · [Migrations](docs/architecture/migrations.md)
- [The Query Executor Pattern](docs/architecture/query_executor.md)
- [Protocols & Discovery](docs/architecture/protocols.md) · [Configuration](docs/architecture/configuration.md) · [Caching](docs/architecture/caching.md)
- [Distributed Tasks](docs/architecture/distributed-tasks.md)
- [Collection Lifecycle](docs/architecture/collection-lifecycle.md) · [Collection Metadata](docs/collection-metadata-architecture.md)

### OGC API & STAC surface
- [OGC API – Features](docs/components/features.md) · [STAC API](docs/components/stac.md) · [Records](docs/components/records.md)
- [Coverages](docs/components/coverages.md) · [EDR](docs/components/edr.md) · [DGGS](docs/components/dggs.md)
- [Tiles](docs/components/tiles.md) · [Maps](docs/components/maps.md) · [Styles](docs/components/styles.md)
- [3D GeoVolumes](docs/components/volumes.md) · [Joins](docs/components/joins.md) · [Moving Features](docs/components/moving_features.md) · [Connected Systems](docs/components/connected_systems.md)
- [Legacy WFS](docs/components/wfs.md) · [OGC extension map](docs/components/ogc-extensions.md)

### Data, storage & search
- [Catalog Module](docs/components/catalog.md) · [Items Schema](docs/components/items_schema.md) · [Field Types](docs/components/field-types.md)
- [Storage Drivers](docs/components/storage_drivers.md) · [Platform Engines](docs/components/platform_engines.md) · [Sidecar Configs](docs/components/sidecar_configs.md) · [Schema Evolution](docs/components/schema_evolution.md)
- [Elasticsearch Integration](docs/components/elasticsearch.md) · [STAC Harvest](docs/components/stac-harvest.md)
- [Asynchronous Task Ecosystem](docs/components/tasks.md) · [Processes](docs/components/processes.md) · [Events](docs/components/events.md)

### Access, admin & web
- [Authentication](docs/authentication.md) · [Auth & Policy Engine](docs/components/auth.md) · [Admin API](docs/components/admin.md)
- [Configs API](docs/components/configs_api.md) · [Web UI](docs/components/web.md) · [GCP Extension](docs/components/gcp.md)

### Operations
- [Catalog Lifecycle Readiness & Recovery](docs/operations/catalog-lifecycle-readiness-and-recovery.md)
- [Asset Upload Smoke Test](docs/operations/asset-upload-smoke.md) · [Rate Limits & Quotas](docs/operations/rate-limit-and-quotas.md)

### Extending & contributing
- [Contributing & Plugin Naming Convention](docs/contributing.md)
- [Example Project Template](examples/my-project/) · [GeoParquet → DuckDB → OpenSearch walkthrough](examples/geoparquet-duckdb-opensearch/)
- [Roadmap](docs/roadmap.md)

### Standards research (proposals)
- [STAC Asset Transactions Extension](docs/proposals/asset-transactions-extension.md)
- [STAC Datacube Scalable Dimensions](docs/proposals/stac-datacube-scalable-dimensions.md)

### Testing
- [Local Development](docs/testing/local-development.md) · [Coverage Report](docs/testing/coverage-report.md)
