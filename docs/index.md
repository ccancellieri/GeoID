# Documentation Index

This is the navigable map of all GeoID / DynaStore documentation. Use it to jump
to the page that matches your task, and to understand how the docs are layered so
you read only what's relevant.

## How the docs are layered

```
Layer 1 — ENTRY        README.md (root)  +  this index
Layer 2 — HIGH-LEVEL   docs/architecture/* and docs/components/*  — concept + why, links down
Layer 3 — CODE-ADJACENT packages/.../<module|extension>/README.md — the how, lives next to the code
```

Rule of thumb: **`docs/` explains the concept and the why; the in-tree README
explains the implementation.** When a topic has a Layer-3 README, the Layer-2
page summarizes and links down to it instead of duplicating detail. When you
change behavior, update the Layer-3 README first; touch the Layer-2 page only if
the concept itself changed.

## Routing table

| Topic | Layer 2 — concept page | Layer 3 — code-adjacent README | Source |
|---|---|---|---|
| Architecture overview | [overview](architecture/overview.md) | — | `packages/core/src/dynastore/` |
| Database layer | [database](architecture/database.md), [migrations](architecture/migrations.md) | — | `modules/db_config/` |
| Query executor | [query_executor](architecture/query_executor.md) | — | `tools/` |
| Protocols & discovery | [protocols](architecture/protocols.md) | — | `models/protocols/` |
| Configuration | [configuration](architecture/configuration.md) | — | `models/plugin_config/` |
| Caching | [caching](architecture/caching.md) | — | `modules/` (cache) |
| Distributed tasks | [distributed-tasks](architecture/distributed-tasks.md) | [task system](../packages/core/src/dynastore/models/README.md), [tasks module](../packages/core/src/dynastore/modules/tasks/README.md) | `modules/tasks/` |
| Async task ecosystem | [tasks](components/tasks.md) | [tasks module](../packages/core/src/dynastore/modules/tasks/README.md) | `modules/tasks/` |
| Processes (OGC) | [processes](components/processes.md) | [processes module](../packages/core/src/dynastore/modules/processes/README.md) | `modules/processes/` |
| Storage drivers | [storage_drivers](components/storage_drivers.md), [platform_engines](components/platform_engines.md), [sidecar_configs](components/sidecar_configs.md), [schema_evolution](components/schema_evolution.md) | [storage module](../packages/core/src/dynastore/modules/storage/README.md) | `modules/storage/` |
| Elasticsearch / search | [elasticsearch](components/elasticsearch.md) | [elasticsearch module](../packages/core/src/dynastore/modules/elasticsearch/README.md), [bulk indexer task](../packages/core/src/dynastore/tasks/elasticsearch_indexer/README.md) | `modules/elasticsearch/` |
| STAC harvest | [stac-harvest](components/stac-harvest.md) | — | `tasks/` |
| Catalog & collections | [catalog](components/catalog.md), [collection-lifecycle](architecture/collection-lifecycle.md), [collection-metadata](collection-metadata-architecture.md) | — | `modules/catalog/` |
| Items schema & field types | [items_schema](components/items_schema.md), [field-types](components/field-types.md) | — | `modules/catalog/` |
| Auth & identity | [authentication](authentication.md), [auth & policy](components/auth.md) | [identity providers](../packages/core/src/dynastore/modules/iam/identity_providers/README.md) | `modules/iam/` |
| Admin & configs API | [admin](components/admin.md), [configs_api](components/configs_api.md) | — | `extensions/` |
| Web UI | [web](components/web.md) | — | `extensions/web/` |
| GCP integration | [gcp](components/gcp.md) | — | `modules/gcp/` |
| Events | [events](components/events.md) | — | `modules/` (events) |
| OGC API – Features | [features](components/features.md) | — | `extensions/features/` |
| STAC API | [stac](components/stac.md) | — | `extensions/stac/` |
| OGC API – Records | [records](components/records.md) | — | `extensions/records/` |
| OGC API – Coverages | [coverages](components/coverages.md) | — | `extensions/coverages/` |
| OGC API – EDR | [edr](components/edr.md) | — | `extensions/edr/` |
| OGC API – DGGS | [dggs](components/dggs.md) | — | `extensions/dggs/` |
| OGC API – Tiles | [tiles](components/tiles.md) | [tiles extension](../packages/extensions/tiles/src/dynastore/extensions/tiles/README.md) | `extensions/tiles/` |
| OGC API – Maps / Styles | [maps](components/maps.md), [styles](components/styles.md) | — | `extensions/maps/`, `extensions/styles/` |
| 3D GeoVolumes | [volumes](components/volumes.md) | — | `extensions/volumes/` |
| OGC API – Joins | [joins](components/joins.md) | — | `extensions/joins/` |
| Moving Features | [moving_features](components/moving_features.md) | — | `extensions/moving_features/` |
| Connected Systems | [connected_systems](components/connected_systems.md) | — | `extensions/connected_systems/` |
| Legacy WFS | [wfs](components/wfs.md) | [wfs extension](../packages/extensions/wfs/src/dynastore/extensions/wfs/README.md) | `extensions/wfs/` |
| OGC extension map | [ogc-extensions](components/ogc-extensions.md) | — | `extensions/` |
| Shared tools / utilities | [tools-reference](tools-reference.md) | — | `tools/` |
| Offline web assets | [offline-assets](offline-assets.md) | — | `extensions/web/static/` |

## Operations

- [Catalog Lifecycle Readiness & Recovery](operations/catalog-lifecycle-readiness-and-recovery.md)
- [Asset Upload Smoke Test](operations/asset-upload-smoke.md)
- [Rate Limits & Quotas](operations/rate-limit-and-quotas.md)
- [OGC API Compliance Verification](operations/ogc-api-compliance-verification.md)
- [Startup DDL Peer Races & Deploy Triage](operations/startup-ddl-and-deploy-triage.md)

## Standards research (public proposals)

- [STAC Asset Transactions Extension](proposals/asset-transactions-extension.md)
- [STAC Datacube Scalable Dimensions](proposals/stac-datacube-scalable-dimensions.md)

## Contributing & testing

- [Contributing & Plugin Naming](contributing.md)
- [Local Development](testing/local-development.md) · [Coverage Report](testing/coverage-report.md)
- [Example Project Template](../examples/my-project/) · [GeoParquet → DuckDB → OpenSearch](../examples/geoparquet-duckdb-opensearch/)
- [Roadmap](roadmap.md)

---

> **Maintainers:** keep this index and the layering accurate when adding or moving
> docs. Public docs must not contain internal issue/PR numbers, internal refactor
> codenames, concrete cloud project/bucket/service names, or AI planning artifacts —
> that material belongs in the private deployment repository or local notes.
