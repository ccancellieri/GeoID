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

**Foundations**
- [Getting Started](docs/getting-started.md)
- [Architecture Overview](docs/architecture/overview.md)
- [The Database Layer](docs/architecture/database.md)
- [The Query Executor Pattern](docs/architecture/query_executor.md)
- [Distributed Tasks](docs/architecture/distributed-tasks.md)

**Extensions**
- [Catalog Module](docs/components/catalog.md)
- [Asynchronous Task Ecosystem](docs/components/tasks.md)
- [OGC API – Features](docs/components/features.md)
- [STAC API](docs/components/stac.md)
- [OGC API – Coverages](docs/components/coverages.md)
- [OGC API – Tiles](docs/components/tiles.md)
- [OGC API – Maps](docs/components/maps.md)
- [OGC API – Records](docs/components/records.md)
- [OGC API – EDR](docs/components/edr.md)
- [OGC API – DGGS](docs/components/dggs.md)
- [OGC API – Styles](docs/components/styles.md)
- [3D GeoVolumes](docs/components/volumes.md)
- [OGC API – Joins](docs/components/joins.md)
- [Moving Features](docs/components/moving_features.md)
- [Elasticsearch Integration](docs/components/elasticsearch.md)
- [Legacy WFS](docs/components/wfs.md)
- [GCP Extension](docs/components/gcp.md)

**Extending & contributing**
- [Contributing & Plugin Naming Convention](docs/contributing.md)
- [Example Project Template](examples/my-project/)
- [Roadmap](docs/roadmap.md)

**Testing**
- [Coverage Report](docs/testing/coverage-report.md)
