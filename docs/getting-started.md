# Getting Started

Quickstart for engineers and integrators evaluating the Agro-Informatics Platform's catalog services.

## What it is

A multi-tenant, OGC-compliant geospatial catalog service. Each catalog you create gets a physically isolated PostgreSQL schema. The platform exposes the catalog through every major OGC API standard so existing GIS clients (QGIS, ArcGIS Pro, web map libraries) consume it without bespoke connectors.

## What it ships

The running service publishes conformance declarations for:

- **STAC API 1.0.0** — `/stac/conformance`
- **OGC API – Features** (Parts 1-4) — `/features/conformance`
- **OGC API – Processes** — `/processes/conformance`
- **OGC API – Records** — `/records/conformance`
- **OGC API – Coverages** — `/coverages/conformance`
- **OGC API – DGGS** — `/dggs/conformance`
- **OGC API – Connected Systems** — `/consys/conformance`
- **OGC API – Moving Features** — `/movingfeatures/conformance`
- **OGC API – Tiles, Maps, Styles** — exposed by the maps service

Plus a custom OGC Dimensions extension (research) for paginated datacube dimensions.

## Try it locally

1. Bring up the dev stack:

   ```bash
   docker compose -f src/dynastore/docker/dev.compose.yml up -d
   ```

2. Discover catalogs:

   ```bash
   curl http://localhost/stac/catalogs
   ```

3. Open the SPA at `http://localhost/web/`. Sign in with the seeded admin account (your local `.env` has the credentials — never commit them).

4. Open the docs at `http://localhost/web/pages/docs`.

## Three Pillars in one paragraph

Modules are backend-agnostic libraries — one module owns one table, no HTTP. Extensions are stateless HTTP adapters that translate requests into module calls — adding a new OGC API standard is adding an extension, not refactoring the core. Tasks are isolated background workers exposed through OGC API – Processes — ingestion, indexing, and analysis run in their own containers and never block customer-facing APIs.

## Logical → Physical mapping

A catalog `code` like `"agriculture"` resolves to an immutable schema like `"s_a1b2c3"`. Collections become partitioned tables; partitions are created just-in-time by database triggers on first insert. Renaming a logical code never moves a byte of data.

## Next steps

- **Architecture** — start with [Architecture Overview](architecture/overview.md), then [The Database Layer](architecture/database.md). The "logical → physical" mapping is the most important concept in the platform.
- **Pick an extension** — read the doc for the OGC standard you care about under `docs/components/`.
- **Contributing** — see [Contributing](contributing.md) for the plugin naming convention and how to add a new extension.

## Where to ask

- File issues at the repository tracker.
- For OGC standards questions, see the [OGC API specs](https://ogcapi.ogc.org/).
