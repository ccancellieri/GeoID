# Roadmap

This page reflects engineering work that is in progress or imminent on the
Catalog Services platform. Items already shipped (Tiles, Maps, Styles,
Connected Systems, Moving Features, Coverages, DGGS, Records, EDR, Joins,
3D GeoVolumes) live in `docs/components/` and on the live OGC conformance
matrix on the home page.

## Reliability

- **Outbox-driven multi-driver indexing** — atomic per-tenant outbox table
  + drain task have shipped. Operational hardening (replay tooling, drain
  metrics, alerting) is in progress.
- **Engine instance protocol & cache** — Cycle F.5 / F.6 of the driver-ref
  refactor have shipped. Remaining slices (catalogue + Cycle F.4 c/d/e
  schema-touching cutovers) are planned and require a coordinated DB
  reset on dev / review environments.
- **Identity & access** — IAM seeding race against multi-service boots is
  closed by partition-key-aware policy storage. Remaining items in the
  IAM hardening list (`role_hierarchy` self-heal canary, sibling-service
  cache invalidation under Valkey, principal-id migration) are tracked
  in the issue queue.

## Storage & multi-tenancy

- **Tenant storage workspace** — today each Catalog (tenant) is backed by an
  isolated PostgreSQL schema, recorded in the `catalog.catalogs` registry and
  resolved through a single seam. The roadmap generalises this into a
  driver-agnostic *tenant storage workspace* so a tenant could be realised by a
  PostgreSQL schema, a disseminated Iceberg tenant catalog, or an object-store
  prefix, with plugins writing through storage drivers transparently. The design,
  invariants (bootstrap key, transient cache, tenant isolation), and a phased,
  non-breaking migration are in
  [`docs/architecture/tenant-storage-workspace.md`](architecture/tenant-storage-workspace.md).
  Later phases are gated on real demand for a second tenant-isolating backend and
  a coordinated storage migration.

## OGC API surface

- **OGC API – EDR (Environmental Data Retrieval)** — shipped (position,
  area, and cube queries; CoverageJSON / GeoJSON output). Known gaps:
  `locations` endpoints, vertical (`z`) subsetting, and output CRS
  reprojection are not yet implemented. See `docs/components/edr.md`.
- **OGC API – Routes** — exploratory; no immediate plans to implement.
- **SensorThings API** — **not planned**. OGC API – Connected Systems is the
  successor standard for the IoT / observation domain and is already shipped,
  so SensorThings is a deliberate non-goal rather than pending work.

## Research

- **Paginated datacube dimensions** — FAO-driven research proposal that
  bridges OGC API to OLAP query patterns. This is a research extension,
  not an OGC standard, and is intentionally surfaced separately from the
  conformance matrix. The proposal lives under `docs/proposals/`.

## Tracking

For active issues, current sprints, and real-time planning, see the
repository issue tracker.
