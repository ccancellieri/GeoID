# Roadmap

This page reflects engineering work that is in progress or imminent on the
Catalog Services platform. Items already shipped (Tiles, Maps, Styles,
Connected Systems, Moving Features, Coverages, DGGS, Records) live in
`docs/components/` and on the live OGC conformance matrix on the home page.

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

## OGC API surface

- **OGC API – EDR (Environmental Data Retrieval)** — not yet implemented.
  Not committed to a specific milestone.
- **OGC API – Routes / Joins / 3D GeoVolumes** — exploratory; no immediate
  plans to implement.
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
