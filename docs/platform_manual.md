# Catalog Services — Platform Manual

A navigational index to the deeper documentation. For a quickstart, see
[Getting Started](getting-started.md).

## What this platform is

A multi-tenant, OGC-compliant geospatial catalog service. Each catalog
gets a physically isolated PostgreSQL schema. Collections partition the
asset table by collection identifier, and partitions are created
just-in-time on first insert by database triggers. Renaming a catalog's
logical code never moves a byte of data.

## Architecture

The "Three Pillars" — modules, extensions, tasks — separate concerns
so adding a new OGC API standard is adding an extension, not refactoring
the core. Start here:

- [Architecture Overview](architecture/overview.md) — the load-bearing read
- [The Database Layer](architecture/database.md) — logical → physical mapping
- [Authentication](authentication.md) — IAM model, roles, policies

## Live API surface

The home page renders a [live OGC conformance matrix](/) that probes
every standard's `/conformance` endpoint and shows the platform's actual
implementation state. The standards themselves are documented under
[Components](components/), one file per extension.

Catalog-service standards (port 80 in dev compose):

- STAC API 1.0.0 · Features · Processes · Records · Coverages · DGGS
  · Connected Systems · Moving Features

Maps-service standards (port 8083 in dev compose):

- Tiles · Maps · Styles

The roadmap surface (EDR, Routes, Joins, 3D GeoVolumes) is listed in
[Roadmap](roadmap.md), alongside the SensorThings non-goal entry
(superseded by OGC API – Connected Systems).

## Development

- [Contributing](contributing.md) — plugin naming convention, how to add an
  OGC API extension, three-repo sync expectations
- [Tools reference](tools-reference.md) — internal tooling overview
- [Testing](testing/) — patterns and fixtures

## Where to ask

Issues and active sprints live on the repository tracker.
