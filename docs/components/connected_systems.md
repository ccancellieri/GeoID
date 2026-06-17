# The Connected Systems Extension

The `connected_systems` extension implements **OGC API – Connected Systems
Part 1**. It provides a REST surface for registering IoT sensors, weather
stations, and field devices — the successor domain to the SensorThings API —
together with their datastreams (sensor channels) and timestamped
observations. All resources are catalog-scoped, and reads and writes respect
the same catalog-readiness guards as the rest of the platform.

The service mounts under `/consys` (priority 100).

## Conformance

The extension declares the following OGC conformance URIs:

```
http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/system-features
http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/datastreams
http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/observations
```

These are aggregated into the platform conformance matrix by
`extensions/tools/conformance.py` (family `OGC API Connected Systems`).

## Resources

The extension models four resources, persisted in the `consys` schema with
tables partitioned by `catalog_id`:

- **System** — a sensor, platform, or actuator, with an optional location
  geometry and free-form `properties`.
- **Deployment** — a deployment period for a system (start/end, site
  geometry). Created alongside systems and read back per system.
- **Datastream** — a single observed property of a system (e.g.
  `temperature` in `degC`).
- **Observation** — a timestamped measurement value on a datastream.

The authoritative schema lives in
`modules/connected_systems/ddl.py`; the Pydantic models are in
`modules/connected_systems/models.py`.

## Endpoints

`catalog_id` is a required query parameter on every operation; list
operations accept `limit` (1–1000, default 100) and `offset` (default 0).

| Method | Path | Description |
|---|---|---|
| `GET` | `/consys/` | Landing page |
| `GET` | `/consys/conformance` | Declared conformance classes |
| `GET` / `POST` | `/consys/systems` | List / create systems |
| `GET` / `PUT` / `DELETE` | `/consys/systems/{system_id}` | Get / update / delete a system |
| `GET` | `/consys/systems/{system_id}/deployments` | List deployments for a system |
| `GET` | `/consys/systems/{system_id}/datastreams` | List datastreams for a system |
| `GET` / `POST` | `/consys/datastreams` | List / create datastreams |
| `GET` | `/consys/datastreams/{datastream_id}` | Get a datastream |
| `GET` / `POST` | `/consys/datastreams/{datastream_id}/observations` | List / create observations |

Creating a datastream validates that its parent system exists; creating an
observation validates that its parent datastream exists. Duplicate
`system_id` / `datastream_id` within a catalog return `409`.

## Known limitations

- **Deployments are read-only over the API** — they are created with their
  parent system and listed via `GET /consys/systems/{id}/deployments`, but
  there is no create/update endpoint for deployments yet.
- **Sampling features are not modelled** — the OGC Part 1 `SamplingFeature`
  resource (the spatial footprint of an observation, distinct from system
  geometry) is not yet implemented.
- **No spatial or temporal filtering** — list endpoints page by
  `catalog_id` only; there is no `bbox`, geometry, or `phenomenon_time`
  window filter. Clients must post-filter.
- **Parent links are not enforced by the database** — `deployment.system_id`,
  `datastream.system_id`, and `observation.datastream_id` are validated in
  the service layer rather than by foreign-key constraints.

## Key files

| File | Responsibility |
|---|---|
| `extensions/connected_systems/consys_service.py` | FastAPI router and request orchestration |
| `extensions/connected_systems/config.py` | Service-exposure registration |
| `modules/connected_systems/models.py` | Resource models |
| `modules/connected_systems/db.py` | CRUD query definitions |
| `modules/connected_systems/ddl.py` | Schema and table DDL |
