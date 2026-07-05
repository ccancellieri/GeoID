# Per-deployment config — example

Documentation only. **These files are not copied into any image automatically.**

This folder shows the on-disk shape that geoid expects when service-affinity
routing and JSON-defaults bootstrap are enabled. Your *deployment repo* (e.g.
`dynastore` for FAO Cloud Run, your own repo for on-prem) ships the real
versions of these files.

## Layout

```
${DYNASTORE_CONFIG_ROOT:-${APP_DIR}/config}/
  instance.json             ← this process's identity (one per service)
  defaults/
    *.json                  ← seeded PluginConfig defaults (shared)
```

`DYNASTORE_CONFIG_ROOT` defaults to `${APP_DIR}/config` (in the FAO image:
`/dynastore/config`). The folder is fully self-contained — point the env at any
mounted volume to externalize.

## `instance.json`

A one-shot identifier read at process startup by
`modules/tasks/dispatcher.py`:

```json
{ "service_name": "catalog" }
```

When the file is missing or has no `service_name`, the dispatcher falls back to
legacy "any capable service may claim any task" behaviour. Fully
backward-compatible.

## `defaults/*.json`

Seed payloads for any `PluginConfig` subclass. Each file:

```json
{
  "class_key": "task_routing_config",
  "value":     { "...": "..." },
  "override":  false
}
```

- `class_key` — the snake_case identity (`cls.class_key()`) of the
  `PluginConfig` subclass (e.g. `task_routing_config`, `tasks_plugin_config`).
  Same wire name as `plugin_id` on the configs API surface.
- `value` — the config payload (validated against the Pydantic model).
- `override` — optional. When `false` (default), the seed is skipped if a
  config row already exists for that `class_key`. Set `true` to force apply.

Files are processed in lexical order; later files override earlier ones for
the same `class_key`. This lets a wrapper repo (e.g. `fao-aip-catalog`) drop
overlay files on top of a base repo's defaults (e.g. `dynastore`).

A PostgreSQL advisory lock (`config_seeder.defaults`) guards the bootstrap so
only one process per cluster applies the seeds on each boot.

## Per-deployment recipes

The deployment repo (dynastore for cloud, operator's repo for on-prem)
provides the real `instance.json` per service and the shared `defaults/`
folder. Typical patterns:

- **on-prem docker compose**: mount per-service `instance.json` and a
  shared `defaults/` directory into each container at the path resolved by
  `DYNASTORE_CONFIG_ROOT`.
- **Cloud Run**: bake `instance.json` per service image at build time
  (one-shot identity) and ship `defaults/` into the image as well, so the
  service is fully self-contained.
- **wrapper repo overlay**: a downstream image can `COPY` extra
  `defaults/*.json` files on top of a base image — lexical order means a
  later file overrides an earlier one for the same `class_key`.

## Pitfall: docker bind-mount overlap

The "shared `defaults/` directory" recipe above does NOT work with two
overlapping read-only bind mounts on a single container — Docker cannot
create the sub-mountpoint inside an already-mounted `:ro` parent
(`mkdirat … read-only file system`). Three workable options:

1. **Self-contained per-service dir** (used by `docker-compose.dev.yml`):
   each service has its own dir holding both `instance.json` and the full
   `defaults/`. One mount per container, no overlap. Minor duplication of
   `defaults/*.json` content across service dirs — drift is caught by
   `tests/dynastore/test_dev_compose_config_in_sync.py`.
2. **Shared dir without `:ro`** — drop the read-only flag from the parent
   mount; Docker can then create the sub-mountpoint. Loses the read-only
   guard. Used by `docker-compose.dev.yml` to layer `config/local/defaults/
   idp-config.json` (the compose-local Keycloak values) on top of each
   auth-fronting service's self-contained `defaults/` dir — geoid's tracked
   `catalog`/`maps`/`tools` defaults ship no `idp-config.json` at all (the
   onprem baseline is the neutral `IdpConfig` pydantic default,
   unconfigured), so local dev needs this second mount to get a working IdP.
3. **Bake into the image** — `COPY` everything into the image at build time
   so the runtime is fully self-contained. No bind mount, no overlap.
