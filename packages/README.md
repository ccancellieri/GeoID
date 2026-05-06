# `packages/` — per-extension wheel migration

This directory hosts the per-extension wheel split (Phase 1+ of the
wheel migration plan). Each extension migrates to its own pip-installable
distribution from this same monorepo.

## Layout (target)

```
packages/
├── core/                           # produces dynastore-core (FUTURE)
│   ├── pyproject.toml
│   └── src/dynastore/
│       ├── modules/
│       ├── tools/
│       ├── models/
│       └── extensions/             # PEP 420 namespace pkg root
│           ├── protocols.py
│           ├── ogc_base.py
│           ├── ogc_models_shared.py
│           ├── lifespan.py
│           ├── registry.py
│           ├── bootstrap.py
│           └── tools/
└── extensions/
    ├── tasks/                      # produces dynastore-ext-tasks (PILOT)
    │   ├── pyproject.toml
    │   └── src/dynastore/extensions/tasks/
    ├── stats/                      # produces dynastore-ext-stats (NEXT)
    ├── ... (28+ more)
    └── gdal/
```

## Status (2026-05-07)

**Migrated to wheels (Phase 1) — 28 of ~33 extensions:**

Ungatable (5 of 7 — structurally isolated by package install boundary):
- `tasks` · `stats` · `edr` · `moving_features` · `connected_systems`

Dep-gated (18 of 24 — pattern validated, dep sentinels carried over):
- `dimensions` · `notebooks` · `proxy` · `processes` · `search`
- `crs` · `dggs` · `features` · `records` · `coverages`
- `wfs` · `dwh` · `joins` · `styles` · `gcp_bucket` (source dir `gcp/`)
- `geoid` · `events` · `httpx`

**Remaining ungatable (need real refactor before they can wheel-split):**
- `assets` — extract `asset_service` / `asset_distributed` / `write_policy_assets` from `modules/catalog/` into `extensions/assets/`
- `logs` — externalise the catalog event-listener registration API into `dynastore-core`

- `gdal` · `maps` · `tiles` · `stac` · `template` — heavy surface

**Remaining (need design before mechanical migration):**
- `iam` · `auth` · `configs` · `admin` · `web` — foundational. `web` is imported by ≥5 already-migrated wheels (`maps`, `notebooks`, `stac`, `stats`, `tiles`); its decorators (`expose_web_page`, `expose_static`) are framework helpers. Either:
  1. Keep web's framework helpers in `dynastore-core`; carve out only the `/` index route into `dynastore-ext-web` — small refactor inside web first.
  2. Make `web` a regular wheel and add it as an explicit dep of every wheel that uses its decorators — explicit but coupling proliferates.
- Same shape applies to `iam`, `auth`, `configs`, `admin` to a lesser degree (PolicyContributor cross-talk).

**Stays in `dynastore-core` (Phase 2):**
- `extensions/protocols.py`, `extensions/ogc_base.py`, `extensions/ogc_models_shared.py`, `extensions/lifespan.py`, `extensions/registry.py`, `extensions/bootstrap.py`, `extensions/tools/`, `extensions/documentation/` — these are framework, not per-route extensions; lifespan.py imports from them at module top.
- `modules/`, `tools/`, `models/` — framework.
- Top-level `extensions/__init__.py` collapses to PEP 420 namespace once dynastore-core split lands; the transitional re-export facade from PR #376 stays until then.

**Orphaned (no entry-point in pyproject):**
- `volumes` — has source under `extensions/volumes/` but isn't registered. Migration deferred pending its registration story.

## Verification (2026-05-07)

End-to-end install verified locally:
1. `uv build .` — main `dynastore` wheel builds cleanly with the migrated extensions removed from its source tree (28 entry-points dropped from its `entry_points.txt`).
2. `for ext in packages/extensions/*; do uv build $ext; done` — all 28 ext wheels build cleanly.
3. Fresh venv + `uv pip install --no-deps dist/*.whl fastapi sqlalchemy[asyncio]` + targeted module deps:
   - **35 entry-points** discoverable in `importlib.metadata.entry_points(group="dynastore.extensions")` — 28 from per-ext wheels + 7 still in main (admin/assets/auth/configs/iam/logs/web).
   - **8 of 35 load successfully** with just core deps (admin, connected_systems, dggs, edr, events, iam, notebooks, stats).
   - **27 of 35 fail with the expected missing-dep `ImportError`** — `pyproj` for crs/dwh, `rasterio` for coverages, `pygeofilter` for features, `GDAL` for gdal, `pystac` for stac, `lxml` for styles, `google-cloud-*` for gcp_bucket, etc. The dep-import sentinels from PRs #369/#370/#372 carry over cleanly through the wheel split.

This is exactly the structural isolation the migration aims for: extensions whose deps aren't installed are skipped at discovery, no artificial markers, no SCOPE env reads.

## Build

Each wheel builds independently:

```bash
uv build packages/extensions/tasks
# produces dist/dynastore_ext_tasks-*.whl
```

Workspace-wide build (when `[tool.uv.workspace]` is enabled at the repo
root):

```bash
uv build --all-packages
```

## Why

See the migration plan for context (`~/.claude/plans/...`):
- 7 extensions in dynastore have no natural unique non-SCOPE dep and
  cannot be gated by the dep-import mechanism (assets, logs, stats,
  moving_features, tasks, connected_systems, edr).
- Per-extension wheels solve this structurally: an extension that isn't
  installed isn't discoverable. No SCOPE env, no marker deps.
- Same mechanism handles the 24 currently dep-gated extensions and any
  future extension uniformly.

## Cross-repo

- **geoid** (this repo): SSOT — the source of every wheel lives here.
- **dynastore**: deployment wrapper; `pip install dynastore[$SCOPE] @ git+geoid` resolves to the new wheel set transparently once Phase 5 lands.
- **fao-aip-catalog**: plugin layer; declares `dynastore[api_open, events, module_cache] @ git+geoid` — continues to work unchanged.

The three repos share a `cc/wheels-*` worktree per phase. Land
synchronised commits per `feedback_geoid_dynastore_catalog_sync.md`.
