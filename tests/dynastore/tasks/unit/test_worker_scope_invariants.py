#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Regression tests for the SCOPE → entry-point → meta-extras invariants.

Each Cloud Run Job (or in-process catalog task) is built with a
``<family>_task_<name>`` extras group from ``pyproject.toml`` — currently
``worker_task_*`` (separate Cloud Run Job images) and ``catalog_task_*``
(bundled into the catalog/worker images and dispatched in-process). If
build-config drifts from runtime expectations the container deploys
fine then crashes at first dispatch. This file pins three classes of
invariant that catch that drift at CI time.

1. **SCOPE-must-include-module_catalog** (B6 + Phase H):
   per-worker assertions that each catalog-touching task's SCOPE pulls
   in the modules its imports / Protocol calls need. Five entries today
   (elasticsearch_indexer, tiles_preseed, gdal, dimensions_materialize,
   dwh_join), each with a short comment naming the file:line that would
   crash without ``module_catalog``.

2. **SCOPE ↔ entry-point mapping** (PR #142):
   every ``worker_task_<name>`` extras key must have a matching
   ``dynastore.tasks.<name>`` entry-point and that entry-point's
   ``module:Class`` target must resolve to a real file on disk.

3. **meta-extras consistency** (PR #142):
   ``worker_service`` (in-process worker composition) and ``scope_worker``
   (Cloud Run worker image SCOPE) must agree on which ``worker_task_*``
   they pull in — drift means the deployed image carries different task
   code than the local worker, so bugs reproduce in one and not the other.

The original audit incident (2026-04-29) was on
``worker_task_elasticsearch_indexer``: module discovery left
``protocol_resolvers={'Catalogs': None, ...}`` and BulkCatalog/Collection
ReindexTask crashed at first dispatch. PR #131 fixed the SCOPE; the
remaining four invariants in class 1 above are prophylactic for the same
failure shape on the other catalog-touching tasks.
"""
import re
from pathlib import Path


_REPO_ROOT = Path(__file__).parent.parent.parent.parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"
# Entry-points (and the dynastore source tree) live in the core sub-package
# since the Phase 1 packages restructure (PR #397). SCOPE extras stayed in the
# root pyproject.toml; entry-points + sources moved to packages/core/.
_CORE_PYPROJECT = _REPO_ROOT / "packages" / "core" / "pyproject.toml"


def _scope_definition(scope: str) -> str:
    """Return the line defining ``<scope> = [...]`` from pyproject.toml."""
    text = _PYPROJECT.read_text()
    for line in text.splitlines():
        if line.startswith(f"{scope} = "):
            return line
    raise AssertionError(f"SCOPE '{scope}' not found in pyproject.toml")


def _extract_dynastore_extras(line: str) -> set:
    """Pull the comma-separated extras from any `dynastore[...]` clause(s)
    in a pyproject.toml line."""
    extras = set()
    for match in re.finditer(r"dynastore\[([^\]]+)\]", line):
        for x in match.group(1).split(","):
            extras.add(x.strip())
    return extras


def _assert_scope_has_module_catalog(scope: str, why: str) -> None:
    line = _scope_definition(scope)
    extras = _extract_dynastore_extras(line)
    assert "module_catalog" in extras, (
        f"{scope} SCOPE is missing module_catalog. {why} Current extras: {extras}"
    )


def test_async_writer_scope_includes_catalog() -> None:
    """B6 regression: SCOPE must include module_catalog so CatalogsProtocol
    has an implementor in the Cloud Run Job container.

    `worker_task_elasticsearch_indexer` was renamed to `worker_task_async_writer`
    (#2622) when the job was generalized beyond Elasticsearch; the back-compat
    alias was retired once the last consumer moved to the new name (#2673)."""
    _assert_scope_has_module_catalog(
        "worker_task_async_writer",
        "Without it, BulkCatalog/CollectionReindexTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_tiles_preseed_scope_includes_catalog() -> None:
    """B6 follow-up: TilesPreseedTask reads catalog metadata via
    get_protocol(CatalogsProtocol) at tasks/tiles_preseed/task.py:65."""
    _assert_scope_has_module_catalog(
        "worker_task_tiles_preseed",
        "Without it, TilesPreseedTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_gdal_scope_includes_catalog() -> None:
    """B6 follow-up: GDAL task imports from dynastore.modules.catalog.*
    (gdalinfo_task.py imports asset_service); without module_catalog those
    imports fail at task startup."""
    _assert_scope_has_module_catalog(
        "worker_task_gdal",
        "Without it, GDAL task imports from dynastore.modules.catalog.* fail "
        "at startup (asset_service).",
    )


def test_dimensions_materialize_scope_includes_catalog() -> None:
    """B6 follow-up: DimensionsMaterializeTask reads catalog metadata via
    get_protocol(CatalogsProtocol) at tasks/dimensions_materialize/task.py:67."""
    _assert_scope_has_module_catalog(
        "worker_task_dimensions_materialize",
        "Without it, DimensionsMaterializeTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_dwh_join_scope_includes_catalog() -> None:
    """Phase H follow-up: DwhJoinExportTask doesn't call CatalogsProtocol
    directly, but its entry-point load chain transitively pulls shapely via
    extensions/dwh/models.py → tools/geospatial. Without module_catalog
    (which provides geospatial_core → shapely), the dispatcher logs
    `Skipping plugin 'dwh_join': No module named 'shapely'` and the task
    never registers. Confirmed by `dynastore-dwh-join-export-job-24476`
    container log on 2026-04-29."""
    _assert_scope_has_module_catalog(
        "worker_task_dwh_join",
        "Without it, DwhJoinExportTask entry-point load fails with "
        "ImportError: No module named 'shapely' (transitively via "
        "extensions/dwh/models.py → tools/geospatial).",
    )


def test_dwh_join_scope_includes_crs() -> None:
    """Phase H second follow-up: even with module_catalog (PR #141), the
    dwh_join entry-point load chain transitively imports pyproj via
    extensions/dwh/dwh.py → modules/tiles/tms_definitions.py:22
    `from pyproj import CRS`. module_crs provides pyproj. Without it,
    the dispatcher registers a DefinitionOnlyTask placeholder and
    main_task.py raises 'Task has run method without payload annotation'
    (misleading error — actual cause is the placeholder having no `run`).
    Confirmed by `dynastore-dwh-join-export-job-snq2n` container log on
    2026-04-29."""
    line = _scope_definition("worker_task_dwh_join")
    extras = _extract_dynastore_extras(line)
    assert "module_crs" in extras, (
        f"worker_task_dwh_join SCOPE is missing module_crs. Without it the "
        f"entry-point load fails with `No module named 'pyproj'` and dwh_join "
        f"runs as a no-run-method placeholder. Current extras: {extras}"
    )


def test_dwh_join_scope_includes_dwh_extension() -> None:
    """Phase H fourth follow-up: the dwh_join task module imports the dwh
    *extension* package directly — `dynastore.extensions.dwh.models`
    (DWHJoinRequest, at tasks/dwh_join/models.py) and
    `dynastore.extensions.dwh.dwh` (execute_bigquery_async, at
    tasks/dwh_join/dwh_join_export_task.py). The bare `dwh` extra only pulls the
    BigQuery join-query deps (dynastore-ext-joins), NOT the `dynastore-ext-dwh`
    distribution that provides the `dynastore.extensions.dwh` package. With only
    `dwh`, discovery logs `Skipping dynastore.tasks plugin 'dwh_join': No module
    named 'dynastore.extensions.dwh'` and every async job dies with `Task
    'dwh_join' not found or failed to initialize`. Confirmed by
    `dynastore-dwh-join-export-job-fljr4` container log on 2026-05-29. Fix: use
    `extension_dwh` (a superset of `dwh` that bundles `dynastore-ext-dwh`)."""
    line = _scope_definition("worker_task_dwh_join")
    extras = _extract_dynastore_extras(line)
    assert "extension_dwh" in extras or "dynastore-ext-dwh" in line, (
        f"worker_task_dwh_join SCOPE does not pull the dwh extension package. "
        f"The bare `dwh` extra installs only BigQuery query deps, not "
        f"`dynastore-ext-dwh`, so the dwh_join task plugin is silently skipped at "
        f"discovery and the async job reports 'Task dwh_join not found'. Use "
        f"`extension_dwh`. Current extras: {extras}"
    )


def test_export_features_scope_includes_gcp() -> None:
    """Phase H third follow-up: ExportFeaturesTask writes its output bytes
    to GCS via dynastore.modules.gcp clients. Without module_gcp the
    worker container raises at run time: 'GCPModule has not been
    initialized or failed to create a storage client.' Confirmed by
    019dda8e-a239-7bd8-... OGC poll on 2026-04-29."""
    line = _scope_definition("worker_task_export_features")
    extras = _extract_dynastore_extras(line)
    assert "module_gcp" in extras, (
        f"worker_task_export_features SCOPE is missing module_gcp. Without "
        f"it the task crashes at run time when trying to create a GCS "
        f"storage client to write the export output. Current extras: {extras}"
    )


def test_dwh_join_scope_includes_gcp() -> None:
    """Phase H fifth follow-up: DwhJoinExportTask both queries BigQuery
    (execute_bigquery_async) and uploads its export bytes to GCS
    (tasks/dwh_join/dwh_join_export_task.py:22 upload_stream_to_gcs). Both
    need module_gcp (google-cloud-storage + google-cloud-bigquery client
    init). Without it, GCPModule.reinitialize_clients bails
    ('google-cloud-storage/pubsub not installed; storage/BigQuery clients
    will be unavailable'), the task fails with 'GCPModule has not been
    initialized or failed to create a storage client', and the BigQuery
    client built without the resolved Cloud Run SA credentials hits a
    `403 bigquery.jobs.create` even though the runtime SA holds
    bigquery.jobUser. Mirrors worker_task_export_features, which carries
    module_gcp for the same GCS-write reason. Confirmed by
    `dynastore-dwh-join-export-job-5k56c` container log on 2026-05-29."""
    line = _scope_definition("worker_task_dwh_join")
    extras = _extract_dynastore_extras(line)
    assert "module_gcp" in extras, (
        f"worker_task_dwh_join SCOPE is missing module_gcp. Without it the "
        f"GCP client init bails (no google-cloud-storage), the task crashes "
        f"creating a storage client, and the BigQuery client 403s on "
        f"jobs.create. Current extras: {extras}"
    )


# ---------------------------------------------------------------------------
# Mapping invariants — tighten the scope ↔ entry-point ↔ meta-extra wiring
# so a typo or missing entry-point breaks CI before it breaks deploy.
# ---------------------------------------------------------------------------


def _all_task_scope_names(prefix: str = "worker_task_") -> list[str]:
    """Return every ``<prefix><name>`` extras key from pyproject.toml.

    Default ``prefix='worker_task_'`` returns Cloud Run Job SCOPEs. Pass
    ``catalog_task_`` for the in-process task family bundled into the
    catalog/worker images.
    """
    text = _PYPROJECT.read_text()
    names: list[str] = []
    for line in text.splitlines():
        if line.startswith(prefix) and " = " in line:
            names.append(line.split(" = ", 1)[0].strip())
    return names


def _all_worker_task_scope_names() -> list[str]:
    """Backwards-compat shim — use _all_task_scope_names() directly in new code."""
    return _all_task_scope_names("worker_task_")


def _all_dynastore_tasks_entry_points() -> dict[str, str]:
    """Return ``{entry_point_name: target_str}`` for the ``dynastore.tasks``
    entry-point group declared in packages/core/pyproject.toml."""
    text = _CORE_PYPROJECT.read_text()
    in_group = False
    out: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == '[project.entry-points."dynastore.tasks"]':
            in_group = True
            continue
        if in_group:
            if stripped.startswith("[") and stripped.endswith("]"):
                break  # next TOML section
            if " = " in stripped and not stripped.startswith("#"):
                k, v = stripped.split(" = ", 1)
                out[k.strip()] = v.strip().strip('"')
    return out


def _meta_extra_definition(name: str) -> str:
    """Return the line defining ``<name> = [...]`` for a meta-extra."""
    return _scope_definition(name)



# SCOPEs whose suffix does NOT name a single `dynastore.tasks` entry-point
# because the job deliberately hosts several task types (#2622's generic
# async_writer job: the elasticsearch_indexer bulk-reindex backstop plus the
# driver-agnostic storage_drain / event_drain outbox drainers, all in one
# container). Declared explicitly here rather than silently skipped — the
# `test_async_writer_scope_hosts_declared_entry_points` check below still
# verifies every listed task type resolves to a real entry-point.
_MULTI_TASK_WORKER_SCOPES: dict[str, frozenset[str]] = {
    "worker_task_async_writer": frozenset(
        {"elasticsearch_indexer", "envelope_attrs_backfill", "storage_drain", "event_drain"}
    ),
}


def _assert_scope_family_maps_to_entry_points(prefix: str) -> None:
    """Assert every ``<prefix><name>`` extras key has a matching
    ``dynastore.tasks.<name>`` entry-point declared in pyproject.toml.

    SCOPEs listed in ``_MULTI_TASK_WORKER_SCOPES`` are exempt from the 1:1
    suffix match (they intentionally host several task types); see
    ``test_async_writer_scope_hosts_declared_entry_points`` for their check.
    """
    scope_names = _all_task_scope_names(prefix)
    entry_points = _all_dynastore_tasks_entry_points()

    missing: list[str] = []
    for scope in scope_names:
        if scope in _MULTI_TASK_WORKER_SCOPES:
            continue
        short = scope.removeprefix(prefix)
        if short not in entry_points:
            missing.append(short)

    assert not missing, (
        f"{prefix}<name> SCOPEs without a matching `dynastore.tasks` "
        f"entry-point: {missing}. Either add the entry-point or remove the "
        f"orphan SCOPE. Available entry-points: {sorted(entry_points)}"
    )


def test_async_writer_scope_hosts_declared_entry_points() -> None:
    """Every task type `_MULTI_TASK_WORKER_SCOPES` declares for
    `worker_task_async_writer` must resolve to a real `dynastore.tasks`
    entry-point — the multi-task exemption above must not become a way to
    silently orphan one of the hosted task types."""
    entry_points = _all_dynastore_tasks_entry_points()
    for scope, task_types in _MULTI_TASK_WORKER_SCOPES.items():
        missing = sorted(t for t in task_types if t not in entry_points)
        assert not missing, (
            f"{scope} declares task types with no matching `dynastore.tasks` "
            f"entry-point: {missing}. Available: {sorted(entry_points)}"
        )


def test_every_worker_task_scope_has_matching_entry_point() -> None:
    """Each `worker_task_<name>` extras key must correspond to a
    `dynastore.tasks.<name>` entry-point. Catches the class of bug where
    a SCOPE is defined but the task entry-point was renamed/removed (or
    vice versa) — Cloud Run Job container would deploy fine then crash
    at first dispatch with `Task '<name>' not found`."""
    _assert_scope_family_maps_to_entry_points("worker_task_")


def test_every_catalog_task_scope_has_matching_entry_point() -> None:
    """Each `catalog_task_<name>` extras key must correspond to a
    `dynastore.tasks.<name>` entry-point. Catalog tasks are dispatched
    in-process by the catalog service (not as separate Cloud Run Jobs);
    a SCOPE/entry-point mismatch surfaces the same way — `Task '<name>'
    not found` at first dispatch. Two entries today: gcp_provision,
    gcp_catalog_cleanup. (gcs_storage_event was retired in Stage 4.2 in
    favour of an inline Pub/Sub activator — no task hop, no SCOPE
    entry.)"""
    _assert_scope_family_maps_to_entry_points("catalog_task_")


def test_worker_service_and_scope_worker_agree_on_task_membership() -> None:
    """`worker_service` (runtime composition) and `scope_worker` (Cloud Run
    image SCOPE) must reference the same set of `worker_task_*` extras.

    Drift between them means the deployed image carries different task code
    than the local development worker — bugs reproduce in one and not the
    other. (This is a *consistency* check, not a *completeness* check —
    some `worker_task_*` are intentionally excluded from both because they
    only run as their own dedicated Cloud Run Job images, e.g.
    `worker_task_async_writer` and `worker_task_ingestion`.)"""
    scope_names = set(_all_worker_task_scope_names())
    worker_service_extras = _extract_dynastore_extras(
        _meta_extra_definition("worker_service")
    ) & scope_names
    scope_worker_extras = _extract_dynastore_extras(
        _meta_extra_definition("scope_worker")
    ) & scope_names

    only_in_service = worker_service_extras - scope_worker_extras
    only_in_scope = scope_worker_extras - worker_service_extras

    assert not only_in_service and not only_in_scope, (
        f"worker_service and scope_worker disagree on which worker_task_* "
        f"to include.\n"
        f"  Only in worker_service: {sorted(only_in_service)}\n"
        f"  Only in scope_worker:   {sorted(only_in_scope)}\n"
        f"Either add the missing entries to align them, or document the "
        f"intentional asymmetry by relaxing this test."
    )


def test_every_dynastore_tasks_entry_point_resolves_to_existing_module() -> None:
    """Each `dynastore.tasks` entry-point's `module:Class` target must point
    at a module file that exists on disk. Catches typos in the entry-point
    declaration (path drift after a rename refactor)."""
    entry_points = _all_dynastore_tasks_entry_points()
    src_root = _CORE_PYPROJECT.parent / "src"

    bad: list[str] = []
    for ep_name, ep_target in entry_points.items():
        module_path = ep_target.split(":")[0]
        # dynastore.tasks.foo.bar -> packages/core/src/dynastore/tasks/foo/bar.py OR
        #                            packages/core/src/dynastore/tasks/foo/bar/__init__.py
        rel = module_path.replace(".", "/")
        as_file = src_root / f"{rel}.py"
        as_pkg = src_root / rel / "__init__.py"
        if not (as_file.exists() or as_pkg.exists()):
            bad.append(f"{ep_name} → {ep_target} (no such module)")

    assert not bad, (
        "dynastore.tasks entry-points pointing at non-existent modules:\n  "
        + "\n  ".join(bad)
    )


def _assert_entry_point_group_loads(group: str) -> None:
    """Helper: load every entry-point in ``group`` and assert no real bugs.

    Mirrors ``packages/core/src/dynastore/tools/discovery.py::discover_and_load_plugins``
    (the function called by ``discover_modules``, ``discover_extensions`` and
    ``discover_tasks`` at every service startup):
    - ``EntryPoint.load()`` per entry — executes module top-level statements
    - ``ImportError`` is silently skipped (optional dep not installed in this
      SCOPE; runtime treats it the same)
    - Any other exception is a real bug — surfaced as test failure

    Distinguishes a missing-third-party-dep (expected when this venv lacks
    an extras group, e.g. ``osgeo`` for GDAL) from an accidentally-broken
    dynastore-internal import (real bug) by examining ``ImportError.name``:
    an import that originates inside the dynastore package itself fails
    the test; a third-party miss is logged and skipped.
    """
    import importlib.metadata

    bad: list[str] = []
    skipped_optional: list[str] = []

    for ep in importlib.metadata.entry_points(group=group):
        try:
            ep.load()
        except ImportError as e:
            missing = getattr(e, "name", "") or ""
            if missing.startswith("dynastore"):
                bad.append(
                    f"{ep.name} → {ep.value}: ImportError on internal "
                    f"dynastore module '{missing}': {e}"
                )
            else:
                skipped_optional.append(f"{ep.name} (missing: {missing or '?'})")
        except Exception as e:
            bad.append(f"{ep.name} → {ep.value}: {type(e).__name__}: {e}")

    assert not bad, (
        f"{group} entry-points failing to load in the test env with errors "
        f"that would NOT be silently demoted by discover_and_load_plugins "
        f"(real bugs):\n  " + "\n  ".join(bad)
    )

    if skipped_optional:
        # Reviewer-visible info; doesn't fail the suite.
        # The SCOPE-↔-entry-point invariants above ensure each entry is
        # reachable from at least one deployment image.
        print(
            f"[info] {group} entry-points skipped (optional deps not "
            f"installed in this venv — expected for system-level libs or "
            f"out-of-SCOPE extras): " + ", ".join(skipped_optional)
        )


def test_every_dynastore_tasks_entry_point_loads_or_misses_optional_dep() -> None:
    """Each `dynastore.tasks` entry-point class must actually `load()` cleanly
    OR fail cleanly with `ImportError` (the ``discover_tasks`` placeholder
    fallback path).

    Strictly stronger than ``test_every_dynastore_tasks_entry_point_resolves_to_existing_module``
    (that test only confirms the file path exists). Catches syntax errors,
    circular imports, class-not-found, accidentally-promoted internal
    imports — see ``_assert_entry_point_group_loads`` docstring.
    """
    _assert_entry_point_group_loads("dynastore.tasks")


def test_every_dynastore_modules_entry_point_loads_or_misses_optional_dep() -> None:
    """Each `dynastore.modules` entry-point class must `load()` cleanly OR
    fail cleanly with `ImportError` (the ``discover_modules`` skip path).

    Same shape as the tasks invariant, applied to the ~22 module entry-points
    declared at ``[project.entry-points."dynastore.modules"]``. A broken
    module entry-point is *more* dangerous than a broken task: tasks have
    a definition-only placeholder fallback that preserves OGC Process
    metadata for the dispatcher; modules have no equivalent — a load
    failure silently disables the module and every Protocol it provides.
    """
    _assert_entry_point_group_loads("dynastore.modules")


def test_every_dynastore_extensions_entry_point_loads_or_misses_optional_dep() -> None:
    """Each `dynastore.extensions` entry-point class must `load()` cleanly
    OR fail cleanly with `ImportError` (the ``instantiate_extensions`` skip
    path).

    Same shape as the modules invariant, applied to the 34 extension
    entry-points. Extensions register routes, web pages, OGC services, etc.;
    a load failure silently disables every endpoint the extension would
    have mounted, with no visible CI signal until a route 404s in
    integration tests (which is exactly the bug class this invariant is
    designed to catch earlier).
    """
    _assert_entry_point_group_loads("dynastore.extensions")


# ---------------------------------------------------------------------------
# #506: every SCOPE that boots the full task dispatcher must pin at least one
# Indexer-providing module — otherwise an Indexer-dependent task entry point
# (e.g. ``elasticsearch_indexer``) loads but no class with
# ``is_<tier>_indexer = True`` is registered, and first dispatch
# dead-letters at runtime (the claim predicate from #491 catches it
# post-deploy, but CI should catch it pre-deploy).
# ---------------------------------------------------------------------------


def _all_extras_definitions() -> dict[str, str]:
    """Return ``{extras_key: definition_line}`` for every single-line
    ``<name> = [...]`` extras key in the root pyproject.toml.

    Multi-line extras (e.g. ``geospatial_core``) don't carry
    ``dynastore[...]`` cross-refs in practice, so the single-line scan is
    sufficient for closure resolution.
    """
    out: dict[str, str] = {}
    for line in _PYPROJECT.read_text().splitlines():
        if " = [" not in line:
            continue
        key = line.split(" = ", 1)[0].strip()
        if not key or not key.replace("_", "").isalnum():
            continue
        out[key] = line
    return out


def _resolve_extras_closure(scope: str, defs: dict[str, str]) -> set[str]:
    """Return the transitive set of extras keys reachable from ``scope`` by
    following ``dynastore[a,b,c]`` references."""
    seen: set[str] = set()
    stack: list[str] = [scope]
    while stack:
        name = stack.pop()
        if name in seen:
            continue
        seen.add(name)
        line = defs.get(name)
        if not line:
            continue  # leaf / external dep / undefined key
        for ref in _extract_dynastore_extras(line):
            if ref not in seen:
                stack.append(ref)
    return seen


# Extras whose presence in a SCOPE's transitive closure makes an Indexer
# driver class (with ``is_<tier>_indexer = True``) load-able. Every entry
# either is, or transitively pulls, ``module_elasticsearch`` — the gate
# that lets the source-tree driver modules (``dynastore.modules.elasticsearch.*``
# and ``dynastore.modules.storage.drivers.elasticsearch*``) import cleanly.
_INDEXER_GATING_EXTRAS: frozenset[str] = frozenset({
    "module_elasticsearch",
    "elasticsearch",
    "index_grp",
    "drivers_grp",
    "module_collection_elasticsearch",
    "module_catalog_elasticsearch",
    "module_storage_elasticsearch",
    "module_storage_elasticsearch_private",
    "module_storage_elasticsearch_assets",
    "task_elasticsearch_deps",
    "task_elasticsearch_indexer_deps",
})


def test_every_scope_with_task_dispatch_pins_an_indexer_module() -> None:
    """B6 #506: any deployable SCOPE whose closure contains ``core`` must
    also contain at least one Indexer-providing extras key.

    Rationale: ``core`` is the marker for services that boot the full
    dispatcher (db_async + web + configs + storage + cache). Such a
    service will discover Indexer-dependent ``dynastore.tasks``
    entry-points (e.g. ``elasticsearch_indexer``) at startup. If no
    Indexer driver class is in the import closure, the very first
    dispatch finds zero implementors of ``is_<tier>_indexer = True`` and
    dead-letters — exactly the regression class #491's claim predicate
    guards against at *runtime*. This test enforces the same property at
    *build* time so a mis-deployed SCOPE breaks CI, not production.

    Filter: SCOPEs whose closure contains BOTH ``core`` and ``tasks``.
    ``core`` marks a service that boots the full dispatcher; ``tasks``
    is the canonical extras key that pulls in ``module_tasks`` +
    ``extension_tasks`` — the actual task-dispatch surface. A service
    with ``core`` but no ``tasks`` (e.g. ``scope_tools``, which forwards
    task dispatch to the catalog service per its task routing) is a
    router, not a dispatcher, and is out of scope for this invariant.
    Likewise
    ``worker_task_*`` images intentionally drop ``core`` and use
    ``task_base`` instead — a dedicated job image only runs the one
    task its SCOPE pins.
    """
    defs = _all_extras_definitions()
    deployable_scopes = [
        k for k in defs
        if k.startswith(("scope_", "worker_service", "worker_task_"))
    ]
    bad: list[tuple[str, set[str]]] = []
    for scope in deployable_scopes:
        closure = _resolve_extras_closure(scope, defs)
        if "core" not in closure or "tasks" not in closure:
            continue
        if not (_INDEXER_GATING_EXTRAS & closure):
            # Trim closure for the failure message to the module_*/grp keys
            # — that's the actionable subset.
            trimmed = {
                c for c in closure
                if c.startswith("module_") or c.endswith("_grp")
            }
            bad.append((scope, trimmed))

    assert not bad, (
        "SCOPE(s) that boot the task dispatcher (via `core` + `tasks`) but "
        "do not pin any Indexer-providing extras — first dispatch will "
        "dead-letter:\n  "
        + "\n  ".join(
            f"{scope}: module/grp closure = {sorted(extras)}"
            for scope, extras in bad
        )
        + f"\nAdd one of: {sorted(_INDEXER_GATING_EXTRAS)} (canonical: `index_grp`)."
    )
