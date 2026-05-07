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


_PYPROJECT = Path(__file__).parent.parent.parent.parent.parent / "pyproject.toml"


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


def test_elasticsearch_indexer_scope_includes_catalog() -> None:
    """B6 regression: SCOPE must include module_catalog so CatalogsProtocol
    has an implementor in the Cloud Run Job container."""
    _assert_scope_has_module_catalog(
        "worker_task_elasticsearch_indexer",
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
    (asset_process.py:25, gdalinfo_task.py:32,40); without module_catalog
    those imports fail at task startup."""
    _assert_scope_has_module_catalog(
        "worker_task_gdal",
        "Without it, GDAL task imports from dynastore.modules.catalog.* fail "
        "at startup (asset_service, asset_tasks_spi).",
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
    entry-point group declared in pyproject.toml."""
    text = _PYPROJECT.read_text()
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


def _assert_scope_family_maps_to_entry_points(prefix: str) -> None:
    """Assert every ``<prefix><name>`` extras key has a matching
    ``dynastore.tasks.<name>`` entry-point declared in pyproject.toml."""
    scope_names = _all_task_scope_names(prefix)
    entry_points = _all_dynastore_tasks_entry_points()

    missing: list[str] = []
    for scope in scope_names:
        short = scope.removeprefix(prefix)
        if short not in entry_points:
            missing.append(short)

    assert not missing, (
        f"{prefix}<name> SCOPEs without a matching `dynastore.tasks` "
        f"entry-point: {missing}. Either add the entry-point or remove the "
        f"orphan SCOPE. Available entry-points: {sorted(entry_points)}"
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
    `worker_task_elasticsearch_indexer` and `worker_task_ingestion`.)"""
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
    src_root = _PYPROJECT.parent / "src"

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
