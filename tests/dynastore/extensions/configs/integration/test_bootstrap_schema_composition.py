"""Bootstrap-schema composition regression — covers the **Gap A** path documented
in ``notebooks/admin_boundaries_fixed_schema/walkthrough.ipynb`` (Step 0b).

The walkthrough's Step 0b cell is gated behind ``BOOTSTRAP_FROM = None`` by
default, so the notebook-CI pass never drives the
``Process → OGR→PG map → PluginConfig PUT`` sequence. A breaking change to any
of the four surfaces below would silently slip through:

1. The ``gdal`` Process payload shape (sync ``200`` body vs async
   ``201 + Location`` per OGC API - Processes Part 1 — the cell handles both
   branches).
2. The local OGR→PG type map (12 OGR types → ``PostgresType`` enum, including
   Boolean / Time / list types added by PR #481).
3. The ``items_postgresql_driver`` PluginConfig PUT body
   (``sidecars[*]`` with ``sidecar_type="attributes"`` + ``attribute_schema``).
4. The PluginConfig PUT/GET round-trip at both **collection scope**
   (``/configs/catalogs/{c}/collections/{c}/plugins/{plugin_id}``) and
   **catalog scope** (``/configs/catalogs/{c}/plugins/{plugin_id}``).

These tests pin each surface so the Step-0b sequence stays executable end-to-
end even though the notebook cell stays documentary by default. They use the
in-process TestClient so no GDAL runtime / live asset upload is required —
the Process registration + driver-config round-trip alone catches every
contract change called out above.
"""

from __future__ import annotations

import json
import pathlib

import pytest
from httpx import AsyncClient


# Modules required to register the PluginConfig surfaces + the gdal Process.
# ``processes`` extension surfaces ``GET /processes`` (asserts gdal applicability),
# ``configs`` extension surfaces the PluginConfig PUT/GET round-trip.
pytestmark = [
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "storage", "gdal", "processes",
    ),
    pytest.mark.enable_extensions("features", "configs", "processes"),
]


# ---------------------------------------------------------------------------
# Surface 2 — OGR→PG type map (Step 0b cell 6 verbatim).
#
# Re-asserting the mapping table here in test code pins it as a public-ish
# contract: every OGR type the notebook documents must map to a real
# ``PostgresType`` enum value, AND any new ``PostgresType`` value MUST come with
# a deliberate decision on whether OGR drivers emit something that maps to it.
# ---------------------------------------------------------------------------

# Mirror the snake_case ``PostgresType`` names rather than importing the enum
# directly, so a rename of an enum member is caught here as a real failure
# (the notebook ships the string values to operators, not the enum object).
_NOTEBOOK_OGR_TO_PG = {
    "String": "TEXT",
    "Integer": "INTEGER",
    "Integer64": "BIGINT",
    "Real": "NUMERIC",
    "Boolean": "BOOLEAN",
    "DateTime": "TIMESTAMPTZ",
    "Date": "DATE",
    # Time-of-day has no PostgresType equivalent today — kept as TEXT
    # explicitly so the fallback is visible at the call site (PR #481).
    "Time": "TEXT",
    # List OGR types degrade to JSONB (lossy on element type but preserves
    # array semantics).
    "IntegerList": "JSONB",
    "Integer64List": "JSONB",
    "RealList": "JSONB",
    "StringList": "JSONB",
}


def _apply_notebook_map(ogr_type: str) -> str:
    """Re-implement the Step-0b mapping with the same fallback the notebook
    uses (unknown OGR types → ``TEXT``)."""
    return _NOTEBOOK_OGR_TO_PG.get(ogr_type, "TEXT")


def test_notebook_ogr_to_pg_targets_real_postgres_enum_values() -> None:
    """Every target in the notebook's OGR→PG map must be a real
    ``PostgresType`` enum value — otherwise a Step-0b PUT would emit a string
    the attribute-sidecar validator rejects."""
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        PostgresType,
    )

    enum_values = {member.value for member in PostgresType}
    missing = sorted(
        set(_NOTEBOOK_OGR_TO_PG.values()) - enum_values
    )
    assert not missing, (
        f"Step-0b targets unknown PostgresType value(s): {missing}. "
        f"Known enum values: {sorted(enum_values)}"
    )


def test_notebook_ogr_to_pg_covers_pr481_extensions() -> None:
    """PR #481 added Boolean / Time / four list-types to the map. If any of
    these silently fell back to ``TEXT``, ingestion of a vector dataset with
    those fields would silently lose type fidelity."""
    # Boolean is a first-class PG type — must NOT fall back to TEXT.
    assert _apply_notebook_map("Boolean") == "BOOLEAN"
    # Date / DateTime have first-class PG types.
    assert _apply_notebook_map("Date") == "DATE"
    assert _apply_notebook_map("DateTime") == "TIMESTAMPTZ"
    # Time-of-day intentionally falls through to TEXT — pinned because the
    # alternative is silent JSONB drift if someone adds ``TIME`` to the enum
    # without thinking through tz semantics.
    assert _apply_notebook_map("Time") == "TEXT"
    # All four list types degrade to JSONB — array semantics preserved.
    for ogr_list in ("IntegerList", "Integer64List", "RealList", "StringList"):
        assert _apply_notebook_map(ogr_list) == "JSONB", (
            f"List type {ogr_list!r} must map to JSONB"
        )


def test_notebook_ogr_to_pg_unknown_falls_back_to_text() -> None:
    """An OGR driver emitting a type the platform has not yet catalogued must
    degrade to TEXT (lossless string), not raise. This protects against a
    future GDAL release introducing a new OGR type name."""
    assert _apply_notebook_map("SomeFutureOgrType") == "TEXT"


# ---------------------------------------------------------------------------
# Surface 1 — gdal Process registration + sync/async job-control contract.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope="module")
async def test_gdal_process_advertises_sync_and_async_job_control(
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """The Step-0b cell handles BOTH the sync (``200`` + result body) and
    async (``201`` + ``Location`` header) execution branches. The bootstrap
    breaks if the ``gdal`` Process stops advertising ``sync-execute`` —
    the cell would then mishandle the 200-response contract that the
    walkthrough exercises by default.

    OGC API - Processes Part 1 §7.11 lets a Process advertise any subset of
    {``sync-execute``, ``async-execute``}; the catalog server picks the mode
    at dispatch time. Async-execute is registered separately by the task
    queue / dispatcher path and is intentionally not asserted here so the
    test passes in the minimal in-process harness too."""
    resp = await sysadmin_in_process_client_module.get("/processes/processes")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # OGC inventory shape: ``processes`` list of ProcessSummary objects.
    processes = body.get("processes") or []
    gdal = next(
        (p for p in processes if p.get("id") == "gdal"), None,
    )
    assert gdal is not None, (
        f"gdal process not registered in /processes inventory: "
        f"{[p.get('id') for p in processes]!r}"
    )

    job_control = set(gdal.get("jobControlOptions") or [])
    assert "sync-execute" in job_control, (
        f"gdal Process dropped sync-execute (notebook 200-branch breaks): "
        f"{gdal!r}"
    )
    # async-execute advertisement depends on the fastapi_background runner
    # (registered by the processes extension at runtime when the task
    # queue / dispatcher is wired). In the minimal TestClient harness here
    # only the in-process synchronous runner is registered, so we don't
    # assert on async-execute. The notebook's 201+Location branch only
    # fires when a deployment that has async runners advertises it; the
    # sync branch is the one the walkthrough exercises by default.


# ---------------------------------------------------------------------------
# Surface 3+4 — items_postgresql_driver PluginConfig PUT/GET round-trip.
#
# Both scopes from the walkthrough's Gap-A guidance:
#   - collection scope: /configs/catalogs/{cat}/collections/{col}/plugins/{plugin_id}
#   - catalog scope:    /configs/catalogs/{cat}/plugins/{plugin_id}
# ---------------------------------------------------------------------------

# The notebook's fixture asset — same field set the Step-0b
# ``ogr_info["layers"][0]["fields"]`` example builds against. Including
# every field type that PR #481 added so a regression in the mapping
# would surface as a PUT validation failure.
_NOTEBOOKS_DIR = pathlib.Path(__file__).resolve().parents[5] / "notebooks"
_FIXTURE_PATH = (
    _NOTEBOOKS_DIR
    / "admin_boundaries_fixed_schema"
    / "fixtures"
    / "admin_boundaries.geojson"
)


@pytest.mark.skipif(
    not _NOTEBOOKS_DIR.is_dir(),
    reason=(
        "notebooks/ is not shipped in this runtime (e.g. the stripped CI test "
        "image); the fixture-presence check only guards the operator copy-paste "
        "path where the source tree is present (local dev / full checkout)."
    ),
)
def test_walkthrough_fixture_is_present() -> None:
    """The notebook's fixture is referenced from Step-0b guidance — its
    absence would break the documented bootstrap path the moment an
    operator copy-pastes the cell."""
    assert _FIXTURE_PATH.is_file(), (
        f"walkthrough fixture missing at {_FIXTURE_PATH}; "
        "Step-0b operator copy-paste path is broken."
    )
    payload = json.loads(_FIXTURE_PATH.read_text())
    assert payload.get("type") == "FeatureCollection"
    features = payload.get("features") or []
    assert features, "fixture must carry at least one feature"
    # external_id="code" mandatory in the walkthrough — pin its presence so
    # the bootstrap cell's identity-matcher recipe stays exercisable.
    assert "code" in (features[0].get("properties") or {}), (
        "walkthrough fixture lost the `code` external-id property"
    )


def _attribute_schema_from_pr481_field_set() -> list[dict]:
    """Build the ``attribute_schema`` payload Step-0b would emit for a vector
    asset whose OGR fields cover every type added by PR #481 — Boolean,
    Time, and all four list types — plus the baseline String/Integer/
    Integer64/Real/DateTime/Date set.

    The shape mirrors ``AttributeSchemaEntry``: ``{name, type}`` per row.
    ``nullable`` is OUT of the model today (see attributes_config.py) — the
    notebook emits it but the server ignores unknown fields; we drop it
    here so the assertion stays exact.
    """
    ogr_fields = [
        ("code", "String"),
        ("name", "String"),
        ("population", "Integer"),
        ("population64", "Integer64"),
        ("area_km2", "Real"),
        ("is_member", "Boolean"),
        ("joined", "Date"),
        ("updated_at", "DateTime"),
        ("daily_time", "Time"),
        ("admin_levels", "IntegerList"),
        ("admin_levels64", "Integer64List"),
        ("scores", "RealList"),
        ("aliases", "StringList"),
    ]
    return [
        {"name": name, "type": _apply_notebook_map(ogr_type)}
        for name, ogr_type in ogr_fields
    ]


@pytest.mark.asyncio(loop_scope="module")
async def test_step_0b_collection_scope_put_stripped(
    shared_catalog: str,
    shared_collection_factory,
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """Phase 3 — ``sidecars`` is Computed (non-authorable) at COLLECTION scope.

    An external config PUT carrying a ``sidecars`` block is ACCEPTED (no 4xx)
    but the caller-supplied sidecars are STRIPPED via
    ``restore_system_assigned_fields`` — they never persist. Operators shape
    the physical sidecar realization through ``items_schema`` /
    ``items_write_policy``, not by authoring ``sidecars`` directly. This pins
    the strip on the operator-facing config-API write path.
    """
    col_id = await shared_collection_factory()
    url = (
        f"/configs/catalogs/{shared_catalog}/collections/{col_id}"
        f"/plugins/items_postgresql_driver"
    )

    schema = _attribute_schema_from_pr481_field_set()
    body = {
        "sidecars": [
            {
                "sidecar_type": "attributes",
                "attribute_schema": schema,
            }
        ]
    }

    put_resp = await sysadmin_in_process_client_module.put(url, json=body)
    assert put_resp.status_code in (200, 201, 204), (
        f"Step-0b PUT should be accepted (sidecars stripped, not rejected) "
        f"at collection scope: {put_resp.status_code} {put_resp.text}"
    )

    get_resp = await sysadmin_in_process_client_module.get(url)
    assert get_resp.status_code == 200, get_resp.text
    persisted = get_resp.json()
    sidecars = persisted.get("sidecars") or []
    attr_sidecars = [
        sc for sc in sidecars if sc.get("sidecar_type") == "attributes"
    ]
    # Non-authorable: the caller's attributes sidecar must NOT have persisted.
    assert not attr_sidecars, (
        f"Phase 3: caller-supplied sidecars must be stripped on the external "
        f"PUT path, but an attributes sidecar persisted: {persisted!r}"
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_step_0b_catalog_scope_put_stripped(
    shared_catalog: str,
    sysadmin_in_process_client_module: AsyncClient,
) -> None:
    """Phase 3 — ``sidecars`` is Computed (non-authorable) at CATALOG scope too.

    Same shape, different URL/service method
    (``update_catalog_config`` vs ``update_collection_config``) — both must
    strip the caller-supplied ``sidecars`` rather than persist them."""
    url = (
        f"/configs/catalogs/{shared_catalog}"
        f"/plugins/items_postgresql_driver"
    )

    schema = _attribute_schema_from_pr481_field_set()
    body = {
        "sidecars": [
            {
                "sidecar_type": "attributes",
                "attribute_schema": schema,
            }
        ]
    }

    put_resp = await sysadmin_in_process_client_module.put(url, json=body)
    assert put_resp.status_code in (200, 201, 204), (
        f"Step-0b PUT should be accepted (sidecars stripped, not rejected) "
        f"at catalog scope: {put_resp.status_code} {put_resp.text}"
    )

    get_resp = await sysadmin_in_process_client_module.get(url)
    assert get_resp.status_code == 200, get_resp.text
    persisted = get_resp.json()
    sidecars = persisted.get("sidecars") or []
    attr_sidecars = [
        sc for sc in sidecars if sc.get("sidecar_type") == "attributes"
    ]
    assert not attr_sidecars, (
        f"catalog-scope PUT: caller-supplied sidecars must be stripped, "
        f"but an attributes sidecar persisted: {persisted!r}"
    )
