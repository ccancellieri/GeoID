"""Real-DB integration: a review-routed ``gdal`` runs on the in-process
background runner on the catalog tier — the #1676 review path.

Proves the worker dispatch path end-to-end against a live Postgres:

1. Under the ``review`` routing preset, ``gdal`` resolves to
   ``runner="background"``, ``consumers=["catalog"]`` — in-process on the
   catalog pod, not offloaded to a Cloud Run Job.
2. The behavioural pivot: when ``gdal`` is a real (non-placeholder) task —
   which it is in the review/local image where ``module_gdal`` brings osgeo —
   ``BackgroundRunner.can_handle("gdal")`` is ``True``. In prod ``scope_catalog``
   ``module_gdal`` is excluded, ``gdal`` is a definition-only placeholder, and
   ``can_handle`` is ``False`` (so the cloud preset offloads it to a Job).
3. The background runner creates the audit task row and actually executes the
   task IN-PROCESS (on this pod), passing the request inputs through to the
   task and producing its result — versus today's sync/offloaded execution.

The real ``GdalInfoTask`` hard-imports ``from osgeo import gdal`` and cannot be
loaded without the native GDAL stack, so this test registers a no-osgeo
stand-in under the same ``gdal`` key. Only the raster computation is stubbed —
the routing decision and the runner dispatch are the production code under test.

Why in-process is review/local-ONLY and prod offloads to a Cloud Run Job:
each prod instance is a stateless Cloud Run service running ~2 worker
processes with request-scoped CPU; a heavy in-process background gdal would
contend with request serving, be CPU-throttled between requests, and be lost
when the instance is recycled. The cloud preset therefore keeps gdal on
``gcp_cloud_run`` (full-CPU, isolated, survives recycle). This test exercises
the review/local convenience where the service runs as a single always-on
container.

Scope note: this asserts the routing + in-process *execution*. The terminal
COMPLETED status flip is owned by ``tasks_module.update_task`` (covered by the
liveness ``complete_task`` round-trips) and is orthogonal to the #1676 change.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import RunnerContext
from dynastore.modules.tasks.routing.exec_hints import ExecHint
from dynastore.modules.tasks.routing.matrix import InventoryItem, build_routing_matrix
from dynastore.modules.tasks.runners import BackgroundRunner


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
    pytest.mark.enable_modules(
        "db_config", "db", "tasks", "catalog", "stats", "iam",
        "stac", "collection_postgresql", "catalog_postgresql",
    ),
]

# Records the in-process invocation of the stand-in gdal task (the runner
# constructs its own instance, so the task reports back through here).
_RUN_RECORD: dict = {}


class _StubGdalInfoTask:
    """Stand-in for ``GdalInfoTask`` with the osgeo import removed.

    Registered under the ``gdal`` key so ``get_task_instance("gdal")`` returns a
    real, claimable instance (mirroring the review/local image where osgeo is
    present) without pulling in the native GDAL library.
    """

    task_type = "gdal"
    priority = 50
    affinity_tier = "catalog"
    is_placeholder = False

    async def run(self, payload):
        # Mirrors GdalInfoTask returning a metadata dict; no raster I/O.
        result = {
            "stub": True,
            "asset_id": payload.inputs.get("asset_id"),
            "driverShortName": "GTiff",
        }
        _RUN_RECORD["called"] = True
        _RUN_RECORD["asset_id"] = payload.inputs.get("asset_id")
        _RUN_RECORD["result"] = result
        return result


@pytest.fixture
def _stub_gdal_task():
    """Register the no-osgeo gdal stand-in for the duration of one test."""
    from dynastore.tasks import _DYNASTORE_TASKS, TaskConfig

    _RUN_RECORD.clear()
    original = _DYNASTORE_TASKS.get("gdal")
    _DYNASTORE_TASKS["gdal"] = TaskConfig(
        cls=_StubGdalInfoTask,
        module_name="test_review_gdal_worker",
        name="gdal",
        type="task",
        definition=None,
        instance=None,
    )
    try:
        yield
    finally:
        if original is not None:
            _DYNASTORE_TASKS["gdal"] = original
        else:
            _DYNASTORE_TASKS.pop("gdal", None)


async def test_review_routed_gdal_executes_on_background_runner_in_process(
    task_app_state, _stub_gdal_task
):
    engine = task_app_state.engine

    # 1. Review routing decision: gdal -> in-process background runner on catalog.
    _, procs = build_routing_matrix(
        [InventoryItem(task_key="gdal", kind="process", affinity_tier=None)],
        preset="review",
    )
    target = procs["gdal"][0]
    assert target.runner == "background"
    assert target.consumers == ["catalog"]
    assert ExecHint.BACKGROUND in target.hints

    # 2. The pivot: with osgeo present (stubbed here) gdal is a real, claimable
    #    in-process task — can_handle is True. It is False in prod scope_catalog
    #    where module_gdal is excluded and gdal is a definition-only placeholder.
    runner = BackgroundRunner()
    assert runner.can_handle("gdal") is True

    # 3. Drive an actual in-process execution through the background runner.
    #    Patch only the executor's *scheduling* so the background coroutine runs
    #    on this test loop — the production executor does the same on its own
    #    loop. The task execution below is real.
    scheduled = {}

    class _InlineExecutor:
        def submit(self, coro, task_name=None):
            fut = asyncio.ensure_future(coro)
            scheduled["fut"] = fut
            return fut

    ctx = RunnerContext(
        engine=engine,
        task_type="gdal",
        caller_id="review-gdal-worker-test",
        inputs={"asset_id": "demo-asset"},
        db_schema="s_review_demo",
        extra_context={},
    )
    with patch(
        "dynastore.modules.tasks.runners.get_background_executor",
        return_value=_InlineExecutor(),
    ):
        status_info = await runner.run(ctx)
    assert status_info is not None
    assert status_info.status == "accepted"
    task_id = status_info.jobID

    task_schema = tasks_module.get_task_schema()
    try:
        # 4. The runner created the audit task row (dispatch happened).
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(
                f'SELECT task_type, schema_name FROM "{task_schema}".tasks '
                "WHERE task_id = :tid",
                result_handler=ResultHandler.ONE_DICT,
            ).execute(conn, tid=task_id)
        assert row is not None, "runner must create an audit task row"
        assert row["task_type"] == "gdal"

        # 5. Await the scheduled coroutine — the gdal task ran IN-PROCESS here
        #    (the catalog pod), with the request inputs threaded through, and
        #    produced its result. This is the background-async-on-catalog path.
        await asyncio.wait_for(scheduled["fut"], timeout=30)
        assert _RUN_RECORD.get("called") is True
        assert _RUN_RECORD.get("asset_id") == "demo-asset"
        assert _RUN_RECORD.get("result", {}).get("driverShortName") == "GTiff"
    finally:
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                f'DELETE FROM "{task_schema}".tasks WHERE task_id = :tid',
                result_handler=ResultHandler.NONE,
            ).execute(conn, tid=task_id)
