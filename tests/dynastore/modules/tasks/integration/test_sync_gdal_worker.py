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

"""Real-DB integration: gdal sync execution in-process on the maps tier via
``SyncRunner`` (``Prefer: respond-sync``).

Design under test
-----------------
Under both ``cloud`` and ``review`` presets, gdal is routed to
``runner="gcp_cloud_run"`` with ``consumers=["catalog", "maps"]`` — the async
default offloads to a Cloud Run Job.  Gdal sync execution on the maps service
is handled by ``SyncRunner`` when ``Prefer: respond-sync`` is sent.  The maps
service ships osgeo + ``worker_task_gdal``, so
``SyncRunner.can_handle("gdal")`` is True there.

What this test covers
---------------------
1. Routing parity: both ``cloud`` and ``review`` presets resolve gdal to
   ``runner="gcp_cloud_run"`` with ``consumers=["catalog", "maps"]``.  The
   review preset no longer has a gdal in-process special-case.
2. ``SyncRunner.can_handle("gdal")`` is ``True`` when a real (non-placeholder)
   gdal task instance is registered, mirroring the maps service.
3. End-to-end in-process sync execution via ``SyncRunner.run``:
   - creates an audit task row,
   - executes the stand-in gdal task synchronously in-process,
   - returns the task result directly (not a ``StatusInfo`` / ``Task`` object),
   - marks the audit row COMPLETED.

Stand-in task
-------------
The real ``GdalInfoTask`` hard-imports ``from osgeo import gdal`` and cannot be
loaded without the native GDAL stack, so this test registers a no-osgeo
stand-in under the ``gdal`` key.  Only the raster computation is stubbed;
the routing decision and runner dispatch are the production code under test.
"""

from __future__ import annotations

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
from dynastore.modules.tasks.runners import SyncRunner


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.timeout(120),
    pytest.mark.enable_modules(
        "db_config", "db", "tasks", "catalog", "stats", "iam",
        "stac", "collection_postgresql", "catalog_postgresql",
    ),
]

# Records the in-process invocation of the stand-in gdal task.
_RUN_RECORD: dict = {}


class _StubGdalInfoTask:
    """Stand-in for ``GdalInfoTask`` with the osgeo import removed.

    Registered under the ``gdal`` key so ``get_task_instance("gdal")`` returns
    a real, claimable instance — mirroring the maps service where osgeo and
    ``worker_task_gdal`` are installed — without requiring the native GDAL
    library.
    """

    task_type = "gdal"
    priority = 50
    affinity_tier = "maps"
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
        module_name="test_sync_gdal_worker",
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


async def test_sync_gdal_worker_executes_in_process_on_maps_tier(
    task_app_state, _stub_gdal_task
):
    engine = task_app_state.engine

    # 1. Routing parity: both cloud and review resolve gdal to gcp_cloud_run
    #    with consumers=["catalog", "maps"]. The review preset no longer has a
    #    gdal in-process special-case; it mirrors cloud exactly.
    _, procs_cloud = build_routing_matrix(
        [InventoryItem(task_key="gdal", kind="process", affinity_tier=None)],
        preset="cloud",
    )
    _, procs_review = build_routing_matrix(
        [InventoryItem(task_key="gdal", kind="process", affinity_tier=None)],
        preset="review",
    )
    target_cloud = procs_cloud["gdal"][0]
    target_review = procs_review["gdal"][0]

    assert target_cloud.runner == "gcp_cloud_run"
    assert target_review.runner == "gcp_cloud_run"
    assert target_cloud.consumers == ["catalog", "maps"]
    assert target_review.consumers == ["catalog", "maps"]
    assert target_cloud.runner == target_review.runner, (
        "review and cloud must produce identical gdal targets"
    )
    assert target_cloud.consumers == target_review.consumers, (
        "review and cloud must produce identical gdal consumer lists"
    )
    assert ExecHint.OFFLOAD in target_review.hints
    assert ExecHint.HEAVY in target_review.hints
    assert ExecHint.BACKGROUND not in target_review.hints

    # 2. With a real (non-placeholder) gdal task registered — as on the maps
    #    service where osgeo + worker_task_gdal are installed — SyncRunner
    #    can_handle returns True.
    runner = SyncRunner()
    assert runner.can_handle("gdal") is True

    # 3. Drive a full in-process sync execution through SyncRunner.
    #    SyncRunner.run executes the task inline (no background scheduling),
    #    so no executor patching is needed — the task runs on this test loop.
    ctx = RunnerContext(
        engine=engine,
        task_type="gdal",
        caller_id="sync-gdal-worker-test",
        inputs={"asset_id": "demo-asset"},
        db_schema="s_sync_gdal_demo",
        extra_context={},
    )
    result = await runner.run(ctx)

    # SyncRunner returns the task's own result directly, not a StatusInfo.
    assert result is not None
    assert isinstance(result, dict), (
        "SyncRunner must return the task result inline, not a StatusInfo"
    )
    assert result.get("driverShortName") == "GTiff"

    # 4. The runner created an audit task row and marked it COMPLETED.
    task_schema = tasks_module.get_task_schema()
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(
            f'SELECT task_type, status FROM "{task_schema}".tasks '
            "WHERE caller_id = :caller_id ORDER BY timestamp DESC LIMIT 1",
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn, caller_id="sync-gdal-worker-test")
    assert row is not None, "SyncRunner must create an audit task row"
    assert row["task_type"] == "gdal"
    assert row["status"] == "COMPLETED"

    # 5. The stand-in task ran in-process and recorded its invocation.
    assert _RUN_RECORD.get("called") is True
    assert _RUN_RECORD.get("asset_id") == "demo-asset"
    assert _RUN_RECORD.get("result", {}).get("driverShortName") == "GTiff"

    # Cleanup: remove the audit row created by this test run.
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f'DELETE FROM "{task_schema}".tasks WHERE caller_id = :caller_id',
            result_handler=ResultHandler.NONE,
        ).execute(conn, caller_id="sync-gdal-worker-test")
