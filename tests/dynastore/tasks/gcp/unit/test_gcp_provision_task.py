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

"""Unit tests for the fail-soft eventing path in GcpProvisionCatalogTask.

No DB, no GCP clients — all collaborators are replaced with stubs.

Scenario A: eventing PermissionDenied → catalog reaches ready, gcp_eventing
            step is 'degraded', task does NOT mark gcp_bucket failed.
Scenario B: bucket failure → gcp_bucket step is 'failed', catalog stays failed
            (unchanged existing behaviour).
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Module-level guard: skip this entire test file when google-cloud is absent
# (same pattern as test_bucket_reconcile_task.py). The guard runs at
# collection time so the module-level import of task.py (which hard-imports
# google.cloud.storage) never executes on environments without the SCOPE.
pytest.importorskip("google.cloud.storage")

import uuid  # noqa: E402

from dynastore.tasks.gcp_provision.task import (  # noqa: E402
    GcpProvisionInputs,
    ProvisioningTask,
)
from dynastore.modules.tasks.models import TaskPayload, PermanentTaskFailure  # noqa: E402


def _make_payload(catalog_id: str) -> TaskPayload:
    return TaskPayload(
        task_id=uuid.uuid4(),
        caller_id="test:unit",
        inputs=GcpProvisionInputs(catalog_id=catalog_id),
    )


# ---------------------------------------------------------------------------
# Minimal exception stubs matching the google.api_core shapes we care about
# ---------------------------------------------------------------------------

class _FakePermissionDenied(Exception):
    """Simulates google.api_core.exceptions.PermissionDenied."""


class _FakeForbidden(Exception):
    """Simulates google.api_core.exceptions.Forbidden."""


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_GET_PROTOCOL = "dynastore.tasks.gcp_provision.task.get_protocol"
_GET_STORAGE = "dynastore.tasks.gcp_provision.task._get_storage_protocol"
_GET_CATALOGS = "dynastore.tasks.gcp_provision.task._get_catalog_protocol"
_EVENTING_PERMISSION_ERRORS = (
    "dynastore.tasks.gcp_provision.task._EVENTING_PERMISSION_ERRORS"
)
_LOG_WARNING = "dynastore.tasks.gcp_provision.task.log_warning"
_LOG_ERROR = "dynastore.tasks.gcp_provision.task.log_error"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_storage_stub(bucket_name: str = "test-bucket") -> MagicMock:
    stub = MagicMock()
    stub.ensure_storage_for_catalog = AsyncMock(return_value=bucket_name)
    # Not a GcpCatalogProvisioning instance by default
    return stub


def _make_catalogs_stub() -> MagicMock:
    stub = MagicMock()
    stub.mark_provisioning_step = AsyncMock(return_value=True)
    stub.update_provisioning_status = AsyncMock(return_value=True)
    return stub


def _make_eventing_stub(raises: Optional[Exception] = None) -> MagicMock:
    stub = MagicMock()
    if raises is not None:
        stub.setup_catalog_eventing = AsyncMock(side_effect=raises)
    else:
        stub.setup_catalog_eventing = AsyncMock(return_value=("topic", None))
    return stub


async def _run_task(
    storage_stub: MagicMock,
    catalogs_stub: MagicMock,
    eventing_stub: Optional[MagicMock] = None,
    permission_error_types: tuple = (),
) -> Dict[str, Any]:
    task = ProvisioningTask()
    payload = _make_payload("cat-test")

    def _get_proto(cls):
        from dynastore.models.protocols import EventingProtocol
        if cls is EventingProtocol:
            return eventing_stub
        return None

    with (
        patch(_GET_STORAGE, return_value=storage_stub),
        patch(_GET_CATALOGS, return_value=catalogs_stub),
        patch(_GET_PROTOCOL, side_effect=_get_proto),
        patch(_EVENTING_PERMISSION_ERRORS, permission_error_types),
        patch(_LOG_WARNING, new=AsyncMock()),
        patch(_LOG_ERROR, new=AsyncMock()),
    ):
        return await task.run(payload)


# ---------------------------------------------------------------------------
# Test A — eventing PermissionDenied → catalog reaches ready, eventing degraded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_eventing_permission_denied_catalog_reaches_ready() -> None:
    """When eventing raises PermissionDenied after a successful bucket, the
    catalog must reach ready (gcp_eventing='degraded', gcp_bucket='complete').
    The task must NOT call mark_provisioning_step with 'failed'."""
    storage = _make_storage_stub()
    catalogs = _make_catalogs_stub()
    eventing = _make_eventing_stub(raises=_FakePermissionDenied("403 IAM_PERMISSION_DENIED"))

    result = await _run_task(
        storage_stub=storage,
        catalogs_stub=catalogs,
        eventing_stub=eventing,
        permission_error_types=(_FakePermissionDenied, _FakeForbidden),
    )

    assert result["status"] == "ready"
    assert result["eventing_status"] == "degraded"
    assert result["catalog_id"] == "cat-test"

    # gcp_bucket must be marked complete
    bucket_calls = [
        call for call in catalogs.mark_provisioning_step.call_args_list
        if call.args[1] == "gcp_bucket"
    ]
    assert bucket_calls, "gcp_bucket step must be marked"
    assert bucket_calls[0].args[2] == "complete", (
        f"gcp_bucket must be 'complete', got {bucket_calls[0].args[2]!r}"
    )

    # gcp_eventing must be marked degraded
    eventing_calls = [
        call for call in catalogs.mark_provisioning_step.call_args_list
        if call.args[1] == "gcp_eventing"
    ]
    assert eventing_calls, "gcp_eventing step must be marked"
    assert eventing_calls[0].args[2] == "degraded", (
        f"gcp_eventing must be 'degraded', got {eventing_calls[0].args[2]!r}"
    )

    # Must NOT call update_provisioning_status or mark anything 'failed'
    catalogs.update_provisioning_status.assert_not_called()
    failed_calls = [
        call for call in catalogs.mark_provisioning_step.call_args_list
        if call.args[2] == "failed"
    ]
    assert not failed_calls, (
        f"No step should be marked 'failed' on eventing PermissionDenied: {failed_calls}"
    )


@pytest.mark.asyncio
async def test_eventing_generic_error_also_degrades() -> None:
    """Any non-OrphanSubscriptionClash eventing error after the bucket is
    healthy degrades eventing without failing the catalog."""
    storage = _make_storage_stub()
    catalogs = _make_catalogs_stub()
    eventing = _make_eventing_stub(raises=RuntimeError("pubsub unavailable"))

    result = await _run_task(
        storage_stub=storage,
        catalogs_stub=catalogs,
        eventing_stub=eventing,
    )

    assert result["status"] == "ready"
    assert result["eventing_status"] == "degraded"


# ---------------------------------------------------------------------------
# Test B — bucket failure → gcp_bucket 'failed', catalog stays failed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bucket_retryable_failure_does_not_mark_failed() -> None:
    """A retryable RuntimeError on the bucket step must NOT permanently mark
    gcp_bucket 'failed'. The task re-raises so the dispatcher can retry."""
    storage = _make_storage_stub()
    storage.ensure_storage_for_catalog = AsyncMock(
        side_effect=RuntimeError("GCS 503 service unavailable")
    )
    catalogs = _make_catalogs_stub()

    task = ProvisioningTask()
    payload = _make_payload("cat-fail")

    def _get_proto(cls):
        return None

    with (
        patch(_GET_STORAGE, return_value=storage),
        patch(_GET_CATALOGS, return_value=catalogs),
        patch(_GET_PROTOCOL, side_effect=_get_proto),
        patch(_LOG_WARNING, new=AsyncMock()),
        patch(_LOG_ERROR, new=AsyncMock()),
    ):
        with pytest.raises(RuntimeError, match="GCS 503"):
            await task.run(payload)

    # The generic RuntimeError path does NOT mark failed (retryable).
    all_step_calls = catalogs.mark_provisioning_step.call_args_list
    failed_calls = [c for c in all_step_calls if "failed" in c.args]
    # Non-credential RuntimeError → retryable → no permanent failure marking
    assert not failed_calls, (
        f"Retryable RuntimeError must not permanently mark failed: {failed_calls}"
    )


@pytest.mark.asyncio
async def test_bucket_credential_failure_marks_skipped() -> None:
    """A RuntimeError with 'credentials' in the message → permanent skip
    so on-prem/unauthorized deployments still reach ready."""
    storage = _make_storage_stub()
    storage.ensure_storage_for_catalog = AsyncMock(
        side_effect=RuntimeError("failed to create a storage client: credentials error")
    )
    catalogs = _make_catalogs_stub()

    task = ProvisioningTask()
    payload = _make_payload("cat-creds")

    def _get_proto(cls):
        return None

    with (
        patch(_GET_STORAGE, return_value=storage),
        patch(_GET_CATALOGS, return_value=catalogs),
        patch(_GET_PROTOCOL, side_effect=_get_proto),
        patch(_LOG_WARNING, new=AsyncMock()),
        patch(_LOG_ERROR, new=AsyncMock()),
    ):
        with pytest.raises(PermanentTaskFailure):
            await task.run(payload)

    skipped_calls = [
        c for c in catalogs.mark_provisioning_step.call_args_list
        if "skipped" in c.args
    ]
    assert skipped_calls, "Credentials error must mark step 'skipped'"


# ---------------------------------------------------------------------------
# Test C — evaluate_checklist honours 'degraded' as terminal-good
# ---------------------------------------------------------------------------

def test_evaluate_checklist_degraded_is_terminal_good() -> None:
    """A checklist with 'complete' + 'degraded' must resolve to STATUS_READY."""
    from dynastore.modules.catalog.provisioning_registry import (
        evaluate_checklist,
        STATUS_READY,
    )
    result = evaluate_checklist({"gcp_bucket": "complete", "gcp_eventing": "degraded"})
    assert result == STATUS_READY, (
        f"complete+degraded must yield STATUS_READY, got {result!r}"
    )


def test_evaluate_checklist_failed_still_fails() -> None:
    """A checklist where gcp_bucket='failed' must still resolve to STATUS_FAILED
    even when gcp_eventing='degraded'."""
    from dynastore.modules.catalog.provisioning_registry import (
        evaluate_checklist,
        STATUS_FAILED,
    )
    result = evaluate_checklist({"gcp_bucket": "failed", "gcp_eventing": "degraded"})
    assert result == STATUS_FAILED, (
        f"failed+degraded must yield STATUS_FAILED, got {result!r}"
    )
