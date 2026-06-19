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

"""Unit tests for GCP provisioning task error-handling contract.

Regression guard for the 2026-04-22 production incident where both
"StorageProtocol not available" (Mode 1) and "Bucket name returned as None"
(Mode 2) were incorrectly classified as PermanentTaskFailure, causing every
catalog after the first to be permanently stuck in 'failed' state with zero
retries consumed.

Expected behaviour after the fix:
- Transient errors (module unavailable, GCS conflicts) → plain RuntimeError
  so the dispatcher increments retry_count and tries again.
- Permanent errors (bad credentials, client init failure) → PermanentTaskFailure
  so the dispatcher dead-letters the task immediately.
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

pytest.importorskip("google")  # optional dep — skip when SCOPE excludes it

from dynastore.tasks.gcp_provision.task import (
    _get_storage_protocol,
    ProvisioningTask,
)
from dynastore.modules.tasks.models import PermanentTaskFailure


# ---------------------------------------------------------------------------
# _get_storage_protocol — must raise RuntimeError (retryable), never
# PermanentTaskFailure, when no StorageProtocol is registered
# ---------------------------------------------------------------------------


def test_get_storage_protocol_raises_runtime_error_when_unavailable():
    """StorageProtocol missing → RuntimeError (dispatcher retries), not PermanentTaskFailure."""
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=None
    ):
        with pytest.raises(RuntimeError, match="StorageProtocol not available"):
            _get_storage_protocol()


def test_get_storage_protocol_not_permanent_failure():
    """StorageProtocol missing must NOT raise PermanentTaskFailure."""
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=None
    ):
        with pytest.raises(Exception) as exc_info:
            _get_storage_protocol()
        assert not isinstance(exc_info.value, PermanentTaskFailure), (
            "StorageProtocol unavailable should be retryable, not permanent"
        )


def test_get_storage_protocol_returns_instance_when_available():
    mock_storage = MagicMock()
    with patch(
        "dynastore.tasks.gcp_provision.task.get_protocol", return_value=mock_storage
    ):
        result = _get_storage_protocol()
    assert result is mock_storage


# ---------------------------------------------------------------------------
# ProvisioningTask.run — permanent vs retryable classification
# ---------------------------------------------------------------------------


def _make_payload(catalog_id: str = "test_cat"):
    payload = MagicMock()
    payload.inputs.catalog_id = catalog_id
    return payload


@pytest.mark.asyncio
async def test_credentials_error_skips_step_and_is_permanent():
    """RuntimeError containing 'credentials' → PermanentTaskFailure, and BOTH
    gcp_bucket and gcp_eventing steps are marked 'skipped' so the catalog still
    becomes ready (on-prem / unauthorized is not a provisioning failure — #1175)."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError("GCPModule credentials not available"),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(PermanentTaskFailure):
            await task.run(_make_payload("c1"))

    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_bucket", "skipped")
    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_eventing", "skipped")


@pytest.mark.asyncio
async def test_client_init_error_skips_step_and_is_permanent():
    """'failed to create a storage client' → PermanentTaskFailure, and BOTH
    gcp_bucket and gcp_eventing steps are marked 'skipped' (GCP not usable on
    this host → catalog still ready, not wedged in 'failed' — #1175)."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError(
                "GCPModule has not been initialized or failed to create a storage client"
            ),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(PermanentTaskFailure):
            await task.run(_make_payload("c1"))

    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_bucket", "skipped")
    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_eventing", "skipped")


@pytest.mark.asyncio
async def test_storage_protocol_unavailable_is_retryable():
    """'StorageProtocol not available' → plain RuntimeError, catalog NOT marked failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError(
                "StorageProtocol not available - GCP module not loaded"
            ),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="StorageProtocol not available"):
            await task.run(_make_payload("c1"))

    # Catalog must NOT be marked failed — leave in 'provisioning' so retry can succeed
    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_bucket_name_none_is_retryable():
    """'Bucket name returned as None' → retryable RuntimeError, catalog NOT marked failed."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value=None)

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="ensure_storage_for_catalog returned None"):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_lifespan_not_ready_is_retryable():
    """'GCPModule has not been initialized' (no bucket service yet) → retryable."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            side_effect=RuntimeError("GCPModule has not been initialized."),
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(RuntimeError, match="not been initialized"):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_not_called()


@pytest.mark.asyncio
async def test_successful_provision_completes_step():
    """Happy path: bucket provisioned and eventing complete → both steps marked,
    result status is 'ready' (which flips the catalog ready — #1175)."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value="d88971-test-catalog-ok")

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._provision_eventing",
            new=AsyncMock(return_value="complete"),
        ),
    ):
        result = await task.run(_make_payload("c1"))

    assert result["status"] == "ready"
    assert result["bucket_name"] == "d88971-test-catalog-ok"
    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_bucket", "complete")
    mock_catalogs.mark_provisioning_step.assert_any_await("c1", "gcp_eventing", "complete")


@pytest.mark.asyncio
async def test_bucket_conflict_marks_conflict_and_is_permanent():
    """BucketConflictError → catalog marked 'conflict' + PermanentTaskFailure.

    A deterministic bucket name owned by another project/catalog can never be
    claimed on retry, so the task must dead-letter (permanent) rather than spin.
    The catalog goes to 'conflict' (distinct from 'failed') and — critically —
    no GCS resource is deleted: that is asserted at the bucket-service layer.
    """
    from dynastore.modules.gcp.tools.bucket import BucketConflictError

    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(
        side_effect=BucketConflictError(
            "Bucket 'proj-c1' is already linked to another catalog"
        )
    )

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
    ):
        with pytest.raises(PermanentTaskFailure, match="conflict"):
            await task.run(_make_payload("c1"))

    mock_catalogs.update_provisioning_status.assert_awaited_once_with("c1", "conflict")


# ---------------------------------------------------------------------------
# Note: the legacy ``@requires(StorageProtocol)`` / ``are_protocols_satisfied``
# gate was removed in favour of operator-controlled task routing
# (TaskRoutingConfig). The two tests that asserted that gate were dropped:
# routing is now a deployment concern, not a source-level declaration.
# Hard top-level imports of runtime deps in the task module + the routing
# config combine to deliver the same outcome (wrong-SCOPE services can't
# load the task class, so it never enters get_loaded_task_types()).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# GcpCatalogProvisioning sub-Protocol — capability gating
# ---------------------------------------------------------------------------


def test_gcp_module_satisfies_gcp_catalog_provisioning():
    """GCPModule must structurally satisfy the GcpCatalogProvisioning sub-Protocol
    so the typed isinstance dispatch in ProvisioningTask.run picks the
    combined bucket+eventing setup path instead of the cross-vendor fallback.
    """
    from dynastore.modules.gcp.gcp_module import GCPModule
    from dynastore.models.protocols import GcpCatalogProvisioning

    assert issubclass(GCPModule, GcpCatalogProvisioning)


def test_storage_without_setup_method_falls_back():
    """A StorageProtocol implementation that does NOT satisfy
    GcpCatalogProvisioning must NOT pass the isinstance check —
    callers fall back to ensure_storage_for_catalog + setup_catalog_eventing.
    """
    from dynastore.models.protocols import GcpCatalogProvisioning

    class MinimalStorage:
        async def ensure_storage_for_catalog(self, catalog_id, conn=None):
            return f"bucket-{catalog_id}"

    assert not isinstance(MinimalStorage(), GcpCatalogProvisioning)


@pytest.mark.asyncio
async def test_destroy_task_invokes_typed_destruction():
    """GcpDestroyCatalogTask must call EventingProtocol.teardown_catalog_eventing
    AND StorageProtocol.drop_storage directly — previously these were getattr-dispatched
    to non-existent methods and silently no-opped.  Path A bug-fix regression guard.
    """
    from dynastore.tasks.gcp_provision.task import GcpDestroyCatalogTask

    mock_storage = MagicMock()
    mock_storage.drop_storage = AsyncMock(return_value=True)

    mock_eventing = MagicMock()
    mock_eventing.teardown_catalog_eventing = AsyncMock(return_value=None)

    def _get_protocol_dispatch(proto):
        from dynastore.models.protocols import EventingProtocol
        if proto is EventingProtocol:
            return mock_eventing
        return None

    task = GcpDestroyCatalogTask()
    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task.get_protocol",
            side_effect=_get_protocol_dispatch,
        ),
    ):
        result = await task.run(_make_payload("destroy_test_cat"))

    mock_eventing.teardown_catalog_eventing.assert_awaited_once_with("destroy_test_cat")
    mock_storage.drop_storage.assert_awaited_once_with("destroy_test_cat")
    assert result["status"] == "destroyed"


# ---------------------------------------------------------------------------
# Eventing step — transient failure path: a generic (non-permission,
# non-clash) eventing error must be retried by the task queue, not silently
# degraded. gcp_eventing stays 'pending'; the catalog is NOT marked ready.
# Regression guard: the old "soft" handler swallowed errors into 'degraded';
# the new atomic contract retries transient eventing errors instead.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generic_eventing_error_is_transient_retry():
    """A generic (non-permission, non-clash) error from _provision_eventing is
    treated as transient: run() re-raises as RuntimeError starting 'Transient
    eventing failure', the catalog does NOT reach ready, and gcp_eventing is
    NOT marked 'degraded' (left pending for the task queue to retry)."""
    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value="bucket-c1")

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._provision_eventing",
            new=AsyncMock(side_effect=Exception("boom")),
        ),
    ):
        with pytest.raises(RuntimeError, match="Transient eventing failure"):
            await task.run(_make_payload("c1"))

    # gcp_eventing must NOT be marked 'degraded' — it stays pending for retry
    degraded_calls = [
        c for c in mock_catalogs.mark_provisioning_step.await_args_list
        if len(c.args) >= 3 and c.args[1] == "gcp_eventing" and c.args[2] == "degraded"
    ]
    assert not degraded_calls, (
        f"gcp_eventing must not be marked 'degraded' on transient error: {degraded_calls}"
    )


@pytest.mark.asyncio
async def test_orphan_subscription_clash_marks_eventing_failed():
    """A structural OrphanSubscriptionClash escaping the soft handler marks the
    gcp_eventing step 'failed' (terminal) and re-raises so the task dead-letters
    — the catalog is not left wedged with gcp_eventing 'pending'."""
    from dynastore.modules.gcp.gcp_eventing_ops import OrphanSubscriptionClash

    task = ProvisioningTask()
    mock_catalogs = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.ensure_storage_for_catalog = AsyncMock(return_value="bucket-c1")

    with (
        patch(
            "dynastore.tasks.gcp_provision.task._get_storage_protocol",
            return_value=mock_storage,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ),
        patch(
            "dynastore.tasks.gcp_provision.task._provision_eventing",
            new=AsyncMock(
                side_effect=OrphanSubscriptionClash(
                    sub_path="projects/p/subscriptions/ds-c1-default-sub",
                    bound_to="projects/p/topics/other",
                    expected="projects/p/topics/ds-c1-events",
                    project_id="p",
                )
            ),
        ),
    ):
        with pytest.raises((OrphanSubscriptionClash, PermanentTaskFailure)):
            await task.run(_make_payload("c1"))

    mock_catalogs.mark_provisioning_step.assert_any_await(
        "c1", "gcp_eventing", "failed"
    )
