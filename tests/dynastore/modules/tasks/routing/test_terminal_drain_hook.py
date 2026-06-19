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

"""Unit tests for the zero-latency checklist drain hook in apply_terminal_action (#1909).

Covers _drain_provisioning_checklist and the integration with apply_terminal_action:

- Terminal COMPLETED fires the drain for gcp_provision_catalog tasks.
- Retryable failure (status PENDING) does NOT fire the drain.
- DEAD_LETTER fires the drain for gcp_provision_catalog tasks.
- A drain exception does not propagate (fail-soft).
- Non-provisioning task types are not drained.
- Missing catalog_id in inputs skips the drain gracefully.

All DB-free: CatalogsProtocol and status reads are mocked.
Run with --noconftest (isolated) — no live DB required.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import execution


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_PROV_TASK_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_CATALOG_ID = "my-catalog-123"

_BASE_FIELDS = dict(
    task_id=_PROV_TASK_ID,
    task_type="gcp_provision_catalog",
    inputs={"catalog_id": _CATALOG_ID},
    caller_id="system",
    collection_id=None,
    schema="tasks",
    scope="CATALOG",
)


def _make_catalogs_mock(drain_return: bool = False) -> MagicMock:
    mock = MagicMock()
    mock.drain_pending_checklist_steps = AsyncMock(return_value=drain_return)
    return mock


# ---------------------------------------------------------------------------
# _drain_provisioning_checklist: unit tests
# ---------------------------------------------------------------------------

class TestDrainProvisioningChecklist:
    """Direct tests for the _drain_provisioning_checklist helper."""

    @pytest.mark.asyncio
    async def test_success_outcome_drains_immediately(self):
        """outcome='success' (COMPLETED) → drain called without re-reading status."""
        mock_catalogs = _make_catalogs_mock(drain_return=True)
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="success",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_awaited_once_with(
            _CATALOG_ID, terminal_status="degraded",
        )

    @pytest.mark.asyncio
    async def test_dead_letter_outcome_drains(self):
        """outcome='failure' with DEAD_LETTER ground truth → drain fires.

        Atomic contract: a terminally-failed provisioning task drains its
        pending steps to 'failed' (catalog → 'failed'), never 'degraded'
        (which would misreport an unprovisioned catalog as 'ready').
        """
        mock_catalogs = _make_catalogs_mock(drain_return=True)
        with (
            patch(
                "dynastore.modules.tasks.execution._read_task_status",
                AsyncMock(return_value="DEAD_LETTER"),
            ),
            patch(
                "dynastore.tools.discovery.get_protocol",
                return_value=mock_catalogs,
            ),
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="failure",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_awaited_once_with(
            _CATALOG_ID, terminal_status="failed",
        )

    @pytest.mark.asyncio
    async def test_retryable_failure_does_not_drain(self):
        """outcome='failure' with PENDING ground truth → no drain (retryable)."""
        mock_catalogs = _make_catalogs_mock()
        with (
            patch(
                "dynastore.modules.tasks.execution._read_task_status",
                AsyncMock(return_value="PENDING"),
            ),
            patch(
                "dynastore.tools.discovery.get_protocol",
                return_value=mock_catalogs,
            ),
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="failure",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_timeout_with_dead_letter_drains(self):
        """outcome='timeout' with DEAD_LETTER ground truth → drain fires."""
        mock_catalogs = _make_catalogs_mock(drain_return=True)
        with (
            patch(
                "dynastore.modules.tasks.execution._read_task_status",
                AsyncMock(return_value="DEAD_LETTER"),
            ),
            patch(
                "dynastore.tools.discovery.get_protocol",
                return_value=mock_catalogs,
            ),
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="timeout",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_provisioning_task_type_skipped(self):
        """A task_type not in _PROVISIONING_TASK_TYPES → drain not attempted."""
        mock_catalogs = _make_catalogs_mock()
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="ingestion",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="success",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_catalog_id_skips_gracefully(self):
        """inputs without catalog_id → drain skipped, no exception."""
        mock_catalogs = _make_catalogs_mock()
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ):
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"something_else": "x"},
                outcome="success",
            )
        mock_catalogs.drain_pending_checklist_steps.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_drain_exception_is_swallowed(self):
        """A drain failure must not propagate (fail-soft contract)."""
        mock_catalogs = MagicMock()
        mock_catalogs.drain_pending_checklist_steps = AsyncMock(
            side_effect=RuntimeError("DB down")
        )
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ):
            # Should not raise.
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="success",
            )

    @pytest.mark.asyncio
    async def test_protocol_unavailable_skips_gracefully(self):
        """CatalogsProtocol not registered → drain skipped, no exception."""
        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=None,
        ):
            # Should not raise.
            await execution._drain_provisioning_checklist(
                MagicMock(),
                task_id=_PROV_TASK_ID,
                task_type="gcp_provision_catalog",
                inputs={"catalog_id": _CATALOG_ID},
                outcome="success",
            )


# ---------------------------------------------------------------------------
# apply_terminal_action: integration with drain hook
# ---------------------------------------------------------------------------

class TestApplyTerminalActionDrainIntegration:
    """Drain is triggered through apply_terminal_action for provisioning tasks."""

    @pytest.mark.asyncio
    async def test_completed_triggers_drain(self, monkeypatch):
        """apply_terminal_action with outcome='success' fires the drain."""
        from dynastore.modules.tasks.routing.model import Action, ActionVerb

        drain = AsyncMock()
        monkeypatch.setattr(execution, "_drain_provisioning_checklist", drain)

        await execution.apply_terminal_action(
            MagicMock(),
            outcome="success",
            action=Action(action=ActionVerb.REPORT),
            **_BASE_FIELDS,
        )

        drain.assert_awaited_once()
        _, kwargs = drain.await_args
        assert kwargs["task_type"] == "gcp_provision_catalog"
        assert kwargs["outcome"] == "success"
        assert kwargs["inputs"]["catalog_id"] == _CATALOG_ID

    @pytest.mark.asyncio
    async def test_dead_letter_triggers_drain(self, monkeypatch):
        """apply_terminal_action with outcome='failure' fires the drain (gate is
        inside _drain_provisioning_checklist, not apply_terminal_action)."""
        from dynastore.modules.tasks.routing.model import Action, ActionVerb

        drain = AsyncMock()
        monkeypatch.setattr(execution, "_drain_provisioning_checklist", drain)

        await execution.apply_terminal_action(
            MagicMock(),
            outcome="failure",
            action=Action(action=ActionVerb.DEAD_LETTER),
            **_BASE_FIELDS,
        )

        drain.assert_awaited_once()
        _, kwargs = drain.await_args
        assert kwargs["outcome"] == "failure"

    @pytest.mark.asyncio
    async def test_drain_called_even_without_route_action(self, monkeypatch):
        """Drain fires regardless of whether the routing Action is ROUTE or not."""
        from dynastore.modules.tasks.routing.model import Action, ActionVerb

        drain = AsyncMock()
        monkeypatch.setattr(execution, "_drain_provisioning_checklist", drain)

        # REPORT action → apply_terminal_action returns early after drain
        await execution.apply_terminal_action(
            MagicMock(),
            outcome="success",
            action=Action(action=ActionVerb.REPORT),
            **_BASE_FIELDS,
        )

        drain.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_drain_failure_does_not_break_routing(self, monkeypatch):
        """A drain exception inside _drain_provisioning_checklist must not
        surface into apply_terminal_action (the fail-soft contract)."""
        from dynastore.modules.tasks.routing.model import Action, ActionVerb

        monkeypatch.setattr(
            execution,
            "_drain_provisioning_checklist",
            AsyncMock(side_effect=RuntimeError("network")),
        )
        create = AsyncMock(return_value=object())
        monkeypatch.setattr(
            "dynastore.modules.tasks.tasks_module.create_task", create,
        )
        monkeypatch.setattr(execution, "_route_target_kind", lambda _p: "task")

        # Should not raise — ROUTE continuation still runs.
        await execution.apply_terminal_action(
            MagicMock(),
            outcome="success",
            action=Action(action=ActionVerb.ROUTE, process="next_step"),
            **_BASE_FIELDS,
        )
        create.assert_awaited_once()
