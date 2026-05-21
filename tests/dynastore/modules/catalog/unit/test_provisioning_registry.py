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

"""Unit tests for the catalog provisioning-checklist registry (#1175).

Covers the pure pieces — the terminal :func:`evaluate_checklist` rule and the
:class:`ProvisioningRegistry` (active/inactive predicates, idempotent
re-registration, predicate-failure isolation). The DB-bound parts
(``create_catalog`` checklist build, ``mark_provisioning_step``) are exercised
by the integration suite against a live database.
"""

from __future__ import annotations

import pytest

from dynastore.modules.catalog.provisioning_registry import (
    STATUS_FAILED,
    STATUS_READY,
    STEP_COMPLETE,
    STEP_FAILED,
    STEP_PENDING,
    STEP_SKIPPED,
    ProvisioningRegistry,
    evaluate_checklist,
)


class TestEvaluateChecklist:
    """The terminal "default last" rule mapping a checklist to a catalog status."""

    def test_empty_checklist_is_ready(self):
        assert evaluate_checklist({}) == STATUS_READY
        assert evaluate_checklist(None) == STATUS_READY

    def test_all_complete_is_ready(self):
        assert evaluate_checklist({"a": STEP_COMPLETE, "b": STEP_COMPLETE}) == STATUS_READY

    def test_complete_plus_skipped_is_ready(self):
        # 'skipped' counts as terminal-good (on-prem / inactive provider).
        assert evaluate_checklist({"a": STEP_COMPLETE, "b": STEP_SKIPPED}) == STATUS_READY

    def test_all_skipped_is_ready(self):
        assert evaluate_checklist({"a": STEP_SKIPPED}) == STATUS_READY

    def test_any_failed_is_failed(self):
        assert evaluate_checklist({"a": STEP_COMPLETE, "b": STEP_FAILED}) == STATUS_FAILED

    def test_failed_wins_over_pending(self):
        # A genuine failure surfaces immediately, even with steps outstanding.
        assert evaluate_checklist({"a": STEP_FAILED, "b": STEP_PENDING}) == STATUS_FAILED

    def test_any_pending_keeps_provisioning(self):
        # None => no status change => stays 'provisioning'.
        assert evaluate_checklist({"a": STEP_COMPLETE, "b": STEP_PENDING}) is None
        assert evaluate_checklist({"a": STEP_PENDING}) is None


async def _active(catalog_id, conn):
    return True


async def _inactive(catalog_id, conn):
    return False


async def _boom(catalog_id, conn):
    raise RuntimeError("predicate exploded")


class TestProvisioningRegistry:
    @pytest.mark.asyncio
    async def test_empty_registry_builds_empty_checklist(self):
        reg = ProvisioningRegistry()
        assert await reg.build_checklist("cat") == {}

    @pytest.mark.asyncio
    async def test_only_active_provisioners_contribute(self):
        reg = ProvisioningRegistry()
        reg.register("gcp_bucket", _active)
        reg.register("other", _inactive)
        checklist = await reg.build_checklist("cat")
        assert checklist == {"gcp_bucket": STEP_PENDING}

    @pytest.mark.asyncio
    async def test_predicate_failure_is_treated_as_inactive(self):
        reg = ProvisioningRegistry()
        reg.register("gcp_bucket", _active)
        reg.register("flaky", _boom)
        # A misbehaving predicate must not block readiness — it just drops out.
        checklist = await reg.build_checklist("cat")
        assert checklist == {"gcp_bucket": STEP_PENDING}

    @pytest.mark.asyncio
    async def test_reregistration_is_idempotent_by_key(self):
        reg = ProvisioningRegistry()
        reg.register("gcp_bucket", _inactive)
        reg.register("gcp_bucket", _active)  # latest wins
        assert reg.keys == ["gcp_bucket"]
        assert await reg.build_checklist("cat") == {"gcp_bucket": STEP_PENDING}

    def test_register_rejects_empty_key(self):
        reg = ProvisioningRegistry()
        with pytest.raises(ValueError):
            reg.register("", _active)

    def test_unregister_and_clear(self):
        reg = ProvisioningRegistry()
        reg.register("a", _active)
        reg.register("b", _active)
        reg.unregister("a")
        assert reg.keys == ["b"]
        reg.clear()
        assert reg.keys == []
