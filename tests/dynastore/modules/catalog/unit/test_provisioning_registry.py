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

"""Unit tests for the catalog provisioning-checklist registry (#1175).

Covers the pure pieces — the terminal :func:`evaluate_checklist` rule and the
:class:`ProvisioningRegistry` (active/inactive predicates, idempotent
re-registration, predicate-failure isolation, priority ordering, scope
filtering, and the ``active_provisioners`` grouped accessor). The DB-bound
parts (``create_catalog`` checklist build, ``mark_provisioning_step``) are
exercised by the integration suite against a live database.
"""

from __future__ import annotations

import pytest

from dynastore.modules.catalog.provisioning_registry import (
    SCOPE_CATALOG,
    SCOPE_COLLECTION,
    STATUS_FAILED,
    STATUS_READY,
    STEP_COMPLETE,
    STEP_FAILED,
    STEP_PENDING,
    STEP_SKIPPED,
    LocalizedText,
    Provisioner,
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


class TestPriorityOrdering:
    """Provisioners in build_checklist appear in (priority, key) order."""

    @pytest.mark.asyncio
    async def test_priority_order_in_checklist(self):
        reg = ProvisioningRegistry()
        # Register out of priority order deliberately.
        reg.register("z_step", _active, priority=200)
        reg.register("a_step", _active, priority=50)
        reg.register("m_step", _active, priority=100)
        checklist = await reg.build_checklist("cat")
        assert list(checklist.keys()) == ["a_step", "m_step", "z_step"]

    @pytest.mark.asyncio
    async def test_equal_priority_sorted_by_key(self):
        reg = ProvisioningRegistry()
        reg.register("beta", _active, priority=10)
        reg.register("alpha", _active, priority=10)
        checklist = await reg.build_checklist("cat")
        assert list(checklist.keys()) == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_inactive_provisioners_excluded_from_ordering(self):
        reg = ProvisioningRegistry()
        reg.register("low", _active, priority=1)
        reg.register("mid", _inactive, priority=5)
        reg.register("high", _active, priority=10)
        checklist = await reg.build_checklist("cat")
        assert list(checklist.keys()) == ["low", "high"]
        assert "mid" not in checklist


class TestScopeFiltering:
    """build_checklist only includes provisioners matching the requested scope."""

    @pytest.mark.asyncio
    async def test_collection_scope_excluded_from_catalog_checklist(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_step", _active, scope=SCOPE_CATALOG)
        reg.register("collection_step", _active, scope=SCOPE_COLLECTION)
        checklist = await reg.build_checklist("cat")
        assert "catalog_step" in checklist
        assert "collection_step" not in checklist

    @pytest.mark.asyncio
    async def test_collection_scope_included_when_requested(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_step", _active, scope=SCOPE_CATALOG)
        reg.register("collection_step", _active, scope=SCOPE_COLLECTION)
        checklist = await reg.build_checklist("cat", scope=SCOPE_COLLECTION)
        assert "collection_step" in checklist
        assert "catalog_step" not in checklist

    @pytest.mark.asyncio
    async def test_default_scope_is_catalog(self):
        reg = ProvisioningRegistry()
        reg.register("only_catalog", _active, scope=SCOPE_CATALOG)
        checklist = await reg.build_checklist("cat")
        assert checklist == {"only_catalog": STEP_PENDING}


class TestDeferrableProvisioners:
    """``deferrable`` provisioners run at create by default, and are held back
    only when the checklist is built with ``defer=True`` (a ``?hints=defer``
    create). An explicit provision builds with ``defer=False`` to include them."""

    @pytest.mark.asyncio
    async def test_deferrable_included_at_create_by_default(self):
        # Default (no defer): a deferrable provisioner still runs at creation —
        # this is what keeps auto-provisioning the unchanged default behaviour.
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)
        checklist = await reg.build_checklist("cat")
        assert checklist == {"catalog_core": STEP_PENDING, "gcp_bucket": STEP_PENDING}

    @pytest.mark.asyncio
    async def test_deferrable_held_back_when_defer(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)
        checklist = await reg.build_checklist("cat", defer=True)
        assert checklist == {"catalog_core": STEP_PENDING}

    @pytest.mark.asyncio
    async def test_non_deferrable_unaffected_by_defer(self):
        # ``defer`` only holds back deferrable provisioners; plain ones still run.
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)
        checklist = await reg.build_checklist("cat", defer=True)
        assert "catalog_core" in checklist
        assert "gcp_bucket" not in checklist

    @pytest.mark.asyncio
    async def test_active_provisioners_include_deferrable_by_default(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)
        groups = await reg.active_provisioners("cat")
        keys = [p.key for group in groups for p in group]
        assert keys == ["catalog_core", "gcp_bucket"]

    @pytest.mark.asyncio
    async def test_active_provisioners_hold_back_deferrable_when_defer(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)
        groups = await reg.active_provisioners("cat", defer=True)
        keys = [p.key for group in groups for p in group]
        assert keys == ["catalog_core"]

    @pytest.mark.asyncio
    async def test_deferrable_default_is_false(self):
        reg = ProvisioningRegistry()
        reg.register("plain", _active)
        assert reg._provisioners["plain"].deferrable is False


class TestReprovisionDefeatsDefer:
    """KNOWN GAP (un-fao/GeoID#2678): ``defer`` is a request-time flag only —
    a checklist built with ``defer=True`` simply excludes deferrable
    provisioners rather than recording that they were intentionally held
    back.  A later rebuild with the default ``defer=False`` (what a generic
    ``catalog_provision`` reprovision run uses unless it is explicitly told
    the catalog was deferred) folds the deferrable provisioners back in.

    This characterises the *current* registry behaviour so a future fix
    (persisting a terminal ``deferred`` checklist state) has a red test to
    turn green.  It is a pre-existing platform gap, not something introduced
    or fixed by any single preset's use of ``Hint.DEFER``.
    """

    @pytest.mark.asyncio
    async def test_deferred_checklist_has_no_trace_of_the_held_back_step(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)

        deferred_checklist = await reg.build_checklist("cat", defer=True)

        # Bucket-free intent has no persisted marker — the key is simply absent.
        assert deferred_checklist == {"catalog_core": STEP_PENDING}
        assert "gcp_bucket" not in deferred_checklist

    @pytest.mark.asyncio
    async def test_undeferred_rebuild_resurrects_the_held_back_provisioner(self):
        reg = ProvisioningRegistry()
        reg.register("catalog_core", _active, priority=0)
        reg.register("gcp_bucket", _active, priority=100, deferrable=True)

        await reg.build_checklist("cat", defer=True)  # simulates the deferred create

        # A generic reprovision run (defer defaults False) has no way to know
        # gcp_bucket was ever deferred — it comes right back.
        rebuilt = await reg.active_provisioners("cat", defer=False)
        keys = [p.key for group in rebuilt for p in group]
        assert "gcp_bucket" in keys, (
            "documents un-fao/GeoID#2678 — remove this assertion once "
            "deferred state is persisted and reprovision honours it"
        )


class TestBackwardCompatRegister:
    """Two-argument register(key, is_active) keeps working unchanged."""

    @pytest.mark.asyncio
    async def test_two_arg_register_defaults(self):
        reg = ProvisioningRegistry()
        reg.register("gcp_bucket", _active)
        provisioner = reg._provisioners["gcp_bucket"]
        assert provisioner.priority == 100
        assert provisioner.scope == SCOPE_CATALOG
        assert provisioner.provision is None
        assert provisioner.deprovision is None

    @pytest.mark.asyncio
    async def test_two_arg_register_contributes_to_catalog_checklist(self):
        reg = ProvisioningRegistry()
        reg.register("gcp_bucket", _active)
        reg.register("gcp_eventing", _active)
        checklist = await reg.build_checklist("cat")
        assert checklist == {
            "gcp_bucket": STEP_PENDING,
            "gcp_eventing": STEP_PENDING,
        }


class TestActiveProvisioners:
    """active_provisioners groups equal priorities and excludes inactive ones."""

    @pytest.mark.asyncio
    async def test_empty_registry_returns_empty_groups(self):
        reg = ProvisioningRegistry()
        groups = await reg.active_provisioners("cat")
        assert groups == []

    @pytest.mark.asyncio
    async def test_groups_by_priority_ascending(self):
        reg = ProvisioningRegistry()
        reg.register("b_high", _active, priority=200)
        reg.register("a_low", _active, priority=10)
        reg.register("c_mid", _active, priority=100)
        groups = await reg.active_provisioners("cat")
        assert len(groups) == 3
        assert [g[0].priority for g in groups] == [10, 100, 200]

    @pytest.mark.asyncio
    async def test_equal_priority_in_same_group(self):
        reg = ProvisioningRegistry()
        reg.register("alpha", _active, priority=50)
        reg.register("beta", _active, priority=50)
        reg.register("solo", _active, priority=100)
        groups = await reg.active_provisioners("cat")
        assert len(groups) == 2
        assert len(groups[0]) == 2
        assert {p.key for p in groups[0]} == {"alpha", "beta"}
        assert groups[1][0].key == "solo"

    @pytest.mark.asyncio
    async def test_inactive_provisioners_excluded(self):
        reg = ProvisioningRegistry()
        reg.register("active_one", _active, priority=10)
        reg.register("inactive_one", _inactive, priority=10)
        groups = await reg.active_provisioners("cat")
        assert len(groups) == 1
        assert groups[0][0].key == "active_one"

    @pytest.mark.asyncio
    async def test_failing_predicate_excluded(self):
        reg = ProvisioningRegistry()
        reg.register("good", _active, priority=10)
        reg.register("bad", _boom, priority=10)
        groups = await reg.active_provisioners("cat")
        assert len(groups) == 1
        assert groups[0][0].key == "good"

    @pytest.mark.asyncio
    async def test_scope_filtering_in_active_provisioners(self):
        reg = ProvisioningRegistry()
        reg.register("cat_step", _active, scope=SCOPE_CATALOG)
        reg.register("col_step", _active, scope=SCOPE_COLLECTION)
        catalog_groups = await reg.active_provisioners("cat", scope=SCOPE_CATALOG)
        collection_groups = await reg.active_provisioners("cat", scope=SCOPE_COLLECTION)
        assert len(catalog_groups) == 1
        assert catalog_groups[0][0].key == "cat_step"
        assert len(collection_groups) == 1
        assert collection_groups[0][0].key == "col_step"


class TestProvisionerRecord:
    """Provisioner record stores all fields including lifecycle callables."""

    def test_provision_and_deprovision_stored(self):
        async def my_provision(catalog_id, conn):
            pass

        async def my_deprovision(catalog_id, conn):
            pass

        reg = ProvisioningRegistry()
        reg.register(
            "step",
            _active,
            provision=my_provision,
            deprovision=my_deprovision,
        )
        p = reg._provisioners["step"]
        assert isinstance(p, Provisioner)
        assert p.provision is my_provision
        assert p.deprovision is my_deprovision

    def test_provisioner_without_lifecycle_callables(self):
        reg = ProvisioningRegistry()
        reg.register("step", _active)
        p = reg._provisioners["step"]
        assert p.provision is None
        assert p.deprovision is None


class TestLocalizedNameDescription:
    """name and description fields support plain strings and multilanguage maps."""

    def test_plain_string_name_and_description(self):
        reg = ProvisioningRegistry()
        reg.register(
            "step",
            _active,
            name="GCP Bucket",
            description="Provisions the GCS bucket for this catalog.",
        )
        p = reg._provisioners["step"]
        assert p.name == "GCP Bucket"
        assert p.description == "Provisions the GCS bucket for this catalog."

    def test_multilanguage_name_and_description(self):
        name: LocalizedText = {"en": "GCP Bucket", "fr": "Seau GCP", "es": "Cubo GCP"}
        description: LocalizedText = {
            "en": "Provisions the GCS bucket for this catalog.",
            "fr": "Provisionne le seau GCS pour ce catalogue.",
        }
        reg = ProvisioningRegistry()
        reg.register("step", _active, name=name, description=description)
        p = reg._provisioners["step"]
        assert p.name == name
        assert p.description == description
        assert isinstance(p.name, dict)
        assert p.name["fr"] == "Seau GCP"  # type: ignore[index]

    def test_name_description_default_to_none(self):
        reg = ProvisioningRegistry()
        reg.register("step", _active)
        p = reg._provisioners["step"]
        assert p.name is None
        assert p.description is None

    def test_reregistration_updates_name(self):
        reg = ProvisioningRegistry()
        reg.register("step", _active, name="Old name")
        reg.register("step", _active, name={"en": "New name", "it": "Nuovo nome"})
        p = reg._provisioners["step"]
        assert isinstance(p.name, dict)
        assert p.name["it"] == "Nuovo nome"  # type: ignore[index]
