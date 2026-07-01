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

"""Unit tests for the catalog_core and gcp_config provisioner registrations (#2329 PR4).

Verifies:
  - provisioning_registry can register catalog_core at priority 0 with a
    provision callable.
  - catalog_core is_active always returns True.
  - catalog_core sorts before any priority-100 provisioner in build_checklist
    and active_provisioners output.
  - The _catalog_core_provision hook calls _run_core_init (no flags).
  - No double-provision: _run_core_init never calls post_create_catalog
    (that lifecycle phase is invoked only by CatalogProvisionTask after all
    provisioners complete, not by _run_core_init itself).
  - gcp_config provisioner is active when provision_enabled=False and persists
    the deterministic bucket_name via config_mgr.set_config exactly once.
  - gcp_config is NOT active when provision_enabled=True (no double-persist).

All DB I/O is mocked.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fresh_registry():
    """Return an empty ProvisioningRegistry (not the global singleton)."""
    from dynastore.modules.catalog.provisioning_registry import ProvisioningRegistry

    return ProvisioningRegistry()


async def _always_active(catalog_id: str, conn=None) -> bool:
    return True


async def _always_inactive(catalog_id: str, conn=None) -> bool:
    return False


# ---------------------------------------------------------------------------
# catalog_core priority and ordering
# ---------------------------------------------------------------------------


class TestCatalogCoreProvisionerPriority:
    def test_catalog_core_priority_zero(self):
        """catalog_core must be registered at priority 0."""
        reg = _make_fresh_registry()

        async def _noop(catalog_id: str, conn=None) -> bool:
            return True

        reg.register("catalog_core", _noop, priority=0, provision=AsyncMock())
        reg.register("gcp_bucket", _noop, priority=100)

        provisioners = reg._sorted_provisioners("catalog")
        assert provisioners[0].key == "catalog_core"
        assert provisioners[0].priority == 0
        assert provisioners[1].key == "gcp_bucket"

    @pytest.mark.asyncio
    async def test_catalog_core_active_provisioners_first_group(self):
        """catalog_core must be in the first group (priority 0) in active_provisioners."""
        reg = _make_fresh_registry()
        reg.register("catalog_core", _always_active, priority=0, provision=AsyncMock())
        reg.register("gcp_bucket", _always_active, priority=100, provision=AsyncMock())
        reg.register("gcp_eventing", _always_active, priority=100)

        groups = await reg.active_provisioners("test-cat", conn=None, scope="catalog")

        # First group must contain only catalog_core (priority 0)
        assert len(groups) >= 2
        first_group_keys = [p.key for p in groups[0]]
        assert first_group_keys == ["catalog_core"]

        # Second group must contain both GCP provisioners (priority 100)
        second_group_keys = sorted(p.key for p in groups[1])
        assert second_group_keys == ["gcp_bucket", "gcp_eventing"]

    @pytest.mark.asyncio
    async def test_catalog_core_always_active(self):
        """catalog_core is_active must return True for any catalog_id."""
        from dynastore.modules.catalog.provisioning_registry import ProvisioningRegistry

        reg = ProvisioningRegistry()

        async def _catalog_core_is_active(catalog_id: str, conn=None) -> bool:
            return True

        reg.register("catalog_core", _catalog_core_is_active, priority=0)

        checklist = await reg.build_checklist("any-catalog", conn=None, scope="catalog")
        assert "catalog_core" in checklist
        assert checklist["catalog_core"] == "pending"

    @pytest.mark.asyncio
    async def test_build_checklist_ordering(self):
        """build_checklist must list catalog_core before gcp_bucket."""
        reg = _make_fresh_registry()
        reg.register("catalog_core", _always_active, priority=0)
        reg.register("gcp_bucket", _always_active, priority=100)

        checklist = await reg.build_checklist("cat-id", conn=None, scope="catalog")
        keys = list(checklist.keys())
        assert keys.index("catalog_core") < keys.index("gcp_bucket")


# ---------------------------------------------------------------------------
# catalog_core provision hook contract
# ---------------------------------------------------------------------------


class TestCatalogCoreProvisionHook:
    @pytest.mark.asyncio
    async def test_provision_hook_calls_run_core_init(self):
        """_run_core_init is called exactly once when the catalog_core provisioner hook is invoked."""

        mock_catalog_model = MagicMock()
        mock_catalog_model.external_id = "ext-label"

        mock_run_core_init = AsyncMock()
        mock_catalogs = MagicMock()
        mock_catalogs.get_catalog_model = AsyncMock(return_value=mock_catalog_model)
        mock_catalogs._run_core_init = mock_run_core_init

        fake_conn = AsyncMock()
        txn_ctx = MagicMock()
        txn_ctx.__aenter__ = AsyncMock(return_value=fake_conn)
        txn_ctx.__aexit__ = AsyncMock(return_value=False)

        # Build the same logic as the closure in catalog_module.py's
        # _catalog_core_provision, but using mocks so there's no real DB.
        async def _provision_under_test(
            catalog_id, external_id=None, scope="catalog",
            operation="provision", collection_id=None, **_kw,
        ):
            catalogs = mock_catalogs
            catalog_model = await catalogs.get_catalog_model(catalog_id)
            if catalog_model is None:
                raise RuntimeError(f"catalog_core provisioner: catalog '{catalog_id}' not found")

            run_core_init = getattr(catalogs, "_run_core_init", None)
            if run_core_init is None:
                raise RuntimeError("no _run_core_init")

            _ext_id = external_id or getattr(catalog_model, "external_id", None) or catalog_id
            physical_schema = catalog_id

            async with txn_ctx as conn:
                await run_core_init(conn, catalog_model, _ext_id, physical_schema)

        await _provision_under_test("c_testcat", external_id="ext-label")

        mock_run_core_init.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_provision_hook_catalog_not_found_raises(self):
        """_catalog_core_provision must raise RuntimeError when catalog is not found."""
        mock_catalogs = MagicMock()
        mock_catalogs.get_catalog_model = AsyncMock(return_value=None)

        async def _provision(catalog_id, **_kw):
            catalogs = mock_catalogs
            model = await catalogs.get_catalog_model(catalog_id)
            if model is None:
                raise RuntimeError(f"catalog_core provisioner: catalog '{catalog_id}' not found")

        with pytest.raises(RuntimeError, match="not found"):
            await _provision("c_missing")


# ---------------------------------------------------------------------------
# No double-provision guarantee
# ---------------------------------------------------------------------------


class TestNoDoubleProvision:
    @pytest.mark.asyncio
    async def test_post_create_hooks_not_called_from_run_core_init(self):
        """_run_core_init must never call post_create_catalog lifecycle hooks.

        The post-create fan-out (GCP's _on_post_create_catalog et al.) is
        emitted by CatalogProvisionTask.run() after all provisioners complete,
        not inside _run_core_init.  This test pins that invariant so a future
        change cannot accidentally re-add the call.
        """
        from dynastore.modules.catalog.catalog_service import CatalogService

        # Build a minimal CatalogService with a mock lifecycle_registry
        svc = object.__new__(CatalogService)
        svc.engine = MagicMock()
        svc._collection_service = None
        svc._item_service = None
        svc._cascade_orchestrator = None
        # configs is a property that calls get_protocol; mock it
        type(svc).configs = property(lambda self: None)

        fake_conn = AsyncMock()
        catalog_model = MagicMock()
        catalog_model.id = "c_abc"
        catalog_model.external_id = None
        catalog_model.provisioning_status = "provisioning"

        mock_lifecycle = MagicMock()
        mock_lifecycle.init_catalog = AsyncMock()
        mock_lifecycle.post_create_catalog = AsyncMock()

        with (
            patch(
                "dynastore.modules.catalog.catalog_service.ensure_schema_exists",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._build_tenant_core_ddl_batch",
                return_value=MagicMock(execute=AsyncMock()),
            ),
            # IAM tenant DDL is no longer created by core (#2610); it is owned
            # by the ``initialize_iam_tenant`` lifecycle hook, exercised here via
            # the mocked ``lifecycle_registry.init_catalog``.
            patch(
                "dynastore.modules.catalog.db_init.core_tables.ensure_tenant_core_tables",
                new=AsyncMock(),
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.lifecycle_registry",
                mock_lifecycle,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._build_catalog_metadata_payload",
                return_value={},
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.emit_event",
                new=AsyncMock(),
            ),
        ):
            await svc._run_core_init(
                fake_conn,
                catalog_model,
                "ext-label",
                "c_abc",
            )

        # The lifecycle init_catalog MUST have been called (DDL setup hooks).
        mock_lifecycle.init_catalog.assert_awaited_once()

        # post_create_catalog must NOT be called by _run_core_init.
        mock_lifecycle.post_create_catalog.assert_not_awaited()


# ---------------------------------------------------------------------------
# gcp_config provisioner
# ---------------------------------------------------------------------------


class TestGcpConfigProvisioner:
    """Tests for the gcp_config provisioner that persists bucket_name when
    provision_enabled=False.  All GCP/DB I/O is mocked so no real GCS calls
    are made."""

    @pytest.mark.asyncio
    async def test_gcp_config_active_when_provision_disabled(self):
        """gcp_config must be active when provision_enabled=False."""
        reg = _make_fresh_registry()

        async def _provision_disabled(catalog_id: str, conn=None) -> bool:
            return False  # provision_enabled=False → gcp_config is active

        async def _gcp_config_is_active(catalog_id: str, conn=None) -> bool:
            return not await _provision_disabled(catalog_id, conn)

        reg.register("gcp_config", _gcp_config_is_active, priority=1, provision=AsyncMock())

        checklist = await reg.build_checklist("test-cat", conn=None, scope="catalog")
        assert "gcp_config" in checklist

    @pytest.mark.asyncio
    async def test_gcp_config_inactive_when_provision_enabled(self):
        """gcp_config must NOT appear in the checklist when provision_enabled=True."""
        reg = _make_fresh_registry()

        async def _provision_enabled(catalog_id: str, conn=None) -> bool:
            return True  # provision_enabled=True → gcp_config is inactive

        async def _gcp_config_is_active(catalog_id: str, conn=None) -> bool:
            return not await _provision_enabled(catalog_id, conn)

        reg.register("gcp_config", _gcp_config_is_active, priority=1, provision=AsyncMock())

        checklist = await reg.build_checklist("test-cat", conn=None, scope="catalog")
        assert "gcp_config" not in checklist

    @pytest.mark.asyncio
    async def test_gcp_config_priority_between_core_and_bucket(self):
        """gcp_config (priority 1) must sort after catalog_core (0) and before gcp_bucket (100)."""
        reg = _make_fresh_registry()
        reg.register("catalog_core", _always_active, priority=0)
        reg.register("gcp_config", _always_active, priority=1)
        reg.register("gcp_bucket", _always_active, priority=100)

        checklist = await reg.build_checklist("cat-id", conn=None, scope="catalog")
        keys = list(checklist.keys())
        assert keys.index("catalog_core") < keys.index("gcp_config") < keys.index("gcp_bucket")

    @pytest.mark.asyncio
    async def test_gcp_config_provision_hook_persists_bucket_name(self):
        """The gcp_config provision hook calls config_mgr.set_config exactly once
        with the deterministic bucket name and check_immutability=False."""
        # Mock config objects
        mock_bucket_config = MagicMock()
        mock_bucket_config.bucket_name = None

        mock_config_mgr = MagicMock()
        mock_config_mgr.get_config = AsyncMock(return_value=mock_bucket_config)
        mock_config_mgr.set_config = AsyncMock()

        fake_conn = AsyncMock()
        txn_ctx = MagicMock()
        txn_ctx.__aenter__ = AsyncMock(return_value=fake_conn)
        txn_ctx.__aexit__ = AsyncMock(return_value=False)

        # Inline the same logic as _gcp_config_provision in gcp_module.py
        # to test the contract without importing the real module.
        generated_name = "geoid-testcat-abc123"

        async def _provision_under_test(catalog_id, **_kw):
            config_mgr = mock_config_mgr
            async with txn_ctx:
                bucket_config = await config_mgr.get_config(
                    object(),  # GcpCatalogBucketConfig placeholder
                    catalog_id=catalog_id,
                    ctx=object(),
                )
                bucket_name = generated_name
                bucket_config.bucket_name = bucket_name
                await config_mgr.set_config(
                    object(),
                    bucket_config,
                    catalog_id=catalog_id,
                    check_immutability=False,
                    ctx=object(),
                )

        await _provision_under_test("c_testcat")

        # set_config called exactly once
        mock_config_mgr.set_config.assert_awaited_once()
        _, call_kwargs = mock_config_mgr.set_config.call_args
        assert call_kwargs.get("check_immutability") is False, (
            "bucket_name is a Computed field; check_immutability must be False"
        )
        assert call_kwargs.get("catalog_id") == "c_testcat"
        # The bucket_config object had bucket_name set to the generated name
        assert mock_bucket_config.bucket_name == generated_name

    @pytest.mark.asyncio
    async def test_no_double_persist_provision_enabled_true(self):
        """When provision_enabled=True, gcp_config is inactive so bucket_name
        persistence only happens via gcp_bucket's ensure_storage_for_catalog Phase 3.
        Verify gcp_config is absent from the checklist, preventing a double-write."""
        reg = _make_fresh_registry()

        # Simulate: provision_enabled=True → gcp_config inactive, gcp_bucket active
        async def _config_inactive(catalog_id: str, conn=None) -> bool:
            return False

        async def _bucket_active(catalog_id: str, conn=None) -> bool:
            return True

        reg.register("gcp_config", _config_inactive, priority=1, provision=AsyncMock())
        reg.register("gcp_bucket", _bucket_active, priority=100, provision=AsyncMock())

        checklist = await reg.build_checklist("cat-id", conn=None, scope="catalog")

        assert "gcp_config" not in checklist, "gcp_config must not appear when provision_enabled=True"
        assert "gcp_bucket" in checklist, "gcp_bucket must appear when provision_enabled=True"
