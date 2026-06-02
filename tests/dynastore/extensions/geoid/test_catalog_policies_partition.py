"""Unit tests for partition-hardening in register_geoid_policies_for_catalog (#1697).

Asserts that ensure_policy_partition is called with partition_key=catalog_id
before any policies are created, so per-catalog policies land in a dedicated
policies_{catalog_id} partition rather than falling through to policies_default.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_async_cm(return_value: object = None):
    """Return an async context manager that yields *return_value*."""
    @asynccontextmanager
    async def _cm(*args, **kwargs):  # noqa: ANN001
        yield return_value
    return _cm


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_env(catalog_id: str, catalog_schema: str):
    """Build the standard mock environment for register_geoid_policies_for_catalog."""
    mock_pm = MagicMock()
    mock_pm.get_policy = AsyncMock(return_value=None)
    mock_pm.create_policy = AsyncMock(return_value=MagicMock())

    mock_policy_storage = MagicMock()
    mock_policy_storage.ensure_policy_partition = AsyncMock()

    mock_engine = MagicMock()
    mock_db = MagicMock()
    mock_db.engine = mock_engine

    mock_catalogs = MagicMock()
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value=catalog_schema)

    return mock_pm, mock_policy_storage, mock_db, mock_catalogs, MagicMock()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnsurePartitionCalledBeforeCreate:
    """ensure_policy_partition must be invoked with partition_key=catalog_id."""

    def test_ensure_partition_called_with_catalog_id(self, monkeypatch):
        """Belt-and-suspenders: partition DDL is run with the correct catalog_id."""
        from dynastore.extensions.geoid import catalog_policies as cp

        catalog_id = "test-catalog-42"
        catalog_schema = "tenant_test_catalog_42"

        mock_pm, mock_policy_storage, mock_db, mock_catalogs, fake_conn = _make_env(
            catalog_id, catalog_schema
        )

        def fake_get_protocol(proto):
            from dynastore.models.protocols.policies import PermissionProtocol
            from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
            if proto is PermissionProtocol:
                return mock_pm
            if proto is DatabaseProtocol:
                return mock_db
            if proto is CatalogsProtocol:
                return mock_catalogs
            return None

        monkeypatch.setattr(cp, "get_protocol", fake_get_protocol)

        with patch(
            "dynastore.extensions.geoid.catalog_policies.PostgresPolicyStorage",
            return_value=mock_policy_storage,
        ):
            with patch(
                "dynastore.extensions.geoid.catalog_policies.managed_transaction",
                side_effect=_make_async_cm(fake_conn),
            ):
                with patch(
                    "dynastore.extensions.geoid.catalog_policies.DriverContext",
                    return_value=MagicMock(),
                ):
                    _run(cp.register_geoid_policies_for_catalog(catalog_id))

        mock_policy_storage.ensure_policy_partition.assert_called_once_with(
            fake_conn, catalog_id, schema=catalog_schema
        )

    def test_ensure_partition_called_before_create_policy(self, monkeypatch):
        """Ordering guard: ensure_policy_partition precedes the first create_policy call."""
        from dynastore.extensions.geoid import catalog_policies as cp

        catalog_id = "ordering-test-cat"
        catalog_schema = "tenant_ordering_test_cat"
        call_order: list[str] = []

        mock_pm = MagicMock()
        mock_pm.get_policy = AsyncMock(return_value=None)

        async def _mock_create(policy, catalog_id=None):  # noqa: ANN001
            call_order.append("create_policy")
            return MagicMock()

        mock_pm.create_policy = _mock_create

        mock_policy_storage = MagicMock()

        async def _mock_ensure(conn, partition_key, schema="iam"):  # noqa: ANN001
            call_order.append("ensure_policy_partition")

        mock_policy_storage.ensure_policy_partition = _mock_ensure

        mock_db = MagicMock()
        mock_db.engine = MagicMock()
        mock_catalogs = MagicMock()
        mock_catalogs.resolve_physical_schema = AsyncMock(return_value=catalog_schema)

        def fake_get_protocol(proto):
            from dynastore.models.protocols.policies import PermissionProtocol
            from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
            if proto is PermissionProtocol:
                return mock_pm
            if proto is DatabaseProtocol:
                return mock_db
            if proto is CatalogsProtocol:
                return mock_catalogs
            return None

        monkeypatch.setattr(cp, "get_protocol", fake_get_protocol)

        with patch(
            "dynastore.extensions.geoid.catalog_policies.PostgresPolicyStorage",
            return_value=mock_policy_storage,
        ):
            with patch(
                "dynastore.extensions.geoid.catalog_policies.managed_transaction",
                side_effect=_make_async_cm(MagicMock()),
            ):
                with patch(
                    "dynastore.extensions.geoid.catalog_policies.DriverContext",
                    return_value=MagicMock(),
                ):
                    _run(cp.register_geoid_policies_for_catalog(catalog_id))

        assert "ensure_policy_partition" in call_order, (
            "ensure_policy_partition was never called"
        )
        assert call_order.index("ensure_policy_partition") < call_order.index("create_policy"), (
            "ensure_policy_partition must be called before the first create_policy"
        )

    def test_no_ensure_partition_when_protocol_missing(self, monkeypatch, caplog):
        """When PermissionProtocol is absent, function exits early with a warning."""
        from dynastore.extensions.geoid import catalog_policies as cp

        monkeypatch.setattr(cp, "get_protocol", lambda _proto: None)

        with caplog.at_level("WARNING"):
            _run(cp.register_geoid_policies_for_catalog("irrelevant-cat"))

        assert any("PermissionProtocol not available" in rec.message for rec in caplog.records)
