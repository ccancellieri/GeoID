"""Regression pin (#792): collection/catalog ``is_materialized`` dispatch
must call ``CatalogsProtocol.resolve_physical_schema`` directly — NOT via
a non-existent ``.catalog_manager`` indirection.

The bug shipped in #738/PR #746 because the broad ``except Exception`` in
``is_materialized`` swallows ``AttributeError`` and demotes it to a DEBUG
log, then returns ``False`` (fail-open).  Operators only saw a noisy
``DEBUG`` traceback on every ``PUT /configs/.../plugins/{plugin_id}``;
the gate silently treated every resource as not-materialized, neutering
the Immutable-field enforcement the 3-phase lifecycle was supposed to
add.

The dispatch must therefore be exercised against a strict-spec mock so
that an ``AttributeError`` would fail the test loudly instead of being
swallowed by the same except clause that hid the original bug.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.models.protocols import CatalogsProtocol
from dynastore.modules.db_config import platform_config_service as svc


class _FakeCollectionConfig:
    _visibility = "collection"


class _FakeCatalogConfig:
    _visibility = "catalog"


def _fake_conn() -> MagicMock:
    """An AsyncConnection-shaped mock that satisfies ``DriverContext``'s
    Pydantic union validator (``is_instance_of[AsyncConnection]``)."""
    return MagicMock(spec=AsyncConnection)


@pytest.mark.asyncio
async def test_collection_dispatch_calls_resolve_physical_schema_directly():
    """Bug: ``catalogs.catalog_manager.resolve_physical_schema(...)`` —
    `CatalogService` has no ``catalog_manager`` attr.  Fix: call
    ``catalogs.resolve_physical_schema(...)`` directly.
    """
    catalogs = MagicMock(spec=CatalogsProtocol)
    catalogs.resolve_physical_schema = AsyncMock(return_value=None)

    with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
        result = await svc.is_materialized(
            _FakeCollectionConfig, "cat1", "col1", conn=_fake_conn()
        )

    assert result is False  # phys_schema=None → not materialized
    catalogs.resolve_physical_schema.assert_awaited_once()
    # The bug — `.catalog_manager` indirection — would have surfaced as
    # AttributeError on the spec'd mock and been swallowed by the broad
    # ``except Exception`` in ``is_materialized``.  Asserting the await
    # happened proves the dispatch reached the right method.


@pytest.mark.asyncio
async def test_catalog_dispatch_calls_resolve_physical_schema_directly():
    """Same regression, catalog-tier sibling at line 237."""
    catalogs = MagicMock(spec=CatalogsProtocol)
    catalogs.resolve_physical_schema = AsyncMock(return_value=None)

    with patch("dynastore.tools.discovery.get_protocol", return_value=catalogs):
        result = await svc.is_materialized(
            _FakeCatalogConfig, "cat1", None, conn=_fake_conn()
        )

    assert result is False
    catalogs.resolve_physical_schema.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_catalog_manager_attribute_access():
    """Belt-and-braces: source-level grep — neither dispatch helper may
    reference ``.catalog_manager`` (the historical bug shape)."""
    import inspect

    for fn in (svc._collection_is_materialized, svc._catalog_is_materialized):
        source = inspect.getsource(fn)
        assert ".catalog_manager" not in source, (
            f"{fn.__qualname__} reintroduced the #792 `.catalog_manager` "
            f"indirection — call `catalogs.resolve_physical_schema(...)` "
            f"directly instead."
        )
