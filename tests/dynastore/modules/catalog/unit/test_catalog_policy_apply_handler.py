"""Cycle E.2.c slice 2 — pin the catalog-tier lifecycle hook.

When an operator writes a ``CatalogPolicyConfig`` with
``default_collection_privacy=="private"``, the apply handler must
proactively call ``ensure_storage(catalog_id)`` on both per-tenant
private drivers (items + collection envelope) so the indexes exist
before any write lands.

These tests pin the handler's contract:

- Public default → no-op (no ensure_storage calls).
- Private default + missing catalog_id → no-op.
- Private default + drivers discoverable → both drivers' ensure_storage
  called with the catalog_id.
- Private default + only one driver discoverable (deployment SCOPE
  excludes one tier) → graceful no-op for the missing one, the
  other still gets called.
- Private default + ensure_storage raises on one driver → handler
  swallows + logs, the other driver still gets called.

The handler itself is registered on ``CatalogPolicyConfig`` at module
import time; we exercise it directly here rather than going through
``ConfigsProtocol.set_config``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_config import (
    CatalogPolicyConfig,
    _on_apply_catalog_policy_config,
)


def _stub_drivers(*, items: object = None, coll: object = None) -> list[object]:
    """Build the discovery-protocol return value the handler iterates.

    Both ``CollectionItemsStore`` and ``CollectionStore`` queries are
    expected to return iterables of registered drivers; the handler
    isinstance-filters to find the private variants.  Tests pass the
    relevant private-driver instance and any number of unrelated
    drivers (always ignored by the isinstance filter).
    """
    out: list[object] = []
    if items is not None:
        out.append(items)
    if coll is not None:
        out.append(coll)
    return out


@pytest.mark.asyncio
async def test_handler_noop_when_policy_is_public():
    """``default_collection_privacy="public"`` (default) must NOT trigger
    eager-create.  The catalog policy applies to NEW collections only,
    and the existing per-tenant private indexes (if any) must remain
    untouched."""
    items_driver = MagicMock()
    items_driver.ensure_storage = AsyncMock()
    coll_driver = MagicMock()
    coll_driver.ensure_storage = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[items_driver, coll_driver],
    ):
        await _on_apply_catalog_policy_config(
            CatalogPolicyConfig(default_collection_privacy="public"),
            "cat-a", None, None,
        )

    items_driver.ensure_storage.assert_not_awaited()
    coll_driver.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_noop_when_catalog_id_missing():
    """Platform-tier write (``catalog_id=None``) — no tenant to bootstrap."""
    items_driver = MagicMock()
    items_driver.ensure_storage = AsyncMock()
    coll_driver = MagicMock()
    coll_driver.ensure_storage = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[items_driver, coll_driver],
    ):
        await _on_apply_catalog_policy_config(
            CatalogPolicyConfig(default_collection_privacy="private"),
            None, None, None,
        )

    items_driver.ensure_storage.assert_not_awaited()
    coll_driver.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_calls_both_private_drivers_when_private_and_discoverable():
    """The load-bearing happy path — both private drivers are discoverable
    and both get ensure_storage(catalog_id) called."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.ensure_storage = AsyncMock()
    coll_private = MagicMock(spec=CollectionElasticsearchPrivateDriver)
    coll_private.ensure_storage = AsyncMock()

    def fake_get_protocols(proto):
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        if proto is CollectionItemsStore:
            return [items_private]
        if proto is CollectionStore:
            return [coll_private]
        return []

    with patch(
        "dynastore.tools.discovery.get_protocols",
        side_effect=fake_get_protocols,
    ):
        await _on_apply_catalog_policy_config(
            CatalogPolicyConfig(default_collection_privacy="private"),
            "cat-a", None, None,
        )

    items_private.ensure_storage.assert_awaited_once_with("cat-a")
    coll_private.ensure_storage.assert_awaited_once_with("cat-a")


@pytest.mark.asyncio
async def test_handler_skips_missing_driver_gracefully():
    """If only the items-private driver is installed (e.g. deployment
    SCOPE excludes the collection-private subpackage), the handler
    must call the items driver and silently skip the missing one."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.ensure_storage = AsyncMock()

    def fake_get_protocols(proto):
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        if proto is CollectionItemsStore:
            return [items_private]
        if proto is CollectionStore:
            return []  # collection-private not installed
        return []

    with patch(
        "dynastore.tools.discovery.get_protocols",
        side_effect=fake_get_protocols,
    ):
        # Should not raise even though collection-private is missing.
        await _on_apply_catalog_policy_config(
            CatalogPolicyConfig(default_collection_privacy="private"),
            "cat-a", None, None,
        )

    items_private.ensure_storage.assert_awaited_once_with("cat-a")


@pytest.mark.asyncio
async def test_handler_swallows_ensure_storage_exceptions(caplog):
    """A transient ES failure on one driver must not block the OTHER
    driver from getting its ensure_storage call.  The handler logs a
    warning and continues — same pattern as
    ``_on_apply_asset_routing_config``."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.ensure_storage = AsyncMock(side_effect=RuntimeError("ES boom"))
    coll_private = MagicMock(spec=CollectionElasticsearchPrivateDriver)
    coll_private.ensure_storage = AsyncMock()

    def fake_get_protocols(proto):
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        if proto is CollectionItemsStore:
            return [items_private]
        if proto is CollectionStore:
            return [coll_private]
        return []

    with patch(
        "dynastore.tools.discovery.get_protocols",
        side_effect=fake_get_protocols,
    ):
        # Should not raise.
        await _on_apply_catalog_policy_config(
            CatalogPolicyConfig(default_collection_privacy="private"),
            "cat-a", None, None,
        )

    # Items-private was called and raised; collection-private still gets called.
    items_private.ensure_storage.assert_awaited_once_with("cat-a")
    coll_private.ensure_storage.assert_awaited_once_with("cat-a")
    # Failure recorded as a warning, not a blocking error.
    assert any(
        "items-private ensure_storage" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_handler_registered_on_catalog_policy_config():
    """The handler must be in ``CatalogPolicyConfig.get_apply_handlers()``
    so the apply pipeline picks it up.  Pins the registration call at
    module load time."""
    handlers = CatalogPolicyConfig.get_apply_handlers()
    assert _on_apply_catalog_policy_config in handlers, (
        "Cycle E.2.c slice 2: _on_apply_catalog_policy_config must be "
        "registered on CatalogPolicyConfig at module import time."
    )
