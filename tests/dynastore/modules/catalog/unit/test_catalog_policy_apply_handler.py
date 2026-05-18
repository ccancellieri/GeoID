"""#733 — pin the catalog-tier lifecycle hook on ``CatalogPrivacy``.

When an operator writes a ``CatalogPrivacy`` whose
``collection_defaults`` templates pin private driver variants, the apply
handler must proactively call ``ensure_storage(catalog_id)`` on the
relevant per-tenant private drivers (items + collection envelope) so the
indexes exist before any write lands.

These tests pin the handler's contract:

- Both templates ``None`` → no-op.
- Templates set but no private driver pinned → no-op.
- Missing catalog_id → no-op.
- items-routing template pins items-private + collection-routing template
  pins collection-private → both drivers' ``ensure_storage`` get called.
- Only one tier's template pins a private driver → only the matching
  driver gets called.
- Driver discovery returns nothing for a tier → graceful no-op on that tier.
- ``ensure_storage`` raises → handler logs warning, the other tier still
  gets its call.

The handler itself is registered on ``CatalogPrivacy`` at module
import time; we exercise it directly here rather than going through
``ConfigsProtocol.set_config``.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_config import (
    CatalogPrivacy,
    _build_private_collection_routing,
    _build_private_items_routing,
    _on_apply_catalog_privacy,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingDefaults,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
)


def _both_private_defaults() -> CatalogRoutingDefaults:
    return CatalogRoutingDefaults(
        items_routing=_build_private_items_routing(),
        collection_routing=_build_private_collection_routing(),
    )


def _public_items_template() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _public_collection_template() -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


@pytest.mark.asyncio
async def test_handler_noop_when_both_templates_are_none():
    """The default ``CatalogPrivacy()`` has both templates ``None`` —
    no eager create."""
    items_driver = MagicMock()
    items_driver.ensure_storage = AsyncMock()
    coll_driver = MagicMock()
    coll_driver.ensure_storage = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[items_driver, coll_driver],
    ):
        await _on_apply_catalog_privacy(
            CatalogPrivacy(),
            "cat-a", None, None,
        )

    items_driver.ensure_storage.assert_not_awaited()
    coll_driver.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_noop_when_templates_pin_only_public_drivers():
    """Templates set but no private driver in either — no eager create."""
    items_driver = MagicMock()
    items_driver.ensure_storage = AsyncMock()
    coll_driver = MagicMock()
    coll_driver.ensure_storage = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocols",
        return_value=[items_driver, coll_driver],
    ):
        await _on_apply_catalog_privacy(
            CatalogPrivacy(
                collection_defaults=CatalogRoutingDefaults(
                    items_routing=_public_items_template(),
                    collection_routing=_public_collection_template(),
                ),
            ),
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
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
            None, None, None,
        )

    items_driver.ensure_storage.assert_not_awaited()
    coll_driver.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_calls_both_private_drivers_when_both_tiers_private():
    """The load-bearing happy path — both templates pin a private driver
    and both per-tenant drivers get ensure_storage(catalog_id) called."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.models.protocols.storage_driver import Capability

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.capabilities = frozenset({Capability.TENANT_ISOLATED})
    items_private.ensure_storage = AsyncMock()
    coll_private = MagicMock(spec=CollectionElasticsearchPrivateDriver)
    coll_private.capabilities = frozenset({EntityStoreCapability.TENANT_ISOLATED})
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
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
            "cat-a", None, None,
        )

    items_private.ensure_storage.assert_awaited_once_with("cat-a")
    coll_private.ensure_storage.assert_awaited_once_with("cat-a")


@pytest.mark.asyncio
async def test_handler_calls_only_items_when_only_items_template_is_private():
    """Asymmetric seed: items-routing template pins private, collection-
    routing template is None → only the items-private driver gets called."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.storage.drivers.elasticsearch_private.collection_driver import (
        CollectionElasticsearchPrivateDriver,
    )

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.models.protocols.storage_driver import Capability

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.capabilities = frozenset({Capability.TENANT_ISOLATED})
    items_private.ensure_storage = AsyncMock()
    coll_private = MagicMock(spec=CollectionElasticsearchPrivateDriver)
    coll_private.capabilities = frozenset({EntityStoreCapability.TENANT_ISOLATED})
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
        await _on_apply_catalog_privacy(
            CatalogPrivacy(
                collection_defaults=CatalogRoutingDefaults(
                    items_routing=_build_private_items_routing(),
                    collection_routing=None,
                ),
            ),
            "cat-a", None, None,
        )

    items_private.ensure_storage.assert_awaited_once_with("cat-a")
    coll_private.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_skips_missing_driver_gracefully():
    """If only the items-private driver is installed (e.g. deployment
    SCOPE excludes the collection-private subpackage), the handler
    must call the items driver and silently skip the missing one."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.models.protocols.storage_driver import Capability

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.capabilities = frozenset({Capability.TENANT_ISOLATED})
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
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
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

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.models.protocols.storage_driver import Capability

    items_private = MagicMock(spec=ItemsElasticsearchPrivateDriver)
    items_private.capabilities = frozenset({Capability.TENANT_ISOLATED})
    items_private.ensure_storage = AsyncMock(side_effect=RuntimeError("ES boom"))
    coll_private = MagicMock(spec=CollectionElasticsearchPrivateDriver)
    coll_private.capabilities = frozenset({EntityStoreCapability.TENANT_ISOLATED})
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
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
            "cat-a", None, None,
        )

    # Items-private was called and raised; collection-private still gets called.
    items_private.ensure_storage.assert_awaited_once_with("cat-a")
    coll_private.ensure_storage.assert_awaited_once_with("cat-a")
    # Failure recorded as a warning, not a blocking error.
    assert any(
        "items ensure_storage" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_handler_picks_driver_with_tenant_isolated_capability():
    """#562 G1 positive: a driver advertising
    ``Capability.TENANT_ISOLATED`` is picked by the items loop;
    a sibling driver without the capability is skipped.

    Pins the capability-filter loop in
    ``catalog_config.py:_on_apply_catalog_privacy`` independent of
    isinstance inspection or ``MagicMock(spec=...)`` introspection
    behaviour — only the ``capabilities`` frozenset is consulted.
    """
    from dynastore.models.protocols.storage_driver import Capability

    tenant_isolated_driver = MagicMock()
    tenant_isolated_driver.capabilities = frozenset({Capability.TENANT_ISOLATED})
    tenant_isolated_driver.ensure_storage = AsyncMock()

    non_isolated_driver = MagicMock()
    non_isolated_driver.capabilities = frozenset()
    non_isolated_driver.ensure_storage = AsyncMock()

    def fake_get_protocols(proto):
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        if proto is CollectionItemsStore:
            return [non_isolated_driver, tenant_isolated_driver]
        if proto is CollectionStore:
            return []
        return []

    with patch(
        "dynastore.tools.discovery.get_protocols",
        side_effect=fake_get_protocols,
    ):
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
            "cat-a", None, None,
        )

    tenant_isolated_driver.ensure_storage.assert_awaited_once_with("cat-a")
    non_isolated_driver.ensure_storage.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_noop_when_no_driver_advertises_tenant_isolated():
    """#562 G1 negative / fail-closed: if no discoverable
    ``CollectionItemsStore`` declares ``Capability.TENANT_ISOLATED``
    the handler is a graceful no-op for the items tier. The handler
    must not raise, and no driver's ``ensure_storage`` is called.

    This pins the "no eligible driver" path as silent. Operators are
    expected to surface this via deployment-time invariant checks
    (e.g. SCOPE pulled the wrong drivers), not via privacy-write
    failure — the cascade handler is not the right place for a
    fail-loud error since the write itself is otherwise valid.
    """
    plain_driver = MagicMock()
    plain_driver.capabilities = frozenset()
    plain_driver.ensure_storage = AsyncMock()

    def fake_get_protocols(proto):
        from dynastore.models.protocols.entity_store import CollectionStore
        from dynastore.models.protocols.storage_driver import CollectionItemsStore

        if proto is CollectionItemsStore:
            return [plain_driver]
        if proto is CollectionStore:
            return []
        return []

    with patch(
        "dynastore.tools.discovery.get_protocols",
        side_effect=fake_get_protocols,
    ):
        await _on_apply_catalog_privacy(
            CatalogPrivacy(collection_defaults=_both_private_defaults()),
            "cat-a", None, None,
        )

    plain_driver.ensure_storage.assert_not_awaited()


def test_tenant_isolated_string_matches_entity_store_mirror():
    """#562 G2: the two ``TENANT_ISOLATED`` enum values live in
    different protocol modules but MUST share the same string. The
    privacy-cascade handler reads ``Capability.TENANT_ISOLATED`` in
    the items loop and ``EntityStoreCapability.TENANT_ISOLATED`` in
    the collection loop — a rename in only one place would silently
    break the other branch.
    """
    from dynastore.models.protocols.entity_store import (
        EntityStoreCapability,
    )
    from dynastore.models.protocols.storage_driver import Capability

    assert Capability.TENANT_ISOLATED == EntityStoreCapability.TENANT_ISOLATED, (
        "Capability.TENANT_ISOLATED and EntityStoreCapability.TENANT_ISOLATED "
        "must share the same string value (see KEEP IN SYNC notes in both "
        "constant definitions)."
    )


@pytest.mark.asyncio
async def test_handler_registered_on_catalog_privacy():
    """The handler must be in ``CatalogPrivacy.get_apply_handlers()``
    so the apply pipeline picks it up.  Pins the registration call at
    module load time."""
    handlers = CatalogPrivacy.get_apply_handlers()
    assert _on_apply_catalog_privacy in handlers, (
        "#733: _on_apply_catalog_privacy must be registered on "
        "CatalogPrivacy at module import time."
    )
