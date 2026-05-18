"""#733 — routing-config-driven privacy cascade.

Cascade rule:
    collection-private REQUIRES items-private. Reverse direction allowed.
    Items-public + collection-private is rejected.

Detection (no more ``CollectionPrivacy.is_private`` flag — privacy lives
in the routing configs themselves):
- Items privacy:      presence of ``items_elasticsearch_private_driver`` in
                       any operation of ``ItemsRoutingConfig.operations``.
- Collection privacy: presence of ``collection_elasticsearch_private_driver``
                       in any operation of ``CollectionRoutingConfig.operations``.

Enforcement points (validate handlers on the routing configs themselves —
pure intra-routing invariant):
- On ``CollectionRoutingConfig``: when the new routing pins the
  collection-private driver and the sibling items routing lacks the
  items-private driver, reject.
- On ``ItemsRoutingConfig``: when the new routing drops the items-private
  driver and the sibling collection routing still pins the collection-
  private driver, reject.

The handlers no-op when ConfigsProtocol discovery is unavailable (early
test fixtures, partial deployments) — the OTHER side of the cascade
catches the violation on its next write.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
    _collection_routing_has_private_driver,
    _enforce_collection_routing_privacy_cascade,
    _enforce_items_routing_privacy_cascade,
    _items_routing_has_private_driver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _items_routing_with_private(*, operation: str = Operation.INDEX) -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _items_routing_without_private() -> ItemsRoutingConfig:
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


def _collection_routing_with_private(
    *, operation: str = Operation.INDEX,
) -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _collection_routing_without_private() -> CollectionRoutingConfig:
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


def _stub_configs_protocol(returned_config: Optional[object]) -> MagicMock:
    """A get_protocol(ConfigsProtocol) stub that returns ``returned_config``
    for every ``get_config(...)`` call regardless of class arg."""
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=returned_config)
    return proto


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def test_items_has_private_driver_detects_pinned_entry():
    assert _items_routing_has_private_driver(_items_routing_with_private()) is True


def test_items_has_private_driver_returns_false_when_absent():
    assert _items_routing_has_private_driver(_items_routing_without_private()) is False


def test_items_has_private_driver_finds_entry_in_any_operation():
    for op in (Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.INDEX):
        routing = _items_routing_with_private(operation=op)
        assert _items_routing_has_private_driver(routing) is True, (
            f"private driver in operations[{op}] must satisfy the cascade gate"
        )


def test_collection_has_private_driver_detects_pinned_entry():
    assert _collection_routing_has_private_driver(
        _collection_routing_with_private(),
    ) is True


def test_collection_has_private_driver_returns_false_when_absent():
    assert _collection_routing_has_private_driver(
        _collection_routing_without_private(),
    ) is False


# ---------------------------------------------------------------------------
# _enforce_collection_routing_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_collection_routing_is_public():
    """Public collection routing imposes no constraint on items routing."""
    cfg = _collection_routing_without_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_items_routing_has_private_driver():
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )
    proto.get_config.assert_called_once()


@pytest.mark.asyncio
async def test_collection_routing_pinning_private_without_items_private_rejected():
    """Regression — applying a CollectionRoutingConfig with the
    collection-private driver while items routing lacks the items-private
    driver MUST raise ValueError citing the routing-config mechanism."""
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(
            ValueError,
            match=r"Privacy cascade violation.*collection_elasticsearch_private_driver",
        ):
            await _enforce_collection_routing_privacy_cascade(
                cfg, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_collection_cascade_noop_at_platform_scope():
    """Apply at platform/catalog scope (no collection_id) bypasses cascade."""
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(cfg, None, None, None)
        await _enforce_collection_routing_privacy_cascade(cfg, "cat-a", None, None)
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_collection_cascade_noop_when_configs_protocol_unavailable():
    """ConfigsProtocol discovery unavailable → cascade defers."""
    cfg = _collection_routing_with_private()
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ):
        # No exception — the OTHER side enforces.
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )


# ---------------------------------------------------------------------------
# _enforce_items_routing_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_items_cascade_passes_when_routing_keeps_private_driver():
    """Routing still pins the private driver — cascade trivially OK."""
    routing = _items_routing_with_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )
    # Routing has the private driver → we never need to look up the collection.
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_items_cascade_passes_when_collection_routing_is_public():
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )


@pytest.mark.asyncio
async def test_items_routing_drops_private_while_collection_still_private_rejected():
    """Regression — applying an ItemsRoutingConfig without the items-
    private driver while sibling CollectionRoutingConfig still pins the
    collection-private driver MUST raise."""
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(
            ValueError,
            match=r"Privacy cascade violation.*items_elasticsearch_private_driver",
        ):
            await _enforce_items_routing_privacy_cascade(
                routing, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_items_cascade_noop_at_platform_scope():
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(routing, None, None, None)
        await _enforce_items_routing_privacy_cascade(routing, "cat-a", None, None)
    proto.get_config.assert_not_called()


# ---------------------------------------------------------------------------
# Registration regression — both handlers must be wired at module-load
# ---------------------------------------------------------------------------


def test_cascade_handlers_registered_on_both_routing_configs():
    items_handlers = ItemsRoutingConfig.get_validate_handlers()
    coll_handlers = CollectionRoutingConfig.get_validate_handlers()
    assert _enforce_items_routing_privacy_cascade in items_handlers
    assert _enforce_collection_routing_privacy_cascade in coll_handlers


# ---------------------------------------------------------------------------
# apply_catalog_default_routing_seed — happy-path regression
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_catalog_default_routing_seed_writes_both_templates():
    """Happy path — CatalogPrivacy.collection_defaults with both templates
    set ⇒ helper writes both per-collection configs."""
    from dynastore.modules.catalog.catalog_config import (
        CatalogPrivacy,
        _build_private_collection_routing,
        _build_private_items_routing,
        apply_catalog_default_routing_seed,
    )
    from dynastore.modules.storage.routing_config import CatalogRoutingDefaults

    policy = CatalogPrivacy(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=_build_private_items_routing(),
            collection_routing=_build_private_collection_routing(),
        ),
    )
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy)
    proto.set_config = AsyncMock()

    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True
    assert proto.set_config.await_count == 2
    classes_written = [
        call.args[0] for call in proto.set_config.await_args_list
    ]
    assert classes_written == [ItemsRoutingConfig, CollectionRoutingConfig]
