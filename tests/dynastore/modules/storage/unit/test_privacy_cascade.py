"""Cycle E.2 cascade-validator tests.

Pins the cascade rule resolved with the user 2026-05-05:

    collection-private REQUIRES items-private.  Reverse direction allowed.
    Items-public + collection-private is rejected.

Detection:
- Collection privacy: ``CollectionPluginConfig.is_private == True``
- Items privacy:    presence of ``items_elasticsearch_private_driver`` in
                     any operation of ``ItemsRoutingConfig.operations``

Enforcement points:
- Apply on ``CollectionPluginConfig``: rejects when is_private=True and
  the sibling items routing lacks the private driver.
- Apply on ``ItemsRoutingConfig``: rejects when the new routing drops
  the private driver but the sibling collection still claims is_private.

The handlers no-op when ConfigsProtocol discovery is unavailable (early
test fixtures, partial deployments) — the OTHER side of the cascade
catches the violation on its next write.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
    _enforce_collection_privacy_cascade,
    _enforce_items_routing_privacy_cascade,
    _items_routing_has_private_driver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _routing_with_private(*, operation: str = Operation.INDEX) -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_id="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _routing_without_private() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_id="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _stub_configs_protocol(returned_config: Optional[object]) -> MagicMock:
    """A get_protocol(ConfigsProtocol) stub that returns ``returned_config``
    for every ``get_config(...)`` call regardless of class arg.
    """
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=returned_config)
    return proto


# ---------------------------------------------------------------------------
# _items_routing_has_private_driver
# ---------------------------------------------------------------------------


def test_has_private_driver_detects_pinned_entry():
    routing = _routing_with_private(operation=Operation.INDEX)
    assert _items_routing_has_private_driver(routing) is True


def test_has_private_driver_returns_false_when_absent():
    routing = _routing_without_private()
    assert _items_routing_has_private_driver(routing) is False


def test_has_private_driver_finds_entry_in_any_operation():
    """Cascade is satisfied if the private driver is pinned in ANY
    operation, not just INDEX — operators may pin it under READ/SEARCH
    for tenant-isolated lookups."""
    for op in (Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.INDEX):
        routing = _routing_with_private(operation=op)
        assert _items_routing_has_private_driver(routing) is True, (
            f"private driver in operations[{op}] must satisfy the cascade gate"
        )


# ---------------------------------------------------------------------------
# _enforce_collection_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_is_private_false():
    """Public collections impose no constraint on items routing."""
    cfg = CollectionPluginConfig(is_private=False)
    proto = _stub_configs_protocol(_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_privacy_cascade(cfg, "cat-a", "col-a", None)
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_items_routing_has_private_driver():
    cfg = CollectionPluginConfig(is_private=True)
    proto = _stub_configs_protocol(_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_privacy_cascade(cfg, "cat-a", "col-a", None)
    proto.get_config.assert_called_once()


@pytest.mark.asyncio
async def test_collection_cascade_rejects_private_collection_with_public_items():
    cfg = CollectionPluginConfig(is_private=True)
    proto = _stub_configs_protocol(_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(ValueError, match=r"Privacy cascade violation.*is_private=True"):
            await _enforce_collection_privacy_cascade(
                cfg, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_collection_cascade_noop_at_platform_scope():
    """Apply at platform/catalog scope (no collection_id) bypasses cascade."""
    cfg = CollectionPluginConfig(is_private=True)
    proto = _stub_configs_protocol(_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_privacy_cascade(cfg, None, None, None)
        await _enforce_collection_privacy_cascade(cfg, "cat-a", None, None)
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_collection_cascade_noop_when_configs_protocol_unavailable():
    """ConfigsProtocol discovery unavailable (early fixture / partial
    deployment) → cascade defers to the items-routing apply handler on
    its next write."""
    cfg = CollectionPluginConfig(is_private=True)
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ):
        # No exception — the OTHER side enforces.
        await _enforce_collection_privacy_cascade(cfg, "cat-a", "col-a", None)


@pytest.mark.asyncio
async def test_collection_cascade_noop_when_routing_lookup_returns_none():
    """No items routing yet for this catalog/collection — cascade
    defers."""
    cfg = CollectionPluginConfig(is_private=True)
    proto = _stub_configs_protocol(None)
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_privacy_cascade(cfg, "cat-a", "col-a", None)


# ---------------------------------------------------------------------------
# _enforce_items_routing_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_items_cascade_passes_when_routing_keeps_private_driver():
    """Routing still pins the private driver — cascade trivially OK."""
    routing = _routing_with_private()
    proto = _stub_configs_protocol(CollectionPluginConfig(is_private=True))
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )
    # The routing has the private driver → we never need to look up the collection.
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_items_cascade_passes_when_collection_is_public():
    routing = _routing_without_private()
    proto = _stub_configs_protocol(CollectionPluginConfig(is_private=False))
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )


@pytest.mark.asyncio
async def test_items_cascade_rejects_dropping_private_driver_when_collection_is_private():
    routing = _routing_without_private()
    proto = _stub_configs_protocol(CollectionPluginConfig(is_private=True))
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(
            ValueError, match=r"Privacy cascade violation.*is_private=True",
        ):
            await _enforce_items_routing_privacy_cascade(
                routing, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_items_cascade_noop_at_platform_scope():
    routing = _routing_without_private()
    proto = _stub_configs_protocol(CollectionPluginConfig(is_private=True))
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(routing, None, None, None)
        await _enforce_items_routing_privacy_cascade(routing, "cat-a", None, None)
    proto.get_config.assert_not_called()
