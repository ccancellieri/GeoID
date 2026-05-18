"""#733 — pin the create-time routing-seed flow.

``apply_catalog_default_routing_seed`` is invoked from
``CollectionService.create_collection`` to seed a freshly-created
collection's routing configs from the catalog's
``CatalogPrivacy.collection_defaults.{items_routing, collection_routing}``
templates.

These tests pin the helper's contract:
- catalog policy missing → no-op (False return)
- catalog policy with both templates None → no-op (False return)
- catalog policy with both templates set → writes BOTH routings, items
  before collection (cascade-satisfying order)
- only items template set → only items routing written
- only collection template set → only collection routing written
- ConfigsProtocol unavailable → no-op (False return)
- Exception during catalog-policy lookup → no-op (False return)
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog.catalog_config import (
    CatalogPrivacy,
    _build_private_collection_routing,
    _build_private_items_routing,
    apply_catalog_default_routing_seed,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingDefaults,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    Operation,
)


def _configs_returning(policy_or_none) -> MagicMock:
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy_or_none)
    proto.set_config = AsyncMock(return_value=None)
    return proto


def _private_default_catalog_privacy() -> CatalogPrivacy:
    return CatalogPrivacy(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=_build_private_items_routing(),
            collection_routing=_build_private_collection_routing(),
        ),
    )


@pytest.mark.asyncio
async def test_seed_noop_when_configs_protocol_is_none():
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=None,
    )
    assert applied is False


@pytest.mark.asyncio
async def test_seed_noop_when_catalog_has_no_policy_row():
    """No CatalogPrivacy row → nothing to seed."""
    proto = _configs_returning(None)
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_both_templates_are_none():
    """Default ``CatalogPrivacy()`` carries no templates — nothing to seed."""
    proto = _configs_returning(CatalogPrivacy())
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_get_config_raises():
    """Transient ConfigsProtocol failure → defer."""
    proto = MagicMock()
    proto.get_config = AsyncMock(side_effect=RuntimeError("transient"))
    proto.set_config = AsyncMock()
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_writes_both_routings_in_cascade_satisfying_order():
    """Both templates set → both routings written.  Items first so the
    cascade validator on the second write finds the items-private
    driver already pinned."""
    proto = _configs_returning(_private_default_catalog_privacy())
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True

    assert proto.set_config.await_count == 2
    classes_in_order = [
        proto.set_config.await_args_list[i].args[0]
        for i in range(2)
    ]
    assert classes_in_order[0] is ItemsRoutingConfig, (
        "Items routing must land FIRST so the cascade validator on the "
        "CollectionRoutingConfig apply finds the items-private driver "
        "already pinned."
    )
    assert classes_in_order[1] is CollectionRoutingConfig

    # The items-routing payload must pin the items-private driver.
    items_routing: ItemsRoutingConfig = (
        proto.set_config.await_args_list[0].args[1]
    )
    items_pinned = {
        e.driver_ref
        for entries in items_routing.operations.values()
        for e in entries
    }
    assert "items_elasticsearch_private_driver" in items_pinned
    # Public driver MUST NOT be pinned — privacy safety.
    assert "items_elasticsearch_driver" not in items_pinned

    # The collection-routing payload must pin the collection-envelope
    # private driver — same privacy-safety property.
    coll_routing: CollectionRoutingConfig = (
        proto.set_config.await_args_list[1].args[1]
    )
    coll_pinned = {
        e.driver_ref
        for entries in coll_routing.operations.values()
        for e in entries
    }
    assert "collection_elasticsearch_private_driver" in coll_pinned
    assert "collection_elasticsearch_driver" not in coll_pinned, (
        "Privacy safety: seed must NOT pin the PUBLIC collection ES "
        "driver — that would put the envelope into the shared "
        "{prefix}-collections index."
    )


@pytest.mark.asyncio
async def test_seed_writes_only_items_when_only_items_template_set():
    """Asymmetric seed: items-routing template set, collection-routing
    template ``None`` → only items routing written."""
    policy = CatalogPrivacy(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=_build_private_items_routing(),
            collection_routing=None,
        ),
    )
    proto = _configs_returning(policy)
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True
    assert proto.set_config.await_count == 1
    assert proto.set_config.await_args_list[0].args[0] is ItemsRoutingConfig


@pytest.mark.asyncio
async def test_seed_writes_only_collection_when_only_collection_template_set():
    """Asymmetric seed: only collection-routing template set → only
    collection routing written."""
    policy = CatalogPrivacy(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=None,
            collection_routing=_build_private_collection_routing(),
        ),
    )
    proto = _configs_returning(policy)
    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True
    assert proto.set_config.await_count == 1
    assert proto.set_config.await_args_list[0].args[0] is CollectionRoutingConfig


@pytest.mark.asyncio
async def test_seed_passes_catalog_and_collection_ids_through_to_set_config():
    proto = _configs_returning(_private_default_catalog_privacy())
    await apply_catalog_default_routing_seed(
        "cat-x", "col-y", configs=proto,
    )
    for call in proto.set_config.await_args_list:
        kwargs = call.kwargs
        assert kwargs["catalog_id"] == "cat-x"
        assert kwargs["collection_id"] == "col-y"


@pytest.mark.asyncio
async def test_seed_routing_payload_has_postgresql_in_write_for_durability():
    """WRITE without PG = no SoR.  The default items template pins PG
    WRITE (FATAL) + private ES (ASYNC + OUTBOX) so a private collection
    still has a durable write target."""
    proto = _configs_returning(_private_default_catalog_privacy())
    await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    routing_payload: ItemsRoutingConfig = proto.set_config.await_args_list[0].args[1]
    write_entries = routing_payload.operations.get(Operation.WRITE, [])
    write_drivers = [e.driver_ref for e in write_entries]
    assert "items_postgresql_driver" in write_drivers, (
        "PG must be the durable WRITE target for a private collection — "
        "without it the seed routing has no source-of-truth."
    )
