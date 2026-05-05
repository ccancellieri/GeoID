"""Cycle E.2.c slice — pin the create-time seed flow.

``apply_catalog_default_privacy_seed`` is invoked from
``CollectionService.create_collection`` to seed a freshly-created
collection's privacy state from the catalog's
``CatalogPolicyConfig.default_collection_privacy``.

These tests pin the helper's contract:
- catalog policy missing → no-op (False return)
- catalog policy public → no-op (False return)
- catalog policy private → writes BOTH ``ItemsRoutingConfig`` (with
  the private items driver pinned) AND ``CollectionPluginConfig`` (with
  ``is_private=True``), in that exact order so the cascade validator
  on the second write finds the private driver already pinned
- ConfigsProtocol unavailable → no-op (False return), defer to the
  cascade validator on the operator's next config write
- Exception during catalog-policy lookup → no-op (False return),
  same defer-to-cascade behaviour
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog.catalog_config import (
    CatalogPolicyConfig,
    CollectionPluginConfig,
    apply_catalog_default_privacy_seed,
)
from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    Operation,
)


def _configs_returning(policy_or_none) -> MagicMock:
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy_or_none)
    proto.set_config = AsyncMock(return_value=None)
    return proto


@pytest.mark.asyncio
async def test_seed_noop_when_configs_protocol_is_none():
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=None,
    )
    assert applied is False


@pytest.mark.asyncio
async def test_seed_noop_when_catalog_has_no_policy_row():
    """Catalog default is implicitly ``"public"`` — nothing to seed."""
    proto = _configs_returning(None)
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_policy_is_public():
    proto = _configs_returning(
        CatalogPolicyConfig(default_collection_privacy="public"),
    )
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_get_config_raises():
    """Transient ConfigsProtocol failure → defer to the cascade
    validator on the operator's next config write."""
    proto = MagicMock()
    proto.get_config = AsyncMock(side_effect=RuntimeError("transient"))
    proto.set_config = AsyncMock()
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_writes_routing_then_collection_plugin_when_private():
    """Cascade-satisfying order is load-bearing: ItemsRoutingConfig
    MUST land BEFORE CollectionPluginConfig(is_private=True), so the
    cascade validator on the second write finds the private driver
    already pinned in the sibling routing.
    """
    proto = _configs_returning(
        CatalogPolicyConfig(default_collection_privacy="private"),
    )
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True

    # Two set_config calls — order matters.
    assert proto.set_config.await_count == 2
    first_call_args = proto.set_config.await_args_list[0]
    second_call_args = proto.set_config.await_args_list[1]
    first_cls = first_call_args.args[0]
    second_cls = second_call_args.args[0]
    assert first_cls is ItemsRoutingConfig, (
        "Routing must land FIRST so the cascade validator on the "
        "CollectionPluginConfig apply finds the private driver already pinned."
    )
    assert second_cls is CollectionPluginConfig

    # The routing payload must pin the private items driver in WRITE
    # (or any operation — cascade gate is "in any operation").
    routing_payload: ItemsRoutingConfig = first_call_args.args[1]
    pinned = {
        e.driver_id
        for entries in routing_payload.operations.values()
        for e in entries
    }
    assert "items_elasticsearch_private_driver" in pinned
    # And the public driver is NOT pinned in any operation — privacy
    # safety: the seed must NOT introduce a leak path to the public
    # per-tenant index.
    assert "items_elasticsearch_driver" not in pinned

    # The CollectionPluginConfig payload must have is_private=True.
    coll_payload: CollectionPluginConfig = second_call_args.args[1]
    assert coll_payload.is_private is True


@pytest.mark.asyncio
async def test_seed_passes_catalog_and_collection_ids_through_to_set_config():
    proto = _configs_returning(
        CatalogPolicyConfig(default_collection_privacy="private"),
    )
    await apply_catalog_default_privacy_seed(
        "cat-x", "col-y", configs=proto,
    )
    for call in proto.set_config.await_args_list:
        kwargs = call.kwargs
        assert kwargs["catalog_id"] == "cat-x"
        assert kwargs["collection_id"] == "col-y"


@pytest.mark.asyncio
async def test_seed_routing_payload_has_postgresql_in_write_for_durability():
    """WRITE without PG = no SoR.  The seed routing pins PG WRITE
    (FATAL) + private ES (ASYNC + OUTBOX) so a private collection
    still has a durable write target."""
    proto = _configs_returning(
        CatalogPolicyConfig(default_collection_privacy="private"),
    )
    await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    routing_payload: ItemsRoutingConfig = proto.set_config.await_args_list[0].args[1]
    write_entries = routing_payload.operations.get(Operation.WRITE, [])
    write_drivers = [e.driver_id for e in write_entries]
    assert "items_postgresql_driver" in write_drivers, (
        "PG must be the durable WRITE target for a private collection — "
        "without it the seed routing has no source-of-truth."
    )
