"""Cycle E.2 — pin the cascade-aware ``_restore_deny_policies``
behaviour on the items-private driver.

Pre-Cycle-E.2 the lifespan startup hook used
``routing.secondary_driver_ids`` (which the operation-based routing
migration in PR #261 removed; the broad ``except Exception`` swallowed
the AttributeError and DENY policies never restored on cold boot).

The fix in PR #270 rewrites the loop to scan each catalog's
collections for any ``CollectionPluginConfig.is_private == True``
and apply the catalog-wide DENY policy when at least one private
collection exists.

These tests exercise the static helper
``_catalog_has_private_collection`` directly (pure logic, fully
mockable) and the lifespan loop's integration with that helper.
"""
from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
    ItemsElasticsearchPrivateDriver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_collection(col_id: str) -> MagicMock:
    col = MagicMock()
    col.id = col_id
    return col


def _stub_catalog(cat_id: str) -> MagicMock:
    cat = MagicMock()
    cat.id = cat_id
    return cat


def _catalogs_proto_with(
    catalogs: List[MagicMock],
    collections_by_cat: dict[str, List[MagicMock]],
) -> MagicMock:
    proto = MagicMock()

    async def list_catalogs(*, limit: int, offset: int) -> List[MagicMock]:
        return catalogs[offset:offset + limit]

    async def list_collections(
        catalog_id: str, *, limit: int, offset: int,
    ) -> List[MagicMock]:
        cols = collections_by_cat.get(catalog_id, [])
        return cols[offset:offset + limit]

    proto.list_catalogs = list_catalogs
    proto.list_collections = list_collections
    return proto


def _configs_proto_with(
    privacy_by_pair: dict[tuple[str, str], bool],
) -> MagicMock:
    """Returns ``CollectionPluginConfig(is_private=...)`` for the
    requested (catalog, collection) pair.  Returns ``None`` for
    unmapped pairs (mimicking ConfigsService behaviour when no row
    exists for the collection)."""
    proto = MagicMock()

    async def get_config(cls: type, *, catalog_id: str, collection_id: str = "", **kwargs):
        if cls is not CollectionPluginConfig:
            return None
        flag = privacy_by_pair.get((catalog_id, collection_id))
        if flag is None:
            return None
        return CollectionPluginConfig(is_private=flag)

    proto.get_config = get_config
    return proto


# ---------------------------------------------------------------------------
# _catalog_has_private_collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_returns_true_when_one_collection_is_private():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-public"), _stub_collection("col-private")]},
    )
    configs = _configs_proto_with({
        ("cat-a", "col-public"): False,
        ("cat-a", "col-private"): True,
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a", CollectionPluginConfig,
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_false_when_all_collections_public():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-1"), _stub_collection("col-2")]},
    )
    configs = _configs_proto_with({
        ("cat-a", "col-1"): False,
        ("cat-a", "col-2"): False,
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a", CollectionPluginConfig,
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_when_catalog_has_no_collections():
    catalogs_proto = _catalogs_proto_with([_stub_catalog("cat-empty")], {"cat-empty": []})
    configs = _configs_proto_with({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-empty", CollectionPluginConfig,
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_on_list_collections_exception():
    """A transient ``list_collections`` failure should NOT propagate
    or be misinterpreted as 'has private' — defer to the next apply."""
    catalogs_proto = MagicMock()
    catalogs_proto.list_collections = AsyncMock(side_effect=RuntimeError("transient"))
    configs = _configs_proto_with({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-flaky", CollectionPluginConfig,
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_skips_collection_when_get_config_raises():
    """One bad config doesn't stop the scan — keep looking through
    the catalog's other collections."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-bad"), _stub_collection("col-good")]},
    )
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, collection_id="", **kwargs):
        if collection_id == "col-bad":
            raise RuntimeError("transient")
        return CollectionPluginConfig(is_private=True)

    proto.get_config = get_config
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, proto, "cat-a", CollectionPluginConfig,
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_paginates_through_collections():
    """The N+1 collection scan paginates in batches of 100; with 250
    collections we expect three list_collections calls (offsets
    0/100/200) before terminating on the short batch."""
    cols = [_stub_collection(f"col-{i:03d}") for i in range(250)]
    privacy_map = {("cat-big", c.id): False for c in cols}
    privacy_map[("cat-big", "col-249")] = True  # only the very last is private
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-big")], {"cat-big": cols},
    )
    configs = _configs_proto_with(privacy_map)
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-big", CollectionPluginConfig,
    )
    assert result is True


# ---------------------------------------------------------------------------
# _restore_deny_policies — full lifespan loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_applies_deny_only_for_catalogs_with_private_collections():
    """End-to-end of the lifespan loop: two catalogs, one has a
    private collection and one is fully public; ``_apply_deny_policy``
    must fire only for the catalog with the private collection."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-private"), _stub_catalog("cat-public")],
        {
            "cat-private": [_stub_collection("col-secret")],
            "cat-public": [_stub_collection("col-1"), _stub_collection("col-2")],
        },
    )
    configs = _configs_proto_with({
        ("cat-private", "col-secret"): True,
        ("cat-public", "col-1"): False,
        ("cat-public", "col-2"): False,
    })

    # Patch get_protocol so the lifespan helper resolves both protos.
    def _get_protocol(p):
        from dynastore.models.protocols import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol
        if p is CatalogsProtocol:
            return catalogs_proto
        if p is ConfigsProtocol:
            return configs
        return None

    driver = ItemsElasticsearchPrivateDriver()
    apply_calls: list[str] = []

    async def fake_apply(cat_id):
        apply_calls.append(cat_id)

    with patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ), patch.object(driver, "_apply_deny_policy", side_effect=fake_apply):
        await driver._restore_deny_policies()

    assert apply_calls == ["cat-private"]


@pytest.mark.asyncio
async def test_restore_is_no_op_when_protocols_unavailable():
    """No CatalogsProtocol or ConfigsProtocol → bail silently (the
    next apply on a real config write is the safety net)."""
    driver = ItemsElasticsearchPrivateDriver()
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ), patch.object(driver, "_apply_deny_policy", new=AsyncMock()) as apply_mock:
        await driver._restore_deny_policies()
    apply_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_swallows_unexpected_failures():
    """The outer try/except in _restore_deny_policies must catch
    surprises — lifespan should never abort because of a
    privacy-recovery hiccup."""
    driver = ItemsElasticsearchPrivateDriver()

    def _get_protocol(p):
        raise RuntimeError("boom")

    with patch(
        "dynastore.tools.discovery.get_protocol", side_effect=_get_protocol,
    ):
        # No exception should propagate.
        await driver._restore_deny_policies()
