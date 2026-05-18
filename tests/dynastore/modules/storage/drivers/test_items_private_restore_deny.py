"""#733 — pin the cascade-aware ``_restore_deny_policies`` behaviour on
the items-private driver.

Privacy is now expressed via the routing configs themselves: a collection
is "private" iff its ``ItemsRoutingConfig`` pins
``items_elasticsearch_private_driver`` or its ``CollectionRoutingConfig``
pins ``collection_elasticsearch_private_driver``.  The lifespan startup
hook scans each catalog's collections and re-applies the catalog-wide
DENY policy idempotently for any catalog with at least one such
collection.

These tests exercise the static helper
``_catalog_has_private_collection`` directly (pure logic, fully
mockable) and the lifespan loop's integration with that helper.
"""
from __future__ import annotations

import re
from typing import Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
    ItemsElasticsearchPrivateDriver,
)
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
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


def _items_private_routing() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _items_public_routing() -> ItemsRoutingConfig:
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


def _coll_private_routing() -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            Operation.INDEX: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _coll_public_routing() -> CollectionRoutingConfig:
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


def _configs_proto_with_routing(
    routing_by_pair: Dict[tuple, object],
) -> MagicMock:
    """Returns the supplied routing config for (cls, cat, col) lookups.

    ``routing_by_pair`` maps ``(catalog_id, collection_id, cls.__name__)``
    → routing instance; missing pairs return ``None``.
    """
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, collection_id="", **kwargs):
        return routing_by_pair.get(
            (catalog_id, collection_id, cls.__name__),
        )

    proto.get_config = get_config
    return proto


# ---------------------------------------------------------------------------
# _catalog_has_private_collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_helper_returns_true_when_items_routing_pins_private_driver():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-public"), _stub_collection("col-private")]},
    )
    configs = _configs_proto_with_routing({
        ("cat-a", "col-public", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-private", "ItemsRoutingConfig"): _items_private_routing(),
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_true_when_only_collection_routing_pins_private_driver():
    """A collection that pins the collection-private driver but leaves
    items routing public is still "private" for DENY-scan purposes —
    the catalog-wide DENY covers the envelope path."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-envelope-private")]},
    )
    configs = _configs_proto_with_routing({
        ("cat-a", "col-envelope-private", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-envelope-private", "CollectionRoutingConfig"): _coll_private_routing(),
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_returns_false_when_all_collections_public():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-1"), _stub_collection("col-2")]},
    )
    configs = _configs_proto_with_routing({
        ("cat-a", "col-1", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-1", "CollectionRoutingConfig"): _coll_public_routing(),
        ("cat-a", "col-2", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-a", "col-2", "CollectionRoutingConfig"): _coll_public_routing(),
    })
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-a",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_when_catalog_has_no_collections():
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-empty")], {"cat-empty": []},
    )
    configs = _configs_proto_with_routing({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-empty",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_returns_false_on_list_collections_exception():
    """A transient ``list_collections`` failure should NOT propagate or
    be misinterpreted as 'has private' — defer to the next apply."""
    catalogs_proto = MagicMock()
    catalogs_proto.list_collections = AsyncMock(side_effect=RuntimeError("transient"))
    configs = _configs_proto_with_routing({})
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-flaky",
    )
    assert result is False


@pytest.mark.asyncio
async def test_helper_skips_collection_when_get_config_raises():
    """A transient items-routing lookup failure must not stop the scan
    — it should still check collection-routing for that same collection
    AND the other collections."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-a")],
        {"cat-a": [_stub_collection("col-bad"), _stub_collection("col-good")]},
    )
    proto = MagicMock()

    async def get_config(cls, *, catalog_id, collection_id="", **kwargs):
        if collection_id == "col-bad":
            raise RuntimeError("transient")
        if cls is ItemsRoutingConfig:
            return _items_private_routing()
        return None

    proto.get_config = get_config
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, proto, "cat-a",
    )
    assert result is True


@pytest.mark.asyncio
async def test_helper_paginates_through_collections():
    """The N+1 collection scan paginates in batches of 100; with 250
    collections we expect three list_collections calls (offsets
    0/100/200) before terminating on the short batch."""
    cols = [_stub_collection(f"col-{i:03d}") for i in range(250)]
    routing_map: Dict[tuple, object] = {}
    for c in cols:
        routing_map[("cat-big", c.id, "ItemsRoutingConfig")] = _items_public_routing()
        routing_map[("cat-big", c.id, "CollectionRoutingConfig")] = _coll_public_routing()
    # Only the very last is private (via items routing).
    routing_map[("cat-big", "col-249", "ItemsRoutingConfig")] = _items_private_routing()

    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-big")], {"cat-big": cols},
    )
    configs = _configs_proto_with_routing(routing_map)
    result = await ItemsElasticsearchPrivateDriver._catalog_has_private_collection(
        catalogs_proto, configs, "cat-big",
    )
    assert result is True


# ---------------------------------------------------------------------------
# _restore_deny_policies — full lifespan loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_restore_applies_deny_only_for_catalogs_with_private_collections():
    """End-to-end of the lifespan loop: two catalogs, one has a
    private-routing collection and one is fully public; ``_apply_deny_policy``
    must fire only for the catalog with the private collection."""
    catalogs_proto = _catalogs_proto_with(
        [_stub_catalog("cat-private"), _stub_catalog("cat-public")],
        {
            "cat-private": [_stub_collection("col-secret")],
            "cat-public": [_stub_collection("col-1"), _stub_collection("col-2")],
        },
    )
    configs = _configs_proto_with_routing({
        ("cat-private", "col-secret", "ItemsRoutingConfig"): _items_private_routing(),
        ("cat-public", "col-1", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-public", "col-1", "CollectionRoutingConfig"): _coll_public_routing(),
        ("cat-public", "col-2", "ItemsRoutingConfig"): _items_public_routing(),
        ("cat-public", "col-2", "CollectionRoutingConfig"): _coll_public_routing(),
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
    """No CatalogsProtocol or ConfigsProtocol → bail silently."""
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


# ---------------------------------------------------------------------------
# DENY resource pattern is built from the OGCServiceMixin registry
# (issue #454 item 2 — Fix 1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_deny_policy_uses_ogc_prefix_registry():
    """``_apply_deny_policy`` must build its resource regex from
    ``get_ogc_service_prefixes()`` so the pattern self-maintains as new
    OGC protocols come online — no hardcoded protocol list to drift."""
    captured: list = []

    fake_perm = MagicMock()
    fake_perm.register_policy.side_effect = lambda p: captured.append(p) or p
    fake_perm.register_role.return_value = None
    fake_perm.create_policy = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=fake_perm,
    ), patch(
        "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
        return_value=["features", "maps", "records", "stac", "tiles"],
    ):
        await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-x")

    assert len(captured) == 1
    pol = captured[0]
    assert pol.effect == "DENY"
    assert pol.actions == ["GET"]
    pat = pol.resources[0]
    # All registry-supplied prefixes appear in the alternation
    for p in ("features", "maps", "records", "stac", "tiles"):
        assert p in pat
    # Catalog id is regex-escaped and present
    assert re.escape("cat-x") in pat
    # No legacy hardcoded entries remain
    assert "wfs" not in pat
    # `catalog` (singular) is not auto-included unless a real prefix exists
    assert "/(catalog|" not in pat


@pytest.mark.asyncio
async def test_apply_deny_policy_falls_back_to_wildcard_when_registry_empty():
    """If discovery returns no OGC contributors (early lifecycle / test
    fixture), DENY must still fail-closed by emitting a wildcard
    pattern and logging a warning rather than skipping the policy."""
    captured: list = []

    fake_perm = MagicMock()
    fake_perm.register_policy.side_effect = lambda p: captured.append(p) or p
    fake_perm.register_role.return_value = None
    fake_perm.create_policy = AsyncMock()

    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=fake_perm,
    ), patch(
        "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
        return_value=[],
    ):
        await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-y")

    assert len(captured) == 1
    pat = captured[0].resources[0]
    assert pat.startswith("/[^/]+/catalogs/")
    assert re.escape("cat-y") in pat


def test_get_ogc_service_prefixes_filters_to_path_prefixes():
    """The helper must accept only ``"/x"``-shaped prefixes (drops empty
    string defaults from the mixin and any non-path values)."""
    from dynastore.extensions.tools.conformance import get_ogc_service_prefixes

    fake_a = MagicMock(prefix="/maps")
    fake_b = MagicMock(prefix="")  # default mixin value — must be dropped
    fake_c = MagicMock(prefix="/records")
    fake_d = MagicMock(spec=[])  # no `prefix` attribute at all
    with patch(
        "dynastore.extensions.tools.conformance.get_protocols",
        return_value=[fake_a, fake_b, fake_c, fake_d],
    ):
        result = get_ogc_service_prefixes()

    assert result == ["maps", "records"]
