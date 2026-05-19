"""Cycle D.4 closure: regression coverage for tier-first ``_address`` paths.

Cycle D shipped in five slices:

* D.0 / D.0-fixup — variable-length ``_address`` Tuple, sentinel-shape-independent
  validation (PRs #297 / #299).
* D.1 — composer rewrite to walk variable-length addresses (PR #301).
* D.2 — migrate every ``_address`` tuple to tier-first paths (PR #304).
* D.3 — drop ``inherited_from_catalog`` sibling + retype top-level
  ``inherited`` to a hierarchical tree mirroring ``configs`` (PR #306).

D.4 (this file) closes the cycle with cross-cutting regression tests:

1. **Real-class ``_address`` pin tests** — every load-bearing class still
   resolves to its post-D.2 tier-first tuple.  Catches accidental
   regressions (someone moves a class but forgets to rewrite the
   address).
2. **PATCH transparency** — ``PATCH /configs/.../plugins/items_write_policy``
   followed by ``GET`` finds the value at the post-D.2 read path
   (``configs.platform.catalog.collection.items.policy.items_write_policy``).  The
   ``class_key`` is the URL segment AND the leaf key — the round-trip
   wires unchanged.
3. **Hierarchical ``inherited`` shape** — at collection scope under slim
   mode, the ``inherited`` tree mirrors the ``configs`` shape and carries
   ``{source: <tier>}`` leaves at the same address the resolved value
   would land at if rendered (D.3 contract, real-class scenario).
"""

from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

import dynastore.extensions.features  # noqa: F401  — populate plugin registry
import dynastore.modules.tiles        # noqa: F401  — populate plugin registry


# ---------------------------------------------------------------------------
# 1. Real-class ``_address`` pin tests (post-D.2 tier-first)
# ---------------------------------------------------------------------------

def test_items_write_policy_address_is_tier_first():
    """Items-tier write policy lives at ``storage.items.policy``."""
    from dynastore.modules.storage.driver_config import ItemsWritePolicy
    assert ItemsWritePolicy._address == ("platform", "catalog", "collection", "items", "policy")


def test_items_schema_address_is_tier_first():
    """Items-tier schema lives at ``storage.items.schema``."""
    from dynastore.modules.storage.driver_config import ItemsSchema
    assert ItemsSchema._address == ("platform", "catalog", "collection", "items", "schema")


def test_items_routing_address_is_tier_first():
    """Items routing lives at ``storage.items.routing``."""
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig
    assert ItemsRoutingConfig._address == ("platform", "catalog", "collection", "items", "routing")


def test_asset_routing_address_is_tier_first():
    """Asset routing lives at ``storage.assets.routing``."""
    from dynastore.modules.storage.routing_config import AssetRoutingConfig
    assert AssetRoutingConfig._address == ("platform", "catalog", "assets", "routing")


def test_catalog_routing_address_is_tier_first():
    """Catalog routing lives at ``storage.catalog.routing``."""
    from dynastore.modules.storage.routing_config import CatalogRoutingConfig
    assert CatalogRoutingConfig._address == ("platform", "catalog", "routing")


def test_collection_routing_address_is_2_tuple():
    """Collection-envelope routing kept as 2-tuple ``("storage","routing")``
    per the plan — structurally distinct from items/assets/catalog routing
    (it dispatches ``CollectionStore`` drivers, not items drivers).
    """
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig
    assert CollectionRoutingConfig._address == ("platform", "catalog", "collection", "routing")


def test_items_postgresql_driver_address_is_tier_first():
    """Items-tier PG driver lives at ``storage.items.drivers``."""
    from dynastore.modules.storage.driver_config import (
        ItemsPostgresqlDriverConfig,
    )
    assert ItemsPostgresqlDriverConfig._address == ("platform", "catalog", "collection", "items", "drivers")


def test_items_elasticsearch_driver_address_is_tier_first():
    """Items-tier ES driver lives at ``storage.items.drivers``."""
    from dynastore.modules.storage.driver_config import (
        ItemsElasticsearchDriverConfig,
    )
    assert ItemsElasticsearchDriverConfig._address == ("platform", "catalog", "collection", "items", "drivers")


def test_items_elasticsearch_private_driver_address_is_tier_first():
    """Items-tier ES private driver (Cycle E.2.b) lives at ``storage.items.drivers``."""
    from dynastore.modules.storage.driver_config import (
        ItemsElasticsearchPrivateDriverConfig,
    )
    assert ItemsElasticsearchPrivateDriverConfig._address == (
        "platform", "catalog", "collection", "items", "drivers",
    )


def test_items_duckdb_driver_address_is_tier_first():
    """Items-tier DuckDB driver lives at ``storage.items.drivers``."""
    from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
    assert ItemsDuckdbDriverConfig._address == ("platform", "catalog", "collection", "items", "drivers")


def test_items_iceberg_driver_address_is_tier_first():
    """Items-tier Iceberg driver lives at ``storage.items.drivers``."""
    from dynastore.modules.storage.driver_config import ItemsIcebergDriverConfig
    assert ItemsIcebergDriverConfig._address == ("platform", "catalog", "collection", "items", "drivers")


def test_asset_postgresql_driver_address_is_tier_first():
    """Asset-tier PG driver lives at ``storage.assets.drivers``."""
    from dynastore.modules.storage.driver_config import (
        AssetPostgresqlDriverConfig,
    )
    assert AssetPostgresqlDriverConfig._address == ("platform", "catalog", "assets", "drivers")


def test_asset_elasticsearch_driver_address_is_tier_first():
    """Asset-tier ES driver lives at ``storage.assets.drivers``."""
    from dynastore.modules.storage.driver_config import (
        AssetElasticsearchDriverConfig,
    )
    assert AssetElasticsearchDriverConfig._address == ("platform", "catalog", "assets", "drivers")


def test_catalog_es_driver_address_is_tier_first():
    """Catalog ES driver lives at ``storage.catalog.drivers``."""
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriverConfig,
    )
    assert CatalogElasticsearchDriverConfig._address == (
        "platform", "catalog", "drivers",
    )


def test_collection_es_driver_address_is_tier_first():
    """Collection ES driver lives at ``storage.collection.drivers``."""
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriverConfig,
    )
    assert CollectionElasticsearchDriverConfig._address == (
        "platform", "catalog", "collection", "drivers",
    )


# ---------------------------------------------------------------------------
# 2. PATCH transparency: round-trip lands at the tier-first read path
# ---------------------------------------------------------------------------


def _make_config_service(stored: Dict[Any, Any]):
    """Mock ``ConfigsProtocol`` with the minimum surface compose+patch use."""
    svc = MagicMock()

    async def _get_config(cls, catalog_id=None, collection_id=None, **_):
        return cls()

    async def _get_persisted_config(cls, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        value = stored.get(key)
        if value is None:
            return None
        return (
            value.model_dump(exclude_unset=True)
            if hasattr(value, "model_dump")
            else value
        )

    async def _set_config(cls, value, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored[key] = value
        return value

    async def _delete_config(cls, catalog_id=None, collection_id=None, **_):
        key = (cls.__name__, catalog_id, collection_id)
        stored.pop(key, None)

    async def _list_configs(catalog_id=None, collection_id=None, **_):
        results = [
            {
                "plugin_id": _to_plugin_id(k[0]),
                "config": (
                    v.model_dump() if hasattr(v, "model_dump") else dict(v)
                ),
            }
            for k, v in stored.items()
            if k[1] == catalog_id and k[2] == collection_id
        ]
        return {"results": results, "total": len(results)}

    svc.get_config = AsyncMock(side_effect=_get_config)
    svc.get_persisted_config = AsyncMock(side_effect=_get_persisted_config)
    svc.set_config = AsyncMock(side_effect=_set_config)
    svc.delete_config = AsyncMock(side_effect=_delete_config)
    svc.list_configs = AsyncMock(side_effect=_list_configs)
    # `_get_extra_refs` does `getattr(svc, "list_refs_at_scope", None)` and
    # bails when None — but MagicMock auto-creates attributes, so we have to
    # explicitly null them out for the getattr-is-None branch to fire.
    svc.list_refs_at_scope = None
    svc.get_config_by_ref = None
    return svc


def _to_plugin_id(class_name: str) -> str:
    """Mirror ``_to_snake`` — the registry key derivation."""
    from dynastore.tools.typed_store.base import _to_snake
    return _to_snake(class_name)


@pytest.mark.asyncio
async def test_patch_items_write_policy_then_get_lands_at_tier_first_path():
    """PATCH-transparency contract: a PATCH against
    ``/configs/.../plugins/items_write_policy`` updates the policy, and
    a subsequent GET surfaces the value at the post-D.2 tier-first path
    ``configs.platform.catalog.collection.items.policy.items_write_policy``.

    The class_key (URL segment) IS the leaf key in the GET tree — the
    round-trip wires unchanged regardless of the address restructure.
    """
    from dynastore.extensions.configs.config_api_service import ConfigApiService

    from unittest.mock import AsyncMock, patch as _patch

    stored: Dict[Any, Any] = {}
    config_svc = _make_config_service(stored)
    api = ConfigApiService(config_service=config_svc)

    # PATCH at collection scope.
    result = await api.patch_config(
        catalog_id="cat-x",
        collection_id="coll-y",
        body={"items_write_policy": {"on_conflict": "refuse_fail"}},
    )
    assert "items_write_policy" in result["updated"]

    # GET via compose_collection_config (resolved=True default; include=upstream
    # to surface even at the active scope without the slim filter so we can
    # see the patched value inline at its tier-first address).
    with _patch.object(api, "_get_extra_refs", new=AsyncMock(return_value={})):
        response = await api.compose_collection_config(
            base_url="http://test",
            catalog_id="cat-x",
            collection_id="coll-y",
            include="upstream",
        )

    leaf = (
        response.configs.get("platform", {})
        .get("catalog", {})
        .get("collection", {})
        .get("items", {})
        .get("policy", {})
        .get("items_write_policy")
    )
    assert leaf is not None, (
        "PATCH transparency broken: items_write_policy did not surface at "
        "post-F.0 path configs.platform.catalog.collection.items.policy.items_write_policy"
    )
    assert leaf["on_conflict"] == "refuse_fail"


# ---------------------------------------------------------------------------
# 3. Hierarchical ``inherited`` real-class scenario
# ---------------------------------------------------------------------------

def test_compose_collection_inherited_mirrors_configs_for_real_classes():
    """At collection scope under slim mode (default), upstream-tier real
    classes surface in the hierarchical ``inherited`` tree at their
    natural address — NOT inlined in ``configs`` and NOT in a sibling
    ``inherited_from_catalog`` block (D.3 dropped it).
    """
    from dynastore.extensions.configs.config_api_service import ConfigApiService
    from dynastore.modules.db_config.plugin_config import list_registered_configs

    # Use real registered classes — the registry is populated at import
    # time (extensions.features + modules.tiles imported above).
    by_class: Dict[str, Dict[str, Any]] = {
        "tiles_config":    {"enabled": True},   # platform-vis
        "items_routing_config": {"enabled": False},   # collection-vis
    }
    sources = {
        "tiles_config":         "platform",
        "items_routing_config": "collection",
    }
    if "items_routing_config" not in list_registered_configs():
        pytest.skip("items_routing_config not registered (slim runtime build)")
    if "tiles_config" not in list_registered_configs():
        pytest.skip("tiles_config not registered (slim runtime build)")

    tree = ConfigApiService._compose_tree(
        by_class, sources=sources, active_scope="collection",
        meta_mode="field",  # #946: opt into the _meta envelope
    )

    # collection-vis stays in body; _meta carries the active-tier provenance.
    leaf = (
        tree["platform"]["catalog"]["collection"]
        ["items"]["routing"]["items_routing_config"]
    )
    assert leaf["enabled"] is False
    assert leaf["_meta"]["tier"] == "collection"
    assert leaf["_meta"]["source"] == "collection"

    # Per #665 slice 3 the parallel ``inherited`` tree is retired —
    # platform-tier configs are filtered out under slim mode, full stop.
    assert "tiles" not in tree.get("platform", {})

    # No ``inherited_from_catalog`` sibling — D.3 dropped it (also gone in slice 3).
    assert "inherited_from_catalog" not in tree
