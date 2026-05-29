"""Generic ``PresetBundle`` entries surface (#972 PR-1, #1657).

Pins the post-#1657 contract:

* ``PresetBundle`` is constructed with ``entries=(PresetBundleEntry(...),
  ...)`` — the single canonical form — and exposes a flat tuple via
  ``iter_apply`` / ``iter_rollback``.
* The tier-field accessors (``catalog_routing`` / ``collection_template``
  / ``items_template`` / ``asset_template`` / ``audience_configs``) are
  read-only computed views over ``entries``.
* ``rollback_priority`` controls leaf-first ordering: items/asset
  templates unapply first, audiences trail, ties keep insertion order.
"""
from __future__ import annotations

from dynastore.modules.storage.presets import (
    PresetBundle,
    PresetBundleEntry,
)
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)


class _AudCfg:
    """Minimal stand-in for an audience PluginConfig (only ``model_dump``
    is needed for equality; iter_apply / iter_rollback are pure
    metadata)."""

    def model_dump(self, mode: str = "json"):
        return {"k": "v"}


def _routing_entries(*, asset: bool = False, audiences: tuple = ()) -> tuple:
    """Build the standard routing entries (the shape the migrated presets
    emit), optionally with an asset leaf and trailing audience entries."""
    entries = [
        PresetBundleEntry(
            slot="catalog_routing",
            config_cls=CatalogRoutingConfig,
            instance=CatalogRoutingConfig(operations={}),
            rollback_priority=30,
        ),
        PresetBundleEntry(
            slot="collection_template",
            config_cls=CollectionRoutingConfig,
            instance=CollectionRoutingConfig(operations={}),
            rollback_priority=20,
        ),
        PresetBundleEntry(
            slot="items_template",
            config_cls=ItemsRoutingConfig,
            instance=ItemsRoutingConfig(operations={}),
            rollback_priority=10,
        ),
    ]
    if asset:
        entries.append(PresetBundleEntry(
            slot="asset_template",
            config_cls=AssetRoutingConfig,
            instance=AssetRoutingConfig(operations={}),
            rollback_priority=10,
        ))
    for name in audiences:
        entries.append(PresetBundleEntry(
            slot=f"audience:{name}",
            config_cls=_AudCfg,
            instance=_AudCfg(),
            rollback_priority=100,
        ))
    return tuple(entries)


def test_apply_order_follows_entry_order():
    bundle = PresetBundle(entries=_routing_entries(audiences=("my_aud",)))
    slots = [e.slot for e in bundle.iter_apply()]
    assert slots == [
        "catalog_routing",
        "collection_template",
        "items_template",
        "audience:my_aud",
    ]


def test_asset_template_accessor_views_into_entries():
    """The ``asset_template`` accessor surfaces the asset entry instance."""
    asset = AssetRoutingConfig(operations={})
    bundle = PresetBundle(entries=(
        PresetBundleEntry("catalog_routing", CatalogRoutingConfig,
                          CatalogRoutingConfig(operations={}), rollback_priority=30),
        PresetBundleEntry("items_template", ItemsRoutingConfig,
                          ItemsRoutingConfig(operations={}), rollback_priority=10),
        PresetBundleEntry("asset_template", AssetRoutingConfig, asset, rollback_priority=10),
    ))
    assert [e.slot for e in bundle.iter_apply()] == [
        "catalog_routing",
        "items_template",
        "asset_template",
    ]
    assert bundle.asset_template is asset


def test_asset_template_rollback_is_leaf_first():
    """``asset_template`` unapplies at the leaf priority (alongside
    ``items_template``), before the collection/catalog envelopes."""
    bundle = PresetBundle(entries=_routing_entries(asset=True))
    slots = [e.slot for e in bundle.iter_rollback()]
    assert slots == [
        "items_template",
        "asset_template",
        "collection_template",
        "catalog_routing",
    ]


def test_accessors_view_into_entries():
    cat = CatalogRoutingConfig(operations={})
    items = ItemsRoutingConfig(operations={})
    aud = _AudCfg()
    bundle = PresetBundle(entries=(
        PresetBundleEntry("catalog_routing", CatalogRoutingConfig, cat, rollback_priority=30),
        PresetBundleEntry("items_template", ItemsRoutingConfig, items, rollback_priority=10),
        PresetBundleEntry("audience:x", _AudCfg, aud, rollback_priority=100),
    ))
    assert bundle.catalog_routing is cat
    assert bundle.items_template is items
    assert bundle.collection_template is None
    assert bundle.audience_configs == {"x": aud}


def test_iter_rollback_orders_leaves_first_audiences_last():
    bundle = PresetBundle(entries=_routing_entries(audiences=("a1", "a2")))
    slots = [e.slot for e in bundle.iter_rollback()]
    assert slots == [
        "items_template",
        "collection_template",
        "catalog_routing",
        "audience:a1",
        "audience:a2",
    ]


def test_rollback_tie_break_preserves_insertion_order():
    """Two entries with the same ``rollback_priority`` must come back
    in their original insertion order, not in some hashed / sorted
    order — operators diff the response payloads."""
    items = ItemsRoutingConfig(operations={})
    bundle = PresetBundle(entries=(
        PresetBundleEntry("first", ItemsRoutingConfig, items, rollback_priority=50),
        PresetBundleEntry("second", ItemsRoutingConfig, items, rollback_priority=50),
        PresetBundleEntry("third", ItemsRoutingConfig, items, rollback_priority=50),
    ))
    assert [e.slot for e in bundle.iter_rollback()] == ["first", "second", "third"]


def test_empty_bundle_iterates_empty():
    bundle = PresetBundle()
    assert list(bundle.iter_apply()) == []
    assert list(bundle.iter_rollback()) == []
    assert bundle.catalog_routing is None
    assert bundle.collection_template is None
    assert bundle.items_template is None
    assert bundle.asset_template is None
    assert bundle.audience_configs == {}


def test_entry_scope_defaults_empty_and_overlays_at_walker():
    """Entries default to empty scope; the admin walker layers
    URL-derived scope (``catalog_id``) on top — exercised end-to-end in
    ``test_admin_presets_endpoint.py``."""
    items = ItemsRoutingConfig(operations={})
    e = PresetBundleEntry("items_template", ItemsRoutingConfig, items)
    assert dict(e.scope) == {}
