"""Generic ``PresetBundle`` entries surface (#972 PR-1).

Pins the post-#972 contract:

* ``PresetBundle`` exposes a flat tuple of ``PresetBundleEntry`` via
  ``iter_apply``/``iter_rollback``.
* The legacy tier-field constructor (``catalog_routing=...``,
  ``items_template=...``, ``audience_configs={...}``) lifts the inputs
  into ``entries`` with the documented apply + rollback orderings, so
  the pre-#972 surface keeps working unchanged.
* ``rollback_priority`` controls leaf-first ordering: items templates
  unapply first, audiences trail, ties keep insertion order.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.presets import (
    PresetBundle,
    PresetBundleEntry,
)
from dynastore.modules.storage.routing_config import (
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


def test_legacy_constructor_lifts_to_entries():
    cat = CatalogRoutingConfig(operations={})
    coll = CollectionRoutingConfig(operations={})
    items = ItemsRoutingConfig(operations={})
    aud = _AudCfg()

    bundle = PresetBundle(
        catalog_routing=cat,
        collection_template=coll,
        items_template=items,
        audience_configs={"my_aud": aud},
    )
    slots = [e.slot for e in bundle.iter_apply()]
    assert slots == [
        "catalog_routing",
        "collection_template",
        "items_template",
        "audience:my_aud",
    ]


def test_legacy_accessors_view_into_entries():
    cat = CatalogRoutingConfig(operations={})
    items = ItemsRoutingConfig(operations={})
    aud = _AudCfg()
    bundle = PresetBundle(
        catalog_routing=cat,
        items_template=items,
        audience_configs={"x": aud},
    )
    assert bundle.catalog_routing is cat
    assert bundle.items_template is items
    assert bundle.collection_template is None
    assert bundle.audience_configs == {"x": aud}


def test_iter_rollback_orders_leaves_first_audiences_last():
    bundle = PresetBundle(
        catalog_routing=CatalogRoutingConfig(operations={}),
        collection_template=CollectionRoutingConfig(operations={}),
        items_template=ItemsRoutingConfig(operations={}),
        audience_configs={"a1": _AudCfg(), "a2": _AudCfg()},
    )
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


def test_mixing_entries_and_legacy_fields_raises():
    with pytest.raises(TypeError, match="entries"):
        PresetBundle(
            entries=(PresetBundleEntry("x", ItemsRoutingConfig, ItemsRoutingConfig(operations={})),),
            catalog_routing=CatalogRoutingConfig(operations={}),
        )


def test_empty_bundle_iterates_empty():
    bundle = PresetBundle()
    assert list(bundle.iter_apply()) == []
    assert list(bundle.iter_rollback()) == []
    assert bundle.catalog_routing is None
    assert bundle.collection_template is None
    assert bundle.items_template is None
    assert bundle.audience_configs == {}


def test_entry_scope_defaults_empty_and_overlays_at_walker():
    """Entries default to empty scope; the admin walker layers
    URL-derived scope (``catalog_id``) on top — exercised end-to-end in
    ``test_admin_presets_endpoint.py``."""
    items = ItemsRoutingConfig(operations={})
    e = PresetBundleEntry("items_template", ItemsRoutingConfig, items)
    assert dict(e.scope) == {}
