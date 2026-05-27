"""Unit tests for Tier-2 overlay + validator + #756 ``mapping`` revival (#887 commit 2).

Two anchor tests:

* **f** — Tier-2 overlay surfaces in the items mapping and the
  projection routes overlay keys to ``properties.<key>`` (not ``extras``).
* **g** — Tier-2 collision with Tier 1 at a different type raises
  ``ValueError`` via :func:`validate_tier_2`. Same-type re-declarations
  are tolerated.

Also pins the dead-field drops on ``ItemsElasticsearchDriverConfig``
and ``AssetElasticsearchDriverConfig`` per #756 Round 6.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest

from dynastore.modules.elasticsearch.items_projection import (
    build_known_fields,
    project_item_for_es,
    resolve_es_field_path,
    validate_tier_2,
)
from dynastore.modules.elasticsearch.mappings import build_item_mapping


class _FakeCatalogConfig:
    """Minimal stand-in for ``ItemsElasticsearchDriverConfig`` — only the
    ``mapping`` attribute is read by :func:`build_known_fields`."""

    def __init__(self, mapping: Dict[str, Dict[str, Any]]):
        self.mapping = mapping


# --- f -----------------------------------------------------------------


def test_tier_2_overlay_surfaces_in_known_fields() -> None:
    cfg = _FakeCatalogConfig(
        mapping={
            "fao:custom_score": {"type": "float"},
            "fao:custom_label": {"type": "keyword"},
        },
    )
    known = build_known_fields(cfg)
    # Tier-1 fields still present (sanity).
    assert "datetime" in known
    assert "eo:cloud_cover" in known
    # Tier-2 fields merged in.
    assert known["fao:custom_score"] == {"type": "float"}
    assert known["fao:custom_label"] == {"type": "keyword"}


def test_tier_2_overlay_in_built_mapping() -> None:
    cfg = _FakeCatalogConfig(mapping={"fao:custom_score": {"type": "float"}})
    mapping = build_item_mapping(build_known_fields(cfg))
    nested = mapping["properties"]["properties"]["properties"]
    assert nested["fao:custom_score"] == {"type": "float"}
    # ``extras`` lane still present alongside as a cap-safe flattened field (#1295).
    assert nested["extras"] == {"type": "flattened"}


def test_tier_2_overlay_routes_field_at_top_level_not_extras() -> None:
    """The projection helper must place Tier-2 keys at ``properties.<k>``,
    not under ``properties.extras``. Otherwise the index mapping and the
    written doc disagree on where the field lives.
    """
    cfg = _FakeCatalogConfig(mapping={"fao:custom_score": {"type": "float"}})
    known = build_known_fields(cfg)
    doc = {
        "id": "x",
        "properties": {
            "datetime": "2026-05-17T00:00:00Z",
            "fao:custom_score": 0.42,
            "vendor:unknown": "y",
        },
    }
    out = project_item_for_es(doc, known)
    # Tier-2 field stays at top level.
    assert out["properties"]["fao:custom_score"] == 0.42
    # Still-unknown field lands in extras.
    assert out["properties"].get("extras", {}).get("vendor:unknown") == "y"
    assert "fao:custom_score" not in out["properties"].get("extras", {})


def test_resolve_es_field_path_with_tier_2() -> None:
    cfg = _FakeCatalogConfig(mapping={"fao:custom_score": {"type": "float"}})
    known = build_known_fields(cfg)
    # Tier-2 field passes through as-is when its known-fields map includes it.
    assert (
        resolve_es_field_path("properties.fao:custom_score", known)
        == "properties.fao:custom_score"
    )
    # Without the overlay (Tier-1-only), the same path routes to extras.
    tier_1_only = build_known_fields(None)
    assert (
        resolve_es_field_path("properties.fao:custom_score", tier_1_only)
        == "properties.extras.fao:custom_score"
    )


# --- g -----------------------------------------------------------------


def test_validate_tier_2_rejects_collision_at_different_type() -> None:
    """Collision on a Tier-1 key at a different type breaks the alias contract."""
    bad = {"datetime": {"type": "keyword"}}  # Tier 1 has datetime: date
    with pytest.raises(ValueError, match="collides with platform Tier-1"):
        validate_tier_2(bad)


def test_validate_tier_2_tolerates_same_type_redeclaration() -> None:
    """Re-declaring a Tier-1 key with the same type is a no-op overlay
    (useful for operators who want to mirror Tier 1 in their catalog
    config for documentation purposes)."""
    good = {"datetime": {"type": "date"}}
    validate_tier_2(good)  # no raise


def test_validate_tier_2_rejects_value_without_type() -> None:
    with pytest.raises(ValueError, match="dict with at least a 'type'"):
        validate_tier_2({"fao:bad": {"description": "no type field"}})


def test_validate_tier_2_rejects_non_dict_value() -> None:
    with pytest.raises(ValueError, match="dict with at least a 'type'"):
        validate_tier_2({"fao:bad": "not-a-dict"})


def test_validate_tier_2_empty_overlay_passes() -> None:
    validate_tier_2({})  # no raise


def test_validate_tier_2_accepts_pure_extension_addition() -> None:
    validate_tier_2(
        {
            "fao:custom_score": {"type": "float"},
            "fao:custom_label": {"type": "keyword"},
        }
    )


def test_validate_tier_2_rejects_non_dict_root() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        validate_tier_2([{"fao:bad": {"type": "float"}}])  # type: ignore[arg-type]


# --- #756 Round 6 dead-field drops -----------------------------------


def test_items_es_driver_config_no_longer_carries_index_prefix() -> None:
    """#756 Round 6 — ``ItemsElasticsearchDriverConfig.index_prefix`` was a
    dead field. The actual prefix routes through
    ``modules.elasticsearch.client.get_index_prefix()`` (module-global,
    env-driven). Dropping the field surfaces silent no-ops if an operator
    tries to set ``ES_ITEMS_INDEX_PREFIX`` per-catalog.
    """
    from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

    fields = ItemsElasticsearchDriverConfig.model_fields
    assert "index_prefix" not in fields, (
        "index_prefix should have been dropped per #756 Round 6"
    )
    # But the live ``mapping`` field is now the Tier-2 carrier.
    assert "mapping" in fields


def test_asset_es_driver_config_no_longer_carries_index_prefix() -> None:
    """Same dead-field drop for assets per #756 Round 6 — assets index
    prefix also routes through the module-global helper.
    """
    from dynastore.modules.storage.driver_config import AssetElasticsearchDriverConfig

    fields = AssetElasticsearchDriverConfig.model_fields
    assert "index_prefix" not in fields


def test_items_es_driver_config_validate_handler_registered() -> None:
    """The driver_config module must wire validate_tier_2 through the
    PluginConfig validate-handler protocol so a colliding PUT fails with
    400 at the HTTP boundary."""
    from dynastore.modules.storage.driver_config import ItemsElasticsearchDriverConfig

    # The handler list lives on the PluginConfig class as a class attr;
    # check by registering shape rather than inspecting private internals.
    # The smoking test is that an instance built with a colliding mapping
    # round-trips through the configured validator.
    cfg = ItemsElasticsearchDriverConfig(mapping={"datetime": {"type": "keyword"}})
    # Validators are async and bound to the platform configs service; the
    # local invocation just confirms the schema accepts the field (the
    # 400-at-PUT regression lives in the integration suite). What we can
    # pin here: the handler we register exists and rejects shape.
    from dynastore.modules.storage.driver_config import (
        _validate_items_es_driver_config,
    )
    import asyncio

    async def _runner() -> None:
        with pytest.raises(ValueError):
            await _validate_items_es_driver_config(cfg, None, None, None)

    asyncio.run(_runner())
