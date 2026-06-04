#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
"""Unit tests for geometry_simplification in the private ES driver (#1828 Phase 2).

Covers:
  - Write: when simplification mode != "none", the doc carries
    ``system.geometry_simplification.{factor,mode}`` and NOT flat root keys.
  - Write: when mode == "none" (no simplification), ``system.geometry_simplification``
    is absent.
  - Read (_private_source_to_feature): canonical path reads from system container.
  - Read (_private_source_to_feature): back-compat fallback reads old flat keys
    on docs written before #1828 Phase 2.
  - Read: when neither canonical nor flat keys are present, simplification fields
    are absent from properties.
  - Mapping: ``_PRIVATE_SYSTEM_FIELDS`` declares ``geometry_simplification`` with
    correct ES types; flat root-level keys are absent.
  - Transformer (restore_from_index): same canonical+back-compat read logic.
"""
from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_POINT_GEOMETRY = {"type": "Point", "coordinates": [12.5, 42.0]}

_BASE_ITEM = {
    "id": "019e6318-d99e-7da2-bdd9-1223a0d9cd35",
    "type": "Feature",
    "geometry": _POINT_GEOMETRY,
    "properties": {"title": "Roma"},
}


def _make_source(**overrides):
    """Build a minimal private ES doc ``_source`` dict."""
    src = {
        "geoid": "019e6318-d99e-7da2-bdd9-1223a0d9cd35",
        "catalog_id": "cat1",
        "collection_id": "col1",
        "geometry": _POINT_GEOMETRY,
        "properties": {"title": "Roma"},
    }
    src.update(overrides)
    return src


# ---------------------------------------------------------------------------
# Write path — doc shape produced by the write helpers
# ---------------------------------------------------------------------------

def _apply_simplification(doc, factor, mode):
    """Replicate the write-path logic from driver.py and transformer.py."""
    if mode != "none":
        doc.setdefault("system", {})["geometry_simplification"] = {
            "factor": factor,
            "mode": mode,
        }
    return doc


def test_write_sets_system_geometry_simplification_when_simplified():
    """When simplification runs (mode != 'none'), the doc must carry
    ``system.geometry_simplification`` and NOT flat root-level keys."""
    doc = {"id": "abc", "geometry": _POINT_GEOMETRY, "properties": {}}
    _apply_simplification(doc, factor=0.001, mode="rdp")

    assert "system" in doc
    gs = doc["system"]["geometry_simplification"]
    assert gs["factor"] == pytest.approx(0.001)
    assert gs["mode"] == "rdp"
    # flat root keys must NOT be present
    assert "simplification_factor" not in doc
    assert "simplification_mode" not in doc


def test_write_does_not_set_system_geometry_simplification_when_none():
    """When mode == 'none' (geometry was not simplified), the
    ``system.geometry_simplification`` key must be absent."""
    doc = {"id": "abc", "geometry": _POINT_GEOMETRY, "properties": {}}
    _apply_simplification(doc, factor=1.0, mode="none")

    # system key was never created for this doc
    assert "geometry_simplification" not in doc.get("system", {})
    assert "simplification_factor" not in doc
    assert "simplification_mode" not in doc


def test_write_preserves_existing_system_fields():
    """When ``system`` already carries other fields, the write-path must
    add ``geometry_simplification`` without clobbering them."""
    doc = {
        "id": "abc",
        "system": {"geoid": "abc", "geometry_hash": "gh123"},
        "geometry": _POINT_GEOMETRY,
        "properties": {},
    }
    _apply_simplification(doc, factor=0.005, mode="vw")

    assert doc["system"]["geoid"] == "abc"
    assert doc["system"]["geometry_hash"] == "gh123"
    gs = doc["system"]["geometry_simplification"]
    assert gs["factor"] == pytest.approx(0.005)
    assert gs["mode"] == "vw"


# ---------------------------------------------------------------------------
# Read path — _private_source_to_feature
# ---------------------------------------------------------------------------

def test_read_surfaces_simplification_from_system_container():
    """_private_source_to_feature must read factor/mode from
    ``system.geometry_simplification`` and surface them into properties."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    source = _make_source(system={
        "geoid": "019e6318-d99e-7da2-bdd9-1223a0d9cd35",
        "geometry_simplification": {"factor": 0.002, "mode": "rdp"},
    })
    feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
        source, catalog_id="cat1", collection_id="col1", fallback_id="fallback",
    )
    assert feature.properties["simplification_factor"] == pytest.approx(0.002)
    assert feature.properties["simplification_mode"] == "rdp"


def test_read_back_compat_flat_keys_when_no_system_container():
    """Docs written before #1828 Phase 2 store factor/mode at the doc root.
    _private_source_to_feature must fall back to those flat keys."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    source = _make_source(
        simplification_factor=0.0001,
        simplification_mode="vw",
    )
    # No ``system.geometry_simplification`` key — old flat shape.
    assert "system" not in source
    feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
        source, catalog_id="cat1", collection_id="col1", fallback_id="fallback",
    )
    assert feature.properties["simplification_factor"] == pytest.approx(0.0001)
    assert feature.properties["simplification_mode"] == "vw"


def test_read_no_simplification_fields_when_absent():
    """When neither canonical nor flat keys are present, simplification fields
    must be absent from properties (geometry was not simplified)."""
    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    source = _make_source()
    feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
        source, catalog_id="cat1", collection_id="col1", fallback_id="fallback",
    )
    assert "simplification_factor" not in feature.properties
    assert "simplification_mode" not in feature.properties


# ---------------------------------------------------------------------------
# Mapping — system container has geometry_simplification; flat keys are gone
# ---------------------------------------------------------------------------

def test_private_system_fields_declare_geometry_simplification():
    """_PRIVATE_SYSTEM_FIELDS must include ``geometry_simplification`` with the
    correct ES type structure."""
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        _PRIVATE_SYSTEM_FIELDS,
    )
    assert "geometry_simplification" in _PRIVATE_SYSTEM_FIELDS
    gs = _PRIVATE_SYSTEM_FIELDS["geometry_simplification"]
    assert gs["type"] == "object"
    props = gs["properties"]
    assert props["factor"]["type"] == "float"
    assert props["mode"]["type"] == "keyword"


def test_tenant_root_fields_no_longer_contain_flat_simplification_keys():
    """The ``_tenant_root_fields()`` helper must NOT declare flat
    ``simplification_factor`` / ``simplification_mode`` entries — new docs
    write these under ``system.geometry_simplification``."""
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        _tenant_root_fields,
    )
    root = _tenant_root_fields()
    assert "simplification_factor" not in root
    assert "simplification_mode" not in root


def test_private_reserved_root_fields_excludes_flat_simplification():
    """_PRIVATE_RESERVED_ROOT_FIELDS must not include the old flat keys
    (they are no longer written at the doc root)."""
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        _PRIVATE_RESERVED_ROOT_FIELDS,
    )
    assert "simplification_factor" not in _PRIVATE_RESERVED_ROOT_FIELDS
    assert "simplification_mode" not in _PRIVATE_RESERVED_ROOT_FIELDS


# ---------------------------------------------------------------------------
# Transformer restore_from_index — canonical + back-compat read
# ---------------------------------------------------------------------------

def test_transformer_restore_reads_from_system_container():
    """PrivateEntityTransformer.restore_from_index must surface
    simplification fields from ``system.geometry_simplification``."""
    import asyncio
    from dynastore.modules.storage.drivers.elasticsearch_private.transformer import (
        PrivateEntityTransformer,
    )
    from dynastore.models.protocols.entity_transform import TransformChainContext

    transformer = PrivateEntityTransformer()
    doc = {
        "geoid": "abc-123",
        "catalog_id": "cat1",
        "collection_id": "col1",
        "geometry": _POINT_GEOMETRY,
        "properties": {"title": "Roma"},
        "system": {
            "geoid": "abc-123",
            "geometry_simplification": {"factor": 0.003, "mode": "rdp"},
        },
    }
    ctx = TransformChainContext(cache={})

    result = asyncio.get_event_loop().run_until_complete(
        transformer.restore_from_index(
            doc, catalog_id="cat1", collection_id="col1",
            entity_kind="item", ctx=ctx,
        )
    )
    assert isinstance(result, dict)
    props = result.get("properties", {})
    assert props.get("simplification_factor") == pytest.approx(0.003)
    assert props.get("simplification_mode") == "rdp"


def test_transformer_restore_back_compat_flat_keys():
    """PrivateEntityTransformer.restore_from_index must fall back to flat
    root keys when ``system.geometry_simplification`` is absent."""
    import asyncio
    from dynastore.modules.storage.drivers.elasticsearch_private.transformer import (
        PrivateEntityTransformer,
    )
    from dynastore.models.protocols.entity_transform import TransformChainContext

    transformer = PrivateEntityTransformer()
    doc = {
        "geoid": "abc-123",
        "catalog_id": "cat1",
        "collection_id": "col1",
        "geometry": _POINT_GEOMETRY,
        "properties": {"title": "Roma"},
        "simplification_factor": 0.0005,
        "simplification_mode": "vw",
    }
    ctx = TransformChainContext(cache={})

    result = asyncio.get_event_loop().run_until_complete(
        transformer.restore_from_index(
            doc, catalog_id="cat1", collection_id="col1",
            entity_kind="item", ctx=ctx,
        )
    )
    props = result.get("properties", {})
    assert props.get("simplification_factor") == pytest.approx(0.0005)
    assert props.get("simplification_mode") == "vw"
