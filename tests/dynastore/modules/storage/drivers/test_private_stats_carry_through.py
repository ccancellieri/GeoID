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

"""Private/STAC-dict canonical path — stats carry-through (refs #1839/#1828).

``build_tenant_feature_doc`` on the Feature/dict path used to drop
already-computed stat values (s2/h3/geohash spatial-cell keys, area, centroid,
etc.) that ride flat on the source after ``map_row_to_feature``.  These tests
pin the carry-through fix: stat-classified keys are moved into ``stats{}`` via
``classify_container`` and verified to round-trip back via
``_private_source_to_feature``.

Constraint #1585: ``_private_source_to_feature`` must stay round-trip-correct —
``properties`` carries only user attrs; ``stats``/``system`` are reconstructed
from the canonical sections.
"""
from __future__ import annotations

from typing import Any, Dict

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GEOID = "019e6318-1839-stats-carry-through"
_CAT = "test_cat"
_COL = "test_col"


def _make_item_with_flat_stats(**extra_top_level) -> Dict[str, Any]:
    """Feature dict that carries flat stat keys the way map_row_to_feature emits them."""
    item: Dict[str, Any] = {
        "id": _GEOID,
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "bbox": [12.5, 41.9, 12.5, 41.9],
        "properties": {"name": "Rome"},
    }
    item.update(extra_top_level)
    return item


# ---------------------------------------------------------------------------
# _collect_flat_stats unit tests
# ---------------------------------------------------------------------------


class TestCollectFlatStats:
    """Unit-level tests for the internal helper."""

    def _collect(self, src, exclude=None):
        from dynastore.modules.storage.computed_fields import (
            SYSTEM_FIELD_KEYS,
            _IDENTITY_FIELD_NAMES,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            _collect_flat_stats,
        )

        _exclude = frozenset(SYSTEM_FIELD_KEYS) | _IDENTITY_FIELD_NAMES
        return _collect_flat_stats(src, exclude if exclude is not None else _exclude)

    def test_s2_top_level_classified_as_stats(self):
        src = {"id": _GEOID, "s2_res12": "89c25a3ffffffff", "properties": {}}
        result = self._collect(src)
        assert "s2_res12" in result
        assert result["s2_res12"] == "89c25a3ffffffff"

    def test_h3_top_level_classified_as_stats(self):
        src = {"id": _GEOID, "h3_res5": "851fa4d3fffffff", "properties": {}}
        result = self._collect(src)
        assert "h3_res5" in result
        assert result["h3_res5"] == "851fa4d3fffffff"

    def test_geohash_top_level_classified_as_stats(self):
        src = {"id": _GEOID, "geohash_6": "sr2yk1", "properties": {}}
        result = self._collect(src)
        assert "geohash_6" in result

    def test_area_top_level_classified_as_stats(self):
        src = {"id": _GEOID, "area": 12345.6, "properties": {}}
        result = self._collect(src)
        assert "area" in result
        assert result["area"] == 12345.6

    def test_centroid_top_level_classified_as_stats(self):
        src = {"id": _GEOID, "centroid": [12.5, 41.9], "properties": {}}
        result = self._collect(src)
        assert "centroid" in result

    def test_system_keys_excluded(self):
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        src = {"id": _GEOID, "geometry_hash": "ghash", "s2_res12": "cell", "properties": {}}
        result = self._collect(src)
        for sk in SYSTEM_FIELD_KEYS:
            assert sk not in result, f"system key {sk!r} leaked into stats"
        assert "s2_res12" in result

    def test_user_property_not_misclassified_as_stats(self):
        src = {"id": _GEOID, "properties": {"name": "Rome", "population": 100}}
        result = self._collect(src)
        assert "name" not in result
        assert "population" not in result

    def test_none_values_skipped(self):
        src = {"id": _GEOID, "s2_res12": None, "area": None, "properties": {}}
        result = self._collect(src)
        assert "s2_res12" not in result
        assert "area" not in result

    def test_stats_in_properties_bag_also_collected(self):
        """Stats buried inside properties bag (some serialisation paths)."""
        src = {"id": _GEOID, "properties": {"s2_res7": "89c25a3ffffffff", "name": "x"}}
        result = self._collect(src)
        assert "s2_res7" in result
        assert "name" not in result

    def test_top_level_wins_over_properties_duplicate(self):
        """When the same key appears both flat and inside properties, top-level wins."""
        src = {
            "id": _GEOID,
            "s2_res12": "top-level-cell",
            "properties": {"s2_res12": "props-cell"},
        }
        result = self._collect(src)
        assert result["s2_res12"] == "top-level-cell"


# ---------------------------------------------------------------------------
# build_tenant_feature_doc integration tests
# ---------------------------------------------------------------------------


class TestBuildTenantFeatureDocStats:
    """Flat stat values on the source dict land under ``stats{}`` in the doc."""

    def _build(self, item: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )

        return build_tenant_feature_doc(
            item, catalog_id=_CAT, collection_id=_COL, **kwargs
        )

    def test_s2_flat_lands_in_stats(self):
        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff")
        doc = self._build(item)
        assert "stats" in doc, f"stats section missing; keys: {list(doc.keys())}"
        assert doc["stats"].get("s2_res12") == "89c25a3ffffffff"

    def test_h3_flat_lands_in_stats(self):
        item = _make_item_with_flat_stats(h3_res5="851fa4d3fffffff")
        doc = self._build(item)
        assert doc["stats"].get("h3_res5") == "851fa4d3fffffff"

    def test_geohash_flat_lands_in_stats(self):
        item = _make_item_with_flat_stats(geohash_6="sr2yk1")
        doc = self._build(item)
        assert "geohash_6" in doc.get("stats", {})

    def test_area_flat_lands_in_stats(self):
        item = _make_item_with_flat_stats(area=99.9)
        doc = self._build(item)
        assert doc.get("stats", {}).get("area") == pytest.approx(99.9)

    def test_multiple_stat_keys_all_present(self):
        item = _make_item_with_flat_stats(
            s2_res12="89c25a3ffffffff",
            h3_res5="851fa4d3fffffff",
            area=100.0,
            centroid=[12.5, 41.9],
        )
        doc = self._build(item)
        stats = doc.get("stats", {})
        assert "s2_res12" in stats
        assert "h3_res5" in stats
        assert "area" in stats
        assert "centroid" in stats

    def test_stats_not_present_in_properties(self):
        """Stat values must NOT bleed into ``properties``."""
        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff", area=50.0)
        doc = self._build(item)
        props = doc.get("properties", {})
        assert "s2_res12" not in props
        assert "area" not in props

    def test_stats_not_present_in_system(self):
        """Stat values must NOT bleed into ``system``."""
        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff")
        doc = self._build(item)
        system = doc.get("system", {})
        assert "s2_res12" not in system

    def test_user_properties_not_misclassified(self):
        """User properties must stay in ``properties`` (or ``properties.extras``
        for undeclared keys), not ``stats``."""
        item = {
            "id": _GEOID,
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"name": "Rome", "population": 5000},
        }
        doc = self._build(item)
        # With known_fields={}, undeclared keys move to properties.extras.
        # Either way, they must NOT appear in stats.
        props = doc.get("properties", {})
        all_props = dict(props)
        all_props.update(props.get("extras") or {})
        assert all_props.get("name") == "Rome"
        assert "name" not in doc.get("stats", {})
        assert "population" not in doc.get("stats", {})

    def test_no_flat_stats_produces_no_stats_section(self):
        """When no stat keys are present the stats section is absent."""
        item = {
            "id": _GEOID,
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"name": "x"},
        }
        doc = self._build(item)
        assert "stats" not in doc, (
            f"Unexpected stats section: {doc.get('stats')}"
        )


# ---------------------------------------------------------------------------
# Round-trip: build_tenant_feature_doc → _private_source_to_feature (#1585)
# ---------------------------------------------------------------------------


class TestPrivateDocRoundTrip:
    """Stats land in ``stats{}``; ``_private_source_to_feature`` must not
    surface them into ``properties`` — that section stays user-only."""

    def test_round_trip_stats_not_in_feature_properties(self):
        """Stats must NOT appear in the reconstructed Feature.properties.

        ``_private_source_to_feature`` reads ``properties`` (user attrs) and
        ``system`` (simplification info). It does not read ``stats``; that
        section is for the ES-internal canonical envelope only.  After round-
        trip, ``properties`` must be identical to the original user attributes.
        """
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff", area=42.0)
        item["properties"]["name"] = "Rome"

        doc = build_tenant_feature_doc(item, catalog_id=_CAT, collection_id=_COL)

        # Simulate the ES read-back path.
        feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            doc, _CAT, _COL, doc.get("id"),
        )

        props = dict(feature.properties or {})
        # Stat keys must NOT appear in feature.properties (or extras).
        all_props = dict(props)
        all_props.update((props.get("extras") or {}))
        assert "s2_res12" not in all_props, (
            f"s2_res12 leaked into feature.properties: {all_props}"
        )
        assert "area" not in all_props, (
            f"area leaked into feature.properties: {all_props}"
        )
        # User attrs are present somewhere in the properties bag.
        assert all_props.get("name") == "Rome"

    def test_round_trip_system_keys_not_in_feature_properties(self):
        """System key values (geometry_hash, etc.) must not appear in properties
        after round-trip — they live in the ``system`` section only."""
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        item = {
            "id": _GEOID,
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"user_attr": "value"},
            "geometry_hash": "ghash123",
        }
        doc = build_tenant_feature_doc(item, catalog_id=_CAT, collection_id=_COL)

        feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            doc, _CAT, _COL, doc.get("id"),
        )
        props = dict(feature.properties or {})
        for sk in SYSTEM_FIELD_KEYS:
            # external_id is explicitly promoted into props by _private_source_to_feature;
            # all other system keys must stay out.
            if sk == "external_id":
                continue
            assert sk not in props, (
                f"system key {sk!r} leaked into feature.properties: {props}"
            )

    def test_round_trip_id_preserved(self):
        """The reconstructed Feature.id must equal the original geoid."""
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff")
        doc = build_tenant_feature_doc(item, catalog_id=_CAT, collection_id=_COL)
        feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            doc, _CAT, _COL, doc.get("id"),
        )
        assert str(feature.id) == _GEOID

    def test_round_trip_geoid_alias_at_root_for_canonical_docs(self):
        """Canonical docs (post-#1828) must carry ``geoid`` at root alongside ``id``.

        ``build_tenant_feature_doc`` adds a ``geoid`` root alias equal to ``id``
        so PRIVATE_ENVELOPE_FIELDS structural queries (targeting ``geoid``) can
        find canonical private docs, and ``_private_source_to_feature`` never
        reconstructs a feature with id=None from the search path.
        """
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        item = _make_item_with_flat_stats(s2_res12="89c25a3ffffffff")
        doc = build_tenant_feature_doc(item, catalog_id=_CAT, collection_id=_COL)

        # The canonical doc must have both ``id`` and ``geoid`` at root.
        assert doc.get("id") == _GEOID, f"id missing or wrong: {doc.get('id')!r}"
        assert doc.get("geoid") == _GEOID, (
            f"geoid alias missing or wrong: {doc.get('geoid')!r}"
        )
        assert doc["id"] == doc["geoid"], "id and geoid alias must be equal"

        # Simulate the search path: ES hit passes _id as fallback, not src.get("geoid").
        # _private_source_to_feature must prefer ``geoid`` at root (canonical alias)
        # so Feature.id is always the correct geoid.
        feature_from_search = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            doc, _CAT, _COL, "es-hit-fallback-id",
        )
        assert str(feature_from_search.id) == _GEOID, (
            f"Search-path round-trip: Feature.id must be the geoid, "
            f"got {feature_from_search.id!r}"
        )

    def test_round_trip_id_from_id_field_when_geoid_alias_missing(self):
        """``_private_source_to_feature`` falls back to ``source['id']`` when
        ``source['geoid']`` is absent (docs written without the alias).

        This covers the transition window: docs written by old code that only
        set ``id`` at root (no ``geoid`` alias). The fallback chain is:
        ``geoid`` → ``id`` → caller-supplied fallback.
        """
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        # Simulate a canonical doc that has ``id`` but no ``geoid`` alias.
        source_without_geoid_alias = {
            "id": _GEOID,
            "catalog_id": _CAT,
            "collection_id": _COL,
            "properties": {"name": "Rome"},
            "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        }
        feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            source_without_geoid_alias, _CAT, _COL, "es-fallback",
        )
        assert str(feature.id) == _GEOID, (
            f"Fallback to 'id' field failed: got {feature.id!r}"
        )

    def test_round_trip_falls_back_to_caller_id_when_both_root_keys_absent(self):
        """``_private_source_to_feature`` uses the caller-supplied fallback
        when neither ``geoid`` nor ``id`` is present in the source dict
        (corrupt / minimal doc — defensive fallback).
        """
        from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
            ItemsElasticsearchPrivateDriver,
        )

        # Pre-canonical legacy doc: only ``catalog_id``/``collection_id`` at root.
        minimal_source = {
            "catalog_id": _CAT,
            "collection_id": _COL,
            "properties": {"name": "x"},
        }
        feature = ItemsElasticsearchPrivateDriver._private_source_to_feature(
            minimal_source, _CAT, _COL, "caller-supplied-geoid",
        )
        assert str(feature.id) == "caller-supplied-geoid", (
            f"Fallback to caller-supplied id failed: got {feature.id!r}"
        )
