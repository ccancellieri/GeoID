"""
Unit tests for modules/tools/item_stream.py.

Covers:
  - normalize_feature_attributes: model_extra lifting into properties
  - stream_normalized_items: driver-agnostic stream boundary (regression test
    for #1818 where PG-path features had properties={} with all attributes only
    in model_extra, causing join consumers to drop every row silently).
  - resolve_join_value: section-aware join key resolution (#1827).
"""

import pytest
from unittest.mock import MagicMock

from dynastore.models.ogc import Feature
from dynastore.modules.tools.item_stream import (
    normalize_feature_attributes,
    resolve_join_value,
    stream_join_features,
    stream_normalized_items,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _feat(fid, props=None, **extra_kwargs):
    """Build a Feature. Extra kwargs end up in model_extra (extra='allow')."""
    return Feature(type="Feature", id=fid, geometry=None, properties=props or {}, **extra_kwargs)


# ---------------------------------------------------------------------------
# normalize_feature_attributes
# ---------------------------------------------------------------------------

def test_normalize_lifts_model_extra_into_properties():
    """PG-path extras (adm0_name, code) land in properties after normalization."""
    extra = {"adm0_name": "Italy", "code": "1616"}
    feat = Feature(type="Feature", id="x", geometry=None, properties={}, **extra)
    result = normalize_feature_attributes(feat)
    # Mutate-in-place: same object returned.
    assert result is feat
    assert (result.properties or {}) == {"adm0_name": "Italy", "code": "1616"}
    # Lifted keys must not remain in model_extra — no duplication.
    assert (result.model_extra or {}) == {}


def test_normalize_existing_properties_win_on_collision():
    """When properties already contains a key that is also in model_extra,
    the explicit property value wins (properties override extras)."""
    extra = {"code": "FROM_EXTRA"}
    feat = Feature(
        type="Feature", id="y", geometry=None,
        properties={"code": "EXPLICIT"},
        **extra,
    )
    result = normalize_feature_attributes(feat)
    assert result is feat
    assert (result.properties or {})["code"] == "EXPLICIT"


def test_normalize_idempotent_empty_extra():
    """A feature with no model_extra is returned unchanged (same object)."""
    feat = Feature(type="Feature", id="z", geometry=None, properties={"a": 1})
    result = normalize_feature_attributes(feat)
    # Same identity when there is nothing to lift.
    assert result is feat


def test_normalize_idempotent_second_pass():
    """Calling normalize twice produces the same result as calling it once."""
    extra = {"score": 42}
    feat = Feature(type="Feature", id="w", geometry=None, properties={}, **extra)
    once = normalize_feature_attributes(feat)
    twice = normalize_feature_attributes(once)
    # Both calls return the same object.
    assert once is feat
    assert twice is feat
    assert (once.properties or {}) == {"score": 42}
    assert (twice.properties or {}) == {"score": 42}
    assert (twice.model_extra or {}) == {}


def test_normalize_preserves_all_fields():
    """bbox, links, geometry and id are faithfully preserved on the same object.

    bbox is a declared pydantic field on Feature so it stays in .bbox (pydantic
    coerces list→tuple); extra_attr is an unknown key so it lands in model_extra
    and gets lifted into .properties by normalize_feature_attributes.
    """
    from dynastore.models.shared_models import Link
    link = Link(href="http://example.com", rel="self")
    # bbox is a declared field — pass directly, not via extra spread.
    # extra_attr is an unknown key — use dict-spread so pyright does not flag it.
    feat = Feature(
        type="Feature",
        id="abc",
        geometry={"type": "Point", "coordinates": [12.0, 41.0]},
        bbox=[12.0, 41.0, 12.0, 41.0],
        links=[link],
        properties={},
        **{"extra_attr": "kept"},
    )
    result = normalize_feature_attributes(feat)
    assert result is feat
    assert result.id == "abc"
    # geometry is parsed by geojson_pydantic into a typed object; compare by
    # round-tripping to dict so the assertion is model-version agnostic.
    assert result.geometry == feat.geometry
    # bbox is a declared field; pydantic coerces list to tuple on assignment.
    assert tuple(result.bbox) == (12.0, 41.0, 12.0, 41.0)  # type: ignore[arg-type]
    assert result.links == [link]
    assert (result.properties or {}).get("extra_attr") == "kept"


# ---------------------------------------------------------------------------
# stream_normalized_items — #1818 regression test
# ---------------------------------------------------------------------------

def _make_items_svc_with_model_extra(features):
    """Return a fake ItemsProtocol whose stream_items yields features with
    attributes only in model_extra (simulating the PG read path)."""

    async def _gen():
        for f in features:
            yield f

    async def _stream_items(
        catalog_id, collection_id, request,
        config=None, ctx=None, consumer=None, hints=frozenset(),
    ):
        resp = MagicMock()
        resp.items = _gen()
        return resp

    svc = MagicMock()
    svc.stream_items = _stream_items
    return svc


@pytest.mark.asyncio
async def test_stream_normalized_items_exposes_model_extra_as_properties():
    """Regression test for #1818: features coming from the PG driver with
    join-column values only in model_extra must be visible in .properties
    after passing through stream_normalized_items."""
    pg_features = [
        # PG path: properties={}, join columns in model_extra
        Feature(type="Feature", id="f1", geometry=None, properties={},
                **{"adm0_name": "Italy", "code": "1616"}),
        Feature(type="Feature", id="f2", geometry=None, properties={},
                **{"adm0_name": "France", "code": "1250"}),
    ]
    svc = _make_items_svc_with_model_extra(pg_features)

    from dynastore.models.query_builder import QueryRequest, FieldSelection
    req = QueryRequest(select=[FieldSelection(field="*")])

    results = []
    async for feat in stream_normalized_items(svc, "cat", "coll", req):
        results.append(feat)

    assert len(results) == 2
    # Properties must now contain what was in model_extra — the #1818 fix.
    assert (results[0].properties or {}) == {"adm0_name": "Italy", "code": "1616"}
    assert (results[1].properties or {}) == {"adm0_name": "France", "code": "1250"}
    # Extras must be cleared (not duplicated at both levels).
    assert (results[0].model_extra or {}) == {}
    assert (results[1].model_extra or {}) == {}


@pytest.mark.asyncio
async def test_stream_normalized_items_es_path_unchanged():
    """ES-path features already have properties populated; normalization is
    a no-op and properties values are preserved exactly."""
    es_features = [
        Feature(type="Feature", id="e1", geometry=None,
                properties={"adm0_name": "Spain", "code": "0724"}),
    ]
    svc = _make_items_svc_with_model_extra(es_features)

    from dynastore.models.query_builder import QueryRequest, FieldSelection
    req = QueryRequest(select=[FieldSelection(field="*")])

    results = []
    async for feat in stream_normalized_items(svc, "cat", "coll", req):
        results.append(feat)

    assert len(results) == 1
    assert (results[0].properties or {}) == {"adm0_name": "Spain", "code": "0724"}


# ---------------------------------------------------------------------------
# resolve_join_value — #1827
# ---------------------------------------------------------------------------


def _feat_with_system(fid, system_dict=None, props=None):
    """Build a Feature with a system foreign-member section in model_extra."""
    f = Feature(type="Feature", id=fid, geometry=None, properties=props or {})
    if system_dict and f.__pydantic_extra__ is not None:
        f.__pydantic_extra__["system"] = system_dict
    return f


def _feat_with_system_in_props(fid, system_dict, props=None):
    """Build a Feature with system dict merged into properties (post-normalization)."""
    p = dict(props or {})
    p["system"] = system_dict
    return Feature(type="Feature", id=fid, geometry=None, properties=p)


# -- join_source="properties" (default behavior, back-compat) ---------------

def test_resolve_join_value_properties_flat():
    """Default source: returns value from flat feature.properties."""
    f = _feat("x", props={"CODE": "IT"})
    assert resolve_join_value(f, "CODE") == "IT"
    assert resolve_join_value(f, "CODE", "properties") == "IT"


def test_resolve_join_value_properties_falls_back_to_feature_id():
    """Default source: absent key falls back to feature.id."""
    f = _feat("uuid-1", props={})
    assert resolve_join_value(f, "geoid") == "uuid-1"


def test_resolve_join_value_properties_explicit_none_returns_none():
    """Default source: an explicit None in properties is returned (not skipped)."""
    f = _feat("x", props={"CODE": None})
    assert resolve_join_value(f, "CODE") is None


# -- join_source="system" ---------------------------------------------------

def test_resolve_join_value_system_reads_from_model_extra_system():
    """system source: reads from model_extra["system"][key]."""
    f = _feat_with_system("uuid-1", system_dict={"external_id": "EXT-001"})
    assert resolve_join_value(f, "external_id", "system") == "EXT-001"


def test_resolve_join_value_system_reads_from_props_system():
    """system source: also finds key in properties["system"] (post-normalization)."""
    f = _feat_with_system_in_props("uuid-2", system_dict={"external_id": "EXT-002"})
    assert resolve_join_value(f, "external_id", "system") == "EXT-002"


def test_resolve_join_value_system_returns_none_when_key_absent():
    """system source: returns None when key is not in system section — no feature.id leak."""
    f = _feat_with_system("uuid-3", system_dict={"geoid": "uuid-3"})
    assert resolve_join_value(f, "external_id", "system") is None


def test_resolve_join_value_system_returns_none_when_section_absent():
    """system source: returns None when feature has no system section at all."""
    f = _feat("uuid-4", props={"external_id": "PROP-EID"})
    assert resolve_join_value(f, "external_id", "system") is None


def test_resolve_join_value_system_does_not_read_flat_property():
    """system source: a property named 'external_id' is NOT returned when
    join_source='system' — this is the whole point of join_source disambiguation."""
    # Feature has external_id ONLY in flat properties (not in system section).
    f = _feat("uuid-5", props={"external_id": "FROM_PROPERTY"})
    result = resolve_join_value(f, "external_id", "system")
    # Must be None; the flat property must NOT be picked up by the system source.
    assert result is None


def test_resolve_join_value_system_prefers_section_over_flat_property():
    """When both flat property and system section contain the key, system source
    returns the section value, not the flat property value."""
    f = _feat_with_system_in_props(
        "uuid-6",
        system_dict={"external_id": "SYSTEM-VAL"},
        props={"external_id": "PROP-VAL"},
    )
    # The system section value wins because we look in properties["system"] first.
    assert resolve_join_value(f, "external_id", "system") == "SYSTEM-VAL"


# -- join_source="stats" ----------------------------------------------------

def test_resolve_join_value_stats_reads_from_model_extra_stats():
    """stats source: reads from model_extra["stats"][key]."""
    f = Feature(type="Feature", id="s1", geometry=None, properties={})
    if f.__pydantic_extra__ is not None:
        f.__pydantic_extra__["stats"] = {"area_ha": 42.5}
    assert resolve_join_value(f, "area_ha", "stats") == 42.5


def test_resolve_join_value_stats_returns_none_when_absent():
    """stats source: returns None (no feature.id fallback)."""
    f = _feat("s2", props={"area_ha": 10.0})
    assert resolve_join_value(f, "area_ha", "stats") is None


# ---------------------------------------------------------------------------
# stream_join_features — shared streaming-join primitive (#1835)
# ---------------------------------------------------------------------------


async def _astream(items):
    for it in items:
        yield it


@pytest.mark.asyncio
async def test_stream_join_features_inner_join_drops_unmatched():
    """Default inner_join=True: features with no secondary match are dropped."""
    primary = _astream([
        _feat("f1", props={"code": "A", "name": "Alpha"}),
        _feat("f2", props={"code": "B", "name": "Beta"}),
        _feat("f3", props={"code": "MISSING"}),
    ])
    secondary = {
        "A": {"code": "A", "score": 1},
        "B": {"code": "B", "score": 2},
    }
    out = [f async for f in stream_join_features(primary, secondary, "code")]
    assert [f.id for f in out] == ["f1", "f2"]
    assert out[0].properties["score"] == 1
    assert out[0].properties["name"] == "Alpha"


@pytest.mark.asyncio
async def test_stream_join_features_left_join_passes_unmatched():
    """inner_join=False: features without a match are yielded unmodified."""
    primary = _astream([
        _feat("f1", props={"code": "A"}),
        _feat("f2", props={"code": "NOMATCH"}),
    ])
    secondary = {"A": {"code": "A", "score": 10}}
    out = [f async for f in stream_join_features(primary, secondary, "code", inner_join=False)]
    assert [f.id for f in out] == ["f1", "f2"]
    assert out[0].properties["score"] == 10
    # Unmatched feature keeps only its original properties.
    assert "score" not in out[1].properties
    assert out[1].properties["code"] == "NOMATCH"


@pytest.mark.asyncio
async def test_stream_join_features_secondary_wins_on_collision():
    """When both sides have the same key, secondary value wins (enrichment overrides)."""
    primary = _astream([_feat("f1", props={"code": "X", "label": "primary-label"})])
    secondary = {"X": {"code": "X", "label": "secondary-label", "extra": "bonus"}}
    out = [f async for f in stream_join_features(primary, secondary, "code")]
    assert len(out) == 1
    assert out[0].properties["label"] == "secondary-label"
    assert out[0].properties["extra"] == "bonus"


@pytest.mark.asyncio
async def test_stream_join_features_preserves_geometry():
    """Geometry is carried through unchanged on matched features."""
    geo = {"type": "Point", "coordinates": [12.0, 41.0]}
    primary = _astream([
        Feature(type="Feature", id="f1", geometry=geo, properties={"code": "IT"}),
    ])
    secondary = {"IT": {"score": 5}}
    out = [f async for f in stream_join_features(primary, secondary, "code")]
    assert len(out) == 1
    # geometry round-trips via geojson_pydantic; compare via model_dump
    assert out[0].geometry is not None


@pytest.mark.asyncio
async def test_stream_join_features_none_key_not_matched():
    """A feature whose join key resolves to None is never matched (dropped on inner join)."""
    primary = _astream([
        _feat("f1", props={"code": None}),    # explicit None → no match
        _feat("f2", props={}),                 # absent key → falls back to feat.id
    ])
    secondary = {"f2": {"from_id": True}}
    out = [f async for f in stream_join_features(primary, secondary, "code")]
    # f1 has explicit None → dropped; f2's id "f2" matches the secondary.
    assert [f.id for f in out] == ["f2"]
    assert out[0].properties["from_id"] is True


@pytest.mark.asyncio
async def test_stream_join_features_join_source_system():
    """join_source='system' resolves the key from the system foreign-member section."""
    f = Feature(type="Feature", id="uuid-1", geometry=None, properties={"name": "Italy"})
    if f.__pydantic_extra__ is not None:
        f.__pydantic_extra__["system"] = {"external_id": "EXT-001"}
    primary = _astream([f])
    secondary = {"EXT-001": {"country_code": "IT"}}
    out = [f async for f in stream_join_features(
        primary, secondary, "external_id", join_source="system"
    )]
    assert len(out) == 1
    assert out[0].properties["country_code"] == "IT"
    assert out[0].properties["name"] == "Italy"


@pytest.mark.asyncio
async def test_stream_join_features_empty_secondary_drops_all_on_inner_join():
    """No secondary rows → all features dropped (inner join with empty index)."""
    primary = _astream([_feat("f1", props={"code": "A"})])
    out = [f async for f in stream_join_features(primary, {}, "code")]
    assert out == []


@pytest.mark.asyncio
async def test_stream_join_features_empty_primary_yields_nothing():
    """Empty primary stream → nothing yielded regardless of secondary."""
    async def empty():
        return
        yield  # make it an async generator

    secondary = {"A": {"score": 1}}
    out = [f async for f in stream_join_features(empty(), secondary, "code")]
    assert out == []
