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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for WFS GetFeature JSON wire-shape contract.

Three regressions introduced by the canonical-envelope refactor:

Bug 2 -- system/stats/access foreign-member leakage
    Each serialised GeoJSON Feature must contain only RFC 7946 top-level
    members (``type``, ``id``, ``geometry``, ``properties``, ``bbox``,
    ``links``). Internal sections injected into ``Feature.__pydantic_extra__``
    by ``_apply_expose_all_sections`` or the sidecar bridge (``system``,
    ``stats``, ``access``, ...) must be stripped before the bytes leave the
    process.  The serialiser uses ``model_dump(by_alias=True)`` which
    includes ``__pydantic_extra__`` verbatim, so the strip must happen
    before or during serialisation.

Bug 3 -- empty FeatureCollection when PG schema missing
    WFS GetFeature passes ``EXACT_READ_HINTS`` so the PG driver (which
    declares ``Hint.GEOMETRY_EXACT``) is preferred.  When the PG schema has
    not been provisioned yet, ``SchemaNotFoundError`` is raised.  The current
    code returns an empty FeatureCollection; the correct behaviour is to
    retry without hints so the ES driver can serve the request.

All tests are pure unit tests -- no DB, no ES, no HTTP server.
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, List

import pytest

pytest.importorskip("pyproj")  # SCOPE gate: wfs_service imports pyproj at module load


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_POINT_GEOM = {"type": "Point", "coordinates": [12.0, 41.0]}


async def _drain_stream(stream: AsyncIterator[bytes]) -> bytes:
    """Consume an async byte iterator into a single bytes value."""
    chunks: List[bytes] = []
    async for chunk in stream:
        chunks.append(chunk)
    return b"".join(chunks)


# ---------------------------------------------------------------------------
# 1.  ES _source -> Feature  (smoke-check: ES path is already clean)
# ---------------------------------------------------------------------------


def test_es_source_to_feature_strips_system_stats_access() -> None:
    """``_es_source_to_feature`` must return a Feature with no ``system`` /
    ``stats`` / ``access`` / ``attributes`` top-level keys.

    The canonical ES ``_source`` carries those sections but the read
    reconstruction (``unproject_item_from_es``) must drop them via the
    ``_RESERVED_MEMBER_KEYS`` allowlist.  This is a sanity check that the
    ES path is already clean -- not a regression for the current bugs.
    """
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )

    source: Dict[str, Any] = {
        "id": "geoid-abc123",
        "catalog_id": "cat1",
        "collection_id": "col1",
        "collection": "col1",
        "external_id": "ext-001",
        "geometry": _POINT_GEOM,
        "properties": {"CODE": "IT", "NAME": "Italy"},
        "system": {"geometry_hash": "sha256:abc"},
        "stats": {"area": 301_338.0},
        "access": {"roles": ["admin"]},
    }
    feature = ItemsElasticsearchDriver._es_source_to_feature(source)
    dumped = feature.model_dump(exclude_none=True, by_alias=True)

    assert dumped.get("type") == "Feature"
    assert "properties" in dumped
    assert dumped["properties"].get("CODE") == "IT"
    assert "geometry" in dumped

    assert "system" not in dumped
    assert "stats" not in dumped
    assert "access" not in dumped
    assert "catalog_id" not in dumped
    assert "collection_id" not in dumped
    assert "attributes" not in dumped


# ---------------------------------------------------------------------------
# 2.  _stream_ogc_json serialisation  (Bug 2: foreign-member leakage)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_wfs_json_strips_extra_foreign_members() -> None:
    """WFS GeoJSON output must NOT carry non-RFC-7946 foreign members.

    ``Feature`` uses ``extra="allow"`` so ``model_dump`` includes any key
    injected into ``__pydantic_extra__``.  Internal sidecar data (system,
    stats, access) ends up there via ``_apply_expose_all_sections`` and
    currently leaks onto the wire.

    The WFS layer strips those keys via ``_strip_wfs_foreign_members`` before
    passing items to ``stream_ogc_features`` / ``_stream_ogc_json``.  The
    emitted Feature must contain only: ``type``, ``id``, ``geometry``,
    ``properties``, and optionally ``bbox`` / ``links``.

    FAILS currently: ``_strip_wfs_foreign_members`` does not exist yet.
    """
    from dynastore.extensions.tools.formatters import (
        OGCResponseMetadata,
        OutputFormatEnum,
        _stream_ogc_json,
    )
    from dynastore.extensions.wfs.wfs_service import _strip_wfs_foreign_members
    from dynastore.models.ogc import Feature

    feat = Feature(
        type="Feature",
        id="f1",
        geometry=_POINT_GEOM,
        properties={"CODE": "IT"},
    )
    # Simulate what _apply_expose_all_sections or the sidecar bridge puts in.
    assert feat.__pydantic_extra__ is not None
    feat.__pydantic_extra__["system"] = {"geometry_hash": "sha256:abc"}
    feat.__pydantic_extra__["stats"] = {"area": 301_338.0}
    feat.__pydantic_extra__["access"] = {"roles": ["admin"]}

    async def _polluted() -> AsyncIterator[Feature]:
        yield feat

    # Apply the WFS strip (as get_feature does before stream_ogc_features).
    async def _cleaned() -> AsyncIterator[Feature]:
        async for item in _strip_wfs_foreign_members(_polluted()):
            yield item

    raw = await _drain_stream(
        _stream_ogc_json(_cleaned(), OGCResponseMetadata(), OutputFormatEnum.GEOJSON)
    )
    body = json.loads(raw)
    assert body["type"] == "FeatureCollection"
    assert len(body["features"]) == 1
    wire_feat = body["features"][0]

    # RFC 7946 contract.
    assert wire_feat.get("type") == "Feature"
    assert "properties" in wire_feat
    assert wire_feat["properties"].get("CODE") == "IT"
    assert "geometry" in wire_feat

    # These must NOT appear as top-level Feature members.
    assert "system" not in wire_feat, f"system leaked: {wire_feat.get('system')}"
    assert "stats" not in wire_feat, f"stats leaked: {wire_feat.get('stats')}"
    assert "access" not in wire_feat, f"access leaked: {wire_feat.get('access')}"


@pytest.mark.asyncio
async def test_wfs_json_geometry_present_as_null_when_skipped() -> None:
    """RFC 7946 section 3.2: a Feature MUST have a ``geometry`` member, even when
    it is ``null`` (an unlocated Feature).

    With ``propertyName=geoid`` the WFS layer sets ``skip_geometry=True``
    so the driver omits the geometry column; the Feature object has
    ``geometry=None``.  ``model_dump(exclude_none=True)`` drops the key;
    the ``_stream_ogc_json`` guard must put it back as ``null``.
    """
    from dynastore.extensions.tools.formatters import (
        OGCResponseMetadata,
        OutputFormatEnum,
        _stream_ogc_json,
    )
    from dynastore.models.ogc import Feature

    feat = Feature(type="Feature", id="f1", geometry=None, properties={"CODE": "IT"})

    async def _gen() -> AsyncIterator[Feature]:
        yield feat

    raw = await _drain_stream(
        _stream_ogc_json(_gen(), OGCResponseMetadata(), OutputFormatEnum.GEOJSON)
    )
    body = json.loads(raw)
    wire_feat = body["features"][0]

    assert "geometry" in wire_feat, "geometry member must always be present (RFC 7946)"
    assert wire_feat["geometry"] is None


@pytest.mark.asyncio
async def test_wfs_json_properties_only_no_attributes_container() -> None:
    """User attributes must appear under ``properties`` only; no ``attributes``
    sibling (legacy pre-canonical shape).
    """
    from dynastore.extensions.tools.formatters import (
        OGCResponseMetadata,
        OutputFormatEnum,
        _stream_ogc_json,
    )
    from dynastore.models.ogc import Feature

    feat = Feature(
        type="Feature",
        id="f1",
        geometry=_POINT_GEOM,
        properties={"CODE": "IT", "NAME": "Italy"},
    )

    async def _gen() -> AsyncIterator[Feature]:
        yield feat

    raw = await _drain_stream(
        _stream_ogc_json(_gen(), OGCResponseMetadata(), OutputFormatEnum.GEOJSON)
    )
    body = json.loads(raw)
    wire_feat = body["features"][0]

    assert "properties" in wire_feat
    assert wire_feat["properties"] == {"CODE": "IT", "NAME": "Italy"}
    assert "attributes" not in wire_feat


# ---------------------------------------------------------------------------
# 3.  ES fallback when PG schema missing  (Bug 3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_pg_or_es_fallback_retries_es_when_pg_schema_missing() -> None:
    """When PG raises ``SchemaNotFoundError`` the helper must retry with
    empty hints so the ES driver can serve the request.

    FAILS currently: the helper does not exist -- the retry is added by the
    fix.  After the fix this test verifies:
    1. ``stream_items`` is called twice: first with ``EXACT_READ_HINTS``,
       then with ``frozenset()`` (allowing ES).
    2. The result is the ES ``QueryResponse`` (not an empty one).
    """
    from dynastore.extensions.wfs.wfs_service import _query_pg_or_es_fallback
    from dynastore.modules.db_config.exceptions import SchemaNotFoundError
    from dynastore.modules.storage.hints import EXACT_READ_HINTS
    from dynastore.models.ogc import Feature
    from dynastore.models.query_builder import QueryResponse
    from dynastore.extensions.tools.query import parse_ogc_query_request

    es_feature = Feature(
        type="Feature",
        id="f-es-001",
        geometry=_POINT_GEOM,
        properties={"CODE": "IT"},
    )

    call_log: List[Any] = []

    class _FakeItemsSvc:
        async def stream_items(self, **kw: Any) -> QueryResponse:
            hints: frozenset = kw.get("hints", frozenset())
            call_log.append(frozenset(hints))
            if hints:
                raise SchemaNotFoundError("cat", "no pg schema")

            async def _gen() -> AsyncIterator[Feature]:
                yield es_feature

            return QueryResponse(
                items=_gen(),
                total_count=1,
                catalog_id="cat",
                collection_id="col",
            )

    request_obj = parse_ogc_query_request()

    result = await _query_pg_or_es_fallback(
        items_svc=_FakeItemsSvc(),
        catalog_id="cat",
        collection_id="col",
        request_obj=request_obj,
    )

    # Must have attempted PG (non-empty hints) then ES (empty hints).
    assert len(call_log) == 2, (
        f"Expected 2 stream_items calls (PG then ES), got {len(call_log)}: {call_log}"
    )
    assert EXACT_READ_HINTS.issubset(call_log[0]), (
        f"first call must carry EXACT_READ_HINTS; got {call_log[0]!r}"
    )
    assert not call_log[1], (
        f"second call (ES fallback) must carry empty hints; got {call_log[1]!r}"
    )

    features = [f async for f in result.items]
    assert len(features) == 1
    assert features[0].id == "f-es-001"


@pytest.mark.asyncio
async def test_query_pg_or_es_fallback_returns_empty_when_both_missing() -> None:
    """When both PG and ES raise ``SchemaNotFoundError`` the helper returns
    an empty ``QueryResponse`` rather than propagating the exception.
    """
    from dynastore.extensions.wfs.wfs_service import _query_pg_or_es_fallback
    from dynastore.modules.db_config.exceptions import SchemaNotFoundError
    from dynastore.models.query_builder import QueryResponse
    from dynastore.extensions.tools.query import parse_ogc_query_request

    class _NoDriversSvc:
        async def stream_items(self, **kw: Any) -> QueryResponse:
            raise SchemaNotFoundError("cat", "no schema anywhere")

    request_obj = parse_ogc_query_request()

    result = await _query_pg_or_es_fallback(
        items_svc=_NoDriversSvc(),
        catalog_id="cat",
        collection_id="col",
        request_obj=request_obj,
    )

    assert result.total_count == 0
    features = [f async for f in result.items]
    assert features == []
