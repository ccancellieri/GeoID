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

"""Unit tests for STAC collection extent/license/stac_extensions round-trip (#361).

Covers the data-loss cluster confirmed live:
  1. extent.spatial.bbox → [0,0,0,0]  (write-side: extent not in extra_metadata;
                                         read-side: no fallback to extra_metadata)
  2. extent.temporal.interval → [null,null]  (same path + half-open guard bug)
  3. license → "proprietary"  (localize-then-fallback ordering bug)
  4. stac_extensions → only language URI  (contributors replace instead of union)

No live DB or HTTP stack required — all tests exercise pure functions.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_collection(
    extra_fields: Optional[Dict[str, Any]] = None,
    license: str = "proprietary",
    providers=None,
    stac_extensions: Optional[List[str]] = None,
):
    """Build a minimal pystac.Collection with optional pre-populated extra_fields."""
    import pystac

    coll = pystac.Collection(
        id="test",
        description="test",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        license=license,
        stac_extensions=list(stac_extensions or []),
    )
    if providers is not None:
        coll.providers = providers
    if extra_fields:
        coll.extra_fields.update(extra_fields)
    return coll


# ---------------------------------------------------------------------------
# 1. Write-path: _pack_stac_extras folds extent + stac_extensions
# ---------------------------------------------------------------------------


def test_pack_stac_extras_folds_extent_into_extra_metadata():
    """extent must be stored in extra_metadata when sidecar is absent."""
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    extent = {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    }
    input_data = {
        "id": "my-coll",
        "type": "Collection",
        "description": "desc",
        "extent": extent,
        "license": "CC-BY-4.0",
    }
    out = _pack_stac_extras(dict(input_data), "en")
    em = out.get("extra_metadata", {})
    assert "extent" in em, "extent must be folded into extra_metadata"
    assert em["extent"] == extent


def test_pack_stac_extras_folds_stac_extensions_into_extra_metadata():
    """stac_extensions must be stored in extra_metadata when sidecar is absent."""
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    exts = ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"]
    input_data = {
        "id": "my-coll",
        "type": "Collection",
        "description": "desc",
        "extent": {},
        "license": "CC-BY-4.0",
        "stac_extensions": exts,
    }
    out = _pack_stac_extras(dict(input_data), "en")
    em = out.get("extra_metadata", {})
    assert "stac_extensions" in em, "stac_extensions must be folded into extra_metadata"
    assert em["stac_extensions"] == exts


def test_pack_stac_extras_preserves_extent_at_top_level():
    """extent stays at top level (for the collection_stac sidecar path)."""
    from dynastore.extensions.stac.stac_service import _pack_stac_extras

    extent = {"spatial": {"bbox": [[-180, -90, 180, 90]]}}
    input_data = {
        "id": "x",
        "type": "Collection",
        "description": "d",
        "extent": extent,
        "license": "CC-BY-4.0",
    }
    out = _pack_stac_extras(dict(input_data), "en")
    assert out.get("extent") == extent, "extent must remain at top level for sidecar path"


# ---------------------------------------------------------------------------
# 2. Read-path: stac_generator restores extent from extra_metadata fallback
# ---------------------------------------------------------------------------


def test_restore_extent_from_extra_metadata_fallback():
    """When metadata_model.extent is None, extent is read from extra_metadata."""
    from dynastore.extensions.stac.stac_generator import _restore_extent_from_extras

    stored_extent = {
        "spatial": {"bbox": [[-10, -20, 10, 20]]},
        "temporal": {"interval": [["2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z"]]},
    }
    spatial, temporal = _restore_extent_from_extras(stored_extent)
    assert spatial is not None
    assert temporal is not None
    # spatial bbox round-trips
    assert spatial.bboxes[0] == [-10, -20, 10, 20]
    # temporal interval dates
    intervals = temporal.intervals
    assert len(intervals) == 1
    start, end = intervals[0]
    assert start is not None
    assert end is not None


def test_restore_extent_half_open_start_only():
    """Half-open interval [start, None] must survive — only start is set."""
    from dynastore.extensions.stac.stac_generator import _restore_extent_from_extras

    stored_extent = {
        "spatial": {"bbox": [[0, 0, 1, 1]]},
        "temporal": {"interval": [["2021-06-01T00:00:00Z", None]]},
    }
    spatial, temporal = _restore_extent_from_extras(stored_extent)
    assert temporal is not None
    start, end = temporal.intervals[0]
    assert start is not None, "start must be preserved in half-open [start, None]"
    assert end is None, "end must remain None in half-open [start, None]"


def test_restore_extent_half_open_end_only():
    """Half-open interval [None, end] must survive."""
    from dynastore.extensions.stac.stac_generator import _restore_extent_from_extras

    stored_extent = {
        "spatial": {"bbox": [[0, 0, 1, 1]]},
        "temporal": {"interval": [[None, "2022-12-31T00:00:00Z"]]},
    }
    _, temporal = _restore_extent_from_extras(stored_extent)
    assert temporal is not None
    start, end = temporal.intervals[0]
    assert start is None
    assert end is not None


def test_restore_extent_null_interval_yields_null_null():
    """A [null, null] stored interval maps to [None, None] — open interval."""
    from dynastore.extensions.stac.stac_generator import _restore_extent_from_extras

    stored_extent = {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[None, None]]},
    }
    _, temporal = _restore_extent_from_extras(stored_extent)
    assert temporal is not None
    start, end = temporal.intervals[0]
    assert start is None
    assert end is None


def test_restore_extent_returns_none_when_extra_absent():
    """Returns (None, None) when no extent is stored in extra_metadata."""
    from dynastore.extensions.stac.stac_generator import _restore_extent_from_extras

    assert _restore_extent_from_extras(None) == (None, None)
    assert _restore_extent_from_extras({}) == (None, None)
    assert _restore_extent_from_extras({"other": "field"}) == (None, None)


# ---------------------------------------------------------------------------
# 3. Read-path: existing half-open interval guard fixed in create_collection
# ---------------------------------------------------------------------------


def test_half_open_interval_preserved_by_create_extent():
    """Half-open intervals [start, None] must not be collapsed to [None, None]."""
    from dynastore.extensions.stac.stac_generator import _build_temporal_extent

    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    temporal = _build_temporal_extent(start, None)
    assert temporal is not None
    s, e = temporal.intervals[0]
    assert s == start
    assert e is None


def test_closed_interval_preserved_by_create_extent():
    """Closed interval [start, end] must round-trip correctly."""
    from dynastore.extensions.stac.stac_generator import _build_temporal_extent

    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2021, 1, 1, tzinfo=timezone.utc)
    temporal = _build_temporal_extent(start, end)
    s, e = temporal.intervals[0]
    assert s == start
    assert e == end


# ---------------------------------------------------------------------------
# 4. Read-path: license falls back to stored value before "proprietary"
# ---------------------------------------------------------------------------


def test_resolve_collection_license_uses_stored_value():
    """When localize returns empty, the stored raw license must be used."""
    from dynastore.extensions.stac.stac_generator import _resolve_collection_license

    # Localization returns None (e.g. language mismatch), but stored value exists
    assert _resolve_collection_license(None, "CC-BY-4.0") == "CC-BY-4.0"


def test_resolve_collection_license_prefers_localized():
    """Localized value takes priority over stored."""
    from dynastore.extensions.stac.stac_generator import _resolve_collection_license

    assert _resolve_collection_license("Apache-2.0", "CC-BY-4.0") == "Apache-2.0"


def test_resolve_collection_license_falls_back_to_proprietary_only_when_nothing():
    """Only truly absent license falls back to 'proprietary'."""
    from dynastore.extensions.stac.stac_generator import _resolve_collection_license

    assert _resolve_collection_license(None, None) == "proprietary"
    assert _resolve_collection_license("", "") == "proprietary"


# ---------------------------------------------------------------------------
# 5. Read-path: stac_extensions — union of stored + contributor additions
# ---------------------------------------------------------------------------


def test_apply_extra_stac_extensions_seeds_from_stored():
    """stac_extensions from extra_metadata must seed the collection's list."""
    from dynastore.extensions.stac.stac_generator import _apply_stac_extensions_from_extras

    coll = _make_collection(stac_extensions=["https://example.com/contrib/v1.0/schema.json"])
    stored = ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"]
    _apply_stac_extensions_from_extras(coll, stored)
    # Both the already-added contributor URI and the stored EO URI must be present
    assert "https://example.com/contrib/v1.0/schema.json" in coll.stac_extensions
    assert "https://stac-extensions.github.io/eo/v1.1.0/schema.json" in coll.stac_extensions


def test_apply_extra_stac_extensions_no_duplicates():
    """If stored URI is already in the list, no duplicate is added."""
    from dynastore.extensions.stac.stac_generator import _apply_stac_extensions_from_extras

    existing_uri = "https://stac-extensions.github.io/eo/v1.1.0/schema.json"
    coll = _make_collection(stac_extensions=[existing_uri])
    _apply_stac_extensions_from_extras(coll, [existing_uri])
    assert coll.stac_extensions.count(existing_uri) == 1


def test_apply_extra_stac_extensions_noop_on_empty():
    """No error and no change when stored list is empty or None."""
    from dynastore.extensions.stac.stac_generator import _apply_stac_extensions_from_extras

    coll = _make_collection(stac_extensions=["https://example.com/v1.0/schema.json"])
    _apply_stac_extensions_from_extras(coll, [])
    _apply_stac_extensions_from_extras(coll, None)
    assert coll.stac_extensions == ["https://example.com/v1.0/schema.json"]


# ---------------------------------------------------------------------------
# 6. Item bbox: geometries sidecar derives bbox from shapely_geom.bounds
# ---------------------------------------------------------------------------


def test_geometries_sidecar_derives_bbox_from_shapely_bounds():
    """When bbox_coords is None, bbox must be derived from shapely_geom.bounds."""
    import pytest
    pytest.importorskip("shapely")

    from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
        _derive_bbox_from_shapely,
    )
    from shapely.geometry import box as shapely_box

    geom = shapely_box(10.0, 20.0, 30.0, 40.0)
    result = _derive_bbox_from_shapely(geom)
    assert result is not None
    xmin, ymin, xmax, ymax = result
    assert xmin == pytest.approx(10.0)
    assert ymin == pytest.approx(20.0)
    assert xmax == pytest.approx(30.0)
    assert ymax == pytest.approx(40.0)


def test_geometries_sidecar_derive_bbox_returns_none_on_error():
    """_derive_bbox_from_shapely must not raise — return None on failure."""
    import pytest
    pytest.importorskip("shapely")

    from dynastore.modules.storage.drivers.pg_sidecars.geometries import (
        _derive_bbox_from_shapely,
    )

    assert _derive_bbox_from_shapely(None) is None  # type: ignore[arg-type]
