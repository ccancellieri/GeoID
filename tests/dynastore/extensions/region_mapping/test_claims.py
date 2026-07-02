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

"""DB-free unit tests for ``claims`` -- the region_mapping extension's
claim-computation kernel + source-collection read helpers (dynastore#2821).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# compute_claim_set
# ---------------------------------------------------------------------------


def test_compute_claim_set_default_alias_is_column() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias=None, extra_aliases=[],
    )

    # column == canonical_alias -> {column, "fao_column"} = 2 distinct claims.
    values = {claim for claim, _role in claims.values()}
    assert values == {"adm0_code", "fao_adm0_code"}


def test_compute_claim_set_explicit_alias_marks_primary() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="country", extra_aliases=["adm0"],
    )

    roles_by_claim = {claim: role for claim, role in claims.values()}
    assert roles_by_claim["country"] == "primary"
    assert roles_by_claim["adm0_code"] == "alias"
    assert roles_by_claim["adm0"] == "alias"
    assert roles_by_claim["fao_country"] == "alias"
    assert len(roles_by_claim) == 4


def test_compute_claim_set_casefold_dedup() -> None:
    """Two candidates differing only by case collapse to one claim record."""
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="Country", alias="country", extra_aliases=[],
    )

    assert len(claims) == 2
    assert "country" in claims  # casefolded key


def test_compute_claim_set_exactly_one_primary() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="country", extra_aliases=["adm0", "iso3"],
    )

    primaries = [claim for claim, role in claims.values() if role == "primary"]
    assert primaries == ["country"]


def test_compute_claim_set_rejects_regex_metacharacters() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(
            catalog_id="fao", collection_id="countries",
            column="adm0.code", alias=None, extra_aliases=[],
        )


def test_compute_claim_set_rejects_regex_metacharacters_in_extra_alias() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(
            catalog_id="fao", collection_id="countries",
            column="adm0", alias="country", extra_aliases=["adm(0)"],
        )


def test_validate_claim_text_accepts_plain_literal() -> None:
    from dynastore.extensions.region_mapping.claims import validate_claim_text

    validate_claim_text("adm0_code")  # must not raise


# ---------------------------------------------------------------------------
# mapping_id_for / slugify
# ---------------------------------------------------------------------------


def test_mapping_id_for_slugifies() -> None:
    from dynastore.extensions.region_mapping.claims import mapping_id_for

    assert mapping_id_for("FAO Catalog", "Country Boundaries!") == "fao_catalog_country_boundaries"


# ---------------------------------------------------------------------------
# is_degenerate_bbox
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bbox,expected",
    [
        (None, True),
        ([], True),
        ([0.0, 0.0, 0.0], True),  # too short
        ([0.0, 0.0, 0.0, 0.0], True),  # zero-area
        ([10.0, 10.0, 10.0, 20.0], True),  # zero-width
        ([-180.0, -90.0, 180.0, 90.0], False),
        ([10.0, 20.0, 30.0, 40.0], False),
    ],
)
def test_is_degenerate_bbox(bbox, expected) -> None:
    from dynastore.extensions.region_mapping.claims import is_degenerate_bbox

    assert is_degenerate_bbox(bbox) is expected


# ---------------------------------------------------------------------------
# fetch_collection_bbox / fetch_distinct_region_ids -- source-collection
# reads via CatalogsProtocol (unrelated to registry persistence).
# ---------------------------------------------------------------------------


def _feature(properties: Dict[str, Any]) -> MagicMock:
    f = MagicMock()
    f.properties = properties
    return f


class _StubCatalogs:
    def __init__(
        self,
        region_id_pages: Optional[List[List[str]]] = None,
        collection_extent_bbox: Optional[List[float]] = None,
    ) -> None:
        self._region_id_pages = region_id_pages or []
        self._collection_extent_bbox = collection_extent_bbox

    async def search_items(self, catalog_id: str, collection_id: str, request: Any) -> List[MagicMock]:
        offset = request.offset or 0
        page_index = offset // (request.limit or 1)
        if page_index >= len(self._region_id_pages):
            return []
        return [_feature({request.select[0].field: v}) for v in self._region_id_pages[page_index]]

    async def get_collection(self, catalog_id: str, collection_id: str) -> Optional[MagicMock]:
        collection = MagicMock()
        if self._collection_extent_bbox is None:
            collection.extent = None
        else:
            collection.extent.spatial.bbox = [self._collection_extent_bbox]
        return collection


@pytest.fixture(autouse=True)
def _reset_caches():
    from dynastore.extensions.region_mapping.claims import fetch_collection_bbox, fetch_distinct_region_ids
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
    yield
    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)


@pytest.mark.asyncio
async def test_fetch_collection_bbox_returns_extent(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs(collection_extent_bbox=[10.0, 20.0, 30.0, 40.0])
    monkeypatch.setattr(claims, "get_protocol", lambda _t: catalogs)

    bbox = await claims.fetch_collection_bbox("fao", "countries")
    assert bbox == [10.0, 20.0, 30.0, 40.0]


@pytest.mark.asyncio
async def test_fetch_collection_bbox_falls_back_to_world_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs(collection_extent_bbox=None)
    monkeypatch.setattr(claims, "get_protocol", lambda _t: catalogs)

    bbox = await claims.fetch_collection_bbox("fao", "countries")
    assert bbox == list(claims.WORLD_BBOX)


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_sorted_and_deduped(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs(region_id_pages=[["ITA", "FRA", "ITA", "DEU"]])
    monkeypatch.setattr(claims, "get_protocol", lambda _t: catalogs)

    values = await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    assert values == ["DEU", "FRA", "ITA"]


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_no_catalogs_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    from dynastore.extensions.region_mapping import claims

    monkeypatch.setattr(claims, "get_protocol", lambda _t: None)

    values = await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    assert values == []
