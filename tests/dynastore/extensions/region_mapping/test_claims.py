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

from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# compute_claim_set
# ---------------------------------------------------------------------------


def test_compute_claim_set_alias_equal_to_column_is_still_one_primary() -> None:
    """``alias`` is required now (no column fallback), but a caller is free to
    pass the same value as ``column`` -- that must still collapse to the
    2-claim set it always did."""
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="adm0_code", extra_aliases=[],
    )

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


def test_compute_claim_set_column_casefold_equals_alias_has_one_primary() -> None:
    """Column and (defaulted-from-column) alias differing only by case must
    still produce exactly one primary row (dynastore#2821 regression): the
    role is decided case-insensitively, not by exact string equality, and
    the claim text kept is the first-seen candidate's spelling."""
    from dynastore.extensions.region_mapping.claims import ROLE_PRIMARY, compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="GAUL1_CODE", alias="gaul1_code", extra_aliases=[],
    )

    primaries = [
        (claim, role) for claim, role in claims.values() if role == ROLE_PRIMARY
    ]
    assert len(primaries) == 1
    # "GAUL1_CODE" is the first candidate built (column before canonical_alias),
    # so its spelling wins the slot even though role resolution is
    # case-insensitive.
    assert primaries[0][0] == "GAUL1_CODE"


def test_compute_claim_set_extra_alias_casefold_equals_canonical_has_one_primary() -> None:
    """An extra_alias that casefold-equals the canonical alias must not
    steal the primary slot nor create a second primary."""
    from dynastore.extensions.region_mapping.claims import ROLE_PRIMARY, compute_claim_set

    claims = compute_claim_set(
        catalog_id="fao", collection_id="countries",
        column="adm0_code", alias="Country", extra_aliases=["country"],
    )

    primaries = [claim for claim, role in claims.values() if role == ROLE_PRIMARY]
    assert primaries == ["Country"]


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
            column="adm0.code", alias="country", extra_aliases=[],
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
# reads (unrelated to registry persistence). ``fetch_distinct_region_ids``
# now issues a dedicated SQL query directly against the source collection's
# physical attributes table (bypassing CatalogsProtocol's item-query surface
# entirely), so only its guard clauses are unit-tested here; the query itself
# is covered by a real-PG integration test.
# ---------------------------------------------------------------------------


class _StubCatalogs:
    def __init__(
        self,
        collection_extent_bbox: Optional[List[float]] = None,
        physical_schema: Optional[str] = "cat_schema",
    ) -> None:
        self._collection_extent_bbox = collection_extent_bbox
        self._physical_schema = physical_schema

    async def get_collection(self, catalog_id: str, collection_id: str) -> Optional[MagicMock]:
        collection = MagicMock()
        if self._collection_extent_bbox is None:
            collection.extent = None
        else:
            collection.extent.spatial.bbox = [self._collection_extent_bbox]
        return collection

    async def resolve_physical_schema(self, catalog_id: str, **kwargs: Any) -> Optional[str]:
        return self._physical_schema


class _StubConfigs:
    def __init__(self, col_config: Any) -> None:
        self._col_config = col_config

    async def get_config(self, config_cls: Any, **kwargs: Any) -> Any:
        return self._col_config


def _protocol_router(*, catalogs: Any = None, configs: Any = None):
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol

    def _get(protocol_type: Any) -> Any:
        if protocol_type is CatalogsProtocol:
            return catalogs
        if protocol_type is ConfigsProtocol:
            return configs
        return None

    return _get


@pytest.fixture(autouse=True)
def _reset_caches():
    from dynastore.extensions.region_mapping.claims import (
        fetch_collection_bbox,
        fetch_distinct_region_ids,
        fetch_region_ids_by_unique_id,
    )
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
    cache_clear(fetch_region_ids_by_unique_id)
    yield
    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
    cache_clear(fetch_region_ids_by_unique_id)


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
async def test_fetch_collection_bbox_second_call_is_served_from_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second call for the same (catalog_id, collection_id) within the TTL
    must not re-invoke ``catalogs.get_collection`` -- that's the whole point
    of caching this lookup: region.json calls it once per registered mapping
    on every request, and ``get_collection`` is a heavyweight, multi-round-trip
    hydration."""
    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs(collection_extent_bbox=[10.0, 20.0, 30.0, 40.0])
    get_collection = AsyncMock(wraps=catalogs.get_collection)
    monkeypatch.setattr(catalogs, "get_collection", get_collection)
    monkeypatch.setattr(claims, "get_protocol", lambda _t: catalogs)

    first = await claims.fetch_collection_bbox("fao", "countries")
    second = await claims.fetch_collection_bbox("fao", "countries")

    assert first == second == [10.0, 20.0, 30.0, 40.0]
    get_collection.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_no_protocols_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims

    monkeypatch.setattr(claims, "get_protocol", _protocol_router())

    values = await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_no_physical_table_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    catalogs = _StubCatalogs()
    configs = _StubConfigs(ItemsPostgresqlDriverConfig())  # physical_table unset -> pending collection
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_no_attributes_sidecar_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    catalogs = _StubCatalogs()
    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[])
    configs = _StubConfigs(col_config)
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_columnar_rejects_undeclared_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A columnar collection whose ``attribute_schema`` doesn't declare the
    claimed property has nothing to read -- guard clause short-circuits
    before any SQL is built."""
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        AttributeStorageMode,
        FeatureAttributeSidecarConfig,
    )

    attrs = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[AttributeSchemaEntry(name="adm0_code")],
    )
    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[attrs])
    catalogs = _StubCatalogs()
    configs = _StubConfigs(col_config)
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_distinct_region_ids("fao", "countries", "not_declared")
    assert values == []


# ---------------------------------------------------------------------------
# fetch_region_ids_by_unique_id -- FID-ordered (not deduplicated/sorted)
# per-feature values, for TerriaJS's positional uniqueIdProp matching. Only
# guard clauses are unit-tested here; the query itself is covered by a real-PG
# integration test.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_no_protocols_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims

    monkeypatch.setattr(claims, "get_protocol", _protocol_router())

    values = await claims.fetch_region_ids_by_unique_id("fao", "countries", "adm0_code", "FID")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_no_physical_table_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    catalogs = _StubCatalogs()
    configs = _StubConfigs(ItemsPostgresqlDriverConfig())  # physical_table unset -> pending collection
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_region_ids_by_unique_id("fao", "countries", "adm0_code", "FID")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_no_attributes_sidecar_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    catalogs = _StubCatalogs()
    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[])
    configs = _StubConfigs(col_config)
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_region_ids_by_unique_id("fao", "countries", "adm0_code", "FID")
    assert values == []


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_columnar_rejects_undeclared_unique_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A columnar collection whose ``attribute_schema`` declares the region
    property but not the unique-id column has nothing to order by -- guard
    clause short-circuits before any SQL is built."""
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        AttributeStorageMode,
        FeatureAttributeSidecarConfig,
    )

    attrs = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[AttributeSchemaEntry(name="adm0_code")],
    )
    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[attrs])
    catalogs = _StubCatalogs()
    configs = _StubConfigs(col_config)
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    values = await claims.fetch_region_ids_by_unique_id("fao", "countries", "adm0_code", "FID")
    assert values == []
