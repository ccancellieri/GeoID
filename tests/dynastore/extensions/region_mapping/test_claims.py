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


def test_compute_claim_set_alias_equal_to_region_prop_collapses_to_one() -> None:
    """An alias identical to region_prop (case-insensitively) collapses into
    the single primary claim -- no duplicate row, no second primary. The claim
    set is exactly ``{region_prop} ∪ aliases``: there is no longer a
    ``{catalog}_{alias}`` token."""
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(region_prop="adm0_code", aliases=["adm0_code"])

    values = {claim for claim, _role in claims.values()}
    assert values == {"adm0_code"}
    assert len(claims) == 1
    assert list(claims.values())[0][1] == "primary"


def test_compute_claim_set_region_prop_is_primary_aliases_are_alias() -> None:
    """region_prop's token is the sole primary; every alias is role='alias'."""
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(region_prop="adm0_code", aliases=["country", "adm0"])

    roles_by_claim = {claim: role for claim, role in claims.values()}
    assert roles_by_claim["adm0_code"] == "primary"
    assert roles_by_claim["country"] == "alias"
    assert roles_by_claim["adm0"] == "alias"
    assert len(roles_by_claim) == 3


def test_compute_claim_set_casefold_dedup() -> None:
    """A region_prop and an alias differing only by case collapse to one claim."""
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(region_prop="Country", aliases=["country"])

    assert len(claims) == 1
    assert "country" in claims  # casefolded key


def test_compute_claim_set_alias_casefold_equals_region_prop_has_one_primary() -> None:
    """An alias that differs from region_prop only by case must still produce
    exactly one primary row (dynastore#2821 regression): the role is decided
    case-insensitively, not by exact string equality, and the claim text kept
    is the first-seen candidate's spelling (region_prop is built first)."""
    from dynastore.extensions.region_mapping.claims import ROLE_PRIMARY, compute_claim_set

    claims = compute_claim_set(region_prop="GAUL1_CODE", aliases=["gaul1_code"])

    primaries = [
        (claim, role) for claim, role in claims.values() if role == ROLE_PRIMARY
    ]
    assert len(primaries) == 1
    assert primaries[0][0] == "GAUL1_CODE"
    assert len(claims) == 1


def test_compute_claim_set_duplicate_aliases_casefold_collapse() -> None:
    """Two aliases that casefold-equal each other collapse to one alias row,
    keeping the first-seen spelling, without touching the primary."""
    from dynastore.extensions.region_mapping.claims import ROLE_ALIAS, ROLE_PRIMARY, compute_claim_set

    claims = compute_claim_set(region_prop="adm0_code", aliases=["Country", "country"])

    primaries = [claim for claim, role in claims.values() if role == ROLE_PRIMARY]
    assert primaries == ["adm0_code"]
    aliases = sorted(claim for claim, role in claims.values() if role == ROLE_ALIAS)
    assert aliases == ["Country"]


def test_compute_claim_set_exactly_one_primary() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    claims = compute_claim_set(region_prop="adm0_code", aliases=["country", "adm0", "iso3"])

    primaries = [claim for claim, role in claims.values() if role == "primary"]
    assert primaries == ["adm0_code"]


def test_compute_claim_set_rejects_regex_metacharacters_in_region_prop() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(region_prop="adm0.code", aliases=["country"])


def test_compute_claim_set_rejects_regex_metacharacters_in_alias() -> None:
    from dynastore.extensions.region_mapping.claims import compute_claim_set

    with pytest.raises(ValueError, match="regex metacharacters"):
        compute_claim_set(region_prop="adm0", aliases=["adm(0)"])


def test_validate_claim_text_accepts_plain_literal() -> None:
    from dynastore.extensions.region_mapping.claims import validate_claim_text

    validate_claim_text("adm0_code")  # must not raise


# ---------------------------------------------------------------------------
# resolve_unique_id_prop / CollectionColumns -- uniqueIdProp resolution and
# the columnar-schema check behind POST validation (dynastore region-mapping
# object API).
# ---------------------------------------------------------------------------


def test_resolve_unique_id_prop_prefers_supplied() -> None:
    from dynastore.extensions.region_mapping.claims import resolve_unique_id_prop

    assert resolve_unique_id_prop("MY_ID", "CODE", True) == "MY_ID"


def test_resolve_unique_id_prop_falls_back_to_external_id_source_column() -> None:
    from dynastore.extensions.region_mapping.claims import resolve_unique_id_prop

    # The external_id SOURCE column (external_id_path -- e.g. "CODE"), not the
    # internal "external_id" storage column which the tiles never expose.
    assert resolve_unique_id_prop(None, "CODE", True) == "CODE"


def test_resolve_unique_id_prop_falls_back_to_fid_when_no_external_id() -> None:
    from dynastore.extensions.region_mapping.claims import (
        FALLBACK_UNIQUE_ID_PROP,
        resolve_unique_id_prop,
    )

    assert resolve_unique_id_prop(None, None, True) == FALLBACK_UNIQUE_ID_PROP == "FID"


def test_resolve_unique_id_prop_none_when_no_external_id_and_no_fid() -> None:
    from dynastore.extensions.region_mapping.claims import resolve_unique_id_prop

    assert resolve_unique_id_prop(None, None, False) is None


def test_uncached_if_returns_wrapped_original_only_when_no_cache() -> None:
    """?no_cache=true reaches the raw ``__wrapped__`` coroutine the @cached
    decorator hides; without it, the cached wrapper is used unchanged."""
    from dynastore.extensions.region_mapping.claims import uncached_if

    def raw() -> str:
        return "raw"

    def wrapper() -> str:
        return "cached"

    wrapper.__wrapped__ = raw  # type: ignore[attr-defined]  # what functools.wraps sets

    assert uncached_if(wrapper, True) is raw
    assert uncached_if(wrapper, False) is wrapper

    # A plain (un-decorated) callable has no __wrapped__ -- returned as-is.
    def plain() -> str:
        return "x"

    assert uncached_if(plain, True) is plain


def test_collection_columns_has_column_declared_and_external_id() -> None:
    """``has_column`` is True for a declared columnar attribute AND for the
    driver-managed external_id column (which lives outside attribute_schema),
    False for anything else."""
    from dynastore.extensions.region_mapping.claims import CollectionColumns

    cols = CollectionColumns(
        is_columnar=True,
        declared=frozenset({"GAUL1_CODE", "FID"}),
        external_id_field="external_id",
        external_id_path=None,
        validity_column=None,
    )
    assert cols.has_column("GAUL1_CODE") is True
    assert cols.has_column("FID") is True
    assert cols.has_column("external_id") is True  # system column, not declared
    assert cols.has_column("nope") is False
    assert cols.enable_external_id is True


def test_collection_columns_no_external_id() -> None:
    from dynastore.extensions.region_mapping.claims import CollectionColumns

    cols = CollectionColumns(
        is_columnar=True, declared=frozenset({"FID"}),
        external_id_field=None, external_id_path=None, validity_column=None,
    )
    assert cols.has_column("external_id") is False
    assert cols.enable_external_id is False


@pytest.mark.asyncio
async def test_resolve_collection_columns_columnar_includes_external_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A columnar collection resolves to its declared attribute names plus its
    configured external_id column, flagged columnar."""
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        AttributeStorageMode,
        FeatureAttributeSidecarConfig,
    )

    attrs = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[AttributeSchemaEntry(name="GAUL1_CODE"), AttributeSchemaEntry(name="FID")],
        external_id_field="external_id",
    )
    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[attrs])
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(configs=_StubConfigs(col_config)),
    )

    cols = await claims.resolve_collection_columns("fao", "gaul")
    assert cols is not None
    assert cols.is_columnar is True
    assert cols.declared == frozenset({"GAUL1_CODE", "FID"})
    assert cols.external_id_field == "external_id"
    assert cols.has_column("GAUL1_CODE") and cols.has_column("external_id")


@pytest.mark.asyncio
async def test_resolve_collection_columns_none_without_attributes_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    col_config = ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[])
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(configs=_StubConfigs(col_config)),
    )

    assert await claims.resolve_collection_columns("fao", "gaul") is None


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
        fetch_region_mapping_cardinality,
    )
    from dynastore.tools.cache import cache_clear

    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
    cache_clear(fetch_region_ids_by_unique_id)
    cache_clear(fetch_region_mapping_cardinality)
    yield
    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
    cache_clear(fetch_region_ids_by_unique_id)
    cache_clear(fetch_region_mapping_cardinality)


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


# ---------------------------------------------------------------------------
# fetch_region_mapping_cardinality / validate_region_mapping_stats -- the
# misconfiguration-detection signal behind GET .../validate and region.json's
# exclusion of unsound mappings. Only guard clauses are unit-tested here for
# the fetch side; the query itself is covered by a real-PG integration test.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_region_mapping_cardinality_no_protocols_returns_zeros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims

    monkeypatch.setattr(claims, "get_protocol", _protocol_router())

    stats = await claims.fetch_region_mapping_cardinality("fao", "countries", "adm0_code", "FID")
    assert stats == {
        "feature_count": 0, "distinct_region_count": 0, "distinct_unique_id_count": 0,
        "null_unique_id_count": 0,
    }


@pytest.mark.asyncio
async def test_fetch_region_mapping_cardinality_no_physical_table_returns_zeros(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig

    catalogs = _StubCatalogs()
    configs = _StubConfigs(ItemsPostgresqlDriverConfig())
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    stats = await claims.fetch_region_mapping_cardinality("fao", "countries", "adm0_code", "FID")
    assert stats["feature_count"] == 0


@pytest.mark.asyncio
async def test_fetch_region_mapping_cardinality_columnar_rejects_undeclared_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs()
    configs = _StubConfigs(_columnar_col_config(["adm0_code"]))  # "FID" not declared
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    stats = await claims.fetch_region_mapping_cardinality("fao", "countries", "adm0_code", "FID")
    assert stats == {
        "feature_count": 0, "distinct_region_count": 0, "distinct_unique_id_count": 0,
        "null_unique_id_count": 0,
    }


def test_validate_region_mapping_stats_sound_mapping_has_no_reasons() -> None:
    from dynastore.extensions.region_mapping.claims import validate_region_mapping_stats

    reasons = validate_region_mapping_stats(
        {"feature_count": 10, "distinct_region_count": 10, "distinct_unique_id_count": 10},
    )
    assert reasons == []


def test_validate_region_mapping_stats_flags_duplicate_region_codes() -> None:
    """E.g. an admin-1 collection (many features per country) claimed under
    an ISO3 country-code column -- exactly the ambiguous-region-mapping case
    from the live gaul_demo_gaul_level_1 mapping."""
    from dynastore.extensions.region_mapping.claims import validate_region_mapping_stats

    reasons = validate_region_mapping_stats(
        {"feature_count": 3102, "distinct_region_count": 200, "distinct_unique_id_count": 3102},
    )
    assert len(reasons) == 1
    assert "regionProp is not unique per feature" in reasons[0]


def test_validate_region_mapping_stats_flags_duplicate_unique_ids() -> None:
    from dynastore.extensions.region_mapping.claims import validate_region_mapping_stats

    reasons = validate_region_mapping_stats(
        {"feature_count": 10, "distinct_region_count": 10, "distinct_unique_id_count": 4},
    )
    assert len(reasons) == 1
    assert "uniqueIdProp is not unique per feature" in reasons[0]


def test_validate_region_mapping_stats_zero_features_short_circuits() -> None:
    """Zero matching features is its own single reason -- it must not also
    report duplicate-code/duplicate-id findings computed from a 0-by-0
    ratio, which would be meaningless noise."""
    from dynastore.extensions.region_mapping.claims import validate_region_mapping_stats

    reasons = validate_region_mapping_stats(
        {"feature_count": 0, "distinct_region_count": 0, "distinct_unique_id_count": 0},
    )
    assert len(reasons) == 1
    assert "No features have non-null values" in reasons[0]


# ---------------------------------------------------------------------------
# Bounded pool acquire (dynastore#2902) -- a saturated pool on either query's
# connection acquire must raise PoolSaturationError fast rather than holding
# a checked-out connection for the full 300s request ceiling, mirroring the
# tiles-metadata hardening in dynastore#3023.
# ---------------------------------------------------------------------------


def _columnar_col_config(declared: List[str]):
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeSchemaEntry,
        AttributeStorageMode,
        FeatureAttributeSidecarConfig,
    )

    attrs = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[AttributeSchemaEntry(name=name) for name in declared],
    )
    return ItemsPostgresqlDriverConfig(physical_table="t_abc123", sidecars=[attrs])


@pytest.mark.asyncio
async def test_fetch_distinct_region_ids_acquire_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A saturated pool on this query's connection acquire raises
    ``PoolSaturationError``, bounded by the live fail-fast timeout instead of
    holding the connection for the full request timeout."""
    import asyncio
    from unittest.mock import AsyncMock as _AsyncMock
    from unittest.mock import patch

    from sqlalchemy.ext.asyncio import create_async_engine

    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.db_config.exceptions import PoolSaturationError

    catalogs = _StubCatalogs()
    configs = _StubConfigs(_columnar_col_config(["adm0_code"]))
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setattr(claims, "get_engine", lambda: engine)

    async def _fast_fail_fast_timeout() -> float:
        return 0.01

    async def _never_connects(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.sleep(10)

    monkeypatch.setattr(claims, "_read_live_fg_acquire_timeout", _fast_fail_fast_timeout)

    try:
        with patch(
            "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
            new=_AsyncMock(side_effect=_never_connects),
        ):
            with pytest.raises(PoolSaturationError):
                await claims.fetch_distinct_region_ids("fao", "countries", "adm0_code")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_acquire_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same bounded-acquire hardening for ``fetch_region_ids_by_unique_id``,
    the other direct-acquire query on this read path."""
    import asyncio
    from unittest.mock import AsyncMock as _AsyncMock
    from unittest.mock import patch

    from sqlalchemy.ext.asyncio import create_async_engine

    from dynastore.extensions.region_mapping import claims
    from dynastore.modules.db_config.exceptions import PoolSaturationError

    catalogs = _StubCatalogs()
    configs = _StubConfigs(_columnar_col_config(["adm0_code", "FID"]))
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )

    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setattr(claims, "get_engine", lambda: engine)

    async def _fast_fail_fast_timeout() -> float:
        return 0.01

    async def _never_connects(*_args: Any, **_kwargs: Any) -> None:
        await asyncio.sleep(10)

    monkeypatch.setattr(claims, "_read_live_fg_acquire_timeout", _fast_fail_fast_timeout)

    try:
        with patch(
            "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
            new=_AsyncMock(side_effect=_never_connects),
        ):
            with pytest.raises(PoolSaturationError):
                await claims.fetch_region_ids_by_unique_id(
                    "fao", "countries", "adm0_code", "FID",
                )
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Memory footprint (dynastore#2946): this fetch has no LIMIT (every feature
# of the source collection lands at its positional index), so per-row dict
# overhead multiplies straight onto an already-unbounded, per-worker-cached
# result. Pins that rows are read via plain ``ResultHandler.ALL`` attribute
# access, not re-wrapped into a dict per row.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_region_ids_by_unique_id_reads_rows_by_attribute_not_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``DQLQuery`` stub returning plain attribute-only rows (no
    ``__getitem__``) must still be read correctly -- proves the positional
    array is built without an ``ALL_DICTS``-style ``row["..."]`` round trip
    that would double the per-row cost of this collection-sized fetch."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from dynastore.extensions.region_mapping import claims

    catalogs = _StubCatalogs()
    configs = _StubConfigs(_columnar_col_config(["adm0_code", "FID"]))
    monkeypatch.setattr(
        claims, "get_protocol", _protocol_router(catalogs=catalogs, configs=configs),
    )
    monkeypatch.setattr(claims, "get_engine", lambda: object())

    # fid 0 and 2 share "ITA" -- the many-features-one-code shape the
    # positional array must preserve.
    stub_rows = [
        SimpleNamespace(region_value="ITA", fid=0),
        SimpleNamespace(region_value="FRA", fid=1),
        SimpleNamespace(region_value="ITA", fid=2),
    ]

    class _FakeDQLQuery:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def execute(self, *_args: Any, **_kwargs: Any) -> List[SimpleNamespace]:
            return stub_rows

    monkeypatch.setattr(claims, "DQLQuery", _FakeDQLQuery)

    @asynccontextmanager
    async def _fake_managed_transaction(*_args: Any, **_kwargs: Any):
        yield object()

    monkeypatch.setattr(claims, "managed_transaction", _fake_managed_transaction)

    values = await claims.fetch_region_ids_by_unique_id(
        "fao", "countries", "adm0_code", "FID",
    )

    assert values == ["ITA", "FRA", "ITA"]
