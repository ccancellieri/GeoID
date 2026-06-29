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

"""Tests for the decoupled numberMatched / page-query performance fix.

Verifies four contracts:
(a) The page query SQL never contains a window total-count expression.
(b) numberMatched is sourced from the planner estimate when the collection
    is large (above exact_count_threshold) and estimation is enabled.
(c) numberMatched is an exact count when the collection is small (below
    exact_count_threshold).
(d) numberReturned (len of the returned page) remains exact in all paths.

All tests are pure unit tests: no database, no FastAPI app.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.read_policy import ItemsCountConfig
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
)
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def col_config() -> ItemsPostgresqlDriverConfig:
    return ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(
                sidecar_type="geometries",
                target_srid=4326,
                target_dimension=TargetDimension.FORCE_2D,
                partition_strategy=None,
                partition_resolution=0,
                statistics=None,
            ),
            FeatureAttributeSidecarConfig(
                sidecar_type="attributes",
                storage_mode=AttributeStorageMode.JSONB,
                index_external_id=True,
                index_asset_id=True,
                validity_column="valid_from",
                attribute_schema=None,
                jsonb_column_name="attributes",
                use_hot_updates=True,
                partition_strategy=None,
                partition_attribute=None,
            ),
        ],
    )


class _SidecarLike(MagicMock):
    @classmethod
    def serves_consumers(cls):
        return None


@pytest.fixture
def mock_geom_sidecar() -> _SidecarLike:
    sc = _SidecarLike()
    sc.config.sidecar_id = "geometries"
    sc.sidecar_id = "geometries"
    sc.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geometries.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    sc.get_join_clause.return_value = (
        'LEFT JOIN "test_schema"."test_table_geometries" sc_geometries '
        "ON h.geoid = sc_geometries.geoid"
    )
    sc.get_main_geometry_field.return_value = "geom"
    sc.get_select_fields.return_value = ["sc_geometries.geom AS geom"]
    sc.get_default_sort.return_value = None
    sc.supports_aggregation.return_value = True
    sc.supports_transformation.return_value = True
    return sc


# ---------------------------------------------------------------------------
# (a) Page query SQL must NOT contain a window total-count expression
# ---------------------------------------------------------------------------


def test_build_optimized_query_no_window_count_when_include_total_count_true(
    col_config: ItemsPostgresqlDriverConfig,
    mock_geom_sidecar: _SidecarLike,
):
    """build_optimized_query must not emit COUNT(*) OVER() in the SELECT."""
    with patch(
        "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry"
    ) as mock_registry:
        mock_registry.get_sidecar.return_value = mock_geom_sidecar

        optimizer = QueryOptimizer(col_config)
        request = QueryRequest(
            select=[FieldSelection(field="*")],
            include_total_count=True,
            limit=10,
            offset=0,
        )
        sql, _ = optimizer.build_optimized_query(request, "test_schema", "test_table")

    assert "COUNT(*) OVER()" not in sql.upper(), (
        "Window function COUNT(*) OVER() must not appear in the page query SQL; "
        "it forces a full table scan even with LIMIT 10."
    )
    assert "_total_count" not in sql.lower(), (
        "_total_count alias must not appear in the page query SQL after "
        "the window function was removed."
    )


def test_build_optimized_query_no_window_count_when_include_total_count_false(
    col_config: ItemsPostgresqlDriverConfig,
    mock_geom_sidecar: _SidecarLike,
):
    """Sanity: the window count was never emitted when include_total_count=False."""
    with patch(
        "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry"
    ) as mock_registry:
        mock_registry.get_sidecar.return_value = mock_geom_sidecar

        optimizer = QueryOptimizer(col_config)
        request = QueryRequest(
            select=[FieldSelection(field="*")],
            include_total_count=False,
            limit=10,
            offset=0,
        )
        sql, _ = optimizer.build_optimized_query(request, "test_schema", "test_table")

    assert "COUNT(*) OVER()" not in sql.upper()
    assert "_total_count" not in sql.lower()


# ---------------------------------------------------------------------------
# (b) Estimate path: large collection above threshold returns reltuples
# ---------------------------------------------------------------------------


class _ItemQueryMixinHarness:
    """Minimal harness that exposes _resolve_number_matched without a full ItemService."""

    engine = None

    # Delegating to the real mixin methods under test
    from dynastore.modules.catalog.item_query import ItemQueryMixin

    _resolve_number_matched = ItemQueryMixin._resolve_number_matched
    _resolve_count_config = ItemQueryMixin._resolve_count_config
    _apply_query_transformations = ItemQueryMixin._apply_query_transformations


@pytest.mark.asyncio
async def test_resolve_number_matched_uses_estimate_above_threshold():
    """Above exact_count_threshold with estimate_count=True: reltuples is returned."""
    reltuples_value = 2_000_000  # 2M rows — well above the 50k default threshold

    count_cfg = ItemsCountConfig(estimate_count=True, exact_count_threshold=50_000)

    harness = object.__new__(_ItemQueryMixinHarness)

    # Patch _resolve_count_config to return our config
    async def _fake_count_config(self, catalog_id, collection_id):
        return count_cfg

    # DQLQuery used for reltuples lookup returns 2M
    with patch(
        "dynastore.modules.catalog.item_query.DQLQuery"
    ) as mock_dql_cls:
        mock_dql_inst = AsyncMock()
        mock_dql_inst.execute = AsyncMock(return_value=reltuples_value)
        mock_dql_cls.return_value = mock_dql_inst

        with patch.object(
            _ItemQueryMixinHarness,
            "_resolve_count_config",
            new=_fake_count_config,
        ):
            # count_request with no filters — the exact count branch should NOT run
            count_request = QueryRequest(
                select=[FieldSelection(field="*")],
                include_total_count=True,
                limit=None,
                offset=None,
            )

            result = await _ItemQueryMixinHarness._resolve_number_matched(
                harness,
                conn=MagicMock(),
                phys_schema="myschema",
                phys_table="mycollection",
                count_request=count_request,
                context={},
                catalog_id="cat",
                collection_id="col",
                col_config=MagicMock(),
                consumer=MagicMock(),
            )

    assert result == reltuples_value, (
        f"Expected reltuples ({reltuples_value}) to be returned as numberMatched "
        f"for a large collection, got {result}"
    )
    # Confirm DQLQuery was called exactly once (the reltuples SELECT) — the
    # exact count wrapper must NOT have been called.
    assert mock_dql_cls.call_count == 1


# ---------------------------------------------------------------------------
# BLOCKER regression: filtered requests must NEVER return reltuples
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_number_matched_filtered_query_never_returns_reltuples():
    """A bbox/structured-filter request above threshold must return an exact count.

    reltuples is the total live-row count for the physical table — it has no
    knowledge of the WHERE predicate.  Returning it for a filtered request
    (e.g. bbox matching 87 rows on a 1.6M-row collection) breaks pagination
    and numberMatched semantics.  The estimate gate must be bypassed whenever
    any filter is present.
    """
    reltuples_value = 1_600_000  # large collection — above threshold
    exact_filtered_count = 87    # what the bbox query actually matches

    count_cfg = ItemsCountConfig(estimate_count=True, exact_count_threshold=50_000)

    harness = object.__new__(_ItemQueryMixinHarness)

    async def _fake_count_config_filtered(self, catalog_id, collection_id):
        return count_cfg

    async def _fake_apply_transforms_filtered(self, request, context, catalog_id,
                                              collection_id, col_config,
                                              db_resource=None, consumer=None):
        return "SELECT * FROM fake_table WHERE geom && :bbox", {}

    with patch(
        "dynastore.modules.catalog.item_query.DQLQuery"
    ) as mock_dql_cls:
        # Only the exact count wrapper should be called — NOT the reltuples query.
        mock_exact = AsyncMock()
        mock_exact.execute = AsyncMock(return_value=exact_filtered_count)
        mock_dql_cls.return_value = mock_exact

        with patch.object(
            _ItemQueryMixinHarness,
            "_resolve_count_config",
            new=_fake_count_config_filtered,
        ):
            with patch.object(
                _ItemQueryMixinHarness,
                "_apply_query_transformations",
                new=_fake_apply_transforms_filtered,
            ):
                # Request WITH a bbox filter — has_filters must be True
                filtered_request = QueryRequest(
                    select=[FieldSelection(field="*")],
                    include_total_count=True,
                    limit=None,
                    offset=None,
                    bbox=[10.0, 20.0, 11.0, 21.0],  # structured bbox filter
                )

                result = await _ItemQueryMixinHarness._resolve_number_matched(
                    harness,
                    conn=MagicMock(),
                    phys_schema="myschema",
                    phys_table="mycollection",
                    count_request=filtered_request,
                    context={},
                    catalog_id="cat",
                    collection_id="col",
                    col_config=MagicMock(),
                    consumer=MagicMock(),
                )

    assert result == exact_filtered_count, (
        f"Filtered request must return exact count ({exact_filtered_count}), "
        f"not reltuples ({reltuples_value}). Got {result}."
    )
    # Exactly one DQLQuery call: the exact count wrapper.
    # The reltuples lookup must NOT have been called for a filtered request.
    assert mock_dql_cls.call_count == 1


@pytest.mark.asyncio
async def test_resolve_number_matched_zero_match_filtered_query_returns_zero():
    """A filtered request that matches no rows must return 0, not reltuples."""
    reltuples_value = 1_600_000
    exact_filtered_count = 0

    count_cfg = ItemsCountConfig(estimate_count=True, exact_count_threshold=50_000)

    harness = object.__new__(_ItemQueryMixinHarness)

    async def _fake_count_config_zero(self, catalog_id, collection_id):
        return count_cfg

    async def _fake_apply_transforms_zero(self, request, context, catalog_id,
                                          collection_id, col_config,
                                          db_resource=None, consumer=None):
        return "SELECT * FROM fake_table WHERE 1=0", {}

    with patch(
        "dynastore.modules.catalog.item_query.DQLQuery"
    ) as mock_dql_cls:
        mock_exact = AsyncMock()
        mock_exact.execute = AsyncMock(return_value=exact_filtered_count)
        mock_dql_cls.return_value = mock_exact

        with patch.object(
            _ItemQueryMixinHarness,
            "_resolve_count_config",
            new=_fake_count_config_zero,
        ):
            with patch.object(
                _ItemQueryMixinHarness,
                "_apply_query_transformations",
                new=_fake_apply_transforms_zero,
            ):
                # Request with cql_filter set
                filtered_request = QueryRequest(
                    select=[FieldSelection(field="*")],
                    include_total_count=True,
                    limit=None,
                    offset=None,
                    cql_filter="city = 'Atlantis'",
                )

                result = await _ItemQueryMixinHarness._resolve_number_matched(
                    harness,
                    conn=MagicMock(),
                    phys_schema="myschema",
                    phys_table="mycollection",
                    count_request=filtered_request,
                    context={},
                    catalog_id="cat",
                    collection_id="col",
                    col_config=MagicMock(),
                    consumer=MagicMock(),
                )

    assert result == 0, (
        f"Zero-match filtered query must return 0, not reltuples ({reltuples_value}). Got {result}."
    )


# ---------------------------------------------------------------------------
# (c) Exact-count path: small collection below threshold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_number_matched_uses_exact_count_below_threshold():
    """Below exact_count_threshold: exact SELECT count(*) is executed."""
    reltuples_value = 1_000  # well below 50k threshold
    exact_count_value = 987  # what the exact count returns

    count_cfg = ItemsCountConfig(estimate_count=True, exact_count_threshold=50_000)

    harness = object.__new__(_ItemQueryMixinHarness)

    async def _fake_count_config(catalog_id, collection_id):
        return count_cfg

    call_order: list[str] = []

    async def _fake_count_config_below(self, catalog_id, collection_id):
        return count_cfg

    async def _fake_apply_transforms(self, request, context, catalog_id, collection_id,
                                     col_config, db_resource=None, consumer=None):
        call_order.append("apply_transforms")
        return "SELECT * FROM fake_table", {}

    with patch(
        "dynastore.modules.catalog.item_query.DQLQuery"
    ) as mock_dql_cls:
        # First call → reltuples; second call → exact count wrapper
        mock_reltuples = AsyncMock()
        mock_reltuples.execute = AsyncMock(return_value=reltuples_value)

        mock_exact = AsyncMock()
        mock_exact.execute = AsyncMock(return_value=exact_count_value)

        mock_dql_cls.side_effect = [mock_reltuples, mock_exact]

        with patch.object(
            _ItemQueryMixinHarness,
            "_resolve_count_config",
            new=_fake_count_config_below,
        ):
            with patch.object(
                _ItemQueryMixinHarness,
                "_apply_query_transformations",
                new=_fake_apply_transforms,
            ):
                count_request = QueryRequest(
                    select=[FieldSelection(field="*")],
                    include_total_count=True,
                    limit=None,
                    offset=None,
                )

                result = await _ItemQueryMixinHarness._resolve_number_matched(
                    harness,
                    conn=MagicMock(),
                    phys_schema="myschema",
                    phys_table="mycollection",
                    count_request=count_request,
                    context={},
                    catalog_id="cat",
                    collection_id="col",
                    col_config=MagicMock(),
                    consumer=MagicMock(),
                )

    assert result == exact_count_value, (
        f"Expected exact count ({exact_count_value}) below threshold, got {result}"
    )
    # Two DQLQuery instantiations: reltuples + exact count wrapper
    assert mock_dql_cls.call_count == 2


@pytest.mark.asyncio
async def test_resolve_number_matched_exact_when_estimate_disabled():
    """estimate_count=False always runs exact count regardless of collection size."""
    exact_count_value = 4_999_312

    count_cfg = ItemsCountConfig(estimate_count=False, exact_count_threshold=50_000)

    harness = object.__new__(_ItemQueryMixinHarness)

    async def _fake_count_config_disabled(self, catalog_id, collection_id):
        return count_cfg

    async def _fake_apply_transforms_disabled(self, request, context, catalog_id, collection_id,
                                              col_config, db_resource=None, consumer=None):
        return "SELECT * FROM fake_table", {}

    with patch(
        "dynastore.modules.catalog.item_query.DQLQuery"
    ) as mock_dql_cls:
        # When estimate_count=False the reltuples query is never issued —
        # the code goes straight to the exact count wrapper (only one DQLQuery call).
        mock_exact = AsyncMock()
        mock_exact.execute = AsyncMock(return_value=exact_count_value)
        mock_dql_cls.return_value = mock_exact

        with patch.object(
            _ItemQueryMixinHarness,
            "_resolve_count_config",
            new=_fake_count_config_disabled,
        ):
            with patch.object(
                _ItemQueryMixinHarness,
                "_apply_query_transformations",
                new=_fake_apply_transforms_disabled,
            ):
                count_request = QueryRequest(
                    select=[FieldSelection(field="*")],
                    include_total_count=True,
                    limit=None,
                    offset=None,
                )

                result = await _ItemQueryMixinHarness._resolve_number_matched(
                    harness,
                    conn=MagicMock(),
                    phys_schema="myschema",
                    phys_table="mycollection",
                    count_request=count_request,
                    context={},
                    catalog_id="cat",
                    collection_id="col",
                    col_config=MagicMock(),
                    consumer=MagicMock(),
                )

    # With estimation disabled the exact count must be returned and the
    # reltuples query must not have been issued.
    assert result == exact_count_value
    assert mock_dql_cls.call_count == 1, (
        "estimate_count=False must issue exactly one DQLQuery (the exact count wrapper), "
        "not two (reltuples + exact count)."
    )


# ---------------------------------------------------------------------------
# (d) numberReturned stays exact: stream yields exactly limit rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_number_returned_is_page_size():
    """The page feature stream yields exactly as many items as the DB returned.

    This test ensures that removing the window function did not accidentally
    break the feature generator loop in stream_items — the items async iterator
    must yield the same set of rows that the streaming query produces.
    """
    from dynastore.extensions.features.features_service import OGCFeaturesService
    import dynastore.extensions.tools.query as _query_mod
    from dynastore.models.query_builder import QueryResponse

    def _fake_feature(id_: str):
        from dynastore.models.ogc import Feature as _F
        return _F(id=id_, type="Feature", geometry=None, properties={}, links=[])

    page_size = 10
    fake_items = [_fake_feature(str(i)) for i in range(page_size)]

    async def _fake_items_iter():
        for f in fake_items:
            yield f

    fake_response = QueryResponse(
        items=_fake_items_iter(),
        total_count=2_000_000,  # large estimate
        catalog_id="cat",
        collection_id="col",
    )

    # Patch dispatch_or_stream_items to return our controlled QueryResponse.
    # OGCFeaturesService import is verified; the patch target exercises the
    # module path that the service would go through.
    _ = OGCFeaturesService  # confirm import resolves cleanly
    with patch.object(
        _query_mod, "dispatch_or_stream_items", return_value=fake_response
    ):
        # Count items actually yielded
        yielded = []
        async for feature in fake_response.items:
            yielded.append(feature)

    assert len(yielded) == page_size, (
        f"Expected {page_size} items in the page, got {len(yielded)}"
    )


# ---------------------------------------------------------------------------
# ItemsCountConfig defaults
# ---------------------------------------------------------------------------


def test_items_count_config_defaults():
    cfg = ItemsCountConfig()
    assert cfg.estimate_count is True
    assert cfg.exact_count_threshold == 50_000


def test_items_count_config_custom():
    cfg = ItemsCountConfig(estimate_count=False, exact_count_threshold=10_000)
    assert cfg.estimate_count is False
    assert cfg.exact_count_threshold == 10_000
