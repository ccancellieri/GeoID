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

"""Unit tests for PG read-path ABAC wiring (#1457).

Covers two contracts:

(a) ``dispatch_or_stream_items`` — when ``request`` is supplied and
    ``collection_uses_pg_access_envelope`` returns True, the compiled
    ``access_filter`` is set on the QueryRequest before ``stream_items`` is
    called.  When the collection does NOT use the sidecar, or when
    ``request`` is None, ``access_filter`` is NOT set (no regression).

(b) ``ItemQueryMixin.get_item`` — when ``access_filter`` is passed in,
    it is threaded onto the internal QueryRequest before the query optimizer
    runs.  When it is not passed, the field stays unset (backward compat).

These are pure unit tests: no DB, no asyncpg, no real QueryOptimizer.
All external dependencies are mocked at function boundaries.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.tools.query import dispatch_or_stream_items
from dynastore.models.protocols.access_filter import AccessClause, AccessFilter, FieldPredicate
from dynastore.models.query_builder import FieldSelection, QueryRequest, QueryResponse
from dynastore.modules.catalog.query_optimizer import QueryOptimizer
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)


# ---------------------------------------------------------------------------
# Helpers shared by both test groups
# ---------------------------------------------------------------------------

def _make_request(principal_id: str = "user:alice") -> Any:
    """Minimal Starlette-like request with IAM state."""
    return SimpleNamespace(
        state=SimpleNamespace(
            principal=None,
            principal_id=principal_id,
            principal_role=["reader"],
        )
    )


def _make_query_response() -> QueryResponse:
    """Minimal async QueryResponse."""
    async def _items():
        if False:
            yield  # pragma: no cover

    return QueryResponse(
        items=_items(),
        total_count=0,
        catalog_id="cat1",
        collection_id="col1",
    )


def _make_items_protocol(response: QueryResponse) -> Any:
    """Stub ItemsProtocol whose stream_items captures the QueryRequest."""
    captured: list = []

    async def _stream_items(
        *,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        ctx: Any = None,
        consumer: Any = None,
        hints: Any = None,
    ) -> QueryResponse:
        captured.append(request)
        return response

    proto = MagicMock()
    proto.stream_items = _stream_items
    proto._captured = captured
    return proto


# ---------------------------------------------------------------------------
# (a) dispatch_or_stream_items access_filter wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_or_stream_sets_access_filter_when_envelope_sidecar(
    monkeypatch: Any,
) -> None:
    """When collection_uses_pg_access_envelope is True and request is given,
    access_filter is set on the QueryRequest before stream_items.

    The three helpers are imported locally inside dispatch_or_stream_items, so
    we patch at the source module (access_scope) — the same object the local
    import retrieves at call time.
    """
    canned_filter = AccessFilter.allow_everything()
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])
    request = _make_request()

    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=AsyncMock(return_value=True),
    ), patch(
        "dynastore.modules.storage.access_scope.compile_read_access_filter",
        new=AsyncMock(return_value=canned_filter),
    ), patch(
        "dynastore.modules.storage.access_scope.principals_from_request_state",
        return_value=(["user:alice", "reader"], None),
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=request,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    assert captured_qr.access_filter is canned_filter


@pytest.mark.asyncio
async def test_dispatch_or_stream_no_access_filter_without_envelope_sidecar(
    monkeypatch: Any,
) -> None:
    """When collection_uses_pg_access_envelope is False, access_filter is NOT
    set — no regression for ordinary collections."""
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])
    request = _make_request()

    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=AsyncMock(return_value=False),
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=request,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    # access_filter unset (None is the default) — non-envelope collection
    assert captured_qr.access_filter is None


@pytest.mark.asyncio
async def test_dispatch_or_stream_no_access_filter_when_request_is_none() -> None:
    """When request=None (system/internal call), access_filter is NOT touched
    and collection_uses_pg_access_envelope is never called."""
    response = _make_query_response()
    proto = _make_items_protocol(response)
    qr = QueryRequest(limit=10, offset=0, filters=[])

    envelope_check = AsyncMock(return_value=True)
    with patch(
        "dynastore.modules.storage.access_scope.collection_uses_pg_access_envelope",
        new=envelope_check,
    ):
        await dispatch_or_stream_items(
            proto,
            catalog_id="cat1",
            collection_id="col1",
            query_request=qr,
            consumer="OGC_FEATURES",
            request=None,
        )

    assert len(proto._captured) == 1
    captured_qr = proto._captured[0]
    assert captured_qr.access_filter is None
    # The guard `request is not None` must prevent the sidecar check entirely.
    envelope_check.assert_not_called()


@pytest.mark.asyncio
async def test_dispatch_or_stream_uses_search_dispatch_without_pg_check() -> None:
    """When search_dispatch is supplied, stream_items is not called at all
    (the ES path has already applied its own access scoping)."""
    sd = _make_query_response()
    proto = _make_items_protocol(_make_query_response())
    qr = QueryRequest(limit=10, offset=0, filters=[])

    result = await dispatch_or_stream_items(
        proto,
        catalog_id="cat1",
        collection_id="col1",
        query_request=qr,
        consumer="OGC_FEATURES",
        search_dispatch=sd,
    )

    # stream_items never called — returns search_dispatch directly
    assert len(proto._captured) == 0
    assert result is sd


# ---------------------------------------------------------------------------
# (b) get_item access_filter threading
# ---------------------------------------------------------------------------

class _FakeItemQueryMixin:
    """Thin shim that exercises only the access_filter threading in get_item.

    We cannot instantiate ItemQueryMixin directly (it requires a real engine),
    so we replicate the access_filter injection logic that was added to
    get_item and verify it operates correctly on a QueryRequest.
    """

    @staticmethod
    def _inject_access_filter_into_request(
        item_ids: List[str],
        access_filter: Optional[Any],
    ) -> QueryRequest:
        """Mirror the access_filter injection in ItemQueryMixin.get_item."""
        from dynastore.models.query_builder import FieldSelection

        request = QueryRequest(
            item_ids=item_ids,
            limit=1,
            select=[FieldSelection(field="*")],
        )
        if access_filter is not None:
            request.access_filter = access_filter
        return request


def test_get_item_threads_access_filter_onto_query_request() -> None:
    """access_filter passed to get_item is set on the internal QueryRequest."""
    af = AccessFilter.allow_everything()
    qr = _FakeItemQueryMixin._inject_access_filter_into_request(
        ["item-123"], access_filter=af
    )
    assert qr.access_filter is af


def test_get_item_leaves_access_filter_none_when_not_supplied() -> None:
    """When access_filter is None (default), QueryRequest.access_filter is not set."""
    qr = _FakeItemQueryMixin._inject_access_filter_into_request(
        ["item-123"], access_filter=None
    )
    assert qr.access_filter is None


def test_get_item_allow_everything_is_not_deny() -> None:
    """Sanity: AccessFilter.allow_everything() does not deny anything."""
    af = AccessFilter.allow_everything()
    assert af.allow_all is True
    assert af.deny_all is False


# ---------------------------------------------------------------------------
# Optimizer-level conditional ABAC envelope invariants (#2550)
# ---------------------------------------------------------------------------
#
# The post-filter in QueryOptimizer.determine_required_sidecars removes the
# access_envelope sidecar ONLY when access_filter is provably unconditional
# (AccessFilter.is_unconditional is True — blanket allow, no deny, no union).
# Three hard security invariants:
#   Invariant 1: access_filter=None → envelope RETAINED (fail-closed)
#   Invariant 2: is_unconditional=False → envelope RETAINED (enforcement)
#   Invariant 3: is_unconditional=True → envelope DROPPED (optimization)
# Plus SQL-level checks that the JOIN appears / disappears accordingly.
# ---------------------------------------------------------------------------

_REGISTRY_PATH = (
    "dynastore.modules.storage.drivers.pg_sidecars.registry.SidecarRegistry.get_sidecar"
)


class _EnvSidecarLike(MagicMock):
    """MagicMock subclass that carries the ``serves_consumers`` classmethod.

    ``QueryOptimizer.determine_required_sidecars`` resolves
    ``type(sc).serves_consumers()`` for the select=* branch; returning ``None``
    here means "consumer-agnostic" — the production default for all sidecars
    that do not restrict by consumer.
    """

    @classmethod
    def serves_consumers(cls):
        return None


def _make_3sidecar_col_config() -> ItemsPostgresqlDriverConfig:
    """Three-sidecar PG driver config: geometries + attributes + access_envelope."""
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
            AccessEnvelopeSidecarConfig(),
        ],
    )


def _build_3sidecar_mocks():
    """Return (mock_geom, mock_attr, mock_env) pre-wired for _make_3sidecar_col_config."""
    mock_geom = _EnvSidecarLike()
    mock_geom.sidecar_id = "geometries"
    mock_geom.get_queryable_fields.return_value = {
        "geom": FieldDefinition(
            name="geom",
            sql_expression="sc_geom.geom",
            capabilities=[FieldCapability.SPATIAL],
            data_type="geometry",
        )
    }
    mock_geom.get_main_geometry_field.return_value = "geom"
    mock_geom.get_join_clause.return_value = (
        'LEFT JOIN "s"."t_geometries" sc_geom ON h.geoid = sc_geom.geoid'
    )
    mock_geom.get_default_sort.return_value = None
    mock_geom.provides_feature_id = False

    mock_attr = _EnvSidecarLike()
    mock_attr.sidecar_id = "attributes"
    mock_attr.get_queryable_fields.return_value = {
        "external_id": FieldDefinition(
            name="external_id",
            sql_expression="sc_attr.external_id",
            capabilities=[FieldCapability.FILTERABLE],
            data_type="string",
        )
    }
    mock_attr.get_main_geometry_field.return_value = None
    mock_attr.get_join_clause.return_value = (
        'LEFT JOIN "s"."t_attributes" sc_attr ON h.geoid = sc_attr.geoid'
    )
    mock_attr.get_default_sort.return_value = None
    mock_attr.provides_feature_id = False

    mock_env = _EnvSidecarLike()
    mock_env.sidecar_id = "access_envelope"
    mock_env.get_queryable_fields.return_value = {}
    mock_env.get_main_geometry_field.return_value = None
    mock_env.get_join_clause.return_value = (
        'LEFT JOIN "s"."t_items_access_envelope" ae ON h.geoid = ae.geoid'
    )
    mock_env.get_default_sort.return_value = None
    mock_env.provides_feature_id = False

    return mock_geom, mock_attr, mock_env


def _sidecar_router(mock_geom, mock_attr, mock_env):
    """Return a side_effect callable that dispatches by sidecar_type."""
    mapping = {
        "geometries": mock_geom,
        "attributes": mock_attr,
        "access_envelope": mock_env,
    }

    def _get(sc, lenient=True):
        return mapping.get(getattr(sc, "sidecar_type", ""), None)

    return _get


# ---------------------------------------------------------------------------
# Invariant 1 — access_filter=None → envelope RETAINED (fail-closed)
# ---------------------------------------------------------------------------

def test_abac_invariant_no_filter_retains_envelope() -> None:
    """access_filter=None must keep the envelope so it fails closed (appends FALSE)."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(select=[FieldSelection(field="geom")])
        # access_filter is None by default
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "access_filter=None must retain the envelope (sidecar appends FALSE — fail-closed)"
    )


# ---------------------------------------------------------------------------
# Invariant 2 — is_unconditional=False → envelope RETAINED (enforcement)
# ---------------------------------------------------------------------------

def test_abac_invariant_restricted_filter_retains_envelope() -> None:
    """A restricted access_filter (is_unconditional=False) must keep the envelope."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    restricted_af = AccessFilter.from_clauses(
        allow=[AccessClause((FieldPredicate("_attrs.dept", ("finance",)),))]
    )
    assert restricted_af.is_unconditional is False, "pre-condition: filter must not be unconditional"

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="geom")],
            access_filter=restricted_af,
        )
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "restricted access_filter must retain the envelope so the row-level WHERE clause runs"
    )


# ---------------------------------------------------------------------------
# Invariant 3 — is_unconditional=True → envelope DROPPED (optimization)
# ---------------------------------------------------------------------------

def test_abac_invariant_unconditional_filter_drops_envelope() -> None:
    """AccessFilter.allow_everything() (is_unconditional=True) must drop the envelope JOIN."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    af = AccessFilter.allow_everything()
    assert af.is_unconditional is True, "pre-condition: allow_everything must be unconditional"

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="geom")],
            access_filter=af,
        )
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" not in result_ids, (
        "allow_everything() is unconditional — envelope JOIN is pure overhead and must be dropped"
    )


# ---------------------------------------------------------------------------
# SQL-level checks — JOIN presence in build_optimized_query output
# ---------------------------------------------------------------------------

def test_abac_sql_restricted_has_envelope_join_and_where() -> None:
    """Restricted principal: generated SQL includes the access_envelope JOIN and WHERE."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    # Make the envelope apply_query_context append a WHERE sentinel so we can
    # assert both the JOIN and WHERE clause appear.
    mock_env.apply_query_context.side_effect = lambda req, ctx: ctx[
        "where_conditions"
    ].append("FALSE")

    restricted_af = AccessFilter.from_clauses(
        allow=[AccessClause((FieldPredicate("_attrs.dept", ("finance",)),))]
    )

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="geom")],
            access_filter=restricted_af,
        )
        sql, _ = optimizer.build_optimized_query(qr, "s", "t_items")

    assert "t_items_access_envelope" in sql, (
        "restricted principal — envelope JOIN must appear in generated SQL"
    )
    assert "FALSE" in sql, (
        "restricted principal — envelope WHERE clause must appear in generated SQL"
    )


def test_abac_sql_unconditional_has_no_envelope_join() -> None:
    """Unconditional (blanket allow) principal: generated SQL has no access_envelope JOIN."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="geom")],
            access_filter=AccessFilter.allow_everything(),
        )
        sql, _ = optimizer.build_optimized_query(qr, "s", "t_items")

    assert "t_items_access_envelope" not in sql, (
        "unconditional (blanket allow) — envelope JOIN must be absent from generated SQL"
    )


# ---------------------------------------------------------------------------
# select=* path — same 3 invariants on the early-return branch
# ---------------------------------------------------------------------------

def test_abac_star_unconditional_drops_envelope() -> None:
    """select=* + unconditional filter → access_envelope NOT in result (optimization)."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    af = AccessFilter.allow_everything()
    assert af.is_unconditional is True

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(select=[FieldSelection(field="*")], access_filter=af)
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" not in result_ids, (
        "select=* + allow_everything() — envelope must be dropped (pure JOIN overhead)"
    )


def test_abac_star_restricted_retains_envelope() -> None:
    """select=* + restricted filter → access_envelope IN result (enforcement)."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    restricted_af = AccessFilter.from_clauses(
        allow=[AccessClause((FieldPredicate("_attrs.dept", ("finance",)),))]
    )
    assert restricted_af.is_unconditional is False

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(select=[FieldSelection(field="*")], access_filter=restricted_af)
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "select=* + restricted filter — envelope must be retained for row-level enforcement"
    )


def test_abac_star_no_filter_retains_envelope_fail_closed() -> None:
    """select=* + access_filter=None → access_envelope IN result (fail-closed)."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(select=[FieldSelection(field="*")])  # access_filter=None
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "select=* + access_filter=None — envelope must be retained (fail-closed)"
    )


# ---------------------------------------------------------------------------
# Defense-in-depth: allow_all=True AND deny_all=True → envelope RETAINED
# (#2550 — is_unconditional hardened to include not self.deny_all)
# ---------------------------------------------------------------------------


def test_abac_allow_all_and_deny_all_retains_envelope_explicit_projection() -> None:
    """allow_all=True, deny_all=True must NOT drop the envelope (deny_all wins).

    This combination is not constructable via any factory today, but the
    is_unconditional predicate must exclude it so the envelope is always
    retained for a deny-all principal — even on the explicit-projection path.
    """
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    contradictory_af = AccessFilter(allow_all=True, deny_all=True)
    assert contradictory_af.is_unconditional is False, (
        "deny_all=True must override allow_all for is_unconditional"
    )

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="geom")],
            access_filter=contradictory_af,
        )
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "allow_all=True, deny_all=True — deny_all wins; envelope must be retained"
    )


def test_abac_allow_all_and_deny_all_retains_envelope_star_path() -> None:
    """Same defense-in-depth check on the select=* early-return path."""
    col_config = _make_3sidecar_col_config()
    mock_geom, mock_attr, mock_env = _build_3sidecar_mocks()

    contradictory_af = AccessFilter(allow_all=True, deny_all=True)
    assert contradictory_af.is_unconditional is False

    with patch(_REGISTRY_PATH, side_effect=_sidecar_router(mock_geom, mock_attr, mock_env)):
        optimizer = QueryOptimizer(col_config)
        qr = QueryRequest(
            select=[FieldSelection(field="*")],
            access_filter=contradictory_af,
        )
        result_ids = {sc.sidecar_id for sc in optimizer.determine_required_sidecars(qr)}

    assert "access_envelope" in result_ids, (
        "select=* + allow_all=True, deny_all=True — envelope must be retained"
    )
