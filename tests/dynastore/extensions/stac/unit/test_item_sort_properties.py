"""Unit tests for ``properties.*`` sortby support in STAC item search.

Covers:
- ``_parse_item_sort_entries`` / ``_parse_item_sort_order_by``: parsing, direction,
  alias assignment, SQL-safety validation, error cases.
- ``_build_read_search_body`` (ES driver): ``sort`` clause is built from
  ``request.sortby`` via ``parse_sort``/``resolve_es_field_path``.
- ``_maybe_dispatch_to_es_search``: ``sortby`` is threaded from the search
  request onto the ``QueryRequest`` handed to the driver.
"""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.stac.search import (
    _maybe_dispatch_to_es_search,
    _parse_item_sort_entries,
    _parse_item_sort_order_by,
)
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.models.shared_models import Feature


# ---------------------------------------------------------------------------
# Helpers shared across all test sections
# ---------------------------------------------------------------------------


def _feat(fid: str, collection: Optional[str] = None) -> Feature:
    d: Dict[str, Any] = {
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    if collection:
        d["collection"] = collection
    return Feature.model_validate(d)


@dataclass
class _Resolved:
    driver: Any


# ---------------------------------------------------------------------------
# 1. _parse_item_sort_entries — parsing and SQL-safety
# ---------------------------------------------------------------------------


class TestParseItemSortEntries:
    """Unit tests for the sort-entry parser."""

    def test_direct_column_asc(self):
        entries = _parse_item_sort_entries(["+datetime"])
        assert len(entries) == 1
        assert entries[0].order_by_expr == "valid_from ASC"
        assert entries[0].pg_alias is None

    def test_direct_column_desc(self):
        entries = _parse_item_sort_entries(["-id"])
        assert len(entries) == 1
        assert entries[0].order_by_expr == "id DESC"
        assert entries[0].pg_alias is None

    def test_direct_column_no_prefix_defaults_asc(self):
        entries = _parse_item_sort_entries(["id"])
        assert entries[0].order_by_expr == "id ASC"

    def test_multiple_direct_columns(self):
        entries = _parse_item_sort_entries(["+datetime", "-id"])
        assert len(entries) == 2
        assert entries[0].order_by_expr == "valid_from ASC"
        assert entries[1].order_by_expr == "id DESC"

    def test_properties_sort_assigns_alias(self):
        entries = _parse_item_sort_entries(["-properties.eo:cloud_cover"])
        assert len(entries) == 1
        e = entries[0]
        assert e.pg_alias == "_sort0"
        assert e.order_by_expr == "_sort0 DESC"
        assert e.pg_raw_select_template is not None

    def test_properties_sort_template_expansion(self):
        entries = _parse_item_sort_entries(["-properties.eo:cloud_cover"])
        expanded = entries[0].pg_raw_select_template.format(sidecar="sc_attributes")
        assert expanded == "sc_attributes.attributes->>'eo:cloud_cover' AS _sort0"

    def test_properties_sort_multiple_aliases_are_distinct(self):
        entries = _parse_item_sort_entries([
            "+properties.eo:cloud_cover",
            "-properties.s2:dark_features_percentage",
        ])
        assert entries[0].pg_alias == "_sort0"
        assert entries[1].pg_alias == "_sort1"
        assert entries[0].order_by_expr == "_sort0 ASC"
        assert entries[1].order_by_expr == "_sort1 DESC"

    def test_mixed_direct_and_properties(self):
        entries = _parse_item_sort_entries(["+datetime", "-properties.eo:cloud_cover"])
        assert entries[0].pg_alias is None
        assert entries[1].pg_alias == "_sort0"

    def test_properties_plain_field_no_namespace(self):
        entries = _parse_item_sort_entries(["+properties.title"])
        assert entries[0].pg_alias == "_sort0"
        expanded = entries[0].pg_raw_select_template.format(sidecar="sc_attributes")
        assert "->>'title'" in expanded

    def test_properties_with_hyphen(self):
        entries = _parse_item_sort_entries(["+properties.created-at"])
        assert entries[0].pg_alias == "_sort0"

    def test_empty_list_returns_empty(self):
        assert _parse_item_sort_entries([]) == []

    def test_none_returns_empty(self):
        assert _parse_item_sort_entries(None) == []

    def test_unknown_top_level_field_raises(self):
        with pytest.raises(ValueError, match="Invalid item sort field"):
            _parse_item_sort_entries(["+nonexistent_xyz"])

    def test_unknown_field_error_mentions_valid_fields(self):
        with pytest.raises(ValueError, match="valid_from"):
            _parse_item_sort_entries(["+unknown_field"])

    def test_sql_injection_via_single_quote_rejected(self):
        with pytest.raises(ValueError, match="unsafe characters"):
            _parse_item_sort_entries(["-properties.evil'quote"])

    def test_hyphenated_property_key_allowed(self):
        """Hyphens are valid in STAC property keys (e.g. 'processing-level');
        a double-hyphen inside single-quoted JSONB accessor is also safe because
        SQL comments (--) are only parsed outside string literals.
        """
        entries = _parse_item_sort_entries(["+properties.a--comment"])
        assert entries[0].pg_alias == "_sort0"

    def test_sql_injection_via_semicolon_rejected(self):
        with pytest.raises(ValueError, match="unsafe characters"):
            _parse_item_sort_entries(["+properties.a;drop"])

    def test_sql_injection_via_space_rejected(self):
        with pytest.raises(ValueError, match="unsafe characters"):
            _parse_item_sort_entries(["+properties.a b"])

    def test_properties_prefix_alone_is_rejected(self):
        """'properties.' with no tail must not produce a sort alias."""
        with pytest.raises(ValueError, match="unsafe characters|Invalid item sort field"):
            _parse_item_sort_entries(["+properties."])


# ---------------------------------------------------------------------------
# 2. _parse_item_sort_order_by — public ORDER BY clause wrapper
# ---------------------------------------------------------------------------


class TestParseItemSortOrderBy:
    def test_default_on_none(self):
        assert _parse_item_sort_order_by(None) == "valid_from DESC, id"

    def test_default_on_empty(self):
        assert _parse_item_sort_order_by([]) == "valid_from DESC, id"

    def test_datetime_alias(self):
        assert _parse_item_sort_order_by(["+datetime"]) == "valid_from ASC"

    def test_direct_desc(self):
        assert _parse_item_sort_order_by(["-id"]) == "id DESC"

    def test_properties_returns_alias(self):
        result = _parse_item_sort_order_by(["-properties.eo:cloud_cover"])
        assert result == "_sort0 DESC"

    def test_mixed_order_by(self):
        result = _parse_item_sort_order_by(["+datetime", "-properties.eo:cloud_cover"])
        assert result == "valid_from ASC, _sort0 DESC"


# ---------------------------------------------------------------------------
# 3. ES driver _build_read_search_body — sort clause injected
# ---------------------------------------------------------------------------


class TestEsDriverSortClause:
    """Validate that _build_read_search_body produces a ``sort`` key when
    the QueryRequest carries a ``sortby`` list.
    """

    def _build(self, sortby: Optional[List[str]] = None, collections=None):
        from dynastore.modules.storage.drivers.elasticsearch import _ItemsElasticsearchBase
        from dynastore.modules.elasticsearch.items_query import PUBLIC_ENVELOPE_FIELDS
        req = QueryRequest(
            collections=collections or ["col-a"],
            limit=10,
            offset=0,
            sortby=sortby,
        )
        body, params = _ItemsElasticsearchBase._build_read_search_body(
            "col-a", req, 10, 0, PUBLIC_ENVELOPE_FIELDS
        )
        return body, params

    def test_no_sortby_has_no_sort_key(self):
        body, _ = self._build(sortby=None)
        assert "sort" not in body

    def test_datetime_sortby_asc(self):
        body, _ = self._build(sortby=["+datetime"])
        assert "sort" in body
        sort = body["sort"]
        # parse_sort maps bare "datetime" → ES field "datetime" (top-level passthrough
        # via resolve_es_field_path since the field doesn't start with "properties.")
        asc_entry = next(
            (c for c in sort if "datetime" in c and "properties" not in "datetime"),
            None,
        )
        assert asc_entry is not None, f"expected datetime entry in {sort}"
        assert asc_entry["datetime"]["order"] == "asc"

    def test_id_sortby_desc(self):
        body, _ = self._build(sortby=["-id"])
        assert "sort" in body
        sort = body["sort"]
        id_entry = next(c for c in sort if "id" in c)
        assert id_entry["id"]["order"] == "desc"

    def test_properties_known_field(self):
        """properties.datetime is Tier-1 and should pass through unchanged."""
        body, _ = self._build(sortby=["+properties.datetime"])
        assert "sort" in body

    def test_properties_extension_field_known_passthrough(self):
        """``eo:cloud_cover`` is a Tier-1 known field; ``resolve_es_field_path``
        passes it through unchanged (no extras redirect).
        """
        body, _ = self._build(sortby=["-properties.eo:cloud_cover"])
        assert "sort" in body
        sort = body["sort"]
        known_entry = next(
            (c for c in sort if "properties.eo:cloud_cover" in c),
            None,
        )
        assert known_entry is not None, f"expected eo:cloud_cover entry in {sort}"
        assert known_entry["properties.eo:cloud_cover"]["order"] == "desc"

    def test_properties_truly_unknown_field_routes_to_extras(self):
        """An unknown extension field (``foo:bar``) is redirected to
        ``properties.extras.foo:bar`` by ``resolve_es_field_path``.
        """
        body, _ = self._build(sortby=["-properties.foo:bar"])
        assert "sort" in body
        sort = body["sort"]
        extras_entry = next(
            (c for c in sort if any("extras" in k for k in c)),
            None,
        )
        assert extras_entry is not None, f"expected extras entry in {sort}"
        key = next(k for k in extras_entry if "extras" in k)
        assert extras_entry[key]["order"] == "desc"

    def test_multiple_sortby_entries(self):
        body, _ = self._build(sortby=["+datetime", "-properties.eo:cloud_cover"])
        assert "sort" in body
        # At least the datetime entry + the extras entry + _score
        assert len(body["sort"]) >= 2


# ---------------------------------------------------------------------------
# 4. _maybe_dispatch_to_es_search — sortby threaded to QueryRequest
# ---------------------------------------------------------------------------


class _FakeEsItemsDriver:
    is_es_items_driver = True
    supports_cql_es = True
    capabilities = frozenset({Capability.READ})

    def __init__(self, features=None, total=1):
        self._features = features or []
        self._total = total
        self.last_request: Optional[QueryRequest] = None

    async def read_entities(
        self, catalog_id, collection_id, *,
        entity_ids=None, request=None, context=None,
        limit=100, offset=0, db_resource=None,
    ):
        self.last_request = request
        for f in self._features:
            yield f

    async def count_entities(self, catalog_id, collection_id, *, request=None, db_resource=None):
        return self._total


@dataclass
class _SearchRequest:
    collections: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[Any] = None
    intersects: Optional[Any] = None
    datetime: Optional[str] = None
    filter: Optional[Any] = None
    limit: int = 10
    offset: int = 0
    sortby: Optional[List[str]] = None


def _patch_resolver(monkeypatch, driver):
    import dynastore.modules.storage.router as _router
    async def _fake(cat, cid=None, **_kw):
        return _Resolved(driver=driver)
    monkeypatch.setattr(_router, "get_items_search_driver", _fake)


@pytest.mark.asyncio
async def test_sortby_threaded_to_query_request(monkeypatch):
    """sortby on the ItemSearchRequest is forwarded to the ES QueryRequest."""
    drv = _FakeEsItemsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, drv)

    sreq = _SearchRequest(
        collections=["col-a"],
        sortby=["+datetime", "-properties.eo:cloud_cover"],
    )
    result = await _maybe_dispatch_to_es_search("cat-x", sreq)
    assert result is not None, "expected ES dispatch to succeed"
    assert drv.last_request is not None
    assert drv.last_request.sortby == ["+datetime", "-properties.eo:cloud_cover"]


@pytest.mark.asyncio
async def test_no_sortby_leaves_request_sortby_none(monkeypatch):
    """When sortby is absent, QueryRequest.sortby stays None."""
    drv = _FakeEsItemsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, drv)

    sreq = _SearchRequest(collections=["col-a"])
    result = await _maybe_dispatch_to_es_search("cat-x", sreq)
    assert result is not None
    assert drv.last_request is not None
    assert drv.last_request.sortby is None


# ---------------------------------------------------------------------------
# 5. Fix 1 — COLUMNAR sidecar raises ValueError for properties.* sort
# ---------------------------------------------------------------------------


def _make_search_request_with_prop_sort(collection_id: str = "col-a") -> Any:
    """Build a minimal ItemSearchRequest with a properties.* sortby."""
    from dynastore.extensions.stac.search import ItemSearchRequest

    return ItemSearchRequest(
        catalog_id="cat-x",
        collections=[collection_id],
        sortby=["-properties.eo:cloud_cover"],
    )


def _make_driver_config(
    storage_mode_str: str, attr_schema=None
) -> Any:
    """Build a minimal ItemsPostgresqlDriverConfig with the given storage mode."""
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeStorageMode,
        AttributeSchemaEntry,
        FeatureAttributeSidecarConfig,
        PostgresType,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    schema = attr_schema or [
        AttributeSchemaEntry(name="eo_cloud_cover", type=PostgresType.TEXT),
    ]
    mode = AttributeStorageMode(storage_mode_str)
    sidecars: list = [GeometriesSidecarConfig()]
    if mode != AttributeStorageMode.JSONB:
        sidecars.append(
            FeatureAttributeSidecarConfig(
                storage_mode=mode,
                attribute_schema=schema,
            )
        )
    else:
        sidecars.append(FeatureAttributeSidecarConfig(storage_mode=mode))
    return ItemsPostgresqlDriverConfig(sidecars=sidecars, physical_table="items")


def _make_driver_config_no_attr_sidecar() -> Any:
    """Build a config with NO attributes sidecar."""
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    return ItemsPostgresqlDriverConfig(
        sidecars=[GeometriesSidecarConfig()],
        physical_table="items",
    )


@pytest.mark.asyncio
async def test_columnar_sidecar_property_sort_raises_value_error(monkeypatch):
    """properties.* sort against a COLUMNAR collection must raise ValueError → 400."""
    import dynastore.extensions.stac.search as search_mod
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeStorageMode,
    )
    from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
    from dynastore.modules.stac.stac_config import StacPluginConfig

    col_id = "col-columnar"
    cfg = _make_driver_config("columnar")

    # Patch _maybe_dispatch_to_es_search to force PG path
    monkeypatch.setattr(
        search_mod, "_maybe_dispatch_to_es_search",
        AsyncMock(return_value=None),
    )

    # Patch CatalogsProtocol
    catalogs_mock = MagicMock()
    catalogs_mock.list_collections = AsyncMock(return_value=[])
    catalogs_mock.get_collection_column_names = AsyncMock(return_value=[])
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="test_schema")
    monkeypatch.setattr(
        "dynastore.extensions.stac.search.get_protocol",
        lambda proto: catalogs_mock,
    )

    # Patch router.get_driver so _check_layer_def returns our COLUMNAR config
    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=cfg)

    async def _fake_get_driver(op, cat_id, cid):
        return fake_driver

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver",
        _fake_get_driver,
    )

    req = _make_search_request_with_prop_sort(col_id)

    with pytest.raises(ValueError, match="COLUMNAR"):
        await search_mod.search_items(
            None,  # type: ignore[arg-type]  # db_resource unused before the guard
            req,
            StacPluginConfig(),
        )


@pytest.mark.asyncio
async def test_no_attr_sidecar_property_sort_raises_value_error(monkeypatch):
    """properties.* sort against a collection with no attributes sidecar must raise ValueError."""
    import dynastore.extensions.stac.search as search_mod
    from dynastore.modules.stac.stac_config import StacPluginConfig

    col_id = "col-no-attr"
    cfg = _make_driver_config_no_attr_sidecar()

    monkeypatch.setattr(
        search_mod, "_maybe_dispatch_to_es_search",
        AsyncMock(return_value=None),
    )
    catalogs_mock = MagicMock()
    catalogs_mock.list_collections = AsyncMock(return_value=[])
    catalogs_mock.get_collection_column_names = AsyncMock(return_value=[])
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="test_schema")
    monkeypatch.setattr(
        "dynastore.extensions.stac.search.get_protocol",
        lambda proto: catalogs_mock,
    )
    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=cfg)

    async def _fake_get_driver(op, cat_id, cid):
        return fake_driver

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver",
        _fake_get_driver,
    )

    req = _make_search_request_with_prop_sort(col_id)

    with pytest.raises(ValueError, match="no attributes sidecar"):
        await search_mod.search_items(
            None,  # type: ignore[arg-type]  # db_resource unused before the guard
            req,
            StacPluginConfig(),
        )


@pytest.mark.asyncio
async def test_jsonb_sidecar_property_sort_does_not_raise_on_guard(monkeypatch):
    """JSONB sidecar must pass the COLUMNAR guard (may still fail later for DB)."""
    import dynastore.extensions.stac.search as search_mod
    from dynastore.modules.stac.stac_config import StacPluginConfig

    col_id = "col-jsonb"
    cfg = _make_driver_config("jsonb")

    monkeypatch.setattr(
        search_mod, "_maybe_dispatch_to_es_search",
        AsyncMock(return_value=None),
    )
    catalogs_mock = MagicMock()
    catalogs_mock.list_collections = AsyncMock(return_value=[])
    catalogs_mock.get_collection_column_names = AsyncMock(return_value=[])
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="test_schema")
    monkeypatch.setattr(
        "dynastore.extensions.stac.search.get_protocol",
        lambda proto: catalogs_mock,
    )
    fake_driver = MagicMock()
    fake_driver.get_driver_config = AsyncMock(return_value=cfg)

    async def _fake_get_driver(op, cat_id, cid):
        return fake_driver

    monkeypatch.setattr(
        "dynastore.modules.storage.router.get_driver",
        _fake_get_driver,
    )

    # Stub build_optimized_query to avoid DB; it's called AFTER the guard.
    monkeypatch.setattr(
        "dynastore.modules.catalog.query_optimizer.QueryOptimizer.build_optimized_query",
        lambda self, *a, **kw: ("SELECT 1", {}),
    )

    req = _make_search_request_with_prop_sort(col_id)

    # The COLUMNAR guard must NOT raise for a JSONB collection.  Any other
    # exception (DB connection failure, etc.) is acceptable here — the test
    # asserts only that the guard itself does not fire.
    try:
        await search_mod.search_items(
            None,  # type: ignore[arg-type]  # db_resource unused before the guard
            req,
            StacPluginConfig(),
        )
    except ValueError as exc:
        assert "COLUMNAR" not in str(exc) and "no attributes sidecar" not in str(exc), (
            f"COLUMNAR guard unexpectedly raised for JSONB collection: {exc}"
        )
    except Exception:
        # Any non-ValueError exception (DB error, QueryExecutionError, …) means
        # the guard passed — exactly the postcondition we want.
        pass


# ---------------------------------------------------------------------------
# 6. Fix 2 — ES multi-field sort produces exactly one trailing _score
# ---------------------------------------------------------------------------


class TestEsDriverMultiFieldSort:
    """Validate that _build_read_search_body emits exactly one _score tiebreaker
    when multiple sortby entries are requested.
    """

    def _build(self, sortby: Optional[List[str]] = None, collections=None):
        from dynastore.modules.storage.drivers.elasticsearch import _ItemsElasticsearchBase
        from dynastore.modules.elasticsearch.items_query import PUBLIC_ENVELOPE_FIELDS

        req = QueryRequest(
            collections=collections or ["col-a"],
            limit=10,
            offset=0,
            sortby=sortby,
        )
        body, params = _ItemsElasticsearchBase._build_read_search_body(
            "col-a", req, 10, 0, PUBLIC_ENVELOPE_FIELDS
        )
        return body, params

    def _count_score_entries(self, sort: list) -> int:
        return sum(1 for c in sort if "_score" in c)

    def test_single_field_has_one_score(self):
        body, _ = self._build(sortby=["+datetime"])
        assert "sort" in body
        sort = body["sort"]
        assert self._count_score_entries(sort) == 1
        assert sort[-1] == {"_score": {"order": "desc"}}

    def test_two_fields_have_exactly_one_score_at_end(self):
        """[+datetime, -properties.eo:cloud_cover] → [datetime, eo:cloud_cover, _score]."""
        body, _ = self._build(sortby=["+datetime", "-properties.eo:cloud_cover"])
        assert "sort" in body
        sort = body["sort"]
        # Exactly one _score tiebreaker, at the tail.
        assert self._count_score_entries(sort) == 1
        assert sort[-1] == {"_score": {"order": "desc"}}
        # datetime field comes before eo:cloud_cover field.
        field_keys = [list(c.keys())[0] for c in sort if "_score" not in c]
        assert field_keys[0] == "datetime"
        assert "eo:cloud_cover" in field_keys[1]

    def test_three_fields_have_exactly_one_score_at_end(self):
        body, _ = self._build(sortby=["+id", "-properties.eo:cloud_cover", "+datetime"])
        sort = body["sort"]
        assert self._count_score_entries(sort) == 1
        assert sort[-1] == {"_score": {"order": "desc"}}
        non_score = [c for c in sort if "_score" not in c]
        assert len(non_score) == 3

    def test_sort_order_preserved(self):
        """Field ordering must match the sortby list order."""
        body, _ = self._build(sortby=["-id", "+datetime"])
        sort = body["sort"]
        non_score = [c for c in sort if "_score" not in c]
        assert list(non_score[0].keys())[0] == "id"
        assert non_score[0]["id"]["order"] == "desc"
        assert list(non_score[1].keys())[0] == "datetime"
        assert non_score[1]["datetime"]["order"] == "asc"

    def test_single_entry_byte_identical_to_before(self):
        """Single-entry sort: shape must be [{field: {order}}, {_score: {order}}]."""
        body, _ = self._build(sortby=["-id"])
        sort = body["sort"]
        assert len(sort) == 2
        assert "id" in sort[0]
        assert sort[0]["id"]["order"] == "desc"
        assert sort[1] == {"_score": {"order": "desc"}}
