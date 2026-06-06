#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Per-protocol geometry-fidelity read routing tests.

Verifies that each protocol passes the correct routing hints to the
driver layer — EXACT_READ_HINTS for WFS/OGC-Features/DWH-tiled/export,
and opt-in behaviour for STAC/Records (no hints by default, EXACT_READ_HINTS
when geometry=exact is requested).

All tests are unit-level: they mock the ``items_protocol.stream_items`` call
(or ``dispatch_or_stream_items`` as patched in the service) to capture which
``hints`` argument was passed.  No live database or Elasticsearch is required.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import dynastore.extensions.tools.query as query_mod
from dynastore.models.query_builder import QueryResponse
from dynastore.modules.storage.hints import EXACT_READ_HINTS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _feature(fid: str):
    from geojson_pydantic import Feature as _F
    return _F(type="Feature", id=fid, geometry=None, properties={"name": fid})


def _qr(cat: str, col: str):
    async def _gen():
        if False:
            yield  # pragma: no cover
    return QueryResponse(items=_gen(), total_count=0, catalog_id=cat, collection_id=col)


def _make_http_request(path: str = "/", query_string: bytes = b""):
    from starlette.requests import Request
    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "http",
        "server": ("test", 80),
        "path": path,
        "raw_path": path.encode(),
        "query_string": query_string,
        "headers": [(b"host", b"test")],
        "root_path": "",
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# dispatch_or_stream_items forwards hints to stream_items
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_or_stream_items_forwards_hints():
    """hints kwarg must be forwarded to items_protocol.stream_items."""
    received: dict = {}

    class _FakeProtocol:
        async def stream_items(self, **kwargs):
            received["hints"] = kwargs.get("hints", frozenset())
            return _qr("cat", "col")

    from dynastore.extensions.tools.query import dispatch_or_stream_items
    from dynastore.models.query_builder import QueryRequest

    await dispatch_or_stream_items(
        _FakeProtocol(),
        catalog_id="cat",
        collection_id="col",
        query_request=QueryRequest(),
        consumer=None,
        hints=EXACT_READ_HINTS,
    )

    assert received["hints"] == EXACT_READ_HINTS


@pytest.mark.asyncio
async def test_dispatch_or_stream_items_default_hints_empty():
    """Default hints must be empty so existing callers are unaffected."""
    received: dict = {}

    class _FakeProtocol:
        async def stream_items(self, **kwargs):
            received["hints"] = kwargs.get("hints", "NOT_SET")
            return _qr("cat", "col")

    from dynastore.extensions.tools.query import dispatch_or_stream_items
    from dynastore.models.query_builder import QueryRequest

    await dispatch_or_stream_items(
        _FakeProtocol(),
        catalog_id="cat",
        collection_id="col",
        query_request=QueryRequest(),
        consumer=None,
    )

    assert received["hints"] == frozenset()


@pytest.mark.asyncio
async def test_dispatch_or_stream_items_prefers_search_dispatch():
    """When search_dispatch is supplied, stream_items must NOT be called."""
    called = {"stream": False}

    class _FakeProtocol:
        async def stream_items(self, **kwargs):
            called["stream"] = True
            return _qr("cat", "col")

    from dynastore.extensions.tools.query import dispatch_or_stream_items
    from dynastore.models.query_builder import QueryRequest

    sentinel = _qr("cat", "col")
    result = await dispatch_or_stream_items(
        _FakeProtocol(),
        catalog_id="cat",
        collection_id="col",
        query_request=QueryRequest(),
        consumer=None,
        search_dispatch=sentinel,
        hints=EXACT_READ_HINTS,
    )

    assert called["stream"] is False
    assert result is sentinel


# ---------------------------------------------------------------------------
# WFS GetFeature — always EXACT
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wfs_get_feature_passes_exact_hints(monkeypatch):
    """WFS GetFeature must pass EXACT_READ_HINTS to stream_items."""
    import dynastore.extensions.wfs.wfs_service as wfs_mod
    from dynastore.extensions.wfs.wfs_service import WFSService

    received: dict = {}

    class _FakeCatalogs:
        async def get_collection(self, *a, **kw):
            return {"id": "col"}

        async def stream_items(self, **kwargs):
            received["hints"] = kwargs.get("hints", frozenset())
            return _qr("cat", "col")

    svc = WFSService.__new__(WFSService)

    async def _get_catalogs():
        return _FakeCatalogs()

    async def _get_configs():
        cfg = SimpleNamespace(cache_on_demand=False)
        return SimpleNamespace(
            get_config=lambda cls, **kw: _acoro(cfg)(),
        )

    async def _get_storage():
        return SimpleNamespace()

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    monkeypatch.setattr(svc, "_get_storage_service", _get_storage, raising=False)

    # Patch ABAC helpers to no-ops
    monkeypatch.setattr(
        wfs_mod, "collection_uses_pg_access_envelope",
        lambda *a, **kw: _acoro(False)(),
        raising=False,
    )

    # Patch the top-level import inside wfs_service at parse-time
    import dynastore.modules.storage.access_scope as access_scope_mod
    monkeypatch.setattr(access_scope_mod, "collection_uses_pg_access_envelope",
                        lambda *a, **kw: _acoro(False)())

    request = _make_http_request("/wfs/cat")
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typename": "cat:col",
    }
    await svc.get_feature(
        request=request,
        conn=None,
        params=params,
        root_wfs_url="http://test/wfs",
        catalog_id_from_path="cat",
    )

    assert received.get("hints") == EXACT_READ_HINTS, (
        f"WFS GetFeature must pass EXACT_READ_HINTS; got {received.get('hints')}"
    )


# ---------------------------------------------------------------------------
# OGC Features /items — always EXACT, ES fast-path skipped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ogc_features_get_items_passes_exact_hints(monkeypatch):
    """OGC Features get_items must pass EXACT_READ_HINTS via dispatch_or_stream_items."""
    import dynastore.extensions.features.features_service as feat_mod

    received: dict = {}

    async def _fake_dispatch(items_protocol, *, hints=frozenset(), **kwargs):
        received["hints"] = hints
        return _qr("cat", "col")

    monkeypatch.setattr(feat_mod, "dispatch_or_stream_items", _fake_dispatch)

    # maybe_dispatch_items_to_search_driver must NOT be called for OGC Features.
    called_maybe = {"yes": False}

    async def _spy_maybe(**kwargs):
        called_maybe["yes"] = True
        return None

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy_maybe)

    from dynastore.extensions.features.features_service import OGCFeaturesService
    from dynastore.extensions.features.features_config import FeaturesPluginConfig
    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    async def _get_catalogs():
        return SimpleNamespace(
            get_collection=_acoro({"id": "col"}),
            get_collection_column_names=_acoro([]),
        )

    plugin_cfg = FeaturesPluginConfig()

    async def _get_configs():
        async def _get_config(cls, **kw):
            return plugin_cfg

        return SimpleNamespace(get_config=_get_config)

    async def _get_storage():
        return SimpleNamespace()

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs, raising=False)
    monkeypatch.setattr(svc, "_get_storage_service", _get_storage, raising=False)
    monkeypatch.setattr(svc, "_resolve_crs_srid", _acoro(None), raising=False)
    monkeypatch.setattr(svc, "_resolve_property_names", _acoro(None), raising=False)

    # Patch stream_ogc_features (called after dispatch) to avoid format machinery.
    import dynastore.extensions.features.features_service as feat_mod2

    def _fake_stream_ogc(*a, **kw):
        from starlette.responses import Response
        return Response(content=b"{}", media_type="application/geo+json")

    monkeypatch.setattr(feat_mod2, "stream_ogc_features", _fake_stream_ogc)

    request = _make_http_request("/features/catalogs/cat/collections/col/items")

    await svc.get_items(
        request=request,
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        bbox=None,
        datetime_param=None,
        filter=None,
        filter_lang="cql2-text",
        filter_crs=None,
        properties=None,
        skip_geometry=None,
        return_geometry=None,
        crs=None,
        bbox_crs=None,
        sortby=None,
        f=None,
    )

    assert received.get("hints") == EXACT_READ_HINTS, (
        f"OGC Features must pass EXACT_READ_HINTS; got {received.get('hints')}"
    )
    # The ES fast-path must NOT have been consulted.
    assert called_maybe["yes"] is False, (
        "OGC Features must not call maybe_dispatch_items_to_search_driver"
    )


# ---------------------------------------------------------------------------
# STAC /items — simplified by default, opt-in to exact
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stac_collection_items_default_uses_es_fastpath(monkeypatch):
    """Without geometry=exact, the ES fast-path must be consulted (default unchanged)."""
    import dynastore.extensions.stac.stac_service as stac_mod

    called_dispatch = {"yes": False}

    async def _spy_dispatch(**kwargs):
        called_dispatch["yes"] = True
        return None  # Decline → PG fallback

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield None

    monkeypatch.setattr(stac_mod, "managed_transaction", _fake_txn)

    async def _spy_collection(*a, **kw):
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(stac_mod.stac_generator, "create_item_collection", _spy_collection)

    from dynastore.extensions.stac.stac_service import STACService
    svc = STACService.__new__(STACService)

    monkeypatch.setattr(svc, "_get_catalogs_service",
                        _acoro(SimpleNamespace(get_collection=_acoro({"id": "col"}))),
                        raising=False)
    monkeypatch.setattr(svc, "_get_stac_config", _acoro(SimpleNamespace()), raising=False)

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col",
        request=_make_http_request(),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=frozenset(),  # no hints — default simplified path
    )

    assert called_dispatch["yes"] is True, "Default path must call the ES fast-path dispatch"


@pytest.mark.asyncio
async def test_stac_collection_items_geometry_exact_skips_es_fastpath(monkeypatch):
    """?hints=geometry_exact must bypass the ES fast-path (search_dispatch=None always)."""
    import dynastore.extensions.stac.stac_service as stac_mod

    called_dispatch = {"yes": False}

    async def _boom(**kwargs):
        called_dispatch["yes"] = True
        return "should_not_be_reached"

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _boom)

    @asynccontextmanager
    async def _fake_txn(_engine):
        yield None

    monkeypatch.setattr(stac_mod, "managed_transaction", _fake_txn)

    seen_collection: dict = {}

    async def _spy_collection(*a, **kw):
        seen_collection.update(kw)
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(stac_mod.stac_generator, "create_item_collection", _spy_collection)

    from dynastore.extensions.stac.stac_service import STACService
    svc = STACService.__new__(STACService)
    monkeypatch.setattr(svc, "_get_catalogs_service",
                        _acoro(SimpleNamespace(get_collection=_acoro({"id": "col"}))),
                        raising=False)
    monkeypatch.setattr(svc, "_get_stac_config", _acoro(SimpleNamespace()), raising=False)

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col",
        request=_make_http_request(),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=EXACT_READ_HINTS,
    )

    assert called_dispatch["yes"] is False, (
        "?hints=geometry_exact must skip maybe_dispatch_items_to_search_driver"
    )
    # search_dispatch stays None → create_item_collection falls through to PG path.
    assert seen_collection.get("search_dispatch") is None


# ---------------------------------------------------------------------------
# Records /items — simplified by default, opt-in to exact
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_records_default_uses_es_fastpath(monkeypatch):
    """Without geometry=exact the ES fast-path is consulted (default unchanged)."""

    called = {"dispatch": False}

    async def _spy(**kwargs):
        called["dispatch"] = True
        return None  # decline → PG path

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _spy)

    from dynastore.extensions.records.records_service import RecordsService
    svc = RecordsService.__new__(RecordsService)

    class _FakeCatalogs:
        async def get_collection(self, *a, **kw):
            return {"id": "col"}
        async def get_collection_config(self, *a, **kw):
            return None
        async def stream_items(self, **kw):
            return _qr("cat", "col")

    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(_FakeCatalogs()), raising=False)

    await svc.get_records(
        request=_make_http_request("/records/catalogs/cat/collections/col/items"),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        filter=None,
        sortby=None,
        q=None,
        request_hints=frozenset(),  # no hints — default simplified path
    )

    assert called["dispatch"] is True, "Default path must call the ES fast-path"


@pytest.mark.asyncio
async def test_records_geometry_exact_skips_es_fastpath_and_passes_hints(monkeypatch):
    """?hints=geometry_exact skips the ES fast-path and threads the hint to stream_items."""

    called = {"dispatch": False}

    async def _boom(**kwargs):
        called["dispatch"] = True
        return "should_not_reach"

    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver", _boom)

    received: dict = {}

    async def _fake_dispatch_or_stream(items_protocol, *, hints=frozenset(), **kwargs):
        received["hints"] = hints
        return _qr(kwargs["catalog_id"], kwargs["collection_id"])

    monkeypatch.setattr(query_mod, "dispatch_or_stream_items", _fake_dispatch_or_stream)

    from dynastore.extensions.records.records_service import RecordsService
    svc = RecordsService.__new__(RecordsService)

    class _FakeCatalogs:
        async def get_collection(self, *a, **kw):
            return {"id": "col"}
        async def get_collection_config(self, *a, **kw):
            return None

    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(_FakeCatalogs()), raising=False)

    await svc.get_records(
        request=_make_http_request("/records/catalogs/cat/collections/col/items"),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        filter=None,
        sortby=None,
        q=None,
        request_hints=EXACT_READ_HINTS,
    )

    assert called["dispatch"] is False, "?hints=geometry_exact must skip the ES fast-path"
    assert received.get("hints") == EXACT_READ_HINTS, (
        f"?hints=geometry_exact must thread the geometry_exact hint; got {received.get('hints')}"
    )


@pytest.mark.asyncio
async def test_records_geometry_absent_uses_empty_hints(monkeypatch):
    """When geometry is not set, dispatch_or_stream_items gets empty hints."""

    received: dict = {}

    async def _fake_dispatch_or_stream(items_protocol, *, hints=frozenset(), **kwargs):
        received["hints"] = hints
        return _qr(kwargs["catalog_id"], kwargs["collection_id"])

    monkeypatch.setattr(query_mod, "dispatch_or_stream_items", _fake_dispatch_or_stream)
    monkeypatch.setattr(query_mod, "maybe_dispatch_items_to_search_driver",
                        lambda **kw: _acoro(None)())

    from dynastore.extensions.records.records_service import RecordsService
    svc = RecordsService.__new__(RecordsService)

    class _FakeCatalogs:
        async def get_collection(self, *a, **kw):
            return {"id": "col"}
        async def get_collection_config(self, *a, **kw):
            return None

    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(_FakeCatalogs()), raising=False)

    await svc.get_records(
        request=_make_http_request("/records/catalogs/cat/collections/col/items"),
        catalog_id="cat",
        collection_id="col",
        conn=None,
        limit=10,
        offset=0,
        filter=None,
        sortby=None,
        q=None,
        request_hints=frozenset(),
    )

    assert received.get("hints") == frozenset(), (
        f"Default path must pass empty hints; got {received.get('hints')}"
    )


# ---------------------------------------------------------------------------
# DWH tiled join — EXACT (source-level architectural assertion)
# ---------------------------------------------------------------------------

def test_dwh_tiled_join_uses_exact_hints():
    """stream_normalized_items call for the tiled join must carry EXACT_READ_HINTS.

    We verify by inspecting the source file directly rather than importing the
    module (the tiled-join module requires shapely which is not in the unit-test
    environment).  This is an architectural assertion: confirm the constant
    appears in the call site without importing shapely.
    """
    import importlib.util
    import pathlib

    dwh_pkg_spec = importlib.util.find_spec("dynastore.extensions.dwh")
    assert dwh_pkg_spec is not None and dwh_pkg_spec.origin is not None
    dwh_path = pathlib.Path(dwh_pkg_spec.origin).parent / "dwh.py"
    source = dwh_path.read_text()
    assert "EXACT_READ_HINTS" in source, (
        "EXACT_READ_HINTS must appear in dwh.py (tiled join call site)"
    )
    # Hint.JOIN call (main join) must still be present and unchanged.
    assert "Hint.JOIN" in source, "Hint.JOIN (main join) must remain unchanged"


# ---------------------------------------------------------------------------
# Export features — EXACT
# ---------------------------------------------------------------------------

def test_export_features_passes_exact_hints():
    """export_features producer must call stream_features with EXACT_READ_HINTS.

    Architectural assertion on the source to keep this fast and dependency-free.
    """
    import inspect
    import dynastore.modules.features_exporter.service as export_mod

    source = inspect.getsource(export_mod)
    assert "EXACT_READ_HINTS" in source, (
        "export_features must pass EXACT_READ_HINTS to stream_features"
    )


# ---------------------------------------------------------------------------
# No concrete driver name at any call site (source-level assertion)
# ---------------------------------------------------------------------------

def test_exact_read_hints_not_using_concrete_driver_import():
    """The EXACT_READ_HINTS constant must not reference any concrete driver class.

    This ensures the geometry-fidelity routing is expressed purely via the hint
    vocabulary, not by naming a specific driver.  We check that EXACT_READ_HINTS
    and its siblings in hints.py contain no driver class names.
    """
    import inspect
    from dynastore.modules.storage import hints as hints_mod

    source = inspect.getsource(hints_mod)
    forbidden = (
        "ItemsPostgresqlDriver",
        "ItemsElasticsearchDriver",
        "ElasticsearchDriver",
        "PostgresqlDriver",
    )
    for cls in forbidden:
        assert cls not in source, (
            f"hints.py must not name concrete driver class '{cls}' — "
            "routing must be driver-agnostic"
        )


def test_geometry_fidelity_call_sites_use_hint_constant_not_driver():
    """All modified call sites use EXACT_READ_HINTS, not a driver class name.

    Checks the three service files that import and use EXACT_READ_HINTS (export,
    features, wfs, records, stac) and the core query helper.  For DWH we verify
    via the importlib-located source (shapely unavailable in unit env).
    """
    import inspect
    import importlib.util
    import pathlib
    import dynastore.modules.features_exporter.service as export_mod
    import dynastore.extensions.features.features_service as feat_mod
    import dynastore.extensions.wfs.wfs_service as wfs_mod
    import dynastore.extensions.records.records_service as rec_mod
    import dynastore.extensions.stac.stac_service as stac_mod
    import dynastore.extensions.tools.query as query_mod

    dwh_pkg_spec = importlib.util.find_spec("dynastore.extensions.dwh")
    assert dwh_pkg_spec is not None and dwh_pkg_spec.origin is not None
    dwh_src = (
        pathlib.Path(dwh_pkg_spec.origin).parent / "dwh.py"
    ).read_text()

    # Exact-by-default protocols pass EXACT_READ_HINTS unconditionally.
    assert "EXACT_READ_HINTS" in inspect.getsource(export_mod), "export_features"
    assert "EXACT_READ_HINTS" in inspect.getsource(feat_mod), "OGC Features"
    assert "EXACT_READ_HINTS" in inspect.getsource(wfs_mod), "WFS"
    assert "EXACT_READ_HINTS" in dwh_src, "DWH tiled join"

    # query.py dispatch_or_stream_items must forward hints.
    assert "hints=hints" in inspect.getsource(query_mod), (
        "dispatch_or_stream_items must forward hints to stream_items"
    )

    # Opt-in protocols (STAC, Records) default to the simplified search backend
    # and request the exact tier only under the ``?hints=geometry_exact`` array
    # parameter — they thread the parsed ``request_hints`` and gate the ES
    # fast-path skip on the geometry_exact hint, never EXACT_READ_HINTS
    # unconditionally.
    for mod, name in ((stac_mod, "STAC"), (rec_mod, "Records")):
        src = inspect.getsource(mod)
        assert "request_hints" in src, f"{name} must thread the parsed hints"
        assert "Hint.GEOMETRY_EXACT in request_hints" in src, (
            f"{name} must gate the exact opt-in on the geometry_exact hint"
        )
        assert "EXACT_READ_HINTS" not in src, (
            f"{name} is simplified-by-default; it must not force EXACT_READ_HINTS"
        )


# ---------------------------------------------------------------------------
# Async coroutine helper (shared)
# ---------------------------------------------------------------------------

from contextlib import asynccontextmanager  # noqa: E402


def _acoro(value):
    """Return a coroutine function that resolves to ``value``."""
    async def _inner(*a, **kw):
        return value
    return _inner


# ---------------------------------------------------------------------------
# Regression: ``hints`` is a reserved control param, not an attribute filter
# ---------------------------------------------------------------------------

def test_hints_is_reserved_query_param_not_attribute_filter():
    """``?hints=geometry_exact`` is the per-request routing-hints array, never a
    ``?{property}={value}`` equality filter.

    Regression for the live 500 ``invalid geometry ... "ex" <-- parse error``:
    the OGC ``/items`` routes (STAC + Features) sweep every non-reserved query
    param into a CQL equality filter, so the control param MUST be in
    OGC_RESERVED_QUERY_PARAMS or its value reaches PostGIS as WKT and 500s.
    """
    from dynastore.extensions.tools.query import OGC_RESERVED_QUERY_PARAMS

    assert "hints" in OGC_RESERVED_QUERY_PARAMS

    # And the OGC sweep expression itself must exclude it.
    extra = {
        k: v
        for k, v in {"hints": "geometry_exact", "name": "ital"}.items()
        if k not in OGC_RESERVED_QUERY_PARAMS and v != ""
    }
    assert "hints" not in extra
    assert extra == {"name": "ital"}


def test_hints_param_parses_comma_array_and_repeats():
    """``?hints=`` is an array: comma-joined (``a,b,c``) or repeated; tokens map
    to the Hint vocabulary, unknowns drop, empty → frozenset()."""
    from dynastore.modules.storage.hints import parse_request_hints, Hint

    assert parse_request_hints(["geometry_exact,tiles"]) == frozenset(
        {Hint.GEOMETRY_EXACT, Hint.TILES}
    )
    assert parse_request_hints(["geometry_exact", "tiles"]) == frozenset(
        {Hint.GEOMETRY_EXACT, Hint.TILES}
    )
    assert parse_request_hints(["nope"]) == frozenset()
    assert parse_request_hints(None) == frozenset()
