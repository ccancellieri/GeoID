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

"""Unit tests for OGCServiceMixin._require_collection_visible.

Verifies that data routes in Coverages, EDR, DGGS, and Tiles raise 404 for
collections the caller has no visibility grant for, and pass through when IAM
is inactive (resolve_collection_listing_ids returns None).

All tests are hermetic: no database, no live service — visibility is injected
via the request-scoped ContextVar helpers in
dynastore.models.protocols.visibility.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    reset_request_visibility,
    set_request_visibility,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Svc(OGCServiceMixin):
    """Bare concrete subclass that exercises the mixin in isolation."""


def _vis(*principals: str) -> RequestVisibility:
    return RequestVisibility(principals=principals)


def _make_visibility_provider(visible_ids: Optional[frozenset[str]]):
    """Return a stub ListingVisibilityProtocol that yields *visible_ids*.

    ``None`` simulates IAM-off (no contextvar set, handled at the
    resolve_collection_listing_ids level rather than inside the provider).
    A frozenset restricts the listing to those ids.
    """
    from dynastore.models.protocols.access_filter import AccessClause, AccessFilter, FieldPredicate

    if visible_ids is None:
        return None  # caller should NOT set the contextvar at all

    if not visible_ids:
        # deny everything
        flt = AccessFilter.deny_everything()
    else:
        flt = AccessFilter.from_clauses(
            [AccessClause((FieldPredicate("id", tuple(visible_ids)),))]
        )

    class _Provider:
        async def collection_listing_filter(
            self, visibility: RequestVisibility, catalog_id: str
        ) -> AccessFilter:
            return flt

        async def catalog_listing_filter(
            self, visibility: RequestVisibility
        ) -> AccessFilter:
            return flt

        async def asset_listing_filter(
            self, visibility: RequestVisibility, catalog_id: str, collection_id: Optional[str]
        ) -> AccessFilter:
            return flt

    return _Provider()


# ---------------------------------------------------------------------------
# OGCServiceMixin._require_collection_visible — core guard tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_guard_passes_when_iam_inactive():
    """When no RequestVisibility is set (IAM off), the guard does not raise."""
    # Do NOT set the contextvar — simulates no authorization layer mounted.
    svc = _Svc()
    # Should not raise; returns None implicitly.
    await svc._require_collection_visible("cat-1", "col-secret")


@pytest.mark.asyncio
async def test_guard_passes_for_visible_collection():
    """Guard passes when collection_id is in the visible set."""
    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-public", "col-other"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            svc = _Svc()
            await svc._require_collection_visible("cat-1", "col-public")
            # No exception raised — test passes implicitly
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_guard_raises_404_for_hidden_collection():
    """Guard raises HTTPException 404 when collection_id is not in the visible set."""
    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-public"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            svc = _Svc()
            with pytest.raises(HTTPException) as exc_info:
                await svc._require_collection_visible("cat-1", "col-secret")
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_guard_raises_404_for_empty_visible_set():
    """Guard raises 404 when the caller can see NO collections (deny-all)."""
    token = set_request_visibility(_vis("user:anonymous"))
    provider = _make_visibility_provider(frozenset())
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            svc = _Svc()
            with pytest.raises(HTTPException) as exc_info:
                await svc._require_collection_visible("cat-1", "col-any")
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


# ---------------------------------------------------------------------------
# Coverages — get_coverage raises 404 for hidden collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coverages_get_coverage_raises_404_for_hidden_collection():
    """get_coverage raises 404 before touching item resolution."""
    pytest.importorskip("rasterio", reason="rasterio not installed")
    from dynastore.extensions.coverages.coverages_service import CoveragesService

    svc = CoveragesService()
    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_coverage(
                    catalog_id="cat-1",
                    collection_id="col-hidden",
                    subset=None,
                    f="geotiff",
                    request_hints=frozenset(),
                )
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_coverages_get_coverage_domainset_raises_404_for_hidden_collection():
    """get_coverage_domainset raises 404 for a hidden collection."""
    pytest.importorskip("rasterio", reason="rasterio not installed")
    from dynastore.extensions.coverages.coverages_service import CoveragesService

    svc = CoveragesService()
    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_coverage_domainset("cat-1", "col-hidden")
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_coverages_get_coverage_rangetype_raises_404_for_hidden_collection():
    """get_coverage_rangetype raises 404 for a hidden collection."""
    pytest.importorskip("rasterio", reason="rasterio not installed")
    from dynastore.extensions.coverages.coverages_service import CoveragesService

    svc = CoveragesService()
    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_coverage_rangetype("cat-1", "col-hidden")
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


# ---------------------------------------------------------------------------
# EDR — query_position raises 404 for hidden collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_edr_query_position_raises_404_for_hidden_collection():
    """query_position raises 404 before any catalog/item resolution."""
    pytest.importorskip("rasterio", reason="rasterio not installed — EDR imports it")
    from unittest.mock import MagicMock
    from dynastore.extensions.edr.edr_service import EDRService

    svc = EDRService()
    # Stub _require_catalog_ready so it does not look up a live catalog.
    svc._require_catalog_ready = AsyncMock(return_value=None)

    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            request = MagicMock()
            with pytest.raises(HTTPException) as exc_info:
                await svc.query_position(
                    catalog_id="cat-1",
                    collection_id="col-hidden",
                    request=request,
                    coords="POINT(12.5 41.9)",
                    z=None,
                    datetime=None,
                    parameter_name=None,
                    crs=None,
                    f="CoverageJSON",
                )
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


# ---------------------------------------------------------------------------
# DGGS — get_dggs_data raises 404 for hidden collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dggs_get_dggs_data_raises_404_for_hidden_collection():
    """get_dggs_data raises 404 before any config/field/feature resolution."""
    pytest.importorskip("h3", reason="h3 not installed")
    from unittest.mock import MagicMock
    from dynastore.extensions.dggs.dggs_service import DGGSService

    svc = DGGSService()

    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            request = MagicMock()
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_dggs_data(
                    catalog_id="cat-1",
                    collection_id="col-hidden",
                    request=request,
                    zone_level=None,
                    bbox=None,
                    datetime=None,
                    parameter_name=None,
                    dggs_id="H3",
                    request_hints=frozenset(),
                )
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


# ---------------------------------------------------------------------------
# Tiles — get_vector_tile raises 404 for hidden collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tiles_get_vector_tile_raises_404_for_hidden_collection():
    """get_vector_tile raises 404 for a collection not in the visible set."""
    pytest.importorskip("morecantile", reason="morecantile not installed — tiles imports it")
    from unittest.mock import MagicMock
    from dynastore.extensions.tiles.tiles_service import TilesService

    svc = TilesService()
    # Stub _resolve_request_config so it does not need a live ConfigsProtocol.
    svc._resolve_request_config = AsyncMock(return_value=MagicMock())

    token = set_request_visibility(_vis("role:reader"))
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            request = MagicMock()
            background_tasks = MagicMock()
            conn = MagicMock()
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_vector_tile(
                    request=request,
                    dataset="cat-1",
                    tileMatrixSetId="WebMercatorQuad",
                    z=5,
                    x=10,
                    y=10,
                    format="mvt",
                    background_tasks=background_tasks,
                    conn=conn,
                    collections="col-hidden",
                    datetime=None,
                    filter=None,
                    filter_lang="cql2-text",
                    subset=None,
                    simplification=None,
                    simplification_by_zoom=None,
                    simplification_algorithm=None,
                    disable_cache=False,
                    refresh_cache=False,
                )
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)


@pytest.mark.asyncio
async def test_tiles_get_vector_tile_raises_404_when_any_collection_hidden():
    """get_vector_tile raises 404 when ANY of the requested collections is hidden."""
    pytest.importorskip("morecantile", reason="morecantile not installed — tiles imports it")
    from unittest.mock import MagicMock
    from dynastore.extensions.tiles.tiles_service import TilesService

    svc = TilesService()
    svc._resolve_request_config = AsyncMock(return_value=MagicMock())

    token = set_request_visibility(_vis("role:reader"))
    # col-visible is allowed; col-hidden is not in the set
    provider = _make_visibility_provider(frozenset({"col-visible"}))
    try:
        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=lambda proto: provider,
        ):
            request = MagicMock()
            background_tasks = MagicMock()
            conn = MagicMock()
            with pytest.raises(HTTPException) as exc_info:
                await svc.get_vector_tile(
                    request=request,
                    dataset="cat-1",
                    tileMatrixSetId="WebMercatorQuad",
                    z=5,
                    x=10,
                    y=10,
                    format="mvt",
                    background_tasks=background_tasks,
                    conn=conn,
                    collections="col-visible,col-hidden",
                    datetime=None,
                    filter=None,
                    filter_lang="cql2-text",
                    subset=None,
                    simplification=None,
                    simplification_by_zoom=None,
                    simplification_algorithm=None,
                    disable_cache=False,
                    refresh_cache=False,
                )
            assert exc_info.value.status_code == 404
    finally:
        reset_request_visibility(token)
