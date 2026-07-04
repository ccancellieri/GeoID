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

"""Route-registration coverage for the DWH path-convention alignment.

`/dwh/join` predates the platform/catalog/collection convention (the catalog
is read from the request body, not the URL) and is kept only for existing
callers -- it must be marked deprecated. The catalog-scoped routes already
match the convention and must stay live (not deprecated).
"""

from typing import Dict

from fastapi import APIRouter
from fastapi.routing import APIRoute

from dynastore.extensions.dwh.dwh import DwhService


def _make_service() -> DwhService:
    # Bypass __init__ (and its ExtensionProtocol dependencies) -- only the
    # route table built by _register_routes() is under test here.
    service = object.__new__(DwhService)
    service.router = APIRouter(prefix="/dwh", tags=["Data Warehouse API"])
    service._register_routes()
    return service


def _routes_by_path(service: DwhService) -> Dict[str, APIRoute]:
    return {route.path: route for route in service.router.routes if isinstance(route, APIRoute)}


class TestDwhRouteAlignment:
    def test_flat_join_route_is_deprecated(self):
        routes = _routes_by_path(_make_service())
        assert routes["/dwh/join"].deprecated is True

    def test_catalog_scoped_join_route_is_not_deprecated(self):
        routes = _routes_by_path(_make_service())
        assert not routes["/dwh/catalogs/{catalog_id}/join"].deprecated

    def test_catalog_scoped_tiled_join_route_is_not_deprecated(self):
        routes = _routes_by_path(_make_service())
        path = "/dwh/catalogs/{catalog_id}/tiles/{z}/{x}/{y}/join.{format}"
        assert not routes[path].deprecated
