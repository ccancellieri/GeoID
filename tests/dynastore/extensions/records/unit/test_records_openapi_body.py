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

"""Regression: the Records POST/PUT/PATCH item routes must declare a typed
request body so the generated OpenAPI schema documents a payload editor
(Swagger UI showed none because ``add_records``/``replace_record``/
``update_record`` read a raw ``Request`` instead of a typed model).
"""
from __future__ import annotations

from fastapi import FastAPI

from dynastore.extensions.records.records_service import RecordsService

_ITEMS_PATH = "/records/catalogs/{catalog_id}/collections/{collection_id}/items"
_ITEM_PATH = (
    "/records/catalogs/{catalog_id}/collections/{collection_id}/items/{record_id}"
)


def _build_schema() -> dict:
    app = FastAPI()
    svc = RecordsService()  # type: ignore[reportAbstractUsage]
    app.include_router(svc.router)
    return app.openapi()


def test_add_records_post_has_request_body():
    schema = _build_schema()
    post_op = schema["paths"][_ITEMS_PATH]["post"]
    assert "requestBody" in post_op


def test_add_records_geometry_not_required():
    schema = _build_schema()
    record_schema = schema["components"]["schemas"]["Record-Input"]
    assert "geometry" in record_schema["properties"]
    assert "geometry" not in (record_schema.get("required") or [])


def test_replace_and_update_record_have_request_body():
    schema = _build_schema()
    for method in ("put", "patch"):
        assert "requestBody" in schema["paths"][_ITEM_PATH][method]
