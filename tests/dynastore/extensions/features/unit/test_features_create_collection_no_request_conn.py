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

"""#2831 (F2) — OGC Features ``create_collection`` no longer passes a
request-scoped connection into ``_ogc_create_collection``.

STAC's ``create_stac_collection`` always passed ``None`` so
``CollectionService.create_collection`` provisions its own dedicated
connection (``managed_transaction(db_resource or self.engine)``) instead of
running under the caller's request transaction. Features passed the
request-scoped ``conn`` instead — this pins the fix: Features now matches
STAC and passes ``None``.
"""
from __future__ import annotations

import inspect

import pytest


@pytest.mark.asyncio
async def test_create_collection_passes_none_not_request_conn(monkeypatch):
    from dynastore.extensions.features.features_service import OGCFeaturesService
    from dynastore.extensions.features import ogc_models

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    captured = {}

    async def _fake_ogc_create_collection(catalog_id, collection_dict, language, db_resource):
        captured["db_resource"] = db_resource
        return "created"

    monkeypatch.setattr(
        svc, "_ogc_create_collection", _fake_ogc_create_collection, raising=False
    )

    collection_def = ogc_models.CollectionDefinition(id="col1", title="Col 1")

    result = await svc.create_collection(
        catalog_id="cat1",
        collection_def=collection_def,
        language="en",
    )

    assert result == "created"
    assert captured["db_resource"] is None, (
        "create_collection must pass None (not a request-scoped connection) "
        "into _ogc_create_collection, so collection creation provisions its "
        "own dedicated connection instead of running under the caller's "
        "request transaction (#2831)."
    )


def test_create_collection_signature_has_no_request_conn_param():
    """Source-level pin: no leftover unused ``conn: AsyncConnection =
    Depends(get_async_connection)`` dependency on this endpoint — Features'
    create_collection must not open a request-scoped transaction it never
    uses."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    sig = inspect.signature(OGCFeaturesService.create_collection)
    assert "conn" not in sig.parameters, (
        "create_collection must not declare an unused request-scoped `conn` "
        "dependency parameter (#2831)."
    )
