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

import uuid
from datetime import datetime, timezone

from dynastore.modules.styles.db import _enrich_style_from_row


def _row(**overrides):
    row = {
        "id": uuid.uuid4(),
        "catalog_id": "c_cat1",
        "collection_id": "c_col1",
        "style_id": "demo",
        "title": None,
        "description": None,
        "keywords": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "stylesheets": [],
    }
    row.update(overrides)
    return row


def test_enrichment_uses_external_ids_when_supplied():
    """Regression for #2952 follow-up: once styles.styles.collection_id
    stores the internal id, the response and its self-link href must expose
    the external (public) id, not the internal one — otherwise a client
    following the returned link or reusing the returned collection_id gets
    a 404 on the next call.
    """
    style = _enrich_style_from_row(
        _row(),
        root_url="http://ex",
        external_catalog_id="cat1",
        external_collection_id="col1",
    )

    assert style is not None
    assert style.catalog_id == "cat1"
    assert style.collection_id == "col1"
    assert style.links[0].href == "http://ex/styles/catalogs/cat1/collections/col1/styles/demo"


def test_enrichment_falls_back_to_row_ids_when_no_external_supplied():
    style = _enrich_style_from_row(_row(), root_url="http://ex")

    assert style is not None
    assert style.catalog_id == "c_cat1"
    assert style.collection_id == "c_col1"


def test_enrichment_prefers_row_external_collection_id_over_internal():
    """Cross-catalog listing supplies the external id via a joined column
    rather than an explicit kwarg — mirrors the existing
    ``catalog_external_id`` fallback used by ``list_all_styles``.
    """
    style = _enrich_style_from_row(
        _row(collection_external_id="col1"),
        root_url="http://ex",
        external_catalog_id="cat1",
    )

    assert style is not None
    assert style.collection_id == "col1"
