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

"""Unit tests for process scope→URL alignment, including asset-targeting work.

Asset-targeting processes (e.g. ``gdal``) declare CATALOG and/or COLLECTION
scope and take ``asset_id`` as a regular ``inputs`` value. They execute at the
catalog mount for a catalog-level asset, or the collection mount for a
collection-level asset — there is no dedicated ``/assets/{asset_id}`` URL
surface. These tests cover the scope-allow rules and the path→inputs injection
(which copies ``catalog_id``/``collection_id`` but never ``asset_id``).
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from dynastore.extensions.processes.processes_service import (
    _allowed_scopes_for,
    _build_execution_links,
    _inject_path_into_inputs,
    _OGC_REL_EXECUTE,
    _validate_process_scope_or_raise,
    router as processes_router,
)
from dynastore.modules.processes import models

PLATFORM = models.ProcessScope.PLATFORM
CATALOG = models.ProcessScope.CATALOG
COLLECTION = models.ProcessScope.COLLECTION


def _process(scopes):
    return models.Process(
        id="gdal",
        title="GDAL Info",
        version="1.0.0",
        scopes=scopes,
        jobControlOptions=[models.JobControlOptions.ASYNC_EXECUTE],
        inputs={},
        outputs={},
    )


def test_allowed_scopes_follow_url_mount():
    assert _allowed_scopes_for(None, None) == frozenset({PLATFORM})
    assert _allowed_scopes_for("c1", None) == frozenset({CATALOG})
    assert _allowed_scopes_for("c1", "col1") == frozenset({COLLECTION})


def test_asset_process_accepted_at_catalog_mount():
    # gdal declares [CATALOG, COLLECTION]; valid at the catalog mount.
    _validate_process_scope_or_raise(
        _process([CATALOG, COLLECTION]), catalog_id="c1", collection_id=None
    )


def test_asset_process_accepted_at_collection_mount():
    _validate_process_scope_or_raise(
        _process([CATALOG, COLLECTION]), catalog_id="c1", collection_id="col1"
    )


def test_catalog_only_process_rejected_at_collection_mount():
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([CATALOG]), catalog_id="c1", collection_id="col1"
        )
    assert exc.value.status_code == 400


def test_collection_only_process_rejected_at_catalog_mount():
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([COLLECTION]), catalog_id="c1", collection_id=None
        )
    assert exc.value.status_code == 400


def test_inject_path_adds_catalog_and_collection_but_not_asset():
    req = models.ExecuteRequest(inputs={"asset_id": "a1"})
    out = _inject_path_into_inputs(req, catalog_id="c1", collection_id="col1")
    # asset_id is a body input, preserved untouched; path ids injected.
    assert out.inputs == {
        "asset_id": "a1",
        "catalog_id": "c1",
        "collection_id": "col1",
    }


def test_inject_path_rejects_conflicting_catalog_id():
    req = models.ExecuteRequest(inputs={"catalog_id": "other"})
    with pytest.raises(HTTPException) as exc:
        _inject_path_into_inputs(req, catalog_id="c1", collection_id=None)
    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# Execute-link HATEOAS hrefs must keep the /processes router-mount prefix.
# Regression guard for #2226: hand-assembled base_url + path dropped the
# prefix, so an OGC client following the rel=execute link hit a 404.
# ---------------------------------------------------------------------------


def _templated_execute_links(scopes):
    """Resolve the canonical-description rel=execute links through the real
    router so url_for reflects the production mount prefix."""
    proc = _process(scopes)
    app = FastAPI()
    app.include_router(processes_router)

    @app.get("/_describe")
    def _describe(request: Request):  # pragma: no cover - exercised via client
        return [link.model_dump() for link in _build_execution_links(proc, request)]

    with TestClient(app) as client:
        return client.get("/_describe").json()


def test_execute_links_preserve_processes_mount_prefix():
    links = _templated_execute_links([PLATFORM, CATALOG, COLLECTION])
    assert len(links) == 3
    by_href = {link["href"] for link in links}

    for link in links:
        assert link["rel"] == _OGC_REL_EXECUTE
        assert link["templated"] is True
        assert link["method"] == "POST"
        # The bug: sentinels must be restored to RFC 6570 template vars.
        assert "__catalog_id__" not in link["href"]
        assert "__collection_id__" not in link["href"]

    # Every href carries the /processes router-mount prefix (the dropped
    # segment in #2226) and the correct templated path variables.
    assert any(h.endswith("/processes/processes/gdal/execution") for h in by_href)
    assert any(
        h.endswith("/processes/catalogs/{catalog_id}/processes/gdal/execution")
        for h in by_href
    )
    assert any(
        h.endswith(
            "/processes/catalogs/{catalog_id}/collections/{collection_id}"
            "/processes/gdal/execution"
        )
        for h in by_href
    )


def test_scope_mismatch_hint_includes_processes_prefix():
    # A 400 scope-mismatch error advertises the valid routes; those hints
    # must match the real mounted paths (incl. the /processes prefix).
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([PLATFORM]), catalog_id="c1", collection_id=None
        )
    assert "/processes/processes/{process_id}/execution" in exc.value.detail
