"""Unit tests for asset-scoped execution support in the OGC Processes ext.

Asset-scoped processes (e.g. ``gdal``) now execute at the standard mount
``/catalogs/{c}[/collections/{col}]/assets/{a}/processes/{id}/execution``.
The ``asset_id`` is taken from the URL path and injected into ``inputs`` so the
task resolves the asset itself. These tests cover the scope-allow rules and the
path→inputs injection (importing the module also asserts the new routes
register without error).
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from dynastore.extensions.processes.processes_service import (
    _allowed_scopes_for,
    _inject_path_into_inputs,
    _validate_process_scope_or_raise,
)
from dynastore.modules.processes import models

ASSET = models.ProcessScope.ASSET
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


def test_asset_id_resolves_to_asset_scope_regardless_of_collection():
    assert _allowed_scopes_for("c1", "col1", "a1") == frozenset({ASSET})
    assert _allowed_scopes_for("c1", None, "a1") == frozenset({ASSET})


def test_asset_process_accepted_at_asset_mount():
    # ASSET-scoped process at an asset mount: allowed.
    _validate_process_scope_or_raise(
        _process([ASSET]), catalog_id="c1", collection_id="col1", asset_id="a1"
    )


def test_asset_process_rejected_at_collection_mount():
    # Without asset_id the asset process cannot execute at a collection URL.
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([ASSET]), catalog_id="c1", collection_id="col1", asset_id=None
        )
    assert exc.value.status_code == 400


def test_collection_process_rejected_at_asset_mount():
    with pytest.raises(HTTPException) as exc:
        _validate_process_scope_or_raise(
            _process([COLLECTION]),
            catalog_id="c1",
            collection_id="col1",
            asset_id="a1",
        )
    assert exc.value.status_code == 400


def test_inject_path_adds_asset_id():
    req = models.ExecuteRequest(inputs={})
    out = _inject_path_into_inputs(
        req, catalog_id="c1", collection_id="col1", asset_id="a1"
    )
    assert out.inputs == {"catalog_id": "c1", "collection_id": "col1", "asset_id": "a1"}


def test_inject_path_rejects_conflicting_asset_id():
    req = models.ExecuteRequest(inputs={"asset_id": "other"})
    with pytest.raises(HTTPException) as exc:
        _inject_path_into_inputs(
            req, catalog_id="c1", collection_id=None, asset_id="a1"
        )
    assert exc.value.status_code == 400
