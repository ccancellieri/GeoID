#    Copyright 2025 FAO
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

"""Unit tests for the Stage 6 ``/assets:drift`` endpoint.

The endpoint resolves the engine + bucket + storage_client through
``resolve_reconcile_context`` and then dispatches to ``reconcile_bucket``
in dry-run mode. We mock both boundaries:

* ``resolve_reconcile_context`` returns sentinel objects (no live PG /
  GCS dependency).
* ``reconcile_bucket`` returns a synthesized ``BucketReconcileReport``
  so the route handler is what's under test, not the diff algorithm
  itself (covered separately in ``tests/dynastore/tasks/gcp/unit``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.assets import assets_service as svc_module
from dynastore.extensions.assets.assets_service import AssetService
from dynastore.tasks.gcp.bucket_reconcile_task import (
    BucketReconcileReport,
    DriftEntry,
    DriftKind,
)


@pytest.fixture
def patched_service(monkeypatch: pytest.MonkeyPatch):
    """Build an ``AssetService`` whose drift path bypasses real PG/GCS."""
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )

    # resolve_reconcile_context — return harmless sentinels.
    sentinel_engine = MagicMock(name="engine")
    sentinel_client = MagicMock(name="client")

    async def fake_resolve(catalog_id: str):
        return ("ds_test", "bkt", sentinel_client, sentinel_engine)

    # The function is imported lazily inside ``_run_reconcile``, so we
    # monkeypatch the module attribute it ultimately resolves to.
    from dynastore.tasks.gcp import bucket_reconcile_task as brt

    monkeypatch.setattr(
        brt, "resolve_reconcile_context", fake_resolve
    )

    # reconcile_bucket — return a canned report so we can assert on the
    # serialized JSON shape without driving the diff algorithm.
    canned = BucketReconcileReport(
        catalog_id="cat-1",
        collection_id=None,
        dry_run=True,
        pending_ttl_minutes=60,
        total_bucket_blobs=1,
        total_db_rows=0,
        orphans_imported=1,
        ghosts_marked_failed=0,
        stuck_pending_failed=0,
        drift_details=[
            DriftEntry(
                kind=DriftKind.ORPHAN_BLOB,
                catalog_id="cat-1",
                collection_id=None,
                asset_id=None,
                filename="orphan.tif",
                uri="gs://bkt/catalog/orphan.tif",
                detail="no_db_row",
            )
        ],
    )

    async def fake_reconcile(*args: Any, **kwargs: Any):
        return canned

    monkeypatch.setattr(brt, "reconcile_bucket", fake_reconcile)
    return canned


def _build_client() -> TestClient:
    app = FastAPI()
    svc = AssetService(app)
    app.include_router(svc.router)
    return TestClient(app)


def test_catalog_drift_endpoint_returns_report(patched_service) -> None:
    client = _build_client()
    resp = client.get("/assets/catalogs/cat-1/assets:drift")
    assert resp.status_code == 200
    body = resp.json()
    assert body["catalog_id"] == "cat-1"
    assert body["dry_run"] is True
    assert body["orphans_imported"] == 1
    assert body["ghosts_marked_failed"] == 0
    assert body["stuck_pending_failed"] == 0
    assert body["pending_ttl_minutes"] == 60
    assert isinstance(body["drift_details"], list)
    assert body["drift_details"][0]["kind"] == "orphan_blob"


def test_collection_drift_endpoint_returns_report(patched_service) -> None:
    client = _build_client()
    resp = client.get(
        "/assets/catalogs/cat-1/collections/col-1/assets:drift"
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["dry_run"] is True
    assert "orphans_imported" in body
    assert "ghosts_marked_failed" in body
    assert "stuck_pending_failed" in body


def test_drift_endpoint_accepts_pending_ttl_param(patched_service) -> None:
    client = _build_client()
    resp = client.get(
        "/assets/catalogs/cat-1/assets:drift?pending_ttl_minutes=120"
    )
    assert resp.status_code == 200
    # The mock ignores the param (returns the canned report) so we just
    # assert the endpoint accepted the query value.


def test_drift_endpoint_validates_ttl_bounds(patched_service) -> None:
    client = _build_client()
    # ``pending_ttl_minutes`` is bounded to [1, 1440] by the route.
    resp = client.get(
        "/assets/catalogs/cat-1/assets:drift?pending_ttl_minutes=0"
    )
    assert resp.status_code == 422
