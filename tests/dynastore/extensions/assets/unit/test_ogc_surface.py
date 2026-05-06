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
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Unit tests for the Stage 5 OGC API - Assets surface.

Covers:

* GET /assets/ landing page (links: self, conformance, service-doc).
* GET /assets/conformance returns the six draft conformance URIs.
* Single-asset GET surfaces HATEOAS ``links`` (self / collection /
  alternate-download).
* Bulk POST returns 201 ``BulkCreationResponse`` when fully accepted.
* Bulk POST returns 207 ``IngestionReport`` on partial rejection (first
  payload refused via ``REFUSE_FAIL``, second one accepted).
* Bulk POST returns 207 ``IngestionReport`` with empty ``accepted_ids``
  and ``is_fully_rejected`` shape when every payload collides.

The OGC mixin's helpers and the Stage 3 chain runner are exercised
end-to-end against a TestClient. Database side effects are mocked at
``upsert_asset`` + ``managed_transaction`` so the tests run in-process
without a live PG instance.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.assets import assets_service as svc_module
from dynastore.extensions.assets.assets_service import AssetService
from dynastore.extensions.assets.conformance import ASSETS_CONFORMANCE_URIS
from dynastore.modules.catalog.asset_distributed import (
    AssetSidecarRejectedError,
    UpsertResult,
)
from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetKind,
    AssetStatus,
    AssetTypeEnum,
)
from dynastore.modules.catalog.write_policy_assets import AssetsWritePolicy


# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


class _FakeConn:
    async def execute(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        return None


@asynccontextmanager
async def _fake_managed_transaction(_engine: Any):
    yield _FakeConn()


def _build_service() -> AssetService:
    """Return a real AssetService bound to a fresh FastAPI app.

    We use ``__new__`` and manually wire ``router`` / ``app`` so the
    constructor's plugin-registration side effects (UploadAssetProcess,
    write_policy_assets import) are exercised once at module import time
    by Pyright but not duplicated per test.
    """
    app = FastAPI()
    svc = AssetService.__new__(AssetService)
    svc.app = app
    from fastapi import APIRouter
    svc.router = APIRouter(prefix="/assets", tags=["Assets"])
    svc._setup_routes()
    app.include_router(svc.router)
    return svc


def _make_asset(
    *,
    asset_id: str = "a1",
    catalog_id: str = "cat",
    collection_id: Optional[str] = None,
    kind: AssetKind = AssetKind.PHYSICAL,
    status: AssetStatus = AssetStatus.ACTIVE,
) -> Asset:
    now = datetime.now(timezone.utc)
    return Asset(
        asset_id=asset_id,
        asset_type=AssetTypeEnum.RASTER,
        kind=kind,
        status=status,
        catalog_id=catalog_id,
        collection_id=collection_id,
        filename=f"{asset_id}.tif" if kind == AssetKind.PHYSICAL else None,
        href=None if kind == AssetKind.PHYSICAL else f"https://x/{asset_id}",
        uri=f"gs://b/{asset_id}.tif" if kind == AssetKind.PHYSICAL else None,
        metadata={},
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def patched_bulk_env(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Patch the protocol-resolution boundaries for the bulk + GET routes."""
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )
    fake_engine = MagicMock(name="fake_engine")
    monkeypatch.setattr(svc_module, "get_engine", lambda: fake_engine)
    monkeypatch.setattr(
        svc_module, "managed_transaction", _fake_managed_transaction
    )

    catalogs_proto = AsyncMock()
    catalogs_proto.resolve_physical_schema = AsyncMock(return_value="ds_test")
    configs_proto = AsyncMock()
    configs_proto.get_config = AsyncMock(return_value=AssetsWritePolicy())

    protocols: Dict[str, Any] = {
        "CatalogsProtocol": catalogs_proto,
        "ConfigsProtocol": configs_proto,
    }

    def fake_get_protocol(proto_cls: Any) -> Any:
        return protocols.get(proto_cls.__name__)

    monkeypatch.setattr(svc_module, "get_protocol", fake_get_protocol)

    upsert_mock = AsyncMock()
    monkeypatch.setattr(svc_module, "upsert_asset", upsert_mock)

    return {"upsert": upsert_mock, "configs": configs_proto, "catalogs": catalogs_proto}


# ---------------------------------------------------------------------------
# Landing page + conformance
# ---------------------------------------------------------------------------


def test_landing_page_lists_self_and_conformance() -> None:
    svc = _build_service()
    client = TestClient(svc.app)

    r = client.get("/assets/")
    assert r.status_code == 200
    body = r.json()
    rels = {link["rel"] for link in body["links"]}
    assert {"self", "conformance", "service-doc"}.issubset(rels)


def test_conformance_lists_all_six_assets_uris() -> None:
    svc = _build_service()
    client = TestClient(svc.app)

    r = client.get("/assets/conformance")
    assert r.status_code == 200
    advertised = set(r.json()["conformsTo"])
    for uri in ASSETS_CONFORMANCE_URIS:
        assert uri in advertised, f"missing conformance URI: {uri}"


# ---------------------------------------------------------------------------
# HATEOAS links on single-asset GET
# ---------------------------------------------------------------------------


def test_single_asset_get_includes_self_link(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _build_service()
    asset = _make_asset(
        asset_id="a1",
        catalog_id="cat",
        collection_id="col",
        kind=AssetKind.PHYSICAL,
        status=AssetStatus.ACTIVE,
    )

    fake_assets = AsyncMock()
    fake_assets.get_asset = AsyncMock(return_value=asset)

    # ``self.assets`` is a property that walks ``get_protocol`` — patch
    # the descriptor with a plain attribute so the route resolves cleanly.
    monkeypatch.setattr(
        AssetService, "assets", property(lambda self: fake_assets)
    )

    client = TestClient(svc.app)
    r = client.get("/assets/catalogs/cat/collections/col/assets/a1")
    assert r.status_code == 200
    body = r.json()
    rels = {link["rel"]: link for link in body["links"]}
    assert "self" in rels
    assert rels["self"]["href"].endswith(
        "/assets/catalogs/cat/collections/col/assets/a1"
    )
    assert "collection" in rels
    assert rels["collection"]["href"].endswith(
        "/assets/catalogs/cat/collections/col"
    )
    # active physical → alternate download link present
    assert "alternate" in rels
    assert "/processes/download/execution" in rels["alternate"]["href"]


# ---------------------------------------------------------------------------
# Bulk POST: 201 fully accepted
# ---------------------------------------------------------------------------


def test_bulk_post_returns_201_when_all_accepted(
    patched_bulk_env: Dict[str, Any],
) -> None:
    svc = _build_service()

    rows = [
        {
            "asset_id": "a1",
            "catalog_id": "cat",
            "collection_id": "col",
            "filename": "a1.tif",
            "status": "active",
            "kind": "physical",
            "metadata": {},
        },
        {
            "asset_id": "a2",
            "catalog_id": "cat",
            "collection_id": "col",
            "filename": "a2.tif",
            "status": "active",
            "kind": "physical",
            "metadata": {},
        },
    ]
    patched_bulk_env["upsert"].side_effect = [
        UpsertResult(action="inserted_active", row=rows[0]),
        UpsertResult(action="inserted_active", row=rows[1]),
    ]

    client = TestClient(svc.app)
    payloads: List[Dict[str, Any]] = [
        {
            "asset_id": "a1",
            "kind": "physical",
            "filename": "a1.tif",
            "asset_type": "RASTER",
        },
        {
            "asset_id": "a2",
            "kind": "physical",
            "filename": "a2.tif",
            "asset_type": "RASTER",
        },
    ]
    r = client.post(
        "/assets/catalogs/cat/collections/col/assets:bulk",
        json=payloads,
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body == {"ids": ["a1", "a2"]}


# ---------------------------------------------------------------------------
# Bulk POST: 207 partial rejection
# ---------------------------------------------------------------------------


def test_bulk_post_returns_207_on_partial_rejection(
    patched_bulk_env: Dict[str, Any],
) -> None:
    svc = _build_service()

    accepted_row = {
        "asset_id": "a2",
        "catalog_id": "cat",
        "collection_id": "col",
        "filename": "a2.tif",
        "status": "active",
        "kind": "physical",
        "metadata": {},
    }
    patched_bulk_env["upsert"].side_effect = [
        AssetSidecarRejectedError(
            "Asset write refused: identity matched via asset_id",
            asset_id="a1",
            matcher="asset_id",
            reason="conflict",
            existing_id="a1",
        ),
        UpsertResult(action="inserted_active", row=accepted_row),
    ]

    client = TestClient(svc.app)
    r = client.post(
        "/assets/catalogs/cat/collections/col/assets:bulk",
        json=[
            {"asset_id": "a1", "kind": "physical", "filename": "a1.tif"},
            {"asset_id": "a2", "kind": "physical", "filename": "a2.tif"},
        ],
    )
    assert r.status_code == 207, r.text
    body = r.json()
    assert body["accepted_ids"] == ["a2"]
    assert body["total"] == 2
    assert len(body["rejections"]) == 1
    rej = body["rejections"][0]
    assert rej["external_id"] == "a1"
    assert rej["matcher"] == "asset_id"
    assert rej["reason"] == "conflict"
    assert "/plugins/" in rej["policy_source"]


# ---------------------------------------------------------------------------
# Bulk POST: 207 fully rejected
# ---------------------------------------------------------------------------


def test_bulk_post_returns_207_when_fully_rejected(
    patched_bulk_env: Dict[str, Any],
) -> None:
    svc = _build_service()

    patched_bulk_env["upsert"].side_effect = [
        AssetSidecarRejectedError(
            "refused via filename",
            asset_id="a1",
            matcher="filename",
            reason="conflict",
        ),
        AssetSidecarRejectedError(
            "refused via filename",
            asset_id="a2",
            matcher="filename",
            reason="conflict",
        ),
    ]

    client = TestClient(svc.app)
    r = client.post(
        "/assets/catalogs/cat/collections/col/assets:bulk",
        json=[
            {"asset_id": "a1", "kind": "physical", "filename": "a1.tif"},
            {"asset_id": "a2", "kind": "physical", "filename": "a2.tif"},
        ],
    )
    assert r.status_code == 207, r.text
    body = r.json()
    assert body["accepted_ids"] == []
    assert body["total"] == 2
    assert len(body["rejections"]) == 2
    # IngestionReport.is_fully_rejected — verify by shape (no accepted, has rejections).
    assert all(r["matcher"] == "filename" for r in body["rejections"])
