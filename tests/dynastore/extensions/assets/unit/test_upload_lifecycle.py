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

"""Unit tests for the Stage 4.1 born-claimed PENDING-INSERT upload flow.

Covers :meth:`AssetService._initiate_upload_with_policy` — the policy gate +
PENDING INSERT + driver URL mint + ticket-stamp orchestration.

Real PG / catalog-readiness fixtures aren't available in this worktree, so we
mock at the protocol-resolution boundaries (``get_protocol``, ``get_engine``,
``managed_transaction``, ``upsert_asset``) and verify the call sequence,
rejection mapping, and rollback semantics.

Pairs with the chain-runner unit tests in
``tests/dynastore/modules/catalog/unit/test_asset_write_policy_chain.py``
which cover :func:`upsert_asset` itself; the integration suite that drives
real PG end-to-end is deferred until the docker test-runner fixture lands
(documented in the Stage 4.1 report).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from dynastore.extensions.assets import assets_service as svc_module
from dynastore.extensions.assets.assets_service import AssetService, UploadRequest
from dynastore.modules.catalog.asset_distributed import (
    AssetSidecarRejectedError,
    Scope,
    UpsertResult,
)
from dynastore.modules.catalog.asset_service import (
    AssetStatus,
    AssetTypeEnum,
    AssetUploadDefinition,
)
from dynastore.modules.catalog.write_policy_assets import (
    AssetIdentityRule,
    AssetsWritePolicy,
    AssetWriteConflictPolicy,
)
from dynastore.models.protocols import UploadTicket


# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


class _FakeConn:
    """Stand-in async connection — execute() is a no-op coroutine."""

    async def execute(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - mocks
        return None


@asynccontextmanager
async def _fake_managed_transaction(_engine: Any):
    yield _FakeConn()


def _make_service() -> AssetService:
    """Return an AssetService instance with the FastAPI app mocked out so the
    constructor's route wiring doesn't try to talk to a live registry."""
    svc = AssetService.__new__(AssetService)
    svc.app = MagicMock()
    svc.router = MagicMock()
    return svc


def _make_upload_request(
    asset_id: str = "asset-1",
    filename: str = "data.tif",
    metadata: Optional[Dict[str, Any]] = None,
) -> UploadRequest:
    return UploadRequest(
        filename=filename,
        content_type="image/tiff",
        asset=AssetUploadDefinition(
            asset_id=asset_id,
            asset_type=AssetTypeEnum.RASTER,
            metadata=metadata or {"version": 1},
        ),
    )


def _make_ticket(asset_id: str = "asset-1") -> UploadTicket:
    return UploadTicket(
        ticket_id=f"tkt-{asset_id}",
        upload_url="https://signed.example/upload",
        method="PUT",
        headers={"Content-Type": "image/tiff"},
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        backend="gcs",
    )


@pytest.fixture
def patched_env(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Patch the four collaborator boundaries the route depends on.

    Returns a dict of handles tests can poke (replace the mock returns,
    inspect call-args, etc.) without each test re-doing the wiring.
    """
    # 1. require_catalog_ready — no-op
    monkeypatch.setattr(
        svc_module, "require_catalog_ready", AsyncMock(return_value=None)
    )

    # 2. get_engine — return a sentinel; managed_transaction is mocked.
    fake_engine = MagicMock(name="fake_engine")
    monkeypatch.setattr(svc_module, "get_engine", lambda: fake_engine)

    # 3. managed_transaction — yield a fake conn (records nothing).
    monkeypatch.setattr(
        svc_module, "managed_transaction", _fake_managed_transaction
    )

    # 4. get_protocol — dispatch by class name. Tests override returns via
    #    the protocols dict.
    catalogs_proto = AsyncMock()
    catalogs_proto.resolve_physical_schema = AsyncMock(return_value="ds_test")
    # resolve_catalog_id / resolve_collection_id return None so the resolution
    # block treats the incoming ids as already internal and leaves them unchanged.
    catalogs_proto.resolve_catalog_id = AsyncMock(return_value=None)
    catalogs_proto.collections = AsyncMock()
    catalogs_proto.collections.resolve_collection_id = AsyncMock(return_value=None)
    configs_proto = AsyncMock()
    configs_proto.get_config = AsyncMock(return_value=AssetsWritePolicy())

    protocols: Dict[str, Any] = {
        "CatalogsProtocol": catalogs_proto,
        "ConfigsProtocol": configs_proto,
    }

    def fake_get_protocol(proto_cls: Any) -> Any:
        return protocols.get(proto_cls.__name__)

    monkeypatch.setattr(svc_module, "get_protocol", fake_get_protocol)

    # 5. upsert_asset — default: fresh INSERT into PENDING.
    inserted_row = {
        "asset_id": "asset-1",
        "catalog_id": "cat",
        "collection_id": "col",
        "filename": "data.tif",
        "status": "pending",
        "kind": "physical",
        "metadata": {},
    }
    upsert_mock = AsyncMock(
        return_value=UpsertResult(action="inserted_pending", row=inserted_row)
    )
    monkeypatch.setattr(svc_module, "upsert_asset", upsert_mock)

    # 6. _stamp_ticket_metadata + _rollback_pending_row — track invocations.
    stamp_mock = AsyncMock(return_value=None)
    rollback_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(AssetService, "_stamp_ticket_metadata", stamp_mock)
    monkeypatch.setattr(AssetService, "_rollback_pending_row", rollback_mock)

    return {
        "engine": fake_engine,
        "catalogs": catalogs_proto,
        "configs": configs_proto,
        "protocols": protocols,
        "upsert": upsert_mock,
        "stamp": stamp_mock,
        "rollback": rollback_mock,
    }


# ---------------------------------------------------------------------------
# Happy path: PENDING insert + URL mint + ticket stamp
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_create_inserts_pending_then_mints_and_stamps(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    ticket = _make_ticket()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    result = await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
    )

    assert result is ticket
    # 1. policy chain ran with initial_status=PENDING
    upsert_kwargs = patched_env["upsert"].await_args.kwargs
    assert upsert_kwargs["initial_status"] == AssetStatus.PENDING
    scope_arg = patched_env["upsert"].await_args.args[1]
    assert isinstance(scope_arg, Scope)
    assert scope_arg.schema == "ds_test"
    assert scope_arg.catalog_id == "cat"
    assert scope_arg.collection_id == "col"
    # 2. driver got the right shape
    driver.initiate_upload.assert_awaited_once()
    drv_kwargs = driver.initiate_upload.await_args.kwargs
    assert drv_kwargs["catalog_id"] == "cat"
    assert drv_kwargs["collection_id"] == "col"
    assert drv_kwargs["filename"] == "data.tif"
    # 3. ticket got stamped post-mint
    patched_env["stamp"].assert_awaited_once()
    stamp_kwargs_or_args = patched_env["stamp"].await_args
    # _stamp_ticket_metadata is called positionally: (engine, scope, asset_id, ticket)
    assert stamp_kwargs_or_args.args[2] == "asset-1"
    assert stamp_kwargs_or_args.args[3] is ticket
    # 4. no rollback on the happy path
    patched_env["rollback"].assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_level_upload_resolves_with_no_collection(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    ticket = _make_ticket()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id=None,
        body=_make_upload_request(),
    )

    # Scope.collection_id must be None at catalog tier so the partial
    # NULLS-NOT-DISTINCT unique index treats the row correctly.
    scope_arg = patched_env["upsert"].await_args.args[1]
    assert scope_arg.collection_id is None
    # Configs.get_config must be called with collection_id=None so the
    # waterfall walks platform → catalog (no collection tier).
    cfg_kwargs = patched_env["configs"].get_config.await_args.kwargs
    assert cfg_kwargs["collection_id"] is None


# ---------------------------------------------------------------------------
# REFUSE_FAIL → 409 via the global exception handler chain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_fail_propagates_so_handler_can_map_to_409(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock()
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    rejection = AssetSidecarRejectedError(
        "Asset write refused: identity matched via filename",
        asset_id="asset-1",
        matcher="filename",
        reason="conflict",
        existing_id="other",
    )
    patched_env["upsert"].side_effect = rejection

    with pytest.raises(AssetSidecarRejectedError) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(asset_id="asset-1"),
        )

    # The exception is raised *before* the driver is touched — load-bearing.
    driver.initiate_upload.assert_not_awaited()
    patched_env["stamp"].assert_not_awaited()
    patched_env["rollback"].assert_not_awaited()
    err = exc_info.value
    assert err.matcher == "filename"
    assert err.reason == "conflict"
    assert err.existing_id == "other"


# ---------------------------------------------------------------------------
# Idempotent paths: REFUSE / REFUSE_RETURN return existing ticket if present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refuse_return_with_existing_ticket_skips_driver_mint(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock()  # should NOT be called
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    existing_expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
    existing_row = {
        "asset_id": "asset-1",
        "metadata": {
            "_upload": {
                "ticket_id": "tkt-prior",
                "upload_url": "https://prior.example/x",
                "method": "PUT",
                "headers": {"Content-Type": "image/tiff"},
                "expires_at": existing_expiry,
                "backend": "gcs",
            },
        },
    }
    patched_env["upsert"].return_value = UpsertResult(
        action="returned_existing", row=existing_row
    )

    result = await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
    )

    assert result.ticket_id == "tkt-prior"
    driver.initiate_upload.assert_not_awaited()
    patched_env["stamp"].assert_not_awaited()


@pytest.mark.asyncio
async def test_refuse_return_without_existing_ticket_falls_through_to_mint(
    patched_env: Dict[str, Any],
) -> None:
    """When the matched row has no live ticket (e.g. process restart wiped
    the in-memory _upload_tickets), the route mints a fresh URL so the
    client can resume rather than dead-ending on an idempotent 409 alias.
    """
    svc = _make_service()
    fresh_ticket = _make_ticket(asset_id="asset-1")
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=fresh_ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    patched_env["upsert"].return_value = UpsertResult(
        action="returned_existing",
        row={"asset_id": "asset-1", "metadata": {}},  # no _upload key
    )

    result = await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
    )

    assert result is fresh_ticket
    driver.initiate_upload.assert_awaited_once()
    patched_env["stamp"].assert_awaited_once()


# ---------------------------------------------------------------------------
# UPDATE: row mutated, mint runs once (no double-stamp)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_action_with_existing_ticket_returns_it(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock()
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    existing_expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
    existing_row = {
        "asset_id": "asset-1",
        "metadata": {
            "_upload": {
                "ticket_id": "tkt-existing",
                "upload_url": "https://existing.example/x",
                "method": "PUT",
                "headers": {},
                "expires_at": existing_expiry,
                "backend": "gcs",
            },
        },
    }
    patched_env["upsert"].return_value = UpsertResult(
        action="updated", row=existing_row
    )

    result = await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
    )

    assert result.ticket_id == "tkt-existing"
    driver.initiate_upload.assert_not_awaited()


# ---------------------------------------------------------------------------
# URL-mint failure: PENDING row gets soft-deleted
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_url_mint_failure_rolls_back_pending_row(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(side_effect=RuntimeError("GCS down"))
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    with pytest.raises(HTTPException) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )

    assert exc_info.value.status_code == 500
    patched_env["rollback"].assert_awaited_once()
    rb_args = patched_env["rollback"].await_args.args
    # signature: (engine, scope, asset_id)
    assert rb_args[2] == "asset-1"
    # ticket was never stamped
    patched_env["stamp"].assert_not_awaited()


@pytest.mark.asyncio
async def test_url_mint_http_exception_also_triggers_rollback(
    patched_env: Dict[str, Any],
) -> None:
    """A driver may surface 503 / 424 directly via HTTPException — those
    must also clean up the orphan PENDING row."""
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(
        side_effect=HTTPException(status_code=503, detail="provisioning")
    )
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    with pytest.raises(HTTPException) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )
    assert exc_info.value.status_code == 503
    patched_env["rollback"].assert_awaited_once()


@pytest.mark.asyncio
async def test_url_mint_cancellation_rolls_back_pending_row(
    patched_env: Dict[str, Any],
) -> None:
    """asyncio.CancelledError (a BaseException, not Exception) must still
    trigger the compensating soft-delete so the PENDING row is not orphaned
    when a client/gateway ingress timeout cancels the handler mid-mint.

    Note: raising CancelledError as a side_effect (rather than task.cancel())
    drives the handler's except branch without putting the task into a
    cancelling state, so it asserts the rollback is reached — not the
    shield's drain-under-re-cancellation behaviour.
    """
    svc = _make_service()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(side_effect=asyncio.CancelledError())
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )

    # The PENDING row must be cleaned up despite the cancellation.
    patched_env["rollback"].assert_awaited_once()
    # signature: (engine, scope, asset_id) — asset_id is the third positional arg
    assert patched_env["rollback"].await_args.args[2] == "asset-1"
    # Ticket was never stamped because minting failed.
    patched_env["stamp"].assert_not_awaited()


# ---------------------------------------------------------------------------
# Driver / engine resolution failures — short-circuit before DB touch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_driver_503_before_db_touch(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    svc.resolve_upload_driver = AsyncMock(return_value=None)  # type: ignore[method-assign]

    with pytest.raises(HTTPException) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )
    assert exc_info.value.status_code == 503
    patched_env["upsert"].assert_not_awaited()
    patched_env["stamp"].assert_not_awaited()


@pytest.mark.asyncio
async def test_no_engine_503_before_anything(
    patched_env: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(svc_module, "get_engine", lambda: None)
    svc = _make_service()
    svc.resolve_upload_driver = AsyncMock(return_value=None)  # type: ignore[method-assign]

    with pytest.raises(HTTPException) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )
    assert exc_info.value.status_code == 503
    patched_env["upsert"].assert_not_awaited()


@pytest.mark.asyncio
async def test_no_schema_424_failed_dependency(
    patched_env: Dict[str, Any],
) -> None:
    patched_env["catalogs"].resolve_physical_schema.return_value = None
    svc = _make_service()
    svc.resolve_upload_driver = AsyncMock(return_value=None)  # type: ignore[method-assign]

    with pytest.raises(HTTPException) as exc_info:
        await svc._initiate_upload_with_policy(
            catalog_id="cat",
            collection_id="col",
            body=_make_upload_request(),
        )
    assert exc_info.value.status_code == 424


# ---------------------------------------------------------------------------
# Policy waterfall is consulted with the right scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_policy_loaded_at_collection_scope(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    ticket = _make_ticket()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    custom_policy = AssetsWritePolicy(
        on_conflict=AssetWriteConflictPolicy.UPDATE,
        identity=[
            AssetIdentityRule(match_on=["asset_id"]),
        ],
    )
    patched_env["configs"].get_config.return_value = custom_policy

    await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col-x",
        body=_make_upload_request(),
    )

    # The policy returned by get_config flows straight into upsert_asset —
    # no re-validation, no defaulting in the route handler.
    upsert_args = patched_env["upsert"].await_args.args
    # (conn, scope, payload, policy) → policy is the 4th positional
    assert upsert_args[3] is custom_policy
    # The configs query was scoped to the collection
    cfg_kwargs = patched_env["configs"].get_config.await_args.kwargs
    assert cfg_kwargs["catalog_id"] == "cat"
    assert cfg_kwargs["collection_id"] == "col-x"


# ---------------------------------------------------------------------------
# Helper: _extract_existing_ticket
# ---------------------------------------------------------------------------


class TestExtractExistingTicket:
    def test_returns_none_when_no_metadata(self) -> None:
        assert AssetService._extract_existing_ticket({}) is None

    def test_returns_none_when_no_upload_subkey(self) -> None:
        row = {"metadata": {"version": 1}}
        assert AssetService._extract_existing_ticket(row) is None

    def test_returns_none_when_metadata_not_dict(self) -> None:
        row = {"metadata": "garbage"}
        assert AssetService._extract_existing_ticket(row) is None

    def test_returns_ticket_when_well_formed(self) -> None:
        expires = datetime.now(timezone.utc) + timedelta(minutes=10)
        row = {
            "metadata": {
                "_upload": {
                    "ticket_id": "abc",
                    "upload_url": "https://x.example",
                    "method": "PUT",
                    "headers": {"Content-Type": "text/csv"},
                    "expires_at": expires,
                    "backend": "gcs",
                },
            },
        }
        ticket = AssetService._extract_existing_ticket(row)
        assert ticket is not None
        assert ticket.ticket_id == "abc"
        assert ticket.backend == "gcs"
        assert ticket.method == "PUT"

    def test_returns_none_on_malformed_payload(self) -> None:
        # Missing required key → silent None (caller falls through to mint).
        row = {"metadata": {"_upload": {"ticket_id": "abc"}}}
        assert AssetService._extract_existing_ticket(row) is None


# ---------------------------------------------------------------------------
# Origin threading: CORS-stamping GCS resumable sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_origin_threads_through_to_driver_mint(
    patched_env: Dict[str, Any],
) -> None:
    """The resolved browser origin must reach the upload driver so GCS can
    CORS-stamp the resumable session at creation time."""
    svc = _make_service()
    ticket = _make_ticket()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
        origin="https://app.example",
    )

    drv_kwargs = driver.initiate_upload.await_args.kwargs
    assert drv_kwargs["origin"] == "https://app.example"


@pytest.mark.asyncio
async def test_origin_defaults_to_none_for_non_browser_clients(
    patched_env: Dict[str, Any],
) -> None:
    svc = _make_service()
    ticket = _make_ticket()
    driver = AsyncMock()
    driver.initiate_upload = AsyncMock(return_value=ticket)
    svc.resolve_upload_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    await svc._initiate_upload_with_policy(
        catalog_id="cat",
        collection_id="col",
        body=_make_upload_request(),
    )

    assert driver.initiate_upload.await_args.kwargs["origin"] is None


class TestResolveUploadOrigin:
    """The declared payload schema is unchanged (backward compatible):
    ``upload_options`` rides in via ``extra="allow"`` and the ``Origin``
    request header is the browser-supplied fallback."""

    @staticmethod
    def _request_with_headers(headers: Dict[str, str]) -> MagicMock:
        request = MagicMock()
        request.headers = headers
        return request

    def test_body_upload_options_origin_wins(self) -> None:
        body = UploadRequest(
            **_make_upload_request().model_dump(),
            upload_options={"origin": "https://from-body.example"},
        )
        request = self._request_with_headers({"origin": "https://from-header.example"})
        assert (
            AssetService._resolve_upload_origin(body, request)
            == "https://from-body.example"
        )

    def test_falls_back_to_origin_header(self) -> None:
        body = _make_upload_request()
        request = self._request_with_headers({"origin": "https://from-header.example"})
        assert (
            AssetService._resolve_upload_origin(body, request)
            == "https://from-header.example"
        )

    def test_none_when_no_origin_anywhere(self) -> None:
        body = _make_upload_request()
        request = self._request_with_headers({})
        assert AssetService._resolve_upload_origin(body, request) is None

    def test_malformed_upload_options_tolerated(self) -> None:
        # Garbage extra payloads must not 500 the route — header fallback.
        body = UploadRequest(
            **_make_upload_request().model_dump(),
            upload_options="not-a-dict",
        )
        request = self._request_with_headers({"origin": "https://h.example"})
        assert (
            AssetService._resolve_upload_origin(body, request)
            == "https://h.example"
        )
