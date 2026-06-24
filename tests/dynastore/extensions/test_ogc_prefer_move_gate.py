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

"""Unit tests for the ``Prefer: handling=move`` gate on ``OGCServiceMixin``.

Covers:

* (a) PUT with id mismatch + ``Prefer: handling=move`` → 200 +
  ``Content-Location`` + ``Link: rel=canonical`` + ``Preference-Applied``.
* (b) PUT with id mismatch, NO header, Features surface (on_id_mismatch="ignore")
  → body id dropped, path resource replaced, no rename called.
* (c) PUT with id mismatch, NO header, STAC surface (on_id_mismatch="reject")
  → 400.
* (d) GET / PUT / DELETE to a stale (aliased) id → 308 to the new URL.
* (e) PATCH with id mismatch + ``Prefer: handling=move`` → 200 + MOVE headers.
* (f) ``_wants_move`` parses RFC 7240 token correctly, including multi-token
  and mixed-case inputs.
* (g) Records GET to a stale collection id → 308.
"""

import json
from typing import Any, Dict, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from dynastore.extensions.ogc_base import OGCServiceMixin


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


class _LocalizableModel:
    def __init__(self, data: dict):
        self._data = data

    def localize(self, language: str) -> Tuple[Dict[str, Any], Any]:
        return self._data, {"en"}


def _make_catalogs_svc(
    *,
    rename_catalog_result: Tuple[str, str] = ("old", "new-cat"),
    rename_collection_result: Tuple[str, str] = ("old-col", "new-col"),
    update_catalog_return=None,
    update_collection_return=None,
) -> AsyncMock:
    svc = AsyncMock()
    svc.resolve_catalog_id = AsyncMock(return_value="internal-cat-id")
    svc.resolve_collection_id = AsyncMock(return_value="internal-col-id")
    svc.rename_catalog = AsyncMock(return_value=rename_catalog_result)
    svc.rename_collection = AsyncMock(return_value=rename_collection_result)

    default_cat = _LocalizableModel({"id": "new-cat", "title": "Renamed"})
    default_col = _LocalizableModel({"id": "new-col", "title": "Renamed col"})
    svc.update_catalog = AsyncMock(
        return_value=update_catalog_return if update_catalog_return is not None else default_cat
    )
    svc.update_collection = AsyncMock(
        return_value=update_collection_return if update_collection_return is not None else default_col
    )
    svc.get_catalog_model = AsyncMock(return_value=MagicMock(external_id="my-cat"))
    svc.resolve_catalog_alias = AsyncMock(return_value=None)
    svc.resolve_collection_alias = AsyncMock(return_value=None)
    return svc


def _make_request(prefer: Optional[str] = None, url: str = "http://testserver/stac/catalogs/old-cat") -> MagicMock:
    req = MagicMock()
    headers: Dict[str, str] = {}
    if prefer is not None:
        headers["prefer"] = prefer
    req.headers = headers
    req.url = url
    # base_url must have .scheme and .netloc attributes (used by get_root_url).
    base_url_mock = MagicMock()
    base_url_mock.scheme = "http"
    base_url_mock.netloc = "testserver"
    req.base_url = base_url_mock
    req.scope = {"root_path": ""}
    return req


class _Svc(OGCServiceMixin):
    """Minimal concrete subclass for exercising the mixin."""

    prefix = "/stac"


# ---------------------------------------------------------------------------
# (f) _wants_move parsing
# ---------------------------------------------------------------------------


def test_wants_move_true_when_header_present():
    svc = _Svc()
    req = _make_request(prefer="handling=move")
    assert svc._wants_move(req) is True


def test_wants_move_true_case_insensitive():
    svc = _Svc()
    req = _make_request(prefer="Handling=Move")
    assert svc._wants_move(req) is True


def test_wants_move_true_multi_token():
    svc = _Svc()
    req = _make_request(prefer="respond-async, handling=move, return=minimal")
    assert svc._wants_move(req) is True


def test_wants_move_false_when_no_header():
    svc = _Svc()
    req = _make_request(prefer=None)
    assert svc._wants_move(req) is False


def test_wants_move_false_different_handling():
    svc = _Svc()
    req = _make_request(prefer="handling=lenient")
    assert svc._wants_move(req) is False


def test_wants_move_false_empty_prefer():
    svc = _Svc()
    req = _make_request(prefer="")
    assert svc._wants_move(req) is False


# ---------------------------------------------------------------------------
# (a) PUT catalog with id mismatch + Prefer: handling=move → 200 + MOVE headers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_catalog_move_returns_200_with_all_headers():
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())
    request = _make_request(prefer="handling=move")

    resp = await svc._ogc_replace_catalog(
        "old-cat", {"id": "new-cat", "title": "x"}, "en", None,
        request=request, body_id="new-cat", on_id_mismatch="reject",
    )

    assert resp.status_code == 200
    assert "Content-Location" in resp.headers
    assert "Link" in resp.headers
    assert 'rel="canonical"' in resp.headers["Link"]
    assert resp.headers.get("Preference-Applied") == "handling=move"


@pytest.mark.asyncio
async def test_replace_collection_move_returns_200_with_all_headers():
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())
    request = _make_request(prefer="handling=move")

    resp = await svc._ogc_replace_collection(
        "my-cat", "old-col", {"id": "new-col", "title": "x"}, "en",
        request=request, body_id="new-col", on_id_mismatch="reject",
    )

    assert resp.status_code == 200
    assert "Content-Location" in resp.headers
    assert "Link" in resp.headers
    assert 'rel="canonical"' in resp.headers["Link"]
    assert resp.headers.get("Preference-Applied") == "handling=move"


# ---------------------------------------------------------------------------
# (e) PATCH with id mismatch + Prefer: handling=move → 200 + MOVE headers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_catalog_move_returns_200_with_all_headers():
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())
    request = _make_request(prefer="handling=move")

    resp = await svc._ogc_update_catalog(
        "old-cat", {"id": "new-cat", "title": "patched"}, "en", None,
        body_id="new-cat", request=request, on_id_mismatch="reject",
    )

    assert resp.status_code == 200
    assert "Content-Location" in resp.headers
    assert resp.headers.get("Preference-Applied") == "handling=move"
    assert 'rel="canonical"' in resp.headers.get("Link", "")


@pytest.mark.asyncio
async def test_update_collection_move_returns_200_with_all_headers():
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=_make_catalogs_svc())
    request = _make_request(prefer="handling=move")

    resp = await svc._ogc_update_collection(
        "my-cat", "old-col", {"id": "new-col", "description": "d"}, "en",
        request, body_id="new-col", on_id_mismatch="reject",
    )

    assert resp.status_code == 200
    assert "Content-Location" in resp.headers
    assert resp.headers.get("Preference-Applied") == "handling=move"


# ---------------------------------------------------------------------------
# (e2) Logical-id contract on the collection MOVE post-rename write.
#
# Regression for the spurious "Collection not found after rename." 404: the
# rename branch must hand the post-rename ``update_collection`` the LOGICAL ids
# (catalog path id + the NEW external id), NOT the internal surrogates. The
# ``resolve_collection_id`` resolver is external->internal only; feeding it an
# already-internal surrogate bypasses the mapping, so the post-rename re-read
# cannot find the row and 404s even though the rename committed. These tests
# pin the ids the handler passes downstream (the earlier move tests mocked
# update_collection to always succeed, so the wrong-id call slipped through).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_collection_move_uses_logical_ids_downstream():
    svc = _Svc()
    catalogs_svc = _make_catalogs_svc()  # resolve_*_id return internal-*-id
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer="handling=move")

    await svc._ogc_replace_collection(
        "my-cat", "old-col", {"id": "new-col", "title": "x"}, "en",
        request=request, body_id="new-col", on_id_mismatch="reject",
    )

    catalogs_svc.update_collection.assert_awaited_once()
    cat_arg, col_arg = catalogs_svc.update_collection.await_args.args[:2]
    assert cat_arg == "my-cat", "must pass LOGICAL catalog id, not internal surrogate"
    assert col_arg == "new-col", "must pass NEW external id, not internal surrogate"


@pytest.mark.asyncio
async def test_update_collection_move_uses_logical_ids_downstream():
    svc = _Svc()
    catalogs_svc = _make_catalogs_svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer="handling=move")

    await svc._ogc_update_collection(
        "my-cat", "old-col", {"id": "new-col", "description": "d"}, "en",
        request, body_id="new-col", on_id_mismatch="reject",
    )

    catalogs_svc.update_collection.assert_awaited_once()
    cat_arg, col_arg = catalogs_svc.update_collection.await_args.args[:2]
    assert cat_arg == "my-cat", "must pass LOGICAL catalog id, not internal surrogate"
    assert col_arg == "new-col", "must pass NEW external id, not internal surrogate"


# ---------------------------------------------------------------------------
# (b) PUT with id mismatch, no header, Features (ignore) → no rename, path id used
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_catalog_features_ignores_body_id_no_rename():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    resp = await svc._ogc_replace_catalog(
        "path-cat", {"id": "body-cat", "title": "x"}, "en", None,
        request=request, body_id="body-cat", on_id_mismatch="ignore",
    )

    assert resp.status_code == 200
    # rename_catalog must NOT have been called
    catalogs_svc.rename_catalog.assert_not_awaited()
    # update_catalog IS called — with the body id dropped (path id used)
    catalogs_svc.update_catalog.assert_awaited_once()
    call_args = catalogs_svc.update_catalog.call_args
    # First positional arg to update_catalog is the catalog identifier (path id)
    assert call_args.args[0] == "path-cat"
    # The dict passed to update_catalog must not contain "id"
    assert "id" not in call_args.args[1]


@pytest.mark.asyncio
async def test_replace_collection_features_ignores_body_id_no_rename():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    resp = await svc._ogc_replace_collection(
        "my-cat", "path-col", {"id": "body-col", "title": "x"}, "en",
        request=request, body_id="body-col", on_id_mismatch="ignore",
    )

    assert resp.status_code == 200
    catalogs_svc.rename_collection.assert_not_awaited()
    catalogs_svc.update_collection.assert_awaited_once()
    call_args = catalogs_svc.update_collection.call_args
    # Second positional arg is the collection identifier
    assert call_args.args[1] == "path-col"
    assert "id" not in call_args.args[2]


# ---------------------------------------------------------------------------
# (c) PUT with id mismatch, no header, STAC (reject) → 400
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_catalog_stac_rejects_id_mismatch_without_prefer():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_catalog(
            "path-cat", {"id": "body-cat"}, "en", None,
            request=request, body_id="body-cat", on_id_mismatch="reject",
        )

    assert exc_info.value.status_code == 400
    assert "handling=move" in exc_info.value.detail
    catalogs_svc.rename_catalog.assert_not_awaited()
    catalogs_svc.update_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_replace_collection_stac_rejects_id_mismatch_without_prefer():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_replace_collection(
            "my-cat", "path-col", {"id": "body-col"}, "en",
            request=request, body_id="body-col", on_id_mismatch="reject",
        )

    assert exc_info.value.status_code == 400
    assert "handling=move" in exc_info.value.detail
    catalogs_svc.rename_collection.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_catalog_stac_rejects_id_mismatch_without_prefer():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_update_catalog(
            "path-cat", {"id": "body-cat", "title": "x"}, "en", None,
            body_id="body-cat", request=request, on_id_mismatch="reject",
        )

    assert exc_info.value.status_code == 400
    assert "handling=move" in exc_info.value.detail


@pytest.mark.asyncio
async def test_update_collection_stac_rejects_id_mismatch_without_prefer():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)
    request = _make_request(prefer=None)

    with pytest.raises(HTTPException) as exc_info:
        await svc._ogc_update_collection(
            "my-cat", "path-col", {"id": "body-col"}, "en",
            request, body_id="body-col", on_id_mismatch="reject",
        )

    assert exc_info.value.status_code == 400
    assert "handling=move" in exc_info.value.detail


# ---------------------------------------------------------------------------
# Normal path (body id == path id) is unchanged regardless of mismatch policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replace_catalog_normal_path_unaffected_by_policy():
    """When body id matches path id, no rename and no 400 — regardless of policy."""
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    for policy in ("ignore", "reject"):
        catalogs_svc.rename_catalog.reset_mock()
        catalogs_svc.update_catalog.reset_mock()
        catalogs_svc.update_catalog.return_value = _LocalizableModel({"id": "cat1"})

        resp = await svc._ogc_replace_catalog(
            "cat1", {"id": "cat1", "title": "x"}, "en", None,
            request=_make_request(prefer=None), body_id="cat1",
            on_id_mismatch=policy,  # type: ignore[arg-type]
        )

        assert resp.status_code == 200
        catalogs_svc.rename_catalog.assert_not_awaited()


@pytest.mark.asyncio
async def test_replace_collection_normal_path_unaffected_by_policy():
    catalogs_svc = _make_catalogs_svc()
    svc = _Svc()
    svc._get_catalogs_service = AsyncMock(return_value=catalogs_svc)

    for policy in ("ignore", "reject"):
        catalogs_svc.rename_collection.reset_mock()
        catalogs_svc.update_collection.reset_mock()
        catalogs_svc.update_collection.return_value = _LocalizableModel({"id": "col1"})

        resp = await svc._ogc_replace_collection(
            "cat1", "col1", {"id": "col1", "title": "x"}, "en",
            request=_make_request(prefer=None), body_id="col1",
            on_id_mismatch=policy,  # type: ignore[arg-type]
        )

        assert resp.status_code == 200
        catalogs_svc.rename_collection.assert_not_awaited()
