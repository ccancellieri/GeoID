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

"""Regression guard for the `ids` push-down fix in `GET /admin/catalogs`.

Before this fix, `list_catalogs_for_admin` fetched a full paginated page and
*then* filtered by admin-granted catalog ids.  A catalog-admin with grants on
page-3 catalogs would receive empty page-1/page-2 responses while the LIMIT
was silently applied to the unfiltered result set.

After the fix:
  (a) `ids` is forwarded to `CatalogsProtocol.list_catalogs` so SQL
      narrowing happens **before** ORDER BY / LIMIT / OFFSET.
  (b) An empty `ids` set short-circuits immediately without calling storage.
  (c) `ids=None` leaves the query unrestricted (sysadmin / platform-admin path).
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"


def _load_handler():
    from dynastore.extensions.admin.admin_service import AdminService  # noqa: PLC0415
    return AdminService.list_catalogs_for_admin


def _make_request(principal: Optional[object]) -> object:
    state = SimpleNamespace(principal=principal)
    return SimpleNamespace(state=state)


def _make_principal(*, roles: list[str] = None) -> object:  # type: ignore[assignment]
    return SimpleNamespace(
        provider="local", subject_id="alice", roles=roles or []
    )


def _make_catalog_obj(cat_id: str) -> object:
    m = MagicMock()
    m.id = cat_id
    m.model_dump = MagicMock(return_value={"title": cat_id})
    return m


def _make_catalogs_svc(cats: list) -> MagicMock:
    svc = MagicMock()
    svc.list_catalogs = AsyncMock(return_value=cats)
    return svc


# ---------------------------------------------------------------------------
# (a) ids forwarded to storage — SQL narrowing before LIMIT/OFFSET
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ids_forwarded_to_list_catalogs_for_catalog_admin():
    """When admin_only_ids is a non-empty set, list_catalogs is called with
    ids=admin_only_ids so the DB applies the filter before pagination.
    """
    handler = _load_handler()
    cats = [_make_catalog_obj("cat-a"), _make_catalog_obj("cat-c")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    admin_ids: Set[str] = {"cat-a", "cat-c"}

    with patch(_GET_PROTOCOL, return_value=catalogs_svc), \
         patch(
             "dynastore.extensions.admin.admin_service._catalog_admin_filter_ids",
             new=AsyncMock(return_value=admin_ids),
         ):
        result = await handler(req, limit=10, offset=0, lang="en", q=None)

    catalogs_svc.list_catalogs.assert_awaited_once()
    _, kwargs = catalogs_svc.list_catalogs.call_args
    assert kwargs.get("ids") == admin_ids, (
        "ids must be forwarded to list_catalogs for SQL-level narrowing"
    )
    assert [c["id"] for c in result] == ["cat-a", "cat-c"]


@pytest.mark.asyncio
async def test_ids_is_none_for_sysadmin():
    """Sysadmin path: _catalog_admin_filter_ids returns None; list_catalogs
    must receive ids=None so no narrowing clause is added.
    """
    handler = _load_handler()
    cats = [_make_catalog_obj("x"), _make_catalog_obj("y")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["sysadmin"]))

    with patch(_GET_PROTOCOL, return_value=catalogs_svc), \
         patch(
             "dynastore.extensions.admin.admin_service._catalog_admin_filter_ids",
             new=AsyncMock(return_value=None),
         ):
        result = await handler(req, limit=10, offset=0, lang="en", q=None)

    catalogs_svc.list_catalogs.assert_awaited_once()
    _, kwargs = catalogs_svc.list_catalogs.call_args
    assert kwargs.get("ids") is None, (
        "ids=None must be forwarded so no WHERE id=ANY(...) clause is emitted"
    )
    assert [c["id"] for c in result] == ["x", "y"]


# ---------------------------------------------------------------------------
# (b) empty-set short-circuit — storage must NOT be called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_ids_short_circuits_without_calling_storage():
    """When the admin has zero catalog-admin grants (empty set), the handler
    must return [] immediately without issuing a SQL query.
    """
    handler = _load_handler()
    catalogs_svc = _make_catalogs_svc([_make_catalog_obj("a")])
    req = _make_request(principal=_make_principal(roles=["catalog_admin"]))

    with patch(_GET_PROTOCOL, return_value=catalogs_svc), \
         patch(
             "dynastore.extensions.admin.admin_service._catalog_admin_filter_ids",
             new=AsyncMock(return_value=set()),
         ):
        result = await handler(req, limit=200, offset=0, lang="en", q=None)

    assert result == [], "empty ids set must short-circuit to []"
    catalogs_svc.list_catalogs.assert_not_awaited()


# ---------------------------------------------------------------------------
# (c) None = unfiltered — protocol signature acceptance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_none_ids_passes_unfiltered_to_storage():
    """ids=None in the protocol means no restriction.  The service stub must
    accept the kwarg without raising, confirming the signature is wired.
    """
    handler = _load_handler()
    cats = [_make_catalog_obj("a"), _make_catalog_obj("b"), _make_catalog_obj("c")]
    catalogs_svc = _make_catalogs_svc(cats)
    req = _make_request(principal=_make_principal(roles=["admin"]))

    with patch(_GET_PROTOCOL, return_value=catalogs_svc), \
         patch(
             "dynastore.extensions.admin.admin_service._catalog_admin_filter_ids",
             new=AsyncMock(return_value=None),
         ):
        result = await handler(req, limit=200, offset=5, lang="en", q=None)

    _, kwargs = catalogs_svc.list_catalogs.call_args
    assert kwargs.get("ids") is None
    assert kwargs.get("offset") == 5
    assert len(result) == 3
