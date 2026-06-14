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

"""Unit tests for IamPageVisibilityFilter catalog-tier role inclusion.

Tests that a page gated on an ``audience_policy_id`` bound to a
catalog-tier role is visible to a caller holding that catalog role and
hidden from callers that do not hold it.

All tests are DB-free: RoleAdminProtocol is monkeypatched to return
in-memory role lists and ``get_protocol`` is patched directly so no
plugin registry is needed.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.iam.page_filter import IamPageVisibilityFilter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_role(name: str, policy_ids: List[str]) -> Any:
    r = MagicMock()
    r.name = name
    r.policies = list(policy_ids)
    return r


def _make_request(
    *,
    principal_role: List[str],
    catalog_id: Optional[str] = None,
) -> Any:
    """Return a Starlette-ish request stub carrying IAM middleware state."""
    return SimpleNamespace(
        state=SimpleNamespace(
            principal_role=principal_role,
            catalog_id=catalog_id,
        ),
    )


def _page(page_id: str, audience_policy_id: str) -> dict:
    return {"id": page_id, "audience_policy_id": audience_policy_id}


# ---------------------------------------------------------------------------
# _build_policy_audience_map — unit-level
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_build_policy_audience_map_includes_catalog_roles(monkeypatch):
    """Catalog-tier roles appear in the audience map when catalog_id is given."""
    platform_role = _make_role("platform_viewer", ["pol-platform"])
    catalog_role = _make_role("tenant_editor", ["pol-tenant"])

    ra = MagicMock()

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        if catalog_id == "cat1":
            return [catalog_role]
        return [platform_role]

    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter.__new__(IamPageVisibilityFilter)
    page_filter._sysadmin_role = "sysadmin"
    page_filter._anonymous_role = "anonymous"

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        result = await page_filter._build_policy_audience_map(catalog_id="cat1")

    assert "pol-platform" in result
    assert "platform_viewer" in result["pol-platform"]
    assert "pol-tenant" in result
    assert "tenant_editor" in result["pol-tenant"]


@pytest.mark.asyncio
async def test_build_policy_audience_map_no_catalog_id_platform_only(monkeypatch):
    """Without catalog_id only platform-scope roles are loaded."""
    platform_role = _make_role("platform_viewer", ["pol-platform"])
    catalog_role = _make_role("tenant_editor", ["pol-tenant"])

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        if catalog_id is not None:
            return [catalog_role]
        return [platform_role]

    ra = MagicMock()
    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter.__new__(IamPageVisibilityFilter)
    page_filter._sysadmin_role = "sysadmin"
    page_filter._anonymous_role = "anonymous"

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        result = await page_filter._build_policy_audience_map(catalog_id=None)

    assert "pol-platform" in result
    assert "pol-tenant" not in result


# ---------------------------------------------------------------------------
# filter_visible — integration-level (still DB-free)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_filter_visible_catalog_role_grants_access():
    """Holder of a catalog-tier role sees a page gated on that role's policy."""
    catalog_role = _make_role("cat_viewer", ["pol-cat-page"])
    platform_role = _make_role("platform_viewer", ["pol-platform-page"])

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        if catalog_id == "my-catalog":
            return [catalog_role]
        return [platform_role]

    ra = MagicMock()
    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter(sysadmin_role_name="sysadmin")

    pages = [_page("catalog-page", "pol-cat-page")]
    request = _make_request(
        principal_role=["cat_viewer"],
        catalog_id="my-catalog",
    )

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        visible = await page_filter.filter_visible(pages, request)

    assert len(visible) == 1
    assert visible[0]["id"] == "catalog-page"


@pytest.mark.asyncio
async def test_filter_visible_non_holder_cannot_see_catalog_gated_page():
    """A caller without the catalog-tier role does not see the page."""
    catalog_role = _make_role("cat_viewer", ["pol-cat-page"])
    platform_role = _make_role("platform_viewer", ["pol-platform-page"])

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        if catalog_id == "my-catalog":
            return [catalog_role]
        return [platform_role]

    ra = MagicMock()
    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter(sysadmin_role_name="sysadmin")

    pages = [_page("catalog-page", "pol-cat-page")]
    request = _make_request(
        principal_role=["other_role"],
        catalog_id="my-catalog",
    )

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        visible = await page_filter.filter_visible(pages, request)

    assert visible == []


@pytest.mark.asyncio
async def test_filter_visible_no_catalog_context_misses_catalog_role():
    """Without catalog_id, catalog-tier roles are not loaded — page hidden."""
    catalog_role = _make_role("cat_viewer", ["pol-cat-page"])
    platform_role = _make_role("platform_viewer", ["pol-platform-page"])

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        if catalog_id is not None:
            return [catalog_role]
        return [platform_role]

    ra = MagicMock()
    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter(sysadmin_role_name="sysadmin")

    pages = [_page("catalog-page", "pol-cat-page")]
    # No catalog_id — simulates a platform-scope request
    request = _make_request(
        principal_role=["cat_viewer"],
        catalog_id=None,
    )

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        visible = await page_filter.filter_visible(pages, request)

    # cat_viewer is only in the catalog schema, not platform — map is empty
    assert visible == []


@pytest.mark.asyncio
async def test_filter_visible_list_roles_failure_yields_empty_map():
    """A list_roles exception yields an empty map (fail-safe, not 500)."""
    ra = MagicMock()

    async def _list_roles(catalog_id: Optional[str] = None) -> List[Any]:
        raise RuntimeError("storage unavailable")

    ra.list_roles = _list_roles

    page_filter = IamPageVisibilityFilter(sysadmin_role_name="sysadmin")

    pages = [_page("gated-page", "pol-gated")]
    request = _make_request(
        principal_role=["some_role"],
        catalog_id="my-catalog",
    )

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        visible = await page_filter.filter_visible(pages, request)

    # Page is hidden because the audience map is empty — not an error
    assert visible == []


@pytest.mark.asyncio
async def test_filter_visible_sysadmin_sees_all_regardless():
    """Sysadmin bypasses policy map entirely — all pages returned."""
    ra = MagicMock()
    ra.list_roles = AsyncMock(return_value=[])

    page_filter = IamPageVisibilityFilter(sysadmin_role_name="sysadmin")

    pages = [
        _page("page-a", "pol-a"),
        _page("page-b", "pol-b"),
    ]
    request = _make_request(
        principal_role=["sysadmin"],
        catalog_id="my-catalog",
    )

    with patch(
        "dynastore.extensions.iam.page_filter.get_protocol",
        return_value=ra,
    ):
        visible = await page_filter.filter_visible(pages, request)

    # list_roles must NOT have been called — sysadmin takes the fast path
    ra.list_roles.assert_not_called()
    assert len(visible) == 2
