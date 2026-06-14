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

"""Regression pin (un-fao/GeoID#2157): ``PolicyService.check_permission``
must forward ``principal_id`` and ``collection_id`` to ``evaluate_access``.

Before #2157, ``check_permission`` called ``evaluate_access`` with only
``principals`` (subject_id + role-name strings). The unified-grants block
inside ``_resolve_effective_policies`` is gated on ``principal_id is not
None``, so grant-based bindings were never resolved — a false-deny on any
principal whose authority came exclusively from a grant row.

Tests here are pure-unit: ``evaluate_access`` is stubbed to capture the
kwargs it receives. No DB, no catalog protocol, no role storage needed.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import pytest

from dynastore.models.auth import Principal
from dynastore.modules.iam.policies import PolicyService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _service_with_capture(capture: Dict[str, Any]) -> PolicyService:
    """Build a PolicyService whose ``evaluate_access`` records its kwargs.

    ``check_permission`` is the only path under test; we stub the delegate
    so the test stays DB-free and deterministic.
    """
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _fake_evaluate_access(**kwargs: Any) -> tuple[bool, str]:  # noqa: ANN401
        capture.update(kwargs)
        return True, "stubbed"

    svc.evaluate_access = _fake_evaluate_access  # type: ignore[assignment,method-assign]
    return svc


def _principal(
    *,
    pid: Optional[UUID] = None,
    subject_id: Optional[str] = None,
    roles: Optional[List[str]] = None,
) -> Principal:
    p = Principal.__new__(Principal)
    object.__setattr__(p, "id", pid)
    object.__setattr__(p, "subject_id", subject_id or "alice")
    object.__setattr__(p, "roles", roles or [])
    object.__setattr__(p, "custom_policies", [])
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_permission_forwards_principal_id_as_uuid() -> None:
    """When ``principal.id`` is a UUID it must arrive at ``evaluate_access``
    as that exact UUID object."""
    capture: Dict[str, Any] = {}
    uid = uuid4()
    svc = _service_with_capture(capture)
    principal = _principal(pid=uid)

    await svc.check_permission(
        principal,
        "GET",
        "/stac/catalogs/mycat/collections/mycol/items",
    )

    assert capture.get("principal_id") == uid, (
        f"expected UUID {uid!r}, got {capture.get('principal_id')!r}"
    )


@pytest.mark.asyncio
async def test_check_permission_forwards_principal_id_as_str_coerced_to_uuid() -> None:
    """When ``principal.id`` is a UUID-shaped string it is coerced to UUID
    before being forwarded, mirroring the middleware coercion."""
    capture: Dict[str, Any] = {}
    uid = uuid4()
    svc = _service_with_capture(capture)
    principal = _principal(pid=uid)
    # Override id with its string form to exercise the str→UUID branch.
    object.__setattr__(principal, "id", str(uid))

    await svc.check_permission(
        principal,
        "GET",
        "/stac/catalogs/mycat/collections/mycol/items",
    )

    result_id = capture.get("principal_id")
    assert isinstance(result_id, UUID), f"expected UUID, got {type(result_id)}"
    assert result_id == uid


@pytest.mark.asyncio
async def test_check_permission_principal_id_none_when_no_id() -> None:
    """When ``principal.id`` is None ``principal_id`` is forwarded as None
    (grant resolution is skipped — behaviour identical to pre-#2157)."""
    capture: Dict[str, Any] = {}
    svc = _service_with_capture(capture)
    principal = _principal(pid=None)

    await svc.check_permission(principal, "GET", "/stac/catalogs/mycat")

    assert capture.get("principal_id") is None


@pytest.mark.asyncio
async def test_check_permission_forwards_collection_id_from_resource_path() -> None:
    """``collection_id`` is extracted from the resource path and forwarded."""
    capture: Dict[str, Any] = {}
    svc = _service_with_capture(capture)
    principal = _principal(pid=uuid4())

    await svc.check_permission(
        principal,
        "GET",
        "/stac/catalogs/mycat/collections/mycol/items/item1",
    )

    assert capture.get("collection_id") == "mycol", (
        f"expected 'mycol', got {capture.get('collection_id')!r}"
    )


@pytest.mark.asyncio
async def test_check_permission_collection_id_none_when_absent_from_path() -> None:
    """When the resource path has no ``/collections/`` segment,
    ``collection_id`` is forwarded as None."""
    capture: Dict[str, Any] = {}
    svc = _service_with_capture(capture)
    principal = _principal(pid=uuid4())

    await svc.check_permission(
        principal,
        "GET",
        "/stac/catalogs/mycat",
    )

    assert capture.get("collection_id") is None


@pytest.mark.asyncio
async def test_check_permission_forwards_catalog_id_from_resource_path() -> None:
    """``catalog_id`` continues to be extracted correctly (regression guard)."""
    capture: Dict[str, Any] = {}
    svc = _service_with_capture(capture)
    principal = _principal(pid=uuid4())

    await svc.check_permission(
        principal,
        "GET",
        "/stac/catalogs/mycat/collections/mycol/items",
    )

    assert capture.get("catalog_id") == "mycat", (
        f"expected 'mycat', got {capture.get('catalog_id')!r}"
    )


@pytest.mark.asyncio
async def test_check_permission_malformed_uuid_string_falls_back_to_none() -> None:
    """A non-UUID string in ``principal.id`` must not raise — it falls back
    to None so grant resolution is safely skipped."""
    capture: Dict[str, Any] = {}
    svc = _service_with_capture(capture)
    principal = _principal(pid=None)
    object.__setattr__(principal, "id", "not-a-uuid!!!")

    await svc.check_permission(principal, "GET", "/stac/catalogs/mycat")

    assert capture.get("principal_id") is None
