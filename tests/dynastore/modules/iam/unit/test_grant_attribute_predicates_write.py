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

"""TDD tests for the attribute_predicates write path on grants (#1443).

Covers:
  1. ``CreateBindingRequest`` accepts a valid ``attribute_predicates`` list and
     rejects keys that violate the ``[A-Za-z_][A-Za-z0-9_]*`` pattern.
  2. ``postgres_iam_storage.grant`` forwards ``attribute_predicates`` to
     ``INSERT_GRANT`` as a json-dumped string; ``None`` is passed through as
     ``None`` (column DEFAULT '[]' handles the absent case).
  3. All three admin binding routes (platform / catalog / collection) forward
     ``body.attribute_predicates`` to ``storage.grant`` and echo the field in
     the response.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from dynastore.models.protocols.policies import CreateBindingRequest


# ---------------------------------------------------------------------------
# 1. CreateBindingRequest validation
# ---------------------------------------------------------------------------


def test_create_binding_request_accepts_valid_attribute_predicates() -> None:
    """Valid key/op/values list is accepted without error."""
    body = CreateBindingRequest(
        principal_id=uuid4(),
        object_kind="role",
        object_ref="editor",
        attribute_predicates=[
            {"key": "dept", "op": "in", "values": ["finance", "global"]},
            {"key": "score", "op": "lte", "values": ["100"]},
        ],
    )
    assert body.attribute_predicates is not None
    assert len(body.attribute_predicates) == 2
    assert body.attribute_predicates[0]["key"] == "dept"  # type: ignore[index]


def test_create_binding_request_accepts_none_attribute_predicates() -> None:
    """Absent (default None) attribute_predicates is accepted."""
    body = CreateBindingRequest(
        principal_id=uuid4(),
        object_kind="role",
        object_ref="editor",
    )
    assert body.attribute_predicates is None


def test_create_binding_request_rejects_key_with_space() -> None:
    """Key with a space must be rejected — 'de pt' is not [A-Za-z_][A-Za-z0-9_]*."""
    with pytest.raises((ValidationError, ValueError)):
        CreateBindingRequest(
            principal_id=uuid4(),
            object_kind="role",
            object_ref="editor",
            attribute_predicates=[{"key": "de pt", "op": "in", "values": ["x"]}],
        )


def test_create_binding_request_rejects_key_with_dot() -> None:
    """Key with a dot must be rejected — 'a.b' is not [A-Za-z_][A-Za-z0-9_]*."""
    with pytest.raises((ValidationError, ValueError)):
        CreateBindingRequest(
            principal_id=uuid4(),
            object_kind="role",
            object_ref="editor",
            attribute_predicates=[{"key": "a.b", "op": "in", "values": ["x"]}],
        )


def test_create_binding_request_rejects_key_starting_with_digit() -> None:
    """Key starting with a digit must be rejected."""
    with pytest.raises((ValidationError, ValueError)):
        CreateBindingRequest(
            principal_id=uuid4(),
            object_kind="role",
            object_ref="editor",
            attribute_predicates=[{"key": "1bad", "op": "in", "values": ["x"]}],
        )


# ---------------------------------------------------------------------------
# 2. postgres_iam_storage.grant forwards attribute_predicates to INSERT_GRANT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_storage_grant_forwards_attribute_predicates_as_json() -> None:
    """grant() must json-dump a non-None attribute_predicates list and pass it
    as ``attribute_predicates`` kwarg to INSERT_GRANT.execute."""
    from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage

    predicates = [{"key": "dept", "op": "in", "values": ["finance"]}]
    captured: Dict[str, Any] = {}

    async def fake_execute(conn: Any, **kwargs: Any) -> Optional[Any]:
        captured.update(kwargs)
        return uuid4()

    storage = PostgresIamStorage.__new__(PostgresIamStorage)

    # Patch INSERT_GRANT.execute and _bump_binding_version so we never touch a DB.
    with (
        patch(
            "dynastore.modules.iam.postgres_iam_storage.INSERT_GRANT"
        ) as mock_insert,
        patch.object(storage, "_bump_binding_version", new=AsyncMock()),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.managed_transaction"
        ) as mock_tx,
    ):
        mock_insert.execute = AsyncMock(side_effect=fake_execute)
        # managed_transaction must yield a fake connection
        fake_conn = MagicMock()
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)
        # Inject a dummy engine so the constructor guard doesn't fire.
        storage.engine = MagicMock()  # type: ignore[attr-defined]

        await storage.grant(
            scope_schema="iam",
            subject_kind="principal",
            subject_ref="user-abc",
            object_kind="role",
            object_ref="editor",
            attribute_predicates=predicates,
        )

    assert "attribute_predicates" in captured, (
        "INSERT_GRANT.execute must receive attribute_predicates kwarg"
    )
    dumped = captured["attribute_predicates"]
    assert dumped is not None
    assert json.loads(dumped) == predicates


@pytest.mark.asyncio
async def test_storage_grant_passes_none_when_attribute_predicates_absent() -> None:
    """grant() with no attribute_predicates must pass None so the DB default
    (``'[]'::jsonb``) takes effect, not an empty string."""
    from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage

    captured: Dict[str, Any] = {}

    async def fake_execute(conn: Any, **kwargs: Any) -> Optional[Any]:
        captured.update(kwargs)
        return uuid4()

    storage = PostgresIamStorage.__new__(PostgresIamStorage)

    with (
        patch(
            "dynastore.modules.iam.postgres_iam_storage.INSERT_GRANT"
        ) as mock_insert,
        patch.object(storage, "_bump_binding_version", new=AsyncMock()),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.managed_transaction"
        ) as mock_tx,
    ):
        mock_insert.execute = AsyncMock(side_effect=fake_execute)
        fake_conn = MagicMock()
        mock_tx.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        mock_tx.return_value.__aexit__ = AsyncMock(return_value=False)
        storage.engine = MagicMock()  # type: ignore[attr-defined]

        await storage.grant(
            scope_schema="iam",
            subject_kind="principal",
            subject_ref="user-abc",
            object_kind="role",
            object_ref="editor",
            # attribute_predicates omitted → should pass None
        )

    assert "attribute_predicates" in captured, (
        "INSERT_GRANT.execute must receive attribute_predicates kwarg even when absent"
    )
    assert captured["attribute_predicates"] is None


# ---------------------------------------------------------------------------
# 3. Admin binding routes forward attribute_predicates and echo it back
# ---------------------------------------------------------------------------


_GET_PROTOCOL = "dynastore.extensions.admin.admin_service.get_protocol"
_CATALOG = "cat-a"
_COLL = "coll-x"
_SCHEMA = "cat_a_schema"
_PREDICATES = [{"key": "dept", "op": "in", "values": ["finance"]}]


def _req(roles: List[str]) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(
            principal=SimpleNamespace(
                id=uuid4(), provider="local", subject_id="alice", roles=roles,
            ),
            principal_role=list(roles),
            policy_allowed=True,
        )
    )


def _mgr() -> MagicMock:
    mgr = MagicMock()
    mgr.list_roles = AsyncMock(
        return_value=[SimpleNamespace(name="admin"), SimpleNamespace(name="editor")]
    )
    mgr.get_principal = AsyncMock(return_value=SimpleNamespace(subject_id="bob"))
    mgr.resolve_schema = AsyncMock(return_value=_SCHEMA)
    mgr.storage = MagicMock()
    mgr.storage.grant = AsyncMock(return_value=uuid4())
    return mgr


def _patch_protocols(mgr: MagicMock, *, policy_exists: bool = True) -> Any:
    from dynastore.extensions.admin.admin_service import AdminService  # noqa: F401 (side-import)
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.modules.iam.iam_service import IamService

    catalogs = MagicMock()
    catalogs.resolve_catalog_id = AsyncMock(return_value=_CATALOG)
    catalogs.get_catalog_model = AsyncMock(return_value=SimpleNamespace(id=_CATALOG))
    catalogs.collections = MagicMock()
    catalogs.collections.get_collection = AsyncMock(
        return_value=SimpleNamespace(id=_COLL)
    )
    catalogs.collections.resolve_collection_id = AsyncMock(return_value=_COLL)

    perm = MagicMock()
    perm.get_policy = AsyncMock(
        return_value=(SimpleNamespace(id="exp42") if policy_exists else None)
    )

    def _get_proto(cls: Any) -> Any:
        if cls is IamService:
            return mgr
        if cls is CatalogsProtocol:
            return catalogs
        if cls is PermissionProtocol:
            return perm
        return None

    return patch(_GET_PROTOCOL, side_effect=_get_proto)


@pytest.mark.asyncio
async def test_platform_binding_forwards_attribute_predicates() -> None:
    """create_platform_binding must pass attribute_predicates to storage.grant."""
    from dynastore.extensions.admin.admin_service import AdminService

    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid,
        object_kind="role",
        object_ref="editor",
        attribute_predicates=_PREDICATES,
    )
    req = _req(roles=["sysadmin"])
    mgr = _mgr()

    with _patch_protocols(mgr):
        out = await AdminService.create_platform_binding(req, body=body)

    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs.get("attribute_predicates") == _PREDICATES, (
        "create_platform_binding must forward attribute_predicates to storage.grant"
    )
    assert "attribute_predicates" in out, "response must echo attribute_predicates"


@pytest.mark.asyncio
async def test_catalog_binding_forwards_attribute_predicates() -> None:
    """create_catalog_binding must pass attribute_predicates to storage.grant."""
    from dynastore.extensions.admin.admin_service import AdminService

    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid,
        object_kind="role",
        object_ref="editor",
        attribute_predicates=_PREDICATES,
    )
    req = _req(roles=["sysadmin"])
    mgr = _mgr()

    with _patch_protocols(mgr):
        out = await AdminService.create_catalog_binding(
            req, catalog_id=_CATALOG, body=body
        )

    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs.get("attribute_predicates") == _PREDICATES, (
        "create_catalog_binding must forward attribute_predicates to storage.grant"
    )
    assert "attribute_predicates" in out, "response must echo attribute_predicates"


@pytest.mark.asyncio
async def test_collection_binding_forwards_attribute_predicates() -> None:
    """create_collection_binding must pass attribute_predicates to storage.grant."""
    from dynastore.extensions.admin.admin_service import AdminService

    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid,
        object_kind="role",
        object_ref="editor",
        attribute_predicates=_PREDICATES,
    )
    req = _req(roles=["sysadmin"])
    mgr = _mgr()

    with _patch_protocols(mgr):
        out = await AdminService.create_collection_binding(
            req, catalog_id=_CATALOG, collection_id=_COLL, body=body
        )

    mgr.storage.grant.assert_awaited_once()
    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs.get("attribute_predicates") == _PREDICATES, (
        "create_collection_binding must forward attribute_predicates to storage.grant"
    )
    assert "attribute_predicates" in out, "response must echo attribute_predicates"


@pytest.mark.asyncio
async def test_binding_with_no_predicates_passes_none_to_storage() -> None:
    """When attribute_predicates is absent, storage.grant must receive None
    (not an empty list), so the DB column default '[]' is used."""
    from dynastore.extensions.admin.admin_service import AdminService

    pid = uuid4()
    body = CreateBindingRequest(
        principal_id=pid,
        object_kind="role",
        object_ref="editor",
        # no attribute_predicates
    )
    req = _req(roles=["sysadmin"])
    mgr = _mgr()

    with _patch_protocols(mgr):
        await AdminService.create_platform_binding(req, body=body)

    kwargs = mgr.storage.grant.await_args.kwargs
    assert kwargs.get("attribute_predicates") is None, (
        "absent attribute_predicates must reach storage as None"
    )
