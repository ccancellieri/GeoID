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

"""Resolver tests for resource-scoped (collection) grants
(un-fao/GeoID#1341).

Pins ``PostgresIamStorage.resolve_effective_grants(collection_id=...)`` and
the ``resource_kind`` / ``resource_ref`` round-trip through
``grant()`` / ``revoke_by_match()``:

  * A role grant scoped to a collection resolves for that collection.
  * A whole-catalog (NULL-resource) grant resolves in a scoped call too
    (allows are additive across scopes).
  * A grant on collection A is excluded from a collection-B scoped call.
  * resource columns round-trip through grant()→resolve_effective_grants().
  * revoke_by_match with a resource scope removes only the scoped row,
    leaving the whole-catalog row intact.

Like ``test_grants_resolver.py`` these run against a live PostgreSQL — the
queries rely on PG semantics (NOW(), COALESCE-keyed uniqueness, partial
index). A dedicated catalog-tier schema is provisioned per module so the
``{catalog_schema}.grants`` table exists with the resource columns.
"""

from __future__ import annotations

import importlib.metadata

import pytest

from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_uuidv7
from dynastore.modules.iam.iam_service import IamService
from dynastore.modules.iam.models import Principal
from dynastore.modules.iam.postgres_iam_storage import (
    EFFECT_ALLOW,
    OBJECT_ROLE,
    SUBJECT_COLLECTION,
    SUBJECT_PRINCIPAL,
)
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.models.protocols import DatabaseProtocol


def _iam_dist_installed() -> bool:
    try:
        importlib.metadata.distribution("dynastore-ext-iam")
        return True
    except importlib.metadata.PackageNotFoundError:
        return False


pytestmark = pytest.mark.skipif(
    not _iam_dist_installed(),
    reason=(
        "dynastore-ext-iam distribution not installed — IamModule will not "
        "register, get_protocol(IamService) returns None; install the iam "
        "extras or run inside the full dev environment"
    ),
)

MARKER = pytest.mark.enable_extensions("features")

_CAT_SCHEMA = "s_scoped_grants_test"
_COLL_A = "collA"
_COLL_B = "collB"


async def _ensure_catalog_schema() -> None:
    """Provision a catalog-tier IAM schema (roles + resource-scoped grants)."""
    iam = get_protocol(IamService)
    assert iam is not None
    storage = iam.storage
    assert storage is not None
    db = get_protocol(DatabaseProtocol)
    assert db is not None
    async with managed_transaction(db.engine) as conn:
        await storage.initialize(conn, schema=_CAT_SCHEMA)


async def _new_principal() -> Principal:
    iam = get_protocol(IamService)
    assert iam is not None
    storage = iam.storage
    assert storage is not None

    pid = generate_uuidv7()
    p = Principal(
        id=pid,
        identifier=f"scoped_{pid.hex[:8]}",
        display_name=f"scoped-{pid.hex[:8]}",
        roles=[],
        is_active=True,
    )
    db = get_protocol(DatabaseProtocol)
    assert db is not None
    async with managed_transaction(db.engine) as conn:
        await storage.create_principal(p, conn=conn)
    return p


def _grant_ids(rows, role_name):
    return [
        (r["resource_kind"], r["resource_ref"])
        for r in rows
        if r["object_kind"] == OBJECT_ROLE and r["object_ref"] == role_name
    ]


@MARKER
@pytest.mark.asyncio
async def test_collection_scoped_grant_resolves_for_its_collection(app_lifespan):
    await _ensure_catalog_schema()
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    try:
        await iam.storage.grant_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=_CAT_SCHEMA, collection_id=_COLL_A
        )
        assert (SUBJECT_COLLECTION, _COLL_A) in _grant_ids(rows, "editor"), (
            f"collA-scoped grant should resolve for collA; got {rows}"
        )
    finally:
        await iam.storage.revoke_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )


@MARKER
@pytest.mark.asyncio
async def test_catalog_wide_grant_resolves_in_scoped_call(app_lifespan):
    await _ensure_catalog_schema()
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    try:
        # Whole-catalog grant (collection_id=None).
        await iam.storage.grant_catalog_role(
            principal_id=p.id,
            role_name="viewer",
            catalog_schema=_CAT_SCHEMA,
        )
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=_CAT_SCHEMA, collection_id=_COLL_A
        )
        assert (None, None) in _grant_ids(rows, "viewer"), (
            f"whole-catalog grant should resolve in a scoped call; got {rows}"
        )
    finally:
        await iam.storage.revoke_catalog_role(
            principal_id=p.id,
            role_name="viewer",
            catalog_schema=_CAT_SCHEMA,
        )


@MARKER
@pytest.mark.asyncio
async def test_grant_on_collA_excluded_from_collB_scoped_call(app_lifespan):
    await _ensure_catalog_schema()
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    try:
        await iam.storage.grant_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=_CAT_SCHEMA, collection_id=_COLL_B
        )
        assert (SUBJECT_COLLECTION, _COLL_A) not in _grant_ids(rows, "editor"), (
            f"collA-scoped grant must NOT resolve for collB; got {rows}"
        )
    finally:
        await iam.storage.revoke_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )


@MARKER
@pytest.mark.asyncio
async def test_resource_columns_roundtrip(app_lifespan):
    await _ensure_catalog_schema()
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    grant_id = await iam.storage.grant(
        scope_schema=_CAT_SCHEMA,
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="editor",
        effect=EFFECT_ALLOW,
        resource_kind=SUBJECT_COLLECTION,
        resource_ref=_COLL_A,
    )
    try:
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=_CAT_SCHEMA, collection_id=_COLL_A
        )
        ours = [r for r in rows if r["id"] == grant_id]
        assert ours, "Inserted scoped grant did not surface from resolver."
        row = ours[0]
        assert row["resource_kind"] == SUBJECT_COLLECTION, row
        assert row["resource_ref"] == _COLL_A, row
    finally:
        if grant_id:
            await iam.storage.revoke(grant_id, scope_schema=_CAT_SCHEMA)


@MARKER
@pytest.mark.asyncio
async def test_scoped_revoke_removes_only_scoped_row(app_lifespan):
    await _ensure_catalog_schema()
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    try:
        # One whole-catalog grant + one collA-scoped grant, same role.
        await iam.storage.grant_catalog_role(
            principal_id=p.id, role_name="editor", catalog_schema=_CAT_SCHEMA
        )
        await iam.storage.grant_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )

        # Revoke only the collA-scoped row.
        removed = await iam.storage.revoke_catalog_role(
            principal_id=p.id,
            role_name="editor",
            catalog_schema=_CAT_SCHEMA,
            collection_id=_COLL_A,
        )
        assert removed is True

        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=_CAT_SCHEMA, collection_id=_COLL_A
        )
        scopes = _grant_ids(rows, "editor")
        assert (None, None) in scopes, (
            f"whole-catalog row must survive a scoped revoke; got {scopes}"
        )
        assert (SUBJECT_COLLECTION, _COLL_A) not in scopes, (
            f"collA-scoped row should be gone; got {scopes}"
        )
    finally:
        await iam.storage.revoke_catalog_role(
            principal_id=p.id, role_name="editor", catalog_schema=_CAT_SCHEMA
        )
