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

"""
Skeleton-resolver tests for the Option B unified grants model.

Pins the PR-1 behaviour of ``PostgresIamStorage.resolve_effective_grants``:
  - Direct principal grants surface as raw rows.
  - Time bounds are honoured: a grant outside its ``valid_from`` /
    ``valid_until`` window is invisible.
  - Allow + deny rows both surface (deny precedence is applied by the
    caller — `PolicyService` — not inside the resolver).
  - ``conditions`` / ``quota`` JSONB round-trips intact (no-op for now;
    PR-2 makes them load-bearing).
  - Platform + catalog scopes union without collision.

These run as integration tests rather than pure unit tests because the
queries depend on PostgreSQL semantics (NOW(), JSONB, regclass). We
exercise them through ``IamService.storage`` which is already wired
against a live engine in the test session.
"""

from __future__ import annotations

import importlib.metadata
from datetime import datetime, timedelta, timezone

import pytest

from dynastore.tools.discovery import get_protocol
from dynastore.tools.identifiers import generate_uuidv7
from dynastore.modules.iam.iam_service import IamService
from dynastore.modules.iam.models import Principal
from dynastore.modules.iam.postgres_iam_storage import (
    EFFECT_ALLOW,
    EFFECT_DENY,
    OBJECT_ROLE,
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


async def _new_principal() -> Principal:
    """Create a fresh principal directly via storage (skip the API)."""
    iam = get_protocol(IamService)
    assert iam is not None
    storage = iam.storage
    assert storage is not None

    pid = generate_uuidv7()
    p = Principal(
        id=pid,
        identifier=f"resolver_{pid.hex[:8]}",
        display_name=f"resolver-{pid.hex[:8]}",
        roles=[],
        is_active=True,
    )
    db = get_protocol(DatabaseProtocol)
    assert db is not None
    async with managed_transaction(db.engine) as conn:
        await storage.create_principal(p, conn=conn)
    return p


@MARKER
@pytest.mark.asyncio
async def test_direct_platform_grant_surfaces(app_lifespan):
    """A platform-scope grant surfaces from resolve_effective_grants."""
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    try:
        await iam.storage.grant_platform_role(
            principal_id=p.id, role_name="sysadmin"
        )
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=None
        )
        names = [(r["object_kind"], r["object_ref"]) for r in rows]
        assert (OBJECT_ROLE, "sysadmin") in names, (
            f"Expected (role, sysadmin) in resolved grants; got {names}"
        )
    finally:
        await iam.storage.revoke_platform_role(
            principal_id=p.id, role_name="sysadmin"
        )


@MARKER
@pytest.mark.asyncio
async def test_grant_outside_validity_window_is_invisible(app_lifespan):
    """A grant with a future ``valid_from`` is invisible to the resolver."""
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    future = datetime.now(timezone.utc) + timedelta(days=30)

    grant_id = await iam.storage.grant(
        scope_schema="iam",
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="sysadmin",
        effect=EFFECT_ALLOW,
        valid_from=future,
    )
    try:
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=None
        )
        names = [(r["object_kind"], r["object_ref"]) for r in rows]
        assert (OBJECT_ROLE, "sysadmin") not in names, (
            f"Future-dated grant should be invisible; got {names}"
        )
    finally:
        if grant_id:
            await iam.storage.revoke(grant_id, scope_schema="iam")


@MARKER
@pytest.mark.asyncio
async def test_expired_grant_is_invisible(app_lifespan):
    """A grant whose ``valid_until`` has passed is invisible to the resolver."""
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    past_start = datetime.now(timezone.utc) - timedelta(days=2)
    past_end = datetime.now(timezone.utc) - timedelta(days=1)

    grant_id = await iam.storage.grant(
        scope_schema="iam",
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="sysadmin",
        effect=EFFECT_ALLOW,
        valid_from=past_start,
        valid_until=past_end,
    )
    try:
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=None
        )
        names = [(r["object_kind"], r["object_ref"]) for r in rows]
        assert (OBJECT_ROLE, "sysadmin") not in names, (
            f"Expired grant should be invisible; got {names}"
        )
    finally:
        if grant_id:
            await iam.storage.revoke(grant_id, scope_schema="iam")


@MARKER
@pytest.mark.asyncio
async def test_deny_row_surfaces_alongside_allow(app_lifespan):
    """Both allow and deny rows surface. Deny precedence (D9) is the
    caller's responsibility — the resolver returns raw rows.
    """
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()

    allow_id = await iam.storage.grant(
        scope_schema="iam",
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="sysadmin",
        effect=EFFECT_ALLOW,
    )
    deny_id = await iam.storage.grant(
        scope_schema="iam",
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="sysadmin",
        effect=EFFECT_DENY,
    )
    try:
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=None
        )
        effects = sorted({r["effect"] for r in rows
                          if r["object_ref"] == "sysadmin"})
        assert effects == [EFFECT_ALLOW, EFFECT_DENY], (
            f"Expected both allow and deny rows for sysadmin; got {effects}"
        )
    finally:
        if allow_id:
            await iam.storage.revoke(allow_id, scope_schema="iam")
        if deny_id:
            await iam.storage.revoke(deny_id, scope_schema="iam")


@MARKER
@pytest.mark.asyncio
async def test_conditions_and_quota_jsonb_roundtrip(app_lifespan):
    """conditions / quota columns round-trip intact (no-op skeleton; PR-2)."""
    iam = get_protocol(IamService)
    assert iam is not None
    p = await _new_principal()
    cond = {"path_prefix": "/stac/", "method_in": ["GET", "HEAD"]}
    quota = {"window_seconds": 60, "max_requests": 100}

    grant_id = await iam.storage.grant(
        scope_schema="iam",
        subject_kind=SUBJECT_PRINCIPAL,
        subject_ref=str(p.id),
        object_kind=OBJECT_ROLE,
        object_ref="sysadmin",
        effect=EFFECT_ALLOW,
        conditions=cond,
        quota=quota,
    )
    try:
        rows = await iam.storage.resolve_effective_grants(
            principal_id=p.id, catalog_schema=None
        )
        ours = [r for r in rows if r["id"] == grant_id]
        assert ours, "Inserted grant did not surface from resolver."
        row = ours[0]
        # asyncpg returns JSONB as either `dict` or `str` depending on
        # codec registration; accept both for resilience.
        import json as _json
        got_cond = row["conditions"]
        if isinstance(got_cond, str):
            got_cond = _json.loads(got_cond)
        got_quota = row["quota"]
        if isinstance(got_quota, str):
            got_quota = _json.loads(got_quota)
        assert got_cond == cond, f"conditions roundtrip mismatch: {got_cond} != {cond}"
        assert got_quota == quota, f"quota roundtrip mismatch: {got_quota} != {quota}"
    finally:
        if grant_id:
            await iam.storage.revoke(grant_id, scope_schema="iam")
