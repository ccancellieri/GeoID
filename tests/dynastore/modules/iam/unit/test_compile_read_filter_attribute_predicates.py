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

"""Unit tests for compile_read_filter extended with grant attribute predicates (#1441).

Covers:
- RBAC-only grant (attribute_predicates=[]) → no attribute clause added
- Grant with attribute_predicates → clause includes _attrs.* predicates
- Multiple grants with predicates → OR-of-clauses
- Mixed RBAC+ABAC grant
- uncompilable op propagates uncompilable=True
- No principal_id → no attribute clause (no DB call)

The service is built exactly as the sibling test_compile_read_filter.py does:
no DB / role-storage dependencies; policies via principal.custom_policies.
We stub the iam_storage.resolve_effective_grants call to inject grant rows.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import pytest

from dynastore.models.auth import Policy, Principal
from dynastore.models.protocols.access_filter import AccessClause, AccessFilter, FieldPredicate
from dynastore.modules.iam.policies import PolicyService


# ---------------------------------------------------------------------------
# Service helpers (mirrors test_compile_read_filter.py)
# ---------------------------------------------------------------------------

def _service(grant_rows: Optional[List[Dict[str, Any]]] = None) -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _fake_resolve_schema(catalog_id, conn=None):
        return "iam"

    svc._resolve_schema = _fake_resolve_schema  # type: ignore[method-assign]

    # Stub iam_storage with a resolve_effective_grants that returns grant_rows.
    class _StubIamStorage:
        async def resolve_effective_grants(
            self,
            principal_id: Any,
            catalog_schema: str = "iam",
            request_path: Optional[str] = None,
            collection_id: Optional[str] = None,
            conn: Any = None,
        ) -> List[Dict[str, Any]]:
            return grant_rows or []

    svc.iam_storage = _StubIamStorage()  # type: ignore[attr-defined]
    return svc


def _allow_all_policy() -> Policy:
    """Unconditional ALLOW spanning all paths (simulates super-admin)."""
    return Policy(id="allow_all", effect="ALLOW", actions=["GET"], resources=[".*"])


def _principal_with_policy(pol: Policy, principal_id: Optional[UUID] = None) -> Principal:
    return Principal(
        id=principal_id or uuid4(),
        custom_policies=[pol],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rbac_only_grant_no_attribute_clause():
    """A grant with attribute_predicates=[] adds no attribute clause."""
    grant_rows = [
        {
            "id": str(uuid4()),
            "object_kind": "role",
            "object_ref": "viewer",
            "attribute_predicates": [],
        }
    ]
    pid = uuid4()
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1", collection_id="col1",
        principal=principal, principal_id=pid,
    )

    # ALLOW from the policy path; no extra attribute clauses.
    assert not af.deny_all
    allow_clauses_with_attrs = [
        c for c in af.allow
        if any(p.field.startswith("_attrs.") for p in c.predicates)
    ]
    assert allow_clauses_with_attrs == []


@pytest.mark.asyncio
async def test_grant_with_single_attribute_predicate():
    """A grant with attribute_predicates=[{in, dept, [finance]}] adds a clause."""
    pid = uuid4()
    grant_rows = [
        {
            "id": str(pid),
            "object_kind": "role",
            "object_ref": "dept_reader",
            "attribute_predicates": [
                {"key": "dept", "op": "in", "values": ["finance", "global"]},
            ],
        }
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1", collection_id="col1",
        principal=principal, principal_id=pid,
    )

    assert not af.deny_all
    # At least one clause must contain an _attrs.dept predicate.
    attrs_clauses = [
        c for c in af.allow
        if any(p.field == "_attrs.dept" for p in c.predicates)
    ]
    assert len(attrs_clauses) >= 1
    dept_pred = next(
        p for c in attrs_clauses for p in c.predicates if p.field == "_attrs.dept"
    )
    assert set(dept_pred.values) == {"finance", "global"}


@pytest.mark.asyncio
async def test_multiple_grants_produce_multiple_clauses():
    """Two grants with different predicates produce OR-of-clauses."""
    pid = uuid4()
    grant_rows = [
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "dept", "op": "in", "values": ["finance"]},
            ],
        },
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "dept", "op": "in", "values": ["legal"]},
            ],
        },
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1", collection_id="col1",
        principal=principal, principal_id=pid,
    )

    attrs_clauses = [
        c for c in af.allow
        if any(p.field == "_attrs.dept" for p in c.predicates)
    ]
    assert len(attrs_clauses) == 2


@pytest.mark.asyncio
async def test_uncompilable_op_sets_uncompilable_flag():
    """An unsupported op sets uncompilable=True on the AccessFilter.

    Uses ``"range"`` which is intentionally NOT a supported op (it was a
    placeholder name in early design notes; the real ops are lte/gte/between).
    """
    pid = uuid4()
    grant_rows = [
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "sensitivity", "op": "range", "values": ["3"]},
            ],
        }
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1",
        principal=principal, principal_id=pid,
    )

    # The grant is excluded from ALLOW (fail-closed); uncompilable is set.
    assert af.uncompilable is True
    # No _attrs clauses from this grant.
    attrs_clauses = [
        c for c in af.allow
        if any(p.field.startswith("_attrs.") for p in c.predicates)
    ]
    assert attrs_clauses == []


@pytest.mark.asyncio
async def test_lte_op_now_compiles_to_range_predicate():
    """``lte`` op is now supported and produces a RangePredicate clause."""
    from dynastore.models.protocols.access_filter import RangePredicate

    pid = uuid4()
    grant_rows = [
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "sensitivity", "op": "lte", "values": ["5"]},
            ],
        }
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1", collection_id="col1",
        principal=principal, principal_id=pid,
    )

    assert not af.deny_all
    # At least one clause must contain a RangePredicate on _attrs.sensitivity.
    range_clauses = [
        c for c in af.allow
        if any(isinstance(p, RangePredicate) and p.field == "_attrs.sensitivity" for p in c.predicates)
    ]
    assert len(range_clauses) >= 1


@pytest.mark.asyncio
async def test_no_principal_id_skips_attribute_clauses():
    """Without a principal_id the DB is not called; attribute clauses absent."""
    svc = _service(grant_rows=[
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "dept", "op": "in", "values": ["finance"]},
            ],
        }
    ])
    pol = _allow_all_policy()
    principal = Principal(custom_policies=[pol])  # no id set

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1",
        principal=principal, principal_id=None,
    )

    # Policy-based allow fires; no attribute clauses.
    attrs_clauses = [
        c for c in af.allow
        if any(p.field.startswith("_attrs.") for p in c.predicates)
    ]
    assert attrs_clauses == []


@pytest.mark.asyncio
async def test_mixed_rbac_and_abac_grant():
    """A grant with predicates alongside a clean RBAC-only grant both work."""
    pid = uuid4()
    grant_rows = [
        {"id": str(uuid4()), "attribute_predicates": []},  # RBAC-only
        {
            "id": str(uuid4()),
            "attribute_predicates": [
                {"key": "dept", "op": "eq", "values": ["legal"]},
            ],
        },
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1", collection_id="col1",
        principal=principal, principal_id=pid,
    )

    assert not af.deny_all
    assert not af.uncompilable
    attrs_clauses = [
        c for c in af.allow
        if any(p.field == "_attrs.dept" for p in c.predicates)
    ]
    assert len(attrs_clauses) == 1


@pytest.mark.asyncio
async def test_string_serialised_jsonb_attribute_predicates():
    """Handles attribute_predicates arriving as a JSON string (driver-dependent)."""
    pid = uuid4()
    grant_rows = [
        {
            "id": str(uuid4()),
            "attribute_predicates": json.dumps([
                {"key": "dept", "op": "in", "values": ["finance"]},
            ]),
        }
    ]
    svc = _service(grant_rows)
    pol = _allow_all_policy()
    principal = _principal_with_policy(pol, pid)

    af = await svc.compile_read_filter(
        principals=[], catalog_id="cat1",
        principal=principal, principal_id=pid,
    )

    attrs_clauses = [
        c for c in af.allow
        if any(p.field == "_attrs.dept" for p in c.predicates)
    ]
    assert len(attrs_clauses) == 1
