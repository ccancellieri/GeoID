"""Regression for geoid#902: INSERT_ROLE ON CONFLICT must UNION policies.

The cold-boot auth path seeds the catalog-tier ``unauthenticated`` role
via ``iam_storage.create_role(Role(policies=["public_access"]))`` and
later re-issues the same role from multiple PolicyContributors with
their own ``*_public_access`` policies. If two seed paths race past the
``get_role`` check concurrently — or if the read-modify-write merge in
``flush_pending_registrations`` is bypassed for any reason — the second
INSERT_ROLE used to hit the original ``policies = EXCLUDED.policies``
clause and silently drop the binding the first path wrote. That is what
manifested as the ``/health`` 403 in geoid#902: the bound list grew with
the per-extension ``*_public_access`` policies, but the seed-declared
``public_access`` (the policy that actually whitelists ``/health``) was
clobbered.

Fix: on conflict, ``policies`` is recomputed as the UNION of the
existing JSONB array and EXCLUDED's. These tests pin the SQL fragments
so a future refactor can't silently flip the clause back to
replace-on-conflict.
"""

from __future__ import annotations

import re


def _normalize(sql: str) -> str:
    """Collapse whitespace so multi-line SQL templates can be matched
    with simple substring assertions."""
    return re.sub(r"\s+", " ", sql).strip()


def test_insert_role_unions_policies_on_conflict() -> None:
    """``policies`` must be recomputed from
    ``jsonb_array_elements_text(roles.policies || EXCLUDED.policies)``
    on conflict, not assigned from ``EXCLUDED.policies`` directly. The
    DISTINCT inside ``jsonb_agg`` dedupes overlapping IDs so repeated
    seeds don't grow the array."""
    from dynastore.modules.iam.iam_queries import INSERT_ROLE

    sql = _normalize(INSERT_ROLE.template)

    # ON CONFLICT clause must be present; otherwise create_role would
    # 23505 on every retry and the platform would never recover from a
    # racing seed.
    assert "ON CONFLICT (id) DO UPDATE" in sql

    # The replace-on-conflict shape MUST be gone — that's the regression
    # vector for geoid#902.
    assert "policies = EXCLUDED.policies" not in sql, (
        "INSERT_ROLE must NOT assign policies = EXCLUDED.policies on "
        "conflict — that silently drops bindings written by a racing "
        "create_role and is the root cause of geoid#902. Use the "
        "jsonb_array_elements_text UNION pattern instead."
    )

    # The additive UNION shape MUST be present. The query references
    # ``roles.policies`` (the existing row) and ``EXCLUDED.policies``
    # (the would-be insert) joined with the JSONB concat operator.
    assert "roles.policies" in sql, (
        "Additive UNION must read the existing row's policies as "
        "``roles.policies`` inside the ON CONFLICT update."
    )
    assert "EXCLUDED.policies" in sql
    assert "jsonb_array_elements_text" in sql, (
        "Additive UNION must call jsonb_array_elements_text to flatten "
        "both arrays before DISTINCT."
    )
    assert "jsonb_agg(DISTINCT value)" in sql, (
        "Additive UNION must dedupe the merged array so repeated seeds "
        "don't accumulate duplicate policy IDs."
    )


def test_update_role_remains_replace_on_policies() -> None:
    """UPDATE_ROLE (used by ``PATCH /admin/roles`` and the merge branch
    of ``flush_pending_registrations``) MUST stay replace-semantics.

    The additive treatment on INSERT_ROLE is for accidental races between
    callers that both intend to *create* the row. Operator-driven REPLACE
    (PATCH /admin/roles) must still be able to remove a policy from a
    role — otherwise there is no way to revoke a binding."""
    from dynastore.modules.iam.iam_queries import UPDATE_ROLE

    sql = _normalize(UPDATE_ROLE.template)

    assert "policies = :policies" in sql, (
        "UPDATE_ROLE must REPLACE policies with the caller-supplied "
        "list. The additive UNION pattern belongs on INSERT_ROLE only."
    )
    assert "jsonb_array_elements_text" not in sql, (
        "UPDATE_ROLE must not adopt the additive UNION pattern — that "
        "would make ``PATCH /admin/roles`` unable to revoke a binding."
    )
