#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Lock in the admin-extension policy + role-binding shape (#723).

These bindings are load-bearing: a stray rename or unbinding silently
shifts the catalog-admin surface back to the all-or-nothing
``admin_access`` model.
"""
from __future__ import annotations

from dynastore.extensions.admin.policies import (
    admin_policies,
    admin_role_bindings,
)


def test_admin_policies_ids_pinned():
    ids = [p.id for p in admin_policies()]
    assert ids == [
        "admin_access",
        "admin_catalog_access",
        "admin_catalogs_list",
        "admin_principal_lookup",
        "admin_task_dispatch",
        "admin_task_dispatch_collection",
    ]


def test_admin_principal_lookup_targets_principals_route_only():
    pol = next(p for p in admin_policies() if p.id == "admin_principal_lookup")
    assert pol.actions == ["GET"]
    assert pol.resources == [r"^/admin/principals/?$"]
    assert pol.effect == "ALLOW"


def test_catalog_admin_sentinel_binds_both_lookup_policies():
    bindings = {r.name: list(r.policies) for r in admin_role_bindings()}
    assert "catalog_admin" in bindings
    assert set(bindings["catalog_admin"]) == {
        "admin_catalogs_list",
        "admin_principal_lookup",
    }


def test_sysadmin_and_admin_task_dispatch_bindings():
    """Privileged platform roles must NOT pick up the narrow lookup
    policies — they reach those routes via admin_access, and an
    accidental binding would let an operator break admin_access without
    noticing a privilege regression.

    Both sysadmin and admin must be bound to admin_task_dispatch (catalog-scope
    task dispatch) AND admin_task_dispatch_collection (collection-scope
    dispatch): the collection scope is a subset of the catalog scope, so it
    carries the same admin+sysadmin binding rather than a tighter one.
    """
    # Build a dict: role_name -> list of all policies across all binding rows.
    # Note: the same role may appear in multiple Role rows (one per policy),
    # so we collect policies across all rows for that role.
    from collections import defaultdict
    policy_sets: dict = defaultdict(set)
    for rb in admin_role_bindings():
        for pol in (rb.policies or []):
            policy_sets[rb.name].add(pol)

    # Catalog-only narrow policies must not appear on privileged platform roles.
    assert "admin_catalogs_list" not in policy_sets.get("sysadmin", set())
    assert "admin_principal_lookup" not in policy_sets.get("sysadmin", set())
    assert "admin_catalogs_list" not in policy_sets.get("admin", set())
    assert "admin_principal_lookup" not in policy_sets.get("admin", set())

    # admin_access must remain on both.
    assert "admin_access" in policy_sets.get("sysadmin", set())
    assert "admin_access" in policy_sets.get("admin", set())

    # Task-dispatch bindings.
    assert "admin_task_dispatch" in policy_sets.get("sysadmin", set())
    assert "admin_task_dispatch" in policy_sets.get("admin", set())
    assert "admin_task_dispatch_collection" in policy_sets.get("sysadmin", set())
    assert "admin_task_dispatch_collection" in policy_sets.get("admin", set()), (
        "admin role must be bound to admin_task_dispatch_collection so a "
        "catalog-admin can act on a single collection (subset of the catalog "
        "scope it can already reindex)"
    )
