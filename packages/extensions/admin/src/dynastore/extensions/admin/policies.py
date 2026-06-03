#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""Pure declarations of admin-extension authorization policies.

The IAM module's ``PolicyContributor`` consumer reads these declarations
through ``AdminService.get_policies`` / ``get_role_bindings`` and
forwards them to ``PermissionProtocol``. This file never calls
``register_policy`` directly — see PR #308 for the architectural rule.
"""

from typing import List, Optional
from dynastore.models.auth import Condition, Policy
from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import IamRolesConfig


def admin_policies() -> List[Policy]:
    """Pure declaration of the admin extension's policies.

    Three policies:

    - ``admin_access`` — broad ``/admin/.*`` ALLOW. Default role
      bindings (returned by ``admin_role_bindings``) carry SYSADMIN +
      ADMIN for back-compat with existing deployments; operators may
      add/remove via REST.

    - ``admin_catalog_access`` — per-catalog mutation surface gated by
      ``catalog_admin_required``. Ships with ``required_roles=[]``: no
      role gains catalog-admin authority unless an operator names it
      (either by editing the policy's condition or by binding a role to
      the policy via ``POST /admin/roles``). Behaves like a custom role
      end-to-end.

    - ``admin_catalogs_list`` — narrow ALLOW for ``GET /admin/catalogs``
      (the catalog picker, #723). Bound by default to the
      ``catalog_admin`` sentinel role (auto-added by ``IamMiddleware``
      to any principal who holds the catalog-tier admin role) so
      catalog admins can list catalogs they administer.  The handler
      filters the response per caller; sysadmin / platform-admin reach
      this route via ``admin_access`` and get the unfiltered list.

    - ``admin_principal_lookup`` — narrow ALLOW for ``GET /admin/principals``
      (#723 follow-up). Bound to the ``catalog_admin`` sentinel so the
      catalog-grant flow can resolve a target principal by subject_id
      before calling ``POST /admin/catalogs/{cid}/principals/{pid}/roles``.
      The handler requires a non-empty ``q`` parameter from catalog-only
      callers so they cannot enumerate the platform principal directory.

    - ``admin_task_dispatch`` — POST on the catalog-level task dispatch
      surface (``/admin/catalogs/{cat}/tasks``). Bound to sysadmin +
      admin so both privileged platform roles can trigger tasks at the
      catalog scope (e.g. reindex). The path pattern uses a regex anchor
      so it does not match the collection-scoped variant, which is
      covered by the separate ``admin_task_dispatch_collection`` policy.

    - ``admin_task_dispatch_collection`` — POST on the collection-level
      task dispatch surface
      (``/admin/catalogs/{cat}/collections/{col}/tasks``). Bound to
      sysadmin + admin so a catalog-admin who can reindex a whole
      catalog can also act on a single collection within it (the
      collection scope is a subset of the catalog scope — gating it more
      tightly than the catalog route would be inconsistent). The
      ``backfill_envelope_attrs`` action is thereby reachable by admin as
      well; per-action privilege gating (keeping backfill sysadmin-only)
      is not expressible with path-pattern policies alone and is tracked
      as a follow-up.
    """
    return [
        Policy(
            id="admin_access",
            description="Grants full access to admin management endpoints.",
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[
                "/admin",
                "/admin/",
                "/admin/.*",
                "/web/pages/admin_panel",
            ],
            effect="ALLOW",
        ),
        Policy(
            id="admin_catalog_access",
            description=(
                "Per-catalog admin access; admits roles declared in this "
                "policy's catalog_admin_required condition. Sysadmin and "
                "platform-grant principals bypass."
            ),
            actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            resources=[r"^/admin/catalogs/[^/]+(/.*)?$"],
            effect="ALLOW",
            conditions=[
                Condition(
                    type="catalog_admin_required",
                    config={"required_roles": []},
                )
            ],
        ),
        Policy(
            id="admin_catalogs_list",
            description=(
                "Lets catalog-tier admins reach GET /admin/catalogs. The "
                "handler filters the response so each catalog admin only "
                "sees the catalogs they actually administer; sysadmin / "
                "platform-admin reach this route via admin_access and "
                "see the full list."
            ),
            actions=["GET"],
            resources=[r"^/admin/catalogs/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_principal_lookup",
            description=(
                "Lets catalog-tier admins reach GET /admin/principals to "
                "resolve a target principal by subject_id before granting a "
                "catalog-scope role. The handler requires a non-empty q "
                "parameter from catalog-only callers so they cannot "
                "enumerate the platform principal directory."
            ),
            actions=["GET"],
            resources=[r"^/admin/principals/?$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_task_dispatch",
            description=(
                "Grants admin and sysadmin the ability to dispatch tasks at "
                "catalog scope via POST /admin/catalogs/{cat}/tasks."
            ),
            actions=["POST"],
            resources=[r"^/admin/catalogs/[^/]+/tasks$"],
            effect="ALLOW",
        ),
        Policy(
            id="admin_task_dispatch_collection",
            description=(
                "Grants admin and sysadmin the ability to dispatch tasks at "
                "collection scope via POST "
                "/admin/catalogs/{cat}/collections/{col}/tasks. Collection "
                "scope is a subset of the catalog route, so it carries the "
                "same admin+sysadmin binding."
            ),
            actions=["POST"],
            resources=[r"^/admin/catalogs/[^/]+/collections/[^/]+/tasks$"],
            effect="ALLOW",
        ),
    ]


def admin_role_bindings(
    sysadmin_role_name: Optional[str] = None,
    admin_role_name: Optional[str] = None,
) -> List[Role]:
    """Pure declaration of the admin extension's role bindings.

    - ``admin_access`` is bound to sysadmin + admin (back-compat).
    - ``admin_catalogs_list`` + ``admin_principal_lookup`` are bound to the
      ``catalog_admin`` sentinel role so catalog-tier admins reach the
      catalog picker (#723) and the target-user lookup that precedes a
      catalog-scope role grant (#723 follow-up).
    - ``admin_catalog_access`` is intentionally unbound — operators wire
      catalog-admin authority through the same DB-resident flow they
      use for any custom role.
    """
    cfg = IamRolesConfig()
    sysadmin_role_name = sysadmin_role_name or cfg.sysadmin_role_name
    admin_role_name = admin_role_name or cfg.admin_role_name
    return [
        Role(name=sysadmin_role_name, policies=["admin_access"]),
        Role(name=admin_role_name, policies=["admin_access"]),
        Role(
            name="catalog_admin",
            policies=["admin_catalogs_list", "admin_principal_lookup"],
        ),
        # Task-dispatch: both admin tiers can trigger catalog- and
        # collection-scoped tasks (e.g. reindex). The collection scope is a
        # subset of the catalog scope, so it carries the same admin+sysadmin
        # binding rather than a tighter one.
        Role(name=sysadmin_role_name, policies=["admin_task_dispatch"]),
        Role(name=admin_role_name, policies=["admin_task_dispatch"]),
        Role(name=sysadmin_role_name, policies=["admin_task_dispatch_collection"]),
        Role(name=admin_role_name, policies=["admin_task_dispatch_collection"]),
    ]
