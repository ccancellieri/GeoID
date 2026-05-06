"""
Multi-Catalog Principal Management APIs

This module provides helper methods for managing principals across multiple
catalogs under the unified grants model (Option B):

- A principal is **platform-global** — one row in `iam.principals`, one
  identity link in `iam.identity_links` (D12).
- Roles are **scope-local grants** — `iam.grants` for platform roles,
  `{catalog_schema}.grants` for catalog roles. There is no "per-catalog
  principal" anymore; the per-catalog dimension is purely the grants table.
- Direct **policies** also live on the platform principal row for now.
  Per-tenant policy grants are PR-2 work (D11); these helpers do not
  attempt to write tenant-scoped policy rows.
"""

from typing import List, Dict, Any, Optional
import logging

from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


async def create_global_principal(
    iam_manager,
    identity: Dict[str, Any],
    roles: List[str],
    policies: List[Any],
) -> Dict[str, Any]:
    """Create or update a principal with platform-wide grants.

    Idempotent on `(provider, subject_id)`: if the principal already
    exists, only the role grants and custom_policies are reconciled.

    Args:
        iam_manager: IamService instance
        identity: Identity dict with 'provider' and 'sub' keys
        roles: Platform role names to grant
        policies: Direct policies (live on the platform principal row)

    Returns:
        Summary dict — `principal_id`, `provider`, `subject_id`, `roles`, `scope`.
    """
    from dynastore.modules.iam.models import Principal
    from uuid import uuid4

    provider = identity.get("provider")
    subject_id = identity.get("sub")
    if not provider or not subject_id:
        raise ValueError("identity must include 'provider' and 'sub'")
    email = identity.get("email", f"{subject_id}@{provider}")

    existing = await iam_manager.storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )

    if existing is None:
        new_principal = Principal(
            id=uuid4(),
            provider=provider,
            subject_id=subject_id,
            display_name=email,
            roles=[],  # roles are not persisted on the principal row anymore
            custom_policies=policies,
            is_active=True,
            attributes={},
        )
        created = await iam_manager.storage.create_principal(new_principal)
        await iam_manager.storage.create_identity_link(
            principal_id=created.id,
            provider=provider,
            subject_id=subject_id,
        )
        principal_id = created.id
    else:
        # Reconcile direct policies on the existing principal row.
        existing.custom_policies = policies
        await iam_manager.storage.update_principal(existing)
        principal_id = existing.id

    # Issue platform-scope role grants. Idempotent at the DB level
    # via the unique (subject, object, effect) index on iam.grants.
    for role_name in roles:
        await iam_manager.storage.grant_platform_role(
            principal_id=principal_id, role_name=role_name
        )

    logger.info(
        f"Provisioned platform principal {principal_id} for {provider}:{subject_id} "
        f"with {len(roles)} platform role(s)"
    )

    return {
        "principal_id": str(principal_id),
        "provider": provider,
        "subject_id": subject_id,
        "roles": list(roles),
        "scope": "platform",
    }


async def sync_principal_across_catalogs(
    iam_manager,
    identity: Dict[str, Any],
    catalog_ids: List[str],
    roles: List[str],
    policies: List[Any],
) -> Dict[str, Any]:
    """Grant the same role set to one principal across multiple catalogs.

    There is exactly one platform principal per identity (D12). This
    helper ensures the platform row exists, reconciles its
    `custom_policies`, and then issues catalog-scope role grants in
    each requested catalog.

    Args:
        iam_manager: IamService instance
        identity: Identity dict with 'provider' and 'sub' keys
        catalog_ids: Catalog codes whose grants should mirror `roles`
        roles: Role names to grant in each catalog (resolved against the
            tenant's `roles` table)
        policies: Direct policies stored on the platform principal row

    Returns:
        Summary dict with `granted` (catalog ids that succeeded) and
        `failed` (per-catalog error records).
    """
    from dynastore.modules.iam.models import Principal
    from uuid import uuid4

    provider = identity.get("provider")
    subject_id = identity.get("sub")
    if not provider or not subject_id:
        raise ValueError("identity must include 'provider' and 'sub'")
    email = identity.get("email", f"{subject_id}@{provider}")

    # Step 1: ensure the platform principal exists (D12 — single row).
    principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is None:
        principal = await iam_manager.storage.create_principal(
            Principal(
                id=uuid4(),
                provider=provider,
                subject_id=subject_id,
                display_name=email,
                roles=[],
                custom_policies=policies,
                is_active=True,
                attributes={},
            )
        )
        await iam_manager.storage.create_identity_link(
            principal_id=principal.id,
            provider=provider,
            subject_id=subject_id,
        )
    else:
        principal.custom_policies = policies
        await iam_manager.storage.update_principal(principal)

    # Step 2: fan out catalog-scope grants.
    results: Dict[str, Any] = {"granted": [], "failed": []}
    for catalog_id in catalog_ids:
        try:
            schema = await iam_manager._resolve_schema(catalog_id)
            for role_name in roles:
                await iam_manager.storage.grant_catalog_role(
                    principal_id=principal.id,
                    role_name=role_name,
                    catalog_schema=schema,
                )
            results["granted"].append(catalog_id)
            logger.info(
                f"Granted {len(roles)} role(s) to {provider}:{subject_id} on {catalog_id}"
            )
        except Exception as e:
            logger.error(f"Failed to grant roles in {catalog_id}: {e}")
            results["failed"].append({"catalog": catalog_id, "error": str(e)})

    return results


async def list_catalogs_for_identity(
    iam_manager,
    provider: str,
    subject_id: str,
) -> Dict[str, Any]:
    """List catalogs where the identity has at least one grant.

    Delegates to ``get_catalog_roles_for_identity`` so a single, indexed
    lookup over each tenant's ``grants`` table populates both the catalog
    membership flag and the per-catalog role list. The platform flag
    reflects whether the identity carries any platform-scope grant.

    Returns a mapping with:
      - ``platform``       bool — identity carries a platform-scope grant
      - ``catalogs``       list[str] — catalog ids with at least one grant
      - ``catalog_roles``  Dict[str, list[str]] — per-catalog role names;
                           used by catalog-scoped condition handlers
                           (e.g. ``catalog_admin_required``) that need to
                           know *which* roles the principal holds, not
                           just whether they have any
      - ``total``          int — convenience for ``len(catalogs)``
    """
    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol

    catalog_roles = await iam_manager.storage.get_catalog_roles_for_identity(
        provider=provider, subject_id=subject_id
    )
    catalog_list = list(catalog_roles.keys())

    has_platform_grant = False
    principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )
    if principal is not None:
        platform_roles = await iam_manager.storage.list_platform_roles(
            principal_id=principal.id
        )
        has_platform_grant = bool(platform_roles)

    _ = get_protocol  # pragma: no cover — kept for callers that imported these symbols
    _ = CatalogsProtocol
    _ = DatabaseProtocol
    _ = DriverContext

    return {
        "platform": has_platform_grant,
        "catalogs": catalog_list,
        "catalog_roles": catalog_roles,
        "total": len(catalog_list),
    }


async def get_effective_permissions_for_catalog(
    iam_manager,
    provider: str,
    subject_id: str,
    catalog_id: str,
) -> Dict[str, Any]:
    """Detailed permission breakdown for `catalog_id`.

    Reports platform-scope roles, catalog-scope roles, the merged
    effective view, and the direct policies attached to the platform
    principal row. Per-tenant policy grants (D11) land in PR-2 — once
    they exist, the `catalog_policies` slot will reflect them; today
    it mirrors `effective_policies` because policies are platform-only.
    """
    identity = {"provider": provider, "sub": subject_id}

    principal = await iam_manager.storage.get_principal_by_identity(
        provider=provider, subject_id=subject_id
    )

    platform_roles: List[str] = []
    catalog_roles: List[str] = []
    if principal is not None:
        platform_roles = await iam_manager.storage.list_platform_roles(
            principal_id=principal.id
        )
        schema = await iam_manager._resolve_schema(catalog_id)
        catalog_roles = await iam_manager.storage.list_catalog_roles(
            principal_id=principal.id, catalog_schema=schema
        )

    effective_principal: Optional[Any] = await iam_manager.get_effective_permissions(
        identity=identity, catalog_id=catalog_id
    )

    platform_policies = (
        [p.dict() for p in (principal.custom_policies or [])] if principal else []
    )
    effective_policies = (
        [p.dict() for p in (effective_principal.custom_policies or [])]
        if effective_principal
        else []
    )

    return {
        "platform_roles": platform_roles,
        "catalog_roles": catalog_roles,
        "effective_roles": effective_principal.roles if effective_principal else [],
        "platform_policies": platform_policies,
        # Per-tenant policy grants are PR-2; mirrors effective until then.
        "catalog_policies": effective_policies,
        "effective_policies": effective_policies,
        "has_access": effective_principal is not None,
    }
