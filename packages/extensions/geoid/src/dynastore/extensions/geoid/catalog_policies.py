"""Geoid per-catalog policy registration and role enrichment.

When the geoid preset is applied to a catalog:
1. Per-catalog geoid policies are created in the tenant schema
2. The catalog's unauthenticated role is enriched with these policies

This enables the lookup-only access model with optional enumeration denial.
"""
import logging

from dynastore.models.protocols.policies import Policy, Role, PermissionProtocol
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# Policy conditions
_LOOKUP_PUBLIC_CONDITION = [{"type": "catalog_lookup_public_allowed"}]
_COLLECTION_WRITE_CONDITION = [{"type": "collection_write_anonymous_allowed"}]


async def register_geoid_policies_for_catalog(catalog_id: str) -> None:
    """Register geoid policies per-catalog and enrich the unauthenticated role.
    
    Creates 4 policies in the catalog's tenant schema:
    1. ALLOW anonymous lookup search
    2. DENY STAC enumeration
    3. DENY Features enumeration
    4. ALLOW anonymous create (per-collection)
    
    Then enriches the catalog's unauthenticated role with these policies.
    
    Called when the geoid preset is applied to a catalog via the
    preset's on_applied() hook.

    Args:
        catalog_id: The catalog ID for which to register policies.
    """
    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning(
            f"PermissionProtocol not available; geoid policies not registered for catalog '{catalog_id}'."
        )
        return

    try:
        # Policy IDs (without catalog suffix since they're already in tenant schema)
        policy_ids = [
            "geoid_anonymous_lookup",
            "geoid_anonymous_stac_deny_lookup_only",
            "geoid_anonymous_features_deny_lookup_only",
            "geoid_anonymous_create_per_collection",
        ]
        
        # Define all policies
        policies_to_create = [
            Policy(
                id="geoid_anonymous_lookup",
                description=(
                    "Anonymous access to POST /search/catalogs/{cat}/items-search "
                    "(resolve one item by geoid or external_id) when the catalog has "
                    "opted in via CatalogLookupAudience.is_public."
                ),
                actions=["POST"],
                resources=[r"/search/catalogs/[^/]+/items-search(/.*)?"],
                conditions=_LOOKUP_PUBLIC_CONDITION,
                effect="ALLOW",
            ),
            Policy(
                id="geoid_anonymous_stac_deny_lookup_only",
                description=(
                    "Block anonymous access to STAC enumeration "
                    "when in lookup-only mode (CatalogLookupAudience.is_public=True). "
                    "Exact-item GET (.../items/{id}) is NOT blocked."
                ),
                actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
                resources=[
                    r"/stac/catalogs/[^/]+",
                    r"/stac/catalogs/[^/]+/collections",
                    r"/stac/catalogs/[^/]+/collections/[^/]+",
                    r"/stac/catalogs/[^/]+/collections/[^/]+/items",
                    r"/stac/catalogs/[^/]+/search",
                    r"/stac/catalogs/[^/]+/collections/search",
                ],
                conditions=_LOOKUP_PUBLIC_CONDITION,
                effect="DENY",
            ),
            Policy(
                id="geoid_anonymous_features_deny_lookup_only",
                description=(
                    "Block anonymous access to OGC Features enumeration "
                    "when in lookup-only mode. Exact-item GET is NOT blocked."
                ),
                actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
                resources=[
                    r"/features/catalogs/[^/]+",
                    r"/features/catalogs/[^/]+/collections",
                    r"/features/catalogs/[^/]+/collections/[^/]+",
                    r"/features/catalogs/[^/]+/collections/[^/]+/items",
                    r"/features/catalogs/[^/]+/search",
                ],
                conditions=_LOOKUP_PUBLIC_CONDITION,
                effect="DENY",
            ),
            Policy(
                id="geoid_anonymous_create_per_collection",
                description=(
                    "Allow anonymous POST to /stac/catalogs/{cat}/collections/{col}/items "
                    "when the collection has opted in via "
                    "CollectionWriteAudience.allow_anonymous_create. "
                    "Note: DENY policies take precedence (deny-wins)."
                ),
                actions=["POST"],
                resources=[r"/stac/catalogs/[^/]+/collections/[^/]+/items"],
                conditions=_COLLECTION_WRITE_CONDITION,
                effect="ALLOW",
            ),
        ]
        
        # Create policies, skipping if they already exist (idempotent)
        created_count = 0
        for policy in policies_to_create:
            try:
                # Check if policy already exists
                existing = await pm.get_policy(policy.id, catalog_id=catalog_id)
                if existing:
                    logger.debug(f"Policy '{policy.id}' already exists, skipping.")
                    continue
                
                # Try to create the policy
                try:
                    await pm.create_policy(policy, catalog_id=catalog_id)
                    created_count += 1
                    logger.debug(f"Created policy '{policy.id}'")
                except ValueError as ve:
                    # Policy already exists (caught exception)
                    if "already exists" in str(ve):
                        logger.debug(f"Policy '{policy.id}' already exists (caught on create), skipping.")
                    else:
                        raise
            except Exception as e:
                logger.error(f"Failed to create policy '{policy.id}': {e}", exc_info=True)

        if created_count > 0:
            logger.info(
                f"Geoid policies created for catalog '{catalog_id}' "
                "(anonymous lookup ALLOW + STAC/Features DENY + anonymous create)."
            )
        
        # Enrich the catalog's unauthenticated role with geoid policies in the tenant schema
        try:
            from dynastore.modules.iam.module import IamModule
            from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.modules.db_config.exceptions import TableNotFoundError
            from dynastore.models.driver_context import DriverContext
            
            anon_role_name = IamRolesConfig().anonymous_role_name
            enriched_role = Role(
                name=anon_role_name,
                policies=policy_ids,
            )
            
            # Try to persist directly to tenant schema
            iam_module = get_protocol(PermissionProtocol)
            if isinstance(iam_module, IamModule):
                storage = iam_module.storage
                db_protocol = get_protocol(DatabaseProtocol)
                catalogs = get_protocol(CatalogsProtocol)
                engine = db_protocol.engine if db_protocol else None
                
                if storage and engine and catalogs:
                    # Resolve the physical schema name for this catalog
                    catalog_schema = await catalogs.resolve_physical_schema(
                        catalog_id,
                        ctx=DriverContext(db_resource=engine),
                        allow_missing=True,
                    )
                    
                    if not catalog_schema:
                        logger.warning(
                            f"Could not resolve physical schema for catalog '{catalog_id}', skipping role enrichment"
                        )
                    else:
                        logger.info(
                            f"Enriching unauthenticated role in schema '{catalog_schema}' for catalog '{catalog_id}'"
                        )
                        
                        try:
                            async with managed_transaction(engine) as conn:
                                # Get existing role from tenant schema
                                existing = await storage.get_role(
                                    anon_role_name, 
                                    schema=catalog_schema, 
                                    conn=conn
                                )
                                
                                # Merge with existing policies if role exists
                                if existing:
                                    merged_policies = list(set(existing.policies + policy_ids))
                                    enriched_role = enriched_role.model_copy(
                                        update={
                                            "policies": merged_policies,
                                            "description": existing.description,
                                            "metadata": existing.metadata,
                                        }
                                    )
                                    await storage.update_role(
                                        enriched_role, 
                                        schema=catalog_schema, 
                                        conn=conn
                                    )
                                    logger.info(
                                        f"Updated unauthenticated role in '{catalog_schema}'. "
                                        f"Policies: {enriched_role.policies}"
                                    )
                                else:
                                    await storage.create_role(
                                        enriched_role, 
                                        schema=catalog_schema, 
                                        conn=conn
                                    )
                                    logger.info(
                                        f"Created unauthenticated role in '{catalog_schema}'. "
                                        f"Policies: {policy_ids}"
                                    )
                        except TableNotFoundError:
                            logger.warning(
                                f"Roles table not found in schema '{catalog_schema}' for catalog '{catalog_id}'. "
                                "Tenant IAM tables may not be provisioned yet."
                            )
                else:
                    logger.warning(
                        f"Cannot enrich role for '{catalog_id}': "
                        f"storage={storage is not None}, engine={engine is not None}, catalogs={catalogs is not None}"
                    )
            else:
                logger.warning(
                    f"PermissionProtocol is not IamModule for '{catalog_id}', cannot enrich role in tenant schema"
                )
                
        except Exception as exc:
            logger.error(
                f"Failed to enrich unauthenticated role for '{catalog_id}': {exc}",
                exc_info=True,
            )
        
    except Exception as exc:
        logger.error(
            f"Failed to register geoid policies for catalog '{catalog_id}': {exc}",
            exc_info=True,
        )
