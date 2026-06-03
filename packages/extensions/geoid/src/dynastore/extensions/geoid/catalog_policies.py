"""Geoid per-catalog policy registration and role enrichment.

When the geoid preset is applied to a catalog:
1. Per-catalog geoid policies are created in the tenant schema
2. The catalog's unauthenticated role is enriched with these policies

This enables the lookup-only access model with optional enumeration denial.
"""
import logging
from typing import Any, List

from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
from dynastore.models.protocols.policies import Policy, Role, PermissionProtocol
from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.iam.postgres_policy_storage import PostgresPolicyStorage
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

# --- Priority bands for geoid anonymous-access policies ---
#
# The IAM engine (PolicyService.evaluate_access) uses highest-priority-wins
# with DENY-on-tie at equal priority (#915). These constants establish a
# deterministic composition order so the geoid posture remains correct when
# an operator layers a generic catch-all DENY at the default priority (0) onto
# the unauthenticated role (e.g. a "deny everything not explicitly permitted"
# backstop).
#
# Band assignment:
#   _PRIORITY_ENUM_DENY  = 100 — block STAC/Features browse/list/search while
#       in lookup-only mode. Sits above a generic operator catch-all (0) so
#       these DENYs take effect even when the catch-all is absent.
#
#   _PRIORITY_LOOKUP_ALLOW = 200 — needle-lookup ALLOW on
#       POST /search/catalogs/{cat}/geoid-search. Must outrank a generic
#       deny-all at priority 0: if ALLOW and that catch-all DENY tied at 0,
#       DENY would win by tie-breaking, silently blocking the public lookup
#       surface. Priority 200 ensures the lookup ALLOW is always the decisive
#       policy for its own path (the enumeration DENYs do NOT cover
#       /search/…/geoid-search, so they do not compete here).
#
#   _PRIORITY_ANON_CREATE = 500 — per-collection anonymous item POST on
#       .../collections/{col}/items. The enumeration DENYs (_PRIORITY_ENUM_DENY)
#       cover that same item-POST path (they match GET/POST/…/items). This
#       ALLOW must outrank them so an opted-in collection's item-POST wins while
#       every browse/list/search verb stays denied by the DENYs.
_PRIORITY_ENUM_DENY = 100
_PRIORITY_LOOKUP_ALLOW = 200
#
#   _PRIORITY_ADMIN = 400 — ALLOW for the catalog admin role. Must outrank
#       the enumeration DENYs at 100 so a catalog admin can read and write
#       via Features and STAC regardless of the lookup-only posture. Sits
#       below _PRIORITY_ANON_CREATE (500) which is for a different role and
#       path set, so there is no interference.
_PRIORITY_ADMIN = 400
_PRIORITY_ANON_CREATE = 500

# --- Anonymous-write rate-limit constants ---
#
# These live here in policy data (not in engine code) so an operator can
# edit or remove the limit via a policy update without a code deployment.
# Caps anonymous writes at 100 000 per day catalog-wide; scope=catalog means
# one shared counter keyed by catalog_id. Requests over the limit get 429.
_ANON_WRITE_LIMIT = 100_000
_ANON_WRITE_WINDOW_SECONDS = 86_400

# Policy conditions
#
# ``_LOOKUP_PUBLIC_CONDITION`` gates the lookup-only posture on the catalog
# having opted in (``CatalogLookupAudience.is_public``). It is SHARED by the
# enumeration-DENY policies below, so it must stay a pure public-mode gate:
# adding a request-shape predicate (e.g. ``lookup_only_search``) here would make
# the DENY policies match only *needle* requests — a broadening enumeration
# request would then skip the DENY entirely and fall through, inverting the
# block into a leak. Request-shape guards for the ALLOW live in
# ``_ANON_LOOKUP_ALLOW_CONDITION`` instead.
_LOOKUP_PUBLIC_CONDITION = [{"type": "catalog_lookup_public_allowed"}]
_COLLECTION_WRITE_CONDITION = [{"type": "collection_write_anonymous_allowed"}]

# Conditions for the anonymous *lookup* ALLOW only. Beyond the public-mode gate,
# the IAM layer itself enforces that the request is a needle lookup — it must
# carry a ``geoid`` / ``external_id`` and no broadening ``bbox`` / ``intersects``
# / ``datetime`` / ``filter`` / ``q`` field (the ``lookup_only_search`` handler).
# The route handler also validates the body; enforcing it declaratively here is
# defence-in-depth, so a future route refactor cannot silently turn the public
# lookup surface into an enumeration surface (un-fao/GeoID#1204 P2-R7). Order
# matters: ``evaluate_all`` is short-circuit AND, so the cheap public-mode gate
# runs first and a broadening request is rejected (the ALLOW simply does not
# match → deny-by-default) before the body is inspected.
_ANON_LOOKUP_ALLOW_CONDITION = [
    {"type": "catalog_lookup_public_allowed"},
    {"type": "lookup_only_search"},
]


async def _enrich_role(
    storage: Any,
    engine: Any,
    catalog_id: str,
    catalog_schema: str,
    role_name: str,
    policy_ids: List[str],
) -> None:
    """Merge *policy_ids* into *role_name* in *catalog_schema*, creating it if absent.

    Mirrors the same idempotent merge pattern for every role the geoid preset
    enriches, avoiding duplication between the unauthenticated and admin blocks.
    Raises :class:`TableNotFoundError` to the caller when tenant IAM tables
    are not yet provisioned.
    """
    from dynastore.modules.db_config.exceptions import TableNotFoundError

    try:
        async with managed_transaction(engine) as conn:
            existing = await storage.get_role(role_name, schema=catalog_schema, conn=conn)
            if existing:
                merged = list(set(existing.policies + policy_ids))
                updated = existing.model_copy(update={"policies": merged})
                await storage.update_role(updated, schema=catalog_schema, conn=conn)
                logger.info(
                    "Updated '%s' role in '%s'. Policies: %s",
                    role_name,
                    catalog_schema,
                    updated.policies,
                )
            else:
                new_role = Role(name=role_name, policies=policy_ids)
                await storage.create_role(new_role, schema=catalog_schema, conn=conn)
                logger.info(
                    "Created '%s' role in '%s'. Policies: %s",
                    role_name,
                    catalog_schema,
                    policy_ids,
                )
    except TableNotFoundError:
        logger.warning(
            "Roles table not found in schema '%s' for catalog '%s'. "
            "Tenant IAM tables may not be provisioned yet.",
            catalog_schema,
            catalog_id,
        )


async def register_geoid_policies_for_catalog(catalog_id: str) -> None:
    """Register geoid policies per-catalog and enrich role sets.

    Creates 6 policies in the catalog's tenant schema:
    1. ALLOW anonymous lookup search
    2. DENY STAC enumeration (lookup-only mode)
    3. DENY Features enumeration (lookup-only mode)
    4. ALLOW anonymous create per-collection (rate-limited)
    5. ALLOW catalog admin full Features access
    6. ALLOW catalog admin full STAC + search access

    Then enriches two catalog roles:
    - unauthenticated: policies 1-4 (public lookup + blind-dropbox)
    - admin: policies 5-6 (full Features + STAC/search read/write)

    NOTE: the admin role grants protocol-level access to catalog data.
    Granting this role to a user is done via the platform grants model
    (/admin/* IAM-management routes), not via a per-catalog policy.
    The /admin/* routes are platform-tier and are not governed here.

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
        # Policy IDs assigned to the unauthenticated (anonymous) role.
        anon_policy_ids = [
            "geoid_anonymous_lookup",
            "geoid_anonymous_stac_deny_lookup_only",
            "geoid_anonymous_features_deny_lookup_only",
            "geoid_anonymous_create_per_collection",
        ]
        # Policy IDs assigned to the catalog admin role.
        admin_policy_ids = [
            "geoid_admin_features",
            "geoid_admin_data",
        ]
        
        # Define all policies
        policies_to_create = [
            Policy(
                id="geoid_anonymous_lookup",
                description=(
                    "Anonymous access to POST /search/catalogs/{cat}/geoid-search "
                    "(resolve one item by geoid or external_id) when the catalog has "
                    "opted in via CatalogLookupAudience.is_public."
                ),
                actions=["POST"],
                resources=[r"/search/catalogs/[^/]+/geoid-search(/.*)?"],
                conditions=_ANON_LOOKUP_ALLOW_CONDITION,
                effect="ALLOW",
                # Must outrank a generic catch-all DENY at priority 0 so a
                # tie does not cause DENY-wins to block the lookup surface.
                # See _PRIORITY_LOOKUP_ALLOW above.
                priority=_PRIORITY_LOOKUP_ALLOW,
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
                # Explicit band; see _PRIORITY_ENUM_DENY above. The create
                # ALLOW (_PRIORITY_ANON_CREATE=500) must outrank this DENY
                # on the shared item-POST path.
                priority=_PRIORITY_ENUM_DENY,
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
                # Explicit band; see _PRIORITY_ENUM_DENY above.
                priority=_PRIORITY_ENUM_DENY,
            ),
            Policy(
                id="geoid_anonymous_create_per_collection",
                description=(
                    "Allow anonymous POST of a new item to "
                    "/stac/catalogs/{cat}/collections/{col}/items or "
                    "/features/catalogs/{cat}/collections/{col}/items when the "
                    "collection has opted in via "
                    "CollectionWriteAudience.allow_anonymous_create. Carries a "
                    "higher priority than the enumeration DENYs so the opted-in "
                    "item-POST wins the #915 ranking, while every browse/list/"
                    "search verb stays denied (the DENYs keep full coverage). "
                    "A catalog-wide daily rate-limit caps anonymous writes "
                    "at _ANON_WRITE_LIMIT per _ANON_WRITE_WINDOW_SECONDS."
                ),
                actions=["POST"],
                # Both intake paths: STAC transactions and OGC Features. The
                # enumeration DENYs match this same item-POST (they cover
                # GET/POST/.../items), so this ALLOW MUST outrank them — see
                # ``priority`` below.
                resources=[
                    r"/stac/catalogs/[^/]+/collections/[^/]+/items",
                    r"/features/catalogs/[^/]+/collections/[^/]+/items",
                ],
                # Conditions are short-circuit AND: the cheap per-collection opt-in
                # gate runs first so only opted-in creates reach the counter.
                # scope=catalog means one shared bucket per catalog_id (not per
                # principal), capping the total anonymous write volume.
                # The limit lives here in policy data so an operator can edit or
                # remove it via a policy update without a code deployment.
                conditions=[
                    # Gate 1 — per-collection opt-in (cheap, no counter I/O).
                    {"type": "collection_write_anonymous_allowed"},
                    # Gate 2 — catalog-wide daily cap on anonymous writes.
                    # Over-limit → 429. scope=catalog → one counter per catalog_id.
                    {
                        "type": "rate_limit",
                        "config": {
                            "limit": _ANON_WRITE_LIMIT,
                            "window_seconds": _ANON_WRITE_WINDOW_SECONDS,
                            "scope": "catalog",
                            "methods": ["POST", "PUT", "PATCH", "DELETE"],
                        },
                    },
                ],
                effect="ALLOW",
                # The STAC/Features enumeration DENYs (_PRIORITY_ENUM_DENY=100)
                # cover the item-POST path. Evaluation is highest-priority-wins
                # with DENY-on-tie (PolicyService.evaluate_access, #915), so a
                # lower- or equal-priority ALLOW would lose to those DENYs.
                # _PRIORITY_ANON_CREATE=500 lets the create ALLOW win — but ONLY
                # for a request that also passes the per-collection
                # ``collection_write_anonymous_allowed`` gate. Browse/list/search
                # verbs never match this POST-only ALLOW, so they remain denied:
                # the "blind dropbox" stays blind.
                priority=_PRIORITY_ANON_CREATE,
            ),
            # --- Catalog admin policies ---
            #
            # These give a principal who holds the catalog's "admin" role full
            # read/write access to the catalog's Features and STAC/search surfaces.
            # They sit at _PRIORITY_ADMIN=400, which outranks the enumeration DENYs
            # (100) so the admin is never blocked by the lookup-only posture.
            Policy(
                id="geoid_admin_features",
                description=(
                    "Catalog admin may use the OGC Features protocol on this catalog "
                    "(full CRUD on collections and items). Priority 400 outranks the "
                    "enumeration DENYs so admin access is never blocked by "
                    "lookup-only mode."
                ),
                actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
                resources=[
                    r"/features/catalogs/[^/]+",
                    r"/features/catalogs/[^/]+/.*",
                ],
                conditions=[],
                effect="ALLOW",
                priority=_PRIORITY_ADMIN,
            ),
            Policy(
                id="geoid_admin_data",
                description=(
                    "Catalog admin may read and write catalog data via STAC and "
                    "the search API. Priority 400 outranks the enumeration DENYs "
                    "so admin access is never blocked by lookup-only mode."
                ),
                actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
                resources=[
                    r"/stac/catalogs/[^/]+",
                    r"/stac/catalogs/[^/]+/.*",
                    r"/search/catalogs/[^/]+",
                    r"/search/catalogs/[^/]+/.*",
                ],
                conditions=[],
                effect="ALLOW",
                priority=_PRIORITY_ADMIN,
            ),
        ]

        # Pin every policy to this catalog's partition. The IAM evaluator reads
        # per-catalog policies with ``partition_key=catalog_id`` (see
        # ``PolicyService.get_policy`` / ``_resolve_effective_policies``), but
        # the ``Policy`` model defaults ``partition_key`` to ``"global"``. Left
        # at the default, ``create_policy(..., catalog_id=cat)`` writes these
        # rows into the tenant schema's ``policies_global`` partition while
        # every read looks in ``policies_{catalog_id}`` — so the policies are
        # created yet never found, and the whole lookup/deny/create posture
        # silently never takes effect. ``partition_key == catalog_id`` is also
        # the convention the catalog cascade-cleanup relies on
        # (``IamCatalogScopedOwner`` deletes ``WHERE partition_key = catalog_id``).
        for _policy in policies_to_create:
            _policy.partition_key = catalog_id

        # Provision the dedicated partition policies_{catalog_id} before any
        # INSERT so this catalog's rows land in a per-catalog slice. The
        # per-catalog partition is what lets catalog-delete cascade cleanly
        # DROP TABLE policies_{catalog_id} (see iam/cascade_owner.py).
        # CREATE TABLE IF NOT EXISTS makes this idempotent. A failure here is
        # non-fatal: without the dedicated partition the rows fall through to
        # the policies_default DEFAULT partition and stay addressable — reads
        # query the parent policies table by partition_key regardless of which
        # partition physically holds the row — so registration still proceeds.
        try:
            _db_proto = get_protocol(DatabaseProtocol)
            _catalogs = get_protocol(CatalogsProtocol)
            _engine = _db_proto.engine if _db_proto else None
            if _engine and _catalogs:
                _tenant_schema = await _catalogs.resolve_physical_schema(
                    catalog_id,
                    ctx=DriverContext(db_resource=_engine),
                    allow_missing=True,
                )
                if _tenant_schema:
                    _ps = PostgresPolicyStorage()
                    async with managed_transaction(_engine) as _conn:
                        await _ps.ensure_policy_partition(
                            _conn, catalog_id, schema=_tenant_schema
                        )
                    logger.debug(
                        "Ensured partition policies_%s in schema '%s'.",
                        catalog_id,
                        _tenant_schema,
                    )
        except Exception as _exc:
            logger.warning(
                "Could not pre-provision policy partition for catalog '%s' "
                "(non-fatal — rows fall back to the policies_default "
                "partition and remain addressable): %s",
                catalog_id,
                _exc,
            )

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
                "Geoid policies created for catalog '%s' "
                "(anonymous lookup ALLOW + STAC/Features DENY + anonymous create "
                "+ admin Features + admin STAC/search).",
                catalog_id,
            )

        # --- Role enrichment ---
        #
        # Two roles get enriched in the catalog's tenant schema:
        #   unauthenticated — public lookup + blind-dropbox create
        #   admin           — full Features + STAC/search read/write
        #
        # NOTE: the "admin" role here grants *protocol-level* access to catalog
        # data. Assigning this role to a user principal is done via the platform
        # grants model (/admin/* IAM-management routes), NOT by creating another
        # per-catalog policy — the /admin/* routes are platform-tier and are
        # managed outside this preset.
        try:
            from dynastore.modules.iam.module import IamModule

            iam_module = get_protocol(PermissionProtocol)
            if isinstance(iam_module, IamModule):
                storage = iam_module.storage
                db_protocol = get_protocol(DatabaseProtocol)
                catalogs = get_protocol(CatalogsProtocol)
                engine = db_protocol.engine if db_protocol else None

                if storage and engine and catalogs:
                    catalog_schema = await catalogs.resolve_physical_schema(
                        catalog_id,
                        ctx=DriverContext(db_resource=engine),
                        allow_missing=True,
                    )

                    if not catalog_schema:
                        logger.warning(
                            "Could not resolve physical schema for catalog '%s', "
                            "skipping role enrichment",
                            catalog_id,
                        )
                    else:
                        await _enrich_role(
                            storage=storage,
                            engine=engine,
                            catalog_id=catalog_id,
                            catalog_schema=catalog_schema,
                            role_name=IamRolesConfig().anonymous_role_name,
                            policy_ids=anon_policy_ids,
                        )
                        await _enrich_role(
                            storage=storage,
                            engine=engine,
                            catalog_id=catalog_id,
                            catalog_schema=catalog_schema,
                            role_name="admin",
                            policy_ids=admin_policy_ids,
                        )
                else:
                    logger.warning(
                        "Cannot enrich roles for '%s': "
                        "storage=%s, engine=%s, catalogs=%s",
                        catalog_id,
                        storage is not None,
                        engine is not None,
                        catalogs is not None,
                    )
            else:
                logger.warning(
                    "PermissionProtocol is not IamModule for '%s', "
                    "cannot enrich roles in tenant schema",
                    catalog_id,
                )

        except Exception as exc:
            logger.error(
                "Failed to enrich roles for '%s': %s",
                catalog_id,
                exc,
                exc_info=True,
            )
        
    except Exception as exc:
        logger.error(
            f"Failed to register geoid policies for catalog '{catalog_id}': {exc}",
            exc_info=True,
        )
