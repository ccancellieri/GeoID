"""Geoid-extension PermissionProtocol policies.

Registered at extension lifespan startup. Only operators that load the
geoid extension are affected — none of these policies fire by default;
they only become active when an operator opts a catalog or collection into
the relevant audience config.
"""
import logging

from dynastore.models.protocols.authorization import IamRolesConfig
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_LOOKUP_PUBLIC_CONDITION = [{"type": "catalog_lookup_public_allowed"}]
_COLLECTION_WRITE_CONDITION = [{"type": "collection_write_anonymous_allowed"}]


def register_geoid_policies():
    """Register all geoid-extension PermissionProtocol policies + role grants.

    1. ALLOW anonymous on POST /search/catalogs/{cat}/items-search — gated on
       CatalogLookupAudience.is_public via catalog_lookup_public_allowed.
    2. DENY anonymous on /stac/catalogs/{cat}/* under the same condition
       (lookup-only mode locks anonymous out of STAC enumeration).
    3. DENY anonymous on /features/catalogs/{cat}/* under the same condition.
    4. ALLOW anonymous POST on /stac/catalogs/{cat}/collections/{col}/items
       when the collection has opted in via
       CollectionWriteAudience.allow_anonymous_create=True.

    Authenticated callers are unaffected — those paths are governed by
    their own (out-of-this-extension) policies.

    Note on interaction between policies 2 and 4: policy 2 (DENY) and
    policy 4 (ALLOW) can overlap on the same path. Deny-precedence means
    DENY wins when BOTH conditions are true simultaneously. In practice:
    - Catalog with is_public=True + collection with allow_anonymous_create=True:
      the DENY fires (deny-precedence). Operators cannot combine lookup-only
      mode and anonymous create on the same catalog.
    - Catalog with is_public=False (default) + collection with
      allow_anonymous_create=True: no DENY fires; the ALLOW takes effect.
      This is the intended use case for intake collections.
    """
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; geoid policies not registered.")
        return

    pm.register_policy(Policy(
        id="geoid_anonymous_lookup_per_catalog",
        description=(
            "Anonymous access to POST /search/catalogs/{cat}/items-search "
            "(resolve one item by geoid or external_id) when the catalog has "
            "opted in via CatalogLookupAudience.is_public."
        ),
        actions=["POST"],
        resources=[r"/search/catalogs/[^/]+/items-search(/.*)?"],
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="ALLOW",
    ))
    # DENY enumeration routes only — exact-item GET
    # ``/{stac|features}/catalogs/{cat}/collections/{coll}/items/{id}`` is
    # deliberately NOT in the resource list so the broad
    # ``stac_public_access`` / ``features_public_access`` ALLOW policy still
    # surfaces it. Anonymous clients on lookup-only catalogs can therefore:
    #   * POST /search/catalogs/{cat}/items-search           (geoid/ext-id resolve)
    #   * GET .../collections/{coll}/items/{id}              (exact item)
    # ...and nothing else. Collection/item enumeration and the broad item
    # search (POST /search/catalogs/{cat}) stay denied-by-default for
    # anonymous — only the items-search sub-path is opened.
    _STAC_ENUMERATION_RESOURCES = [
        r"/stac/catalogs/[^/]+",
        r"/stac/catalogs/[^/]+/collections",
        r"/stac/catalogs/[^/]+/collections/[^/]+",
        r"/stac/catalogs/[^/]+/collections/[^/]+/items",
        r"/stac/catalogs/[^/]+/search",
        r"/stac/catalogs/[^/]+/collections/search",
    ]
    _FEATURES_ENUMERATION_RESOURCES = [
        r"/features/catalogs/[^/]+",
        r"/features/catalogs/[^/]+/collections",
        r"/features/catalogs/[^/]+/collections/[^/]+",
        r"/features/catalogs/[^/]+/collections/[^/]+/items",
        r"/features/catalogs/[^/]+/search",
    ]
    pm.register_policy(Policy(
        id="geoid_anonymous_stac_deny_lookup_only",
        description=(
            "Block anonymous access to STAC enumeration on catalogs that have "
            "opted into lookup-only mode (CatalogLookupAudience.is_public=True). "
            "Exact-item GET (.../items/{id}) is NOT in this resource list and "
            "remains allowed via stac_public_access."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=_STAC_ENUMERATION_RESOURCES,
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    ))
    pm.register_policy(Policy(
        id="geoid_anonymous_features_deny_lookup_only",
        description=(
            "Block anonymous access to OGC Features enumeration on catalogs "
            "that have opted into lookup-only mode. Exact-item GET is NOT in "
            "this resource list and remains allowed via features_public_access."
        ),
        actions=["GET", "POST", "PUT", "PATCH", "DELETE"],
        resources=_FEATURES_ENUMERATION_RESOURCES,
        conditions=_LOOKUP_PUBLIC_CONDITION,
        effect="DENY",
    ))
    pm.register_policy(Policy(
        id="geoid_anonymous_create_per_collection",
        description=(
            "Allow anonymous POST to /stac/catalogs/{cat}/collections/{col}/items "
            "when the collection has opted in via "
            "CollectionWriteAudience.allow_anonymous_create. "
            "Note: if the catalog also has CatalogLookupAudience.is_public=True, "
            "the STAC DENY policy takes precedence (deny-wins). Keep is_public=False "
            "on catalogs that use anonymous-create collections."
        ),
        actions=["POST"],
        resources=[r"/stac/catalogs/[^/]+/collections/[^/]+/items"],
        conditions=_COLLECTION_WRITE_CONDITION,
        effect="ALLOW",
    ))
    pm.register_role(Role(
        name=IamRolesConfig().anonymous_role_name,
        policies=[
            "geoid_anonymous_lookup_per_catalog",
            "geoid_anonymous_stac_deny_lookup_only",
            "geoid_anonymous_features_deny_lookup_only",
            "geoid_anonymous_create_per_collection",
        ],
    ))
    logger.debug("Geoid policies registered (anonymous lookup ALLOW + STAC/Features DENY + anonymous create).")
