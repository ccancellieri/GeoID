"""Geoid-extension PluginConfigs — customer-specific operator-facing knobs.

These configs are persisted via the standard configs API at
``/configs/catalogs/{cat}/plugins/{plugin_id}`` and read by the geoid
extension's ConditionHandlers + policy logic.
"""
from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.modules.db_config.platform_config_service import PluginConfig


class CatalogLookupAudience(PluginConfig):
    """Per-catalog opt-in to anonymous lookup endpoints.

    When ``is_public=True``, the GET/POST /search/catalogs/{this_catalog}/geoid
    endpoints accept unauthenticated requests for THIS catalog only. Lookups
    still hit the per-tenant data — no cross-catalog leakage.

    Default is ``is_public=False`` (auth-required); anonymous lookup is a
    deliberate, per-catalog opt-in. Activated by:

        PUT /configs/catalogs/{customer_catalog}/plugins/catalog_lookup_audience
        {"is_public": true}
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "lookup_audience")
    _visibility: ClassVar[Optional[str]] = "catalog"

    is_public: bool = Field(
        default=False,
        description=(
            "When True, the catalog's /search/catalogs/{cat}/geoid lookup "
            "endpoints accept anonymous (no Authorization header) requests."
        ),
    )


class CollectionWriteAudience(PluginConfig):
    """Per-collection opt-in to anonymous item creation.

    When ``allow_anonymous_create=True``, unauthenticated callers can POST
    new items to ``/stac/catalogs/{this_cat}/collections/{this_col}/items``.
    All other operations on the collection follow the normal IAM rules.

    Default is False (auth required). Use cases: a public 'intake' collection
    where citizen-science contributors submit observations without onboarding,
    governed by the platform's duplicate-handling policy and edge rate limits.

        PUT /configs/catalogs/{cat}/collections/{col}/plugins/collection_write_audience
        {"allow_anonymous_create": true}
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "write_audience")
    _visibility: ClassVar[Optional[str]] = "collection"

    allow_anonymous_create: bool = Field(
        default=False,
        description=(
            "When True, POST /stac/catalogs/{cat}/collections/{col}/items "
            "accepts unauthenticated requests for THIS collection only."
        ),
    )
