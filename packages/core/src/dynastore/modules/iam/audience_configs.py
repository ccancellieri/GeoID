"""Audience PluginConfigs — per-catalog / per-collection opt-ins for
anonymous (unauthenticated) access.

Migrated from the geoid extension as part of the dynamic-policy
foundation (#286). The two configs are the runtime knobs the audience
ConditionHandlers (see ``audience_handlers``) consult when deciding
whether anonymous traffic should reach a given catalog or collection.

The configs are policy-agnostic: which routes the audience opens is
declared by the operator-PUT'd policies that reference the matching
condition handler (``catalog_lookup_public_allowed`` and
``collection_write_anonymous_allowed``). The handler answers
"does this catalog/collection accept anonymous traffic at all?"; the
policy answers "for which routes?".
"""
from typing import ClassVar, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig


class CatalogLookupAudience(PluginConfig):
    """Per-catalog opt-in to anonymous lookup-only access.

    When ``is_public=True``, the operator has declared that this catalog
    accepts unauthenticated traffic on whichever routes their policies
    open (typically the lookup-only ``/search`` and
    ``/search/catalogs/{cat}`` surface gated by ``lookup_only_search``).

    Default is ``is_public=False`` (auth-required); anonymous lookup is
    a deliberate, per-catalog opt-in. Activate via:

        PUT /configs/catalogs/{cat}/plugins/catalog_lookup_audience
        {"is_public": true}
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "lookup_audience")
    _freeze_at: ClassVar[Optional[str]] = "catalog"

    is_public: Mutable[bool] = Field(
        default=False,
        description=(
            "When True, this catalog accepts anonymous (no Authorization "
            "header) requests on whichever routes operator-defined policies "
            "open. Read by the ``catalog_lookup_public_allowed`` condition "
            "handler."
        ),
    )


class CollectionWriteAudience(PluginConfig):
    """Per-collection opt-in to anonymous item creation.

    When ``allow_anonymous_create=True``, the operator has declared that
    this collection accepts anonymous (unauthenticated) item-creation
    traffic. The exact write routes that are opened are declared by the
    operator-PUT'd policies that reference the
    ``collection_write_anonymous_allowed`` condition; today's deployments
    typically open both the STAC and OGC-Features POST surfaces:

      * ``POST /stac/catalogs/{cat}/collections/{col}/items``
      * ``POST /features/catalogs/{cat}/collections/{col}/items``

    Default is ``allow_anonymous_create=False`` (auth required). Use
    cases: a public 'intake' collection where contributors submit
    observations without onboarding, governed by the platform's
    duplicate-handling policy and edge rate limits. Activate via:

        PUT /configs/catalogs/{cat}/collections/{col}/plugins/collection_write_audience
        {"allow_anonymous_create": true}
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "write_audience")
    _freeze_at: ClassVar[Optional[str]] = "collection"

    allow_anonymous_create: Mutable[bool] = Field(
        default=False,
        description=(
            "When True, anonymous callers may create items in this "
            "collection on whichever POST surfaces operator-defined "
            "policies open (typically STAC items + OGC Features items). "
            "Read by the ``collection_write_anonymous_allowed`` condition "
            "handler."
        ),
    )
