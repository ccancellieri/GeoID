"""Geoid-extension ConditionHandlers — register at extension lifespan startup."""
import re
from typing import Any, Dict, Optional

from dynastore.modules.iam.conditions import ConditionHandler, EvaluationContext
from dynastore.tools.discovery import get_protocol

_COLLECTION_PATH_RE = re.compile(r"/catalogs/[^/]+/collections/(?P<col>[^/]+)(?:/|$)")


def _resolve_collection_id(ctx: EvaluationContext) -> Optional[str]:
    """Return collection_id from ctx.extras or by parsing ctx.path.

    The IamMiddleware extracts catalog_id from /catalogs/{cat}/... but does
    not extract collection_id today. We parse it from the path here so the
    handler stays self-contained inside the geoid extension.
    """
    extras = getattr(ctx, "extras", None) or {}
    col = extras.get("collection_id")
    if col:
        return col
    path = getattr(ctx, "path", "") or ""
    m = _COLLECTION_PATH_RE.search(path)
    return m.group("col") if m else None


class CatalogLookupAudienceHandler(ConditionHandler):
    """Allow when ``CatalogLookupAudience.is_public`` is True for the request's
    catalog. Used by the geoid-extension anonymous-lookup policy.

    Fails closed on every uncertainty: missing ``ctx.catalog_id``,
    ``ConfigsProtocol`` not registered, ``get_config`` raises, or the
    resolved policy is not a ``CatalogLookupAudience`` instance.
    """

    @property
    def type(self) -> str:
        return "catalog_lookup_public_allowed"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.extensions.geoid.configs import CatalogLookupAudience

        catalog_id = ctx.catalog_id
        if not catalog_id:
            return False
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return False
        try:
            policy = await configs.get_config(CatalogLookupAudience, catalog_id=catalog_id)
        except Exception:
            return False
        return isinstance(policy, CatalogLookupAudience) and bool(policy.is_public)


class CollectionWriteAudienceHandler(ConditionHandler):
    """Allow when ``CollectionWriteAudience.allow_anonymous_create`` is True
    for the request's (catalog_id, collection_id). Used by the geoid-extension
    anonymous-create policy on /stac/.../collections/{col}/items.

    The collection_id is resolved by ``_resolve_collection_id``: prefer
    ``ctx.extras['collection_id']`` (if some future middleware populates it),
    otherwise fall back to a regex parse of ``ctx.path``. The handler stays
    self-contained inside the geoid extension — no core middleware change
    required.

    Fails closed on missing catalog_id, missing collection_id, missing
    ConfigsProtocol, or any error.
    """

    @property
    def type(self) -> str:
        return "collection_write_anonymous_allowed"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.extensions.geoid.configs import CollectionWriteAudience

        catalog_id = ctx.catalog_id
        if not catalog_id:
            return False
        collection_id = _resolve_collection_id(ctx)
        if not collection_id:
            return False
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return False
        try:
            policy = await configs.get_config(
                CollectionWriteAudience,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return False
        return isinstance(policy, CollectionWriteAudience) and bool(policy.allow_anonymous_create)
