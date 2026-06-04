#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""
PrivateEntityTransformer — first implementer of EntityTransformProtocol.

Reshapes a STAC item into the tenant-feature shape on the way to the
private index, and reverses the projection on the way back out for
clients. Pairs with :class:`ItemsElasticsearchPrivateDriver` (the
indexer + searcher) but is registered separately so the same
transformation can be reused with a different storage backend in the
future (e.g. BigQuery) without driver-class proliferation.

Discovery: implements :class:`EntityTransformProtocol`. Active when
``PrivateEntityTransformer`` is listed in the ``transformers`` registry
of the relevant routing config. The auto-augment helper
``_self_register_transformers_into`` will also register it automatically
when the package is loaded.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from dynastore.models.protocols.entity_transform import (
    EntityKind,
    TransformChainContext,
)

logger = logging.getLogger(__name__)


class PrivateEntityTransformer:
    """Transforms STAC items into the tenant-feature shape and back.

    On indexing: builds a tenant-feature doc via
    :func:`build_tenant_feature_doc`. Geometry is indexed EXACTLY by
    default (#1248); :func:`simplify_to_fit` only runs when the private
    driver's ``simplify_geometry`` config flag is enabled. The resulting
    simplification metadata is persisted under ``system.geometry_simplification``
    (#1828 Phase 2) so clients can detect when geometry fidelity was reduced.

    On READ (restore): lifts the per-tenant flat fields back into a
    standard Feature shape — geometry, bbox, properties (with
    ``external_id`` and simplification metadata surfaced into
    properties) — matching what the regular STAC item readers expect.

    Delegates to the subpackage-private :func:`build_tenant_feature_doc`
    plus :func:`dynastore.tools.geometry_simplify.simplify_to_fit`.
    """

    async def transform_for_index(
        self,
        entity: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        """Build the tenant-feature doc + simplify to fit the ES doc-size limit.

        Only meaningful for ``entity_kind == "item"``. For other entity kinds,
        returns the input unchanged (defensive — the private driver is
        an items driver; the routing config should not apply this
        transformer outside that scope, but the no-op keeps things safe
        if it is mis-configured).

        The ``simplify_geometry`` config lookup is memoized on ``ctx.cache``
        keyed by ``(catalog_id, collection_id)`` — a bulk index of N items in
        one collection resolves the flag once, not N times (#1568).
        """
        if entity_kind != "item":
            return entity

        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.tools.geometry_simplify import maybe_simplify_for_es

        # build_tenant_feature_doc accepts a Feature/dict and lifts geoid /
        # external_id / geometry / bbox / properties into the tenant-feature
        # shape. collection_id is required by the helper signature.
        doc = build_tenant_feature_doc(
            entity,
            catalog_id=catalog_id,
            collection_id=collection_id or "",
        )
        # #1248: exact geometry by default — simplification is opt-in via the
        # private driver's ``simplify_geometry`` config flag.
        simplify_geometry = await self._resolve_simplify_geometry(
            catalog_id, collection_id, ctx,
        )
        doc, factor, mode = maybe_simplify_for_es(doc, simplify=simplify_geometry)
        if mode != "none":
            doc.setdefault("system", {})["geometry_simplification"] = {
                "factor": factor,
                "mode": mode,
            }
        return doc

    @staticmethod
    async def _resolve_simplify_geometry(
        catalog_id: str, collection_id: Optional[str],
        ctx: TransformChainContext,
    ) -> bool:
        """Resolve the private driver's ``simplify_geometry`` flag (#1248).

        Exact geometry is indexed by default; simplification is opt-in via
        ``ItemsElasticsearchPrivateDriverConfig.simplify_geometry``.

        Memoized on ``ctx.cache`` so a batch of items in the same
        collection triggers a single ``ConfigsProtocol`` lookup (#1568).
        """
        cache_key = (
            "PrivateEntityTransformer.simplify_geometry",
            catalog_id,
            collection_id,
        )
        cached = ctx.cache.get(cache_key)
        if cached is not None:
            return cached

        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            resolved = False
        else:
            config = await configs.get_config(
                ItemsElasticsearchPrivateDriverConfig,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            resolved = bool(getattr(config, "simplify_geometry", False))
        ctx.cache[cache_key] = resolved
        return resolved

    async def restore_from_index(
        self,
        doc: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
        ctx: TransformChainContext,
    ) -> Any:
        """Reverse the tenant-feature projection back to a STAC-shaped Feature.

        Mirrors the inverse projection currently hand-coded in
        ``ItemsElasticsearchPrivateDriver.read_entities``. Returned
        shape:

            {
                "type":     "Feature",
                "id":       <geoid>,
                "geometry": <doc.geometry>,
                "bbox":     <doc.bbox>,
                "properties": {
                    **<doc.properties>,
                    "external_id":           <doc.external_id>,
                    "simplification_factor": <doc.system.geometry_simplification.factor>,
                    "simplification_mode":   <doc.system.geometry_simplification.mode>,
                    "catalog_id":            <doc.catalog_id>,
                    "collection_id":         <doc.collection_id>,
                },
            }
        """
        if entity_kind != "item" or not isinstance(doc, dict):
            return doc

        props = dict(doc.get("properties") or {})
        for surfaced in ("external_id", "catalog_id", "collection_id"):
            if surfaced in doc and surfaced not in props:
                props[surfaced] = doc[surfaced]
        # Read geometry_simplification from canonical system container (#1828).
        # Defensive fallback reads old flat keys for docs written before this change.
        _gs = doc.get("system", {}).get("geometry_simplification")
        if _gs:
            props["simplification_factor"] = _gs.get("factor")
            props["simplification_mode"] = _gs.get("mode")
        else:
            # Back-compat: flat keys on docs written before #1828 Phase 2.
            for _flat in ("simplification_factor", "simplification_mode"):
                if _flat in doc and _flat not in props:
                    props[_flat] = doc[_flat]

        feature: dict = {
            "type": "Feature",
            "id": doc.get("geoid"),
        }
        if "geometry" in doc:
            feature["geometry"] = doc["geometry"]
        if "bbox" in doc:
            feature["bbox"] = doc["bbox"]
        if props:
            feature["properties"] = props
        return feature
