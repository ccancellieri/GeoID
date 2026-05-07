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
``PrivateEntityTransformer`` is listed in
``operations[TRANSFORM]`` of the relevant routing config. The
auto-augment helper ``_self_register_transformers_into`` will also
register it automatically when the package is loaded.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from dynastore.models.protocols.entity_transform import EntityKind

logger = logging.getLogger(__name__)


class PrivateEntityTransformer:
    """Transforms STAC items into the tenant-feature shape and back.

    On INDEX: builds a tenant-feature doc via
    :func:`build_tenant_feature_doc` and then applies
    :func:`simplify_to_fit` to keep the doc within ES's per-document size
    limit. The resulting ``simplification_factor`` and
    ``simplification_mode`` are persisted on the doc so clients can
    detect when geometry fidelity was reduced.

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
    ) -> Any:
        """Build the tenant-feature doc + simplify to fit the ES doc-size limit.

        Only meaningful for ``entity_kind == "item"``. For other entity kinds,
        returns the input unchanged (defensive — the private driver is
        an items driver; the routing config should not apply this
        transformer outside that scope, but the no-op keeps things safe
        if it is mis-configured).
        """
        if entity_kind != "item":
            return entity

        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        from dynastore.tools.geometry_simplify import simplify_to_fit

        # build_tenant_feature_doc accepts a Feature/dict and lifts geoid /
        # external_id / geometry / bbox / properties into the tenant-feature
        # shape. collection_id is required by the helper signature.
        doc = build_tenant_feature_doc(
            entity,
            catalog_id=catalog_id,
            collection_id=collection_id or "",
        )
        doc, factor, mode = simplify_to_fit(doc)
        doc["simplification_factor"] = factor
        doc["simplification_mode"] = mode
        return doc

    async def restore_from_index(
        self,
        doc: Any,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        entity_kind: EntityKind,
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
                    "simplification_factor": <doc.simplification_factor>,
                    "simplification_mode":   <doc.simplification_mode>,
                    "catalog_id":            <doc.catalog_id>,
                    "collection_id":         <doc.collection_id>,
                },
            }
        """
        if entity_kind != "item" or not isinstance(doc, dict):
            return doc

        props = dict(doc.get("properties") or {})
        for surfaced in (
            "external_id",
            "simplification_factor",
            "simplification_mode",
            "catalog_id",
            "collection_id",
        ):
            if surfaced in doc and surfaced not in props:
                props[surfaced] = doc[surfaced]

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
