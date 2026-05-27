#    Copyright 2025 FAO
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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
ItemsReadPolicy — the read-side companion to :class:`ItemsWritePolicy`.

Part of the items-policy consolidation (see
``docs/architecture/items-policy-consolidation-957-950.md``). A collection
declares its read-time wire-shape contract via ``feature_type``; the PG
read path consumes it (``ItemService`` / ``QueryOptimizer``) to surface
``feature_type.expose`` computed values and honour
``external_id_as_feature_id``.

Transformer *wiring* is NOT here: the ordered transformer chain
lives on the routing config (``OperationDriverEntry.output_transformers``).
``ItemsReadPolicy`` owns the read *shape* only.

Collection-scoped only.
"""

import logging
from typing import Any, ClassVar, Iterable, List, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.computed_fields import FeatureType

_logger = logging.getLogger(__name__)


class ItemsReadPolicy(PluginConfig):
    """Collection-scoped read-time wire-shape contract.

    ``feature_type`` (:class:`FeatureType`) declares which computed fields to
    surface beyond the schema's declared properties (``expose``), how a
    missing enriched value is handled (``failure_mode``), and whether
    ``external_id`` becomes the feature id (``external_id_as_feature_id``).
    The response ``properties`` are derived from ``items_schema`` plus the
    exposed computed fields.

    Transformer wiring is intentionally absent: the ordered
    transformer chain lives on the routing config
    (``OperationDriverEntry.output_transformers``), not here — this policy
    owns the read *shape* only.

    There is no ``ItemsReadPolicy`` at the catalog or platform tier —
    item-shape decisions are inherently per-collection (see the
    auto-memory feedback rule on collection-scoped-only).
    """

    # Grouped with ItemsWritePolicy under ``items.policy`` — the composer keys
    # each leaf by ``class_key``, so this nests as ``items.policy.items_read_policy``
    # alongside ``items.policy.items_write_policy``. Storage/lookup is by class_key,
    # independent of this address.
    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "items",
        "policy",
    )
    _freeze_at: ClassVar[Optional[str]] = "collection"

    feature_type: Mutable[FeatureType] = Field(
        default_factory=FeatureType,
        description=(
            "Wire-shape contract: ``expose`` surfaces additional computed "
            "fields beyond the declared schema; ``failure_mode`` controls "
            "missing-value handling; ``external_id_as_feature_id`` selects "
            "the feature id source."
        ),
    )


async def _validate_read_policy(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[object],
) -> None:
    """Reject ``feature_type.expose`` names the collection can't surface.

    Each ``expose`` entry must resolve to a declared ``ItemsSchema.fields`` key
    or a *readable* ``ComputedField`` from ``ItemsWritePolicy.compute`` at the
    same scope. A statistic derivation declared with ``store=None`` is computed
    but never persisted, so it cannot be read back — exposing it would silently
    yield a missing property (the GLOSIS foot-gun). Both unknown names and
    exposed-but-unstored statistics are rejected fail-fast at config-save so a
    typo or a ``store: null`` never quietly drops an expected output property.
    Skipped when the sibling configs are not yet present.
    """
    if not isinstance(config, ItemsReadPolicy):
        return
    # ``expose=None`` (default) is "surface all declared schema fields" — there
    # is nothing additional to validate; the schema is its own SSOT. ``[]`` is
    # "surface nothing" — also no names to validate. Only a non-empty list
    # names computed/derived fields that the validator must resolve against
    # ``ItemsWritePolicy.compute`` and ``ItemsSchema.fields``.
    if config.feature_type.expose is None:
        return
    expose = list(config.feature_type.expose)
    if not expose or not (catalog_id and collection_id):
        return

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.computed_fields import (
            _STATISTIC_STORAGE_KINDS,
        )
        from dynastore.modules.storage.driver_config import (
            ItemsSchema,
            ItemsWritePolicy,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return

        wp = await configs.get_config(
            ItemsWritePolicy, catalog_id=catalog_id, collection_id=collection_id
        )
        computed = {cf.resolved_name: cf for cf in getattr(wp, "compute", []) or []}
        schema = await configs.get_config(
            ItemsSchema, catalog_id=catalog_id, collection_id=collection_id
        )
        declared = set(getattr(schema, "fields", {}) or {})

        unknown: list[str] = []
        unstored: list[str] = []
        for e in expose:
            if e in declared:
                continue
            cf = computed.get(e)
            if cf is None:
                unknown.append(e)
            elif cf.kind in _STATISTIC_STORAGE_KINDS and cf.storage_mode is None:
                # Computed every write but persisted nowhere — unreadable.
                unstored.append(e)

        if unknown or unstored:
            parts: list[str] = []
            if unknown:
                parts.append(
                    "reference field(s) not produced by ItemsWritePolicy.compute "
                    f"nor declared in ItemsSchema.fields: {sorted(unknown)}"
                )
            if unstored:
                parts.append(
                    "reference computed statistic(s) declared with store=None "
                    "(computed but never persisted, so unreadable): "
                    f"{sorted(unstored)} — set a store (jsonb/columnar) on the "
                    "geometry_stats/attribute_stats entry to surface them"
                )
            raise ValueError(
                "ItemsReadPolicy.feature_type.expose for "
                f"{catalog_id}/{collection_id} " + "; ".join(parts) + ". "
                f"Computed: {sorted(computed)}; declared: {sorted(declared)}."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "read_policy expose validation skipped for %s/%s: %s",
            catalog_id,
            collection_id,
            exc,
        )


ItemsReadPolicy.register_validate_handler(_validate_read_policy)


def project_select_for_feature_type(
    feature_type: FeatureType,
    declared_fields: Optional[Iterable[str]] = None,
) -> List[Any]:
    """Build the SELECT list a read path needs to honour ``feature_type``.

    Sibling SSOT to ``ItemService.map_row_to_feature``: the Python read path
    fetches every property column and lets the row-mapper drop what
    ``feature_type`` does not surface; the SQL-aggregating tile path cannot
    project per row (``ST_AsMVT`` emits every selected column as a tile
    property) so the same wire-shape contract is materialised here as the
    SELECT list. Both paths thus answer to a single declarative contract
    (``ItemsReadPolicy.feature_type``) — never two divergent code paths.

    ``expose`` is trinary (see :class:`FeatureType`):

      * ``None`` (default) — surface every declared schema field
        (``declared_fields`` supplied by the caller; usually
        ``ItemsSchema.fields`` keys).
      * ``[]`` (explicit empty) — surface no schema or computed properties;
        the SELECT list contains only the ``expose_geoid`` / ``expose_created``
        toggles (and the geometry, added separately by the tile transform).
      * Non-empty list — additive: declared schema fields PLUS the listed
        computed ``ComputedField.resolved_name`` values from
        ``ItemsWritePolicy.compute``. ``_validate_read_policy`` already
        rejects unknown / unstored names at config-save time.
    """
    from dynastore.models.query_builder import FieldSelection

    selects: List[FieldSelection] = []

    if feature_type.expose_geoid:
        selects.append(FieldSelection(field="geoid"))

    if feature_type.expose_created:
        selects.append(FieldSelection(field="transaction_time", alias="created"))

    schema = list(declared_fields or [])

    if feature_type.expose is None:
        # Default: mirror the write schema.
        for name in schema:
            selects.append(FieldSelection(field=name))
    elif len(feature_type.expose) == 0:
        # Explicit empty: surface nothing beyond the geoid/created toggles.
        pass
    else:
        # Schema baseline + listed computed/derived (additive).
        for name in schema:
            selects.append(FieldSelection(field=name))
        for name in feature_type.expose:
            selects.append(FieldSelection(field=name))

    return selects


__all__ = ["ItemsReadPolicy", "project_select_for_feature_type"]
