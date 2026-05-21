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
Item introspection protocol — schema discovery and row-to-feature mapping.

Used by WFS (DescribeFeatureType), OGC Features (field capability validation),
and any extension that needs to know which fields a collection exposes.
"""

from typing import TYPE_CHECKING, Protocol, Optional, Any, Dict, runtime_checkable

from dynastore.models.ogc import Feature

if TYPE_CHECKING:
    from dynastore.models.protocols.field_definition import FieldDefinition


@runtime_checkable
class ItemIntrospectionProtocol(Protocol):
    """
    Protocol for collection schema introspection and row transformation.

    Aggregates field definitions and JSON Schema fragments from all active
    sidecars via ``QueryOptimizer``, presenting a unified view of the
    collection's queryable surface to extensions like WFS and OGC Features.
    """

    async def get_collection_fields(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> "Dict[str, FieldDefinition]":
        """
        Return a mapping of field name → ``FieldDefinition`` for the collection.

        Aggregates fields from all active sidecars via ``QueryOptimizer``.
        Each ``FieldDefinition`` carries the field's SQL expression and its
        ``FieldCapability`` flags (FILTERABLE, SORTABLE, AGGREGATABLE, …).
        Used by WFS DescribeFeatureType and CQL2 filter validation.
        """
        ...

    async def get_collection_schema(
        self,
        catalog_id: str,
        collection_id: str,
        db_resource: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Return the composed JSON Schema for the collection's Feature output.

        User-data ``properties`` are sourced from ``ItemsWritePolicy.resolved_schema``
        when declared (#976); sidecars contribute cross-cutting fragments
        (``geometry``, STAC ``stac_extensions``/``assets``, item-metadata
        ``title``/``description``/``keywords``) which are overlaid on the
        policy's ``properties``. Falls back to pure sidecar aggregation when
        no policy schema is declared.
        """
        ...

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: Any,
        lang: str = "en",
        context: Optional[Any] = None,
        read_policy: Optional[Any] = None,
    ) -> Feature:
        """
        Transform a database row dict into a GeoJSON ``Feature``.

        Chains all active sidecars in declaration order via
        ``FeaturePipelineContext`` (blackboard pattern).  Each sidecar
        contributes its domain: geometry, attributes, STAC metadata, etc.

        ``read_policy`` is the resolved :class:`ItemsReadPolicy` for the
        collection, fetched once per query. It carries the wire-shape
        contract — ``feature_type.expose`` computed-value merge and
        ``feature_type.external_id_as_feature_id``. When ``None`` the row
        mapper applies the default wire shape (external_id-as-id on, no
        expose merge).
        """
        ...

    @property
    def count_items_by_asset_id_query(self) -> Any:
        """
        Pre-built query for counting items grouped by asset ID.

        Used by OGC-API extensions for collection-level metadata generation.
        """
        ...
