"""
Shared feature streaming utilities for querying and streaming features from the database.

This module provides reusable async generators for streaming features with support for:
- CQL filtering (using pygeofilter for security)
- Property projection (selecting specific columns)
- Pagination (limit/offset)
- Geometry handling

Used by export tasks, WFS service, and other components that need to stream features.
"""

import logging
from typing import AsyncIterator, Dict, Any, Optional, List
from pydantic import BaseModel, Field
from sqlalchemy import literal_column

from dynastore.modules.db_config.query_executor import DbResource
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import ItemsProtocol
from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
from dynastore.modules.tools.cql import parse_cql_filter
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class FeatureStreamConfig(BaseModel):
    """Configuration for streaming features from a collection."""

    catalog: str = Field(..., description="Catalog ID (schema name)")
    collection: str = Field(..., description="Collection ID (table name)")
    cql_filter: Optional[str] = Field(None, description="CQL2/ECQL filter expression")
    property_names: Optional[List[str]] = Field(
        None, description="List of properties to include (None = all)"
    )
    limit: Optional[int] = Field(
        None, description="Maximum number of features to return"
    )
    offset: Optional[int] = Field(None, description="Number of features to skip")
    include_geometry: bool = Field(
        True, description="Whether to include geometry in results"
    )
    target_srid: Optional[int] = Field(
        None, description="Target SRID for geometry transformation"
    )


async def stream_features(
    config: FeatureStreamConfig, db_resource: DbResource
) -> AsyncIterator[Dict[str, Any]]:
    """
    Stream features from a collection with optional filtering and projection.

    Args:
        config: Configuration for the feature stream
        db_resource: Database engine or connection

    Yields:
        Feature dictionaries with selected properties

    Raises:
        ValueError: If CQL filter is invalid or contains unknown properties
        RuntimeError: If collection doesn't exist
    """
    # Use ItemsProtocol for optimized, sidecar-aware streaming
    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        raise RuntimeError("ItemsProtocol not available")

    # 1. Build Selects
    selects = []
    if config.property_names:
        for prop in config.property_names:
            if prop == "geom":
                if config.include_geometry:
                    # Explicit transformation handled by FieldSelection if needed,
                    # but typically we just select 'geom' and let sidecar/optimizer handle defaults.
                    # If target_srid provided, we use transformation.
                    if config.target_srid:
                        selects.append(
                            FieldSelection(
                                field="geom",
                                transformation="ST_Transform",
                                transform_args={"srid": config.target_srid},
                            )
                        )
                    else:
                        selects.append(FieldSelection(field="geom"))
            elif prop == "bbox_geom":
                # Special handling for bbox if requested explicitly
                if config.target_srid:
                    selects.append(
                        FieldSelection(
                            field="bbox",  # 'bbox' is the alias in GeometrySidecar
                            alias="bbox_geom",
                            transformation="ST_Transform",
                            transform_args={"srid": config.target_srid},
                        )
                    )
                else:
                    selects.append(FieldSelection(field="bbox", alias="bbox_geom"))
            else:
                selects.append(FieldSelection(field=prop))

        # If geometry requested but not in property_names (common case), add it
        if config.include_geometry and "geom" not in config.property_names:
            if config.target_srid:
                selects.append(
                    FieldSelection(
                        field="geom",
                        transformation="ST_Transform",
                        transform_args={"srid": config.target_srid},
                    )
                )
            else:
                selects.append(FieldSelection(field="geom"))
    else:
        # Select all *
        # When a target SRID is requested, emit an explicit ST_Transform for geom
        # so the optimizer knows to skip the sidecar's raw geom projection when
        # expanding *.  Without a SRID override, * alone is sufficient because
        # the geometry sidecar expansion already includes the raw geom column.
        if config.include_geometry and config.target_srid:
            selects.append(
                FieldSelection(
                    field="geom",
                    transformation="ST_Transform",
                    transform_args={"srid": config.target_srid},
                )
            )
        selects.append(FieldSelection(field="*"))

    # 2. Build Request
    req = QueryRequest(
        select=selects,
        limit=config.limit,
        offset=config.offset,
        raw_where=None,
        include_total_count=False,
    )

    # 3. Handle CQL Filter
    if config.cql_filter:
        # We need to parse CQL to SQL raw_where.
        # Ideally ItemService exposes this, but for now we do it here.
        # We need valid props for validation.
        field_defs = await items_svc.get_collection_fields(
            config.catalog, config.collection, db_resource=db_resource
        )

        valid_props = set(field_defs.keys())
        # Add common aliases
        valid_props.add("geometry")

        # Build mapping for parser.
        # ``literal_column`` (not ``text``) so pygeofilter's ``to_filter`` can
        # build real bound-parameter comparisons; a ``TextClause`` has no
        # comparison operators and silently collapses every predicate to 1=0.
        field_mapping = {}
        for name, definition in field_defs.items():
            field_mapping[name] = literal_column(definition.sql_expression)

        # Parse
        try:
            where_sql, bind_params = parse_cql_filter(
                cql_text=config.cql_filter,
                field_mapping=field_mapping,
                valid_props=valid_props,
                parser_type="cql2",
            )
            if where_sql:
                req.raw_where = where_sql
                req.raw_params = bind_params
        except Exception as e:
            logger.error(f"CQL parse error: {e}")
            raise ValueError(f"Invalid CQL filter: {e}")

    # 4. Stream
    try:
        response = await items_svc.stream_items(
            catalog_id=config.catalog,
            collection_id=config.collection,
            request=req,
            ctx=DriverContext(db_resource=db_resource) if db_resource else None,
            consumer=ConsumerType.OGC_FEATURES,
        )
        async for item in response.items:
            yield item
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise


