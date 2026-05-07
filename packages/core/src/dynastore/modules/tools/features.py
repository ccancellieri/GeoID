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
from typing import AsyncIterator, Dict, Any, Optional, List, Set
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.sql import column as sql_column

from dynastore.modules.db_config.query_executor import DbResource, managed_transaction
from dynastore.modules.db_config import shared_queries
from dynastore.models.driver_context import DriverContext
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.tools.cql import parse_cql_filter, PYGEOFILTER_AVAILABLE

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


from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import ItemsProtocol, CatalogsProtocol
from dynastore.models.query_builder import QueryRequest, FieldSelection


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
        # But we need to handle geometry transformation if srid is set
        if config.include_geometry:
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

        # We also want all attributes.
        # '*' in QueryRequest.select handles this (expands to all sidecar fields).
        # But we already added 'geom'.
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

        # Build mapping for parser
        field_mapping = {}
        for name, definition in field_defs.items():
            field_mapping[name] = text(definition.sql_expression)

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
        )
        async for item in response.items:
            yield item
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise


def _build_select_expressions(
    config: FeatureStreamConfig, available_columns: List[str]
) -> List[str]:
    """
    Build SELECT expressions based on property projection and geometry settings.

    Args:
        config: Feature stream configuration
        available_columns: List of available column names in the table

    Returns:
        List of SQL select expressions
    """
    select_expressions = []

    # Handle geometry
    if config.include_geometry and "geom" in available_columns:
        if config.target_srid:
            select_expressions.append(
                f"ST_Transform(geom, {config.target_srid}) AS geom"
            )
        else:
            select_expressions.append("geom")

    # Handle property projection
    if config.property_names:
        # Specific properties requested
        for prop in config.property_names:
            if prop == "geom":
                continue  # Already handled above

            if prop in available_columns:
                # Direct column
                if prop == "bbox_geom" and "bbox_geom" in available_columns:
                    if config.target_srid:
                        select_expressions.append(
                            f"ST_Transform(bbox_geom, {config.target_srid}) AS bbox_geom"
                        )
                    else:
                        select_expressions.append("bbox_geom")
                else:
                    select_expressions.append(f'"{prop}"')
            else:
                # Assume it's in JSONB attributes column
                if "attributes" in available_columns:
                    select_expressions.append(f"(attributes->>'{prop}') AS \"{prop}\"")
    else:
        # No projection - select all non-geometry columns
        for col in available_columns:
            if col == "geom":
                continue  # Already handled
            if col == "bbox_geom":
                if config.target_srid:
                    select_expressions.append(
                        f"ST_Transform(bbox_geom, {config.target_srid}) AS bbox_geom"
                    )
                else:
                    select_expressions.append("bbox_geom")
            else:
                select_expressions.append(f'"{col}"')

    # Fallback to * if nothing selected
    if not select_expressions:
        select_expressions = ["*"]

    return select_expressions


def _build_where_clause(
    config: FeatureStreamConfig, available_columns: List[str]
) -> tuple[str, Dict[str, Any]]:
    """
    Build WHERE clause from CQL filter.

    Args:
        config: Feature stream configuration
        available_columns: List of available column names

    Returns:
        Tuple of (where_clause_string, bind_parameters)

    Raises:
        ValueError: If CQL filter is invalid
    """
    if not config.cql_filter:
        return "", {}

    if not PYGEOFILTER_AVAILABLE:
        raise ValueError(
            "CQL filtering requires pygeofilter to be installed. "
            "Install with: pip install pygeofilter"
        )

    # Build field mapping for CQL parser
    # Map property names to SQL expressions
    field_mapping = {}

    # Direct columns
    for col in available_columns:
        if col == "geom":
            # Geometry column - use PostGIS functions
            field_mapping[col] = sql_column("geom")
        elif col == "attributes":
            # JSONB column - handle separately for nested properties
            continue
        else:
            field_mapping[col] = sql_column(col)

    # For attributes in JSONB, we need to handle them dynamically
    # The CQL parser will validate against valid_props
    # For now, we'll add common JSONB attribute access patterns
    # This is a simplified approach - a full implementation would introspect the schema

    # Valid properties include all columns
    valid_props = set(available_columns)

    try:
        where_sql, bind_params = parse_cql_filter(
            cql_text=config.cql_filter,
            field_mapping=field_mapping,
            valid_props=valid_props,
            parser_type="cql2",
        )
        return where_sql, bind_params
    except Exception as e:
        logger.error(f"CQL filter parsing failed: {e}")
        raise ValueError(f"Invalid CQL filter: {e}") from e
