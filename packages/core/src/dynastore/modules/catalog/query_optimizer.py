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

import logging
from typing import Dict, List, Any, Tuple, Set, Optional
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarProtocol,
    FieldDefinition,
    FieldCapability,
    SidecarConfig,
)
from dynastore.models.query_builder import (
    QueryRequest,
    FieldSelection,
    FilterCondition,
    FilterOperator,
    SortOrder,
)
from dynastore.models.ogc import Feature
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.items import ItemsProtocol

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """Optimizes queries based on sidecar capabilities and requested operations.

    OPTIMIZED / SECONDARY query path
    ─────────────────────────────────
    ``build_optimized_query()`` selectively JOINs only the sidecars required for
    a given ``QueryRequest`` (via ``determine_required_sidecars()``), avoiding
    unnecessary table JOINs and fetching only the columns actually needed.

    All ItemService query operations go through this optimizer — it is the
    sole query path.  Sidecars are joined selectively based on the fields
    requested in ``QueryRequest``, avoiding unnecessary table JOINs.
    """

    def __init__(self, col_config: ItemsPostgresqlDriverConfig):
        self.col_config = col_config
        self.field_index: Dict[str, Tuple[SidecarProtocol, FieldDefinition]] = {}
        self._build_capability_index()

    def _build_capability_index(self):
        """Build index of all available fields and their capabilities from active sidecars."""
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        for sc_config in driver_sidecars(self.col_config):
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if not sidecar:
                logger.debug(
                    f"QueryOptimizer: Skipping unavailable sidecar type: {sc_config.sidecar_type}"
                )
                continue

            # Use new protocol method get_queryable_fields()
            for field_name, field_def in sidecar.get_queryable_fields().items():
                self.field_index[field_name] = (sidecar, field_def)
                # Also index by alias if present
                if hasattr(field_def, "alias") and field_def.alias:
                    self.field_index[field_def.alias] = (sidecar, field_def)

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        col_config: ItemsPostgresqlDriverConfig,
        lang: str = "en",
    ) -> Feature:
        """
        Populate a GeoJSON feature from a database row.
        Each sidecar is responsible for mapping its specialized columns/data
        back into the standard GeoJSON structure (geometry, properties).

        Must be stateless (no DB lookups) to support O(1) streaming.
        """
        items_service = get_protocol(ItemsProtocol)
        if items_service is None:
            raise RuntimeError("ItemsProtocol implementation is not registered")
        return items_service.map_row_to_feature(row, col_config, lang=lang)

    def validate_query(self, query: QueryRequest) -> List[str]:
        """
        Validate query against available capabilities.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        # Validate SELECT fields
        for sel in query.select:
            if sel.field == "*":
                continue

            if sel.field not in self.field_index:
                # Try to resolve dynamically
                found_dynamic = False
                for sc_config in driver_sidecars(self.col_config):
                    sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                    if not sidecar:
                        continue
                    dynamic_field = sidecar.get_dynamic_field_definition(sel.field)
                    if dynamic_field:
                        # Cache it for future use in this optimizer instance
                        self.field_index[sel.field] = (sidecar, dynamic_field)
                        found_dynamic = True
                        break

                if not found_dynamic:
                    errors.append(f"Unknown field: {sel.field}")
                    continue

            sidecar, field_def = self.field_index[sel.field]

            if sel.aggregation and not field_def.supports_aggregation(sel.aggregation):
                errors.append(
                    f"Field {sel.field} does not support aggregation {sel.aggregation}"
                )

            if sel.transformation and not field_def.supports_transformation(
                sel.transformation
            ):
                errors.append(
                    f"Field {sel.field} does not support transformation {sel.transformation}"
                )

        # Validate filters
        for filt in query.filters:
            if filt.field not in self.field_index:
                # Allow special Hub fields
                if filt.field in ["geoid", "deleted_at", "transaction_time"]:
                    continue

                # Try to resolve dynamically
                found_dynamic = False
                for sc_config in driver_sidecars(self.col_config):
                    sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                    if not sidecar:
                        continue
                    dynamic_field = sidecar.get_dynamic_field_definition(filt.field)
                    if dynamic_field:
                        self.field_index[filt.field] = (sidecar, dynamic_field)
                        found_dynamic = True
                        break

                if not found_dynamic:
                    errors.append(f"Cannot filter by unknown field: {filt.field}")
                    continue

            _, field_def = self.field_index[filt.field]
            if FieldCapability.FILTERABLE not in field_def.capabilities:
                # errors.append(f"Field {filt.field} is not filterable")
                # Temporarily allow until capability propagation is fully rigorous
                pass

        # Validate sort
        if query.sort:
            for sort in query.sort:
                if sort.field not in self.field_index:
                    # Allow special Hub fields
                    if sort.field in ["geoid", "deleted_at", "transaction_time"]:
                        continue

                    # Try to resolve dynamically
                    found_dynamic = False
                    for sc_config in driver_sidecars(self.col_config):
                        sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                        if not sidecar:
                            continue
                        dynamic_field = sidecar.get_dynamic_field_definition(sort.field)
                        if dynamic_field:
                            self.field_index[sort.field] = (sidecar, dynamic_field)
                            found_dynamic = True
                            break

                    if not found_dynamic:
                        errors.append(f"Cannot sort by unknown field: {sort.field}")
                        continue

                _, field_def = self.field_index[sort.field]
                if FieldCapability.SORTABLE not in field_def.capabilities:
                    errors.append(f"Field {sort.field} is not sortable")

        # Validate group by
        if query.group_by:
            for field in query.group_by:
                if field not in self.field_index:
                    # Allow special Hub fields
                    if field in ["geoid", "deleted_at", "transaction_time"]:
                        continue

                    # Try to resolve dynamically
                    found_dynamic = False
                    for sc_config in driver_sidecars(self.col_config):
                        sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                        if not sidecar:
                            continue
                        dynamic_field = sidecar.get_dynamic_field_definition(field)
                        if dynamic_field:
                            self.field_index[field] = (sidecar, dynamic_field)
                            found_dynamic = True
                            break

                    if not found_dynamic:
                        errors.append(f"Cannot group by unknown field: {field}")
                        continue

                _, field_def = self.field_index[field]
                if FieldCapability.GROUPABLE not in field_def.capabilities:
                    errors.append(f"Field {field} is not groupable")

        return errors

    def get_all_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """
        Returns all fields available for querying across all active sidecars.

        This aggregates static fields from get_queryable_fields() and can be
        extended to include common dynamic fields if needed.
        """
        return {
            name: field_def for name, (sidecar, field_def) in self.field_index.items()
        }

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """
        Composes the combined JSON Schema for Feature output from all active sidecars.

        aggregates:
        - 'geometry' from sidecars that provide it
        - 'properties' contributions from all sidecars
        """
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        properties = {}
        geometry_schema = None

        for sc_config in driver_sidecars(self.col_config):
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if not sidecar:
                continue
            schema = sidecar.get_feature_type_schema()

            # If the sidecar contribution has a 'geometry' key, treat it as the main geometry schema
            if "geometry" in schema:
                geometry_schema = schema["geometry"]
                # Merge any non-geometry keys into properties
                for k, v in schema.items():
                    if k != "geometry":
                        properties[k] = v
            else:
                # Merge directly into properties
                properties.update(schema)

        return {
            "type": "object",
            "properties": properties,
            "geometry": geometry_schema
            or {"type": "object", "description": "GeoJSON geometry"},
            "required": ["geometry", "properties"],
        }

    def determine_required_sidecars(
        self,
        query: QueryRequest,
        require_geometry: bool = True,
    ) -> List[SidecarConfig]:
        """
        Determine which sidecars are actually needed for this query.

        Args:
            query: The QueryRequest to analyse.
            require_geometry: When True (default), always include any sidecar that
                provides a main geometry field.  This ensures GeoJSON Feature
                responses always have geometry even when the query only filters by
                non-spatial fields (e.g. asset_id for virtual collections).
                Pass False for pure aggregate / COUNT queries where geometry is
                not part of the output.

        Returns:
            List of sidecar configs that must be joined, in declaration order.
        """
        required_sidecars = set()

        # Check SELECT fields
        for sel in query.select:
            if sel.field == "*":
                # Need all sidecars if selecting *
                return list(driver_sidecars(self.col_config))

            if sel.field in self.field_index:
                sidecar, _ = self.field_index[sel.field]
                required_sidecars.add(sidecar.sidecar_id)

        # Check filters
        for filt in query.filters:
            if filt.field in self.field_index:
                sidecar, _ = self.field_index[filt.field]
                required_sidecars.add(sidecar.sidecar_id)

        # Check sort
        if query.sort:
            for sort in query.sort:
                if sort.field in self.field_index:
                    sidecar, _ = self.field_index[sort.field]
                    required_sidecars.add(sidecar.sidecar_id)

        # Check group by
        if query.group_by:
            for field in query.group_by:
                if field in self.field_index:
                    sidecar, _ = self.field_index[field]
                    required_sidecars.add(sidecar.sidecar_id)

        # Always include the geometry sidecar for full Feature responses.
        # Without this, queries that only filter by non-spatial fields (e.g.
        # asset_id) would produce features without geometry.
        if require_geometry:
            from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
            for sc_config in driver_sidecars(self.col_config):
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar and sidecar.get_main_geometry_field() is not None:
                    required_sidecars.add(sc_config.sidecar_id)

        # Return only required sidecar configs, preserving declaration order
        return [
            sc for sc in driver_sidecars(self.col_config) if sc.sidecar_id in required_sidecars
        ]

    def build_optimized_query(
        self,
        query: QueryRequest,
        schema: str,
        table: str,
        param_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Build optimized SQL query based on request.

        Returns:
            (sql_string, parameters)
        """
        from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

        # Validate first
        errors = self.validate_query(query)
        if errors:
            raise ValueError(f"Invalid query: {'; '.join(errors)}")

        # Determine required sidecars.
        # Skip geometry for pure aggregate queries (e.g. COUNT-only) — they produce
        # no Feature output and don't need the geometry JOIN.
        is_count_only = bool(query.select) and all(
            sel.aggregation for sel in query.select
        )
        required_sidecars = self.determine_required_sidecars(
            query, require_geometry=not is_count_only
        )

        # Build SELECT clause
        select_fields = []

        if query.include_total_count:
            select_fields.append("COUNT(*) OVER() AS _total_count")

        if any(sel.field == "*" for sel in query.select):
            select_fields.append("h.*")
            # Also include all sidecar SELECT fields — h.* only covers the Hub table.
            for sc_config in required_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    sc_alias = f"sc_{sidecar.sidecar_id}"
                    for f in sidecar.get_select_fields(
                        request=query, hub_alias="h", sidecar_alias=sc_alias, include_all=True
                    ):
                        if f not in select_fields:
                            select_fields.append(f)
        elif not query.select:
            # Default empty select -> similar to `select *` just without h.*
            pass
        else:
            # Always check if geoid is explicitly requested or aggregation implies it
            has_aggregations = any(sel.aggregation for sel in query.select)
            if not has_aggregations and "geoid" not in [s.field for s in query.select]:
                # Only add geoid implicit selection if no aggregations (don't break GROUP BY)
                select_fields.append("h.geoid")

            for sel in query.select:
                if sel.field == "geoid":
                    select_fields.append("h.geoid")
                    continue

                sidecar, field_def = self.field_index[sel.field]
                expr = field_def.sql_expression

                # Apply transformation
                if sel.transformation:
                    # PERFORMANCE OPTIMIZATION: Skip ST_Transform if target SRID matches native SRID
                    if (
                        sel.transformation == "ST_Transform"
                        and field_def.data_type
                        and "geometry" in field_def.data_type
                    ):
                        target_srid = sel.transform_args.get(
                            "srid"
                        ) or sel.transform_args.get("0")
                        if str(target_srid) in field_def.data_type:
                            # Skip transform
                            pass
                        else:
                            args_str = ", ".join(
                                f"'{v}'" if isinstance(v, str) else str(v)
                                for v in sel.transform_args.values()
                            )
                            expr = f"{sel.transformation}({expr}{', ' + args_str if args_str else ''})"
                    else:
                        args_str = ", ".join(
                            f"'{v}'" if isinstance(v, str) else str(v)
                            for v in sel.transform_args.values()
                        )
                        expr = f"{sel.transformation}({expr}{', ' + args_str if args_str else ''})"

                # Apply aggregation
                if sel.aggregation:
                    expr = f"{sel.aggregation}({expr})"

                # Apply alias
                alias = sel.alias or sel.field
                select_fields.append(f"{expr} as {alias}")

        if query.raw_selects:
            select_fields.extend(query.raw_selects)

        # Build WHERE clause
        where_conditions = ["h.deleted_at IS NULL"]
        params = {}

        for i, filt in enumerate(query.filters):
            # Special handling for Hub fields
            if filt.field in ["geoid", "deleted_at", "transaction_time"]:
                expr = f"h.{filt.field}"
            else:
                _, field_def = self.field_index[filt.field]
                expr = field_def.sql_expression

            param_name = f"filter_{i}"
            if param_prefix:
                param_name = f"{param_prefix}_{param_name}"

            # Resolve operator via FilterOperator.to_sql() — single source of truth
            if isinstance(filt.operator, FilterOperator):
                op_sql = filt.operator.to_sql()
                is_spatial = filt.spatial_op or filt.operator.is_spatial
                is_range = filt.operator.is_range
            else:
                try:
                    fo = FilterOperator.from_str(str(filt.operator))
                    op_sql = fo.to_sql()
                    is_spatial = filt.spatial_op or fo.is_spatial
                    is_range = fo.is_range
                except ValueError:
                    op_sql = str(filt.operator)
                    is_spatial = filt.spatial_op or op_sql.upper().startswith("ST_")
                    is_range = op_sql in ("&&", "@>", "<@", "||", "&<", "&>")

            if is_spatial:
                where_conditions.append(f"{op_sql}({expr}, :{param_name})")
            elif is_range or op_sql in ("&&", "@>", "<@", "||", "&<", "&>"):
                if op_sql == "@>" and not str(filt.value).startswith(("[", "(")):
                    where_conditions.append(
                        f"{expr} {op_sql} CAST(:{param_name} AS timestamptz)"
                    )
                else:
                    where_conditions.append(f"{expr} {op_sql} :{param_name}")
            elif op_sql.upper() == "IN":
                where_conditions.append(f"{expr} = ANY(:{param_name})")
            elif op_sql.upper() in ("NIN", "NOT IN"):
                where_conditions.append(f"NOT ({expr} = ANY(:{param_name}))")
            elif op_sql.upper() in ("IS NULL", "IS NOT NULL"):
                where_conditions.append(f"{expr} {op_sql.upper()}")
                continue  # no param bound
            else:
                where_conditions.append(f"{expr} {op_sql} :{param_name}")

            params[param_name] = filt.value

        if query.raw_where is not None:
            # Apply field mapping to raw_where to support sidecar fields (e.g. 'geom' -> 'sc_geom.geom')
            import re

            processed_where = query.raw_where

            # Sort fields by length descending to avoid partial replacement issues (e.g. 'geometry' vs 'geom')
            # But word boundary \b should handle most cases.
            for field_name, (sidecar, field_def) in self.field_index.items():
                if field_name == "geoid":
                    continue

                pattern = rf"\b{re.escape(field_name)}\b"
                processed_where = re.sub(
                    pattern, field_def.sql_expression, processed_where
                )

            where_conditions.append(f"({processed_where})")

        if query.raw_params:
            params.update(query.raw_params)

        # Build JOINs (only for required sidecars)
        joins = []
        # Allow sidecars to inspect the request and contribute via apply_query_context.
        # schema/table are included so sidecars that build their own JOINs don't need
        # a separate call to get_join_clause with missing arguments.
        query_context: Dict[str, Any] = {
            "joins": [],
            "params": params,
            "select_fields": select_fields,
            "where_conditions": where_conditions,
            "schema": schema,
            "table": table,
        }

        for sc_config in required_sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if not sidecar:
                continue

            # Use a stable alias for the sidecar
            sc_alias = f"sc_{sidecar.sidecar_id}"

            # 1. Let sidecar populate context (SELECTs, JOINs, parameters)
            sidecar.apply_query_context(query, query_context)

            # 2. Add default JOIN for the sidecar if not already added by it.
            # Check using "AS {sc_alias} " because JOIN strings look like:
            #   LEFT JOIN "schema"."table_foo" AS sc_foo ON ...
            # The old " {sc_alias} " check (space on both sides) was fragile because
            # the alias is followed by " ON", not a space.
            join_exists = any(f" AS {sc_alias} " in j for j in query_context["joins"])
            if not join_exists:
                query_context["joins"].append(
                    sidecar.get_join_clause(schema, table, hub_alias="h", sidecar_alias=sc_alias)
                )
        
        # Add any extra joins contributed by sidecars
        if query_context["joins"]:
            joins.extend(query_context["joins"])
            
        # Deduplicate select fields while preserving order
        unique_selects = []
        seen = set()
        for field in query_context["select_fields"]:
            if field not in seen:
                unique_selects.append(field)
                seen.add(field)
        select_fields = unique_selects

        # Handle provides_feature_id: ensure the correct column is aliased as 'id'.
        # One sidecar at most can be the feature-id provider (validated at config time).
        # Default is h.geoid; a configured sidecar (e.g. attributes with external_id)
        # can override this for all optimizer-generated queries.
        # Resolve the feature-ID expression once — used for both SELECT alias and item_ids filter.
        # Use COALESCE(sidecar_field, h.geoid) so that items without an external_id still
        # expose their geoid as the public 'id', keeping GET-by-id consistent.
        feature_id_expr: str = "h.geoid"
        for sc_config in required_sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if sidecar and sidecar.provides_feature_id and sidecar.feature_id_field_name:
                sc_alias = f"sc_{sidecar.sidecar_id}"
                feature_id_expr = f"COALESCE({sc_alias}.{sidecar.feature_id_field_name}, h.geoid::text)"
                break

        if not any("AS id" in f or f.rstrip().endswith(" id") for f in select_fields):
            select_fields.append(f"{feature_id_expr} AS id")

        # Filter by item_ids using the resolved feature-ID expression.
        if query.item_ids:
            params["_item_ids"] = query.item_ids
            where_conditions.append(f"({feature_id_expr}) = ANY(:_item_ids)")

        # Build GROUP BY
        group_by_clause = ""
        if query.group_by:
            group_fields = []
            for field in query.group_by:
                if field in ["geoid", "deleted_at"]:
                    group_fields.append(f"h.{field}")
                else:
                    _, field_def = self.field_index[field]
                    group_fields.append(field_def.sql_expression)
            group_by_clause = f"GROUP BY {', '.join(group_fields)}"
        elif any(sel.aggregation for sel in query.select):
            # Implicit GROUP BY for non-aggregated selected fields
            group_fields = []
            for sel in query.select:
                if not sel.aggregation and sel.field != "*":
                    if sel.field in ["geoid", "deleted_at"]:
                        group_fields.append(f"h.{sel.field}")
                    else:
                        _, field_def = self.field_index[sel.field]
                        group_fields.append(field_def.sql_expression)
            if group_fields:
                group_by_clause = f"GROUP BY {', '.join(group_fields)}"

        # Build ORDER BY
        order_by_clause = ""
        if query.sort:
            order_fields = []
            for sort in query.sort:
                if sort.field in ["geoid", "deleted_at"]:
                    order_fields.append(f"h.{sort.field} {sort.direction}")
                else:
                    _, field_def = self.field_index[sort.field]
                    order_fields.append(f"{field_def.sql_expression} {sort.direction}")
            order_by_clause = f"ORDER BY {', '.join(order_fields)}"
        elif not query.group_by and not any(sel.aggregation for sel in query.select):
            # Apply default sort from sidecars if no explicit sort and no aggregation
            for sc_config in required_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if not sidecar:
                    continue
                default_sort = sidecar.get_default_sort()
                if default_sort:
                    order_fields = [
                        f"{field} {direction}" for field, direction in default_sort
                    ]
                    order_by_clause = f"ORDER BY {', '.join(order_fields)}"
                    break

        # Build LIMIT/OFFSET using bind parameters (not direct interpolation)
        limit_clause = ""
        offset_clause = ""
        if query.limit is not None:
            limit_clause = "LIMIT :_limit"
            params["_limit"] = query.limit
        if query.offset is not None:
            offset_clause = "OFFSET :_offset"
            params["_offset"] = query.offset

        # Assemble final query
        sql = f"""
            SELECT {", ".join(select_fields)}
            FROM "{schema}"."{table}" h
            {" ".join(joins)}
            WHERE {" AND ".join(where_conditions)}
            {group_by_clause}
            {order_by_clause}
            {limit_clause}
            {offset_clause}
        """.strip()

        logger.debug(f"Generated optimized query: {sql}")
        return sql, params
