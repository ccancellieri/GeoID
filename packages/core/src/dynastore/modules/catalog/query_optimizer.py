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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import logging
from typing import Dict, List, Any, Mapping, Tuple, Optional
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.read_policy import (
    ItemsReadPolicy,
    is_user_readable_schema_field,
)
from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    ConsumerType,
    SidecarProtocol,
    FieldDefinition,
    FieldCapability,
    SidecarConfig,
)
from dynastore.models.query_builder import (
    QueryRequest,
    FilterOperator,
)
from dynastore.models.ogc import Feature
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.items import ItemsProtocol

logger = logging.getLogger(__name__)


def _extract_alias(sql_fragment: str) -> str:
    """Return the logical output column name from a SELECT-list SQL fragment.

    Handles ``expr AS alias`` / ``expr as alias`` (case-insensitive) and bare
    ``table.col`` forms.  Used to detect alias collisions between sidecar
    ``*`` expansion and caller-provided overrides (``FieldSelection`` or
    ``raw_selects``) so the optimizer can skip a sidecar projection whose
    alias is already covered.
    """
    f_lower = sql_fragment.lower()
    as_idx = f_lower.rfind(" as ")
    if as_idx != -1:
        return sql_fragment[as_idx + 4:].strip().strip('"')
    return sql_fragment.rsplit(".", 1)[-1].strip().strip('"')


def _quote_alias(alias: str) -> str:
    """Double-quote a projection alias so Postgres preserves its exact case.

    Unquoted aliases are folded to lowercase by Postgres, which breaks any
    consumer that references the alias with its declared (mixed/upper) case —
    e.g. the MVT outer wrapper that selects ``"CODE"`` from the inner subquery.
    Idempotent: an already-quoted alias is returned unchanged. ``*`` is never
    quoted. See #719.
    """
    a = alias.strip()
    if a == "*":
        return a
    if a.startswith('"') and a.endswith('"'):
        return a
    return '"' + a.replace('"', '""') + '"'


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

    def __init__(
        self,
        col_config: ItemsPostgresqlDriverConfig,
        consumer: Optional[ConsumerType] = None,
        read_policy: Optional[ItemsReadPolicy] = None,
        schema_fields: Optional[Mapping[str, FieldDefinition]] = None,
    ):
        self.col_config = col_config
        # Default to GENERIC when no consumer is supplied — preserves the
        # pre-consumer-aware behaviour for ad-hoc/internal callers.
        self.consumer: ConsumerType = consumer or ConsumerType.GENERIC
        # ``read_policy`` carries the wire-shape contract. ``None`` means
        # "use defaults" (external_id_as_feature_id=False → geoid is the id,
        # #1212). Callers thread the resolved ItemsReadPolicy where they have it.
        self.read_policy: Optional[ItemsReadPolicy] = read_policy
        # ``schema_fields`` is the collection's ``ItemsSchema.fields`` mapping —
        # the SSOT for user-declared properties. Threaded in by read paths that
        # build a server-side SELECT projection from the schema (notably MVT,
        # via ``project_select_for_feature_type``) so this optimizer can
        # validate those names against the schema even when a field lives in
        # the JSONB attributes blob and not in a native column.
        self.schema_fields: Optional[Mapping[str, FieldDefinition]] = schema_fields
        self.field_index: Dict[str, Tuple[SidecarProtocol, FieldDefinition]] = {}
        self._build_capability_index()
        self._enrich_field_index_from_schema()

    def _external_id_as_feature_id(self) -> bool:
        """Resolve the wire-shape decision from the read policy, defaulting to
        False when no policy is supplied — the stable internal ``geoid`` is the
        feature id unless a collection opts in (#1212)."""
        if self.read_policy is None:
            return False
        return self.read_policy.feature_type.external_id_as_feature_id

    def resolve_validity_expression(self) -> Optional[str]:
        """Resolve the SQL expression for the ``validity`` column.

        Returns ``"h.validity"`` when validity is a hub partition key, or
        the sidecar-qualified expression (e.g. ``"sc_attributes.validity"``)
        when a sidecar materialises validity. Returns ``None`` when the
        collection has no validity column at all (the post-#974 default
        when ``ItemsWritePolicy.enable_validity`` is ``False``). Callers
        that need a SQL-safe placeholder can use ``NULL::tstzrange``.
        """
        from dynastore.modules.storage.drivers.pg_sidecars.registry import (
            SidecarRegistry,
        )

        partition_keys = (
            getattr(
                getattr(self.col_config, "partitioning", None),
                "partition_keys",
                None,
            )
            or []
        )
        if "validity" in partition_keys:
            return "h.validity"
        for sc_config in driver_sidecars(self.col_config):
            sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if sidecar and sidecar.has_validity():
                return f"sc_{sidecar.sidecar_id}.validity"
        return None

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

            for field_name, field_def in sidecar.get_queryable_fields().items():
                self.field_index[field_name] = (sidecar, field_def)
                # Also index by alias if present
                if hasattr(field_def, "alias") and field_def.alias:
                    self.field_index[field_def.alias] = (sidecar, field_def)

    def _enrich_field_index_from_schema(self) -> None:
        """Surface ``ItemsSchema.fields`` entries not already in the index.

        ``ItemsSchema`` is the SSOT for a collection's queryable properties.
        Sidecars and items_schema can mix columnar and JSONB storage in the
        same collection (per-field ``FieldDefinition.access``); this method
        is the read-side router that picks the right SQL form per field:

          * Native column expected (``schema_field_materializes_as_column``
            is True for the field): the attributes sidecar must already have
            advertised it via ``get_queryable_fields()``. If we still see it
            absent from ``field_index``, the sidecar config is out of sync
            with the items_schema — skip + log; injecting a JSONB extract
            here would silently NULL-out every row in the wire response.
          * JSONB-stored field (the predicate is False): emit a typed JSONB
            extraction via :meth:`FeatureAttributeSidecar.jsonb_property_field`.

        Names already in ``field_index`` (native columns, identity, validity,
        hub fields) win unconditionally — sidecars publish the authoritative
        SQL expression. Geometry-typed, ``expose=False`` and reserved names
        are skipped via :func:`...read_policy.is_user_readable_schema_field`
        and the ``ItemsSchema`` reserved-name validator at config-save.

        Read-side mirror of the write-side
        :func:`...field_constraints.bridge_schema_to_attribute_sidecar`
        (column synthesis) — neither path leaks fields the other does not see.
        """
        if not self.schema_fields:
            return
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )
        from dynastore.modules.storage.drivers.pg_sidecars.registry import (
            SidecarRegistry,
        )
        from dynastore.modules.storage.field_constraints import (
            schema_field_materializes_as_column,
        )

        attr_sidecar: Optional[FeatureAttributeSidecar] = None
        for sc_config in driver_sidecars(self.col_config):
            sc = SidecarRegistry.get_sidecar(sc_config, lenient=True)
            if isinstance(sc, FeatureAttributeSidecar):
                attr_sidecar = sc
                break
        if attr_sidecar is None:
            return

        for name, fd in self.schema_fields.items():
            if name in self.field_index:
                # Sidecar already advertised this — its sql_expression is
                # authoritative (covers both columnar and any sidecar-managed
                # JSONB form). Never shadow it from items_schema.
                continue
            if not is_user_readable_schema_field(fd):
                continue
            if schema_field_materializes_as_column(fd):
                # Field is supposed to be a native column but the sidecar
                # didn't surface it — likely an out-of-sync attribute_schema.
                # Skip rather than fabricate either SQL form: a JSONB extract
                # would silently NULL-out the column; a synthesised column
                # reference would crash the query if the column does not yet
                # exist. The right next step is to re-ensure the sidecar
                # config from items_schema (write-side bridge), not to paper
                # over it here.
                logger.warning(
                    "items_schema field %r expects a native column but the "
                    "attributes sidecar did not advertise it — skipping "
                    "enrichment. Re-ensure the sidecar config from "
                    "ItemsSchema (bridge_schema_to_attribute_sidecar).",
                    name,
                )
                continue
            jsonb_fd = attr_sidecar.jsonb_property_field(name, fd)
            if jsonb_fd is None:
                # Sidecar is COLUMNAR-only — there is no JSONB blob column
                # to extract from. The items_schema declared a field that is
                # not materialised as a native column AND can't be reached as
                # a JSONB key. Likely an out-of-sync schema (FAST/required not
                # set on the field, or sidecar storage_mode pinned COLUMNAR).
                # Skip rather than synthesise SQL that references a
                # non-existent column.
                logger.warning(
                    "items_schema field %r cannot be enriched: sidecar "
                    "storage_mode is COLUMNAR-only and the field is not "
                    "advertised as a native column. Either mark the field "
                    "FAST/required, or switch the sidecar to JSONB/AUTOMATIC.",
                    name,
                )
                continue
            self.field_index[name] = (attr_sidecar, jsonb_fd)

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
        # Thread the resolved wire-shape contract so the row mapper can honour
        # ``feature_type.external_id_as_feature_id`` and merge the
        # ``feature_type.expose`` computed values onto ``properties``.
        return items_service.map_row_to_feature(
            row, col_config, lang=lang, read_policy=self.read_policy
        )

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
                # Allow special Hub fields
                if sel.field in ["geoid", "deleted_at", "transaction_time"]:
                    continue

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

                # #974: ``validity`` is special. On a collection with no
                # temporal axis (no hub partition key and no sidecar
                # materialises validity) the column does not exist anywhere,
                # so ``build_optimized_query`` silently drops a ``validity``
                # filter (see ``resolve_validity_expression`` -> None). The
                # OGC Features endpoint always appends a default
                # ``validity @> now()`` filter, so rejecting it here would
                # turn every listing on such a collection into a 400. Defer
                # to the build step: allow it when there is no validity
                # column to test against.
                if filt.field == "validity" and self.resolve_validity_expression() is None:
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
            # Filterable by default (see FieldDefinition.is_filterable): a field
            # is rejected only when it declares an explicit capability set that
            # omits FILTERABLE — the deliberate opt-out. Fields that leave their
            # capabilities unspecified stay filterable, so authors need not spell
            # out FILTERABLE on every field.
            if not field_def.is_filterable():
                errors.append(f"Field {filt.field} is not filterable")

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
                # Sortable by default, mirroring is_filterable: only an explicit
                # capability set that omits SORTABLE blocks ordering. Avoids the
                # surprise of a field being filterable-by-default but not
                # sortable.
                if not field_def.is_sortable():
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

        # Helper: a sidecar serves the active consumer when its
        # ``serves_consumers()`` is None (consumer-agnostic) or includes
        # the active consumer.  Used both for the ``select *`` shortcut
        # and the final filter step below.
        from dynastore.modules.storage.drivers.pg_sidecars.registry import (
            SidecarRegistry as _SR,
        )

        def _serves_active_consumer(sc_config: SidecarConfig) -> bool:
            sc = _SR.get_sidecar(sc_config, lenient=True)
            if sc is None:
                return True  # unknown sidecar — keep prior behaviour
            served = type(sc).serves_consumers()
            return served is None or self.consumer in served

        def _drop_unconditional_envelope(
            sidecars: List[SidecarConfig],
        ) -> List[SidecarConfig]:
            """Remove the access_envelope sidecar when access is provably
            unconditional (blanket allow, no deny, no union).

            The envelope sidecar is RETAINED when:
            * ``access_filter`` is ``None`` — fail-closed; sidecar appends FALSE.
            * ``is_unconditional`` is False — row-level WHERE must run.
            Only dropped when ``is_unconditional`` is True (pure JOIN overhead).
            """
            _af = getattr(query, "access_filter", None)
            if _af is not None and _af.is_unconditional:
                return [
                    sc for sc in sidecars
                    if getattr(sc, "sidecar_type", None) != "access_envelope"
                ]
            return sidecars

        # Check SELECT fields
        for sel in query.select:
            if sel.field == "*":
                # Need all sidecars that serve the active consumer when
                # selecting *.  Consumer-specific sidecars (e.g.
                # stac_metadata for non-STAC consumers) are skipped — the
                # caller asked for "everything for *this* response shape".
                # Conditional ABAC envelope: drop the sidecar when access is
                # provably unconditional (same invariant as the explicit-
                # projection path below — see _drop_unconditional_envelope).
                return _drop_unconditional_envelope([
                    sc for sc in driver_sidecars(self.col_config)
                    if _serves_active_consumer(sc)
                ])

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

        # Check raw SQL (WHERE + SELECT projections). CQL filters are compiled
        # into ``raw_where`` against the resolved sidecar expressions (e.g.
        # ``sc_attributes.adm2_pcode``), bypassing the structured
        # ``query.filters`` checked above; server-side code also injects raw
        # SELECT projections into ``raw_selects`` that reference sidecar columns
        # directly (e.g. the STAC ``/search`` sort key
        # ``(sc_attributes.attributes->>'datetime')::timestamptz AS valid_from``
        # used when a collection has no validity column — post-#974 default).
        # Either form references a sidecar that the structured analysis never
        # saw, so without scanning both a sidecar column would reference a table
        # that was never JOINed -> "missing FROM-clause entry for table
        # sc_<id>". Mark any sidecar whose alias (``sc_<id>``) or whose
        # queryable field name appears in the raw SQL as required.
        import re as _re

        def _mark_required_from_raw_sql(raw_sql: str) -> None:
            for sc_config in driver_sidecars(self.col_config):
                alias = f"sc_{sc_config.sidecar_id}"
                if _re.search(rf"\b{_re.escape(alias)}\b", raw_sql):
                    required_sidecars.add(sc_config.sidecar_id)
            for field_name, (sidecar, _field_def) in self.field_index.items():
                if field_name == "geoid":
                    continue
                if _re.search(rf"\b{_re.escape(field_name)}\b", raw_sql):
                    required_sidecars.add(sidecar.sidecar_id)

        if query.raw_where:
            _mark_required_from_raw_sql(query.raw_where)
        for raw_select in query.raw_selects:
            _mark_required_from_raw_sql(raw_select)

        # Always include the geometry sidecar for full Feature responses.
        # Without this, queries that only filter by non-spatial fields (e.g.
        # asset_id) would produce features without geometry.
        if require_geometry:
            from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
            for sc_config in driver_sidecars(self.col_config):
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar and sidecar.get_main_geometry_field() is not None:
                    required_sidecars.add(sc_config.sidecar_id)

        # Row-level ABAC (#1457): force-include the access_envelope sidecar
        # whenever the collection declares it, regardless of whether any field
        # of it appears in SELECT / filters / sort. The envelope exposes no
        # SELECT fields (expose=False), so the heuristics above never pick it up
        # on an explicit-projection query — yet it MUST always JOIN so its WHERE
        # clause runs. ``apply_query_context`` fails closed (appends ``FALSE``)
        # when ``request.access_filter`` is unset, so a read that forgets to
        # compile a scope returns zero rows rather than leaking the whole table.
        # A user-facing read sets ``access_filter`` via
        # ``access_scope.compile_read_access_filter``; a privileged/system read
        # must pass ``AccessFilter.allow_everything()`` explicitly to opt out.
        # (The ``select=*`` path already includes the sidecar via the consumer-
        # serving branch above; this closes the explicit-projection gap.)
        for sc_config in driver_sidecars(self.col_config):
            if getattr(sc_config, "sidecar_type", None) == "access_envelope":
                required_sidecars.add(sc_config.sidecar_id)

        # Conditional ABAC envelope (read-perf, #2550): see
        # _drop_unconditional_envelope above.  Both the select=* early-return
        # path and this explicit-projection path apply the same drop so that
        # unconditional (blanket-allow) principals skip the envelope JOIN on
        # all read shapes.
        return _drop_unconditional_envelope([
            sc for sc in driver_sidecars(self.col_config)
            if sc.sidecar_id in required_sidecars
        ])

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

        if any(sel.field == "*" for sel in query.select):
            select_fields.append("h.*")
            # Collect the set of fields explicitly overridden by non-* FieldSelections
            # AND by raw_selects entries (used by the MVT transform to inject
            # ``ST_AsMVTGeom(...) AS geom``), so sidecar expansion can skip them
            # and avoid duplicate AS aliases that would surface as Postgres
            # ``column reference is ambiguous`` errors when the inner SELECT is
            # wrapped (e.g. tile post-processing ``SELECT "geom" FROM (...)``).
            explicit_field_names = {
                sel.field for sel in query.select if sel.field != "*"
            }
            explicit_field_names.update(
                _extract_alias(rs) for rs in query.raw_selects
            )
            # Also include all sidecar SELECT fields — h.* only covers the Hub table.
            for sc_config in required_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config, lenient=True)
                if sidecar:
                    sc_alias = f"sc_{sidecar.sidecar_id}"
                    for f in sidecar.get_select_fields(
                        request=query, sidecar_alias=sc_alias, include_all=True
                    ):
                        logical_name = _extract_alias(f)
                        if logical_name in explicit_field_names:
                            # An explicit FieldSelection already covers this field;
                            # skip the sidecar projection to avoid a duplicate alias.
                            continue
                        if f not in select_fields:
                            select_fields.append(f)
            # Render any explicit non-* FieldSelections (e.g. ST_Transform overrides).
            # These are added AFTER h.* so the explicit projection appears in the
            # SELECT list alongside the wildcard expansion.
            for sel in query.select:
                if sel.field == "*" or sel.field not in self.field_index:
                    continue
                _, field_def = self.field_index[sel.field]
                expr = field_def.sql_expression
                if sel.transformation:
                    if (
                        sel.transformation == "ST_Transform"
                        and field_def.data_type
                        and "geometry" in field_def.data_type
                    ):
                        target_srid = sel.transform_args.get("srid") or sel.transform_args.get("0")
                        if str(target_srid) not in field_def.data_type:
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
                alias = sel.alias or sel.field
                rendered = f"{expr} as {_quote_alias(alias)}"
                if rendered not in select_fields:
                    select_fields.append(rendered)
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
                select_fields.append(f"{expr} as {_quote_alias(alias)}")

        if query.raw_selects:
            select_fields.extend(query.raw_selects)

        # Build WHERE clause
        where_conditions = ["h.deleted_at IS NULL"]
        params = {}

        for i, filt in enumerate(query.filters):
            # Special handling for Hub fields
            if filt.field in ["geoid", "deleted_at", "transaction_time"]:
                expr = f"h.{filt.field}"
            elif filt.field == "validity":
                # #974: validity is no longer present on the hub by default
                # (``ItemsWritePolicy.enable_validity`` defaults to False).
                # Resolve the actual SQL expression — when no hub partition
                # key and no sidecar materialise validity, the column does
                # not exist anywhere, so we silently drop the filter
                # (matches OGC behaviour: an unsatisfiable temporal filter
                # produces no rows; here we have no column to test against).
                resolved = self.resolve_validity_expression()
                if resolved is None:
                    continue
                expr = resolved
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

            if is_spatial and op_sql.upper().startswith("ST_"):
                # ST_* function form: ST_Intersects(expr, :param)
                where_conditions.append(f"{op_sql}({expr}, :{param_name})")
            elif is_spatial:
                # Infix spatial operator (e.g. BBOX → "&&"): the value is an
                # EWKT geometry literal.  Rendering as ``&&(expr, :param)``
                # causes PostgreSQL to parse the two-element parenthesised list
                # as a composite/record type and then apply ``&&`` to that
                # single record operand, producing:
                #   operator does not exist: && record
                # The correct form is ``expr && ST_GeomFromEWKT(:param)`` so
                # each side of the infix operator is a typed geometry.
                where_conditions.append(f"{expr} {op_sql} ST_GeomFromEWKT(:{param_name})")
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

            # Map bare field tokens to their sidecar expressions (e.g.
            # 'geom' -> 'sc_geom.geom'). The negative lookbehind on '.' (and a
            # word char) skips tokens that are ALREADY table-qualified — CQL
            # filters compile straight to resolved expressions like
            # 'sc_attributes.adm2_pcode', and a bare '\b' would re-match the
            # trailing 'adm2_pcode' and double-prefix it into the invalid
            # 'sc_attributes.sc_attributes.adm2_pcode'.
            for field_name, (sidecar, field_def) in self.field_index.items():
                if field_name == "geoid":
                    continue

                # Idempotency guard (#1255): a CQL filter compiles straight to
                # the resolved ``sql_expression`` before reaching here. If that
                # expression is already present, re-running the substitution
                # would match the bare ``field_name`` *inside* it — the leading
                # ``"`` of a quoted COLUMNAR identifier
                # (``sc_attributes."adm2_pcode"``) is neither ``.`` nor a word
                # char, so the lookbehind below does not skip it — producing the
                # invalid ``sc_attributes."sc_attributes."adm2_pcode""``. Skip
                # fields whose resolved expression is already in place.
                if field_def.sql_expression in processed_where:
                    continue

                pattern = rf"(?<![.\w])\b{re.escape(field_name)}\b"
                processed_where = re.sub(
                    pattern, field_def.sql_expression, processed_where
                )

            # #974: When the bare ``validity`` token survives the field-index
            # substitution above, it means no sidecar exposes ``validity`` as
            # a queryable field (``ItemsWritePolicy.enable_validity`` is
            # False on this collection). Rewrite remaining references to the
            # resolved hub/sidecar expression — or to the unbounded range
            # ``'(,)'::tstzrange`` when the column does not exist anywhere —
            # so an upstream ``where_sql`` emitter (e.g.
            # ``shared_queries.build_filter_clause``) that hard-codes
            # ``validity`` does not produce
            # ``column "validity" does not exist``.
            #
            # ``NULL::tstzrange`` MUST NOT be used as the placeholder: under
            # three-valued logic ``NULL && tstzrange(...)`` and
            # ``NULL @> :dt`` both evaluate to NULL, so every row is silently
            # dropped and a datetime-filtered PG-only /search returns nothing.
            # A collection with no validity column is "always valid", so the
            # all-of-time range ``'(,)'::tstzrange`` is the correct sentinel —
            # it matches every temporal predicate and preserves the full result.
            if re.search(r"\bvalidity\b", processed_where):
                resolved = self.resolve_validity_expression()
                replacement = resolved if resolved is not None else "'(,)'::tstzrange"
                processed_where = re.sub(
                    r"\bvalidity\b", replacement, processed_where
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

        # Pre-compute the sort-providing sidecar config for the INNER-join optimisation.
        # When the default-sort path is active (no explicit sort, no GROUP BY, no
        # aggregation), the sidecar that provides the default sort (typically the
        # attributes sidecar via external_id) can drive the query from its
        # idx_ext_id_sort index — but only when joined as INNER, not LEFT.
        # LEFT JOIN forces hub to be the outer table and blocks index-ordered
        # streaming.  We detect the sort-provider here so the JOIN loop below
        # can pass join_type="INNER" for just that one sidecar without affecting
        # all other sidecars' join semantics.
        _default_sort_sc_config = None
        if (
            not query.sort
            and not query.group_by
            and not any(sel.aggregation for sel in query.select)
        ):
            for _sc_conf in required_sidecars:
                _sc = SidecarRegistry.get_sidecar(_sc_conf, lenient=True)
                if _sc and _sc.get_default_sort():
                    _default_sort_sc_config = _sc_conf
                    break

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
                # Use INNER JOIN for the sort-providing sidecar so the planner can
                # drive from its ext_id_sort index and stream rows in ORDER BY order
                # without a full filesort.  Every other sidecar keeps LEFT semantics.
                _join_type = "INNER" if sc_config is _default_sort_sc_config else "LEFT"
                query_context["joins"].append(
                    sidecar.get_join_clause(
                        schema, table, hub_alias="h", sidecar_alias=sc_alias,
                        join_type=_join_type,
                    )
                )
        
        # Add any extra joins contributed by sidecars
        if query_context["joins"]:
            joins.extend(query_context["joins"])
            
        # Deduplicate select fields by their logical OUTPUT column name (not by
        # exact SQL string), preserving order and keeping the first occurrence.
        # Two fragments that resolve to the same output name — e.g. an explicit
        # ``sc_geometries.area as area`` from a FieldSelection plus a sidecar's
        # own projection of the same column — would otherwise both reach the
        # SELECT, and the wrapping ``SELECT "area" FROM (...)`` would fail with
        # PostgreSQL ``column reference "area" is ambiguous``. ``_extract_alias``
        # maps both ``... as name`` and bare ``tbl.col`` to ``name``; ``tbl.*``
        # maps to the sentinel ``*``, which never collides with a real column.
        unique_selects = []
        seen_names: set[str] = set()
        for field in query_context["select_fields"]:
            name = _extract_alias(field)
            if name not in seen_names:
                unique_selects.append(field)
                seen_names.add(name)
        select_fields = unique_selects

        # Resolve the feature-ID expression. ``provides_feature_id`` on the
        # sidecar is a capability flag (at most one sidecar can provide it).
        #
        # Two cases:
        # 1. Non-STAC consumers: the wire-shape decision lives on the read
        #    policy — when ``external_id_as_feature_id`` is False,
        #    ``feature.id == geoid`` regardless of sidecar capability.
        # 2. STAC consumers: STAC items MUST expose the authored external_id
        #    (the original STAC item id) as the stable, client-facing identifier.
        #    Use COALESCE(external_id, geoid) unconditionally so that GET /items
        #    and POST /search produce the same id regardless of the per-collection
        #    read-policy setting (search.py uses the same COALESCE convention).
        #    Items without an external_id fall back to the geoid UUID.
        feature_id_expr: str = "h.geoid"
        use_coalesce = self._external_id_as_feature_id() or self.consumer == ConsumerType.STAC
        if use_coalesce:
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
            # ``_item_ids`` is always bound as a text[] (QueryRequest.item_ids is
            # List[str]). When the feature-id expression is the bare ``h.geoid``
            # uuid column — i.e. no external-id/STAC COALESCE was applied, which
            # is exactly the canonical-index read (read_policy=None) — Postgres
            # rejects the ``uuid = text`` comparison with
            # ``operator does not exist: uuid = text``. That error was being
            # swallowed by ``canonical_index_read._fetch_raw_rows`` (returns {}),
            # silently dropping the enriched row so the ES indexer fell back to a
            # minimal doc with no ``stats`` lane and only ``system.geoid``. Cast
            # the parameter array to ``uuid[]`` on this branch so the comparison
            # is uuid = uuid (and the geoid index stays usable). The COALESCE
            # branch is already text, so it is left untouched.
            if feature_id_expr == "h.geoid":
                where_conditions.append("h.geoid = ANY(CAST(:_item_ids AS uuid[]))")
            else:
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
