"""
Distributed insert/update mixin for ItemService.

Extracted from item_service.py to reduce file size.  All methods access
``self.*`` helpers defined on the main ``ItemService`` class, which
inherits from this mixin.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict, Set, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from dynastore.models.ogc import Feature as _Feature

from dynastore.models.driver_context import DriverContext
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DbResource,
    ResultHandler,
)
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    ComputedKind,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
    ResolvedIdentityRule,
    WriteConflictPolicy,
)
from dynastore.modules.storage.errors import ConflictError, SidecarRejectedError
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.query_optimizer import QueryOptimizer

if TYPE_CHECKING:
    class _Host:
        async def _resolve_physical_schema(
            self, catalog_id: str, *, db_resource: Any = None
        ) -> str: ...
        async def _resolve_physical_table(
            self, catalog_id: str, collection_id: str, *, db_resource: Any = None
        ) -> Optional[str]: ...
        async def _resolve_read_policy(
            self, catalog_id: str, collection_id: str
        ) -> Optional[Any]: ...
        def map_row_to_feature(
            self,
            row: Dict[str, Any],
            col_config: Any,
            read_policy: Optional[Any] = None,
        ) -> "_Feature": ...
else:
    class _Host: ...

logger = logging.getLogger(__name__)


def _select_effective_on_conflict(
    write_policy: Optional["ItemsWritePolicy"],
    matched_rule: Optional["ResolvedIdentityRule"],
) -> "WriteConflictPolicy":
    """Resolve the conflict action for the rule that won identity resolution.

    Per-rule ``on_match`` overrides the policy's ``on_conflict``. With no
    policy at all the fallback is ``UPDATE`` (preserving prior semantics).
    """
    if write_policy is None:
        return WriteConflictPolicy.UPDATE
    if matched_rule is not None and matched_rule.on_match is not None:
        return matched_rule.on_match
    return write_policy.on_conflict


async def _resolve_rule(
    rule: "ResolvedIdentityRule",
    conn: Any,
    phys_schema: str,
    phys_table: str,
    processing_context: Dict[str, Any],
    sidecars: List["SidecarProtocol"],
) -> Optional[Dict[str, Any]]:
    """Resolve identity for a single (resolved) identity rule.

    Semantics: every :class:`ComputedField` in ``rule.match_on`` must
    resolve to the SAME existing row (AND within the rule). The rule
    matches iff the geoid intersection across every match_on field is
    non-empty; the first (canonical) row wins.

    Single-field rules collapse to the prior linear matcher walk —
    walking sidecars in order and returning the first match.
    """
    if not rule.match_on:
        return None

    field_hits: List[Dict[Any, Dict[str, Any]]] = []
    for cf in rule.match_on:
        matcher_str = str(cf.kind)
        by_geoid: Dict[Any, Dict[str, Any]] = {}
        for sidecar in sidecars:
            rec = await sidecar.resolve_existing_item(
                conn, phys_schema, phys_table, processing_context,
                matcher=matcher_str,
            )
            if rec and "geoid" in rec:
                by_geoid.setdefault(rec["geoid"], rec)
        if not by_geoid:
            return None  # rule cannot match: at least one ComputedField unresolved
        field_hits.append(by_geoid)

    # Intersect the geoid sets across every match_on field.
    common = set(field_hits[0].keys())
    for nxt in field_hits[1:]:
        common &= set(nxt.keys())
    if not common:
        return None
    # Pick the row by the first geoid in the first match (stable order).
    for g in field_hits[0]:
        if g in common:
            return field_hits[0][g]
    return None


class ItemDistributedMixin(_Host):
    """Distributed insert/update operations for ItemService."""

    async def insert_or_update_distributed(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Dict[str, Dict[str, Any]],
        col_config: ItemsPostgresqlDriverConfig,
        sidecars: List[SidecarProtocol],
        processing_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Coordinates multi-table upsert for Hub and Sidecars."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            phys_table = collection_id

        # Previously logged a per-item DEBUG line here ("DISTRIBUTED UPSERT:
        # collection=..., phys=..., sidecars=..."). Removed because callers
        # invoke this in tight loops (dimension materialisation, bulk
        # ingestion, migrations) — thousands of identical lines per second
        # flooded Cloud Logging and produced no signal the batch-level
        # loggers don't already carry. If you need per-row tracing for
        # debugging, enable TRACE-style logging at the caller.

        # 1. Resolve write policy from the config waterfall (same as all drivers)
        configs = get_protocol(ConfigsProtocol)
        write_policy: Optional[ItemsWritePolicy] = None
        if configs is not None:
            wp = await configs.get_config(
                ItemsWritePolicy, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
            ))
            if isinstance(wp, ItemsWritePolicy):
                write_policy = wp
        on_conflict = (
            write_policy.on_conflict if write_policy else WriteConflictPolicy.UPDATE
        )

        # 1.5 Acceptance Check — rejections are surfaced to callers as
        # structured SidecarRejectedError, never a silent None. Upper layers
        # aggregate these into an IngestionReport and return 200/207 with
        # the rejection list instead of dropping features without notice.
        for sidecar in sidecars:
            if not sidecar.is_acceptable(hub_payload, processing_context):
                external_id = (
                    processing_context.get("external_id")
                    if isinstance(processing_context, dict)
                    else None
                )
                logger.warning(
                    "Feature rejected by sidecar %s (external_id=%s)",
                    sidecar.sidecar_id, external_id,
                )
                raise SidecarRejectedError(
                    f"Sidecar '{sidecar.sidecar_id}' refused the feature "
                    f"for collection '{catalog_id}/{collection_id}'",
                    external_id=external_id,
                    sidecar_id=sidecar.sidecar_id,
                    reason="sidecar_not_acceptable",
                )

        # Standardized Identity Resolution via Sidecar Protocol.
        # Walk the policy's :class:`IdentityRule` chain in order. Each rule
        # ANDs its ``match_on`` ComputedFields (every field must resolve to
        # the same row); rules OR across the list. First rule that matches
        # wins. Per-rule ``on_match`` overrides ``on_conflict``.
        active_rec = None
        matched_rule: Optional[ResolvedIdentityRule] = None
        rules = (
            write_policy.resolved_identity()
            if write_policy
            else [ResolvedIdentityRule(match_on=[ComputedField(kind=ComputedKind.EXTERNAL_ID)])]
        )
        if on_conflict != WriteConflictPolicy.NEW_VERSION:
            for rule in rules:
                rec = await _resolve_rule(
                    rule, conn, phys_schema, phys_table, processing_context, sidecars,
                )
                if rec:
                    active_rec = rec
                    matched_rule = rule
                    kinds = ",".join(str(cf.kind) for cf in rule.match_on)
                    logger.info(
                        f"DISTRIBUTED UPSERT: found active record "
                        f"geoid={rec.get('geoid')} (rule.match_on=[{kinds}])"
                    )
                    break

        effective_on_conflict = _select_effective_on_conflict(write_policy, matched_rule)

        # 1.6 Batch-level collision guard.
        # Uses active_rec from identity resolution — if a duplicate was found
        # AND the batch policy is refuse_batch, abort the whole batch via
        # ConflictError so the transaction rolls back and the caller returns 409.
        if active_rec and write_policy and write_policy.on_batch_conflict is not None:
            from dynastore.modules.storage.driver_config import BatchConflictPolicy
            if write_policy.on_batch_conflict == BatchConflictPolicy.REFUSE:
                rule_name = (
                    ",".join(str(cf.kind) for cf in matched_rule.match_on)
                    if matched_rule else "unknown"
                )
                logger.warning(
                    "Feature rejected: batch-level collision (refuse_batch) via rule=[%s] "
                    "geoid=%s", rule_name, active_rec.get("geoid")
                )
                raise ConflictError(
                    f"Write refused: duplicate detected via [{rule_name}] "
                    f"(geoid={active_rec.get('geoid')}); policy=refuse_batch",
                    geoid=active_rec.get("geoid"),
                    matcher=rule_name,
                )

        # 1.7 Hash gating: if enabled and an unchanged geometry_hash matches,
        # short-circuit the action to avoid churning identical rows.
        # Issue #220: geometry_hash lives on the geometries sidecar (PG-
        # generated STORED column).  ``active_rec`` carries it via the
        # matcher's JOIN; ``processing_context`` carries the incoming
        # hash computed by the geometries sidecar's pre-write hook.
        if (
            active_rec
            and write_policy
            and write_policy.geometries.skip_if_unchanged_geometry_hash
        ):
            incoming_ch = processing_context.get("geometry_hash")
            if incoming_ch and active_rec.get("geometry_hash") == incoming_ch:
                logger.info(
                    "DISTRIBUTED UPSERT: geometry_hash unchanged — collapsing "
                    f"{on_conflict} to REFUSE_RETURN (geoid={active_rec.get('geoid')})"
                )
                on_conflict = WriteConflictPolicy.REFUSE_RETURN
                effective_on_conflict = WriteConflictPolicy.REFUSE_RETURN

        # 1.8 REFUSE_FAIL: raise immediately so the batch aborts.
        if active_rec and effective_on_conflict == WriteConflictPolicy.REFUSE_FAIL:
            rule_name = (
                ",".join(str(cf.kind) for cf in matched_rule.match_on)
                if matched_rule else "unknown"
            )
            raise ConflictError(
                f"Write refused: identity match via [{rule_name}] "
                f"(geoid={active_rec.get('geoid')}); policy=REFUSE_FAIL",
                geoid=active_rec.get("geoid"),
                matcher=rule_name,
            )

        # 1.9 REFUSE_RETURN: echo the existing record without writing. Caller
        # picks it up via the bulk read-back keyed on the returned geoid.
        if active_rec and effective_on_conflict == WriteConflictPolicy.REFUSE_RETURN:
            logger.info(
                "DISTRIBUTED UPSERT: REFUSE_RETURN — keeping existing record "
                f"geoid={active_rec.get('geoid')}"
            )
            return {"geoid": active_rec["geoid"], "_refuse_return": True}

        result = None
        # 2. Execution Path
        if not active_rec or effective_on_conflict == WriteConflictPolicy.NEW_VERSION:
            if active_rec and effective_on_conflict == WriteConflictPolicy.NEW_VERSION:
                # Archive the existing version before inserting
                expire_at = hub_payload.get("valid_from") or datetime.now(timezone.utc)
                for sidecar in sidecars:
                    await sidecar.expire_version(
                        conn,
                        phys_schema,
                        phys_table,
                        geoid=active_rec["geoid"],
                        expire_at=expire_at,
                    )

            # INSERT NEW
            result = await self._execute_distributed_insert(
                conn,
                phys_schema,
                phys_table,
                hub_payload,
                sidecar_payloads,
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context,
            )

        elif effective_on_conflict == WriteConflictPolicy.REFUSE:
            logger.info(
                "DISTRIBUTED UPSERT: identity matched and REFUSE set. Skipping."
            )
            external_id = processing_context.get("external_id")
            raise SidecarRejectedError(
                f"Feature refused by write policy for collection "
                f"'{catalog_id}/{collection_id}'",
                geoid=str(active_rec["geoid"]) if active_rec and active_rec.get("geoid") is not None else None,
                external_id=external_id if isinstance(external_id, str) else None,
                reason="write_policy_refuse",
            )

        else:
            # UPDATE path (WriteConflictPolicy.UPDATE)
            processing_context["operation"] = "update"
            for sidecar in sidecars:
                val_result = sidecar.validate_update(
                    sidecar_payloads.get(sidecar.sidecar_id, {}),
                    active_rec,
                    processing_context,
                )
                if not val_result.valid:
                    raise ValueError(
                        f"Sidecar {sidecar.sidecar_id} rejected update: {val_result.error}"
                    )

            # Resolve Validity for Hub & Sidecars
            valid_from = processing_context.get("valid_from") or datetime.now(
                timezone.utc
            )
            valid_to = processing_context.get("valid_to")

            # Driver-agnostic tstzrange wrapper — sync workers don't ship
            # asyncpg.  See ``pg_sidecars.attributes._make_tstzrange``.
            from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
                _make_tstzrange,
            )
            validity = _make_tstzrange(
                valid_from, valid_to, lower_inc=True, upper_inc=False,
            )

            # Only write validity to the hub row when the hub table actually has
            # the column — i.e. when partitioning is enabled and "validity" is a
            # declared partition key.  Sidecars track validity independently via
            # their own finalize_upsert_payload() call.
            hub_has_validity = (
                col_config.partitioning is not None
                and col_config.partitioning.enabled
                and "validity" in (col_config.partitioning.partition_keys or [])
            )
            if hub_has_validity:
                if "validity" not in hub_payload:
                    hub_payload["validity"] = validity
                # UPDATE EXISTING: preserve the existing validity range if present
                if active_rec and "validity" in active_rec:
                    validity = active_rec["validity"]
                    hub_payload["validity"] = validity
            elif active_rec and "validity" in active_rec:
                # Non-partitioned: keep validity in context for sidecars but not hub
                validity = active_rec["validity"]

            # Propagate the resolved validity to processing_context so sidecars'
            # finalize_upsert_payload() reuses it instead of synthesising a fresh
            # Range(now(), None) — which would miss ON CONFLICT (geoid, validity)
            # and trip the (geoid, external_id) unique index on re-upsert.
            processing_context["validity"] = validity

            result = await self._execute_distributed_update(
                conn,
                phys_schema,
                phys_table,
                active_rec["geoid"],
                hub_payload,
                sidecar_payloads,
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context,
                active_rec=active_rec,
            )

        return result

    async def _execute_distributed_insert(
        self,
        conn,
        schema,
        hub_table,
        hub_payload,
        sc_data_map,
        col_config,
        sidecars: Optional[List[SidecarProtocol]] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Inserts the hub row + every relevant sidecar row.

        Returns the inserted hub row (dict). The caller is responsible for
        reading back the joined Feature *after* the write transaction has
        committed — see ``fetch_features_bulk``. Doing the read-back inside
        the same transaction would accumulate ``AccessShare`` locks across
        every iteration of a batch loop and pin the connection until commit.
        """
        sidecars = sidecars or []
        processing_context = processing_context or {}
        # A. Insert Hub
        hub_row = await self._insert_table_raw(conn, schema, hub_table, hub_payload)
        hub_data = getattr(hub_row, "_mapping", hub_row)
        geoid = hub_data["geoid"]

        # B. Insert Sidecars
        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # 1. Identity Columns (Conflict Target)
            conflict_cols = sidecar.get_identity_columns()

            # 2. Add partitioning keys to conflict target if enabled
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 3. Finalize Payload (Inject validity, geoid, etc.)
            if sc_id not in sc_data_map and not sidecar.is_mandatory():
                continue

            if "geoid" not in sc_payload:
                sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            full_payload = self._strip_undeclared_columns(
                sidecar, full_payload, col_config
            )

            _gvc = getattr(sidecar, "geometry_value_columns", None)
            geom_cols = cast("Optional[Set[str]]", _gvc()) if callable(_gvc) else None

            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols,
                geom_cols=geom_cols,
            )

            # JSON-FG Place Statistics: insert into <hub_table>_place if configured
            _prep_place = getattr(sidecar, "prepare_place_upsert_payload", None)
            if _prep_place is not None:
                try:
                    place_payload = _prep_place(
                        processing_context.get("_raw_item", {}), processing_context
                    )
                    if place_payload:
                        place_table = f"{hub_table}_place"
                        if "geoid" not in place_payload:
                            place_payload["geoid"] = geoid
                        await self._upsert_sidecar_table_raw(
                            conn, schema, place_table, place_payload, conflict_cols=["geoid"],
                            geom_cols=geom_cols,
                        )
                except Exception as e:
                    logger.warning(f"Place stats upsert skipped for geoid {geoid}: {e}")

        return dict(hub_data)

    async def _execute_distributed_update(
        self,
        conn,
        schema,
        hub_table,
        geoid,
        hub_data,
        sc_data_map,
        col_config,
        sidecars: Optional[List[SidecarProtocol]] = None,
        processing_context: Optional[Dict[str, Any]] = None,
        active_rec=None,
    ) -> Optional[Dict[str, Any]]:
        """Updates the hub row + every relevant sidecar row.

        Returns the updated hub row (dict). Read-back of the joined Feature
        is the caller's responsibility, post-commit — see
        ``fetch_features_bulk``. See ``_execute_distributed_insert`` for the
        rationale (no shared-lock accumulation inside the write tx).
        """
        sidecars = sidecars or []
        # A. Update Hub
        hub_row = await self._update_table_raw(conn, schema, hub_table, geoid, hub_data)
        if not hub_row:
            return None

        row_data = getattr(hub_row, "_mapping", hub_row)

        # B. Resolve Identity and Finalize Payloads for Sidecars
        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # Skip non-mandatory sidecars with no data — they have no table.
            # Mirrors the INSERT path guard: sidecars like StacItemsSidecar that
            # have is_mandatory()=False and no DDL must not attempt a DB write.
            if sc_id not in sc_data_map and not sidecar.is_mandatory():
                continue

            # 1. Identity Columns
            conflict_cols = sidecar.get_identity_columns()
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 2. Finalize Payload
            # Always override geoid: sidecar payloads were prepared with a
            # freshly-generated UUID from item_context; in the UPDATE path we
            # must use the existing hub geoid (=active_rec["geoid"]).
            sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            full_payload = self._strip_undeclared_columns(
                sidecar, full_payload, col_config
            )

            _gvc = getattr(sidecar, "geometry_value_columns", None)
            geom_cols = cast("Optional[Set[str]]", _gvc()) if callable(_gvc) else None

            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols,
                geom_cols=geom_cols,
            )

        return dict(row_data)

    async def _insert_table_raw(self, conn, schema, table, data) -> Dict[str, Any]:
        """Generic table insert (No special geometry handling here, already processed by sidecars)."""
        cols = []
        vals = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
            vals.append(f":{k}")
            params[k] = v

        sql = f'INSERT INTO "{schema}"."{table}" ({", ".join(cols)}) VALUES ({", ".join(vals)}) RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    async def _update_table_raw(
        self, conn, schema, table, geoid, data
    ) -> Dict[str, Any]:
        """Generic table update by geoid."""
        clauses = []
        params = {"geoid": geoid}
        for k, v in data.items():
            if k == "geoid":
                continue
            clauses.append(f'"{k}" = :{k}')
            params[k] = v

        sql = f'UPDATE "{schema}"."{table}" SET {", ".join(clauses)} WHERE geoid = :geoid RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    @staticmethod
    def _strip_undeclared_columns(
        sidecar: SidecarProtocol,
        payload: Dict[str, Any],
        col_config: Any,
    ) -> Dict[str, Any]:
        """Strip payload keys that the sidecar's DDL does not declare.

        Protocol-level guard against DDL/payload drift. Today the only
        optional schema axis on SidecarProtocol is ``validity`` — its column
        exists iff ``sidecar.has_validity()`` or ``"validity"`` is a global
        partition key (see each sidecar's ``get_ddl`` gate). Without this,
        sidecars that unconditionally inject ``validity`` from context/hub
        into their payload will trip UndefinedColumnError (42703) whenever
        they were provisioned without the column.

        New axes with the same DDL/payload-optionality shape should be
        added here rather than duplicated across every sidecar's
        ``finalize_upsert_payload``.
        """
        partition_keys: List[str] = []
        if (
            getattr(col_config, "partitioning", None) is not None
            and getattr(col_config.partitioning, "enabled", False)
        ):
            partition_keys = list(col_config.partitioning.partition_keys or [])

        if "validity" in payload and not (
            sidecar.has_validity() or "validity" in partition_keys
        ):
            payload = {k: v for k, v in payload.items() if k != "validity"}

        return payload

    async def _upsert_sidecar_table_raw(
        self, conn, schema, table, data, conflict_cols: List[str] = ["geoid"],
        geom_cols: Optional[Set[str]] = None,
    ):
        """Sidecar upsert with ON CONFLICT (conflict_cols).

        ``geom_cols`` is the set of columns whose string values are WKB hex and
        must be wrapped with ``ST_GeomFromEWKB`` (a geometry column rejects a raw
        bind). The geometries sidecar supplies it via ``geometry_value_columns``
        so renamed centroid columns and ``centroid_3d`` are covered, not only a
        column literally named ``centroid``. Falls back to the historical fixed
        set when a caller doesn't supply one.
        """
        if geom_cols is None:
            geom_cols = {"geom", "bbox_geom", "centroid"}
        cols: list = []
        vals: list = []
        updates: list = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
            # Geometry columns: pass WKB hex through ST_GeomFromEWKB
            if k in geom_cols and isinstance(v, str):
                vals.append(f"ST_GeomFromEWKB(decode(:{k}, 'hex'))")
                params[k] = v
            # Range columns (e.g. validity TSTZRANGE): duck-type for any Range-like
            # object (asyncpg.Range, etc.) which psycopg2 cannot serialise directly.
            # Expand into lower/upper params and emit tstzrange() so both drivers work.
            elif hasattr(v, "lower") and hasattr(v, "upper") and hasattr(v, "lower_inc"):
                lk, uk = f"{k}_lower", f"{k}_upper"
                lb = "[" if v.lower_inc else "("
                ub = "]" if v.upper_inc else ")"
                vals.append(f"tstzrange(:{lk}, :{uk}, '{lb}{ub}')")
                params[lk] = v.lower
                params[uk] = v.upper
            else:
                vals.append(f":{k}")
                params[k] = v
            if k not in conflict_cols:
                updates.append(f'"{k}" = EXCLUDED."{k}"')

        conflict_target = ", ".join([f'"{c}"' for c in conflict_cols])
        if updates:
            on_conflict_clause = f"DO UPDATE SET {', '.join(updates)}"
        else:
            on_conflict_clause = "DO NOTHING"
        sql = f"""
INSERT INTO "{schema}"."{table}" ({", ".join(cols)})
VALUES ({", ".join(vals)})
ON CONFLICT ({conflict_target}) {on_conflict_clause};
"""
        # DML — must use DQLQuery, not DDLQuery. DDLQuery wraps every
        # statement in a savepoint + pg_try_advisory_xact_lock + 30s
        # statement_timeout that's correct for CREATE/ALTER but adds
        # 5-10x overhead to a per-item upsert hot path.
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params
        )

    async def fetch_features_bulk(
        self,
        conn: DbResource,
        schema: str,
        hub_table: str,
        geoids: List[Any],
        col_config,
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> "List[_Feature]":
        """Bulk-load joined Features for a list of geoids in a single SELECT.

        Intended to be called *after* the write transaction has committed,
        on a fresh connection. This replaces the per-item read-back that
        used to live inside ``_execute_distributed_insert`` /
        ``_execute_distributed_update`` and used to accumulate
        ``AccessShare`` locks across the whole batch.

        Returns a list of Feature objects in the same order as ``geoids``.
        Missing geoids are skipped silently.

        When ``catalog_id``/``collection_id`` are supplied the collection's
        :class:`ItemsReadPolicy` is resolved once and threaded into both the
        :class:`QueryOptimizer` (so SQL ``external_id``-as-id aliasing honours
        the policy) and ``map_row_to_feature`` (so the ``feature_type.expose``
        merge fires) — keeping the post-write read-back wire shape identical to
        the canonical read paths.
        """
        if not geoids:
            return []

        read_policy = None
        if catalog_id is not None and collection_id is not None:
            read_policy = await self._resolve_read_policy(catalog_id, collection_id)

        optimizer = QueryOptimizer(col_config, read_policy=read_policy)
        fetch_req = QueryRequest(
            raw_where="h.geoid = ANY(:bulk_geoids)",
            raw_params={"bulk_geoids": list(geoids)},
            limit=len(geoids),
        )
        sql, params = optimizer.build_optimized_query(fetch_req, schema, hub_table)
        rows = await DQLQuery(
            sql, result_handler=ResultHandler.ALL_DICTS
        ).execute(conn, **params)
        if not rows:
            return []

        # Preserve caller's geoid order so the response lines up 1:1 with the
        # input batch — important for IngestionReport row indexing.
        by_geoid = {row["geoid"]: row for row in rows}
        out: List[Any] = []
        for g in geoids:
            row = by_geoid.get(g)
            if row is not None:
                out.append(
                    self.map_row_to_feature(
                        dict(row), col_config, read_policy=read_policy
                    )
                )
        return out
