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

"""
Distributed insert/update mixin for ItemService.

Extracted from item_service.py to reduce file size.  All methods access
``self.*`` helpers defined on the main ``ItemService`` class, which
inherits from this mixin.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict, Set, Tuple, TYPE_CHECKING, cast

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
from dynastore.tools.db import qualify_table, quote_ident
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
        # Identity resolution runs for EVERY conflict policy — NEW_VERSION
        # included. NEW_VERSION always inserts a fresh geoid regardless of a
        # match, but the archive step still needs the matched row to close the
        # prior version's validity upper bound (close_on_new_version). Skipping
        # resolution here left ``active_rec`` None, so the archive never fired
        # and re-ingests produced overlapping, never-closed versions.
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

        # NEW_VERSION needs a temporal-validity window to archive the prior row
        # (close its upper bound). Without a ValiditySpec there is no closable
        # bound, so degrade to UPDATE rather than silently appending an
        # un-closeable duplicate — the documented fallback on
        # ``ItemsWritePolicy.validity``.
        if effective_on_conflict == WriteConflictPolicy.NEW_VERSION and (
            write_policy is None or not write_policy.enable_validity
        ):
            effective_on_conflict = WriteConflictPolicy.UPDATE

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
                # Archive the existing version before inserting. The close-point
                # MUST equal the incoming version's validity start so the
                # temporal history stays contiguous (no gap/overlap).
                #
                # The new version's ``valid_from`` is resolved the same way the
                # attributes sidecar resolves it in ``finalize_upsert_payload``:
                #   1. an explicit feature/context ``valid_from`` if present;
                #   2. otherwise the new hub row's ``transaction_time`` (the
                #      authoritative per-item ingestion instant, set once in
                #      ``item_service``) — NOT ``now()``, which is evaluated
                #      per-row at archive time and lands a few ms/s later than
                #      the new row's ``valid_from``, producing overlapping
                #      windows. ``expire_at`` must read that SAME source so the
                #      archived upper bound equals the new lower bound exactly.
                expire_at = (
                    processing_context.get("valid_from")
                    or hub_payload.get("valid_from")
                    or hub_payload.get("transaction_time")
                    or datetime.now(timezone.utc)
                )
                for sidecar in sidecars:
                    # Only archive sidecars that actually carry a validity
                    # column. Calling expire_version on a validity-less sidecar
                    # runs an UPDATE that errors and aborts the whole
                    # transaction — the failed statement poisons it even when
                    # the Python exception is caught, so the next sidecar's
                    # expire then fails with InFailedSQLTransactionError. The
                    # attributes sidecar is the validity SSOT and exposes
                    # ``enable_validity``; other sidecars opt in the same way.
                    if not getattr(
                        getattr(sidecar, "config", None), "enable_validity", False
                    ):
                        continue
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
            cols.append(quote_ident(k))
            vals.append(f":{k}")
            params[k] = v

        sql = f'INSERT INTO {qualify_table(schema, table)} ({", ".join(cols)}) VALUES ({", ".join(vals)}) RETURNING *;'
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
            clauses.append(f'{quote_ident(k)} = :{k}')
            params[k] = v

        sql = f'UPDATE {qualify_table(schema, table)} SET {", ".join(clauses)} WHERE geoid = :geoid RETURNING *;'
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
        self, conn, schema, table, data, conflict_cols: Optional[List[str]] = None,
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
        if conflict_cols is None:
            conflict_cols = ["geoid"]
        if geom_cols is None:
            geom_cols = {"geom", "bbox_geom", "centroid"}
        cols: list = []
        vals: list = []
        updates: list = []
        params = {}
        for k, v in data.items():
            cols.append(quote_ident(k))
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
                updates.append(f'{quote_ident(k)} = EXCLUDED.{quote_ident(k)}')

        conflict_target = ", ".join([quote_ident(c) for c in conflict_cols])
        if updates:
            on_conflict_clause = f"DO UPDATE SET {', '.join(updates)}"
        else:
            on_conflict_clause = "DO NOTHING"
        sql = f"""
INSERT INTO {qualify_table(schema, table)} ({", ".join(cols)})
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

    # ── BATCH INSERT FAST PATH ─────────────────────────────────────────────

    async def _batch_resolve_by_external_id(
        self,
        conn: DbResource,
        phys_schema: str,
        phys_table: str,
        external_ids: List[str],
        sidecars: List[SidecarProtocol],
        write_policy: Optional[ItemsWritePolicy],
    ) -> Dict[str, Dict[str, Any]]:
        """Resolve external_id → existing row for a batch of ids in one SELECT.

        Mirrors the single-row query in
        ``FeatureAttributeSidecar.resolve_existing_item`` (external_id matcher)
        but replaces the equality predicate with ``= ANY(:ids)`` so one round-
        trip covers the whole chunk.  Returns a dict keyed on external_id
        for O(1) per-plan lookup.

        Returns an empty dict when the attributes sidecar is absent or its
        external_id storage is disabled — callers degrade to per-row.
        """
        if not external_ids:
            return {}

        attrs_sidecar = next(
            (s for s in sidecars if s.sidecar_id == "attributes"), None
        )
        if attrs_sidecar is None:
            return {}

        sc_cfg = getattr(attrs_sidecar, "config", None)
        if sc_cfg is None or not getattr(sc_cfg, "enable_external_id", False):
            return {}

        sc_table = f"{phys_table}_attributes"
        geom_sc_table = f"{phys_table}_geometries"
        enable_validity: bool = bool(getattr(sc_cfg, "enable_validity", False))

        validity_clause = "AND upper(s.validity) IS NULL" if enable_validity else ""
        validity_col = ", s.validity" if enable_validity else ""

        sql = f"""
            SELECT DISTINCT ON (s.external_id)
                s.external_id, h.geoid, g.geometry_hash{validity_col}
            FROM {qualify_table(phys_schema, phys_table)} h,
                 {qualify_table(phys_schema, sc_table)} s,
                 {qualify_table(phys_schema, geom_sc_table)} g
            WHERE s.external_id = ANY(CAST(:ids AS TEXT[]))
              AND h.deleted_at IS NULL
              AND h.geoid = s.geoid
              AND g.geoid = h.geoid
              {validity_clause}
            ORDER BY s.external_id, h.transaction_time DESC;
        """
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, ids=list(external_ids)
        )
        return {row["external_id"]: dict(row) for row in (rows or [])}

    async def _batch_resolve_by_geometry_hash(
        self,
        conn: DbResource,
        phys_schema: str,
        phys_table: str,
        geometry_hashes: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Resolve geometry_hash → existing row for a batch in one SELECT.

        Mirrors ``FeatureAttributeSidecar._resolve_by_geometry_hash`` but
        replaces the equality predicate with ``= ANY(:hashes)`` so the whole
        chunk is resolved in a single round-trip.  Returns a dict keyed on
        geometry_hash for O(1) per-plan lookup.
        """
        if not geometry_hashes:
            return {}

        geom_sc_table = f"{phys_table}_geometries"
        sql = f"""
            SELECT DISTINCT ON (s.geometry_hash)
                s.geometry_hash, h.geoid
            FROM {qualify_table(phys_schema, phys_table)} h
            JOIN {qualify_table(phys_schema, geom_sc_table)} s
              ON s.geoid = h.geoid
            WHERE s.geometry_hash = ANY(CAST(:hashes AS TEXT[]))
              AND h.deleted_at IS NULL
            ORDER BY s.geometry_hash, h.transaction_time DESC;
        """
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, hashes=list(geometry_hashes)
        )
        return {row["geometry_hash"]: dict(row) for row in (rows or [])}

    async def _batch_hub_insert(
        self,
        conn: DbResource,
        schema: str,
        table: str,
        payloads: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Multi-row hub INSERT RETURNING *.

        Column set is the union across all payloads in declaration order;
        missing keys are emitted as NULL so column-level DB defaults apply.
        Range columns (e.g. ``validity``) are wrapped with ``tstzrange()``,
        using the same lower/upper/bounds expansion as ``_upsert_sidecar_table_raw``.

        Returns hub rows mapped back to input order by geoid (pre-generated
        in Phase 2, so no ordering assumption is placed on RETURNING).
        """
        if not payloads:
            return []

        seen: Dict[str, None] = {}
        for p in payloads:
            for k in p:
                seen[k] = None
        all_cols = list(seen.keys())

        params: Dict[str, Any] = {}
        value_tuples: List[str] = []

        for i, payload in enumerate(payloads):
            row_vals: List[str] = []
            for col in all_cols:
                if col not in payload:
                    row_vals.append("NULL")
                    continue
                v = payload[col]
                pk = f"_h_{col}_{i}"
                if (
                    hasattr(v, "lower")
                    and hasattr(v, "upper")
                    and hasattr(v, "lower_inc")
                ):
                    lk = f"_h_{col}_lo_{i}"
                    uk = f"_h_{col}_hi_{i}"
                    lb = "[" if v.lower_inc else "("
                    ub = "]" if v.upper_inc else ")"
                    row_vals.append(f"tstzrange(:{lk}, :{uk}, '{lb}{ub}')")
                    params[lk] = v.lower
                    params[uk] = v.upper
                else:
                    row_vals.append(f":{pk}")
                    params[pk] = v
            value_tuples.append(f"({', '.join(row_vals)})")

        col_list = ", ".join(quote_ident(c) for c in all_cols)
        sql = (
            f'INSERT INTO {qualify_table(schema, table)} ({col_list})\n'
            f"VALUES {', '.join(value_tuples)}\n"
            f"RETURNING *;"
        )
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, **params
        )
        # PG INSERT…RETURNING never silently drops rows that were inserted.
        # An empty result is an invariant violation, not a recoverable path.
        # The [{}]*N fallback that was here before would produce empty dicts
        # that survive the `if row is None` guard in the caller and then
        # crash with KeyError on `row["geoid"]` in result_geoids.
        if not rows:
            raise RuntimeError(
                f"batch hub insert returned no rows for {len(payloads)} payload(s) "
                f"into \"{schema}\".\"{table}\" — RETURNING clause produced empty result"
            )

        # asyncpg returns UUID columns as uuid.UUID objects; payload geoids are
        # pre-generated strings.  Normalize to str on both sides so the dict
        # lookup never silently misses every key.
        by_geoid: Dict[str, Dict[str, Any]] = {
            str(row["geoid"]): dict(row) for row in rows
        }
        return [
            by_geoid.get(
                str(p["geoid"]) if p.get("geoid") is not None else "",
                {},
            )
            for p in payloads
        ]

    async def _batch_upsert_sidecar_rows(
        self,
        conn: DbResource,
        schema: str,
        table: str,
        payloads: List[Dict[str, Any]],
        conflict_cols: Optional[List[str]] = None,
        geom_cols: Optional[Set[str]] = None,
    ) -> None:
        """Multi-row sidecar INSERT … ON CONFLICT for a list of finalized payloads.

        Mirrors ``_upsert_sidecar_table_raw`` for a single row but collapses
        N finalized payloads into one VALUES clause.  Geometry columns receive
        the same ``ST_GeomFromEWKB(decode(:k, 'hex'))`` expression; range
        columns (e.g. ``validity``) use the same ``tstzrange()`` expansion.
        Missing keys in any payload are emitted as NULL so optional columns
        don't cause column-count errors between rows.

        Bind-parameter names are suffixed with ``_s_<col>_<row>`` to avoid
        collisions across rows.
        """
        if not payloads:
            return
        if conflict_cols is None:
            conflict_cols = ["geoid"]
        if geom_cols is None:
            geom_cols = {"geom", "bbox_geom", "centroid"}

        seen: Dict[str, None] = {}
        for p in payloads:
            for k in p:
                seen[k] = None
        all_cols = list(seen.keys())

        updates = [
            f'{quote_ident(c)} = EXCLUDED.{quote_ident(c)}'
            for c in all_cols if c not in conflict_cols
        ]
        params: Dict[str, Any] = {}
        value_tuples: List[str] = []

        for i, payload in enumerate(payloads):
            row_vals: List[str] = []
            for col in all_cols:
                if col not in payload:
                    row_vals.append("NULL")
                    continue
                v = payload[col]
                pk = f"_s_{col}_{i}"
                if col in geom_cols and isinstance(v, str):
                    row_vals.append(f"ST_GeomFromEWKB(decode(:{pk}, 'hex'))")
                    params[pk] = v
                elif (
                    hasattr(v, "lower")
                    and hasattr(v, "upper")
                    and hasattr(v, "lower_inc")
                ):
                    lk = f"_s_{col}_lo_{i}"
                    uk = f"_s_{col}_hi_{i}"
                    lb = "[" if v.lower_inc else "("
                    ub = "]" if v.upper_inc else ")"
                    row_vals.append(f"tstzrange(:{lk}, :{uk}, '{lb}{ub}')")
                    params[lk] = v.lower
                    params[uk] = v.upper
                else:
                    row_vals.append(f":{pk}")
                    params[pk] = v
            value_tuples.append(f"({', '.join(row_vals)})")

        col_list = ", ".join(quote_ident(c) for c in all_cols)
        conflict_target = ", ".join(quote_ident(c) for c in conflict_cols)
        on_conflict = (
            f"DO UPDATE SET {', '.join(updates)}" if updates else "DO NOTHING"
        )
        sql = (
            f'INSERT INTO {qualify_table(schema, table)} ({col_list})\n'
            f"VALUES {', '.join(value_tuples)}\n"
            f"ON CONFLICT ({conflict_target}) {on_conflict};"
        )
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, **params
        )

    async def batch_insert_or_update_distributed(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        plans: List[Dict[str, Any]],
        col_config: ItemsPostgresqlDriverConfig,
        sidecars: List[SidecarProtocol],
        write_policy: Optional[ItemsWritePolicy],
    ) -> Tuple[List[Optional[Dict[str, Any]]], List[Dict[str, Any]]]:
        """Batched multi-row write for a chunk of prepared plans.

        Replaces per-row ``insert_or_update_distributed`` calls for the
        INSERT-dominant bulk-load case.  Round-trip reduction per chunk with
        default sidecars (attributes + geometries + item_metadata):

          Per-row path: N × (1 identity SELECT + 1 hub INSERT + 3 sidecar
          INSERTs) ≈ 5N round-trips.

          Batch path: 2 identity SELECTs (external_id ANY + geometry_hash ANY)
          + 1 multi-row hub INSERT + 3 multi-row sidecar INSERTs + per-row
          fallback for the UPDATE partition ≈ 5 + U×5 round-trips (where U is
          the number of matching/update rows — typically 0 in a fresh load).

        Identity-resolution batching covers ``external_id`` and
        ``geometry_hash`` matchers.  Rules whose ``match_on`` includes
        ``geohash`` or ``attributes_hash`` fall back to per-row identity
        resolution (those matchers require per-feature SQL expressions that
        cannot collapse into a single ANY lookup).

        The UPDATE partition always uses the existing per-row
        ``insert_or_update_distributed`` path — updates are rare in a fresh
        bulk load and the archive step (NEW_VERSION) makes batching unsafe.

        Returns:
            ``(per_plan_results, rejections)`` — ``per_plan_results`` is
            parallel to ``plans``: ``None`` for rejected plans, a hub-row dict
            for accepted ones (same shape as the per-row path).

        Raises:
            ConflictError: for REFUSE_FAIL / refuse_batch policies (same as the
                per-row path — must abort the whole chunk transaction).
        """
        phys_schema = await self._resolve_physical_schema(
            catalog_id, db_resource=conn
        )
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            phys_table = collection_id

        results: List[Optional[Dict[str, Any]]] = [None] * len(plans)
        rejections: List[Dict[str, Any]] = []

        # ── 1. Sidecar acceptance check ──────────────────────────────────
        accepted_idx: List[int] = []
        for i, plan in enumerate(plans):
            rejected = False
            for sidecar in sidecars:
                if not sidecar.is_acceptable(plan["hub_payload"], plan["item_context"]):
                    external_id = plan["item_context"].get("external_id")
                    logger.warning(
                        "Feature rejected by sidecar %s (external_id=%s)",
                        sidecar.sidecar_id, external_id,
                    )
                    rejections.append({
                        "geoid": str(plan["geoid"]),
                        "external_id": external_id,
                        "sidecar_id": sidecar.sidecar_id,
                        "matcher": None,
                        "reason": "sidecar_not_acceptable",
                        "message": (
                            f"Sidecar '{sidecar.sidecar_id}' refused the feature"
                        ),
                        "record": plan["item_context"].get("_raw_item"),
                    })
                    rejected = True
                    break
            if not rejected:
                accepted_idx.append(i)

        if not accepted_idx:
            return results, rejections

        # ── 2. Identity resolution ────────────────────────────────────────
        # "Batchable" means every match_on field resolves via a single
        # ANY(:ids) lookup (external_id, geometry_hash).  Geohash requires
        # per-feature ST_GeoHash(wkb) evaluation; attributes_hash requires
        # per-feature JSONB hashing — both fall back to per-row to preserve
        # exact match semantics.
        _BATCHABLE: Set[str] = {"external_id", "geometry_hash"}

        rules = (
            write_policy.resolved_identity()
            if write_policy
            else [
                ResolvedIdentityRule(
                    match_on=[ComputedField(kind=ComputedKind.EXTERNAL_ID)]
                )
            ]
        )

        all_rules_batchable = all(
            all(str(cf.kind) in _BATCHABLE for cf in rule.match_on)
            for rule in rules
        )

        active_recs: Dict[int, Optional[Dict[str, Any]]] = {}
        matched_rules_map: Dict[int, Optional[ResolvedIdentityRule]] = {}

        if all_rules_batchable:
            ext_ids: List[Optional[str]] = [
                plans[i]["item_context"].get("external_id") for i in accepted_idx
            ]
            geom_hashes: List[Optional[str]] = [
                plans[i]["item_context"].get("geometry_hash") for i in accepted_idx
            ]

            non_null_ext_ids = [e for e in ext_ids if e]
            non_null_geom_hashes = [h for h in geom_hashes if h]

            ext_id_map: Dict[str, Dict[str, Any]] = {}
            geom_hash_map: Dict[str, Dict[str, Any]] = {}

            if non_null_ext_ids:
                ext_id_map = await self._batch_resolve_by_external_id(
                    conn, phys_schema, phys_table,
                    non_null_ext_ids, sidecars, write_policy,
                )
            if non_null_geom_hashes:
                geom_hash_map = await self._batch_resolve_by_geometry_hash(
                    conn, phys_schema, phys_table, non_null_geom_hashes,
                )

            for i in accepted_idx:
                ctx = plans[i]["item_context"]
                active_rec: Optional[Dict[str, Any]] = None
                matched_rule: Optional[ResolvedIdentityRule] = None

                for rule in rules:
                    geoid_sets: List[Set[Any]] = []
                    row_by_geoid: Dict[Any, Dict[str, Any]] = {}
                    rule_ok = True

                    for cf in rule.match_on:
                        matcher_str = str(cf.kind)
                        if matcher_str == "external_id":
                            ext_id = ctx.get("external_id")
                            if ext_id and ext_id in ext_id_map:
                                row = ext_id_map[ext_id]
                                g = row.get("geoid")
                                geoid_sets.append({g})
                                row_by_geoid[g] = row
                            else:
                                rule_ok = False
                                break
                        elif matcher_str == "geometry_hash":
                            gh = ctx.get("geometry_hash")
                            if gh and gh in geom_hash_map:
                                row = geom_hash_map[gh]
                                g = row.get("geoid")
                                geoid_sets.append({g})
                                row_by_geoid[g] = row
                            else:
                                rule_ok = False
                                break

                    if not rule_ok or not geoid_sets:
                        continue

                    # AND semantics: intersect geoid sets across match_on fields.
                    common: Set[Any] = geoid_sets[0]
                    for nxt in geoid_sets[1:]:
                        common = common & nxt
                    if not common:
                        continue

                    for g in geoid_sets[0]:
                        if g in common:
                            active_rec = row_by_geoid[g]
                            matched_rule = rule
                            break

                    if active_rec:
                        kinds = ",".join(str(cf.kind) for cf in rule.match_on)
                        logger.info(
                            "BATCH UPSERT: found active record geoid=%s (rule=[%s])",
                            active_rec.get("geoid"), kinds,
                        )
                        break

                active_recs[i] = active_rec
                matched_rules_map[i] = matched_rule
        else:
            # Non-batchable matchers: per-row identity resolution only.
            for i in accepted_idx:
                active_rec = None
                matched_rule = None
                for rule in rules:
                    rec = await _resolve_rule(
                        rule, conn, phys_schema, phys_table,
                        plans[i]["item_context"], sidecars,
                    )
                    if rec:
                        active_rec = rec
                        matched_rule = rule
                        break
                active_recs[i] = active_rec
                matched_rules_map[i] = matched_rule

        # ── 3. Classify each accepted plan ───────────────────────────────
        insert_idx: List[int] = []
        update_idx: List[int] = []

        for i in accepted_idx:
            plan = plans[i]
            active_rec = active_recs[i]
            matched_rule = matched_rules_map[i]
            effective_on_conflict = _select_effective_on_conflict(
                write_policy, matched_rule
            )

            if effective_on_conflict == WriteConflictPolicy.NEW_VERSION and (
                write_policy is None or not write_policy.enable_validity
            ):
                effective_on_conflict = WriteConflictPolicy.UPDATE

            if active_rec and write_policy and write_policy.on_batch_conflict is not None:
                from dynastore.modules.storage.driver_config import BatchConflictPolicy
                if write_policy.on_batch_conflict == BatchConflictPolicy.REFUSE:
                    rule_name = (
                        ",".join(str(cf.kind) for cf in matched_rule.match_on)
                        if matched_rule else "unknown"
                    )
                    raise ConflictError(
                        f"Write refused: duplicate detected via [{rule_name}] "
                        f"(geoid={active_rec.get('geoid')}); policy=refuse_batch",
                        geoid=active_rec.get("geoid"),
                        matcher=rule_name,
                    )

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

            if active_rec and effective_on_conflict == WriteConflictPolicy.REFUSE_RETURN:
                logger.info(
                    "BATCH UPSERT: REFUSE_RETURN — keeping existing record geoid=%s",
                    active_rec.get("geoid"),
                )
                results[i] = {"geoid": active_rec["geoid"], "_refuse_return": True}
                continue

            if active_rec and effective_on_conflict == WriteConflictPolicy.REFUSE:
                logger.info(
                    "BATCH UPSERT: identity matched and REFUSE set. Skipping."
                )
                rejections.append({
                    "geoid": str(active_rec["geoid"]),
                    "external_id": plan["item_context"].get("external_id"),
                    "sidecar_id": None,
                    "matcher": (
                        ",".join(str(cf.kind) for cf in matched_rule.match_on)
                        if matched_rule else None
                    ),
                    "reason": "write_policy_refuse",
                    "message": "Feature refused by write policy",
                    "record": plan["item_context"].get("_raw_item"),
                })
                continue

            if not active_rec or effective_on_conflict == WriteConflictPolicy.NEW_VERSION:
                if active_rec and effective_on_conflict == WriteConflictPolicy.NEW_VERSION:
                    # Archive step required — fall back to per-row.
                    update_idx.append(i)
                else:
                    insert_idx.append(i)
            else:
                update_idx.append(i)

        # ── 4. Batched hub INSERT for the INSERT partition ────────────────
        if insert_idx:
            hub_payloads = [plans[i]["hub_payload"] for i in insert_idx]
            hub_rows = await self._batch_hub_insert(
                conn, phys_schema, phys_table, hub_payloads
            )

            for list_pos, plan_i in enumerate(insert_idx):
                hub_row = hub_rows[list_pos]
                # Defense-in-depth: _batch_hub_insert raises RuntimeError on
                # empty RETURNING, so a missing geoid here should not occur in
                # practice.  Guard anyway so a bug surfaces as a clear error
                # rather than a silent empty-dict propagating to result_geoids.
                if not hub_row or "geoid" not in hub_row:
                    raise RuntimeError(
                        f"batch hub insert returned a row without 'geoid' for "
                        f"plan index {plan_i} (geoid={plans[plan_i]['geoid']})"
                    )
                results[plan_i] = hub_row

            # ── 5. Batched sidecar INSERT per sidecar ────────────────────
            for sidecar in sidecars:
                sc_id = sidecar.sidecar_id
                sc_table = f"{phys_table}_{sc_id}"

                conflict_cols = sidecar.get_identity_columns()
                if col_config.partitioning and col_config.partitioning.enabled:
                    for key in col_config.partitioning.partition_keys:
                        if key not in conflict_cols:
                            conflict_cols.insert(0, key)

                _gvc = getattr(sidecar, "geometry_value_columns", None)
                geom_cols_sc = (
                    cast("Optional[Set[str]]", _gvc()) if callable(_gvc) else None
                )

                sc_finalized: List[Dict[str, Any]] = []
                for list_pos, plan_i in enumerate(insert_idx):
                    plan = plans[plan_i]
                    hub_row = hub_rows[list_pos]
                    sc_data_map = plan["sidecar_payloads"]

                    if sc_id not in sc_data_map and not sidecar.is_mandatory():
                        continue

                    sc_payload = dict(sc_data_map.get(sc_id, {}))
                    if "geoid" not in sc_payload:
                        sc_payload["geoid"] = hub_row.get(
                            "geoid", plan["hub_payload"]["geoid"]
                        )

                    full_payload = sidecar.finalize_upsert_payload(
                        sc_payload, hub_row, plan["item_context"]
                    )
                    full_payload = self._strip_undeclared_columns(
                        sidecar, full_payload, col_config
                    )
                    sc_finalized.append(full_payload)

                if sc_finalized:
                    await self._batch_upsert_sidecar_rows(
                        conn, phys_schema, sc_table, sc_finalized,
                        conflict_cols=conflict_cols,
                        geom_cols=geom_cols_sc,
                    )

                # JSON-FG Place Statistics — optional per sidecar.
                _prep_place = getattr(sidecar, "prepare_place_upsert_payload", None)
                if _prep_place is not None:
                    place_payloads: List[Dict[str, Any]] = []
                    for list_pos, plan_i in enumerate(insert_idx):
                        plan = plans[plan_i]
                        hub_row = hub_rows[list_pos]
                        geoid_val = hub_row.get("geoid", plan["hub_payload"]["geoid"])
                        # Mirror the per-row path (_execute_distributed_insert):
                        # the `continue` inside the sidecar loop exits the WHOLE
                        # sidecar block (main INSERT + place stats) for that plan.
                        # A non-mandatory sidecar with no payload data must
                        # therefore produce no place row for the feature either.
                        sc_data_map = plan["sidecar_payloads"]
                        if sc_id not in sc_data_map and not sidecar.is_mandatory():
                            continue
                        try:
                            pp = _prep_place(
                                plan["item_context"].get("_raw_item", {}),
                                plan["item_context"],
                            )
                            if pp:
                                if "geoid" not in pp:
                                    pp["geoid"] = geoid_val
                                place_payloads.append(pp)
                        except Exception as exc:
                            logger.warning(
                                "Place stats prep skipped for geoid %s: %s",
                                geoid_val, exc,
                            )
                    if place_payloads:
                        try:
                            await self._batch_upsert_sidecar_rows(
                                conn, phys_schema, f"{phys_table}_place",
                                place_payloads,
                                conflict_cols=["geoid"],
                                geom_cols=geom_cols_sc,
                            )
                        except Exception as exc:
                            logger.warning(
                                "Place stats batch upsert skipped: %s", exc
                            )

        # ── 6. UPDATE partition — per-row via existing path ──────────────
        # Updates are rare in bulk-load.  NEW_VERSION's archive step
        # (expire_version) and validate_update checks make batching complex
        # and the correctness risk outweighs the gain for a small partition.
        for i in update_idx:
            plan = plans[i]
            row = await self.insert_or_update_distributed(
                conn,
                catalog_id,
                collection_id,
                plan["hub_payload"],
                plan["sidecar_payloads"],
                col_config=col_config,
                sidecars=sidecars,
                processing_context=plan["item_context"],
            )
            if row is not None:
                results[i] = row

        return results, rejections
