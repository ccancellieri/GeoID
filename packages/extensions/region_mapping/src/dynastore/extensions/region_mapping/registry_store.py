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

"""Write orchestration + cached reads over ``region_mapping.mappings``
(dynastore#2821).

Thin service layer between the router (``region_mapping_service.py``) and
the raw SQL in ``registry_queries.py``: resolves the DB engine via
``get_engine()`` (mirrors ``dynastore.modules.local.local_upload``), owns
the ``@cached`` read wrappers used by the hot serving paths
(``/region.json``, ``/{mapping_id}/regionIds``), and their invalidation on
write.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from dynastore.models.localization import LocalizedText
from dynastore.modules.db_config.exceptions import UniqueViolationError
from dynastore.modules.db_config.query_executor import DbResource, managed_transaction
from dynastore.tools.cache import cache_clear, cached
from dynastore.tools.protocol_helpers import get_engine

from . import registry_queries as _q
from .claims import (
    ROLE_PRIMARY,
    compute_claim_set,
    fetch_collection_bbox,
    fetch_distinct_region_ids,
    mapping_id_for,
)

logger = logging.getLogger(__name__)

# The registry is bounded by (registered collections x aliases per
# collection) -- not by regionIds cardinality -- so a single generous fetch
# followed by client-side grouping/pagination is simpler and cheap for
# /region-mappings/region.json.
DEFINITIONS_FETCH_CAP = 5000


class MappingNotFoundError(LookupError):
    """Raised by :func:`delete_mapping` when ``mapping_id`` has no claims."""


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


async def apply_mapping(
    engine: DbResource,
    *,
    catalog_id: str,
    collection_id: str,
    column: str,
    alias: str,
    extra_aliases: Sequence[str],
    title: Optional[Union[str, Dict[str, str]]],
    lang: str = "en",
    layer_name: str = "default",
    server_type: str = "MVT",
    server_subdomains: Optional[Sequence[str]] = None,
    server_min_zoom: int = 0,
    server_max_native_zoom: int = 12,
    server_max_zoom: int = 28,
    unique_id_prop: Optional[str] = None,
    digits: int = 255,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Register (or re-apply) one mapping's claim set.

    Transactional: deletes claims stale to this ``mapping_id`` (a changed
    alias/column set must not leave old rows squatting the PK forever), then
    updates-or-inserts every claim in the freshly computed set. A
    cross-mapping ``claim_ci`` collision surfaces PG's real ``23505`` from
    :data:`registry_queries.INSERT_CLAIM` -- never caught here, propagated to
    the global exception-handler chain (-> HTTP 409). Two racing first-applies
    of the *same* mapping (both see ``UPDATE_OWN_CLAIM`` touch 0 rows) resolve
    without a spurious conflict -- see :func:`_insert_claim_idempotent`.

    ``title`` (TerriaJS's ``description``) accepts a plain string -- wrapped
    under ``lang`` via :meth:`LocalizedText.delocalize_input`, the same
    single-language-input convention as the rest of the platform -- or an
    already language-keyed dict, stored as-is. The remaining keyword
    arguments are the rest of the TerriaJS ``regionWmsMap`` entry
    (dynastore#443) that used to be hardcoded in ``definitions.json.j2``;
    every claim row of a mapping carries the same values, exactly like the
    pre-existing ``title``/``alias``/``region_prop`` duplication.

    Returns ``(mapping_id, claim_rows)``.
    """
    mapping_id = mapping_id_for(catalog_id, collection_id)
    row_title = LocalizedText.delocalize_input(title, lang) if title else {lang: collection_id}
    row_subdomains = list(server_subdomains) if server_subdomains else []

    claims = compute_claim_set(
        catalog_id=catalog_id, collection_id=collection_id,
        column=column, alias=alias, extra_aliases=extra_aliases,
    )

    claim_rows: List[Dict[str, Any]] = []
    async with managed_transaction(engine) as conn:
        await _q.DELETE_STALE_CLAIMS.execute(
            conn, mapping_id=mapping_id, keep_claim_ci=list(claims.keys()),
        )
        for claim_ci, (claim, role) in claims.items():
            params = dict(
                claim_ci=claim_ci, claim=claim, mapping_id=mapping_id, role=role,
                src_catalog=catalog_id, src_collection=collection_id,
                region_prop=column, alias=alias, title=json.dumps(row_title),
                layer_name=layer_name, server_type=server_type,
                server_subdomains=json.dumps(row_subdomains),
                server_min_zoom=server_min_zoom,
                server_max_native_zoom=server_max_native_zoom,
                server_max_zoom=server_max_zoom,
                unique_id_prop=unique_id_prop, digits=digits,
            )
            row = await _q.UPDATE_OWN_CLAIM.execute(conn, **params)
            if row is None:
                # Brand-new claim_ci, or one owned by a *different* mapping.
                row = await _insert_claim_idempotent(conn, mapping_id, claim_ci, params)
            claim_rows.append(row)

    invalidate_serving_caches()
    return mapping_id, claim_rows


async def _insert_claim_idempotent(
    conn: Any, mapping_id: str, claim_ci: str, params: Dict[str, Any],
) -> Dict[str, Any]:
    """INSERT one claim, absorbing the concurrent-first-apply race.

    Two racing first-applies of the *same* mapping both see
    ``UPDATE_OWN_CLAIM`` touch 0 rows (neither's row is committed yet) and
    both fall through to here; the loser hits PG's real ``23505`` on
    ``claim_ci`` even though the winner's row belongs to the identical
    mapping -- an idempotent duplicate, not a conflict.

    The INSERT runs inside a SAVEPOINT: asyncpg aborts the whole surrounding
    transaction on an uncaught error, so a caught ``23505`` must roll back
    only this insert attempt, leaving the outer transaction usable for the
    re-check that follows. If the surviving row belongs to ``mapping_id``,
    return it as success. Otherwise it's a genuine cross-mapping collision --
    re-raise so it propagates to the global exception-handler chain (->
    HTTP 409), unchanged from before.
    """
    try:
        async with conn.begin_nested():
            return await _q.INSERT_CLAIM.execute(conn, **params)
    except UniqueViolationError:
        existing = await _q.SELECT_CLAIM_BY_CI.execute(conn, claim_ci=claim_ci)
        if existing is not None and existing["mapping_id"] == mapping_id:
            return existing
        raise


async def delete_mapping(engine: DbResource, mapping_id: str) -> int:
    """Delete every claim sharing ``mapping_id``.

    Returns the number of deleted claim rows; raises
    :class:`MappingNotFoundError` when none existed.
    """
    async with managed_transaction(engine) as conn:
        deleted = await _q.DELETE_CLAIMS_BY_MAPPING_ID.execute(conn, mapping_id=mapping_id)
    if not deleted:
        raise MappingNotFoundError(mapping_id)
    invalidate_serving_caches()
    return len(deleted)


async def delete_claims_by_source_collection(
    engine: DbResource, catalog_id: str, collection_id: str,
) -> int:
    """Referential-integrity cleanup: delete every claim sourced from one
    now-deleted collection. Unlike :func:`delete_mapping`, a no-op (nothing
    was ever registered against that collection) is not an error -- called
    from the best-effort event listener in ``lifecycle.py``.
    """
    async with managed_transaction(engine) as conn:
        deleted = await _q.DELETE_CLAIMS_BY_SOURCE_COLLECTION.execute(
            conn, catalog_id=catalog_id, collection_id=collection_id,
        )
    if deleted:
        invalidate_serving_caches()
    return len(deleted)


async def delete_claims_by_source_catalog(engine: DbResource, catalog_id: str) -> int:
    """Same as :func:`delete_claims_by_source_collection`, scoped to every
    claim sourced from an entire now-deleted catalog."""
    async with managed_transaction(engine) as conn:
        deleted = await _q.DELETE_CLAIMS_BY_SOURCE_CATALOG.execute(conn, catalog_id=catalog_id)
    if deleted:
        invalidate_serving_caches()
    return len(deleted)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------


async def list_claims(
    *,
    mapping_id: Optional[str] = None,
    role: Optional[str] = None,
    src_catalog: Optional[str] = None,
    src_collection: Optional[str] = None,
    claim_ci: Optional[str] = None,
    cql_where: str = "",
    cql_params: Optional[Dict[str, Any]] = None,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Uncached claim listing -- backs ``GET /region-mappings`` and the
    CQL2-filtered branch of ``GET /region-mappings/region.json``.

    Arbitrary filter combinations (including a caller-supplied CQL2 clause)
    are not worth caching, so this always reads through to PG.
    """
    engine = get_engine()
    if engine is None:
        return []
    return await _q.list_claims(
        engine,
        mapping_id=mapping_id, role=role,
        src_catalog=src_catalog, src_collection=src_collection,
        claim_ci=claim_ci,
        cql_where=cql_where, cql_params=cql_params,
        order_by="mapping_id, claim", limit=limit, offset=offset,
    )


@cached(maxsize=256, ttl=300, namespace="region_mapping_primary_records")
async def fetch_primary_records(
    catalog: Optional[str], collection: Optional[str], alias_ci: Optional[str],
) -> List[Dict[str, Any]]:
    """Fetch the primary-role (or, when ``alias_ci`` is given, the exact
    claim) records used to build ``/region-mappings/region.json``.

    Bounded by :data:`DEFINITIONS_FETCH_CAP`. Only the unfiltered (no CQL2
    ``filter=``) request path is cached -- see :func:`list_claims` for the
    CQL2 branch.
    """
    engine = get_engine()
    if engine is None:
        return []
    if alias_ci:
        return await _q.list_claims(
            engine, claim_ci=alias_ci, src_catalog=catalog, src_collection=collection,
            order_by="mapping_id", limit=DEFINITIONS_FETCH_CAP,
        )
    return await _q.list_claims(
        engine, role=ROLE_PRIMARY, src_catalog=catalog, src_collection=collection,
        order_by="mapping_id", limit=DEFINITIONS_FETCH_CAP,
    )


@cached(maxsize=256, ttl=300, namespace="region_mapping_claims_for_mapping")
async def fetch_claims_for_mapping(mapping_id: str) -> List[Dict[str, Any]]:
    """All claim records (any role) sharing ``mapping_id`` -- used to build
    the ``aliases`` array of one definitions entry."""
    engine = get_engine()
    if engine is None:
        return []
    return await _q.SELECT_CLAIMS_BY_MAPPING_ID.execute(engine, mapping_id=mapping_id)


@cached(maxsize=256, ttl=300, namespace="region_mapping_mapping_primary")
async def fetch_mapping_primary(mapping_id: str) -> Optional[Dict[str, Any]]:
    """The single primary-role record for ``mapping_id`` -- used by
    ``/region-mappings/{mapping_id}/regionIds`` to resolve ``src_catalog`` /
    ``src_collection`` / ``region_prop``."""
    engine = get_engine()
    if engine is None:
        return None
    return await _q.SELECT_PRIMARY_BY_MAPPING_ID.execute(engine, mapping_id=mapping_id)


def invalidate_serving_caches() -> None:
    """Clear every ``@cached`` region-mapping read used by the extension router.

    Called after apply/delete so newly registered (or removed) claims are
    visible on the next request without waiting out the cache TTL.
    """
    cache_clear(fetch_primary_records)
    cache_clear(fetch_claims_for_mapping)
    cache_clear(fetch_mapping_primary)
    cache_clear(fetch_collection_bbox)
    cache_clear(fetch_distinct_region_ids)
