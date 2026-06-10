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

"""ItemsBigQueryDriver — Phase 4a READ + STREAMING implementation."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, ClassVar, Dict, FrozenSet, Iterable, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols import (
    BigQueryProtocol,
    CloudIdentityProtocol,
    ConfigsProtocol,
)
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.models.protocols.typed_driver import TypedDriver
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.drivers.bigquery_models import (
    BigQueryCredentials,
    ItemsBigQueryDriverConfig,
)
from dynastore.modules.storage.drivers.bigquery_stream import (
    paged_feature_stream,
)
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def _get_bq_service() -> Optional[BigQueryProtocol]:
    return get_protocol(BigQueryProtocol)


class ItemsBigQueryDriver(TypedDriver[ItemsBigQueryDriverConfig]):
    """BigQuery driver — READ + STREAMING (Phase 4a) + reporter-mode WRITE (Phase 3).

    WRITE is gated by ``ItemsBigQueryDriverConfig.reporter_mode``:

    - ``off`` (default): ``write_entities`` returns ``[]`` without touching
      BigQuery.  The driver can still be listed in a routing config's
      ``Operation.WRITE`` list — useful for configs that pre-wire BigQuery
      but haven't turned on reporting yet.
    - ``flat``: one BQ row per feature ({entity_id, ingested_at, …}).
    - ``batch_summary``: one BQ row per write_entities call
      ({collection_id, row_count, first_seen, last_seen}).

    WRITE failures are surfaced as warnings (BQ's partial-failure dict) and
    do NOT raise, so this driver is safe to pin as ``on_failure=warn`` in
    a multi-driver WRITE fan-out (see role-based driver plan §Routing).
    """

    # Explicit-pin only.  Operators who want BQ in items routing set
    # the entry by hand (``source: "operator"``).
    auto_register_for_routing: ClassVar[FrozenSet[str]] = frozenset()

    # Structural backend identity (was ``Hint.BIGQUERY`` on
    # ``supported_hints``).  ``Hint`` is the per-request preference
    # axis; a driver's self-tag is a structural fact and belongs on a
    # dedicated ClassVar, not in the request-side vocabulary.
    # Consumers that need to recognise the BQ driver class can either
    # ``isinstance`` against ``ItemsBigQueryDriver`` or compare
    # ``backend_id == "bigquery"``.
    backend_id: ClassVar[str] = "bigquery"

    priority: int = 50
    preferred_chunk_size: int = 500

    capabilities: FrozenSet[str] = frozenset({
        "READ", "WRITE", "STREAMING", "INTROSPECTION",
    })
    preferred_for: FrozenSet[Hint] = frozenset({Hint.FEATURES})
    supported_hints: FrozenSet[Hint] = frozenset({
        Hint.FEATURES,
        Hint.JOIN,  # BQ can serve as either side of an OGC API - Joins request
        Hint.COUNT, Hint.AGGREGATION,
    })

    def is_available(self) -> bool:
        return _get_bq_service() is not None

    async def lifespan(self, app_state: Any) -> None:
        return None

    async def get_driver_config(
        self, catalog_id: str, collection_id: Optional[str] = None,
        *, db_resource: Optional[Any] = None,
    ) -> Optional[ItemsBigQueryDriverConfig]:
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        return await configs.get_config(
            ItemsBigQueryDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]], List[Feature]],
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        """Reporter-mode streaming write into BigQuery.

        Gated by ``ItemsBigQueryDriverConfig.reporter_mode``:

        - ``off``           — returns ``[]`` without touching BQ.
        - ``flat``          — one row per feature, shaped by
                              ``_rows_flat`` (honours ``include_payload``
                              and ``exclude_fields``).
        - ``batch_summary`` — one row per call, shaped by ``_rows_batch_summary``.

        BQ partial failures are swallowed with a warning log; the method
        returns the canonicalised input feature list so the routing layer
        can still include this driver in a WRITE fan-out without blocking
        downstream on BQ quota hiccups.  See role-based driver plan
        §Routing — ``on_failure=warn`` is the recommended failure policy
        for this driver.
        """
        cfg = await self.get_driver_config(catalog_id, collection_id)
        features = _canonicalise_features(entities)
        if cfg is None or cfg.reporter_mode == "off":
            return features
        if not features:
            return features

        report_target = cfg.report_target or cfg.target
        if not report_target.is_fully_qualified():
            logger.warning(
                "BQ reporter mode %r configured for %s/%s but report_target "
                "is not fully qualified; skipping write.",
                cfg.reporter_mode, catalog_id, collection_id,
            )
            return features

        service = _get_bq_service()
        if service is None:
            logger.warning(
                "BQ reporter WRITE attempted but BigQueryService not "
                "registered; skipping (%s/%s).",
                catalog_id, collection_id,
            )
            return features

        rows = self._shape_rows(cfg, catalog_id, collection_id, features)
        if not rows:
            return features

        project_id = report_target.project_id
        assert project_id is not None  # is_fully_qualified() guarantee

        try:
            errors = await service.insert_rows_json(
                report_target.fqn(),
                rows,
                project_id=project_id,
                credentials=cfg.credentials,
            )
        except Exception as exc:  # noqa: BLE001 — best-effort reporter sink
            logger.warning(
                "BQ reporter WRITE failed for %s/%s (mode=%s): %s",
                catalog_id, collection_id, cfg.reporter_mode, exc,
            )
            return features

        if errors:
            # Partial failure — BQ returns per-row error dicts with
            # indices pointing into the submitted rows list.
            logger.warning(
                "BQ reporter WRITE partial failure for %s/%s (mode=%s): "
                "%d/%d rows rejected",
                catalog_id, collection_id, cfg.reporter_mode,
                len(errors), len(rows),
            )

        return features

    def _shape_rows(
        self,
        cfg: ItemsBigQueryDriverConfig,
        catalog_id: str,
        collection_id: str,
        features: List[Feature],
    ) -> List[Dict[str, Any]]:
        """Dispatch to the row shaper for the active reporter mode."""
        if cfg.reporter_mode == "flat":
            return _rows_flat(
                features,
                catalog_id=catalog_id,
                collection_id=collection_id,
                include_payload=cfg.include_payload,
                exclude_fields=cfg.exclude_fields,
            )
        if cfg.reporter_mode == "batch_summary":
            return _rows_batch_summary(
                features,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        # "off" handled by caller; any unknown value is a bug.
        raise ValueError(f"Unknown reporter_mode: {cfg.reporter_mode!r}")

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        # Read-side (output) transform chains are not applied here by design
        # (geoid#1643). The restore_transform_chain is wired only on
        # Elasticsearch read paths; the routing-config validator emits a WARN
        # if output_transformers are declared against a non-ES driver.
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError(
                f"BigQuery driver requires a fully-qualified target "
                f"(project_id, dataset_id, table_name) for "
                f"{catalog_id}/{collection_id}",
            )
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")

        fqn = cfg.target.fqn()
        project_id = cfg.target.project_id
        assert project_id is not None  # guaranteed by is_fully_qualified()
        id_col = (context or {}).get("id_column", "id")
        geom_col = (context or {}).get("geometry_column")
        select_cols = (context or {}).get("select_columns", "*")
        where = (context or {}).get("where_clause")
        base_query = f"SELECT {select_cols} FROM `{fqn}`"
        if where:
            base_query += f" WHERE {where}"

        creds = cfg.credentials

        async def _execute(q: str, pid: str):
            return await service.execute_query(q, pid, credentials=creds)

        async for feat in paged_feature_stream(
            _execute,
            project_id=project_id,
            base_query=base_query,
            id_column=id_col,
            geometry_column=geom_col,
            page_size=cfg.page_size,
            max_items=limit,
        ):
            yield feat

    async def count_entities(
        self, catalog_id: str, collection_id: str,
        *, request: Optional[Any] = None, db_resource: Optional[Any] = None,
    ) -> int:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")
        project_id = cfg.target.project_id
        assert project_id is not None
        rows = await service.execute_query(
            f"SELECT COUNT(*) FROM `{cfg.target.fqn()}`",
            project_id,
            credentials=cfg.credentials,
        )
        return int(next(iter(rows[0].values()))) if rows else 0

    async def aggregate(
        self, catalog_id: str, collection_id: str,
        *, aggregation_type: str, field: Optional[str] = None,
        request: Optional[Any] = None, db_resource: Optional[Any] = None,
    ) -> Any:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")
        service = _get_bq_service()
        if service is None:
            raise RuntimeError("BigQueryService not registered")

        agg = aggregation_type.upper()
        if agg not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation_type: {aggregation_type!r}")
        if field and not _is_safe_identifier(field):
            raise ValueError(f"Unsafe field identifier: {field!r}")
        expr = "*" if agg == "COUNT" and not field else field
        project_id = cfg.target.project_id
        assert project_id is not None
        rows = await service.execute_query(
            f"SELECT {agg}({expr}) FROM `{cfg.target.fqn()}`",
            project_id,
            credentials=cfg.credentials,
        )
        return next(iter(rows[0].values())) if rows else None


    async def introspect_schema(
        self, catalog_id: str, collection_id: str,
        *, db_resource: Optional[Any] = None,
    ) -> List[FieldDefinition]:
        cfg = await self.get_driver_config(catalog_id, collection_id)
        if cfg is None or not cfg.target.is_fully_qualified():
            raise ValueError("BigQuery driver requires a fully-qualified target")

        client = _make_bq_client(
            cfg.target.project_id,
            credentials=cfg.credentials,
        )
        table = client.get_table(cfg.target.fqn())
        return [_bq_field_to_field_definition(f) for f in table.schema]


def _is_safe_identifier(name: str) -> bool:
    import re
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))


def _make_bq_client(
    project_id: Optional[str],
    *,
    credentials: Optional[BigQueryCredentials] = None,
):
    """Build a short-lived BigQuery client.

    Credential resolution order:
    1. ``credentials.service_account_json`` -- Secret-wrapped SA JSON
       revealed inside this function; never logged.
    2. ``credentials.api_key`` -- Secret-wrapped API key (future, External
       Connections). Not wired to bigquery.Client() yet -- logged as a
       warning until BigQuery API-key auth lands upstream.
    3. Fallback to CloudIdentityProtocol (Phase 4a path) when no
       Secret-wrapped credentials are supplied.
    """
    from google.cloud import bigquery

    if credentials is not None and not credentials.is_empty():
        if credentials.service_account_json is not None:
            import json as _json

            from google.oauth2 import service_account
            # SINGLE reveal site -- never logged, never returned outside this function.
            raw = credentials.service_account_json.reveal()
            info = _json.loads(raw)
            sa_creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(project=project_id, credentials=sa_creds)
        if credentials.api_key is not None:
            logger.warning(
                "BigQueryCredentials.api_key supplied but google-cloud-bigquery "
                "does not yet support api-key auth directly. Falling back to "
                "CloudIdentityProtocol.",
            )

    identity = get_protocol(CloudIdentityProtocol)
    creds = identity.get_credentials_object() if identity else None
    return bigquery.Client(project=project_id, credentials=creds)


def _bq_field_to_field_definition(f) -> FieldDefinition:
    # BigQuery native type -> canonical data_type (see ``dynastore.models.field_types``).
    bq_to_canonical = {
        "STRING": "string", "INTEGER": "bigint", "INT64": "bigint",
        "FLOAT": "double", "FLOAT64": "double", "NUMERIC": "numeric",
        "BIGNUMERIC": "numeric", "BOOL": "boolean", "BOOLEAN": "boolean",
        "TIMESTAMP": "timestamp", "DATETIME": "timestamp", "DATE": "date",
        "TIME": "time", "BYTES": "binary", "JSON": "jsonb",
        "GEOGRAPHY": "geometry",
    }
    dtype = bq_to_canonical.get(f.field_type.upper(), "string")
    return FieldDefinition(
        name=f.name,
        data_type=dtype,
        required=(f.mode == "REQUIRED"),
    )


# ---------------------------------------------------------------------------
# Reporter-mode helpers (Phase 3 WRITE path)
# ---------------------------------------------------------------------------


def _canonicalise_features(
    entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]], List[Feature]],
) -> List[Feature]:
    """Normalise the :meth:`CollectionItemsStore.write_entities` input.

    The WRITE contract accepts six input shapes; the driver needs a flat
    ``List[Feature]`` for both the reporter rows and the return value.
    Dict inputs are validated as Features here so the shaper never has
    to branch on payload shape.
    """
    if entities is None:
        return []
    if isinstance(entities, Feature):
        return [entities]
    if isinstance(entities, FeatureCollection):
        return list(entities.features) if entities.features else []
    if isinstance(entities, dict):
        # GeoJSON FeatureCollection or a single Feature dict
        if entities.get("type") == "FeatureCollection":
            fc = FeatureCollection.model_validate(entities)
            return list(fc.features) if fc.features else []
        return [Feature.model_validate(entities)]
    if isinstance(entities, list):
        out: List[Feature] = []
        for item in entities:
            if isinstance(item, Feature):
                out.append(item)
            elif isinstance(item, dict):
                out.append(Feature.model_validate(item))
            else:
                raise TypeError(
                    f"Unsupported list item for write_entities: {type(item).__name__}"
                )
        return out
    raise TypeError(
        f"Unsupported entities type for write_entities: {type(entities).__name__}"
    )


def _feature_primary_id(feat: Feature) -> Optional[str]:
    """Best-effort primary-key extraction from a Feature for BQ row_ids.

    Checks ``.id`` first, then ``properties.external_id``, then
    ``properties.id``.  Returns None when nothing usable is found — BQ
    then assigns its own insertId (best-effort dedup within the 24h
    streaming-buffer window is lost, but the write still succeeds).
    """
    fid = getattr(feat, "id", None)
    if fid:
        return str(fid)
    props = getattr(feat, "properties", None) or {}
    if isinstance(props, dict):
        for key in ("external_id", "id"):
            value = props.get(key)
            if value:
                return str(value)
    return None


def _rows_flat(
    features: Iterable[Feature],
    *,
    catalog_id: str,
    collection_id: str,
    include_payload: bool,
    exclude_fields: List[str],
) -> List[Dict[str, Any]]:
    """One BQ row per feature: PK + optional payload + timestamp.

    Row schema (writer-side contract; target table must match):

    - ``catalog_id``    STRING   tenant / catalog identifier
    - ``collection_id`` STRING   collection within the catalog
    - ``entity_id``     STRING   feature id / external_id, nullable
    - ``payload``       JSON     full feature.properties when
                                 ``include_payload=True`` and PII-safe
    - ``ingested_at``   TIMESTAMP UTC at shape time (not write time —
                                 close enough for reporter usage)
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    exclude = set(exclude_fields or ())
    rows: List[Dict[str, Any]] = []
    for feat in features:
        row: Dict[str, Any] = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "entity_id": _feature_primary_id(feat),
            "ingested_at": ingested_at,
        }
        if include_payload:
            props = dict(getattr(feat, "properties", None) or {})
            for key in exclude:
                props.pop(key, None)
            # Store as a JSON STRING column — works with both JSON and
            # STRING BigQuery column types and avoids a nested-schema
            # contract this early in the reporter lifecycle.
            import json
            row["payload"] = json.dumps(props, default=str)
        rows.append(row)
    return rows


def _rows_batch_summary(
    features: Iterable[Feature],
    *,
    catalog_id: str,
    collection_id: str,
) -> List[Dict[str, Any]]:
    """One BQ row per ``write_entities`` call: counts + timestamps.

    Row schema:

    - ``catalog_id``    STRING
    - ``collection_id`` STRING
    - ``row_count``     INTEGER  number of features in the batch
    - ``first_entity``  STRING   smallest PK seen, nullable
    - ``last_entity``   STRING   largest PK seen, nullable
    - ``ingested_at``   TIMESTAMP UTC at shape time
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    feature_list = list(features)
    ids = [_feature_primary_id(f) for f in feature_list]
    known_ids = sorted(i for i in ids if i)
    return [{
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "row_count": len(feature_list),
        "first_entity": known_ids[0] if known_ids else None,
        "last_entity": known_ids[-1] if known_ids else None,
        "ingested_at": ingested_at,
    }]


