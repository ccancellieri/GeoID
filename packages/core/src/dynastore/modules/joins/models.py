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

"""Shared join DTOs for OGC API - Joins (extensions/joins/) and the future
tile-join quarantine (extensions/dwh/, untouched in this PR).

Phase 4b PR-1 ships only NamedSecondarySpec. PR-2 adds BigQuerySecondarySpec
with per-request target overrides + Secret-wrapped credentials.
"""

from __future__ import annotations

from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget


class PrimaryFilterSpec(BaseModel):
    """OGC CQL2 filter expression — applied BEFORE the join.

    Used as both ``JoinRequest.primary_filter`` (filters the primary
    collection rows before the join executor sees them) and as
    ``BigQuerySecondarySpec.filter`` (filters the BigQuery secondary
    table — translated to a BQ ``WHERE`` clause via
    ``modules/joins/bq_filter.cql_to_bq_where``). The same DTO on both
    sides means the join body speaks ONE OGC standard filter language.

    CQL2 is the OGC API - Features Part 3 / OGC API - Filter language.
    Two encodings are accepted:

    * ``cql2-text``: human-readable infix — e.g.
      ``"area > 100 AND ADM2_PCODE LIKE 'TG%'"``,
      ``"S_INTERSECTS(geom, BBOX(0, 5, 2, 8))"``.
    * ``cql2-json``: AST-as-JSON form — e.g.
      ``{"op": ">", "args": [{"property": "area"}, 100]}``.

    The CQL2 parser is also the SQL-injection guard: arbitrary user
    input is constrained to nodes pygeofilter recognises before any
    SQL is built. Bad syntax → 400 with the parser error; references
    to unknown columns → 400 listing the available columns.
    """
    model_config = ConfigDict(extra="forbid")

    cql: str = Field(
        ...,
        min_length=1,
        description=(
            "CQL2 expression body. Examples: \"area > 100\", "
            "\"S_INTERSECTS(geom, BBOX(0,5,2,8))\", "
            "\"ADM2_PCODE IN ('TG0309', 'TG0403')\"."
        ),
        examples=[
            "area > 100",
            "S_INTERSECTS(geom, BBOX(0, 5, 2, 8))",
            "temprature > 25 AND rain_fall < 100",
        ],
    )
    cql_lang: Literal["cql2-text", "cql2-json"] = Field(
        default="cql2-text",
        description=(
            "CQL2 dialect. ``cql2-text`` is human-readable infix; "
            "``cql2-json`` is the AST-as-JSON form for generators."
        ),
    )


class NamedSecondarySpec(BaseModel):
    """Join secondary that references an already-registered collection.

    Use when the data you want to enrich with is itself a DynaStore
    collection in the same catalog. The /join executor walks the
    registered READ driver for the secondary collection (any backend —
    PG, BQ, Iceberg, DuckDB, ES) and indexes its rows in memory before
    joining against the primary stream.
    """
    model_config = ConfigDict(extra="forbid")

    driver: Literal["registered"] = "registered"
    ref: str = Field(
        ...,
        description=(
            "Collection id of the registered secondary, in the same "
            "catalog as the primary collection."
        ),
        examples=["population_2024", "temperature_baseline"],
    )


class BigQuerySecondarySpec(BaseModel):
    """Join secondary that points at an inline BigQuery table.

    Use for ad-hoc enrichment from a BigQuery table without registering
    the table as a DynaStore collection first. The target is fully
    qualified inline (project / dataset / table) and the BQ driver
    streams ``SELECT * FROM <fqn>`` (with an optional CQL2-derived
    ``WHERE`` clause from ``filter``) per request.

    Auth currently flows through ``CloudIdentityProtocol`` (Application
    Default Credentials of the running service). Phase 4e adds
    Secret-wrapped per-request credential overrides via a future
    ``credentials`` field.

    For BQ-side complexity beyond CQL2 (multi-table JOIN, GROUP BY,
    window functions, PIVOT, CTEs…): create a BigQuery view in your
    own project that bakes the SQL in, then point ``target.table_name``
    at the view. The OGC join body stays simple and standard, the SQL
    power lives in BQ where the planner can cache the view.
    """
    model_config = ConfigDict(extra="forbid")

    driver: Literal["bigquery"] = "bigquery"
    target: BigQueryTarget = Field(
        ...,
        description=(
            "Fully-qualified BigQuery target. All three of project_id, "
            "dataset_id and table_name are required — partial targets "
            "are rejected at execution time. ``table_name`` may name "
            "either a table or a view."
        ),
    )
    filter: Optional[PrimaryFilterSpec] = Field(
        default=None,
        description=(
            "Optional CQL2 filter applied to the BigQuery secondary "
            "BEFORE the join. Same shape as JoinRequest.primary_filter "
            "— both cql2-text and cql2-json accepted. Translated to a "
            "BigQuery ``WHERE`` clause via "
            "``modules/joins/bq_filter.cql_to_bq_where``. Spatial CQL2 "
            "ops (S_INTERSECTS / S_WITHIN / S_CONTAINS) translate to "
            "BigQuery GEOGRAPHY's ST_* functions; ``BBOX(...)`` "
            "translates to ``ST_GeogFromText('POLYGON(...)')``. Use a "
            "BigQuery view if you need SQL power CQL2 cannot express."
        ),
    )


SecondarySpec = Annotated[
    Union[NamedSecondarySpec, BigQuerySecondarySpec],
    Field(discriminator="driver"),
]


class JoinSpec(BaseModel):
    """The join key columns on each side, plus the merge mode."""
    model_config = ConfigDict(extra="forbid")

    primary_column: str = Field(
        ...,
        description=(
            "Join-key column on the primary collection's features. "
            "Looked up first in feature.properties, then on feature.id "
            "(useful when the join key IS the row identifier — e.g. the "
            "platform-managed ``geoid`` UUID for catalog rows)."
        ),
        examples=["geoid", "id"],
    )
    secondary_column: str = Field(
        ...,
        description=(
            "Join-key column on the secondary's rows. Same lookup "
            "semantics as primary_column (properties first, then id)."
        ),
        examples=["geoid", "id"],
    )
    join_type: Literal["INNER", "LEFT"] = Field(
        default="INNER",
        description=(
            "INNER (default) yields only features with matching secondary rows. "
            "LEFT yields all primary features; non-matching features have "
            "secondary properties set to null."
        ),
    )
    enrichment: bool = Field(
        default=True,
        description=(
            "If True (default), every property of the matching secondary "
            "row is merged into the joined primary feature's properties "
            "(additive — secondary keys win on collision). If False, "
            "only matching primary feature ids are emitted with no "
            "property change (effectively an inner-join filter on the "
            "primary stream)."
        ),
    )


class ProjectionSpec(BaseModel):
    """Output projection (geometry, CRS, attribute subset)."""
    model_config = ConfigDict(extra="forbid")

    with_geometry: bool = Field(
        default=True,
        description="If False, omit the geometry column from the output.",
    )
    destination_crs: int = Field(
        default=4326,
        ge=1,
        description=(
            "Output EPSG code. The primary driver reprojects features "
            "to this CRS before returning. Default 4326 (WGS-84)."
        ),
    )
    attributes: Optional[List[str]] = Field(
        default=None,
        description=(
            "Subset of PRIMARY attributes to emit. ``None`` (default) "
            "keeps every property the primary driver returned. The join "
            "key column is always preserved so the joined output stays "
            "self-describing."
        ),
        examples=[["geoid", "ADM2_PCODE", "area"]],
    )


class PagingSpec(BaseModel):
    """Paging applied AFTER the join (operates on joined features)."""
    model_config = ConfigDict(extra="forbid")

    limit: int = Field(
        default=100,
        ge=1,
        description=(
            "Max joined features to return. A value above the configured "
            "maximum (10 000 by default) is clamped, not rejected — see "
            "``joins_service._resolve_paging``."
        ),
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Page offset into the joined feature stream.",
    )


class OutputSpec(BaseModel):
    """Output format + encoding for the joined FeatureCollection."""
    model_config = ConfigDict(extra="forbid")

    format: Literal["geojson", "json", "csv", "geopackage", "parquet"] = Field(
        default="geojson",
        description=(
            "Output container. ``geojson`` and ``json`` stream; "
            "``csv``, ``geopackage`` and ``parquet`` accumulate the "
            "full result before writing."
        ),
    )
    encoding: str = Field(
        default="utf-8",
        description="Text encoding for non-binary formats.",
    )


class JoinRequest(BaseModel):
    """OGC API - Joins request body.

    Raw SQL is NOT accepted — both sides of the join body filter via
    CQL2 (``primary_filter`` for the primary, ``secondary.filter`` when
    the secondary is a ``BigQuerySecondarySpec``). The CQL2 parser
    doubles as the SQL-injection guard.

    Primary side: any READ-capable storage driver registered for the
    target collection. With a default-routing deployment, the platform
    PG items driver answers — no per-collection ``ItemsRoutingConfig``
    PUT required (PR #107 added zero-config hint routing).

    Secondary side: either ``NamedSecondarySpec`` (registered collection)
    or ``BigQuerySecondarySpec`` (inline per-request BQ table or view).
    """
    model_config = ConfigDict(extra="forbid")

    secondary: SecondarySpec = Field(
        ...,
        description=(
            "Discriminated union — ``driver: 'registered'`` for a "
            "registered collection, ``driver: 'bigquery'`` for an inline "
            "BigQuery target."
        ),
    )
    join: JoinSpec = Field(
        ..., description="Join key columns + merge mode.",
    )
    primary_filter: Optional[PrimaryFilterSpec] = Field(
        default=None,
        description=(
            "Optional CQL2 filter applied to the PRIMARY collection "
            "BEFORE the join. Forwarded to the primary driver via "
            "``QueryRequest.cql_filter``; drivers that support CQL2 "
            "(e.g. ItemsPostgresqlDriver) push the predicate down to "
            "their backend, drivers that don't treat it as a no-op."
        ),
    )
    projection: ProjectionSpec = Field(
        default_factory=ProjectionSpec,
        description="Output projection (geometry, CRS, attribute subset).",
    )
    paging: Optional[PagingSpec] = Field(
        default=None,
        description="Paging applied AFTER the join (default: limit=100).",
    )
    output: OutputSpec = Field(
        default_factory=OutputSpec,
        description="Output format + encoding (default: GeoJSON UTF-8).",
    )
