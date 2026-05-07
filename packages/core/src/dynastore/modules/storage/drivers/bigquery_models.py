"""BigQuery driver DTOs (Phase 4a — READ path; Phase 3/WRITE — reporter path).

Per-request credential overrides + Secret-wrapped credentials land in
Phase 4b/4e. This phase uses CloudIdentityProtocol for all auth, so the
driver config only carries target identity.

Phase 3 of the naming-harmonisation plan adds a reporter-mode WRITE path:
BigQuery acts as an async ingestion-mirror sink under ``Operation.WRITE``
fan-out (on_failure=warn) without pretending to be the source of truth.
Three reporter modes are supported:

- ``off``           — default; WRITE is a no-op, preserving the Phase 4a
                      semantics (READ-only driver).
- ``flat``          — one BQ row per feature: primary key + optional
                      payload + ingestion timestamp.
- ``batch_summary`` — one BQ row per ``write_entities`` call: collection
                      id, row count, min/max timestamps.  Best fit for
                      data-engineering pipelines that just need to know
                      "how many rows landed, when".
"""

from __future__ import annotations

from typing import ClassVar, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from dynastore.modules.storage.driver_config import CollectionDriverConfig
from dynastore.tools.secrets import Secret


class BigQueryTarget(BaseModel):
    """Identity of a BQ read target. All fields optional so Phase 4b's
    per-request overrides can supply them later; Phase 4a requires all
    three to be present on the registered config."""

    model_config = ConfigDict(extra="forbid")

    project_id: Optional[str] = None
    dataset_id: Optional[str] = None
    table_name: Optional[str] = None

    def is_fully_qualified(self) -> bool:
        return bool(self.project_id and self.dataset_id and self.table_name)

    def fqn(self) -> str:
        if not self.is_fully_qualified():
            raise ValueError(
                "BigQueryTarget is not fully qualified; "
                "all of project_id, dataset_id, table_name required",
            )
        return f"{self.project_id}.{self.dataset_id}.{self.table_name}"


class BigQueryCredentials(BaseModel):
    """Secret-wrapped BigQuery credentials.

    Registered-per-collection only — stored encrypted at rest in the
    platform's PluginConfig jsonb, masked in API responses, revealed only
    inside the BQ client constructor. Matches the platform credential
    framework.

    Per-request credential overrides (spec lines 546-581's
    BigQuerySecondarySpec with credentials field) are explicitly NOT
    added in this phase per user direction.
    """

    model_config = ConfigDict(extra="forbid")

    service_account_json: Optional[Secret] = Field(
        default=None,
        description="Full service-account JSON (Secret-wrapped). Preferred.",
    )
    api_key: Optional[Secret] = Field(
        default=None,
        description="BigQuery External Connections API key (Secret-wrapped). Future-proof.",
    )

    def is_empty(self) -> bool:
        """True iff no credential material supplied.

        Drivers fall back to CloudIdentityProtocol (Phase 4a path) when
        ``is_empty()``.
        """
        return self.service_account_json is None and self.api_key is None


class ItemsBigQueryDriverConfig(CollectionDriverConfig):
    """Registered per-collection config for the BigQuery driver.

    Phase 4a: READ-only, credentials via CloudIdentityProtocol.
    Phase 4b adds ``credentials: BigQueryCredentials`` for per-request overrides.
    Phase 4d adds write-path fields (streaming_threshold, partition_column, etc.).
    Phase 4e adds registered-per-collection Secret-wrapped credentials on the
    config itself; per-request overrides remain deferred.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "catalog", "collection", "items", "drivers")
    _visibility: ClassVar[Optional[str]] = "collection"


    target: BigQueryTarget = Field(default_factory=BigQueryTarget)
    credentials: BigQueryCredentials = Field(default_factory=BigQueryCredentials)
    location: str = "EU"
    page_size: int = Field(default=1000, ge=1, le=50000)
    query_timeout_s: int = Field(default=60, ge=1, le=600)

    # ---- Reporter-mode WRITE path (Phase 3) ------------------------------
    # All four fields are inert when ``reporter_mode == "off"`` so the
    # default-fast invariant holds: a bare ``ItemsBigQueryDriverConfig()``
    # has an empty ``model_dump(exclude_unset=True)`` and WRITE is a no-op.
    reporter_mode: Literal["off", "flat", "batch_summary"] = Field(
        default="off",
        description=(
            "BigQuery WRITE-path behaviour: 'off' disables WRITE, 'flat' "
            "streams one row per feature, 'batch_summary' streams one row "
            "per write_entities call."
        ),
    )
    report_target: Optional[BigQueryTarget] = Field(
        default=None,
        description=(
            "Destination table for reporter rows.  When None, falls back "
            "to ``target`` (read and report share the same table — fine "
            "for append-only DWH mirrors; insufficient when reports need "
            "their own schema, which the schema for flat / batch_summary "
            "implies)."
        ),
    )
    include_payload: bool = Field(
        default=False,
        description=(
            "In ``flat`` mode, include the full feature.properties JSON "
            "as a ``payload`` column.  Leave False for PII-sensitive "
            "collections where only counts / IDs should leave the tenant."
        ),
    )
    exclude_fields: List[str] = Field(
        default_factory=list,
        description=(
            "Property names stripped from each feature before streaming to "
            "BigQuery.  Applied only to ``flat`` mode payloads; "
            "batch_summary never emits properties so this is ignored."
        ),
    )


