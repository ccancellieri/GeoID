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
import random
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from dynastore.tasks.reporters import ReportingInterface
from dynastore.tasks.ingestion.reporters import ingestion_reporter

logger = logging.getLogger(__name__)

# GeoJSON/legacy geometry-bearing keys. Stripped unconditionally: this
# reporter is attributes-only by contract, regardless of ``include_geometry``.
_GEOMETRY_KEYS = ("geometry", "geom", "bbox")


class BqFeatureReporterConfig(BaseModel):
    project_id: str = Field(
        ..., description="The GCP project ID owning the destination BigQuery dataset."
    )
    dataset_id: str = Field(..., description="The destination BigQuery dataset ID.")
    table_name: str = Field(
        ..., description="The destination BigQuery table name, within dataset_id."
    )
    include_geometry: bool = Field(
        default=False,
        description=(
            "Reserved for a possible future geometry-inclusion mode; currently "
            "inert. This reporter is attributes-only by contract — geometry, "
            "geom and bbox are unconditionally stripped regardless of this "
            "flag's value."
        ),
    )
    demo_random_column: str = Field(
        default="demo_value",
        description=(
            "Name of the per-row random float column added to every insert, "
            "used as the join-enrichment column by the BigQuery DWH-join "
            "integration test."
        ),
    )


@ingestion_reporter
class BqFeatureReporter(ReportingInterface[BqFeatureReporterConfig]):
    """Streams ingested feature attributes (no geometry) into BigQuery.

    Attributes-only sibling of ``GcsDetailedReporter``: for every successfully
    written batch, each record's ``properties`` are flattened into a row and
    streamed into ``{project_id}.{dataset_id}.{table_name}`` via
    ``BigQueryProtocol.insert_rows_json``, plus one configurable random-float
    column consumed by the BigQuery-join integration test (dynastore#421).
    """

    # Re-declare the inherited ``config`` attribute's type — see the identical
    # annotation on ``GcsDetailedReporter`` for why this is needed.
    config: Optional[BqFeatureReporterConfig]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Dynamically acquire the BigQuery client from the GCP module using protocols.
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import BigQueryProtocol

        self._bq = None
        try:
            self._bq = get_protocol(BigQueryProtocol)
            if not self._bq:
                logger.warning(
                    "BigQueryProtocol (GCP) not found. BqFeatureReporter will be disabled."
                )
        except Exception as e:
            logger.warning(
                f"Failed to acquire BigQuery protocol: {e}. BqFeatureReporter will be disabled."
            )

        if not self.config or not self._bq:
            self.config = None  # Effectively disable the reporter
            return

        self._table_fqn = (
            f"{self.config.project_id}.{self.config.dataset_id}.{self.config.table_name}"
        )

    async def task_started(
        self, task_id: str, collection_id: str, catalog_id: str, source_file: str
    ):
        if not self.config:
            return
        logger.info(
            f"BQ Feature Reporter enabled for task {task_id} -> {self._table_fqn}."
        )

    async def update_progress(
        self, processed_count: int, total_count: Optional[int] = None
    ):
        pass

    async def task_finished(
        self,
        final_status: str,
        error_message: Optional[str] = None,
        summary: Optional[Dict[str, Any]] = None,
    ):
        if not self.config:
            return
        logger.info(
            f"BQ Feature Reporter for task {self.task_id} finished with status "
            f"{final_status} -> {self._table_fqn}."
        )

    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]):
        if not self.config or not self._bq:
            return

        rows = [row for row in (self._to_row(result) for result in batch_results) if row]
        if not rows:
            return

        try:
            errors = await self._bq.insert_rows_json(
                self._table_fqn, rows, project_id=self.config.project_id
            )
            if errors:
                logger.warning(
                    "BqFeatureReporter: %d/%d rows failed to insert into %s: %s",
                    len(errors), len(rows), self._table_fqn, errors,
                )
        except Exception as e:
            logger.error(
                f"BqFeatureReporter: failed to insert rows into {self._table_fqn}: {e}",
                exc_info=True,
            )

    def _to_row(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Flatten one batch outcome's ``record.properties`` into a BQ row.

        Only successfully-written records carry attributes worth joining
        against downstream — a failed record's record dict may be partial or
        absent. Geometry-bearing keys are stripped unconditionally.
        """
        assert self.config is not None
        if result.get("status") != "SUCCESS":
            return None

        record = result.get("record")
        if record is not None and hasattr(record, "model_dump"):
            record = record.model_dump(mode="json", exclude_none=True)
        if not isinstance(record, dict):
            return None

        # GeoJSON Feature shape uses ``properties``; legacy flat DWH records
        # use ``attributes`` (mirrors the split already in bucket_reporter.py).
        attrs = record.get("properties")
        if not isinstance(attrs, dict):
            attrs = record.get("attributes")
        if not isinstance(attrs, dict):
            return None

        row = dict(attrs)
        for key in _GEOMETRY_KEYS:
            row.pop(key, None)

        row[self.config.demo_random_column] = random.random()
        return row
