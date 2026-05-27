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

import json
import logging
import os
import gzip
import tempfile
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Literal, Optional, TYPE_CHECKING

from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules import get_protocol
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from dynastore.tasks.reporters import ReportingInterface
from dynastore.tasks.ingestion.reporters import ingestion_reporter
from dynastore.tools.path import insert_before_extension

logger = logging.getLogger(__name__)

from pydantic import BaseModel, Field
from typing import Optional, Literal, List, Any, Dict

# Canonical GeoJSON/STAC Feature structural keys. The read assembly bridges
# every sidecar attribute into ``feature.__pydantic_extra__`` (so STAC extension
# generators can read them) — which means each attribute is serialized BOTH
# under ``properties`` and echoed at the Feature top level. In a report that
# echo is pure duplication, so we keep only structural keys plus the reporter's
# own legacy DWH fields; attributes then appear exactly once, under ``properties``.
_FEATURE_STRUCTURAL_KEYS = frozenset(
    {
        "type",
        "id",
        "geometry",
        "bbox",
        "properties",
        "links",
        "assets",
        "collection",
        "stac_version",
        "stac_extensions",
    }
)
# Sibling envelope keys to ``properties`` introduced by the reporter-shape
# split (D1): ``stats`` is the flat platform-derived statistic bag (formerly
# mixed into ``properties``); ``system`` carries identity + lifecycle fields
# (formerly stamped at the record top level).
_REPORT_ENVELOPE_KEYS = frozenset({"stats", "system"})

if TYPE_CHECKING:
    from dynastore.modules.gcp.gcp_module import GCPModule


class GcsDetailedReporterConfig(BaseModel):
    report_per_chunk: bool = Field(
        default=False,
        description="If true, a separate report file will be uploaded for each processed chunk.",
    )
    signed_url_enabled: bool = Field(
        default=False,
        description="If true, a signed URL will be generated for the detailed report upon task completion.",
    )
    include_geometry: bool = Field(
        default=False,
        description="If false, geometry fields will be excluded from the detailed report.",
    )
    include_bbox: bool = Field(
        default=False,
        description="If false, bounding box fields will be excluded from the detailed report.",
    )
    reported_fields: Optional[List[str]] = Field(
        default=None,
        description="A list of top-level fields to include for each entry (e.g., 'status', 'record', 'message'). If None, all fields are included.",
    )
    reported_attributes: Optional[List[str]] = Field(
        default=None,
        description="A list of attribute keys to include in the report. If None, all attributes are included.",
    )
    include_geoid: Optional[bool] = Field(
        default=None,
        description="Include the generated `geoid` at the record top level. "
        "None/True = include when the ingestion produced one; False = suppress.",
    )
    include_external_id: Optional[bool] = Field(
        default=None,
        description="Include the source `external_id` at the record top level. "
        "None/True = include when the ingestion produced one; False = suppress.",
    )
    include_asset_id: Optional[bool] = Field(
        default=None,
        description="Include the `asset_id` under `record.system`. "
        "None/True = include when present; False = suppress.",
    )
    report_content: Literal["ALL", "ONLY_SUCCESS", "ONLY_FAILURE"] = Field(
        default="ALL",
        description="Determines which records to include in the detailed report based on their outcome.",
    )
    compress_output: bool = Field(
        default=False,
        description="If true, the detailed report will be compressed with Gzip before upload.",
    )
    output_format: Literal["JSONL", "JSON"] = Field(
        default="JSON", description="The format of the detailed report file."
    )
    report_file_path: str = (
        "gs://your-bucket/ingestion-reports/{task_id}-{timestamp_utc}.json"
    )


@ingestion_reporter
class GcsDetailedReporter(ReportingInterface[GcsDetailedReporterConfig]):
    """
    Buffers detailed row-by-row results and uploads them as a single JSON
    report to a GCS bucket upon task completion for detailed analysis.
    """

    # Re-declare the inherited ``config`` attribute's type. ``__init__`` assigns
    # ``self.config = None`` to disable the reporter when GCP/config is absent;
    # without this annotation the type checker would infer the attribute as
    # ``None``-only from that assignment, collapsing every later
    # ``assert self.config is not None`` to ``Never``.
    config: Optional[GcsDetailedReporterConfig]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Dynamically acquire the storage client from the GCP module using protocols.
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import CloudStorageClientProtocol

        self._storage_client = None
        self._client_provider = None
        try:
            self._client_provider = get_protocol(CloudStorageClientProtocol)
            if self._client_provider:
                self._storage_client = self._client_provider.get_storage_client()
            else:
                logger.warning(
                    "CloudStorageClientProtocol (GCP) not found. GcsDetailedReporter will be disabled."
                )
        except Exception as e:
            logger.warning(
                f"Failed to acquire GCP storage client: {e}. GcsDetailedReporter will be disabled."
            )

        if not self.config or not self._storage_client:
            self._temp_report_file = None
            self.config = None  # Effectively disable the reporter
            return

        self.processed_chunks = 0
        # Correctly access the asset code from the nested 'asset' object.
        # The asset code is now resolved definitively in the main ingestion task before reporters are initialized.
        # However, as a fallback for other contexts, we still check the request.
        asset_code = self.task_request.asset.asset_id or (
            self.task_request.asset.uri.split("/")[-1]
            if self.task_request.asset.uri
            else "unknown"
        )

        self.report_path = self.config.report_file_path.format(
            task_id=self.task_id,
            timestamp_utc=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"),
            asset_code=asset_code,
        )

        if not self.report_path.startswith("gs://"):
            raise Exception(f"Invalid GCS path configured: {self.report_path}")

        # Create a temporary file in the configured TMPDIR to buffer the report.
        # This avoids holding the entire report in memory.
        # If report_per_chunk is true, we don't create a single persistent temp file.
        if not self.config.report_per_chunk:
            self._temp_report_file = tempfile.NamedTemporaryFile(
                mode="w+", delete=False, suffix=".json", dir=os.environ.get("TMPDIR")
            )

            if self.config.output_format == "JSON":
                self._temp_report_file.write("[")  # Start of JSON array

            logger.info(
                f"GCS Detailed Reporter will buffer results to temporary file: {self._temp_report_file.name}"
            )
        else:
            logger.info(
                "GCS Detailed Reporter configured to upload a separate report for each chunk."
            )

    async def update_progress(
        self, processed_count: int, total_count: Optional[int] = None
    ):
        pass

    async def task_started(
        self, task_id: str, collection_id: str, catalog_id: str, source_file: str
    ):
        if not self.config:
            return
        logger.info(f"GCS Detailed Reporter enabled for task {task_id}.")

        # Fail-fast: confirm the report bucket exists before the task burns
        # cycles ingesting features only to find at finish time that we
        # cannot persist the report.  Catches typos like
        # ``gs://dd88971-test-catalog-20/...`` (extra ``d``) up-front.
        self._assert_report_bucket_exists()

        report_path = insert_before_extension(self.report_path, f"_config")
        model = {
            "task_id": self.task_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "source_file": source_file,
            "task_request": self.task_request.model_dump(),
            "report_path": self.report_path,
        }
        self._upload_to_gcs(
            report_path, content=json.dumps(model, indent=2, cls=CustomJSONEncoder)
        )

    def _assert_report_bucket_exists(self) -> None:
        """Raise a clear error if the configured ``report_file_path``
        points at a non-existent bucket.

        Without this check the task runs to completion and only
        attempts the upload at ``task_finished`` — at which point the
        underlying GCS 404 surfaces as a deep ``InvalidResponse``
        traceback that buries the (usually trivial) cause: a typo in
        the bucket name.
        """
        if not self._storage_client or not self.report_path.startswith("gs://"):
            return
        bucket_name = self.report_path[len("gs://"):].split("/", 1)[0]
        if not bucket_name:
            raise ValueError(
                f"GcsDetailedReporter: report_file_path {self.report_path!r} "
                "is missing a bucket name."
            )
        try:
            exists = self._storage_client.bucket(bucket_name).exists()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "GcsDetailedReporter: cannot verify bucket %r (%s); "
                "deferring failure to upload time.", bucket_name, exc,
            )
            return
        if not exists:
            raise FileNotFoundError(
                f"GcsDetailedReporter: GCS bucket {bucket_name!r} (resolved "
                f"from report_file_path={self.report_path!r}) does not exist "
                "or is not accessible to this service account.  Check the "
                "bucket name (common typos: leading/trailing characters) "
                "and the runner's IAM permissions."
            )

    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]):
        if not self.config:
            return

        # Filter records based on the report_content configuration
        records_to_report = self._filter_records_for_reporting(batch_results)
        if not records_to_report:
            return

        if self.config.report_per_chunk:
            await self._process_and_upload_chunk_report(records_to_report)
        else:
            self._write_batch_to_temp_file(records_to_report)

        self.processed_chunks += 1

    def _filter_records_for_reporting(
        self, batch_results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filters a list of results based on the report_content configuration."""
        assert self.config is not None
        records_to_report = []
        if self.config.report_content == "ALL":
            records_to_report = batch_results
        elif self.config.report_content == "ONLY_SUCCESS":
            records_to_report = [
                r for r in batch_results if r.get("status") == "SUCCESS"
            ]
        elif self.config.report_content == "ONLY_FAILURE":
            records_to_report = [
                r for r in batch_results if r.get("status") != "SUCCESS"
            ]
        return records_to_report

    def _write_batch_to_temp_file(self, records_to_report: List[Dict[str, Any]]):
        """Writes a batch of records to the single, persistent temporary file."""
        if not self._temp_report_file:
            return
        assert self.config is not None

        # Stream each result in the batch to the temporary file as a JSON line.
        for result in records_to_report:
            filtered_result = self._filter_result_for_reporting(result)
            json_line = json.dumps(filtered_result, cls=CustomJSONEncoder)

            # Add comma for JSON array format, ensuring it's not the very first record.
            if (
                self.config.output_format == "JSON"
                and self._temp_report_file.tell() > 1
            ):
                self._temp_report_file.write(",")  # Add comma for JSON array
            self._temp_report_file.write(
                json_line + ("\n" if self.config.output_format == "JSONL" else "")
            )

    async def _process_and_upload_chunk_report(
        self, records_to_report: List[Dict[str, Any]]
    ):
        """Creates, writes, and uploads a temporary report file for a single chunk."""
        assert self.config is not None
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".json", dir=os.environ.get("TMPDIR")
        ) as temp_chunk_file:
            if self.config.output_format == "JSON":
                temp_chunk_file.write("[")

            for i, result in enumerate(records_to_report):
                filtered_result = self._filter_result_for_reporting(result)
                json_line = json.dumps(filtered_result, cls=CustomJSONEncoder)

                if self.config.output_format == "JSON" and i > 0:
                    temp_chunk_file.write(",")
                temp_chunk_file.write(
                    json_line + ("\n" if self.config.output_format == "JSONL" else "")
                )

            if self.config.output_format == "JSON":
                temp_chunk_file.write("]")

            temp_chunk_file_path = temp_chunk_file.name

        # Determine the unique path for this chunk's report
        chunk_report_path = insert_before_extension(
            self.report_path, f"_chunk_{self.processed_chunks}"
        )
        content_type = (
            "application/jsonl"
            if self.config.output_format == "JSONL"
            else "application/json"
        )

        upload_file_path = temp_chunk_file_path
        # Handle compression if enabled
        if self.config.compress_output:
            chunk_report_path += ".gz"
            gzipped_file_path = temp_chunk_file_path + ".gz"
            with (
                open(temp_chunk_file_path, "rb") as f_in,
                gzip.open(gzipped_file_path, "wb") as f_out,
            ):
                f_out.writelines(f_in)
            upload_file_path = gzipped_file_path
            content_type = "application/gzip"

        try:
            self._upload_to_gcs(
                chunk_report_path, file_path=upload_file_path, content_type=content_type
            )
        finally:
            # Clean up temporary files for this chunk
            if os.path.exists(temp_chunk_file_path):
                os.remove(temp_chunk_file_path)
            if upload_file_path != temp_chunk_file_path and os.path.exists(
                upload_file_path
            ):
                os.remove(upload_file_path)

    def _filter_result_for_reporting(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filters a result dictionary based on the reporter's configuration, controlling which
        top-level fields, attributes, and geometry are included in the final report.
        """
        assert self.config is not None
        # Make a shallow copy to avoid modifying the original object which might be used elsewhere.
        filtered_result = result.copy()

        record = filtered_result.get("record")
        # The ingestion pipeline hands back GeoJSON Feature objects (keys
        # ``geometry``/``properties``), not the legacy flat DWH dict. Normalise
        # a pydantic model to a plain dict so the exclusion/allow-list below can
        # operate on it; otherwise geometry would leak and the filters no-op.
        if record is not None and hasattr(record, "model_dump"):
            record = record.model_dump(mode="json", exclude_none=True)

        if isinstance(record, dict):
            record = dict(record)

            # Identity lives under ``record["system"]`` since the D1 envelope
            # split: include_*=False strips a single key from that bag rather
            # than the record top level.
            system = record.get("system")
            if isinstance(system, dict):
                system = dict(system)
                if self.config.include_geoid is False:
                    system.pop("geoid", None)
                if self.config.include_external_id is False:
                    system.pop("external_id", None)
                if self.config.include_asset_id is False:
                    system.pop("asset_id", None)
                record["system"] = system
            else:
                system = None

            # 1. Attribute allow-list — applies to the GeoJSON ``properties``
            #    bag and the legacy flat ``attributes`` bag alike. ``stats`` is
            #    its own sibling envelope key (platform-derived) and is not
            #    subject to the user-attribute allow-list.
            reported_attrs = self.config.reported_attributes
            if reported_attrs is not None:
                for attr_key in ("properties", "attributes"):
                    blob = record.get(attr_key)
                    if isinstance(blob, dict):
                        record[attr_key] = {
                            key: blob[key] for key in reported_attrs if key in blob
                        }

            # 2. Exclude geometry — GeoJSON ``geometry`` and legacy ``geom``.
            if not self.config.include_geometry:
                record.pop("geometry", None)
                record.pop("geom", None)

            # 3. Exclude bbox — GeoJSON ``bbox`` and legacy ``bbox_coords``.
            if not self.config.include_bbox:
                record.pop("bbox", None)
                record.pop("bbox_coords", None)

            # 4. De-duplicate the top level. Sidecar attributes are echoed at
            #    the Feature top level (from ``__pydantic_extra__``) as well as
            #    under ``properties``; keep only structural + legacy DWH keys
            #    plus the envelope siblings ``stats``/``system`` so each
            #    attribute appears once, under ``properties``.
            allowed_top_level = (
                set(_FEATURE_STRUCTURAL_KEYS) | set(_REPORT_ENVELOPE_KEYS)
            )
            record = {
                key: value
                for key, value in record.items()
                if key in allowed_top_level
            }

            filtered_result["record"] = record

        # 3. Filter the top-level fields of the result itself.
        if self.config.reported_fields is not None:
            # Ensure essential fields for other filters are temporarily kept
            fields_to_keep = set(self.config.reported_fields)
            final_filtered_result = {
                key: filtered_result[key]
                for key in fields_to_keep
                if key in filtered_result
            }
            return final_filtered_result

        return filtered_result

    async def task_finished(self, final_status: str, error_message: Optional[str] = None):
        if not self.config:
            return

        # If reporting per chunk, the main report is already uploaded.
        if self.config.report_per_chunk:
            return await self._upload_summary_report(final_status, error_message)

        assert self._temp_report_file is not None
        if self.config.output_format == "JSON":
            self._temp_report_file.write("]")  # End of JSON array

        self._temp_report_file.close()  # Close the file to ensure all writes are flushed.

        upload_file_path = self._temp_report_file.name
        report_path = self.report_path
        content_type = (
            "application/jsonl"
            if self.config.output_format == "JSONL"
            else "application/json"
        )

        # Handle compression if enabled
        if self.config.compress_output:
            report_path += ".gz"
            gzipped_file_path = self._temp_report_file.name + ".gz"

            with open(self._temp_report_file.name, "rb") as f_in:
                with gzip.open(gzipped_file_path, "wb") as f_out:
                    f_out.writelines(f_in)

            upload_file_path = gzipped_file_path
            content_type = "application/gzip"

        try:
            # --- Upload the main detailed report from the temporary file ---
            self._upload_to_gcs(
                report_path, file_path=upload_file_path, content_type=content_type
            )
        except Exception as e:
            logger.error(f"Failed to upload detailed report: {e}", exc_info=True)
        finally:
            # Clean up temporary files
            if os.path.exists(self._temp_report_file.name):
                os.remove(self._temp_report_file.name)
            if self.config.compress_output and os.path.exists(upload_file_path):
                os.remove(upload_file_path)
            self._temp_report_file = None

        # --- Upload the summary report ---
        await self._upload_summary_report(
            final_status, error_message, final_detailed_report_path=report_path
        )

    async def _upload_summary_report(
        self,
        final_status: str,
        error_message: Optional[str],
        final_detailed_report_path: Optional[str] = None,
    ):
        """Generates and uploads the final summary report."""
        assert self.config is not None
        # If reporting per chunk, the detailed_report_path is a template. Otherwise, it's the specific file path.
        summary = {
            "task_id": self.task_id,
            "final_status": final_status,
            "error_message": error_message,
            "total_chunks": self.processed_chunks,
            "detailed_report_path": final_detailed_report_path
            if final_detailed_report_path
            else self.report_path,
            "report_generated_at": datetime.now(timezone.utc).isoformat(),
        }
        summary_content = json.dumps(summary, indent=2, cls=CustomJSONEncoder)
        summary_report_path = insert_before_extension(self.report_path, f"_summary")
        self._upload_to_gcs(summary_report_path, content=summary_content)

        if (
            self.config.signed_url_enabled
            and self._client_provider is not None
            and final_detailed_report_path
        ):
            # Signed URLs are only generated for single-file reports.
            try:
                from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

                signed_url = await generate_gcs_signed_url(
                    final_detailed_report_path,
                    method="GET",
                    expiration=timedelta(days=7),
                    client_provider=self._client_provider,
                )
                logger.info(
                    f"Detailed report is accessible for 7 days at: {signed_url}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to generate signed URL for report: {e}", exc_info=True
                )

    def _upload_to_gcs(
        self,
        gcs_path: str,
        content: Optional[str] = None,
        file_path: Optional[str] = None,
        content_type: str = "application/json",
    ):
        """Helper method to upload content or a file to GCS using the provided client."""
        if not self._storage_client:
            logger.error(
                "Storage client not available in GcsDetailedReporter. Cannot upload."
            )
            return
        try:
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            bucket = self._storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            if file_path:
                logger.info(f"Uploading report from {file_path} to {gcs_path}...")
                blob.upload_from_filename(file_path, content_type=content_type)
            elif content is not None:
                logger.info(f"Uploading report content to {gcs_path}...")
                blob.upload_from_string(content, content_type=content_type)
            logger.info(f"Successfully uploaded to {gcs_path}")
        except Exception as e:
            logger.error(f"Failed to upload to GCS path {gcs_path}: {e}", exc_info=True)
