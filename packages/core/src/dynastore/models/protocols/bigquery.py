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

"""
BigQueryProtocol — abstraction for executing queries against Google BigQuery.

Implementations are registered via ``register_plugin(service)`` and discovered
with ``get_protocol(BigQueryProtocol)``.  The DWH extension uses this protocol
to decouple BigQuery access from the ``google.cloud.bigquery`` library.

Typical implementation: ``BigQueryService`` in ``modules/gcp/bigquery_service.py``.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dynastore.modules.storage.drivers.bigquery_models import (
        BigQueryCredentials,
    )


@runtime_checkable
class BigQueryProtocol(Protocol):
    """Execute SQL queries / streaming inserts against Google BigQuery.

    Implementations handle credential resolution, client lifecycle, and
    query execution.  Read results are returned as a list of row dicts;
    writes use the BQ streaming insert API (``insertAll``) with partial
    failure surfaced via the returned error list.

    The ``credentials`` kwarg (Phase 4e step 2) carries the registered-
    per-collection ``BigQueryCredentials``.  When supplied and non-empty,
    the implementation MUST resolve them through the same single-reveal
    site as ``modules/storage/drivers/bigquery._make_bq_client`` so the
    SA JSON / api_key never leaks outside that one function.  When
    omitted or empty, the implementation falls back to
    ``CloudIdentityProtocol`` (Phase 4a default).
    """

    async def execute_query(
        self,
        query: str,
        project_id: str,
        *,
        credentials: Optional["BigQueryCredentials"] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as row dicts.

        Args:
            query:       The BigQuery SQL query to execute.
            project_id:  The GCP project ID to run the query against.
            credentials: Optional Secret-wrapped per-collection
                         credentials.  ``None`` / ``is_empty()`` falls
                         back to ``CloudIdentityProtocol``.

        Returns:
            List of dicts, one per row, with column names as keys.

        Raises:
            RuntimeError: If credentials or client are unavailable.
        """
        ...

    async def insert_rows_json(
        self,
        table_fqn: str,
        rows: List[Dict[str, Any]],
        *,
        project_id: str,
        row_ids: Optional[List[Optional[str]]] = None,
        credentials: Optional["BigQueryCredentials"] = None,
    ) -> List[Dict[str, Any]]:
        """Stream rows into a BigQuery table via ``insertAll``.

        Best-effort write path used by the role-based driver refactor's
        WRITE / BACKUP / INDEX roles on BigQuery.  Individual row failures
        are returned in the result list rather than raised, so callers
        running under ``on_failure=warn`` can log and continue.

        Args:
            table_fqn:  Fully-qualified ``project.dataset.table`` name.
            rows:       List of row dicts whose keys match the table schema.
                        Empty list is a no-op returning ``[]``.
            project_id: GCP project for billing / quota (may differ from
                        ``table_fqn``'s project in cross-project writes).
            row_ids:    Optional per-row ``insertId`` values for
                        best-effort deduplication within the BQ streaming
                        buffer (24-hour window).  ``None`` per row means
                        BQ assigns a UUID.  Length must match ``rows``
                        when supplied.
            credentials: Optional Secret-wrapped per-collection
                        credentials.  ``None`` / ``is_empty()`` falls
                        back to ``CloudIdentityProtocol``.

        Returns:
            The BQ-returned error list — empty on full success; otherwise
            one dict per failing row with ``index`` and ``errors`` keys.
            Caller decides how to surface partial failures to the user.

        Raises:
            RuntimeError: If credentials / client are unavailable.
            ValueError:   If ``row_ids`` is supplied with a mismatched length.
        """
        ...
