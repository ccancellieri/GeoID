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

"""Input model for the OGC API - Joins async export task.

The heavy, non-paginated sibling of the synchronous ``/join`` endpoint: it
streams the full joined FeatureCollection (full-precision geometry via the
PostgreSQL read path) to the catalog's own bucket and returns a time-limited
signed URL by reference. Use it when a single page of the synchronous endpoint
cannot carry the result — dense-geometry, full-collection joins.
"""

from typing import Optional

from pydantic import ConfigDict, Field

from dynastore.modules.joins.models import JoinRequest


class JoinsExportRequest(JoinRequest):
    """Inputs for the async ``joins_export`` Process.

    Extends the standard OGC API - Joins request (``secondary``, ``join``,
    ``primary_filter``, ``projection``, ``output``) with the primary
    collection identity and reporter config. The inherited ``paging`` field is
    ignored — the export always materializes the complete joined result set.

    The output location is **not** a client input: per OGC API - Processes the
    server owns result storage. The task writes the artifact to the catalog's
    own bucket under a server-derived, per-job key
    (``processes/outputs/{process_id}/{job_id}/…``) and surfaces it as a
    time-limited signed URL in the job's results document and status message.
    """

    # The collection-scoped execution route injects ``catalog_id`` /
    # ``collection_id`` into the inputs; tolerate them (the primary identity is
    # carried by the ``catalog`` / ``collection`` body fields, mirroring the
    # dwh_join export contract) rather than rejecting them as unknown keys.
    model_config = ConfigDict(extra="ignore")

    catalog: str = Field(..., description="Catalog id of the primary collection.")
    collection: str = Field(..., description="Primary collection id.")
    reporting: Optional[dict] = Field(None, description="Reporter configuration")
