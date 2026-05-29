#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight input model for the DWH join export task."""

from typing import Optional

from pydantic import Field

from dynastore.extensions.dwh.models import DWHJoinRequest


class DwhJoinExportRequest(DWHJoinRequest):
    """Inputs for the async DWH-join export Process.

    The output location is **not** a client input: per OGC API - Processes,
    the server owns result storage. The task writes the artifact to the
    catalog's own bucket under a server-derived, per-job key
    (``processes/outputs/{process_id}/{job_id}/…``) and surfaces it as a
    time-limited signed URL in the job's ``message`` / results document.
    """
    reporting: Optional[dict] = Field(None, description="Reporter configuration")
