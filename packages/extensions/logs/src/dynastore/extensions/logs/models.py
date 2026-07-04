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

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

# --- SQL Schema ---

# --- Pydantic Models ---

class LogEntry(BaseModel):
    # Backend-assigned id — an Elasticsearch document id string like
    # "_system_:exception:<iso timestamp>" (#2749 removed PG persistence,
    # so there is no integer-id source left). Typing this as int made
    # every ES row fail validation, which search_logs' catch-all turned
    # into a silent empty list on all /logs read endpoints.
    id: Optional[str] = None
    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = Field(None, validation_alias="created_at")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )

class LogEntryCreate(BaseModel):
    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    is_system: bool = False


class LogsListResponse(BaseModel):
    """Response wrapper for log list endpoints with optional Kibana dashboard link."""
    logs: List[LogEntry]
    total: Optional[int] = None
    kibana_dashboard_url: Optional[str] = None
