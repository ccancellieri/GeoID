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

# dynastore/modules/ingestion/reporters_impl.py

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from sqlalchemy.engine import Engine

from dynastore.modules.db_config.query_executor import managed_transaction, run_in_event_loop
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tasks.models import TaskUpdate, TaskStatusEnum

from dynastore.tools.json import CustomJSONEncoder
from dynastore.tasks.reporters import ReportingInterface
from dynastore.tasks.ingestion.reporters import ingestion_reporter

logger = logging.getLogger(__name__)

@ingestion_reporter
class DatabaseStatusReporter(ReportingInterface):
    """
    Reports the high-level status and progress of an ingestion task
    to the central `tasks` table.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.processed_chunks = 0
        self.total_rows_in_source = None
        self.schema = kwargs.get("schema", "tasks")

    async def _update_task_async(self, update_data: TaskUpdate):
        """Internal async helper to perform the update."""
        if not self.task_id:
            return
        async with managed_transaction(self.engine) as conn:
            import uuid as _uuid
            task_uuid = _uuid.UUID(self.task_id) if isinstance(self.task_id, str) else self.task_id
            await tasks_module.update_task(conn, task_uuid, update_data, schema=self.schema)

    async def task_started(self, task_id: str, collection_id: str, catalog_id: str, source_file: str):
        """Asynchronous entrypoint for task_started."""
        update = TaskUpdate(status=TaskStatusEnum.RUNNING)
        await self._update_task_async(update)

    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]):
        if not self.task_id: return
        self.processed_chunks += 1
        # The number of processed rows is now only relevant if we have a total.
        # For chunk-based progress, we just need to trigger an update.
        processed_rows = self.processed_chunks * len(batch_results) # This is an approximation
        await self.update_progress(processed_rows, self.total_rows_in_source)

    async def update_progress(self, processed_count: int, total_count: Optional[int] = None):
        if not self.task_id:
            return
        
        # Store the total count when it's first received.
        if total_count is not None and self.total_rows_in_source is None:
            self.total_rows_in_source = total_count

        progress_value = 0
        # Use the stored total count for accurate percentage calculation.
        if self.total_rows_in_source and self.total_rows_in_source > 0:
            progress_value = min(int((processed_count / self.total_rows_in_source) * 100), 99)
        else:
            # Use the number of processed chunks as a proxy for progress, capped at 99.
            progress_value = min(self.processed_chunks, 99)
        update = TaskUpdate(progress=progress_value)
        await self._update_task_async(update)

    async def task_finished(self, final_status: str, error_message: Optional[str] = None):
        if not self.task_id: return
        
        update_data = {
            "status": final_status,
            "finished_at": datetime.now(timezone.utc),
            "error_message": error_message
        }
        if final_status == TaskStatusEnum.COMPLETED.value:
            update_data["progress"] = 100
        
        update = TaskUpdate(**update_data)
        await self._update_task_async(update)