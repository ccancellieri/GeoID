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

from typing import Protocol, runtime_checkable, Optional, List, Any
import uuid
from dynastore.modules.db_config.query_executor import DbResource

@runtime_checkable
class TasksProtocol(Protocol):
    """Protocol for the tasks management system and persistence."""

    async def create_task(
        self, engine: DbResource, task_data: Any, schema: str, initial_status: str = "PENDING"
    ) -> Any:
        """Creates a new task within a specific physical schema."""
        ...

    async def update_task(
        self, conn: DbResource, task_id: uuid.UUID, update_data: Any, schema: str
    ) -> Optional[Any]:
        """Updates an existing task within a physical schema."""
        ...

    async def get_task(
        self, conn: DbResource, task_id: uuid.UUID, schema: str
    ) -> Optional[Any]:
        """Retrieves a single task by its ID."""
        ...

    async def list_tasks(
        self, conn: DbResource, schema: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        """Lists all tasks for a given schema."""
        ...

    # Catalog-aware helpers (optional but common)
    async def create_task_for_catalog(
        self, engine: DbResource, task_data: Any, catalog_id: str
    ) -> Any:
        ...

    async def get_task_for_catalog(
        self, conn: DbResource, task_id: uuid.UUID, catalog_id: str
    ) -> Optional[Any]:
        ...

    async def list_tasks_for_catalog(
        self, conn: DbResource, catalog_id: str, limit: int = 20, offset: int = 0
    ) -> List[Any]:
        ...
