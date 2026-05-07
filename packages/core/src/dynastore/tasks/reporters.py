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

# dynastore/modules/ingestion/reporters.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar
from pydantic import BaseModel
from sqlalchemy.engine import Engine



# A TypeVar for the configuration model, which must be a Pydantic BaseModel.
T_CONFIG = TypeVar("T_CONFIG", bound=BaseModel)


class ReportingInterface(Generic[T_CONFIG], ABC):
    """
    Abstract base class defining the interface for all ingestion reporters.
    Reporters are used to track and communicate the status and outcome of an ingestion task.
    This is a generic interface, allowing implementations to specify their own Pydantic
    configuration model (e.g., class MyReporter(ReportingInterface[MyConfigModel]))
    """
    def __init__(self, engine: Engine, task_id: str, task_request: Any, config: Optional[T_CONFIG] = None, **kwargs):
        """
        Initializes the reporter with common context.
        :param engine: The database engine for any DB operations.
        :param task_id: The unique ID of the running task.
        :param task_request: The full request model for the ingestion task.
        :param config: The validated Pydantic model for this reporter's specific configuration.
        """
        self.engine = engine
        self.task_id = task_id
        self.task_request = task_request
        self.config = config

    @abstractmethod
    async def task_started(self, task_id: str, collection_id: str, catalog_id: str, source_file: str):
        """Called once when the ingestion task begins."""
        pass

    @abstractmethod
    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]):
        """
        Processes the results of a single written batch. Each dictionary in the
        list represents a single row's outcome.
        
        Example item: {'status': 'SUCCESS', 'message': '...', 'record': {...}}
        """
        pass

    @abstractmethod
    async def update_progress(self, processed_count: int, total_count: Optional[int] = None):
        """Optionally called to update the progress of the task."""
        pass

    @abstractmethod
    async def task_finished(self, final_status: str, error_message: Optional[str] = None):
        """Called once when the task concludes, successfully or with an error."""
        pass
