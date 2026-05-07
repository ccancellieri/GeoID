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

# dynastore/modules/catalog/asset_tasks_spi.py

from typing import Protocol, Any, runtime_checkable, Optional
from dynastore.modules.catalog.asset_service import Asset
from dynastore.modules.processes.models import Process, ExecuteRequest
from dynastore.modules.tasks.models import TaskPayload

@runtime_checkable
class AssetTasksSPI(Protocol):
    """
    SPI for tasks that can be executed on an Asset.
    """
    
    @staticmethod
    def get_process_definition() -> Process:
        """Returns the OGC Process definition for this task."""
        ...

    async def can_run_on_asset(self, asset: Asset) -> bool:
        """Determines if this task can be executed on the given asset."""
        ...

    async def get_execution_request(self, asset: Asset, execution_request: ExecuteRequest) -> ExecuteRequest:
        """
        Adapts the execution request based on the asset.
        This allows the SPI to 'map' asset properties into task inputs.
        """
        return execution_request

    async def run(self, payload: TaskPayload[ExecuteRequest], catalog_id: Optional[str] = None, collection_id: Optional[str] = None) -> Any:
        """
        Executes the task.
        """
        ...
