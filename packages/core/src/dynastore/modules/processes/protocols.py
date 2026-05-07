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

from typing import TYPE_CHECKING, Protocol, TypeVar, Generic, runtime_checkable, Optional, List
from dynastore.tasks.protocols import TaskProtocol

if TYPE_CHECKING:
    from dynastore.modules.processes.models import Process

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)


@runtime_checkable
class ProcessTaskProtocol(TaskProtocol[DefinitionType, PayloadType, ReturnType], Protocol):
    """
    A specialized TaskProtocol for tasks that explicitly expose an OGC Process definition.
    """
    @staticmethod
    def get_definition() -> DefinitionType:
        """
        MUST return the typed task definition model (e.g., OGC Process).
        """
        ...


@runtime_checkable
class ProcessRegistryProtocol(Protocol):
    """
    OGC Process discovery and metadata.

    Provides a structured interface for listing and looking up process
    definitions, replacing the ad-hoc ``get_definitions_by_type(Process)``
    pattern.  Supports tenant-scoped process listings for multi-tenant
    OGC API Processes compliance.
    """

    async def list_processes(
        self, tenant: Optional[str] = None,
    ) -> List["Process"]:
        """Return all available process summaries, optionally filtered by tenant."""
        ...

    async def get_process(
        self, process_id: str, tenant: Optional[str] = None,
    ) -> Optional["Process"]:
        """Return a single process definition by ID, or None."""
        ...
