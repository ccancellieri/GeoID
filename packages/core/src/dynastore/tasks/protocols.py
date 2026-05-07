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

import abc
from typing import Protocol, AsyncGenerator, TypeVar, Generic, runtime_checkable, Any
from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigService

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)


@runtime_checkable
class TaskProtocol(HasConfigService, Protocol, Generic[DefinitionType, PayloadType, ReturnType]):
    """
    Defines the contract for a DynaStore Background Task.

    Service-affinity placement (which service may claim a given task_type)
    is controlled by ``TaskRoutingConfig`` — see
    ``modules/tasks/tasks_config.py``. There is intentionally no
    source-level routing decoration: routing is deployment, not code.
    """

    priority: int = 0

    def is_available(self) -> bool:
        """
        Return True if this task is available for execution.
        """
        return True

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """
        Task lifecycle manager.
        """
        yield

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Skip abstract classes and Protocols
        if cls.__name__.endswith("Protocol") or abc.ABC in cls.__bases__ or getattr(cls, "__is_protocol__", False):
            return

        try:
            from dynastore.tasks import _register_task
            _register_task(cls)
        except ImportError:
            # Fallback for when tasks module is not yet fully initialized or available
            pass

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the task.
        """
        return getattr(cls, "_registered_name", cls.__name__.lower())

    @abc.abstractmethod
    async def run(self, payload: PayloadType) -> ReturnType:
        """
        Executes the task's logic.
        """
        ...
