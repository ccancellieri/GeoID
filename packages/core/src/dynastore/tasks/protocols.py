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
from typing import ClassVar, Protocol, AsyncGenerator, Optional, TypeVar, Generic, runtime_checkable, Any
from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigService
from dynastore.tasks.descriptor import TaskDescriptor

DefinitionType = TypeVar('DefinitionType', covariant=True)
PayloadType = TypeVar('PayloadType', contravariant=True)
ReturnType = TypeVar('ReturnType', covariant=True)


@runtime_checkable
class TaskProtocol(HasConfigService, Protocol, Generic[DefinitionType, PayloadType, ReturnType]):
    """
    Defines the contract for a DynaStore Background Task.

    Service-affinity placement (which service may claim a given task_type)
    is controlled by ``TaskPlacementConfig`` — see
    ``modules/tasks/placement/model.py``. There is intentionally no
    source-level routing decoration: routing is deployment, not code.
    """

    priority: int = 0
    # Declared-once class attributes consumed by the durable task-capability
    # registry and the mandatory-ownership guarantee. Defaults make every existing
    # task a non-mandatory, tier-agnostic participant with zero edits.
    mandatory: ClassVar[bool] = False
    affinity_tier: ClassVar[Optional[str]] = None
    # Optional opt-in: a pydantic model describing this task's payload. When
    # set, describe() exports its JSON Schema. Processes derive their schema
    # from the Process definition at publish time instead; plain tasks opt in
    # here. Left None for tasks that do not declare a payload contract.
    payload_model: ClassVar[Optional[type]] = None

    def is_available(self) -> bool:
        """
        Return True if this task is available for execution.
        """
        return True

    @classmethod
    def required_capability(cls, payload: Any) -> Optional[str]:
        """Return the capability id this row needs, or ``None`` if the row
        is not capability-gated.

        Companion to :meth:`can_claim`. The dispatcher uses this to query
        the shared-cache liveness oracle when ``can_claim`` rejects a row:
        if no live worker advertises this capability anywhere in the
        deployment, the row is DLQed instead of left PENDING (#502).

        Default ``None`` keeps existing tasks out of the reaper's path.
        """
        return None

    @classmethod
    def can_claim(cls, payload: Any) -> bool:
        """Payload-aware claim predicate.

        Called by the dispatcher AFTER ``claim_batch`` returns a row of
        this task_type, BEFORE the task is handed to a runner.  Returning
        ``False`` releases the claim back to PENDING (with a small back-off
        to avoid hot-looping the same worker on the same row).  Default
        returns ``True`` so existing tasks keep their previous behaviour.

        Use cases: a task class whose execution depends on a per-row
        capability that varies across worker pools (e.g. a specific
        Indexer module being registered in this process — see
        :class:`~dynastore.tasks.index_propagation.task.IndexPropagationTask`).
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

    @classmethod
    def payload_schema(cls) -> Optional[dict]:
        """JSON Schema for this task's payload, or None if undeclared.

        Reads the opt-in ``payload_model`` ClassVar. Guarded: a model without a
        ``model_json_schema`` method, or one that raises, yields None rather
        than breaking self-description.
        """
        model = getattr(cls, "payload_model", None)
        if model is None or not hasattr(model, "model_json_schema"):
            return None
        try:
            return model.model_json_schema()
        except Exception:  # noqa: BLE001 - self-description must never raise
            return None

    @classmethod
    def describe(cls) -> "TaskDescriptor":
        """Return this task's static self-description (see TaskDescriptor).

        description is the first non-empty line of the class docstring (so it
        is zero-edit for existing tasks); mandatory/affinity_tier mirror the
        ClassVars; payload_schema comes from payload_schema().
        """
        doc = (cls.__doc__ or "").strip()
        description = doc.splitlines()[0].strip() if doc else ""
        return TaskDescriptor(
            name=cls.get_name(),
            mandatory=bool(getattr(cls, "mandatory", False)),
            affinity_tier=getattr(cls, "affinity_tier", None),
            description=description,
            payload_schema=cls.payload_schema(),
        )

    @abc.abstractmethod
    async def run(self, payload: PayloadType) -> ReturnType:
        """
        Executes the task's logic.
        """
        ...
