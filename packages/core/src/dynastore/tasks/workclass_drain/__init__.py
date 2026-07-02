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

"""The async-write workclass — generic secondary-write drainers.

``AsyncWriteDrainTaskProtocol`` is the placement marker for the drain
workclass (#2732): a task subclasses it, instead of ``TaskProtocol``
directly, to declare that it performs generic secondary-write drain labour
(``StorageDrainTask``, ``EventDrainTask`` today; any future outbox drainer).

Why a marker instead of an enumerated task-key list
-----------------------------------------------------
``dynastore.modules.tasks.execution.offload_required`` used to hardcode
``{"storage_drain", "event_drain"}`` to decide which tasks must never run
in-process on the GCP serving tier. That required editing ``execution.py``
for every new drainer. Subclassing this base instead makes the placement
rule structural: ``offload_required`` resolves the task's registered class
via the task registry and checks ``is_async_write_workclass`` — any task
that inherits from here is covered with zero edits to ``execution.py``.

The marker only says "this task WANTS to be placed off the serving tier
when possible". The actual placement decision still fails open exactly like
the static OFFLOAD/HEAVY routing hints: ``execution._restrict_to_offload_runners``
only drops in-process runners when an offload-capable runner
(``gcp_cloud_run`` / ``worker_queue``) is actually present for the task in
this process — compose / onprem / tests with no deployed job keep running
the drain in-process.
"""
from __future__ import annotations

from typing import ClassVar, Protocol, runtime_checkable

from dynastore.tasks.protocols import TaskProtocol


@runtime_checkable
class AsyncWriteDrainTaskProtocol(TaskProtocol, Protocol):
    """Base for tasks in the async-write (generic secondary-write drain) workclass.

    Subclass this instead of ``TaskProtocol`` to opt a drainer into the
    placement rule in ``dynastore.modules.tasks.execution.offload_required``.
    Carries no behaviour of its own — ``is_async_write_workclass`` is the
    only contract.
    """

    is_async_write_workclass: ClassVar[bool] = True
