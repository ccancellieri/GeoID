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

"""Domain exceptions for the tasks module.

These are framework-free exceptions raised by the tasks runtime.
The extension boundary (extensions/tools/exception_handlers.py) maps
them to HTTP status codes via registered handlers.
"""

__all__ = ["JobLockedError", "JobStateConflictError", "UnclaimableRequeueError"]


class JobLockedError(Exception):
    """Raised when a job is locked and the requested mutation is not allowed.

    A job becomes locked as soon as it leaves the CREATED state (i.e. it
    has been submitted for execution or is already running).  Callers that
    attempt to update its inputs while locked receive HTTP 423 Locked.
    """


class JobStateConflictError(Exception):
    """Raised when a job state transition is invalid.

    The current status of the job does not allow the requested operation
    (e.g. starting an already-running job, or dismissing a terminal one).
    Maps to HTTP 409 Conflict at the extension boundary.
    """


class UnclaimableRequeueError(Exception):
    """Raised by ``requeue_dead_letter_task`` when ``reset_retries=False``
    would produce a PENDING row ``claim_batch`` can never claim.

    ``claim_batch`` only claims rows with ``retry_count < hard_cap``; a
    requeue that keeps the count as-is while it is already at/above that
    cap would create a permanently unclaimable zombie row (the reaper only
    touches ACTIVE rows, so it would never DLQ it again either). Maps to
    HTTP 409 Conflict at the extension boundary.
    """
