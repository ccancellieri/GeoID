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

import contextlib
from contextvars import ContextVar
from typing import Optional

_in_task_run_var: ContextVar[bool] = ContextVar("in_task_run", default=False)
_task_run_catalog_var: ContextVar[Optional[str]] = ContextVar(
    "task_run_catalog", default=None,
)


def in_task_run() -> bool:
    return _in_task_run_var.get()


def current_task_catalog() -> Optional[str]:
    """Catalog id the currently-running task run declared ownership of.

    ``None`` when the enclosing :func:`task_run_scope` was entered without a
    catalog — a cross-tenant/global task, or a call site that predates this
    parameter (e.g. the Cloud Run Job entrypoint). Callers gating in-run
    behaviour on this (#2716) must treat ``None`` as "no restriction known"
    rather than "restricted to nothing".
    """
    return _task_run_catalog_var.get()


@contextlib.contextmanager
def task_run_scope(catalog: Optional[str] = None):
    """Mark the enclosing coroutine tree as executing inside a task run.

    ``catalog``, when supplied, records which catalog this task run owns
    (see :func:`current_task_catalog`) so downstream code can scope
    in-run behaviour (e.g. index-dispatch write absorption, #2716) to the
    task's own catalog instead of applying it unconditionally.
    """
    token = _in_task_run_var.set(True)
    catalog_token = _task_run_catalog_var.set(catalog)
    try:
        yield
    finally:
        _task_run_catalog_var.reset(catalog_token)
        _in_task_run_var.reset(token)
