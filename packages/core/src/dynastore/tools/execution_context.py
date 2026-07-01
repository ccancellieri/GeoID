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

_in_task_run_var: ContextVar[bool] = ContextVar("in_task_run", default=False)


def in_task_run() -> bool:
    return _in_task_run_var.get()


@contextlib.contextmanager
def task_run_scope():
    token = _in_task_run_var.set(True)
    try:
        yield
    finally:
        _in_task_run_var.reset(token)
