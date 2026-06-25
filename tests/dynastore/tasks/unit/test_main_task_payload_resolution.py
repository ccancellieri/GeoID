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

"""``main_task._resolve_payload_model`` resolves PEP 563 string annotations.

The Cloud Run Job entrypoint introspects a task's ``run(self, payload: ...)``
annotation to find the pydantic model to validate the incoming payload against.
A task module that uses ``from __future__ import annotations`` (PEP 563)
stringizes every annotation, so reading the raw ``__annotations__`` dict yields
the *string* ``"SomePayload"`` instead of the class — and ``.model_validate`` /
``.__name__`` then raise ``AttributeError``, aborting every Cloud Run Job
execution for that task.  ``_resolve_payload_model`` must resolve the string to
the live class.

This module itself uses ``from __future__ import annotations`` so the dummy
task below reproduces the exact stringized-annotation condition.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from dynastore.main_task import _resolve_payload_model


class _SamplePayload(BaseModel):
    value: int


class _StringizedTask:
    # Under this module's `from __future__ import annotations`, this annotation
    # is stored as the string "_SamplePayload" — the regression condition.
    async def run(self, payload: _SamplePayload) -> dict:
        return {}


class _NoPayloadTask:
    async def run(self, other: int) -> dict:
        return {}


def test_resolves_stringized_annotation_to_live_class():
    model = _resolve_payload_model(_StringizedTask(), "sample")
    # Raw __annotations__ is the bug condition: a plain str.
    assert isinstance(_StringizedTask.run.__annotations__["payload"], str)
    # The resolver must return the live class, usable for validation.
    assert model is _SamplePayload
    assert hasattr(model, "model_validate")
    assert hasattr(model, "__name__")
    validated = model.model_validate({"value": 7})
    assert validated.value == 7


def test_missing_payload_annotation_raises_typeerror():
    with pytest.raises(TypeError, match="without a 'payload' type annotation"):
        _resolve_payload_model(_NoPayloadTask(), "no_payload")


def test_missing_run_method_raises_typeerror():
    with pytest.raises(TypeError, match="no `run` method"):
        _resolve_payload_model(object(), "no_run")
