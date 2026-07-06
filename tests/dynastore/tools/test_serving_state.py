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

"""Unit tests for the process-global draining flag (geoid#2946 / #2924)."""
from __future__ import annotations

import pytest

from dynastore.tools.serving_state import clear_draining, is_draining, set_draining


@pytest.fixture(autouse=True)
def _reset():
    clear_draining()
    yield
    clear_draining()


def test_starts_not_draining() -> None:
    assert is_draining() is False


def test_set_draining_flips_the_flag() -> None:
    set_draining()
    assert is_draining() is True


def test_clear_draining_resets_the_flag() -> None:
    set_draining()
    clear_draining()
    assert is_draining() is False


def test_set_draining_is_idempotent() -> None:
    set_draining()
    set_draining()
    assert is_draining() is True
