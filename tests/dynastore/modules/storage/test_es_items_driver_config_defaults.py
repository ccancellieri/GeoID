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

"""ES items driver configs simplify geometry by default (revised 2026-06-18)."""
import pytest
from pydantic import ValidationError

from dynastore.modules.storage.driver_config import (
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsElasticsearchEnvelopeDriverConfig,
)

_CONFIGS = [
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsElasticsearchEnvelopeDriverConfig,
]


@pytest.mark.parametrize("cls", _CONFIGS)
def test_simplify_geometry_defaults_on(cls):
    assert cls().simplify_geometry is True


@pytest.mark.parametrize("cls", _CONFIGS)
def test_simplify_target_bytes_defaults_none(cls):
    assert cls().simplify_target_bytes is None


@pytest.mark.parametrize("cls", _CONFIGS)
def test_explicit_disable_is_respected(cls):
    assert cls(simplify_geometry=False).simplify_geometry is False


@pytest.mark.parametrize("cls", _CONFIGS)
def test_simplify_target_bytes_rejects_zero_and_negative(cls):
    # ge=1: a budget of 0 or a negative byte count is meaningless.
    with pytest.raises(ValidationError):
        cls(simplify_target_bytes=0)
    with pytest.raises(ValidationError):
        cls(simplify_target_bytes=-1)
