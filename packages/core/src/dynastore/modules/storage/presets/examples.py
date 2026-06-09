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

"""``PresetExample`` — declarable worked example for a preset.

Presets opt in to self-documentation by declaring ``examples`` as a
``ClassVar[tuple[PresetExample, ...]]`` on their class body.  Each entry
carries the raw ``params`` dict used to exercise that code path.  The
``describe_preset`` helper in ``presets/describe.py`` validates those
params through the preset's ``params_model``, builds a preview bundle
(no DB), and includes the resulting config in the descriptor payload.
"""
from __future__ import annotations

from typing import Any, Dict


class PresetExample:
    """Declarable worked example for a preset.

    Intentionally a plain Python class (not a Pydantic model) so it can
    be assigned as a ``ClassVar`` without triggering Pydantic's field
    collection machinery.

    Attributes:
        name:    Short identifier, e.g. ``"discoverable-geopackage"``.
        summary: One-line human description shown in the descriptor.
        params:  Raw preset-payload dict (matches the preset's
                 ``params_model`` JSON schema).
    """

    __slots__ = ("name", "summary", "params")

    def __init__(self, name: str, summary: str, params: Dict[str, Any]) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "params", params)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("PresetExample is immutable")

    def __repr__(self) -> str:  # pragma: no cover
        return f"PresetExample(name={self.name!r}, summary={self.summary!r})"
