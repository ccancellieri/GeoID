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

"""IAM extension presets — auto-register on import.

Importing this module registers ``iam_baseline`` into the global preset
registry. The IAM extension's ``__init__`` ensures this module is imported
at extension load time so the preset is discoverable via
``GET /admin/presets`` as soon as the extension is installed.

Only registration side-effects happen here — no DB I/O, no state.
"""

from dynastore.modules.storage.presets.registry import register_preset

from .iam_baseline import IamBaseline, IamBaselineParams

register_preset(IamBaseline())

__all__ = [
    "IamBaseline",
    "IamBaselineParams",
]
