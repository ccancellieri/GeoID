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

"""STAC extension preset — auto-register on import.

Registers ``stac_enable`` into the global preset registry.  The STAC
extension's ``__init__`` imports this subpackage so the preset is
discoverable via ``GET /admin/presets`` as soon as the extension is
installed.  No DB I/O or side-effects happen at registration time.

PR-3 of umbrella #1412.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


def _make_stac() -> object:
    from dynastore.extensions.stac.stac_service import STACService
    return STACService.__new__(STACService)


register_preset(PolicyContributorPreset(
    name="stac_enable",
    description="STAC extension IAM policies + anonymous read access",
    keywords=("iam", "stac", "platform"),
    contributor_factory=_make_stac,
))
