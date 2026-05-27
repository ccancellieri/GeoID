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

"""Coverages extension preset — auto-register on import.

PR-3 of umbrella #1412.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


def _make_coverages() -> object:
    from dynastore.extensions.coverages.coverages_service import CoveragesService
    return CoveragesService.__new__(CoveragesService)


register_preset(PolicyContributorPreset(
    name="coverages_enable",
    description="OGC Coverages extension IAM policies + anonymous read access",
    keywords=("iam", "coverages", "platform"),
    contributor_factory=_make_coverages,
))
