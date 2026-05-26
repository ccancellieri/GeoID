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

"""Web extension preset — auto-register on import.

PR-3 of umbrella #1412.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


def _make_web() -> object:
    from dynastore.extensions.web.web import Web
    return Web.__new__(Web)


register_preset(PolicyContributorPreset(
    name="web_enable",
    description="Web extension IAM policies; public UI access + sysadmin + dashboard scoping",
    keywords=("iam", "web", "platform"),
    contributor_factory=_make_web,
))
