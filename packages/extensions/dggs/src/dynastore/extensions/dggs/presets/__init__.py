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

"""DGGS extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _DGGSPolicyContributor:
    def get_policies(self):
        return [ogc_public_access_policy("dggs")]

    def get_role_bindings(self):
        return [ogc_anonymous_role_binding("dggs")]


register_preset(PolicyContributorPreset(
    name="dggs_enable",
    description="OGC DGGS extension IAM policies + anonymous read access",
    keywords=("iam", "dggs", "platform"),
    contributor_factory=_DGGSPolicyContributor,
))
