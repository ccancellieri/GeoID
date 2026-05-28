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

"""Stats extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _StatsPolicyContributor:
    def get_policies(self):
        from dynastore.extensions.stats.extension import _stats_policy
        return [_stats_policy()]

    def get_role_bindings(self):
        from dynastore.extensions.stats.extension import _stats_role_binding
        return [_stats_role_binding()]


register_preset(PolicyContributorPreset(
    name="stats_enable",
    description="Stats extension IAM policies; sysadmin-only platform stats access",
    keywords=("iam", "stats", "platform"),
    contributor_factory=_StatsPolicyContributor,
))
