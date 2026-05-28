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

"""Events extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.events.policies import (
    events_policies,
    events_role_bindings,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _EventsPolicyContributor:
    def get_policies(self):
        return events_policies()

    def get_role_bindings(self):
        return events_role_bindings()


register_preset(PolicyContributorPreset(
    name="events_enable",
    description="Events extension IAM policies; sysadmin platform surface + catalog-member per-catalog access",
    keywords=("iam", "events", "platform"),
    contributor_factory=_EventsPolicyContributor,
))
