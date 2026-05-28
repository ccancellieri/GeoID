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

"""Logs extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _LogsPolicyContributor:
    def get_policies(self):
        from dynastore.extensions.logs.log_extension import (
            _logs_dashboard_policy,
            _logs_per_catalog_policy,
            _logs_system_policy,
        )
        return [
            _logs_system_policy(),
            _logs_dashboard_policy(),
            _logs_per_catalog_policy(),
        ]

    def get_role_bindings(self):
        from dynastore.extensions.logs.log_extension import (
            _logs_dashboard_role_binding,
            _logs_per_catalog_role_bindings,
            _logs_system_role_binding,
        )
        return [
            _logs_system_role_binding(),
            _logs_dashboard_role_binding(),
            *_logs_per_catalog_role_bindings(),
        ]


register_preset(PolicyContributorPreset(
    name="logs_enable",
    description="Logs extension IAM policies; system logs + dashboard + per-catalog access",
    keywords=("iam", "logs", "platform"),
    contributor_factory=_LogsPolicyContributor,
))
