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

"""Auth extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _AuthPolicyContributor:
    def get_policies(self):
        from dynastore.models.protocols.policies import Policy
        return [
            Policy(
                id="auth_extension_public",
                description="Public access to authentication endpoints.",
                actions=["GET", "POST"],
                resources=[r"/auth/.*", r"/web/auth/.*"],
                effect="ALLOW",
            ),
        ]

    def get_role_bindings(self):
        from dynastore.models.protocols.authorization import IamRolesConfig
        from dynastore.models.protocols.policies import Role
        return [
            Role(
                name=IamRolesConfig().anonymous_role_name,
                policies=["auth_extension_public"],
            ),
        ]


register_preset(PolicyContributorPreset(
    name="auth_enable",
    description="Auth extension public-access policy for authentication endpoints",
    keywords=("iam", "auth", "platform"),
    contributor_factory=_AuthPolicyContributor,
))
