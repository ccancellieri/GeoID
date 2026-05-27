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

"""Tiles extension preset — auto-register on import."""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _TilesPolicyContributor:
    def get_policies(self):
        from dynastore.models.protocols.policies import Policy
        return [
            Policy(
                id="tiles_public_access",
                description="Allows anonymous access to tiles endpoints.",
                actions=["GET", "OPTIONS"],
                resources=[
                    "/tiles.*",
                    "/tiles/.*",
                ],
                effect="ALLOW",
            ),
        ]

    def get_role_bindings(self):
        from dynastore.models.protocols.policies import Role
        from dynastore.models.protocols.authorization import IamRolesConfig
        return [
            Role(
                name=IamRolesConfig().anonymous_role_name,
                description="Anonymous user with limited access.",
                policies=["tiles_public_access"],
            ),
        ]


def _make_contributor() -> _TilesPolicyContributor:
    return _TilesPolicyContributor()


register_preset(PolicyContributorPreset(
    name="tiles_enable",
    description="Tiles extension public-access policy",
    keywords=("iam", "tiles", "platform"),
    contributor_factory=_make_contributor,
))
