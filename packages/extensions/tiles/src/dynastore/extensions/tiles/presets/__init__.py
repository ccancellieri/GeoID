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

from dynastore.extensions.ogc_base import OGCServiceMixin


def _tiles_policies():
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


def _tiles_role_bindings():
    from dynastore.models.protocols.policies import Role
    from dynastore.models.protocols.authorization import IamRolesConfig
    return [
        Role(
            name=IamRolesConfig().anonymous_role_name,
            description="Anonymous user with limited access.",
            policies=["tiles_public_access"],
        ),
    ]


OGCServiceMixin.register_ogc_preset(
    name="tiles_enable",
    description="Tiles extension public-access policy",
    keywords=("iam", "tiles", "platform"),
    policies_factory=_tiles_policies,
    role_bindings_factory=_tiles_role_bindings,
)
