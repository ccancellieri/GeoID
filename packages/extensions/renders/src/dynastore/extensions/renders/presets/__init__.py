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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Renders extension preset — public read-access policy for the render tile route."""

from dynastore.extensions.ogc_base import OGCServiceMixin


def _renders_policies():
    from dynastore.models.protocols.policies import Policy
    return [
        Policy(
            id="renders_public_access",
            description="Allows anonymous GET access to the raster render tile endpoint.",
            actions=["GET", "OPTIONS"],
            resources=[
                "/renders.*",
                "/renders/.*",
            ],
            effect="ALLOW",
        ),
    ]


def _renders_role_bindings():
    from dynastore.models.protocols.policies import Role
    from dynastore.models.protocols.authorization import IamRolesConfig
    return [
        Role(
            name=IamRolesConfig().anonymous_role_name,
            description="Anonymous user with limited access.",
            policies=["renders_public_access"],
        ),
    ]


OGCServiceMixin.register_ogc_preset(
    name="renders_enable",
    description="Renders extension public-access policy for COG raster tiles",
    keywords=("iam", "renders", "raster", "platform"),
    policies_factory=_renders_policies,
    role_bindings_factory=_renders_role_bindings,
)
