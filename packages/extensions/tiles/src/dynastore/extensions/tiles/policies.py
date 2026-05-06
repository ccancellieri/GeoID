#    Copyright 2025 FAO
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

import logging
from dynastore.models.protocols.authorization import DefaultRole
from dynastore.models.protocols.policies import Policy, Role
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def register_tiles_policies():
    """Register Tiles public-access policy and anonymous role via PermissionProtocol."""
    from dynastore.models.protocols.policies import PermissionProtocol

    pm = get_protocol(PermissionProtocol)
    if not pm:
        logger.warning("PermissionProtocol not available; tiles policies not registered.")
        return

    tiles_policy = Policy(
        id="tiles_public_access",
        description="Allows anonymous access to tiles endpoints.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/tiles.*",
            "/tiles/.*",
        ],
        effect="ALLOW",
    )
    pm.register_policy(tiles_policy)
    pm.register_role(Role(
        name=DefaultRole.ANONYMOUS.value,
        description="Anonymous user with limited access.",
        policies=["tiles_public_access"],
    ))

    logger.debug("Tiles policies registered via PermissionProtocol.")
