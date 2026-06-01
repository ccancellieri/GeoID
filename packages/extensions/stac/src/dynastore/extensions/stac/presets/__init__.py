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

"""STAC extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.stac.policies import stac_policies, stac_role_bindings

OGCServiceMixin.register_ogc_preset(
    name="stac_enable",
    description="STAC extension IAM policies + anonymous read access",
    keywords=("iam", "stac", "platform"),
    policies_factory=stac_policies,
    role_bindings_factory=stac_role_bindings,
)
