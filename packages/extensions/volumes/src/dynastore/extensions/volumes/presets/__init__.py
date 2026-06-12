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

"""3D GeoVolumes extension preset — auto-register on import.

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


class _VolumesPolicyContributor:
    def get_policies(self):
        return [ogc_public_access_policy("volumes")]

    def get_role_bindings(self):
        return [ogc_anonymous_role_binding("volumes")]


register_preset(PolicyContributorPreset(
    name="volumes_enable",
    description="OGC 3D GeoVolumes extension IAM policies + anonymous read access",
    keywords=("iam", "volumes", "platform"),
    contributor_factory=_VolumesPolicyContributor,
))

# Import demo presets to trigger their self-registration on package import.
# volumes_demo imports the other two, so it must come after them.
from dynastore.extensions.volumes.presets import cityjson_demo as _cityjson_demo  # noqa: E402, F401
from dynastore.extensions.volumes.presets import tiles3d_samples as _tiles3d_samples  # noqa: E402, F401
from dynastore.extensions.volumes.presets import volumes_demo as _volumes_demo  # noqa: E402, F401
