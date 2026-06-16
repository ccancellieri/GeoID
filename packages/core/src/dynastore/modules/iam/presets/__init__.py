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

"""IAM presets — platform-tier presets that operate on IAM data.

Imported by ``dynastore.modules.iam.module`` at module load time so
registrations happen before ``GET /configs/presets`` is served.
"""
from dynastore.modules.storage.presets.registry import register_preset

from .auth_bootstrap import AuthBootstrapPreset, AuthBootstrapParams
from .default_roles_baseline import DefaultRolesBaseline
from .public_access_baseline import PublicAccessBaseline

register_preset(AuthBootstrapPreset())
register_preset(DefaultRolesBaseline())
register_preset(PublicAccessBaseline())

__all__ = [
    "AuthBootstrapPreset",
    "AuthBootstrapParams",
    "DefaultRolesBaseline",
    "PublicAccessBaseline",
]
