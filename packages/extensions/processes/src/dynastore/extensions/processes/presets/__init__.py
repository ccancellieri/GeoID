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

"""Processes extension preset — auto-register on import.

The contributor lives inside the preset, not on the service.  Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.processes.policies import (
    processes_policies,
    processes_role_bindings,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _ProcessesPolicyContributor:
    def get_policies(self):
        return processes_policies()

    def get_role_bindings(self):
        return processes_role_bindings()


register_preset(PolicyContributorPreset(
    name="processes_enable",
    description=(
        "Processes extension IAM policies: public read for the "
        "platform-scope process registry and job surfaces "
        "(processes_public_read), catalog-member read for the "
        "catalog/collection-scoped surfaces (processes_read), "
        "sysadmin-only platform-scope execution "
        "(processes_system_execute), and catalog-admin execution at "
        "catalog/collection scope (processes_admin)."
    ),
    keywords=("iam", "processes", "platform"),
    contributor_factory=_ProcessesPolicyContributor,
))
