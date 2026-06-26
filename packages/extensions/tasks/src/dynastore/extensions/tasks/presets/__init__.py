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

"""Tasks extension preset — auto-register on import.

The contributor lives inside the preset, not on the service.  Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.tasks.policies import (
    tasks_policies,
    tasks_role_bindings,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _TasksPolicyContributor:
    def get_policies(self):
        return tasks_policies()

    def get_role_bindings(self):
        return tasks_role_bindings()


register_preset(PolicyContributorPreset(
    name="tasks_enable",
    description=(
        "Tasks extension IAM policies: read surface for catalog members "
        "(tasks_read), system-scope read for sysadmin (tasks_system_read), "
        "catalog/collection spawn and DLQ for catalog-admin (tasks_admin), "
        "and system-scope mutations for sysadmin (tasks_system_admin)."
    ),
    keywords=("iam", "tasks", "platform"),
    contributor_factory=_TasksPolicyContributor,
))
