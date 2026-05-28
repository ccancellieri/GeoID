#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

"""Configs extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.configs.policies import (
    configs_policies,
    configs_role_bindings,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _ConfigsPolicyContributor:
    def get_policies(self):
        return configs_policies()

    def get_role_bindings(self):
        return configs_role_bindings()


register_preset(PolicyContributorPreset(
    name="configs_enable",
    description="Configuration management extension IAM policies (sysadmin-tier access)",
    keywords=("iam", "configs", "platform"),
    contributor_factory=_ConfigsPolicyContributor,
))
