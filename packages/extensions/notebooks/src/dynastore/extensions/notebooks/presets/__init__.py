#    Copyright 2026 FAO — Apache 2.0; see ../../LICENSE.

"""Notebooks extension preset — auto-register on import.

The contributor lives inside the preset, not on the service. Services
don't mutate platform IAM state; presets do.
"""

from dynastore.extensions.notebooks.policies import (
    notebooks_policies,
    notebooks_role_bindings,
)
from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _NotebooksPolicyContributor:
    def get_policies(self):
        return notebooks_policies()

    def get_role_bindings(self):
        return notebooks_role_bindings()


register_preset(PolicyContributorPreset(
    name="notebooks_enable",
    description="Notebooks extension IAM policies + anonymous read access to platform notebooks",
    keywords=("iam", "notebooks", "platform"),
    contributor_factory=_NotebooksPolicyContributor,
))
