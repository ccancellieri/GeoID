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

"""Search extension preset — auto-register on import."""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset


class _SearchPolicyContributor:
    def get_policies(self):
        from dynastore.models.protocols.policies import Policy
        return [
            Policy(
                id="search_reindex_admin",
                description="Grants access to the bulk reindex trigger endpoints (admin only).",
                actions=["POST"],
                resources=[
                    "/search/reindex",
                    "/search/reindex/",
                    "/search/reindex/.*",
                ],
                effect="ALLOW",
            ),
            Policy(
                id="search_envelope_backfill_sysadmin",
                description=(
                    "Grants access to the envelope-attrs backfill endpoint (sysadmin only). "
                    "This endpoint stamps _attrs onto pre-existing ES envelope-driver docs "
                    "written before #1441 shipped."
                ),
                actions=["POST"],
                resources=[
                    "/search/catalogs/.*/collections/.*/backfill-envelope-attrs",
                ],
                effect="ALLOW",
            ),
        ]

    def get_role_bindings(self):
        from dynastore.models.protocols.policies import Role
        from dynastore.models.protocols.authorization import IamRolesConfig
        cfg = IamRolesConfig()
        return [
            Role(name=cfg.sysadmin_role_name, policies=["search_reindex_admin"]),
            Role(name=cfg.admin_role_name, policies=["search_reindex_admin"]),
            Role(
                name=cfg.sysadmin_role_name,
                policies=["search_envelope_backfill_sysadmin"],
            ),
        ]


def _make_contributor() -> _SearchPolicyContributor:
    return _SearchPolicyContributor()


register_preset(PolicyContributorPreset(
    name="search_enable",
    description="Search extension IAM policies: admin-only reindex and sysadmin-only backfill",
    keywords=("iam", "search", "platform"),
    contributor_factory=_make_contributor,
))
