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

"""Auth extension preset — auto-register on import."""

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
)
from dynastore.modules.storage.presets.registry import register_preset
from dynastore.extensions.auth.policies import _AuthPolicyContributor


def _make_contributor() -> _AuthPolicyContributor:
    return _AuthPolicyContributor()


register_preset(PolicyContributorPreset(
    name="auth_enable",
    description="Auth extension public-access policy for authentication endpoints",
    keywords=("iam", "auth", "platform"),
    contributor_factory=_make_contributor,
))
