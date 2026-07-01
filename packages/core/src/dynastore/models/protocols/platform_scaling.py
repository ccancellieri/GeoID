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

"""Platform autoscaling actuation protocol.

The cloud-agnostic seam between the scaling control loop's decision
(``compute_desired_min``) and the platform-specific lever that applies it
(e.g. Cloud Run ``scaling.min_instance_count``). Kept minimal — one method
to set, one to read back — because only one provider (GCP) exists today;
a second implementation should extend this protocol only when it actually
needs to, not speculatively.
"""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class PlatformScalingProtocol(Protocol):
    """Actuator for the platform's minimum-instance-count lever."""

    async def set_min_instances(self, n: int) -> None:
        """Apply *n* as the platform's minimum instance count.

        Best-effort: implementations must not raise on transient platform
        API errors — log and return so the caller's tick is not aborted.
        """
        ...

    async def get_min_instances(self) -> Optional[int]:
        """Return the platform's current minimum instance count, or ``None``
        when it cannot be determined (no credentials, not on this platform,
        transient API error)."""
        ...
