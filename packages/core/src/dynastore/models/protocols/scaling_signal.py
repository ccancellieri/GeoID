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

"""Autoscaling-signal contribution protocol.

Any component that has an opinion about load — a storage driver's
connection-pool saturation, a DB contention monitor's connection pressure,
a future queue-depth probe — implements this to feed the scaling control
loop. The pattern mirrors ``ConformanceContributor``: implementors set the
method directly (structural typing), no base class required.
"""

from typing import List, Protocol, runtime_checkable

from dynastore.models.scaling import ScalingSignal


@runtime_checkable
class ScalingSignalProtocol(Protocol):
    """Producer of autoscaling signals.

    ``scaling_signals()`` is synchronous and must be cheap — it is called
    every publish cadence on every pod (for ``scope="instance"`` signals)
    and reads only already-computed state. It must never raise; an
    implementation with nothing to report returns an empty list.
    """

    def scaling_signals(self) -> List[ScalingSignal]: ...
