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

"""
Driver-role shared types — DriverSla.

Per-driver SLA wrapper used mainly on TRANSFORM / INDEX / BACKUP driver
entries.  Mandatory on TRANSFORM drivers so the hot path is never
unboundedly delayed; optional on INDEX / BACKUP to govern async
propagation timeouts.  Kept separate from ``FailurePolicy`` so it can be
attached per-entry rather than per-class.

Per-driver "which slice of the payload" identification was previously
done via a ``MetadataDomain`` StrEnum + ``ClassVar`` ClassVar on each
driver.  That coupling was removed when STAC was promoted to its own
module — STAC drivers now satisfy ``StacCollectionEntityStoreCapability``
(a ``@runtime_checkable Protocol`` owned by ``extensions/stac/protocols.py``)
and consumers dispatch via ``isinstance``.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class DriverSla(BaseModel):
    """Per-driver SLA governing TRANSFORM / INDEX / BACKUP execution.

    Mandatory on TRANSFORM drivers — a transform without an SLA can quietly
    tax the hot path.  Optional on INDEX / BACKUP where it governs async
    propagation timeouts rather than user-facing latency.

    Attributes:
        timeout_ms: Hard timeout on a single invocation.  Default 500ms —
            keeps TRANSFORM round-trips bounded on read-through.
        on_timeout: Behaviour when the timeout fires.
            - ``fail``: raise; caller sees an error.
            - ``degrade``: return whatever's available so far; caller sees
              the untransformed envelope + ``X-Transform-Degraded`` header.
            - ``skip``: silently skip this driver and continue the chain.
        required: If True, a timeout / failure escalates to a 504 on
            user-facing read paths regardless of ``on_timeout``.  Use
            sparingly — breaks the "default-fast, transform-optional"
            invariant when true.
    """

    timeout_ms: int = Field(
        default=500,
        ge=1,
        description="Hard timeout on a single invocation, in milliseconds.",
    )
    on_timeout: Literal["fail", "degrade", "skip"] = Field(
        default="degrade",
        description=(
            "Behaviour on timeout: 'fail' (raise), 'degrade' (return partial), "
            "'skip' (continue silently)."
        ),
    )
    required: Optional[bool] = Field(
        default=False,
        description=(
            "If True, timeout / failure escalates to 504 on user-facing paths "
            "even when on_timeout=degrade.  Breaks default-fast invariant; use sparingly."
        ),
    )
