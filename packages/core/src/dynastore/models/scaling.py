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

"""Autoscaling signal model.

A :class:`ScalingSignal` is a single normalized observation contributed by
any component in the system (a storage driver, a DB monitor, a future
queue-depth probe, ...) via ``ScalingSignalProtocol``. The autoscaling
control loop aggregates these into a desired ``min_instances`` value —
see ``dynastore.modules.scaling.aggregator``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ScalingSignal(BaseModel):
    """A single normalized autoscaling observation.

    ``value`` is always normalized to ``[0, 1]`` so signals from different
    sources (connection-pool saturation, DB connection pressure, future
    queue-depth ratios, ...) are directly comparable by the aggregator.
    """

    source: str = Field(description="Identifier of the contributing component, e.g. 'duckdb_pool'.")
    metric: str = Field(description="Name of the observed metric, e.g. 'pool_saturation'.")
    value: float = Field(ge=0.0, le=1.0, description="Normalized observation in [0, 1].")
    scope: Literal["instance", "global"] = Field(
        description=(
            "'instance' — meaningful only for the reporting pod (e.g. its own "
            "connection-pool saturation). 'global' — meaningful fleet-wide "
            "(e.g. DB connection pressure, which is the same for every pod)."
        )
    )
    ts: float = Field(description="Unix timestamp (seconds) the observation was taken.")
