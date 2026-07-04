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

"""``common_dimensions`` preset — lightweight registration (dynastore#2601).

Registration-only: this module imports nothing from the ``dimensions``
extension or ``ogc_dimensions`` — it only enqueues the idempotent
``dimensions_materialize`` OGC Process (also in core; see ``task.py``),
which does the provider-dependent work itself. This lets any process that
serves ``/configs/presets`` (the lightweight ``configs`` extension) list and
apply ``common_dimensions`` without installing the full ``dimensions``
extension and its ``ogc_dimensions`` dependency — that dependency is only
needed by the process that actually runs the task.

Previously this preset also shipped a ``DataContributor`` (``get_data()``)
that synchronously pre-built the same RECORDS-collection skeletons the task
creates moments later (see ``materialize_dimension``'s create-if-missing
block in the dimensions extension's ``dimensions_extension.py``) — redundant
work that also required importing the provider stack at apply time. Dropped
here; the task is solely responsible for creating and filling the
collections. Registered via the ``dynastore.presets`` entry-point group (see
``registry.load_preset_entry_points``) rather than a direct package import,
so it is discovered independently of the ``dimensions`` extension.
"""
from __future__ import annotations

from typing import Iterable

from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.preset import TaskSeed
from dynastore.modules.storage.presets.registry import register_preset


class _CommonDimensionsContributor:
    """Task-only contributor for the standard reusable dimensions.

    Ships only ``get_tasks()``: the preset itself does no data seeding, it
    just triggers the ``dimensions_materialize`` job, which creates (if
    missing) and fills the RECORDS collections under the shared
    ``_dimensions_`` catalog.
    """

    def get_tasks(self) -> Iterable[TaskSeed]:
        # Trigger the idempotent materialise job. Empty inputs => materialise
        # every registered dimension that has drifted. dedup_key collapses
        # repeated applies onto one in-flight job.
        yield TaskSeed(
            process_id="dimensions_materialize",
            inputs={},
            async_mode=True,
            dedup_key="preset:common_dimensions:dimensions_materialize",
        )


register_preset(MultiContributorPreset(
    name="common_dimensions",
    description=(
        "Trigger the dimensions_materialize job, which registers the "
        "standard reusable dimensions (temporal-dekadal, pentadal, "
        "admin-boundaries, indicator-tree, forestry-species, "
        "elevation-bands) as RECORDS collections with cube:dimensions "
        "metadata in the _dimensions_ catalog and populates their members. "
        "Returns a job reference in the applied descriptor for polling."
    ),
    keywords=("dimensions", "data", "platform", "stac", "datacube"),
    contributors_factory=lambda: [_CommonDimensionsContributor()],
))
