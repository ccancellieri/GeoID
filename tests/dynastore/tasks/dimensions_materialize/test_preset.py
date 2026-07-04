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

"""DB-free unit tests for the ``common_dimensions`` preset (dynastore#2601).

The preset now lives entirely in core, alongside the ``dimensions_materialize``
task it triggers, and ships only a ``TaskContributor`` — no ``DataContributor``,
no import of the ``dimensions`` extension or ``ogc_dimensions``. No DB, no
providers, no network access required.  DO NOT run under the repo-root
conftest (it wipes the local ``gis_dev`` DB during collection).
"""
from __future__ import annotations

from dynastore.modules.storage.presets.preset import TaskSeed
from dynastore.tasks.dimensions_materialize.preset import _CommonDimensionsContributor


def test_get_tasks_triggers_dimensions_materialize() -> None:
    """get_tasks() yields a single TaskSeed that triggers the
    dimensions_materialize OGC Process (the provider-dependent fill job)."""
    tasks = list(_CommonDimensionsContributor().get_tasks())

    assert len(tasks) == 1
    assert isinstance(tasks[0], TaskSeed)
    assert tasks[0].process_id == "dimensions_materialize"
    assert tasks[0].async_mode is True
    # A dedup_key collapses repeated applies onto a single in-flight job.
    assert tasks[0].dedup_key


def test_contributor_has_no_data_role() -> None:
    """The contributor must not expose get_data() — registration is
    lightweight and provider-free; all seeding happens in the task."""
    assert not hasattr(_CommonDimensionsContributor(), "get_data")


def test_common_dimensions_preset_is_registered() -> None:
    """Importing this module must register ``common_dimensions`` in the
    global preset registry (side-effect import contract)."""
    import dynastore.tasks.dimensions_materialize.preset  # noqa: F401

    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("common_dimensions")
    assert preset is not None
    assert preset.name == "common_dimensions"
    assert "dimensions" in preset.keywords
    assert "datacube" in preset.keywords
