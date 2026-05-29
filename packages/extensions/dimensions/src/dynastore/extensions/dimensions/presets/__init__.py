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

"""``common_dimensions`` preset — the dimensions provisioner as a contributor.

Registers the standard reusable dimensions as RECORDS collections with
``cube:dimensions`` metadata in the shared ``_dimensions_`` catalog, then
triggers the materialisation job.  Serves dynastore#307 (dimension registry as
catalogued RECORDS) and complements dynastore#329 (config-driven dimension
registry); gap use-case dynastore#277, roadmap dynastore#266.

Two-phase apply (sync-light skeletons + async-heavy fill):

  1. **Skeletons (synchronous, fast).**  A ``DataContributor`` yields one
     ``DataSeed`` per registered dimension — a RECORDS collection carrying the
     correct ``cube:dimensions`` / ``provider`` metadata but no member
     ``items``.  STAC / datacube clients see the dimension immediately, before
     any members are written.

  2. **Fill (asynchronous, heavy).**  A ``TaskContributor`` then triggers the
     ``dimensions_materialize`` OGC Process, which populates the member records
     (3 600+ dekadal, 7 200+ pentadal, …).  The job runs in the background; the
     preset's ``AppliedDescriptor`` records its job id so callers can poll the
     job's status through the normal task-status endpoint.  The preset itself
     stays synchronous and completes ``applied`` once the skeletons exist and
     the job is enqueued — it does not wait for materialisation.

The materialise job is idempotent (per-dimension ``cube:dimensions`` equality
check), so triggering it on every apply is safe; ``dedup_key`` collapses
repeated applies onto a single in-flight job.

Revoke removes only what apply created and never deletes a dimension collection
that already holds materialised members (see the data-seed revoke guard in
``multi_contributor``).  The shared ``_dimensions_`` catalog
(``manage_catalog=False``) is never deleted.

Fail-fast: if the dimensions extension / its OGC dimension providers are not
available, ``get_data()`` raises ``RuntimeError`` rather than silently
registering zero dimensions.
"""
from __future__ import annotations

from typing import Iterable

from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID
from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.registry import register_preset
from dynastore.modules.storage.presets.preset import DataSeed, TaskSeed


# ``catalog_data`` for the shared ``_dimensions_`` catalog. Used only when the
# catalog is absent (idempotent create-if-absent); ``manage_catalog=False`` keeps
# revoke from ever deleting it. Defined here so a cold platform gets a properly
# titled/described catalog instead of a bare id (dynastore#307).
_DIMENSIONS_CATALOG_DATA = {
    "id": DIMENSIONS_CATALOG_ID,
    "title": {"en": "Reusable Dimensions"},
    "description": {
        "en": (
            "Shared catalog of reusable OGC datacube dimensions (temporal, "
            "categorical, spatial) exposed as RECORDS collections with "
            "cube:dimensions metadata. Members are materialised by the "
            "dimensions_materialize process."
        ),
    },
    "keywords": ["dimensions", "datacube", "stac", "records"],
}


class _CommonDimensionsContributor:
    """Data + task contributor for the standard reusable dimensions.

    ``get_data()`` calls ``get_registered_dimensions()`` lazily on every
    invocation so this module stays import-light and worker-safe: the
    OGC dimension providers are imported only when a preset apply / dry_run
    actually executes, not at module load time.

    Each data seed:
    - targets ``catalog_id=DIMENSIONS_CATALOG_ID`` with ``manage_catalog=False``
      because ``_dimensions_`` is a shared platform catalog that must survive an
      individual preset revoke;
    - carries ``layer_config={"collection_type": "RECORDS"}`` and
      ``extra_metadata`` with ``provider``, ``cube:dimensions`` and ``itemType``
      — mirroring the collection dict built by ``materialize_dimension``, but
      without the member ``items`` payload (``items=()``).

    ``get_tasks()`` yields a single ``TaskSeed`` that triggers the
    ``dimensions_materialize`` OGC Process to fill the members after the
    skeletons are registered.
    """

    def get_data(self) -> Iterable[DataSeed]:
        try:
            from dynastore.extensions.dimensions.dimensions_extension import (
                _build_cube_dimensions,
                _build_provider,
                _infer_dim_type,
                get_registered_dimensions,
            )
        except ImportError as exc:  # fail fast — extension/providers unavailable
            raise RuntimeError(
                "common_dimensions requires the dimensions extension and its OGC "
                "dimension providers, which are not importable in this context. "
                "Ensure the dimensions extension is in the deployment SCOPE before "
                "applying this preset."
            ) from exc

        registered = get_registered_dimensions()
        if not registered:
            raise RuntimeError(
                "common_dimensions: no dimensions are registered. Ensure the OGC "
                "dimension providers are installed and registered before applying "
                "this preset (fail-fast rather than registering zero dimensions)."
            )

        for dim_name, dim_config in registered.items():
            generator = dim_config.provider
            dim_type = _infer_dim_type(generator)
            cube_dimensions = _build_cube_dimensions(dim_name, dim_type, generator)
            provider = _build_provider(generator)

            collection_data = {
                "id": dim_name,
                "title": dim_name.replace("-", " ").title(),
                "description": dim_config.description,
                "layer_config": {"collection_type": "RECORDS"},
                "extra_metadata": {
                    "provider": provider,
                    "cube:dimensions": cube_dimensions,
                    "itemType": "record",
                },
            }

            yield DataSeed(
                catalog_id=DIMENSIONS_CATALOG_ID,
                collection_id=dim_name,
                catalog_data=_DIMENSIONS_CATALOG_DATA,
                collection_data=collection_data,
                items=(),
                manage_catalog=False,
                manage_collection=True,
            )

    def get_tasks(self) -> Iterable[TaskSeed]:
        # Trigger the idempotent materialise job after the skeletons exist.
        # Empty inputs => materialise every registered dimension that has
        # drifted. dedup_key collapses repeated applies onto one in-flight job.
        yield TaskSeed(
            process_id="dimensions_materialize",
            inputs={},
            async_mode=True,
            dedup_key="preset:common_dimensions:dimensions_materialize",
        )


register_preset(MultiContributorPreset(
    name="common_dimensions",
    description=(
        "Register the standard reusable dimensions (temporal-dekadal, "
        "pentadal, admin-boundaries, indicator-tree, forestry-species, "
        "elevation-bands) as RECORDS collections with cube:dimensions "
        "metadata in the _dimensions_ catalog, then trigger the "
        "dimensions_materialize job to populate their members. Returns a job "
        "reference in the applied descriptor for polling."
    ),
    keywords=("dimensions", "data", "platform", "stac", "datacube"),
    contributors_factory=lambda: [_CommonDimensionsContributor()],
))
