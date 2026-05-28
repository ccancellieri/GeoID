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

"""``common_dimensions`` preset — register standard reusable dimensions as
RECORDS collections with ``cube:dimensions`` metadata in the
``_dimensions_`` catalog.

Serves dynastore#307 (dimension registry as catalogued RECORDS) and
complements dynastore#329 (future config-driven dimension registry).
The gap use-case is dynastore#277; roadmap context at dynastore#266.

Design split (sync-light vs. async-heavy):
  This preset performs *light* registration only — it ensures each
  dimension has a RECORDS collection with the correct ``cube:dimensions``
  and ``provider`` metadata, leaving ``items`` empty.  The *heavy* member
  materialisation (3 600+ dekadal records, 7 200+ pentadal records, …)
  remains the responsibility of the ``dimensions_materialize`` OGC Process
  task, which callers must trigger explicitly.  This split keeps preset
  apply/revoke synchronous and fast (no generator enumeration, no batch
  upserts), while the task handles idempotent large-scale writes at its
  own pace.

  Design doubt: should the preset trigger the ``dimensions_materialize``
  task asynchronously after light registration?  The current answer is no
  — the task is deliberately separate so operators can control when
  materialization runs (e.g. post-deploy, not on every catalog bootstrap).
  Revisit if the preset lifecycle grows ``TaskContext`` support.
"""
from __future__ import annotations

from typing import Iterable

from dynastore.models.dimensions import DIMENSIONS_CATALOG_ID
from dynastore.modules.storage.presets.multi_contributor import MultiContributorPreset
from dynastore.modules.storage.presets.registry import register_preset
from dynastore.modules.storage.presets.preset import DataSeed


class _CommonDimensionsContributor:
    """Data contributor that yields one ``DataSeed`` per registered dimension.

    ``get_data()`` calls ``get_registered_dimensions()`` lazily on every
    invocation so that this module stays import-light and worker-safe: the
    ``ogc_dimensions`` package (and its provider side-effects) are only
    imported when a preset apply/dry_run actually executes, not at module
    load time.

    Each seed:
    - targets ``catalog_id=DIMENSIONS_CATALOG_ID`` with
      ``manage_catalog=False`` because ``_dimensions_`` is a shared
      platform catalog that must survive an individual preset revoke.
    - carries ``layer_config={"collection_type": "RECORDS"}`` and
      ``extra_metadata`` with ``provider``, ``cube:dimensions``, and
      ``itemType`` — mirroring the collection dict built by
      ``materialize_dimension`` in ``dimensions_extension.py``, but
      without the member ``items`` payload (``items=()``).
    """

    def get_data(self) -> Iterable[DataSeed]:
        from dynastore.extensions.dimensions.dimensions_extension import (
            _build_cube_dimensions,
            _build_provider,
            _infer_dim_type,
            get_registered_dimensions,
        )

        for dim_name, dim_config in get_registered_dimensions().items():
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
                collection_data=collection_data,
                items=(),
                manage_catalog=False,
                manage_collection=True,
            )


register_preset(MultiContributorPreset(
    name="common_dimensions",
    description=(
        "Register the standard reusable dimensions (temporal-dekadal, "
        "pentadal, admin-boundaries, indicator-tree, forestry-species, "
        "elevation-bands) as RECORDS collections with cube:dimensions "
        "metadata in the _dimensions_ catalog. Member materialization is "
        "performed by the dimensions_materialize task."
    ),
    keywords=("dimensions", "data", "platform", "stac", "datacube"),
    contributors_factory=lambda: [_CommonDimensionsContributor()],
))
