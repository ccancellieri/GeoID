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

"""Configuration for the STAC items sidecar.

Lives in ``modules/storage/drivers/pg_sidecars/`` so the items driver's
discriminated union (``ItemsPostgresqlDriverConfig.sidecars``, in
``modules/storage/driver_config.py``) can reference it without crossing
the storage→extensions layer boundary.  The sidecar's behaviour
(``get_ddl``, ``map_row_to_feature``, etc.) lives in
``extensions/stac/stac_items_sidecar.py``.
"""

from typing import Literal

from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarConfig,
    SidecarConfigRegistry,
)


class StacItemsSidecarConfig(SidecarConfig):
    """Configuration for the STAC items sidecar.

    The sidecar owns ``{schema}.{items_table}_stac_metadata`` with
    JSONB columns persisting externally-supplied STAC content
    (``external_extensions``, ``external_assets``, ``extra_fields``).
    """

    # Literal-typed discriminator — required by Pydantic for the
    # Annotated[Union[...], Discriminator("sidecar_type")] dispatch on
    # ItemsPostgresqlDriverConfig.sidecars.
    sidecar_type: Literal["stac_metadata"] = "stac_metadata"


SidecarConfigRegistry.register("stac_metadata", StacItemsSidecarConfig)
