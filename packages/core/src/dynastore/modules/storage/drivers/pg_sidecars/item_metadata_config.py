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

"""Configuration for the generic Item Metadata sidecar."""

from typing import Literal

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig, SidecarConfigRegistry


class ItemMetadataSidecarConfig(SidecarConfig):
    """PG sidecar table for per-item multilanguage metadata + STAC overlay.

    Owns the ``{schema}.{table}_item_metadata`` table — one row per
    item (FK to hub on ``geoid``).  Stores:

    * **Multilanguage core** — ``title`` / ``description`` /
      ``keywords`` as JSONB ``{"en": "...", "fr": "..."}`` resolved at
      read time per ``?lang=`` query param.
    * **STAC external content** — ``external_extensions`` (JSONB
      array) + ``external_assets`` (JSONB dict) + ``extra_fields``
      (JSONB) for stac_extensions / assets / namespaced fields the
      caller supplied that aren't owned by a registered STAC provider.
      Merged back into the rendered STAC Item by the
      ``StacItemsSidecar`` overlay at read time.

    Auto-injected by the STAC extension via ``SidecarRegistry`` — gets
    snapshotted into ``ItemsPostgresqlDriverConfig.sidecars`` at
    ``ensure_storage()`` time whenever the STAC extension is loaded,
    regardless of whether the caller's overrides include it.
    """

    # Literal-typed discriminator — required by Pydantic for the
    # Annotated[Union[...], Discriminator("sidecar_type")] dispatch on
    # ItemsPostgresqlDriverConfig.sidecars.
    sidecar_type: Literal["item_metadata"] = "item_metadata"


SidecarConfigRegistry.register("item_metadata", ItemMetadataSidecarConfig)
