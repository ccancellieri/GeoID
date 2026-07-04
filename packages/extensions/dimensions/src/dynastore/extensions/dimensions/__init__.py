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

from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook
from . import config  # noqa: F401  -- service-exposure plugin registration

# The ``common_dimensions`` preset now lives in core, alongside the
# ``dimensions_materialize`` task it triggers, and registers via the
# ``dynastore.presets`` entry-point group (#2601) — no import needed here.

_examples_dir = Path(__file__).parent / "examples"

_nb12 = _examples_dir / "nb12_datacube_dimensions.ipynb"
if _nb12.is_file():
    register_platform_notebook(
        notebook_id="nb12_datacube_dimensions",
        registered_by="dimensions_extension",
        notebook_path=_nb12,
        title={"en": "OGC Datacube Dimensions — End-to-End"},
        description={
            "en": (
                "Register dimensions via the common_dimensions preset, "
                "query dekadal and admin-hierarchy dimensions via the OGC "
                "Dimensions API, and monitor the materialisation job."
            )
        },
        tags=["dimensions", "ogc", "datacube", "stac", "preset"],
    )
