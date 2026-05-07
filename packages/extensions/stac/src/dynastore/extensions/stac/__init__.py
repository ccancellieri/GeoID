#    Copyright 2025 FAO
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

# Eager imports so the package's `__init__` (loaded whenever the
# `dynastore.extensions:stac` entry point is resolved) populates both
# sidecar registries before any worker accepts traffic. Without this,
# `_coerce_pg_sidecar` can fail with
# `sidecar_type 'stac_metadata' not registered` on a worker whose
# request path hasn't yet pulled `stac_items_sidecar` lazily.
from dynastore.extensions.stac import (  # noqa: F401
    stac_metadata_config as _stac_metadata_config,  # SidecarConfigRegistry
    stac_items_sidecar as _stac_items_sidecar,      # SidecarRegistry
)

