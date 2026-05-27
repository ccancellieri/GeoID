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

# Register IAM extension presets into the global registry on import.
# Importing the subpackage has no DB side-effects — only in-memory registration.
from dynastore.extensions.iam import presets as _iam_presets  # noqa: F401

# After registering iam_baseline (above), retry composite registration so
# that composites which depend on IAM presets (iam_baseline,
# default_roles_baseline) get a second attempt.  The function is a no-op for
# composites already registered and logs an info line for any that still
# cannot satisfy their children.
try:
    from dynastore.modules.storage.presets.composites import (  # noqa: F401
        retry_composite_registration as _retry,
    )
    _retry()
except Exception:  # noqa: BLE001
    pass