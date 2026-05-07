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

"""Re-export of the web-page / static-asset convenience decorators.

Kept for backwards compatibility of import paths inside the ``modules``
tree.  The canonical location is ``dynastore.extensions.web.decorators``.
"""

from dynastore.extensions.web.decorators import expose_static, expose_web_page

__all__ = ["expose_static", "expose_web_page"]
