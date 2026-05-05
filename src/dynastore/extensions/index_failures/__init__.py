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

"""``index_failures`` — tenant-visible REST surface for the per-tenant
``index_failure_log`` table.

Surfaces ``GET /index_failures/catalogs/{catalog_id}/index-failures``
so tenants can self-serve recent indexing failures (transient retries
+ terminal failures) without operator log access. Authz is delegated
to the IAM façade via the ``catalog_membership_required`` condition
on the registered policy.
"""

from .service import IndexFailureService

__all__ = ["IndexFailureService"]
