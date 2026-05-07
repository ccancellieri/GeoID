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

"""OGC Process task — materialise OGC Dimensions into Records collections.

Replaces the in-lifespan materialisation that used to run on every pod
boot. Operators trigger this task once per deploy (manually from a
notebook, or from a Cloud Run post-deploy job) and the task is idempotent
via a ``cube:dimensions`` equality check on the persisted collection row.
"""
