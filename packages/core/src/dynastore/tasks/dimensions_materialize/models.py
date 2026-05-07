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

from typing import List, Optional

from pydantic import BaseModel, Field


class DimensionsMaterializeRequest(BaseModel):
    """Input payload for the dimensions_materialize OGC Process.

    Both fields are optional: calling the task with an empty body
    materialises every registered dimension that has drifted from its
    persisted ``cube:dimensions`` — the common "run after deploy" case.
    """

    dim_names: Optional[List[str]] = Field(
        None,
        description=(
            "Subset of dimension names to materialise. If omitted or null, "
            "every registered dimension is processed."
        ),
    )
    force: bool = Field(
        False,
        description=(
            "Bypass the per-dimension cube:dimensions equality check and "
            "re-upsert every member unconditionally. Use only when you "
            "suspect the persisted collection state has drifted from the "
            "underlying table rows (e.g. after a direct DB repair)."
        ),
    )
