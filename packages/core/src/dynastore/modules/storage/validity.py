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

"""First-class temporal-validity specification for ``ItemsWritePolicy``.

``ValiditySpec`` replaces the bare ``ItemsWritePolicy.validity`` string toggle
with a driver-agnostic null-object value object.  The presence of a
``ValiditySpec`` (``validity is not None``) IS the toggle that enables temporal
``valid_from`` / ``valid_to`` tracking per entity; ``None`` disables validity
entirely.

Resolving the historic #1126 confusion explicitly:

- ``column`` is a **COLUMN NAME** — it names the ``tstzrange`` storage column on
  the temporal sidecar.  It is NOT a source path into the feature.
- The validity **VALUES** come from ``start_from`` / ``end_from``:
    * ``"context"`` (the default for ``start_from``) reads
      ``write_context.valid_from`` / ``write_context.valid_to``;
    * any other string is a **dotted source path** walked into the feature
      (e.g. ``"properties.start_date"``);
    * ``end_from=None`` (the default) means open-ended (no upper bound).
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator

__all__ = ["ValiditySpec"]


class ValiditySpec(BaseModel):
    """Driver-agnostic null-object specification for temporal validity.

    The mere presence of this spec on ``ItemsWritePolicy.validity`` enables
    validity tracking — ``ItemsWritePolicy.enable_validity`` is the derived
    boolean over ``validity is not None``.

    Attributes:
        column: Names the ``tstzrange`` storage column (a COLUMN NAME, NOT a
            source path).  Must be a non-empty, valid SQL identifier.  Its
            presence is the toggle that turns validity on.
        start_from: Where the validity-range **start** value comes from.
            ``"context"`` (default) reads ``write_context.valid_from``; any
            other value is a dotted source path walked into the feature
            (e.g. ``"properties.start_date"``).
        end_from: Where the validity-range **end** value comes from.
            ``None`` (default) means open-ended (no upper bound); ``"context"``
            reads ``write_context.valid_to``; any other value is a dotted
            source path walked into the feature.
        close_on_new_version: When ``on_conflict=NEW_VERSION`` archives a row,
            set the archived (old) row's validity upper bound so the temporal
            history stays non-overlapping.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    column: str
    start_from: str = "context"
    end_from: Optional[str] = None
    close_on_new_version: bool = True

    @field_validator("column")
    @classmethod
    def _validate_column(cls, v: str) -> str:
        """``column`` is a physical column name and flows into DDL/SQL — it must
        be a non-empty, valid SQL identifier (reuses the #1135 helper so the
        same identifier rules apply everywhere)."""
        from dynastore.tools.db import validate_sql_identifier

        return validate_sql_identifier(v)
