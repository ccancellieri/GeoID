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

``ValiditySpec`` is a driver-agnostic, null-object value object describing the
temporal-validity **concept** for an entity.  The presence of a ``ValiditySpec``
(``validity is not None``) IS the toggle that enables temporal
``valid_from`` / ``valid_to`` tracking per entity; ``None`` disables validity
entirely.

Validity is a concept, not a storage layout.  Each driver decides how to
physically persist it — PostgreSQL, for example, stores the window in a single
``tstzrange`` system column, the same way it owns ``geoid`` /
``transaction_time`` / ``deleted_at``.  That choice is an implementation
detail and is deliberately NOT part of this contract: the spec carries only the
concept (where the start / end values come from), never a physical column name.

The validity **VALUES** come from ``start_from`` / ``end_from``:

- ``"context"`` (the default for ``start_from``) reads
  ``write_context.valid_from`` / ``write_context.valid_to``;
- any other string is a **dotted source path** walked into the feature
  (e.g. ``"properties.start_date"``);
- ``None`` means that bound is **open** — there is no value source, so the
  window is unbounded on that side. ``start_from=None`` yields an open lower
  bound; ``end_from=None`` (the default) an open upper bound.

The two bounds are fully independent, so all four states are expressible:
start-only, end-only, both, and neither (a fully-open window).

A feature always receives an ingestion time (``transaction_time``); the
validity ``(from, to)`` window — and each of its bounds — is optional.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict

__all__ = ["ValiditySpec"]


class ValiditySpec(BaseModel):
    """Driver-agnostic null-object specification for temporal validity.

    The mere presence of this spec on ``ItemsWritePolicy.validity`` enables
    validity tracking — ``ItemsWritePolicy.enable_validity`` is the derived
    boolean over ``validity is not None``.  How the validity window is stored
    is the driver's choice; this spec describes only the concept.

    Attributes:
        start_from: Where the validity-range **start** value comes from.
            ``"context"`` (default) reads ``write_context.valid_from``;
            ``None`` means an open lower bound (no start value source — the
            window is unbounded below); any other value is a dotted source
            path walked into the feature (e.g. ``"properties.start_date"``).
        end_from: Where the validity-range **end** value comes from.
            ``None`` (default) means an open upper bound (no end value source);
            ``"context"`` reads ``write_context.valid_to``; any other value is
            a dotted source path walked into the feature.
        close_on_new_version: When ``on_conflict=NEW_VERSION`` archives a row,
            set the archived (old) row's validity upper bound so the temporal
            history stays non-overlapping.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    start_from: Optional[str] = "context"
    end_from: Optional[str] = None
    close_on_new_version: bool = True
