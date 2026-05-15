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

"""Window-bucket math shared by every :class:`UsageCounterProtocol` driver.

A bucket is the floored UTC start of the current rate-limit window. Two
drivers (Postgres durable, Valkey hot path) must agree on the bucket
boundary so reads in one match writes in the other; this module is the
single source of truth for that algebra.

``window_seconds=None`` (or ``<= 0``) marks a lifetime quota: bucket
collapses to the Unix epoch sentinel and ``expires_at`` is ``None``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# Fixed-bucket sentinel for lifetime counters. Postgres ``TIMESTAMPTZ``
# epoch is ``1970-01-01 UTC``; chosen so the PK index keeps lifetime
# rows clustered at the low end of the window_start range.
LIFETIME_BUCKET = datetime.fromtimestamp(0, tz=timezone.utc)


def bucket_for(
    window_seconds: Optional[int], now: Optional[datetime] = None
) -> datetime:
    """Floor ``now`` to the start of the current ``window_seconds`` window."""
    if window_seconds is None or window_seconds <= 0:
        return LIFETIME_BUCKET
    moment = now or datetime.now(timezone.utc)
    floor_ts = (int(moment.timestamp()) // window_seconds) * window_seconds
    return datetime.fromtimestamp(floor_ts, tz=timezone.utc)


def expires_for(
    bucket: datetime, window_seconds: Optional[int]
) -> Optional[datetime]:
    """Compute the row's ``expires_at`` — one window of reaper grace."""
    if window_seconds is None or window_seconds <= 0:
        return None
    # One window of grace past the bucket close — the nightly reaper
    # drops rows whose window has fully aged out. Short-lived in-flight
    # decisions during the trailing window still read a fresh count.
    return datetime.fromtimestamp(
        bucket.timestamp() + 2 * window_seconds, tz=timezone.utc
    )
