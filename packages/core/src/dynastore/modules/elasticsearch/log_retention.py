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

"""Retention for monthly ES/OpenSearch log indices (#2797).

Deliberately a supervisor job rather than ILM/ISM: dev runs OpenSearch,
prod runs Elasticsearch 9.x, and ILM/ISM are two different, non-portable
policy dialects we would otherwise have to keep in sync. A supervisor job
is one code path against both.

The selection logic (:func:`parse_log_index_month`,
:func:`select_expired_log_indices`) is pure and dependency-free so it can
be unit-tested without a live cluster; :func:`run_es_logs_retention` does
the actual ``indices.get`` / ``indices.delete`` round trip and is what the
``es_logs_retention`` maintenance-supervisor job
(``modules/catalog/maintenance_supervisor.py``) calls.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional

logger = logging.getLogger(__name__)

_MONTH_SUFFIX_RE = re.compile(r"-logs-(\d{4})\.(\d{2})$")


def parse_log_index_month(index_name: str) -> Optional[date]:
    """Extract a monthly log index's ``YYYY.MM`` suffix as a first-of-month date.

    Returns ``None`` for names that don't carry the suffix — notably the
    pre-#2797 flat ``{prefix}-logs`` index, which is never auto-deleted by
    the retention job (see the migration note on
    :func:`~.mappings.get_log_read_index_target`).
    """
    match = _MONTH_SUFFIX_RE.search(index_name)
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    return date(year, month, 1)


def _months_before(when: date, months: int) -> date:
    """Return the first-of-month date *months* calendar months before *when*."""
    total = when.year * 12 + (when.month - 1) - months
    year, month0 = divmod(total, 12)
    return date(year, month0 + 1, 1)


def select_expired_log_indices(
    index_names: Iterable[str],
    now: datetime,
    retention_months: int,
) -> List[str]:
    """Return the subset of *index_names* older than the retention window.

    An index is expired when its parsed month is strictly before the month
    that is *retention_months* calendar months before ``now``'s month.
    Names that don't parse as a monthly log index (e.g. the flat pre-#2797
    index) are never selected — this function only ever returns deletion
    candidates it is certain about.
    """
    cutoff = _months_before(now.date(), retention_months)
    return [
        name
        for name in index_names
        if (month := parse_log_index_month(name)) is not None and month < cutoff
    ]


async def run_es_logs_retention(retention_months: int) -> int:
    """Delete monthly log indices older than *retention_months*.

    Returns the number of indices deleted (0 when ES is unconfigured, the
    index listing fails, or nothing is due). Best-effort per index: one
    failed delete is logged and skipped rather than aborting the batch.
    """
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_log_index_pattern

    es = get_client()
    if es is None:
        logger.debug("es_logs_retention: no ES client configured — skipping.")
        return 0

    pattern = get_log_index_pattern(get_index_prefix())
    try:
        existing = await es.indices.get(
            index=pattern, params={"ignore_unavailable": "true"}
        )
    except Exception as exc:
        logger.warning("es_logs_retention: indices.get(%s) failed: %s", pattern, exc)
        return 0

    index_names = list(existing.keys()) if isinstance(existing, dict) else []
    expired = select_expired_log_indices(
        index_names, datetime.now(timezone.utc), retention_months
    )

    deleted = 0
    for name in expired:
        try:
            await es.indices.delete(index=name)
            deleted += 1
            logger.info("es_logs_retention: deleted expired log index '%s'.", name)
        except Exception as exc:
            logger.warning("es_logs_retention: failed to delete '%s': %s", name, exc)

    return deleted
