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

"""Schema-id-keyed migration DAG for :class:`PersistentModel` payloads.

Migrators are pure ``dict -> dict`` functions registered between two
content-addressed ``schema_id`` hashes. On read, if the row's stored
``schema_id`` differs from the class's current ``schema_id``, a BFS
across registered migrators finds a path and applies each step. No-op
when the hashes already match; never touches the DB.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Callable, Dict, List, Tuple

logger = logging.getLogger(__name__)

MigratorFn = Callable[[dict], dict]

# Adjacency: source_schema_id -> [(target_schema_id, fn), ...]
_GRAPH: Dict[str, List[Tuple[str, MigratorFn]]] = {}


def migrates(*, source: str, target: str) -> Callable[[MigratorFn], MigratorFn]:
    """Register a migrator for ``source -> target`` in the DAG.

    Hashes are ``"sha256:<hex>"`` strings as produced by
    :func:`compute_schema_id`. Duplicate edges raise at import time.
    """
    def decorator(fn: MigratorFn) -> MigratorFn:
        edges = _GRAPH.setdefault(source, [])
        if any(tgt == target for tgt, _ in edges):
            raise ValueError(
                f"Duplicate migrator: {source} -> {target} already registered"
            )
        edges.append((target, fn))
        logger.debug("Registered migrator %s -> %s (%s)", source, target, fn.__qualname__)
        return fn

    return decorator


def find_path(
    source: str, target: str
) -> List[MigratorFn]:
    """BFS from ``source`` to ``target``. Empty list if already equal.

    Raises ``LookupError`` when no path exists.
    """
    if source == target:
        return []

    queue: deque = deque([(source, [])])
    seen = {source}
    while queue:
        node, path = queue.popleft()
        for tgt, fn in _GRAPH.get(node, []):
            if tgt in seen:
                continue
            new_path = path + [fn]
            if tgt == target:
                return new_path
            seen.add(tgt)
            queue.append((tgt, new_path))

    raise LookupError(
        f"No migrator path from {source} to {target}. "
        f"Author a @migrates function to bridge them."
    )


def migrate(data: dict, *, source: str, target: str) -> dict:
    """Apply the migrator chain from ``source`` to ``target`` on ``data``."""
    for fn in find_path(source, target):
        data = fn(data)
    return data


def clear() -> None:
    """Test hook only."""
    _GRAPH.clear()
