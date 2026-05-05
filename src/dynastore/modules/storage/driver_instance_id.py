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

"""Stable identity for a (driver_id, catalog, collection) triple.

Routing configs are per-collection, so the same driver_id string can
mean different effective drivers across collections.  driver_instance_id
gives an unambiguous correlation key for outbox rows, failure log
entries, dispatcher logs, and consumer dedup.

UUIDv5 over a fixed namespace -> deterministic, identical across
processes and restarts.
"""
from __future__ import annotations

from uuid import UUID, uuid5

_NS = UUID("4f5b8c12-7a3e-4f1a-9b2d-3a6c8d1e7f04")  # private namespace; never reused


def compute_driver_instance_id(
    driver_id: str, catalog_id: str, collection_id: str,
) -> str:
    return str(uuid5(_NS, f"{driver_id}|{catalog_id}|{collection_id}"))
