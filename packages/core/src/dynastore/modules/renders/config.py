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

"""Operator-tunable configuration for the raster tile render cache.

Mirrors ``TilesCachingConfig`` exactly in structure. Bucket selection is
intentionally absent: buckets are provisioned per-catalog by
``StorageProtocol.ensure_storage_for_catalog`` and the existing per-catalog
bucket is reused — no new storage is provisioned.

The cache key shape is::

    {key_prefix}/{internal_collection_id}/{style_id}/{tms_id}/{z}/{x}/{y}.{fmt}

where ``internal_collection_id`` is the immutable internal id (never the
public ``external_id``) so renaming a collection never silently shifts
all rendered tiles to a new prefix.
"""

import hashlib
from typing import ClassVar, Optional, Sequence, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig


class RenderCachingConfig(PluginConfig):
    """Operator-tunable knobs for the bucket-backed raster tile cache.

    Live edits via the configs API apply on the next render save / fetch
    — no rewrite of already-cached objects.  Changing ``key_prefix`` orphans
    existing cached renders (they remain under the old prefix until the bucket
    TTL evicts them).

    ``cache_enabled`` (default ``True``) gates the bucket-backed cache only.
    When ``False``:

    - ``get_render_url`` / ``get_render`` / ``check_render_exists`` return as
      a miss without touching the bucket (every request falls through to live
      rio-tiler rendering).
    - ``save_render`` is a no-op.
    - Deletes still execute so operators can drop stale blobs.
    """

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "renders")

    cache_enabled: Mutable[bool] = Field(
        default=True,
        description=(
            "Bucket-backed render-tile cache toggle. "
            "Disable to force live rendering on every request."
        ),
    )

    key_prefix: Mutable[str] = Field(
        default="renders/collections",
        min_length=1,
        max_length=128,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_\-/]*[a-zA-Z0-9]$",
        description=(
            "Object-key prefix under the catalog bucket. "
            "Full key: ``{key_prefix}/{internal_collection_id}/{style_id}"
            "/{tms_id}/{z}/{x}/{y}.{format}``. "
            "Uses the INTERNAL immutable collection id, never external_id. "
            "Changing this prefix orphans existing cached renders."
        ),
    )

    ttl_seconds: Mutable[int] = Field(
        default=31536000,
        ge=0,
        le=31536000,
        description=(
            "``Cache-Control: public, max-age=<ttl_seconds>`` set on every "
            "render object written to the bucket. 0 disables browser/CDN "
            "caching (objects still persist server-side). Default is one year."
        ),
    )


def build_render_params_hash(
    bands: Optional[Sequence[int]] = None,
    expression: Optional[str] = None,
    rescale: Optional[Sequence[Tuple[float, float]]] = None,
) -> Optional[str]:
    """Return a short hash over multiband render params, or ``None`` when all are absent.

    Used as a ``params_hash`` component in cache keys so that different
    band selections, expressions, and rescale ranges cache as distinct blobs
    even when they share the same ``style_id``.

    The hash is a 16-char hex prefix of SHA-256 over a canonical string
    representation of the three params.  Single-band requests with no
    expression and no rescale return ``None`` (no hash suffix — same cache key
    shape as Slice 1).

    Args:
        bands: Sequence of band indices (e.g. ``(3, 2, 1)``).
        expression: Band-math expression string (e.g. ``"(B1-B2)/(B1+B2)"``).
        rescale: Per-band rescale ranges as ``[(min, max), ...]``.

    Returns:
        A 16-character hex string, or ``None`` when all params are absent/empty.
    """
    if not bands and not expression and not rescale:
        return None

    parts: list[str] = [
        f"bands={','.join(str(b) for b in bands) if bands else ''}",
        f"expr={expression or ''}",
        f"rescale={'|'.join(f'{lo},{hi}' for lo, hi in rescale) if rescale else ''}",
    ]
    raw = ";".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def build_render_cache_key(
    key_prefix: str,
    internal_collection_id: str,
    style_id: str,
    tms_id: str,
    z: int,
    x: int,
    y: int,
    fmt: str,
    params_hash: Optional[str] = None,
) -> str:
    """Build the GCS object key for a rendered raster tile.

    The ``internal_collection_id`` MUST be the immutable internal id, not the
    public ``external_id``. Callers are responsible for resolving external →
    internal before calling this function.

    When ``params_hash`` is supplied (non-None), it is appended to the
    ``style_id`` segment so that distinct band selections, expressions, and
    rescale ranges cache as distinct blobs:
    ``{key_prefix}/{internal_collection_id}/{style_id}@{params_hash}/...``
    """
    style_segment = f"{style_id}@{params_hash}" if params_hash else style_id
    return (
        f"{key_prefix}/{internal_collection_id}/{style_segment}"
        f"/{tms_id}/{z}/{x}/{y}.{fmt}"
    )
