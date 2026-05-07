"""In-memory cache of the current extension-exposure state."""

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Mapping, Optional, Tuple, Type

from dynastore.models.protocols import ConfigsProtocol


@dataclass(frozen=True)
class ExposureSnapshot:
    platform: Mapping[str, bool]
    catalogs: Mapping[str, Mapping[str, bool]]
    loaded_at: float


class ExposureMatrix:
    def __init__(
        self,
        configs_service: ConfigsProtocol,
        togglable_extensions: FrozenSet[str],
        plugin_class_by_extension: Mapping[str, Type],
        ttl_seconds: float = 30.0,
    ):
        self._configs = configs_service
        self._togglable = togglable_extensions
        self._cls_by_ext = plugin_class_by_extension
        self._ttl = ttl_seconds
        self._snapshot: Optional[ExposureSnapshot] = None
        self._lock = asyncio.Lock()

    async def get(self) -> ExposureSnapshot:
        now = time.monotonic()
        snap = self._snapshot
        if snap is not None and (now - snap.loaded_at) < self._ttl:
            return snap
        async with self._lock:
            snap = self._snapshot
            if snap is not None and (time.monotonic() - snap.loaded_at) < self._ttl:
                return snap
            self._snapshot = await self._load()
            return self._snapshot

    def get_sync(self) -> ExposureSnapshot:
        if self._snapshot is None:
            return ExposureSnapshot(
                platform={e: True for e in self._togglable},
                catalogs={}, loaded_at=0.0,
            )
        return self._snapshot

    def invalidate(self) -> None:
        self._snapshot = None

    async def _load(self) -> ExposureSnapshot:
        platform: Dict[str, bool] = {}
        for ext_id in self._togglable:
            cls = self._cls_by_ext.get(ext_id)
            if cls is None:
                platform[ext_id] = True
                continue
            try:
                cfg = await self._configs.get_config(cls)
                platform[ext_id] = bool(getattr(cfg, "enabled", True))
            except Exception:
                platform[ext_id] = True

        catalogs: Dict[str, Dict[str, bool]] = {}
        for cat_id, cls_names in await self._list_catalog_overrides():
            overrides: Dict[str, bool] = {}
            for cls_name in cls_names:
                ext_id = self._ext_for_class(cls_name)
                if ext_id is None:
                    continue
                cls = self._cls_by_ext[ext_id]
                try:
                    cfg = await self._configs.get_config(cls, catalog_id=cat_id)
                    overrides[ext_id] = bool(getattr(cfg, "enabled", True))
                except Exception:
                    continue
            if overrides:
                catalogs[cat_id] = overrides

        return ExposureSnapshot(platform=platform, catalogs=catalogs, loaded_at=time.monotonic())

    async def _list_catalog_overrides(self) -> List[Tuple[str, List[str]]]:
        lister = getattr(self._configs, "list_catalog_overrides", None)
        if lister is not None:
            return await lister()
        return []

    def _ext_for_class(self, cls_name: str) -> Optional[str]:
        for ext_id, cls in self._cls_by_ext.items():
            if cls.__name__ == cls_name:
                return ext_id
        return None
