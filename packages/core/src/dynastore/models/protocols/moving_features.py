from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class MovingFeaturesProtocol(Protocol):
    """Protocol for the OGC API - Moving Features service."""

    async def list_moving_features(
        self, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0
    ) -> List[Any]:
        """Lists moving features for a collection."""
        ...

    async def get_moving_feature(
        self, catalog_id: str, collection_id: str, mf_id: Any
    ) -> Optional[Any]:
        """Retrieves a single moving feature."""
        ...

    async def list_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: Any,
        dt_start: Optional[Any] = None,
        dt_end: Optional[Any] = None,
    ) -> List[Any]:
        """Lists temporal geometry sequences for a moving feature."""
        ...
