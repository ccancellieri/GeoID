from typing import Protocol, runtime_checkable, List, Optional, Any

@runtime_checkable
class StylesProtocol(Protocol):
    """Protocol for the OGC API - Styles service."""
    
    async def list_styles(self, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[Any]:
        """Lists styles for a collection."""
        ...

    async def get_style(self, catalog_id: str, collection_id: str, style_id: str) -> Optional[Any]:
        """Retrieves a specific style."""
        ...
