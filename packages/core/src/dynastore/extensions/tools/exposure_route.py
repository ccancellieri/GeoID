"""FastAPI dependency that rejects requests to disabled extensions.

Implemented as a request dependency instead of a custom `APIRoute` subclass
because `FastAPI.include_router` re-creates routes via the source route's
`type(route)`, so `router.route_class = ...` set after routes exist has no
effect. A dependency attached at `include_router` time runs for every included
operation, which is exactly the cross-cutting check this feature needs.
"""

from typing import Callable

from fastapi import HTTPException, Request

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix


def make_exposure_dependency(matrix: ExposureMatrix, ext_id: str) -> Callable:
    """Build an async dependency that 503s when `ext_id` is disabled.

    Pass via `app.include_router(..., dependencies=[Depends(dep)])` so the
    check runs before any route in the extension's router.
    """

    async def _check(request: Request) -> None:
        snap = await matrix.get()
        if not snap.platform.get(ext_id, True):
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Service '{ext_id}' is disabled on this platform. "
                    "Contact the administrator."
                ),
            )
        catalog_id = request.path_params.get("catalog_id")
        if catalog_id and snap.catalogs.get(catalog_id, {}).get(ext_id, True) is False:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Service '{ext_id}' is disabled for catalog '{catalog_id}'. "
                    "Contact the administrator."
                ),
            )

    return _check
