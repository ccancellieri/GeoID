"""Regression: deprecated assets routes and shims must not be re-introduced."""
from __future__ import annotations

import pytest


def test_upload_provider_property_is_removed():
    from dynastore.extensions.assets.assets_service import AssetService

    assert not hasattr(AssetService, "upload_provider"), (
        "AssetService.upload_provider was removed; use resolve_upload_driver()"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/assets/search",
        "/assets/catalogs/some-cat/search",
    ],
)
def test_deprecated_search_aliases_are_unmounted(path):
    """The /search aliases were removed; only /assets-search remains."""
    from dynastore.extensions.assets.assets_service import AssetService

    service = AssetService.__new__(AssetService)
    routes = getattr(service, "router", None)
    if routes is None:
        # Service not yet bootstrapped in this lightweight test; assert by source-grep instead.
        import inspect
        src = inspect.getsource(AssetService)
        assert '"/search"' not in src, "deprecated /search alias re-introduced"
        assert '"/catalogs/{catalog_id}/search"' not in src, (
            "deprecated /catalogs/{catalog_id}/search alias re-introduced"
        )
