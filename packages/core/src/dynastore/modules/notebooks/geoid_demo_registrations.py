"""Platform notebook registrations for the GeoID customer-facing demos.

Lives in core (rather than the geoid extension) because the catalog
service runs without ``extension_geoid`` installed but is the surface
that serves JupyterLite. Loaded by ``NotebooksModule.lifespan`` via the
hardcoded module-path list.
"""
from pathlib import Path

from .example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "geoid_demo"


register_platform_notebook(
    notebook_id="lookup_only_demo",
    registered_by=_REG,
    notebook_path=_HERE / "lookup_only_demo.ipynb",
    title={"en": "Lookup-only public catalog — anonymous demo"},
    description={
        "en": (
            "Anonymous demo of the lookup-only profile: geoid lookup + "
            "exact-item GET on STAC/Features both succeed without a token, "
            "while collection list / items list / STAC search return 401/403. "
            "Drives POST /search/catalogs/{cat}/geoid-search (PG-backed) "
            "and contrasts the open catalog surface against the auth-gated "
            "surface to show DENY policies firing as expected."
        )
    },
    tags=["geoid", "lookup", "anonymous", "iam", "demo"],
)
register_platform_notebook(
    notebook_id="public_and_private_collections",
    registered_by=_REG,
    notebook_path=_HERE / "public_and_private_collections.ipynb",
    title={"en": "Public vs Private Collections — customer demo"},
    description={
        "en": (
            "End-to-end walkthrough of the two access scenarios on the "
            "live review env. Scenario A creates a public catalog + "
            "collection, ingests 3 STAC items, polls the OUTBOX drain, "
            "and proves both authenticated and anonymous STAC search "
            "return them. Scenario B repeats the flow with "
            "the items + collection routing configs pinned to the "
            "private driver variants and proves anonymous "
            "STAC search returns 0 features while the authenticated "
            "user still sees everything. Includes catalog-readiness "
            "polls, OUTBOX drain polls, and a PG-hint fallback probe "
            "so failures point at the actual layer at fault."
        )
    },
    tags=["geoid", "stac", "elasticsearch", "privacy", "demo"],
)
