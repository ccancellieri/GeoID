"""Self-contained demonstration of OGC Dimension providers.

Covers the three FAO-relevant provider types:

  Dekadal      — 10-day periods (FAO ASIS, FEWS NET, TUW-GEO)
  Pentadal     — 5-day periods, month-aligned (FAO / CHIRPS / CDT)
  Admin tree   — leveled continent -> country hierarchy with multilingual labels

All temporal providers use the same DailyPeriodProvider with different
config: period_days (5 or 10) and scheme ("monthly" or "annual").

Run with:
    python -m dynastore.extensions.dimensions.examples
"""

from __future__ import annotations


def demo_dekadal() -> None:
    """Dekadal: 10-day periods, 36/year, D3 absorbs remainder of month."""
    from ogc_dimensions.providers import DailyPeriodProvider

    gen = DailyPeriodProvider(period_days=10, scheme="monthly")
    print(f"  provider_type = {gen.provider_type}")
    print(f"  config        = {gen.config_as_dict()}")

    result = gen.generate("2025-01-01", "2025-03-31", limit=3)
    for m in result.members:
        print(f"    {m.code}  {m.start} -> {m.end}")

    inv = gen.inverse("2025-01-15")
    print(f"  inverse('2025-01-15') -> {inv.code}  range=({inv.start}, {inv.end})")

    desc = gen.generate("2025-01-01", "2025-03-31", limit=2, sort_dir="desc")
    print(f"  desc first: {desc.members[0].code}")


def demo_pentadal() -> None:
    """Pentadal monthly: 5-day periods, 72/year, P6 absorbs 26-EOM."""
    from ogc_dimensions.providers import DailyPeriodProvider

    gen = DailyPeriodProvider(period_days=5, scheme="monthly")
    print(f"  provider_type = {gen.provider_type}")
    print(f"  config        = {gen.config_as_dict()}")

    result = gen.generate("2025-01-01", "2025-01-31", limit=6)
    for m in result.members:
        print(f"    {m.code}  {m.start} -> {m.end}")

    inv = gen.inverse("2025-01-27")
    print(f"  inverse('2025-01-27') -> {inv.code}  range=({inv.start}, {inv.end})")


def demo_admin_hierarchy() -> None:
    """Admin boundaries: leveled tree with multilingual labels."""
    from ogc_dimensions.providers import LeveledTreeProvider

    from .use_cases import ADMIN_NODES

    gen = LeveledTreeProvider(nodes=ADMIN_NODES)
    print(f"  provider_type = {gen.provider_type}")
    print(f"  config        = {gen.config_as_dict()}")
    print(f"  hierarchical   = {gen.hierarchical}")

    continents = gen.generate("", "", limit=10)
    print(f"  root members ({continents.number_matched}):")
    for m in continents.members:
        print(f"    {m.code}  label={m.extra.get('label')}  has_children={m.has_children}")

    afr_fr = gen.children("AFR", sort_by="label", language="fr")
    print(f"  children of AFR, sorted by label (fr) ({afr_fr.number_matched}):")
    for m in afr_fr.members[:5]:
        fr_label = m.extra.get("labels", {}).get("fr", m.extra.get("label"))
        print(f"    {m.code}  fr={fr_label}")

    ancestors = gen.ancestors("ETH")
    print(f"  ancestors of ETH: {[n['code'] for n in ancestors]}")

    search = gen.search(
        protocol=gen.search_protocols[1],  # LIKE
        extent_min="",
        extent_max="",
        like="*Tan*",
        language="en",
    )
    print(f"  search(like='*Tan*'): {[m.code for m in search.members]}")


def main() -> None:
    print("=== Dekadal (10-day, daily-period) ===")
    demo_dekadal()
    print()

    print("=== Pentadal monthly (5-day, daily-period) ===")
    demo_pentadal()
    print()

    print("=== Admin boundaries (leveled tree, multilingual) ===")
    demo_admin_hierarchy()


if __name__ == "__main__":
    main()
