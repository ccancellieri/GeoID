"""Collect README.md from each installed module/extension/task."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Set


@dataclass(frozen=True)
class ReadmeEntry:
    module_id: str
    path: Path
    content: str
    priority: int


def collect_readmes(
    *,
    roots: Iterable[Path],
    installed_module_ids: Set[str],
    priority_by_id: Mapping[str, int],
) -> List[ReadmeEntry]:
    """Walk `roots` for direct-child packages; return a ReadmeEntry for each installed
    module that owns a README.md, ascending by priority (first loaded first listed)."""
    entries: List[ReadmeEntry] = []
    for root in roots:
        if not root.is_dir():
            continue
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            module_id = child.name
            if module_id not in installed_module_ids:
                continue
            readme = child / "README.md"
            if not readme.is_file():
                continue
            entries.append(ReadmeEntry(
                module_id=module_id,
                path=readme,
                content=readme.read_text(encoding="utf-8"),
                priority=priority_by_id.get(module_id, 10_000),
            ))
    entries.sort(key=lambda e: (e.priority, e.module_id))
    return entries
