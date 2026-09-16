---
name: geoid-development
description: Use when planning, implementing, reviewing, testing, or documenting GeoID or DynaStore, including storage routing, feature identity, lifecycle workers, Python modules, and OGC extensions.
---

# GeoID / DynaStore development

## Required loading

1. Read this skill and [development guidelines](references/development-guidelines.md) before project work. For a read-only question, apply only the relevant checks; do not turn it into an implementation task.
2. **REQUIRED SUB-SKILL: `ogc-api-standards`.** Load the installed `ogc-api-standards` skill on every project task. Also read its GeoID implementation map as required there. For protocol work, load the relevant family reference and verify normative requirements. Do not copy private endpoints or historical issue identifiers from those references into output.
3. Read the current checkout's agent instructions and package manifests. Use CodeGraph when indexed. Historical paths, deployment descriptions, conformance claims, and issue closure states are not current implementation evidence.
4. For documentation or behavior changes, load the project's `geoid-docs` skill, if available; its older repository visibility statements do not authorize publication.

If a required skill is missing, state the missing dependency, continue safe inspection, and do not claim its checks were performed.

## Working contract

Before changing behavior, identify the owning module, public contract, identity and authorization boundary, relevant failure modes, and a focused regression test. Keep optional capabilities out of unrelated core components. Verify installed/composed capabilities rather than inferring support from a package or backend name.

Use the guidelines' topic-specific test matrix. For lifecycle work, test stale jobs against recreated resources. For routing, test exact reads separately from searches and indexing. For protocol work, compare real wire responses with cited requirements, not internal envelopes.

Finish with: behavior changed; evidence from tests and relevant requirements; remaining limitations. Distinguish a proposed improvement from a verified fix. Do not silently expand a request into deployment, migration, or publication.

## Safety and scope

These are versioned development instructions, not an issue archive. Keep private source material, raw issue exports, credentials, deployment identifiers, and personal data out of skills and public artifacts. Version reviewed, portable project guidance; keep personal runtime configuration and private context untracked.

The `un-fao/GeoID` repository is read-only under the user's current instruction. Inspect the explicit repository target before any remote mutation; do not infer write authority from authentication or the local default remote. A request to develop locally does not authorize publishing externally.
