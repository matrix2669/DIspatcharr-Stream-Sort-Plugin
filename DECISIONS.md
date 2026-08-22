# DECISIONS.md

This file records significant architecture and workflow decisions for Dispatcharr Stream Sort.

---

# ADR-001: Limit the plugin to analysis and stream ordering

## Status

Accepted

## Date

2026-08-17

## Decision

Stream Sort analyzes streams already attached to Dispatcharr channels and may update only `ChannelStream.order` within the explicitly selected channel scope. It does not create, delete, match, rename, regroup, or reassign streams or channels.

## Reason

Stream discovery and channel management have separate ownership. Restricting mutations makes dry runs meaningful and prevents a sorting action from changing channel composition.

## Consequences

New actions and integrations must preserve this boundary. Tests must cover filtering and mutation scope.

## Provenance

- Initial plugin baseline: `b3034a51ad3908e47bd5a75490f945728def4708`
- User documentation: `README.md`

---

# ADR-002: Use component freshness and conservative runtime evidence

## Status

Accepted

## Date

2026-08-20

## Decision

Reachability, content validation, media metadata, and delivery throughput have independent freshness rules in the unified analysis cache. Fresh Dispatcharr playback metadata and clean runtime playback may satisfy only the components they actually prove.

Reliability scoring uses fresh schema-2 URL-attributed evidence with decay and minimum evidence thresholds. Legacy schema-1 counters remain visible but are not scored.

## Reason

Opening provider connections for already fresh evidence wastes capacity, while treating incomplete or ambiguously attributed evidence as authoritative can incorrectly demote streams.

## Consequences

URL changes invalidate cached components. Playback reuse never claims that black, frozen, or silent-content checks ran. Reliability changes require attributable evidence and regression coverage.

## Provenance

- Incremental cache: `dcbbb1ffd4d72acff435d774b5a9c053c674d6de`
- Playback reuse and reliability scoring: `04820e6aa2848eb4c9755e52a70322a46465d483`

---

# ADR-003: Share Dispatcharr profile capacity and serialize provider retries

## Status

Accepted

## Date

2026-08-20

## Decision

Analysis counts every active M3U profile, reserves capacity through Dispatcharr's connection pool, resolves the selected profile URL through Dispatcharr's native rewrite logic, and releases the exact reservation afterward. Initial work may run concurrently within available capacity, but retries are limited to one concurrent check per M3U provider while different providers may retry in parallel.

## Reason

Multiple profiles contribute real capacity, active viewers must retain their reserved slots, profile-specific regex or credential rewriting must be honored, and parallel retries against one provider can repeat the overload that caused an initial failure.

## Consequences

Capacity behavior must be validated against fresh profile configuration. Deferred checks retain cached results and become eligible on the next run. Every exit path must release its reservation.

## Provenance

- Fair multi-source scheduling: `c1b7a23c4b5eced0c0d6ab432ba043dc6505e966`
- Dispatcharr capacity reservations: `db8ddce9153aacc8c02602e56efb87c8b007b76f`
- All active profiles and native resolver: `283da3aa636b443f39efe89a0216e4f7f837247d`

---

# ADR-004: Migrate releases from legacy branches to semantic tags

## Status

Accepted

## Date

2026-08-22

## Decision

Adopt the standalone release workflow defined by `matrix2669/workspace`: `main` contains stable releases, `dev` integrates the next release, beta tags `vMAJOR.MINOR.PATCH-beta.N` publish immutable test builds, and stable tags `vMAJOR.MINOR.PATCH` publish immutable normal releases.

The historical test tag `v0.3.5` anchors the exact commit previously published as version `0.3.5`. New test-channel versions use immutable tags from `dev` and are advertised only in `dispatcharr-plugins:dev-test`. Test tags do not create GitHub Releases.

Only an explicitly approved stable GitHub Release may be added to `dispatcharr-plugins:main`. Stream Sort remains absent from the stable manifest until that approval and release exist.

## Reason

The legacy workflow uses moving branches as version artifacts and has accumulated checkpoint branches. Dispatcharr requires a version increment to install an update, so immutable beta tags provide a controlled test channel without permanent version branches.

## Consequences

`VERSION`, `pyproject.toml`, `stream_sorter/plugin.json`, tests, tags, and registry metadata must remain synchronized. Legacy branch cleanup occurs only after full-ref verification proves no unique work would be lost and the registry no longer depends on the old source branch.

## Provenance

- Workspace workflow commit: `matrix2669/workspace@0ccd235`
- Current published source: `dev-test@283da3aa636b443f39efe89a0216e4f7f837247d`
- Related conversation: Simplify Plugin Versioning (`6a898c9e-1ffc-83ea-8fcc-b44788fea3c0`)
