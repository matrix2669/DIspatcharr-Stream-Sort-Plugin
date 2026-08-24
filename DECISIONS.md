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

Adopt the standalone workflow defined by `matrix2669/workspace`: `main` contains released code, `dev` integrates the next version, beta tags `vMAJOR.MINOR.PATCH-beta.N` identify immutable test builds, and stable tags `vMAJOR.MINOR.PATCH` identify completed feature or fix work. A stable tag does not require a GitHub Release.

The historical test tag `v0.3.5` anchors the exact commit previously published as version `0.3.5`. Stream Sort's newest approved tag is advertised in `dispatcharr-plugins:dev`: beta while testing is active, otherwise the latest completed stable version. Tags do not create GitHub Releases.

Only an explicitly approved GitHub Release may be added to `dispatcharr-plugins:main`. Stream Sort remains absent from the released manifest until that approval and Release exist.

## Reason

The legacy workflow uses moving branches as version artifacts and has accumulated checkpoint branches. Dispatcharr requires a version increment to install an update, so immutable beta tags provide a controlled test channel without permanent version branches.

## Consequences

`VERSION`, `pyproject.toml`, `stream_sorter/plugin.json`, tests, tags, and registry metadata must remain synchronized. Legacy branch cleanup occurs only after full-ref verification proves no unique work would be lost and the registry no longer depends on the old source branch.

## Provenance

- Workspace workflow commit: `matrix2669/workspace@0ccd235`
- Current published source: `dev-test@283da3aa636b443f39efe89a0216e4f7f837247d`
- Related conversation: Simplify Plugin Versioning (`6a898c9e-1ffc-83ea-8fcc-b44788fea3c0`)

# ADR-005: Retain post-scan dead status with scoped stale state and retry behavior

## Status

Accepted

## Date

2026-08-24

## Decision

Keep dead-stream behavior in two phases: immediate intra-scan recovery via retry passes, then cooldown control via `dead_content_ttl_hours`.

- During an analysis run, network failures and provisional media/content failures (`0x0` dimensions, placeholder files, black video, frozen video, and silent audio) pass through the immediate retry pipeline (`Analyze` -> up to 3 retry passes) before the final result is retained.
- If a stream remains dead after retries and the scan completes, it is recorded as dead and marked stale in Dispatcharr (`stream.is_stale = True`).
- The dead stream is then excluded from being treated as non-stale until it is marked alive by a subsequent completed scan.
- No hard minimum dead TTL is enforced by code; scheduling is controlled by the configured UI value (`dead_content_ttl_hours`).

## Reason

This preserves recovery from transient transport/auth/login misses while preventing repeated immediate dead rechecks across scans. It also makes stale state explicit and persistent for scheduling and user visibility.

## Consequences

- `dead_content_ttl_hours` is the primary control for how long known-dead streams are deferred between full scans.
- Operational tuning focuses on keeping provider checks lower while still allowing quick revalidation when needed.
- `media_bitrate_relative_tolerance_percent` and `media_bitrate_absolute_tolerance_kbps` control when metadata changes trigger `media_changed` throughput rechecks (default 30% and 500 kbps). This makes `media_changed` behavior tunable without code changes while still ignoring normal ffprobe bitrate jitter.

## Provenance

- Stream Sort discussion thread: user-guided TTL tuning and stale-state clarification (`this session`).

# ADR-006: Read-only TTL recommendation action and stream-level TTL jitter strategy

## Status

Accepted

## Date

2026-08-24

## Decision

TTL tuning will be driven by a read/report action from collected health telemetry.

- The recommendation action (`recommend_ttls`) must only analyze and report; it must not mutate plugin settings.
- Recommendations use report-derived fields from the latest health trend report:
  - history coverage (`history_rows`, `history_span_hours`),
  - dead ratio (`dead_check_ratio`),
  - directional transition ratio (`status_changes_per_check_ratio`),
  - completed alive episode durations (`alive_episode_duration_hours`),
  - dead-to-alive recovery durations (`dead_recovery_duration_hours`),
  - actual check concentration (`check_concentration`),
  - per-hour dead concentration (`hourly_dead_ratio`),
  - unstable stream pattern indicators.
- Jitter is not applied to dead TTL, only to media analysis/throughput-like TTLs.
- Start with stream-level jitter only (per-stream randomization around TTL) to smooth expiry without account/provider-aware scheduling complexity.
- Add provider-aware spread only if empirical metrics show sustained concentration on specific accounts.

## Reason

Users asked to keep control over settings, but still want data-driven recommendations from historical behavior; stream-level jitter is the lowest-complexity initial control that reduces synchronized TTL expiry.

## Consequences

- Recommendation output needs to include confidence/rationale fields, not just scalar TTL suggestions.
- We should retain health trend artifacts over time so downstream decisions (including potentially removing low-quality streams) can use historical patterns.
- `media_bitrate_relative_tolerance_percent` and `media_bitrate_absolute_tolerance_kbps` are now first-class analyzer settings so users can tune `media_changed` sensitivity and measure recheck behavior with the recommendation report.

## Provenance

- Stream Sort design discussion in this session (`TTL objective, stale handling, recommendation scope, and jitter policy`).

# ADR-007: Provider-owned stale state and atomic scheduled analysis

## Status

Accepted; supersedes the `Stream.is_stale` persistence portion of ADR-005.

## Date

2026-08-24

## Decision

- Do not use Dispatcharr `Stream.is_stale` as analyzer health state. Dispatcharr defines and rewrites it during provider refresh, and playback selection does not exclude it.
- Keep confirmed-dead state in Stream Sort evidence, reports, and sorting until Dispatcharr exposes a supported playback exclusion contract.
- Require an atomic shared-cache claim for every scheduled cron minute because every uWSGI worker loads the plugin.
- Version schedule configuration so a running worker cannot overwrite a newer Apply or Disable action.
- Load current `PluginConfig.settings` for every scheduled run and close database connections on every scheduler cycle.
- Measure directional health episodes. Dead TTL guidance uses dead-to-alive recovery duration; health TTL guidance uses completed alive episodes; jitter guidance uses observed check concentration.
- Classify a stream as problematic only when it has at least four retained checks and more than 75 percent are dead.

## Consequences

- Stream Sort cannot guarantee that Dispatcharr playback skips a confirmed-dead stream without a compatible core feature.
- Scheduled scans remain safe when multiple uWSGI workers are running and when settings change between scans.
- Recommendation confidence remains low until enough history and directional transition samples exist.
