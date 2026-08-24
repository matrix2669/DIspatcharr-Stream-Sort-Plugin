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
- The initial operating value for `dead_content_ttl_hours` is one hour. Dead TTL remains user-configurable and is not jittered.
- Operational tuning focuses on keeping provider checks lower while still allowing quick revalidation when needed.
- `media_bitrate_relative_tolerance_percent` and `media_bitrate_absolute_tolerance_kbps` control when metadata changes trigger `media_changed` throughput rechecks (default 30% and 500 kbps). A bitrate-only change must meet or exceed both thresholds; a 500 kbps difference alone does not trigger a recheck when it is less than 30 percent. This makes `media_changed` behavior tunable without code changes while still ignoring normal ffprobe bitrate jitter.

## Provenance

- Operator requirements review on 2026-08-24: TTL tuning, retry behavior, and stale-state clarification.

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
- Start with stable stream-level jitter only: each stream receives a deterministic multiplier from 0.70 through 1.30 around configured media-analysis and throughput TTLs. The multiplier is not redrawn on every scan. Dead TTL is exact and receives no jitter.
- Add provider-aware spread only if empirical metrics show sustained concentration on specific accounts.

## Reason

Users asked to keep control over settings, but still want data-driven recommendations from historical behavior; stream-level jitter is the lowest-complexity initial control that reduces synchronized TTL expiry.

## Consequences

- Recommendation output needs to include confidence/rationale fields, not just scalar TTL suggestions.
- We should retain health trend artifacts over time so downstream decisions (including potentially removing low-quality streams) can use historical patterns.
- `media_bitrate_relative_tolerance_percent` and `media_bitrate_absolute_tolerance_kbps` are now first-class analyzer settings so users can tune `media_changed` sensitivity and measure recheck behavior with the recommendation report.

## Provenance

- Operator requirements review on 2026-08-24: TTL objective, health-state ownership, recommendation scope, and jitter policy.

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

# ADR-008: Define durable health telemetry and scheduled-scan operating policy

## Status

Accepted. Retention, minimum evidence, and recommendation thresholds are provisional starting values that must be reviewed against collected data.

## Date

2026-08-24

## Decision

### Observation model

- A completed health observation is one stream's terminal `alive` or `dead` result from a completed analysis scan after the initial attempt and up to three immediate retry passes.
- The initial attempt and retries are not separate health observations. They must not inflate alive/dead counts, transition counts, or dead percentages.
- `unknown`, skipped, capacity-deferred, and incomplete individual probe results are excluded from alive/dead percentages and stream-removal eligibility. Completed probes from a stopped scan are retained. A dead result whose configured retry sequence was interrupted is stored as provisional retry-pending evidence and remains excluded until confirmation completes.
- Retry behavior is retained as separate reliability telemetry. Record whether the initial attempt failed, the number of retries needed to recover, the failure categories encountered, the terminal result, and whether all retries were exhausted.
- Reports must expose how often a stream needs retries before becoming alive. Repeated retry-assisted recovery is a reliability warning even when terminal health is alive, but it does not automatically change sorting or removal eligibility until collected data supports a scoring rule.

### Retention and aggregation

- Retain detailed observations and retry telemetry for 90 days.
- Retain daily aggregate health, transition, retry, concentration, and scheduling summaries for 365 days.
- Review storage volume and analytical value after operational use; either retention period may be reduced or extended when evidence supports it.

### Problematic-stream classification

- A stream may be recommended for removal only after at least 20 completed health observations spanning at least seven days, with more than 75 percent of those completed observations terminally dead.
- The seven-day span is intentionally conservative and provisional. Early reports may identify watch-list candidates, but they must not present them as removal-ready.
- The threshold supersedes ADR-007's initial minimum of four retained checks.
- Retry dependence is reported alongside terminal dead percentage so future analysis can determine whether chronically retry-dependent streams need a separate reliability threshold.

### Time and reporting

- Store timestamps in UTC.
- Display and analyze operator-facing time-of-day patterns in `America/New_York` unless the UI later provides an explicit reporting-timezone setting.
- Identify report entries using stable stream ID, channel name, and source name. URLs, credentials, tokens, and sensitive query parameters must be omitted or redacted.

### Scheduled operation

- The initial schedule is hourly at minute zero.
- Scheduled scans use one check at a time; the manual parallel-analysis setting does not increase scheduled concurrency unless the user explicitly enables scheduled parallel checks.
- Sorting runs after each completed scheduled analysis by default.
- Every scheduled run loads the current UI settings and channel filters at run start rather than retaining a stale settings snapshot from when the schedule was applied.
- If an earlier analysis or sort job is still running, skip the newly due run. Do not queue overlapping runs.
- Record and report every skipped schedule occurrence. A skipped hourly run is a tuning signal that TTLs, jitter, provider capacity, or selected scope may need adjustment.
- Media-analysis and throughput TTLs begin with stable per-stream jitter of up to 30 percent in either direction. Dead TTL remains exact and begins at one hour.

### TTL objective

- Prefer the longest evidence-supported TTLs that reduce provider checks without hiding meaningful health transitions.
- TTL recommendations remain read-only. Operators review and apply settings through the UI.
- Initial spreading is stream-level. Provider-aware spreading remains deferred until telemetry shows sustained account-specific pressure.

## Reason

Terminal scan outcomes are the appropriate unit for health analysis because immediate retries are intended to absorb transient network, authentication, and probe failures. Counting retries as separate dead observations would exaggerate failure rates, while discarding retry behavior entirely would hide streams that are technically alive but repeatedly unreliable.

Finite detailed retention controls storage growth, and longer daily rollups preserve enough history to study recurring health patterns and tune TTLs. Skipping overlapping schedules prevents a backlog from increasing provider pressure; the skip itself supplies evidence that scan duration and TTL concentration need adjustment.

## Rejected alternatives

- Count each retry as an independent health result: rejected because retries are recovery attempts within one scan.
- Ignore retry-assisted recovery: rejected because persistent retry dependence may reveal unreliable streams.
- Queue overlapping scheduled scans: rejected because queued work defeats the goal of fewer provider checks.
- Retain detailed history indefinitely: rejected because the analytical benefit is unproven and storage would grow without a bound.
- Apply TTL recommendations automatically: rejected because settings remain operator-controlled.
- Add provider-aware jitter immediately: deferred until stream-level evidence demonstrates that account concentration remains a problem.

## Consequences

- Health reports need separate terminal-health and retry-reliability sections.
- Daily rollups must preserve enough information to analyze dead ratios, transitions, retry dependence, time-of-day concentration, scheduled skips, and TTL expiry concentration after detailed rows expire.
- The initial 12-hour review is exploratory and cannot satisfy the seven-day removal threshold.
- Future threshold changes must cite collected evidence and supersede this ADR rather than relying on conversation history.

## Provenance

- Operator monitoring observations and approved policy review completed on 2026-08-24.
- Builds on ADR-005, ADR-006, and ADR-007; supersedes only ADR-007's four-check problematic-stream minimum.

---

# ADR-009: Serialize every analysis entry point and cancel cooperatively

## Status

Accepted

## Date

2026-08-24

## Decision

Preserve the existing viewer-aware provider reservation logic and established manual parallel-analysis behavior. Every analysis entry point, including direct management-shell calls, must acquire one shared analyzer-level execution lease in addition to the UI and scheduler job lock.

The UI stop action is cooperative and checkpoints completed work. It stops launching new probes, lets already-running probes finish or time out, releases their exact reservations through the normal capacity-manager path, saves every completed media and throughput result, and prevents post-analysis sorting from starting. It cancels only the active job and does not disable the recurring schedule.

A completed dead result receives the configured dead TTL only after its configured immediate retry sequence completes. If stopping interrupts that sequence, the dead result is saved as retry-pending with an effective dead TTL of zero and is immediately due on the next scan. Retry-pending dead evidence is provisional and excluded from terminal health percentages until confirmation completes.

Stream Sort must never clear Dispatcharr aggregate provider counters or reclaim capacity that may belong to a Dispatcharr viewer. It may release only reservations attributable to the active Stream Sort execution.

## Reason

The zero-capacity and deferred scan was caused by two Stream Sort analyses overlapping after a direct management-shell scan bypassed the UI and scheduler lock. The first scan legitimately held provider reservations, so the second scan saw those profiles as unavailable. This was not a regression in the provider-capacity calculation.

## Consequences

- Direct, manual, and scheduled analysis calls share one execution boundary.
- A competing call is rejected rather than probing or altering provider capacity.
- Cancellation may take up to the active probe timeout because in-flight probes are drained safely rather than killed.
- Once completed results begin committing, a late stop request reports that the scan is completing instead of claiming cancellation.

## Provenance

- Live scan and provider-counter investigation completed on 2026-08-24.
- Builds on ADR-003 and ADR-008 without changing their capacity, retry, scheduling, or telemetry decisions.

## ADR-010: Stage media analysis and combine overlapping provider work

**Status:** Accepted

**Date:** 2026-08-24

## Context

FFprobe metadata collection is fast and useful independently of content detection. The previous pipeline coupled it to a separate 6-second FFmpeg content probe, then opened the same provider again for an 8-second throughput measurement when both TTLs were due. Retry logs also showed only aggregate pass counts, hiding which streams repeatedly required recovery.

## Decision

- Run FFprobe metadata and reachability first without FFmpeg content detection, and finish its configured immediate retry sequence before downstream phases.
- Keep the existing 6-second remote FFmpeg content sample when content alone is due.
- Keep the raw wall-clock byte probe when throughput alone is due.
- When content and throughput are both due, capture one MPEG-TS stream-copy sample for 8 seconds of wall-clock provider time, calculate throughput from captured bytes and actual elapsed time, release provider capacity, and run content detection against the local sample.
- Preserve content and throughput timestamps independently. A completed throughput result remains valid if content needs confirmation retries or the scan is canceled.
- Retry content failures with the shorter content-only provider path rather than repeating a valid throughput capture.
- Log every individual retry result with the same identifying, media-statistic, progress, aggregate-health, pending-work, and ETA fields as an initial media result.

## Consequences

- Provider checks are reduced when content and throughput expire together, while metadata-only and throughput-only checks avoid unnecessary decoding.
- Stream-copy output bytes are a remuxed measurement rather than exact raw input bytes. Reports identify this measurement source so collected data can be compared with the existing raw probe before further threshold tuning.
- Provider reservations end after the remote capture and before local decoding, protecting viewer capacity and improving scan concurrency.
- FFprobe and content failures have separate immediate confirmation sequences, while terminal health history still records only the completed scan outcome and aggregate retry dependence.

## ADR-011: Use three scan-start TTL decisions and sustained playback evidence

**Status:** Accepted

**Date:** 2026-08-24

## Decision

- Calculate independent FFprobe, grouped content, and throughput due flags at scan start. The exact confirmed-dead TTL remains a separate recovery gate without jitter.
- Only a completed direct FFprobe resets the FFprobe TTL. Dispatcharr playback and imported stream statistics never reset it.
- Attributable clean Dispatcharr playback of at least 60 seconds satisfies grouped black/frozen/silent content evidence, labeled `dispatcharr_playback_assumed` because it is not direct frame/audio detection.
- Attributable clean unswitched playback of at least 300 seconds supplies sustained throughput from `total_bytes * 8 / runtime`. Ratios at or above `1.10x` nominal are initially healthy, `1.00x` through `1.10x` are marginal, and below `1.00x` is insufficient.
- Buffering-related Dispatcharr failover is immediate insufficient throughput evidence for the replaced stream and supersedes older successful measurements. Other connection or media failures remain health evidence.
- Retain every clean sustained ratio and failure for 90 days and report percentiles plus buckets around `1.03x`, `1.05x`, `1.07x`, and `1.10x`. The initial threshold is provisional and must be changed only after analyzing collected playback outcomes.

---

# ADR-012: Attribute playback by stream and provider and unify terminal-dead recovery

**Status:** Accepted

**Date:** 2026-08-24

## Context

Dispatcharr may resolve the same stream through multiple profiles under one M3U provider. Those profiles share the same base service and differ only by credentials, so their resolved URL hashes are expected to differ even though media and delivery behavior are equivalent. Review also found that unmeasured content probes could be treated as alive, degraded throughput bypassed TTLs, content checks could refresh a legacy FFprobe fallback timestamp, and a combined throughput result could overwrite a content-driven dead invalidation.

## Decision

- Attribute reusable Dispatcharr playback with the pair `stream_id + m3u_account_id`. Treat every profile and credential-only URL variation under that provider account as equivalent. Do not require URL-hash equality for playback evidence.
- Preserve observations without a matching provider ID for historical reporting, but do not use them to satisfy content or throughput TTLs.
- Persist the provider account with active content and throughput evidence. A provider reassignment invalidates that evidence immediately; profile or credential changes under the same provider do not.
- Treat remote FFmpeg content timeouts, exceptions, and nonzero exits as provisional retryable failures. Only a completed initial check plus the configured immediate retries can establish terminal dead content.
- Use the exact Dead stream TTL without jitter for marginal, insufficient, and unknown throughput evidence. Healthy throughput continues to use the configured jittered Throughput TTL.
- After retries establish that the latest completed phase is dead, put FFprobe, content, and throughput behind one exact dead-TTL recovery gate. A canceled retry sequence remains retry-pending with an effective TTL of zero.
- A content-dead result invalidates active throughput evidence even when the shared capture delivered bytes successfully. The measurement may remain historical but cannot satisfy the active throughput TTL.
- Only direct FFprobe completion resets `ffprobe_checked_at`. Separated content checks must not update the generic legacy timestamp used as an FFprobe fallback.
- Provide separate **Reset Scan Statistics** and **Reset All Statistics** actions rather than a scope-setting toggle. Scan reset clears analysis/throughput caches, scan status, health reports, and TTL recommendations. All-statistics reset additionally clears runtime reliability and playback history. Neither action changes the cron schedule, plugin settings, provider configuration, channel order, or sort reports.
- Serialize statistics reset through the same cross-process execution lease as manual, scheduled, and direct analysis. Reset must also clear the legacy throughput migration cache so evidence cannot be restored on the next scan.
- Exclude locked Dispatcharr system M3U accounts from source-score settings and ignore any stale saved dynamic score for an account that is no longer operator-managed. Internal accounts do not represent provider preferences and must not influence sorting.
- Measure combined throughput only across the requested provider capture window, excluding FFmpeg shutdown/flush time. Treat an unexpectedly short successful exit as an incomplete retryable capture.
- When no enabled content detector applies to a stream, record a completed skipped content decision and satisfy the content TTL without reserving provider capacity or affecting sorting viability.

## Consequences

- Profile rotation does not discard valid playback evidence or cause credential-only attribution churn.
- Evidence cannot cross provider boundaries when a stream ID is reassigned or provider metadata changes.
- Degraded streams no longer create direct throughput probes on every scheduled scan.
- Every confirmed-dead path follows the same recovery cadence, while interrupted retries remain immediately due.
- Operators can deliberately start a new measurement window without rebuilding plugin configuration or schedules.

## Rejected alternatives

- URL-hash attribution for playback: rejected because profile credentials change the resolved URL without changing the underlying provider stream.
- Stream-ID-only attribution: rejected because it could reuse evidence after the same ID moves to another provider.
- Immediate rechecks for every degraded throughput result: rejected because it defeats the provider-check reduction goal and the configured dead recovery cadence.
- A reset-scope settings toggle: rejected because separate explicit actions make the destructive scope visible at click and confirmation time.
- Clearing schedules or sorting configuration during statistics reset: rejected because those are operational settings, not collected evidence.
- Exposing or scoring locked Dispatcharr system M3U accounts: rejected because they are internal ownership records rather than operator-managed providers.

## Provenance

- Operator clarification and post-implementation review decisions completed on 2026-08-24.
- Supersedes ADR-011 only where playback attribution and nonhealthy throughput cadence are made more precise; all other ADR-011 thresholds remain provisional and active.
