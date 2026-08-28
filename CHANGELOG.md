# Changelog

## Unreleased

## 0.3.6-beta.14 - 2026-08-27

### Added

- Accept comma-separated stream-name shorthand and regex rules while retaining newline compatibility and regex-internal commas.

### Changed

- Reorder settings and actions around normal analyze/sort workflow, use concise inline help, and move detailed settings/file guidance to the README.
- Replace dynamic M3U numeric scores with integer selectors from -5 through +5; normalize legacy dynamic values to the nearest bounded choice without changing hard scoring tiers.

### Removed

- Remove the Files informational row from the settings page while retaining the complete runtime-file reference in the README.

## 0.3.6-beta.13 - 2026-08-27

### Added

- Add one channel-scope type selector with separate Analyze & Sort and Analyze Only filters.
- Add case-insensitive `*` and `?` wildcard matching for channel group or profile names while keeping ID matching exact.

### Changed

- Analyze the union of both scope lists while always excluding Analyze Only matches from Stream Sort ordering, including scheduled Analyze + Sort runs.
- Ignore removed group/profile scope settings immediately; empty new scope lists use the all-channel default.

## 0.3.6-beta.12 - 2026-08-26

### Added

- Append human-readable wall-clock runtime to analysis, analyze-and-sort, dry-run, and sort completion records.
- Break aggregate dead health into invariant placeholder and other-dead counts, and log per-result health class plus FFprobe mode.

### Changed

- Confirm known placeholders with one one-second FFprobe at the exact dead TTL, without immediate retries or downstream content/throughput checks.
- Require an inconclusive placeholder gate to pass the normal full FFprobe and immediate retry path, then require fresh content and throughput evidence after recovery.
- Preserve historical content and throughput evidence while placeholder health suppresses its active use.
- Relabel the Dispatcharr-required runtime reliability subscription as automatic-only and make manual invocation an explicit no-op because the current action schema cannot hide event subscriptions from the UI.

### Removed

- Remove **Maximum streams per analysis run** from settings and analysis behavior; existing saved values are ignored and channel group/profile filters remain the supported scope controls.

## 0.3.6-beta.11 - 2026-08-25

### Changed

- Treat video below the configurable 500 Kbps floor as retryable provisional dead health.
- Confirm percentage-based bitrate and FPS changes across a seven-result direct-FFprobe history while keeping resolution changes immediate.
- Use jittered 24-hour healthy, 12-hour degraded, and 4-hour unknown throughput TTLs.
- Back off consecutive non-placeholder dead results while keeping placeholders on the exact base TTL and rechecking them through a one-second FFprobe gate.
- Segment placeholder observations from general health and TTL analysis without removing them from raw reporting.
- Keep placeholders in aggregate dead health while exposing a distinct report classification, and preserve adaptive dead streaks across intermediate alive FFprobe results when content remains terminally dead.
- Use video-packet bitrate for the minimum floor, apply a robust median-absolute-deviation envelope to rolling bitrate changes, and require every non-placeholder one-second result to pass the normal FFprobe path.
- Separate placeholder retry and daily-rollup counters, remove derived throughput expiration metadata, and defer evidence-based throughput TTL recommendations until sufficient history exists.
- Make sorting and analysis share current status-specific throughput freshness instead of relying on stored expiration timestamps or the legacy 30-minute scorer TTL.

## 0.3.6-beta.10 - 2026-08-24

### Fixed

- Preserve each stream's pre-scan terminal health when recording the completed observation so multi-phase dead recovery produces accurate dead-to-alive and alive-to-dead reports.
- Separate unique throughput attempts from completed numeric measurements and keep `throughput_checked` aligned with retained `measured_mbps` evidence.
- Exclude attempted-but-unmeasured throughput work from fully cached counts without changing dead TTLs, retry budgets, provider capacity, or sorting behavior.

## 0.3.6-beta.9 - 2026-08-24

- Capture combined content and throughput samples in a writable runtime directory, preferring `/dev/shm/stream-sorter` and falling back safely when it is unavailable.
- Retry the complete combined capture when FFmpeg does not produce a sample, with per-stream failure logging on every retry pass.
- Keep content and throughput incomplete when capture fails instead of recording false completion timestamps or throughput evidence.
- Mark streams dead after exhausted combined retries and gate all subsequent checks with the configured dead-stream TTL.
- Preserve `throughput_missing` as the reason for a fresh baseline instead of misreporting the first throughput check as `media_changed`.
- Retain successfully completed analysis when a scan is stopped while leaving unconfirmed dead results immediately retryable.

## 0.3.6-beta.8 - 2026-08-24

### Fixed

- Bound combined throughput/content capture storage to active workers, analyze and delete samples through a backpressured local pipeline, and automatically use sufficiently sized `/dev/shm` storage without requiring it.
- Make cancellation checks fall back to the persisted execution token so Stop Current Scan remains visible across plugin import contexts and stops replacement probes while active work drains.

## 0.3.6-beta.7 - 2026-08-24

### Added

- Add separate guarded **Reset Scan Statistics** and **Reset All Statistics** actions while preserving schedules, settings, provider configuration, and channel order.

### Changed

- Split incremental analysis into FFprobe metadata/reachability, content-only FFmpeg, combined content-plus-throughput capture, and throughput-only phases.
- Reuse one 8-second wall-clock-bounded FFmpeg stream-copy capture when both content validation and throughput are due, then release provider capacity before decoding that sample locally.
- Log every individual FFprobe and content retry with the same stream health, media statistics, progress, totals, and ETA fields used by initial media checks.
- Attribute reusable Dispatcharr playback by stream ID and M3U provider account while treating profiles and credential-only URL differences as equivalent.
- Apply the exact dead-stream TTL without jitter to marginal, insufficient, and unknown throughput evidence.
- Exclude locked Dispatcharr system accounts from M3U source-score controls and ignore their stale saved dynamic scores.

### Fixed

- Preserve independent content and throughput timestamps and completed phase results when a later phase fails, retries, or is canceled.
- Use direct FFprobe completion as the only FFprobe TTL reset while allowing clean Dispatcharr playback to satisfy content and sustained-throughput TTLs.
- Retain playback Mbps, nominal ratios, duration, source, failures, percentiles, and ratio buckets so the initial `1.10x` sustained threshold can be tuned from observed clean sessions.
- Route remote content execution failures through confirmation retries instead of treating unmeasured content as alive.
- Put FFprobe, content, and throughput behind the same exact dead-TTL recovery gate after retries confirm a terminal dead result.
- Preserve content-driven throughput invalidation when a combined capture has adequate delivery but black, frozen, or silent content.
- Prevent separated content checks from refreshing legacy FFprobe fallback timestamps.
- Serialize statistics reset with every analysis entry point and clear the actual legacy throughput migration cache so reset data cannot be restored on the next scan.
- Enforce the current M3U provider account on cached content and throughput evidence while continuing to reuse evidence across credential-only profile changes.
- Measure combined throughput at the capture boundary rather than after FFmpeg shutdown, and reject captures that end before the requested sampling window.
- Complete content TTL decisions without provider scheduling when no enabled detector applies to the stream.
- Align dead-content regression tests with the exact dead-TTL and retry-pending contract.

All notable user-visible changes to Dispatcharr Stream Sort are documented here.

Historical versions below were published through the legacy `dev-test` workflow and were not Git tags or GitHub Releases.

## 0.3.6-beta.6 - 2026-08-24

### Added

- Add a **Stop Current Scan** action that checkpoints completed probes, stops new probe launches, and drains already-active probes so their exact provider reservations are released normally.

### Changed

- Move the registry tagged-build channel from `dispatcharr-plugins:dev-test` to `dispatcharr-plugins:dev`.
- Distinguish completed stable versions from explicitly approved GitHub Releases.
- Make the accepted 30% per-stream TTL jitter the UI and runtime default, retain detailed health rows with a cap that accommodates hourly checks for 90 days, and retain scheduled outcomes for 365 days.

### Fixed

- Enforce a shared execution lease inside the analyzer so direct management-shell calls cannot overlap UI or scheduled scans by bypassing the plugin job lock.
- Preserve the established viewer-aware provider capacity and manual parallel-scan behavior; cancellation never clears aggregate Dispatcharr provider counters.
- Preserve completed media and throughput results when a scan is stopped, skip post-analysis sorting, and give unconfirmed dead results an effective zero dead TTL until their retry sequence completes.

### Removed

## 0.3.6-beta.5 - 2026-08-24

### Changed

- Treat analyzer-produced `0x0` video dimensions as a provisional dead result and run it through the configured immediate retry passes.
- Run placeholder-file, black-video, frozen-video, and silent-audio detections through the same immediate confirmation retry queue before retaining a dead result.

## 0.3.6-beta.4 - 2026-08-24

### Added

- Add a Health Report action with problematic-stream, directional-transition, recovery-duration, and check-concentration results.
- Add atomic cross-worker schedule claims, final scheduled-job status, and schedule generation guards.
- Add 90-day time-based health history with bounded safety retention.

### Changed

- Load current saved UI settings for every scheduled run instead of retaining a stale snapshot.
- Use standard cron Sunday and day-of-month/day-of-week semantics.
- Base provisional TTL guidance on completed alive episodes and dead-to-alive recoveries rather than current check cadence.
- Treat Dispatcharr `is_stale` as provider-owned lifecycle state and keep analyzer health report-only until Dispatcharr provides a supported playback exclusion contract.

### Fixed

- Actually defer confirmed-dead streams until Dead stream TTL expiry without applying jitter.
- Prevent runtime playback evidence from clearing a newer confirmed-dead result.
- Apply configured bitrate change thresholds to newer Dispatcharr metadata.
- Treat missing bitrate as unknown instead of a measurable change.
- Prevent duplicate scheduled launches and stale schedule-state overwrites across uWSGI workers.
- Close database connections used by long-lived scheduler threads.
- Write empty and custom-path health reports safely, and batch throughput cache persistence.
- Correct status-change ratios, the greater-than-75-percent problematic-stream threshold, and report coverage beyond the unstable top 20.
- Restore a green isolated test path by making health-report destinations injectable.

### Removed

- Remove writes to Dispatcharr's provider-refresh `Stream.is_stale` field.

## 0.3.6-beta.3 - 2026-08-24

### Added

- Add cron-based scheduled analysis with configurable automatic sort and optional parallel-worker control.
- Add dead-stream TTL, media bitrate change thresholds, and health trend reporting and TTL recommendations.

### Changed

- Reduce live scan churn by marking dead streams stale only after scan completion.
- Add stream-level TTL jitter guidance and keep dead TTL retries excluded from jitter.
- Update scheduler and analysis behavior to support scheduled post-analyze sorting while keeping immediate retry semantics within each analyze run.

### Fixed

- Keep recommendation outputs read-only and separate from direct setting mutation.

### Removed

## 0.3.6-beta.2 - 2026-08-22

### Changed

- Clarify that Dispatcharr plugin test tags are published only to the dev-test registry without creating GitHub Releases.
- Keep Stream Sort absent from the stable registry until a stable GitHub Release is explicitly approved.

This beta changes project and distribution metadata only; Stream Sort runtime behavior is unchanged from `0.3.5`.

## 0.3.6-beta.1 - 2026-08-22

### Changed

- Migrate the testing channel from the moving `dev-test` source branch to the immutable `v0.3.6-beta.1` prerelease tag.
- Add repository-local project, branch, decision, release, and version documentation.
- Add a test that keeps `VERSION`, `pyproject.toml`, and `stream_sorter/plugin.json` synchronized.

This beta changes project and distribution metadata only; Stream Sort runtime behavior is unchanged from `0.3.5`.

## 0.3.5 - 2026-08-20

### Changed

- Count all active M3U profiles when determining analyzer capacity.
- Resolve probe URLs with the selected profile's native Dispatcharr rewrite logic.
- Limit retries to one simultaneous recheck per M3U provider while allowing different providers to retry concurrently.

## 0.3.4 - 2026-08-20

### Changed

- Reserve and release Dispatcharr connection-pool capacity for analysis so active viewers reduce available analyzer slots.

## 0.3.3 - 2026-08-20

### Changed

- Allow throughput probes to run concurrently within the configured worker and provider limits.

## 0.3.2 - 2026-08-20

### Changed

- Balance parallel analysis across M3U sources.

## 0.3.1 - 2026-08-20

### Added

- Add conservative runtime reliability scoring with fresh URL attribution, evidence thresholds, decay, and bounded score contribution.
- Reuse qualifying Dispatcharr playback for reachability and newer stream statistics for media metadata.

## 0.2.5 - 2026-08-17

### Fixed

- Wire plugin actions directly to the cache-aware incremental analyzer.

## 0.2.4 - 2026-08-17

### Added

- Collect runtime playback reliability telemetry.

### Fixed

- Suppress reconnect events generated internally by normal stream switching without suppressing genuine reconnects.

## 0.2.2 - 2026-08-17

### Added

- Reuse fresh Dispatcharr media metadata without opening another provider connection.

## 0.2.1 - 2026-08-17

### Added

- Add the cache-aware incremental analyzer with independent health, metadata, content, and throughput refresh policies.
- Unify analysis and throughput evidence while safely migrating fresh legacy throughput cache data.

## 0.1.1 - 2026-08-17

### Added

- Add channel group and channel profile scope filters.
- Render configurable scores for each M3U account.

## 0.1.0 - 2026-08-17

### Added

- Initial plugin with health, quality, throughput, and name/source-based stream ordering.
