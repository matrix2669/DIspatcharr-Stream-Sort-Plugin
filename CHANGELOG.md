# Changelog

All notable user-visible changes to Dispatcharr Stream Sort are documented here.

Historical versions below were published through the legacy `dev-test` workflow and were not Git tags or GitHub Releases.

## Unreleased

### Added

### Changed

- Move the registry tagged-build channel from `dispatcharr-plugins:dev-test` to `dispatcharr-plugins:dev`.
- Distinguish completed stable versions from explicitly approved GitHub Releases.

### Fixed

### Removed

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
