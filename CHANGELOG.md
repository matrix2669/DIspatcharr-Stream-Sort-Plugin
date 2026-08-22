# Changelog

All notable user-visible changes to Dispatcharr Stream Sort are documented here.

Historical versions below were published through the legacy `dev-test` workflow and were not Git tags or GitHub Releases.

## Unreleased

### Added

### Changed

### Fixed

### Removed

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
