# Agent Guide

This file is the operational guide for AI agents and developers working on
Dispatcharr Stream Sort. User-facing setup and behavior belong in `README.md`;
durable architectural choices belong in `DECISIONS.md`; release history belongs
in `CHANGELOG.md`.

## Purpose and boundaries

Stream Sort analyzes streams already assigned to Dispatcharr channels, ranks
them, and changes only `ChannelStream.order` when the user explicitly runs a
sorting action. It must not create, delete, match, rename, or move streams or
channels, and it must not alter EPG or channel-group assignments.

Keep the plugin self-contained. It may use supported Dispatcharr APIs and
models at runtime, but it must not require patches to Dispatcharr core.

## Architecture

- `stream_sorter/plugin.py` is the Dispatcharr entry point. It defines actions,
  dynamic settings, background-job locking, status reporting, and runtime event
  subscriptions.
- `stream_sorter/incremental.py` decides which independent cache components are
  stale, imports trustworthy Dispatcharr playback evidence, balances work
  across providers, and coordinates media and throughput checks.
- `stream_sorter/analyzer.py` runs FFprobe/FFmpeg media and content checks,
  classifies failures, and persists compatible stream statistics.
- `stream_sorter/throughput.py` measures delivery capacity. Throughput is not
  the stream's encoded video bitrate.
- `stream_sorter/capacity.py` reserves provider capacity across every active M3U
  profile, resolves the selected profile's native URL, and releases the exact
  reservation that was acquired.
- `stream_sorter/reliability.py` records schema-2 runtime evidence under a file
  lock and attributes errors to the stream URL before using channel state.
- `stream_sorter/scoring.py` enforces viability and resolution tiers, then
  applies bounded additive scores within a tier.
- `stream_sorter/sorter.py` scopes channels, calculates reports, and atomically
  updates `ChannelStream.order`.

Runtime files are:

- `/data/dispatcharr_stream_sort_analysis.json` — unified analysis and
  throughput cache.
- `/data/dispatcharr_stream_sort_status.json` — current or most recent job.
- `/data/dispatcharr_stream_sort_reliability.json` — runtime event evidence.
- `/data/dispatcharr_stream_sort_report.json` — dry-run or applied-sort report.

The legacy throughput cache is migration input only; do not make it a second
source of truth.

## Non-negotiable behavior

1. Only reorder streams already assigned to channels. Preserve dry-run support
   and atomic order updates.
2. Viability is the first hard tier; resolution is the second hard tier.
   Additive signals may reorder candidates only inside the same hard tiers.
3. Reliability scoring uses fresh schema-2 evidence only. Preserve the 14-day
   half-life, the minimum of 1,800 playback seconds or three starts, and the
   bounded `-20..+20` contribution. Legacy schema-1/v0.2 counters remain
   visible but must not affect ordering.
4. Resolve `channel_error` against its reported URL before cached channel
   state. Do not count the narrow switch-internal reconnect pattern as a stream
   failure.
5. Treat metadata, reachability, content validation, and throughput as
   independently fresh components. A reused playback session proves only what
   its qualification rules support.
6. Count capacity from all active profiles for a provider. Acquire and release
   the exact selected profile/credential reservation, and use Dispatcharr's
   native profile URL resolver so regex and credential rewriting are retained.
7. Different providers may be tested concurrently. Retry/recheck work for the
   same provider must run one at a time.
8. A missing content-check FFmpeg binary may fail open at runtime, but changes
   to that path still require tests. Never expose provider credentials or raw
   sensitive URLs in logs or reports.
9. Preserve `LICENSE` and `THIRD_PARTY_NOTICES.md` when copying, packaging, or
   publishing the plugin.

## Development workflow

The current active integration branch is `dev-test`. It contains the tested
v0.3.5 implementation. The repository README describes promotion from
`dev-test` to a stable production branch named `dev`, but no remote `dev` branch
exists as of this review. `main` contains only the initial repository state.

Until the owner creates or designates `dev`:

- base ordinary development and documentation branches on `dev-test`;
- do not merge or open feature work against `main`;
- do not create, redefine, or force-update `dev` without an explicit owner
  decision;
- keep temporary CI-validation PRs distinct from release/promotion PRs. The
  existing PR history shows validation PRs being closed without merge after
  the exact tested commit was fast-forwarded to `dev-test`.

Before editing, review `README.md`, `DECISIONS.md`, `CHANGELOG.md`, the relevant
module tests, and recent branch history. Preserve unrelated work and keep
changes reviewable.

## Required validation

Use Python 3.11 or newer:

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```

The baseline at v0.3.5 is 77 passing tests. Add focused regression coverage for
every behavior change, then run the entire suite. Provider-capacity changes
also require real concurrency validation before promotion: simultaneous
real-time workloads, 10-second bracketing, and a 30-second boundary
confirmation, accounting for high-frame-rate streams by weighted pixel rate.

## Versioning and publication

- Keep the version in `pyproject.toml` and `stream_sorter/plugin.json` aligned.
- Update `README.md` when user-visible behavior or settings change.
- Update `DECISIONS.md` when an architectural constraint changes.
- Update `CHANGELOG.md` for releases and material behavior changes.
- The Dispatcharr plugin registry must pin the exact validated source commit;
  do not silently mutate an existing release artifact or registry entry.
- Inspect the packaged archive layout and third-party notices before release.

Related systems that must be considered are Dispatcharr's plugin/runtime APIs,
M3U profile connection pools and URL resolver, and the `dispatcharr-plugins`
registry. IPTV Checker is historical provenance only; Stream Sort owns its
built-in analyzer and must not regain a runtime dependency on it.

