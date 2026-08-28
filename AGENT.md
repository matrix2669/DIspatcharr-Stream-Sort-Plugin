# AGENT.md

## Workspace Standards Reconciliation Gate

Before any substantive work, locate the maintained local `matrix2669/workspace` checkout and run `<workspace>/scripts/reconcile-standards --check .` from this repository root. The workspace `AI-INSTRUCTIONS.md`, `AGENT-STANDARD.md`, and Git history must be available.

If `WORKSPACE-STANDARDS.yaml` is missing, pending, or stale, stop project work and run `<workspace>/scripts/reconcile-standards --diff .`. Review the standards change against this complete `AGENT.md`, `DECISIONS.md`, code/configuration contracts, dependencies, `BRANCHES.md`, `RELEASE.md`, upstream requirements when applicable, and related projects.

A contradiction blocks work. Ask focused follow-up questions to establish whether the changed standard, proposed work, new answer, or older accepted decision is authoritative; never choose silently. Record project-decision supersessions in `DECISIONS.md` and realign every affected artifact. Only after no contradiction remains, run `<workspace>/scripts/reconcile-standards --apply --confirm-reviewed-no-conflicts .`.

Missing workspace standards or Git history is a hard block. Standards exceptions require explicit user authorization and must be stated in a dedicated section of this file with exact scope, rationale, authority, approval date, and review/removal trigger; `DECISIONS.md` cannot waive workspace standards.


## Purpose

This repository owns the Dispatcharr Stream Sort plugin. The plugin analyzes streams already assigned to Dispatcharr channels, records runtime reliability, scores viable alternatives, and changes only `ChannelStream.order` when sorting is explicitly requested.

## Architecture

- `stream_sorter/plugin.py` exposes Dispatcharr actions, dynamic settings, background-job locking, progress state, and runtime event handling.
- `stream_sorter/incremental.py` decides which health, content, metadata, and throughput components require refresh and schedules work fairly across M3U accounts.
- `stream_sorter/analyzer.py` performs reachability, FFprobe metadata, and FFmpeg content checks.
- `stream_sorter/capacity.py` reserves Dispatcharr connection-pool slots across active M3U profiles and resolves the selected profile's rewritten live URL.
- `stream_sorter/execution_control.py` serializes every analysis entry point and coordinates cooperative cancellation across plugin workers and direct management-shell calls.
- `stream_sorter/reliability.py` stores URL-attributed playback evidence and produces conservative, decayed reliability scores.
- `stream_sorter/scoring.py` and `stream_sorter/sorter.py` calculate ordering and apply only the selected channel scope.
- `stream_sorter/throughput.py` measures delivery capacity and maintains compatibility with the legacy throughput cache.
- `stream_sorter/plugin.json` is the runtime plugin manifest. Its version must match `pyproject.toml` and `VERSION`.

Runtime state belongs under `/data`:

- `dispatcharr_stream_sort_analysis.json` — unified analysis and throughput cache;
- `dispatcharr_stream_sort_reliability.json` — runtime reliability history;
- `dispatcharr_stream_sort_status.json` — background analysis status;
- `dispatcharr_stream_sort_report.json` — dry-run/apply report;
- `dispatcharr_stream_sort_probe.lock` — cross-worker analysis lock.
- `dispatcharr_stream_sort_analysis_execution.lock` and companion control files — analyzer-level execution lease and cooperative cancellation state.

## Non-Negotiable Rules

- Do not create, delete, match, rename, regroup, or reassign streams or channels. Sorting may update only `ChannelStream.order` for the selected scope.
- Score only fresh URL-attributed reliability evidence. Retain legacy counters for visibility but do not score evidence whose stream attribution is unreliable.
- Count every active M3U profile that contributes capacity. Reserve and release the exact selected profile slot, and use Dispatcharr's native profile URL resolver so regex and credential rewriting remain authoritative.
- Never run simultaneous retry checks against the same M3U provider. Different providers may retry concurrently.
- Preserve capacity for active viewers. When no profile slot is available, defer the check without overwriting cached evidence.
- Select `/dev/shm/stream-sorter` only after a real create/write/delete test succeeds as the Dispatcharr runtime UID/GID; otherwise log the failure and use system temporary storage. Never infer writability from a root `docker exec` check.
- Retry a failed combined capture as combined work because it completed neither content nor throughput. Only split into content-only or throughput-only retries after a valid capture produced the other phase's reusable evidence.
- Keep long-running analyzer work outside the request worker and enforce one cross-worker analysis job with the probe lock.
- Container-side validation must add `/data/plugins` to `sys.path` and import `stream_sorter`, not `plugins.stream_sorter`.

## Development Guidance

Before changing behavior, review `README.md`, `DECISIONS.md`, `BRANCHES.md`, current Git history, the related `matrix2669/dispatcharr-plugins` registry entry, and relevant Dispatcharr APIs. Diagnose provider capacity or reliability behavior from fresh attributable evidence before changing scoring or scheduling.

Keep changes focused and add regression coverage. Treat live profile IDs, installed versions, counters, registry heads, and remote branches as time-sensitive and recheck them before deployment or publication.

## Branch Workflow

The repository uses the standalone workflow:

- `main` — stable, production-ready releases;
- `dev` — integration for the next release;
- `feature/*` and `fix/*` — short-lived work based on and returning to `dev`;
- `vMAJOR.MINOR.PATCH-beta.N` — immutable test tags from `dev`, with no GitHub Release;
- `vMAJOR.MINOR.PATCH` — completed feature or fix versions; they remain in the tagged-build channel until a GitHub Release is explicitly approved.

The `dispatcharr-plugins:dev` registry channel points to Stream Sort's newest approved immutable tag: beta during active testing, otherwise the latest completed stable tag whether released or not. It is not a source branch in this repository. The released registry must not contain Stream Sort until the user explicitly approves a GitHub Release. Track every current branch in `BRANCHES.md`; remove an entry only when its remote branch is deleted and its durable result is already captured in `CHANGELOG.md` or `DECISIONS.md` as applicable.

## Session Completion and Remote Continuity

GitHub is the authoritative continuation source. Start by fetching `origin` and resume from the exact remote head of the branch that owns the change. A repository-change request authorizes checkpoint commits and pushes to an isolated feature or fix branch. Before ending or handing off a session, preserve unrelated work, update branch/TODO/decision/dependency/validation records, run the applicable gates, commit every in-scope committable change, push every local commit, and verify through a fresh remote query that the exact GitHub head matches the intended local checkpoint. Incomplete work is pushed as explicit WIP with failed or unavailable validation recorded; never commit provider data, credentials, runtime `/data` state, excluded artifacts, or unrelated changes merely to clean the worktree.

The checkpoint does not authorize merging into `dev` or `main`, tagging, changing a registry channel, releasing, deploying, force-pushing, or deleting a branch. Report the work branch, `dev` integration, tag, registry, Release, and deployment states separately.

## Release Requirements

Follow `RELEASE.md`. Every tag must agree across `VERSION`, `pyproject.toml`, `stream_sorter/plugin.json`, and `dev` registry metadata; manifest tests enforce the repository-local values. Release notes and artifacts are added only when a GitHub Release is explicitly approved. Never move a published tag or replace a published artifact; issue a new version for corrections.

## Testing Requirements

Run before review:

```bash
python3 -m pytest
python3 -m compileall -q stream_sorter tests
```

Behavior affecting provider connections also requires controlled Dispatcharr integration validation that verifies reservation release on success, failure, timeout, and deferral. Release candidates require plugin installation/update validation and inspection of the exact archive referenced by the registry.

## Troubleshooting

- If a container import reports `ModuleNotFoundError: No module named 'plugins'`, add `/data/plugins` to `sys.path` and import `stream_sorter`.
- If an obsolete analysis owns the probe lock, identify and stop only that worker, then verify any stale profile counters are released.
- Treat cached branch, registry, deployment, and provider-capacity observations as stale until refreshed.

## Scheduler and Health-State Contracts

- Scheduled analysis state is `/data/dispatcharr_stream_sort_schedule_state.json`; configuration is versioned, every due minute is atomically claimed through Django cache, and every run loads current `PluginConfig.settings`.
- Every long-lived scheduler cycle that can use the ORM must call `close_old_connections()` before and after work.
- Health history and report output are `/data/dispatcharr_stream_sort_analysis.json` and `/data/dispatcharr_stream_sort_health_report.json`. History uses a 90-day window with a high safety cap.
- TTL recommendations are read-only at `/data/dispatcharr_stream_sort_ttl_recommendations.json` and must reject missing, empty, or older-than-seven-day reports.
- Do not write `Stream.is_stale` for analyzer health. Dispatcharr owns it as M3U refresh lifecycle state and does not use it to exclude playback candidates.
- Only a completed analyzer scan may clear confirmed-dead state; runtime playback evidence cannot promote a confirmed-dead cache entry.
- Preserve each stream's terminal status from scan start through every intermediate FFprobe, content, combined, and retry phase. The single terminal history row must compare against that snapshot rather than a phase-local status.
- Treat direct FFprobe video-packet bitrate below the configured 500 Kbps default floor as retryable provisional dead health. Never substitute aggregate container bitrate for this video floor; missing video bitrate is not a floor violation. Resolution changes trigger throughput immediately, while FPS and percentage-based bitrate changes require two consecutive direct observations outside the seven-result rolling median and median-absolute-deviation envelope.
- Recheck known placeholders with a one-second FFprobe gate, require the normal FFprobe path before clearing placeholder health, keep aggregate status `dead` with `error_type=placeholder_file` and report `health_class=placeholder`, keep placeholders on the exact base dead TTL, and exclude their observations from general health/TTL analysis while retaining a separate report section.
- Apply jittered throughput TTLs independently by status: healthy 24 hours, marginal/insufficient 12 hours, and unknown 4 hours by default. Non-placeholder dead streaks use exact base-TTL multipliers of `1x` for results one and two, `4x` for three through five, and `12x` thereafter.
- Keep the unified cache as current eligibility state, but do not persist derived `expires_at` values; calculate due state from `checked_at`, status, stable jitter, and current UI settings.
- Sorting and analysis must apply the same healthy/degraded/unknown throughput freshness settings. The throughput compatibility loader extracts evidence only; it must not expire unified entries or strip their current-state timestamps.
- `throughput_attempted` counts unique streams for which a throughput provider operation started; `throughput_checked` counts unique streams that produced and retained a numeric measurement. Capacity deferrals count as neither, and retries do not inflate either unique-stream total.

## Future Agent Checklist

- [ ] Read this file, `DECISIONS.md`, and `BRANCHES.md`
- [ ] Refresh remote branches, registry metadata, and relevant Dispatcharr state
- [ ] Confirm the active branch and release channel
- [ ] Create or refresh the branch record before substantive work
- [ ] Run automated and proportionate integration tests
- [ ] Synchronize every version source for a published beta or stable release
- [ ] Before deleting a branch, update durable documentation and remove its branch record
