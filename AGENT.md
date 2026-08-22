# AGENT.md

## Purpose

This repository owns the Dispatcharr Stream Sort plugin. The plugin analyzes streams already assigned to Dispatcharr channels, records runtime reliability, scores viable alternatives, and changes only `ChannelStream.order` when sorting is explicitly requested.

## Architecture

- `stream_sorter/plugin.py` exposes Dispatcharr actions, dynamic settings, background-job locking, progress state, and runtime event handling.
- `stream_sorter/incremental.py` decides which health, content, metadata, and throughput components require refresh and schedules work fairly across M3U accounts.
- `stream_sorter/analyzer.py` performs reachability, FFprobe metadata, and FFmpeg content checks.
- `stream_sorter/capacity.py` reserves Dispatcharr connection-pool slots across active M3U profiles and resolves the selected profile's rewritten live URL.
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

## Non-Negotiable Rules

- Do not create, delete, match, rename, regroup, or reassign streams or channels. Sorting may update only `ChannelStream.order` for the selected scope.
- Score only fresh URL-attributed reliability evidence. Retain legacy counters for visibility but do not score evidence whose stream attribution is unreliable.
- Count every active M3U profile that contributes capacity. Reserve and release the exact selected profile slot, and use Dispatcharr's native profile URL resolver so regex and credential rewriting remain authoritative.
- Never run simultaneous retry checks against the same M3U provider. Different providers may retry concurrently.
- Preserve capacity for active viewers. When no profile slot is available, defer the check without overwriting cached evidence.
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
- `vMAJOR.MINOR.PATCH-beta.N` — immutable test releases from `dev`;
- `vMAJOR.MINOR.PATCH` — immutable stable releases from `main`.

The `dispatcharr-plugins:dev-test` registry channel points to immutable beta tags from `dev`; it is not a source branch in this repository. Track every current branch in `BRANCHES.md`; remove an entry only when its remote branch is deleted and its durable result is already captured in `CHANGELOG.md` or `DECISIONS.md` as applicable.

## Release Requirements

Follow `RELEASE.md`. A release version must agree across `VERSION`, `pyproject.toml`, `stream_sorter/plugin.json`, Git tag, registry metadata, release notes, and artifact names; manifest tests enforce the repository-local values. Never move a published tag or replace a published artifact; issue a new version for corrections.

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

## Future Agent Checklist

- [ ] Read this file, `DECISIONS.md`, and `BRANCHES.md`
- [ ] Refresh remote branches, registry metadata, and relevant Dispatcharr state
- [ ] Confirm the active branch and release channel
- [ ] Create or refresh the branch record before substantive work
- [ ] Run automated and proportionate integration tests
- [ ] Synchronize every version source for a published beta or stable release
- [ ] Before deleting a branch, update durable documentation and remove its branch record
