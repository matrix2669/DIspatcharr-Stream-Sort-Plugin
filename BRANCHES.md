# Branches

This ledger records every current branch on `matrix2669/DIspatcharr-Stream-Sort-Plugin`. GitHub remains authoritative for live refs, commits, pull requests, and checks.

Before deleting a branch, record user-visible results in `CHANGELOG.md` and durable architectural rationale in `DECISIONS.md` when applicable, then remove its index row and detailed record.

## Branch Index

| Branch | Type | Status | Base | Target | Purpose |
|---|---|---|---|---|---|
| `main` | long-lived | active | historical repository root | approved stable releases | Reserved stable-release branch; currently carries the released `0.3.6` baseline. |
| `dev` | long-lived | active | `main` | `main` | Integrates and validates the next beta and stable release. |
| `feature/storage-efficiency-review` | short-lived | active | `dev` at `8d3afff` | `dev` | Reduce avoidable persistence and evaluate PostgreSQL storage and shared captures. |
| `feature/analysis-telemetry-retention` | short-lived | integrated | `dev` | `dev` | Retain direct throughput/media-change evidence and applied-sort movement history. |
| `feature/issue-7-settings-controls` | short-lived | active | `dev` | `dev` | Simplify settings/action layout, bound M3U scores, and add comma-separated name rules. |
| `fix/v0.3.6-beta.6-analysis-control` | short-lived | active | `dev` | `dev` | Prevent overlapping analysis entry points, add safe cancellation, and publish immutable beta.6. |
| `fix/v0.3.6-beta.10-telemetry-integrity` | short-lived | active | `dev` | `dev` | Preserve scan-boundary health transitions and distinguish attempted throughput probes from completed measurements. |

## Branch Records

### `feature/storage-efficiency-review`

- Current source status: ADR-024 implementation is independently reviewed at `52483cb3480d4a03089a1463be95634488b743c5`; all R1-R5 findings closed. Accepted implementation checkpoint `db1834501c912a083737537d3e7d2a51766b5cb2` passed original full CI run `41109866936`: media/unit job `124249192216` and PostgreSQL job `124249191782` (22 passed in 4.56s). Local validation was 235 passed, one PostgreSQL module skipped. The controlled Dispatcharr rehearsal below passes migration/restore/ranking invariants; capture equivalence and connection reduction are not established. JSON/capture-disabled defaults remain unchanged; production is not migrated or deployed.
- Base: `dev` at `8d3afff3bdd0bc0dbc780f211065d81f4da75149`.
- Scope: ten-second UI status persistence and startup RAM imports under ADR-023; subsequently authorized phase-boundary PostgreSQL batches, targeted re-query and bounded capture reuse under ADR-024, with explicit schema/migration lifecycle and isolated validation.
- Ownership: one bounded implementation worker owns plugin/incremental changes and focused tests; coordinator owns documentation and integration; independent review follows completed implementation.
- Delivery: isolated checkpoint only; implementation and disposable schema/fixture validation are authorized. Production schema initialization, live migration/provider testing, deployment, tag, registry update and integration remain separately gated.
- Acceptance: focused and full test suites, compilation, documentation checks, standards reconciliation, finished independent review, exact remote checkpoint. Database permissions, migration/backup behavior, crash-fencing tests, and capture equivalence remain future acceptance work.
- Validation: 77 focused/status/control tests and the 174-test full suite pass; forced warning-as-error compilation, diff checks, project validation and standards reconciliation pass. Initial CI run `41096827144` passed at `ecc49de`. Independent review found two progress-parser issues (alternating combined phases bypassed throttling; combined/throughput retry prefixes were omitted); both are fixed with regression tests and closed by the same independent reviewer at `b6ea1f5b5cde3ac42d716a193064f54f1d0a866b`. Final checkpoint CI is collected separately for the exact published head; prior CI is not reused as proof of that head.
- Phase result: bounded JSON write reductions are implemented and independently reviewed. PostgreSQL lifecycle evaluation is documented against installed Dispatcharr 0.33.0; operator selected evaluation of a plugin-owned runner. Database prototype, production DDL/backup verification, migration, batch metadata update behavior and shared-capture equivalence remain pending. No production changes or release were performed.
- ADR-024 continuation checkpoint: experimental PostgreSQL adapter, explicit schema/import/retention actions, phase-state writes/targeted queries, run-token heartbeat/fencing, playback isolation and bounded raw capture reuse are implemented but not accepted. Local suite: 195 passed, one skipped PostgreSQL module; 13 real database tests await the new PostgreSQL16 CI service. Generated-media fixtures pass. Known blockers: forced placeholder recovery reason after phase re-query, provider reservation held during local sample processing, PostgreSQL sort/report/TTL action support and legacy playback/sort-history import. Backend remains opt-in and rejects unsupported actions; do not enable it in production. Next implementation wave must close these blockers before acceptance review.
- Continuation implementation closes those known wiring blockers: forced recovery checks survive targeted re-query; provider reservations end before local capture analysis; PostgreSQL main analysis/sort/report/TTL workflow and legacy playback/sort import are wired. Schema version 2 upgrades explicitly from version 1. Local validation: 206 passed, one skipped PostgreSQL module, forced warning-as-error compilation and whitespace checks pass. The module now contains 19 real PostgreSQL tests. WIP run `41101708660` for `b6fa6a0` remained queued without test steps through a seven-minute bounded collection; this is unavailable evidence, not a test failure or pass. Independent review and final-revision database/media CI remain required. Legacy throughput-only compatibility entry points remain explicitly unsupported with PostgreSQL and are not manifest actions. Production validation, backup/restore and measured I/O/runtime acceptance remain pending.
- Exact `bb4cd515ebf802090bfb248b1d6ee044e9f9c582` CI run `41104169453` passes media/unit job `124228978710` and PostgreSQL job `124228978257`; raw database job summary confirms 19 tests passed. Independent review blocks acceptance on R1 content-dead recovery checks lost at re-query; R2 incomplete-content samples bypassing retries; R3 total acquisition deadline/cancellation; R4 reset/playback interleaving; R5 incomplete/canceled scope refreshing TTL-report age. These findings are assigned to the original owner for one batched repair and targeted reviewer verification. Passing prior CI does not prove them fixed.
- Review resolution: R2-R5 and configurable RAM admission close at `80f68189584c6560a6c082d7ac14c52c789fcd05`. R1 required a second repair: durable `content_recovery_required` survives intervening exhausted media failures and clears only after successful measured non-skipped applicable content. Separate JSON/PostgreSQL-contract multi-scan regression and a real PostgreSQL durability test cover it; targeted review closes R1 at `52483cb`. The new acquisition child has a parent-enforced total deadline and bounded cancellation, receives URLs/headers on stdin, and is killed/reaped before provider capacity is returned. No external download tool is required.

#### Controlled Dispatcharr rehearsal — 2026-10-30

- User authorized the migration rehearsal and comparison scans. Executed accepted source `db1834501c912a083737537d3e7d2a51766b5cb2` separately under runtime UID/GID 1000:1000 on Dispatcharr 0.33.0 / PostgreSQL 17.11, in two restored disposable databases on the existing server. With active playback, the rehearsal avoided replacing the production plugin. Production settings, installed plugin and tables remained unchanged; the temporarily paused hourly `18 * * * *` schedule was restored. Native provider capacity controls remained enabled; all measured reservations balanced and no scan reported a capacity deferral.
- Private operation `stream-sort-rehearsal-20261030-2` completed 19:14:46–19:34:23 UTC with all nine runner stages passing, no runner warnings, and both disposable databases dropped. Full dumps, original JSON snapshots, source manifest, summary and comparison logs remain private on the host; do not commit runtime data or credentials. First attempt `stream-sort-rehearsal-20261030-1` stopped at its database-routing guard before schema initialization or provider checks. Clearing the rehearsal process's alias-keyed gevent pool corrected isolation; the second attempt reused the frozen backup/snapshots rather than repeating production acquisition.
- Initial full database dump: 67,854,620 bytes, SHA-256 `d1cd1b8f65a11f47aee2d95215699ffe0737794c80378bda3e97877e2c97bbbe`. Restored 21,202 Dispatcharr streams, 503 channels and 1,318 attachments. Explicit schema-v2 initialization was repeatable. Legacy import plus repeat-import/parity verification took 214.660 seconds: 3,193 current stream records, 232,537 history rows, 88,257 daily rollups, 456 playback rows, 1,271 sort runs and 18,108 channel-sort rows. Every current record matched its legacy representation; repeat import changed no imported evidence and source files remained unchanged.
- Migrated full dump: 89,464,052 bytes, SHA-256 `a0b2e87a91b7e2842e1419ebcb47fefbf8c434b4face16631dfc85863bcb23ae`. Restore reproduced all plugin-table row counts and fingerprints. This proves the explicit custom-dump/restore procedure, not an uninspected household backup service. JSON/PostgreSQL dry-run ranking matched on all 188 evaluated TV channels before probing.

| Same eight streams, two per four providers, serial and forced due | JSON, capture reuse off | PostgreSQL, capture reuse on |
|---|---:|---:|
| Wall time | 160.078 s | 107.620 s |
| Final health alive/dead | 8 / 0 | 8 / 0 |
| Metadata/content/throughput measured | 8 / 8 / 8 | 8 / 8 / 8 |
| Throughput healthy/marginal/insufficient | 6 / 1 / 1 | 8 / 0 / 0 |
| Logical source-opening helper attempts | 23 | 23 |
| Parent process `write_bytes` | 233,115,648 | 40,960 |
| Rehearsal database commits / rollbacks | 114 / 0 | 157 / 0 |

- Paired runtime was 32.8% lower, but storage and capture mode both changed and the tests were sequential. Three captures satisfied reuse; five required direct metadata/content/throughput fallback. The current summary does not identify each rejection cause. Different retry counts and live stream variation confound attribution. Logical helper attempts are not instrumented HTTP requests/connections. The two changed throughput statuses do not prove measurement equivalence; raw acquisition and the prior remux path also differ. Parent `write_bytes` excludes PostgreSQL server writes and is not physical Ceph I/O. Cluster-wide WAL includes concurrent service traffic. No measured peak-memory or reusable-sample byte/playable-duration distribution was collected. Do not call this capture acceptance or claim fewer provider connections.
- Full PostgreSQL/capture-enabled serial scan used current configured TTLs and selected 1,145 streams: runtime 627.823 seconds (10m27s), 64 metadata checks, 10 content checks, 22 throughput attempts / 21 numeric results, 1,057 fully cached, zero capacity deferrals. Terminal health: alive 974, dead 171 (placeholder 91, other dead 80). Throughput: healthy 926, marginal 12, insufficient 32, unknown 4. Imported 105 Dispatcharr metadata updates and 57 playback observations. Provider-opening helper attempts: 106. Parent writes: 212,992 bytes; database commits 532, rollbacks zero. Clone-only sort changed 13 channels / 28 rows; Health Report and Sort History returned `ok`.
- Previous production scan: 1,145 streams in 1,548.679 seconds (25m48s), metadata 190, content 18, throughput attempts 36 / measurements 34, fully cached 913; health alive 976, dead 169 (placeholder 90). Different due workloads, wall times and earlier controlled-arm observations prevent treating the full-run runtime difference or health changes as backend causation. Direct management-shell execution did not measure UI-status throttling.
- Disposition: database migration, repeatability, explicit backup restoration, ranking parity and full normal workflows passed on the installed platform. Keep capture reuse disabled for deployment until fallback-reason telemetry and bounded equivalence/connection/memory measurements close the above gaps. Live forced cancellation, production backup automation coverage and real cutover/rollback procedure remain adoption gates; disposable CI cancellation/fencing tests do not replace those operational checks. No production schema migration, installation, version bump, dev/main merge, tag, registry publication or Release occurred.


#### Cutover checks and capture diagnostics continuation — 2026-10-30

- Authorized scope: remaining PostgreSQL cutover checks and capture fallback telemetry; capture reuse remains disabled. No production installation/backend selection, dev/main integration, version bump, registry publication or release is included.
- Private operation `stream-sort-cutover-20261030-1` ran accepted source `9eb9b749fc34f30f9d89f7154614fbf04f6115e3` (unchanged runtime from `db18345`) 19:54:49–19:56:25 UTC, all five stages pass in 96.158 seconds. Reused the prior immutable migrated archive, restored two disposable databases, and retained native viewer-aware provider controls. A shell-quoting startup error was corrected before runner initialization, schedule changes or provider attempts; no scan was repeated for it.
- Cooperative stop before retries: two newly completed metadata results persisted; one explicitly injected `stream_unreachable` result retained `retry_pending=true` with next reason `status_dead_retry_pending` and no confirmed terminal dead observation. The injected failure consumed no provider request; native reservation acquisition/release remained real. Stop after the first completed combined check retained one measured throughput and one measured content result; canceled scope did not refresh successful full-scan freshness. Both runs kept capture reuse false and released database/provider leases.
- Built-in Dispatcharr archive/restore reproduced every plugin-table fingerprint, including the new completed and pending results. Archive size 88,371,785 bytes; SHA-256 `7904addf4e721931bcfe1cbc63ea2d26dfdc0246d277bf6b964929a70440f1dc`. Clone-only backend configuration switched back to JSON only after preserving/recovering the PostgreSQL archive; no reverse JSON exporter or older-plugin compatibility is claimed. Production settings/plugin/schema were preserved, original schedule restored, both disposable databases dropped, no warnings or unbalanced reservations.
- Backup evidence: installed full `pg_dump -Fc` service, daily 03:00 America/New_York task, retain three, recent archive/execution proof. Operator confirms daily LXC/PBS backup; PBS restoration not exercised. Cancellation and same-path backup/restore gates are closed; documented production cutover and deliberate recovery remain separate operations. Capture equivalence, actual connection reduction and memory measurements remain open.
- Telemetry implementation: finite acquisition/local-validation reasons, numeric partial evidence/provenance, safe per-stream system logs and bounded RAM counters returned with each scan (including disabled/no-attempt state). No telemetry files, raw error/credential exposure, schema/default/health/TTL/provider-flow changes. Local worker validation: 59 focused and 280 full tests pass, one PostgreSQL module skipped; warning-as-error compile and whitespace checks pass. A loopback success test initially failed, then passed isolated and both final suites. Independent completion review of tree `fbd88a51b2552de5481a288723ba2e7ff7f38e19` against `9eb9b74` found no substantive correctness issues across all 12 scoped files. Final original full CI for the published checkpoint is collected at closeout; prior CI is not reused for this new source.

### `feature/analysis-telemetry-retention`

- Type: short-lived feature branch
- Status: integrated into `dev` for immutable `v0.3.7-beta.1` publication; feature checkpoint remains available on GitHub
- Base and target: `dev` at `c1b1172177c87c93ab5edeec2d45870c9328ed81`
- Purpose: use the first six days of stable scheduled evidence to make media-change, throughput, and repeated sorting behavior directly measurable
- Scope: direct throughput retention, exact media-change causes, applied-sort movement and score-delta retention, Sort History action, reset integration, focused tests, user documentation, changelog, and decision record
- Exclusions: probing thresholds, placeholder TTL behavior, scoring weights, registry metadata, version bump, deployment, and persistent problematic-stream notifications
- Completion: focused and full tests, Python compilation, workspace validation, contradiction review, chat-independence review, and remote checkpoint before separate integration or publication approval
- Validation: 164 full-suite tests and 67 initial focused tests passed; Python compilation, diff checks, governed-project validation, and standards reconciliation passed on 2026-09-02.

### `feature/v0.3.6-beta.15-selector-order`

- Type: short-lived feature branch
- Status: published through source `dev` and immutable tag `v0.3.6-beta.15`; public archive, registry workflow `33138165588`, and managed beta.15 installation pass
- Base and target: `dev`
- Purpose: prepare the next beta revision with descending M3U selectors, concise schedule actions, evidence-aware TTL recommendations, and validated live defaults
- Scope: selector presentation, new-install defaults, channel-scope guidance, recommendation evidence reporting, focused regression coverage, documentation, and this branch record
- Exclusions: source-score migration, score calculation, registry metadata, deployment, and persistent problematic-stream notification implementation
- Completion: beta.15 is the verified stable-release candidate and is superseded by the approved `release/v0.3.6` promotion

### `feature/issue-7-settings-controls`

- Type: short-lived feature branch
- Status: active
- Base: `dev` at `0d83cdb80f882223bceb67ec1afd09d348a4d084`
- Target: `dev`
- Purpose: implement GitHub issue #7 without changing established hard scoring precedence
- Scope: settings/action presentation, dynamic M3U score controls and migration, stream-name rule parsing, focused tests, user documentation, changelog, decision record, and this ledger
- Exclusions: analyzer probing, TTL calculations, provider capacity, channel scope semantics, stable `main`, registry publication, version bump, deployment, and unrelated issues
- Completion: focused and full tests, Python compilation, workspace validation, complete contradiction review, remote checkpoint, then separate approval for integration or beta publication

### `main`

- Type: long-lived
- Status: active
- Purpose: source prepared for an explicitly approved future stable release
- Current release state: no GitHub Release and no entry in `matrix2669/dispatcharr-plugins:main`
- Historical test tag: `v0.3.5` at `283da3aa636b443f39efe89a0216e4f7f837247d`
- Future distribution target after release approval: `matrix2669/dispatcharr-plugins:main`
- Validation: the v0.3.5 plugin passed 77 automated tests, live installation, no-stream profile integration, native URL rewriting, capacity reservation/release checks, and GitHub Actions
- Notes: a Git tag alone is a test publication for this plugin; only an explicitly created GitHub Release may be advertised in the stable manifest

### `dev`

- Type: long-lived
- Status: active
- Base: the documented `main` migration baseline
- Purpose: integrate upcoming changes and publish immutable test tags without creating GitHub Releases
- Target: `main` after beta validation
- Distribution target: `matrix2669/dispatcharr-plugins:dev`
- Validation: every publishable beta requires the full automated suite, archive inspection, Dispatcharr update/install verification, and proportionate live integration testing

## Migration record

Before cleanup, the legacy `dev-test`, checkpoint, and duplicate `noop-check` branches were refreshed from GitHub and verified to contain no commits absent from the new `dev` history. Their user-visible results are recorded in `CHANGELOG.md`; architectural rationale is recorded in `DECISIONS.md`. They are not retained in this current-branch ledger after deletion.
# Corrective release work

- `fix/v0.3.6-beta.4-review-corrections`: active corrective branch from `dev`; fixes all beta.3 review findings, expands validation, and targets immutable `v0.3.6-beta.4` before integration back to `dev`.
- `fix/v0.3.6-beta.6-analysis-control`: active corrective branch from `dev`; preserves viewer-aware capacity while serializing direct/UI/scheduled scans, adds cooperative checkpoint-and-stop behavior, and targets immutable `v0.3.6-beta.6`. Validation: 107 automated tests, Python compilation, diff checks, and workspace standards reconciliation pass; exact archive installation and live Dispatcharr reservation/cancellation checks remain publication gates.
- `fix/v0.3.6-beta.10-telemetry-integrity`: active corrective branch from `dev`; fixes beta.9 terminal transition attribution and throughput completion accounting, adds regression coverage, and targets immutable `v0.3.6-beta.10`. Excludes TTL, retry-budget, scheduling, provider-capacity, scoring, and stream-order changes. Validation and live clean-scan evidence remain publication gates.

### `release/v0.3.6`

- Type: short-lived stable release branch
- Status: published through source `dev` and `main`, immutable tag `v0.3.6`, normal GitHub Release, both registry channels, and managed stable installation
- Base: `dev` at `e795ecebb4c531b4b801476f43c708dc21c34dee` after beta.15 source, public archive, registry, workflow, and managed-install validation passed
- Targets: source `dev` and `main`, then focused registry `dev` and `main` publication
- Purpose: promote the approved beta.15 behavior without functional changes as stable Dispatcharr Stream Sort `0.3.6`
- Scope: synchronized stable version metadata, cumulative release notes, branch evidence, immutable tag, GitHub Release, focused registry metadata, and managed stable installation
- Exclusions: no new runtime behavior, setting changes, dependency changes, unrelated plugin metadata, or Dispatcharr core changes
- Validation: 159 source tests on the release tree and public archive, Python compilation, manifest parsing, standards reconciliation, project validation, byte-verified manual ZIP and checksum, registry workflows `33138596704` and `33138696338`, public raw manifests, and managed installations pass; the attached ZIP also passed a controlled Dispatcharr manual import as trusted loaded `0.3.6`, followed by successful restoration to managed stable repository 3
- Completion: tag `v0.3.6` remains pinned to `bbae86f2ded0a1bcd09d2906e0530e70380ce5a4`; source `main` promotion is `d16af98828fa8428cccea73e7dda672f7998fe24`; GitHub Release, `dev` and `main` registry publication, and stable repository 3 deployment are complete
- Started: `2026-08-27`
