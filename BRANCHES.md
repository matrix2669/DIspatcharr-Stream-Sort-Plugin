# Branches

This ledger records every current branch on `matrix2669/DIspatcharr-Stream-Sort-Plugin`. GitHub remains authoritative for live refs, commits, pull requests, and checks.

Before deleting a branch, record user-visible results in `CHANGELOG.md` and durable architectural rationale in `DECISIONS.md` when applicable, then remove its index row and detailed record.

## Branch Index

| Branch | Type | Status | Base | Target | Purpose |
|---|---|---|---|---|---|
| `main` | long-lived | active | historical repository root | approved stable releases | Reserved stable-release branch; Stream Sort is not yet published in the stable registry. |
| `dev` | long-lived | active | `main` | `main` | Integrates and validates the next beta and stable release. |
| `feature/analysis-telemetry-retention` | short-lived | active | `dev` | `dev` | Retain direct throughput/media-change evidence and applied-sort movement history. |
| `feature/issue-7-settings-controls` | short-lived | active | `dev` | `dev` | Simplify settings/action layout, bound M3U scores, and add comma-separated name rules. |
| `fix/v0.3.6-beta.6-analysis-control` | short-lived | active | `dev` | `dev` | Prevent overlapping analysis entry points, add safe cancellation, and publish immutable beta.6. |
| `fix/v0.3.6-beta.10-telemetry-integrity` | short-lived | active | `dev` | `dev` | Preserve scan-boundary health transitions and distinguish attempted throughput probes from completed measurements. |

## Branch Records

### `feature/analysis-telemetry-retention`

- Type: short-lived feature branch
- Status: active
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
