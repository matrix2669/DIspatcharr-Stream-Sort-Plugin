# Branches

This ledger records every branch currently present on `matrix2669/DIspatcharr-Stream-Sort-Plugin`. GitHub remains authoritative for live refs, commits, pull requests, and checks. The observations below were refreshed at `2026-08-22T12:34:00Z`.

Before deleting a branch, record user-visible results in `CHANGELOG.md` and durable architectural rationale in `DECISIONS.md` when applicable, then remove its index row and detailed record.

## Branch Index

| Branch | Type | Status | Base | Target | Purpose |
|---|---|---|---|---|---|
| `main` | long-lived | active | root | future stable releases | Legacy initial baseline; it does not yet contain the plugin. |
| `dev-test` | long-lived | active | `main` | future `dev`/`main` migration | Current plugin source and dev-test registry publication branch. |
| `dev-test-cache-work` | checkpoint | merged | `main` | `dev-test` | Historical unified-cache and safe legacy-throughput migration checkpoint. |
| `dev-test-incremental-wiring` | checkpoint | merged | `main` | `dev-test` | Historical direct incremental-analyzer wiring and v0.2.5 checkpoint. |
| `dev-test-playback-metadata` | checkpoint | merged | `main` | `dev-test` | Historical Dispatcharr playback-metadata reuse and v0.2.2 checkpoint. |
| `dev-test-reconnect-filter` | checkpoint | merged | `main` | `dev-test` | Historical switch-internal reconnect suppression and v0.2.4 checkpoint. |
| `dev-test-reliability-telemetry` | checkpoint | merged | `main` | `dev-test` | Historical runtime reliability telemetry checkpoint. |
| `noop-check` | checkpoint | superseded | `main` | `dev-test` | Duplicate historical pointer at the built-in analyzer commit; original reason is unavailable. |
| `noop-check2` | checkpoint | superseded | `main` | `dev-test` | Duplicate historical pointer at the built-in analyzer commit; original reason is unavailable. |

All checkpoint branches are strict ancestors of `dev-test`; none contains commits absent from `dev-test` as of the observation time.

## Branch Records

### `main`

- Status: active legacy baseline
- Head: `fc588f5e45c1860786aa5187a0dbf7f248a349b8`
- Intended role: stable, production-ready releases after the standalone workflow migration
- Current contents: initial repository commit only; it is 45 commits behind `dev-test`
- Risk: treating it as the current stable plugin source would publish an empty baseline
- Planned outcome: populate stable code only through an explicit, validated migration

### `dev-test`

- Status: active and published
- Base: `main` at `fc588f5e45c1860786aa5187a0dbf7f248a349b8`
- Head: `283da3aa636b443f39efe89a0216e4f7f837247d`
- Purpose: current integration and testing source for Dispatcharr Stream Sort
- Published version: `0.3.5`
- Registry relationship: `matrix2669/dispatcharr-plugins:dev-test` currently references this exact commit through a moving `zipball/dev-test` URL
- Validation recorded for the head: automated suite, live no-stream integration, profile reservation/release checks, native URL rewriting, and GitHub Actions
- Planned outcome: use this history to create the new `dev` integration branch and promote a separately validated stable release to `main`; do not migrate or delete without explicit approval

### `dev-test-cache-work`

- Status: merged into `dev-test`; remote branch still exists
- Head: `beb2873fe07a9303c54e0486fc3dc34f5cf31a1c`
- Purpose: checkpoint for unified analysis/throughput caching and safe migration of fresh legacy throughput evidence
- Relationship: 23 commits behind `dev-test`, with no unique commits
- Validation: cache expiration and legacy migration regression tests are present in `dev-test`

### `dev-test-incremental-wiring`

- Status: merged into `dev-test`; remote branch still exists
- Head: `845478cc5b5587bfd02ef06369cd0b9bf29ececb`
- Purpose: checkpoint for direct incremental-analyzer wiring and version `0.2.5`
- Relationship: 5 commits behind `dev-test`, with no unique commits
- Validation: direct wiring and manifest tests are present in `dev-test`

### `dev-test-playback-metadata`

- Status: merged into `dev-test`; remote branch still exists
- Head: `8eef5b0aaf07cdbf5abb8231783a1a492c78cfe9`
- Purpose: checkpoint for reuse of fresh Dispatcharr stream metadata and version `0.2.2`
- Relationship: 17 commits behind `dev-test`, with no unique commits
- Validation: playback metadata refresh tests are present in `dev-test`

### `dev-test-reconnect-filter`

- Status: merged into `dev-test`; remote branch still exists
- Head: `9ef0cf39006cd77c7506794987653927130bed6b`
- Purpose: checkpoint for suppressing reconnect telemetry caused internally by normal stream switching
- Relationship: 10 commits behind `dev-test`, with no unique commits
- Validation: reconnect-classification regression tests are present in `dev-test`

### `dev-test-reliability-telemetry`

- Status: merged into `dev-test`; remote branch still exists
- Head: `b850f045ee8b8adbae34fb8a4434a9323afd54b8`
- Purpose: checkpoint for initial runtime stream reliability collection
- Relationship: 16 commits behind `dev-test`, with no unique commits
- Validation: later reliability tests and conservative schema-2 scoring are present in `dev-test`

### `noop-check`

- Status: superseded; remote branch still exists
- Head: `cdb65e633949a1775c8948f208b9c9e1c4eca498`
- Purpose: exact original intent is unavailable; the branch points to the initial built-in analyzer commit
- Relationship: 34 commits behind `dev-test`, with no unique commits; identical to `noop-check2`
- Planned outcome: eligible for deletion only after user approval

### `noop-check2`

- Status: superseded; remote branch still exists
- Head: `cdb65e633949a1775c8948f208b9c9e1c4eca498`
- Purpose: exact original intent is unavailable; the branch points to the initial built-in analyzer commit
- Relationship: 34 commits behind `dev-test`, with no unique commits; identical to `noop-check`
- Planned outcome: eligible for deletion only after user approval
