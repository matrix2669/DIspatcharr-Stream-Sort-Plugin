# Branches

This ledger records every current branch on `matrix2669/DIspatcharr-Stream-Sort-Plugin`. GitHub remains authoritative for live refs, commits, pull requests, and checks.

Before deleting a branch, record user-visible results in `CHANGELOG.md` and durable architectural rationale in `DECISIONS.md` when applicable, then remove its index row and detailed record.

## Branch Index

| Branch | Type | Status | Base | Target | Purpose |
|---|---|---|---|---|---|
| `main` | long-lived | active | historical repository root | stable tags | Production-ready code and stable release history. |
| `dev` | long-lived | active | `main` | `main` | Integrates and validates the next beta and stable release. |

## Branch Records

### `main`

- Type: long-lived
- Status: active
- Purpose: production-ready source and stable releases
- Stable baseline: `v0.3.5` at `283da3aa636b443f39efe89a0216e4f7f837247d`
- Distribution target: `matrix2669/dispatcharr-plugins:main`
- Validation: the v0.3.5 plugin passed 77 automated tests, live installation, no-stream profile integration, native URL rewriting, capacity reservation/release checks, and GitHub Actions
- Notes: documentation-only commits may follow a stable tag while `main` remains production-ready; runtime changes require a new version and release

### `dev`

- Type: long-lived
- Status: active
- Base: the documented `main` migration baseline
- Purpose: integrate upcoming changes and publish immutable `vMAJOR.MINOR.PATCH-beta.N` test releases
- Target: `main` after beta validation
- Distribution target: `matrix2669/dispatcharr-plugins:dev-test`
- Validation: every publishable beta requires the full automated suite, archive inspection, Dispatcharr update/install verification, and proportionate live integration testing

## Migration record

Before cleanup, the legacy `dev-test`, checkpoint, and duplicate `noop-check` branches were refreshed from GitHub and verified to contain no commits absent from the new `dev` history. Their user-visible results are recorded in `CHANGELOG.md`; architectural rationale is recorded in `DECISIONS.md`. They are not retained in this current-branch ledger after deletion.
