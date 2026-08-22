# Branches

This ledger records every current branch on `matrix2669/DIspatcharr-Stream-Sort-Plugin`. GitHub remains authoritative for live refs, commits, pull requests, and checks.

Before deleting a branch, record user-visible results in `CHANGELOG.md` and durable architectural rationale in `DECISIONS.md` when applicable, then remove its index row and detailed record.

## Branch Index

| Branch | Type | Status | Base | Target | Purpose |
|---|---|---|---|---|---|
| `main` | long-lived | active | historical repository root | approved stable releases | Reserved stable-release branch; Stream Sort is not yet published in the stable registry. |
| `dev` | long-lived | active | `main` | `main` | Integrates and validates the next beta and stable release. |

## Branch Records

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
- Distribution target: `matrix2669/dispatcharr-plugins:dev-test`
- Validation: every publishable beta requires the full automated suite, archive inspection, Dispatcharr update/install verification, and proportionate live integration testing

## Migration record

Before cleanup, the legacy `dev-test`, checkpoint, and duplicate `noop-check` branches were refreshed from GitHub and verified to contain no commits absent from the new `dev` history. Their user-visible results are recorded in `CHANGELOG.md`; architectural rationale is recorded in `DECISIONS.md`. They are not retained in this current-branch ledger after deletion.
