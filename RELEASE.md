# Release Process

Dispatcharr Stream Sort uses `main` for stable releases and `dev` for the next release. Published versions are immutable Git tags; registry channels never install from a moving source branch.

## Version sources

Every publishable build must use the same version in:

- `VERSION` without the `v` prefix;
- `pyproject.toml`;
- `stream_sorter/plugin.json`;
- `CHANGELOG.md` and release notes;
- the Git tag and `matrix2669/dispatcharr-plugins` registry entry.

Testing builds use `MAJOR.MINOR.PATCH-beta.N`; stable builds use `MAJOR.MINOR.PATCH`. Never move a published tag or replace a published artifact. Publish a new version for corrections.

## Required validation

1. Run `python3 -m pytest`.
2. Run `python3 -m compileall -q stream_sorter tests`.
3. Validate installation/update behavior in Dispatcharr.
4. For connection-policy changes, validate all active profiles, native URL rewriting, viewer-capacity preservation, serialized same-provider retries, and reservation release on every exit path.
5. Inspect the exact tag archive that the registry will install and confirm it contains the `stream_sorter/` package with `plugin.json` at the expected depth.

## Beta release

1. Integrate and validate the intended changes on `dev`.
2. Set every version source to the next beta, such as `0.4.0-beta.1`, and finalize the matching changelog content.
3. Commit the versioned state and tag that exact `dev` commit with the matching `v`-prefixed tag.
4. Publish the GitHub Release as a prerelease and attach the beta ZIP plus checksum.
5. Update the `dispatcharr-plugins:dev-test` manifest to the immutable tag archive and matching commit.
6. Confirm Dispatcharr detects the version increment and installs that exact build.

## Stable release

1. Promote the exact tested plugin code to `main`, changing only required release metadata afterward.
2. Set every version source to the stable version and rerun the full validation suite.
3. Tag the release commit on `main` and publish a normal GitHub Release.
4. Attach the manually installable ZIP and checksum. Do not rely only on GitHub's automatic source archive.
5. Update the stable `dispatcharr-plugins:main` manifest to the immutable stable tag and commit.
6. Test both registry installation and the attached manual-install artifact.
7. Synchronize `dev` with the released state before starting the next cycle.

## Historical baseline

Version `0.3.5` was originally validated and published through the legacy `dev-test` branch at commit `283da3aa636b443f39efe89a0216e4f7f837247d`. The `v0.3.5` tag anchors that exact historical source rather than assigning the same version to the later documentation commit.
