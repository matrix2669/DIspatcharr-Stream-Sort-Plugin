# Release Process

Dispatcharr Stream Sort uses `dev` tags for beta and completed versions, and `main` only for explicitly approved Releases. Registry channels never install from a moving source branch.

For this plugin, a tag is not a GitHub Release:

- the newest approved tag from `dev` is published to `matrix2669/dispatcharr-plugins:dev`;
- beta and completed stable tags do not require GitHub Releases;
- only an explicitly approved GitHub Release is published to `matrix2669/dispatcharr-plugins:main`.

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
4. Push the tag without creating a GitHub Release.
5. Update the `dispatcharr-plugins:dev` manifest to the immutable tag archive and matching commit.
6. Confirm Dispatcharr detects the version increment and installs that exact build.

## Completed stable version

1. Complete the intended feature or fix work on `dev` and rerun the full validation suite.
2. Set every version source to the normal Semantic Version and tag that exact `dev` commit.
3. Push the tag without requiring a GitHub Release.
4. Update `dispatcharr-plugins:dev` to the immutable stable tag and matching commit.
5. Confirm Dispatcharr detects and installs the completed version.

## GitHub Release

1. Obtain explicit user approval to release a completed stable version.
2. Promote the exact tagged commit to `main` without changing its code or moving the tag.
3. Publish a normal GitHub Release for the existing stable tag.
4. Attach the manually installable ZIP and checksum. Do not rely only on GitHub's automatic source archive.
5. Update `dispatcharr-plugins:main` to the immutable released tag and commit.
6. Test both registry installation and the attached manual-install artifact.
7. Synchronize `dev` with the released state before starting the next cycle.

## Historical baseline

Version `0.3.5` was originally validated and published for testing through the legacy `dev-test` branch at commit `283da3aa636b443f39efe89a0216e4f7f837247d`. The `v0.3.5` tag anchors that exact historical test source; it does not declare a stable GitHub Release.
