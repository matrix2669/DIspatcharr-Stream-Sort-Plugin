# Dependencies

## Runtime platform

- Dispatcharr `v0.29.0` or newer is the supported baseline.
- Python and Django are supplied by the Dispatcharr plugin runtime; this repository does not vendor them.
- FFprobe and FFmpeg paths are configurable in the plugin UI and default to `/usr/local/bin/ffprobe` and `/usr/local/bin/ffmpeg`.

## Dispatcharr contracts used

- `apps.channels.models.ChannelStream` supplies existing channel assignments and the only field Stream Sort mutates during sorting: `ChannelStream.order`.
- `apps.channels.models.Stream` supplies URL, M3U ownership, `stream_stats`, and `stream_stats_updated_at`. Stream Sort does not write provider-owned `is_stale`.
- `apps.m3u.models.M3UAccount` and active profiles supply provider identity, capacity, user agent, and native URL rewriting.
- `apps.m3u.connection_pool` supplies atomic profile reservations so analysis respects active viewers and shared credentials.
- `apps.plugins.models.PluginConfig` supplies current enabled state and saved settings for every scheduled run.
- Django's shared cache supplies atomic per-minute schedule claims across uWSGI workers.
- Dispatcharr plugin events supply runtime reliability evidence. Only fresh URL-attributed schema-2 evidence is scoreable.

## Compatibility review

### Storage evaluation, 2026-10-30

Inspected installed Dispatcharr 0.33.0 plugin loader, app startup, plugin models, install/delete API, and application registration from image `sha256:96bc548978c68e684677da7895692eeeefb7f0b2edfa38311dac2b702c61d236` (`ghcr.io/dispatcharr/dispatcharr:latest`); version confirmed in `/app/version.py`. Evidence is installed code, not a claim about newer upstream releases. The loader dynamically imports a Python plugin and constructs its Plugin instance; plugin packages are not added to Django INSTALLED_APPS and no plugin-owned migration runner was found in these lifecycle paths. The core plugins application's post_migrate hook discovers/synchronizes plugin metadata; it does not apply migrations inside plugin archives.

ADR-024 authorizes implementation through Django's existing PostgreSQL connection after this lifecycle review. Plugin table DDL permission, backup coverage, operational schema initialization, and upgrade/rollback/uninstall policy need verification before production adoption. No production schema or role privileges were modified. The backend must not require changes to Dispatcharr-owned tables or global migration/app registration. Core Stream metadata batch updates must preserve update-field semantics and be reviewed for signal-dependent behavior before replacing individual saves.

The disposable PostgreSQL16 CI service validates table ownership under a restricted non-superuser role explicitly granted CONNECT and schema USAGE/CREATE. This is separate from production-role permission and backup proof. The original 19 database tests passed at `bb4cd51`; final review repairs expand the suite to 22 and require their own exact-revision execution. RAM acquisition uses a supervised Python stdlib child, not curl or additional runtime packages. Keep `capture_worker.py` in every distributable plugin archive and validate child startup/cleanup under the Dispatcharr runtime identity before deployment.

- Re-review these contracts against the exact Dispatcharr revision before raising the minimum supported version or changing scheduling, capacity, event, or playback-health behavior.
- Validate background ORM connection cleanup, multi-worker claims, profile reservations, retries, URL rewriting, and plugin lifecycle in a controlled Dispatcharr instance before release.

### Installed-platform rehearsal, 2026-10-30

Accepted source `db1834501c912a083737537d3e7d2a51766b5cb2` passed the controlled schema-v2 legacy import, idempotence, full custom-dump/restore fingerprints, 188-channel ranking parity, normal analysis/sort/report checks in restored databases on Dispatcharr 0.33.0 with PostgreSQL 17.11. The live `dispatch` role has superuser/CREATEDB privileges, so this proves available permissions, not least privilege; restricted-role coverage remains the PostgreSQL16 CI job. Production has no plugin-owned PostgreSQL tables and remains JSON-backed. Explicit dumps were validated; automatic backup coverage is still unverified. See the owning branch ledger for measurements and capture-equivalence limitations.

The installed custom `dispatcharr.db.backends.postgresql_psycopg3` backend uses django-db-geventpool's alias-keyed class-level connection pools. Merely replacing Django's default database NAME/wrapper can reuse the old database connection. A rehearsal that changes database routing must first close and remove the default alias pool in its own isolated process, replace the wrapper/configuration, then assert `SELECT current_database()` before any DDL/import. Never clear service-worker pools or route a production worker to a rehearsal database. The routing guard stopped the first attempt before migration or probes; corrected isolation passed. This is a rehearsal-harness dependency constraint, not a plugin requirement to switch production databases.
