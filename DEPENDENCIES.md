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

- Re-review these contracts against the exact Dispatcharr revision before raising the minimum supported version or changing scheduling, capacity, event, or playback-health behavior.
- Validate background ORM connection cleanup, multi-worker claims, profile reservations, retries, URL rewriting, and plugin lifecycle in a controlled Dispatcharr instance before release.
