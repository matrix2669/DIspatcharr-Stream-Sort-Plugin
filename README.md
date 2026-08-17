# Dispatcharr Stream Sort Plugin

Dispatcharr Stream Sort now combines **stream checking, scoring, and ordering** for streams already attached to Dispatcharr channels. It does not match streams, create channels, rename channels, move channels, delete channels, or alter EPG assignments.

## Built-in stream analyzer

The plugin no longer requires IPTV Checker to determine whether a stream is usable. **Analyze Streams** runs its own `ffprobe`/`ffmpeg` checks and updates Dispatcharr's `stream_stats` directly.

Per stream it can determine:

- reachable / failed / skipped status
- resolution and frame rate
- video/audio codecs and pixel format
- measured video bitrate, including a packet-based fallback for live MPEG-TS/HLS
- audio sample rate/channels/bitrate
- fixed-duration placeholder files
- black video
- frozen video
- silent audio

Rate-limited responses, audio-only streams, and known Streamlink-only hosts are **Skipped**, not Dead. Transient network failures are retried after the first pass. Black/frozen/silent checks share one FFmpeg decode pass and fail open if FFmpeg itself cannot complete the content check.

The analyzer writes detailed results to:

```text
/data/dispatcharr_stream_sort_analysis.json
```

Alive results refresh Dispatcharr stream metadata. Dead results clear `stream_stats` and stamp the check time so Stream Sort's viability gate can demote them. Skipped results preserve previously-known metadata.

### No channel management

The checker functionality is deliberately stream-focused. There are no actions to rename, move, restore, tag, or delete channels. The only channel mutation Stream Sort performs is updating `ChannelStream.order` when **Sort Streams**, **Analyze + Sort**, or **Probe + Sort** is explicitly run.

## Sorting model

1. Unusable streams are demoted first: stale, inactive-source, missing-URL, and known-dead streams.
2. Extremely content-starved streams (video bitrate below 10% of the target for their reported resolution) are demoted below normal usable streams.
3. Resolution is a hard tier: 2160p > 1440p > 1080p > 720p > 576p > 480p > lower/unknown.
4. Streams inside the same resolution tier use an additive score:
   - bitrate adequacy
   - frame rate
   - M3U source preference
   - stream-name prefix/regex rules
   - cached delivery-throughput health
5. Existing `ChannelStream.order` is the final stable tie-breaker.

## Channel scope filters

Every Analyze, Dry Run, Sort, and Throughput action can be restricted by channel group and/or channel profile.

Multiple values inside one filter are ORed. If both filters are populated, the result is their intersection. Empty filters mean all channels. Unknown names/IDs are configuration errors rather than silently selecting nothing.

Examples:

```text
Local
Sports
id:7
```

## M3U source scores

When the plugin is enabled, every configured M3U account is rendered as its own numeric score field. All accounts start at `0`.

Positive values promote a source, negative values demote it. Source account IDs are used internally so renaming an M3U account does not lose its score.

## Stream-name scoring

Current text syntax remains:

```text
US=20
GO=10
TUBI=0
PRIME=-10
ROKU=-20
```

`PREFIX=score` is shorthand for a case-insensitive anchored prefix. Advanced rules use `score::regex`:

```text
-50::\bBACKUP\b
10::^USA?\s*[|:_-]
```

All matching name rules are additive.

## Delivery throughput

Throughput remains a separate delivery-health measurement from content bitrate.

**Probe Throughput** measures sustained delivery and writes:

```text
/data/dispatcharr_stream_sort_throughput.json
```

Known-dead streams from the built-in analyzer are skipped. Each log line includes the current stream result, overall status counts, progress, and ETA.

Classification:

- Healthy: >= 1.50x nominal class bitrate
- Marginal: >= 1.10x nominal
- Insufficient: < 1.10x nominal
- Unknown: no usable/current measurement

Cached throughput expires after the configured TTL (30 minutes by default).

## Actions

- **Analyze Streams** — run the built-in checker and refresh stream health/metadata.
- **Dry Run** — generate `/data/dispatcharr_stream_sort_report.json` without changing order.
- **Sort Streams** — update only `ChannelStream.order`.
- **Analyze + Sort** — analyze first, then apply ordering using refreshed health/metadata and any current throughput cache.
- **Probe Throughput** — refresh delivery-throughput measurements.
- **Probe + Sort** — refresh throughput and then apply ordering.

Only one analyzer/throughput background job may run at a time across Dispatcharr workers.

## Logging

Stream Sort uses its own `plugins.stream_sorter` logger rather than Dispatcharr's shared plugin-loader logger. This prevents another plugin's logger filter from relabeling Stream Sort output.

Typical analyzer line:

```text
INFO plugins.stream_sorter [Stream Sort] [Analyze] 77% (67/87) stream=2674 health=alive reason=ok resolution=1920x1080 fps=59.9 bitrate=4583kbps | overall alive=65 dead=1 skipped=1 pending=20 | ETA=44s
```

Typical throughput line:

```text
INFO plugins.stream_sorter [Stream Sort] [Throughput] 77% (67/87) stream=2674 health=healthy throughput=12.9338Mbps nominal=6000kbps | overall healthy=64 marginal=2 insufficient=0 unknown=1 pending=20 | ETA=44s
```

## Development testing

Development changes land on `dev-test` first. Refresh the dev-test plugin registry in Dispatcharr, install the advertised version, and test on a restricted channel group/profile before any promotion.

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```

## Attribution

The health-check behavior is adapted from the MIT-licensed **Dispatcharr IPTV Checker Plugin** by Pirates IRC. See `THIRD_PARTY_NOTICES.md` for the required license notice.
