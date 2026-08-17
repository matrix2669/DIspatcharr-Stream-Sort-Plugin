# Dispatcharr Stream Sort Plugin

Dispatcharr Stream Sort analyzes the streams already attached to Dispatcharr channels, scores them, and updates only `ChannelStream.order` when sorting is explicitly requested. It does not match/create/delete streams or channels, rename channels, or change EPG/group assignments.

## Built-in incremental checker

Stream Sort no longer requires IPTV Checker for sorting data. `Analyze Streams` owns the health/media and delivery measurements used by the sorter:

- reachability and Alive / Dead / Skipped classification
- resolution, FPS, codecs, pixel format, audio details, and measured video bitrate
- fixed-duration placeholder detection
- black-video, frozen-video, and silent-audio detection
- measured delivery throughput and throughput health
- rate-limit-aware retry behavior

The analyzer is cache-aware rather than rechecking every stream on every run.

Default refresh policy:

- **Stream/media data TTL:** 12 hours
- **Healthy throughput TTL:** 6 hours
- **Dead or skipped media:** recheck every Analyze run
- **Marginal / insufficient / unknown throughput:** recheck every Analyze run
- **Changed stream URL:** invalidate and recheck both media and throughput
- **Fresh alive media + fresh healthy throughput:** no provider connection is opened

The two TTLs are independently configurable. A value of `0` forces that component to be rechecked on every Analyze run.

Analysis and throughput now share one cache:

```text
/data/dispatcharr_stream_sort_analysis.json
```

The previous `/data/dispatcharr_stream_sort_throughput.json` file is read as a migration fallback but is no longer the primary cache.

## Health ordering

Viability is the first hard ordering gate:

1. usable streams
2. content-starved streams
3. unusable streams such as known-dead, stale, inactive-source, or missing-URL streams

That means a known-dead 2160p/1080p stream is placed below **all usable streams in the same channel**, not merely below usable streams of its own resolution.

Among streams with the same viability, resolution remains a hard tier:

```text
2160 > 1440 > 1080 > 720 > 576 > 480 > 360 > 240/unknown
```

Inside the same viability/resolution tier, the additive score uses bitrate adequacy, FPS, M3U source preference, stream-name rules, and current delivery-throughput health. Existing `ChannelStream.order` is the final stable tie-breaker.

## Throughput scoring

Throughput is delivery capacity, not the stream's encoded video bitrate.

- Healthy: >= 1.50x nominal delivery requirement: **+15**
- Marginal: >= 1.10x nominal: **+5**
- Insufficient: < 1.10x nominal: **-30**
- Unknown: **0**

Healthy throughput is cached for the configured TTL. Any non-healthy throughput result is eligible for another probe on the next Analyze run.

## Channel scope

Every action can be restricted by channel group and/or channel profile. Multiple values within a filter are ORed. If both group and profile filters are set, a channel must match both. Group matching uses the channel's effective group, including overrides; profile membership must be enabled.

## M3U source scores

When enabled, the plugin dynamically lists every configured M3U account with a numeric score box. All sources default to `0`. Positive values promote a source and negative values demote it.

## Name rules

Prefix shorthand:

```text
US=20
GO=10
TUBI=0
PRIME=-10
ROKU=-20
```

Advanced regex:

```text
15::^USA?\s*[|:_-]
-50::\bBACKUP\b
```

All matching name rules are additive.

## Actions

- **Analyze Streams** — incrementally refresh only media/health or throughput components that require checking.
- **Dry Run** — write `/data/dispatcharr_stream_sort_report.json` without changing order.
- **Sort Streams** — apply the calculated `ChannelStream.order` only.
- **Analyze + Sort** — incrementally analyze, then apply the refreshed ordering.

Separate `Probe Throughput` actions are no longer shown because throughput is part of Analyze Streams.

## Logging

The plugin owns the `plugins.stream_sorter` logger and prefixes its messages with `[Stream Sort]`. Incremental runs report media checks, throughput checks, cached counts, pending work, health totals, throughput totals, and ETA.

## Development

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```

Changes are validated on `dev-test` before promotion to `dev`.
