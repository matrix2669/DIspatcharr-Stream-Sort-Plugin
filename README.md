# Dispatcharr Stream Sort Plugin

Dispatcharr Stream Sort analyzes the streams already attached to Dispatcharr channels, scores them, and updates only `ChannelStream.order` when sorting is explicitly requested. It does not match/create/delete streams or channels, rename channels, or change EPG/group assignments.

## Built-in incremental checker

Stream Sort no longer requires IPTV Checker for sorting data. `Analyze Streams` owns direct health/content and delivery measurements while reusing newer media metadata and successful reachability evidence already learned during Dispatcharr playback.

- reachability and Alive / Dead / Skipped classification
- resolution, FPS, codecs, pixel format, audio details, and measured video bitrate
- fixed-duration placeholder detection
- black-video, frozen-video, and silent-audio detection
- measured delivery throughput and throughput health
- rate-limit-aware retry behavior
- account-aware parallel scheduling that distributes active tests evenly across M3U sources
- atomic Dispatcharr connection-pool reservations that preserve capacity for active viewers

The analyzer tracks four independent freshness components:

- **Stream metadata TTL:** 12 hours by default. Newer `Stream.stream_stats` / `stream_stats_updated_at` from Dispatcharr playback can refresh resolution/FPS/codec/bitrate metadata without opening another Stream Sort connection.
- **Reachability TTL:** 24 hours after a direct analyzer check. A clean runtime session of at least 60 seconds, or five minutes of established playback, can independently satisfy reachability for 6 hours without another provider connection.
- **Content validation interval:** 7 days by default. Reused playback does not claim that black/frozen/silent checks ran; it only defers an initially missing content validation until this interval expires.
- **Healthy throughput TTL:** 6 hours by default. Marginal, insufficient, and unknown throughput are rechecked on every Analyze run.

Dead, skipped, or unknown health states are rechecked on every Analyze run. A changed stream URL invalidates the cached components. A value of `0` forces the corresponding TTL-controlled component to be refreshed on every Analyze run.

When newer Dispatcharr stream data is imported, the cache records `metadata_source: "dispatcharr_stream_stats"`. Qualifying runtime playback records `health_source: "runtime_playback"`; sessions containing a channel error or failover do not qualify as clean. If the imported resolution/FPS/bitrate signature changed, throughput is rechecked so its delivery classification uses the new media characteristics.

Analysis and throughput share one cache:

```text
/data/dispatcharr_stream_sort_analysis.json
```

The previous `/data/dispatcharr_stream_sort_throughput.json` file is read as a migration fallback but is no longer the primary cache.

## Runtime reliability telemetry

Stream Sort also subscribes to Dispatcharr's runtime channel events and keeps a separate per-stream reliability history in:

```text
/data/dispatcharr_stream_sort_reliability.json
```

The collector tracks playback starts/stops, estimated active playback seconds, reconnects, errors, failovers, buffering-triggered failovers, stream switches, and `channel_buffering` events when the installed Dispatcharr version exposes that event through Connect/plugin hooks.

When Dispatcharr switches streams before emitting a failover event, Stream Sort remembers the stream being left so a `buffering_timeout` failover can still be attributed to the failing stream. It also resolves missing stream IDs from Dispatcharr stream metadata when possible.

Dispatcharr can emit a `channel_reconnect` immediately after a normal stream switch while the event payload still identifies the stream being left. Stream Sort recognizes that narrow pattern when it occurs within two seconds of the switch, keeps the event in `recent_events` as `classification: "switch_internal"` with `counted: false`, and increments `reconnects_suppressed` instead of the reliability `reconnects` counter. A reconnect that identifies the new stream, or a reconnect outside that suppression window, is still counted normally.

Version 0.3 introduces a bounded reliability contribution of **-20 to +20 points** inside the existing viability and resolution tiers. Legacy v0.2 counters remain visible but are excluded because older `channel_error` events could be attributed from stale channel state. New evidence uses a 14-day half-life and remains neutral until a stream has at least 30 minutes of playback or three starts.

`channel_error` events now resolve Dispatcharr's failing `url` back to the stream before falling back to cached channel state. The collector preserves `error_type`, attempts, and exception details and classifies failures occurring inside the first 60 seconds as startup failures.

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

The **Parallel tests** setting controls both media checks and integrated throughput probes (maximum 16). When the worker count is less than or equal to the number of M3U sources with pending work, every active worker uses a different source. Additional workers are distributed round-robin across sources, and capacity is reassigned when a source runs out of pending streams. The per-source start delay still applies, but different M3U sources can start throughput probes at the same time.

Before a worker opens an M3U source, it selects from all active profiles using Dispatcharr's atomic profile and shared-credential connection pool. Connections already held by viewers therefore reduce analyzer capacity, while two one-stream profiles provide two analyzer slots when both are free. The selected profile's native Dispatcharr URL resolver applies its regex or Xtream credential rewrite before the probe connects. A profile with `max_streams = 0` remains unlimited. If every remaining profile is at capacity, those checks are deferred without replacing their cached health or throughput data and are eligible again on the next Analyze run.

Analysis runs in the background. Use **Check Status** to see the active media, retry, throughput, or sorting phase and its latest progress, or to review the outcome of the last run.

Retry passes run at most one recheck per M3U source at a time, while different sources may retry in parallel. This prevents a provider-wide connection burst from being repeated while confirming an initial failure.

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

- **Analyze Streams** — incrementally refresh only health/content, metadata, or throughput components that require checking.
- **Dry Run** — write `/data/dispatcharr_stream_sort_report.json` without changing order.
- **Sort Streams** — apply the calculated `ChannelStream.order` only.
- **Analyze + Sort** — incrementally analyze, then apply the refreshed ordering.
- **Runtime Reliability Collector** — automatic event-triggered collector for Dispatcharr runtime telemetry; manual invocation does not synthesize reliability events.

Separate `Probe Throughput` actions are no longer shown because throughput is part of Analyze Streams.

## Logging

The plugin owns the `plugins.stream_sorter` logger and prefixes its messages with `[Stream Sort]`. Incremental runs report media checks, throughput checks, Dispatcharr metadata refresh counts, cached counts, pending work, health totals, throughput totals, and ETA. Runtime telemetry is logged with `[Reliability]` inside the Stream Sort logger, including whether an event was counted and its classification.

## Development

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```
