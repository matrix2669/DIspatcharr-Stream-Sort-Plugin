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

The analyzer tracks five independent freshness components:

- **FFprobe statistics TTL:** 12 hours by default. Only a completed direct FFprobe resets it; playback and imported Dispatcharr metadata never move this clock.
- **Dead stream TTL:** dead streams are rechecked when this period expires (1 hour by default). This reduces repetitive dead-stream retries across full scans while still confirming network failures, invalid `0x0` video dimensions, placeholder files, black video, frozen video, and silent audio through retry passes inside the same analyze job.
- **Content validation TTL:** 7 days by default. Direct black/frozen/silent analysis or at least 60 seconds of clean attributable Dispatcharr playback resets it; playback evidence is explicitly labeled as assumed rather than direct detection. If no enabled detector applies, Stream Sort records a completed skipped decision without reserving provider capacity.
- **Throughput TTL:** 6 hours by default for healthy evidence. Direct probes and at least 300 seconds of clean provider-attributed playback at `1.10x` nominal or higher can reset it. Every clean ratio is retained for threshold analysis; marginal, insufficient, and unknown throughput wait for the exact Dead stream TTL before rechecking.

Confirmed-dead streams are deferred by the exact Dead stream TTL between Analyze runs. Skipped and unknown health states remain eligible for revalidation.

Each analyze run writes a compact health trend report for all selected streams to:

`/data/dispatcharr_stream_sort_health_report.json`

and the **Recommend TTLs** action writes a companion recommendation file to:

`/data/dispatcharr_stream_sort_ttl_recommendations.json`

Use the **Health Report** action, or inspect that file directly, to find:
- streams that are repeatedly dead,
- streams with frequent health transitions,
- and whether dead checks are concentrated at specific hours.

If you want to reduce scan waves when many streams are discovered at the same time, set **TTL jitter percent** (0-100) to spread expiry windows per stream.

When newer Dispatcharr stream data is imported, the cache records `metadata_source: "dispatcharr_stream_stats"`. Qualifying runtime playback is matched by stream ID and M3U provider account. Profiles under that provider are equivalent because credential-only URL differences do not change the underlying stream. Sessions containing a channel error or failover do not qualify as clean. Runtime evidence never clears a confirmed-dead result; only a completed analyzer scan can do that. If imported resolution/FPS changes, or bitrate changes beyond configured thresholds, throughput is rechecked so its delivery classification uses the new media characteristics.

Dispatcharr's `Stream.is_stale` field belongs to provider refresh and is not a playback exclusion flag. Stream Sort therefore does not write it. Confirmed-dead state is reported and used for sorting, but current Dispatcharr releases require a core-supported exclusion mechanism before a plugin can guarantee that playback skips a dead stream.

Significant bitrate change currently uses:
- media bitrate relative tolerance (default `30%`), and
- media bitrate absolute tolerance (default `500 kbps`),

so bitrate changes count only when the delta is greater than both of these limits. Set the thresholds in:

- `Media bitrate relative tolerance (%)`
- `Media bitrate absolute tolerance (kbps)`

Analysis uses three independent scan-start decisions: FFprobe statistics/reachability, grouped content validation, and throughput. FFprobe runs only when its direct TTL is due. Streams due only for content use the configured 6-second FFmpeg sample. Streams due for both content and throughput use one 8-second wall-clock-bounded stream-copy capture; capture bytes are divided by provider capture time before FFmpeg shutdown, and unexpectedly short captures are retried rather than scored. Stream Sort releases the provider reservation before decoding that local sample. Streams due only for throughput keep the raw byte-delivery probe. Clean Dispatcharr playback can independently satisfy content and sustained-throughput decisions.

Combined captures use a bounded two-stage pipeline and are deleted by the local-analysis worker immediately after use. When `/dev/shm` has conservative worker-scaled headroom and `/dev/shm/stream-sorter` passes an actual create/write/delete test as the Dispatcharr runtime user, samples are written there; otherwise Stream Sort logs the reason and falls back to the system temporary directory. A host `/dev/shm` passthrough can avoid overlay or persistent-volume writes, but it is optional, must have enough real memory for the configured worker count, and must expose a capture directory writable by the container's runtime UID/GID. Backpressure limits retained samples to active capture and local-analysis workers instead of the total number of due streams.

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

## Operational tuning for scan load and TTL staggering

- Use **Dead stream TTL** to defer repeated dead-stream rechecks between scans.
- The default **TTL jitter percent** is `30`, assigning each stream a stable media/throughput TTL between 70% and 130% of the configured value so streams do not expire together. Dead TTL is exact and receives no jitter.
- Use **Maximum streams per analysis run** to cap per-run load and spread full rechecks over multiple windows.
- Use the single scheduled Analyze job and choose whether **Apply sort after scheduled analysis** is enabled.
- Click **Recommend TTLs** after analyzing to get data-driven reachability/dead-TTL and jitter recommendations before changing settings.

The 90-day health trend report (`/data/dispatcharr_stream_sort_health_report.json`) includes:

- hourly dead-check concentration,
- streams with unstable status histories,
- explicit alive-to-dead and dead-to-alive transition counts,
- completed dead-recovery and alive-episode durations,
- censored currently-dead episodes, and
- actual check concentration used to tune jitter.

Suggested process:

1. Start with conservative TTLs for a day or two.
2. Watch dead ratio, unstable stream count, and status-change interval trends.
3. Raise TTLs only while dead spikes remain acceptable, then re-evaluate.

## Throughput scoring

Throughput is delivery capacity, not the stream's encoded video bitrate.

- Healthy: >= 1.50x nominal delivery requirement: **+15**
- Marginal: >= 1.10x nominal: **+5**
- Insufficient: < 1.10x nominal: **-30**
- Unknown: **0**

Healthy throughput is cached for the configured jittered Throughput TTL. Marginal, insufficient, and unknown results are cached for the exact Dead stream TTL without jitter, preventing hourly schedules from repeatedly probing the same degraded stream.

The **Parallel tests** setting controls both media checks and integrated throughput probes (maximum 16). When the worker count is less than or equal to the number of M3U sources with pending work, every active worker uses a different source. Additional workers are distributed round-robin across sources, and capacity is reassigned when a source runs out of pending streams. The per-source start delay still applies, but different M3U sources can start throughput probes at the same time.

Before a worker opens an M3U source, it selects from all active profiles using Dispatcharr's atomic profile and shared-credential connection pool. Connections already held by viewers therefore reduce analyzer capacity, while two one-stream profiles provide two analyzer slots when both are free. The selected profile's native Dispatcharr URL resolver applies its regex or Xtream credential rewrite before the probe connects. A profile with `max_streams = 0` remains unlimited. If every remaining profile is at capacity, those checks are deferred without replacing their cached health or throughput data and are eligible again on the next Analyze run.

Analysis runs in the background. Use **Check Status** to see the active media, retry, throughput, or sorting phase and its latest progress, or to review the outcome of the last run.

Retry passes run at most one recheck per M3U source at a time, while different sources may retry in parallel. FFprobe retries finish before downstream content and throughput phases begin. Network errors and provisional media/content failures (`0x0`, placeholder, black, frozen, or silent) all enter confirmation queues. A combined capture that never completes retries the same 8-second combined operation because neither content nor throughput was measured; it does not increment throughput-checked totals or satisfy the throughput TTL. After a valid combined capture, its throughput result is retained while content-only failures use the shorter 6-second retry path, and a content success with an incomplete throughput calculation uses a throughput-only retry. Every retry logs an individual stream result using the same health, resolution, FPS, bitrate, progress, totals, and ETA fields as the initial check.

## Channel scope

Every action can be restricted by channel group and/or channel profile. Multiple values within a filter are ORed. If both group and profile filters are set, a channel must match both. Group matching uses the channel's effective group, including overrides; profile membership must be enabled.

## M3U source scores

When enabled, the plugin dynamically lists every operator-managed M3U account with a numeric score box. Locked Dispatcharr system accounts such as the internal `custom` account are excluded. All listed sources default to `0`. Positive values promote a source and negative values demote it.

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
- **Stop Current Scan** — stop launching new probes and safely drain already-running probes so Stream Sort releases only its own provider reservations. It does not clear Dispatcharr provider counters or reclaim viewer capacity.
- **Apply Schedule** — save a standard five-field UTC cron schedule. Each run loads current saved UI settings and is atomically claimed once across workers.
- **Check Schedule** — show current schedule configuration and final status of the latest scheduled job.
- **Disable Schedule** — stop automatic scheduled analysis and clear any in-memory due-minute state.
- **Health Report** — show problematic streams, directional health transitions, dead recovery, and time concentration in the UI.
- **Recommend TTLs** — compute read-only health/dead TTL and jitter recommendations from recent directional evidence, with confidence and sparse-data warnings.
- **Reset Scan Statistics** — clear unified and legacy scan caches, scan status, health reports, and TTL recommendations while preserving runtime reliability and playback history.
- **Reset All Statistics** — clear the same scan evidence plus all runtime reliability and playback history.

Both reset actions use the same cross-process lease as analysis and are refused while any scan is active. Schedules, settings, channel order, sort reports, and provider configuration are preserved.
- **Dry Run** — write `/data/dispatcharr_stream_sort_report.json` without changing order.
- **Sort Streams** — apply the calculated `ChannelStream.order` only.
- **Analyze + Sort** — incrementally analyze, then apply the refreshed ordering.
- **Runtime Reliability Collector** — automatic event-triggered collector for Dispatcharr runtime telemetry; manual invocation does not synthesize reliability events.

Separate `Probe Throughput` actions are no longer shown because throughput is part of Analyze Streams.

Stopping a scan checkpoints every media and throughput probe that has already completed, then skips remaining probes and post-analysis sorting. A dead media result that has not completed its configured retry sequence is saved as retry-pending with an effective dead TTL of zero, so the next scan immediately resumes confirmation instead of treating it as confirmed dead. Once retries confirm the terminal result as dead, FFprobe, content, and throughput all wait behind the same exact Dead stream TTL recovery gate. Retry-pending observations remain visible as provisional evidence but are excluded from terminal dead percentages. Stopping a scan does not disable its recurring schedule.

## Logging

The plugin owns the `plugins.stream_sorter` logger and prefixes its messages with `[Stream Sort]`. Incremental runs report FFprobe, content, combined, retry, and throughput work with per-stream progress. Runtime telemetry retains clean playback duration, bytes, measured Mbps, nominal ratio, threshold classification, buffering failures, and attribution. Health reports include sustained-throughput percentiles and buckets around `1.03x`, `1.05x`, `1.07x`, and `1.10x` for evidence-based tuning.

## Development

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```
