# Dispatcharr Stream Sort Plugin

A focused Dispatcharr plugin that **only reorders streams already assigned to channels**. It does not match streams, create channels, rename channels, or change EPG assignments.

## Sorting model

1. Unusable streams are demoted first: stale, inactive-source, missing-URL, and known-dead streams.
2. Extremely content-starved streams (video bitrate below 10% of the target for their reported resolution) are demoted below normal usable streams.
3. Resolution is a hard tier: 2160p > 1440p > 1080p > 720p > 576p > 480p > lower/unknown.
4. Streams inside the same resolution tier use an additive score:
   - bitrate adequacy
   - frame rate
   - M3U source preference
   - positive/negative stream-name prefix or regex rules
   - cached delivery-throughput health
5. Existing `ChannelStream.order` is the final stable tie-breaker.

This gives operator preferences enough weight to keep, for example, a slightly higher-bitrate `ROKU` stream below a preferred `US` stream, while still allowing a *materially* better same-resolution stream to win.

## Channel scope filters

Every action can optionally be restricted to a subset of Dispatcharr channels.

### Channel group filter

Enter one or more exact channel-group names or IDs, separated by commas, semicolons, or new lines:

```text
Local
Sports
id:7
```

The filter uses the channel's **effective** group, so an explicit `ChannelOverride.channel_group` takes precedence over the raw channel group.

### Channel profile filter

Enter one or more exact channel-profile names or IDs:

```text
Stream Sort Test
id:3
```

Only memberships with `enabled=true` are included.

Multiple values inside one filter are ORed. If both filters are populated, the result is their intersection. For example, `Local` plus profile `Stream Sort Test` processes only enabled profile members that are also effectively in `Local`.

Empty group/profile filters mean all channels. Unknown names/IDs are treated as configuration errors instead of silently producing an empty scope.

The same scope applies to **Dry Run**, **Sort Streams**, **Probe Throughput**, and **Probe + Sort**, which makes a temporary profile or a high-stream-count group such as `Local` useful for controlled testing.

## Default name rules

```text
US=20
GO=10
TUBI=0
PRIME=-10
ROKU=-20
```

`PREFIX=score` is shorthand for a case-insensitive anchored prefix match. Advanced rules use `score::regex`, for example:

```text
-50::\bBACKUP\b
10::^USA?\s*[|:_-]
```

All matching name rules are additive.

## M3U source scores

Match by exact account name or account ID:

```text
Preferred Provider=20
id:4=10
Backup Provider=-10
```

The first matching source rule is used.

## Actions

- **Dry Run** — generates `/data/dispatcharr_stream_sort_report.json` without changing stream order.
- **Sort Streams** — updates only `ChannelStream.order` using a transaction and `bulk_update()`.
- **Probe Throughput** — starts a background job that measures sustained delivery speed and caches it in `/data/dispatcharr_stream_sort_throughput.json`. The baseline uses an 8-second window, a 6-probe/minute global start cap, a 1-second per-M3U-source delay, and each source's configured Dispatcharr User-Agent.
- **Probe + Sort** — runs the background throughput job and applies the sort when probing completes.

Throughput is classified relative to a coarse nominal bitrate for the stream's resolution/FPS class (separate from IPTV Checker's measured content bitrate):

- Healthy: >= 1.50x nominal bitrate
- Marginal: >= 1.10x nominal bitrate
- Insufficient: < 1.10x nominal bitrate
- Unknown: no usable/current measurement

Cached throughput expires after the configured TTL (30 minutes by default) and then stops affecting ranking until refreshed. Only one throughput job can run at a time across Dispatcharr workers. Failed probes are treated as unknown/retryable rather than proof that a stream is dead.

## Installation for development testing

Copy the `stream_sorter` directory into Dispatcharr's plugin directory (`/data/plugins/stream_sorter`), reload plugins, enable **Dispatcharr Stream Sort**, configure rules, and start with **Dry Run**.

A good first live test is to set `Channel group filter` to `Local`, or create a temporary channel profile containing several channels with many attached streams. Review `/data/dispatcharr_stream_sort_report.json` before applying the first sort.

## Development

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```

Initial development is intended for the `dev-test` branch before promotion to `dev`/release branches.
