# Dispatcharr Stream Sort Plugin

A focused Dispatcharr plugin that **only reorders streams already assigned to channels**. It does not match streams, create channels, rename channels, or change EPG assignments.

## Sorting model

1. Unusable streams are demoted first: stale, inactive-source, missing-URL, known-dead, and throughput-probe-dead streams.
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
- **Probe Throughput** — measures delivery speed and caches it in `/data/dispatcharr_stream_sort_throughput.json`.
- **Probe + Sort** — refreshes throughput data and immediately sorts.

Throughput is classified relative to the stream's measured video bitrate:

- Healthy: >= 1.50x nominal bitrate
- Marginal: >= 1.10x nominal bitrate
- Insufficient: < 1.10x nominal bitrate
- Unknown: no usable/current measurement

Cached throughput expires after the configured TTL and then stops affecting ranking until refreshed.

## Installation for development testing

Copy the `stream_sorter` directory into Dispatcharr's plugin directory (`/data/plugins/stream_sorter`), reload plugins, enable **Dispatcharr Stream Sort**, configure rules, and start with **Dry Run**.

Review `/data/dispatcharr_stream_sort_report.json` before applying the first sort.

## Development

```bash
python -m pytest
python -m compileall -q stream_sorter tests
```

Initial development is intended for the `dev-test` branch before promotion to `dev`/release branches.
