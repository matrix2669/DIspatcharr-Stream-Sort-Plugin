# Stream Sort TODO

- [ ] Add a Dispatcharr-supported playback exclusion mechanism if core exposes one; keep current dead-stream handling report-only and non-destructive.
- [ ] Simplify the settings UI and scoring, possibly using select boxes with values from -5 to +5.
- [ ] Add multi-line input boxes for prefix rules.
- [ ] Consider an optional scheduler pause while streams are actively playing.
- [ ] Evaluate provider-aware TTL spreading only if stream-level concentration reports show specific accounts are being hammered.

## Draft: Event Channel Stream Monitor supplement

Status: Draft and deferred until Stream Sort's base analysis, retry, TTL, reporting, scheduling, and sorting behavior has accumulated clean operational evidence.

### Goal

- Determine whether streams that are expected to be offline between scheduled events materially distort Stream Sort's dead-stream reports and TTL recommendations.
- If the evidence supports it, create a separate supplemental plugin that monitors event channels and asks Stream Sort to analyze and sort only those channels before their events.
- Keep EPG interpretation, event-name matching, and event scheduling outside Stream Sort so Stream Sort remains responsible only for standard scoped analysis and sorting.

### Proposed ownership boundary

- The Event Channel Stream Monitor owns event-channel configuration, EPG lookup and interpretation, optional stream/channel-name matching, pre-event timing, duplicate-event handling, and trigger history.
- Stream Sort owns FFprobe and FFmpeg checks, retries, provider capacity, TTL evidence, health and throughput history, scoring, and `ChannelStream.order` changes.
- The supplemental plugin does not perform media probes, mutate Stream Sort settings, import private Stream Sort implementation functions, or create, delete, match, rename, regroup, or reassign channels, streams, or EPG data.
- Event channels should be excluded from the ordinary hourly Stream Sort schedule after the monitor becomes responsible for them, preventing expected event inactivity from distorting general dead-stream statistics.

### Proposed workflow

- Allow operators to define event-channel scope through explicit channels or groups; use EPG scheduling as the primary timing signal and optional name patterns only as a configurable fallback or discovery aid.
- Monitor upcoming events and submit a scoped Stream Sort analyze-and-sort request early enough to complete initial probes and immediate retries before the broadcast starts.
- Submit channel IDs through Stream Sort's supported external contract without changing Stream Sort's saved UI filters or schedule.
- Default supplemental scheduled work to serial checks unless later evidence supports parallel execution.
- Leave the latest Stream Sort result cached between events; the next pre-event request establishes fresh health and order without teaching Stream Sort an event-specific health state.
- Record the event identity, affected channels, schedule source, planned and actual trigger times, Stream Sort request identity, queue outcome, completion outcome, and any missed-event reason.

### Stream Sort prerequisites

- Preserve a supported external entry point for scoped analysis with optional sorting, as defined by ADR-016.
- Record every current channel attachment for each stream in health reporting so current dead-stream results can be correlated with event channels and a stream attached to multiple channels is represented accurately.
- Return durable request state that lets the supplemental plugin distinguish queued, running, completed, rejected, expired, canceled, and failed work.

### Queue and safety expectations

- Queue an external request behind an active Stream Sort scan instead of returning busy solely because the execution lease is held.
- Enforce a configurable finite maximum queue depth; return a clear queue-full/busy result only after that limit is reached.
- Execute every queued request through Stream Sort's existing execution lease, cancellation, provider-capacity, viewer-protection, retry, checkpoint, and sorting safeguards.
- Do not let queue admission reserve provider capacity before the request starts.
- Define request deduplication, ordering, expiration/deadline behavior, restart persistence, cancellation ownership, and the exact default queue depth before implementation.

### Evidence and open decisions

- After the current clean scan, join terminal-dead stream IDs to current Dispatcharr channel assignments and inspect channel names, groups, and current or upcoming EPG data to estimate the event-channel share.
- Add passive channel attribution before relying on historical event-channel conclusions; a post-scan database join describes current assignments but cannot reconstruct past attachment changes.
- Determine the event lead time, post-event window, missing/stale EPG behavior, name-pattern precedence, rescheduled/canceled event behavior, and retry cadence from observed scan duration and event data.
- Determine whether one pre-event request is sufficient or whether evidence supports an earlier preparation pass plus a near-event confirmation pass without undermining the fewer-provider-checks goal.
- Revisit implementation only after Stream Sort's base functionality is operating cleanly and event-channel correlation shows enough impact to justify the supplemental plugin.
