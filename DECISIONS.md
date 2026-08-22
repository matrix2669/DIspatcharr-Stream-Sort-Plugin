# Architecture Decisions

## ADR-001: Keep project memory inside this standalone repository

- **Status:** Accepted
- **Decision:** Store user documentation in `README.md`, AI/developer guidance
  in `AGENT.md`, architectural decisions here, and release history in
  `CHANGELOG.md`.
- **Reason:** This is a standalone project, so its implementation and durable
  project memory should travel together.

## ADR-002: Limit mutations to stream order

- **Status:** Accepted
- **Decision:** Stream Sort may update only `ChannelStream.order` for streams
  already assigned to a channel.
- **Reason:** Matching, lifecycle management, naming, EPG, and grouping belong
  to Dispatcharr or other dedicated tools. A narrow mutation boundary makes a
  dry run meaningful and limits operational risk.

## ADR-003: Use viability and resolution as hard ordering tiers

- **Status:** Accepted
- **Decision:** Rank viability first and resolution second. Apply bitrate, FPS,
  source preference, name rules, throughput, and reliability only inside those
  tiers; retain existing order as the stable final tie-breaker.
- **Reason:** A dead high-resolution stream must never outrank a usable lower-
  resolution stream, and soft scoring must not erase the user's resolution
  preference.

## ADR-004: Score only attributable schema-2 reliability evidence

- **Status:** Accepted
- **Decision:** Preserve legacy counters for inspection but exclude them from
  scores. Score schema-2 evidence only after 1,800 playback seconds or three
  starts, decay it with a 14-day half-life, and bound its contribution to
  `-20..+20`.
- **Reason:** Older events could be attributed from stale channel state. Fresh
  URL-based evidence is safer, while thresholds, decay, and bounds prevent a
  small or old sample from overwhelming stream quality.

## ADR-005: Refresh analysis components independently

- **Status:** Accepted
- **Decision:** Track metadata, reachability, content validation, and throughput
  freshness independently. Import only qualifying Dispatcharr stream stats and
  clean runtime playback observations; invalidate all components when the URL
  changes.
- **Reason:** This avoids unnecessary provider connections without claiming
  that reused evidence proves checks that did not run.

## ADR-006: Share provider capacity with viewers

- **Status:** Accepted
- **Decision:** Calculate remaining analysis capacity across all active M3U
  profiles, reserve through Dispatcharr's atomic connection pool, resolve the
  URL through the selected profile, and release the exact reservation. Allow
  different providers to run concurrently but limit retries to one per
  provider.
- **Reason:** Analyzer work must not steal viewer capacity or bypass profile-
  specific URL rewriting, and a retry wave must not reproduce a provider-wide
  connection burst.

## ADR-007: Treat the production branch name as unresolved repository state

- **Status:** Proposed
- **Decision:** Continue testing changes on `dev-test`. Do not infer, create, or
  repurpose a production branch until the owner explicitly establishes it.
- **Reason:** Documentation says validated work is promoted to `dev`, but the
  remote currently has no `dev` branch and `main` is only the initial state.
  The repository cannot encode a safe promotion rule until that mismatch is
  resolved.

