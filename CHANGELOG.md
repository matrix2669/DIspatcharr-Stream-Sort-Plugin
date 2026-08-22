# Changelog

Material changes to Dispatcharr Stream Sort are recorded here. Dates and
version groupings are reconstructed from repository history where releases did
not previously have a changelog.

## Unreleased

- Add repository-local agent guidance, architecture decisions, and release
  history for the standalone project workflow.

## 0.3.5 - 2026-08-20

- Reserve analyzer capacity across all active M3U profiles instead of treating
  a provider as a single slot.
- Use Dispatcharr's selected-profile URL resolver so regex and credential
  rewrites are applied to probes.
- Release the exact selected profile/credential reservation.
- Serialize retry checks per provider while retaining concurrency across
  different providers.

## 0.3.0 - 2026-08-17

- Add schema-2 runtime reliability scoring based on attributable stream
  evidence, with minimum sample thresholds, time decay, and a bounded score.
- Resolve channel errors by URL and classify startup failures.
- Reuse qualifying runtime playback as fresh reachability evidence.

## 0.2.5 - 2026-08-17

- Wire plugin analysis actions directly to the incremental analyzer.

## 0.2.4 - 2026-08-17

- Suppress the narrow reconnect event emitted internally after a normal stream
  switch while retaining it as non-counted diagnostic history.

## 0.2.2 - 2026-08-17

- Reuse newer Dispatcharr media metadata and qualifying playback reachability.
- Migrate fresh legacy throughput data into the unified analysis cache.

## 0.2.1 - 2026-08-17

- Add component-specific freshness controls for metadata, reachability, content
  validation, and throughput.
- Unify throughput and media analysis caching.

## 0.1.x

- Introduce the built-in stream analyzer, health and content checks, delivery
  throughput measurement, scoring, dry-run reporting, and order-only updates.
