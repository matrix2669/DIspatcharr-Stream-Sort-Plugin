from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional


DEFAULT_PREFIX_RULES = """# Prefix=score. Prefix rules are case-insensitive and anchored to the start of the stream name.
US=20
GO=10
TUBI=0
PRIME=-10
ROKU=-20
"""

# Approximate "adequate" video bitrate targets in kbps. They are intentionally
# codec-agnostic in v0.1.0; the score is capped so bitrate cannot dominate the
# operator's source/name preferences by itself.
BITRATE_TARGET_KBPS = {
    2160: 16000,
    1440: 10000,
    1080: 6000,
    720: 3500,
    576: 2200,
    480: 1500,
    360: 900,
    240: 500,
}

# Delivery-throughput baseline mirrors Stream-Mapparr's coarse resolution/FPS
# heuristic. This is intentionally separate from IPTV Checker's measured video
# bitrate: throughput answers "is delivery keeping up with a reasonable stream
# of this class?", while measured video bitrate is scored as content quality.
NOMINAL_THROUGHPUT_KBPS = {
    (2160, "low"): 16000,
    (2160, "high"): 25000,
    (1080, "low"): 4000,
    (1080, "high"): 6000,
    (720, "low"): 2500,
    (720, "high"): 4000,
    (480, "low"): 1500,
    (480, "high"): 2000,
    (360, "low"): 800,
    (360, "high"): 1200,
}
NOMINAL_THROUGHPUT_FALLBACK_KBPS = 2500

# Higher number means higher hard resolution tier.
RESOLUTION_RANK = {
    0: 0,
    240: 1,
    360: 2,
    480: 3,
    576: 4,
    720: 5,
    1080: 6,
    1440: 7,
    2160: 8,
}

THROUGHPUT_SCORES = {
    "healthy": 15.0,
    "marginal": 5.0,
    "unknown": 0.0,
    "insufficient": -30.0,
}
RELIABILITY_MIN_PLAYBACK_SECONDS = 1800.0
RELIABILITY_MIN_STARTS = 3.0
RELIABILITY_SCORE_LIMIT = 20.0


@dataclass(frozen=True)
class NameRule:
    pattern: str
    score: float
    label: str

    def matches(self, value: str) -> bool:
        return re.search(self.pattern, value or "", flags=re.IGNORECASE) is not None


@dataclass(frozen=True)
class SourceRule:
    key: str
    score: float


@dataclass
class StreamCandidate:
    stream_id: int
    name: str
    original_order: int
    stats: Mapping[str, Any] | None = None
    stats_updated_at: datetime | None = None
    m3u_account_id: int | None = None
    m3u_account_name: str = ""
    m3u_account_active: bool = True
    is_stale: bool = False
    url: str | None = None
    throughput: Mapping[str, Any] | None = None
    reliability: Mapping[str, Any] | None = None


@dataclass
class Evaluation:
    stream_id: int
    name: str
    original_order: int
    viability_rank: int
    viability: str
    resolution_tier: int
    resolution_rank: int
    total_score: float
    width: int | None
    height: int | None
    fps: float | None
    video_bitrate_kbps: float | None
    throughput_status: str
    reliability_status: str = "insufficient_evidence"
    breakdown: dict[str, float] = field(default_factory=dict)
    matched_name_rules: list[str] = field(default_factory=list)
    source_rule: str | None = None
    notes: list[str] = field(default_factory=list)

    def sort_key(self) -> tuple[float, ...]:
        # Ascending key: higher viability/resolution/score sorts first, existing
        # order remains the stable final tie-breaker.
        return (
            -self.viability_rank,
            -self.resolution_rank,
            -self.total_score,
            self.original_order,
            self.stream_id,
        )


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(n):
        return None
    return n


def _int_number(value: Any) -> int | None:
    n = _number(value)
    if n is None:
        return None
    return int(round(n))


def parse_resolution(stats: Mapping[str, Any] | None) -> tuple[int | None, int | None]:
    stats = stats or {}
    width = _int_number(stats.get("width"))
    height = _int_number(stats.get("height"))
    if width and height:
        return width, height

    resolution = stats.get("resolution")
    if isinstance(resolution, str):
        match = re.search(r"(?P<w>\d{3,5})\s*[xX×]\s*(?P<h>\d{3,5})", resolution)
        if match:
            return int(match.group("w")), int(match.group("h"))
    return width, height


def resolution_tier_from_height(height: int | None) -> int:
    if not height or height <= 0:
        return 0
    # Tolerances account for cropped broadcast frames and imperfect metadata.
    if height >= 2000:
        return 2160
    if height >= 1300:
        return 1440
    if height >= 1000:
        return 1080
    if height >= 700:
        return 720
    if height >= 560:
        return 576
    if height >= 470:
        return 480
    if height >= 340:
        return 360
    return 240


def parse_fps(stats: Mapping[str, Any] | None) -> float | None:
    stats = stats or {}
    for key in ("source_fps", "fps", "frame_rate", "framerate"):
        value = stats.get(key)
        n = _number(value)
        if n is not None and n > 0:
            return n
        if isinstance(value, str) and "/" in value:
            try:
                num, den = value.split("/", 1)
                den_f = float(den)
                if den_f:
                    n = float(num) / den_f
                    if n > 0:
                        return n
            except (TypeError, ValueError, ZeroDivisionError):
                pass
    return None


def parse_video_bitrate_kbps(stats: Mapping[str, Any] | None) -> float | None:
    stats = stats or {}
    for key in (
        "video_bitrate",
        "video_bitrate_kbps",
        "calculated_bitrate_kbps",
        "bitrate",
        "bit_rate",
    ):
        n = _number(stats.get(key))
        if n is None or n <= 0:
            continue
        # IPTV Checker stores video_bitrate as kbps. Other probes often store
        # bit_rate in bits/s, so normalize obviously-large values.
        if n >= 100_000:
            n /= 1000.0
        return n
    return None


def bitrate_target_kbps(resolution_tier: int) -> float | None:
    return float(BITRATE_TARGET_KBPS.get(resolution_tier, 0)) or None


def bitrate_score(video_bitrate_kbps: float | None, resolution_tier: int) -> float:
    target = bitrate_target_kbps(resolution_tier)
    if video_bitrate_kbps is None or target is None:
        return 0.0
    ratio = max(0.0, video_bitrate_kbps / target)

    # Capped piecewise curve. Going from genuinely poor to adequate matters a
    # lot; going from adequate to excessive matters little.
    if ratio >= 1.25:
        return 30.0
    if ratio >= 1.0:
        return 20.0 + ((ratio - 1.0) / 0.25) * 10.0
    if ratio >= 0.75:
        return 5.0 + ((ratio - 0.75) / 0.25) * 15.0
    if ratio >= 0.50:
        return -10.0 + ((ratio - 0.50) / 0.25) * 15.0
    if ratio >= 0.35:
        return -25.0 + ((ratio - 0.35) / 0.15) * 15.0
    return -35.0


def fps_score(fps: float | None) -> float:
    if fps is None:
        return 0.0
    if fps >= 50:
        return 10.0
    if fps >= 29:
        return 5.0
    if fps >= 23:
        return 2.0
    return -8.0


def estimate_nominal_throughput_kbps(height: int | None, fps: float | None) -> float:
    try:
        h = int(height or 0)
        f = float(fps or 0)
    except (TypeError, ValueError):
        return float(NOMINAL_THROUGHPUT_FALLBACK_KBPS)
    if h <= 0:
        return float(NOMINAL_THROUGHPUT_FALLBACK_KBPS)
    for bucket in (2160, 1080, 720, 480, 360):
        if h >= bucket:
            band = "high" if f >= 35 else "low"
            return float(NOMINAL_THROUGHPUT_KBPS[(bucket, band)])
    return float(NOMINAL_THROUGHPUT_FALLBACK_KBPS)


def classify_throughput(
    measured_mbps: float | None,
    nominal_video_kbps: float | None,
) -> str:
    if measured_mbps is None or nominal_video_kbps is None or nominal_video_kbps <= 0:
        return "unknown"
    ratio = measured_mbps / (nominal_video_kbps / 1000.0)
    if ratio >= 1.50:
        return "healthy"
    if ratio >= 1.10:
        return "marginal"
    return "insufficient"


def throughput_ttl_with_jitter(
    ttl_hours: float,
    *,
    identity: Any,
    jitter_percent: float,
) -> float:
    if ttl_hours <= 0 or jitter_percent <= 0:
        return ttl_hours
    jitter_ratio = max(0.0, min(100.0, float(jitter_percent))) / 100.0
    digest = int(hashlib.md5(str(identity or "").encode("utf-8")).hexdigest()[:8], 16)
    variance = (digest / float(0xFFFFFFFF)) * 2.0 - 1.0
    return max(0.0, ttl_hours * (1.0 + variance * jitter_ratio))


def parse_source_rules(text: str | None) -> list[SourceRule]:
    rules: list[SourceRule] = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid source score rule {raw_line!r}; expected source=score")
        key, raw_score = line.rsplit("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid source score rule {raw_line!r}; source is empty")
        try:
            score = float(raw_score.strip())
        except ValueError as exc:
            raise ValueError(f"Invalid source score in {raw_line!r}") from exc
        rules.append(SourceRule(key=key, score=score))
    return rules


def _prefix_pattern(prefix: str) -> str:
    escaped = re.escape(prefix.strip())
    return rf"^{escaped}(?:\s*[|:_-]\s*|\s+|$)"


def parse_name_rules(text: str | None) -> list[NameRule]:
    """Parse name score rules.

    Shorthand:
        US=20
        ROKU=-20

    Advanced raw regex:
        15::^USA?\\s*[|:_-]
        -50::\\bBACKUP\\b
    """
    def looks_like_rule_start(value: str) -> bool:
        value = value.lstrip()
        number = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)"
        return bool(
            re.match(rf"{number}\s*::", value)
            or re.match(rf"[^,=\n]+?=\s*{number}(?:\s*(?:,|$))", value)
        )

    def split_entries(value: str | None) -> list[str]:
        entries: list[str] = []
        for physical_line in (value or "").splitlines():
            line = physical_line.strip()
            if not line or line.startswith("#"):
                continue
            start = 0
            escaped = False
            square_depth = 0
            round_depth = 0
            brace_depth = 0
            for index, char in enumerate(line):
                if escaped:
                    escaped = False
                    continue
                if char == "\\":
                    escaped = True
                    continue
                if char == "[":
                    square_depth += 1
                elif char == "]" and square_depth:
                    square_depth -= 1
                elif char == "(" and not square_depth:
                    round_depth += 1
                elif char == ")" and round_depth and not square_depth:
                    round_depth -= 1
                elif char == "{" and not square_depth:
                    brace_depth += 1
                elif char == "}" and brace_depth and not square_depth:
                    brace_depth -= 1
                elif (
                    char == ","
                    and not square_depth
                    and not round_depth
                    and not brace_depth
                    and looks_like_rule_start(line[index + 1 :])
                ):
                    entries.append(line[start:index].strip())
                    start = index + 1
            entries.append(line[start:].strip())
        return [entry for entry in entries if entry]

    rules: list[NameRule] = []
    for raw_line in split_entries(text):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "::" in line:
            raw_score, pattern = line.split("::", 1)
            try:
                score = float(raw_score.strip())
            except ValueError as exc:
                raise ValueError(f"Invalid regex score in {raw_line!r}") from exc
            pattern = pattern.strip()
            if not pattern:
                raise ValueError(f"Invalid regex rule {raw_line!r}; pattern is empty")
            try:
                re.compile(pattern, flags=re.IGNORECASE)
            except re.error as exc:
                raise ValueError(f"Invalid regex in {raw_line!r}: {exc}") from exc
            rules.append(NameRule(pattern=pattern, score=score, label=pattern))
            continue

        if "=" not in line:
            raise ValueError(
                f"Invalid name score rule {raw_line!r}; expected PREFIX=score or score::regex"
            )
        prefix, raw_score = line.rsplit("=", 1)
        prefix = prefix.strip()
        if not prefix:
            raise ValueError(f"Invalid name score rule {raw_line!r}; prefix is empty")
        try:
            score = float(raw_score.strip())
        except ValueError as exc:
            raise ValueError(f"Invalid name score in {raw_line!r}") from exc
        rules.append(NameRule(pattern=_prefix_pattern(prefix), score=score, label=prefix))
    return rules


def source_score(
    rules: Iterable[SourceRule],
    account_id: int | None,
    account_name: str | None,
) -> tuple[float, str | None]:
    name_norm = (account_name or "").strip().casefold()
    for rule in rules:
        key = rule.key.strip()
        key_norm = key.casefold()
        numeric_key = key_norm[3:] if key_norm.startswith("id:") else key_norm
        if account_id is not None and numeric_key.isdigit() and int(numeric_key) == int(account_id):
            return rule.score, rule.key
        if name_norm and key_norm == name_norm:
            return rule.score, rule.key
    return 0.0, None


def _throughput_entry_status(
    entry: Mapping[str, Any] | None,
    nominal_video_kbps: float | None,
    healthy_ttl_hours: float,
    degraded_ttl_hours: float,
    unknown_ttl_hours: float,
    ttl_jitter_percent: float,
    identity: Any,
    now: datetime | None,
) -> tuple[str, list[str]]:
    if not entry:
        return "unknown", []
    notes: list[str] = []
    status = str(entry.get("status") or "").strip().lower()

    checked_at = entry.get("checked_at") or entry.get("tested_at")
    status_ttl_hours = unknown_ttl_hours if status == "unknown" else degraded_ttl_hours if status != "healthy" else healthy_ttl_hours
    effective_ttl_hours = throughput_ttl_with_jitter(
        status_ttl_hours,
        identity=entry.get("url_hash") or identity,
        jitter_percent=ttl_jitter_percent,
    )
    if not checked_at:
        notes.append("throughput cache missing checked_at")
        return "unknown", notes
    try:
        tested_dt = datetime.fromisoformat(str(checked_at).replace("Z", "+00:00"))
        if tested_dt.tzinfo is None:
            tested_dt = tested_dt.replace(tzinfo=timezone.utc)
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        age_hours = (now_dt - tested_dt).total_seconds() / 3600.0
        if effective_ttl_hours <= 0 or age_hours >= effective_ttl_hours:
            notes.append(f"throughput cache stale ({age_hours:.2f}h; ttl={effective_ttl_hours:.2f}h)")
            return "unknown", notes
    except (TypeError, ValueError):
        notes.append("invalid throughput checked_at")
        return "unknown", notes

    if status in THROUGHPUT_SCORES:
        return status, notes
    measured = _number(entry.get("measured_mbps"))
    return classify_throughput(measured, nominal_video_kbps), notes


def reliability_score(entry: Mapping[str, Any] | None) -> tuple[float, str, list[str]]:
    """Return a bounded soft score from schema-2, time-decayed evidence."""
    evidence = (entry or {}).get("reliability_evidence")
    if not isinstance(evidence, Mapping):
        return 0.0, "insufficient_evidence", []

    playback = max(0.0, _number(evidence.get("playback_seconds")) or 0.0)
    starts = max(0.0, _number(evidence.get("starts")) or 0.0)
    if playback < RELIABILITY_MIN_PLAYBACK_SECONDS and starts < RELIABILITY_MIN_STARTS:
        return 0.0, "insufficient_evidence", [
            f"reliability evidence pending ({playback / 60.0:.1f}m, {starts:.1f} starts)"
        ]

    hours = max(playback / 3600.0, 0.25)
    confidence = min(1.0, max(playback / 36000.0, starts / 10.0))
    clean_stops = max(0.0, _number(evidence.get("clean_stops")) or 0.0)
    startup_failures = max(0.0, _number(evidence.get("startup_failures")) or 0.0)
    playback_failures = max(0.0, _number(evidence.get("playback_failures")) or 0.0)
    reconnects = max(0.0, _number(evidence.get("reconnects")) or 0.0)
    buffering = max(0.0, _number(evidence.get("buffering_events")) or 0.0)
    failovers = max(0.0, _number(evidence.get("failovers")) or 0.0)

    clean_ratio = clean_stops / max(1.0, starts)
    positive = 10.0 * clean_ratio
    penalty = min(
        30.0,
        (startup_failures * 4.0 + playback_failures * 10.0 + failovers * 10.0
         + reconnects * 2.0 + buffering) / hours,
    )
    score = max(-RELIABILITY_SCORE_LIMIT, min(RELIABILITY_SCORE_LIMIT, (positive - penalty) * confidence))
    status = "healthy" if score >= 5 else "degraded" if score <= -5 else "neutral"
    return round(score, 3), status, [
        f"reliability uses {playback / 3600.0:.2f} decayed playback hours and {starts:.1f} starts"
    ]


def evaluate_candidate(
    candidate: StreamCandidate,
    *,
    source_rules: Iterable[SourceRule] = (),
    name_rules: Iterable[NameRule] = (),
    healthy_throughput_ttl_hours: float = 48.0,
    degraded_throughput_ttl_hours: float = 24.0,
    unknown_throughput_ttl_hours: float = 4.0,
    throughput_ttl_jitter_percent: float = 30.0,
    reliability_scoring_enabled: bool = True,
    now: datetime | None = None,
) -> Evaluation:
    stats = candidate.stats or {}
    width, height = parse_resolution(stats)
    tier = resolution_tier_from_height(height)
    fps = parse_fps(stats)
    bitrate = parse_video_bitrate_kbps(stats)

    viability_rank = 2
    viability = "usable"
    notes: list[str] = []

    if candidate.is_stale:
        viability_rank = 0
        viability = "stale"
        notes.append("stream is marked stale")
    elif not candidate.url:
        viability_rank = 0
        viability = "no_url"
        notes.append("stream has no URL")
    elif not candidate.m3u_account_active:
        viability_rank = 0
        viability = "inactive_source"
        notes.append("M3U source is inactive")
    elif candidate.stats is not None and len(candidate.stats) == 0 and candidate.stats_updated_at is not None:
        # IPTV Checker clears stream_stats to {} for a known-dead stream. Null
        # stats with no timestamp remain "not yet measured" rather than dead.
        viability_rank = 0
        viability = "known_dead"
        notes.append("empty stats with a previous stats timestamp")

    target = bitrate_target_kbps(tier)
    if viability_rank > 0 and bitrate is not None and target is not None:
        if bitrate < target * 0.10:
            viability_rank = 1
            viability = "content_starved"
            notes.append("video bitrate is below 10% of the tier target")

    throughput_nominal_kbps = estimate_nominal_throughput_kbps(height, fps)
    throughput_status, throughput_notes = _throughput_entry_status(
        candidate.throughput,
        throughput_nominal_kbps,
        healthy_throughput_ttl_hours,
        degraded_throughput_ttl_hours,
        unknown_throughput_ttl_hours,
        throughput_ttl_jitter_percent,
        candidate.url or candidate.stream_id,
        now,
    )
    notes.extend(throughput_notes)
    b_score = bitrate_score(bitrate, tier)
    f_score = fps_score(fps)
    s_score, matched_source = source_score(
        source_rules, candidate.m3u_account_id, candidate.m3u_account_name
    )
    n_score = 0.0
    matched_names: list[str] = []
    for rule in name_rules:
        if rule.matches(candidate.name):
            n_score += rule.score
            matched_names.append(rule.label)
    t_score = THROUGHPUT_SCORES.get(throughput_status, 0.0)
    r_score, reliability_status, reliability_notes = reliability_score(candidate.reliability)
    if not reliability_scoring_enabled:
        r_score = 0.0
        reliability_status = "disabled"
        reliability_notes = []
    notes.extend(reliability_notes)

    breakdown = {
        "bitrate": round(b_score, 3),
        "fps": round(f_score, 3),
        "source": round(s_score, 3),
        "name_rules": round(n_score, 3),
        "throughput": round(t_score, 3),
        "reliability": round(r_score, 3),
    }
    total = round(sum(breakdown.values()), 3)

    return Evaluation(
        stream_id=candidate.stream_id,
        name=candidate.name,
        original_order=candidate.original_order,
        viability_rank=viability_rank,
        viability=viability,
        resolution_tier=tier,
        resolution_rank=RESOLUTION_RANK.get(tier, 0),
        total_score=total,
        width=width,
        height=height,
        fps=fps,
        video_bitrate_kbps=bitrate,
        throughput_status=throughput_status,
        reliability_status=reliability_status,
        breakdown=breakdown,
        matched_name_rules=matched_names,
        source_rule=matched_source,
        notes=notes,
    )


def rank_candidates(
    candidates: Iterable[StreamCandidate],
    *,
    source_rules: Iterable[SourceRule] = (),
    name_rules: Iterable[NameRule] = (),
    healthy_throughput_ttl_hours: float = 48.0,
    degraded_throughput_ttl_hours: float = 24.0,
    unknown_throughput_ttl_hours: float = 4.0,
    throughput_ttl_jitter_percent: float = 30.0,
    reliability_scoring_enabled: bool = True,
    now: datetime | None = None,
) -> list[Evaluation]:
    evaluations = [
        evaluate_candidate(
            candidate,
            source_rules=source_rules,
            name_rules=name_rules,
            healthy_throughput_ttl_hours=healthy_throughput_ttl_hours,
            degraded_throughput_ttl_hours=degraded_throughput_ttl_hours,
            unknown_throughput_ttl_hours=unknown_throughput_ttl_hours,
            throughput_ttl_jitter_percent=throughput_ttl_jitter_percent,
            reliability_scoring_enabled=reliability_scoring_enabled,
            now=now,
        )
        for candidate in candidates
    ]
    return sorted(evaluations, key=Evaluation.sort_key)
