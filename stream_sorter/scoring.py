from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


DEFAULT_PREFIX_RULES = """# Prefix=score. Prefix rules are case-insensitive and anchored to the start of the stream name.
US=20
GO=10
TUBI=0
PRIME=-10
ROKU=-20
"""

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

RESOLUTION_RANK = {0: 0, 240: 1, 360: 2, 480: 3, 576: 4, 720: 5, 1080: 6, 1440: 7, 2160: 8}

THROUGHPUT_SCORES = {
    "healthy": 15.0,
    "marginal": 5.0,
    "unknown": 0.0,
    "insufficient": -30.0,
    "dead": -100.0,
}


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
    breakdown: dict[str, float] = field(default_factory=dict)
    matched_name_rules: list[str] = field(default_factory=list)
    source_rule: str | None = None
    notes: list[str] = field(default_factory=list)

    def sort_key(self) -> tuple[float, ...]:
        return (-self.viability_rank, -self.resolution_rank, -self.total_score, self.original_order, self.stream_id)


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
    return None if n is None else int(round(n))


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
    if height >= 2000: return 2160
    if height >= 1300: return 1440
    if height >= 1000: return 1080
    if height >= 700: return 720
    if height >= 560: return 576
    if height >= 470: return 480
    if height >= 340: return 360
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
    for key in ("video_bitrate", "video_bitrate_kbps", "calculated_bitrate_kbps", "bitrate", "bit_rate"):
        n = _number(stats.get(key))
        if n is None or n <= 0:
            continue
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
    if ratio >= 1.25: return 30.0
    if ratio >= 1.0: return 20.0 + ((ratio - 1.0) / 0.25) * 10.0
    if ratio >= 0.75: return 5.0 + ((ratio - 0.75) / 0.25) * 15.0
    if ratio >= 0.50: return -10.0 + ((ratio - 0.50) / 0.25) * 15.0
    if ratio >= 0.35: return -25.0 + ((ratio - 0.35) / 0.15) * 15.0
    return -35.0


def fps_score(fps: float | None) -> float:
    if fps is None: return 0.0
    if fps >= 50: return 10.0
    if fps >= 29: return 5.0
    if fps >= 23: return 2.0
    return -8.0


def classify_throughput(measured_mbps: float | None, nominal_video_kbps: float | None) -> str:
    if measured_mbps is None or nominal_video_kbps is None or nominal_video_kbps <= 0:
        return "unknown"
    ratio = measured_mbps / (nominal_video_kbps / 1000.0)
    if ratio >= 1.50: return "healthy"
    if ratio >= 1.10: return "marginal"
    return "insufficient"


def parse_source_rules(text: str | None) -> list[SourceRule]:
    rules = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"): continue
        if "=" not in line: raise ValueError(f"Invalid source score rule {raw_line!r}; expected source=score")
        key, raw_score = line.rsplit("=", 1)
        key = key.strip()
        if not key: raise ValueError(f"Invalid source score rule {raw_line!r}; source is empty")
        try: score = float(raw_score.strip())
        except ValueError as exc: raise ValueError(f"Invalid source score in {raw_line!r}") from exc
        rules.append(SourceRule(key=key, score=score))
    return rules


def _prefix_pattern(prefix: str) -> str:
    return rf"^{re.escape(prefix.strip())}(?:\s*[|:_-]\s*|\s+|$)"


def parse_name_rules(text: str | None) -> list[NameRule]:
    rules = []
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"): continue
        if "::" in line:
            raw_score, pattern = line.split("::", 1)
            try: score = float(raw_score.strip())
            except ValueError as exc: raise ValueError(f"Invalid regex score in {raw_line!r}") from exc
            pattern = pattern.strip()
            if not pattern: raise ValueError(f"Invalid regex rule {raw_line!r}; pattern is empty")
            try: re.compile(pattern, flags=re.IGNORECASE)
            except re.error as exc: raise ValueError(f"Invalid regex in {raw_line!r}: {exc}") from exc
            rules.append(NameRule(pattern=pattern, score=score, label=pattern))
            continue
        if "=" not in line: raise ValueError(f"Invalid name score rule {raw_line!r}; expected PREFIX=score or score::regex")
        prefix, raw_score = line.rsplit("=", 1)
        prefix = prefix.strip()
        if not prefix: raise ValueError(f"Invalid name score rule {raw_line!r}; prefix is empty")
        try: score = float(raw_score.strip())
        except ValueError as exc: raise ValueError(f"Invalid name score in {raw_line!r}") from exc
        rules.append(NameRule(pattern=_prefix_pattern(prefix), score=score, label=prefix))
    return rules


def source_score(rules: Iterable[SourceRule], account_id: int | None, account_name: str | None) -> tuple[float, str | None]:
    name_norm = (account_name or "").strip().casefold()
    for rule in rules:
        key = rule.key.strip(); key_norm = key.casefold(); numeric_key = key_norm[3:] if key_norm.startswith("id:") else key_norm
        if account_id is not None and numeric_key.isdigit() and int(numeric_key) == int(account_id): return rule.score, rule.key
        if name_norm and key_norm == name_norm: return rule.score, rule.key
    return 0.0, None


def _throughput_entry_status(entry, nominal_video_kbps, cache_ttl_hours, now):
    if not entry: return "unknown", []
    notes = []; status = str(entry.get("status") or "").strip().lower(); tested_at = entry.get("tested_at")
    if cache_ttl_hours and tested_at:
        try:
            tested_dt = datetime.fromisoformat(str(tested_at).replace("Z", "+00:00"))
            if tested_dt.tzinfo is None: tested_dt = tested_dt.replace(tzinfo=timezone.utc)
            now_dt = now or datetime.now(timezone.utc)
            if now_dt.tzinfo is None: now_dt = now_dt.replace(tzinfo=timezone.utc)
            age_hours = (now_dt - tested_dt).total_seconds() / 3600.0
            if age_hours > cache_ttl_hours:
                notes.append(f"throughput cache stale ({age_hours:.1f}h)"); return "unknown", notes
        except (TypeError, ValueError): notes.append("invalid throughput tested_at")
    if status in THROUGHPUT_SCORES: return status, notes
    return classify_throughput(_number(entry.get("measured_mbps")), nominal_video_kbps), notes


def evaluate_candidate(candidate: StreamCandidate, *, source_rules=(), name_rules=(), throughput_cache_ttl_hours=24.0, now=None) -> Evaluation:
    stats = candidate.stats or {}; width, height = parse_resolution(stats); tier = resolution_tier_from_height(height); fps = parse_fps(stats); bitrate = parse_video_bitrate_kbps(stats)
    viability_rank = 2; viability = "usable"; notes = []
    if candidate.is_stale:
        viability_rank = 0; viability = "stale"; notes.append("stream is marked stale")
    elif not candidate.url:
        viability_rank = 0; viability = "no_url"; notes.append("stream has no URL")
    elif not candidate.m3u_account_active:
        viability_rank = 0; viability = "inactive_source"; notes.append("M3U source is inactive")
    elif candidate.stats is not None and len(candidate.stats) == 0 and candidate.stats_updated_at is not None:
        viability_rank = 0; viability = "known_dead"; notes.append("empty stats with a previous stats timestamp")
    target = bitrate_target_kbps(tier)
    if viability_rank > 0 and bitrate is not None and target is not None and bitrate < target * 0.10:
        viability_rank = 1; viability = "content_starved"; notes.append("video bitrate is below 10% of the tier target")
    throughput_status, throughput_notes = _throughput_entry_status(candidate.throughput, bitrate, throughput_cache_ttl_hours, now); notes.extend(throughput_notes)
    if throughput_status == "dead": viability_rank = 0; viability = "probe_dead"; notes.append("throughput probe failed")
    b_score = bitrate_score(bitrate, tier); f_score = fps_score(fps); s_score, matched_source = source_score(source_rules, candidate.m3u_account_id, candidate.m3u_account_name)
    n_score = 0.0; matched_names = []
    for rule in name_rules:
        if rule.matches(candidate.name): n_score += rule.score; matched_names.append(rule.label)
    t_score = THROUGHPUT_SCORES.get(throughput_status, 0.0)
    breakdown = {"bitrate": round(b_score, 3), "fps": round(f_score, 3), "source": round(s_score, 3), "name_rules": round(n_score, 3), "throughput": round(t_score, 3)}
    return Evaluation(candidate.stream_id, candidate.name, candidate.original_order, viability_rank, viability, tier, RESOLUTION_RANK.get(tier, 0), round(sum(breakdown.values()), 3), width, height, fps, bitrate, throughput_status, breakdown, matched_names, matched_source, notes)


def rank_candidates(candidates: Iterable[StreamCandidate], *, source_rules=(), name_rules=(), throughput_cache_ttl_hours=24.0, now=None) -> list[Evaluation]:
    return sorted([evaluate_candidate(c, source_rules=source_rules, name_rules=name_rules, throughput_cache_ttl_hours=throughput_cache_ttl_hours, now=now) for c in candidates], key=Evaluation.sort_key)
