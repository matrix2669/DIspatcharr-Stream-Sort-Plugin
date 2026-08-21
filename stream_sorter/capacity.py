from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class DispatcharrReservation:
    profile_id: int
    reserved: bool
    url: str
    user_agent: str


class DispatcharrCapacityManager:
    """Reserve analyzer connections across all active Dispatcharr profiles."""

    def __init__(
        self,
        *,
        profiles: Mapping[int, Iterable[Any]],
        streams: Mapping[int, Any],
        redis_client: Any,
        reserve_profile_slot,
        release_profile_slot,
        resolve_live_stream_url,
        logger,
    ) -> None:
        self.profiles = {
            int(account_id): tuple(account_profiles)
            for account_id, account_profiles in profiles.items()
        }
        self.streams = {int(stream_id): stream for stream_id, stream in streams.items()}
        self.redis_client = redis_client
        self.reserve_profile_slot = reserve_profile_slot
        self.release_profile_slot = release_profile_slot
        self.resolve_live_stream_url = resolve_live_stream_url
        self.logger = logger
        self._warned: set[tuple[str, int]] = set()

    def _warn_once(self, reason: str, object_id: int, message: str, *args) -> None:
        key = (reason, object_id)
        if key in self._warned:
            return
        self._warned.add(key)
        self.logger.warning(message, *args)

    def _resolve_reservation(
        self,
        *,
        stream: Any,
        profile: Any,
        reserved: bool,
    ) -> DispatcharrReservation | None:
        try:
            url = self.resolve_live_stream_url(stream, profile.m3u_account, profile)
            user_agent = profile.m3u_account.get_user_agent_string()
        except Exception as exc:
            if reserved:
                self.release_profile_slot(int(profile.id), self.redis_client)
            self._warn_once(
                "resolve_error",
                int(profile.id),
                "[Analyze Capacity] Could not resolve M3U profile %s URL: %s",
                profile.id,
                exc,
            )
            return None
        if not url:
            if reserved:
                self.release_profile_slot(int(profile.id), self.redis_client)
            self._warn_once(
                "empty_url",
                int(profile.id),
                "[Analyze Capacity] M3U profile %s produced an empty stream URL",
                profile.id,
            )
            return None
        return DispatcharrReservation(
            profile_id=int(profile.id),
            reserved=reserved,
            url=str(url),
            user_agent=str(user_agent or ""),
        )

    def try_acquire(
        self,
        item: Mapping[str, Any],
    ) -> tuple[bool, DispatcharrReservation | None]:
        account_id = item.get("account_id")
        if account_id is None:
            return True, None
        account_id = int(account_id)
        profiles = self.profiles.get(account_id)
        if not profiles:
            self._warn_once(
                "missing_profile",
                account_id,
                "[Analyze Capacity] M3U account %s has no active profile; deferring its checks",
                account_id,
            )
            return False, None
        stream_id = int(item["id"])
        stream = self.streams.get(stream_id)
        if stream is None:
            self._warn_once(
                "missing_stream",
                stream_id,
                "[Analyze Capacity] Stream %s disappeared; deferring its checks",
                stream_id,
            )
            return False, None

        for profile in profiles:
            profile_limit = max(0, int(profile.max_streams or 0))
            if profile_limit == 0:
                reservation = self._resolve_reservation(
                    stream=stream,
                    profile=profile,
                    reserved=False,
                )
                return (True, reservation) if reservation is not None else (False, None)
            if self.redis_client is None:
                continue
            try:
                reserved, _current_count, _failure_reason = self.reserve_profile_slot(
                    profile,
                    self.redis_client,
                )
            except Exception as exc:
                self._warn_once(
                    "reserve_error",
                    int(profile.id),
                    "[Analyze Capacity] Could not reserve M3U profile %s: %s",
                    profile.id,
                    exc,
                )
                continue
            if not reserved:
                continue
            reservation = self._resolve_reservation(
                stream=stream,
                profile=profile,
                reserved=True,
            )
            return (True, reservation) if reservation is not None else (False, None)

        if self.redis_client is None:
            self._warn_once(
                "missing_redis",
                account_id,
                "[Analyze Capacity] Redis is unavailable; deferring limited M3U account %s",
                account_id,
            )
        return False, None

    @staticmethod
    def prepare_item(
        item: Mapping[str, Any],
        reservation: DispatcharrReservation | None,
    ) -> Mapping[str, Any]:
        if reservation is None:
            return item
        prepared = dict(item)
        prepared["url"] = reservation.url
        prepared["user_agent"] = reservation.user_agent
        prepared["m3u_profile_id"] = reservation.profile_id
        return prepared

    def release(self, reservation: DispatcharrReservation | None) -> None:
        if reservation is None or not reservation.reserved:
            return
        try:
            self.release_profile_slot(reservation.profile_id, self.redis_client)
        except Exception as exc:
            self.logger.error(
                "[Analyze Capacity] Could not release M3U profile %s: %s",
                reservation.profile_id,
                exc,
            )


def build_capacity_manager(items: Iterable[Mapping[str, Any]], *, logger) -> DispatcharrCapacityManager:
    """Load every active profile and Dispatcharr's native URL/pool helpers."""
    from apps.channels.models import Stream
    from apps.m3u.connection_pool import release_profile_slot, reserve_profile_slot
    from apps.m3u.models import M3UAccountProfile
    from apps.proxy.live_proxy.url_utils import _resolve_live_stream_url
    from core.utils import RedisClient

    items = list(items)
    account_ids = {
        int(item["account_id"])
        for item in items
        if item.get("account_id") is not None
    }
    stream_ids = {int(item["id"]) for item in items}
    streams = {
        int(stream.id): stream
        for stream in Stream.objects.filter(id__in=stream_ids).select_related(
            "m3u_account",
            "m3u_account__user_agent",
        )
    }
    profiles: dict[int, list[Any]] = {}
    queryset = M3UAccountProfile.objects.filter(
        m3u_account_id__in=account_ids,
        is_active=True,
    ).select_related(
        "m3u_account",
        "m3u_account__server_group",
        "m3u_account__user_agent",
    )
    for profile in queryset:
        profiles.setdefault(int(profile.m3u_account_id), []).append(profile)
    for account_profiles in profiles.values():
        account_profiles.sort(
            key=lambda profile: (not bool(profile.is_default), int(profile.id))
        )

    try:
        redis_client = RedisClient.get_client()
    except Exception as exc:
        logger.warning("[Analyze Capacity] Redis connection failed: %s", exc)
        redis_client = None

    return DispatcharrCapacityManager(
        profiles=profiles,
        streams=streams,
        redis_client=redis_client,
        reserve_profile_slot=reserve_profile_slot,
        release_profile_slot=release_profile_slot,
        resolve_live_stream_url=_resolve_live_stream_url,
        logger=logger,
    )
