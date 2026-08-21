from __future__ import annotations

from typing import Any, Iterable, Mapping


class DispatcharrCapacityManager:
    """Reserve analyzer connections in Dispatcharr's viewer connection pool."""

    def __init__(
        self,
        *,
        limits: Mapping[int, int],
        profiles: Mapping[int, Any],
        redis_client: Any,
        reserve_profile_slot,
        release_profile_slot,
        logger,
    ) -> None:
        self.limits = {int(key): max(0, int(value)) for key, value in limits.items()}
        self.profiles = {int(key): value for key, value in profiles.items()}
        self.redis_client = redis_client
        self.reserve_profile_slot = reserve_profile_slot
        self.release_profile_slot = release_profile_slot
        self.logger = logger
        self._warned: set[tuple[str, int]] = set()

    def _warn_once(self, reason: str, account_id: int, message: str, *args) -> None:
        key = (reason, account_id)
        if key in self._warned:
            return
        self._warned.add(key)
        self.logger.warning(message, *args)

    def try_acquire(self, item: Mapping[str, Any]) -> tuple[bool, int | None]:
        account_id = item.get("account_id")
        if account_id is None:
            return True, None
        account_id = int(account_id)
        limit = self.limits.get(account_id)
        if limit is None:
            self._warn_once(
                "missing_account",
                account_id,
                "[Analyze Capacity] M3U account %s disappeared; deferring its checks",
                account_id,
            )
            return False, None
        if limit == 0:
            return True, None
        profile = self.profiles.get(account_id)
        if profile is None:
            self._warn_once(
                "missing_profile",
                account_id,
                "[Analyze Capacity] M3U account %s has no active default profile; deferring its checks",
                account_id,
            )
            return False, None
        if self.redis_client is None:
            self._warn_once(
                "missing_redis",
                account_id,
                "[Analyze Capacity] Redis is unavailable; deferring limited M3U account %s",
                account_id,
            )
            return False, None
        try:
            reserved, _current_count, _failure_reason = self.reserve_profile_slot(
                profile,
                self.redis_client,
            )
        except Exception as exc:
            self._warn_once(
                "reserve_error",
                account_id,
                "[Analyze Capacity] Could not reserve M3U account %s: %s",
                account_id,
                exc,
            )
            return False, None
        return (True, int(profile.id)) if reserved else (False, None)

    def release(self, profile_id: int | None) -> None:
        if profile_id is None:
            return
        try:
            self.release_profile_slot(int(profile_id), self.redis_client)
        except Exception as exc:
            self.logger.error(
                "[Analyze Capacity] Could not release M3U profile %s: %s",
                profile_id,
                exc,
            )


def build_capacity_manager(items: Iterable[Mapping[str, Any]], *, logger) -> DispatcharrCapacityManager:
    """Load default M3U profiles and Dispatcharr's atomic Redis reservation API."""
    from apps.m3u.connection_pool import release_profile_slot, reserve_profile_slot
    from apps.m3u.models import M3UAccount, M3UAccountProfile
    from core.utils import RedisClient

    account_ids = {
        int(item["account_id"])
        for item in items
        if item.get("account_id") is not None
    }
    limits = {
        int(row["id"]): int(row["max_streams"] or 0)
        for row in M3UAccount.objects.filter(id__in=account_ids).values("id", "max_streams")
    }
    profiles = {}
    queryset = M3UAccountProfile.objects.filter(
        m3u_account_id__in=account_ids,
        is_default=True,
        is_active=True,
    ).select_related("m3u_account", "m3u_account__server_group")
    for profile in queryset:
        account_id = int(profile.m3u_account_id)
        # M3UAccount.max_streams is the user-facing limit and Dispatcharr
        # normally mirrors it to the default profile. Use it in memory as the
        # source of truth without changing database configuration.
        profile.max_streams = limits.get(account_id, int(profile.max_streams or 0))
        profiles[account_id] = profile

    try:
        redis_client = RedisClient.get_client()
    except Exception as exc:
        logger.warning("[Analyze Capacity] Redis connection failed: %s", exc)
        redis_client = None

    return DispatcharrCapacityManager(
        limits=limits,
        profiles=profiles,
        redis_client=redis_client,
        reserve_profile_slot=reserve_profile_slot,
        release_profile_slot=release_profile_slot,
        logger=logger,
    )
