import time
from collections import defaultdict
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery

from config import THROTTLE_RATE, BAN_THRESHOLD, BAN_DURATION_HOURS
from database import is_banned, ban_user, is_admin

# Порог предупреждения — за 3 нарушения до бана
WARN_THRESHOLD = BAN_THRESHOLD - 3

# Кэш админов чтобы не лезть в БД на каждое событие
_admin_cache: dict[int, float] = {}   # user_id -> expiry (monotonic)
_ADMIN_CACHE_TTL = 300  # 5 мин


async def _is_admin_cached(user_id: int) -> bool:
    now = time.monotonic()
    expiry = _admin_cache.get(user_id)
    if expiry and now < expiry:
        return True
    result = await is_admin(user_id)
    if result:
        _admin_cache[user_id] = now + _ADMIN_CACHE_TTL
    return result


def invalidate_admin_cache(user_id: int | None = None):
    """Сбросить кеш админа. Если user_id=None — сбросить весь кеш."""
    if user_id is None:
        _admin_cache.clear()
    else:
        _admin_cache.pop(user_id, None)


class ThrottleMiddleware(BaseMiddleware):
    """
    Антифлуд middleware:
    - 1 сообщение в THROTTLE_RATE секунд на пользователя
    - Предупреждение за 3 нарушения до бана
    - При BAN_THRESHOLD нарушениях — бан на BAN_DURATION_HOURS часов
    - После бана счётчик сбрасывается
    - Админы не ограничиваются
    """

    def __init__(self):
        super().__init__()
        self._timestamps: dict[int, float] = {}
        self._violations: dict[int, int] = defaultdict(int)
        self._last_violation_time: dict[int, float] = {}
        # Автосброс нарушений через 5 минут тишины
        self._violation_reset_seconds = 300
        # Счётчик событий за окно (burst detection)
        self._burst_events: dict[int, list[float]] = defaultdict(list)
        self._burst_window = 10.0  # секунд
        self._burst_limit = 8      # макс событий за окно

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = None
        if isinstance(event, Message):
            user = event.from_user
        elif isinstance(event, CallbackQuery):
            user = event.from_user

        if not user:
            return await handler(event, data)

        user_id = user.id

        # Админы не ограничиваются
        if await _is_admin_cached(user_id):
            return await handler(event, data)

        # Проверка бана
        if await is_banned(user_id):
            if isinstance(event, Message):
                await event.answer("⛔ Вы временно заблокированы за флуд. Попробуйте позже.")
            elif isinstance(event, CallbackQuery):
                await event.answer("⛔ Вы временно заблокированы за флуд.", show_alert=True)
            return

        now = time.monotonic()

        # Автосброс нарушений если пользователь затих на 5 минут
        last_v = self._last_violation_time.get(user_id, 0)
        if now - last_v > self._violation_reset_seconds and self._violations[user_id] > 0:
            self._violations[user_id] = 0

        # Throttle
        last = self._timestamps.get(user_id, 0)
        if now - last < THROTTLE_RATE:
            self._violations[user_id] += 1
            self._last_violation_time[user_id] = now
            v = self._violations[user_id]

            if v >= BAN_THRESHOLD:
                await ban_user(user_id, BAN_DURATION_HOURS, "Автоматический бан за флуд")
                self._violations[user_id] = 0
                if isinstance(event, Message):
                    await event.answer(
                        f"⛔ Вы заблокированы на {BAN_DURATION_HOURS} ч. за флуд."
                    )
                elif isinstance(event, CallbackQuery):
                    await event.answer(
                        f"⛔ Заблокированы на {BAN_DURATION_HOURS} ч. за флуд.",
                        show_alert=True,
                    )
            elif v >= WARN_THRESHOLD:
                remaining = BAN_THRESHOLD - v
                if isinstance(event, Message):
                    await event.answer(
                        f"⚠️ Вы отправляете сообщения слишком часто! "
                        f"Ещё {remaining} нарушений — бан на {BAN_DURATION_HOURS} ч."
                    )
                elif isinstance(event, CallbackQuery):
                    await event.answer(
                        f"⚠️ Слишком часто! Ещё {remaining} — бан.",
                        show_alert=True,
                    )
            return

        self._timestamps[user_id] = now

        # Burst detection: много событий за короткое время (даже если каждое чуть > THROTTLE_RATE)
        events = self._burst_events[user_id]
        events.append(now)
        # Очистить старые события за пределами окна
        self._burst_events[user_id] = [e for e in events if now - e < self._burst_window]
        if len(self._burst_events[user_id]) > self._burst_limit:
            self._violations[user_id] += 2
            self._last_violation_time[user_id] = now
            v = self._violations[user_id]
            if v >= BAN_THRESHOLD:
                await ban_user(user_id, BAN_DURATION_HOURS, "Автобан: burst-флуд")
                self._violations[user_id] = 0
                self._burst_events[user_id] = []
                if isinstance(event, CallbackQuery):
                    await event.answer(f"⛔ Заблокированы на {BAN_DURATION_HOURS} ч. за флуд.", show_alert=True)
                return
            if isinstance(event, CallbackQuery):
                await event.answer("⚠️ Помедленнее!", show_alert=True)
                return

        return await handler(event, data)
