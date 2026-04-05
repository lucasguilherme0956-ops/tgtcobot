import asyncio
import logging
import os

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, InlineQuery, InlineQueryResultArticle, InputTextMessageContent

from config import BOT_TOKEN, CATEGORIES, STATUSES, PRIORITIES, GAME_API_KEY, STATS_SECRET
from database import init_db, close_pool, search_tasks_inline, process_match_report
from handlers import user, admin
from middlewares.throttle import ThrottleMiddleware
from scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Bot reference for sending messages from HTTP handlers
_bot_ref: Bot | None = None


async def health_handler(request):
    return web.Response(text="OK")


async def api_match_report(request):
    """Receive match results from Roblox game server."""
    # Auth check
    auth = request.headers.get("X-API-Key", "")
    if not auth or auth != GAME_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    # Validate required fields
    roblox_id = data.get("roblox_id")
    roblox_username = data.get("roblox_username")
    if not roblox_id or not roblox_username:
        return web.json_response({"error": "missing roblox_id or roblox_username"}, status=400)

    try:
        roblox_id = int(roblox_id)
    except (ValueError, TypeError):
        return web.json_response({"error": "roblox_id must be integer"}, status=400)

    try:
        match_id = await process_match_report(roblox_id, str(roblox_username), data)
        return web.json_response({"ok": True, "match_id": match_id})
    except Exception as e:
        logger.error(f"API match report error: {e}")
        return web.json_response({"error": "internal error"}, status=500)


async def api_heartbeat(request):
    """Receive periodic heartbeat with playtime from Roblox server."""
    auth = request.headers.get("X-API-Key", "")
    if not auth or auth != GAME_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    from database import upsert_player
    players = data.get("players", [])
    for p in players:
        rid = p.get("roblox_id")
        name = p.get("roblox_username")
        if rid and name:
            await upsert_player(int(rid), str(name))

    return web.json_response({"ok": True, "count": len(players)})


async def api_pending(request):
    """Return pending stats requests for Roblox server to poll."""
    secret = request.query.get("secret", "")
    if secret != STATS_SECRET:
        return web.json_response({"requests": []})

    import stats_queue
    place = request.query.get("place", "public")
    if place not in ("public", "private"):
        place = "public"
    reqs = stats_queue.pending_stats[place][:]
    stats_queue.pending_stats[place].clear()

    # Deduplicate by username (keep unique)
    seen = set()
    unique = []
    for r in reqs:
        key = r["username"].lower()
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return web.json_response({"requests": unique})


async def api_stats_receive(request):
    """Receive player stats from Roblox game server."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    if data.get("secret") != STATS_SECRET:
        return web.json_response({"error": "unauthorized"}, status=401)

    username = data.get("username", "")
    error = data.get("error")
    stats = data.get("stats")
    key = username.lower()

    # Cache stats in DB if we got valid data
    if stats and not error:
        import json
        from database import save_stats_cache
        try:
            await save_stats_cache(username, json.dumps(stats))
        except Exception as e:
            logger.error(f"Failed to cache stats for {username}: {e}")

    import stats_queue
    waiters = stats_queue.stats_waiters.pop(key, [])
    if not waiters:
        return web.json_response({"ok": True, "delivered": 0})

    bot = _bot_ref
    if not bot:
        return web.json_response({"error": "bot not ready"}, status=503)

    delivered = 0
    for w in waiters:
        try:
            if error:
                await bot.edit_message_text(
                    chat_id=w["chat_id"],
                    message_id=w["message_id"],
                    text=f"❌ {error}",
                )
            elif stats:
                text = _format_roblox_stats(stats)
                await bot.edit_message_text(
                    chat_id=w["chat_id"],
                    message_id=w["message_id"],
                    text=text,
                )
            w["event"].set()
            delivered += 1
        except Exception as e:
            logger.error(f"Failed to deliver stats to {w['chat_id']}: {e}")

    return web.json_response({"ok": True, "delivered": delivered})


def _format_roblox_stats(stats: dict) -> str:
    """Format Roblox player stats into a pretty message."""
    username = stats.get("username", "???")
    level = stats.get("level", 0)
    exp = stats.get("exp", 0)
    exp_next = stats.get("expToNextLevel", 0)
    money = stats.get("money", 0)
    event_money = stats.get("eventMoney", 0)
    wins = stats.get("wins", 0)
    daily_streak = stats.get("dailyStreak", 0)
    play_time = stats.get("playTime", 0)

    owned_towers = stats.get("ownedTowers", [])
    selected_towers = stats.get("selectedTowers", [])
    owned_skins = stats.get("ownedSkins", [])
    selected_skins = stats.get("selectedSkins", [])

    coin_boosts = stats.get("coinBoosts", 0)
    exp_boosts = stats.get("expBoosts", 0)

    owned_tags = stats.get("ownedTags", [])
    selected_tag = stats.get("selectedTag", "")

    money_rank = stats.get("moneyRank")
    wins_rank = stats.get("winsRank")
    is_online = stats.get("isOnline", False)

    # Format money with commas
    money_str = f"{money:,}"
    event_money_str = str(event_money)
    pt_hours = play_time // 60
    pt_mins = play_time % 60

    lines = [
        f"🎮 Статистика игрока: {username}\n",
        "📊 Основное:",
        f"├ Уровень: {level} ({exp}/{exp_next} XP)",
        f"├ Монеты: {money_str} 💰",
        f"├ Ивент монеты: {event_money_str}",
        f"├ Победы: {wins} 🏆",
        f"├ Дейли стрик: {daily_streak} дней 🔥",
        f"└ Время в игре: {pt_hours}ч {pt_mins}м ⏰",
        "",
        f"🏰 Башни ({len(owned_towers)}):",
        ", ".join(owned_towers) if owned_towers else "нет",
        "",
        "⚔️ Текущий лодаут:",
        ", ".join(selected_towers) if selected_towers else "нет",
        "",
    ]

    # Skins
    lines.append(f"🎨 Скины ({len(owned_skins)}):")
    if owned_skins:
        lines.append(", ".join(owned_skins))
    else:
        lines.append("нет")

    # Selected skins: format "TowerName: SkinName" -> "TowerName → SkinName"
    if selected_skins:
        formatted = []
        for s in selected_skins:
            if ": " in s:
                tower, skin = s.split(": ", 1)
                formatted.append(f"{tower} → {skin}")
            else:
                formatted.append(s)
        lines.append("Надеты: " + ", ".join(formatted))
    lines.append("")

    # Tag
    lines.append(f"🏷️ Тег: {selected_tag or 'нет'}")
    lines.append("")

    # Boosts
    lines.append("🚀 Бусты:")
    if coin_boosts > 0:
        lines.append(f"├ Монеты: x2 ({coin_boosts} осталось)")
    else:
        lines.append("├ Монеты: нет")
    if exp_boosts > 0:
        lines.append(f"└ Опыт: x2 ({exp_boosts} осталось)")
    else:
        lines.append("└ Опыт: нет")
    lines.append("")

    # Ranking
    lines.append("📈 Рейтинг:")
    lines.append(f"├ Топ монет: #{money_rank}" if money_rank else "├ Топ монет: не в топ-100")
    lines.append(f"└ Топ побед: #{wins_rank}" if wins_rank else "└ Топ побед: не в топ-100")
    lines.append("")

    # Online status
    lines.append("🟢 Сейчас в игре" if is_online else "🔴 Не в игре")

    return "\n".join(lines)


async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан в .env!")
        return

    # Инициализация БД
    await init_db()
    logger.info("База данных инициализирована")

    # Бот и диспетчер
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=None))
    global _bot_ref
    _bot_ref = bot
    dp = Dispatcher(storage=MemoryStorage())

    # Middleware
    throttle = ThrottleMiddleware()
    dp.message.middleware(throttle)
    dp.callback_query.middleware(throttle)

    # Роутеры
    dp.include_router(user.router)
    dp.include_router(admin.router)

    # Inline mode handler
    @dp.inline_query()
    async def inline_search(query: InlineQuery):
        text = (query.query or "").strip()
        if len(text) < 2:
            await query.answer([], cache_time=5, is_personal=True)
            return

        tasks = await search_tasks_inline(text, limit=10)
        results = []
        for t in tasks:
            cat = CATEGORIES.get(t["category"], "❓").split()[0]
            status = STATUSES.get(t["status"], "❓").split()[0]
            prio = PRIORITIES.get(t["priority"], "").split()[0]
            short = t["description"][:80] + ("..." if len(t["description"]) > 80 else "")
            title = f"{cat}{status}{prio} #{t['id']}"
            msg_text = (
                f"📋 **Задача #{t['id']}**\n\n"
                f"Категория: {CATEGORIES.get(t['category'], '❓')}\n"
                f"Статус: {STATUSES.get(t['status'], '❓')}\n"
                f"Приоритет: {PRIORITIES.get(t['priority'], '❓')}\n"
                f"Описание: {t['description'][:500]}"
            )
            results.append(
                InlineQueryResultArticle(
                    id=str(t["id"]),
                    title=title,
                    description=short,
                    input_message_content=InputTextMessageContent(
                        message_text=msg_text,
                        parse_mode="Markdown",
                    ),
                )
            )
        await query.answer(results, cache_time=10, is_personal=True)

    # Планировщик
    sched = setup_scheduler(bot)
    sched.start()
    logger.info("Планировщик запущен")

    # Мини-веб-сервер для Render (health check)
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_post("/api/match", api_match_report)
    app.router.add_post("/api/heartbeat", api_heartbeat)
    app.router.add_get("/api/pending", api_pending)
    app.router.add_post("/api/stats", api_stats_receive)
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health-check сервер на порту {port}")

    # Запуск
    logger.info("Бот запускается...")
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="feed", description="Лента идей / Ideas Feed"),
        BotCommand(command="profile", description="Мой профиль / My profile"),
        BotCommand(command="stats", description="Статистика игрока (публичный)"),
        BotCommand(command="statsprivate", description="Статистика (приватный сервер)"),
        BotCommand(command="link", description="Привязать Roblox / Link Roblox"),
        BotCommand(command="top", description="Топ игроков / Leaderboard"),
        BotCommand(command="admin", description="Админ-панель"),
        BotCommand(command="export", description="Экспорт задач (CSV)"),
        BotCommand(command="lang", description="Сменить язык / Change language"),
    ])
    try:
        await dp.start_polling(bot)
    finally:
        sched.shutdown()
        await runner.cleanup()
        await close_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
