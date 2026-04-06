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
    place = data.get("place", "public")
    key = username.lower()

    # Cache stats in DB if we got valid data
    if stats and not error:
        import json
        from database import save_stats_cache
        try:
            await save_stats_cache(username, json.dumps(stats), place)
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
                from stats_queue import format_roblox_stats
                text = format_roblox_stats(stats)
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


async def api_bulk_stats(request):
    """Receive stats for all online players from Roblox — cache them."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    if data.get("secret") != STATS_SECRET:
        return web.json_response({"error": "unauthorized"}, status=401)

    import json
    from database import save_stats_cache

    players = data.get("players", [])
    saved = 0
    for p in players:
        username = p.get("username")
        if not username:
            continue
        try:
            await save_stats_cache(username, json.dumps(p))
            saved += 1
        except Exception as e:
            logger.error(f"bulk-stats cache error for {username}: {e}")

    return web.json_response({"ok": True, "saved": saved})


async def api_check_code(request):
    """Roblox validates a promo code entered by a player."""
    auth = request.headers.get("X-API-Key", "")
    if not auth or auth != GAME_API_KEY:
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)
    code = (data.get("code") or "").strip()
    roblox_username = (data.get("roblox_username") or "").strip()
    if not code or not roblox_username:
        return web.json_response({"error": "missing code or roblox_username"}, status=400)
    from database import check_code_for_roblox
    result = await check_code_for_roblox(code, roblox_username)
    return web.json_response(result)


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
    app.router.add_post("/api/bulk-stats", api_bulk_stats)
    app.router.add_post("/api/check-code", api_check_code)
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
        BotCommand(command="news", description="Опубликовать новость (админ)"),
        BotCommand(command="export", description="Экспорт задач (CSV)"),
        BotCommand(command="lang", description="Сменить язык / Change language"),
        BotCommand(command="redeem", description="Активировать промокод"),
        BotCommand(command="faq", description="Часто задаваемые вопросы"),
        BotCommand(command="polls", description="Опросы"),
        BotCommand(command="server", description="Статус сервера"),
        BotCommand(command="weeklytop", description="Топ недели"),
        BotCommand(command="giveaways", description="Розыгрыши"),
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
