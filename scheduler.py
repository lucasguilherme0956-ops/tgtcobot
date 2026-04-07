import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import TIMEZONE
from database import (
    get_all_admin_ids, get_notify_settings, now_msk, auto_archive_old_done,
    get_tasks_for_auto_priority, update_task_priority,
    get_weekly_report_data, keepalive,
    get_upcoming_deadlines,
    get_ending_giveaways, pick_giveaway_winners, generate_winner_code,
    get_expiring_polls, close_poll,
    log_server_status,
    compute_weekly_top, save_weekly_top, get_all_subscribers,
)
from utils.notifications import build_summary_text

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None
_bot = None
_last_sent: dict[int, str] = {}


async def _send_summary():
    """Проверяет расписание каждого админа и отправляет сводку если время совпадает."""
    if not _bot:
        return

    now = now_msk()
    current_time = now.strftime("%H:%M")

    admin_ids = await get_all_admin_ids()
    summary = None

    for admin_id in admin_ids:
        settings = await get_notify_settings(admin_id)
        if not settings["enabled"]:
            continue
        if current_time not in settings["schedule_times"]:
            continue

        last = _last_sent.get(admin_id)
        if last == current_time:
            continue

        if summary is None:
            summary = await build_summary_text()
        if summary is None:
            return

        try:
            await _bot.send_message(admin_id, summary, parse_mode="Markdown")
            _last_sent[admin_id] = current_time
            logger.info(f"Сводка отправлена админу {admin_id}")
        except Exception as e:
            logger.warning(f"Не удалось отправить сводку админу {admin_id}: {e}")


async def _auto_archive():
    """Auto-archive done tasks older than 30 days."""
    from config import AUTO_ARCHIVE_DAYS
    count = await auto_archive_old_done(AUTO_ARCHIVE_DAYS)
    if count:
        logger.info(f"Авто-архив: {count} задач")


async def _auto_priority():
    """Auto-adjust priority of idea/balance tasks based on votes."""
    tasks = await get_tasks_for_auto_priority()
    for t in tasks:
        votes = t["vote_count"]
        current = t["priority"]
        # Determine priority based on vote count
        if votes >= 10:
            target = "critical"
        elif votes >= 5:
            target = "high"
        elif votes >= 2:
            target = "medium"
        else:
            target = "low"

        if target != current:
            await update_task_priority(t["id"], target)
            logger.info(f"Авто-приоритет: задача #{t['id']} {current} → {target} ({votes} голосов)")


async def _weekly_report():
    """Send weekly report to all admins on Monday at 10:00."""
    if not _bot:
        return

    data = await get_weekly_report_data()
    from config import CATEGORIES

    text = (
        f"📊 **Еженедельный отчёт**\n\n"
        f"📝 Создано задач: {data['created']}\n"
        f"✅ Закрыто задач: {data['closed']}\n"
        f"📦 Архивировано: {data['archived']}\n"
        f"📋 Открыто всего: {data['open_total']}\n"
    )

    if data["top_voted"]:
        text += "\n🏆 **Топ по голосам:**\n"
        for i, tv in enumerate(data["top_voted"], 1):
            cat = CATEGORIES.get(tv["category"], "❓").split()[0]
            short = tv["description"][:50] + ("..." if len(tv["description"]) > 50 else "")
            text += f"  {i}. {cat} #{tv['id']}: {short} ({tv['votes']} 👍)\n"

    admin_ids = await get_all_admin_ids()
    for admin_id in admin_ids:
        try:
            await _bot.send_message(admin_id, text, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Weekly report failed for {admin_id}: {e}")


async def _deadline_reminder():
    """Send reminders for tasks with deadlines in the next 24 hours."""
    if not _bot:
        return

    tasks = await get_upcoming_deadlines(24)
    for task in tasks:
        admin_id = task.get("assigned_admin_id")
        if not admin_id:
            continue
        dl = str(task["deadline"])[:16].replace("T", " ")
        try:
            await _bot.send_message(
                admin_id,
                f"⏰ **Напоминание о дедлайне**\n\n"
                f"Задача #{task['id']}: {task['description'][:60]}\n"
                f"📅 Дедлайн: {dl}",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning(f"Deadline reminder failed for {admin_id}: {e}")


async def _check_giveaways():
    """Завершает розыгрыши по времени, выбирает победителей."""
    if not _bot:
        return
    try:
        ending = await get_ending_giveaways()
    except Exception as e:
        logger.warning(f"_check_giveaways error: {e}")
        return
    for g in ending:
        from database import count_giveaway_entries, get_pool
        entries = await count_giveaway_entries(g["id"])
        if entries == 0:
            pool = await get_pool()
            await pool.execute("UPDATE giveaways SET status = 'ended' WHERE id = $1", g["id"])
            continue
        winners = await pick_giveaway_winners(g["id"], g["winner_count"])
        for w in winners:
            code_text = ""
            if g.get("prize_promo_reward"):
                code = await generate_winner_code(
                    w["telegram_id"], g["prize_text"], g["prize_promo_reward"], g["created_by"])
                code_text = f"\n\n🎟 Ваш код: `{code}`\nАктивируйте: /redeem {code}"
            try:
                await _bot.send_message(
                    w["telegram_id"],
                    f"🎉 Поздравляем! Вы выиграли в розыгрыше «{g['title']}»!\n\n"
                    f"🎖 Приз: {g['prize_text']}{code_text}",
                    parse_mode="Markdown",
                )
            except Exception:
                pass


async def _check_polls():
    """Закрывает опросы по истечении времени."""
    try:
        expiring = await get_expiring_polls()
        for p in expiring:
            await close_poll(p["id"])
    except Exception as e:
        logger.warning(f"_check_polls error: {e}")


async def _monitor_server():
    """Считает онлайн из stats_cache, логирует."""
    if not _bot:
        return
    try:
        import json
        from database import get_pool
        from datetime import timedelta
        pool = await get_pool()
        cutoff = (now_msk() - timedelta(minutes=10)).isoformat()
        rows = await pool.fetch(
            "SELECT stats_json FROM stats_cache WHERE place = 'public' AND updated_at > $1",
            cutoff)
        online = 0
        for r in rows:
            try:
                data = json.loads(r["stats_json"])
                if data.get("isOnline"):
                    online += 1
            except Exception:
                pass
        await log_server_status(online)
    except Exception as e:
        logger.warning(f"_monitor_server error: {e}")


async def _weekly_top_broadcast():
    """Каждый понедельник: топ за неделю, рассылка подписчикам."""
    if not _bot:
        return
    try:
        import asyncio
        from datetime import timedelta
        now = now_msk()
        week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
        for stat in ["wins", "money", "timePlayed"]:
            top = await compute_weekly_top(stat, 10)
            if top:
                await save_weekly_top(week_start, stat, top)
        wins_top = await compute_weekly_top("wins", 10)
        if not wins_top:
            return
        medals = ["🥇", "🥈", "🥉"]
        text = f"📊 **Топ недели** (🏆 Победы):\n🗓 {week_start}\n\n"
        for i, entry in enumerate(wins_top):
            pos = medals[i] if i < 3 else f"{i + 1}."
            text += f"{pos} **{entry['username']}** — {entry['value']}\n"
        subs = await get_all_subscribers()
        for uid in subs:
            try:
                await _bot.send_message(uid, text, parse_mode="Markdown")
            except Exception:
                pass
            await asyncio.sleep(0.05)
    except Exception as e:
        logger.warning(f"_weekly_top_broadcast error: {e}")


def setup_scheduler(bot) -> AsyncIOScheduler:
    global _scheduler, _bot
    _bot = bot

    _scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    _scheduler.add_job(_send_summary, CronTrigger(minute="*"), id="summary_check")
    _scheduler.add_job(_auto_archive, CronTrigger(hour=3, minute=0), id="auto_archive")
    _scheduler.add_job(_auto_priority, CronTrigger(hour="*/6", minute=0), id="auto_priority")
    _scheduler.add_job(_weekly_report, CronTrigger(day_of_week="mon", hour=10, minute=0), id="weekly_report")
    _scheduler.add_job(_deadline_reminder, CronTrigger(hour="*/4", minute=30), id="deadline_reminder")
    _scheduler.add_job(keepalive, CronTrigger(minute="*/4"), id="db_keepalive")
    _scheduler.add_job(_check_giveaways, CronTrigger(minute="*"), id="check_giveaways")
    _scheduler.add_job(_check_polls, CronTrigger(minute="*"), id="check_polls")
    _scheduler.add_job(_monitor_server, CronTrigger(minute="*/5"), id="server_monitor")
    _scheduler.add_job(_weekly_top_broadcast, CronTrigger(day_of_week="mon", hour=10, minute=30), id="weekly_top")
    return _scheduler


def get_scheduler() -> AsyncIOScheduler | None:
    return _scheduler
