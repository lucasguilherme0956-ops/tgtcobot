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
    return _scheduler


def get_scheduler() -> AsyncIOScheduler | None:
    return _scheduler
