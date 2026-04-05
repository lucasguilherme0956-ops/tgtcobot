from config import CATEGORIES, STATUSES, PRIORITIES
from database import get_task_stats, get_tasks_filtered, count_tasks_filtered, get_overdue_tasks


async def build_summary_text() -> str | None:
    """Формирует текст сводки для админа. Возвращает None если задач нет."""
    stats = await get_task_stats()

    new_count = stats.get("new", 0)
    in_progress_count = stats.get("in_progress", 0)
    done_count = stats.get("done", 0)

    overdue = await get_overdue_tasks()

    if new_count == 0 and in_progress_count == 0 and not overdue:
        return None

    lines = ["📊 **Сводка задач:**\n"]

    if new_count > 0:
        lines.append(f"🆕 Новых: {new_count}")
    if in_progress_count > 0:
        lines.append(f"🔄 В процессе: {in_progress_count}")
    if done_count > 0:
        lines.append(f"✅ Выполнено: {done_count}")

    # Показать до 5 последних новых задач
    if new_count > 0:
        lines.append("\n**Невзятые задачи:**")
        new_tasks = await get_tasks_filtered(status="new", limit=5)
        for t in new_tasks:
            cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
            prio_emoji = PRIORITIES.get(t["priority"], "").split()[0]
            short = t["description"][:50] + ("..." if len(t["description"]) > 50 else "")
            lines.append(f"  {cat_emoji}{prio_emoji} #{t['id']}: {short}")
        if new_count > 5:
            lines.append(f"  ... и ещё {new_count - 5}")

    # Показать задачи в процессе
    if in_progress_count > 0:
        lines.append("\n**В процессе:**")
        ip_tasks = await get_tasks_filtered(status="in_progress", limit=5)
        for t in ip_tasks:
            cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
            short = t["description"][:50] + ("..." if len(t["description"]) > 50 else "")
            lines.append(f"  {cat_emoji} #{t['id']}: {short}")
        if in_progress_count > 5:
            lines.append(f"  ... и ещё {in_progress_count - 5}")

    # Просроченные задачи
    if overdue:
        lines.append(f"\n🔥 **Просрочено ({len(overdue)}):**")
        for t in overdue[:5]:
            cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
            deadline = t["deadline"][:10] if t.get("deadline") else ""
            short = t["description"][:40] + ("..." if len(t["description"]) > 40 else "")
            lines.append(f"  {cat_emoji} #{t['id']}: {short} (до {deadline})")
        if len(overdue) > 5:
            lines.append(f"  ... и ещё {len(overdue) - 5}")

    lines.append("\n💪 Пора за работу! /admin")
    return "\n".join(lines)
