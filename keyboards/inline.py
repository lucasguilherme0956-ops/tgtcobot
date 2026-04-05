from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import CATEGORIES, STATUSES, PRIORITIES
from texts import t
import math


# ─── Пользовательские клавиатуры ───

def main_menu_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_bug", lang), callback_data="cat:bug"),
            InlineKeyboardButton(text=t("btn_idea", lang), callback_data="cat:idea"),
            InlineKeyboardButton(text=t("btn_balance", lang), callback_data="cat:balance"),
        ],
        [InlineKeyboardButton(text=t("btn_feed", lang), callback_data="feed:0")],
        [
            InlineKeyboardButton(text=t("btn_my_tasks", lang), callback_data="my_tasks:0"),
            InlineKeyboardButton(text=t("btn_profile", lang), callback_data="user:profile"),
        ],
        [
            InlineKeyboardButton(text=t("btn_stats", lang), callback_data="game:stats"),
            InlineKeyboardButton(text=t("btn_top", lang), callback_data="game:top"),
            InlineKeyboardButton(text=t("btn_link", lang), callback_data="game:link"),
        ],
        [InlineKeyboardButton(text=t("btn_lang", lang), callback_data="change_lang")],
    ])


def skip_photo_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("btn_skip_photo", lang), callback_data="skip_photo")],
    ])


def photo_progress_kb(count: int, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown during multi-photo upload."""
    buttons = []
    if count > 0:
        buttons.append([InlineKeyboardButton(text=t("btn_done_photo", lang), callback_data="done_photos")])
    buttons.append([InlineKeyboardButton(text=t("btn_skip_photo", lang), callback_data="skip_photo")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def confirm_task_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_send", lang), callback_data="confirm_task"),
            InlineKeyboardButton(text=t("btn_cancel", lang), callback_data="cancel_task"),
        ],
    ])


def back_to_menu_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("btn_menu", lang), callback_data="main_menu")],
    ])


def lang_select_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_lang:ru"),
            InlineKeyboardButton(text="🇬🇧 English", callback_data="set_lang:en"),
        ],
        [InlineKeyboardButton(text="◀️", callback_data="main_menu")],
    ])


# ─── Админские клавиатуры ───

def admin_main_kb(total: int = 0, new: int = 0) -> InlineKeyboardMarkup:
    counter = f"📦 Задачи: {total}"
    if new:
        counter += f"  ·  🆕 {new}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=counter, callback_data="adm:list:all:0")],
        [
            InlineKeyboardButton(text="🆕", callback_data="adm:list:new:0"),
            InlineKeyboardButton(text="🔄", callback_data="adm:list:in_progress:0"),
            InlineKeyboardButton(text="✅", callback_data="adm:list:done:0"),
        ],
        [
            InlineKeyboardButton(text="🔍 Фильтр", callback_data="adm:filter"),
            InlineKeyboardButton(text="📊 Стата", callback_data="adm:stats"),
        ],
        [InlineKeyboardButton(text="⚙️ Инструменты", callback_data="adm:tools")],
    ])


def admin_filter_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🐛 Баги", callback_data="adm:fcat:bug:0"),
            InlineKeyboardButton(text="💡 Идеи", callback_data="adm:fcat:idea:0"),
            InlineKeyboardButton(text="⚖️ Баланс", callback_data="adm:fcat:balance:0"),
        ],
        [InlineKeyboardButton(text="📦 Архив", callback_data="adm:list:archived:0")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:menu")],
    ])


def admin_tools_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 Поиск", callback_data="adm:search"),
            InlineKeyboardButton(text="🔔 Уведомления", callback_data="adm:notify_settings"),
        ],
        [
            InlineKeyboardButton(text="📥 Экспорт", callback_data="adm:export"),
            InlineKeyboardButton(text="👥 Админы", callback_data="adm:admins"),
        ],
        [
            InlineKeyboardButton(text="🚫 Бан / Преды", callback_data="adm:moderation"),
            InlineKeyboardButton(text="📋 Лог", callback_data="adm:log"),
        ],
        [
            InlineKeyboardButton(text="✅ Массовые", callback_data="adm:bulk_start"),
            InlineKeyboardButton(text="🔗 Дубликат", callback_data="adm:link_start"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:menu")],
    ])


def admin_moderation_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚫 Забанить", callback_data="adm:ban_user"),
            InlineKeyboardButton(text="✅ Разбанить", callback_data="adm:unban_user"),
        ],
        [
            InlineKeyboardButton(text="⚠️ Выдать пред", callback_data="adm:warn_user"),
            InlineKeyboardButton(text="📋 Преды юзера", callback_data="adm:check_warns"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:tools")],
    ])


def ban_duration_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1ч", callback_data=f"adm:doban:{user_id}:1"),
            InlineKeyboardButton(text="6ч", callback_data=f"adm:doban:{user_id}:6"),
            InlineKeyboardButton(text="24ч", callback_data=f"adm:doban:{user_id}:24"),
        ],
        [
            InlineKeyboardButton(text="3 дня", callback_data=f"adm:doban:{user_id}:72"),
            InlineKeyboardButton(text="7 дней", callback_data=f"adm:doban:{user_id}:168"),
            InlineKeyboardButton(text="30 дней", callback_data=f"adm:doban:{user_id}:720"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:moderation")],
    ])


def admin_task_kb(task_id: int, current_status: str, pinned: bool = False,
                  has_deadline: bool = False) -> InlineKeyboardMarkup:
    buttons = []

    # Статусы — только эмодзи
    status_icons = {"new": "🆕", "in_progress": "🔄", "done": "✅"}
    status_row = []
    for s_key, icon in status_icons.items():
        if s_key != current_status:
            status_row.append(
                InlineKeyboardButton(text=icon, callback_data=f"adm:status:{task_id}:{s_key}")
            )
    if status_row:
        buttons.append(status_row)

    # Приоритеты
    buttons.append([
        InlineKeyboardButton(text="🔴", callback_data=f"adm:prio:{task_id}:critical"),
        InlineKeyboardButton(text="🟠", callback_data=f"adm:prio:{task_id}:high"),
        InlineKeyboardButton(text="🟡", callback_data=f"adm:prio:{task_id}:medium"),
        InlineKeyboardButton(text="🟢", callback_data=f"adm:prio:{task_id}:low"),
    ])

    # Действия — иконки
    buttons.append([
        InlineKeyboardButton(text="💬", callback_data=f"adm:comment:{task_id}"),
        InlineKeyboardButton(text="📎", callback_data=f"adm:view_comments:{task_id}"),
        InlineKeyboardButton(text="👤", callback_data=f"adm:assign:{task_id}"),
        InlineKeyboardButton(text="🏷", callback_data=f"adm:tags:{task_id}"),
        InlineKeyboardButton(text="📜", callback_data=f"adm:history:{task_id}"),
    ])

    # Управление
    pin_icon = "📌✓" if pinned else "📌"
    buttons.append([
        InlineKeyboardButton(text=pin_icon, callback_data=f"adm:pin:{task_id}"),
        InlineKeyboardButton(text="📅", callback_data=f"adm:set_deadline:{task_id}"),
        InlineKeyboardButton(text="�", callback_data=f"adm:link:{task_id}"),
        InlineKeyboardButton(text="�📦", callback_data=f"adm:archive:{task_id}"),
        InlineKeyboardButton(text="🗑", callback_data=f"adm:delete:{task_id}"),
    ])

    # Назад
    buttons.append([
        InlineKeyboardButton(text="◀️ Список", callback_data="adm:list:all:0"),
        InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_new_task_kb(task_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для уведомления о новой задаче."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Взять", callback_data=f"adm:status:{task_id}:in_progress"),
            InlineKeyboardButton(text="👁 Открыть", callback_data=f"adm:view:{task_id}"),
        ],
    ])


def task_list_kb(tasks: list[dict], prefix: str, offset: int, total: int,
                 page_size: int = 5) -> InlineKeyboardMarkup:
    buttons = []
    for t in tasks:
        status_emoji = STATUSES.get(t["status"], "❓").split()[0]
        cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
        prio_emoji = PRIORITIES.get(t["priority"], "").split()[0]
        pin = "📌" if t.get("pinned") else ""
        short_desc = t["description"][:28] + ("…" if len(t["description"]) > 28 else "")
        buttons.append([
            InlineKeyboardButton(
                text=f"{pin}{status_emoji}{cat_emoji}{prio_emoji} #{t['id']} {short_desc}",
                callback_data=f"adm:view:{t['id']}",
            )
        ])

    # Навигация
    current_page = offset // page_size + 1
    total_pages = max(1, math.ceil(total / page_size))
    nav_row = []
    if offset > 0:
        nav_row.append(
            InlineKeyboardButton(text="◀️", callback_data=f"adm:list:{prefix}:{offset - page_size}")
        )
    nav_row.append(
        InlineKeyboardButton(text=f"· {current_page}/{total_pages} ·", callback_data="tip:Страница списка")
    )
    if offset + page_size < total:
        nav_row.append(
            InlineKeyboardButton(text="▶️", callback_data=f"adm:list:{prefix}:{offset + page_size}")
        )
    buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def user_task_list_kb(tasks: list[dict], offset: int, total: int,
                      page_size: int = 5) -> InlineKeyboardMarkup:
    buttons = []
    for t in tasks:
        status_emoji = STATUSES.get(t["status"], "❓").split()[0]
        cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
        short_desc = t["description"][:30] + ("…" if len(t["description"]) > 30 else "")
        buttons.append([
            InlineKeyboardButton(
                text=f"{status_emoji}{cat_emoji} #{t['id']} {short_desc}",
                callback_data=f"user:view:{t['id']}",
            )
        ])

    current_page = offset // page_size + 1
    total_pages = max(1, math.ceil(total / page_size))
    nav_row = []
    if offset > 0:
        nav_row.append(
            InlineKeyboardButton(text="◀️", callback_data=f"my_tasks:{offset - page_size}")
        )
    nav_row.append(
        InlineKeyboardButton(text=f"· {current_page}/{total_pages} ·", callback_data="tip:Страница списка")
    )
    if offset + page_size < total:
        nav_row.append(
            InlineKeyboardButton(text="▶️", callback_data=f"my_tasks:{offset + page_size}")
        )
    buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def notify_settings_kb(enabled: bool, times: list[str]) -> InlineKeyboardMarkup:
    toggle_text = "🔕 Выкл" if enabled else "🔔 Вкл"
    buttons = [
        [
            InlineKeyboardButton(text=toggle_text, callback_data="adm:notify_toggle"),
            InlineKeyboardButton(text="⏰ Расписание", callback_data="adm:notify_edit_times"),
        ],
        [InlineKeyboardButton(text=f"⏱ {', '.join(times)}", callback_data="tip:Текущее расписание уведомлений")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="adm:menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def user_task_view_kb(task_id: int, lang: str = "ru", can_edit: bool = False) -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(text=t("btn_write", lang), callback_data=f"user:comment:{task_id}"),
        InlineKeyboardButton(text=t("btn_chat", lang), callback_data=f"user:view_comments:{task_id}"),
    ]
    if can_edit:
        row1.append(InlineKeyboardButton(text=t("btn_edit", lang), callback_data=f"user:edit:{task_id}"))
    return InlineKeyboardMarkup(inline_keyboard=[
        row1,
        [InlineKeyboardButton(text=t("btn_back_tasks", lang), callback_data="my_tasks:0")],
    ])


def user_status_notify_kb(task_id: int, lang: str = "ru") -> InlineKeyboardMarkup:
    """Кнопки под уведомлением о смене статуса для пользователя."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_open", lang), callback_data=f"user:view:{task_id}"),
            InlineKeyboardButton(text=t("btn_write", lang), callback_data=f"user:comment:{task_id}"),
        ],
    ])


def confirm_delete_kb(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"adm:confirm_delete:{task_id}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"adm:view:{task_id}"),
        ],
    ])


def archived_task_kb(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for viewing an archived task (restore or permanent delete)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="♻️ Восстановить", callback_data=f"adm:restore:{task_id}"),
            InlineKeyboardButton(text="🗑 Удалить навсегда", callback_data=f"adm:delete:{task_id}"),
        ],
        [InlineKeyboardButton(text="◀️ Архив", callback_data="adm:list:archived:0")],
    ])


def feed_card_kb(task_id: int, likes: int, dislikes: int, user_vote: int | None,
                 offset: int, total: int, lang: str = "ru",
                 sort: str = "rating") -> InlineKeyboardMarkup:
    """Keyboard for a feed card: like/dislike + sort + navigation + menu."""
    like_text = f"👍✓ {likes}" if user_vote == 1 else f"👍 {likes}"
    dislike_text = f"👎✓ {dislikes}" if user_vote == -1 else f"👎 {dislikes}"

    buttons = [
        [
            InlineKeyboardButton(text=like_text, callback_data=f"fl:{task_id}:{offset}"),
            InlineKeyboardButton(text=dislike_text, callback_data=f"fd:{task_id}:{offset}"),
        ],
    ]

    # Sort buttons
    sort_icons = {"rating": "🔥", "new": "🕐", "controversial": "⚡"}
    sort_row = []
    for s_key, icon in sort_icons.items():
        label = f"[{icon}]" if s_key == sort else icon
        sort_row.append(InlineKeyboardButton(text=label, callback_data=f"fsort:{s_key}:{offset}"))
    buttons.append(sort_row)

    nav_row = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"fn:{offset - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"· {offset + 1}/{total} ·", callback_data="tip:Позиция в ленте"))
    if offset + 1 < total:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"fn:{offset + 1}"))
    buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text=t("btn_menu", lang), callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def user_task_view_kb_with_vote(task_id: int, vote_count: int, voted: bool,
                                lang: str = "ru", can_edit: bool = False) -> InlineKeyboardMarkup:
    """User task view with vote button for ideas/balance."""
    if voted:
        vote_text = f"👍✓ {vote_count}"
    elif vote_count:
        vote_text = f"👍 {vote_count}"
    else:
        vote_text = "👍"
    row1 = [
        InlineKeyboardButton(text=vote_text, callback_data=f"vote:{task_id}"),
        InlineKeyboardButton(text=t("btn_write", lang), callback_data=f"user:comment:{task_id}"),
        InlineKeyboardButton(text=t("btn_chat", lang), callback_data=f"user:view_comments:{task_id}"),
    ]
    rows = [row1]
    if can_edit:
        rows.append([InlineKeyboardButton(text=t("btn_edit", lang), callback_data=f"user:edit:{task_id}")])
    rows.append([InlineKeyboardButton(text=t("btn_back_tasks", lang), callback_data="my_tasks:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def fix_confirm_kb(task_id: int, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard for user to confirm/reject fix."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=t("btn_yes_fixed", lang), callback_data=f"confirm_fix:{task_id}"),
            InlineKeyboardButton(text=t("btn_no_not_fixed", lang), callback_data=f"reject_fix:{task_id}"),
        ],
    ])


def duplicate_found_kb(task_id: int | None = None, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown when duplicates found: proceed or cancel."""
    buttons = [
        [
            InlineKeyboardButton(text=t("btn_send_anyway", lang), callback_data="confirm_task"),
            InlineKeyboardButton(text=t("btn_cancel", lang), callback_data="cancel_task"),
        ],
    ]
    if task_id:
        buttons.insert(0, [
            InlineKeyboardButton(text=f"👁 #{task_id}", callback_data=f"user:view:{task_id}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Bulk actions ───

def bulk_status_kb(task_ids: list[int]) -> InlineKeyboardMarkup:
    ids_str = ",".join(str(i) for i in task_ids)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🆕 Новая", callback_data=f"adm:bulk:new:{ids_str}"),
            InlineKeyboardButton(text="🔄 В работу", callback_data=f"adm:bulk:in_progress:{ids_str}"),
        ],
        [
            InlineKeyboardButton(text="✅ Готово", callback_data=f"adm:bulk:done:{ids_str}"),
            InlineKeyboardButton(text="📦 Архив", callback_data=f"adm:bulk:archived:{ids_str}"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="adm:menu")],
    ])


# ─── Link duplicate ───

def link_duplicate_kb(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К задаче", callback_data=f"adm:view:{task_id}")],
    ])


# ─── Player stats keyboards ───

def player_stats_kb(roblox_id: int, lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Матчи", callback_data=f"game:matches:{roblox_id}")],
        [InlineKeyboardButton(text=t("btn_menu", lang), callback_data="main_menu")],
    ])


def leaderboard_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚔️", callback_data="game:top:enemies_killed"),
            InlineKeyboardButton(text="🌊", callback_data="game:top:highest_wave"),
            InlineKeyboardButton(text="🏆", callback_data="game:top:games_won"),
        ],
        [
            InlineKeyboardButton(text="💰", callback_data="game:top:coins_earned"),
            InlineKeyboardButton(text="💥", callback_data="game:top:damage_dealt"),
            InlineKeyboardButton(text="⏰", callback_data="game:top:playtime_minutes"),
        ],
        [InlineKeyboardButton(text=t("btn_menu", lang), callback_data="main_menu")],
    ])
