import asyncio

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InputMediaPhoto

from config import CATEGORIES, STATUSES, PRIORITIES, MAIN_ADMIN_ID
from database import (
    is_admin, get_task, get_tasks_filtered, count_tasks_filtered,
    update_task_status, update_task_priority,
    add_comment, get_comments,
    add_admin, remove_admin, get_all_admin_ids,
    get_notify_settings, set_notify_settings,
    delete_task, search_tasks, get_extended_stats,
    toggle_pin, archive_task, restore_task,
    get_archived_tasks, count_archived_tasks,
    set_deadline, get_vote_count, get_all_tasks_for_export,
    add_history_entry, get_task_history,
    assign_task, get_task_tags, add_tag_to_task, remove_tag_from_task, get_all_tags,
    get_task_photos, get_user_lang,
    ban_user, unban_user, get_ban_info, is_banned,
    warn_user, get_warning_count, get_warnings, clear_warnings,
    add_admin_log, get_admin_log,
    link_tasks, get_linked_tasks,
    get_dashboard_stats, bulk_update_status,
    create_promo_code, list_promo_codes, count_promo_codes, deactivate_promo_code, get_promo_code,
    create_faq, get_faq, delete_faq, get_faq_categories, get_faqs_by_category, count_faqs,
    create_poll, get_poll, get_poll_results, close_poll, get_active_polls, get_expiring_polls,
    create_giveaway, get_giveaway, get_active_giveaways, pick_giveaway_winners, count_giveaway_entries, generate_winner_code,
)
from keyboards.inline import (
    admin_main_kb, admin_task_kb, task_list_kb, notify_settings_kb,
    confirm_delete_kb, archived_task_kb, fix_confirm_kb,
    user_status_notify_kb, admin_filter_kb, admin_tools_kb,
    admin_moderation_kb, ban_duration_kb,
    bulk_status_kb, link_duplicate_kb,
    news_confirm_kb,
    promo_admin_kb, promo_list_kb, promo_view_kb,
    faq_admin_kb, faq_admin_entry_kb,
    poll_admin_kb, poll_admin_view_kb, poll_vote_kb,
    giveaway_admin_kb, giveaway_admin_view_kb,
)
from texts import t
from middlewares.throttle import invalidate_admin_cache

router = Router()


async def _safe_delete(*msgs):
    """Safely delete messages, ignoring errors."""
    for m in msgs:
        try:
            await m.delete()
        except Exception:
            pass


async def _auto_delete(msg, delay: float = 4.0):
    """Delete a message after a delay."""
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


async def _del_bot_prompt(message: Message, state: FSMContext):
    """Delete the previous bot prompt stored in FSM."""
    data = await state.get_data()
    mid = data.get("_prompt_msg_id")
    if mid:
        try:
            await message.bot.delete_message(message.chat.id, mid)
        except Exception:
            pass


async def _admin_menu_kb():
    """Build admin_main_kb with actual task counts."""
    total = await count_tasks_filtered()
    new = await count_tasks_filtered(status="new")
    return admin_main_kb(total=total, new=new)


class AdminComment(StatesGroup):
    waiting_text = State()


class AdminNotifyTimes(StatesGroup):
    waiting_times = State()


class AdminAddUser(StatesGroup):
    waiting_id = State()


class AdminRemoveUser(StatesGroup):
    waiting_id = State()


class AdminSearch(StatesGroup):
    waiting_query = State()


class AdminDeadline(StatesGroup):
    waiting_date = State()


class AdminAssign(StatesGroup):
    waiting_id = State()


class AdminTag(StatesGroup):
    waiting_tag = State()


class AdminBanUser(StatesGroup):
    waiting_id = State()


class AdminUnbanUser(StatesGroup):
    waiting_id = State()


class AdminWarnUser(StatesGroup):
    waiting_id = State()
    waiting_reason = State()


class AdminCheckWarns(StatesGroup):
    waiting_id = State()


class AdminBulkSelect(StatesGroup):
    selecting = State()


class AdminLinkDuplicate(StatesGroup):
    waiting_task_id = State()
    waiting_target_id = State()


class AdminNews(StatesGroup):
    waiting_content = State()
    waiting_link = State()
    waiting_target_id = State()


class AdminPromo(StatesGroup):
    waiting_code = State()
    waiting_reward = State()
    waiting_max_uses = State()
    waiting_roblox_reward = State()
    waiting_place = State()
    waiting_expiry = State()


class AdminFAQ(StatesGroup):
    waiting_category = State()
    waiting_question = State()
    waiting_answer = State()


class AdminPoll(StatesGroup):
    waiting_question = State()
    waiting_options = State()
    waiting_end_time = State()


class AdminGiveaway(StatesGroup):
    waiting_title = State()
    waiting_description = State()
    waiting_prize = State()
    waiting_roblox_reward = State()
    waiting_winner_count = State()
    waiting_end_time = State()


# ─── Команда /cancel (admin) ───

@router.message(Command("cancel"))
async def cmd_cancel_admin(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    current = await state.get_state()
    await _safe_delete(message)
    if current is None:
        return
    await state.clear()
    await message.answer("❌ Отменено.", reply_markup=await _admin_menu_kb())


# ─── Команда /admin ───

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await state.clear()
    dash = await get_dashboard_stats()
    text = (
        "🔧 **Админ-панель**\n\n"
        f"📦 Открыто: {dash['total_open']}  |  🔄 В работе: {dash['in_progress']}\n"
        f"🆕 За 24ч: {dash['new_24h']}  |  🔥 Просрочено: {dash['overdue']}\n"
    )
    if dash["top_voted"]:
        text += "\n🏆 **Топ голосов:**\n"
        for tv in dash["top_voted"]:
            cat_e = CATEGORIES.get(tv["category"], "❓").split()[0]
            short = tv["description"][:40] + ("…" if len(tv["description"]) > 40 else "")
            text += f"  {cat_e} #{tv['id']} ({tv['votes']}👍) {short}\n"
    await message.answer(text, reply_markup=await _admin_menu_kb(), parse_mode="Markdown")


@router.callback_query(F.data == "adm:menu")
async def cb_admin_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    dash = await get_dashboard_stats()
    text = (
        "🔧 **Админ-панель**\n\n"
        f"📦 Открыто: {dash['total_open']}  |  🔄 В работе: {dash['in_progress']}\n"
        f"🆕 За 24ч: {dash['new_24h']}  |  🔥 Просрочено: {dash['overdue']}\n"
    )
    if dash["top_voted"]:
        text += "\n🏆 **Топ голосов:**\n"
        for tv in dash["top_voted"]:
            cat_e = CATEGORIES.get(tv["category"], "❓").split()[0]
            short = tv["description"][:40] + ("…" if len(tv["description"]) > 40 else "")
            text += f"  {cat_e} #{tv['id']} ({tv['votes']}👍) {short}\n"
    try:
        await callback.message.edit_text(text, reply_markup=await _admin_menu_kb(), parse_mode="Markdown")
    except Exception:
        await callback.message.answer(text, reply_markup=await _admin_menu_kb(), parse_mode="Markdown")
    await callback.answer()


# ─── Список задач ───

@router.callback_query(F.data.startswith("adm:list:"))
async def cb_admin_list(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    filter_key = parts[2]  # all, new, in_progress, done
    offset = int(parts[3])
    page_size = 5

    status_filter = None if filter_key == "all" else filter_key
    tasks = await get_tasks_filtered(status=status_filter, limit=page_size, offset=offset)
    total = await count_tasks_filtered(status=status_filter)

    if not tasks:
        await callback.message.edit_text(
            "📋 Задач не найдено.",
            reply_markup=await _admin_menu_kb(),
        )
        await callback.answer()
        return

    filter_label = STATUSES.get(filter_key, "📋 Все")
    await callback.message.edit_text(
        f"{filter_label} задачи ({offset + 1}-{min(offset + page_size, total)} из {total}):",
        reply_markup=task_list_kb(tasks, filter_key, offset, total, page_size),
    )
    await callback.answer()


# ─── Просмотр задачи ───

@router.callback_query(F.data.startswith("adm:view:"))
async def cb_admin_view_task(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    votes = await get_vote_count(task_id) if task["category"] in ("idea", "balance") else 0
    tags = await get_task_tags(task_id)
    photos = await get_task_photos(task_id)
    links = await get_linked_tasks(task_id)
    text = _format_task(task, votes, tags, len(photos), links=links)

    if task["status"] == "archived":
        kb = archived_task_kb(task_id)
    else:
        kb = admin_task_kb(task_id, task["status"], bool(task.get("pinned")))

    if len(photos) > 1:
        try:
            await callback.message.delete()
        except Exception:
            pass
        media = [InputMediaPhoto(media=p["file_id"]) for p in photos[:5]]
        media[0] = InputMediaPhoto(media=photos[0]["file_id"], caption=text, parse_mode="Markdown")
        await callback.message.answer_media_group(media=media)
        await callback.message.answer("⬆️", reply_markup=kb)
    elif photos or task.get("photo_file_id"):
        photo_id = photos[0]["file_id"] if photos else task["photo_file_id"]
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer_photo(
            photo=photo_id,
            caption=text,
            reply_markup=kb,
            parse_mode="Markdown",
        )
    else:
        try:
            await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            await callback.message.answer(text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


def _format_task(task: dict, votes: int = 0, tags: list[str] | None = None,
                 photos_count: int = 0, links: list | None = None) -> str:
    cat_label = CATEGORIES.get(task["category"], "❓")
    status_label = STATUSES.get(task["status"], "❓")
    prio_label = PRIORITIES.get(task["priority"], "❓")

    text = (
        f"📋 **Задача #{task['id']}**\n\n"
        f"👤 От: @{task['username'] or 'N/A'} (ID: `{task['user_id']}`)\n"
        f"📂 Категория: {cat_label}\n"
        f"📊 Статус: {status_label}\n"
        f"⚡ Приоритет: {prio_label}\n"
        f"📝 Описание: {task['description']}\n"
        f"🕐 Создана: {task['created_at'][:16].replace('T', ' ')}\n"
        f"🔄 Обновлена: {task['updated_at'][:16].replace('T', ' ')}"
    )

    if task.get("pinned"):
        text += "\n📌 **Закреплена**"
    if task.get("deadline"):
        text += f"\n📅 Дедлайн: {task['deadline'][:16].replace('T', ' ')}"
    if task.get("assigned_admin_name"):
        text += f"\n👤 Назначен: @{task['assigned_admin_name']}"
    if tags:
        text += f"\n🏷 Теги: {', '.join(tags)}"
    if votes:
        text += f"\n👍 Голосов: {votes}"
    if photos_count > 1:
        text += f"\n📸 Фото: {photos_count} шт."
    if links:
        link_ids = [f"#{l['linked_task_id']}" for l in links]
        text += f"\n🔗 Связанные: {', '.join(link_ids)}"

    return text


# ─── Изменение статуса ───

@router.callback_query(F.data.startswith("adm:status:"))
async def cb_admin_change_status(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    task_id = int(parts[2])
    new_status = parts[3]

    if new_status not in STATUSES:
        await callback.answer("❌ Неизвестный статус", show_alert=True)
        return

    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    old_status = task["status"]
    await update_task_status(task_id, new_status)

    # Add history entry
    admin_name = callback.from_user.username or callback.from_user.full_name
    await add_history_entry(task_id, callback.from_user.id, admin_name,
                            "status", old_status, new_status)
    await add_admin_log(callback.from_user.id, admin_name, "status_change",
                        f"#{task_id} {old_status} → {new_status}")

    # Уведомить автора задачи о смене статуса
    old_label = STATUSES.get(old_status, "❓")
    new_label = STATUSES.get(new_status, "❓")
    user_lang = await get_user_lang(task["user_id"])
    try:
        if new_status == "done" and task["category"] == "bug":
            # Fix confirmation flow: ask user to confirm
            await callback.bot.send_message(
                task["user_id"],
                t("fix_confirm_prompt", user_lang, id=task_id),
                reply_markup=fix_confirm_kb(task_id, user_lang),
            )
        else:
            await callback.bot.send_message(
                task["user_id"],
                t("status_changed", user_lang, id=task_id,
                  old=old_label, new=new_label, admin=admin_name),
                reply_markup=user_status_notify_kb(task_id, user_lang),
            )
    except Exception:
        pass

    # Обновить карточку
    task = await get_task(task_id)
    text = _format_task(task)

    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_task_kb(task_id, new_status, bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    except Exception:
        # Могло быть фото — тогда просто ответим
        await callback.message.answer(
            text,
            reply_markup=admin_task_kb(task_id, new_status, bool(task.get("pinned"))),
            parse_mode="Markdown",
        )

    await callback.answer(f"Статус → {new_label}")


# ─── Изменение приоритета ───

@router.callback_query(F.data.startswith("adm:prio:"))
async def cb_admin_change_priority(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    task_id = int(parts[2])
    new_priority = parts[3]

    if new_priority not in PRIORITIES:
        await callback.answer("❌ Неизвестный приоритет", show_alert=True)
        return

    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    old_priority = task["priority"]
    await update_task_priority(task_id, new_priority)

    # Add history
    admin_name = callback.from_user.username or callback.from_user.full_name
    await add_history_entry(task_id, callback.from_user.id, admin_name,
                            "priority", old_priority, new_priority)

    task = await get_task(task_id)
    tags = await get_task_tags(task_id)
    text = _format_task(task, tags=tags)

    prio_label = PRIORITIES[new_priority]
    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )

    await callback.answer(f"Приоритет → {prio_label}")


# ─── Комментарии ───

@router.callback_query(F.data.startswith("adm:comment:"))
async def cb_admin_start_comment(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    await state.set_state(AdminComment.waiting_text)
    await state.update_data(comment_task_id=task_id)
    await callback.message.edit_text(
        f"💬 Напишите комментарий к задаче #{task_id}:\n"
        f"(Он будет отправлен автору задачи)"
    )
    await callback.answer()


@router.message(AdminComment.waiting_text)
async def process_admin_comment(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    task_id = data["comment_task_id"]
    text = message.text

    if not text:
        err = await message.answer("❌ Отправьте текстовый комментарий.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    task = await get_task(task_id)
    if not task:
        await message.answer("❌ Задача не найдена.")
        await _safe_delete(message)
        await state.clear()
        return

    await _safe_delete(message)
    admin_name = message.from_user.username or message.from_user.full_name
    await add_comment(task_id, message.from_user.id, text, author_name=admin_name)
    await state.clear()

    # Уведомить автора
    user_lang = await get_user_lang(task["user_id"])
    try:
        await message.bot.send_message(
            task["user_id"],
            t("comment_notify", user_lang, admin=admin_name, id=task_id, text=text),
        )
    except Exception:
        pass

    await message.answer(
        f"✅ Комментарий к задаче #{task_id} добавлен.",
        reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
    )


@router.callback_query(F.data.startswith("adm:view_comments:"))
async def cb_admin_view_comments(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    comments = await get_comments(task_id)

    if not comments:
        await callback.answer("💬 Комментариев пока нет", show_alert=True)
        return

    lines = [f"💬 **Комментарии к задаче #{task_id}:**\n"]
    for c in comments[-10:]:
        dt = c["created_at"][:16].replace("T", " ")
        name = c.get("author_name") or str(c["author_id"])
        is_adm = await is_admin(c["author_id"])
        role = "🔧" if is_adm else "👤"
        lines.append(f"{role} **@{name}** — {dt}\n{c['text']}\n")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К задаче", callback_data=f"adm:view:{task_id}")],
    ])

    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=kb,
        parse_mode="Markdown",
    )
    await callback.answer()


# ─── Управление админами ───

@router.message(Command("add_admin"))
async def cmd_add_admin(message: Message, state: FSMContext):
    if message.from_user.id != MAIN_ADMIN_ID:
        await message.answer("⛔ Только главный админ может добавлять админов.")
        return
    await _safe_delete(message)
    await state.set_state(AdminAddUser.waiting_id)
    prompt = await message.answer("Введите Telegram ID нового админа:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminAddUser.waiting_id)
async def process_add_admin(message: Message, state: FSMContext):
    try:
        new_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите корректный числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await add_admin(new_id)
    invalidate_admin_cache(new_id)
    await state.clear()
    await message.answer(f"✅ Пользователь {new_id} добавлен как админ.")


@router.message(Command("remove_admin"))
async def cmd_remove_admin(message: Message, state: FSMContext):
    if message.from_user.id != MAIN_ADMIN_ID:
        await message.answer("⛔ Только главный админ может удалять админов.")
        return

    await _safe_delete(message)
    admins = await get_all_admin_ids()
    admins_text = "\n".join(f"• `{a}`" for a in admins)
    await state.set_state(AdminRemoveUser.waiting_id)
    prompt = await message.answer(
        f"Текущие админы:\n{admins_text}\n\nВведите ID админа для удаления:",
        parse_mode="Markdown",
    )
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminRemoveUser.waiting_id)
async def process_remove_admin(message: Message, state: FSMContext):
    try:
        rm_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите корректный числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if rm_id == MAIN_ADMIN_ID:
        err = await message.answer("❌ Нельзя удалить главного админа.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await remove_admin(rm_id)
    invalidate_admin_cache(rm_id)
    await state.clear()
    await message.answer(f"✅ Админ {rm_id} удалён.")


# ─── Настройки уведомлений ───

@router.callback_query(F.data == "adm:notify_settings")
async def cb_notify_settings(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    settings = await get_notify_settings(callback.from_user.id)
    await callback.message.edit_text(
        "🔔 Настройки уведомлений:",
        reply_markup=notify_settings_kb(bool(settings["enabled"]), settings["schedule_times"]),
    )
    await callback.answer()


@router.callback_query(F.data == "adm:notify_toggle")
async def cb_notify_toggle(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    settings = await get_notify_settings(callback.from_user.id)
    new_enabled = not settings["enabled"]
    await set_notify_settings(callback.from_user.id, settings["schedule_times"], new_enabled)

    status = "включены ✅" if new_enabled else "выключены ❌"
    await callback.answer(f"Уведомления {status}")
    settings["enabled"] = int(new_enabled)
    await callback.message.edit_reply_markup(
        reply_markup=notify_settings_kb(new_enabled, settings["schedule_times"]),
    )


@router.callback_query(F.data == "adm:notify_edit_times")
async def cb_notify_edit_times(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    await state.set_state(AdminNotifyTimes.waiting_times)
    await callback.message.edit_text(
        "⏰ Введите времена уведомлений через запятую (формат HH:MM):\n\n"
        "Например: `09:00, 14:00, 18:00`",
        parse_mode="Markdown",
    )
    await callback.answer()


@router.message(AdminNotifyTimes.waiting_times)
async def process_notify_times(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    text = message.text or ""
    times = []
    for part in text.split(","):
        t = part.strip()
        if len(t) == 5 and t[2] == ":":
            try:
                h, m = int(t[:2]), int(t[3:])
                if 0 <= h <= 23 and 0 <= m <= 59:
                    times.append(t)
                    continue
            except ValueError:
                pass
        err = await message.answer(f"❌ Неверный формат времени: `{t}`. Используйте HH:MM.",
                             parse_mode="Markdown")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if not times:
        err = await message.answer("❌ Укажите хотя бы одно время.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if len(times) > 12:
        err = await message.answer("❌ Максимум 12 временных точек.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)
    settings = await get_notify_settings(message.from_user.id)
    await set_notify_settings(message.from_user.id, times, bool(settings["enabled"]))
    await state.clear()
    await message.answer(
        f"✅ Расписание обновлено: {', '.join(times)}",
        reply_markup=await _admin_menu_kb(),
    )


# ─── Тултипы (нажатие на неактивные кнопки) ───

@router.callback_query(F.data.startswith("tip:"))
async def cb_tip(callback: CallbackQuery):
    await callback.answer(callback.data[4:], show_alert=False)


# ─── Подменю: Фильтр ───

@router.callback_query(F.data == "adm:filter")
async def cb_admin_filter(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await callback.message.edit_text("🔍 Фильтр по категории:", reply_markup=admin_filter_kb())
    await callback.answer()


# ─── Подменю: Инструменты ───

@router.callback_query(F.data == "adm:tools")
async def cb_admin_tools(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await callback.message.edit_text("⚙️ Инструменты:", reply_markup=admin_tools_kb())
    await callback.answer()


# ─── Подменю: Модерация ───

@router.callback_query(F.data == "adm:moderation")
async def cb_admin_moderation(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("🚫 Модерация пользователей:", reply_markup=admin_moderation_kb())
    await callback.answer()


# ─── Управление админами ───

@router.callback_query(F.data == "adm:admins")
async def cb_admin_list(callback: CallbackQuery):
    if callback.from_user.id != MAIN_ADMIN_ID:
        await callback.answer("⛔ Только главный админ", show_alert=True)
        return
    admins = await get_all_admin_ids()
    lines = [f"• `{a}`" for a in admins]
    text = "👥 Текущие админы:\n" + "\n".join(lines)
    text += "\n\n➕ /add\\_admin — добавить\n➖ /remove\\_admin — удалить"
    await callback.message.edit_text(text, parse_mode="Markdown",
                                      reply_markup=admin_tools_kb())
    await callback.answer()


# ─── Бан пользователя ───

@router.callback_query(F.data == "adm:ban_user")
async def cb_ban_user_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminBanUser.waiting_id)
    await callback.message.edit_text("🚫 Введите Telegram ID пользователя для бана:")
    await callback.answer()


@router.message(AdminBanUser.waiting_id)
async def process_ban_user_id(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    if await is_admin(user_id):
        err = await message.answer("❌ Нельзя забанить админа.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        await state.clear()
        return
    await _safe_delete(message)
    await state.clear()
    await message.answer(
        f"⏱ Выберите срок бана для `{user_id}`:",
        parse_mode="Markdown",
        reply_markup=ban_duration_kb(user_id),
    )


@router.callback_query(F.data.startswith("adm:doban:"))
async def cb_do_ban(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    parts = callback.data.split(":")
    user_id = int(parts[2])
    hours = int(parts[3])
    await ban_user(user_id, hours, "Ручной бан администратором")
    admin_name = callback.from_user.username or callback.from_user.full_name
    if hours < 24:
        dur = f"{hours}ч"
    else:
        dur = f"{hours // 24}д"
    await add_admin_log(callback.from_user.id, admin_name, "ban_user", f"user {user_id} на {dur}")
    await callback.message.edit_text(
        f"🚫 Пользователь `{user_id}` забанен на {dur}.",
        parse_mode="Markdown",
        reply_markup=admin_moderation_kb(),
    )
    await callback.answer()


# ─── Разбан ───

@router.callback_query(F.data == "adm:unban_user")
async def cb_unban_user_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminUnbanUser.waiting_id)
    await callback.message.edit_text("✅ Введите Telegram ID пользователя для разбана:")
    await callback.answer()


@router.message(AdminUnbanUser.waiting_id)
async def process_unban_user(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    info = await get_ban_info(user_id)
    if not info:
        await message.answer(f"ℹ️ Пользователь `{user_id}` не забанен.", parse_mode="Markdown")
    else:
        await unban_user(user_id)
        await message.answer(f"✅ Пользователь `{user_id}` разбанен.", parse_mode="Markdown")
    await state.clear()


# ─── Предупреждения (преды) ───

@router.callback_query(F.data == "adm:warn_user")
async def cb_warn_user_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminWarnUser.waiting_id)
    await callback.message.edit_text("⚠️ Введите Telegram ID пользователя:")
    await callback.answer()


@router.message(AdminWarnUser.waiting_id)
async def process_warn_user_id(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    if await is_admin(user_id):
        err = await message.answer("❌ Нельзя выдать пред админу.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        await state.clear()
        return
    await _safe_delete(message)
    await state.update_data(warn_target=user_id)
    await state.set_state(AdminWarnUser.waiting_reason)
    prompt = await message.answer("📝 Введите причину предупреждения (или /skip):")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminWarnUser.waiting_reason)
async def process_warn_reason(message: Message, state: FSMContext):
    data = await state.get_data()
    user_id = data["warn_target"]
    reason = "Нарушение правил" if message.text.strip() == "/skip" else message.text.strip()

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await warn_user(user_id, message.from_user.id, reason)
    count = await get_warning_count(user_id)
    await state.clear()

    text = f"⚠️ Пред выдан пользователю `{user_id}`.\n📊 Всего предов: {count}/3"
    # Автобан при 3 предах
    if count >= 3:
        await ban_user(user_id, 24, "Автобан: 3 предупреждения")
        await clear_warnings(user_id)
        text += "\n\n🚫 Автоматический бан на 24ч (3 преда)."

    await message.answer(text, parse_mode="Markdown",
                         reply_markup=admin_moderation_kb())


# ─── Проверка предов юзера ───

@router.callback_query(F.data == "adm:check_warns")
async def cb_check_warns_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminCheckWarns.waiting_id)
    await callback.message.edit_text("📋 Введите Telegram ID пользователя:")
    await callback.answer()


@router.message(AdminCheckWarns.waiting_id)
async def process_check_warns(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    await state.clear()

    count = await get_warning_count(user_id)
    warns = await get_warnings(user_id)
    ban_info = await get_ban_info(user_id)

    text = f"👤 Пользователь `{user_id}`\n"
    text += f"⚠️ Предов: {count}/3\n"
    if ban_info:
        text += f"🚫 Забанен до: {ban_info['banned_until'][:16]}\n"
        text += f"📝 Причина: {ban_info['reason']}\n"
    else:
        text += "✅ Не забанен\n"

    if warns:
        text += "\n📋 Последние преды:\n"
        for w in warns:
            text += f"• {w['created_at'][:16]} — {w['reason']}\n"

    await message.answer(text, parse_mode="Markdown",
                         reply_markup=admin_moderation_kb())


# ─── Удаление задачи ───

@router.callback_query(F.data.startswith("adm:delete:"))
async def cb_admin_delete(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    await callback.message.edit_text(
        f"🗑 Удалить задачу #{task_id}?\n\n"
        f"Описание: {task['description'][:100]}\n\n"
        f"⚠️ Это действие нельзя отменить!",
        reply_markup=confirm_delete_kb(task_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:confirm_delete:"))
async def cb_admin_confirm_delete(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    await delete_task(task_id)
    admin_name = callback.from_user.username or callback.from_user.full_name
    await add_admin_log(callback.from_user.id, admin_name, "delete_task", f"#{task_id}")
    await callback.message.edit_text(
        f"✅ Задача #{task_id} удалена.",
        reply_markup=await _admin_menu_kb(),
    )
    await callback.answer()


# ─── Поиск задач ───

@router.callback_query(F.data == "adm:search")
async def cb_admin_search(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    await state.set_state(AdminSearch.waiting_query)
    await callback.message.edit_text(
        "🔍 Введите текст для поиска или номер задачи (#ID):"
    )
    await callback.answer()


@router.message(AdminSearch.waiting_query)
async def process_admin_search(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    query = (message.text or "").strip()
    if not query:
        err = await message.answer("❌ Введите текст для поиска.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)

    # Убираем # если пользователь ищет по ID
    if query.startswith("#"):
        query = query[1:]

    await state.clear()
    results = await search_tasks(query, limit=10)

    if not results:
        await message.answer(
            "🔍 Ничего не найдено.",
            reply_markup=await _admin_menu_kb(),
        )
        return

    lines = [f"🔍 **Результаты поиска** «{query}» ({len(results)}):\n"]
    for t in results:
        status_emoji = STATUSES.get(t["status"], "❓").split()[0]
        cat_emoji = CATEGORIES.get(t["category"], "❓").split()[0]
        short = t["description"][:50] + ("..." if len(t["description"]) > 50 else "")
        lines.append(f"{status_emoji}{cat_emoji} #{t['id']}: {short}")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for t in results[:5]:
        buttons.append([
            InlineKeyboardButton(
                text=f"#{t['id']}: {t['description'][:35]}",
                callback_data=f"adm:view:{t['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="◀️ Админ-меню", callback_data="adm:menu")])

    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown",
    )


# ─── Статистика ───

@router.callback_query(F.data == "adm:stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    stats = await get_extended_stats()

    # По статусам
    s = stats["status"]
    new = s.get("new", 0)
    ip = s.get("in_progress", 0)
    done = s.get("done", 0)
    archived = s.get("archived", 0)

    # По категориям
    c = stats["category"]
    bugs = c.get("bug", 0)
    ideas = c.get("idea", 0)
    balance = c.get("balance", 0)

    # По приоритетам
    p = stats["priority"]
    crit = p.get("critical", 0)
    high = p.get("high", 0)
    med = p.get("medium", 0)
    low = p.get("low", 0)

    avg = stats["avg_close_days"]
    if avg < 1:
        avg_text = f"{int(avg * 24)} ч."
    else:
        avg_text = f"{avg} дн."

    text = (
        f"📊 **Статистика**\n\n"
        f"📦 Всего задач: {stats['total']}\n"
        f"📅 За последние 7 дней: {stats['week']}\n"
        f"⏱ Среднее время закрытия: {avg_text}\n\n"
        f"**По статусам:**\n"
        f"  🆕 Новых: {new}\n"
        f"  🔄 В процессе: {ip}\n"
        f"  ✅ Выполнено: {done}\n"
        f"  📦 В архиве: {archived}\n\n"
        f"**По категориям:**\n"
        f"  🐛 Баги: {bugs}\n"
        f"  💡 Идеи: {ideas}\n"
        f"  ⚖️ Баланс: {balance}\n\n"
        f"**По приоритетам:**\n"
        f"  🔴 Критический: {crit}\n"
        f"  🟠 Высокий: {high}\n"
        f"  🟡 Средний: {med}\n"
        f"  🟢 Низкий: {low}"
    )

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-меню", callback_data="adm:menu")],
    ])

    await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


# ─── Фильтр по категории ───

@router.callback_query(F.data.startswith("adm:fcat:"))
async def cb_admin_filter_category(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    category = parts[2]
    offset = int(parts[3])
    page_size = 5

    if category not in CATEGORIES:
        await callback.answer("❌ Неизвестная категория", show_alert=True)
        return

    tasks = await get_tasks_filtered(category=category, limit=page_size, offset=offset)
    total = await count_tasks_filtered(category=category)

    if not tasks:
        await callback.message.edit_text(
            f"📋 Задач в категории {CATEGORIES[category]} не найдено.",
            reply_markup=await _admin_menu_kb(),
        )
        await callback.answer()
        return

    cat_label = CATEGORIES[category]

    # Пагинация с prefix для категории
    from keyboards.inline import InlineKeyboardButton, InlineKeyboardMarkup
    import math

    buttons = []
    for t in tasks:
        status_emoji = STATUSES.get(t["status"], "❓").split()[0]
        prio_emoji = PRIORITIES.get(t["priority"], "").split()[0]
        short_desc = t["description"][:35] + ("..." if len(t["description"]) > 35 else "")
        buttons.append([
            InlineKeyboardButton(
                text=f"{status_emoji}{prio_emoji} #{t['id']}: {short_desc}",
                callback_data=f"adm:view:{t['id']}",
            )
        ])

    current_page = offset // page_size + 1
    total_pages = max(1, math.ceil(total / page_size))
    nav_row = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton(text="◀️", callback_data=f"adm:fcat:{category}:{offset - page_size}"))
    nav_row.append(InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data="tip:Страница категории"))
    if offset + page_size < total:
        nav_row.append(InlineKeyboardButton(text="▶️", callback_data=f"adm:fcat:{category}:{offset + page_size}"))
    buttons.append(nav_row)
    buttons.append([InlineKeyboardButton(text="◀️ Админ-меню", callback_data="adm:menu")])

    await callback.message.edit_text(
        f"{cat_label} ({offset + 1}-{min(offset + page_size, total)} из {total}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


# ─── Закрепление задачи ───

@router.callback_query(F.data.startswith("adm:pin:"))
async def cb_admin_pin(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    new_state = await toggle_pin(task_id)
    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    text = _format_task(task)
    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], new_state),
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], new_state),
            parse_mode="Markdown",
        )
    await callback.answer("📌 Закреплена" if new_state else "📌 Откреплена")


# ─── Архивирование задачи ───

@router.callback_query(F.data.startswith("adm:archive:"))
async def cb_admin_archive(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    if not task:
        await callback.answer("❌ Задача не найдена", show_alert=True)
        return

    await archive_task(task_id)
    admin_name = callback.from_user.username or callback.from_user.full_name
    await add_admin_log(callback.from_user.id, admin_name, "archive_task", f"#{task_id}")
    await callback.message.edit_text(
        f"📦 Задача #{task_id} перемещена в архив.",
        reply_markup=await _admin_menu_kb(),
    )
    await callback.answer()


# ─── Восстановление из архива ───

@router.callback_query(F.data.startswith("adm:restore:"))
async def cb_admin_restore(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    await restore_task(task_id)
    await callback.message.edit_text(
        f"♻️ Задача #{task_id} восстановлена (статус: Новая).",
        reply_markup=await _admin_menu_kb(),
    )
    await callback.answer()


# ─── Установка дедлайна ───

@router.callback_query(F.data.startswith("adm:set_deadline:"))
async def cb_admin_set_deadline(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    await state.set_state(AdminDeadline.waiting_date)
    await state.update_data(deadline_task_id=task_id)
    await callback.message.edit_text(
        f"📅 Введите дедлайн для задачи #{task_id}:\n\n"
        f"Формат: `ДД.ММ.ГГГГ` или `ДД.ММ.ГГГГ ЧЧ:ММ`\n"
        f"Например: `25.12.2025` или `25.12.2025 18:00`\n\n"
        f"Отправьте `нет` чтобы убрать дедлайн.",
        parse_mode="Markdown",
    )
    await callback.answer()


@router.message(AdminDeadline.waiting_date)
async def process_admin_deadline(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    task_id = data["deadline_task_id"]
    text = (message.text or "").strip()

    if text.lower() in ("нет", "no", "-", "убрать"):
        await set_deadline(task_id, None)
        await state.clear()
        await _safe_delete(message)
        task = await get_task(task_id)
        await message.answer(
            f"✅ Дедлайн для задачи #{task_id} убран.",
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))) if task else await _admin_menu_kb(),
        )
        return

    from datetime import datetime as dt
    deadline = None
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            deadline = dt.strptime(text, fmt)
            break
        except ValueError:
            continue

    if not deadline:
        err = await message.answer("❌ Неверный формат. Используйте `ДД.ММ.ГГГГ` или `ДД.ММ.ГГГГ ЧЧ:ММ`",
                             parse_mode="Markdown")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)
    await set_deadline(task_id, deadline.isoformat())
    await state.clear()
    task = await get_task(task_id)
    await message.answer(
        f"✅ Дедлайн для задачи #{task_id} установлен: {text}",
        reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))) if task else await _admin_menu_kb(),
    )


# ─── Экспорт CSV ───

@router.callback_query(F.data == "adm:export")
async def cb_admin_export_btn(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await _do_export(callback.from_user.id, callback.bot)
    await callback.answer("📥 Файл отправлен")


@router.message(Command("export"))
async def cmd_export(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return
    await _do_export(message.from_user.id, message.bot)


async def _do_export(user_id: int, bot):
    import csv
    import io
    from aiogram.types import BufferedInputFile

    tasks = await get_all_tasks_for_export()
    if not tasks:
        await bot.send_message(user_id, "📥 Нет задач для экспорта.")
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Автор", "Username", "Категория", "Статус",
                     "Приоритет", "Описание", "Создана", "Обновлена", "Дедлайн",
                     "Закреплена", "Назначен"])
    for t in tasks:
        writer.writerow([
            t["id"], t["user_id"], t.get("username", ""),
            t["category"], t["status"], t["priority"],
            t["description"],
            t["created_at"][:19].replace("T", " "),
            t["updated_at"][:19].replace("T", " "),
            (t.get("deadline") or "")[:19].replace("T", " "),
            "Да" if t.get("pinned") else "Нет",
            t.get("assigned_admin_name") or "",
        ])

    file_bytes = output.getvalue().encode("utf-8-sig")  # BOM for Excel
    doc = BufferedInputFile(file_bytes, filename="tasks_export.csv")
    await bot.send_document(user_id, doc, caption="📥 Экспорт всех задач")


# ─── History ───

@router.callback_query(F.data.startswith("adm:history:"))
async def cb_admin_history(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    history = await get_task_history(task_id)

    if not history:
        await callback.answer("📜 История изменений пуста", show_alert=True)
        return

    lines = [f"📜 **История задачи #{task_id}:**\n"]
    for h in history[:15]:
        dt = h["created_at"][:16].replace("T", " ")
        name = h.get("admin_name") or "system"
        old_v = h["old_value"] or "—"
        new_v = h["new_value"] or "—"
        lines.append(f"**{h['field']}**: {old_v} → {new_v}\n🔧 @{name} — {dt}\n")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К задаче", callback_data=f"adm:view:{task_id}")],
    ])

    try:
        await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await callback.message.answer("\n".join(lines), reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


# ─── Admin Assignment ───

@router.callback_query(F.data.startswith("adm:assign:"))
async def cb_admin_assign(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])

    # Show options: assign to self, enter ID, or unassign
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Назначить себе", callback_data=f"adm:assign_self:{task_id}")],
        [InlineKeyboardButton(text="✏️ Ввести ID админа", callback_data=f"adm:assign_input:{task_id}")],
        [InlineKeyboardButton(text="❌ Снять назначение", callback_data=f"adm:assign_remove:{task_id}")],
        [InlineKeyboardButton(text="◀️ К задаче", callback_data=f"adm:view:{task_id}")],
    ])

    try:
        await callback.message.edit_text(
            f"👤 Назначение задачи #{task_id}:", reply_markup=kb
        )
    except Exception:
        await callback.message.answer(
            f"👤 Назначение задачи #{task_id}:", reply_markup=kb
        )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:assign_self:"))
async def cb_admin_assign_self(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    admin_name = callback.from_user.username or callback.from_user.full_name
    task = await get_task(task_id)
    old_assigned = task.get("assigned_admin_name") or "—"

    await assign_task(task_id, callback.from_user.id, admin_name)
    await add_history_entry(task_id, callback.from_user.id, admin_name,
                            "assigned", old_assigned, admin_name)

    task = await get_task(task_id)
    tags = await get_task_tags(task_id)
    text = _format_task(task, tags=tags)
    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    await callback.answer(f"✅ Назначено: @{admin_name}")


@router.callback_query(F.data.startswith("adm:assign_input:"))
async def cb_admin_assign_input(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    await state.set_state(AdminAssign.waiting_id)
    await state.update_data(assign_task_id=task_id)
    try:
        await callback.message.edit_text(f"Введите Telegram ID админа для назначения на задачу #{task_id}:")
    except Exception:
        await callback.message.answer(f"Введите Telegram ID админа для назначения на задачу #{task_id}:")
    await callback.answer()


@router.message(AdminAssign.waiting_id)
async def process_admin_assign(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    task_id = data["assign_task_id"]

    try:
        assign_id = int(message.text.strip())
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите числовой ID.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if not await is_admin(assign_id):
        err = await message.answer("❌ Этот ID не является админом.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)

    task = await get_task(task_id)
    old_assigned = task.get("assigned_admin_name") or "—"
    admin_name = message.from_user.username or message.from_user.full_name

    # Try to get the target admin's name
    try:
        chat = await message.bot.get_chat(assign_id)
        target_name = chat.username or chat.full_name
    except Exception:
        target_name = str(assign_id)

    await assign_task(task_id, assign_id, target_name)
    await add_history_entry(task_id, message.from_user.id, admin_name,
                            "assigned", old_assigned, target_name)
    await state.clear()

    task = await get_task(task_id)
    await message.answer(
        f"✅ Задача #{task_id} назначена на @{target_name}",
        reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
    )


@router.callback_query(F.data.startswith("adm:assign_remove:"))
async def cb_admin_assign_remove(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    old_assigned = task.get("assigned_admin_name") or "—"
    admin_name = callback.from_user.username or callback.from_user.full_name

    await assign_task(task_id, None, None)
    await add_history_entry(task_id, callback.from_user.id, admin_name,
                            "assigned", old_assigned, "—")

    task = await get_task(task_id)
    tags = await get_task_tags(task_id)
    text = _format_task(task, tags=tags)
    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(text,
            reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            parse_mode="Markdown",
        )
    await callback.answer("✅ Назначение снято")


# ─── Tags ───

@router.callback_query(F.data.startswith("adm:tags:"))
async def cb_admin_tags(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    tags = await get_task_tags(task_id)
    all_tags = await get_all_tags()

    tags_text = ", ".join(tags) if tags else "нет"
    all_text = ", ".join(all_tags[:20]) if all_tags else "нет тегов"

    await state.set_state(AdminTag.waiting_tag)
    await state.update_data(tag_task_id=task_id)

    try:
        await callback.message.edit_text(
            f"🏷 **Теги задачи #{task_id}:** {tags_text}\n\n"
            f"Существующие теги: {all_text}\n\n"
            f"Введите тег для добавления или `-тег` для удаления.\n"
            f"Например: `gameplay` или `-gameplay`\n"
            f"Отправьте `отмена` для выхода.",
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            f"🏷 **Теги задачи #{task_id}:** {tags_text}\n\n"
            f"Введите тег для добавления или `-тег` для удаления.",
            parse_mode="Markdown",
        )
    await callback.answer()


@router.message(AdminTag.waiting_tag)
async def process_admin_tag(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    task_id = data["tag_task_id"]
    text = (message.text or "").strip()

    if text.lower() in ("отмена", "cancel", "выход"):
        await _safe_delete(message)
        await state.clear()
        task = await get_task(task_id)
        if task:
            await message.answer(
                "✅ Готово.",
                reply_markup=admin_task_kb(task_id, task["status"], bool(task.get("pinned"))),
            )
        return

    admin_name = message.from_user.username or message.from_user.full_name
    await _safe_delete(message)

    if text.startswith("-"):
        tag_name = text[1:].strip()
        if tag_name:
            await remove_tag_from_task(task_id, tag_name)
            await add_history_entry(task_id, message.from_user.id, admin_name,
                                    "tag_removed", tag_name, None)
            reply = await message.answer(f"🏷 Тег `{tag_name}` удалён.", parse_mode="Markdown")
            asyncio.create_task(_auto_delete(reply))
    else:
        if len(text) > 30:
            err = await message.answer("❌ Тег слишком длинный (макс 30 символов).")
            asyncio.create_task(_auto_delete(err))
            return
        await add_tag_to_task(task_id, text)
        await add_history_entry(task_id, message.from_user.id, admin_name,
                                "tag_added", None, text)
        reply = await message.answer(f"🏷 Тег `{text}` добавлен. Ещё тег или `отмена`.", parse_mode="Markdown")
        asyncio.create_task(_auto_delete(reply, 6.0))


# ─── Лог админ-активности ───

@router.callback_query(F.data == "adm:log")
async def cb_admin_log(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    entries = await get_admin_log(30)
    if not entries:
        await callback.answer("📋 Лог пуст", show_alert=True)
        return

    lines = ["📋 **Лог активности (последние 30):**\n"]
    for e in entries:
        dt = str(e["created_at"])[:16].replace("T", " ")
        name = e.get("admin_name") or str(e["admin_id"])
        detail = e.get("details") or ""
        lines.append(f"`{dt}` — **{name}**: {e['action']} {detail}")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n…"
    try:
        await callback.message.edit_text(
            text,
            reply_markup=admin_tools_kb(),
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(text, reply_markup=admin_tools_kb(), parse_mode="Markdown")
    await callback.answer()


# ─── Массовые действия ───

@router.callback_query(F.data == "adm:bulk_start")
async def cb_bulk_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    await state.set_state(AdminBulkSelect.selecting)
    try:
        await callback.message.edit_text(
            "✅ **Массовые действия**\n\n"
            "Отправьте ID задач через запятую или пробел.\n"
            "Например: `12, 15, 20`\n\n"
            "Отправьте `отмена` для выхода.",
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            "✅ **Массовые действия**\n\n"
            "Отправьте ID задач через запятую или пробел.",
            parse_mode="Markdown",
        )
    await callback.answer()


@router.message(AdminBulkSelect.selecting)
async def process_bulk_select(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if text.lower() in ("отмена", "cancel"):
        await _safe_delete(message)
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=admin_tools_kb())
        return

    import re
    raw_ids = re.split(r"[,\s]+", text)
    task_ids = []
    for r in raw_ids:
        if r.lstrip("#").isdigit():
            task_ids.append(int(r.lstrip("#")))

    if not task_ids:
        err = await message.answer("❌ Не удалось распознать ID. Попробуйте ещё раз.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if len(task_ids) > 50:
        err = await message.answer("❌ Максимум 50 задач за раз.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)
    await state.clear()
    ids_str = ", ".join(f"#{i}" for i in task_ids)
    await message.answer(
        f"Выбрано {len(task_ids)} задач: {ids_str}\n\nВыберите новый статус:",
        reply_markup=bulk_status_kb(task_ids),
    )


@router.callback_query(F.data.startswith("adm:bulk:"))
async def cb_bulk_action(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":")
    new_status = parts[2]
    ids_str = parts[3]
    task_ids = [int(x) for x in ids_str.split(",") if x.isdigit()]

    if not task_ids:
        await callback.answer("❌ Нет задач", show_alert=True)
        return

    updated = await bulk_update_status(task_ids, new_status)
    admin_name = callback.from_user.username or callback.from_user.full_name
    await add_admin_log(callback.from_user.id, admin_name, "bulk_status",
                        f"{len(task_ids)} tasks → {new_status}")

    await callback.message.edit_text(
        f"✅ Обновлено {updated} задач → {STATUSES.get(new_status, new_status)}",
        reply_markup=await _admin_menu_kb(),
    )
    await callback.answer()


# ─── Связь дубликатов ───

@router.callback_query(F.data == "adm:link_start")
async def cb_link_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    await state.set_state(AdminLinkDuplicate.waiting_task_id)
    try:
        await callback.message.edit_text(
            "🔗 **Связать задачи**\n\n"
            "Отправьте ID первой задачи:",
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            "🔗 **Связать задачи**\n\nОтправьте ID первой задачи:",
            parse_mode="Markdown",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:link:"))
async def cb_link_from_task(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    task_id = int(callback.data.split(":")[2])
    await state.set_state(AdminLinkDuplicate.waiting_target_id)
    await state.update_data(link_task_id=task_id)
    try:
        await callback.message.edit_text(
            f"🔗 Связать задачу #{task_id}\n\n"
            f"Отправьте ID второй задачи (дубликата):",
            parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(
            f"🔗 Связать #{task_id}\nОтправьте ID дубликата:",
            parse_mode="Markdown",
        )
    await callback.answer()


@router.message(AdminLinkDuplicate.waiting_task_id)
async def process_link_task_id(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip().lstrip("#")
    if text.lower() in ("отмена", "cancel"):
        await _safe_delete(message)
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=admin_tools_kb())
        return

    if not text.isdigit():
        err = await message.answer("❌ Отправьте числовой ID задачи.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    task_id = int(text)
    task = await get_task(task_id)
    if not task:
        err = await message.answer("❌ Задача не найдена.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)
    await state.set_state(AdminLinkDuplicate.waiting_target_id)
    await state.update_data(link_task_id=task_id)
    prompt = await message.answer(f"✅ Задача #{task_id}. Теперь отправьте ID второй задачи:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminLinkDuplicate.waiting_target_id)
async def process_link_target_id(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip().lstrip("#")
    if text.lower() in ("отмена", "cancel"):
        await _safe_delete(message)
        await state.clear()
        await message.answer("❌ Отменено.", reply_markup=admin_tools_kb())
        return

    if not text.isdigit():
        err = await message.answer("❌ Отправьте числовой ID задачи.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    data = await state.get_data()
    task_id = data["link_task_id"]
    target_id = int(text)

    if task_id == target_id:
        err = await message.answer("❌ Нельзя связать задачу саму с собой.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    target = await get_task(target_id)
    if not target:
        err = await message.answer("❌ Задача не найдена.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await link_tasks(task_id, target_id, "duplicate")
    admin_name = message.from_user.username or message.from_user.full_name
    await add_admin_log(message.from_user.id, admin_name, "link_tasks",
                        f"#{task_id} ↔ #{target_id}")
    await state.clear()
    await message.answer(
        f"🔗 Задачи #{task_id} и #{target_id} связаны как дубликаты.",
        reply_markup=link_duplicate_kb(task_id),
    )


# ─── News broadcast ───

@router.message(Command("news"))
async def cmd_news(message: Message, state: FSMContext):
    if message.from_user.id != MAIN_ADMIN_ID:
        return
    await _safe_delete(message)
    await state.set_state(AdminNews.waiting_content)
    prompt = await message.answer(t("news_prompt", "ru"))
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.callback_query(F.data == "adm:news_start")
async def cb_news_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != MAIN_ADMIN_ID:
        await callback.answer("⛔ Только главный админ", show_alert=True)
        return
    await state.set_state(AdminNews.waiting_content)
    prompt = await callback.message.answer(t("news_prompt", "ru"))
    await state.update_data(_prompt_msg_id=prompt.message_id)
    await callback.answer()


@router.message(AdminNews.waiting_content)
async def process_news_content(message: Message, state: FSMContext):
    # Store the message info for later broadcast
    data = {}
    if message.photo:
        data["type"] = "photo"
        data["file_id"] = message.photo[-1].file_id
        data["caption"] = message.caption or ""
    elif message.video:
        data["type"] = "video"
        data["file_id"] = message.video.file_id
        data["caption"] = message.caption or ""
    elif message.text:
        data["type"] = "text"
        data["text"] = message.text
    else:
        err = await message.answer("❌ Отправьте текст, фото или видео.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(news=data)
    await state.set_state(AdminNews.waiting_link)
    prompt = await message.answer(t("news_link_prompt", "ru"), parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminNews.waiting_link)
async def process_news_link(message: Message, state: FSMContext):
    from database import get_all_subscribers

    text = (message.text or "").strip()
    fsm_data = await state.get_data()
    news = fsm_data.get("news", {})

    # Parse link
    link_url = None
    link_text = None
    if text and text != "/skip":
        parts = text.split(maxsplit=1)
        if parts[0].startswith("http"):
            link_url = parts[0]
            link_text = parts[1] if len(parts) > 1 else "Подробнее"
        else:
            err = await message.answer("❌ Некорректная ссылка. Начните с http:// или /skip")
            await _safe_delete(message)
            asyncio.create_task(_auto_delete(err))
            return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)

    news["link_url"] = link_url
    news["link_text"] = link_text
    await state.update_data(news=news)

    # Preview
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    preview_kb = None
    if link_url:
        preview_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=link_text, url=link_url)],
        ])

    news_header = "📢 Новости игры!\n\n"
    if news["type"] == "photo":
        preview_msg = await message.answer_photo(news["file_id"], caption=news_header + (news.get("caption") or ""), reply_markup=preview_kb)
    elif news["type"] == "video":
        preview_msg = await message.answer_video(news["file_id"], caption=news_header + (news.get("caption") or ""), reply_markup=preview_kb)
    else:
        preview_msg = await message.answer(news_header + news["text"], reply_markup=preview_kb)

    subs = await get_all_subscribers()
    if not subs:
        await state.clear()
        await message.answer(t("news_no_subs", "ru"))
        return

    await state.update_data(_preview_msg_id=preview_msg.message_id)
    await message.answer(t("news_confirm", "ru", count=len(subs)), reply_markup=news_confirm_kb())


@router.callback_query(F.data == "adm:news_confirm")
async def cb_news_confirm(callback: CallbackQuery, state: FSMContext):
    from database import get_all_subscribers
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    fsm_data = await state.get_data()
    news = fsm_data.get("news")
    if not news:
        await callback.answer("❌ Нет данных")
        await state.clear()
        return

    # Delete preview message
    preview_mid = fsm_data.get("_preview_msg_id")
    if preview_mid:
        try:
            await callback.bot.delete_message(callback.message.chat.id, preview_mid)
        except Exception:
            pass

    subs = await get_all_subscribers()
    await state.clear()
    await callback.message.edit_text("📢 Рассылка...")
    await callback.answer()

    link_kb = None
    if news.get("link_url"):
        link_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=news["link_text"], url=news["link_url"])],
        ])

    ok = 0
    fail = 0
    bot = callback.bot
    news_header = "📢 Новости игры!\n\n"
    for uid in subs:
        try:
            if news["type"] == "photo":
                caption = news_header + (news.get("caption") or "")
                await bot.send_photo(uid, news["file_id"], caption=caption, reply_markup=link_kb)
            elif news["type"] == "video":
                caption = news_header + (news.get("caption") or "")
                await bot.send_video(uid, news["file_id"], caption=caption, reply_markup=link_kb)
            else:
                await bot.send_message(uid, news_header + news["text"], reply_markup=link_kb)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)

    await callback.message.edit_text(t("news_sent", "ru", ok=ok, fail=fail))


@router.callback_query(F.data == "adm:news_cancel")
async def cb_news_cancel(callback: CallbackQuery, state: FSMContext):
    fsm_data = await state.get_data()
    preview_mid = fsm_data.get("_preview_msg_id")
    if preview_mid:
        try:
            await callback.bot.delete_message(callback.message.chat.id, preview_mid)
        except Exception:
            pass
    await state.clear()
    await callback.message.edit_text(t("news_cancel", "ru"))
    await callback.answer()


# ─── Promo codes admin ───

@router.callback_query(F.data == "adm:promo")
async def cb_promo_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("🎟 Управление промокодами:", reply_markup=promo_admin_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:promo_create")
async def cb_promo_create(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminPromo.waiting_code)
    await callback.message.edit_text("🎟 Введите код промокода (например: SUMMER2026):")
    await callback.answer()


@router.message(AdminPromo.waiting_code)
async def process_promo_code(message: Message, state: FSMContext):
    code = (message.text or "").strip().upper()
    if not code or len(code) > 30:
        err = await message.answer("❌ Код должен быть 1-30 символов.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    existing = await get_promo_code(code)
    if existing:
        err = await message.answer("❌ Такой код уже существует.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    await state.update_data(promo_code=code)
    await state.set_state(AdminPromo.waiting_reward)
    prompt = await message.answer(f"✅ Код: `{code}`\n\n📝 Введите описание награды:", parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPromo.waiting_reward)
async def process_promo_reward(message: Message, state: FSMContext):
    reward = (message.text or "").strip()
    if not reward:
        err = await message.answer("❌ Введите описание награды.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(promo_reward=reward)
    await state.set_state(AdminPromo.waiting_max_uses)
    prompt = await message.answer("🔢 Максимум активаций (число, например 100):")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPromo.waiting_max_uses)
async def process_promo_max_uses(message: Message, state: FSMContext):
    try:
        max_uses = int(message.text.strip())
        if max_uses < 1:
            raise ValueError
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите положительное число.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(promo_max_uses=max_uses)
    await state.set_state(AdminPromo.waiting_roblox_reward)
    prompt = await message.answer(
        "🎮 Награда в Roblox (JSON):\n"
        "Примеры: `{\"Money\": 500}` или `{\"Skin\": \"Alpha Soldier\"}`\n"
        "Или /skip если код только для Telegram.",
        parse_mode="Markdown",
    )
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPromo.waiting_roblox_reward)
async def process_promo_roblox_reward(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    roblox_data = None
    if text != "/skip":
        import json
        try:
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError
            roblox_data = text
        except (json.JSONDecodeError, ValueError):
            err = await message.answer("❌ Неверный JSON. Пример: `{\"Money\": 500}` или /skip",
                                       parse_mode="Markdown")
            await _safe_delete(message)
            asyncio.create_task(_auto_delete(err))
            return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(promo_roblox_data=roblox_data)
    await state.set_state(AdminPromo.waiting_place)
    prompt = await message.answer("🌐 Для какого плейса?\n\n1️⃣ `all` — оба\n2️⃣ `public` — только публик\n3️⃣ `private` — только приватка",
                                  parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPromo.waiting_place)
async def process_promo_place(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text in ("1", "all"):
        place = "all"
    elif text in ("2", "public"):
        place = "public"
    elif text in ("3", "private"):
        place = "private"
    else:
        err = await message.answer("❌ Введите: all, public или private")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(promo_place=place)
    await state.set_state(AdminPromo.waiting_expiry)
    prompt = await message.answer("📅 Дата истечения (ДД.ММ.ГГГГ) или /skip:")
    await state.update_data(_prompt_msg_id=prompt.message_id)@router.message(AdminPromo.waiting_expiry)
async def process_promo_expiry(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    expires_at = None
    if text != "/skip":
        from datetime import datetime as dt
        try:
            expires_at = dt.strptime(text, "%d.%m.%Y").isoformat()
        except ValueError:
            err = await message.answer("❌ Формат: ДД.ММ.ГГГГ или /skip")
            await _safe_delete(message)
            asyncio.create_task(_auto_delete(err))
            return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    data = await state.get_data()
    await state.clear()
    code_id = await create_promo_code(
        code=data["promo_code"],
        reward_text=data["promo_reward"],
        max_uses=data["promo_max_uses"],
        expires_at=expires_at,
        created_by=message.from_user.id,
        roblox_reward_data=data.get("promo_roblox_data"),
        place=data.get("promo_place", "all"),
    )
    place_label = {"all": "все", "public": "публик", "private": "приватка"}.get(data.get("promo_place", "all"), "все")
    roblox_info = f"\n🎮 Roblox: `{data.get('promo_roblox_data', 'нет')}`" if data.get("promo_roblox_data") else ""
    await message.answer(
        f"✅ Промокод `{data['promo_code']}` создан!\n"
        f"Награда: {data['promo_reward']}\n"
        f"Макс. активаций: {data['promo_max_uses']}\n"
        f"Плейс: {place_label}\n"
        f"Истекает: {text if text != '/skip' else 'никогда'}{roblox_info}",
        reply_markup=promo_admin_kb(),
        parse_mode="Markdown",
    )


@router.callback_query(F.data.startswith("adm:promo_list:"))
async def cb_promo_list(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    offset = int(callback.data.split(":")[2])
    page_size = 5
    promos = await list_promo_codes(limit=page_size, offset=offset)
    total = await count_promo_codes()
    if not promos:
        await callback.message.edit_text("📭 Промокодов нет.", reply_markup=promo_admin_kb())
        await callback.answer()
        return
    await callback.message.edit_text(
        f"🎟 Промокоды ({offset + 1}-{min(offset + page_size, total)} из {total}):",
        reply_markup=promo_list_kb(promos, offset, total, page_size),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:promo_view:"))
async def cb_promo_view(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    code_id = int(callback.data.split(":")[2])
    from database import get_pool, _record_to_dict
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM promo_codes WHERE id = $1", code_id)
    if not row:
        await callback.answer("❌ Не найден", show_alert=True)
        return
    p = _record_to_dict(row)
    status = "✅ Активен" if p["active"] else "❌ Неактивен"
    exp = p["expires_at"][:10] if p["expires_at"] else "никогда"
    personal = f"\n👤 Персональный: ID {p['telegram_id']}" if p["telegram_id"] else ""
    text = (
        f"🎟 **Промокод: {p['code']}**\n\n"
        f"📝 Награда: {p['reward_text']}\n"
        f"📊 Использований: {p['used_count']}/{p['max_uses']}\n"
        f"📅 Истекает: {exp}\n"
        f"Статус: {status}{personal}"
    )
    await callback.message.edit_text(text, reply_markup=promo_view_kb(code_id), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("adm:promo_deactivate:"))
async def cb_promo_deactivate(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    code_id = int(callback.data.split(":")[2])
    await deactivate_promo_code(code_id)
    await callback.message.edit_text("✅ Промокод деактивирован.", reply_markup=promo_admin_kb())
    await callback.answer()


# ─── FAQ admin ───

@router.callback_query(F.data == "adm:faq")
async def cb_faq_admin_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    total = await count_faqs()
    await callback.message.edit_text(f"❓ FAQ ({total} записей):", reply_markup=faq_admin_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:faq_create")
async def cb_faq_create(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminFAQ.waiting_category)
    await callback.message.edit_text("📁 Введите категорию (например: Геймплей, Баги, Общее):")
    await callback.answer()


@router.message(AdminFAQ.waiting_category)
async def process_faq_category(message: Message, state: FSMContext):
    cat = (message.text or "").strip()
    if not cat or len(cat) > 50:
        err = await message.answer("❌ Категория 1-50 символов.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    await state.update_data(faq_category=cat)
    await state.set_state(AdminFAQ.waiting_question)
    prompt = await message.answer(f"📁 Категория: {cat}\n\n❓ Введите вопрос:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminFAQ.waiting_question)
async def process_faq_question(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        err = await message.answer("❌ Введите вопрос.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(faq_question=q)
    await state.set_state(AdminFAQ.waiting_answer)
    prompt = await message.answer(f"❓ {q}\n\n📝 Введите ответ:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminFAQ.waiting_answer)
async def process_faq_answer(message: Message, state: FSMContext):
    a = (message.text or "").strip()
    if not a:
        err = await message.answer("❌ Введите ответ.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    data = await state.get_data()
    await state.clear()
    faq_id = await create_faq(
        category=data["faq_category"],
        question=data["faq_question"],
        answer=a,
        created_by=message.from_user.id,
    )
    await message.answer(f"✅ FAQ #{faq_id} создан!", reply_markup=faq_admin_kb())


@router.callback_query(F.data.startswith("adm:faq_list:"))
async def cb_faq_admin_list(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    categories = await get_faq_categories()
    if not categories:
        await callback.message.edit_text("📭 FAQ пуст.", reply_markup=faq_admin_kb())
        await callback.answer()
        return
    # Show all FAQs grouped by category
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for cat in categories:
        faqs = await get_faqs_by_category(cat)
        for f in faqs:
            short = f["question"][:35] + ("..." if len(f["question"]) > 35 else "")
            buttons.append([InlineKeyboardButton(
                text=f"[{cat}] #{f['id']}: {short}",
                callback_data=f"adm:faq_view:{f['id']}",
            )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm:faq")])
    await callback.message.edit_text("❓ Все FAQ записи:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("adm:faq_view:"))
async def cb_faq_admin_view(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    faq_id = int(callback.data.split(":")[2])
    faq = await get_faq(faq_id)
    if not faq:
        await callback.answer("❌ Не найден", show_alert=True)
        return
    text = f"❓ **FAQ #{faq_id}**\n📁 {faq['category']}\n\n**Q:** {faq['question']}\n**A:** {faq['answer']}"
    await callback.message.edit_text(text, reply_markup=faq_admin_entry_kb(faq_id), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("adm:faq_delete:"))
async def cb_faq_delete(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    faq_id = int(callback.data.split(":")[2])
    await delete_faq(faq_id)
    await callback.message.edit_text(f"✅ FAQ #{faq_id} удалён.", reply_markup=faq_admin_kb())
    await callback.answer()


# ─── Polls admin ───

@router.callback_query(F.data == "adm:poll")
async def cb_poll_admin_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("📊 Управление опросами:", reply_markup=poll_admin_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:poll_create")
async def cb_poll_create(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminPoll.waiting_question)
    await callback.message.edit_text("📊 Введите вопрос опроса:")
    await callback.answer()


@router.message(AdminPoll.waiting_question)
async def process_poll_question(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        err = await message.answer("❌ Введите вопрос.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    await state.update_data(poll_question=q)
    await state.set_state(AdminPoll.waiting_options)
    prompt = await message.answer(f"❓ {q}\n\n📋 Введите варианты через запятую (2-6):\nНапример: Да, Нет, Не знаю")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPoll.waiting_options)
async def process_poll_options(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    options = [o.strip() for o in text.split(",") if o.strip()]
    if len(options) < 2:
        err = await message.answer("❌ Минимум 2 варианта через запятую.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    if len(options) > 6:
        err = await message.answer("❌ Максимум 6 вариантов.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(poll_options=options)
    await state.set_state(AdminPoll.waiting_end_time)
    prompt = await message.answer("⏰ Когда закрыть? (ДД.ММ.ГГГГ ЧЧ:ММ) или /skip для бессрочного:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminPoll.waiting_end_time)
async def process_poll_end_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    ends_at = None
    if text != "/skip":
        from datetime import datetime as dt
        try:
            ends_at = dt.strptime(text, "%d.%m.%Y %H:%M").isoformat()
        except ValueError:
            try:
                ends_at = dt.strptime(text, "%d.%m.%Y").isoformat()
            except ValueError:
                err = await message.answer("❌ Формат: ДД.ММ.ГГГГ ЧЧ:ММ или /skip")
                await _safe_delete(message)
                asyncio.create_task(_auto_delete(err))
                return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    data = await state.get_data()
    await state.clear()
    poll_id = await create_poll(
        question=data["poll_question"],
        options=data["poll_options"],
        ends_at=ends_at,
        created_by=message.from_user.id,
    )
    opts_text = "\n".join(f"  • {o}" for o in data["poll_options"])
    await message.answer(
        f"✅ Опрос #{poll_id} создан!\n\n"
        f"❓ {data['poll_question']}\n{opts_text}\n\n"
        f"⏰ {'Бессрочный' if not ends_at else f'До: {text}'}",
        reply_markup=poll_admin_kb(),
    )


@router.callback_query(F.data.startswith("adm:poll_list:"))
async def cb_poll_admin_list(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    polls = await get_active_polls()
    if not polls:
        await callback.message.edit_text("📭 Нет активных опросов.", reply_markup=poll_admin_kb())
        await callback.answer()
        return
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for p in polls:
        short = p["question"][:35] + ("..." if len(p["question"]) > 35 else "")
        buttons.append([InlineKeyboardButton(
            text=f"📊 #{p['id']}: {short}",
            callback_data=f"adm:poll_view:{p['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm:poll")])
    await callback.message.edit_text("📊 Активные опросы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("adm:poll_view:"))
async def cb_poll_admin_view(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    poll_id = int(callback.data.split(":")[2])
    poll = await get_poll(poll_id)
    if not poll:
        await callback.answer("❌ Не найден", show_alert=True)
        return
    results = await get_poll_results(poll_id)
    total_votes = sum(r["votes"] for r in results)
    lines = [f"📊 **Опрос #{poll_id}:** {poll['question']}\n"]
    lines.append(f"📊 Всего голосов: {total_votes}\n")
    for r in results:
        pct = round(r["votes"] / max(total_votes, 1) * 100)
        filled = round(r["votes"] / max(total_votes, 1) * 10)
        bar = "█" * filled + "░" * (10 - filled)
        lines.append(f"{r['option_text']}: {bar} {pct}% ({r['votes']})")
    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=poll_admin_view_kb(poll_id, poll["status"] == "active"),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:poll_close:"))
async def cb_poll_close(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    poll_id = int(callback.data.split(":")[2])
    await close_poll(poll_id)
    await callback.message.edit_text(f"✅ Опрос #{poll_id} закрыт.", reply_markup=poll_admin_kb())
    await callback.answer()


# ─── Giveaways admin ───

@router.callback_query(F.data == "adm:giveaway")
async def cb_giveaway_admin_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("🎁 Управление розыгрышами:", reply_markup=giveaway_admin_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:giveaway_create")
async def cb_giveaway_create(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminGiveaway.waiting_title)
    await callback.message.edit_text("🎁 Введите название розыгрыша:")
    await callback.answer()


@router.message(AdminGiveaway.waiting_title)
async def process_giveaway_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()
    if not title:
        err = await message.answer("❌ Введите название.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _safe_delete(message)
    await state.update_data(gw_title=title)
    await state.set_state(AdminGiveaway.waiting_description)
    prompt = await message.answer(f"🎁 {title}\n\n📝 Введите описание розыгрыша:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminGiveaway.waiting_description)
async def process_giveaway_desc(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if not desc:
        err = await message.answer("❌ Введите описание.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(gw_description=desc)
    await state.set_state(AdminGiveaway.waiting_prize)
    prompt = await message.answer("🎖 Введите приз:")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminGiveaway.waiting_prize)
async def process_giveaway_prize(message: Message, state: FSMContext):
    prize = (message.text or "").strip()
    if not prize:
        err = await message.answer("❌ Введите приз.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(gw_prize=prize)
    await state.set_state(AdminGiveaway.waiting_roblox_reward)
    prompt = await message.answer(
        "🎮 Награда в Roblox (JSON):\n"
        "Пример: `{\"Money\": 133767}` или `{\"Skin\": \"Alpha Soldier\"}`\n"
        "Или /skip если приз без кода в игре.",
        parse_mode="Markdown",
    )
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminGiveaway.waiting_roblox_reward)
async def process_giveaway_roblox_reward(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    roblox_data = None
    if text != "/skip":
        import json
        try:
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError
            roblox_data = text
        except (json.JSONDecodeError, ValueError):
            err = await message.answer("❌ Неверный JSON. Пример: `{\"Money\": 500}` или /skip",
                                       parse_mode="Markdown")
            await _safe_delete(message)
            asyncio.create_task(_auto_delete(err))
            return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(gw_roblox_reward=roblox_data)
    await state.set_state(AdminGiveaway.waiting_winner_count)
    prompt = await message.answer("🏆 Сколько победителей? (число):")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminGiveaway.waiting_winner_count)
async def process_giveaway_winners(message: Message, state: FSMContext):
    try:
        count = int(message.text.strip())
        if count < 1:
            raise ValueError
    except (ValueError, AttributeError):
        err = await message.answer("❌ Введите положительное число.")
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.update_data(gw_winner_count=count)
    await state.set_state(AdminGiveaway.waiting_end_time)
    prompt = await message.answer("⏰ Когда завершить? (ДД.ММ.ГГГГ ЧЧ:ММ):")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(AdminGiveaway.waiting_end_time)
async def process_giveaway_end(message: Message, state: FSMContext):
    from datetime import datetime as dt
    text = (message.text or "").strip()
    try:
        ends_at = dt.strptime(text, "%d.%m.%Y %H:%M").isoformat()
    except ValueError:
        try:
            ends_at = dt.strptime(text, "%d.%m.%Y").isoformat()
        except ValueError:
            err = await message.answer("❌ Формат: ДД.ММ.ГГГГ ЧЧ:ММ")
            await _safe_delete(message)
            asyncio.create_task(_auto_delete(err))
            return
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    data = await state.get_data()
    await state.clear()
    gw_id = await create_giveaway(
        title=data["gw_title"],
        description=data["gw_description"],
        prize_text=data["gw_prize"],
        prize_promo_reward=data.get("gw_roblox_reward"),
        winner_count=data["gw_winner_count"],
        ends_at=ends_at,
        created_by=message.from_user.id,
    )
    await message.answer(
        f"✅ Розыгрыш #{gw_id} создан!\n\n"
        f"🎁 {data['gw_title']}\n"
        f"🎖 Приз: {data['gw_prize']}\n"
        f"🏆 Победителей: {data['gw_winner_count']}\n"
        f"⏰ До: {text}",
        reply_markup=giveaway_admin_kb(),
    )


@router.callback_query(F.data.startswith("adm:giveaway_list:"))
async def cb_giveaway_admin_list(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    giveaways = await get_active_giveaways()
    if not giveaways:
        await callback.message.edit_text("📭 Нет активных розыгрышей.", reply_markup=giveaway_admin_kb())
        await callback.answer()
        return
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for g in giveaways:
        entries = await count_giveaway_entries(g["id"])
        buttons.append([InlineKeyboardButton(
            text=f"🎁 #{g['id']}: {g['title'][:25]} ({entries} уч.)",
            callback_data=f"adm:giveaway_view:{g['id']}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm:giveaway")])
    await callback.message.edit_text("🎁 Активные розыгрыши:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@router.callback_query(F.data.startswith("adm:giveaway_view:"))
async def cb_giveaway_admin_view(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    gw_id = int(callback.data.split(":")[2])
    g = await get_giveaway(gw_id)
    if not g:
        await callback.answer("❌ Не найден", show_alert=True)
        return
    entries = await count_giveaway_entries(gw_id)
    end_time = g["ends_at"][:16].replace("T", " ") if g["ends_at"] else "—"
    text = (
        f"🎁 **Розыгрыш #{gw_id}: {g['title']}**\n\n"
        f"📝 {g['description']}\n"
        f"🎖 Приз: {g['prize_text']}\n"
        f"👥 Участников: {entries}\n"
        f"🏆 Победителей: {g['winner_count']}\n"
        f"⏰ До: {end_time}\n"
        f"Статус: {g['status']}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=giveaway_admin_view_kb(gw_id, g["status"] == "active"),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm:giveaway_end:"))
async def cb_giveaway_end_now(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    gw_id = int(callback.data.split(":")[2])
    g = await get_giveaway(gw_id)
    if not g or g["status"] != "active":
        await callback.answer("❌ Розыгрыш не активен", show_alert=True)
        return
    entries = await count_giveaway_entries(gw_id)
    if entries == 0:
        from database import get_pool
        pool = await get_pool()
        await pool.execute("UPDATE giveaways SET status = 'ended' WHERE id = $1", gw_id)
        await callback.message.edit_text("❌ Нет участников. Розыгрыш отменён.", reply_markup=giveaway_admin_kb())
        await callback.answer()
        return
    winners = await pick_giveaway_winners(gw_id, g["winner_count"])
    # Notify winners
    bot = callback.bot
    winner_names = []
    for w in winners:
        winner_names.append(w.get("username") or str(w["telegram_id"]))
        code_text = ""
        if g.get("prize_promo_reward"):
            code = await generate_winner_code(
                w["telegram_id"], g["prize_text"], g["prize_promo_reward"], g["created_by"])
            code_text = f"\n\n🎟 Ваш код: `{code}`\nАктивируйте: /redeem {code}"
        try:
            await bot.send_message(
                w["telegram_id"],
                f"🎉 Поздравляем! Вы выиграли в розыгрыше «{g['title']}»!\n\n🎖 Приз: {g['prize_text']}{code_text}",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    await callback.message.edit_text(
        f"🏆 Розыгрыш «{g['title']}» завершён!\n\n"
        f"Победители ({len(winners)}):\n" +
        "\n".join(f"• @{n}" for n in winner_names),
        reply_markup=giveaway_admin_kb(),
    )
    await callback.answer()
