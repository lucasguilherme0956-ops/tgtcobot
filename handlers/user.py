import asyncio
import re

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InputMediaPhoto

from config import CATEGORIES, STATUSES, PRIORITIES, MAX_DESCRIPTION_LENGTH, MAX_TASKS_PER_HOUR
from database import (
    create_task, get_user_tasks, count_user_tasks,
    count_user_tasks_last_hour, get_task, get_all_admin_ids,
    add_comment, get_comments,
    toggle_vote, get_vote_count, has_voted, find_similar_tasks,
    get_user_lang, set_user_lang,
    update_task_description, add_history_entry,
    add_task_photo, get_task_photos, update_task_status,
    get_task_tags,
    toggle_dislike, get_dislike_count, get_user_vote_type,
    get_feed_tasks, count_feed_tasks, get_feed_tasks_sorted,
    get_user_profile,
    get_player_by_telegram, get_player_by_username, get_player_stats,
    get_player_matches, get_player_leaderboard, link_player_telegram,
    subscribe_news, unsubscribe_news, is_news_subscriber,
    link_telegram_roblox, get_linked_roblox_username, get_stats_cache,
    redeem_promo_code, get_public_active_codes, is_admin,
    get_faq_categories, get_faqs_by_category, get_faq, count_faqs,
    get_active_polls, get_poll, get_poll_results, vote_poll, get_user_poll_vote, count_poll_votes,
    get_server_status_current, get_server_peak_today,
    compute_weekly_top, get_weekly_top, get_previous_weekly_top, save_weekly_top,
    get_active_giveaways, get_giveaway, enter_giveaway, leave_giveaway, count_giveaway_entries, is_giveaway_entered,
)
from keyboards.inline import (
    main_menu_kb, skip_photo_kb, photo_progress_kb, confirm_task_kb,
    back_to_menu_kb, user_task_list_kb, user_task_view_kb,
    user_task_view_kb_with_vote, duplicate_found_kb,
    fix_confirm_kb, lang_select_kb, feed_card_kb,
    player_stats_kb, leaderboard_kb,
    faq_categories_kb, faq_list_kb, faq_view_kb,
    poll_vote_kb,
    server_status_kb,
    weekly_top_kb,
    giveaway_user_kb,
)
from texts import t

router = Router()

MAX_PHOTOS = 5
MIN_DESCRIPTION_LENGTH = 10


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

# Анти-спам: паттерны мусорных описаний
_SPAM_RE = re.compile(
    r'^(.)(\1{5,})|'          # один символ повторяется 6+ раз (аааааа)
    r'^[^\w\s]{5,}$|'         # только спецсимволы
    r'^(.)\1*$',              # весь текст — один символ ("aaaa", "1111")
    re.UNICODE
)


def _is_spam(text: str) -> bool:
    """Check if text is obviously spam/gibberish."""
    stripped = text.strip()
    if len(stripped) < MIN_DESCRIPTION_LENGTH:
        return True
    # Один символ повторяется
    if _SPAM_RE.search(stripped):
        return True
    # Меньше 2 уникальных слов
    words = stripped.split()
    unique_words = set(w.lower() for w in words if len(w) >= 2)
    if len(unique_words) < 2:
        return True
    # Нет ни одной буквы (кириллица/латиница)
    if not re.search(r'[a-zA-Zа-яА-ЯёЁ]', stripped):
        return True
    return False


class CreateTask(StatesGroup):
    category = State()
    description = State()
    photo = State()
    confirm = State()


class UserComment(StatesGroup):
    waiting_text = State()


class EditTask(StatesGroup):
    waiting_text = State()


class LinkRoblox(StatesGroup):
    waiting_username = State()


class StatsLookup(StatesGroup):
    waiting_username = State()


class RedeemCode(StatesGroup):
    waiting_code = State()


# ─── /start ───

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id)
    await message.answer(t("welcome", lang), reply_markup=main_menu_kb(lang))


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    await _safe_delete(message)
    if current is None:
        return
    await state.clear()
    lang = await get_user_lang(message.from_user.id)
    await message.answer("❌ Отменено.", reply_markup=main_menu_kb(lang))


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t("main_menu", lang), reply_markup=main_menu_kb(lang))
    await callback.answer()


# ─── Language ───

@router.callback_query(F.data == "change_lang")
async def cb_change_lang(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t("lang_prompt", lang), reply_markup=lang_select_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("set_lang:"))
async def cb_set_lang(callback: CallbackQuery):
    new_lang = callback.data.split(":")[1]
    if new_lang not in ("ru", "en"):
        await callback.answer("❌", show_alert=True)
        return
    await set_user_lang(callback.from_user.id, new_lang)
    await callback.message.edit_text(
        t("lang_changed", new_lang),
        reply_markup=main_menu_kb(new_lang),
    )
    await callback.answer()


@router.message(Command("lang"))
async def cmd_lang(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await message.answer(t("lang_prompt", lang), reply_markup=lang_select_kb())


# ─── News subscribe/unsubscribe ───

@router.callback_query(F.data == "news:toggle")
async def cb_news_toggle(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    uid = callback.from_user.id
    if await is_news_subscriber(uid):
        await unsubscribe_news(uid)
        await callback.message.edit_text(
            t("news_unsubscribed", lang), reply_markup=main_menu_kb(lang),
        )
    else:
        await subscribe_news(uid)
        await callback.message.edit_text(
            t("news_subscribed", lang), reply_markup=main_menu_kb(lang),
        )
    await callback.answer()


# ─── Профиль ───

@router.callback_query(F.data == "user:profile")
async def cb_user_profile(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    profile = await get_user_profile(callback.from_user.id)

    status_lines = ""
    status_emojis = {"new": "🆕", "in_progress": "🔄", "done": "✅", "archived": "📦"}
    status_labels_map = {"new": "Новые", "in_progress": "В работе", "done": "Готово", "archived": "Архив"}
    if lang == "en":
        status_labels_map = {"new": "New", "in_progress": "In progress", "done": "Done", "archived": "Archived"}
    for s_key, count in profile["by_status"].items():
        emoji = status_emojis.get(s_key, "❓")
        label = status_labels_map.get(s_key, s_key)
        status_lines += t("profile_status_line", lang, emoji=emoji, label=label, count=count)

    first_date = ""
    if profile["first_task_date"]:
        first_date = t("profile_first_date", lang, date=profile["first_task_date"][:10])

    text = t("profile_title", lang,
             tasks=profile["tasks_created"],
             comments=profile["comments"],
             votes=profile["votes"],
             warnings=profile["warnings"],
             status_line=status_lines,
             first_date=first_date)

    await callback.message.edit_text(text, reply_markup=back_to_menu_kb(lang), parse_mode="Markdown")
    await callback.answer()


@router.message(Command("profile"))
async def cmd_profile(message: Message):
    lang = await get_user_lang(message.from_user.id)
    profile = await get_user_profile(message.from_user.id)

    status_lines = ""
    status_emojis = {"new": "🆕", "in_progress": "🔄", "done": "✅", "archived": "📦"}
    status_labels_map = {"new": "Новые", "in_progress": "В работе", "done": "Готово", "archived": "Архив"}
    if lang == "en":
        status_labels_map = {"new": "New", "in_progress": "In progress", "done": "Done", "archived": "Archived"}
    for s_key, count in profile["by_status"].items():
        emoji = status_emojis.get(s_key, "❓")
        label = status_labels_map.get(s_key, s_key)
        status_lines += t("profile_status_line", lang, emoji=emoji, label=label, count=count)

    first_date = ""
    if profile["first_task_date"]:
        first_date = t("profile_first_date", lang, date=profile["first_task_date"][:10])

    text = t("profile_title", lang,
             tasks=profile["tasks_created"],
             comments=profile["comments"],
             votes=profile["votes"],
             warnings=profile["warnings"],
             status_line=status_lines,
             first_date=first_date)

    await message.answer(text, reply_markup=back_to_menu_kb(lang), parse_mode="Markdown")


# ─── Создание задачи ───

@router.callback_query(F.data.startswith("cat:"))
async def cb_select_category(callback: CallbackQuery, state: FSMContext):
    category = callback.data.split(":")[1]
    if category not in CATEGORIES:
        await callback.answer("❌", show_alert=True)
        return

    lang = await get_user_lang(callback.from_user.id)

    tasks_hour = await count_user_tasks_last_hour(callback.from_user.id)
    if tasks_hour >= MAX_TASKS_PER_HOUR:
        await callback.answer(t("hourly_limit", lang, max=MAX_TASKS_PER_HOUR), show_alert=True)
        return

    await state.set_state(CreateTask.description)
    await state.update_data(category=category, lang=lang)

    cat_label = CATEGORIES[category]
    await callback.message.edit_text(
        t("send_description", lang, cat=cat_label, max_len=MAX_DESCRIPTION_LENGTH),
    )
    await callback.answer()


@router.message(CreateTask.description)
async def process_description(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("lang", "ru")
    text = message.text
    if not text:
        err = await message.answer(t("desc_text_only", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if len(text) > MAX_DESCRIPTION_LENGTH:
        err = await message.answer(t("desc_too_long", lang, cur=len(text), max=MAX_DESCRIPTION_LENGTH))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if _is_spam(text):
        err = await message.answer(t("spam_rejected", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    await _safe_delete(message)
    await state.update_data(description=text, photos=[])
    await state.set_state(CreateTask.photo)
    await message.answer(t("send_photo_first", lang), reply_markup=skip_photo_kb(lang))


@router.message(CreateTask.photo, F.photo)
async def process_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("lang", "ru")
    photos = data.get("photos", [])

    if len(photos) >= MAX_PHOTOS:
        err = await message.answer(t("photo_limit", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    photo_file_id = message.photo[-1].file_id
    photos.append(photo_file_id)
    await state.update_data(photos=photos)
    await _safe_delete(message)

    if len(photos) >= MAX_PHOTOS:
        await _show_confirm(message, state)
    else:
        await message.answer(
            t("send_photo", lang, count=len(photos)),
            reply_markup=photo_progress_kb(len(photos), lang),
        )


@router.callback_query(CreateTask.photo, F.data == "done_photos")
async def cb_done_photos(callback: CallbackQuery, state: FSMContext):
    await _show_confirm_cb(callback, state)
    await callback.answer()


@router.callback_query(CreateTask.photo, F.data == "skip_photo")
async def cb_skip_photo(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photos=[])
    await _show_confirm_cb(callback, state)
    await callback.answer()


@router.message(CreateTask.photo)
async def process_photo_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("lang", "ru")
    err = await message.answer(t("photo_invalid", lang))
    await _safe_delete(message)
    asyncio.create_task(_auto_delete(err))


async def _build_confirm_text(state: FSMContext):
    """Build confirmation text and keyboard from FSM data."""
    data = await state.get_data()
    lang = data.get("lang", "ru")
    cat_label = CATEGORIES.get(data["category"], "❓")
    photos = data.get("photos", [])

    similar = await find_similar_tasks(data["description"], data["category"])
    dup_text = ""
    dup_kb = None
    if similar:
        dup_lines = [t("duplicates_found", lang)]
        for s in similar:
            short = s["description"][:60] + ("..." if len(s["description"]) > 60 else "")
            dup_lines.append(f"  #{s['id']}: {short}")
        dup_text = "\n".join(dup_lines) + "\n\n"
        dup_kb = duplicate_found_kb(similar[0]["id"], lang)

    photos_text = t("photos_attached", lang, count=len(photos)) if photos else t("photos_none", lang)
    text = t("confirm_task", lang, cat=cat_label, desc=data["description"],
             photos=photos_text, dup_text=dup_text)
    kb = dup_kb or confirm_task_kb(lang)
    await state.set_state(CreateTask.confirm)
    return text, kb


async def _show_confirm(message: Message, state: FSMContext):
    text, kb = await _build_confirm_text(state)
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")


async def _show_confirm_cb(callback: CallbackQuery, state: FSMContext):
    text, kb = await _build_confirm_text(state)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")


@router.callback_query(CreateTask.confirm, F.data == "confirm_task")
async def cb_confirm_task(callback: CallbackQuery, state: FSMContext, bot=None):
    data = await state.get_data()
    lang = data.get("lang", "ru")
    user = callback.from_user
    username = user.username or user.full_name
    photos = data.get("photos", [])

    first_photo = photos[0] if photos else None

    task_id = await create_task(
        user_id=user.id,
        username=username,
        category=data["category"],
        description=data["description"],
        photo_file_id=first_photo,
    )

    for file_id in photos:
        await add_task_photo(task_id, file_id)

    await state.clear()
    await callback.message.edit_text(
        t("task_created", lang, id=task_id),
        reply_markup=back_to_menu_kb(lang),
    )
    await callback.answer()

    from keyboards.inline import admin_new_task_kb
    cat_label = CATEGORIES.get(data["category"], "❓")
    notify_text = (
        f"🆕 **Новая задача #{task_id}**\n\n"
        f"От: @{username} (ID: {user.id})\n"
        f"Категория: {cat_label}\n"
        f"Описание: {data['description'][:200]}"
    )
    if len(photos) > 1:
        notify_text += f"\n📸 Фото: {len(photos)} шт."

    admin_ids = await get_all_admin_ids()
    actual_bot = bot or callback.bot
    for admin_id in admin_ids:
        try:
            if first_photo:
                await actual_bot.send_photo(
                    admin_id,
                    photo=first_photo,
                    caption=notify_text,
                    reply_markup=admin_new_task_kb(task_id),
                    parse_mode="Markdown",
                )
            else:
                await actual_bot.send_message(
                    admin_id,
                    notify_text,
                    reply_markup=admin_new_task_kb(task_id),
                    parse_mode="Markdown",
                )
        except Exception:
            pass


@router.callback_query(CreateTask.confirm, F.data == "cancel_task")
async def cb_cancel_task(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = data.get("lang", "ru")
    await state.clear()
    await callback.message.edit_text(t("task_cancelled", lang), reply_markup=back_to_menu_kb(lang))
    await callback.answer()


# ─── Мои задачи ───

@router.callback_query(F.data.startswith("my_tasks:"))
async def cb_my_tasks(callback: CallbackQuery):
    offset = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    lang = await get_user_lang(user_id)
    page_size = 5

    tasks = await get_user_tasks(user_id, limit=page_size, offset=offset)
    total = await count_user_tasks(user_id)

    if not tasks:
        await callback.message.edit_text(
            t("my_tasks_empty", lang),
            reply_markup=back_to_menu_kb(lang),
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        t("my_tasks_title", lang, start=offset + 1, end=min(offset + page_size, total), total=total),
        reply_markup=user_task_list_kb(tasks, offset, total, page_size),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("user:view:"))
async def cb_user_view_task(callback: CallbackQuery):
    await callback.answer()
    task_id = int(callback.data.split(":")[2])
    task, lang = await asyncio.gather(
        get_task(task_id),
        get_user_lang(callback.from_user.id),
    )

    if not task:
        return

    cat_label = CATEGORIES.get(task["category"], "❓")
    status_label = STATUSES.get(task["status"], "❓")
    prio_label = PRIORITIES.get(task["priority"], "❓")

    text = (
        f"📋 **Задача #{task['id']}**\n\n"
        f"Категория: {cat_label}\n"
        f"Статус: {status_label}\n"
        f"Приоритет: {prio_label}\n"
        f"Описание: {task['description']}\n"
        f"Создана: {task['created_at'][:16].replace('T', ' ')}"
    )

    tags, photos, votes, voted = await asyncio.gather(
        get_task_tags(task_id),
        get_task_photos(task_id),
        get_vote_count(task["id"]),
        has_voted(task["id"], callback.from_user.id),
    )

    if tags:
        text += f"\n🏷 Теги: {', '.join(tags)}"

    if task.get("assigned_admin_name"):
        text += f"\n👤 Назначен: @{task['assigned_admin_name']}"

    can_edit = (task["status"] == "new" and task["user_id"] == callback.from_user.id)

    if votes:
        text += f"\n👍 Голосов: {votes}"
    kb = user_task_view_kb_with_vote(task_id, votes, voted, lang, can_edit)

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


# ─── Редактирование задачи ───

@router.callback_query(F.data.startswith("user:edit:"))
async def cb_user_edit_task(callback: CallbackQuery, state: FSMContext):
    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    lang = await get_user_lang(callback.from_user.id)

    if not task or task["user_id"] != callback.from_user.id:
        await callback.answer(t("task_not_found", lang), show_alert=True)
        return

    if task["status"] != "new":
        await callback.answer(t("edit_only_new", lang), show_alert=True)
        return

    await state.set_state(EditTask.waiting_text)
    await state.update_data(edit_task_id=task_id, lang=lang)
    try:
        await callback.message.edit_text(
            t("edit_prompt", lang, id=task_id, max_len=MAX_DESCRIPTION_LENGTH),
        )
    except Exception:
        await callback.message.answer(
            t("edit_prompt", lang, id=task_id, max_len=MAX_DESCRIPTION_LENGTH),
        )
    await callback.answer()


@router.message(EditTask.waiting_text)
async def process_edit_task(message: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["edit_task_id"]
    lang = data.get("lang", "ru")
    text = message.text

    if not text or not text.strip():
        err = await message.answer(t("desc_text_only", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if len(text) > MAX_DESCRIPTION_LENGTH:
        err = await message.answer(t("desc_too_long", lang, cur=len(text), max=MAX_DESCRIPTION_LENGTH))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    task = await get_task(task_id)
    if not task:
        await message.answer(t("task_not_found", lang))
        await _safe_delete(message)
        await state.clear()
        return

    await _safe_delete(message)
    old_desc = task["description"]
    await update_task_description(task_id, text.strip())
    username = message.from_user.username or message.from_user.full_name
    await add_history_entry(task_id, message.from_user.id, username,
                            "description", old_desc[:100], text.strip()[:100])
    await state.clear()

    await message.answer(
        t("edit_done", lang, id=task_id),
        reply_markup=user_task_view_kb(task_id, lang, task["status"] == "new"),
    )


# ─── Комментарии пользователя ───

@router.callback_query(F.data.startswith("user:comment:"))
async def cb_user_start_comment(callback: CallbackQuery, state: FSMContext):
    task_id = int(callback.data.split(":")[2])
    task = await get_task(task_id)
    lang = await get_user_lang(callback.from_user.id)
    if not task or task["user_id"] != callback.from_user.id:
        await callback.answer(t("task_not_found", lang), show_alert=True)
        return

    await state.set_state(UserComment.waiting_text)
    await state.update_data(comment_task_id=task_id, lang=lang)
    try:
        await callback.message.edit_text(t("comment_prompt", lang, id=task_id))
    except Exception:
        await callback.message.answer(t("comment_prompt", lang, id=task_id))
    await callback.answer()


@router.message(UserComment.waiting_text)
async def process_user_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["comment_task_id"]
    lang = data.get("lang", "ru")
    text = message.text

    if not text or not text.strip():
        err = await message.answer(t("comment_empty", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    if len(text) > 1000:
        err = await message.answer(t("comment_too_long", lang))
        await _safe_delete(message)
        asyncio.create_task(_auto_delete(err))
        return

    task = await get_task(task_id)
    if not task:
        await message.answer(t("task_not_found", lang))
        await _safe_delete(message)
        await state.clear()
        return

    await _safe_delete(message)
    username = message.from_user.username or message.from_user.full_name
    await add_comment(task_id, message.from_user.id, text.strip(), author_name=username)
    await state.clear()

    admin_ids = await get_all_admin_ids()
    for admin_id in admin_ids:
        try:
            await message.bot.send_message(
                admin_id,
                f"💬 Комментарий от @{username} к задаче #{task_id}:\n\n{text}",
            )
        except Exception:
            pass

    await message.answer(
        t("comment_sent", lang, id=task_id),
        reply_markup=user_task_view_kb(task_id, lang),
    )


@router.callback_query(F.data.startswith("user:view_comments:"))
async def cb_user_view_comments(callback: CallbackQuery):
    task_id = int(callback.data.split(":")[2])
    lang = await get_user_lang(callback.from_user.id)
    comments = await get_comments(task_id)

    if not comments:
        await callback.answer(t("no_comments", lang), show_alert=True)
        return

    lines = [t("comments_title", lang, id=task_id)]
    for c in comments[-10:]:
        dt = c["created_at"][:16].replace("T", " ")
        name = c.get("author_name") or ""
        if c["author_id"] == callback.from_user.id:
            author = t("comment_you", lang)
        else:
            author = t("comment_admin", lang, name=name) if name else "🔧"
        lines.append(f"{author} — {dt}\n{c['text']}\n")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️", callback_data=f"user:view:{task_id}")],
    ])

    try:
        await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="Markdown")
    except Exception:
        await callback.message.answer("\n".join(lines), reply_markup=kb, parse_mode="Markdown")
    await callback.answer()


# ─── Голосование ───

@router.callback_query(F.data.startswith("vote:"))
async def cb_vote(callback: CallbackQuery):
    task_id = int(callback.data.split(":")[1])
    task = await get_task(task_id)
    lang = await get_user_lang(callback.from_user.id)
    if not task:
        await callback.answer(t("task_not_found", lang), show_alert=True)
        return

    voted, votes = await toggle_vote(task_id, callback.from_user.id)

    if voted:
        await callback.answer(t("vote_added", lang))
    else:
        await callback.answer(t("vote_removed", lang))

    can_edit = (task["status"] == "new" and task["user_id"] == callback.from_user.id)
    kb = user_task_view_kb_with_vote(task_id, votes, voted, lang, can_edit)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


# ─── Лента идей (Feed) ───

async def _show_feed_card(callback: CallbackQuery, offset: int, sort: str = "rating"):
    """Display a single feed card at the given offset."""
    lang, total = await asyncio.gather(
        get_user_lang(callback.from_user.id),
        count_feed_tasks(),
    )

    if total == 0:
        await callback.message.edit_text(
            t("feed_empty", lang), reply_markup=back_to_menu_kb(lang),
        )
        return

    offset = max(0, min(offset, total - 1))
    tasks = await get_feed_tasks_sorted(sort=sort, limit=1, offset=offset)
    if not tasks:
        await callback.message.edit_text(
            t("feed_empty", lang), reply_markup=back_to_menu_kb(lang),
        )
        return

    task = tasks[0]
    cat_emoji = CATEGORIES.get(task["category"], "❓").split()[0]
    cat_label = CATEGORIES.get(task["category"], "❓")

    user_vote, tags, photos = await asyncio.gather(
        get_user_vote_type(task["id"], callback.from_user.id),
        get_task_tags(task["id"]),
        get_task_photos(task["id"]),
    )

    text = t("feed_card", lang,
             pos=offset + 1, total=total,
             cat_emoji=cat_emoji, cat_label=cat_label,
             id=task["id"], desc=task["description"],
             likes=task["likes"], dislikes=task["dislikes"],
             rating=task["rating"],
             author=task.get("username", "?"))

    if tags:
        text += "\n🏷 " + ", ".join(tags)

    kb = feed_card_kb(task["id"], task["likes"], task["dislikes"],
                      user_vote, offset, total, lang, sort=sort)

    photo_id = photos[0]["file_id"] if photos else task.get("photo_file_id")

    if photo_id:
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer_photo(
            photo=photo_id, caption=text,
            reply_markup=kb, parse_mode="Markdown",
        )
    else:
        try:
            await callback.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(text, reply_markup=kb, parse_mode="Markdown")


@router.message(Command("feed"))
async def cmd_feed(message: Message, state: FSMContext):
    await state.clear()
    lang = await get_user_lang(message.from_user.id)
    total = await count_feed_tasks()

    if total == 0:
        await message.answer(t("feed_empty", lang), reply_markup=back_to_menu_kb(lang))
        return

    tasks = await get_feed_tasks_sorted(sort="rating", limit=1, offset=0)
    if not tasks:
        await message.answer(t("feed_empty", lang), reply_markup=back_to_menu_kb(lang))
        return
    task = tasks[0]
    cat_emoji = CATEGORIES.get(task["category"], "❓").split()[0]
    cat_label = CATEGORIES.get(task["category"], "❓")
    user_vote = await get_user_vote_type(task["id"], message.from_user.id)

    text = t("feed_card", lang,
             pos=1, total=total,
             cat_emoji=cat_emoji, cat_label=cat_label,
             id=task["id"], desc=task["description"],
             likes=task["likes"], dislikes=task["dislikes"],
             rating=task["rating"],
             author=task.get("username", "?"))

    tags = await get_task_tags(task["id"])
    if tags:
        text += "\n🏷 " + ", ".join(tags)

    kb = feed_card_kb(task["id"], task["likes"], task["dislikes"],
                      user_vote, 0, total, lang, sort="rating")

    photos = await get_task_photos(task["id"])
    photo_id = photos[0]["file_id"] if photos else task.get("photo_file_id")

    if photo_id:
        await message.answer_photo(
            photo=photo_id, caption=text,
            reply_markup=kb, parse_mode="Markdown",
        )
    else:
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")


@router.callback_query(F.data.startswith("feed:"))
async def cb_feed_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    offset = int(callback.data.split(":")[1])
    await _show_feed_card(callback, offset)


@router.callback_query(F.data.startswith("fsort:"))
async def cb_feed_sort(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split(":")
    sort_mode = parts[1]
    offset = int(parts[2]) if len(parts) > 2 else 0
    await _show_feed_card(callback, 0, sort=sort_mode)


@router.callback_query(F.data.startswith("fn:"))
async def cb_feed_navigate(callback: CallbackQuery):
    await callback.answer()
    offset = int(callback.data.split(":")[1])
    await _show_feed_card(callback, offset)


@router.callback_query(F.data.startswith("fl:"))
async def cb_feed_like(callback: CallbackQuery):
    parts = callback.data.split(":")
    task_id = int(parts[1])
    offset = int(parts[2])
    lang = await get_user_lang(callback.from_user.id)

    voted, _ = await toggle_vote(task_id, callback.from_user.id)
    if voted:
        await callback.answer(t("vote_added", lang))
    else:
        await callback.answer(t("vote_removed", lang))

    await _show_feed_card(callback, offset)


@router.callback_query(F.data.startswith("fd:"))
async def cb_feed_dislike(callback: CallbackQuery):
    parts = callback.data.split(":")
    task_id = int(parts[1])
    offset = int(parts[2])
    lang = await get_user_lang(callback.from_user.id)

    voted, _ = await toggle_dislike(task_id, callback.from_user.id)
    if voted:
        await callback.answer(t("dislike_added", lang))
    else:
        await callback.answer(t("dislike_removed", lang))

    await _show_feed_card(callback, offset)


# ─── Fix confirmation (user side) ───

@router.callback_query(F.data.startswith("confirm_fix:"))
async def cb_confirm_fix(callback: CallbackQuery):
    task_id = int(callback.data.split(":")[1])
    lang = await get_user_lang(callback.from_user.id)

    task = await get_task(task_id)
    if not task:
        await callback.answer(t("task_not_found", lang), show_alert=True)
        return

    await callback.message.edit_text(t("fix_confirmed", lang, id=task_id))
    await callback.answer()


@router.callback_query(F.data.startswith("reject_fix:"))
async def cb_reject_fix(callback: CallbackQuery):
    task_id = int(callback.data.split(":")[1])
    lang = await get_user_lang(callback.from_user.id)

    task = await get_task(task_id)
    if not task:
        await callback.answer(t("task_not_found", lang), show_alert=True)
        return

    await update_task_status(task_id, "in_progress")
    username = callback.from_user.username or callback.from_user.full_name
    await add_history_entry(task_id, callback.from_user.id, username,
                            "status", "done", "in_progress")

    await callback.message.edit_text(t("fix_rejected", lang, id=task_id))
    await callback.answer()

    admin_ids = await get_all_admin_ids()
    for admin_id in admin_ids:
        try:
            await callback.bot.send_message(
                admin_id,
                t("fix_rejected_admin", "ru", user=username, id=task_id),
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════
# ─── Player Stats ───
# ═══════════════════════════════════════════════

def _format_player_stats(player: dict, stats: dict, lang: str) -> str:
    """Build formatted stats text."""
    wr = round(stats["games_won"] / max(stats["games_played"], 1) * 100)
    hours = stats["playtime_minutes"] // 60
    mins = stats["playtime_minutes"] % 60

    def _short(n: int) -> str:
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return str(n)

    text = t("stats_title", lang, name=player["roblox_username"])
    text += t("stats_games", lang, played=stats["games_played"], won=stats["games_won"], wr=wr)
    text += t("stats_waves", lang, total=stats["total_waves"], highest=stats["highest_wave"])
    text += t("stats_combat", lang, enemies=_short(stats["enemies_killed"]), bosses=stats["bosses_killed"])
    text += t("stats_towers", lang, placed=stats["towers_placed"], damage=_short(stats["damage_dealt"]))
    text += t("stats_economy", lang, earned=_short(stats["coins_earned"]), spent=_short(stats["coins_spent"]))
    text += t("stats_time", lang, hours=hours, mins=mins)
    return text


@router.callback_query(F.data == "game:stats")
async def cb_game_stats(callback: CallbackQuery, state: FSMContext):
    lang = await get_user_lang(callback.from_user.id)

    # Try linked account first
    linked_name = await get_linked_roblox_username(callback.from_user.id)
    if linked_name:
        await callback.answer()
        await _request_roblox_stats(callback.message, linked_name, lang, place="public")
        return

    # No linked account — ask for username
    await state.set_state(StatsLookup.waiting_username)
    try:
        await callback.message.edit_text(t("stats_prompt", lang), parse_mode="Markdown")
    except Exception:
        await callback.message.answer(t("stats_prompt", lang), parse_mode="Markdown")
    await callback.answer()


@router.message(Command("stats"))
async def cmd_stats(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    args = (message.text or "").split(maxsplit=1)
    await _safe_delete(message)

    if len(args) > 1:
        username = args[1].strip()
        await _request_roblox_stats(message, username, lang, place="public")
        return

    linked_name = await get_linked_roblox_username(message.from_user.id)
    if linked_name:
        await _request_roblox_stats(message, linked_name, lang, place="public")
        return

    await state.set_state(StatsLookup.waiting_username)
    await state.update_data(place="public")
    prompt = await message.answer(t("stats_prompt", lang), parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(Command("statsprivate"))
async def cmd_stats_private(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    args = (message.text or "").split(maxsplit=1)
    await _safe_delete(message)

    if len(args) > 1:
        username = args[1].strip()
        await _request_roblox_stats(message, username, lang, place="private")
        return

    linked_name = await get_linked_roblox_username(message.from_user.id)
    if linked_name:
        await _request_roblox_stats(message, linked_name, lang, place="private")
        return

    await state.set_state(StatsLookup.waiting_username)
    await state.update_data(place="private")
    prompt = await message.answer(t("stats_prompt", lang), parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(StatsLookup.waiting_username)
async def process_stats_lookup(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    username = (message.text or "").strip()
    data = await state.get_data()
    place = data.get("place", "public")
    await _del_bot_prompt(message, state)
    await state.clear()
    await _safe_delete(message)
    if not username or len(username) > 50:
        await message.answer(t("stats_not_found", lang))
        return

    await _request_roblox_stats(message, username, lang, place=place)


async def _request_roblox_stats(message: Message, username: str, lang: str, place: str = "public"):
    """Add username to pending queue and wait for Roblox to respond."""
    import json
    import stats_queue
    from database import get_stats_cache
    from stats_queue import format_roblox_stats

    loading_msg = await message.answer(t("stats_loading", lang, name=username))

    event = asyncio.Event()
    waiter = {
        "chat_id": message.chat.id,
        "message_id": loading_msg.message_id,
        "event": event,
    }

    key = username.lower()

    if key not in stats_queue.stats_waiters:
        stats_queue.stats_waiters[key] = []
    stats_queue.stats_waiters[key].append(waiter)

    stats_queue.pending_stats[place].append({"username": username})

    # Wait 5s for live Roblox response
    try:
        await asyncio.wait_for(event.wait(), timeout=10.0)
        return  # Roblox answered, message already edited
    except asyncio.TimeoutError:
        pass

    # Remove waiter
    waiters = stats_queue.stats_waiters.get(key, [])
    if waiter in waiters:
        waiters.remove(waiter)
    if not waiters and key in stats_queue.stats_waiters:
        del stats_queue.stats_waiters[key]

    # Fallback 1: stats_cache
    try:
        cached = await get_stats_cache(username, place)
        if cached:
            stats = json.loads(cached["stats_json"])
            stats["isOnline"] = False
            text = format_roblox_stats(stats)
            text += f"\n\n⚠️ Кэш от {cached['updated_at'][:16]}"
            await loading_msg.edit_text(text)
            return
    except Exception:
        pass

    # Fallback 2: old player_stats DB
    try:
        player = await get_player_by_username(username)
        if player:
            old_stats = await get_player_stats(player["roblox_id"])
            if old_stats:
                text = _format_player_stats(player, old_stats, lang)
                text += "\n\n⚠️ Данные из базы (сервер недоступен)"
                await loading_msg.edit_text(text, parse_mode="Markdown")
                return
    except Exception:
        pass

    try:
        await loading_msg.edit_text(t("stats_timeout", lang))
    except Exception:
        pass


# ─── Match history ───

@router.callback_query(F.data.startswith("game:matches:"))
async def cb_game_matches(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    roblox_id = int(callback.data.split(":")[2])
    matches = await get_player_matches(roblox_id, limit=5)
    if not matches:
        await callback.answer("📄 Нет матчей", show_alert=True)
        return

    text = t("match_title", lang)
    for m in matches:
        result = "✅" if m["won"] else "❌"
        map_name = m.get("map_name") or "?"
        diff = m.get("difficulty") or "?"
        text += t("match_line", lang,
                  result=result, map=map_name, diff=diff,
                  wave=m["wave_reached"], kills=m["enemies_killed"])

    try:
        await callback.message.edit_text(
            text, reply_markup=player_stats_kb(roblox_id, lang), parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(text, reply_markup=player_stats_kb(roblox_id, lang),
                                      parse_mode="Markdown")
    await callback.answer()


# ─── Leaderboard ───

_TOP_LABELS = {
    "enemies_killed": "⚔️ Убийства",
    "highest_wave": "🌊 Макс. волна",
    "games_won": "🏆 Победы",
    "coins_earned": "💰 Заработок",
    "damage_dealt": "💥 Урон",
    "playtime_minutes": "⏰ Время",
}


@router.callback_query(F.data.startswith("game:top"))
async def cb_game_top(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    parts = callback.data.split(":")
    stat = parts[2] if len(parts) > 2 else "enemies_killed"
    label = _TOP_LABELS.get(stat, stat)

    top = await get_player_leaderboard(stat, 10)
    if not top:
        await callback.answer("🏆 Пока нет данных", show_alert=True)
        return

    text = t("top_title", lang, stat=label)
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(top):
        pos = medals[i] if i < 3 else f"{i+1}."
        val = row["value"]
        if stat == "playtime_minutes":
            val = f"{val // 60}ч {val % 60}м"
        text += t("top_line", lang, pos=pos, name=row["roblox_username"], value=val)

    try:
        await callback.message.edit_text(
            text, reply_markup=leaderboard_kb(lang), parse_mode="Markdown",
        )
    except Exception:
        await callback.message.answer(text, reply_markup=leaderboard_kb(lang),
                                      parse_mode="Markdown")
    await callback.answer()


@router.message(Command("top"))
async def cmd_top(message: Message):
    lang = await get_user_lang(message.from_user.id)
    top = await get_player_leaderboard("enemies_killed", 10)
    if not top:
        await message.answer("🏆 Пока нет данных.")
        return

    text = t("top_title", lang, stat="⚔️ Убийства")
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(top):
        pos = medals[i] if i < 3 else f"{i+1}."
        text += t("top_line", lang, pos=pos, name=row["roblox_username"], value=row["value"])

    await message.answer(text, reply_markup=leaderboard_kb(lang), parse_mode="Markdown")


# ─── Link Roblox account ───

@router.callback_query(F.data == "game:link")
async def cb_game_link(callback: CallbackQuery, state: FSMContext):
    lang = await get_user_lang(callback.from_user.id)
    await state.set_state(LinkRoblox.waiting_username)
    try:
        await callback.message.edit_text(t("link_prompt", lang), parse_mode="Markdown")
    except Exception:
        await callback.message.answer(t("link_prompt", lang), parse_mode="Markdown")
    await callback.answer()


@router.message(Command("link"))
async def cmd_link(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    args = (message.text or "").split(maxsplit=1)
    await _safe_delete(message)
    if len(args) > 1:
        username = args[1].strip()
        # Check stats_cache or players
        player = await get_player_by_username(username)
        cached = await get_stats_cache(username, "public") if not player else None
        if not player and not cached:
            cached = await get_stats_cache(username, "private")
        if not player and not cached:
            await message.answer(t("link_fail", lang, name=username))
            return
        display_name = player["roblox_username"] if player else username
        if player:
            await link_player_telegram(player["roblox_id"], message.from_user.id)
        await link_telegram_roblox(message.from_user.id, display_name)
        await message.answer(t("link_success", lang, name=display_name),
                             reply_markup=back_to_menu_kb(lang), parse_mode="Markdown")
        return

    await state.set_state(LinkRoblox.waiting_username)
    prompt = await message.answer(t("link_prompt", lang), parse_mode="Markdown")
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.message(LinkRoblox.waiting_username)
async def process_link_roblox(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    username = (message.text or "").strip()
    if not username or len(username) > 50:
        await _del_bot_prompt(message, state)
        await state.clear()
        await _safe_delete(message)
        await message.answer(t("link_fail", lang, name=username or "?"))
        return

    # Check stats_cache or players
    player = await get_player_by_username(username)
    cached = await get_stats_cache(username, "public") if not player else None
    if not player and not cached:
        cached = await get_stats_cache(username, "private")
    if not player and not cached:
        await _del_bot_prompt(message, state)
        await state.clear()
        await _safe_delete(message)
        await message.answer(t("link_fail", lang, name=username))
        return

    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    display_name = player["roblox_username"] if player else username
    if player:
        await link_player_telegram(player["roblox_id"], message.from_user.id)
    await link_telegram_roblox(message.from_user.id, display_name)
    await state.clear()
    await message.answer(
        t("link_success", lang, name=display_name),
        reply_markup=back_to_menu_kb(lang),
        parse_mode="Markdown",
    )


# ─── Promo Redeem ───

@router.message(Command("redeem"))
async def cmd_redeem(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    args = (message.text or "").split(maxsplit=1)
    await _safe_delete(message)
    if len(args) > 1:
        code = args[1].strip()
        result = await redeem_promo_code(code, message.from_user.id)
        if result["ok"]:
            extra = "\n\n🎮 Зайдите в игру — награда начислится автоматически!" if result.get("roblox_queued") else ""
            await message.answer(t("promo_success", lang, reward=result["reward"]) + extra)
        else:
            key = f"promo_{result['error']}"
            await message.answer(t(key, lang))
        return
    await state.set_state(RedeemCode.waiting_code)
    prompt = await message.answer(t("promo_enter_code", lang))
    await state.update_data(_prompt_msg_id=prompt.message_id)


@router.callback_query(F.data == "redeem:start")
async def cb_redeem_start(callback: CallbackQuery, state: FSMContext):
    lang = await get_user_lang(callback.from_user.id)
    user_is_admin = await is_admin(callback.from_user.id)
    codes = await get_public_active_codes(include_private=user_is_admin)
    text = t("promo_enter_code", lang)
    if codes:
        text += "\n\n📋 Активные коды:\n"
        for c in codes:
            left = c["max_uses"] - c["used_count"]
            place_tag = ""
            p = c.get("place", "all")
            if p == "private":
                place_tag = " 🔒"
            elif p == "public":
                place_tag = " 🌐"
            text += f"▸ `{c['code']}` — {c['reward_text']} ({left} ост.){place_tag}\n"
    await state.set_state(RedeemCode.waiting_code)
    try:
        await callback.message.edit_text(text, parse_mode="Markdown")
    except Exception:
        await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


@router.message(RedeemCode.waiting_code)
async def process_redeem_code(message: Message, state: FSMContext):
    lang = await get_user_lang(message.from_user.id)
    code = (message.text or "").strip()
    await _del_bot_prompt(message, state)
    await _safe_delete(message)
    await state.clear()
    if not code:
        await message.answer(t("promo_not_found", lang))
        return
    result = await redeem_promo_code(code, message.from_user.id)
    if result["ok"]:
        extra = "\n\n🎮 Зайдите в игру — награда начислится автоматически!" if result.get("roblox_queued") else ""
        await message.answer(t("promo_success", lang, reward=result["reward"]) + extra,
                             reply_markup=back_to_menu_kb(lang))
    else:
        key = f"promo_{result['error']}"
        await message.answer(t(key, lang), reply_markup=back_to_menu_kb(lang))


# ─── FAQ User ───

@router.message(Command("faq"))
async def cmd_faq(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await _safe_delete(message)
    categories = await get_faq_categories()
    if not categories:
        await message.answer(t("faq_empty", lang), reply_markup=back_to_menu_kb(lang))
        return
    await message.answer(t("faq_title", lang), reply_markup=faq_categories_kb(categories, lang),
                         parse_mode="Markdown")


@router.callback_query(F.data == "faq:categories")
async def cb_faq_categories(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    categories = await get_faq_categories()
    if not categories:
        await callback.message.edit_text(t("faq_empty", lang), reply_markup=back_to_menu_kb(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t("faq_title", lang),
                                      reply_markup=faq_categories_kb(categories, lang),
                                      parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("faq:cat:"))
async def cb_faq_category(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    parts = callback.data.split(":")
    category = parts[2]
    offset = int(parts[3])
    page_size = 5
    faqs = await get_faqs_by_category(category)
    total = len(faqs)
    page_faqs = faqs[offset:offset + page_size]
    if not page_faqs:
        await callback.message.edit_text(t("faq_cat_empty", lang),
                                          reply_markup=faq_categories_kb(await get_faq_categories(), lang))
        await callback.answer()
        return
    await callback.message.edit_text(
        f"❓ **FAQ: {category}** ({offset + 1}-{min(offset + page_size, total)} из {total})",
        reply_markup=faq_list_kb(page_faqs, category, offset, total, lang, page_size),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("faq:view:"))
async def cb_faq_view(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    faq_id = int(callback.data.split(":")[2])
    faq = await get_faq(faq_id)
    if not faq:
        await callback.answer("❌ Не найдено", show_alert=True)
        return
    await callback.message.edit_text(
        t("faq_entry", lang, question=faq["question"], answer=faq["answer"]),
        reply_markup=faq_view_kb(faq_id, faq["category"], lang),
        parse_mode="Markdown",
    )
    await callback.answer()


# ─── Polls User ───

@router.message(Command("polls"))
async def cmd_polls(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await _safe_delete(message)
    polls = await get_active_polls()
    if not polls:
        await message.answer(t("poll_no_active", lang), reply_markup=back_to_menu_kb(lang))
        return
    # Show first active poll
    await _show_poll(message, polls[0]["id"], lang)


@router.callback_query(F.data.startswith("poll:list:"))
async def cb_poll_list(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    polls = await get_active_polls()
    if not polls:
        await callback.message.edit_text(t("poll_no_active", lang), reply_markup=back_to_menu_kb(lang))
        await callback.answer()
        return
    await _show_poll_cb(callback, polls[0]["id"], lang)


@router.callback_query(F.data.startswith("poll:vote:"))
async def cb_poll_vote(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    parts = callback.data.split(":")
    poll_id = int(parts[2])
    option_id = int(parts[3])
    poll = await get_poll(poll_id)
    if not poll or poll["status"] != "active":
        await callback.answer(t("poll_closed", lang), show_alert=True)
        return
    old_vote = await get_user_poll_vote(poll_id, callback.from_user.id)
    await vote_poll(poll_id, callback.from_user.id, option_id)
    results = await get_poll_results(poll_id)
    opt_text = next((r["option_text"] for r in results if r["id"] == option_id), "?")
    if old_vote and old_vote != option_id:
        await callback.answer(t("poll_changed", lang, option=opt_text))
    else:
        await callback.answer(t("poll_voted", lang, option=opt_text))
    user_vote = await get_user_poll_vote(poll_id, callback.from_user.id)
    try:
        await callback.message.edit_text(
            t("poll_question", lang, question=poll["question"]),
            reply_markup=poll_vote_kb(poll_id, results, user_vote),
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def _show_poll(message: Message, poll_id: int, lang: str):
    poll = await get_poll(poll_id)
    results = await get_poll_results(poll_id)
    user_vote = await get_user_poll_vote(poll_id, message.chat.id)
    closed = poll["status"] != "active"
    await message.answer(
        t("poll_question", lang, question=poll["question"]),
        reply_markup=poll_vote_kb(poll_id, results, user_vote, closed),
        parse_mode="Markdown",
    )


async def _show_poll_cb(callback: CallbackQuery, poll_id: int, lang: str):
    poll = await get_poll(poll_id)
    results = await get_poll_results(poll_id)
    user_vote = await get_user_poll_vote(poll_id, callback.from_user.id)
    closed = poll["status"] != "active"
    try:
        await callback.message.edit_text(
            t("poll_question", lang, question=poll["question"]),
            reply_markup=poll_vote_kb(poll_id, results, user_vote, closed),
            parse_mode="Markdown",
        )
    except Exception:
        pass
    await callback.answer()


# ─── Server Status ───

@router.message(Command("server"))
async def cmd_server(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await _safe_delete(message)
    await _show_server(message, lang)


@router.callback_query(F.data == "server:status")
async def cb_server_status(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    current = await get_server_status_current()
    peak = await get_server_peak_today()
    online = current["online_count"] if current else 0
    status_emoji = "🟢" if online > 0 else "🔴"
    time_str = current["recorded_at"][:16].replace("T", " ") if current else "—"
    try:
        await callback.message.edit_text(
            t("server_status", lang, status=status_emoji, online=online, peak=peak, time=time_str),
            reply_markup=server_status_kb(lang),
            parse_mode="Markdown",
        )
    except Exception:
        pass
    await callback.answer()


async def _show_server(message: Message, lang: str):
    current = await get_server_status_current()
    peak = await get_server_peak_today()
    online = current["online_count"] if current else 0
    status_emoji = "🟢" if online > 0 else "🔴"
    time_str = current["recorded_at"][:16].replace("T", " ") if current else "—"
    await message.answer(
        t("server_status", lang, status=status_emoji, online=online, peak=peak, time=time_str),
        reply_markup=server_status_kb(lang),
        parse_mode="Markdown",
    )


# ─── Weekly Top ───

@router.message(Command("weeklytop"))
async def cmd_weeklytop(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await _safe_delete(message)
    await _show_weekly_top(message, "wins", lang)


@router.callback_query(F.data == "weeklytop:view")
async def cb_weeklytop_view(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _show_weekly_top_cb(callback, "wins", lang)


@router.callback_query(F.data.startswith("weeklytop:stat:"))
async def cb_weeklytop_stat(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    stat = callback.data.split(":")[2]
    await _show_weekly_top_cb(callback, stat, lang)


async def _show_weekly_top(message: Message, stat_name: str, lang: str):
    from datetime import timedelta
    now = __import__("database").now_msk()
    week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    top = await get_weekly_top(week_start, stat_name)
    if not top:
        top = await compute_weekly_top(stat_name, 10)
        if top:
            await save_weekly_top(week_start, stat_name, top)
    if not top:
        await message.answer(t("weekly_top_empty", lang), reply_markup=weekly_top_kb(stat_name, lang))
        return
    stat_labels = {"wins": "🏆 Победы", "money": "💰 Монеты", "timePlayed": "⏰ Время"}
    prev = await get_previous_weekly_top(stat_name)
    prev_map = {}
    if prev:
        for i, p in enumerate(prev):
            prev_map[p["username"].lower()] = i + 1
    text = t("weekly_top_title", lang, stat=stat_labels.get(stat_name, stat_name), week=week_start)
    medals = ["🥇", "🥈", "🥉"]
    for i, entry in enumerate(top):
        pos = medals[i] if i < 3 else f"{i + 1}."
        val = entry["value"]
        if stat_name == "timePlayed":
            val = f"{val // 3600}ч {(val % 3600) // 60}м"
        elif stat_name == "money":
            val = f"{val:,}"
        arrow = ""
        prev_pos = prev_map.get(entry["username"].lower())
        if prev_pos:
            diff = prev_pos - (i + 1)
            if diff > 0:
                arrow = f"(↑{diff})"
            elif diff < 0:
                arrow = f"(↓{abs(diff)})"
        else:
            arrow = "(new)"
        text += t("weekly_top_line", lang, pos=pos, name=entry["username"], value=val, arrow=arrow)
    await message.answer(text, reply_markup=weekly_top_kb(stat_name, lang), parse_mode="Markdown")


async def _show_weekly_top_cb(callback: CallbackQuery, stat_name: str, lang: str):
    from datetime import timedelta
    now = __import__("database").now_msk()
    week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
    top = await get_weekly_top(week_start, stat_name)
    if not top:
        top = await compute_weekly_top(stat_name, 10)
        if top:
            await save_weekly_top(week_start, stat_name, top)
    if not top:
        try:
            await callback.message.edit_text(t("weekly_top_empty", lang),
                                              reply_markup=weekly_top_kb(stat_name, lang))
        except Exception:
            pass
        await callback.answer()
        return
    stat_labels = {"wins": "🏆 Победы", "money": "💰 Монеты", "timePlayed": "⏰ Время"}
    prev = await get_previous_weekly_top(stat_name)
    prev_map = {}
    if prev:
        for i, p in enumerate(prev):
            prev_map[p["username"].lower()] = i + 1
    text = t("weekly_top_title", lang, stat=stat_labels.get(stat_name, stat_name), week=week_start)
    medals = ["🥇", "🥈", "🥉"]
    for i, entry in enumerate(top):
        pos = medals[i] if i < 3 else f"{i + 1}."
        val = entry["value"]
        if stat_name == "timePlayed":
            val = f"{val // 3600}ч {(val % 3600) // 60}м"
        elif stat_name == "money":
            val = f"{val:,}"
        arrow = ""
        prev_pos = prev_map.get(entry["username"].lower())
        if prev_pos:
            diff = prev_pos - (i + 1)
            if diff > 0:
                arrow = f"(↑{diff})"
            elif diff < 0:
                arrow = f"(↓{abs(diff)})"
        else:
            arrow = "(new)"
        text += t("weekly_top_line", lang, pos=pos, name=entry["username"], value=val, arrow=arrow)
    try:
        await callback.message.edit_text(text, reply_markup=weekly_top_kb(stat_name, lang),
                                          parse_mode="Markdown")
    except Exception:
        pass
    await callback.answer()


# ─── Giveaways User ───

@router.message(Command("giveaways"))
async def cmd_giveaways(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await _safe_delete(message)
    giveaways = await get_active_giveaways()
    if not giveaways:
        await message.answer(t("giveaway_no_active", lang), reply_markup=back_to_menu_kb(lang))
        return
    g = giveaways[0]
    count = await count_giveaway_entries(g["id"])
    entered = await is_giveaway_entered(g["id"], message.from_user.id)
    end_time = g["ends_at"][:16].replace("T", " ") if g["ends_at"] else "—"
    await message.answer(
        t("giveaway_card", lang, title=g["title"], description=g["description"],
          prize=g["prize_text"], count=count, end_time=end_time, winners=g["winner_count"]),
        reply_markup=giveaway_user_kb(g["id"], entered, count, lang),
        parse_mode="Markdown",
    )


@router.callback_query(F.data.startswith("giveaway:list:"))
async def cb_giveaway_list(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    giveaways = await get_active_giveaways()
    if not giveaways:
        await callback.message.edit_text(t("giveaway_no_active", lang), reply_markup=back_to_menu_kb(lang))
        await callback.answer()
        return
    g = giveaways[0]
    count = await count_giveaway_entries(g["id"])
    entered = await is_giveaway_entered(g["id"], callback.from_user.id)
    end_time = g["ends_at"][:16].replace("T", " ") if g["ends_at"] else "—"
    try:
        await callback.message.edit_text(
            t("giveaway_card", lang, title=g["title"], description=g["description"],
              prize=g["prize_text"], count=count, end_time=end_time, winners=g["winner_count"]),
            reply_markup=giveaway_user_kb(g["id"], entered, count, lang),
            parse_mode="Markdown",
        )
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("giveaway:join:"))
async def cb_giveaway_join(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    giveaway_id = int(callback.data.split(":")[2])
    g = await get_giveaway(giveaway_id)
    if not g or g["status"] != "active":
        await callback.answer("❌ Розыгрыш завершён", show_alert=True)
        return
    username = callback.from_user.username or callback.from_user.full_name
    entered = await enter_giveaway(giveaway_id, callback.from_user.id, username)
    if entered:
        await callback.answer(t("giveaway_joined", lang))
    else:
        await callback.answer("Вы уже участвуете")
    count = await count_giveaway_entries(giveaway_id)
    end_time = g["ends_at"][:16].replace("T", " ") if g["ends_at"] else "—"
    try:
        await callback.message.edit_text(
            t("giveaway_card", lang, title=g["title"], description=g["description"],
              prize=g["prize_text"], count=count, end_time=end_time, winners=g["winner_count"]),
            reply_markup=giveaway_user_kb(giveaway_id, True, count, lang),
            parse_mode="Markdown",
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("giveaway:leave:"))
async def cb_giveaway_leave(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    giveaway_id = int(callback.data.split(":")[2])
    await leave_giveaway(giveaway_id, callback.from_user.id)
    await callback.answer(t("giveaway_left", lang))
    g = await get_giveaway(giveaway_id)
    if not g:
        return
    count = await count_giveaway_entries(giveaway_id)
    end_time = g["ends_at"][:16].replace("T", " ") if g["ends_at"] else "—"
    try:
        await callback.message.edit_text(
            t("giveaway_card", lang, title=g["title"], description=g["description"],
              prize=g["prize_text"], count=count, end_time=end_time, winners=g["winner_count"]),
            reply_markup=giveaway_user_kb(giveaway_id, False, count, lang),
            parse_mode="Markdown",
        )
    except Exception:
        pass
