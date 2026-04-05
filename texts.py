"""Multilanguage texts (RU/EN)."""

TEXTS = {
    "ru": {
        "welcome": (
            "👋 Привет! Я бот для отслеживания задач.\n\n"
            "Ты можешь сообщить о баге, предложить идею или дать предложение по балансу.\n"
            "Выбери действие:"
        ),
        "main_menu": "📌 Главное меню — выбери действие:",
        "my_tasks_empty": "📋 У вас пока нет задач.",
        "my_tasks_title": "📋 Ваши задачи ({start}-{end} из {total}):",
        "task_not_found": "❌ Задача не найдена",
        "send_description": "Категория: {cat}\n\n📝 Опишите задачу (до {max_len} символов):",
        "desc_too_long": "❌ Слишком длинное описание ({cur}/{max}). Сократите текст.",
        "desc_text_only": "❌ Пожалуйста, отправьте текстовое описание.",
        "send_photo": "📸 Прикрепите скриншоты (до 5 шт.) или нажмите «Пропустить».\nОтправлено: {count}/5",
        "send_photo_first": "📸 Прикрепите скриншоты (до 5 шт.) или нажмите «Пропустить»:",
        "photo_limit": "📸 Максимум 5 фото. Нажмите «Готово» или «Пропустить».",
        "photo_invalid": "❌ Пожалуйста, отправьте фото (не файл/документ) или нажмите «Пропустить».",
        "confirm_task": (
            "📋 **Подтверждение задачи**\n\n"
            "Категория: {cat}\n"
            "Описание: {desc}\n"
            "Скриншоты: {photos}\n\n"
            "{dup_text}"
            "Отправить?"
        ),
        "photos_attached": "✅ {count} шт.",
        "photos_none": "❌ Нет",
        "task_created": (
            "✅ Задача #{id} создана! Спасибо за обратную связь.\n"
            "Статус можно проверить в «📋 Мои задачи»."
        ),
        "task_cancelled": "❌ Задача отменена.",
        "duplicates_found": "⚠️ **Похожие задачи найдены:**\n",
        "hourly_limit": "⏳ Вы уже отправили {max} задач за последний час. Подождите.",
        "comment_prompt": "💬 Напишите комментарий к задаче #{id}:",
        "comment_empty": "❌ Отправьте текстовый комментарий.",
        "comment_too_long": "❌ Комментарий слишком длинный (макс. 1000 символов).",
        "comment_sent": "✅ Комментарий к задаче #{id} отправлен.",
        "no_comments": "💬 Комментариев пока нет",
        "comments_title": "💬 **Комментарии к задаче #{id}:**\n",
        "comment_you": "👤 Вы",
        "comment_admin": "🔧 @{name}",
        "vote_added": "👍 Голос учтён",
        "vote_removed": "👍 Голос убран",
        "edit_prompt": "✏️ Введите новое описание задачи #{id}:\n(до {max_len} символов)",
        "edit_only_new": "❌ Можно редактировать только задачи со статусом «Новая».",
        "edit_done": "✅ Описание задачи #{id} обновлено.",
        "status_changed": (
            "🔔 Статус вашей задачи #{id} изменён:\n"
            "{old} → {new}\n"
            "Изменил: @{admin}"
        ),
        "fix_confirm_prompt": (
            "✅ Задача #{id} помечена как выполненная.\n\n"
            "Подтвердите, что проблема действительно исправлена:"
        ),
        "fix_confirmed": "✅ Спасибо! Задача #{id} подтверждена как исправленная.",
        "fix_rejected": "🔄 Задача #{id} возвращена в работу. Спасибо за обратную связь!",
        "fix_rejected_admin": "⚠️ Пользователь @{user} отклонил исправление задачи #{id}. Задача возвращена в работу.",
        "lang_changed": "✅ Язык изменён на Русский 🇷🇺",
        "lang_prompt": "🌐 Выберите язык / Choose language:",
        # Buttons
        "btn_bug": "🐛 Баг",
        "btn_idea": "💡 Идея",
        "btn_balance": "⚖️ Баланс",
        "btn_my_tasks": "📋 Мои задачи",
        "btn_lang": "🌐 Язык",
        "btn_skip_photo": "⏩ Пропустить",
        "btn_done_photo": "✅ Готово",
        "btn_send": "✅ Отправить",
        "btn_cancel": "❌ Отменить",
        "btn_menu": "◀️ Меню",
        "btn_write": "💬 Написать",
        "btn_chat": "📎 Чат",
        "btn_edit": "✏️ Редак.",
        "btn_back_tasks": "◀️ Мои задачи",
        "btn_open": "👁 Открыть",
        "btn_yes_fixed": "✅ Да, исправлено",
        "btn_no_not_fixed": "❌ Нет, не исправлено",
        "btn_send_anyway": "✅ Всё равно отправить",
        # Feed
        "btn_feed": "📰 Лента",
        "feed_empty": "📰 Пока нет задач для просмотра.",
        "feed_card": (
            "📰 **Лента** ({pos}/{total})\n\n"
            "{cat_emoji} {cat_label} #{id}\n"
            "────────────────\n"
            "{desc}\n\n"
            "👍 {likes}  👎 {dislikes}  |  Рейтинг: {rating}\n"
            "👤 @{author}"
        ),
        "dislike_added": "👎 Голос учтён",
        "dislike_removed": "👎 Голос убран",
        "spam_rejected": "❌ Описание похоже на спам. Напишите осмысленное описание на русском или английском (мин. 10 символов).",
        # Profile
        "profile_title": (
            "👤 **Ваш профиль**\n\n"
            "📋 Задач создано: {tasks}\n"
            "💬 Комментариев: {comments}\n"
            "👍 Голосов: {votes}\n"
            "⚠️ Предупреждений: {warnings}\n"
            "{status_line}"
            "{first_date}"
        ),
        "profile_first_date": "📅 Первая задача: {date}\n",
        "profile_status_line": "  {emoji} {label}: {count}\n",
        "btn_profile": "👤 Профиль",
        # Feed sort
        "feed_sort_new": "🕐 Новые",
        "feed_sort_popular": "🔥 Популярные",
        "feed_sort_controversial": "⚡ Спорные",
        # Comment notification
        "comment_notify": "💬 Админ @{admin} прокомментировал вашу задачу #{id}:\n\n{text}",
        # Player stats
        "stats_title": "🎮 **Статистика: {name}**\n\n",
        "stats_games": "🎲 Игр: {played} | 🏆 Побед: {won} ({wr}%)\n",
        "stats_waves": "🌊 Волн пройдено: {total} | 🔝 Макс: {highest}\n",
        "stats_combat": "⚔️ Убито: {enemies} | 👹 Боссов: {bosses}\n",
        "stats_towers": "🛡 Башен: {placed} | 💥 Урон: {damage}\n",
        "stats_economy": "💰 Заработано: {earned} | Траты: {spent}\n",
        "stats_time": "⏰ Время в игре: {hours}ч {mins}м\n",
        "stats_not_found": "❌ Игрок не найден.",
        "stats_no_data": "📊 Статистика пока пуста.",
        "link_prompt": "🔗 Отправьте ваш **ник в Roblox** для привязки:",
        "link_success": "✅ Аккаунт **{name}** привязан!",
        "link_fail": "❌ Игрок **{name}** не найден. Сначала зайдите в игру.",
        "top_title": "🏆 **Топ игроков** ({stat}):\n\n",
        "top_line": "{pos}. **{name}** — {value}\n",
        "btn_stats": "🎮 Статистика",
        "btn_top": "🏆 Топ",
        "btn_link": "🔗 Привязать",
        "stats_prompt": "🎮 Отправьте ник игрока или /stats без аргументов для своих статов.",
        "stats_loading": "⏳ Загружаю статистику {name}...",
        "stats_timeout": "❌ Игрок не найден или сервер недоступен. Попробуйте позже.",
        "match_title": "📄 **Последние матчи:**\n\n",
        "match_line": "{result} {map} ({diff}) — волна {wave}, {kills}☠\n",
    },
    "en": {
        "welcome": (
            "👋 Hi! I'm a task tracking bot.\n\n"
            "You can report a bug, suggest an idea, or submit a balance proposal.\n"
            "Choose an action:"
        ),
        "main_menu": "📌 Main menu — choose an action:",
        "my_tasks_empty": "📋 You have no tasks yet.",
        "my_tasks_title": "📋 Your tasks ({start}-{end} of {total}):",
        "task_not_found": "❌ Task not found",
        "send_description": "Category: {cat}\n\n📝 Describe the task (up to {max_len} chars):",
        "desc_too_long": "❌ Description too long ({cur}/{max}). Shorten it.",
        "desc_text_only": "❌ Please send a text description.",
        "send_photo": "📸 Attach screenshots (up to 5) or press «Skip».\nSent: {count}/5",
        "send_photo_first": "📸 Attach screenshots (up to 5) or press «Skip»:",
        "photo_limit": "📸 Max 5 photos. Press «Done» or «Skip».",
        "photo_invalid": "❌ Please send a photo (not a file/document) or press «Skip».",
        "confirm_task": (
            "📋 **Task confirmation**\n\n"
            "Category: {cat}\n"
            "Description: {desc}\n"
            "Screenshots: {photos}\n\n"
            "{dup_text}"
            "Submit?"
        ),
        "photos_attached": "✅ {count} pcs.",
        "photos_none": "❌ None",
        "task_created": (
            "✅ Task #{id} created! Thanks for your feedback.\n"
            "Check status in «📋 My tasks»."
        ),
        "task_cancelled": "❌ Task cancelled.",
        "duplicates_found": "⚠️ **Similar tasks found:**\n",
        "hourly_limit": "⏳ You've already submitted {max} tasks in the last hour. Please wait.",
        "comment_prompt": "💬 Write a comment for task #{id}:",
        "comment_empty": "❌ Send a text comment.",
        "comment_too_long": "❌ Comment too long (max 1000 chars).",
        "comment_sent": "✅ Comment for task #{id} sent.",
        "no_comments": "💬 No comments yet",
        "comments_title": "💬 **Comments for task #{id}:**\n",
        "comment_you": "👤 You",
        "comment_admin": "🔧 @{name}",
        "vote_added": "👍 Vote counted",
        "vote_removed": "👍 Vote removed",
        "edit_prompt": "✏️ Enter new description for task #{id}:\n(up to {max_len} chars)",
        "edit_only_new": "❌ Only tasks with status «New» can be edited.",
        "edit_done": "✅ Task #{id} description updated.",
        "status_changed": (
            "🔔 Your task #{id} status changed:\n"
            "{old} → {new}\n"
            "Changed by: @{admin}"
        ),
        "fix_confirm_prompt": (
            "✅ Task #{id} marked as done.\n\n"
            "Please confirm the issue is actually fixed:"
        ),
        "fix_confirmed": "✅ Thanks! Task #{id} confirmed as fixed.",
        "fix_rejected": "🔄 Task #{id} returned to work. Thanks for the feedback!",
        "fix_rejected_admin": "⚠️ User @{user} rejected the fix for task #{id}. Task returned to in_progress.",
        "lang_changed": "✅ Language changed to English 🇬🇧",
        "lang_prompt": "🌐 Выберите язык / Choose language:",
        # Buttons
        "btn_bug": "🐛 Bug",
        "btn_idea": "💡 Idea",
        "btn_balance": "⚖️ Balance",
        "btn_my_tasks": "📋 My tasks",
        "btn_lang": "🌐 Language",
        "btn_skip_photo": "⏩ Skip",
        "btn_done_photo": "✅ Done",
        "btn_send": "✅ Submit",
        "btn_cancel": "❌ Cancel",
        "btn_menu": "◀️ Menu",
        "btn_write": "💬 Comment",
        "btn_chat": "📎 Chat",
        "btn_edit": "✏️ Edit",
        "btn_back_tasks": "◀️ My tasks",
        "btn_open": "👁 Open",
        "btn_yes_fixed": "✅ Yes, fixed",
        "btn_no_not_fixed": "❌ No, not fixed",
        "btn_send_anyway": "✅ Submit anyway",
        # Feed
        "btn_feed": "📰 Feed",
        "feed_empty": "📰 No tasks to browse yet.",
        "feed_card": (
            "📰 **Feed** ({pos}/{total})\n\n"
            "{cat_emoji} {cat_label} #{id}\n"
            "────────────────\n"
            "{desc}\n\n"
            "👍 {likes}  👎 {dislikes}  |  Rating: {rating}\n"
            "👤 @{author}"
        ),
        "dislike_added": "👎 Vote counted",
        "dislike_removed": "👎 Vote removed",
        "spam_rejected": "❌ Your description looks like spam. Write a meaningful description (min. 10 characters).",
        # Profile
        "profile_title": (
            "👤 **Your profile**\n\n"
            "📋 Tasks created: {tasks}\n"
            "💬 Comments: {comments}\n"
            "👍 Votes: {votes}\n"
            "⚠️ Warnings: {warnings}\n"
            "{status_line}"
            "{first_date}"
        ),
        "profile_first_date": "📅 First task: {date}\n",
        "profile_status_line": "  {emoji} {label}: {count}\n",
        "btn_profile": "👤 Profile",
        # Feed sort
        "feed_sort_new": "🕐 New",
        "feed_sort_popular": "🔥 Popular",
        "feed_sort_controversial": "⚡ Controversial",
        # Comment notification
        "comment_notify": "💬 Admin @{admin} commented on your task #{id}:\n\n{text}",
        # Player stats
        "stats_title": "🎮 **Stats: {name}**\n\n",
        "stats_games": "🎲 Games: {played} | 🏆 Wins: {won} ({wr}%)\n",
        "stats_waves": "🌊 Waves: {total} | 🔝 Best: {highest}\n",
        "stats_combat": "⚔️ Killed: {enemies} | 👹 Bosses: {bosses}\n",
        "stats_towers": "🛡 Towers: {placed} | 💥 Damage: {damage}\n",
        "stats_economy": "💰 Earned: {earned} | Spent: {spent}\n",
        "stats_time": "⏰ Playtime: {hours}h {mins}m\n",
        "stats_not_found": "❌ Player not found.",
        "stats_no_data": "📊 No stats yet.",
        "link_prompt": "🔗 Send your **Roblox username** to link:",
        "link_success": "✅ Account **{name}** linked!",
        "link_fail": "❌ Player **{name}** not found. Play the game first.",
        "top_title": "🏆 **Leaderboard** ({stat}):\n\n",
        "top_line": "{pos}. **{name}** — {value}\n",
        "btn_stats": "🎮 Stats",
        "btn_top": "🏆 Top",
        "btn_link": "🔗 Link",
        "stats_prompt": "🎮 Send player name or /stats for your own stats.",
        "stats_loading": "⏳ Loading stats for {name}...",
        "stats_timeout": "❌ Player not found or server unavailable. Try again later.",
        "match_title": "📄 **Recent matches:**\n\n",
        "match_line": "{result} {map} ({diff}) — wave {wave}, {kills}☠\n",
    },
}


def t(key: str, lang: str = "ru", **kwargs) -> str:
    """Get translated text by key and language."""
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, TEXTS["ru"].get(key, key))
    if kwargs:
        return text.format(**kwargs)
    return text
