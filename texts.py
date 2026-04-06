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
        # News
        "news_subscribed": "✅ Вы подписались на новости игры!",
        "news_unsubscribed": "🔕 Вы отписались от новостей.",
        "news_prompt": "📢 Отправьте сообщение для рассылки (текст, фото или видео):",
        "news_link_prompt": "🔗 Отправьте URL для кнопки-ссылки и текст через пробел (например: `https://example.com Подробнее`)\nИли /skip чтобы пропустить.",
        "news_confirm": "📢 Предпросмотр отправлен выше. Разослать {count} подписчикам?",
        "news_sent": "✅ Рассылка завершена!\nОтправлено: {ok} | Ошибок: {fail}",
        "news_cancel": "❌ Рассылка отменена.",
        "news_no_subs": "📭 Нет подписчиков.",
        "btn_news_sub": "📢 Подписаться на новости",
        "btn_news_unsub": "🔕 Отписаться от новостей",
        # Promo codes
        "promo_enter_code": "🎟 Введите промокод:",
        "promo_success": "🎉 Промокод активирован!\n\nНаграда: {reward}",
        "promo_expired": "❌ Промокод истёк.",
        "promo_used_up": "❌ Промокод использован максимальное число раз.",
        "promo_already_redeemed": "❌ Вы уже активировали этот промокод.",
        "promo_not_found": "❌ Промокод не найден.",
        "promo_personal": "❌ Этот промокод предназначен для другого пользователя.",
        "promo_inactive": "❌ Промокод неактивен.",
        "btn_redeem": "🎟 Промокод",
        # FAQ
        "faq_title": "❓ **FAQ — Часто задаваемые вопросы**\n\nВыберите категорию:",
        "faq_entry": "❓ **{question}**\n\n{answer}",
        "faq_empty": "📭 FAQ пока пуст.",
        "faq_cat_empty": "📭 В этой категории пока нет вопросов.",
        "btn_faq": "❓ FAQ",
        # Polls
        "poll_question": "📊 **Опрос:** {question}",
        "poll_voted": "✅ Голос учтён: {option}",
        "poll_changed": "🔄 Голос изменён: {option}",
        "poll_closed": "📊 Опрос закрыт!",
        "poll_no_active": "📭 Нет активных опросов.",
        "btn_polls": "📊 Опросы",
        # Server
        "server_status": "{status} **Сервер**\n\n👥 Онлайн: {online}\n📈 Пик сегодня: {peak}\n🕐 Обновлено: {time}",
        "btn_server": "🖥 Сервер",
        # Weekly top
        "weekly_top_title": "📊 **Топ недели** ({stat}):\n🗓 {week}\n\n",
        "weekly_top_line": "{pos}. **{name}** — {value} {arrow}\n",
        "weekly_top_empty": "📭 Данных за эту неделю пока нет.",
        "btn_weekly_top": "📊 Топ недели",
        # Giveaways
        "giveaway_card": "🎁 **Розыгрыш: {title}**\n\n{description}\n\n🎖 Приз: {prize}\n👥 Участников: {count}\n⏰ До: {end_time}\n🏆 Победителей: {winners}",
        "giveaway_joined": "✅ Вы участвуете в розыгрыше!",
        "giveaway_left": "❌ Вы вышли из розыгрыша.",
        "giveaway_ended": "🏆 Розыгрыш «{title}» завершён!",
        "giveaway_winner_notify": "🎉 Поздравляем! Вы выиграли в розыгрыше «{title}»!\n\n🎖 Приз: {prize}{code_text}",
        "giveaway_no_active": "📭 Нет активных розыгрышей.",
        "btn_giveaways": "🎁 Розыгрыши",
        "btn_participate": "🎁 Участвовать ({count})",
        "btn_leave_giveaway": "❌ Выйти",
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
        # News
        "news_subscribed": "✅ You subscribed to game news!",
        "news_unsubscribed": "🔕 You unsubscribed from news.",
        "news_prompt": "📢 Send a message to broadcast (text, photo or video):",
        "news_link_prompt": "🔗 Send a URL and button text separated by space (e.g. `https://example.com Read more`)\nOr /skip to skip.",
        "news_confirm": "📢 Preview sent above. Broadcast to {count} subscribers?",
        "news_sent": "✅ Broadcast complete!\nSent: {ok} | Errors: {fail}",
        "news_cancel": "❌ Broadcast cancelled.",
        "news_no_subs": "📭 No subscribers.",
        "btn_news_sub": "📢 Subscribe to news",
        "btn_news_unsub": "🔕 Unsubscribe from news",
        # Promo codes
        "promo_enter_code": "🎟 Enter promo code:",
        "promo_success": "🎉 Promo code activated!\n\nReward: {reward}",
        "promo_expired": "❌ Promo code expired.",
        "promo_used_up": "❌ Promo code has been fully used.",
        "promo_already_redeemed": "❌ You already used this promo code.",
        "promo_not_found": "❌ Promo code not found.",
        "promo_personal": "❌ This promo code is for another user.",
        "promo_inactive": "❌ Promo code is inactive.",
        "btn_redeem": "🎟 Promo code",
        # FAQ
        "faq_title": "❓ **FAQ — Frequently Asked Questions**\n\nChoose a category:",
        "faq_entry": "❓ **{question}**\n\n{answer}",
        "faq_empty": "📭 FAQ is empty.",
        "faq_cat_empty": "📭 No questions in this category yet.",
        "btn_faq": "❓ FAQ",
        # Polls
        "poll_question": "📊 **Poll:** {question}",
        "poll_voted": "✅ Vote recorded: {option}",
        "poll_changed": "🔄 Vote changed: {option}",
        "poll_closed": "📊 Poll closed!",
        "poll_no_active": "📭 No active polls.",
        "btn_polls": "📊 Polls",
        # Server
        "server_status": "{status} **Server**\n\n👥 Online: {online}\n📈 Peak today: {peak}\n🕐 Updated: {time}",
        "btn_server": "🖥 Server",
        # Weekly top
        "weekly_top_title": "📊 **Weekly Top** ({stat}):\n🗓 {week}\n\n",
        "weekly_top_line": "{pos}. **{name}** — {value} {arrow}\n",
        "weekly_top_empty": "📭 No data for this week yet.",
        "btn_weekly_top": "📊 Weekly Top",
        # Giveaways
        "giveaway_card": "🎁 **Giveaway: {title}**\n\n{description}\n\n🎖 Prize: {prize}\n👥 Participants: {count}\n⏰ Until: {end_time}\n🏆 Winners: {winners}",
        "giveaway_joined": "✅ You joined the giveaway!",
        "giveaway_left": "❌ You left the giveaway.",
        "giveaway_ended": "🏆 Giveaway «{title}» ended!",
        "giveaway_winner_notify": "🎉 Congratulations! You won the giveaway «{title}»!\n\n🎖 Prize: {prize}{code_text}",
        "giveaway_no_active": "📭 No active giveaways.",
        "btn_giveaways": "🎁 Giveaways",
        "btn_participate": "🎁 Join ({count})",
        "btn_leave_giveaway": "❌ Leave",
    },
}


def t(key: str, lang: str = "ru", **kwargs) -> str:
    """Get translated text by key and language."""
    text = TEXTS.get(lang, TEXTS["ru"]).get(key, TEXTS["ru"].get(key, key))
    if kwargs:
        return text.format(**kwargs)
    return text
