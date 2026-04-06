"""Shared state and helpers for Roblox stats polling system."""
import asyncio

# pending_stats: place -> list of {"username": str}
pending_stats: dict[str, list[dict]] = {
    "public": [],
    "private": [],
}

# stats_waiters: username_lower -> list of {chat_id, message_id, event}
stats_waiters: dict[str, list[dict]] = {}


def format_roblox_stats(stats: dict) -> str:
    """Format Roblox player stats into a pretty message."""
    username = stats.get("username", "???")
    level = stats.get("level", 0)
    exp = stats.get("exp", 0)
    exp_next = stats.get("expToNextLevel", 0)
    money = stats.get("money", 0)
    event_money = stats.get("eventMoney", 0)
    wins = stats.get("wins", 0)
    daily_streak = stats.get("dailyStreak", 0)
    play_time = int(stats.get("timePlayed", 0))  # в секундах

    owned_towers = stats.get("ownedTowers", [])
    selected_towers = stats.get("selectedTowers", [])
    owned_skins = stats.get("ownedSkins", [])
    selected_skins = stats.get("selectedSkins", [])

    coin_boosts = stats.get("coinBoosts", 0)
    exp_boosts = stats.get("expBoosts", 0)

    selected_tag = stats.get("selectedTag", "")

    money_rank = stats.get("moneyRank")
    wins_rank = stats.get("winsRank")
    is_online = stats.get("isOnline", False)

    money_str = f"{money:,}"
    event_money_str = str(event_money)
    pt_hours = play_time // 3600
    pt_mins = (play_time % 3600) // 60

    lines = [
        f"🎮 Статистика игрока: {username}\n",
        "📊 Основное:",
        f"├ Уровень: {level} ({exp}/{exp_next} XP)",
        f"├ Монеты: {money_str} 💰",
        f"├ Ивент монеты: {event_money_str}",
        f"├ Победы: {wins} 🏆",
        f"├ Дейли стрик: {daily_streak} дней 🔥",
        f"└ Время в игре: {pt_hours}ч {pt_mins}м ⏱️",
        "",
        f"🏰 Башни ({len(owned_towers)}):",
        ", ".join(owned_towers) if owned_towers else "нет",
        "",
        "⚔️ Текущий лодаут:",
        ", ".join(selected_towers) if selected_towers else "нет",
        "",
    ]

    lines.append(f"🎨 Скины ({len(owned_skins)}):")
    if owned_skins:
        lines.append(", ".join(owned_skins))
    else:
        lines.append("нет")

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

    lines.append(f"🏷️ Тег: {selected_tag or 'нет'}")
    lines.append("")

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

    lines.append("📈 Рейтинг:")
    lines.append(f"├ Топ монет: #{money_rank}" if money_rank else "├ Топ монет: не в топ-100")
    lines.append(f"└ Топ побед: #{wins_rank}" if wins_rank else "└ Топ побед: не в топ-100")
    lines.append("")

    lines.append("🟢 Сейчас в игре" if is_online else "🔴 Не в игре")

    return "\n".join(lines)
