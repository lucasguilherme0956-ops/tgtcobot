import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_ADMIN_ID = int(os.getenv("MAIN_ADMIN_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")
DATABASE_URL = os.getenv("DATABASE_URL")
GAME_API_KEY = os.getenv("GAME_API_KEY", "change-me-secret-key-123")
STATS_SECRET = os.getenv("STATS_SECRET", "tco_stats_secret_key_2026")

# Антифлуд
THROTTLE_RATE = 2  # секунд между сообщениями
MAX_TASKS_PER_HOUR = 5
BAN_THRESHOLD = 10  # нарушений до бана
BAN_DURATION_HOURS = 1
MAX_DESCRIPTION_LENGTH = 2000

CATEGORIES = {
    "bug": "🐛 Баг",
    "idea": "💡 Идея",
    "balance": "⚖️ Баланс",
}

STATUSES = {
    "new": "🆕 Новая",
    "in_progress": "🔄 В процессе",
    "done": "✅ Выполнено",
    "archived": "📦 Архив",
}

AUTO_ARCHIVE_DAYS = 30

PRIORITIES = {
    "critical": "🔴 Критический",
    "high": "🟠 Высокий",
    "medium": "🟡 Средний",
    "low": "🟢 Низкий",
}
