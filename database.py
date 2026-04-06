import asyncpg
from datetime import datetime, timezone, timedelta
import json
import time as _time

from config import DATABASE_URL, TIMEZONE

try:
    import zoneinfo
    TZ = zoneinfo.ZoneInfo(TIMEZONE)
except Exception:
    TZ = timezone(timedelta(hours=3))

_pool: asyncpg.Pool | None = None


# ─── TTL Cache ───

class _TTLCache:
    """Simple in-memory cache with TTL to reduce DB round-trips."""
    __slots__ = ("_store",)

    def __init__(self):
        self._store: dict[str, tuple] = {}

    def get(self, key: str):
        entry = self._store.get(key)
        if entry and _time.monotonic() < entry[1]:
            return entry[0]
        if entry:
            del self._store[key]
        return None

    def set(self, key: str, value, ttl: float = 30.0):
        self._store[key] = (value, _time.monotonic() + ttl)

    def invalidate(self, prefix: str):
        self._store = {k: v for k, v in self._store.items() if not k.startswith(prefix)}

    def delete(self, key: str):
        self._store.pop(key, None)


_cache = _TTLCache()


def now_msk() -> datetime:
    return datetime.now(TZ)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=8)
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def keepalive():
    """Lightweight ping to prevent Neon cold starts."""
    pool = await get_pool()
    await pool.fetchval("SELECT 1")


def _record_to_dict(record: asyncpg.Record) -> dict:
    return dict(record)


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                category TEXT NOT NULL,
                description TEXT NOT NULL,
                photo_file_id TEXT,
                priority TEXT NOT NULL DEFAULT 'medium',
                status TEXT NOT NULL DEFAULT 'new',
                pinned INTEGER NOT NULL DEFAULT 0,
                deadline TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                author_id BIGINT NOT NULL,
                author_name TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notification_settings (
                admin_id BIGINT PRIMARY KEY,
                schedule_times TEXT NOT NULL DEFAULT '["09:00","18:00"]',
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_bans (
                user_id BIGINT PRIMARY KEY,
                banned_until TEXT NOT NULL,
                reason TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                user_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (task_id, user_id)
            )
        """)

        # ─── New tables for features ───

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS task_history (
                id SERIAL PRIMARY KEY,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                admin_id BIGINT,
                admin_name TEXT NOT NULL DEFAULT '',
                field TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TEXT NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS task_photos (
                id SERIAL PRIMARY KEY,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                file_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id BIGINT PRIMARY KEY,
                lang TEXT NOT NULL DEFAULT 'ru'
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS task_tags (
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                tag_id INTEGER NOT NULL REFERENCES tags(id),
                PRIMARY KEY (task_id, tag_id)
            )
        """)

        # ─── Migrations for new columns ───
        for col, typ, default in [
            ("assigned_admin_id", "BIGINT", None),
            ("assigned_admin_name", "TEXT", None),
        ]:
            try:
                if default is not None:
                    await conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {typ} DEFAULT {default}")
                else:
                    await conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {typ}")
            except Exception:
                pass  # column already exists

        # Add value column to votes (1 = like, -1 = dislike)
        try:
            await conn.execute("ALTER TABLE votes ADD COLUMN value INTEGER NOT NULL DEFAULT 1")
        except Exception:
            pass  # column already exists

        # Warnings table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_warnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                admin_id BIGINT NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Admin activity log
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_log (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                admin_name TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Task links (duplicates)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS task_links (
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                linked_task_id INTEGER NOT NULL REFERENCES tasks(id),
                link_type TEXT NOT NULL DEFAULT 'duplicate',
                created_at TEXT NOT NULL,
                PRIMARY KEY (task_id, linked_task_id)
            )
        """)

        # Reply-to for comments
        try:
            await conn.execute("ALTER TABLE comments ADD COLUMN reply_to_id INTEGER")
        except Exception:
            pass

        # ─── Player stats (Roblox) ───

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                roblox_id BIGINT PRIMARY KEY,
                roblox_username TEXT NOT NULL,
                telegram_id BIGINT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                roblox_id BIGINT PRIMARY KEY REFERENCES players(roblox_id),
                playtime_minutes INTEGER NOT NULL DEFAULT 0,
                games_played INTEGER NOT NULL DEFAULT 0,
                games_won INTEGER NOT NULL DEFAULT 0,
                highest_wave INTEGER NOT NULL DEFAULT 0,
                total_waves INTEGER NOT NULL DEFAULT 0,
                enemies_killed INTEGER NOT NULL DEFAULT 0,
                bosses_killed INTEGER NOT NULL DEFAULT 0,
                towers_placed INTEGER NOT NULL DEFAULT 0,
                coins_earned BIGINT NOT NULL DEFAULT 0,
                coins_spent BIGINT NOT NULL DEFAULT 0,
                damage_dealt BIGINT NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS player_matches (
                id SERIAL PRIMARY KEY,
                roblox_id BIGINT NOT NULL REFERENCES players(roblox_id),
                map_name TEXT,
                difficulty TEXT,
                wave_reached INTEGER NOT NULL DEFAULT 0,
                max_wave INTEGER,
                won BOOLEAN NOT NULL DEFAULT FALSE,
                enemies_killed INTEGER NOT NULL DEFAULT 0,
                bosses_killed INTEGER NOT NULL DEFAULT 0,
                towers_placed INTEGER NOT NULL DEFAULT 0,
                coins_earned INTEGER NOT NULL DEFAULT 0,
                damage_dealt BIGINT NOT NULL DEFAULT 0,
                duration_seconds INTEGER NOT NULL DEFAULT 0,
                played_at TEXT NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats_cache (
                username TEXT PRIMARY KEY,
                stats_json TEXT NOT NULL,
                place TEXT NOT NULL DEFAULT 'public',
                updated_at TEXT NOT NULL
            )
        """)

        # Index for fast player lookup
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_players_tg ON players(telegram_id)")
        except Exception:
            pass
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_roblox ON player_matches(roblox_id)")
        except Exception:
            pass


# ─── Tasks ───

async def create_task(user_id: int, username: str | None, category: str,
                      description: str, photo_file_id: str | None = None) -> int:
    ts = now_msk().isoformat()
    pool = await get_pool()
    row = await pool.fetchval(
        "INSERT INTO tasks (user_id, username, category, description, photo_file_id, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
        user_id, username, category, description, photo_file_id, ts, ts,
    )
    return row


async def get_task(task_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
    return _record_to_dict(row) if row else None


async def get_tasks_filtered(status: str | None = None, category: str | None = None,
                             priority: str | None = None, limit: int = 5,
                             offset: int = 0) -> list[dict]:
    conditions = []
    params = []
    idx = 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    else:
        conditions.append("status != 'archived'")
    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1
    if priority:
        conditions.append(f"priority = ${idx}")
        params.append(priority)
        idx += 1

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT * FROM tasks{where} ORDER BY pinned DESC, created_at DESC LIMIT ${idx} OFFSET ${idx + 1}"
    params.extend([limit, offset])

    pool = await get_pool()
    rows = await pool.fetch(query, *params)
    return [_record_to_dict(r) for r in rows]


async def count_tasks_filtered(status: str | None = None, category: str | None = None,
                               priority: str | None = None) -> int:
    conditions = []
    params = []
    idx = 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    else:
        conditions.append("status != 'archived'")
    if category:
        conditions.append(f"category = ${idx}")
        params.append(category)
        idx += 1
    if priority:
        conditions.append(f"priority = ${idx}")
        params.append(priority)
        idx += 1

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT COUNT(*) FROM tasks{where}"

    pool = await get_pool()
    return await pool.fetchval(query, *params)


async def update_task_status(task_id: int, status: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET status = $1, updated_at = $2 WHERE id = $3",
                       status, ts, task_id)


async def update_task_priority(task_id: int, priority: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET priority = $1, updated_at = $2 WHERE id = $3",
                       priority, ts, task_id)


async def get_user_tasks(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE user_id = $1 AND status != 'archived' "
        "ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        user_id, limit, offset,
    )
    return [_record_to_dict(r) for r in rows]


async def count_user_tasks(user_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM tasks WHERE user_id = $1 AND status != 'archived'", user_id
    )


async def count_user_tasks_last_hour(user_id: int) -> int:
    one_hour_ago = (now_msk() - timedelta(hours=1)).isoformat()
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM tasks WHERE user_id = $1 AND created_at > $2",
        user_id, one_hour_ago,
    )


# ─── Admins ───

async def is_admin(user_id: int) -> bool:
    from config import MAIN_ADMIN_ID
    if user_id == MAIN_ADMIN_ID:
        return True
    pool = await get_pool()
    row = await pool.fetchrow("SELECT 1 FROM admins WHERE user_id = $1", user_id)
    return row is not None


async def add_admin(user_id: int):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
    )


async def remove_admin(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM admins WHERE user_id = $1", user_id)


async def get_all_admin_ids() -> list[int]:
    from config import MAIN_ADMIN_ID
    pool = await get_pool()
    rows = await pool.fetch("SELECT user_id FROM admins")
    ids = {row["user_id"] for row in rows}
    ids.add(MAIN_ADMIN_ID)
    return list(ids)


# ─── Comments ───

async def add_comment(task_id: int, author_id: int, text: str, author_name: str = "") -> int:
    ts = now_msk().isoformat()
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO comments (task_id, author_id, author_name, text, created_at) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING id",
        task_id, author_id, author_name, text, ts,
    )


async def get_comments(task_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM comments WHERE task_id = $1 ORDER BY created_at ASC", task_id
    )
    return [_record_to_dict(r) for r in rows]


# ─── Notification settings ───

async def get_notify_settings(admin_id: int) -> dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM notification_settings WHERE admin_id = $1", admin_id
    )
    if row:
        d = _record_to_dict(row)
        d["schedule_times"] = json.loads(d["schedule_times"])
        return d
    return {"admin_id": admin_id, "schedule_times": ["09:00", "18:00"], "enabled": 1}


async def set_notify_settings(admin_id: int, schedule_times: list[str], enabled: bool):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO notification_settings (admin_id, schedule_times, enabled) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT(admin_id) DO UPDATE SET schedule_times = EXCLUDED.schedule_times, "
        "enabled = EXCLUDED.enabled",
        admin_id, json.dumps(schedule_times), int(enabled),
    )


# ─── Bans ───

async def ban_user(user_id: int, hours: int, reason: str = "Флуд"):
    until = (now_msk() + timedelta(hours=hours)).isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO user_bans (user_id, banned_until, reason) VALUES ($1, $2, $3) "
        "ON CONFLICT(user_id) DO UPDATE SET banned_until = EXCLUDED.banned_until, "
        "reason = EXCLUDED.reason",
        user_id, until, reason,
    )


async def is_banned(user_id: int) -> bool:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT banned_until FROM user_bans WHERE user_id = $1", user_id
    )
    if not row:
        return False
    banned_until = datetime.fromisoformat(row["banned_until"])
    if now_msk() >= banned_until:
        await pool.execute("DELETE FROM user_bans WHERE user_id = $1", user_id)
        return False
    return True


async def unban_user(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM user_bans WHERE user_id = $1", user_id)


async def get_ban_info(user_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT banned_until, reason FROM user_bans WHERE user_id = $1", user_id
    )
    if not row:
        return None
    return {"banned_until": row["banned_until"], "reason": row["reason"]}


async def warn_user(user_id: int, admin_id: int, reason: str = "Нарушение правил"):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO user_warnings (user_id, admin_id, reason, created_at) "
        "VALUES ($1, $2, $3, $4)",
        user_id, admin_id, reason, ts,
    )


async def get_warning_count(user_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM user_warnings WHERE user_id = $1", user_id
    )


async def get_warnings(user_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT reason, created_at FROM user_warnings WHERE user_id = $1 ORDER BY created_at DESC LIMIT 10",
        user_id,
    )
    return [dict(r) for r in rows]


async def clear_warnings(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM user_warnings WHERE user_id = $1", user_id)


# ─── Stats for notifications ───

async def get_task_stats() -> dict:
    pool = await get_pool()
    rows = await pool.fetch("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status")
    return {row["status"]: row["cnt"] for row in rows}


async def get_recent_new_tasks(hours: int = 24) -> list[dict]:
    since = (now_msk() - timedelta(hours=hours)).isoformat()
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE status = 'new' AND created_at > $1 ORDER BY created_at DESC",
        since,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Delete ───

async def delete_task(task_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM votes WHERE task_id = $1", task_id)
            await conn.execute("DELETE FROM comments WHERE task_id = $1", task_id)
            await conn.execute("DELETE FROM task_history WHERE task_id = $1", task_id)
            await conn.execute("DELETE FROM task_photos WHERE task_id = $1", task_id)
            await conn.execute("DELETE FROM task_tags WHERE task_id = $1", task_id)
            await conn.execute("DELETE FROM tasks WHERE id = $1", task_id)


# ─── Search ───

async def search_tasks(query: str, limit: int = 10) -> list[dict]:
    pool = await get_pool()
    safe_query = query.replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{safe_query}%"
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE description ILIKE $1 OR "
        "CAST(id AS TEXT) = $2 ORDER BY created_at DESC LIMIT $3",
        pattern, query.strip(), limit,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Extended stats ───

async def get_extended_stats() -> dict:
    pool = await get_pool()
    week_ago = (now_msk() - timedelta(days=7)).isoformat()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT "
            "  COUNT(*) as total, "
            "  COUNT(*) FILTER (WHERE created_at > $1) as week, "
            "  AVG(EXTRACT(EPOCH FROM (updated_at::timestamp - created_at::timestamp)) / 86400.0) "
            "    FILTER (WHERE status = 'done') as avg_days "
            "FROM tasks",
            week_ago,
        )

        rows = await conn.fetch(
            "SELECT 'status' as dim, status as key, COUNT(*) as cnt FROM tasks GROUP BY status "
            "UNION ALL "
            "SELECT 'category', category, COUNT(*) FROM tasks GROUP BY category "
            "UNION ALL "
            "SELECT 'priority', priority, COUNT(*) FROM tasks GROUP BY priority"
        )

        status_stats, cat_stats, prio_stats = {}, {}, {}
        for r in rows:
            d = r["dim"]
            if d == "status":
                status_stats[r["key"]] = r["cnt"]
            elif d == "category":
                cat_stats[r["key"]] = r["cnt"]
            else:
                prio_stats[r["key"]] = r["cnt"]

        return {
            "status": status_stats,
            "category": cat_stats,
            "priority": prio_stats,
            "total": row["total"],
            "week": row["week"],
            "avg_close_days": round(float(row["avg_days"]), 1) if row["avg_days"] else 0,
        }


# ─── Votes ───

async def toggle_vote(task_id: int, user_id: int) -> bool:
    """Toggle like vote. Returns True if voted, False if removed."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT value FROM votes WHERE task_id = $1 AND user_id = $2",
        task_id, user_id,
    )
    if row:
        if row["value"] == 1:
            await pool.execute("DELETE FROM votes WHERE task_id = $1 AND user_id = $2",
                               task_id, user_id)
            _cache.invalidate("feed")
            return False
        else:
            await pool.execute("UPDATE votes SET value = 1, created_at = $1 WHERE task_id = $2 AND user_id = $3",
                               ts, task_id, user_id)
            _cache.invalidate("feed")
            return True
    else:
        await pool.execute("INSERT INTO votes (task_id, user_id, created_at, value) VALUES ($1, $2, $3, 1)",
                           task_id, user_id, ts)
        _cache.invalidate("feed")
        return True


async def get_vote_count(task_id: int) -> int:
    """Count of likes (value=1)."""
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM votes WHERE task_id = $1 AND value = 1", task_id
    )


async def has_voted(task_id: int, user_id: int) -> bool:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM votes WHERE task_id = $1 AND user_id = $2 AND value = 1",
        task_id, user_id,
    )
    return row is not None


async def toggle_dislike(task_id: int, user_id: int) -> bool:
    """Toggle dislike. Returns True if disliked, False if removed."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT value FROM votes WHERE task_id = $1 AND user_id = $2",
        task_id, user_id,
    )
    if row:
        if row["value"] == -1:
            await pool.execute("DELETE FROM votes WHERE task_id = $1 AND user_id = $2",
                               task_id, user_id)
            _cache.invalidate("feed")
            return False
        else:
            await pool.execute("UPDATE votes SET value = -1, created_at = $1 WHERE task_id = $2 AND user_id = $3",
                               ts, task_id, user_id)
            _cache.invalidate("feed")
            return True
    else:
        await pool.execute("INSERT INTO votes (task_id, user_id, created_at, value) VALUES ($1, $2, $3, -1)",
                           task_id, user_id, ts)
        _cache.invalidate("feed")
        return True


async def get_dislike_count(task_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM votes WHERE task_id = $1 AND value = -1", task_id
    )


async def get_user_vote_type(task_id: int, user_id: int) -> int | None:
    """Returns 1 (like), -1 (dislike), or None."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT value FROM votes WHERE task_id = $1 AND user_id = $2",
        task_id, user_id,
    )
    return row["value"] if row else None


# ─── Feed ───

async def get_feed_tasks(limit: int = 1, offset: int = 0) -> list[dict]:
    """Get all tasks sorted by rating. Cached 30s."""
    key = f"feed_tasks:{limit}:{offset}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT t.*, "
        "COALESCE(SUM(CASE WHEN v.value = 1 THEN 1 ELSE 0 END), 0)::int AS likes, "
        "COALESCE(SUM(CASE WHEN v.value = -1 THEN 1 ELSE 0 END), 0)::int AS dislikes, "
        "COALESCE(SUM(v.value), 0)::int AS rating "
        "FROM tasks t LEFT JOIN votes v ON t.id = v.task_id "
        "WHERE t.status NOT IN ('done', 'archived') "
        "GROUP BY t.id "
        "ORDER BY rating DESC, t.created_at DESC "
        "LIMIT $1 OFFSET $2",
        limit, offset,
    )
    result = [_record_to_dict(r) for r in rows]
    _cache.set(key, result, ttl=30)
    return result


async def count_feed_tasks() -> int:
    """Cached 60s."""
    cached = _cache.get("feed_count")
    if cached is not None:
        return cached
    pool = await get_pool()
    val = await pool.fetchval(
        "SELECT COUNT(*) FROM tasks "
        "WHERE status NOT IN ('done', 'archived')"
    )
    _cache.set("feed_count", val, ttl=60)
    return val


# ─── Pin ───

async def toggle_pin(task_id: int) -> bool:
    """Toggle pin. Returns new pinned state."""
    pool = await get_pool()
    row = await pool.fetchrow("SELECT pinned FROM tasks WHERE id = $1", task_id)
    if not row:
        return False
    new_val = 0 if row["pinned"] else 1
    await pool.execute("UPDATE tasks SET pinned = $1 WHERE id = $2", new_val, task_id)
    return bool(new_val)


# ─── Archive ───

async def archive_task(task_id: int):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET status = 'archived', updated_at = $1 WHERE id = $2",
                       ts, task_id)


async def restore_task(task_id: int):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET status = 'new', updated_at = $1 WHERE id = $2",
                       ts, task_id)


async def get_archived_tasks(limit: int = 5, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE status = 'archived' ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
        limit, offset,
    )
    return [_record_to_dict(r) for r in rows]


async def count_archived_tasks() -> int:
    pool = await get_pool()
    return await pool.fetchval("SELECT COUNT(*) FROM tasks WHERE status = 'archived'")


# ─── Deadline ───

async def set_deadline(task_id: int, deadline: str | None):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET deadline = $1, updated_at = $2 WHERE id = $3",
                       deadline, ts, task_id)


async def get_overdue_tasks() -> list[dict]:
    now = now_msk().isoformat()
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE deadline IS NOT NULL AND deadline < $1 "
        "AND status NOT IN ('done', 'archived') ORDER BY deadline ASC",
        now,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Duplicate detection ───

async def find_similar_tasks(description: str, category: str, limit: int = 3) -> list[dict]:
    words = [w for w in description.lower().split() if len(w) >= 4][:5]
    if not words:
        return []

    pool = await get_pool()
    conditions = []
    params = []
    idx = 1
    for w in words:
        conditions.append(f"LOWER(description) LIKE ${idx}")
        params.append(f"%{w}%")
        idx += 1
    params.append(category)
    params.append(limit)

    query = (
        f"SELECT * FROM tasks WHERE ({' OR '.join(conditions)}) AND category = ${idx} "
        f"AND status != 'archived' ORDER BY created_at DESC LIMIT ${idx + 1}"
    )
    rows = await pool.fetch(query, *params)
    return [_record_to_dict(r) for r in rows]


# ─── Auto-archive ───

async def auto_archive_old_done(days: int = 30) -> int:
    cutoff = (now_msk() - timedelta(days=days)).isoformat()
    ts = now_msk().isoformat()
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE tasks SET status = 'archived', updated_at = $1 "
        "WHERE status = 'done' AND updated_at < $2",
        ts, cutoff,
    )
    # asyncpg returns 'UPDATE N'
    return int(result.split()[-1])


# ─── Export ───

async def get_all_tasks_for_export() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM tasks ORDER BY id ASC")
    return [_record_to_dict(r) for r in rows]


# ─── Task History ───

async def add_history_entry(task_id: int, admin_id: int | None, admin_name: str,
                            field: str, old_value: str | None, new_value: str | None):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO task_history (task_id, admin_id, admin_name, field, old_value, new_value, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
        task_id, admin_id, admin_name, field, old_value, new_value, ts,
    )


async def get_task_history(task_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM task_history WHERE task_id = $1 ORDER BY created_at DESC LIMIT 20",
        task_id,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Task Photos ───

async def add_task_photo(task_id: int, file_id: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO task_photos (task_id, file_id, created_at) VALUES ($1, $2, $3)",
        task_id, file_id, ts,
    )
    _cache.delete(f"photos:{task_id}")


async def get_task_photos(task_id: int) -> list[dict]:
    """Cached 120s."""
    key = f"photos:{task_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM task_photos WHERE task_id = $1 ORDER BY created_at ASC", task_id
    )
    result = [_record_to_dict(r) for r in rows]
    _cache.set(key, result, ttl=120)
    return result


async def migrate_photo_to_table(task_id: int, file_id: str):
    """Move legacy photo_file_id into task_photos table."""
    existing = await get_task_photos(task_id)
    if not any(p["file_id"] == file_id for p in existing):
        await add_task_photo(task_id, file_id)


# ─── Task Edit (user) ───

async def update_task_description(task_id: int, new_description: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "UPDATE tasks SET description = $1, updated_at = $2 WHERE id = $3",
        new_description, ts, task_id,
    )


# ─── Admin Assignment ───

async def assign_task(task_id: int, admin_id: int | None, admin_name: str | None):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "UPDATE tasks SET assigned_admin_id = $1, assigned_admin_name = $2, updated_at = $3 WHERE id = $4",
        admin_id, admin_name, ts, task_id,
    )


# ─── Tags ───

async def get_or_create_tag(name: str) -> int:
    name = name.lower().strip()
    pool = await get_pool()
    row = await pool.fetchrow("SELECT id FROM tags WHERE name = $1", name)
    if row:
        return row["id"]
    return await pool.fetchval(
        "INSERT INTO tags (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        name,
    )


async def add_tag_to_task(task_id: int, tag_name: str):
    tag_id = await get_or_create_tag(tag_name)
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO task_tags (task_id, tag_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        task_id, tag_id,
    )
    _cache.delete(f"tags:{task_id}")


async def remove_tag_from_task(task_id: int, tag_name: str):
    tag_name = tag_name.lower().strip()
    pool = await get_pool()
    row = await pool.fetchrow("SELECT id FROM tags WHERE name = $1", tag_name)
    if row:
        await pool.execute(
            "DELETE FROM task_tags WHERE task_id = $1 AND tag_id = $2",
            task_id, row["id"],
        )
    _cache.delete(f"tags:{task_id}")


async def get_task_tags(task_id: int) -> list[str]:
    """Cached 120s."""
    key = f"tags:{task_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT t.name FROM tags t JOIN task_tags tt ON t.id = tt.tag_id WHERE tt.task_id = $1 ORDER BY t.name",
        task_id,
    )
    result = [r["name"] for r in rows]
    _cache.set(key, result, ttl=120)
    return result


async def get_all_tags() -> list[str]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT DISTINCT name FROM tags ORDER BY name")
    return [r["name"] for r in rows]


# ─── Auto-priority by votes ───

async def get_tasks_for_auto_priority() -> list[dict]:
    """Get idea/balance tasks with their vote counts for auto-priority."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT t.id, t.priority, "
        "COALESCE(SUM(CASE WHEN v.value = 1 THEN 1 ELSE 0 END), 0) as vote_count "
        "FROM tasks t LEFT JOIN votes v ON t.id = v.task_id "
        "WHERE t.category IN ('idea', 'balance') AND t.status NOT IN ('done', 'archived') "
        "GROUP BY t.id, t.priority "
        "ORDER BY vote_count DESC"
    )
    return [_record_to_dict(r) for r in rows]


# ─── User Settings (lang) ───

async def get_user_lang(user_id: int) -> str:
    """Cached 300s."""
    key = f"lang:{user_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    row = await pool.fetchrow("SELECT lang FROM user_settings WHERE user_id = $1", user_id)
    result = row["lang"] if row else "ru"
    _cache.set(key, result, ttl=300)
    return result


async def set_user_lang(user_id: int, lang: str):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO user_settings (user_id, lang) VALUES ($1, $2) "
        "ON CONFLICT(user_id) DO UPDATE SET lang = EXCLUDED.lang",
        user_id, lang,
    )
    _cache.delete(f"lang:{user_id}")


# ─── Inline search ───

async def search_tasks_inline(query: str, limit: int = 10) -> list[dict]:
    pool = await get_pool()
    pattern = f"%{query}%"
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE (description ILIKE $1 OR CAST(id AS TEXT) = $2) "
        "AND status != 'archived' ORDER BY created_at DESC LIMIT $3",
        pattern, query.strip(), limit,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Weekly report ───

async def get_weekly_report_data() -> dict:
    pool = await get_pool()
    week_ago = (now_msk() - timedelta(days=7)).isoformat()
    async with pool.acquire() as conn:
        created = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE created_at > $1", week_ago
        )
        closed = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'done' AND updated_at > $1", week_ago
        )
        archived = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'archived' AND updated_at > $1", week_ago
        )
        open_total = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done', 'archived')"
        )
        top_voted = await conn.fetch(
            "SELECT t.id, t.description, t.category, COUNT(v.user_id) as votes "
            "FROM tasks t JOIN votes v ON t.id = v.task_id "
            "WHERE t.status NOT IN ('done', 'archived') "
            "GROUP BY t.id, t.description, t.category "
            "ORDER BY votes DESC LIMIT 3"
        )
        return {
            "created": created,
            "closed": closed,
            "archived": archived,
            "open_total": open_total,
            "top_voted": [_record_to_dict(r) for r in top_voted],
        }


# ─── Admin Activity Log ───

async def add_admin_log(admin_id: int, admin_name: str, action: str, details: str | None = None):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO admin_log (admin_id, admin_name, action, details, created_at) "
        "VALUES ($1, $2, $3, $4, $5)",
        admin_id, admin_name, action, details, ts,
    )


async def get_admin_log(limit: int = 30) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM admin_log ORDER BY created_at DESC LIMIT $1", limit
    )
    return [_record_to_dict(r) for r in rows]


# ─── Task Links (duplicates) ───

async def link_tasks(task_id: int, linked_task_id: int, link_type: str = "duplicate"):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO task_links (task_id, linked_task_id, link_type, created_at) "
        "VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        task_id, linked_task_id, link_type, ts,
    )


async def get_linked_tasks(task_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT tl.linked_task_id, tl.link_type, t.description, t.status "
        "FROM task_links tl JOIN tasks t ON tl.linked_task_id = t.id "
        "WHERE tl.task_id = $1 "
        "UNION "
        "SELECT tl.task_id, tl.link_type, t.description, t.status "
        "FROM task_links tl JOIN tasks t ON tl.task_id = t.id "
        "WHERE tl.linked_task_id = $1",
        task_id,
    )
    return [_record_to_dict(r) for r in rows]


# ─── User Profile ───

async def get_user_profile(user_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        tasks_created = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE user_id = $1", user_id
        )
        comments_count = await conn.fetchval(
            "SELECT COUNT(*) FROM comments WHERE author_id = $1", user_id
        )
        votes_count = await conn.fetchval(
            "SELECT COUNT(*) FROM votes WHERE user_id = $1", user_id
        )
        warns_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_warnings WHERE user_id = $1", user_id
        )
        first_task = await conn.fetchval(
            "SELECT MIN(created_at) FROM tasks WHERE user_id = $1", user_id
        )
        rows = await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM tasks WHERE user_id = $1 GROUP BY status",
            user_id,
        )
        by_status = {r["status"]: r["cnt"] for r in rows}

        return {
            "tasks_created": tasks_created,
            "comments": comments_count,
            "votes": votes_count,
            "warnings": warns_count,
            "first_task_date": first_task,
            "by_status": by_status,
        }


# ─── Feed sort modes ───

async def get_feed_tasks_sorted(sort: str = "rating", limit: int = 1, offset: int = 0) -> list[dict]:
    key = f"feed:{sort}:{limit}:{offset}"
    cached = _cache.get(key)
    if cached is not None:
        return cached

    if sort == "new":
        order = "t.created_at DESC"
    elif sort == "controversial":
        order = ("(COALESCE(SUM(CASE WHEN v.value = 1 THEN 1 ELSE 0 END), 0) + "
                 "COALESCE(SUM(CASE WHEN v.value = -1 THEN 1 ELSE 0 END), 0)) DESC, "
                 "t.created_at DESC")
    else:  # rating
        order = "rating DESC, t.created_at DESC"

    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT t.*, "
        "COALESCE(SUM(CASE WHEN v.value = 1 THEN 1 ELSE 0 END), 0)::int AS likes, "
        "COALESCE(SUM(CASE WHEN v.value = -1 THEN 1 ELSE 0 END), 0)::int AS dislikes, "
        "COALESCE(SUM(v.value), 0)::int AS rating "
        "FROM tasks t LEFT JOIN votes v ON t.id = v.task_id "
        "WHERE t.status NOT IN ('done', 'archived') "
        f"GROUP BY t.id ORDER BY {order} "
        "LIMIT $1 OFFSET $2",
        limit, offset,
    )
    result = [_record_to_dict(r) for r in rows]
    _cache.set(key, result, ttl=30)
    return result


# ─── Deadline reminders ───

async def get_upcoming_deadlines(hours: int = 24) -> list[dict]:
    now = now_msk()
    future = (now + timedelta(hours=hours)).isoformat()
    now_iso = now.isoformat()
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM tasks WHERE deadline IS NOT NULL "
        "AND deadline > $1 AND deadline <= $2 "
        "AND status NOT IN ('done', 'archived') "
        "ORDER BY deadline ASC",
        now_iso, future,
    )
    return [_record_to_dict(r) for r in rows]


# ─── Dashboard stats ───

async def get_dashboard_stats() -> dict:
    pool = await get_pool()
    day_ago = (now_msk() - timedelta(hours=24)).isoformat()
    now_iso = now_msk().isoformat()
    async with pool.acquire() as conn:
        new_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE created_at > $1", day_ago
        )
        overdue = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE deadline IS NOT NULL AND deadline < $1 "
            "AND status NOT IN ('done', 'archived')", now_iso
        )
        total_open = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done', 'archived')"
        )
        in_progress = await conn.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'in_progress'"
        )
        top_voted = await conn.fetch(
            "SELECT t.id, t.description, t.category, "
            "COALESCE(SUM(CASE WHEN v.value = 1 THEN 1 ELSE 0 END), 0) as votes "
            "FROM tasks t JOIN votes v ON t.id = v.task_id "
            "WHERE t.status NOT IN ('done', 'archived') "
            "GROUP BY t.id ORDER BY votes DESC LIMIT 3"
        )
        return {
            "new_24h": new_24h,
            "overdue": overdue,
            "total_open": total_open,
            "in_progress": in_progress,
            "top_voted": [_record_to_dict(r) for r in top_voted],
        }


# ─── Bulk status update ───

async def bulk_update_status(task_ids: list[int], new_status: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "UPDATE tasks SET status = $1, updated_at = $2 WHERE id = ANY($3::int[])",
        new_status, ts, task_ids,
    )


# ═══════════════════════════════════════════════
# ─── Player Stats (Roblox) ───
# ═══════════════════════════════════════════════

async def upsert_player(roblox_id: int, roblox_username: str) -> dict:
    """Create or update player record. Returns player dict."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    row = await pool.fetchrow("""
        INSERT INTO players (roblox_id, roblox_username, first_seen, last_seen)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (roblox_id) DO UPDATE
            SET roblox_username = $2, last_seen = $4
        RETURNING *
    """, roblox_id, roblox_username, ts, ts)
    return _record_to_dict(row)


async def link_player_telegram(roblox_id: int, telegram_id: int) -> bool:
    """Link Roblox account to Telegram user. Returns True if successful."""
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE players SET telegram_id = $1 WHERE roblox_id = $2",
        telegram_id, roblox_id,
    )
    return result.split()[-1] != "0"


async def get_player_by_roblox(roblox_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM players WHERE roblox_id = $1", roblox_id)
    return _record_to_dict(row) if row else None


async def get_player_by_telegram(telegram_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM players WHERE telegram_id = $1", telegram_id)
    return _record_to_dict(row) if row else None


async def get_player_by_username(username: str) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM players WHERE LOWER(roblox_username) = LOWER($1)", username
    )
    return _record_to_dict(row) if row else None


async def upsert_player_stats(roblox_id: int, **kwargs) -> dict:
    """Update cumulative player stats. Pass only the fields to increment."""
    ts = now_msk().isoformat()
    pool = await get_pool()

    # Ensure player_stats row exists
    await pool.execute("""
        INSERT INTO player_stats (roblox_id, updated_at)
        VALUES ($1, $2)
        ON CONFLICT (roblox_id) DO NOTHING
    """, roblox_id, ts)

    # Build SET clause for increments
    allowed = {
        "playtime_minutes", "games_played", "games_won",
        "highest_wave", "total_waves",
        "enemies_killed", "bosses_killed", "towers_placed",
        "coins_earned", "coins_spent", "damage_dealt",
    }
    sets = ["updated_at = $1"]
    vals = [ts]
    idx = 2
    for key, val in kwargs.items():
        if key not in allowed or not isinstance(val, int):
            continue
        if key == "highest_wave":
            sets.append(f"highest_wave = GREATEST(highest_wave, ${idx})")
        else:
            sets.append(f"{key} = {key} + ${idx}")
        vals.append(val)
        idx += 1

    if idx == 2:
        # nothing to update
        row = await pool.fetchrow("SELECT * FROM player_stats WHERE roblox_id = $1", roblox_id)
        return _record_to_dict(row) if row else {}

    vals.append(roblox_id)
    query = f"UPDATE player_stats SET {', '.join(sets)} WHERE roblox_id = ${idx} RETURNING *"
    row = await pool.fetchrow(query, *vals)
    return _record_to_dict(row) if row else {}


async def get_player_stats(roblox_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM player_stats WHERE roblox_id = $1", roblox_id)
    return _record_to_dict(row) if row else None


async def add_player_match(roblox_id: int, map_name: str | None,
                           difficulty: str | None, wave_reached: int,
                           max_wave: int | None, won: bool,
                           enemies_killed: int, bosses_killed: int,
                           towers_placed: int, coins_earned: int,
                           damage_dealt: int, duration_seconds: int) -> int:
    """Record a single match result. Returns match id."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    match_id = await pool.fetchval("""
        INSERT INTO player_matches
            (roblox_id, map_name, difficulty, wave_reached, max_wave, won,
             enemies_killed, bosses_killed, towers_placed, coins_earned,
             damage_dealt, duration_seconds, played_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        RETURNING id
    """, roblox_id, map_name, difficulty, wave_reached, max_wave, won,
         enemies_killed, bosses_killed, towers_placed, coins_earned,
         damage_dealt, duration_seconds, ts)
    return match_id


# ─── Stats Cache ───

async def save_stats_cache(username: str, stats_json: str, place: str = "public"):
    pool = await get_pool()
    now = now_msk().isoformat()
    await pool.execute("""
        INSERT INTO stats_cache (username, stats_json, place, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (username) DO UPDATE SET stats_json=$2, place=$3, updated_at=$4
    """, username.lower(), stats_json, place, now)


async def get_stats_cache(username: str):
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM stats_cache WHERE username=$1", username.lower()
    )
async def get_player_matches(roblox_id: int, limit: int = 10) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM player_matches WHERE roblox_id = $1 ORDER BY played_at DESC LIMIT $2",
        roblox_id, limit,
    )
    return [_record_to_dict(r) for r in rows]


async def get_player_leaderboard(stat: str = "enemies_killed", limit: int = 10) -> list[dict]:
    """Get top players by a stat column."""
    allowed = {
        "playtime_minutes", "games_played", "games_won",
        "highest_wave", "total_waves",
        "enemies_killed", "bosses_killed", "towers_placed",
        "coins_earned", "damage_dealt",
    }
    if stat not in allowed:
        stat = "enemies_killed"
    pool = await get_pool()
    rows = await pool.fetch(f"""
        SELECT p.roblox_id, p.roblox_username, s.{stat} as value
        FROM player_stats s
        JOIN players p ON p.roblox_id = s.roblox_id
        ORDER BY s.{stat} DESC
        LIMIT $1
    """, limit)
    return [_record_to_dict(r) for r in rows]


async def process_match_report(roblox_id: int, roblox_username: str,
                               match_data: dict) -> int:
    """Full pipeline: upsert player, record match, update cumulative stats.
    Returns match id. Called by the API endpoint."""
    await upsert_player(roblox_id, roblox_username)

    match_id = await add_player_match(
        roblox_id=roblox_id,
        map_name=match_data.get("map"),
        difficulty=match_data.get("difficulty"),
        wave_reached=match_data.get("wave_reached", 0),
        max_wave=match_data.get("max_wave"),
        won=match_data.get("won", False),
        enemies_killed=match_data.get("enemies_killed", 0),
        bosses_killed=match_data.get("bosses_killed", 0),
        towers_placed=match_data.get("towers_placed", 0),
        coins_earned=match_data.get("coins_earned", 0),
        damage_dealt=match_data.get("damage_dealt", 0),
        duration_seconds=match_data.get("duration_seconds", 0),
    )

    await upsert_player_stats(
        roblox_id,
        games_played=1,
        games_won=1 if match_data.get("won") else 0,
        highest_wave=match_data.get("wave_reached", 0),
        total_waves=match_data.get("wave_reached", 0),
        enemies_killed=match_data.get("enemies_killed", 0),
        bosses_killed=match_data.get("bosses_killed", 0),
        towers_placed=match_data.get("towers_placed", 0),
        coins_earned=match_data.get("coins_earned", 0),
        damage_dealt=match_data.get("damage_dealt", 0),
        playtime_minutes=max(1, match_data.get("duration_seconds", 0) // 60),
    )

    return match_id
