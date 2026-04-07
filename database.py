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
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=4, max_size=20)
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

        # Recreate stats_cache with composite PK (username, place)
        await conn.execute("DROP TABLE IF EXISTS stats_cache")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats_cache (
                username TEXT NOT NULL,
                place TEXT NOT NULL DEFAULT 'public',
                stats_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (username, place)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_roblox_links (
                telegram_id BIGINT PRIMARY KEY,
                roblox_username TEXT NOT NULL
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

        # Performance indexes for tasks and task_photos
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_user_id ON tasks(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_task_photos_task ON task_photos(task_id)",
        ]:
            try:
                await conn.execute(idx_sql)
            except Exception:
                pass

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news_subscribers (
                user_id BIGINT PRIMARY KEY,
                subscribed_at TEXT NOT NULL
            )
        """)

        # ─── Promo codes ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_codes (
                id SERIAL PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                reward_text TEXT NOT NULL,
                max_uses INTEGER NOT NULL DEFAULT 1,
                used_count INTEGER NOT NULL DEFAULT 0,
                expires_at TEXT,
                created_by BIGINT NOT NULL,
                telegram_id BIGINT,
                roblox_reward_data TEXT,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TEXT NOT NULL
            )
        """)
        try:
            await conn.execute("ALTER TABLE promo_codes ADD COLUMN place TEXT NOT NULL DEFAULT 'all'")
        except Exception:
            pass
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS promo_redemptions (
                id SERIAL PRIMARY KEY,
                code_id INTEGER NOT NULL,
                telegram_id BIGINT NOT NULL,
                roblox_username TEXT,
                redeemed_at TEXT NOT NULL,
                UNIQUE (code_id, telegram_id)
            )
        """)

        # ─── FAQ ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS faqs (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL DEFAULT 'general',
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_by BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # ─── Polls ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS polls (
                id SERIAL PRIMARY KEY,
                question TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                ends_at TEXT,
                created_by BIGINT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_options (
                id SERIAL PRIMARY KEY,
                poll_id INTEGER NOT NULL,
                option_text TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS poll_votes (
                poll_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                option_id INTEGER NOT NULL,
                voted_at TEXT NOT NULL,
                PRIMARY KEY (poll_id, user_id)
            )
        """)

        # ─── Server monitor ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS server_status_log (
                id SERIAL PRIMARY KEY,
                online_count INTEGER NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_server_status_time ON server_status_log(recorded_at)")
        except Exception:
            pass

        # ─── Weekly top ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS weekly_tops (
                id SERIAL PRIMARY KEY,
                week_start TEXT NOT NULL,
                stat_name TEXT NOT NULL,
                rankings_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (week_start, stat_name)
            )
        """)

        # ─── Giveaways ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                prize_text TEXT NOT NULL,
                prize_promo_reward TEXT,
                winner_count INTEGER NOT NULL DEFAULT 1,
                ends_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_by BIGINT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS giveaway_entries (
                giveaway_id INTEGER NOT NULL,
                telegram_id BIGINT NOT NULL,
                username TEXT,
                entered_at TEXT NOT NULL,
                is_winner BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (giveaway_id, telegram_id)
            )
        """)

        # ─── Pending rewards (for Roblox to pick up) ───
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_rewards (
                id SERIAL PRIMARY KEY,
                roblox_username TEXT NOT NULL,
                reward_json TEXT NOT NULL,
                reward_text TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'redeem',
                created_at TEXT NOT NULL,
                claimed BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)
        try:
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_rewards_user ON pending_rewards(roblox_username, claimed)")
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
    key = f"task:{task_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
    result = _record_to_dict(row) if row else None
    if result is not None:
        _cache.set(key, result, ttl=60)
    return result


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
    _cache.delete(f"task:{task_id}")
    _cache.delete("dashboard_stats")


async def update_task_priority(task_id: int, priority: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET priority = $1, updated_at = $2 WHERE id = $3",
                       priority, ts, task_id)
    _cache.delete(f"task:{task_id}")


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
    _cache.delete(f"banned:{user_id}")


async def is_banned(user_id: int) -> bool:
    key = f"banned:{user_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT banned_until FROM user_bans WHERE user_id = $1", user_id
    )
    if not row:
        _cache.set(key, False, ttl=10)
        return False
    banned_until = datetime.fromisoformat(row["banned_until"])
    if now_msk() >= banned_until:
        await pool.execute("DELETE FROM user_bans WHERE user_id = $1", user_id)
        _cache.set(key, False, ttl=10)
        return False
    _cache.set(key, True, ttl=10)
    return True


async def unban_user(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM user_bans WHERE user_id = $1", user_id)
    _cache.delete(f"banned:{user_id}")


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
    _cache.delete(f"task:{task_id}")
    _cache.delete("dashboard_stats")


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

async def toggle_vote(task_id: int, user_id: int) -> tuple[bool, int]:
    """Toggle like vote. Returns (voted, like_count)."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM votes WHERE task_id = $1 AND user_id = $2",
            task_id, user_id,
        )
        if row:
            if row["value"] == 1:
                await conn.execute("DELETE FROM votes WHERE task_id = $1 AND user_id = $2",
                                   task_id, user_id)
                voted = False
            else:
                await conn.execute("UPDATE votes SET value = 1, created_at = $1 WHERE task_id = $2 AND user_id = $3",
                                   ts, task_id, user_id)
                voted = True
        else:
            await conn.execute("INSERT INTO votes (task_id, user_id, created_at, value) VALUES ($1, $2, $3, 1)",
                               task_id, user_id, ts)
            voted = True
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM votes WHERE task_id = $1 AND value = 1", task_id
        )
    _cache.invalidate("feed:")
    _cache.invalidate("feed_tasks:")
    _cache.delete(f"votes:{task_id}")
    _cache.delete(f"has_voted:{task_id}:{user_id}")
    return voted, count


async def get_vote_count(task_id: int) -> int:
    """Count of likes (value=1)."""
    key = f"votes:{task_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    result = await pool.fetchval(
        "SELECT COUNT(*) FROM votes WHERE task_id = $1 AND value = 1", task_id
    )
    _cache.set(key, result, ttl=15)
    return result


async def has_voted(task_id: int, user_id: int) -> bool:
    key = f"has_voted:{task_id}:{user_id}"
    cached = _cache.get(key)
    if cached is not None:
        return cached
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM votes WHERE task_id = $1 AND user_id = $2 AND value = 1",
        task_id, user_id,
    )
    result = row is not None
    _cache.set(key, result, ttl=15)
    return result


async def toggle_dislike(task_id: int, user_id: int) -> tuple[bool, int]:
    """Toggle dislike. Returns (disliked, dislike_count)."""
    ts = now_msk().isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT value FROM votes WHERE task_id = $1 AND user_id = $2",
            task_id, user_id,
        )
        if row:
            if row["value"] == -1:
                await conn.execute("DELETE FROM votes WHERE task_id = $1 AND user_id = $2",
                                   task_id, user_id)
                voted = False
            else:
                await conn.execute("UPDATE votes SET value = -1, created_at = $1 WHERE task_id = $2 AND user_id = $3",
                                   ts, task_id, user_id)
                voted = True
        else:
            await conn.execute("INSERT INTO votes (task_id, user_id, created_at, value) VALUES ($1, $2, $3, -1)",
                               task_id, user_id, ts)
            voted = True
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM votes WHERE task_id = $1 AND value = -1", task_id
        )
    _cache.invalidate("feed:")
    _cache.invalidate("feed_tasks:")
    _cache.delete(f"votes:{task_id}")
    _cache.delete(f"has_voted:{task_id}:{user_id}")
    return voted, count


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
    _cache.delete(f"task:{task_id}")
    return bool(new_val)


# ─── Archive ───

async def archive_task(task_id: int):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET status = 'archived', updated_at = $1 WHERE id = $2",
                       ts, task_id)
    _cache.delete(f"task:{task_id}")
    _cache.delete("dashboard_stats")


async def restore_task(task_id: int):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute("UPDATE tasks SET status = 'new', updated_at = $1 WHERE id = $2",
                       ts, task_id)
    _cache.delete(f"task:{task_id}")
    _cache.delete("dashboard_stats")


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
    _cache.delete(f"task:{task_id}")


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
    _cache.delete(f"task:{task_id}")


# ─── Admin Assignment ───

async def assign_task(task_id: int, admin_id: int | None, admin_name: str | None):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "UPDATE tasks SET assigned_admin_id = $1, assigned_admin_name = $2, updated_at = $3 WHERE id = $4",
        admin_id, admin_name, ts, task_id,
    )
    _cache.delete(f"task:{task_id}")


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
    cached = _cache.get("dashboard_stats")
    if cached is not None:
        return cached
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
        result = {
            "new_24h": new_24h,
            "overdue": overdue,
            "total_open": total_open,
            "in_progress": in_progress,
            "top_voted": [_record_to_dict(r) for r in top_voted],
        }
        _cache.set("dashboard_stats", result, ttl=60)
        return result


# ─── Bulk status update ───

async def bulk_update_status(task_ids: list[int], new_status: str):
    ts = now_msk().isoformat()
    pool = await get_pool()
    await pool.execute(
        "UPDATE tasks SET status = $1, updated_at = $2 WHERE id = ANY($3::int[])",
        new_status, ts, task_ids,
    )
    for tid in task_ids:
        _cache.delete(f"task:{tid}")
    _cache.delete("dashboard_stats")


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


async def link_telegram_roblox(telegram_id: int, roblox_username: str):
    """Link Telegram user to Roblox username (no roblox_id needed)."""
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO telegram_roblox_links (telegram_id, roblox_username)
        VALUES ($1, $2)
        ON CONFLICT (telegram_id) DO UPDATE SET roblox_username = $2
    """, telegram_id, roblox_username)


async def get_linked_roblox_username(telegram_id: int) -> str | None:
    """Get linked Roblox username by Telegram ID."""
    pool = await get_pool()
    # First check direct link table
    row = await pool.fetchval(
        "SELECT roblox_username FROM telegram_roblox_links WHERE telegram_id = $1",
        telegram_id,
    )
    if row:
        return row
    # Fallback: check players table
    row2 = await pool.fetchval(
        "SELECT roblox_username FROM players WHERE telegram_id = $1",
        telegram_id,
    )
    return row2


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


# ─── News Subscribers ───

async def subscribe_news(user_id: int):
    pool = await get_pool()
    now = now_msk().isoformat()
    await pool.execute("""
        INSERT INTO news_subscribers (user_id, subscribed_at)
        VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING
    """, user_id, now)


async def unsubscribe_news(user_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM news_subscribers WHERE user_id = $1", user_id)


async def is_news_subscriber(user_id: int) -> bool:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT 1 FROM news_subscribers WHERE user_id = $1", user_id)
    return row is not None


async def get_all_subscribers() -> list[int]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT user_id FROM news_subscribers")
    return [r["user_id"] for r in rows]


# ─── Stats Cache ───

async def save_stats_cache(username: str, stats_json: str, place: str = "public"):
    pool = await get_pool()
    now = now_msk().isoformat()
    await pool.execute("""
        INSERT INTO stats_cache (username, place, stats_json, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (username, place) DO UPDATE SET stats_json=$3, updated_at=$4
    """, username.lower(), place, stats_json, now)


async def get_stats_cache(username: str, place: str = "public"):
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM stats_cache WHERE username=$1 AND place=$2",
        username.lower(), place,
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


# ═══════════════════════════════════════════════
# Promo Codes
# ═══════════════════════════════════════════════

async def create_promo_code(code: str, reward_text: str, max_uses: int,
                            expires_at: str | None, created_by: int,
                            telegram_id: int | None = None,
                            roblox_reward_data: str | None = None,
                            place: str = "all") -> int:
    pool = await get_pool()
    return await pool.fetchval("""
        INSERT INTO promo_codes (code, reward_text, max_uses, expires_at,
                                 created_by, telegram_id, roblox_reward_data, place, created_at)
        VALUES (UPPER($1), $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
    """, code, reward_text, max_uses, expires_at,
        created_by, telegram_id, roblox_reward_data, place, now_msk().isoformat())


async def get_promo_code(code: str) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM promo_codes WHERE UPPER(code) = UPPER($1)", code)
    return _record_to_dict(row) if row else None


async def redeem_promo_code(code: str, telegram_id: int,
                            roblox_username: str | None = None) -> dict:
    pool = await get_pool()
    promo = await get_promo_code(code)
    if not promo:
        return {"ok": False, "error": "not_found"}
    if not promo["active"]:
        return {"ok": False, "error": "inactive"}
    if promo["expires_at"] and promo["expires_at"] < now_msk().isoformat():
        return {"ok": False, "error": "expired"}
    if promo["used_count"] >= promo["max_uses"]:
        return {"ok": False, "error": "used_up"}
    if promo["telegram_id"] and promo["telegram_id"] != telegram_id:
        return {"ok": False, "error": "personal"}
    existing = await pool.fetchrow(
        "SELECT 1 FROM promo_redemptions WHERE code_id=$1 AND telegram_id=$2",
        promo["id"], telegram_id)
    if existing:
        return {"ok": False, "error": "already_redeemed"}
    # Atomic: only increment if still under limit
    result = await pool.execute(
        "UPDATE promo_codes SET used_count = used_count + 1 WHERE id = $1 AND used_count < max_uses",
        promo["id"])
    if not result.endswith("1"):
        return {"ok": False, "error": "used_up"}
    try:
        await pool.execute("""
            INSERT INTO promo_redemptions (code_id, telegram_id, roblox_username, redeemed_at)
            VALUES ($1, $2, $3, $4)
        """, promo["id"], telegram_id, roblox_username, now_msk().isoformat())
    except Exception:
        return {"ok": False, "error": "already_redeemed"}
    # Queue reward for Roblox if roblox_reward_data exists and user has linked account
    roblox_queued = False
    if promo.get("roblox_reward_data"):
        linked = await pool.fetchval(
            "SELECT roblox_username FROM telegram_roblox_links WHERE telegram_id = $1",
            telegram_id)
        if linked:
            await add_pending_reward(linked, promo["roblox_reward_data"],
                                     promo["reward_text"], source="redeem")
            roblox_queued = True
    return {"ok": True, "reward": promo["reward_text"], "roblox_queued": roblox_queued}


async def list_promo_codes(limit: int = 20, offset: int = 0) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM promo_codes ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit, offset)
    return [_record_to_dict(r) for r in rows]


async def get_public_active_codes(include_private: bool = False) -> list[dict]:
    """Get active public codes for display. include_private=True shows private place codes too."""
    pool = await get_pool()
    if include_private:
        place_filter = "AND place IN ('all', 'public', 'private')"
    else:
        place_filter = "AND place IN ('all', 'public')"
    rows = await pool.fetch(f"""
        SELECT code, reward_text, max_uses, used_count, expires_at, place
        FROM promo_codes
        WHERE active = TRUE AND telegram_id IS NULL
          AND (expires_at IS NULL OR expires_at > $1)
          AND used_count < max_uses
          {place_filter}
        ORDER BY created_at DESC
    """, now_msk().isoformat())
    return [_record_to_dict(r) for r in rows]


async def sync_roblox_codes(codes: dict, place: str = "all"):
    """Sync codes from Roblox validCodes table. Creates missing, deactivates removed."""
    pool = await get_pool()
    roblox_code_names = set()
    for code_name, reward in codes.items():
        upper = code_name.upper()
        roblox_code_names.add(upper)
        existing = await get_promo_code(code_name)
        if existing:
            if not existing["active"]:
                await pool.execute(
                    "UPDATE promo_codes SET active = TRUE WHERE id = $1", existing["id"])
            continue
        import json
        reward_text_parts = []
        if reward.get("Money"):
            reward_text_parts.append(f"{reward['Money']} монет")
        if reward.get("EventMoney"):
            reward_text_parts.append(f"{reward['EventMoney']} ивент монет")
        if reward.get("Skin"):
            reward_text_parts.append(f"скин {reward['Skin']}")
        if reward.get("Tower"):
            reward_text_parts.append(f"башня {reward['Tower']}")
        if reward.get("GiveAll"):
            reward_text_parts.append("все башни и скины")
        reward_text = ", ".join(reward_text_parts) or "награда"
        limit = reward.get("Limit", 1000000)
        await create_promo_code(
            code=code_name, reward_text=reward_text, max_uses=limit,
            expires_at=None, created_by=0,
            roblox_reward_data=json.dumps(reward, ensure_ascii=False),
            place=place,
        )
    # Deactivate codes from Roblox source that are no longer in the list
    rows = await pool.fetch(
        "SELECT id, code FROM promo_codes WHERE created_by = 0 AND active = TRUE")
    for r in rows:
        if r["code"] not in roblox_code_names:
            await pool.execute(
                "UPDATE promo_codes SET active = FALSE WHERE id = $1", r["id"])


async def count_promo_codes() -> int:
    pool = await get_pool()
    return await pool.fetchval("SELECT COUNT(*) FROM promo_codes") or 0


async def deactivate_promo_code(code_id: int):
    pool = await get_pool()
    await pool.execute("UPDATE promo_codes SET active = FALSE WHERE id = $1", code_id)


async def check_code_for_roblox(code: str, roblox_username: str) -> dict:
    """Validate a promo code for Roblox redemption."""
    promo = await get_promo_code(code)
    if not promo:
        return {"ok": False, "error": "Code not found"}
    if not promo["active"]:
        return {"ok": False, "error": "Code inactive"}
    if promo["expires_at"] and promo["expires_at"] < now_msk().isoformat():
        return {"ok": False, "error": "Code expired"}
    if promo["used_count"] >= promo["max_uses"]:
        return {"ok": False, "error": "Code fully used"}
    if promo["telegram_id"]:
        pool = await get_pool()
        linked = await pool.fetchval(
            "SELECT roblox_username FROM telegram_roblox_links WHERE telegram_id = $1",
            promo["telegram_id"])
        if not linked or linked.lower() != roblox_username.lower():
            return {"ok": False, "error": "Code belongs to another player"}
    pool = await get_pool()
    # Atomic update
    result = await pool.execute(
        "UPDATE promo_codes SET used_count = used_count + 1 WHERE id = $1 AND used_count < max_uses",
        promo["id"])
    if not result.endswith("1"):
        return {"ok": False, "error": "Code fully used"}
    import json
    reward = promo.get("roblox_reward_data")
    try:
        reward_data = json.loads(reward) if reward else {}
    except Exception:
        reward_data = {}
    return {"ok": True, "reward": reward_data, "reward_text": promo["reward_text"]}


# ═══════════════════════════════════════════════
# FAQ
# ═══════════════════════════════════════════════

async def create_faq(category: str, question: str, answer: str,
                     created_by: int, sort_order: int = 0) -> int:
    pool = await get_pool()
    ts = now_msk().isoformat()
    return await pool.fetchval("""
        INSERT INTO faqs (category, question, answer, sort_order, created_by, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $6) RETURNING id
    """, category, question, answer, sort_order, created_by, ts)


async def get_faq(faq_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM faqs WHERE id = $1", faq_id)
    return _record_to_dict(row) if row else None


async def update_faq(faq_id: int, **kwargs):
    pool = await get_pool()
    sets = []
    vals = []
    i = 1
    for k, v in kwargs.items():
        if v is not None:
            sets.append(f"{k} = ${i}")
            vals.append(v)
            i += 1
    sets.append(f"updated_at = ${i}")
    vals.append(now_msk().isoformat())
    vals.append(faq_id)
    await pool.execute(
        f"UPDATE faqs SET {', '.join(sets)} WHERE id = ${i + 1}", *vals)


async def delete_faq(faq_id: int):
    pool = await get_pool()
    await pool.execute("DELETE FROM faqs WHERE id = $1", faq_id)


async def get_faq_categories() -> list[str]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT DISTINCT category FROM faqs ORDER BY category")
    return [r["category"] for r in rows]


async def get_faqs_by_category(category: str) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM faqs WHERE category = $1 ORDER BY sort_order, id", category)
    return [_record_to_dict(r) for r in rows]


async def count_faqs() -> int:
    pool = await get_pool()
    return await pool.fetchval("SELECT COUNT(*) FROM faqs") or 0


# ═══════════════════════════════════════════════
# Polls
# ═══════════════════════════════════════════════

async def create_poll(question: str, options: list[str], ends_at: str | None,
                      created_by: int) -> int:
    pool = await get_pool()
    ts = now_msk().isoformat()
    poll_id = await pool.fetchval("""
        INSERT INTO polls (question, ends_at, created_by, created_at)
        VALUES ($1, $2, $3, $4) RETURNING id
    """, question, ends_at, created_by, ts)
    for i, opt in enumerate(options):
        await pool.execute("""
            INSERT INTO poll_options (poll_id, option_text, sort_order)
            VALUES ($1, $2, $3)
        """, poll_id, opt.strip(), i)
    return poll_id


async def get_poll(poll_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM polls WHERE id = $1", poll_id)
    return _record_to_dict(row) if row else None


async def get_poll_options(poll_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM poll_options WHERE poll_id = $1 ORDER BY sort_order", poll_id)
    return [_record_to_dict(r) for r in rows]


async def get_poll_results(poll_id: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT o.id, o.option_text, o.sort_order,
               COUNT(v.user_id) AS votes
        FROM poll_options o
        LEFT JOIN poll_votes v ON v.option_id = o.id AND v.poll_id = o.poll_id
        WHERE o.poll_id = $1
        GROUP BY o.id, o.option_text, o.sort_order
        ORDER BY o.sort_order
    """, poll_id)
    return [_record_to_dict(r) for r in rows]


async def vote_poll(poll_id: int, user_id: int, option_id: int):
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO poll_votes (poll_id, user_id, option_id, voted_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (poll_id, user_id) DO UPDATE SET option_id = $3, voted_at = $4
    """, poll_id, user_id, option_id, now_msk().isoformat())


async def get_user_poll_vote(poll_id: int, user_id: int) -> int | None:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT option_id FROM poll_votes WHERE poll_id=$1 AND user_id=$2",
        poll_id, user_id)


async def close_poll(poll_id: int):
    pool = await get_pool()
    await pool.execute("UPDATE polls SET status = 'closed' WHERE id = $1", poll_id)


async def get_active_polls() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM polls WHERE status = 'active' ORDER BY created_at DESC")
    return [_record_to_dict(r) for r in rows]


async def get_expiring_polls() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT * FROM polls
        WHERE status = 'active' AND ends_at IS NOT NULL AND ends_at <= $1
    """, now_msk().isoformat())
    return [_record_to_dict(r) for r in rows]


async def count_poll_votes(poll_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM poll_votes WHERE poll_id = $1", poll_id) or 0


# ═══════════════════════════════════════════════
# Server Monitor
# ═══════════════════════════════════════════════

async def log_server_status(online_count: int):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO server_status_log (online_count, recorded_at) VALUES ($1, $2)",
        online_count, now_msk().isoformat())


async def get_server_status_current() -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM server_status_log ORDER BY recorded_at DESC LIMIT 1")
    return _record_to_dict(row) if row else None


async def get_server_peak_today() -> int:
    pool = await get_pool()
    today = now_msk().strftime("%Y-%m-%d")
    val = await pool.fetchval(
        "SELECT MAX(online_count) FROM server_status_log WHERE recorded_at >= $1",
        today)
    return val or 0


async def get_server_downtime_minutes() -> int:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT online_count FROM server_status_log
        ORDER BY recorded_at DESC LIMIT 6
    """)
    if not rows:
        return 0
    count = 0
    for r in rows:
        if r["online_count"] == 0:
            count += 5
        else:
            break
    return count


# ═══════════════════════════════════════════════
# Weekly Top
# ═══════════════════════════════════════════════

async def compute_weekly_top(stat_name: str, limit: int = 10) -> list[dict]:
    import json as _json
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT username, stats_json FROM stats_cache WHERE place = 'public'")
    entries = []
    for r in rows:
        try:
            data = _json.loads(r["stats_json"])
            val = data.get(stat_name, 0)
            entries.append({"username": r["username"], "value": int(val)})
        except Exception:
            pass
    entries.sort(key=lambda x: x["value"], reverse=True)
    return entries[:limit]


async def save_weekly_top(week_start: str, stat_name: str, rankings: list[dict]):
    import json as _json
    pool = await get_pool()
    await pool.execute("""
        INSERT INTO weekly_tops (week_start, stat_name, rankings_json, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (week_start, stat_name) DO UPDATE SET rankings_json = $3, created_at = $4
    """, week_start, stat_name, _json.dumps(rankings, ensure_ascii=False),
        now_msk().isoformat())


async def get_weekly_top(week_start: str, stat_name: str) -> list[dict] | None:
    import json as _json
    pool = await get_pool()
    val = await pool.fetchval(
        "SELECT rankings_json FROM weekly_tops WHERE week_start=$1 AND stat_name=$2",
        week_start, stat_name)
    return _json.loads(val) if val else None


async def get_previous_weekly_top(stat_name: str) -> list[dict] | None:
    import json as _json
    from datetime import timedelta
    pool = await get_pool()
    now = now_msk()
    prev_monday = (now - timedelta(days=now.weekday() + 7)).strftime("%Y-%m-%d")
    val = await pool.fetchval(
        "SELECT rankings_json FROM weekly_tops WHERE week_start=$1 AND stat_name=$2",
        prev_monday, stat_name)
    return _json.loads(val) if val else None


# ═══════════════════════════════════════════════
# Giveaways
# ═══════════════════════════════════════════════

async def create_giveaway(title: str, description: str, prize_text: str,
                          prize_promo_reward: str | None, winner_count: int,
                          ends_at: str, created_by: int) -> int:
    pool = await get_pool()
    return await pool.fetchval("""
        INSERT INTO giveaways (title, description, prize_text, prize_promo_reward,
                               winner_count, ends_at, created_by, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
    """, title, description, prize_text, prize_promo_reward,
        winner_count, ends_at, created_by, now_msk().isoformat())


async def get_giveaway(giveaway_id: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM giveaways WHERE id = $1", giveaway_id)
    return _record_to_dict(row) if row else None


async def enter_giveaway(giveaway_id: int, telegram_id: int, username: str) -> bool:
    pool = await get_pool()
    try:
        await pool.execute("""
            INSERT INTO giveaway_entries (giveaway_id, telegram_id, username, entered_at)
            VALUES ($1, $2, $3, $4)
        """, giveaway_id, telegram_id, username, now_msk().isoformat())
        return True
    except Exception:
        return False


async def leave_giveaway(giveaway_id: int, telegram_id: int) -> bool:
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM giveaway_entries WHERE giveaway_id=$1 AND telegram_id=$2",
        giveaway_id, telegram_id)
    return result.split()[-1] != "0"


async def count_giveaway_entries(giveaway_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT COUNT(*) FROM giveaway_entries WHERE giveaway_id = $1",
        giveaway_id) or 0


async def is_giveaway_entered(giveaway_id: int, telegram_id: int) -> bool:
    pool = await get_pool()
    return bool(await pool.fetchval(
        "SELECT 1 FROM giveaway_entries WHERE giveaway_id=$1 AND telegram_id=$2",
        giveaway_id, telegram_id))


async def pick_giveaway_winners(giveaway_id: int, count: int) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT * FROM giveaway_entries
        WHERE giveaway_id = $1
        ORDER BY RANDOM() LIMIT $2
    """, giveaway_id, count)
    winners = [_record_to_dict(r) for r in rows]
    for w in winners:
        await pool.execute(
            "UPDATE giveaway_entries SET is_winner = TRUE WHERE giveaway_id=$1 AND telegram_id=$2",
            giveaway_id, w["telegram_id"])
    await pool.execute(
        "UPDATE giveaways SET status = 'ended' WHERE id = $1", giveaway_id)
    return winners


async def get_active_giveaways() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM giveaways WHERE status = 'active' ORDER BY ends_at")
    return [_record_to_dict(r) for r in rows]


async def get_ending_giveaways() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT * FROM giveaways
        WHERE status = 'active' AND ends_at <= $1
    """, now_msk().isoformat())
    return [_record_to_dict(r) for r in rows]


async def generate_winner_code(telegram_id: int, reward_text: str,
                               roblox_reward_data: str, created_by: int) -> str:
    import secrets
    code = f"WIN-{secrets.token_hex(4).upper()}"
    while await get_promo_code(code):
        code = f"WIN-{secrets.token_hex(4).upper()}"
    await create_promo_code(
        code=code, reward_text=reward_text, max_uses=1,
        expires_at=None, created_by=created_by,
        telegram_id=telegram_id, roblox_reward_data=roblox_reward_data,
    )
    return code


# ═══════════════════════════════════════════════
# Pending Rewards (Roblox picks up)
# ═══════════════════════════════════════════════

async def add_pending_reward(roblox_username: str, reward_json: str,
                             reward_text: str, source: str = "redeem") -> int:
    pool = await get_pool()
    return await pool.fetchval("""
        INSERT INTO pending_rewards (roblox_username, reward_json, reward_text, source, created_at)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
    """, roblox_username.lower(), reward_json, reward_text, source, now_msk().isoformat())


async def get_pending_rewards(roblox_username: str) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT * FROM pending_rewards
        WHERE roblox_username = $1 AND claimed = FALSE
        ORDER BY created_at
    """, roblox_username.lower())
    return [_record_to_dict(r) for r in rows]


async def claim_pending_rewards(reward_ids: list[int]):
    pool = await get_pool()
    if reward_ids:
        await pool.execute(
            "UPDATE pending_rewards SET claimed = TRUE WHERE id = ANY($1::int[])",
            reward_ids)
