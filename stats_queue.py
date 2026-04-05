"""Shared state for Roblox stats polling system (Roblox <-> Telegram)."""
import asyncio

# pending_stats: place -> list of {"username": str}
pending_stats: dict[str, list[dict]] = {
    "public": [],
    "private": [],
}

# stats_waiters: username_lower -> list of {chat_id, message_id, event}
stats_waiters: dict[str, list[dict]] = {}
