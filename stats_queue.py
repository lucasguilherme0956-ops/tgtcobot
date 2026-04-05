"""Shared state for Roblox stats polling system (Roblox <-> Telegram)."""
import asyncio

# pending_stats: list of {"username": str} waiting to be polled by Roblox
pending_stats: list[dict] = []

# stats_waiters: username_lower -> list of {chat_id, message_id, event}
stats_waiters: dict[str, list[dict]] = {}
