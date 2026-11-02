"""
config.py — env-driven configuration (leaf dependency).

All tunables live here so the same code runs node1 and node2 
and tests can point at an isolated Redis DB without code changes. This is
a LEAF module: it imports only the standard library and nothing from the
project, so it can be imported everywhere without creating cycles.

Every value can be overridden via environment variables. The defaults are
chosen for local two-node development on loopback.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final



# Helpers


def _env_str(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value else default


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        # Fail fast at import: a bad PORT is a deployment bug, not a
        # runtime condition to handle deep in the server.
        raise ValueError(f"env {key}={raw!r} is not an int") from exc



# Node identity and network


# Which node this process is. Two nodes: "node1" and "node2".
NODE_ID: Final[str] = _env_str("CHAT_NODE_ID", "node1")

# Bind address and port for the asyncio TCP server.
HOST: Final[str] = _env_str("CHAT_HOST", "127.0.0.1")
PORT: Final[int] = _env_int("CHAT_PORT", 9001 if NODE_ID == "node1" else 9002)

# Static peer list for client failover (Decision 5, Option A).
# Order matters: the client tries these in sequence on connect failure.
NODES: Final[list[tuple[str, int]]] = [
    ("127.0.0.1", 9001),
    ("127.0.0.1", 9002),
]

# Client connect timeout per node attempt, and backoff between full passes.
CONNECT_TIMEOUT: Final[float] = float(_env_str("CHAT_CONNECT_TIMEOUT", "2.0"))
FAILOVER_BACKOFF: Final[float] = float(_env_str("CHAT_FAILOVER_BACKOFF", "1.0"))


# Redis

REDIS_URL: Final[str] = _env_str("REDIS_URL", "redis://localhost:6379/0")

# Stream and key names. Overridable so tests use chat:test:* and never
# touch dev/demo data.
STREAM_KEY: Final[str] = _env_str("CHAT_STREAM_KEY", "chat:stream")
USER_KEY_PREFIX: Final[str] = _env_str("CHAT_USER_PREFIX", "chat:user:")

# Approximate stream trimming (Item 4). Keeps ~1000 messages; the history
# window (50) has 20x headroom.
STREAM_MAXLEN: Final[int] = _env_int("CHAT_STREAM_MAXLEN", 1000)

# Username reservation TTL (Item 2). Safety net for crashed clients.
USERNAME_TTL_SECONDS: Final[int] = _env_int("CHAT_USERNAME_TTL", 3600)


# Server behavior


# How many history messages to send to a joining client (Item 3).
HISTORY_COUNT: Final[int] = _env_int("CHAT_HISTORY_COUNT", 50)

# Per-client outbound queue size (R4). Bounded to prevent a slow client
# from consuming unbounded memory.
CLIENT_QUEUE_MAXSIZE: Final[int] = _env_int("CHAT_CLIENT_QUEUE", 256)

# How long the stream consumer blocks on XREAD before looping (ms).
CONSUMER_BLOCK_MS: Final[int] = _env_int("CHAT_CONSUMER_BLOCK_MS", 1000)



# Filesystem (cursor persistence)


# Directory where each node writes its last-seen stream ID. Created on
# first write; gitignored. Overridable so tests use a temp dir.
DATA_DIR: Final[Path] = Path(_env_str("CHAT_DATA_DIR", "data"))