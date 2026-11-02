"""
failover.py — ordered connect retry with backoff .

This module implements the client-driven failover policy: on connect
failure, try each node in the static list (from config.NODES), wrap
around, back off between full passes, and stop after a bounded number
of attempts. On success, return the live (reader, writer) pair.

Design decisions :

  * Strategy pattern seam: `connect_with_failover` is a pure async
    function. Future dynamic discovery replaces this one function
    without touching the I/O loops in client/main.py.

  * Per-attempt timeout: asyncio.wait_for(open_connection, timeout) so
    a black-holed node (accepts TCP but never responds) does not hang
    the client. 2s default per attempt is short enough to feel snappy,
    long enough for localhost and slow-but-alive peers.

  * Backoff between passes: when every node in the list has failed
    once, sleep `backoff` seconds before retrying. Prevents a hot loop
    against a down cluster while still retrying quickly when recovery
    is plausible (1s default).

  * No reconnect logic here: this function is called once at startup,
    and again by the read loop when it detects a mid-session
    disconnect. The retry policy is identical in both cases; that is
    what makes mid-session failover "just work" without duplicated code.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("chat.client.failover")


async def connect_with_failover(
    nodes: list[tuple[str, int]],
    *,
    connect_timeout: float = 2.0,
    backoff: float = 1.0,
    max_passes: int | None = None,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Try each (host, port) in order until one connects.

    Parameters:
      nodes            — static peer list (config.NODES).
      connect_timeout  — seconds per attempt (asyncio.wait_for).
      backoff          — seconds between full passes.
      max_passes       — None means retry forever; an int bounds the
                         number of full passes (useful for tests).

    Returns (reader, writer) for the first successful connection.

    Raises asyncio.CancelledError if the caller cancels (used for prompt
    shutdown of the connect attempt), and ConnectionRefusedError (or the
    last underlying exception) if max_passes is exhausted.
    """
    if not nodes:
        raise ValueError("nodes list is empty")

    pass_num = 0
    last_error: BaseException | None = None

    while max_passes is None or pass_num < max_passes:
        pass_num += 1
        for host, port in nodes:
            try:
                # Bound each attempt: a node that accepts the TCP
                # handshake but never speaks WebSocket/NDJSON would
                # otherwise hang us here.
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=connect_timeout,
                )
                logger.info("connected to %s:%d (pass %d)", host, port, pass_num)
                return reader, writer
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "connect to %s:%d failed: %s", host, port, exc
                )
                continue

        if max_passes is not None and pass_num >= max_passes:
            break
        logger.info("all nodes failed pass %d; backing off %.1fs", pass_num, backoff)
        await asyncio.sleep(backoff)

    # Exhausted passes.
    if last_error is not None:
        raise last_error
    raise ConnectionRefusedError("no node could be reached")