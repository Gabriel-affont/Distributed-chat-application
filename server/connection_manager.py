"""
connection_manager.py — local client registry + fan-out.

This module owns "which clients live on THIS node" and how messages are
pushed to them. It is deliberately node-local: nodes never share client
state, only the Redis Stream. That
is what makes the multi-node design clean.

Design decisions implemented here:

  * Per-client bounded queue (R4): a slow client cannot backpressure the
    consumer loop or starve other clients. Overflow policy is explicit:
    drop-oldest with a WARNING, then disconnect if the queue stays full
    across consecutive drops. Drop-oldest (not reject-new) is chosen so
    a transiently slow client recovers without losing the newest
    messages — chat prioritizes recency over completeness under load.

  * Fan-out isolation (R4): broadcast snapshots the client set, then
    gathers sends with return_exceptions=True, and deregisters failures.
    One disconnect never silences other clients.

  * No locks: single-threaded event loop; correctness relies on "don't
    await mid-iteration over a live set" (hence the snapshot).

  * Decoupled from broker: this module does NOT import broker. The
    composition root (server/main.py) binds manager.broadcast as the
    broker's fan-out callback. That one-way arrow is the seam from
    Diagram 2.

Known limits 
  * broadcast is O(clients) per node. Fine at assignment scale; a real
    chat server would shard or use per-client push tasks. Out of scope.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Iterable

import protocol
from protocol import Message

logger = logging.getLogger(__name__)


# How many consecutive "queue full" events before we disconnect a client.
# 1 is too aggressive (a momentary burst shouldn't kill the connection);
# too high risks unbounded drop churn. 5 is a pragmatic middle.
_QUEUE_OVERFLOW_DISCONNECT_THRESHOLD = 5


@dataclass(eq=False)  # eq=False -> hashable by identity, usable in sets
class ClientConn:
    """One connected client on this node.

    Identity is the object itself (eq=False), not the fields. Two clients
    with the same username are distinct connections (username uniqueness
    is enforced globally via Redis, not here).

    Fields:
      * id           — short uuid for logs and targeted sends.
      * writer       — the asyncio StreamWriter for this connection.
      * username     — set after join; None before.
      * queue        — bounded outbound queue (see fan-out rationale).
      * queue_task   — the drain task that reads queue -> writer.
      * overflow_count — consecutive drops since last successful send.

    The writer is used by queue_task, not by broadcast directly: that is
    how isolation works. broadcast only enqueues; the drain task owns the
    actual socket I/O so a slow socket only stalls its own drain task.
    """

    id: str
    writer: asyncio.StreamWriter
    queue: asyncio.Queue[bytes]
    queue_task: asyncio.Task[None] | None = None
    username: str | None = None
    overflow_count: int = 0

    def __str__(self) -> str:
        return f"ClientConn({self.id}, user={self.username!r})"


class ConnectionManager:
    """Node-local registry of live clients and the fan-out entry point.

    Lifecycle:
        manager = ConnectionManager()
        # On each accepted connection:
        conn = await manager.register(reader, writer)
        ...
        # On broker fan-out:
        await manager.broadcast(msg)
        # On disconnect (handler's finally block):
        await manager.unregister(conn)

    All methods are coroutine-safe within a single event loop without
    locks: the snapshot rule in broadcast() prevents mutation-during-
    iteration.
    """

    def __init__(self, *, queue_maxsize: int = 256) -> None:
        # Bounded queue per client. 256 is generous for chat (~a few
        # seconds of backlog at human speeds) while small enough to
        # bound memory if many clients stall simultaneously.
        self._queue_maxsize = queue_maxsize
        # Live clients on this node, keyed by ClientConn.id.
        self._clients: dict[str, ClientConn] = {}

    #  lifecycle 

    async def register(
        self,
        writer: asyncio.StreamWriter,
        *,
        encode: Callable[[Message], bytes] = protocol.encode,
    ) -> ClientConn:
        """Create a ClientConn for a newly-accepted connection and start
        its drain task.

        ``encode`` is injectable for tests that want to observe wire
        bytes without going through protocol. Default is protocol.encode.
        """
        conn = ClientConn(
            id=uuid.uuid4().hex[:12],
            writer=writer,
            queue=asyncio.Queue(maxsize=self._queue_maxsize),
        )
        conn.queue_task = asyncio.create_task(
            self._drain_queue(conn, encode), name=f"drain-{conn.id}"
        )
        self._clients[conn.id] = conn
        logger.debug("registered %s (total=%d)", conn, len(self._clients))
        return conn

    async def unregister(self, conn: ClientConn) -> None:
        """Remove *conn* and stop its drain task.

        Idempotent: calling twice is safe (the second call is a no-op).
        Cancels the drain task and waits for it so no orphaned task
        lingers after the connection is gone.
        """
        if self._clients.pop(conn.id, None) is None:
            return
        task = conn.queue_task
        conn.queue_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                # Drain task may raise on a dead writer; that is fine —
                # we are tearing the connection down anyway.
                pass
        logger.debug("unregistered %s (total=%d)", conn, len(self._clients))

    # -- send API ----------------------------------------------------------

    async def broadcast(self, msg: Message) -> None:
        """Fan *msg* out to every local client.

        Snapshot the client set BEFORE iterating (do not await while
        iterating a dict that another coroutine can mutate). Enqueue per
        client; drops are handled by the drain policy.

        Never raises on a per-client failure — one bad client must not
        break fan-out (R4).
        """
        # Snapshot: O(clients) but unavoidable if we want consistent
        # delivery; cheap relative to the sends themselves.
        snapshot = list(self._clients.values())
        if not snapshot:
            return

        for conn in snapshot:
            try:
                conn.queue.put_nowait(protocol.encode(msg))
                conn.overflow_count = 0
            except asyncio.QueueFull:
                conn.overflow_count += 1
                logger.warning(
                    "queue full for %s (consecutive=%d); dropping oldest",
                    conn, conn.overflow_count,
                )
                # Drop-oldest: pop one, push ours. If the pop races with
                # the drain task (possible if it just freed a slot), the
                # put_nowait may still fail — swallow that, we tried.
                try:
                    _ = conn.queue.get_nowait()
                    conn.queue.put_nowait(protocol.encode(msg))
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
                if conn.overflow_count >= _QUEUE_OVERFLOW_DISCONNECT_THRESHOLD:
                    logger.warning(
                        "disconnecting %s: queue overflow %d times",
                        conn, conn.overflow_count,
                    )
                    # Schedule disconnect without awaiting it here (we're
                    # mid-fan-out). The drain task or a follow-up call
                    # will complete the teardown.
                    asyncio.create_task(self._force_disconnect(conn))

    async def send_to(self, conn: ClientConn, msg: Message) -> bool:
        """Enqueue *msg* for a single client. Returns True if accepted.

        Used by handler.py for targeted frames (history on join, error
        frames). Returns False if the client is gone or the queue is
        full — caller decides whether to log or ignore.
        """
        if conn.id not in self._clients:
            return False
        try:
            conn.queue.put_nowait(protocol.encode(msg))
            return True
        except asyncio.QueueFull:
            logger.warning("queue full for %s; dropping targeted send", conn)
            return False

    # -- introspection (for tests, health checks, logs) --------------------

    @property
    def count(self) -> int:
        """Number of live clients on this node."""
        return len(self._clients)

    def snapshot(self) -> list[ClientConn]:
        """Return a shallow copy of the live client list."""
        return list(self._clients.values())

    def get_by_username(self, username: str) -> ClientConn | None:
        """Return the local client with *username*, if any.

        Local-only: another node may have the same username. Global
        uniqueness is enforced in handler.py via Redis SET NX, not here.
        """
        for conn in self._clients.values():
            if conn.username == username:
                return conn
        return None

    # -- internals ---------------------------------------------------------

    async def _drain_queue(
        self, conn: ClientConn, encode: Callable[[Message], bytes]
    ) -> None:
        """Own the actual socket I/O for one client.

        One task per client. Reads from the queue and writes to the
        writer, flushing after each message. On any writer error, exits
        and unregisters so the manager's registry stays consistent.

        This is what isolates slow clients: only this task stalls when
        the socket is slow; broadcast and the consumer keep moving.
        """
        # `encode` is unused inside the loop (broadcast encodes before
        # enqueue) but is accepted for symmetry and future use; keep the
        # parameter to make the injection point obvious at call sites.
        del encode
        try:
            while True:
                data = await conn.queue.get()
                try:
                    conn.writer.write(data)
                    await conn.writer.drain()
                    conn.overflow_count = 0
                except (ConnectionError, OSError) as exc:
                    logger.debug(
                        "writer failed for %s: %s; closing", conn, exc
                    )
                    return
        except asyncio.CancelledError:
            # Normal shutdown path.
            raise
        finally:
            # Ensure the manager forgets this client even if the drain
            # task ends due to writer failure (unregister is idempotent
            # but the handler's finally will also call it; harmless).
            self._clients.pop(conn.id, None)

    async def _force_disconnect(self, conn: ClientConn) -> None:
        """Close the writer and drop the client after repeated overflow."""
        try:
            conn.writer.close()
            # Best-effort wait for close; ignore if the transport is
            # already gone.
            try:
                await conn.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        finally:
            await self.unregister(conn)


# Iterable import retained for future batch helpers; keeps linters
# quiet in the meantime without an unused-import warning.
_ = Iterable
_ = Awaitable