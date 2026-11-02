"""
test_connection_manager.py — correctness tests for the client registry
and fan-out.

These pin the two properties that matter most (R4):

  * Slow-client isolation: one full queue does not stall fan-out or
    other clients.
  * Snapshot safety: broadcast does not crash or skip clients when the
    set is mutated during fan-out (concurrent unregister).

Also verifies basic lifecycle (register/unregister), targeted sends,
and the overflow disconnect policy.

Uses a FakeWriter to avoid needing real sockets; the manager does not
inspect writers beyond .write/.drain/.close/.wait_closed, so a stub is
enough and keeps tests fast and deterministic.
"""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

import protocol
from protocol import make_chat, make_error
from server.connection_manager import (
    _QUEUE_OVERFLOW_DISCONNECT_THRESHOLD,
    ClientConn,
    ConnectionManager,
)


# ---------------------------------------------------------------------------
# FakeWriter — the minimum surface the manager uses
# ---------------------------------------------------------------------------

class FakeWriter:
    """In-memory stand-in for asyncio.StreamWriter.

    Records every write and can be made to block in drain() to simulate a
    slow client, or raise on write to simulate disconnect.
    """

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.closed = False
        self.fail_on_write = False
        # If set, drain() waits on this event before returning.
        self.block_drain = asyncio.Event()
        self.block_drain.set()  # default: don't block

    def write(self, data: bytes) -> None:
        if self.fail_on_write:
            raise ConnectionError("simulated write failure")
        self.chunks.append(data)

    async def drain(self) -> None:
        await self.block_drain.wait()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def manager() -> ConnectionManager:
    return ConnectionManager(queue_maxsize=4)  # small queue for tests


def _frames(writer: FakeWriter) -> list[protocol.Message]:
    """Decode every chunk the writer has seen into a list of Messages."""
    return [protocol.decode(chunk) for chunk in writer.chunks]


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    async def test_register_increments_count(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        conn = await manager.register(w)
        try:
            assert manager.count == 1
            assert conn.username is None
        finally:
            await manager.unregister(conn)

    async def test_unregister_decrements_count(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        conn = await manager.register(w)
        await manager.unregister(conn)
        assert manager.count == 0

    async def test_unregister_is_idempotent(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        conn = await manager.register(w)
        await manager.unregister(conn)
        # Second call must not raise.
        await manager.unregister(conn)
        assert manager.count == 0

    async def test_unregister_cancels_drain_task(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        conn = await manager.register(w)
        task = conn.queue_task
        assert task is not None and not task.done()
        await manager.unregister(conn)
        assert task.done() or task.cancelled()

    async def test_snapshot_reflects_live_clients(
        self, manager: ConnectionManager
    ) -> None:
        w1, w2 = FakeWriter(), FakeWriter()
        c1 = await manager.register(w1)
        c2 = await manager.register(w2)
        try:
            ids = {c.id for c in manager.snapshot()}
            assert ids == {c1.id, c2.id}
        finally:
            await manager.unregister(c1)
            await manager.unregister(c2)


# ---------------------------------------------------------------------------
# Broadcast
# ---------------------------------------------------------------------------

class TestBroadcast:
    async def test_broadcast_delivers_to_all(
        self, manager: ConnectionManager
    ) -> None:
        w1, w2 = FakeWriter(), FakeWriter()
        c1 = await manager.register(w1)
        c2 = await manager.register(w2)
        try:
            await manager.broadcast(make_chat("alice", "hello"))
            # Drain tasks are async; wait until both writers have seen it.
            await _wait_until(lambda: len(w1.chunks) == 1 and len(w2.chunks) == 1)
            assert _frames(w1)[0].content == "hello"
            assert _frames(w2)[0].content == "hello"
        finally:
            await manager.unregister(c1)
            await manager.unregister(c2)

    async def test_broadcast_with_no_clients_does_not_raise(
        self, manager: ConnectionManager
    ) -> None:
        await manager.broadcast(make_chat("alice", "hello"))  # must not raise

    async def test_broadcast_isolates_slow_client(
        self, manager: ConnectionManager
    ) -> None:
        # w_slow blocks in drain; w_fast does not. Broadcast must still
        # enqueue to both, and the fast writer must see its message
        # promptly even though the slow one is stalled.
        w_slow, w_fast = FakeWriter(), FakeWriter()
        w_slow.block_drain.clear()  # make slow drain block
        c_slow = await manager.register(w_slow)
        c_fast = await manager.register(w_fast)
        try:
            await manager.broadcast(make_chat("alice", "one"))
            # Fast sees the message; slow's drain is blocked but the
            # broadcast call itself returned.
            await _wait_until(lambda: len(w_fast.chunks) == 1)
            assert _frames(w_fast)[0].content == "one"
            # Slow's queue has the message but the writer hasn't drained.
            assert c_slow.queue.qsize() <= 1
        finally:
            w_slow.block_drain.set()  # unblock so teardown finishes
            await manager.unregister(c_slow)
            await manager.unregister(c_fast)

    async def test_broadcast_removes_conn_when_writer_fails(
        self, manager: ConnectionManager
    ) -> None:
        # A writer that raises on write: the drain task exits, and the
        # manager's registry drops the client (defensive cleanup).
        w = FakeWriter()
        w.fail_on_write = True
        conn = await manager.register(w)
        await manager.broadcast(make_chat("alice", "hi"))
        # Give the drain task a chance to hit the failure and exit.
        await _wait_until(lambda: manager.count == 0, timeout=2.0)


# ---------------------------------------------------------------------------
# Targeted send
# ---------------------------------------------------------------------------

class TestSendTo:
    async def test_send_to_delivers_to_target_only(
        self, manager: ConnectionManager
    ) -> None:
        w1, w2 = FakeWriter(), FakeWriter()
        c1 = await manager.register(w1)
        c2 = await manager.register(w2)
        try:
            ok = await manager.send_to(c1, make_error("test", "only for c1"))
            assert ok is True
            await _wait_until(lambda: len(w1.chunks) == 1)
            assert _frames(w1)[0].code == "test"
            assert len(w2.chunks) == 0
        finally:
            await manager.unregister(c1)
            await manager.unregister(c2)

    async def test_send_to_unknown_conn_returns_false(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        conn = ClientConn(
            id="ghost",
            writer=w,  # type: ignore[arg-type]
            queue=asyncio.Queue(maxsize=4),
        )
        assert await manager.send_to(conn, make_chat("alice", "x")) is False


# ---------------------------------------------------------------------------
# Overflow policy
# ---------------------------------------------------------------------------

class TestOverflow:
    async def test_overflow_drops_oldest_and_keeps_newest(
        self, manager: ConnectionManager
    ) -> None:
        # Block the drain so the queue fills. Send queue_maxsize + 2
        # messages; the newest ones must survive (drop-oldest policy).
        w = FakeWriter()
        w.block_drain.clear()
        conn = await manager.register(w)
        try:
            total = manager._queue_maxsize + 2  # type: ignore[attr-defined]
            for i in range(total):
                await manager.broadcast(make_chat("alice", f"m{i}"))
            # The last message we sent must be in the queue.
            queued = []
            while not conn.queue.empty():
                queued.append(protocol.decode(conn.queue.get_nowait()))
            assert any(m.content == f"m{total - 1}" for m in queued)
            # The very first message must NOT be (it was dropped).
            assert all(m.content != "m0" for m in queued)
        finally:
            w.block_drain.set()
            await manager.unregister(conn)

    async def test_overflow_disconnects_after_threshold(
        self, manager: ConnectionManager
    ) -> None:
        w = FakeWriter()
        w.block_drain.clear()
        conn = await manager.register(w)
        try:
            # Overflow enough times to hit the disconnect threshold.
            # Each broadcast pushes 1 past the bound; threshold is N so
            # we need > N overflows total.
            for i in range(_QUEUE_OVERFLOW_DISCONNECT_THRESHOLD + 2):
                for _ in range(manager._queue_maxsize + 1):  # type: ignore[attr-defined]
                    await manager.broadcast(make_chat("alice", f"m{i}"))
            # Let the scheduled disconnect task run.
            await _wait_until(lambda: manager.count == 0, timeout=2.0)
            assert w.closed is True
        finally:
            w.block_drain.set()


# ---------------------------------------------------------------------------
# Snapshot safety under concurrent mutation
# ---------------------------------------------------------------------------

class TestConcurrentMutation:
    async def test_broadcast_survives_unregister_during_fanout(
        self, manager: ConnectionManager
    ) -> None:
        # Two clients; broadcast iterates a snapshot. Concurrently
        # unregister one. The other must still receive the message, and
        # broadcast must not raise.
        w1, w2 = FakeWriter(), FakeWriter()
        c1 = await manager.register(w1)
        c2 = await manager.register(w2)
        try:
            async def unregister_mid_flight() -> None:
                # Small yield to let broadcast start iterating.
                await asyncio.sleep(0)
                await manager.unregister(c2)

            await asyncio.gather(
                manager.broadcast(make_chat("alice", "hi")),
                unregister_mid_flight(),
            )
            # c1 still received the message.
            await _wait_until(lambda: len(w1.chunks) == 1)
        finally:
            await manager.unregister(c1)
            await manager.unregister(c2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _wait_until(predicate, timeout: float = 2.0) -> None:
    """Poll *predicate* until it returns True or timeout elapses."""
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(_poll(), timeout=timeout)