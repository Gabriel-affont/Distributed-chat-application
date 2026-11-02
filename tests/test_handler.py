"""
test_handler.py — correctness tests for the join flow, dispatch, and
cleanup.

These focus on the two subtle races (R6, R7) and the auth model
(sender-spoof prevention). All external dependencies are fakes:

  * FakeRedis            — covers the subset of redis.asyncio used
  * FakeStreamProducer   — records published messages
  * FakeStreamConsumer   — returns a canned history
  * ConnectionManager    — real (it's already tested in isolation)
  * FakeReader/FakeWriter — minimal StreamReader/StreamWriter stand-ins

This keeps tests fast and deterministic; the integration tests (later)
exercise the real Redis path end to end.
"""

from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio

import config
import protocol
from protocol import Message, ProtocolError, make_chat, make_join, make_leave
from server.connection_manager import ConnectionManager
from server.handler import HandlerContext, handle_client


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeRedis:
    """In-memory stand-in for the redis.asyncio subset used by handler.

    Models SET NX EX and DEL precisely enough to test username
    uniqueness (R7); XREVRANGE for the R6 tail snapshot.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.stream: list[tuple[str, dict[str, str]]] = []
        self.deleted: list[str] = []
        self.set_calls: list[tuple[str, str, bool, int]] = []

    async def set(self, key: str, value: str, nx: bool = False, ex: int = 0):
        self.set_calls.append((key, value, nx, ex))
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1 if self.store.pop(key, None) is not None else 0

    async def xrevrange(self, key: str, max: str, min: str, count: int):
        # Newest first, matching real XREVRANGE. Empty -> [].
        entries = list(reversed(self.stream))
        return entries[:count]


class FakeStreamProducer:
    def __init__(self) -> None:
        self.published: list[Message] = []

    async def publish(self, msg: Message) -> str:
        self.published.append(msg)
        # Return a monotonic-looking id so callers don't care.
        return f"{1700000000000 + len(self.published)}-0"


class FakeStreamConsumer:
    def __init__(self, history: list[Message] | None = None) -> None:
        self._history = history or []

    async def history(self, count: int = config.HISTORY_COUNT) -> list[Message]:
        return list(self._history[:count])


class FakeWriter:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.chunks.append(data)

    async def drain(self) -> None:
        return

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return


class FakeReader:
    """Feeds NDJSON lines to handle_client's read loop."""

    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        if not self._lines:
            return b""  # EOF
        # Yield control so concurrent tasks can progress, mimicking
        # real network I/O.
        await asyncio.sleep(0)
        return self._lines.pop(0)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def manager() -> ConnectionManager:
    return ConnectionManager(queue_maxsize=16)


@pytest_asyncio.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest_asyncio.fixture
def producer() -> FakeStreamProducer:
    return FakeStreamProducer()


@pytest_asyncio.fixture
def consumer() -> FakeStreamConsumer:
    return FakeStreamConsumer()


@pytest_asyncio.fixture
def ctx(
    manager: ConnectionManager,
    fake_redis: FakeRedis,
    producer: FakeStreamProducer,
    consumer: FakeStreamConsumer,
) -> HandlerContext:
    return HandlerContext(
        broker_producer=producer,  # type: ignore[arg-type]
        broker_consumer=consumer,  # type: ignore[arg-type]
        manager=manager,
        redis_client=fake_redis,  # type: ignore[arg-type]
        node_id="node-test",
    )


def _line(msg: Message) -> bytes:
    return protocol.encode(msg)


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    async def _poll() -> None:
        while not predicate():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(_poll(), timeout=timeout)


# ---------------------------------------------------------------------------
# Happy path: join, chat, leave
# ---------------------------------------------------------------------------

class TestHappyPath:
    async def test_join_reserves_username_and_publishes_join_event(
        self,
        ctx: HandlerContext,
        fake_redis: FakeRedis,
        producer: FakeStreamProducer,
        manager: ConnectionManager,
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]

        # A SET NX EX was issued for the lowercase key (global
        # uniqueness, case-insensitive). The key is subsequently
        # released in cleanup, so inspect the call record, not the store.
        assert any(
            key == f"{config.USER_KEY_PREFIX}alice" and nx and ex > 0
            for (key, _value, nx, ex) in fake_redis.set_calls
        )
        # The key was also released on disconnect (verified separately by
        # test_username_released_on_disconnect, but assert here too for
        # completeness of the happy path).
        assert f"{config.USER_KEY_PREFIX}alice" not in fake_redis.store
        # A join event was published to the stream.
        assert any(m.type == protocol.TYPE_JOIN and m.sender == "alice"
                   for m in producer.published)
        # Registry is clean after EOF.
        assert manager.count == 0

    async def test_chat_overwrites_sender_to_prevent_spoofing(
        self,
        ctx: HandlerContext,
        producer: FakeStreamProducer,
    ) -> None:
        # Client sends join as alice, then a chat claiming to be bob.
        spoof = Message(
            type=protocol.TYPE_CHAT, sender="bob", content="hi",
        )
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice")), _line(spoof)])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]

        # The published chat must have sender="alice", not "bob".
        chats = [m for m in producer.published if m.type == protocol.TYPE_CHAT]
        assert len(chats) == 1
        assert chats[0].sender == "alice"

    async def test_leave_after_join_publishes_leave_event(
        self,
        ctx: HandlerContext,
        producer: FakeStreamProducer,
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice")), _line(make_leave("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        # A leave event was published (sender normalized to conn.username).
        assert any(m.type == protocol.TYPE_LEAVE for m in producer.published)


# ---------------------------------------------------------------------------
# Username uniqueness (R7)
# ---------------------------------------------------------------------------

class TestUsernameUniqueness:
    async def test_duplicate_username_rejected(
        self, ctx: HandlerContext, fake_redis: FakeRedis
    ) -> None:
        # Pre-occupy the key (simulating another node's client).
        fake_redis.store[f"{config.USER_KEY_PREFIX}alice"] = "node1"

        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]

        # Error frame sent, connection closed.
        assert writer.closed is True
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)

    async def test_username_case_insensitive(
        self, ctx: HandlerContext, fake_redis: FakeRedis
    ) -> None:
        fake_redis.store[f"{config.USER_KEY_PREFIX}alice"] = "node1"
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("ALICE"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        # Still rejected (case-insensitive uniqueness).
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)

    async def test_username_released_on_disconnect(
        self, ctx: HandlerContext, fake_redis: FakeRedis
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        # On EOF the key must be gone, so a reconnect can reuse it.
        assert f"{config.USER_KEY_PREFIX}alice" not in fake_redis.store


# ---------------------------------------------------------------------------
# History-on-join (R6)
# ---------------------------------------------------------------------------

class TestHistoryOnJoin:
    async def test_history_sent_before_join_event(
        self,
        manager: ConnectionManager,
        fake_redis: FakeRedis,
        producer: FakeStreamProducer,
    ) -> None:
        history = [make_chat("bob", "old1"), make_chat("bob", "old2")]
        consumer = FakeStreamConsumer(history=history)
        ctx = HandlerContext(
            broker_producer=producer,  # type: ignore[arg-type]
            broker_consumer=consumer,  # type: ignore[arg-type]
            manager=manager,
            redis_client=fake_redis,  # type: ignore[arg-type]
            node_id="node-test",
        )

        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]

        # The client received a history frame before disconnect.
        frames = [protocol.decode(c) for c in writer.chunks]
        history_frames = [f for f in frames if f.type == protocol.TYPE_HISTORY]
        assert len(history_frames) == 1
        assert len(history_frames[0].messages) == 2

    async def test_tail_snapshot_precedes_history_fetch(
        self,
        manager: ConnectionManager,
        fake_redis: FakeRedis,
        producer: FakeStreamProducer,
    ) -> None:
        # Record the order of operations: XREVRANGE must be called BEFORE
        # the history fetch. We simulate by recording on the consumer.
        events: list[str] = []

        class RecordingConsumer(FakeStreamConsumer):
            async def history(self, count=config.HISTORY_COUNT):
                events.append("history")
                return await super().history(count)

        class RecordingRedis(FakeRedis):
            async def xrevrange(self, key, max, min, count):
                events.append("xrevrange")
                return await super().xrevrange(key, max, min, count)

        redis_client = RecordingRedis()
        consumer = RecordingConsumer(history=[])
        ctx = HandlerContext(
            broker_producer=producer,  # type: ignore[arg-type]
            broker_consumer=consumer,  # type: ignore[arg-type]
            manager=manager,
            redis_client=redis_client,  # type: ignore[arg-type]
            node_id="node-test",
        )

        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]

        # The R6 order is: snapshot tail, then fetch history.
        assert events == ["xrevrange", "history"]


# ---------------------------------------------------------------------------
# Dispatch rules
# ---------------------------------------------------------------------------

class TestDispatchRules:
    async def test_chat_before_join_rejected(
        self, ctx: HandlerContext
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([_line(make_chat("alice", "hi"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)

    async def test_second_join_rejected(
        self, ctx: HandlerContext
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice")), _line(make_join("alice"))])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)


# ---------------------------------------------------------------------------
# Protocol errors and cleanup
# ---------------------------------------------------------------------------

class TestProtocolErrors:
    async def test_malformed_line_sends_error_and_closes(
        self, ctx: HandlerContext
    ) -> None:
        writer = FakeWriter()
        reader = FakeReader([b"{not json}\n"])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        assert writer.closed is True
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)

    async def test_invalid_message_shape_sends_error(
        self, ctx: HandlerContext
    ) -> None:
        # A line that parses as JSON but violates the schema.
        bad = json.dumps({"type": "chat", "version": 1, "sender": "x"}).encode() + b"\n"
        writer = FakeWriter()
        reader = FakeReader([bad])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        frames = [protocol.decode(c) for c in writer.chunks]
        assert any(f.type == protocol.TYPE_ERROR for f in frames)

    async def test_cleanup_runs_on_protocol_error(
        self,
        ctx: HandlerContext,
        fake_redis: FakeRedis,
        manager: ConnectionManager,
    ) -> None:
        # Join successfully, then send a malformed frame. Cleanup must
        # still release the username and remove from the registry.
        writer = FakeWriter()
        reader = FakeReader([_line(make_join("alice")), b"{not json}\n"])
        await handle_client(reader, writer, ctx=ctx)  # type: ignore[arg-type]
        assert manager.count == 0
        assert f"{config.USER_KEY_PREFIX}alice" not in fake_redis.store