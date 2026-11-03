"""
test_integration.py — end-to-end tests for the distributed chat (M4).

Two real server nodes on ephemeral ports, sharing one Redis DB (14),
talked to by real TCP clients speaking NDJSON. This is the milestone
test: it exercises the whole system — handler, manager, broker,
consumer, fan-out, cross-node delivery — at once.

What it covers that unit tests cannot:
  * A chat published by a client on node1 is delivered to a client on
    node2 (the "distributed" requirement).
  * History-on-join works when the history was produced by ANOTHER node.
  * Username uniqueness across nodes (Redis SET NX).
  * A running system can lose one node and serve new clients on the
    other (client-side failover, Decision 5).
  * The history filter added in the last fix works end-to-end (chat
    only, no join/leave noise).

Isolation:
  * Redis DB 14 (broker tests use 15, demo uses 0).
  * Ephemeral ports from the OS.
  * Per-test temp dirs for cursor files; stream flushed before/after.
  * Every node's consumer and TCP server stopped in teardown.

Design note: this file intentionally DOES NOT spawn subprocesses or
install signal handlers. Those paths are covered by (a) the manual
demo walkthrough in the README and (b) server/main.py's own smoke
check. Integration tests here stay in-process for reliability on
Windows and fast CI.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Callable

import pytest
import pytest_asyncio
import redis.asyncio as redis

import config
import protocol
from client.failover import connect_with_failover
from protocol import MAX_LINE_BYTES, Message
from server.broker import StreamConsumer, StreamProducer
from server.connection_manager import ConnectionManager
from server.handler import HandlerContext, handle_client
from server.state import LastSeenStore


# ---------------------------------------------------------------------------
# Redis isolation (DB 14) — mirrors test_broker.py's pattern
# ---------------------------------------------------------------------------

def _test_redis_url() -> str:
    base = config.REDIS_URL.rstrip("/")
    head, _, _db = base.rpartition("/")
    return f"{head}/14"


TEST_REDIS_URL = _test_redis_url()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        await client.flushdb()
        yield client
    finally:
        with contextlib.suppress(Exception):
            await client.flushdb()
        await client.aclose()


# ---------------------------------------------------------------------------
# Test node — a self-contained server, no signals, no __main__
# ---------------------------------------------------------------------------

@dataclass
class TestNode:
    node_id: str
    ctx: HandlerContext
    consumer: StreamConsumer
    consumer_task: asyncio.Task[None]
    server: asyncio.AbstractServer
    port: int


async def _start_test_node(
    node_id: str,
    data_dir: Path,
    redis_client: redis.Redis,
) -> TestNode:
    """Build and start a complete node, mirroring server/main.build_runtime.

    Kept local so tests can parametrize node_id, data_dir, and Redis
    without touching config (which is module-level).
    """
    store = LastSeenStore(data_dir, node_id)
    producer = StreamProducer(redis_client, stream_key=config.STREAM_KEY)
    consumer = StreamConsumer(
        redis_client, store,
        stream_key=config.STREAM_KEY,
        block_ms=100,  # fast loop for tests
    )
    manager = ConnectionManager(queue_maxsize=16)
    ctx = HandlerContext(
        broker_producer=producer,
        broker_consumer=consumer,
        manager=manager,
        redis_client=redis_client,
        node_id=node_id,
    )
    # Start the consumer before the TCP server so joins never race it.
    consumer_task = asyncio.create_task(
        consumer.run(manager.broadcast), name=f"consumer-{node_id}"
    )
    await asyncio.wait_for(consumer.started.wait(), timeout=2.0)

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, ctx=ctx),
        host="127.0.0.1",
        port=0,  # OS-assigned ephemeral port
        limit=MAX_LINE_BYTES,
    )
    port = server.sockets[0].getsockname()[1]
    return TestNode(
        node_id=node_id,
        ctx=ctx,
        consumer=consumer,
        consumer_task=consumer_task,
        server=server,
        port=port,
    )


async def _stop_test_node(node: TestNode) -> None:
    """Idempotent teardown: close server, stop consumer, await task."""
    with contextlib.suppress(Exception):
        node.server.close()
        await node.server.wait_closed()
    with contextlib.suppress(Exception):
        await node.consumer.stop()
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(node.consumer_task, timeout=2.0)
    if not node.consumer_task.done():
        node.consumer_task.cancel()
        with contextlib.suppress(Exception):
            await node.consumer_task


@pytest_asyncio.fixture
async def two_nodes(
    redis_client: redis.Redis, tmp_path: Path
) -> AsyncIterator[tuple[TestNode, TestNode]]:
    """Two independently-started nodes sharing one Redis DB."""
    nodes: list[TestNode] = []
    try:
        for node_id in ("node1", "node2"):
            node = await _start_test_node(
                node_id, tmp_path / node_id, redis_client
            )
            nodes.append(node)
        yield nodes[0], nodes[1]
    finally:
        for node in reversed(nodes):
            await _stop_test_node(node)


# ---------------------------------------------------------------------------
# Test client — speaks the wire protocol directly, no TUI
# ---------------------------------------------------------------------------

class TestClient:
    """Minimal protocol-level client for integration tests.

    Unlike client/main.py, this does no rendering or input handling —
    tests drive it programmatically. It buffers every received Message
    and offers `wait_for(predicate)` which returns the NEXT message
    matching a predicate (starting from the last consumed position).
    """

    def __init__(self) -> None:
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.received: list[Message] = []
        self._consumed = 0
        self._read_task: asyncio.Task[None] | None = None

    async def connect(self, port: int) -> None:
        self.reader, self.writer = await asyncio.open_connection(
            "127.0.0.1", port
        )
        self._read_task = asyncio.create_task(self._read_loop())

    async def connect_via_failover(
        self, nodes: list[tuple[str, int]], **kwargs
    ) -> None:
        self.reader, self.writer = await connect_with_failover(
            nodes, **kwargs
        )
        self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                line = await self.reader.readline()
                if not line:
                    return
                try:
                    self.received.append(protocol.decode(line))
                except protocol.ProtocolError:
                    # Server sent a malformed frame — a real bug, but
                    # we don't want to hang the test; log via append
                    # would fail so just skip. Tests that care about
                    # malformed output will assert separately.
                    continue
        except (ConnectionError, OSError):
            return

    async def send(self, msg: Message) -> None:
        assert self.writer is not None
        self.writer.write(protocol.encode(msg))
        await self.writer.drain()

    async def join(self, username: str) -> None:
        await self.send(protocol.make_join(username))

    async def chat(self, username: str, text: str) -> None:
        await self.send(protocol.make_chat(username, text))

    async def leave(self, username: str) -> None:
        await self.send(protocol.make_leave(username))

    async def wait_for(
        self,
        predicate: Callable[[Message], bool],
        timeout: float = 3.0,
    ) -> Message:
        """Return the next received message matching *predicate*.

        Only considers messages received after the last call (mirrors
        "next message" semantics). Raises TimeoutError if none arrives.
        """
        start = self._consumed

        async def _poll() -> Message:
            while True:
                for i in range(start, len(self.received)):
                    if predicate(self.received[i]):
                        self._consumed = i + 1
                        return self.received[i]
                await asyncio.sleep(0.01)

        return await asyncio.wait_for(_poll(), timeout=timeout)

    async def wait_for_join_event(self, username: str, timeout: float = 3.0) -> Message:
        return await self.wait_for(
            lambda m: m.type == protocol.TYPE_JOIN and m.sender == username,
            timeout=timeout,
        )

    async def wait_for_chat(self, content: str, timeout: float = 3.0) -> Message:
        return await self.wait_for(
            lambda m: m.type == protocol.TYPE_CHAT and m.content == content,
            timeout=timeout,
        )

    async def wait_for_error(self, timeout: float = 3.0) -> Message:
        return await self.wait_for(
            lambda m: m.type == protocol.TYPE_ERROR, timeout=timeout
        )

    async def close(self) -> None:
        if self.writer is not None:
            with contextlib.suppress(Exception):
                self.writer.close()
                await self.writer.wait_closed()
        if self._read_task is not None and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(Exception):
                await self._read_task


@contextlib.asynccontextmanager
async def _connected_client(
    port: int, *, username: str | None = None
) -> AsyncIterator[TestClient]:
    """Open a TestClient, optionally join, and clean up on exit."""
    client = TestClient()
    try:
        await client.connect(port)
        if username is not None:
            await client.join(username)
            await client.wait_for_join_event(username)
        yield client
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# M4: cross-node delivery
# ---------------------------------------------------------------------------

class TestCrossNodeDelivery:
    async def test_chat_from_node1_reaches_client_on_node2(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        async with _connected_client(node1.port, username="alice") as alice, \
                   _connected_client(node2.port, username="bob") as bob:
            await alice.chat("alice", "hello from node1")
            # bob is on node2 — the message must cross the Redis Stream.
            msg = await bob.wait_for_chat("hello from node1")
            assert msg.sender == "alice"
            assert msg.origin_node == "node1"

    async def test_broadcast_reaches_all_clients_on_both_nodes(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        async with _connected_client(node1.port, username="alice") as alice, \
                   _connected_client(node1.port, username="bob") as bob, \
                   _connected_client(node2.port, username="carol") as carol, \
                   _connected_client(node2.port, username="dave") as dave:
            await alice.chat("alice", "hello everyone")
            for name, client in (("bob", bob), ("carol", carol), ("dave", dave)):
                msg = await client.wait_for_chat("hello everyone")
                assert msg.sender == "alice", f"{name} got wrong sender"
            # Alice also sees her own message (stream-only delivery).
            echo = await alice.wait_for_chat("hello everyone")
            assert echo.sender == "alice"

    async def test_leave_event_propagates_across_nodes(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        async with _connected_client(node2.port, username="bob") as bob:
            alice = TestClient()
            await alice.connect(node1.port)
            try:
                await alice.join("alice")
                await alice.wait_for_join_event("alice")
                await bob.wait_for_join_event("alice")
                await alice.leave("alice")
                await alice.close()
                # bob must see the leave event published to the stream.
                await bob.wait_for(
                    lambda m: m.type == protocol.TYPE_LEAVE and m.sender == "alice"
                )
            finally:
                await alice.close()


# ---------------------------------------------------------------------------
# Username uniqueness across nodes (R7)
# ---------------------------------------------------------------------------

class TestUsernameUniqueness:
    async def test_same_username_rejected_on_other_node(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        async with _connected_client(node1.port, username="alice"):
            # Second client, different node, same name -> rejected.
            other = TestClient()
            await other.connect(node2.port)
            try:
                await other.join("alice")
                err = await other.wait_for_error()
                assert err.code == protocol.ERR_INVALID_USERNAME
            finally:
                await other.close()

    async def test_username_released_on_disconnect(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        # First client occupies "alice" on node1, then disconnects.
        first = TestClient()
        await first.connect(node1.port)
        try:
            await first.join("alice")
            await first.wait_for_join_event("alice")
        finally:
            await first.close()
        # Give the server a moment to run cleanup (redis DEL).
        await asyncio.sleep(0.1)
        # Now "alice" can join on node2.
        async with _connected_client(node2.port, username="alice"):
            pass  # joined successfully if no exception


# ---------------------------------------------------------------------------
# History on join (R6 + the recent chat-only filter fix)
# ---------------------------------------------------------------------------

class TestHistoryAcrossNodes:
    async def test_history_produced_by_one_node_visible_on_other(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        # Produce a chat on node1, then join a fresh client on node2.
        async with _connected_client(node1.port, username="alice") as alice:
            await alice.chat("alice", "before bob joins")
            # Let node1's consumer deliver (proves the message is
            # persisted to the stream before bob asks for history).
            await alice.wait_for_chat("before bob joins")

            async with _connected_client(node2.port) as bob:
                await bob.join("bob")
                # History frame must be received before live stream.
                history = await bob.wait_for(
                    lambda m: m.type == protocol.TYPE_HISTORY
                )
                contents = [m.get("content") for m in history.messages]
                assert "before bob joins" in contents

    async def test_history_excludes_join_and_leave_events_end_to_end(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        # Send join, chat, leave to produce mixed events. A new joiner's
        # history must contain only the chat (fix from the previous step).
        node1, node2 = two_nodes
        carol = TestClient()
        await carol.connect(node1.port)
        try:
            await carol.join("carol")
            await carol.wait_for_join_event("carol")
            await carol.chat("carol", "the only chat")
            await carol.wait_for_chat("the only chat")
            await carol.leave("carol")
            await carol.close()
        finally:
            await carol.close()

        async with _connected_client(node2.port) as dave:
            await dave.join("dave")
            history = await dave.wait_for(
                lambda m: m.type == protocol.TYPE_HISTORY
            )
            # Only chats survive the server-side filter.
            kinds = {m.get("type") for m in history.messages}
            assert kinds == {protocol.TYPE_CHAT}
            assert any(
                m.get("content") == "the only chat"
                for m in history.messages
            )


# ---------------------------------------------------------------------------
# Failover (Decision 5): a live node keeps serving when another is down
# ---------------------------------------------------------------------------

class TestFailover:
    async def test_new_client_skips_dead_node_and_connects_to_live_one(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        # A stable client on node2 that the new client will talk to.
        async with _connected_client(node2.port, username="bob") as bob:
            # Kill node1 (stop accepting connections + stop consumer).
            await _stop_test_node(node1)

            # New client uses failover across [node1, node2]. node1 is
            # now dead; connect_with_failover must skip it.
            newcomer = TestClient()
            try:
                await newcomer.connect_via_failover(
                    [("127.0.0.1", node1.port), ("127.0.0.1", node2.port)],
                    connect_timeout=0.5,
                    backoff=0.0,
                    max_passes=2,
                )
                await newcomer.join("alice")
                await newcomer.wait_for_join_event("alice")
                # Prove the new client is fully functional on node2.
                await newcomer.chat("alice", "hello from the newcomer")
                msg = await bob.wait_for_chat("hello from the newcomer")
                assert msg.sender == "alice"
            finally:
                await newcomer.close()

    async def test_existing_clients_on_live_node_unaffected_by_other_node_death(
        self, two_nodes: tuple[TestNode, TestNode]
    ) -> None:
        node1, node2 = two_nodes
        async with _connected_client(node2.port, username="bob") as bob, \
                   _connected_client(node2.port, username="carol") as carol:
            # Sanity: chat works on node2.
            await bob.chat("bob", "before")
            await carol.wait_for_chat("before")

            # Kill node1; clients on node2 must not be affected.
            await _stop_test_node(node1)

            await bob.chat("bob", "after node1 died")
            msg = await carol.wait_for_chat("after node1 died")
            assert msg.sender == "bob"