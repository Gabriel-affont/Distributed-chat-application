"""
test_broker.py — correctness tests for server/broker.py.

This file makes POC-3 (two consumers agree on order) permanent and pins
every property the broker claims in its module docstring:

  * publish round-trips through the stream
  * cursor persists AFTER fan-out (at-most-once on crash)
  * consumer resumes from persisted cursor after simulated restart
  * corrupt entries are skipped and the cursor advances past them
  * Redis outage triggers backoff without crashing the loop
  * history returns chronological order, clamped to MAX_HISTORY_BATCH
  * two consumers against one stream see identical ordered sequences

Isolation:
  * Uses Redis DB 15 (never DB 0 where the app/demo runs).
  * Wipes the stream and any test cursor dirs before each test.
  * Injects a fake fan-out callback — no server code involved, which is
    exactly the DI seam the architecture analysis calls out.

Risks exercised: R2 (cursor correctness), R5 (two-node debugging),
R16 (consistent ordering), R17 (throughput ceiling noted, not tested).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError

import config
import protocol
from protocol import MAX_HISTORY_BATCH, make_chat, make_error, make_join, Message
from server.broker import StreamConsumer, StreamProducer
from server.state import LastSeenStore


# ---------------------------------------------------------------------------
# Redis fixtures — isolate to DB 15, wipe between tests
# ---------------------------------------------------------------------------

# Use a distinct DB so tests never touch dev/demo data (DB 0). The URL
# is derived from config so a custom REDIS_URL env still works: we swap
# the DB number in the path.
def _test_redis_url() -> str:
    base = config.REDIS_URL.rstrip("/")
    # config.REDIS_URL looks like redis://host:port/0
    head, _, _db = base.rpartition("/")
    return f"{head}/15"


TEST_REDIS_URL = _test_redis_url()
TEST_STREAM_KEY = "chat:test:stream"


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[redis.Redis]:
    """A str-decoding client on DB 15, flushed before and after."""
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    try:
        await client.flushdb()
        yield client
    finally:
        try:
            await client.flushdb()
        finally:
            await client.aclose()


@pytest_asyncio.fixture
async def producer(redis_client: redis.Redis) -> StreamProducer:
    return StreamProducer(redis_client, stream_key=TEST_STREAM_KEY)


@pytest_asyncio.fixture
def store(tmp_path: Path) -> LastSeenStore:
    """Per-test cursor store; never touches the real data/ dir."""
    return LastSeenStore(data_dir=tmp_path, node_id="test-node")


@pytest_asyncio.fixture
async def consumer(
    redis_client: redis.Redis, store: LastSeenStore
) -> StreamConsumer:
    return StreamConsumer(
        redis_client,
        store,
        stream_key=TEST_STREAM_KEY,
        # Short block so tests don't wait a full second per iteration.
        block_ms=100,
    )


class FakeFanout:
    """Collects fanned-out messages; can be made to raise on demand.

    Async callable matching FanoutCallback. Also exposes `wait_for(n)` so
    tests can await delivery without sleeping arbitrarily.
    """

    def __init__(self) -> None:
        self.messages: list[Message] = []
        self._event = asyncio.Event()
        self.fail_next: Exception | None = None

    async def __call__(self, msg: Message) -> None:
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        self.messages.append(msg)
        self._event.set()

    async def wait_for(self, n: int, timeout: float = 5.0) -> None:
        """Wait until at least *n* messages have been collected."""
        async def _poll() -> None:
            while len(self.messages) < n:
                self._event.clear()
                await self._event.wait()
        await asyncio.wait_for(_poll(), timeout=timeout)


# ---------------------------------------------------------------------------
# StreamProducer
# ---------------------------------------------------------------------------

class TestProducer:
    async def test_publish_returns_valid_stream_id(
        self, producer: StreamProducer
    ) -> None:
        entry_id = await producer.publish(make_chat("alice", "hi"))
        # "<ms>-<seq>" with integer parts, matching state.py's contract.
        import re
        assert re.fullmatch(r"\d+-\d+", entry_id), entry_id

    async def test_publish_encodes_message_in_stream(
        self, producer: StreamProducer, redis_client: redis.Redis
    ) -> None:
        await producer.publish(make_chat("alice", "hello"))
        entries = await redis_client.xrange(TEST_STREAM_KEY)
        assert len(entries) == 1
        _id, fields = entries[0]
        # The stream stores the wire JSON under "data".
        decoded = protocol.decode(fields["data"])
        assert decoded.sender == "alice"
        assert decoded.content == "hello"

    async def test_publish_rejects_invalid_message(
        self, producer: StreamProducer
    ) -> None:
        # Construct a Message that fails validation (bad username).
        bad = Message(
            type=protocol.TYPE_CHAT, sender="bad name", content="x"
        )
        with pytest.raises(protocol.ProtocolError):
            await producer.publish(bad)

    async def test_publish_respects_maxlen(
        self, redis_client: redis.Redis
    ) -> None:
        # MAXLEN ~ N is approximate: Redis trims in whole macro-nodes
        # (~100 entries by default), so a short burst of adds may not
        # trigger trimming. Assert that the stream stays bounded after
        # enough entries to cross a macro-node boundary, not that it
        # trims exactly at N.
        p = StreamProducer(redis_client, stream_key=TEST_STREAM_KEY, maxlen=10)
        for i in range(300):
            await p.publish(make_chat("alice", f"m{i}"))
        length = await redis_client.xlen(TEST_STREAM_KEY)
        # Bounded well under the naive 300; not zero (we kept something).
        assert 0 < length < 300


# ---------------------------------------------------------------------------
# StreamConsumer: basic delivery
# ---------------------------------------------------------------------------

class TestConsumerDelivery:
    async def test_consumes_published_message(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            await producer.publish(make_chat("alice", "hello"))
            await fanout.wait_for(1)
            assert fanout.messages[0].content == "hello"
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)

    async def test_consumes_multiple_in_order(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            for i in range(5):
                await producer.publish(make_chat("alice", f"m{i}"))
            await fanout.wait_for(5)
            assert [m.content for m in fanout.messages] == [
                "m0", "m1", "m2", "m3", "m4"
            ]
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)


# ---------------------------------------------------------------------------
# Cursor persistence and resume (R2)
# ---------------------------------------------------------------------------

class TestCursorResume:
    async def test_cursor_persisted_after_delivery(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
        store: LastSeenStore,
    ) -> None:
        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            entry_id = await producer.publish(make_chat("alice", "hi"))
            await fanout.wait_for(1)
            # The persisted cursor must equal the delivered entry's id.
            assert store.load() == entry_id
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)

    async def test_cursor_not_advanced_when_fanout_raises(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
        store: LastSeenStore,
    ) -> None:
        # Fan-out raising must leave the cursor un-advanced so the entry
        # is retried, not silently dropped.
        fanout = FakeFanout()
        fanout.fail_next = RuntimeError("boom")
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            await producer.publish(make_chat("alice", "hi"))
            # Give the consumer a few iterations to retry the failing
            # entry and eventually succeed on the retry.
            await fanout.wait_for(1)
            assert fanout.messages[0].content == "hi"
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)

    async def test_restart_resumes_from_persisted_cursor(
        self,
        redis_client: redis.Redis,
        store: LastSeenStore,
    ) -> None:
        # Two consumers over the same store: the first consumes and
        # persists a cursor; the second (simulating a restart) starts
        # from that cursor and only sees NEW messages.
        p = StreamProducer(redis_client, stream_key=TEST_STREAM_KEY)

        c1 = StreamConsumer(redis_client, store, stream_key=TEST_STREAM_KEY, block_ms=100)
        f1 = FakeFanout()
        t1 = asyncio.create_task(c1.run(f1))
        await c1.started.wait()
        try:
            await p.publish(make_chat("alice", "before"))
            await f1.wait_for(1)
        finally:
            await c1.stop()
            await asyncio.wait_for(t1, timeout=5)

        # "Restart": a fresh consumer over the same store.
        c2 = StreamConsumer(redis_client, store, stream_key=TEST_STREAM_KEY, block_ms=100)
        f2 = FakeFanout()
        t2 = asyncio.create_task(c2.run(f2))
        await c2.started.wait()
        try:
            await p.publish(make_chat("alice", "after"))
            await f2.wait_for(1)
            # Only "after" should be seen; "before" was already consumed.
            assert [m.content for m in f2.messages] == ["after"]
        finally:
            await c2.stop()
            await asyncio.wait_for(t2, timeout=5)

    async def test_missing_cursor_starts_from_dollar(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        # Publish BEFORE the consumer starts. With no cursor, the
        # consumer starts from $ and must NOT replay the pre-existing
        # message (documented default).
        await producer.publish(make_chat("alice", "old"))
        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            await producer.publish(make_chat("alice", "new"))
            await fanout.wait_for(1)
            assert [m.content for m in fanout.messages] == ["new"]
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)


# ---------------------------------------------------------------------------
# Corrupt entry handling
# ---------------------------------------------------------------------------

class TestCorruptEntries:
    async def test_corrupt_entry_skipped_and_cursor_advances(
        self,
        redis_client: redis.Redis,
        consumer: StreamConsumer,
        store: LastSeenStore,
    ) -> None:
        # Write one good, one corrupt, one good entry directly into the
        # stream. The consumer should deliver the two good ones and skip
        # the corrupt one, advancing the cursor past all three.
        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        await consumer.started.wait()
        try:
            # Write one good, one corrupt, one good entry directly into
            # the stream AFTER the consumer started, so they land after
            # the resolved tail and are delivered. The consumer should
            # deliver the two good ones and skip the corrupt one,
            # advancing the cursor past all three.
            good1 = await redis_client.xadd(
                TEST_STREAM_KEY,
                {"data": protocol.encode(make_chat("alice", "one")).decode().rstrip("\n")},
            )
            bad = await redis_client.xadd(TEST_STREAM_KEY, {"data": "not-json"})
            good2 = await redis_client.xadd(
                TEST_STREAM_KEY,
                {"data": protocol.encode(make_chat("alice", "two")).decode().rstrip("\n")},
            )
            await fanout.wait_for(2)
            assert [m.content for m in fanout.messages] == ["one", "two"]
            # Cursor must be past good2 (not stuck at `bad`).
            assert store.load() == good2
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)

# ---------------------------------------------------------------------------
# Redis outage: backoff, no crash
# ---------------------------------------------------------------------------

class TestRedisOutage:
    async def test_consumer_survives_redis_connection_error(
        self,
        consumer: StreamConsumer,
    ) -> None:
        # Simulate Redis being down by monkeypatching XREAD to raise
        # ConnectionError for the first two calls, then succeed. The
        # consumer must back off, not raise, and eventually deliver.
        calls = {"n": 0}
        real_xread = consumer._client.xread  # type: ignore[attr-defined]

        async def flaky_xread(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise RedisConnectionError("simulated outage")
            return await real_xread(*args, **kwargs)

        consumer._client.xread = flaky_xread  # type: ignore[attr-defined]

        # Shrink backoff so the test runs fast.
        consumer._backoff_base = 0.01  # type: ignore[attr-defined]
        consumer._backoff_max = 0.02  # type: ignore[attr-defined]

        fanout = FakeFanout()
        task = asyncio.create_task(consumer.run(fanout))
        try:
            await asyncio.sleep(0.1)  # let the consumer eat the two failures
            await consumer._client.xadd(  # type: ignore[attr-defined]
                TEST_STREAM_KEY,
                {"data": protocol.encode(make_chat("alice", "recovered")).decode().rstrip("\n")},
            )
            await fanout.wait_for(1, timeout=5)
            assert fanout.messages[0].content == "recovered"
            assert calls["n"] >= 3
        finally:
            await consumer.stop()
            await asyncio.wait_for(task, timeout=5)


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

class TestHistory:
    async def test_history_empty_stream(
        self, consumer: StreamConsumer
    ) -> None:
        assert await consumer.history() == []

    async def test_history_chronological_order(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        for i in range(5):
            await producer.publish(make_chat("alice", f"m{i}"))
        history = await consumer.history(count=10)
        assert [m.content for m in history] == ["m0", "m1", "m2", "m3", "m4"]

    async def test_history_respects_count(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        for i in range(10):
            await producer.publish(make_chat("alice", f"m{i}"))
        history = await consumer.history(count=3)
        # XREVRANGE returns newest-first; reversed -> last 3 chronological.
        assert [m.content for m in history] == ["m7", "m8", "m9"]

    async def test_history_clamped_to_protocol_max(
        self,
        producer: StreamProducer,
        consumer: StreamConsumer,
    ) -> None:
        for i in range(MAX_HISTORY_BATCH + 5):
            await producer.publish(make_chat("alice", f"m{i}"))
        # Asking for more than the protocol cap must be clamped, not sent.
        history = await consumer.history(count=MAX_HISTORY_BATCH + 100)
        assert len(history) == MAX_HISTORY_BATCH

    async def test_history_skips_corrupt_entries(
        self,
        redis_client: redis.Redis,
        consumer: StreamConsumer,
    ) -> None:
        await redis_client.xadd(
            TEST_STREAM_KEY,
            {"data": protocol.encode(make_chat("alice", "ok")).decode().rstrip("\n")},
        )
        await redis_client.xadd(TEST_STREAM_KEY, {"data": "garbage"})
        history = await consumer.history(count=10)
        # Corrupt entry skipped, valid one survives.
        assert [m.content for m in history] == ["ok"]


# ---------------------------------------------------------------------------
# POC-3 permanent: two consumers agree on order (R16)
# ---------------------------------------------------------------------------

class TestTwoConsumersAgree:
    async def test_two_consumers_see_identical_sequence(
        self,
        redis_client: redis.Redis,
        tmp_path: Path,
    ) -> None:
        # Two independent consumer instances (separate stores = separate
        # nodes) reading the same stream must see identical messages in
        # identical order. This is the permanent form of POC-3.
        p = StreamProducer(redis_client, stream_key=TEST_STREAM_KEY)

        store_a = LastSeenStore(data_dir=tmp_path, node_id="nodeA")
        store_b = LastSeenStore(data_dir=tmp_path, node_id="nodeB")
        c_a = StreamConsumer(redis_client, store_a, stream_key=TEST_STREAM_KEY, block_ms=100)
        c_b = StreamConsumer(redis_client, store_b, stream_key=TEST_STREAM_KEY, block_ms=100)

        fan_a, fan_b = FakeFanout(), FakeFanout()
        t_a = asyncio.create_task(c_a.run(fan_a))
        t_b = asyncio.create_task(c_b.run(fan_b))
        try:
            # Give both consumers a moment to start at $ (no prior
            # messages, so both start from "now").
            await asyncio.sleep(0.1)
            for i in range(20):
                await p.publish(make_chat("alice", f"m{i}"))
            await fan_a.wait_for(20)
            await fan_b.wait_for(20)
            assert [m.content for m in fan_a.messages] == \
                   [m.content for m in fan_b.messages]
            assert [m.content for m in fan_a.messages] == \
                   [f"m{i}" for i in range(20)]
        finally:
            await c_a.stop()
            await c_b.stop()
            await asyncio.wait_for(t_a, timeout=5)
            await asyncio.wait_for(t_b, timeout=5)


# ---------------------------------------------------------------------------
# Stop behavior
# ---------------------------------------------------------------------------

class TestStop:
    async def test_stop_is_prompt_during_idle(
        self, consumer: StreamConsumer
    ) -> None:
        task = asyncio.create_task(consumer.run(FakeFanout()))
        await asyncio.sleep(0.05)
        await consumer.stop()
        # Must return promptly; block_ms is 100, but stop wakes it.
        await asyncio.wait_for(task, timeout=2)

    async def test_stop_is_idempotent(
        self, consumer: StreamConsumer
    ) -> None:
        task = asyncio.create_task(consumer.run(FakeFanout())) 
        await consumer.stop()
        await consumer.stop()  # second call must not raise
        await asyncio.wait_for(task, timeout=2)