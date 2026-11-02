"""
broker.py — Redis Streams producer/consumer (the distributed heart).

This module is the ONLY place that talks to Redis Streams for message
relay. It has two small classes with one responsibility each:

  * StreamProducer.publish(msg) -> str   — XADD one validated Message.
  * StreamConsumer.run(fanout_cb)        — XREAD loop, fan out, persist.
  * StreamConsumer.history(count)        — XREVRANGE for join replay.

Design decisions implemented here 

  * Dependency injection: the Redis client, LastSeenStore, and fan-out
    callback are constructor/argument inputs, not imports. This keeps
    broker decoupled from connection_manager and makes
    both classes testable against a fake fan-out 

  * At-most-once on crash: the cursor is persisted AFTER fan-out, not
    before. A crash between consume and save replays the last in-flight
    batch; a crash between save and consume would silently drop messages.
    For chat, replay is the safer failure mode .

  * Circuit-breaker-lite: on Redis ConnectionError the consumer backs
    off with capped exponential delay, logs WARN, and never crashes the
    node. A dead Redis must not take the whole server down; the node
    keeps serving its existing clients (their sends will fail until Redis
    returns, and the producer surfaces that as an error to the handler).

  * decode_responses=True on the injected client: the caller supplies
    the client, but this module assumes str (stream IDs, field values)
    per the POC-1 lesson. The convention is documented in config and
    enforced by the caller in server/main.py.

Known limits 
  * One consumer task per node is a throughput ceiling; sharding by
    stream or consumer groups is future work, out of scope for v1.
  * History is O(N) per join; fine at HISTORY_COUNT=50.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError

import config
import protocol
from protocol import Message, ProtocolError
from server.state import LastSeenStore

logger = logging.getLogger(__name__)


# Fan-out callback signature: the consumer hands each decoded Message to
# this coroutine. server/main.py binds it to ConnectionManager.broadcast,
# which is how broker stays decoupled from client management.
FanoutCallback = Callable[[Message], Awaitable[None]]


# Helpers

def _now_ms() -> int:
    """Monotonic-ish wall-clock in ms, used only for backoff jitter logs."""
    return int(asyncio.get_event_loop().time() * 1000)


def _decode_entry(entry_id: str, fields: dict[str, str]) -> Message:
    """Reconstruct a Message from a stream entry.

    The producer stores the wire JSON in the "data" field (a single
    string) so the stream stays schema-agnostic: the payload is exactly
    what protocol.encode emits minus the newline. This means history and
    live delivery share one decoder and the stream contents are
    forward-compatible with future Message types.

    Raises ProtocolError if the stored payload is corrupt — callers
    decide whether to skip the entry (log + continue) or fail.
    """
    raw = fields.get("data")
    if raw is None:
        raise ProtocolError("missing_field", "stream entry has no 'data'")
    # protocol.decode accepts str or bytes and tolerates a trailing
    # newline; the producer writes without one, but this is robust to
    # both.
    return protocol.decode(raw)


# StreamProducer

class StreamProducer:
    """XADD-side of the broker.

    Stateless beyond the injected client and stream key, so it is safe to
    share one instance across all handlers on a node. publish() is called
    from handler.py when a validated chat/join/leave needs to be relayed.
    """

    def __init__(
        self,
        client: redis.Redis,
        *,
        stream_key: str = config.STREAM_KEY,
        maxlen: int = config.STREAM_MAXLEN,
    ) -> None:
        self._client = client
        self._stream_key = stream_key
        # Approximate trimming keeps the stream bounded without the cost
        # of exact XTRIM on every add (Item 4).
        self._maxlen = maxlen

    async def publish(self, msg: Message) -> str:
        """Validate, XADD, return the new stream ID.

        Validates BEFORE writing so an invalid outbound message fails at
        the producer (where the caller can send a typed error frame)
        rather than landing corrupt in the stream for every node to
        choke on. protocol.encode already validates; we rely on it.

        Raises ProtocolError on invalid Message; lets Redis errors
        propagate so handler.py can decide how to respond (usually a
        typed error frame and a logged warning — the node stays up).
        """
        # encode() enforces the contract + byte cap; use the resulting
        # bytes as the "data" payload, stripped of the trailing newline
        # (stream entries are not line-framed).
        wire = protocol.encode(msg).rstrip(b"\n").decode("utf-8")
        # XADD with approximate MAXLEN. `approximate=True` maps to
        # MAXLEN ~ N, which is far cheaper than exact trimming and is
        # the right choice at chat throughput.
        entry_id = await self._client.xadd(
            self._stream_key,
            {"data": wire},
            maxlen=self._maxlen,
            approximate=True,
        )
        # With decode_responses=True this is already a str; the str()
        # cast is defensive and costs nothing.
        return str(entry_id)


# StreamConsumer


class StreamConsumer:
    """XREAD-side of the broker — one instance per node.

    Started as a long-lived task by server/main.py. It owns the node's
    last-seen cursor (backed by LastSeenStore) and fans out decoded
    Messages to the injected callback.

    Lifecycle:
        consumer = StreamConsumer(client, store, stream_key=...)
        task = asyncio.create_task(consumer.run(manager.broadcast))
        ...
        await consumer.stop()
        await task
    """

    def __init__(
        self,
        client: redis.Redis,
        store: LastSeenStore,
        *,
        stream_key: str = config.STREAM_KEY,
        block_ms: int = config.CONSUMER_BLOCK_MS,
    ) -> None:
        self._client = client
        self._store = store
        self._stream_key = stream_key
        self._block_ms = block_ms

        # In-memory mirror of the persisted cursor. None means "not yet
        # loaded" — run() loads it on start. Storing it here avoids a
        # disk read per XREAD.
        self._last_id: str | None = None

        # Cooperative stop signal. run() checks it between XREAD batches
        # and on backoff sleeps, so stop() returns promptly even mid-loop.
        self._stopping = asyncio.Event()

        # Backoff state for Redis outage handling (circuit-breaker-lite).
        self._backoff_base = 0.5   # seconds
        self._backoff_max = 30.0   # cap so a long outage logs ~once/30s

        # Cached fan-out callback bound at run() time; also used by
        # _drain_pending() during shutdown for the final in-flight batch.
        self._fanout: FanoutCallback | None = None

        # Diagnostics the server can surface in logs / health checks.
        self.consumed_count: int = 0
        self.last_error: str | None = None

    #  public lifecycle 

    async def run(self, fanout: FanoutCallback) -> None:
        """Main loop: XREAD from cursor, fan out, persist, repeat.

        Returns only when stop() has been called. On Redis errors it backs
        off and retries — it never raises out of the loop (a dead Redis
        must not kill the node).
        """
        self._fanout = fanout

        # Load the persisted cursor once. `None` means first boot; start
        # from "$" so a fresh node does not replay the entire retained
        # stream to its clients (the safer default for chat, see state.py).
        loaded = self._store.load()
        if loaded is None:
            logger.info(
                "no persisted cursor for stream %s; starting from $ (new messages only)",
                self._stream_key,
            )
            # "$" means "only entries added after this XREAD". On the
            # very first call Redis resolves it to the current tail; on
            # subsequent calls we always pass a concrete ID.
            self._last_id = "$"
        else:
            logger.info(
                "resuming stream %s from cursor %s", self._stream_key, loaded
            )
            self._last_id = loaded

        attempt = 0
        while not self._stopping.is_set():
            try:
                await self._read_once()
                # Successful batch resets the backoff — we only back off
                # on *consecutive* failures, not on occasional blips.
                attempt = 0
            except (RedisConnectionError, RedisTimeoutError) as exc:
                # Transient: Redis is down or the network is flaky.
                # Back off, log, and keep going. The node's clients keep
                # their connections; only new sends fail until Redis
                # returns (surfaced by the producer).
                attempt += 1
                delay = min(self._backoff_max, self._backoff_base * (2 ** attempt))
                self.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "redis stream read failed (attempt %d): %s; retrying in %.1fs",
                    attempt, self.last_error, delay,
                )
                await self._sleep_or_stop(delay)
            except ResponseError as exc:
                # A ResponseError usually means a malformed stream key or
                # cursor (e.g. corrupt last_id that slipped past load()).
                # Log at ERROR with the cursor for diagnosis, then reset
                # to "$" to avoid a hot loop against a bad ID.
                self.last_error = f"ResponseError: {exc}"
                logger.error(
                    "redis rejected XREAD (%s) for cursor %r; resetting to $",
                    self.last_error, self._last_id,
                )
                self._last_id = "$"
                await self._sleep_or_stop(self._backoff_base)
            except asyncio.CancelledError:
                # Propagate cancellation; run() is expected to be
                # cancelled via task.cancel() during shutdown. We still
                # flush the cursor in the finally below.
                raise
            except Exception:  # noqa: BLE001 — last-resort guard
                # Unexpected: log with traceback, back off briefly, and
                # keep the loop alive. The alternative (crash) takes the
                # node's fan-out down for all clients.
                self.last_error = "unexpected error in consumer loop"
                logger.exception("unexpected error in stream consumer")
                await self._sleep_or_stop(self._backoff_base)

        logger.info(
            "stream consumer stopped (consumed=%d, last_id=%s)",
            self.consumed_count, self._last_id,
        )

    async def stop(self) -> None:
        """Signal the loop to exit. Idempotent and safe from any task."""
        self._stopping.set()

    # -- public API: history ------------------------------------------------

    async def history(self, count: int = config.HISTORY_COUNT) -> list[Message]:
        """Return the last *count* messages in chronological order.

        Used by handler.py when a client joins. XREVRANGE returns newest
        first, so we reverse to chronological. Corrupt entries are
        skipped with a warning rather than failing the whole join —
        one bad historical row should not block a user from connecting.
        """
        # Clamp to the protocol cap so we never request more than we can
        # legally send back (protocol.MAX_HISTORY_BATCH).
        count = max(0, min(count, protocol.MAX_HISTORY_BATCH))
        if count == 0:
            return []

        try:
            # XREVRANGE <stream> + - COUNT n  -> newest first.
            entries = await self._client.xrevrange(
                self._stream_key, max="+", min="-", count=count
            )
        except (RedisConnectionError, RedisTimeoutError, ResponseError) as exc:
            # History is best-effort: if Redis is unhappy, send no
            # history rather than blocking the join. The client will
            # still receive live messages once the stream recovers.
            self.last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "history fetch failed for %s: %s; sending empty history",
                self._stream_key, self.last_error,
            )
            return []

        out: list[Message] = []
        for entry_id, fields in entries:
            try:
                out.append(_decode_entry(entry_id, fields))
            except ProtocolError as exc:
                logger.warning(
                    "skipping corrupt history entry %s: %s", entry_id, exc
                )
                continue
        # XREVRANGE is newest-first; reverse for chronological delivery.
        out.reverse()
        return out

    # internals 
    async def _read_once(self) -> None:
        """One XREAD round-trip + fan-out + cursor persist.

        Extraction of a single iteration keeps run()'s error handling
        simple and makes the ordering rule ("persist AFTER fan-out")
        visible in one place.
        """
        # XREAD BLOCK returns [] on timeout, or a list of (stream, entries).
        # Passing a concrete ID resumes exactly after it; passing "$"
        # starts from "now" (used only on first boot).
        response = await self._client.xread(
            {self._stream_key: self._last_id},
            count=100,
            block=self._block_ms,
        )
        if not response:
            # Timeout — no new messages. Loop again.
            return

        for _stream_name, entries in response:
            for entry_id, fields in entries:
                if self._stopping.is_set():
                    # Don't start a new delivery while stopping; the
                    # finally in run() flushes the cursor for what we
                    # already delivered.
                    return
                try:
                    msg = _decode_entry(entry_id, fields)
                except ProtocolError as exc:
                    # A corrupt row must not halt the node. Log, advance
                    # the cursor past it, and continue: otherwise the
                    # consumer would hot-loop on the same bad entry.
                    logger.warning(
                        "skipping corrupt stream entry %s: %s", entry_id, exc
                    )
                    self._last_id = str(entry_id)
                    self._store.save(self._last_id)
                    continue

                # Fan out FIRST. If fan-out raises (a buggy callback), we
                # do NOT advance the cursor, so the entry is retried on
                # the next loop rather than silently dropped. In practice
                # the callback (ConnectionManager.broadcast) swallows
                # per-client exceptions, so this is a belt-and-braces rule.
                assert self._fanout is not None  # set in run() before loop
                await self._fanout(msg)
                self.consumed_count += 1

                # Persist AFTER fan-out (R2 at-most-once trade-off).
                self._last_id = str(entry_id)
                self._store.save(self._last_id)

    async def _sleep_or_stop(self, delay: float) -> None:
        """Sleep up to *delay* seconds, waking early on stop().

        A plain asyncio.sleep would delay shutdown by up to _backoff_max
        (30s) during an outage. Racing against the stop event keeps
        shutdown prompt without polling.
        """
        try:
            await asyncio.wait_for(self._stopping.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass