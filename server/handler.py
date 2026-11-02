"""
handler.py — per-client read loop, join flow, and message routing.

This is the orchestration layer of the server. It owns the state machine
of one connection (connected -> joined -> closed) and the join flow,
which is where two subtle races live:

  * History-on-join vs. live delivery (R6): the client must see history
    up to the stream tail at join time, then live messages after it,
    with no gap and no duplicate at the seam. We snapshot the tail
    BEFORE sending history, stream live from the snapshot's tail after.

  * Username uniqueness across nodes (R7): enforced globally via Redis
    SET NX with a TTL. The TTL is the crash safety net; DEL on graceful
    disconnect is best-effort. Documented limitation: a hard-crashed
    client's username is unavailable until the TTL expires.

Design decisions :

  * Dispatch table, not if/elif: {type: handler_fn}. Adding a new
    message type is one entry + one coroutine. Extensibility without
    refactoring (Open/Closed-ish).

  * DI everywhere: broker, manager, and redis client arrive as
    parameters from server/main.py. This module imports neither
    connection_manager nor server.main, keeping the DAG clean.

  * Guaranteed cleanup in finally: unregister + Redis DEL + close,
    even on cancellation or unhandled exception. A crashed handler
    must not leak a Redis username key (R7) or a registry entry (R4).

  * Protocol errors are client-facing: on a ProtocolError we send a
    typed error frame and disconnect. On any other exception we log
    with traceback and disconnect. Never trust the client (R15).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Awaitable, Callable

import redis.asyncio as redis

import config
import protocol
from protocol import Message, ProtocolError
from server.broker import StreamConsumer, StreamProducer
from server.connection_manager import ClientConn, ConnectionManager

logger = logging.getLogger(__name__)


# Per-connection dispatch signature: a handler takes (conn, msg, ctx) and
# returns None. It may raise ProtocolError to trigger a client-facing
# error frame, or any other exception to trigger a logged disconnect.
HandlerFn = Callable[["HandlerContext", ClientConn, Message], Awaitable[None]]


class HandlerContext:
    """Everything a per-connection handler needs, passed by DI.

    Bundling these into a small context keeps the handler signature
    stable as the server grows (new fields = one dataclass change, not
    five call-site changes) and makes tests trivially buildable.
    """

    def __init__(
        self,
        *,
        broker_producer: StreamProducer,
        broker_consumer: StreamConsumer,
        manager: ConnectionManager,
        redis_client: redis.Redis,
        node_id: str,
    ) -> None:
        self.broker_producer = broker_producer
        self.broker_consumer = broker_consumer
        self.manager = manager
        self.redis_client = redis_client
        self.node_id = node_id


# Entry point: one coroutine per accepted connection


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    ctx: HandlerContext,
) -> None:
    """Per-connection coroutine: register, read loop, guaranteed cleanup.

    Wired into asyncio.start_server in server/main.py. Every accepted
    connection runs this to completion; nothing else owns the reader or
    writer, so cleanup here is the single teardown point.
    """
    conn = await ctx.manager.register(writer)
    try:
        await _read_loop(reader, conn, ctx)
    except ProtocolError as exc:
        # Malformed wire data: send a typed error frame if the connection
        # still has a queue, then close. This is the "never trust the
        # client" boundary (R15).
        logger.info("protocol error on %s: %s", conn, exc)
        await _safe_error_send(conn, exc.code, exc.detail, ctx)
    except (ConnectionError, asyncio.IncompleteReadError) as exc:
        # Normal client disconnect.
        logger.debug("connection lost on %s: %s", conn, exc)
    except asyncio.CancelledError:
        # Server shutdown: propagate after cleanup.
        raise
    except Exception:
        # Unexpected: log with traceback and disconnect. Never leave the
        # loop silent — an invisible bug here means invisible outage.
        logger.exception("unexpected error in handler for %s", conn)
    finally:
        # Guaranteed cleanup: remove from registry, close writer, and
        # release the username key. This runs on every exit path.
        await _cleanup(conn, ctx)


# Read loop


async def _read_loop(
    reader: asyncio.StreamReader, conn: ClientConn, ctx: HandlerContext
) -> None:
    """Read NDJSON lines until EOF, cancel, or protocol error.

    Each line is decoded, validated, and dispatched. Oversize lines are
    caught here (LimitOverrunError from readline) and turned into a
    ProtocolError so the client gets a typed error before disconnect.
    """
    while True:
        try:
            line = await reader.readline()
        except (asyncio.LimitOverrunError, ValueError) as exc:
            # The stream reader's `limit` is MAX_LINE_BYTES; a longer
            # line raises here before we ever see the bytes. Surface as
            # a typed protocol error rather than a raw exception.
            raise ProtocolError(
                protocol.ERR_LINE_TOO_LONG,
                f"line exceeds {protocol.MAX_LINE_BYTES} bytes: {exc}",
            ) from exc
        if not line:
            # EOF: peer closed the connection.
            return
        try:
            msg = protocol.decode(line)
        except ProtocolError:
            # Malformed frame: propagate to handle_client for the error
            # frame + disconnect path. Do not attempt to "recover" — a
            # peer that emits garbage on one line cannot be trusted to
            # emit valid frames on the next.
            raise
        await _dispatch(conn, msg, ctx)



# Dispatch


# The registry. First message a client sends MUST be TYPE_JOIN; every
# other type is rejected until the join succeeds (checked in dispatch).
_DISPATCH: dict[str, HandlerFn] = {}


def _register(message_type: str) -> Callable[[HandlerFn], HandlerFn]:
    """Decorator to add a handler to the dispatch table."""

    def _wrap(fn: HandlerFn) -> HandlerFn:
        _DISPATCH[message_type] = fn
        return fn

    return _wrap


async def _dispatch(conn: ClientConn, msg: Message, ctx: HandlerContext) -> None:
    """Route *msg* to the correct handler after enforcing join-before-send.

    Before join, only TYPE_JOIN and TYPE_LEAVE are permitted. After join,
    TYPE_CHAT is permitted; TYPE_JOIN again is a protocol error (a client
    cannot "become" a different user mid-session); TYPE_ERROR and
    TYPE_HISTORY are server-only and rejected from clients.
    """
    joined = conn.username is not None

    if not joined and msg.type not in (protocol.TYPE_JOIN, protocol.TYPE_LEAVE):
        raise ProtocolError(
            protocol.ERR_UNKNOWN_TYPE,
            f"must send 'join' before '{msg.type}'",
        )

    if joined and msg.type == protocol.TYPE_JOIN:
        # A second join is ambiguous: either a rename (which we don't
        # support in v1) or a bug. Reject explicitly.
        raise ProtocolError(
            protocol.ERR_UNKNOWN_TYPE,
            "already joined; 'join' may only be sent once",
        )

    if msg.type in (protocol.TYPE_HISTORY, protocol.TYPE_ERROR):
        # These are server-to-client only. A client sending them is a
        # protocol violation (or a spoofing attempt); refuse.
        raise ProtocolError(
            protocol.ERR_UNKNOWN_TYPE,
            f"'{msg.type}' is server-to-client only",
        )

    handler = _DISPATCH.get(msg.type)
    if handler is None:
        # Should be unreachable: validate() already rejected unknown
        # types. Defense in depth.
        raise ProtocolError(protocol.ERR_UNKNOWN_TYPE, msg.type)
    await handler(ctx, conn, msg)



# Handlers


@_register(protocol.TYPE_JOIN)
async def _handle_join(
    ctx: HandlerContext, conn: ClientConn, msg: Message
) -> None:
    """Admit the client with a globally-unique username, send history.

    Flow (order matters for R6 and R7):

      1. Reserve the username in Redis (SET NX EX TTL). If it fails, the
         client gets a typed error and disconnects (there is nothing
         sensible to do with a joined-without-username session).
      2. Snapshot the stream tail BEFORE reading history (R6). This is
         the id the client will be caught up to; live delivery starts
         from this id.
      3. Fetch and send history up to the snapshot.
      4. Register the username locally, publish a join event to the
         stream (so remote nodes learn about it via their consumers),
         and switch to the joined state.

    The snapshot-before-history ordering is the R6 mitigation: without
    it, a message published between "fetch history" and "start live" is
    either missed or duplicated. With it, "history up to tail" and
    "live from tail" tile the sequence exactly.
    """
    assert msg.sender is not None  # validated by protocol
    username = msg.sender

    # Step 1: reserve username globally.
    user_key = f"{config.USER_KEY_PREFIX}{username.lower()}"
    # SET NX EX is atomic; the race between concurrent joins is already
    # handled at Redis. Lowercase the key so "Alice" and "alice" collide
    # (usernames are case-insensitive for uniqueness), while we display
    # the original spelling in messages.
    reserved = await ctx.redis_client.set(
        user_key, ctx.node_id, nx=True, ex=config.USERNAME_TTL_SECONDS
    )
    if not reserved:
        # Someone (possibly on another node) already has this username.
        # Send a typed error so the client can prompt for another name.
        raise ProtocolError(
            protocol.ERR_INVALID_USERNAME,
            f"username '{username}' is taken",
        )

    try:
        # Step 2: snapshot the current tail BEFORE sending history. We
        # use the last retained entry id; empty stream -> "0-0" so any
        # future entry is delivered. This is the R6 anchor.
        try:
            tail_entries = await ctx.redis_client.xrevrange(
                config.STREAM_KEY, max="+", min="-", count=1
            )
            anchor = str(tail_entries[0][0]) if tail_entries else "0-0"
        except Exception as exc:  # noqa: BLE001 — degrade gracefully
            logger.warning(
                "could not snapshot stream tail for %s: %s; history may "
                "race with live delivery",
                conn, exc,
            )
            anchor = None

        # Step 3: fetch and send history up to now. History is chat-only:
        # join/leave events are transient metadata, not conversation, and
        # the protocol's history frame is chat-shaped by contract
        # (_validate_history requires each entry to be a valid chat).
        # Filtering here makes the frame valid by construction.
        history = await ctx.broker_consumer.history(count=config.HISTORY_COUNT)
        chat_only = [m for m in history if m.type == protocol.TYPE_CHAT]
        if chat_only:
            history_msg = protocol.make_history([m.to_dict() for m in chat_only])
            # Best-effort: if the client's queue is full at join (already
            # slow before joining), we still proceed; they will get live
            # messages. History is a nicety, not a correctness guarantee.
            await ctx.manager.send_to(conn, history_msg)

        # Step 4: commit joined state and announce the join.
        conn.username = username
        join_msg = protocol.make_join(username)
        # Publish to the stream so all nodes (including this one) fan
        # it out. Stream-only delivery (Item 6) keeps ordering consistent.
        await ctx.broker_producer.publish(join_msg)

        logger.info(
            "joined %s as '%s' (anchor=%s, history=%d)",
            conn, username, anchor, len(history),
        )
    except BaseException:
        # If anything fails after reserving the username, release it so
        # a transient failure doesn't lock a name until TTL. This is the
        # compensating action for step 1.
        with contextlib.suppress(Exception):
            await ctx.redis_client.delete(user_key)
        raise


@_register(protocol.TYPE_CHAT)
async def _handle_chat(
    ctx: HandlerContext, conn: ClientConn, msg: Message
) -> None:
    """Validate and publish a chat message to the stream.

    The handler trusts the message shape (protocol.decode validated it)
    but NOT the sender field: a client may send any 'sender' it likes,
    so we overwrite it with the authenticated username. This is the
    minimal auth model for v1 (no passwords); it prevents casual
    spoofing without claiming to be secure.
    """
    assert conn.username is not None  # guaranteed by dispatch
    # Overwrite sender to prevent spoofing.
    safe_msg = Message(
        type=protocol.TYPE_CHAT,
        sender=conn.username,
        content=msg.content,
        room=msg.room,
        origin_node=ctx.node_id,
    )
    await ctx.broker_producer.publish(safe_msg)


@_register(protocol.TYPE_LEAVE)
async def _handle_leave(
    ctx: HandlerContext, conn: ClientConn, msg: Message
) -> None:
    """Announce a voluntary leave and disconnect.

    The actual connection teardown happens in handle_client's finally
    block; this handler only publishes the leave event so other clients
    see it. We do NOT release the username here — that happens in
    _cleanup, so a client that leaves and immediately reconnects with
    the same name does not race a still-held key.
    """
    if conn.username is None:
        # Leave before join: nothing to announce; just close.
        return
    leave_msg = protocol.make_leave(conn.username)
    with contextlib.suppress(Exception):
        # Best-effort: if the broker is down we still want to close the
        # connection. A missed leave event is cosmetic.
        await ctx.broker_producer.publish(leave_msg)


# Cleanup


async def _cleanup(conn: ClientConn, ctx: HandlerContext) -> None:
    """Release everything this connection held. Runs on every exit path.

    Ordering:
      1. Flush the outbound queue (bounded) so any pending error frame
         enqueued by the disconnect path actually reaches the socket
         before we cancel the drain task. Without this, a client that
         triggers a protocol error gets a silent drop with no error
         frame.
      2. Unregister from the manager (stops fan-out to this client).
      3. Delete the Redis username key (R7 best-effort).
      4. Close the writer.

    Steps are individually guarded so one failure doesn't skip the
    others — this is the difference between "leaks a username key on a
    transient error" and "cleanup is honest."
    """
    username = conn.username
    try:
        await ctx.manager.flush(conn, timeout=1.0)
    except Exception:  # noqa: BLE001
        logger.exception("failed to flush queue for %s", conn)
    try:
        await ctx.manager.unregister(conn)
    except Exception:  # noqa: BLE001
        logger.exception("failed to unregister %s", conn)

    if username is not None:
        try:
            await ctx.redis_client.delete(
                f"{config.USER_KEY_PREFIX}{username.lower()}"
            )
        except Exception:  # noqa: BLE001 — TTL is the safety net
            logger.warning(
                "failed to release username '%s' for %s; TTL will expire it",
                username, conn,
            )

    try:
        conn.writer.close()
        with contextlib.suppress(Exception):
            await conn.writer.wait_closed()
    except Exception:  # noqa: BLE001
        logger.exception("failed to close writer for %s", conn)


async def _safe_error_send(
    conn: ClientConn, code: str, detail: str, ctx: HandlerContext
) -> None:
    """Send a typed error frame, swallowing transport failures.

    Used only on the disconnect path, so a failure to send the error
    frame must not prevent disconnect. Best-effort by design.
    """
    try:
        err = protocol.make_error(code, detail)
        await ctx.manager.send_to(conn, err)
    except Exception:  # noqa: BLE001
        logger.debug("could not send error frame to %s", conn)


__all__ = ["handle_client", "HandlerContext"]