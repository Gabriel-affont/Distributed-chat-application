"""
main.py — composition root for one chat server node.

This is the ONLY file that knows how all the pieces fit together. It
imports every server module and constructs them in dependency order,
then runs the server until a signal arrives. Handlers, broker, and
manager stay testable in isolation precisely because none of them
import this file 

Lifecycle, in order:

  1. Configure logging (INFO default; DEBUG via CHAT_LOG_LEVEL).
  2. Build the runtime: Redis client, LastSeenStore, StreamProducer,
     StreamConsumer, ConnectionManager, HandlerContext.
  3. Preflight: PING Redis. Fail loudly if unreachable (R12, R19).
  4. Start the consumer as a background task, bound to manager.broadcast.
  5. Start the asyncio TCP server with the reader limit = MAX_LINE_BYTES.
  6. Wait for SIGINT/SIGTERM, then tear down in reverse.

Teardown order matters (R2):
  consumer.stop() -> await consumer task (flushes cursor)
  server.close()  -> await wait_closed (stops accepting)
  cancel handler tasks, close Redis client

Env overrides (see config.py):
  CHAT_NODE_ID, CHAT_HOST, CHAT_PORT, REDIS_URL, CHAT_DATA_DIR,
  CHAT_LOG_LEVEL, CHAT_STREAM_KEY, CHAT_USER_PREFIX
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
from dataclasses import dataclass

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError

import config
import protocol
from server.broker import StreamConsumer, StreamProducer
from server.connection_manager import ConnectionManager
from server.handler import HandlerContext, handle_client
from server.state import LastSeenStore

logger = logging.getLogger("chat.server")



# Runtime bundle 
@dataclass
class Runtime:
    """All the long-lived objects for one node, constructed once.

    Grouping them here (rather than in locals of run_node) makes the
    teardown path explicit and lets future health-check endpoints /
    test harnesses get at them without globals.
    """

    redis_client: redis.Redis
    store: LastSeenStore
    producer: StreamProducer
    consumer: StreamConsumer
    manager: ConnectionManager
    ctx: HandlerContext
    consumer_task: asyncio.Task[None]


# Logging


def configure_logging() -> None:
    """Configure root logging once, before anything else logs.

    Format includes the node id so two-node dev output is distinguishable
    when logs from node1 and node2 interleave in a terminal.
    """
    level_name = (config.__dict__.get("LOG_LEVEL") or "INFO").upper()
    # config doesn't define LOG_LEVEL as a constant (kept simple); read
    # the env var directly to avoid touching config.py for one knob.
    import os
    level_name = os.environ.get("CHAT_LOG_LEVEL", level_name).upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format=(
            f"%(asctime)s %(levelname)-7s [node={config.NODE_ID}] "
            "%(name)s: %(message)s"
        ),
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )



# Construction

async def build_runtime() -> Runtime:
    """Construct every long-lived object, in dependency order.

    Fails fast if Redis is unreachable: a node that cannot talk to the
    stream relay cannot serve chat, and starting anyway produces a
    confusing half-broken experience. Clear error, non-zero exit.
    """
    # decode_responses=True: the whole stack expects str (POC-1 lesson,
    # documented in config). Applied once here so broker, handler, and
    # the preflight all agree.
    redis_client = redis.from_url(config.REDIS_URL, decode_responses=True)

    # Preflight: PING Redis now with a short timeout. If we get here and
    # Redis is down, the deployment is misconfigured (R12/R19); log a
    # clear message and re-raise so run_node exits non-zero.
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=5.0)
    except (RedisConnectionError, asyncio.TimeoutError, OSError) as exc:
        logger.error(
            "cannot reach Redis at %s (%s). Is `docker compose up -d` "
            "running, or REDIS_URL correct?",
            config.REDIS_URL, exc,
        )
        await redis_client.aclose()
        raise

    store = LastSeenStore(config.DATA_DIR, config.NODE_ID)
    producer = StreamProducer(
        redis_client, stream_key=config.STREAM_KEY, maxlen=config.STREAM_MAXLEN
    )
    consumer = StreamConsumer(
        redis_client, store,
        stream_key=config.STREAM_KEY, block_ms=config.CONSUMER_BLOCK_MS,
    )
    manager = ConnectionManager(queue_maxsize=config.CLIENT_QUEUE_MAXSIZE)
    ctx = HandlerContext(
        broker_producer=producer,
        broker_consumer=consumer,
        manager=manager,
        redis_client=redis_client,
        node_id=config.NODE_ID,
    )

    # Consumer task is created here (before the TCP server starts) so
    # handler joins never race an unstarted consumer. bind manager.broadcast
    # as the fan-out callback — this is the seam from Diagram 2.
    consumer_task = asyncio.create_task(
        consumer.run(manager.broadcast),
        name=f"consumer-{config.NODE_ID}",
    )

    return Runtime(
        redis_client=redis_client,
        store=store,
        producer=producer,
        consumer=consumer,
        manager=manager,
        ctx=ctx,
        consumer_task=consumer_task,
    )



# Signal handling

def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Install SIGINT/SIGTERM handlers that set *stop_event*.

    Idempotent: calling this twice would double-register; we only call
    it once from run_node. On Windows, signal.SIGTERM is available but
    only SIGINT (Ctrl-C) is reliably delivered; SIGTERM support here
    is a no-op on Windows but valuable if you later deploy to Linux.
    """

    def _trigger() -> None:
        logger.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger)
        except (NotImplementedError, RuntimeError):
            # Windows ProactorEventLoop doesn't support add_signal_handler
            # for SIGTERM; fall back to signal.signal for SIGINT.
            try:
                signal.signal(sig, lambda *_: _trigger())
            except (ValueError, OSError):
                # Some signals cannot be trapped; keep going.
                pass



# Run loop

async def run_node() -> None:
    """Build, serve, and tear down one node. The public entry point."""
    configure_logging()
    logger.info(
        "starting node %s on %s:%d (stream=%s)",
        config.NODE_ID, config.HOST, config.PORT, config.STREAM_KEY,
    )

    runtime: Runtime | None = None
    server: asyncio.AbstractServer | None = None
    stop_event = asyncio.Event()

    try:
        runtime = await build_runtime()
        _install_signal_handlers(stop_event)

        # Start the TCP server. The `limit` MUST match MAX_LINE_BYTES so
        # oversize lines surface as LimitOverrunError at the transport
        # layer (protocol.MAX_LINE_BYTES is the same constant; see
        # protocol.py for the defense-in-depth rationale).
        server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, ctx=runtime.ctx),
            host=config.HOST,
            port=config.PORT,
            limit=protocol.MAX_LINE_BYTES,
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
        logger.info("listening on %s", addrs)
        logger.info("ready (Ctrl-C to stop)")

        # Serve until a signal arrives. `start_server` returns once the
        # server is listening; we await a shutdown event.
        async with server:
            await stop_event.wait()

    except asyncio.CancelledError:
        # External cancellation (e.g., tests). Treated like a signal.
        logger.info("cancelled; shutting down")
    except Exception:
        logger.exception("fatal error in run_node")
        raise
    finally:
        await _teardown(runtime, server)


async def _teardown(
    runtime: Runtime | None, server: asyncio.AbstractServer | None
) -> None:
    """Ordered shutdown. Every step is guarded so one failure can't skip
    the rest (a leaked Redis client or consumer task matters).

    Order (R2):
      1. Stop the consumer and await its task (flushes the cursor via
         the ordering inside StreamConsumer.run + finally).
      2. Stop accepting new connections (server.close + wait_closed).
      3. Close Redis (after the consumer has flushed).
    """
    if runtime is None:
        return

    # 1. Stop the consumer first so we don't deliver during teardown.
    try:
        await runtime.consumer.stop()
        await asyncio.wait_for(runtime.consumer_task, timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("consumer did not stop within 5s; cancelling")
        runtime.consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await runtime.consumer_task
    except Exception:  # noqa: BLE001
        logger.exception("error while stopping consumer")

    # 2. Stop accepting new connections.
    if server is not None:
        try:
            server.close()
            await server.wait_closed()
        except Exception:  # noqa: BLE001
            logger.exception("error while closing server")

    # 3. Close the Redis client last — the consumer needed it to finish
    #    flushing, and handlers may still be draining error frames.
    try:
        await runtime.redis_client.aclose()
    except Exception:  # noqa: BLE001
        logger.exception("error while closing redis client")

    logger.info("node %s stopped", config.NODE_ID)


def main() -> int:
    """Sync entry point for `python -m server.main`. Returns exit code.

    Using `asyncio.run` here keeps the top level free of event-loop
    plumbing and matches how the client (client/main.py) will start.
    """
    try:
        asyncio.run(run_node())
        return 0
    except KeyboardInterrupt:
        # Belt-and-braces: if a signal handler is unavailable (Windows),
        # the KeyboardInterrupt path still produces a clean exit.
        return 0
    except Exception:
        # Logged already in run_node; just signal failure.
        return 1


if __name__ == "__main__":
    raise SystemExit(main())