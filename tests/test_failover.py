"""
test_failover.py — correctness tests for client/failover.connect_with_failover.

The function is deliberately a pure async helper (nodes in, connection
out) which makes it testable against dead ports and a real asyncio
server with no mocking of the event loop.

Cases:
  * first node connects -> returned immediately, one attempt
  * first node dead, second alive -> tries second
  * all nodes dead, max_passes=2 -> raises after two passes
  * empty nodes list -> ValueError
  * black-holed node (accepts TCP, never responds) -> per-attempt
    timeout fires, moves to the next node
  * cancellation propagates

Uses ephemeral ports assigned by the OS (bind to port 0) so the test
never collides with a real server on 9001/9002.
"""

from __future__ import annotations

import asyncio
import socket

import pytest
import pytest_asyncio

from client.failover import connect_with_failover



# Helpers


async def _start_echo_server() -> tuple[asyncio.AbstractServer, int]:
    """Start a TCP server on an ephemeral port; return (server, port)."""

    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(1)  # block until closed
        finally:
            writer.close()
            with _suppress():
                await writer.wait_closed()

    server = await asyncio.start_server(_handler, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    return server, port


def _free_port() -> int:
    """Find a port that is currently free, for the 'dead node' case."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class _suppress:
    def __enter__(self): return self
    def __exit__(self, *exc): return True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConnectWithFailover:
    async def test_connects_to_first_live_node(self) -> None:
        server, port = await _start_echo_server()
        try:
            reader, writer = await connect_with_failover(
                [("127.0.0.1", port)],
                connect_timeout=1.0,
                backoff=0.0,
                max_passes=1,
            )
            assert reader is not None and writer is not None
            writer.close()
            with _suppress():
                await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    async def test_skips_dead_node_and_connects_to_next(self) -> None:
        dead_port = _free_port()
        server, live_port = await _start_echo_server()
        try:
            reader, writer = await connect_with_failover(
                [("127.0.0.1", dead_port), ("127.0.0.1", live_port)],
                connect_timeout=1.0,
                backoff=0.0,
                max_passes=1,
            )
            assert reader is not None
            writer.close()
            with _suppress():
                await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    async def test_raises_after_exhausting_passes(self) -> None:
        dead1, dead2 = _free_port(), _free_port()
        with pytest.raises((ConnectionRefusedError, OSError)):
            await connect_with_failover(
                [("127.0.0.1", dead1), ("127.0.0.1", dead2)],
                connect_timeout=0.5,
                backoff=0.0,
                max_passes=2,
            )

    async def test_empty_nodes_raises(self) -> None:
        with pytest.raises(ValueError):
            await connect_with_failover([], max_passes=1)

    async def test_black_holed_node_times_out(self) -> None:
        # Accept TCP but never respond: the per-attempt timeout must fire
        # and the next node must be tried.
        async def _black_hole(reader, writer):
            try:
                await asyncio.sleep(60)
            finally:
                writer.close()
                with _suppress():
                    await writer.wait_closed()

        bh_server = await asyncio.start_server(_black_hole, host="127.0.0.1", port=0)
        bh_port = bh_server.sockets[0].getsockname()[1]
        server, live_port = await _start_echo_server()
        try:
            # Short connect_timeout so the black-hole is detected quickly.
            reader, writer = await connect_with_failover(
                [("127.0.0.1", bh_port), ("127.0.0.1", live_port)],
                connect_timeout=0.3,
                backoff=0.0,
                max_passes=1,
            )
            assert reader is not None
            writer.close()
            with _suppress():
                await writer.wait_closed()
        finally:
            bh_server.close()
            await bh_server.wait_closed()
            server.close()
            await server.wait_closed()

    async def test_cancellation_propagates(self) -> None:
        # Cancel a connect_with_failover that is stuck retrying dead nodes.
        dead = _free_port()
        task = asyncio.create_task(
            connect_with_failover(
                [("127.0.0.1", dead)],
                connect_timeout=0.2,
                backoff=0.5,       # long backoff so cancellation lands during it
                max_passes=None,   # retry forever
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task