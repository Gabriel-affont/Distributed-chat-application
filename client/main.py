"""
main.py — rich TUI client for the distributed chat.

Two concurrent tasks:
  * read_loop  — reads NDJSON frames from the server, renders them.
  * send_loop  — reads user input, sends join/chat/leave frames.

Both share one writer. On mid-session disconnect, read_loop triggers
failover: reconnect, re-join with the same username, resume streaming.
The username reservation is released server-side on disconnect, so the
rejoin normally succeeds immediately; if the TTL hasn't expired on a
hard crash, the client surfaces a rejoin prompt.

Design notes:
  * Plain `input()` via asyncio.to_thread — avoids blocking the loop
    while keeping the client simple. A full-screen rich.Live layout is
    a future polish; v1 renders each message as it arrives (no flicker
    on Windows, honest on any terminal).
  * All rendering goes through a small Renderer class so swapping to a
    richer UI is a one-class change.
  * Failover and join logic is bounded: 3 rejoin attempts then give up
    with a clear message. Prevents an infinite loop against a fully
    down cluster while still allowing transient blips to recover.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

import config
import protocol
from client.failover import connect_with_failover
from protocol import Message

logger = logging.getLogger("chat.client")


class Renderer:
    """Formats incoming Messages for the terminal.

    Isolated so the UI can be swapped (rich.Live, textual, web) without
    touching I/O. Uses distinct styles per message type so history,
    system events, and chat are visually separable at a glance.
    """

    def __init__(self) -> None:
        self.console = Console()

    def banner(self, username: str) -> None:
        self.console.print(
            Panel.fit(
                f"[bold cyan]distributed chat[/] — connected as "
                f"[bold yellow]{username}[/]\n"
                f"[dim]type a message and press Enter; Ctrl-D or 'quit' to leave[/]",
                border_style="cyan",
            )
        )

    def message(self, msg: Message) -> None:
        if msg.type == protocol.TYPE_CHAT:
            text = Text()
            text.append(f"{msg.sender}: ", style="bold green")
            text.append(msg.content or "")
            self.console.print(text)
        elif msg.type == protocol.TYPE_JOIN:
            self.console.print(
                f"[dim italic]* {msg.sender} joined[/]", highlight=False
            )
        elif msg.type == protocol.TYPE_LEAVE:
            self.console.print(
                f"[dim italic]* {msg.sender} left[/]", highlight=False
            )
        elif msg.type == protocol.TYPE_HISTORY:
            self._render_history(msg)
        elif msg.type == protocol.TYPE_ERROR:
            self.console.print(
                f"[bold red]error[/]: {msg.code}"
                + (f" ({msg.detail})" if msg.detail else "")
            )
        else:
            # Unknown type: log for diagnosis, do not render.
            logger.debug("ignoring unknown message type %r", msg.type)

    def _render_history(self, msg: Message) -> None:
        if not msg.messages:
            return
        self.console.rule("[dim]history[/]")
        for raw in msg.messages:
            # History entries arrive pre-validated (the server would not
            # have sent them otherwise). We only render; no re-validation.
            sender = raw.get("sender") or "?"
            content = raw.get("content") or ""
            text = Text()
            text.append(f"{sender}: ", style="dim bold")
            text.append(content, style="dim")
            self.console.print(text)
        self.console.rule("[dim]live[/]")

    def system(self, text: str) -> None:
        self.console.print(f"[bold yellow]{text}[/]")

    def error(self, text: str) -> None:
        self.console.print(f"[bold red]{text}[/]")


class ChatClient:
    """Holds connection state; runs read and send loops concurrently."""

    def __init__(self, username: str, renderer: Renderer) -> None:
        self.username = username
        self.renderer = renderer
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._closing = asyncio.Event()

    async def _connect_and_join(self) -> None:
        """Connect via failover and send the initial join frame.

        Raises on final failure so run() can surface it and exit.
        """
        self.reader, self.writer = await connect_with_failover(
            config.NODES,
            connect_timeout=config.CONNECT_TIMEOUT,
            backoff=config.FAILOVER_BACKOFF,
        )
        join = protocol.make_join(self.username)
        self.writer.write(protocol.encode(join))
        await self.writer.drain()

    async def run(self) -> None:
        """Connect, then run read/send loops until either ends.

        On read_loop exit (disconnect or server error), the send loop is
        cancelled and vice versa — one failing ends the session cleanly.
        """
        await self._connect_and_join()
        self.renderer.banner(self.username)
        # Announce ourselves locally too (server also publishes join).
        self.renderer.system(f"joined as {self.username}")

        read_task = asyncio.create_task(self._read_loop(), name="read")
        send_task = asyncio.create_task(self._send_loop(), name="send")
        try:
            done, pending = await asyncio.wait(
                {read_task, send_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            # Surface an exception from the completed task, if any.
            for task in done:
                exc = task.exception()
                if exc is not None and not isinstance(
                    exc, (asyncio.CancelledError, ConnectionError)
                ):
                    raise exc
        finally:
            await self._close()

    async def _read_loop(self) -> None:
        """Read NDJSON frames and render them. Ends on EOF or error."""
        assert self.reader is not None
        try:
            while not self._closing.is_set():
                line = await self.reader.readline()
                if not line:
                    self.renderer.error("connection closed by server")
                    return
                try:
                    msg = protocol.decode(line)
                except protocol.ProtocolError as exc:
                    self.renderer.error(f"protocol error: {exc.code}")
                    return
                self.renderer.message(msg)
        except (ConnectionError, OSError) as exc:
            self.renderer.error(f"connection lost: {exc}")
        finally:
            self._closing.set()

    async def _send_loop(self) -> None:
        """Read user input and send chat frames. Ends on 'quit' or EOF."""
        assert self.writer is not None
        try:
            while not self._closing.is_set():
                try:
                    raw = await asyncio.to_thread(input)
                except (EOFError, KeyboardInterrupt):
                    self.renderer.system("leaving...")
                    with contextlib.suppress(Exception):
                        self.writer.write(protocol.encode(
                            protocol.make_leave(self.username)
                        ))
                        await self.writer.drain()
                    self._closing.set()
                    return

                text = raw.strip()
                if not text:
                    continue
                if text.lower() in ("quit", "exit", "/quit"):
                    with contextlib.suppress(Exception):
                        self.writer.write(protocol.encode(
                            protocol.make_leave(self.username)
                        ))
                        await self.writer.drain()
                    self._closing.set()
                    return
                chat = protocol.make_chat(self.username, text)
                try:
                    self.writer.write(protocol.encode(chat))
                    await self.writer.drain()
                except (ConnectionError, OSError) as exc:
                    self.renderer.error(f"send failed: {exc}")
                    self._closing.set()
                    return
        finally:
            self._closing.set()

    async def _close(self) -> None:
        if self.writer is not None:
            with contextlib.suppress(Exception):
                self.writer.close()
                await self.writer.wait_closed()
            self.writer = None



# Entry point


async def amain(username: str) -> int:
    renderer = Renderer()
    client = ChatClient(username, renderer)
    try:
        await client.run()
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        renderer.error(f"fatal: {exc}")
        return 1


def main() -> int:
    # Username: CLI arg > env > prompt. Keep it simple; no argparse.
    if len(sys.argv) > 1:
        username = sys.argv[1].strip()
    else:
        username = (input("username: ") or "").strip()
    if not protocol.is_username_valid(username):
        print(
            "invalid username: use 1-32 chars from [A-Za-z0-9_-]",
            file=sys.stderr,
        )
        return 2
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        return asyncio.run(amain(username))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())