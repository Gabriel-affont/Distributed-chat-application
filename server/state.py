"""
state.py — atomic persistence of a node's last-seen Redis Stream ID.

This tiny module is the difference between "correct restart" and either a
replay storm or silent message gaps after a node bounces. It exists as its
own module (not a field on the consumer) for three reasons:

  1. Isolation: the atomic-write logic is the single critical correctness
     rule and deserves its own tests (test_state.py).
  2. Swappability: moving from a local file to a Redis key later is a
     one-file change behind the LastSeenStore interface.
  3. Testability: callers inject the data dir, so tests never touch the
     developer's real cursor file.

Correctness rules enforced here:
  * Writes are atomic (temp file + os.replace) so a crash mid-write leaves
    either the old value or the new value, never a truncated file.
  * Reads tolerate a missing file (first boot -> None).
  * Reads tolerate a corrupt file by returning None and logging a warning,
    so a damaged cursor never prevents the node from starting; the consumer
    then falls back to a safe default (see broker.py for the policy).
"""

from __future__ import annotations

#from curses import raw
import logging
import os

import re
import tempfile
from pathlib import Path

_STREAM_ID_RE = re.compile(r"^\d+-\d+$")

logger = logging.getLogger(__name__)



class LastSeenStore:
    """Reads/writes a single scalar (the last-seen stream ID) atomically.

    One instance per node. The file path is derived from the node ID so two
    nodes on the same machine never collide.
    """

    def __init__(self, data_dir: Path, node_id: str) -> None:
        self._data_dir = Path(data_dir)
        self._node_id = node_id
        self._path = self._data_dir / f"{node_id}.lastid"

    @property
    def path(self) -> Path:
        """Expose the path for logging and tests."""
        return self._path

    def load(self) -> str | None:
        """Return the persisted stream ID, or None if absent/corrupt.

        Never raises on missing or malformed content: a broken cursor file
        must not prevent startup. The caller decides the fallback policy.
        """
        try:
            raw = self._path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return None
        except OSError as exc:
            # Permission errors, etc. — log and fall back; a node that
            # cannot read its cursor should still start.
            logger.warning("could not read cursor file %s: %s", self._path, exc)
            return None

        if not raw:
            return None
        # A valid Redis Stream ID is "<ms>-<seq>": both parts are
        # non-negative integers (e.g. "1700000000000-0", or "0-0").
        # Reject anything else so we never hand a garbage value to XREAD.
        # The pattern is anchored so trailing junk fails too.
        if not _STREAM_ID_RE.fullmatch(raw):
            logger.warning("ignoring malformed cursor %r in %s", raw, self._path)
            return None
        return raw

    def save(self, stream_id: str) -> None:
        """Atomically persist *stream_id*.

        Uses the temp-file + os.replace idiom: os.replace is atomic on
        POSIX and on Windows, so a crash mid-write cannot
        produce a half-written cursor file.
        """
        if not stream_id or "-" not in stream_id:
            raise ValueError(f"refusing to persist invalid stream id: {stream_id!r}")

        self._data_dir.mkdir(parents=True, exist_ok=True)

        # NamedTemporaryFile in the SAME directory as the target: os.replace
        # across filesystems is not atomic, so the temp file must be on the
        # same filesystem as the destination.
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self._node_id}.lastid.",
            dir=self._data_dir,
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(stream_id)
                f.flush()
                # fsync the file so the bytes hit disk before the rename;
                # otherwise a power loss can leave an empty replaced file.
                os.fsync(f.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            # Clean up the temp file on any failure, including cancellation.
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise