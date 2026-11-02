"""
test_state.py — correctness tests for LastSeenStore.

The atomic-write rule is the single most important correctness property in
the cursor subsystem (R2). These tests pin it and the corruption-tolerance
contract so a refactor cannot silently regress them.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from server.state import LastSeenStore


@pytest.fixture
def store(tmp_path: Path) -> LastSeenStore:
    return LastSeenStore(data_dir=tmp_path, node_id="node1")


class TestLoad:
    def test_missing_file_returns_none(self, store: LastSeenStore) -> None:
        assert store.load() is None

    def test_empty_file_returns_none(self, store: LastSeenStore) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text("", encoding="utf-8")
        assert store.load() is None

    def test_valid_id_roundtrip(self, store: LastSeenStore) -> None:
        store.save("1700000000000-0")
        assert store.load() == "1700000000000-0"

    def test_whitespace_stripped_on_load(self, store: LastSeenStore) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text("  1700000000000-0\n", encoding="utf-8")
        assert store.load() == "1700000000000-0"

    def test_malformed_returns_none(self, store: LastSeenStore) -> None:
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text("not-a-stream-id", encoding="utf-8")
        assert store.load() is None

    def test_unreadable_file_returns_none(
        self, store: LastSeenStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate a read OSError (e.g. permissions); must not raise.
        store.path.parent.mkdir(parents=True, exist_ok=True)
        store.path.write_text("1700000000000-0", encoding="utf-8")

        real_read = Path.read_text

        def boom(self: Path, *args, **kwargs):  # type: ignore[no-untyped-def]
            if self == store.path:
                raise OSError("permission denied")
            return real_read(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", boom)
        assert store.load() is None


class TestSave:
    def test_save_rejects_invalid_id(self, store: LastSeenStore) -> None:
        with pytest.raises(ValueError):
            store.save("garbage")
        with pytest.raises(ValueError):
            store.save("")

    def test_save_creates_data_dir(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b" / "c"
        s = LastSeenStore(data_dir=nested, node_id="node1")
        s.save("1-0")
        assert (nested / "node1.lastid").read_text(encoding="utf-8") == "1-0"

    def test_save_overwrites_atomically(self, store: LastSeenStore) -> None:
        store.save("1-0")
        store.save("2-0")
        assert store.load() == "2-0"

    def test_no_temp_files_left_behind(self, store: LastSeenStore) -> None:
        # The atomic-write idiom must clean up its temp file. A leftover
        # temp file would indicate an exception path leaked it.
        store.save("1-0")
        store.save("2-0")
        leftovers = [p for p in store.path.parent.iterdir()
                     if p.name.startswith(".")]
        assert leftovers == []

    def test_crash_during_write_leaves_old_value(
        self, store: LastSeenStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate a crash between write and os.replace: the previous value
        # must remain intact and no truncated file may appear.
        store.save("1-0")

        real_replace = os.replace

        def boom(src, dst):  # type: ignore[no-untyped-def]
            raise RuntimeError("simulated crash")

        monkeypatch.setattr(os, "replace", boom)
        with pytest.raises(RuntimeError):
            store.save("2-0")
        # Restore for cleanup assertions.
        monkeypatch.setattr(os, "replace", real_replace)

        assert store.load() == "1-0"
        leftovers = [p for p in store.path.parent.iterdir()
                     if p.name.startswith(".")]
        assert leftovers == []

    def test_two_nodes_do_not_collide(self, tmp_path: Path) -> None:
        n1 = LastSeenStore(data_dir=tmp_path, node_id="node1")
        n2 = LastSeenStore(data_dir=tmp_path, node_id="node2")
        n1.save("1-0")
        n2.save("9-0")
        assert n1.load() == "1-0"
        assert n2.load() == "9-0"