"""
test_protocol.py — contract tests for protocol.py.

These tests are the executable specification of the wire format. They exist
to catch two classes of failure before they reach three consumers:

  * R1  (TCP framing)    — adversarial line shapes (split writes, CRLF,
                           whitespace, empty lines, oversize, bad UTF-8).
  * R15 (protocol drift) — every ERR_* code has at least one test that
                           triggers it, and every factory round-trips.

If a schema change lands without a corresponding change here, the contract
has silently evolved — treat a green suite with no diff as a review flag.
"""

from __future__ import annotations

import json

import pytest

import protocol
from protocol import (
    ALL_MESSAGE_TYPES,
    ERR_BAD_FIELD_TYPE,
    ERR_CONTENT_TOO_LONG,
    ERR_EMPTY_LINE,
    ERR_INVALID_USERNAME,
    ERR_LINE_TOO_LONG,
    ERR_MALFORMED_JSON,
    ERR_MISSING_FIELD,
    ERR_NOT_AN_OBJECT,
    ERR_UNKNOWN_TYPE,
    ERR_VERSION_MISMATCH,
    MAX_CONTENT_CHARS,
    MAX_HISTORY_BATCH,
    MAX_LINE_BYTES,
    PROTOCOL_VERSION,
    TYPE_CHAT,
    TYPE_ERROR,
    TYPE_HISTORY,
    TYPE_JOIN,
    TYPE_LEAVE,
    Message,
    ProtocolError,
    check_version,
    decode,
    encode,
    is_username_valid,
    make_chat,
    make_error,
    make_history,
    make_join,
    make_leave,
    validate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_chat_dict(**overrides) -> dict:
    """A minimal valid TYPE_CHAT payload, overridable per test.

    Centralizing the baseline means each rejection test only states the
    ONE thing it is breaking, which makes failures self-explanatory.
    """
    base = {
        "type": TYPE_CHAT,
        "version": PROTOCOL_VERSION,
        "sender": "alice",
        "content": "hello",
        "ts": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _assert_code(exc: ProtocolError, code: str) -> None:
    """Assert a ProtocolError carries the expected machine-readable code."""
    assert exc.code == code, f"expected {code!r}, got {exc.code!r} ({exc})"


# ---------------------------------------------------------------------------
# Round-trip: encode/decode are exact inverses for every factory
# ---------------------------------------------------------------------------

class TestRoundTrip:
    """Every valid Message must survive encode -> decode unchanged.

    This is the core invariant of the module. If it holds for the factories
    (the intended construction paths), callers who use them cannot produce
    a wire form the decoder rejects.
    """

    def test_chat_round_trip(self) -> None:
        msg = make_chat("alice", "hello world")
        decoded = decode(encode(msg))
        assert decoded == msg

    def test_join_round_trip(self) -> None:
        msg = make_join("bob")
        decoded = decode(encode(msg))
        assert decoded == msg

    def test_leave_round_trip(self) -> None:
        msg = make_leave("carol")
        decoded = decode(encode(msg))
        assert decoded == msg

    def test_error_round_trip(self) -> None:
        msg = make_error(ERR_INVALID_USERNAME, "bad name")
        decoded = decode(encode(msg))
        assert decoded == msg

    def test_history_round_trip(self) -> None:
        entries = [_valid_chat_dict(), _valid_chat_dict(content="second")]
        msg = make_history(entries)
        decoded = decode(encode(msg))
        assert decoded == msg

    def test_chat_with_origin_node_and_room_round_trip(self) -> None:
        msg = make_chat("alice", "hi", origin_node="node1", room="lobby")
        decoded = decode(encode(msg))
        assert decoded.origin_node == "node1"
        assert decoded.room == "lobby"

    def test_default_room_is_not_emitted_on_wire(self) -> None:
        # 'general' is the default; emitting it would break v1 peers that
        # assume a single global room (see Message.to_dict docstring).
        wire = encode(make_chat("alice", "hi"))
        assert b'"room"' not in wire

    def test_encode_returns_single_newline_terminated_line(self) -> None:
        wire = encode(make_chat("alice", "hi"))
        assert wire.endswith(b"\n")
        assert wire.count(b"\n") == 1


# ---------------------------------------------------------------------------
# Username validation
# ---------------------------------------------------------------------------

class TestUsernameValidation:
    """Grammar rules live in one place; test the boundaries explicitly."""

    @pytest.mark.parametrize("name", ["a", "alice", "A_b-9", "x" * 32])
    def test_accepts_valid(self, name: str) -> None:
        assert is_username_valid(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "",                    # empty
            "x" * 33,              # too long
            "has space",           # whitespace
            "has.dot",             # punctuation outside grammar
            "café",                # non-ASCII
            "line\nbreak",         # control char
            None,                  # non-str
            123,                   # non-str
            ["alice"],             # non-str
        ],
    )
    def test_rejects_invalid(self, name) -> None:
        assert is_username_valid(name) is False


# ---------------------------------------------------------------------------
# Validation: one test per ERR_* code
# ---------------------------------------------------------------------------

class TestValidationErrors:
    """Each rejection path gets at least one test, asserting the exact code.

    Asserting the code (not just that *some* error was raised) is what makes
    these tests catch drift: if a rule changes its error code, the test fails
    and forces an intentional decision.
    """

    def test_not_an_object(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(["not", "a", "dict"])  # type: ignore[arg-type]
        _assert_code(ei.value, ERR_NOT_AN_OBJECT)

    def test_missing_type(self) -> None:
        payload = _valid_chat_dict()
        del payload["type"]
        with pytest.raises(ProtocolError) as ei:
            validate(payload)
        _assert_code(ei.value, ERR_MISSING_FIELD)

    def test_unknown_type(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(type="teleport"))
        _assert_code(ei.value, ERR_UNKNOWN_TYPE)

    def test_missing_version(self) -> None:
        payload = _valid_chat_dict()
        del payload["version"]
        with pytest.raises(ProtocolError) as ei:
            validate(payload)
        _assert_code(ei.value, ERR_MISSING_FIELD)

    def test_version_mismatch(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(version=PROTOCOL_VERSION + 1))
        _assert_code(ei.value, ERR_VERSION_MISMATCH)

    def test_version_bool_is_rejected(self) -> None:
        # bool is a subclass of int; guard against True slipping through.
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(version=True))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_missing_sender(self) -> None:
        payload = _valid_chat_dict()
        del payload["sender"]
        with pytest.raises(ProtocolError) as ei:
            validate(payload)
        _assert_code(ei.value, ERR_MISSING_FIELD)

    def test_invalid_username(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(sender="bad name"))
        _assert_code(ei.value, ERR_INVALID_USERNAME)

    def test_missing_content(self) -> None:
        payload = _valid_chat_dict()
        del payload["content"]
        with pytest.raises(ProtocolError) as ei:
            validate(payload)
        _assert_code(ei.value, ERR_MISSING_FIELD)

    def test_empty_content_rejected(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(content=""))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_content_too_long(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(content="x" * (MAX_CONTENT_CHARS + 1)))
        _assert_code(ei.value, ERR_CONTENT_TOO_LONG)

    def test_content_at_limit_is_ok(self) -> None:
        # Boundary: exactly MAX_CONTENT_CHARS must be accepted.
        validate(_valid_chat_dict(content="x" * MAX_CONTENT_CHARS))

    def test_ts_wrong_type(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(ts=123))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_room_empty_rejected(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(room=""))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_origin_node_empty_rejected(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(origin_node=""))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)


# ---------------------------------------------------------------------------
# History-specific validation (recursive shape checks)
# ---------------------------------------------------------------------------

class TestHistoryValidation:
    """History is the one type with a nested payload; test the recursion."""

    def test_valid_batch(self) -> None:
        validate(_valid_chat_dict(type=TYPE_HISTORY,
                                  messages=[_valid_chat_dict()]))

    def test_entry_not_an_object(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(type=TYPE_HISTORY, messages=["nope"]))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_bad_entry_reports_index(self) -> None:
        # The detail must point at the offending index for actionable logs.
        bad = {"type": TYPE_CHAT, "version": PROTOCOL_VERSION,
               "sender": "bad name", "content": "x"}
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(type=TYPE_HISTORY,
                                      messages=[_valid_chat_dict(), bad]))
        _assert_code(ei.value, ERR_INVALID_USERNAME)
        assert "messages[1]" in ei.value.detail

    def test_batch_over_limit_rejected(self) -> None:
        entries = [_valid_chat_dict() for _ in range(MAX_HISTORY_BATCH + 1)]
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(type=TYPE_HISTORY, messages=entries))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)

    def test_make_history_truncates_oversized_batch(self) -> None:
        # Defense in depth: the factory must never emit an outbound frame
        # the receiver would reject.
        entries = [_valid_chat_dict(content=str(i))
                   for i in range(MAX_HISTORY_BATCH + 5)]
        msg = make_history(entries)
        assert len(msg.messages) == MAX_HISTORY_BATCH
        # Truncation keeps the most RECENT entries.
        assert msg.messages[-1]["content"] == str(MAX_HISTORY_BATCH + 4)


# ---------------------------------------------------------------------------
# Error type validation
# ---------------------------------------------------------------------------

class TestErrorValidation:
    def test_valid_error(self) -> None:
        validate(_valid_chat_dict(type=TYPE_ERROR, code="boom"))

    def test_missing_code(self) -> None:
        payload = _valid_chat_dict(type=TYPE_ERROR)
        with pytest.raises(ProtocolError) as ei:
            validate(payload)
        _assert_code(ei.value, ERR_MISSING_FIELD)

    def test_empty_code_rejected(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            validate(_valid_chat_dict(type=TYPE_ERROR, code=""))
        _assert_code(ei.value, ERR_BAD_FIELD_TYPE)


# ---------------------------------------------------------------------------
# Adversarial framing (R1)
# ---------------------------------------------------------------------------

class TestDecodeFraming:
    """Transport-adjacent edge cases that surface as ProtocolError at the
    codec layer. Each maps to a real TCP failure mode."""

    def test_crlf_tolerated(self) -> None:
        wire = encode(make_chat("alice", "hi")).rstrip(b"\n") + b"\r\n"
        assert decode(wire).content == "hi"

    def test_no_trailing_newline_tolerated(self) -> None:
        # A well-formed final line without newline (EOF edge) must parse.
        wire = encode(make_chat("alice", "hi")).rstrip(b"\n")
        assert decode(wire).content == "hi"

    def test_str_input_accepted(self) -> None:
        # Convenience path for tests/logs; must behave identically to bytes.
        wire = encode(make_chat("alice", "hi")).decode("utf-8")
        assert decode(wire).content == "hi"

    def test_empty_line(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            decode(b"")
        _assert_code(ei.value, ERR_EMPTY_LINE)

    def test_newline_only(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            decode(b"\n")
        _assert_code(ei.value, ERR_EMPTY_LINE)

    def test_leading_whitespace_rejected(self) -> None:
        # Usually signals a framing bug upstream (R1); fail loudly.
        with pytest.raises(ProtocolError) as ei:
            decode(b" " + encode(make_chat("alice", "hi")))
        _assert_code(ei.value, ERR_MALFORMED_JSON)

    def test_trailing_whitespace_rejected(self) -> None:
        wire = encode(make_chat("alice", "hi")).rstrip(b"\n") + b" \n"
        with pytest.raises(ProtocolError) as ei:
            decode(wire)
        _assert_code(ei.value, ERR_MALFORMED_JSON)

    def test_invalid_utf8(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            decode(b"\xff\xfe\x00\n")
        _assert_code(ei.value, ERR_MALFORMED_JSON)

    def test_malformed_json(self) -> None:
        with pytest.raises(ProtocolError) as ei:
            decode(b"{not json}\n")
        _assert_code(ei.value, ERR_MALFORMED_JSON)

    def test_oversize_rejected_before_parsing(self) -> None:
        # Must raise LINE_TOO_LONG, not MALFORMED_JSON: the size check runs
        # BEFORE json.loads so a huge payload cannot force a big allocation.
        huge = b"x" * (MAX_LINE_BYTES + 1)
        with pytest.raises(ProtocolError) as ei:
            decode(huge)
        _assert_code(ei.value, ERR_LINE_TOO_LONG)

    def test_oversize_encoded_message_rejected(self) -> None:
        # encode() must also refuse to produce an oversize line. Build a
        # Message directly (bypassing factories) with an oversize content.
        # Use a non-ASCII char to force UTF-8 expansion past the byte cap
        # even if content length is bounded.
        big = "é" * (MAX_CONTENT_CHARS)  # 2 bytes/char in UTF-8 -> 8 KiB
        # Not oversize yet; force it by padding the room field at validate
        # time is not possible (room is a str with no cap in v1), so instead
        # we test the byte cap via a crafted dict through validate/encode.
        # The realistic oversize path is decode(); this test documents that
        # encode() raises for a directly-constructed oversize Message.
        msg = Message(type=TYPE_CHAT, sender="alice", content=big,
                      room="r" * (MAX_LINE_BYTES))  # room pushes past cap
        with pytest.raises(ProtocolError) as ei:
            encode(msg)
        _assert_code(ei.value, ERR_LINE_TOO_LONG)


# ---------------------------------------------------------------------------
# Version helper
# ---------------------------------------------------------------------------

class TestCheckVersion:
    def test_current_version(self) -> None:
        assert check_version(make_chat("alice", "hi")) is True

    def test_future_version(self) -> None:
        msg = make_chat("alice", "hi")
        # Bypass frozen dataclass with replace to simulate a future peer.
        from dataclasses import replace
        assert check_version(replace(msg, version=PROTOCOL_VERSION + 1)) is False


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

class TestVocabularyCoverage:
    """Lightweight meta-tests that keep the module honest as it grows."""

    def test_every_declared_type_has_a_validator(self) -> None:
        # If someone adds TYPE_ROOM to ALL_MESSAGE_TYPES but forgets the
        # validator, this fails instead of the bug surfacing at runtime.
        for msg_type in ALL_MESSAGE_TYPES:
            payload = _valid_chat_dict(type=msg_type)
            try:
                validate(payload)
            except ProtocolError as exc:
                # VALID payloads must not raise; a missing required field
                # for THIS type is fine (it means the validator exists).
                assert exc.code != ERR_UNKNOWN_TYPE
                assert exc.code != "validator_missing"

    def test_public_api_is_exported(self) -> None:
        for name in protocol.__all__:
            assert hasattr(protocol, name), f"{name} in __all__ but undefined"