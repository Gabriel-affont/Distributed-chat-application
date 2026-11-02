"""
protocol.py — the shared NDJSON wire contract.

This module is the SINGLE source of truth for the wire format between
clients, server nodes, and the Redis Streams relay. It is deliberately
a LEAF dependency: it imports only the Python standard library and no
other project module. All other components import from here, never the
reverse.

Design invariants (see architecture analysis, Section 1):
  * encode() and decode() are exact inverses for every valid Message.
  * Every message carries PROTOCOL_VERSION; mismatches are rejected
    with a typed error before any further processing.
  * Unknown top-level keys are ignored on decode (forward-compatible
    schema evolution); missing required keys are rejected.
  * No I/O, no global mutable state, no async — pure functions only,
    so the whole module is trivially unit-testable.

Risk mitigations implemented here:
  * R1  (TCP framing)      -> strict line-oriented decode + size caps.
  * R15 (protocol drift)   -> one codec/validator, versioned schema.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Final


# Constants — the wire vocabulary


# Bump this whenever the wire format changes in a way that older peers
# could misinterpret. Receivers reject mismatches explicitly (safe evolution).
PROTOCOL_VERSION: Final[int] = 1

# Message type discriminators. Using module constants (not raw strings)
# centralizes the vocabulary so a typo becomes an AttributeError at import
# time rather than a silent runtime mismatch.
TYPE_CHAT: Final[str] = "chat"
TYPE_JOIN: Final[str] = "join"
TYPE_LEAVE: Final[str] = "leave"
TYPE_HISTORY: Final[str] = "history"
TYPE_ERROR: Final[str] = "error"

# The full set of types this protocol version accepts. Any other value
# in a decoded message is a ProtocolError (fail closed, never guess).
ALL_MESSAGE_TYPES: Final[frozenset[str]] = frozenset(
    {TYPE_CHAT, TYPE_JOIN, TYPE_LEAVE, TYPE_HISTORY, TYPE_ERROR}
)

# Hard limits — enforced identically on both ends of every connection.


# Maximum bytes for a single NDJSON line INCLUDING the trailing newline.
# This must stay <= the asyncio StreamReader `limit` used by the server,
# so an oversize line surfaces as LimitOverrunError at the transport layer
# AND as ProtocolError at the codec layer (defense in depth).
MAX_LINE_BYTES: Final[int] = 65_536  # 64 KiB

# Maximum characters in a chat message body. Kept far below the line cap
# so metadata (sender, ts, type, version) always fits alongside content.
MAX_CONTENT_CHARS: Final[int] = 4_096

# Maximum username length. Kept short so it is cheap to validate, log,
# and use as a Redis key suffix (chat:user:<name>).
MAX_USERNAME_LEN: Final[int] = 32

# Username grammar: letters, digits, underscore, hyphen. Deliberately
# ASCII-only to avoid Unicode normalization ambiguity in Redis keys and
# logs. Case-insensitive uniqueness is enforced by the caller (broker/
# handler), NOT here — this module only answers "is the name well-formed".
USERNAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

# Maximum number of history messages a server will send in one frame.
# Kept here because it is a protocol-level cap both ends must agree on.
MAX_HISTORY_BATCH: Final[int] = 50


# Errors


class ProtocolError(ValueError):
    """A message violated the wire contract.

    Subclasses ValueError so generic ``except ValueError`` clauses in
    callers still catch it, but remains a distinct type so handlers can
    distinguish "malformed wire data" (disconnect the peer) from
    "application error" (send an error frame).

    The ``code`` attribute is a short machine-readable string that the
    server may echo back to the client as {"type":"error","code":code}.
    """

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        # Human-readable message for logs; the code is what peers see.
        super().__init__(f"{code}: {detail}" if detail else code)


# Machine-readable error codes. These are part of the wire contract for
# error frames, so they live here with the rest of the vocabulary.
ERR_MALFORMED_JSON: Final[str] = "malformed_json"
ERR_NOT_AN_OBJECT: Final[str] = "not_an_object"
ERR_MISSING_FIELD: Final[str] = "missing_field"
ERR_UNKNOWN_TYPE: Final[str] = "unknown_type"
ERR_VERSION_MISMATCH: Final[str] = "version_mismatch"
ERR_INVALID_USERNAME: Final[str] = "invalid_username"
ERR_CONTENT_TOO_LONG: Final[str] = "content_too_long"
ERR_LINE_TOO_LONG: Final[str] = "line_too_long"
ERR_EMPTY_LINE: Final[str] = "empty_line"
ERR_BAD_FIELD_TYPE: Final[str] = "bad_field_type"


# Message model


@dataclass(frozen=True, slots=True)
class Message:
    """An immutable value object representing one wire message.

    ``frozen=True`` prevents accidental mutation after construction (the
    same Message instance may be fanned out to many clients). ``slots=True``
    reduces per-instance memory, which matters when the consumer holds a
    backlog of messages during fan-out.

    Fields are intentionally a small superset of all message types rather
    than a class hierarchy: a flat structure is cheaper to encode/decode,
    easier to log, and reads naturally as JSON. Validation (below) enforces
    which fields are required per ``type``.

    Field notes:
      * ``ts``        — ISO-8601 UTC timestamp, second precision. Stored as
                        a string (not datetime) so the wire representation
                        is exact and no timezone conversion is implied.
      * ``origin_node`` — set by the producing node; used for debugging and
                        logs only (Item 6: stream-only delivery means it is
                        NOT used for routing).
      * ``room``      — reserved for future named rooms (Item 1). Defaults
                        to "general" so v1 is a single global room.
      * ``messages``  — only used by TYPE_HISTORY; a tuple (immutable) of
                        nested Message-like dicts, already validated.
      * ``code``/``detail`` — only used by TYPE_ERROR.
    """

    type: str
    version: int = PROTOCOL_VERSION
    sender: str | None = None
    content: str | None = None
    ts: str = field(default_factory=lambda: _utc_now_iso())
    origin_node: str | None = None
    room: str = "general"
    # TYPE_HISTORY payload: tuple of already-validated message dicts.
    messages: tuple[dict[str, Any], ...] = ()
    # TYPE_ERROR payload.
    code: str | None = None
    detail: str | None = None

    # serialization 

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-ready dict, omitting None/empty optional fields.

        Omitting absent optionals keeps the wire compact and makes logs
        easier to read. We never omit ``type`` or ``version`` — those are
        always present so the receiver can dispatch and version-check.
        """
        out: dict[str, Any] = {
            "type": self.type,
            "version": self.version,
        }
        if self.sender is not None:
            out["sender"] = self.sender
        if self.content is not None:
            out["content"] = self.content
        if self.ts:
            out["ts"] = self.ts
        if self.origin_node is not None:
            out["origin_node"] = self.origin_node
        if self.room and self.room != "general":
            # Only emit non-default room to stay backward-compatible with
            # v1 peers that assume a single global room.
            out["room"] = self.room
        if self.messages:
            # Convert the tuple back to a list for JSON.
            out["messages"] = [dict(m) for m in self.messages]
        if self.code is not None:
            out["code"] = self.code
        if self.detail is not None:
            out["detail"] = self.detail
        return out


def _utc_now_iso() -> str:
    """Current UTC time as ISO-8601 with 'Z' suffix, second precision.

    Second precision is deliberate: sub-second timestamps add bytes and
    invite clients to sort by wall clock, which is incorrect across nodes
    (ordering must come from the Redis Stream ID)
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


# Username validation


def is_username_valid(name: Any) -> bool:
    """Return True iff *name* is a well-formed username.

    Accepts only str matching USERNAME_RE. Everything else (None, int,
    whitespace, too long, control chars) returns False rather than raising,
    so callers can use it as a predicate in a validation table.
    """
    return isinstance(name, str) and USERNAME_RE.fullmatch(name) is not None


# Per-type validators
# Each validator receives the raw dict and returns None (valid) or raises
# ProtocolError (invalid). Keeping them as small standalone functions makes
# the registry below readable and each rule independently testable.

def _require(obj: dict[str, Any], key: str, expected: type) -> Any:
    """Fetch obj[key] and assert it is exactly ``expected``.

    'exactly' (not isinstance) is intentional for str/int/bool to reject
    subclasses and avoid e.g. bool leaking where an int is expected. For
    ``str`` we accept only str, not bytes.
    """
    if key not in obj:
        raise ProtocolError(ERR_MISSING_FIELD, f"missing '{key}'")
    value = obj[key]
    # bool is a subclass of int; guard so True/False don't pass as int/str.
    if expected is int and isinstance(value, bool):
        raise ProtocolError(ERR_BAD_FIELD_TYPE, f"'{key}' must be int")
    if not isinstance(value, expected):
        raise ProtocolError(
            ERR_BAD_FIELD_TYPE,
            f"'{key}' must be {expected.__name__}, got {type(value).__name__}",
        )
    return value


def _validate_common(obj: dict[str, Any]) -> None:
    """Checks applied to every message regardless of type."""
    # version is mandatory and must match exactly. A future v2 peer talking
    # to a v1 peer is a ProtocolError, not a best-effort parse (fail closed).
    version = _require(obj, "version", int)
    if version != PROTOCOL_VERSION:
        raise ProtocolError(
            ERR_VERSION_MISMATCH,
            f"expected {PROTOCOL_VERSION}, got {version}",
        )
    # ts is optional on the wire but if present must be a non-empty str.
    if "ts" in obj:
        _require(obj, "ts", str)
        if not obj["ts"]:
            raise ProtocolError(ERR_BAD_FIELD_TYPE, "'ts' must be non-empty")
    # room, if present, must be a non-empty string.
    if "room" in obj:
        _require(obj, "room", str)
        if not obj["room"]:
            raise ProtocolError(ERR_BAD_FIELD_TYPE, "'room' must be non-empty")
    # origin_node, if present, must be a non-empty string.
    if "origin_node" in obj:
        _require(obj, "origin_node", str)
        if not obj["origin_node"]:
            raise ProtocolError(
                ERR_BAD_FIELD_TYPE, "'origin_node' must be non-empty"
            )


def _validate_chat(obj: dict[str, Any]) -> None:
    sender = _require(obj, "sender", str)
    if not is_username_valid(sender):
        raise ProtocolError(ERR_INVALID_USERNAME, sender)
    content = _require(obj, "content", str)
    # Empty chat is meaningless; reject to keep logs and UI clean.
    if not content:
        raise ProtocolError(ERR_BAD_FIELD_TYPE, "'content' must be non-empty")
    if len(content) > MAX_CONTENT_CHARS:
        raise ProtocolError(
            ERR_CONTENT_TOO_LONG,
            f"{len(content)} > {MAX_CONTENT_CHARS}",
        )


def _validate_join(obj: dict[str, Any]) -> None:
    sender = _require(obj, "sender", str)
    if not is_username_valid(sender):
        raise ProtocolError(ERR_INVALID_USERNAME, sender)


def _validate_leave(obj: dict[str, Any]) -> None:
    # Leave may be generated by the server on disconnect; sender is the
    # only required field so it can be attributed in the UI.
    sender = _require(obj, "sender", str)
    if not is_username_valid(sender):
        raise ProtocolError(ERR_INVALID_USERNAME, sender)


def _validate_history(obj: dict[str, Any]) -> None:
    msgs = _require(obj, "messages", list)
    if len(msgs) > MAX_HISTORY_BATCH:
        raise ProtocolError(
            ERR_BAD_FIELD_TYPE,
            f"history batch {len(msgs)} > {MAX_HISTORY_BATCH}",
        )
    # Each entry must itself be a valid chat-like dict. We do NOT recurse
    # through full decode here (history entries are chat records only) but
    # we do enforce the essential shape so the client never renders garbage.
    for i, entry in enumerate(msgs):
        if not isinstance(entry, dict):
            raise ProtocolError(
                ERR_BAD_FIELD_TYPE, f"messages[{i}] must be an object"
            )
        try:
            _validate_chat({**entry, "version": PROTOCOL_VERSION})
        except ProtocolError as exc:
            # Re-raise with index context for actionable logs.
            raise ProtocolError(
                exc.code, f"messages[{i}]: {exc.detail}"
            ) from exc


def _validate_error(obj: dict[str, Any]) -> None:
    code = _require(obj, "code", str)
    if not code:
        raise ProtocolError(ERR_BAD_FIELD_TYPE, "'code' must be non-empty")
    if "detail" in obj:
        _require(obj, "detail", str)


# The validator registry — the small extensible table referenced in the
# design. Adding a new message type later is one entry + one function;
# no if/elif ladder to touch (Open/Closed-ish).
_VALIDATORS: Final[dict[str, Callable[[dict[str, Any]], None]]] = {
    TYPE_CHAT: _validate_chat,
    TYPE_JOIN: _validate_join,
    TYPE_LEAVE: _validate_leave,
    TYPE_HISTORY: _validate_history,
    TYPE_ERROR: _validate_error,
}


def validate(obj: dict[str, Any]) -> None:
    """Validate a raw message dict against the wire contract.

    Raises ProtocolError on the first violation. Order matters:
      1. shape (must be dict)
      2. type (must be known)
      3. common checks (version, ts, room, origin_node)
      4. per-type checks
    Doing common checks first means a version mismatch is reported even
    if per-type fields are also wrong — the most actionable error wins.
    """
    if not isinstance(obj, dict):
        raise ProtocolError(ERR_NOT_AN_OBJECT, type(obj).__name__)
    msg_type = _require(obj, "type", str)
    if msg_type not in ALL_MESSAGE_TYPES:
        raise ProtocolError(ERR_UNKNOWN_TYPE, msg_type)
    _validate_common(obj)
    _VALIDATORS[msg_type](obj)


# Codec


def encode(msg: Message) -> bytes:
    """Serialize *msg* to a single NDJSON line (bytes, newline-terminated).

    Raises ProtocolError if the caller hand-built an invalid Message —
    defense in depth: even a Message constructed directly (bypassing
    decode) cannot be sent if it violates the contract.
    """
    payload = msg.to_dict()
    # Validate before serializing so invalid outbound messages fail here,
    # not opaquely on the peer.
    validate(payload)
    # separators=(',', ':') removes whitespace for compactness; ensure_ascii
    # =False keeps UTF-8 content byte-for-byte (no \\uXXXX expansion), which
    # also keeps the line shorter and logs readable.
    line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    data = line.encode("utf-8") + b"\n"
    # Enforce the byte cap on the ENCODED form (UTF-8 is variable width,
    # so char count is not a safe proxy for byte count).
    if len(data) > MAX_LINE_BYTES:
        raise ProtocolError(
            ERR_LINE_TOO_LONG,
            f"{len(data)} > {MAX_LINE_BYTES}",
        )
    return data


def decode(line: bytes | str) -> Message:
    """Parse and validate a single NDJSON line into a Message.

    Accepts bytes (the normal case from the transport) or str (convenience
    for tests and logs). A trailing newline is tolerated and stripped; any
    other trailing/leading whitespace is treated as malformed because it
    usually signals a framing bug upstream .

    Raises ProtocolError with a specific code on any violation.
    """
    # Normalize to bytes for a single, unambiguous decoding path.
    if isinstance(line, str):
        data = line.encode("utf-8")
    elif isinstance(line, bytes):
        data = line
    else:
        raise ProtocolError(ERR_BAD_FIELD_TYPE, f"line must be bytes|str")

    # Enforce the transport cap BEFORE any JSON work, so an attacker cannot
    # make us allocate a huge string. The server's StreamReader limit is the
    # first line of defense; this is the second.
    if len(data) > MAX_LINE_BYTES:
        raise ProtocolError(
            ERR_LINE_TOO_LONG, f"{len(data)} > {MAX_LINE_BYTES}"
        )

    # Strip exactly one trailing newline (and an optional CR for CRLF peers).
    if data.endswith(b"\r\n"):
        data = data[:-2]
    elif data.endswith(b"\n"):
        data = data[:-1]

    # Empty line is a framing error, not a valid message. Explicit code so
    # callers can log it distinctly (helps diagnose R1).
    if not data:
        raise ProtocolError(ERR_EMPTY_LINE)

    # Reject leading/trailing whitespace that survived newline stripping:
    # it almost always means the peer is not sending clean NDJSON.
    if data[:1].isspace() or data[-1:].isspace():
        raise ProtocolError(ERR_MALFORMED_JSON, "unexpected whitespace")

    try:
        # decode() on bytes handles UTF-8 strictly; invalid UTF-8 raises
        # UnicodeDecodeError, which we translate to a ProtocolError so the
        # caller has a single exception type to handle.
        obj = json.loads(data.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ProtocolError(ERR_MALFORMED_JSON, f"invalid utf-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ProtocolError(ERR_MALFORMED_JSON, str(exc)) from exc

    # Validate the full contract (shape, type, version, per-type rules).
    validate(obj)

    # Build the Message. Unknown top-level keys are intentionally ignored
    # here (forward-compatible evolution, documented in the module docstring).
    return Message(
        type=obj["type"],
        version=obj["version"],
        sender=obj.get("sender"),
        content=obj.get("content"),
        ts=obj.get("ts") or _utc_now_iso(),
        origin_node=obj.get("origin_node"),
        room=obj.get("room", "general"),
        messages=tuple(obj.get("messages", ())),
        code=obj.get("code"),
        detail=obj.get("detail"),
    )


# Convenience constructors

# Small factories so callers never hand-build a Message with the wrong
# field names. These also document the intended shape of each type.

def make_chat(sender: str, content: str, *, origin_node: str | None = None,
              ts: str | None = None, room: str = "general") -> Message:
    return Message(
        type=TYPE_CHAT, sender=sender, content=content,
        origin_node=origin_node, ts=ts or _utc_now_iso(), room=room,
    )


def make_join(sender: str) -> Message:
    return Message(type=TYPE_JOIN, sender=sender)


def make_leave(sender: str) -> Message:
    return Message(type=TYPE_LEAVE, sender=sender)


def make_history(messages: list[dict[str, Any]]) -> Message:
    if len(messages) > MAX_HISTORY_BATCH:
        # Defensive: never construct an outbound history frame the receiver
        # would reject. Truncate to the most recent batch.
        messages = messages[-MAX_HISTORY_BATCH:]
    return Message(type=TYPE_HISTORY, messages=tuple(messages))


def make_error(code: str, detail: str = "") -> Message:
    return Message(type=TYPE_ERROR, code=code, detail=detail or None)



# Version helper


def check_version(msg: Message) -> bool:
    """Return True iff *msg* uses this module's protocol version.

    Kept as an explicit predicate (in addition to the hard check in
    _validate_common) for callers that hold a Message constructed elsewhere
    and want to gate behavior without re-decoding.
    """
    return msg.version == PROTOCOL_VERSION


__all__ = [
    # version + vocabulary
    "PROTOCOL_VERSION",
    "TYPE_CHAT", "TYPE_JOIN", "TYPE_LEAVE", "TYPE_HISTORY", "TYPE_ERROR",
    "ALL_MESSAGE_TYPES",
    # limits
    "MAX_LINE_BYTES", "MAX_CONTENT_CHARS", "MAX_USERNAME_LEN",
    "MAX_HISTORY_BATCH", "USERNAME_RE",
    # errors
    "ProtocolError",
    "ERR_MALFORMED_JSON", "ERR_NOT_AN_OBJECT", "ERR_MISSING_FIELD",
    "ERR_UNKNOWN_TYPE", "ERR_VERSION_MISMATCH", "ERR_INVALID_USERNAME",
    "ERR_CONTENT_TOO_LONG", "ERR_LINE_TOO_LONG", "ERR_EMPTY_LINE",
    "ERR_BAD_FIELD_TYPE",
    # model + codec
    "Message", "encode", "decode", "validate",
    "is_username_valid", "check_version",
    # factories
    "make_chat", "make_join", "make_leave", "make_history", "make_error",
]   