"""Singularis Compact (SP1) codec — Python, driven by the published spec.

The block and enum tables are NOT re-typed here. They are loaded from the spec
JSON that the Space Child Research Collective publishes at
https://research.spacechild.love/spec/singularis-compact.json (a copy is
bundled in spec/). The TypeScript reference implementation renders that spec
from its own tables, so this decoder cannot drift from the wire format unless
the published spec changes — and then it follows the spec.

Wire format (see the spec page for the full grammar):

    FROM>TO:SP1 MM:S/N PAYLOAD #CCCC

Cross-checked against the TypeScript encoder with tests/vectors.json.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

PROTOCOL = "SP1"
DEFAULT_FRAME_LENGTH = 67
MIN_FRAME_LENGTH = 40
BUNDLED_SPEC = Path(__file__).resolve().parent.parent / "spec" / "singularis-compact.json"

_RESERVED = set("+%;=|#~>: ")
_FRAME_RE = re.compile(
    r"^([A-Za-z0-9-]{1,16}|\*)>([A-Za-z0-9-]{1,16}|\*):(SP\d+) ([0-9A-Za-z]{2}):(\d+)/(\d+) (.*) #([0-9A-Fa-f]{4})$"
)
_IDENT_RE = re.compile(r"^[A-Z0-9][A-Z0-9-]{0,15}$")


class CodecError(ValueError):
    """Malformed frame or payload."""


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FieldSpec:
    key: str
    field: str
    kind: str
    required: bool
    codes: dict[str, str] | None


@dataclass
class Spec:
    protocol: str
    version: str
    blocks: dict[str, str]                      # code -> type
    fields: dict[str, list[FieldSpec]]          # code -> fields
    raw: dict[str, Any]

    @property
    def code_by_type(self) -> dict[str, str]:
        return {v: k for k, v in self.blocks.items()}

    @classmethod
    def load(cls, path: str | Path = BUNDLED_SPEC) -> "Spec":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_json(data)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Spec":
        blocks = {code: b["type"] for code, b in data["blocks"].items()}
        fields = {
            code: [FieldSpec(f["key"], f["field"], f["kind"], bool(f.get("required")), f.get("codes")) for f in b["fields"]]
            for code, b in data["blocks"].items()
        }
        return cls(protocol=data["protocol"], version=data["version"], blocks=blocks, fields=fields, raw=data)


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------

def crc16(text: str) -> str:
    """CRC-16/CCITT-FALSE, uppercase 4-hex. check('123456789') == '29B1'."""
    crc = 0xFFFF
    for byte in text.encode("utf-8"):
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return f"{crc:04X}"


def escape_value(value: str) -> str:
    out: list[str] = []
    for ch in value:
        if ch == " ":
            out.append("+")
        elif ch in _RESERVED or not (0x20 <= ord(ch) <= 0x7E):
            out.extend(f"%{b:02X}" for b in ch.encode("utf-8"))
        else:
            out.append(ch)
    return "".join(out)


def unescape_value(token: str) -> str:
    out = bytearray()
    i = 0
    while i < len(token):
        ch = token[i]
        if ch == "%" and re.fullmatch(r"[0-9A-Fa-f]{2}", token[i + 1:i + 3] or ""):
            out.append(int(token[i + 1:i + 3], 16))
            i += 3
            continue
        out.extend((" " if ch == "+" else ch).encode("utf-8"))
        i += 1
    return out.decode("utf-8")


def normalize_identifier(ident: str) -> str:
    if ident in ("", "*"):
        return "*"
    up = re.sub(r"[^A-Z0-9-]+", "-", ident.upper()).strip("-")[:16]
    if not _IDENT_RE.match(up):
        raise CodecError(f"Invalid station/agent identifier: {ident}")
    return up


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------

def _code_for(codes: dict[str, str], value: str, what: str) -> str:
    for k, v in codes.items():
        if v == value:
            return k
    raise CodecError(f"Unknown {what} value: {value}")


def _value_for(codes: dict[str, str], code: str, what: str) -> str:
    for cand in (code, code.upper(), code.lower()):
        if cand in codes:
            return codes[cand]
    raise CodecError(f"Unknown {what} code: {code}")


def _format_unit(x: float) -> str:
    x = max(0.0, min(1.0, float(x)))
    s = f"{x:.3f}"
    if s.startswith("0"):
        s = s[1:]
    s = s.rstrip("0")
    if s.endswith("."):
        s += "0"
    if s == ".0":
        return "0"
    if s == "1.0":
        return "1"
    return s


def encode_block(spec: Spec, block: dict[str, Any]) -> str:
    code = spec.code_by_type.get(block.get("type", ""))
    if not code:
        raise CodecError(f"Unknown block type: {block.get('type')}")
    tokens = [code]
    for f in spec.fields[code]:
        raw = block.get(f.field)
        if raw is None or (isinstance(raw, list) and not raw and not f.required):
            if f.required:
                raise CodecError(f"{block['type']} is missing required field {f.field}")
            continue
        kind = f.kind
        if kind == "text":
            v = escape_value(str(raw))
        elif kind == "list":
            v = ";".join(escape_value(str(s)) for s in raw)
        elif kind == "targets":
            v = ";".join(escape_value(str(s)) for s in (raw if isinstance(raw, list) else [raw]))
        elif kind == "enum":
            v = _code_for(f.codes or {}, str(raw), f.field)
        elif kind == "enumlist":
            v = "".join(_code_for(f.codes or {}, s, f.field) for s in raw)
        elif kind == "bool":
            v = "1" if raw else "0"
        elif kind == "int":
            v = str(int(raw))
        elif kind == "unit":
            v = _format_unit(raw)
        elif kind == "pct":
            v = str(max(0, min(100, round(float(raw)))))
        else:
            raise CodecError(f"Unknown field kind {kind}")
        if v == "" and not f.required:
            continue
        tokens.append(f"{f.key}={v}")
    return " ".join(tokens)


def decode_block(spec: Spec, tokens: list[str]) -> dict[str, Any]:
    if not tokens:
        raise CodecError("Empty block")
    code = tokens[0].upper()
    if code not in spec.blocks:
        raise CodecError(f"Unknown block code: {tokens[0]}")
    fields = spec.fields[code]
    block: dict[str, Any] = {"type": spec.blocks[code]}
    seen: set[str] = set()
    for tok in tokens[1:]:
        eq = tok.find("=")
        if eq < 1:
            raise CodecError(f"Malformed token in {block['type']}: {tok}")
        key, val = tok[:eq], tok[eq + 1:]
        f = next((x for x in fields if x.key == key or x.key == key.lower()), None)
        if f is None:
            raise CodecError(f"Unknown key '{key}' for {block['type']}")
        if f.key in seen:
            raise CodecError(f"Duplicate key '{key}' in {block['type']}")
        seen.add(f.key)
        kind = f.kind
        if kind == "text":
            block[f.field] = unescape_value(val)
        elif kind == "list":
            block[f.field] = [] if val == "" else [unescape_value(p) for p in val.split(";")]
        elif kind == "targets":
            parts = [unescape_value(p) for p in val.split(";")]
            block[f.field] = parts[0] if len(parts) == 1 else parts
        elif kind == "enum":
            block[f.field] = _value_for(f.codes or {}, val, f.field)
        elif kind == "enumlist":
            block[f.field] = [_value_for(f.codes or {}, c, f.field) for c in val]
        elif kind == "bool":
            if val not in ("0", "1"):
                raise CodecError(f"Boolean {f.field} must be 0 or 1")
            block[f.field] = val == "1"
        elif kind == "int":
            if not re.fullmatch(r"-?\d+", val):
                raise CodecError(f"Integer {f.field} malformed: {val}")
            block[f.field] = int(val)
        elif kind == "unit":
            try:
                n = float(val)
            except ValueError as e:
                raise CodecError(f"Unit {f.field} malformed: {val}") from e
            if not 0 <= n <= 1:
                raise CodecError(f"Unit {f.field} out of range: {val}")
            block[f.field] = n
        elif kind == "pct":
            if not re.fullmatch(r"\d+", val) or not 0 <= int(val) <= 100:
                raise CodecError(f"Percent {f.field} out of range: {val}")
            block[f.field] = int(val)
        else:
            raise CodecError(f"Unknown field kind {kind}")
    for f in fields:
        if f.required and f.field not in block:
            raise CodecError(f"{block['type']} is missing required field {f.field} (key {f.key})")
    return block


# ---------------------------------------------------------------------------
# Frames
# ---------------------------------------------------------------------------

@dataclass
class Frame:
    from_: str
    to: str
    message_id: str
    seq: int
    total: int
    payload: str
    continues: bool
    crc: str
    crc_ok: bool
    raw: str


@dataclass
class Message:
    from_: str
    to: str
    message_id: str
    blocks: list[dict[str, Any]]
    frames: int
    experiment_id: str | None = None
    session_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = {"from": self.from_, "to": self.to, "messageId": self.message_id, "blocks": self.blocks, "frames": self.frames}
        if self.experiment_id is not None:
            d["experimentId"] = self.experiment_id
        if self.session_id is not None:
            d["sessionId"] = self.session_id
        return d


def parse_frame(line: str) -> Frame:
    raw = line.strip()
    m = _FRAME_RE.match(raw)
    if not m:
        raise CodecError("Not a Singularis Compact frame")
    frm, to, proto, mid, seq, total, payload, crc = m.groups()
    if proto.upper() != PROTOCOL:
        raise CodecError(f"Unsupported protocol {proto}")
    body = raw[: raw.rfind(" #")]
    continues = payload.endswith("~")
    return Frame(
        from_=frm.upper(), to=to.upper(), message_id=mid.upper(), seq=int(seq), total=int(total),
        payload=payload[:-1] if continues else payload, continues=continues,
        crc=crc.upper(), crc_ok=crc16(body) == crc.upper(), raw=raw,
    )


def looks_like_frame(text: str) -> bool:
    return bool(_FRAME_RE.match(text.strip()))


def decode_payload(spec: Spec, payload: str) -> tuple[list[dict[str, Any]], str | None, str | None]:
    tokens = [t for t in payload.split(" ") if t]
    experiment_id = session_id = None
    while tokens and tokens[0].startswith("@"):
        t = tokens.pop(0)
        key = t[:3].lower()  # meta keys are case-insensitive like every other code
        if key == "@x=":
            experiment_id = unescape_value(t[3:])
        elif key == "@s=":
            session_id = unescape_value(t[3:])
        else:
            raise CodecError(f"Unknown meta token {t}")
    groups: list[list[str]] = [[]]
    for t in tokens:
        if t == "|":
            groups.append([])
        else:
            groups[-1].append(t)
    blocks = [decode_block(spec, g) for g in groups if g]
    if not blocks:
        raise CodecError("Payload carries no blocks")
    return blocks, experiment_id, session_id


def decode_frames(spec: Spec, lines: Iterable[str]) -> Message:
    frames = [parse_frame(l) for l in lines]
    if not frames:
        raise CodecError("No frames")
    bad = [f.seq for f in frames if not f.crc_ok]
    if bad:
        raise CodecError(f"CRC failure in frame {','.join(map(str, bad))}")
    first = frames[0]
    for f in frames:
        if (f.from_, f.to, f.message_id, f.total) != (first.from_, first.to, first.message_id, first.total):
            raise CodecError("Frames belong to different messages")
    by_seq = {f.seq: f for f in frames}
    payload = ""
    for s in range(1, first.total + 1):
        f = by_seq.get(s)
        if f is None:
            raise CodecError(f"Missing frame {s}/{first.total}")
        prev = by_seq.get(s - 1)
        payload += f.payload if (s == 1 or (prev and prev.continues)) else f" {f.payload}"
    blocks, exp, sess = decode_payload(spec, payload)
    return Message(first.from_, first.to, first.message_id, blocks, first.total, exp, sess)


def encode_payload(spec: Spec, blocks: list[dict[str, Any]], experiment_id: str | None = None, session_id: str | None = None) -> str:
    if not blocks:
        raise CodecError("A message needs at least one block")
    head = []
    if experiment_id:
        head.append(f"@x={escape_value(experiment_id)}")
    if session_id:
        head.append(f"@s={escape_value(session_id)}")
    return " ".join(head + [" | ".join(encode_block(spec, b) for b in blocks)])


def _header(frm: str, to: str, mid: str, seq: int, total: int) -> str:
    return f"{frm}>{to}:{PROTOCOL} {mid}:{seq}/{total} "


_BASE36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def random_message_id() -> str:
    import random
    return random.choice(_BASE36) + random.choice(_BASE36)


def encode_message(spec: Spec, blocks: list[dict[str, Any]], *, from_: str, to: str | None = None, message_id: str,
                   experiment_id: str | None = None, session_id: str | None = None, frame_length: int = DEFAULT_FRAME_LENGTH,
                   case_fold: bool = False) -> list[str]:
    """Mirror of the TypeScript encoder (used for tests and for the TX path).

    case_fold=True upper-cases the whole body BEFORE the CRC is computed, for transports that
    fold case (some digital modes send uppercase only). Codes and keys are case-insensitive so
    nothing structural is lost; free-text values come out uppercase (documented as lossy).
    """
    frm = normalize_identifier(from_)
    dest = normalize_identifier(to or "*")
    mid = message_id.upper()
    if not re.fullmatch(r"[0-9A-Z]{2}", mid):
        raise CodecError("message_id must be two base-36 characters")
    frame_length = max(MIN_FRAME_LENGTH, frame_length)
    tokens = encode_payload(spec, blocks, experiment_id, session_id).split(" ")

    def layout(total_guess: int) -> list[str]:
        capacity = frame_length - len(_header(frm, dest, mid, total_guess, total_guess)) - 6
        if capacity < 8:
            raise CodecError(f"frame_length {frame_length} leaves no room for payload with these identifiers")
        frames: list[str] = []
        cur = ""
        for tok in tokens:
            if len(tok) <= capacity:
                cand = f"{cur} {tok}" if cur else tok
                if len(cand) <= capacity:
                    cur = cand
                    continue
                if cur:
                    frames.append(cur)
                cur = tok
                continue
            if cur:
                frames.append(cur)
                cur = ""
            rest = tok
            while len(rest) > capacity:
                cut = capacity - 1
                tail = rest[:cut]
                pct = tail.rfind("%")
                if pct >= 0 and pct > cut - 3:
                    cut = pct
                frames.append(rest[:cut] + "~")
                rest = rest[cut:]
            cur = rest
        if cur:
            frames.append(cur)
        return frames

    payloads = layout(9)
    if len(payloads) > 9:
        payloads = layout(len(payloads))
    total = len(payloads)
    out = []
    for i, p in enumerate(payloads, 1):
        body = _header(frm, dest, mid, i, total) + p
        if case_fold:
            body = body.upper()
        out.append(f"{body} #{crc16(body)}")
    return out


# ---------------------------------------------------------------------------
# Receiver-side reassembly
# ---------------------------------------------------------------------------

@dataclass
class _Pending:
    frames: dict[int, Frame] = field(default_factory=dict)
    first_seen: float = 0.0


class FrameAssembler:
    """Feed heard lines in any order; get a Message back when one completes."""

    def __init__(self, spec: Spec, ttl_s: float = 15 * 60):
        self.spec = spec
        self.ttl_s = ttl_s
        self._pending: dict[str, _Pending] = {}

    def feed(self, line: str, now: float | None = None) -> tuple[Message, list[Frame]] | None:
        now = time.time() if now is None else now
        try:
            frame = parse_frame(line)
        except CodecError:
            return None
        if not frame.crc_ok:
            return None
        self._expire(now)
        key = f"{frame.from_}>{frame.to}:{frame.message_id}:{frame.total}"
        entry = self._pending.setdefault(key, _Pending(first_seen=now))
        entry.frames[frame.seq] = frame
        if len(entry.frames) < frame.total:
            return None
        del self._pending[key]
        frames = sorted(entry.frames.values(), key=lambda f: f.seq)
        try:
            return decode_frames(self.spec, [f.raw for f in frames]), frames
        except CodecError:
            return None

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def _expire(self, now: float) -> None:
        for k in [k for k, v in self._pending.items() if now - v.first_seen > self.ttl_s]:
            del self._pending[k]
