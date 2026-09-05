"""KISS framing and AX.25 UI-frame parsing, plus APRS message extraction.

Direwolf (fed by an RTL-SDR via rtl_fm, or by any audio) exposes decoded
packets on a KISS TCP port. This module turns that byte stream into
(source, destination, text) so the Singularis Compact assembler can be fed.

References: KISS (Chepponis & Karn), AX.25 v2.2 §3, APRS spec ch. 14 (messages).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

FEND, FESC, TFEND, TFESC = 0xC0, 0xDB, 0xDC, 0xDD


class KissDecoder:
    """Incremental KISS de-framer. Feed bytes, iterate complete frame payloads (port nibble stripped)."""

    def __init__(self) -> None:
        self._buf = bytearray()
        self._in_frame = False
        self._escape = False

    def feed(self, data: bytes) -> Iterator[bytes]:
        for b in data:
            if b == FEND:
                if self._in_frame and len(self._buf) > 1:
                    payload = bytes(self._buf)
                    self._buf.clear()
                    self._in_frame = True
                    if (payload[0] & 0x0F) == 0:  # data frame on this port
                        yield payload[1:]
                    continue
                self._buf.clear()
                self._in_frame = True
                self._escape = False
                continue
            if not self._in_frame:
                continue
            if self._escape:
                self._buf.append(FEND if b == TFEND else FESC if b == TFESC else b)
                self._escape = False
            elif b == FESC:
                self._escape = True
            else:
                self._buf.append(b)


def kiss_encode(ax25_frame: bytes, port: int = 0) -> bytes:
    """Wrap an AX.25 frame as a KISS data frame (used by tests and any TX path)."""
    body = bytearray([port << 4])
    for b in ax25_frame:
        if b == FEND:
            body += bytes([FESC, TFEND])
        elif b == FESC:
            body += bytes([FESC, TFESC])
        else:
            body.append(b)
    return bytes([FEND]) + bytes(body) + bytes([FEND])


@dataclass
class Ax25Frame:
    destination: str
    source: str
    digipeaters: list[str] = field(default_factory=list)
    control: int = 0x03
    pid: int = 0xF0
    info: bytes = b""

    @property
    def is_ui(self) -> bool:
        return (self.control & 0xEF) == 0x03

    @property
    def text(self) -> str:
        return self.info.decode("utf-8", errors="replace")


def _decode_address(chunk: bytes) -> tuple[str, bool]:
    call = bytes(b >> 1 for b in chunk[:6]).decode("ascii", errors="replace").strip()
    ssid_byte = chunk[6]
    ssid = (ssid_byte >> 1) & 0x0F
    last = bool(ssid_byte & 0x01)
    return (f"{call}-{ssid}" if ssid else call), last


def _encode_address(callsign: str, last: bool, command: bool = False) -> bytes:
    call, _, ssid = callsign.upper().partition("-")
    if not (1 <= len(call) <= 6):
        raise ValueError(f"Bad callsign for AX.25 address: {callsign}")
    out = bytearray((ord(c) << 1) for c in call.ljust(6))
    s = int(ssid or 0) & 0x0F
    out.append(0x60 | (s << 1) | (1 if last else 0) | (0x80 if command else 0))
    return bytes(out)


def parse_ax25(frame: bytes) -> Ax25Frame:
    if len(frame) < 16:
        raise ValueError("AX.25 frame too short")
    dest, _ = _decode_address(frame[0:7])
    src, last = _decode_address(frame[7:14])
    pos = 14
    digis: list[str] = []
    while not last:
        if pos + 7 > len(frame):
            raise ValueError("AX.25 address field runs past frame end")
        d, last = _decode_address(frame[pos:pos + 7])
        digis.append(d.rstrip("*"))
        pos += 7
    if pos >= len(frame):
        raise ValueError("AX.25 frame has no control field")
    control = frame[pos]
    pos += 1
    pid = 0
    info = b""
    if (control & 0x01) == 0x01 and (control & 0x03) != 0x01:  # U frame (incl. UI 0x03)
        if (control & 0xEF) == 0x03:
            pid = frame[pos]
            pos += 1
            info = frame[pos:]
    return Ax25Frame(destination=dest, source=src, digipeaters=digis, control=control, pid=pid, info=info)


def build_ui_frame(source: str, destination: str, info: str | bytes, digipeaters: list[str] | None = None) -> bytes:
    """Build an AX.25 UI frame (what Direwolf/gen_packets produce for APRS)."""
    digis = digipeaters or []
    addr = bytearray(_encode_address(destination, last=False, command=True))
    addr += _encode_address(source, last=not digis)
    for i, d in enumerate(digis):
        addr += _encode_address(d, last=(i == len(digis) - 1))
    payload = info.encode("utf-8") if isinstance(info, str) else info
    return bytes(addr) + bytes([0x03, 0xF0]) + payload


@dataclass
class HeardText:
    source: str
    destination: str
    text: str
    kind: str  # "aprs-message" | "text"
    addressee: str | None = None
    msg_no: str | None = None


def extract_text(frame: Ax25Frame) -> HeardText | None:
    """Pull the human/agent text out of an APRS packet.

    APRS message: info = ':' + 9-char addressee + ':' + text [+ '{' msgno]. Anything else
    is returned whole as plain text (status, comments) so the membrane still hears it.
    """
    if not frame.is_ui or not frame.info:
        return None
    text = frame.text
    if len(text) >= 11 and text[0] == ":" and text[10] == ":":
        addressee = text[1:10].strip()
        body = text[11:]
        msg_no = None
        if "{" in body:
            body, _, msg_no = body.rpartition("{")
        if body.startswith("ack") or body.startswith("rej"):
            return HeardText(frame.source, frame.destination, body, "aprs-ack", addressee, msg_no)
        return HeardText(frame.source, frame.destination, body, "aprs-message", addressee, msg_no)
    return HeardText(frame.source, frame.destination, text, "text")


def aprs_message_info(addressee: str, text: str, msg_no: str | None = None) -> str:
    """Info field for an APRS message (≤67 chars of text per spec)."""
    if len(text) > 67:
        raise ValueError("APRS message text is limited to 67 characters")
    return f":{addressee.upper()[:9].ljust(9)}:{text}" + (f"{{{msg_no}" if msg_no else "")
