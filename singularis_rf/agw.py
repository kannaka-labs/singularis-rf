"""Minimal AGWPE client for Direwolf's AGW port (default 8000) — monitoring only.

Direwolf speaks the AGWPE TCP protocol: fixed 36-byte little-endian header
  port(1) pad(3) datakind(1) pad(1) pid(1) pad(1) callfrom(10) callto(10) datalen(4) user(4)
followed by `datalen` bytes. After a client sends 'm' (enable monitoring), Direwolf
emits 'U' frames for received UI packets and 'T' frames for packets it TRANSMITTED.
That 'T' is the only confirmation the radio side gives that a KISS frame went out,
so the APRS transmitter uses it as its "frame on the air" signal.
"""
from __future__ import annotations

import asyncio
import struct
import sys
from dataclasses import dataclass
from typing import AsyncIterator

HEADER = struct.Struct("<B3xB1xB1x10s10sII")
HEADER_LEN = HEADER.size  # 36


@dataclass
class AgwFrame:
    port: int
    kind: str
    pid: int
    call_from: str
    call_to: str
    data: bytes


def encode_frame(kind: str, call_from: str = "", call_to: str = "", data: bytes = b"", port: int = 0, pid: int = 0) -> bytes:
    return HEADER.pack(port, ord(kind), pid, call_from.encode().ljust(10, b"\0"), call_to.encode().ljust(10, b"\0"), len(data), 0) + data


def decode_frames(buf: bytearray) -> list[AgwFrame]:
    """Consume complete frames from buf (mutated); return them."""
    out: list[AgwFrame] = []
    while len(buf) >= HEADER_LEN:
        port, kind, pid, cf, ct, dlen, _user = HEADER.unpack(bytes(buf[:HEADER_LEN]))
        if len(buf) < HEADER_LEN + dlen:
            break
        data = bytes(buf[HEADER_LEN:HEADER_LEN + dlen])
        del buf[:HEADER_LEN + dlen]
        out.append(AgwFrame(port, chr(kind), pid, cf.rstrip(b"\0").decode("ascii", "replace"), ct.rstrip(b"\0").decode("ascii", "replace"), data))
    return out


class AgwMonitor:
    """Connect, enable monitoring, and yield 'T' (transmitted) and 'U' (received) frames."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.host, self.port = host, port
        self.connected = False

    async def frames(self) -> AsyncIterator[AgwFrame]:
        attempt = 0
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                self.connected = True
                attempt = 0
                writer.write(encode_frame("m"))
                await writer.drain()
                buf = bytearray()
                while True:
                    data = await reader.read(4096)
                    if not data:
                        raise ConnectionError("AGW peer closed")
                    buf += data
                    for f in decode_frames(buf):
                        yield f
            except (OSError, ConnectionError) as e:
                self.connected = False
                attempt += 1
                print(f"[agw] {e}; reconnecting", file=sys.stderr)
                await asyncio.sleep(min(60, 2 ** min(attempt, 6)))


def monitor_text(frame: AgwFrame) -> str:
    """Direwolf's monitor text is 'SRC>DST,PATH:info' possibly with a trailing newline."""
    return frame.data.decode("utf-8", "replace").strip()
