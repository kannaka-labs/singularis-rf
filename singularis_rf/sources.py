"""Input sources for the RX-only decoder. Each yields HeardText items.

- KissTcpSource   : Direwolf (or any TNC) KISS over TCP → AX.25 → APRS/plain text
- Js8CallSource   : JS8Call's TCP JSON API (RX.DIRECTED / RX.ACTIVITY)
- LineSource      : newline-delimited text from stdin/a file (tests, other decoders)

All sources reconnect with backoff and never raise into the service loop.
"""
from __future__ import annotations

import asyncio
import json
import sys
from typing import AsyncIterator

from .ax25 import HeardText, KissDecoder, extract_text, parse_ax25


async def _backoff(attempt: int) -> None:
    await asyncio.sleep(min(60, 2 ** min(attempt, 6)))


class KissTcpSource:
    def __init__(self, host: str = "127.0.0.1", port: int = 8001, name: str = "direwolf"):
        self.host, self.port, self.name = host, port, name
        self.connected = False
        self.frames = 0
        self.bad_frames = 0

    async def run(self) -> AsyncIterator[HeardText]:
        attempt = 0
        while True:
            try:
                reader, _writer = await asyncio.open_connection(self.host, self.port)
                self.connected = True
                attempt = 0
                print(f"[kiss] connected to {self.host}:{self.port}", file=sys.stderr)
                dec = KissDecoder()
                while True:
                    data = await reader.read(4096)
                    if not data:
                        raise ConnectionError("KISS peer closed")
                    for raw in dec.feed(data):
                        self.frames += 1
                        try:
                            heard = extract_text(parse_ax25(raw))
                        except ValueError as e:
                            self.bad_frames += 1
                            print(f"[kiss] undecodable AX.25 frame: {e}", file=sys.stderr)
                            continue
                        if heard:
                            yield heard
            except (OSError, ConnectionError) as e:
                self.connected = False
                attempt += 1
                print(f"[kiss] {e}; reconnecting", file=sys.stderr)
                await _backoff(attempt)


class Js8CallSource:
    """JS8Call TCP API (Settings → Reporting → API, default 127.0.0.1:2442). Newline-delimited JSON."""

    def __init__(self, host: str = "127.0.0.1", port: int = 2442, name: str = "js8call"):
        self.host, self.port, self.name = host, port, name
        self.connected = False
        self.frames = 0

    async def run(self) -> AsyncIterator[HeardText]:
        attempt = 0
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                self.connected = True
                attempt = 0
                print(f"[js8call] connected to {self.host}:{self.port}", file=sys.stderr)
                writer.write((json.dumps({"type": "STATION.GET_CALLSIGN", "params": {}}) + "\n").encode())
                await writer.drain()
                while True:
                    line = await reader.readline()
                    if not line:
                        raise ConnectionError("JS8Call closed the API socket")
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    kind = msg.get("type", "")
                    params = msg.get("params", {}) or {}
                    if kind in ("RX.DIRECTED", "RX.ACTIVITY"):
                        text = str(msg.get("value") or params.get("TEXT") or "").strip()
                        if not text:
                            continue
                        self.frames += 1
                        yield HeardText(
                            source=str(params.get("FROM") or "UNKNOWN"),
                            destination=str(params.get("TO") or "*"),
                            text=text,
                            kind="js8-directed" if kind == "RX.DIRECTED" else "js8-activity",
                        )
            except (OSError, ConnectionError) as e:
                self.connected = False
                attempt += 1
                print(f"[js8call] {e}; reconnecting", file=sys.stderr)
                await _backoff(attempt)


class LineSource:
    """Text lines, optionally 'FROM>TO:text' prefixed; else source=stdin, destination='*'."""

    def __init__(self, stream=None, name: str = "stdin"):
        self.stream = stream or sys.stdin
        self.name = name
        self.connected = True
        self.frames = 0

    async def run(self) -> AsyncIterator[HeardText]:
        loop = asyncio.get_event_loop()
        while True:
            line = await loop.run_in_executor(None, self.stream.readline)
            if not line:
                self.connected = False
                return
            line = line.rstrip("\n")
            if not line.strip():
                continue
            self.frames += 1
            src, dst, text = "STDIN", "*", line
            if ">" in line and ":" in line and line.index(">") < line.index(":") and " " not in line[: line.index(":")]:
                # Looks like a Singularis frame or a monitor-format line; hand the whole line over as text.
                pass
            yield HeardText(source=src, destination=dst, text=text, kind="text")
