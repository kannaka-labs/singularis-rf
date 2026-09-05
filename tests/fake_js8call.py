"""A protocol-faithful stand-in for JS8Call's TCP JSON API, for tests.

Implements the request/response shapes the driver relies on (with _ID echo) and the
TX lifecycle: TX.SEND_MESSAGE → TX.TEXT(value) → one TX.FRAME event per 'over' →
TX.TEXT("") when done. Optionally loops a directed reply back as RX.DIRECTED so the
receive side can be exercised in the same run. It records every request it saw.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeJs8Call:
    callsign: str = "AE0RM"
    grid: str = "EM48"
    chars_per_over: int = 12          # JS8 normal speed carries roughly this much free text per 15 s frame
    over_seconds: float = 0.05        # compressed time for tests
    loopback_from: str | None = None  # if set, every sent message is echoed back as RX.DIRECTED from this call
    requests: list[dict[str, Any]] = field(default_factory=list)
    sent_texts: list[str] = field(default_factory=list)
    _server: asyncio.AbstractServer | None = None
    _writers: list[asyncio.StreamWriter] = field(default_factory=list)
    port: int = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for w in self._writers:
            w.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _emit(self, writer: asyncio.StreamWriter, type_: str, value: str = "", params: dict[str, Any] | None = None) -> None:
        writer.write((json.dumps({"type": type_, "value": value, "params": params or {}}) + "\n").encode())
        await writer.drain()

    async def emit_all(self, type_: str, value: str = "", params: dict[str, Any] | None = None) -> None:
        for w in list(self._writers):
            try:
                await self._emit(w, type_, value, params)
            except (ConnectionError, OSError):
                pass

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.append(writer)
        self._tx_text = ""
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line)
                self.requests.append(msg)
                t, value, params = msg.get("type"), msg.get("value", ""), msg.get("params", {}) or {}
                echo = {"_ID": params["_ID"]} if "_ID" in params else {}
                if t == "STATION.GET_CALLSIGN":
                    await self._emit(writer, "STATION.CALLSIGN", self.callsign, echo)
                elif t == "STATION.GET_GRID":
                    await self._emit(writer, "STATION.GRID", self.grid, echo)
                elif t == "RIG.GET_FREQ":
                    await self._emit(writer, "RIG.FREQ", "", {**echo, "DIAL": 7078000, "FREQ": 7079000, "OFFSET": 1000})
                elif t == "MODE.GET_SPEED":
                    await self._emit(writer, "MODE.SPEED", "", {**echo, "SPEED": 0})
                elif t == "TX.GET_TEXT":
                    await self._emit(writer, "TX.TEXT", self._tx_text, echo)
                elif t == "TX.SET_TEXT":
                    self._tx_text = value
                    await self._emit(writer, "TX.TEXT", self._tx_text, echo)
                elif t == "TX.SEND_MESSAGE":
                    self._tx_text = value
                    self.sent_texts.append(value)
                    await self._emit(writer, "TX.TEXT", self._tx_text, echo)
                    asyncio.create_task(self._transmit(writer, value))
                elif t == "PING":
                    await self._emit(writer, "PING", "", echo)
                else:
                    await self._emit(writer, "ERROR", f"unknown type {t}", echo)
        except (ConnectionError, OSError, json.JSONDecodeError):
            pass
        finally:
            if writer in self._writers:
                self._writers.remove(writer)

    async def _transmit(self, writer: asyncio.StreamWriter, text: str) -> None:
        # The station's tx buffer shrinks as overs go out, then empties.
        remaining = text
        while remaining:
            await asyncio.sleep(self.over_seconds)
            chunk, remaining = remaining[: self.chars_per_over], remaining[self.chars_per_over:]
            self._tx_text = remaining
            await self._emit(writer, "TX.FRAME", chunk, {"TONES": [0] * 79})
            await self._emit(writer, "TX.TEXT", remaining)
        # Everyone connected sees the tx text empty (the station polls TX.GET_TEXT too).
        self._tx_done = True
        # Reflect the transmission back as a directed message from the far end, if requested.
        if self.loopback_from:
            to, _, body = text.partition(": ")
            await asyncio.sleep(self.over_seconds)
            await self.emit_all("RX.DIRECTED", f"{self.loopback_from}: {self.callsign} {body}", {"FROM": self.loopback_from, "TO": self.callsign, "TEXT": body, "SNR": 5, "FREQ": 7079000})
        # Keep TX.GET_TEXT consistent after completion.
        self._last_tx = ""

    _tx_done: bool = False
    _last_tx: str = ""
